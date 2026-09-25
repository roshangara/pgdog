//! One LSN check per server, shared by every pool of that server.
//!
//! PgDog keeps a pool per (user, database, server), and each pool ran its
//! own LSN check; with `replica_down_detection` on a connection of its own,
//! held for good. A door serving many tenants paid a server connection per
//! tenant per server, to learn many times over the same four numbers: is the
//! server in recovery, its WAL position, its timeline, how old its replay
//! is. Here the check runs once per server (host and port), on one
//! connection, and its result goes to every pool of that server: a pool that
//! joins later has its role and lag at once, and an idle tenant's pools can
//! hold nothing.
//!
//! The check logs in with the credentials of one of the server's pools and
//! keeps them while they work. A login refused for that user or database (a
//! tenant dropped, a password changed) is not the server failing: another
//! pool's credentials are tried. A connection that can't be made, or a check
//! not answered in `lsn_check_timeout`, is the server failing: every pool of
//! it is told (`Pool::lsn_check_failed`; a replica leaves reads at once).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use super::Pool;
use super::lsn_monitor::{AURORA_DETECTION_QUERY, AURORA_LSN_QUERY, LSN_QUERY, LsnStats};
use crate::backend::{ConnectReason, DisconnectReason, Error as BackendError, Server};
use crate::net::DataRow;
use crate::tasks;
use crate::util::{safe_interval, safe_sleep, safe_timeout};

/// Pools whose credentials a check tries in one round when logins are
/// refused.
const LOGINS_PER_ROUND: usize = 3;

type Key = (String, u16);

static CHECKS: Lazy<Mutex<HashMap<Key, Arc<ServerCheck>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[derive(Default)]
struct ServerCheck {
    /// Pools of this server, oldest first.
    pools: Mutex<Vec<Pool>>,
    /// The last stats the check read.
    latest: Mutex<Option<LsnStats>>,
    /// The check holds a server connection now.
    connected: AtomicBool,
    shutdown: CancellationToken,
}

fn key(pool: &Pool) -> Key {
    (pool.addr().host.clone(), pool.addr().port)
}

/// A pool of this server came online: its stats come from the server's
/// check, started with the first pool.
pub(super) fn join(pool: &Pool) {
    let (check, started) = {
        let mut checks = CHECKS.lock();
        let check = checks.entry(key(pool)).or_default().clone();
        let mut pools = check.pools.lock();
        let started = !pools.is_empty();
        pools.push(pool.clone());
        drop(pools);
        (check, started)
    };

    let latest = *check.latest.lock();
    if let Some(stats) = latest {
        pool.store_lsn_stats(stats);
    }

    if !started {
        let key = key(pool);
        tasks::spawn("server lsn check", async move { check.run(key).await });
    }
}

/// The pool went offline. The last pool of a server stops its check and
/// closes its connection.
pub(super) fn leave(pool: &Pool) {
    let mut checks = CHECKS.lock();
    let key = key(pool);
    let Some(check) = checks.get(&key) else {
        return;
    };
    let empty = {
        let mut pools = check.pools.lock();
        pools.retain(|p| p.id() != pool.id());
        pools.is_empty()
    };
    if empty {
        check.shutdown.cancel();
        checks.remove(&key);
    }
}

/// Server connections the checks hold, by server.
pub(crate) fn connections() -> Vec<(Key, usize)> {
    CHECKS
        .lock()
        .iter()
        .filter(|(_, check)| check.connected.load(Ordering::Relaxed))
        .map(|(key, _)| (key.clone(), 1))
        .collect()
}

/// A login that failed.
enum Refusal {
    /// The server refused this user or database: it answered.
    Credentials(String),
    /// The server did not answer.
    Down(String),
}

/// What one round learned.
enum Round {
    Stats(LsnStats),
    /// The server answered but nothing is known of its WAL yet: every
    /// login was refused, or Aurora detection is not done.
    Unknown,
    Failed(String),
}

/// The check's connection and whose credentials it used: a user and a
/// database, which outlive their pools (a reload makes new pools of the
/// same users).
struct Own {
    server: Box<Server>,
    user: String,
    database: String,
}

impl Own {
    fn of(&self, pool: &Pool) -> bool {
        pool.addr().user == self.user && pool.addr().database_name == self.database
    }
}

impl ServerCheck {
    async fn run(&self, key: Key) {
        let first = self.pools.lock().first().cloned();
        let Some(first) = first else {
            return;
        };
        let config = *first.config();

        select! {
            _ = safe_sleep(config.lsn_check_delay) => {}
            _ = self.shutdown.cancelled() => return,
        }

        debug!("lsn check running [{}:{}]", key.0, key.1);

        let mut interval = safe_interval(config.lsn_check_interval);
        let mut own: Option<Own> = None;
        let mut aurora: Option<bool> = None;
        let mut next = 0;

        loop {
            select! {
                _ = interval.tick() => {}
                _ = self.shutdown.cancelled() => break,
            }

            let pools = self.pools.lock().clone();
            if pools.is_empty() {
                continue;
            }

            match self.round(&pools, &mut own, &mut aurora, &mut next).await {
                Round::Stats(stats) => {
                    *self.latest.lock() = Some(stats);
                    for pool in &pools {
                        pool.store_lsn_stats(stats);
                        pool.lsn_check_passed();
                    }
                }
                Round::Unknown => (),
                Round::Failed(reason) => {
                    for pool in &pools {
                        pool.lsn_check_failed(&reason);
                    }
                }
            }
            self.connected.store(own.is_some(), Ordering::Relaxed);
        }

        if let Some(mut own) = own.take() {
            own.server.disconnect_reason(DisconnectReason::Offline);
        }
        self.connected.store(false, Ordering::Relaxed);
        debug!("lsn check stopped [{}:{}]", key.0, key.1);
    }

    async fn round(
        &self,
        pools: &[Pool],
        own: &mut Option<Own>,
        aurora: &mut Option<bool>,
        next: &mut usize,
    ) -> Round {
        // Keep the connection while it works and its user and database
        // have a pool here.
        if own
            .as_ref()
            .is_some_and(|own| own.server.error() || !pools.iter().any(|p| own.of(p)))
        {
            *own = None;
        }

        if own.is_none() {
            match Self::login(pools, next).await {
                Ok(Some(new)) => *own = Some(new),
                Ok(None) => return Round::Unknown,
                Err(reason) => return Round::Failed(reason),
            }
        }

        let Some(current) = own.as_mut() else {
            return Round::Unknown;
        };
        let timeout = pools[0].config().lsn_check_timeout;

        if aurora.is_none() {
            match safe_timeout(
                timeout,
                current.server.fetch_all::<DataRow>(AURORA_DETECTION_QUERY),
            )
            .await
            {
                Ok(Ok(_)) => *aurora = Some(true),
                Ok(Err(BackendError::ExecutionError(_))) => *aurora = Some(false),
                Ok(Err(err)) => {
                    *own = None;
                    return Round::Failed(format!("its LSN check failed: {err}"));
                }
                Err(_) => {
                    *own = None;
                    return Round::Failed(
                        "its LSN check did not answer in lsn_check_timeout".into(),
                    );
                }
            }
        }

        let is_aurora = aurora.unwrap_or_default();
        let query = if is_aurora {
            AURORA_LSN_QUERY
        } else {
            LSN_QUERY
        };

        match safe_timeout(timeout, current.server.fetch_all::<DataRow>(query)).await {
            Ok(Ok(rows)) => match rows.into_iter().next() {
                Some(row) => Round::Stats(LsnStats::from_row(row, is_aurora)),
                None => Round::Unknown,
            },
            Ok(Err(err)) => {
                error!("lsn check query error: {} [{}]", err, current.server.addr());
                *own = None;
                Round::Failed(format!("its LSN check failed: {err}"))
            }
            Err(_) => {
                error!("lsn check query timeout [{}]", current.server.addr());
                *own = None;
                Round::Failed("its LSN check did not answer in lsn_check_timeout".into())
            }
        }
    }

    /// Log in with a pool's credentials, newest pool first: a few per
    /// round, carrying on from where the last round stopped. `Ok(None)`:
    /// every one tried was refused.
    async fn login(pools: &[Pool], next: &mut usize) -> Result<Option<Own>, String> {
        let mut refused = vec![];
        for _ in 0..pools.len().min(LOGINS_PER_ROUND) {
            let pool = &pools[pools.len() - 1 - (*next % pools.len())];
            match Self::connect(pool).await {
                Ok(server) => {
                    return Ok(Some(Own {
                        server: Box::new(server),
                        user: pool.addr().user.clone(),
                        database: pool.addr().database_name.clone(),
                    }));
                }
                Err(Refusal::Credentials(reason)) => {
                    *next += 1;
                    refused.push(reason);
                }
                Err(Refusal::Down(reason)) => {
                    return Err(format!("its LSN check could not connect: {reason}"));
                }
            }
        }
        debug!(
            "lsn check logins refused, the server answers: {}",
            refused.join("; ")
        );
        Ok(None)
    }

    async fn connect(pool: &Pool) -> Result<Server, Refusal> {
        match safe_timeout(
            pool.config().connect_timeout,
            Box::pin(Server::connect(
                pool.addr(),
                pool.server_options(),
                ConnectReason::LsnCheck,
                Arc::clone(&pool.inner().oids),
            )),
        )
        .await
        {
            Ok(Ok(server)) => Ok(server),
            Ok(Err(err)) if refused_login(&err) => Err(Refusal::Credentials(err.to_string())),
            Ok(Err(err)) => Err(Refusal::Down(err.to_string())),
            Err(_) => Err(Refusal::Down("no answer in connect_timeout".into())),
        }
    }
}

/// The server refused the login for its user or database, which says the
/// server answers: authentication (class 28), a database that doesn't
/// exist (3D000), no right to connect (42501).
fn refused_login(err: &BackendError) -> bool {
    match err {
        BackendError::Auth(_) => true,
        BackendError::ConnectionError(response) => {
            response.code.starts_with("28") || response.code == "3D000" || response.code == "42501"
        }
        _ => false,
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::*;
    use crate::backend::pool::{Address, Config, PoolConfig};

    fn pool(user: &str, password: &str) -> Pool {
        let pool = Pool::new(&PoolConfig {
            address: Address {
                user: user.into(),
                passwords: vec![password.into()],
                ..Address::new_test()
            },
            config: Config {
                min: 0,
                replica_down_detection: true,
                lsn_check_delay: Duration::ZERO,
                lsn_check_interval: Duration::from_millis(50),
                lsn_check_timeout: Duration::from_secs(1),
                ..Config::default()
            },
        });
        pool.launch();
        pool
    }

    async fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
        for _ in 0..100 {
            if done() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("timed out waiting for {what}");
    }

    fn checks_of_test_server() -> usize {
        CHECKS
            .lock()
            .get(&("127.0.0.1".to_string(), 5432))
            .map(|check| check.pools.lock().len())
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn test_one_check_and_one_connection_for_every_pool_of_a_server() {
        crate::logger();
        let pools: Vec<Pool> = (0..5).map(|_| pool("pgdog", "pgdog")).collect();

        wait_for("every pool's stats", || {
            pools.iter().all(|pool| pool.lsn_stats().valid())
        })
        .await;
        assert_eq!(checks_of_test_server(), 5);
        let held: usize = connections()
            .into_iter()
            .filter(|((host, port), _)| host == "127.0.0.1" && *port == 5432)
            .map(|(_, count)| count)
            .sum();
        assert_eq!(held, 1, "one connection for the server, not one per pool");

        // No pool holds a connection for the check.
        for pool in &pools {
            assert_eq!(pool.lock().total(), 0);
        }

        // A pool that joins later has the stats at once.
        let late = pool("pgdog", "pgdog");
        assert!(late.lsn_stats().valid());

        for pool in pools.iter().chain([&late]) {
            pool.shutdown();
        }
        assert_eq!(checks_of_test_server(), 0);
        wait_for("the check to close its connection", || {
            connections().is_empty()
        })
        .await;
    }

    #[tokio::test]
    async fn test_new_pools_of_the_same_users_keep_the_connection() {
        crate::logger();
        let before = crate::stats::connections::opened(ConnectReason::LsnCheck);
        let old = pool("pgdog", "pgdog");
        wait_for("the first check", || old.lsn_stats().valid()).await;

        // A reload: new pools of the same user and database, the old ones go.
        let new = pool("pgdog", "pgdog");
        old.shutdown();
        let fetched = new.lsn_stats().fetched;
        wait_for("a check after the reload", || {
            new.lsn_stats().fetched > fetched
        })
        .await;

        assert_eq!(
            crate::stats::connections::opened(ConnectReason::LsnCheck) - before,
            1,
            "one connection, kept across the reload"
        );
        new.shutdown();
    }

    #[tokio::test]
    async fn test_a_refused_login_is_not_the_server_down() {
        crate::logger();
        // The newest pool's credentials are tried first and refused.
        let good = pool("pgdog", "pgdog");
        let bad = pool("pgdog", "not the password");

        wait_for("stats through the other pool's login", || {
            bad.lsn_stats().valid() && good.lsn_stats().valid()
        })
        .await;
        assert!(bad.healthy());
        assert!(good.healthy());

        bad.shutdown();
        good.shutdown();
    }

    #[tokio::test]
    async fn test_a_server_that_does_not_answer_fails_every_pool() {
        crate::logger();
        // Nothing listens there.
        let address = Address {
            port: 1,
            ..Address::new_test()
        };
        let pools: Vec<Pool> = (0..2)
            .map(|_| {
                let pool = Pool::new(&PoolConfig {
                    address: address.clone(),
                    config: Config {
                        min: 0,
                        replica_down_detection: true,
                        lsn_check_delay: Duration::ZERO,
                        lsn_check_interval: Duration::from_millis(50),
                        connect_timeout: Duration::from_millis(200),
                        ..Config::default()
                    },
                });
                pool.launch();
                pool
            })
            .collect();

        wait_for("both pools unhealthy", || {
            pools.iter().all(|pool| !pool.healthy())
        })
        .await;

        for pool in &pools {
            pool.shutdown();
        }
    }
}
