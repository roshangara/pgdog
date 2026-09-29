//! Primary election with automatic roles, and writes during a failover.

use std::time::SystemTime;

use crate::backend::pool::lsn_monitor::LsnStats;

use super::*;

const CHECKOUT_TIMEOUT: Duration = Duration::from_millis(2_000);

/// Auto targets; a closed port stands for a dead server.
fn auto_lb(targets: &[(&str, u16)]) -> LoadBalancer {
    let configs = targets
        .iter()
        .map(|(host, port)| {
            let mut config = create_auto_test_pool_config(host, *port);
            config.config.checkout_timeout = CHECKOUT_TIMEOUT;
            config.config.connect_timeout = Duration::from_millis(100);
            config
        })
        .collect::<Vec<_>>();

    LoadBalancer::new(
        &None,
        &configs,
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::ExcludePrimary,
        Default::default(),
    )
}

fn stats(target: &Target, replica: bool, timeline: i64, lsn: i64, age: Duration) {
    let stats: LsnStats = StatsLsnStats {
        replica,
        timeline,
        lsn: Lsn::from_i64(lsn),
        offset_bytes: lsn,
        fetched: SystemTime::now() - age,
        ..Default::default()
    }
    .into();
    *target.pool.inner().lsn_stats.write() = stats;
}

fn primary_port(lb: &LoadBalancer) -> Option<u16> {
    lb.primary().map(|pool| pool.addr().port)
}

const FRESH: Duration = Duration::ZERO;

#[test]
fn test_election_prefers_latest_timeline() {
    let lb = auto_lb(&[("127.0.0.1", 5001), ("127.0.0.1", 5002)]);
    stats(&lb.targets[0], false, 7, 900, FRESH);
    stats(&lb.targets[1], false, 8, 100, FRESH);

    assert!(lb.redetect_roles());
    assert_eq!(primary_port(&lb), Some(5002));
}

#[test]
fn test_election_same_timeline_most_wal() {
    let lb = auto_lb(&[("127.0.0.1", 5001), ("127.0.0.1", 5002)]);
    stats(&lb.targets[0], false, 8, 900, FRESH);
    stats(&lb.targets[1], false, 8, 100, FRESH);

    assert!(lb.redetect_roles());
    assert_eq!(primary_port(&lb), Some(5001));
}

#[test]
fn test_crashed_primary_loses_to_promoted_replica() {
    let lb = auto_lb(&[("127.0.0.1", 5001), ("127.0.0.1", 5002)]);
    stats(&lb.targets[0], false, 7, 500, FRESH);
    stats(&lb.targets[1], true, 0, 500, FRESH);
    assert!(lb.redetect_roles());
    assert_eq!(primary_port(&lb), Some(5001));

    // 5001 stops answering: its last stats stay. 5002 is promoted.
    stats(&lb.targets[0], false, 7, 500, Duration::from_secs(60));
    stats(&lb.targets[1], false, 8, 520, FRESH);
    assert!(lb.redetect_roles());
    assert_eq!(primary_port(&lb), Some(5002));
}

/// GH#1255: the old primary comes back before it is made a replica. It
/// reports fresh stats and more WAL, on the old timeline: it must not win.
#[test]
fn test_old_primary_back_as_primary_does_not_win() {
    let lb = auto_lb(&[("127.0.0.1", 5001), ("127.0.0.1", 5002)]);
    stats(&lb.targets[0], true, 0, 500, FRESH);
    stats(&lb.targets[1], false, 8, 520, Duration::from_secs(2));
    assert!(lb.redetect_roles());

    stats(&lb.targets[0], false, 7, 9_000, FRESH);
    assert!(!lb.redetect_roles());
    assert_eq!(primary_port(&lb), Some(5002));
}

/// GH#1255: the elected primary reports it's in recovery (it was demoted,
/// or came back as a replica). Nobody else is primary yet: drop it at once.
#[test]
fn test_primary_in_recovery_is_dropped() {
    let lb = auto_lb(&[("127.0.0.1", 5001), ("127.0.0.1", 5002)]);
    stats(&lb.targets[0], false, 7, 500, FRESH);
    stats(&lb.targets[1], true, 0, 500, FRESH);
    assert!(lb.redetect_roles());
    assert_eq!(primary_port(&lb), Some(5001));

    stats(&lb.targets[0], true, 0, 510, FRESH);
    assert!(lb.redetect_roles());
    assert_eq!(primary_port(&lb), None);
    assert!(
        lb.targets
            .iter()
            .all(|target| target.role() == Role::Replica)
    );
}

#[tokio::test]
async fn test_write_waits_for_election() {
    let lb = auto_lb(&[("127.0.0.1", 5432), ("127.0.0.1", 1)]);
    lb.launch();
    stats(&lb.targets[0], true, 0, 500, FRESH);
    stats(&lb.targets[1], true, 0, 500, FRESH);
    lb.redetect_roles();

    let started = Instant::now();
    let write = {
        let lb = lb.clone();
        tokio::spawn(async move { lb.get_primary(&Request::default()).await })
    };

    sleep(Duration::from_millis(300)).await;
    assert!(!write.is_finished(), "no primary: the write must wait");

    stats(&lb.targets[0], false, 8, 600, FRESH);
    assert!(lb.redetect_roles());

    let conn = timeout(Duration::from_secs(1), write)
        .await
        .expect("the write must resume right after the election")
        .unwrap()
        .expect("connection to the new primary");
    assert_eq!(conn.pool.addr().port, 5432);
    assert!(started.elapsed() < CHECKOUT_TIMEOUT);
    drop(conn);

    lb.shutdown();
}

#[tokio::test]
async fn test_write_moves_to_new_primary() {
    let lb = auto_lb(&[("127.0.0.1", 1), ("127.0.0.1", 5432)]);
    lb.launch();
    // The elected primary is dead: the write waits on its pool.
    stats(&lb.targets[0], false, 7, 500, FRESH);
    stats(&lb.targets[1], true, 0, 500, FRESH);
    assert!(lb.redetect_roles());

    let started = Instant::now();
    let write = {
        let lb = lb.clone();
        tokio::spawn(async move { lb.get_primary(&Request::default()).await })
    };

    sleep(Duration::from_millis(300)).await;
    assert!(!write.is_finished());

    // A replica is promoted.
    stats(&lb.targets[1], false, 8, 520, FRESH);
    assert!(lb.redetect_roles());

    let conn = timeout(Duration::from_secs(1), write)
        .await
        .expect("the write must move to the new primary")
        .unwrap()
        .expect("connection to the new primary");
    assert_eq!(conn.pool.addr().port, 5432);
    assert!(started.elapsed() < CHECKOUT_TIMEOUT);
    drop(conn);

    lb.shutdown();
}

#[tokio::test]
async fn test_write_without_primary_waits_checkout_timeout() {
    let lb = auto_lb(&[("127.0.0.1", 5432), ("127.0.0.1", 1)]);
    lb.launch();
    stats(&lb.targets[0], true, 0, 500, FRESH);
    stats(&lb.targets[1], true, 0, 500, FRESH);
    lb.redetect_roles();

    let started = Instant::now();
    let result = lb.get_primary(&Request::default()).await;
    let waited = started.elapsed();

    assert!(matches!(result, Err(Error::CheckoutTimeout)));
    assert!(
        waited >= CHECKOUT_TIMEOUT,
        "must not fail early: {waited:?}"
    );
    assert!(waited < CHECKOUT_TIMEOUT + Duration::from_millis(500));

    lb.shutdown();
}

/// The elected primary refuses connections (a closed port). The write
/// waits for the next election instead of failing.
#[tokio::test]
async fn test_write_waits_when_primary_refuses() {
    let lb = auto_lb(&[("127.0.0.1", 1), ("localhost", 5432)]);
    lb.targets[0].pool.launch();
    lb.targets[1].pool.launch();
    stats(&lb.targets[0], false, 7, 500, FRESH);
    stats(&lb.targets[1], true, 0, 500, FRESH);
    assert!(lb.redetect_roles());
    assert_eq!(
        lb.primary().map(|pool| pool.addr().host.clone()),
        Some("127.0.0.1".into())
    );

    let write = {
        let lb = lb.clone();
        tokio::spawn(async move { lb.get_primary(&Request::default()).await })
    };

    sleep(Duration::from_millis(600)).await;
    assert!(
        !write.is_finished(),
        "a refusing primary must not fail the write"
    );

    stats(&lb.targets[1], false, 8, 520, FRESH);
    assert!(lb.redetect_roles());

    let conn = timeout(Duration::from_secs(1), write)
        .await
        .expect("the write must move to the new primary")
        .unwrap()
        .expect("connection to the new primary");
    assert_eq!(conn.pool.addr().host, "localhost");
    drop(conn);

    lb.shutdown();
}

/// A reload shuts down every pool (a new passthrough user, a changed
/// password, SIGHUP). A write waiting on the old primary pool must end at
/// once so it moves to the new pools, not wait out checkout_timeout for an
/// election the old load balancer will never hold (ganjban lab P-3).
#[tokio::test]
async fn test_reload_ends_a_waiting_write_at_once() {
    // Waiting after the elected primary refused.
    let lb = auto_lb(&[("127.0.0.1", 1), ("localhost", 5432)]);
    lb.launch();
    stats(&lb.targets[0], false, 7, 500, FRESH);
    stats(&lb.targets[1], true, 0, 500, FRESH);
    assert!(lb.redetect_roles());

    let write = {
        let lb = lb.clone();
        tokio::spawn(async move { lb.get_primary(&Request::default()).await })
    };
    sleep(Duration::from_millis(400)).await;
    assert!(!write.is_finished());

    lb.shutdown();
    let result = timeout(Duration::from_millis(300), write)
        .await
        .expect("the write must end at once")
        .unwrap();
    assert!(matches!(result, Err(Error::Offline)), "{result:?}");

    // Waiting with no primary elected at all.
    let lb = auto_lb(&[("127.0.0.1", 1), ("localhost", 5432)]);
    lb.launch();
    let write = {
        let lb = lb.clone();
        tokio::spawn(async move { lb.get_primary(&Request::default()).await })
    };
    sleep(Duration::from_millis(200)).await;
    assert!(!write.is_finished());

    lb.shutdown();
    let result = timeout(Duration::from_millis(300), write)
        .await
        .expect("the write must end at once")
        .unwrap();
    assert!(matches!(result, Err(Error::Offline)), "{result:?}");
}

/// A role at its connection limit, made for one test and dropped after.
struct LimitedRole {
    name: String,
}

impl LimitedRole {
    /// A role no connection is allowed (CONNECTION LIMIT 0): every login of
    /// it gets 53300 "too many connections for role".
    async fn new() -> Self {
        use rand::Rng;

        let name = format!(
            "door_limited_{}",
            rand::rng().random_range(1_000_000..u32::MAX)
        );
        let mut admin = crate::backend::server::test::test_server().await;
        admin
            .execute_checked(format!(
                "CREATE ROLE {name} LOGIN NOSUPERUSER PASSWORD 'pgdog' CONNECTION LIMIT 0"
            ))
            .await
            .unwrap();
        Self { name }
    }

    async fn drop_role(self) {
        let mut admin = crate::backend::server::test::test_server().await;
        admin
            .execute_checked(format!("DROP ROLE {}", self.name))
            .await
            .unwrap();
    }

    fn config(&self, host: &str, port: u16) -> PoolConfig {
        let mut config = create_auto_test_pool_config(host, port);
        config.address.user = self.name.clone();
        config.config.checkout_timeout = CHECKOUT_TIMEOUT;
        config.config.connect_timeout = Duration::from_millis(500);
        config
    }
}

/// The elected primary refuses the login with 53300, too many connections
/// for the role (a tenant at its connection limit, its connections held by
/// other doors): that is the primary's answer, not its failure. The write
/// gets it at once instead of waiting checkout_timeout for an election and
/// failing with "checkout timeout" (ganjban.7: 35 s).
#[tokio::test]
async fn test_a_primary_refusing_the_login_answers_the_write_at_once() {
    crate::logger();
    let role = LimitedRole::new().await;
    let lb = LoadBalancer::new(
        &None,
        &[role.config("127.0.0.1", 5432)],
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::ExcludePrimary,
        Default::default(),
    );
    lb.targets[0].pool.launch();
    stats(&lb.targets[0], false, 7, 500, FRESH);
    lb.redetect_roles();
    assert_eq!(primary_port(&lb), Some(5432));

    let started = Instant::now();
    let result = lb.get_primary(&Request::default()).await;
    let elapsed = started.elapsed();
    lb.shutdown();
    role.drop_role().await;

    let err = result.expect_err("the login is refused");
    assert!(
        elapsed < Duration::from_millis(1000),
        "the refusal must reach the write at once, not after {elapsed:?}"
    );
    assert!(
        err.to_string().contains("53300") && err.to_string().contains("too many connections"),
        "the server's own answer: {err}"
    );
    assert!(
        lb.targets[0].pool.healthy(),
        "a server that answers is not unhealthy"
    );
}

/// A pool whose connections are all out keeps its waiters waiting when the
/// server refuses a new one: a connection coming back serves them.
#[tokio::test]
async fn test_a_refusal_keeps_waiting_while_connections_are_out() {
    crate::logger();
    let role = LimitedRole::new().await;
    let mut admin = crate::backend::server::test::test_server().await;
    // One connection allowed, and the pool gets it.
    admin
        .execute_checked(format!("ALTER ROLE {} CONNECTION LIMIT 1", role.name))
        .await
        .unwrap();
    let mut config = role.config("127.0.0.1", 5432);
    config.config.max = 2;
    let pool = Pool::new(&config);
    pool.launch();

    let first = pool
        .get(&Request::default())
        .await
        .expect("the one connection");
    let second = {
        let pool = pool.clone();
        tokio::spawn(async move { pool.get(&Request::default()).await })
    };
    sleep(Duration::from_millis(500)).await;
    assert!(
        !second.is_finished(),
        "the first connection comes back to it: it waits"
    );
    drop(first);
    let second = timeout(Duration::from_secs(1), second)
        .await
        .expect("served by the connection that came back")
        .unwrap();
    assert!(second.is_ok(), "{:?}", second.err());
    drop(second);

    pool.shutdown();
    role.drop_role().await;
}

/// A new client's server parameters come from a server that answers. The
/// nearest entry (weight 255 in ganjban's door) may be a readers address
/// with nothing behind it; right after a reload or for a new user its pool
/// is not banned yet and the primary not yet elected. ganjban.7 asked that
/// entry alone and aborted the client: "connection pool ... is down".
#[tokio::test]
async fn test_a_new_clients_parameters_come_from_a_server_that_answers() {
    crate::logger();
    let configs = [("127.0.0.1", 1), ("127.0.0.1", 5432)]
        .iter()
        .map(|(host, port)| {
            let mut config = create_auto_test_pool_config(host, *port);
            config.config.checkout_timeout = CHECKOUT_TIMEOUT;
            config.config.connect_timeout = Duration::from_millis(100);
            config.config.replica_checkout_timeout = Duration::from_millis(300);
            config.config.replica_down_detection = true;
            config
        })
        .collect::<Vec<_>>();
    let lb = LoadBalancer::new(
        &None,
        &configs,
        LoadBalancingStrategy::WeightedRoundRobin,
        ReadWriteSplit::ExcludePrimary,
        Default::default(),
    );
    for target in &lb.targets {
        target.pool.launch();
    }
    assert!(lb.primary().is_none(), "not elected yet");

    let started = Instant::now();
    let params = lb
        .params(&Request::default())
        .await
        .map(|params| params.len());
    let elapsed = started.elapsed();
    lb.shutdown();

    assert!(params.is_ok(), "{params:?}");
    assert!(
        elapsed < Duration::from_millis(1000),
        "the next server answers at once, took {elapsed:?}"
    );
}

/// A reload keeps a server found down out of reads: its ban and its health
/// move to the new pool. Before, the new pool was healthy and unbanned until
/// the next ban check, and a read in that window went to it alone and failed
/// with "all replicas down" (a readers address with nothing behind it,
/// after every new passthrough user's first login).
#[tokio::test]
async fn test_a_reload_keeps_a_down_replica_out_of_reads() {
    crate::logger();
    let old = auto_lb(&[("127.0.0.1", 1), ("127.0.0.1", 5432)]);
    old.launch();
    stats(&old.targets[0], true, 7, 500, FRESH);
    stats(&old.targets[1], false, 7, 500, FRESH);
    old.redetect_roles();
    old.targets[0].pool.inner().health.toggle(false);
    old.targets[0]
        .ban
        .ban(Error::PoolUnhealthy, Duration::from_secs(10));

    let new = auto_lb(&[("127.0.0.1", 1), ("127.0.0.1", 5432)]);
    old.move_conns_to(&new).unwrap();
    new.launch();

    assert!(new.targets[0].ban.banned(), "the ban moves with the reload");
    assert!(!new.targets[0].health().healthy());

    let read = new
        .get(&Request::new(FrontendPid::new(), true, false))
        .await
        .expect("the read goes to the primary");
    assert_eq!(read.pool.addr().port, 5432);
    drop(read);

    old.shutdown();
    new.shutdown();
}
