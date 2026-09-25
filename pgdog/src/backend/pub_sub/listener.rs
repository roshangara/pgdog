//! Pub/sub listener.
//!
//! Handles notifications from Postgres and sends them out
//! to a broadcast channel.
//!
use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
    sync::Arc,
    time::Duration,
};

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use tokio::{
    select,
    sync::{Notify, broadcast, mpsc},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

use super::{Stats, StatsSnapshot, channel_size};
use crate::log_sink::CONNECTIONS;
use crate::util::{safe_interval, safe_sleep};
use crate::{
    backend::{self, ConnectReason, DisconnectReason, Pool, databases::User, pool::Error},
    config::config,
    net::{
        FromBytes, FrontendPid, NotificationResponse, Parameter, Parameters, Protocol,
        ProtocolMessage, Query, ToBytes,
    },
    tasks,
};

#[derive(Debug, Clone)]
enum Request {
    Unsubscribe(String),
    Subscribe(String),
    Notify { channel: String, payload: String },
}

impl From<Request> for ProtocolMessage {
    fn from(val: Request) -> Self {
        match val {
            Request::Unsubscribe(channel) => Query::new(format!("UNLISTEN \"{}\"", channel)).into(),
            Request::Subscribe(channel) => Query::new(format!("LISTEN \"{}\"", channel)).into(),
            Request::Notify { channel, payload } => {
                Query::new(format!("NOTIFY \"{}\", '{}'", channel, payload)).into()
            }
        }
    }
}

/// Pool a set of channels belongs to. `NOTIFY` is scoped to a database, so
/// channels have to be scoped the same way.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct PoolKey {
    /// Database name, as configured in pgdog.
    pub(crate) database: String,
    /// User, as configured in pgdog.
    pub(crate) user: String,
    /// Shard number.
    pub(crate) shard: usize,
}

impl PoolKey {
    fn new(identifier: &User, shard: usize) -> Self {
        Self {
            database: identifier.database.clone(),
            user: identifier.user.clone(),
            shard,
        }
    }

    /// Key for one of this pool's channels.
    fn channel(&self, channel: &str) -> ChannelKey {
        ChannelKey {
            pool: self.clone(),
            channel: channel.to_owned(),
        }
    }
}

/// One channel on one pool.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct ChannelKey {
    /// Pool the channel belongs to.
    pub(crate) pool: PoolKey,
    /// Channel name used by `LISTEN`/`NOTIFY`.
    pub(crate) channel: String,
}

type Channels = Arc<Mutex<HashMap<PoolKey, HashMap<String, Channel>>>>;

static CHANNELS: Lazy<Channels> = Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

/// Get stats for all channels.
pub(crate) fn stats() -> HashMap<ChannelKey, StatsSnapshot> {
    CHANNELS
        .lock()
        .iter()
        .flat_map(|(pool, channels)| {
            channels
                .iter()
                .map(|(channel, state)| (pool.channel(channel), state.stats.get()))
        })
        .collect()
}

/// Remove a channel from its pool's set, dropping the pool's entry once no
/// channels remain, so pools that churn don't accumulate empty maps.
fn remove_channel(channels: &Channels, pool_key: &PoolKey, channel: &str) {
    let mut guard = channels.lock();
    if let Some(pool_channels) = guard.get_mut(pool_key) {
        pool_channels.remove(channel);
        if pool_channels.is_empty() {
            guard.remove(pool_key);
        }
    }
}

#[derive(Debug)]
struct Channel {
    tx: broadcast::Sender<NotificationResponse>,
    stats: Arc<Stats>,
}

#[derive(Debug)]
pub(crate) struct Listener {
    rx: broadcast::Receiver<NotificationResponse>,
    stats: Arc<Stats>,
}

impl Listener {
    fn new(channel: &Channel) -> Self {
        channel.stats.incr_listeners();

        Self {
            rx: channel.tx.subscribe(),
            stats: channel.stats.clone(),
        }
    }

    pub(crate) fn stats(&self) -> &Stats {
        &self.stats
    }
}

impl Drop for Listener {
    fn drop(&mut self) {
        self.stats.decr_listeners();
    }
}

impl Deref for Listener {
    type Target = broadcast::Receiver<NotificationResponse>;

    fn deref(&self) -> &Self::Target {
        &self.rx
    }
}

impl DerefMut for Listener {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rx
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub(crate) struct TestChannel {
        tx: broadcast::Sender<NotificationResponse>,
        stats: Arc<Stats>,
    }

    impl TestChannel {
        pub(crate) fn new() -> Self {
            let (tx, _) = broadcast::channel(4);

            Self {
                tx,
                stats: Arc::new(Stats::default()),
            }
        }

        pub(crate) fn listener(&self) -> Listener {
            Listener::new(&Channel {
                tx: self.tx.clone(),
                stats: self.stats.clone(),
            })
        }

        pub(crate) fn send(
            &self,
            notification: NotificationResponse,
        ) -> Result<usize, broadcast::error::SendError<NotificationResponse>> {
            self.tx.send(notification)
        }

        pub(crate) fn stats(&self) -> StatsSnapshot {
            self.stats.get()
        }
    }
}

#[derive(Debug)]
struct Comms {
    start: Notify,
    shutdown: CancellationToken,
}

/// Notification listener.
#[derive(Debug, Clone)]
pub(crate) struct PubSubListener {
    id: FrontendPid,
    pool: Pool,
    pool_key: PoolKey,
    tx: mpsc::Sender<Request>,
    channels: Channels,
    comms: Arc<Comms>,
}

impl PubSubListener {
    /// Create new listener on the server connection.
    ///
    /// `identifier` and `shard` scope the channels this listener owns. They are
    /// the pgdog-side identity of the pool, so they survive a primary being
    /// promoted underneath us, which the server address would not.
    pub(crate) fn new(pool: &Pool, identifier: &User, shard: usize) -> Self {
        let (tx, mut rx) = mpsc::channel(channel_size());

        let pool = pool.clone();
        let channels = CHANNELS.clone();

        let listener = Self {
            id: FrontendPid::new(),
            pool: pool.clone(),
            pool_key: PoolKey::new(identifier, shard),
            tx,
            channels,
            comms: Arc::new(Comms {
                start: Notify::new(),
                shutdown: CancellationToken::new(),
            }),
        };

        let id = listener.id;
        let channels = listener.channels.clone();
        let pool_key = listener.pool_key.clone();
        let pool = listener.pool.clone();
        let comms = listener.comms.clone();
        tasks::spawn("pub/sub listener", async move {
            select! {
                _ = comms.start.notified() => {}
                _ = comms.shutdown.cancelled() => return,
            }

            // The server connection is made for the first LISTEN or NOTIFY,
            // and closed after idle_timeout with no channel listened to: a
            // tenant that never uses LISTEN costs no connection, one that
            // stopped costs none after idle_timeout. After an error, the
            // channels clients still listen on are listened on again at once.
            let mut first = None;
            loop {
                if first.is_none() && !has_listeners(&channels, &pool_key) {
                    first = select! {
                        request = rx.recv() => match request {
                            Some(request) => Some(request),
                            None => break,
                        },
                        _ = comms.shutdown.cancelled() => break,
                    };
                }

                let result = select! {
                    _ = comms.shutdown.cancelled() => break,
                    result = Self::run(id, &pool, &pool_key, &mut rx, channels.clone(), first.take()) => result,
                };

                match result {
                    Ok(Ended::Idle) => continue,
                    Ok(Ended::Closed) => break,
                    Err(err) => {
                        error!("pub/sub error: {} [{}]", err, pool.addr());
                        // Don't reconnect for another connect attempt delay
                        // to avoid connection storms during incidents.
                        select! {
                            _ = safe_sleep(Duration::from_millis(config().config.general.connect_attempt_delay)) => {}
                            _ = comms.shutdown.cancelled() => break,
                        }
                    }
                }
            }
            rx.close();
        });

        listener
    }

    /// Launch the listener.
    pub(crate) fn launch(&self) {
        self.comms.start.notify_one();
    }

    /// Shutdown the listener.
    pub(crate) fn shutdown(&self) {
        self.comms.shutdown.cancel();
    }

    /// Listen on a channel.
    pub(crate) async fn listen(&self, channel_name: &str) -> Result<Listener, Error> {
        let listener = {
            let mut guard = self.channels.lock();
            let channels = guard.entry(self.pool_key.clone()).or_default();

            if let Some(channel) = channels.get(channel_name) {
                return Ok(Listener::new(channel));
            }

            let (tx, _) = broadcast::channel(channel_size());
            let stats = Arc::new(Stats::default());

            let channel = Channel {
                tx,
                stats: stats.clone(),
            };
            let listener = Listener::new(&channel);

            channels.insert(channel_name.to_string(), channel);

            listener
        };

        self.tx
            .send(Request::Subscribe(channel_name.to_string()))
            .await
            .map_err(|_| Error::Offline)?;

        Ok(listener)
    }

    /// Notify a channel with payload.
    pub(crate) async fn notify(&self, channel: &str, payload: &str) -> Result<(), Error> {
        self.tx
            .send(Request::Notify {
                channel: channel.to_string(),
                payload: payload.to_string(),
            })
            .await
            .map_err(|_| Error::Offline)
    }

    // Run the listener task: until the connection fails, the listener is
    // shut down (`Ended::Closed`) or nothing needed it for idle_timeout
    // (`Ended::Idle`).
    async fn run(
        id: FrontendPid,
        pool: &Pool,
        pool_key: &PoolKey,
        rx: &mut mpsc::Receiver<Request>,
        channels: Channels,
        first: Option<Request>,
    ) -> Result<Ended, backend::Error> {
        info!(target: CONNECTIONS, "pub/sub started [{}]", pool.addr());

        let mut server = pool.standalone(ConnectReason::PubSub).await?;
        let _connected = Connected::new(pool);

        server
            .link_client(
                id,
                &Parameters::from(vec![Parameter {
                    name: "application_name".into(),
                    value: "PgDog Pub/Sub Listener".into(),
                }]),
                None,
            )
            .await?;

        // Re-listen on this pool's channels when re-starting the task.
        // We don't lose LISTEN commands.
        let mut resub = channels
            .lock()
            .get(pool_key)
            .map(|channels| {
                channels
                    .keys()
                    .map(|channel| Request::Subscribe(channel.clone()).into())
                    .collect::<Vec<ProtocolMessage>>()
            })
            .unwrap_or_default();
        resub.extend(first.map(ProtocolMessage::from));

        if !resub.is_empty() {
            server.send(&resub.into()).await?;
        }

        let idle_timeout = pool.config().idle_timeout;
        let mut idle = safe_interval(idle_timeout);
        idle.tick().await;
        let mut used = false;

        loop {
            select! {
                message = server.read() => {
                    let message = message?;

                    // NotificationResponse (B)
                    if message.code() == 'A' {
                        let notification = NotificationResponse::from_bytes(message.to_bytes())?;
                        let mut unsub = None;
                        if let Some(channel) = channels
                            .lock()
                            .get(pool_key)
                            .and_then(|channels| channels.get(notification.channel()))
                        {
                            match channel.tx.send(notification) {
                                Ok(_) => (),
                                Err(err) => unsub = Some(err.0.channel().to_string()),
                            }
                        }

                        if let Some(unsub) = unsub {
                            remove_channel(&channels, pool_key, &unsub);
                            server.send(&vec![Request::Unsubscribe(unsub).into()].into()).await?;
                        }
                    }

                    // Terminate (B)
                    if message.code() == 'X' {
                        break;
                    }
                }

                req = rx.recv() => {
                    if let Some(req) = req {
                        debug!("pub/sub request {:?}", req);
                        used = true;
                        server.send(&vec![req.into()].into()).await?;
                    } else {
                        server.disconnect_reason(DisconnectReason::Offline);
                        return Ok(Ended::Closed);
                    }
                }

                _ = idle.tick() => {
                    if !used && forget_unlistened(&channels, pool_key) {
                        server.disconnect_reason(DisconnectReason::Idle);
                        return Ok(Ended::Idle);
                    }
                    used = false;
                }
            }
        }

        Ok(Ended::Idle)
    }
}

/// Why the listener's connection ended.
#[derive(Debug, PartialEq)]
enum Ended {
    /// Nothing needed it for idle_timeout; the next request connects again.
    Idle,
    /// The listener is shut down.
    Closed,
}

/// A client listens on one of this pool's channels.
fn has_listeners(channels: &Channels, pool_key: &PoolKey) -> bool {
    channels.lock().get(pool_key).is_some_and(|channels| {
        channels
            .values()
            .any(|channel| channel.stats.get().listeners > 0)
    })
}

/// Drop this pool's channels no client listens on; true when none is left.
fn forget_unlistened(channels: &Channels, pool_key: &PoolKey) -> bool {
    let mut guard = channels.lock();
    let Some(pool_channels) = guard.get_mut(pool_key) else {
        return true;
    };
    pool_channels.retain(|_, channel| channel.stats.get().listeners > 0);
    if pool_channels.is_empty() {
        guard.remove(pool_key);
        true
    } else {
        false
    }
}

/// Listener connections held now, by server.
static CONNECTED: Lazy<Mutex<HashMap<(String, u16), usize>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Counts a listener's server connection while it is held.
struct Connected {
    key: (String, u16),
}

impl Connected {
    fn new(pool: &Pool) -> Self {
        let key = (pool.addr().host.clone(), pool.addr().port);
        *CONNECTED.lock().entry(key.clone()).or_default() += 1;
        Self { key }
    }
}

impl Drop for Connected {
    fn drop(&mut self) {
        let mut connected = CONNECTED.lock();
        if let Some(count) = connected.get_mut(&self.key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                connected.remove(&self.key);
            }
        }
    }
}

/// Listener connections held now, by server.
pub(crate) fn connections() -> Vec<((String, u16), usize)> {
    CONNECTED
        .lock()
        .iter()
        .map(|(key, count)| (key.clone(), *count))
        .collect()
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::Arc};

    use parking_lot::Mutex;
    use tokio::sync::{Notify, mpsc};

    use super::{test_support::TestChannel, *};

    fn test_user(user: &str, database: &str) -> User {
        User {
            user: user.into(),
            database: database.into(),
        }
    }

    fn test_pub_sub_listener() -> (PubSubListener, mpsc::Receiver<Request>) {
        test_pub_sub_listener_on(
            Arc::new(Mutex::new(HashMap::new())),
            &test_user("pgdog", "pgdog"),
            0,
        )
    }

    /// A listener for `identifier`/`shard`, sharing `channels` with any other
    /// listener built from the same map.
    fn test_pub_sub_listener_on(
        channels: Channels,
        identifier: &User,
        shard: usize,
    ) -> (PubSubListener, mpsc::Receiver<Request>) {
        let (tx, rx) = mpsc::channel(4);

        (
            PubSubListener {
                id: FrontendPid::new(),
                pool: Pool::new_test(),
                pool_key: PoolKey::new(identifier, shard),
                tx,
                channels,
                comms: Arc::new(Comms {
                    start: Notify::new(),
                    shutdown: CancellationToken::new(),
                }),
            },
            rx,
        )
    }

    /// Assert a Subscribe request is already queued. Deliberately non-blocking:
    /// the sender is alive, so an `await` here would hang rather than fail if a
    /// regression stopped the request being sent.
    fn expect_subscribe_now(rx: &mut mpsc::Receiver<Request>, expected: &str) {
        match rx.try_recv() {
            Ok(Request::Subscribe(channel)) => assert_eq!(channel, expected),
            other => panic!("expected subscribe request for {expected}, got {other:?}"),
        }
    }

    fn assert_snapshot(snapshot: StatsSnapshot, recv: u64, dropped: u64, listeners: u64) {
        assert_eq!(snapshot.recv, recv);
        assert_eq!(snapshot.dropped, dropped);
        assert_eq!(snapshot.listeners, listeners);
    }

    fn assert_request_query(request: Request, expected: &str) {
        let ProtocolMessage::Query(query) = ProtocolMessage::from(request) else {
            panic!("request should convert to a query message");
        };

        assert_eq!(query.query(), expected);
    }

    async fn expect_subscribe(rx: &mut mpsc::Receiver<Request>, expected: &str) {
        let request = rx.recv().await.expect("request");

        match request {
            Request::Subscribe(channel) => assert_eq!(channel, expected),
            request => panic!("expected subscribe request, got {request:?}"),
        }
    }

    async fn expect_notify(
        rx: &mut mpsc::Receiver<Request>,
        expected_channel: &str,
        expected_payload: &str,
    ) {
        let request = rx.recv().await.expect("request");

        match request {
            Request::Notify { channel, payload } => {
                assert_eq!(channel, expected_channel);
                assert_eq!(payload, expected_payload);
            }
            request => panic!("expected notify request, got {request:?}"),
        }
    }

    #[test]
    fn requests_convert_to_expected_sql_queries() {
        assert_request_query(Request::Subscribe("events".into()), "LISTEN \"events\"");
        assert_request_query(Request::Unsubscribe("events".into()), "UNLISTEN \"events\"");
        assert_request_query(
            Request::Notify {
                channel: "events".into(),
                payload: "payload".into(),
            },
            "NOTIFY \"events\", 'payload'",
        );
    }

    #[test]
    fn listener_drop_updates_listener_count() {
        let channel = TestChannel::new();
        assert_snapshot(channel.stats(), 0, 0, 0);

        let first = channel.listener();
        assert_snapshot(channel.stats(), 0, 0, 1);

        {
            let _second = channel.listener();
            assert_snapshot(channel.stats(), 0, 0, 2);
        }

        assert_snapshot(channel.stats(), 0, 0, 1);
        drop(first);
        assert_snapshot(channel.stats(), 0, 0, 0);
    }

    #[tokio::test]
    async fn listen_creates_channel_once_and_reuses_it() {
        let (pub_sub, mut rx) = test_pub_sub_listener();

        let first = pub_sub.listen("events").await.expect("first listen");
        expect_subscribe(&mut rx, "events").await;
        assert_eq!(pub_sub.channels.lock().len(), 1);
        assert_snapshot(first.stats().get(), 0, 0, 1);

        let second = pub_sub.listen("events").await.expect("second listen");
        assert!(matches!(
            rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert_eq!(pub_sub.channels.lock().len(), 1);
        assert_snapshot(second.stats().get(), 0, 0, 2);

        drop(first);
        assert_snapshot(second.stats().get(), 0, 0, 1);

        drop(second);
        let stats = pub_sub
            .channels
            .lock()
            .get(&pub_sub.pool_key)
            .and_then(|channels| channels.get("events"))
            .expect("events channel")
            .stats
            .get();
        assert_snapshot(stats, 0, 0, 0);
    }

    /// Two listeners sharing the registry must not share a channel just because
    /// they were handed the same name. Each has to be sent its own LISTEN,
    /// otherwise the one that misses out never receives its own database's
    /// notifications.
    async fn assert_channels_not_shared(
        left: &User,
        left_shard: usize,
        right: &User,
        right_shard: usize,
    ) {
        let channels: Channels = Arc::new(Mutex::new(HashMap::new()));
        let (first, mut first_rx) = test_pub_sub_listener_on(channels.clone(), left, left_shard);
        let (second, mut second_rx) =
            test_pub_sub_listener_on(channels.clone(), right, right_shard);

        let _first = first.listen("events").await.expect("first listen");
        let _second = second.listen("events").await.expect("second listen");

        expect_subscribe_now(&mut first_rx, "events");
        expect_subscribe_now(&mut second_rx, "events");

        let guard = channels.lock();
        assert_eq!(guard.len(), 2, "each pool needs its own channel set");
        for pool_key in [&first.pool_key, &second.pool_key] {
            let channel = guard
                .get(pool_key)
                .and_then(|channels| channels.get("events"))
                .expect("channel for pool");
            assert_snapshot(channel.stats.get(), 0, 0, 1);
        }
    }

    #[tokio::test]
    async fn channels_are_not_shared_between_databases() {
        assert_channels_not_shared(
            &test_user("pgdog", "first_database"),
            0,
            &test_user("pgdog", "second_database"),
            0,
        )
        .await;
    }

    #[tokio::test]
    async fn channels_are_not_shared_between_users() {
        assert_channels_not_shared(
            &test_user("alice", "pgdog"),
            0,
            &test_user("bob", "pgdog"),
            0,
        )
        .await;
    }

    #[tokio::test]
    async fn channels_are_not_shared_between_shards() {
        let user = test_user("pgdog", "pgdog");
        assert_channels_not_shared(&user, 0, &user, 1).await;
    }

    #[test]
    fn pool_key_is_built_from_the_pgdog_side_identity() {
        let key = PoolKey::new(&test_user("alice", "shop"), 2);

        assert_eq!(key.database, "shop");
        assert_eq!(key.user, "alice");
        assert_eq!(key.shard, 2);

        // Every component discriminates.
        assert_ne!(key, PoolKey::new(&test_user("alice", "other"), 2));
        assert_ne!(key, PoolKey::new(&test_user("bob", "shop"), 2));
        assert_ne!(key, PoolKey::new(&test_user("alice", "shop"), 3));
    }

    #[tokio::test]
    async fn removing_the_last_channel_drops_the_pool_entry() {
        let (pub_sub, mut rx) = test_pub_sub_listener();

        let _listener = pub_sub.listen("events").await.expect("listen");
        expect_subscribe(&mut rx, "events").await;

        remove_channel(&pub_sub.channels, &pub_sub.pool_key, "events");
        assert!(
            pub_sub.channels.lock().is_empty(),
            "empty pool entries must not accumulate"
        );
    }

    #[tokio::test]
    async fn removing_one_channel_keeps_the_pool_entry_for_the_rest() {
        let (pub_sub, mut rx) = test_pub_sub_listener();

        let _events = pub_sub.listen("events").await.expect("listen events");
        let _jobs = pub_sub.listen("jobs").await.expect("listen jobs");
        expect_subscribe(&mut rx, "events").await;
        expect_subscribe(&mut rx, "jobs").await;

        remove_channel(&pub_sub.channels, &pub_sub.pool_key, "events");

        let guard = pub_sub.channels.lock();
        let channels = guard.get(&pub_sub.pool_key).expect("pool entry remains");
        assert!(channels.contains_key("jobs"));
        assert!(!channels.contains_key("events"));
    }

    #[tokio::test]
    async fn notify_queues_notify_request() {
        let (pub_sub, mut rx) = test_pub_sub_listener();

        pub_sub
            .notify("events", "payload")
            .await
            .expect("notify request");

        expect_notify(&mut rx, "events", "payload").await;
    }

    /// The listener connects for the first LISTEN or NOTIFY, not before, and
    /// leaves the server after idle_timeout with nobody listening (A-10).
    #[tokio::test]
    async fn test_the_listener_connects_when_used_and_leaves_when_idle() {
        use crate::backend::pool::{Address, Config, PoolConfig};

        crate::logger();
        let pool = Pool::new(&PoolConfig {
            address: Address::new_test(),
            config: Config {
                min: 0,
                idle_timeout: Duration::from_millis(300),
                ..Config::default()
            },
        });
        pool.launch();
        let held = || -> usize { connections().into_iter().map(|(_, n)| n).sum() };
        let wait_for = |n: usize| async move {
            for _ in 0..100 {
                if held() == n {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            panic!("expected {n} listener connections, got {}", held());
        };

        let listener = PubSubListener::new(&pool, &test_user("pgdog", "pgdog"), 0);
        listener.launch();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(held(), 0, "no connection before a LISTEN");

        let subscription = listener.listen("lazy_listener").await.unwrap();
        wait_for(1).await;

        // Still listened to: it stays past idle_timeout.
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(held(), 1);

        drop(subscription);
        wait_for(0).await;

        // A NOTIFY connects again.
        listener.notify("lazy_listener", "payload").await.unwrap();
        wait_for(1).await;

        listener.shutdown();
        wait_for(0).await;
        pool.shutdown();
    }
}
