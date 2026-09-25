//! Statement events: one JSON object a line for every statement a client
//! sends (`[general] query_events`).
//!
//! The query engine builds an event when a statement's last message has
//! gone to the client and hands it to [`send`], which never waits: the
//! event goes into a bounded queue ([`QUEUE`] events) that a thread of its
//! own writes to the file. A full queue drops the event and counts it
//! (`query_events_dropped_total`); a slow or stalled disk costs events,
//! never a client's time.
//!
//! What an event carries is the contract with the collector that ships the
//! file (ganjban's `ganjban_door_query` table, docs/TELEMETRY.md section 5):
//! the field names are the table's columns.

pub(crate) mod fingerprint;
mod writer;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, TrySendError};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use parking_lot::Mutex;
use pgdog_config::General;

/// Events the queue holds while the writer is behind.
pub(crate) const QUEUE: usize = 65_536;

/// Rotated files kept beside the current one: `.1` (newest) to `.7`.
pub(crate) const ROTATED: usize = 7;

static ENABLED: AtomicBool = AtomicBool::new(false);
static WRITTEN: AtomicU64 = AtomicU64::new(0);
static DROPPED: AtomicU64 = AtomicU64::new(0);
static SENDER: OnceLock<SyncSender<Box<QueryEvent>>> = OnceLock::new();
static TARGET: Mutex<Option<Target>> = Mutex::new(None);
static GENERATION: AtomicU64 = AtomicU64::new(0);

/// Where events go.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Target {
    pub(crate) path: PathBuf,
    pub(crate) max_bytes: u64,
}

/// Events are written: the query engine builds them only then.
#[inline]
pub(crate) fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// Events written to the file.
pub(crate) fn written() -> u64 {
    WRITTEN.load(Ordering::Relaxed)
}

/// Events dropped because the writer was behind (or the file can't be
/// written).
pub(crate) fn dropped() -> u64 {
    DROPPED.load(Ordering::Relaxed)
}

/// Apply the configuration: start, move or stop the events. Called at
/// start and on every reload; never waits for the writer.
pub(crate) fn configure(general: &General) {
    let target = general.query_events.clone().map(|path| Target {
        path,
        max_bytes: general.query_events_max_bytes.max(1),
    });

    let mut current = TARGET.lock();
    if *current == target {
        return;
    }
    let enabled = target.is_some();
    *current = target;
    drop(current);
    GENERATION.fetch_add(1, Ordering::Release);

    if enabled {
        SENDER.get_or_init(|| writer::spawn(QUEUE));
    }
    ENABLED.store(enabled, Ordering::Relaxed);
}

/// Hand an event to the writer. A full queue drops it.
pub(crate) fn send(event: QueryEvent) {
    if let Some(sender) = SENDER.get() {
        send_to(sender, event, &DROPPED);
    }
}

fn send_to(sender: &SyncSender<Box<QueryEvent>>, event: QueryEvent, dropped: &AtomicU64) {
    match sender.try_send(Box::new(event)) {
        Ok(()) => (),
        Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
            dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Where the statement went.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Route {
    Primary,
    Replica,
    /// Answered by PgDog itself: no server saw it.
    Door,
}

impl Route {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Replica => "replica",
            Self::Door => "door",
        }
    }
}

/// How the statement ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    Ok,
    /// An ErrorResponse, from the server or PgDog; the connection stays.
    Error,
    /// SQLSTATE 57014: statement_timeout or a cancel request.
    Cancelled,
    /// PgDog's own timeout (query_timeout, checkout_timeout): the client's
    /// connection was closed.
    Timeout,
    /// The client or the server connection went away before the end.
    Disconnected,
}

impl Outcome {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
            Self::Timeout => "timeout",
            Self::Disconnected => "disconnected",
        }
    }
}

/// The client connection, shared by its events.
#[derive(Debug)]
pub(crate) struct ClientInfo {
    /// `<PgDog instance>-<client pid>`: unique across restarts.
    pub(crate) id: String,
    pub(crate) addr: String,
    pub(crate) port: String,
    pub(crate) application: String,
    pub(crate) tls_version: Option<&'static str>,
    pub(crate) user: String,
    pub(crate) database: String,
}

/// A statement's text: the client's own bytes, or the parser's copy.
/// Shared, never copied, on the query path.
#[derive(Debug, Clone)]
pub(crate) enum Text {
    Shared(Arc<str>),
    Bytes(Bytes),
}

impl Text {
    /// Use the text as a `str` (lossy if it is not UTF-8).
    pub(crate) fn with_str<R>(&self, f: impl FnOnce(&str) -> R) -> R {
        match self {
            Self::Shared(text) => f(text),
            Self::Bytes(bytes) => f(&String::from_utf8_lossy(bytes)),
        }
    }
}

/// An ErrorResponse's identity.
#[derive(Debug, Clone)]
pub(crate) struct EventError {
    pub(crate) sqlstate: String,
    pub(crate) severity: String,
    pub(crate) message: String,
}

/// One statement: a simple Query, an Execute (its Parse and Bind folded
/// in), or a COPY.
#[derive(Debug)]
pub(crate) struct QueryEvent {
    /// The first byte of the request that carried it.
    pub(crate) at: SystemTime,
    pub(crate) client: Arc<ClientInfo>,
    pub(crate) route: Route,
    pub(crate) route_reason: Option<&'static str>,
    /// `host:port` of the server that answered.
    pub(crate) server: Option<Arc<str>>,
    /// Its text, for the fingerprint and command; never written.
    pub(crate) text: Option<Text>,
    pub(crate) extended: bool,
    pub(crate) prepared: bool,
    pub(crate) params: u16,
    pub(crate) in_transaction: bool,
    pub(crate) xact_seq: u64,
    pub(crate) stmt_seq: u64,
    pub(crate) duration: Duration,
    pub(crate) parse: Duration,
    pub(crate) wait: Duration,
    pub(crate) server_time: Duration,
    pub(crate) rows: u64,
    pub(crate) bytes_in: u64,
    pub(crate) bytes_out: u64,
    pub(crate) outcome: Outcome,
    pub(crate) error: Option<EventError>,
    pub(crate) retried: bool,
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    /// A client for events built in tests.
    pub(crate) fn client() -> Arc<ClientInfo> {
        Arc::new(ClientInfo {
            id: "test-1".into(),
            addr: "127.0.0.1".into(),
            port: "5555".into(),
            application: "psql".into(),
            tls_version: Some("TLSv1.3"),
            user: "pgdog".into(),
            database: "pgdog".into(),
        })
    }

    pub(crate) fn event(text: &str) -> QueryEvent {
        QueryEvent {
            at: SystemTime::now(),
            client: client(),
            route: Route::Replica,
            route_reason: Some("read"),
            server: Some("10.0.0.2:5432".into()),
            text: Some(Text::Shared(text.into())),
            extended: true,
            prepared: false,
            params: 1,
            in_transaction: false,
            xact_seq: 1,
            stmt_seq: 1,
            duration: Duration::from_micros(1500),
            parse: Duration::from_micros(20),
            wait: Duration::from_micros(30),
            server_time: Duration::from_micros(1200),
            rows: 1,
            bytes_in: 60,
            bytes_out: 90,
            outcome: Outcome::Ok,
            error: None,
            retried: false,
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::mpsc::sync_channel;
    use std::time::Instant;

    use super::test_support::event;
    use super::*;

    #[test]
    fn test_a_full_queue_drops_and_counts_without_waiting() {
        static TEST_DROPPED: AtomicU64 = AtomicU64::new(0);
        // A writer that never reads: the queue holds 2.
        let (sender, _stalled) = sync_channel(2);

        let start = Instant::now();
        for _ in 0..1000 {
            send_to(&sender, event("SELECT 1"), &TEST_DROPPED);
        }
        assert!(start.elapsed() < Duration::from_secs(1));
        assert_eq!(TEST_DROPPED.load(Ordering::Relaxed), 998);
    }
}
