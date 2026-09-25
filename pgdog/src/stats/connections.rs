//! Connections the door makes and closes, and holds now.
//!
//! The per-connection log lines (target `pgdog::connections`) can be turned
//! down; these counts cannot, so nothing is lost when they are:
//!
//! * `client_connections_total`: client logins accepted.
//! * `server_connections_opened_total{reason}`: server connections made,
//!   by why (`client_waiting`, `below_min`, `lsn_check`, `pub_sub`, ...).
//! * `server_connections_closed_total{reason}`: server connections closed,
//!   by why; `idle` is `idle_timeout` at work.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::backend::{ConnectReason, DisconnectReason};

use super::pools::PoolMetric;
use super::{Measurement, Metric};

static CLIENTS: AtomicU64 = AtomicU64::new(0);

const OPEN_LABELS: [&str; 8] = [
    "client_waiting",
    "below_min",
    "lsn_check",
    "pub_sub",
    "healthcheck",
    "probe",
    "resharding",
    "other",
];

const CLOSE_LABELS: [&str; 12] = [
    "idle",
    "old",
    "error",
    "offline",
    "force_close",
    "out_of_sync",
    "unhealthy",
    "healthcheck",
    "server_closed",
    "credentials_refresh",
    "replication_mode",
    "other",
];

static OPENED: [AtomicU64; OPEN_LABELS.len()] = [const { AtomicU64::new(0) }; OPEN_LABELS.len()];
static CLOSED: [AtomicU64; CLOSE_LABELS.len()] = [const { AtomicU64::new(0) }; CLOSE_LABELS.len()];

fn open_index(reason: ConnectReason) -> usize {
    match reason {
        ConnectReason::ClientWaiting => 0,
        ConnectReason::BelowMin => 1,
        ConnectReason::LsnCheck => 2,
        ConnectReason::PubSub => 3,
        ConnectReason::Healthcheck => 4,
        ConnectReason::Probe => 5,
        ConnectReason::Resharding => 6,
        ConnectReason::Other => 7,
    }
}

fn close_index(reason: DisconnectReason) -> usize {
    match reason {
        DisconnectReason::Idle => 0,
        DisconnectReason::Old => 1,
        DisconnectReason::Error => 2,
        DisconnectReason::Offline => 3,
        DisconnectReason::ForceClose => 4,
        DisconnectReason::OutOfSync => 5,
        DisconnectReason::Unhealthy => 6,
        DisconnectReason::Healthcheck => 7,
        DisconnectReason::ServerClosed => 8,
        DisconnectReason::CredentialsRefresh => 9,
        DisconnectReason::ReplicationMode => 10,
        DisconnectReason::Other => 11,
    }
}

/// A client logged in.
pub(crate) fn client_connected() {
    CLIENTS.fetch_add(1, Ordering::Relaxed);
}

/// A server connection was made.
pub(crate) fn server_opened(reason: ConnectReason) {
    OPENED[open_index(reason)].fetch_add(1, Ordering::Relaxed);
}

/// A server connection was closed.
pub(crate) fn server_closed(reason: DisconnectReason) {
    CLOSED[close_index(reason)].fetch_add(1, Ordering::Relaxed);
}

/// Server connections made, for `reason`.
#[cfg(test)]
pub(crate) fn opened(reason: ConnectReason) -> u64 {
    OPENED[open_index(reason)].load(Ordering::Relaxed)
}

/// Server connections closed, for `reason`.
#[cfg(test)]
pub(crate) fn closed(reason: DisconnectReason) -> u64 {
    CLOSED[close_index(reason)].load(Ordering::Relaxed)
}

pub(crate) struct Connections;

impl Connections {
    pub(crate) fn load() -> Vec<Metric> {
        let counter = |name: &str, help: &str, measurements| {
            Metric::new(PoolMetric {
                name: name.into(),
                measurements,
                help: help.into(),
                unit: None,
                metric_type: Some("counter".into()),
            })
        };

        let clients = vec![Measurement {
            labels: vec![],
            measurement: CLIENTS.load(Ordering::Relaxed).into(),
        }];
        let opened = OPEN_LABELS
            .iter()
            .zip(OPENED.iter())
            .map(|(reason, count)| Measurement {
                labels: vec![("reason".into(), (*reason).into())],
                measurement: count.load(Ordering::Relaxed).into(),
            })
            .collect();
        let closed = CLOSE_LABELS
            .iter()
            .zip(CLOSED.iter())
            .map(|(reason, count)| Measurement {
                labels: vec![("reason".into(), (*reason).into())],
                measurement: count.load(Ordering::Relaxed).into(),
            })
            .collect();
        vec![
            counter(
                "client_connections_total",
                "Client logins accepted.",
                clients,
            ),
            counter(
                "server_connections_opened_total",
                "Server connections made, by why.",
                opened,
            ),
            counter(
                "server_connections_closed_total",
                "Server connections closed, by why (idle: idle_timeout).",
                closed,
            ),
        ]
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_counts_by_reason() {
        let before_idle = closed(DisconnectReason::Idle);
        let before_waiting = opened(ConnectReason::ClientWaiting);
        let before_clients = CLIENTS.load(Ordering::Relaxed);

        server_closed(DisconnectReason::Idle);
        server_opened(ConnectReason::ClientWaiting);
        client_connected();

        // Other tests count concurrently: at least ours.
        assert!(closed(DisconnectReason::Idle) > before_idle);
        assert!(opened(ConnectReason::ClientWaiting) > before_waiting);
        assert!(CLIENTS.load(Ordering::Relaxed) > before_clients);

        let rendered = Connections::load()
            .iter()
            .map(|m| m.to_string())
            .collect::<String>();
        assert!(rendered.contains("# TYPE server_connections_closed_total counter"));
        assert!(rendered.contains(r#"server_connections_closed_total{reason="idle"} "#));
        assert!(rendered.contains(r#"server_connections_opened_total{reason="lsn_check"} "#));
        assert!(rendered.contains("# TYPE client_connections_total counter"));
    }
}
