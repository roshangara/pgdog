use tokio::sync::{Notify, watch};
use tokio_util::sync::CancellationToken;

/// Internal pool notifications.
pub(super) struct Comms {
    /// An idle connection is available in the pool.
    pub(super) ready: Notify,
    /// A client requests a new connection to be open
    /// or waiting for one to be returned to the pool.
    pub(super) request: Notify,
    /// Pool is shutting down.
    pub(super) shutdown: CancellationToken,
    /// Bumped each time the replica is found down: reads in flight on it
    /// that can run again elsewhere stop waiting for it.
    pub(super) down: watch::Sender<u64>,
}

impl Comms {
    /// Create new comms.
    pub(super) fn new() -> Self {
        Self {
            ready: Notify::new(),
            request: Notify::new(),
            shutdown: CancellationToken::new(),
            down: watch::Sender::new(0),
        }
    }
}
