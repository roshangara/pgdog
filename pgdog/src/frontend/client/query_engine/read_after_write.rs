//! Read-after-write: after a client writes, its reads go to the
//! primary for `read_after_write_ms`, so it sees its own writes
//! even when replicas lag behind.

use std::time::Duration;

use tokio::time::Instant;

use crate::{frontend::Command, stats::read_after_write::read_on_primary};

/// Per-client read-after-write state, kept between requests.
#[derive(Debug, Default)]
pub(crate) struct ReadAfterWrite {
    /// A statement that writes went to the primary and its transaction
    /// hasn't finished yet. Holds the window to start when it does.
    pending: Option<Duration>,
    /// Reads go to the primary until then.
    until: Option<Instant>,
}

impl ReadAfterWrite {
    /// The client's reads should go to the primary.
    pub(crate) fn active(&self) -> bool {
        self.pending.is_some() || self.until.is_some_and(|until| Instant::now() < until)
    }

    /// A request was routed: remember a write, count a read
    /// kept on the primary by this rule.
    pub(crate) fn routed(&mut self, command: &Command, window: Duration) {
        match command {
            Command::Query(route) if route.is_read_after_write() => read_on_primary(),
            Command::Query(route) if route.is_write() && route.mutates() => self.wrote(window),
            Command::Copy(_) => self.wrote(window),
            _ => (),
        }
    }

    /// A statement that writes went to the primary. The window
    /// starts when its transaction finishes. A zero window is off.
    fn wrote(&mut self, window: Duration) {
        if !window.is_zero() {
            self.pending = Some(window);
        }
    }

    /// The server is ready for the next query. Once the client is
    /// outside a transaction, its writes are committed: start the window.
    pub(crate) fn ready(&mut self, in_transaction: bool) {
        if in_transaction {
            return;
        }

        if let Some(window) = self.pending.take() {
            self.until = Some(Instant::now() + window);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::frontend::router::{
        Route,
        parser::{Shard, ShardWithPriority},
    };

    fn shard() -> ShardWithPriority {
        ShardWithPriority::new_default_unset(Shard::Direct(0))
    }

    fn write() -> Command {
        Command::Query(Route::write(shard()))
    }

    fn read() -> Command {
        Command::Query(Route::read(shard()))
    }

    /// A read inside a read/write transaction goes to the primary
    /// without being a write.
    fn read_on_primary_in_transaction() -> Command {
        Command::Query(Route::read(shard()).with_read(false))
    }

    const WINDOW: Duration = Duration::from_millis(2_000);

    #[tokio::test(start_paused = true)]
    async fn test_window_starts_when_the_write_finishes() {
        let mut raw = ReadAfterWrite::default();
        assert!(!raw.active());

        raw.routed(&write(), WINDOW);
        assert!(raw.active(), "a write in flight keeps reads on the primary");

        // A long transaction: the window must not run out before it commits.
        tokio::time::advance(WINDOW * 3).await;
        raw.ready(true);
        assert!(raw.active());

        raw.ready(false);
        tokio::time::advance(WINDOW - Duration::from_millis(1)).await;
        assert!(raw.active());

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(!raw.active());
    }

    #[tokio::test(start_paused = true)]
    async fn test_every_write_restarts_the_window() {
        let mut raw = ReadAfterWrite::default();

        raw.routed(&write(), WINDOW);
        raw.ready(false);
        tokio::time::advance(WINDOW / 2).await;

        raw.routed(&write(), WINDOW);
        raw.ready(false);
        tokio::time::advance(WINDOW - Duration::from_millis(1)).await;
        assert!(raw.active());

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(!raw.active());
    }

    #[tokio::test(start_paused = true)]
    async fn test_reads_do_not_open_the_window() {
        let mut raw = ReadAfterWrite::default();

        for command in [read(), read_on_primary_in_transaction()] {
            raw.routed(&command, WINDOW);
            raw.ready(false);
            assert!(!raw.active());
        }

        // A read kept on the primary by the rule doesn't extend it.
        raw.routed(&write(), WINDOW);
        raw.ready(false);
        tokio::time::advance(WINDOW / 2).await;

        let mut kept = Route::read(shard());
        kept.set_read_after_write();
        raw.routed(&Command::Query(kept), WINDOW);
        raw.ready(false);
        tokio::time::advance(WINDOW / 2).await;
        assert!(!raw.active());
    }

    #[test]
    fn test_zero_window_is_off() {
        let mut raw = ReadAfterWrite::default();
        raw.routed(&write(), Duration::ZERO);
        assert!(!raw.active());
        raw.ready(false);
        assert!(!raw.active());
    }
}
