use super::*;

impl QueryEngine {
    /// Check if we need to lock the backend to this client, and do so
    /// if needed.
    pub(super) fn check_lock(&mut self) {
        // The presence of advisory locks or manual pin
        // indicates we cannot release the backend.
        let locked = self.advisory_locks.locked()
            || !self.temp_tables.is_empty()
            || !self.hold_cursors.is_empty()
            || self.manual_lock;

        self.backend.lock(locked);
        self.stats.locked(locked);
    }
}

impl QueryEngine {
    /// Ask the server whether the client still holds a session advisory
    /// lock, and release its connection if it doesn't.
    pub(super) async fn verify_advisory_locks(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        use crate::net::{DataRow, FromBytes, Protocol, ToBytes};

        let messages = self
            .backend
            .execute(advisory_lock::HELD_ADVISORY_LOCKS)
            .await?;
        let held = messages
            .iter()
            .find(|message| message.code() == 'D')
            .and_then(|message| DataRow::from_bytes(message.to_bytes()).ok())
            .and_then(|row| row.get_int(0, true))
            // No answer we can read: keep the client where it is.
            .unwrap_or(1);

        self.advisory_locks.verified(held);
        self.check_lock();
        self.cleanup_backend(context).await
    }
}
