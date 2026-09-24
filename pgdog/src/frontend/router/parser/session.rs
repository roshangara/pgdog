//! Session state a statement leaves on its server connection.
//!
//! In transaction mode a client's session lives in PgDog, not on any one
//! server connection: parameters are replayed on whichever connection the
//! client gets next, and state that can't be replayed (temporary tables,
//! advisory locks, cursors `WITH HOLD`) pins the client to its connection.
//! These are the changes the query engine records for the client once the
//! statement (or every statement of a multi-statement query) has run.

use crate::frontend::client::query_engine::TempTableChange;

use super::SetParam;

/// One change, in statement order.
#[derive(Debug, Clone)]
pub(crate) enum SessionChange {
    /// `SET` (with a value) or `RESET` (without) of one parameter.
    /// `transaction`: made inside a transaction that is still open when
    /// the query ends, so it's committed or rolled back with it.
    Param { param: SetParam, transaction: bool },
    /// `RESET ALL`.
    ResetAll { transaction: bool },
    /// `CREATE TEMP TABLE`, `DROP TABLE`.
    TempTable(TempTableChange),
    /// `DISCARD TEMP`.
    DiscardTemp,
    /// `UNLISTEN <channel>`, or `UNLISTEN *` (`None`).
    Unlisten(Option<String>),
}

/// Session changes of a statement or of a multi-statement query.
#[derive(Debug, Clone, Default)]
pub(crate) struct SessionChanges {
    /// The changes to record for the client, in order.
    pub(crate) changes: Vec<SessionChange>,
    /// The statements changed the server connection's session in ways the
    /// client's parameters don't replay: clean it (`RESET ALL`, unlock,
    /// `DISCARD TEMP`, `CLOSE ALL`) before another client gets it.
    pub(crate) dirty: bool,
}

impl SessionChanges {
    /// Nothing to record and nothing to clean.
    pub(crate) fn is_empty(&self) -> bool {
        self.changes.is_empty() && !self.dirty
    }
}
