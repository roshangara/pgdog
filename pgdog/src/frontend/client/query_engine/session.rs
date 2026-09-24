//! Record the session state a statement left on its server connection.

use crate::frontend::{
    SetParam,
    router::{
        parameter_hints::PGDOG_PIN,
        parser::{SessionChange, SessionChanges},
    },
};

use super::*;

impl QueryEngine {
    /// The statement(s) of this request ran: record what they changed in
    /// the client's session. Called at ReadyForQuery, before the server
    /// connection is released.
    ///
    /// A request that returned an error changed nothing we record: its
    /// implicit transaction was rolled back. Its server connection is still
    /// cleaned before reuse if the statements could have left state on it.
    pub(super) fn record_session_changes(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        changes: &SessionChanges,
    ) {
        if !self.request_error {
            let in_transaction = context.in_transaction();

            for change in &changes.changes {
                match change {
                    SessionChange::Param { param, transaction } => {
                        let transaction = *transaction || in_transaction;
                        Self::record_param(context, param, transaction);
                        // As QueryEngine::set: pgdog.pin outside a transaction.
                        if param.name == PGDOG_PIN && !transaction {
                            self.manual_lock = param
                                .value
                                .as_ref()
                                .and_then(|value| value.as_str())
                                .map(|value| matches!(value, "true" | "t"))
                                .unwrap_or_default();
                        }
                    }
                    SessionChange::ResetAll { .. } => context.params.reset_all(),
                    SessionChange::TempTable(change) => {
                        self.temp_tables.update(change, in_transaction)
                    }
                    SessionChange::DiscardTemp => self.temp_tables.discard(in_transaction),
                    SessionChange::HoldCursor(name) => {
                        self.hold_cursors.declare(name, in_transaction)
                    }
                    SessionChange::CloseCursor(name) => self.hold_cursors.close(name.as_deref()),
                    SessionChange::Unlisten(Some(channel)) => self.backend.unlisten(channel),
                    SessionChange::Unlisten(None) => self.backend.unlisten_all(),
                }
            }

            if !in_transaction {
                self.comms.update_params(context.params);
            }
        }

        if changes.dirty {
            self.backend.mark_dirty();
        }
    }

    /// A `SET` or `RESET` the server ran, as `QueryEngine::set` records one
    /// it answers itself.
    fn record_param(context: &mut QueryEngineContext<'_>, param: &SetParam, transaction: bool) {
        match param.value.clone() {
            Some(value) if transaction => {
                context
                    .params
                    .insert_transaction(&param.name, value, param.local);
            }
            Some(value) => {
                context.params.insert(&param.name, value);
            }
            None => context.params.reset(&param.name),
        }
    }
}
