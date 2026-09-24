//! Several statements in one simple query, on a database with one shard.
//!
//! The query goes to the server whole, as the client wrote it, so its
//! statements share PostgreSQL's implicit transaction and a `SET` applies
//! to the statements after it. It goes to the primary if any statement may
//! write, and the session state its statements leave behind (`SET`,
//! `RESET`, temporary tables, advisory locks, `UNLISTEN`) is recorded for
//! the client as if they had been sent one by one.
//!
//! Statements that may write: anything but a `SELECT` without writes,
//! `SHOW`, `SET`/`RESET`, `DISCARD`, `CLOSE`, `FETCH`, `UNLISTEN`,
//! `PREPARE`/`DEALLOCATE`, `BEGIN READ ONLY`, `COMMIT`/`ROLLBACK`/savepoints.
//! A `SELECT` writes when it has a writing CTE, a locking clause, a function
//! that writes (`primary_functions`, `route_unknown_functions_to_primary`),
//! an advisory lock or an `INTO` clause.

use pg_raw_parse::nodes::{DiscardMode, TransactionStmtKind::*, VariableSetKind::*};

use super::{set_config::parse_args as parse_set_config, *};
use crate::frontend::router::parser::{
    SessionChange, SessionChanges,
    function::FunctionRouting,
    statement::{AdvisoryLock, AdvisoryLocks, LockScope},
};

impl QueryParser {
    /// Route a multi-statement query on a single-shard database.
    ///
    /// Returns `None` for a query this doesn't handle (it contains `COPY`,
    /// which keeps its own path).
    pub(super) fn single_shard_batch(
        &self,
        ast: &Ast,
        context: &QueryParserContext,
    ) -> Result<Option<Command>, Error> {
        if ast
            .ast
            .stmts()
            .any(|stmt| matches!(stmt, Node::CopyStmt(_)))
        {
            return Ok(None);
        }

        let functions = context.router_context.cluster.function_routing();
        let mut batch = Batch::new(context.router_context.transaction().is_some());

        for stmt in ast.ast.stmts() {
            batch.statement(stmt, functions, &context.sharding_schema)?;
        }

        let Batch {
            writes,
            schema_changed,
            locks,
            changes,
            ..
        } = batch.finish();

        let writes = writes || self.write_override;
        let shard = context.shards_calculator.shard();
        let route = if writes {
            Route::write(shard)
        } else {
            Route::read(shard)
        };

        Ok(Some(Command::Query(
            route
                .with_mutates(writes)
                .with_schema_changed(schema_changed)
                .with_advisory_locks(locks)
                .with_session_changes(Some(changes)),
        )))
    }
}

/// What a multi-statement query does, statement by statement.
#[derive(Debug, Default)]
struct Batch {
    /// Some statement may write.
    writes: bool,
    /// Some statement changes the schema.
    schema_changed: bool,
    /// Advisory locks taken and released, net of each other.
    locks: AdvisoryLocks,
    /// Session changes known to be committed.
    changes: SessionChanges,
    /// Session changes since the last commit or rollback.
    pending: Vec<SessionChange>,
    /// Inside an explicit transaction block (BEGIN, or the client's own
    /// transaction the query runs in).
    explicit: bool,
    /// Advisory lock calls in statement order.
    lock_calls: Vec<AdvisoryLock>,
}

impl Batch {
    fn new(in_transaction: bool) -> Self {
        Self {
            explicit: in_transaction,
            ..Default::default()
        }
    }

    /// Account for one statement.
    fn statement(
        &mut self,
        stmt: Node<'_>,
        functions: &FunctionRouting,
        sharding_schema: &ShardingSchema,
    ) -> Result<(), Error> {
        match stmt {
            Node::TransactionStmt(stmt) => self.transaction(stmt),

            Node::VariableSetStmt(stmt) => self.set(stmt)?,

            Node::SelectStmt(select) if select.into_clause().is_some() => {
                self.ddl(stmt, sharding_schema)?
            }

            Node::SelectStmt(select) => {
                if let Some(set_config) = extract_set_config(select)
                    && let Some(param) = parse_set_config(set_config)
                {
                    self.changes.dirty = true;
                    self.param(param);
                } else {
                    let (writes, locks) = select_writes(select, functions, sharding_schema);
                    self.writes |= writes;
                    self.lock_calls.extend(locks.iter().copied());
                }
            }

            Node::VariableShowStmt(_)
            | Node::FetchStmt(_)
            | Node::DeallocateStmt(_)
            | Node::PrepareStmt(_) => (),

            Node::DiscardStmt(stmt) => {
                if stmt.target == DiscardMode::DISCARD_TEMP {
                    self.pending.push(SessionChange::DiscardTemp);
                }
            }

            Node::ClosePortalStmt(_) => (),

            Node::UnlistenStmt(stmt) => {
                self.pending.push(SessionChange::Unlisten(
                    stmt.conditionname().map(ToOwned::to_owned),
                ));
            }

            // PgDog serves LISTEN itself in transaction mode; a LISTEN run
            // on a pooled server connection would outlive the client.
            Node::ListenStmt(_) => return Err(Error::MultiStatementListen),

            node => self.ddl(node, sharding_schema)?,
        }

        Ok(())
    }

    /// A statement that may write: DML, DDL, NOTIFY, CALL, ...
    fn ddl(&mut self, node: Node<'_>, sharding_schema: &ShardingSchema) -> Result<(), Error> {
        self.writes = true;

        let mut calculator = ShardsWithPriority::default();
        if let Command::Query(route) =
            QueryParser::shard_ddl(node, sharding_schema, &mut calculator)?
        {
            self.schema_changed |= route.is_schema_changed();
            if let Some(change) = route.temp_table_change {
                self.pending.push(SessionChange::TempTable(change));
            }
        }

        Ok(())
    }

    fn transaction(&mut self, stmt: &nodes::TransactionStmt) {
        match stmt.kind {
            TRANS_STMT_BEGIN | TRANS_STMT_START => {
                // A read-write transaction goes to the primary.
                if QueryParser::transaction_type(stmt.options())
                    != Some(crate::frontend::client::TransactionType::ReadOnly)
                {
                    self.writes = true;
                }
                self.explicit = true;
            }

            TRANS_STMT_COMMIT => {
                self.changes.changes.append(&mut self.pending);
                self.explicit = stmt.chain;
            }

            TRANS_STMT_ROLLBACK => {
                self.pending.clear();
                self.explicit = stmt.chain;
            }

            TRANS_STMT_PREPARE | TRANS_STMT_COMMIT_PREPARED | TRANS_STMT_ROLLBACK_PREPARED => {
                self.writes = true;
                self.pending.clear();
                self.explicit = false;
            }

            // SAVEPOINT, RELEASE, ROLLBACK TO: the changes stay pending.
            _ => (),
        }
    }

    fn set(&mut self, stmt: &nodes::VariableSetStmt) -> Result<(), Error> {
        self.changes.dirty = true;

        match stmt.kind {
            VAR_RESET_ALL => self
                .pending
                .push(SessionChange::ResetAll { transaction: false }),

            // SET TRANSACTION ... lasts one transaction. SET x FROM CURRENT
            // has no value we can know: the connection is cleaned up
            // when it's released (dirty).
            VAR_SET_MULTI | VAR_SET_CURRENT => (),

            _ => {
                let param = QueryParser::parse_set_param(stmt)?;
                self.param(param);
            }
        }

        Ok(())
    }

    fn param(&mut self, param: SetParam) {
        self.pending.push(SessionChange::Param {
            param,
            transaction: false,
        });
    }

    /// The query ended: what it leaves behind.
    fn finish(mut self) -> Self {
        if self.explicit {
            // The query leaves a transaction open: its changes are
            // committed or rolled back with it.
            for mut change in std::mem::take(&mut self.pending) {
                match &mut change {
                    SessionChange::Param { transaction, .. }
                    | SessionChange::ResetAll { transaction } => *transaction = true,
                    _ => (),
                }
                self.changes.changes.push(change);
            }
        } else {
            // The implicit transaction commits at the end of the query.
            self.changes.changes.append(&mut self.pending);
        }

        // SET LOCAL ends with its transaction.
        self.changes.changes.retain(|change| match change {
            SessionChange::Param { param, transaction } => !param.local || *transaction,
            _ => true,
        });

        self.locks = net_advisory_locks(&self.lock_calls);

        self
    }
}

/// A `SELECT` may write, and the advisory locks it takes or releases.
pub(super) fn select_writes(
    stmt: &nodes::SelectStmt,
    functions: &FunctionRouting,
    sharding_schema: &ShardingSchema,
) -> (bool, AdvisoryLocks) {
    let mut writes = false;
    pg_raw_parse::walk::walk(stmt.into(), |node| match node {
        Node::CommonTableExpr(expr) => {
            if !matches!(expr.ctequery(), Node::SelectStmt(_)) {
                writes = true;
            }
        }
        Node::LockingClause(_) => writes = true,
        Node::FuncCall(f) => {
            if let Some(f) =
                Function::from_strings(f.funcname().into_iter().filter_map(Node::as_str))
            {
                writes = writes || f.behavior(functions).writes;
            }
        }
        _ => (),
    });

    let locks =
        StatementParser::new(stmt.into(), None, sharding_schema, None).extract_advisory_locks();

    (writes || !locks.is_empty(), locks)
}

/// The advisory lock calls of several statements, in order, as one set
/// the query engine applies releases first, then acquisitions.
fn net_advisory_locks(calls: &[AdvisoryLock]) -> AdvisoryLocks {
    use std::collections::BTreeMap;

    // Key -> held after the query (true) or released (false).
    let mut keys: BTreeMap<i64, bool> = BTreeMap::new();
    let mut released_all = false;
    let mut result = vec![];

    for call in calls {
        match (call.id, call.unlock) {
            (_, true) if call.all => {
                keys.clear();
                released_all = true;
            }
            (Some(id), true) => {
                keys.insert(id, false);
            }
            (Some(id), false) => {
                if call.scope == LockScope::Session {
                    keys.insert(id, true);
                }
            }
            // Keys we can't read: the engine asks the server.
            (None, _) => result.push(*call),
        }
    }

    if released_all {
        result.push(AdvisoryLock::unlock_all());
    }

    for (id, held) in keys {
        result.push(AdvisoryLock {
            id: Some(id),
            unlock: !held,
            scope: LockScope::Session,
            all: false,
        });
    }

    AdvisoryLocks::from_iter(result)
}
