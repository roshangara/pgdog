use super::*;
use pg_raw_parse::{Node, nodes, nodes::VariableSetKind::*};

impl QueryParser {
    /// Handle the SET command.
    ///
    /// We allow setting shard/sharding key manually outside
    /// the normal protocol flow. This command is not forwarded to the server.
    ///
    /// All other SETs change the params on the client and are eventually sent to the server
    /// when the client is connected to the server.
    pub(super) fn set(
        &mut self,
        stmt: &nodes::VariableSetStmt,
        context: &QueryParserContext,
    ) -> Result<Command, Error> {
        if stmt.kind == VAR_RESET_ALL {
            Ok(Command::ResetAll)
        } else if stmt.kind == VAR_SET_MULTI {
            // SET SESSION CHARACTERISTICS AS TRANSACTION ... sets the
            // session's defaults for every later transaction: the client's
            // parameters, replayed on whichever connection it gets.
            if let Some(params) = Self::session_characteristics(stmt) {
                return Ok(Command::Set {
                    params,
                    route: Route::write(context.shards_calculator.shard()),
                    set_config: false,
                });
            }

            // SET TRANSACTION
            Ok(Command::Query(
                Route::write(context.shards_calculator.shard().clone())
                    .with_read(context.read_only),
            ))
        } else {
            let param = Self::parse_set_param(stmt)?;
            Ok(Command::Set {
                params: vec![param],
                route: Route::write(context.shards_calculator.shard()),
                set_config: false,
            })
        }
    }

    /// Parse a single SET statement into a SetParam: `SET x = y`,
    /// `SET x TO DEFAULT` and `RESET x`. Any other kind (`RESET ALL`,
    /// `SET TRANSACTION`, `SET x FROM CURRENT`) is an error, never a panic.
    pub(super) fn parse_set_param(stmt: &nodes::VariableSetStmt) -> Result<SetParam, Error> {
        let value = if stmt.kind == VAR_SET_VALUE {
            Some(Self::parse_set_values(stmt)?)
        } else if stmt.kind == VAR_RESET || stmt.kind == VAR_SET_DEFAULT {
            None
        } else {
            return Err(Error::UnsupportedSet(stmt.kind));
        };

        match value {
            value @ Some(_) => Ok(SetParam {
                name: stmt.name().expect("SET always has name").to_string(),
                value,
                local: stmt.is_local,
            }),
            None => Ok(SetParam {
                name: stmt.name().expect("SET always has name").to_string(),
                value: None,
                local: false,
            }),
        }
    }

    /// `SET SESSION CHARACTERISTICS AS TRANSACTION ...` (what pgjdbc's
    /// `setTransactionIsolation` sends) as the parameters it sets:
    /// `default_transaction_isolation`, `default_transaction_read_only` and
    /// `default_transaction_deferrable`. `None` for any other `SET` of
    /// several values, or a form we don't read.
    pub(super) fn session_characteristics(stmt: &nodes::VariableSetStmt) -> Option<Vec<SetParam>> {
        if stmt.kind != VAR_SET_MULTI || stmt.name() != Some("SESSION CHARACTERISTICS") {
            return None;
        }

        stmt.args()
            .iter()
            .map(|arg| {
                let Node::DefElem(elem) = arg else {
                    return None;
                };
                let Node::A_Const(value) = elem.arg() else {
                    return None;
                };
                let value = value.val()?;
                let on_off = |value: i32| if value != 0 { "on" } else { "off" };
                let (name, value) = match elem.defname()? {
                    "transaction_isolation" => (
                        "default_transaction_isolation",
                        value.string_value()?.to_owned(),
                    ),
                    "transaction_read_only" => (
                        "default_transaction_read_only",
                        on_off(value.numeric_value::<i32>()?).to_owned(),
                    ),
                    "transaction_deferrable" => (
                        "default_transaction_deferrable",
                        on_off(value.numeric_value::<i32>()?).to_owned(),
                    ),
                    _ => return None,
                };
                Some(SetParam {
                    name: name.to_owned(),
                    value: Some(ParameterValue::String(value)),
                    local: false,
                })
            })
            .collect()
    }

    /// Try to handle multi-statement queries containing SET commands.
    ///
    /// - All SETs → returns `Ok(Some(Command::Set { .. }))`
    /// - No SETs → returns `Ok(None)`, caller falls through to default parsing
    /// - Mix of SET + non-SET → returns `Err(MultiStatementMixedSet)`
    ///
    /// In session mode, returns `Ok(Some(Command::Query(..)))` immediately so that
    /// all multi-statement queries are forwarded to the server verbatim.
    pub(super) fn try_multi_set<'a>(
        &self,
        stmts: impl IntoIterator<Item = &'a nodes::RawStmt>,
        context: &QueryParserContext,
    ) -> Result<Option<Command>, Error> {
        let mut has_other = false;

        // RESET ALL, SET TRANSACTION and SET ... FROM CURRENT aren't one
        // parameter: they count as other statements.
        let params = stmts
            .into_iter()
            .filter_map(|stmt| match stmt.stmt() {
                Node::VariableSetStmt(stmt)
                    if matches!(stmt.kind, VAR_SET_VALUE | VAR_SET_DEFAULT | VAR_RESET) =>
                {
                    Some(Self::parse_set_param(stmt))
                }
                _ => {
                    has_other = true;
                    None
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        if params.is_empty() {
            Ok(None)
        } else if has_other {
            Err(Error::MultiStatementMixedSet)
        } else {
            Ok(Some(Command::Set {
                params,
                route: Route::write(context.shards_calculator.shard()),
                set_config: false,
            }))
        }
    }

    fn parse_set_values(stmt: &nodes::VariableSetStmt) -> Result<ParameterValue, Error> {
        let mut value = stmt
            .args()
            .iter()
            .map(|node| match node {
                Node::A_Const(a) => Ok(a
                    .val()
                    .expect("SET value TO NULL is a parse error")
                    .to_string()),
                // e.g. SET TIME ZONE INTERVAL '+00:00' HOUR TO MINUTE
                Node::TypeCast(tc) if let Node::A_Const(a) = tc.arg() => Ok(a
                    .val()
                    .expect("SET value TO NULL is a parse error")
                    .to_string()),
                _ => Err(Error::ColumnDecode),
            })
            .collect::<Result<Vec<_>, _>>()?;

        let value = match value.len() {
            0 => panic!("parse_set_values called on RESET or SET TRANSACTION"),
            1 => ParameterValue::String(value.pop().unwrap()),
            _ => ParameterValue::Tuple(value),
        };

        Ok(value)
    }
}
