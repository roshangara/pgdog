use pg_raw_parse::{Node, nodes};

mod pg_catalog;

const WRITE_ONLY: &[&str] = &["nextval", "setval"];

const CROSS_SHARD: &[(Option<&str>, &str)] = &[(Some("pgdog"), "install_sharded_sequence")];

#[derive(Default, Debug, Copy, Clone)]
pub(crate) struct FunctionBehavior {
    pub(crate) writes: bool,
    pub(crate) cross_shard: bool,
}

pub(crate) struct Function<'a> {
    pub(crate) name: &'a str,
    pub(crate) schema: Option<&'a str>,
}

impl<'a> Function<'a> {
    /// Build a Function from a qualified name list (as found in `FuncCall.funcname`).
    /// The last element is the function name; the preceding element (if any) is the
    /// schema.
    pub(crate) fn from_strings(
        mut parts: impl DoubleEndedIterator<Item = &'a str>,
    ) -> Option<Self> {
        Some(Self {
            name: parts.next_back()?,
            schema: parts.next_back(),
        })
    }

    /// This function likely writes.
    pub(crate) fn behavior(&self, routing: &FunctionRouting) -> FunctionBehavior {
        FunctionBehavior {
            writes: WRITE_ONLY.contains(&self.name) || routing.primary(self),
            cross_shard: CROSS_SHARD.contains(&(self.schema, self.name)),
        }
    }

    /// A PostgreSQL built-in known to be safe on a replica:
    /// immutable or stable in every overload.
    fn read_only(&self) -> bool {
        matches!(self.schema, None | Some("pg_catalog"))
            && pg_catalog::READ_ONLY.binary_search(&self.name).is_ok()
    }

    pub(crate) fn extract_func_call(node: Node<'a>) -> Option<&'a nodes::FuncCall> {
        match node {
            Node::FuncCall(func) => Some(func),
            Node::TypeCast(cast) => Self::extract_func_call(cast.arg()),
            Node::NullTest(test) => Self::extract_func_call(test.arg()),
            _ => None,
        }
    }
}

/// Functions that send a `SELECT` to the primary, from
/// `primary_functions` and `route_unknown_functions_to_primary`.
#[derive(Debug, Clone, Default)]
pub(crate) struct FunctionRouting {
    /// `primary_functions` entries as (schema, name).
    primary: Vec<(Option<String>, String)>,
    /// Any function not known to be read-only goes to the primary.
    unknown: bool,
}

impl FunctionRouting {
    pub(crate) fn new(primary_functions: &[String], route_unknown_to_primary: bool) -> Self {
        let primary = primary_functions
            .iter()
            .map(|entry| match entry.rsplit_once('.') {
                Some((schema, name)) => (Some(schema.to_owned()), name.to_owned()),
                None => (None, entry.to_owned()),
            })
            .collect();

        Self {
            primary,
            unknown: route_unknown_to_primary,
        }
    }

    /// The function must run on the primary.
    ///
    /// An unqualified entry matches the function in any schema. A qualified
    /// entry matches calls in its schema and unqualified calls, which
    /// `search_path` may resolve to it.
    fn primary(&self, function: &Function<'_>) -> bool {
        let listed = self.primary.iter().any(|(schema, name)| {
            name == function.name
                && match (schema.as_deref(), function.schema) {
                    (Some(schema), Some(called)) => schema == called,
                    _ => true,
                }
        });

        listed || (self.unknown && !function.read_only())
    }
}

impl<'a> TryFrom<Node<'a>> for Function<'a> {
    type Error = ();

    fn try_from(value: Node<'a>) -> Result<Self, Self::Error> {
        Self::extract_func_call(value)
            .and_then(|f| Self::from_strings(f.funcname().iter().filter_map(Node::as_str)))
            .ok_or(())
    }
}

#[cfg(test)]
mod test {
    use pg_raw_parse::parse;

    use super::*;

    #[test]
    fn test_function() {
        let query = "SELECT pg_advisory_lock(234234), pg_try_advisory_lock(23234)::bool";
        funcs(query, |func| {
            assert!(func.name.contains("advisory_lock"));
            assert!(func.schema.is_none());
            assert!(!func.behavior(&FunctionRouting::default()).cross_shard);
        });
    }

    fn funcs(query: &str, mut check: impl FnMut(Function<'_>)) {
        let ast = parse(query).unwrap();
        let Node::SelectStmt(stmt) = ast.stmts().next().unwrap() else {
            unreachable!();
        };

        for node in stmt.target_list() {
            let func = Function::try_from(node.val()).unwrap();
            check(func);
        }
    }

    fn writes(query: &str, routing: &FunctionRouting) -> bool {
        let mut writes = false;
        funcs(query, |func| {
            writes = writes || func.behavior(routing).writes
        });
        writes
    }

    #[test]
    fn test_pg_catalog_list_is_sorted() {
        // binary_search needs it; a duplicate would mean a bad generator run.
        assert!(pg_catalog::READ_ONLY.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn test_primary_functions() {
        let routing =
            FunctionRouting::new(&["my_writing_fn".into(), "billing.charge".into()], false);

        assert!(writes("SELECT my_writing_fn()", &routing));
        assert!(writes("SELECT app.my_writing_fn(1)", &routing));
        assert!(writes("SELECT billing.charge(1)", &routing));
        // Unqualified: search_path may resolve it to billing.charge.
        assert!(writes("SELECT charge(1)", &routing));
        assert!(!writes("SELECT other.charge(1)", &routing));
        assert!(!writes("SELECT my_reading_fn()", &routing));
        assert!(!writes("SELECT now()", &routing));
        // Built-in writes stay writes.
        assert!(writes("SELECT nextval('seq')", &routing));
        assert!(writes(
            "SELECT setval('seq', 1)",
            &FunctionRouting::default()
        ));
    }

    #[test]
    fn test_unknown_functions_to_primary() {
        let routing = FunctionRouting::new(&[], true);

        for query in [
            "SELECT my_fn()",
            "SELECT public.lower('A')",
            "SELECT random()",
            "SELECT gen_random_uuid()",
            "SELECT pg_is_in_recovery()",
            "SELECT txid_current()",
            "SELECT pg_advisory_lock(1)",
            "SELECT nextval('seq')",
        ] {
            assert!(writes(query, &routing), "{query} should go to the primary");
        }

        for query in [
            "SELECT now()",
            "SELECT pg_catalog.now()",
            "SELECT lower('A'), upper('a'), length('abc')",
            "SELECT count(*)",
            "SELECT inet_server_addr()",
            "SELECT current_setting('search_path')",
            "SELECT to_char(now(), 'YYYY')",
            "SELECT json_build_object('a', 1)",
        ] {
            assert!(!writes(query, &routing), "{query} should stay on a replica");
        }

        // Off by default.
        assert!(!writes("SELECT my_fn()", &FunctionRouting::default()));
    }

    fn first_func(query: &str, check: impl FnOnce(Function<'_>)) {
        let mut check = Some(check);
        funcs(query, |func| {
            if let Some(c) = check.take() {
                c(func)
            }
        });
    }

    #[test]
    fn test_cross_shard_function() {
        first_func(
            "SELECT pgdog.install_sharded_sequence('foo', 'id')",
            |func| {
                assert_eq!(func.name, "install_sharded_sequence");
                assert_eq!(func.schema, Some("pgdog"));
                assert!(func.behavior(&FunctionRouting::default()).cross_shard);
            },
        );

        // Same function name without the schema should not be flagged.
        first_func("SELECT install_sharded_sequence('foo', 'id')", |func| {
            assert_eq!(func.name, "install_sharded_sequence");
            assert!(func.schema.is_none());
            assert!(!func.behavior(&FunctionRouting::default()).cross_shard);
        });

        // Different schema should not be flagged.
        first_func(
            "SELECT other.install_sharded_sequence('foo', 'id')",
            |func| {
                assert_eq!(func.schema, Some("other"));
                assert!(!func.behavior(&FunctionRouting::default()).cross_shard);
            },
        );
    }
}
