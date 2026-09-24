use bytes::Bytes;

use super::setup::*;

#[test]
fn test_write_function_advisory_lock() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SELECT pg_advisory_lock(123)").into()]);

    assert!(command.route().is_write());
    assert!(command.route().is_lock_session());
}

#[test]
fn test_write_functions_prepared() {
    let mut test = QueryParserTest::new();
    let command = test.execute(vec![
        Parse::named("test", "SELECT pg_advisory_lock($1) IS NOT NULL").into(),
        Bind::new_params(
            "test",
            &[crate::net::bind::Parameter {
                len: 4,
                data: Bytes::from(b"1234".to_vec()),
            }],
        )
        .into(),
    ]);
    assert!(command.route().is_write());
    assert!(command.route().is_lock_session());
}

#[test]
fn test_write_function_nextval() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SELECT nextval('234')").into()]);

    assert!(command.route().is_write());
    assert!(!command.route().is_lock_session());
}

#[test]
fn test_cross_shard_install_sharded_sequence() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SELECT pgdog.install_sharded_sequence('foo', 'id')").into(),
    ]);

    assert!(command.route().is_cross_shard());
}

#[test]
fn test_install_sharded_sequence_without_schema_not_cross_shard() {
    // Without the `pgdog.` schema qualifier we should not flag the call
    // as a cross-shard function — it could be any user-defined function.
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![
        Query::new("SELECT install_sharded_sequence('foo', 'id')").into(),
    ]);

    assert!(!command.route().is_cross_shard());
}

fn functions_test(primary_functions: &[&str], unknown_to_primary: bool) -> QueryParserTest {
    let mut config = crate::config::config().as_ref().clone();
    config.config.general.primary_functions =
        primary_functions.iter().map(|f| f.to_string()).collect();
    config.config.general.route_unknown_functions_to_primary = unknown_to_primary;
    QueryParserTest::new_with_config(&config)
}

fn routes_to_primary(test: &mut QueryParserTest, query: &str) -> bool {
    let command = test.execute(vec![Query::new(query).into()]);
    command.route().is_write()
}

#[test]
fn test_writing_function_goes_to_replica_by_default() {
    let mut test = functions_test(&[], false);

    assert!(!routes_to_primary(&mut test, "SELECT my_writing_fn()"));
}

#[test]
fn test_primary_functions_anywhere_in_select() {
    let mut test = functions_test(&["my_writing_fn"], false);

    for query in [
        "SELECT my_writing_fn()",
        "SELECT * FROM my_writing_fn()",
        "SELECT (SELECT my_writing_fn())",
        "SELECT id FROM users WHERE id = my_writing_fn()",
        "SELECT public.my_writing_fn()::text",
    ] {
        let command = test.execute(vec![Query::new(query).into()]);
        assert!(command.route().is_write(), "{query}");
        assert!(command.route().mutates(), "{query}");
    }

    assert!(!routes_to_primary(&mut test, "SELECT my_reading_fn()"));
}

#[test]
fn test_primary_functions_extended_protocol() {
    let mut test = functions_test(&["app.my_writing_fn"], false);

    let command = test.execute(vec![
        Parse::named("fn", "SELECT app.my_writing_fn($1)").into(),
        Bind::new_params(
            "fn",
            &[crate::net::bind::Parameter {
                len: 1,
                data: Bytes::from(b"1".to_vec()),
            }],
        )
        .into(),
    ]);
    assert!(command.route().is_write());
}

#[test]
fn test_unknown_functions_to_primary() {
    let mut test = functions_test(&[], true);

    for query in [
        "SELECT my_writing_fn()",
        "SELECT id FROM users WHERE id = my_fn(1)",
        "SELECT * FROM my_set_returning_fn()",
        "SELECT random()",
        "SELECT txid_current()",
    ] {
        assert!(routes_to_primary(&mut test, query), "{query}");
    }
}

#[test]
fn test_unknown_functions_leave_built_ins_on_replicas() {
    let mut test = functions_test(&[], true);

    // SQL syntax the grammar turns into pg_catalog function calls.
    for query in [
        "SELECT 1",
        "SELECT * FROM users WHERE id = 1",
        "SELECT now(), CURRENT_TIMESTAMP, CURRENT_DATE, LOCALTIMESTAMP, current_user, session_user",
        "SELECT EXTRACT(epoch FROM now()), date_part('year', now()), date_trunc('day', now())",
        "SELECT substring('abc' from 1 for 2), trim(' a '), position('a' in 'abc')",
        "SELECT overlay('abc' placing 'x' from 1), 'a' IS NORMALIZED, normalize('a')",
        "SELECT now() AT TIME ZONE 'UTC', (now(), now()) OVERLAPS (now(), now())",
        "SELECT COALESCE(NULL, 1), NULLIF(1, 2), GREATEST(1, 2), LEAST(1, 2)",
        "SELECT count(*), sum(id), max(id), array_agg(id), string_agg(email, ',') FROM users",
        "SELECT row_number() OVER (), rank() OVER (ORDER BY id) FROM users",
        "SELECT jsonb_build_object('a', 1), to_jsonb(1), json_agg(1), jsonb_set('{}', '{a}', '1')",
        "SELECT * FROM generate_series(1, 10), unnest(ARRAY[1, 2])",
        "SELECT inet_server_addr(), pg_backend_pid(), version(), current_database()",
        "SELECT 'a' LIKE 'b', 'a' ILIKE 'b', 'a' SIMILAR TO 'b', 'a' ~ 'b'",
        "SELECT CAST('1' AS int), '1'::numeric, to_char(now(), 'YYYY'), format('%s', 1)",
        "SELECT pg_catalog.lower('A'), pg_catalog.pg_get_userbyid(10)",
    ] {
        assert!(!routes_to_primary(&mut test, query), "{query}");
    }
}
