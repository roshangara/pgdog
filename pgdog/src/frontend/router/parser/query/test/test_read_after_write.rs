use pgdog_config::ReadWriteStrategy;

use crate::frontend::client::TransactionType;

use super::setup::*;

#[test]
fn test_read_after_write_select_goes_to_primary() {
    let mut test = QueryParserTest::new().with_read_after_write();

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    assert!(command.route().is_write());
    assert!(command.route().is_read_after_write());
    assert!(!command.route().mutates());
}

#[test]
fn test_read_after_write_extended_protocol() {
    let mut test = QueryParserTest::new().with_read_after_write();

    let command = test.execute(vec![
        Parse::named("__rw", "SELECT * FROM users WHERE id = $1").into(),
        Bind::new_params("__rw", &[crate::net::bind::Parameter::new(b"1")]).into(),
        Execute::new().into(),
        Sync.into(),
    ]);

    assert!(command.route().is_write());
    assert!(command.route().is_read_after_write());
}

#[test]
fn test_read_after_write_off() {
    let mut test = QueryParserTest::new();

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    assert!(command.route().is_read());
    assert!(!command.route().is_read_after_write());
}

#[test]
fn test_read_after_write_leaves_writes_alone() {
    let mut test = QueryParserTest::new().with_read_after_write();

    let command = test.execute(vec![Query::new("INSERT INTO users (id) VALUES (1)").into()]);

    assert!(command.route().is_write());
    assert!(command.route().mutates());
    assert!(!command.route().is_read_after_write());
}

#[test]
fn test_read_after_write_transaction_already_on_primary() {
    let mut test = QueryParserTest::new()
        .with_read_after_write()
        .in_transaction(true);

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    // The conservative strategy sends it to the primary, not this rule.
    assert!(command.route().is_write());
    assert!(!command.route().is_read_after_write());
}

#[test]
fn test_read_after_write_read_only_transaction() {
    let mut test = QueryParserTest::new()
        .with_read_after_write()
        .with_transaction(TransactionType::ReadOnly);

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    assert!(command.route().is_write());
    assert!(command.route().is_read_after_write());
}

#[test]
fn test_read_after_write_aggressive_transaction() {
    let mut test = QueryParserTest::new()
        .with_read_after_write()
        .with_read_write_strategy(ReadWriteStrategy::Aggressive)
        .in_transaction(true);

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    assert!(command.route().is_write());
    assert!(command.route().is_read_after_write());
}

#[test]
fn test_read_after_write_explicit_replica_comment_wins() {
    let mut test = QueryParserTest::new().with_read_after_write();

    let command = test.execute(vec![
        Query::new("/* pgdog_role: replica */ SELECT * FROM users WHERE id = 1").into(),
    ]);

    assert!(command.route().is_read());
    assert!(!command.route().is_read_after_write());
}

#[test]
fn test_read_after_write_explicit_replica_parameter_wins() {
    let mut test = QueryParserTest::new()
        .with_read_after_write()
        .with_param("pgdog.role", "replica");

    let command = test.execute(vec![Query::new("SELECT * FROM users WHERE id = 1").into()]);

    assert!(command.route().is_read());
    assert!(!command.route().is_read_after_write());
}
