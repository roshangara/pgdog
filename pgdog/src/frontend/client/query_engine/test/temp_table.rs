use super::prelude::*;

#[tokio::test]
async fn test_creating_temp_tables_locks_client() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new("CREATE TEMP TABLE foo (id int)"))
        .await;
    client.read_until('Z').await.unwrap();

    assert!(client.backend_locked());

    client
        .send_simple(Query::new("CREATE TEMP TABLE bar (id int)"))
        .await;
    client.read_until('Z').await.unwrap();

    assert!(client.backend_locked());

    client.send_simple(Query::new("DROP TABLE foo")).await;
    client.read_until('Z').await.unwrap();

    assert!(client.backend_locked());

    client.send_simple(Query::new("DROP TABLE bar")).await;
    client.read_until('Z').await.unwrap();

    assert!(!client.backend_locked());
}

#[tokio::test]
async fn test_temp_tables_on_commit() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();
    client
        .send_simple(Query::new("CREATE TEMP TABLE foo (id int) ON COMMIT DROP"))
        .await;
    client.read_until('Z').await.unwrap();
    assert!(client.backend_locked());

    client.send_simple(Query::new("COMMIT")).await;
    client.read_until('Z').await.unwrap();
    assert!(!client.backend_locked());
}

#[tokio::test]
async fn test_temp_tables_drop_on_rollback() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();
    client
        .send_simple(Query::new("CREATE TEMP TABLE foo (id int)"))
        .await;
    client.read_until('Z').await.unwrap();
    assert!(client.backend_locked());

    client.send_simple(Query::new("COMMIT")).await;
    client.read_until('Z').await.unwrap();
    assert!(client.backend_locked());

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();
    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();
    assert!(client.backend_locked());

    client.send_simple(Query::new("DROP TABLE foo")).await;
    client.read_until('Z').await.unwrap();
    assert!(!client.backend_locked());

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();
    client
        .send_simple(Query::new("CREATE TEMP TABLE foo (id int)"))
        .await;
    client.read_until('Z').await.unwrap();
    assert!(client.backend_locked());

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();
    assert!(!client.backend_locked());
}

#[tokio::test]
async fn test_discard_temp_unpins_client() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new("CREATE TEMP TABLE foo (id int)"))
        .await;
    client.read_until('Z').await.unwrap();
    assert!(client.backend_locked());

    client.send_simple(Query::new("DISCARD TEMP")).await;
    client.read_until('Z').await.unwrap();

    assert!(!client.backend_locked());
    assert!(!client.backend_connected());
}

#[tokio::test]
async fn test_discard_temp_rollback_restores_pin() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new("CREATE TEMP TABLE foo (id int)"))
        .await;
    client.read_until('Z').await.unwrap();

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();
    client.send_simple(Query::new("DISCARD TEMP")).await;
    client.read_until('Z').await.unwrap();
    assert!(client.backend_locked());
    assert!(client.backend_connected());

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();

    assert!(client.backend_locked());
    assert!(client.backend_connected());
}

#[tokio::test]
async fn test_discard_temp_commit_releases_pin() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new("CREATE TEMP TABLE foo (id int)"))
        .await;
    client.read_until('Z').await.unwrap();

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();
    client.send_simple(Query::new("DISCARD TEMP")).await;
    client.read_until('Z').await.unwrap();
    assert!(client.backend_locked());
    assert!(client.backend_connected());

    client.send_simple(Query::new("COMMIT")).await;
    client.read_until('Z').await.unwrap();

    assert!(!client.backend_locked());
    assert!(!client.backend_connected());
}

/// `CREATE TEMP TABLE ... AS` and `SELECT ... INTO TEMP` create temporary
/// tables too: they go to the primary (a read-only replica refuses SELECT
/// INTO) and pin the client like `CREATE TEMP TABLE` (ganjban lab P-9).
#[tokio::test]
async fn test_temp_table_as_and_select_into_pin() {
    use crate::{
        backend::databases::reload_from_existing,
        config::{config, load_test_replicas, set},
    };
    use pgdog_config::ReadWriteSplit;
    use std::ops::Deref;

    load_test_replicas();
    let mut config = config().deref().clone();
    config.config.general.read_write_split = ReadWriteSplit::ExcludePrimary;
    set(config).unwrap();
    reload_from_existing().unwrap();

    let mut client = TestClient::new(Parameters::default()).await;

    for (create, table) in [
        ("CREATE TEMP TABLE test_tas AS SELECT 1 AS x", "test_tas"),
        ("SELECT 1 AS x INTO TEMP test_tsi", "test_tsi"),
        ("SELECT 1 AS x INTO TEMPORARY TABLE test_tsi2", "test_tsi2"),
    ] {
        client.send_simple(Query::new(create)).await;
        client.read_until('Z').await.unwrap();
        assert!(client.backend_locked(), "{create}");

        for _ in 0..20 {
            client
                .send_simple(Query::new(format!("SELECT count(*) FROM {table}")))
                .await;
            client.read_until('Z').await.unwrap();
        }

        client
            .send_simple(Query::new(format!("DROP TABLE {table}")))
            .await;
        client.read_until('Z').await.unwrap();
        assert!(!client.backend_locked(), "{create}");
    }

    // A permanent table from SELECT ... INTO is a write, not a pin.
    client
        .send_simple(Query::new(
            "DROP TABLE IF EXISTS test_select_into_permanent",
        ))
        .await;
    client.read_until('Z').await.unwrap();
    client
        .send_simple(Query::new("SELECT 1 AS x INTO test_select_into_permanent"))
        .await;
    client.read_until('Z').await.unwrap();
    assert!(!client.backend_locked());
    client
        .send_simple(Query::new("DROP TABLE test_select_into_permanent"))
        .await;
    client.read_until('Z').await.unwrap();
}
