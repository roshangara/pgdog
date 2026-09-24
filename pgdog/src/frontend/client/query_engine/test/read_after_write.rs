use std::ops::Deref;

use pgdog_config::ReadWriteSplit;

use crate::{
    backend::databases::{databases, reload_from_existing},
    config::{Role, config, load_test_replicas, set},
    net::Parameters,
};

use super::prelude::*;

/// Replicas serve reads, never the primary, so a read that
/// reaches the primary was sent there by read-after-write.
async fn client(read_after_write_ms: u64) -> TestClient {
    load_test_replicas();

    let mut config = config().deref().clone();
    config.config.general.read_after_write_ms = read_after_write_ms;
    config.config.general.read_write_split = ReadWriteSplit::ExcludePrimary;
    set(config).unwrap();
    reload_from_existing().unwrap();

    // DDL writes too: create the table from a client of its own.
    let mut setup = TestClient::new(Parameters::default()).await.leak_pool();
    run(
        &mut setup,
        "CREATE TABLE IF NOT EXISTS test_read_after_write (id BIGINT)",
    )
    .await;

    TestClient::new(Parameters::default()).await
}

async fn run(client: &mut TestClient, query: &str) {
    client.send_simple(Query::new(query)).await;
    client.read_until('Z').await.unwrap();
}

/// Requests served by the replica so far.
fn replica_requests() -> usize {
    databases().cluster(("pgdog", "pgdog")).unwrap().shards()[0]
        .pools_with_roles()
        .into_iter()
        .filter(|(role, _)| *role == Role::Replica)
        .map(|(_, pool)| pool.state().stats.counts.server_assignment_count)
        .sum()
}

/// Run a read and tell if the replica served it.
async fn read_on_replica(client: &mut TestClient) -> bool {
    let before = replica_requests();
    run(client, "SELECT * FROM test_read_after_write").await;
    replica_requests() > before
}

#[tokio::test]
async fn test_read_after_write_keeps_reads_on_primary() {
    let mut client = client(60_000).await;

    run(&mut client, "INSERT INTO test_read_after_write VALUES (1)").await;

    assert!(!read_on_replica(&mut client).await);
    assert!(!read_on_replica(&mut client).await);
}

#[tokio::test]
async fn test_read_after_write_after_commit() {
    let mut client = client(60_000).await;

    run(&mut client, "BEGIN").await;
    run(&mut client, "INSERT INTO test_read_after_write VALUES (2)").await;
    run(&mut client, "COMMIT").await;

    assert!(!read_on_replica(&mut client).await);
}

#[tokio::test]
async fn test_read_after_write_window_expires() {
    let mut client = client(100).await;

    run(&mut client, "INSERT INTO test_read_after_write VALUES (3)").await;
    assert!(!read_on_replica(&mut client).await);

    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
    assert!(read_on_replica(&mut client).await);
}

#[tokio::test]
async fn test_read_after_write_disabled() {
    let mut client = client(0).await;

    run(&mut client, "INSERT INTO test_read_after_write VALUES (4)").await;

    assert!(read_on_replica(&mut client).await);
}

#[tokio::test]
async fn test_read_after_write_reads_alone_stay_on_replicas() {
    let mut client = client(60_000).await;

    assert!(read_on_replica(&mut client).await);
    assert!(read_on_replica(&mut client).await);
}
