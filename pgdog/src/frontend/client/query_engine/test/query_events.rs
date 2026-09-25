//! Every statement a client sends is one line of `query_events`.

use std::ops::Deref;
use std::path::Path;
use std::time::Duration;

use pgdog_config::ReadWriteSplit;
use serde_json::Value;

use crate::{
    backend::databases::reload_from_existing,
    config::{config, load_test_replicas, set},
    net::Parameters,
};

use super::prelude::*;

async fn client(path: &Path) -> TestClient {
    load_test_replicas();

    let mut config = config().deref().clone();
    config.config.general.read_write_split = ReadWriteSplit::ExcludePrimary;
    config.config.general.query_events = Some(path.to_owned());
    set(config).unwrap();
    reload_from_existing().unwrap();

    let mut params = Parameters::default();
    params.insert("application_name", "events_test");
    TestClient::new(params).await
}

async fn run(client: &mut TestClient, query: &str) {
    client.send_simple(Query::new(query)).await;
    // An ErrorResponse comes before the ReadyForQuery.
    if client.read_until('Z').await.is_err() {
        client.read_until('Z').await.unwrap();
    }
}

/// The events written so far, once there are `n` of them.
async fn events(path: &Path, n: usize) -> Vec<Value> {
    for _ in 0..200 {
        if let Ok(raw) = std::fs::read_to_string(path) {
            let events: Vec<Value> = raw
                .lines()
                .map(|line| serde_json::from_str(line).expect("one JSON object a line"))
                .collect();
            if events.len() >= n {
                return events;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("fewer than {n} events in {}", path.display());
}

#[tokio::test]
async fn test_every_statement_is_an_event() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.ndjson");
    let mut client = client(&path).await;

    // A read, a transaction that writes, an error, and a prepared read.
    run(&mut client, "SELECT 1").await;
    run(
        &mut client,
        "CREATE TABLE IF NOT EXISTS test_query_events (id BIGINT)",
    )
    .await;
    run(&mut client, "BEGIN").await;
    run(&mut client, "INSERT INTO test_query_events VALUES (1), (2)").await;
    run(&mut client, "COMMIT").await;
    run(&mut client, "SELECT 1/0").await;

    client
        .send(Parse::named("events_stmt", "SELECT $1::bigint AS id"))
        .await;
    client
        .send(Bind::new_params(
            "events_stmt",
            &[Parameter {
                len: 2,
                data: "42".into(),
            }],
        ))
        .await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    let events = events(&path, 7).await;
    assert_eq!(events.len(), 7, "{events:#?}");

    let select = &events[0];
    assert_eq!(select["command"], "SELECT");
    assert_eq!(select["protocol"], "simple");
    assert_eq!(select["route"], "replica");
    assert_eq!(select["route_reason"], "read");
    assert_eq!(select["server"], "127.0.0.1:5432");
    assert_eq!(select["rows"], 1);
    assert_eq!(select["outcome"], "ok");
    assert_eq!(select["database"], "pgdog");
    assert_eq!(select["user"], "pgdog");
    assert_eq!(select["application"], "events_test");
    assert_eq!(select["in_transaction"], false);
    assert!(select["bytes_in"].as_u64().unwrap() > 0);
    assert!(select["bytes_out"].as_u64().unwrap() > 0);
    assert!(select["duration_ms"].as_f64().unwrap() > 0.0);
    assert!(select["server_ms"].as_f64().unwrap() > 0.0);
    assert!(select["fingerprint"].as_str().unwrap().len() == 16);

    // BEGIN is answered by PgDog; the INSERT is the transaction's first
    // statement on a server; the three share the transaction's number.
    let (begin, insert, commit) = (&events[2], &events[3], &events[4]);
    assert_eq!(begin["command"], "BEGIN");
    assert_eq!(begin["route"], "door");
    assert_eq!(insert["command"], "INSERT");
    assert_eq!(insert["route"], "primary");
    assert_eq!(insert["rows"], 2);
    assert_eq!(insert["in_transaction"], true);
    assert_eq!(commit["command"], "COMMIT");
    assert_eq!(begin["xact_seq"], insert["xact_seq"]);
    assert_eq!(insert["xact_seq"], commit["xact_seq"]);
    assert_ne!(events[1]["xact_seq"], begin["xact_seq"]);
    let seqs: Vec<u64> = events
        .iter()
        .map(|e| e["stmt_seq"].as_u64().unwrap())
        .collect();
    assert_eq!(seqs, (1..=7).collect::<Vec<_>>());

    let error = &events[5];
    assert_eq!(error["outcome"], "error");
    assert_eq!(error["sqlstate"], "22012");
    assert_eq!(error["error_severity"], "ERROR");
    assert!(
        error["error_message"]
            .as_str()
            .unwrap()
            .contains("division by zero")
    );

    let prepared = &events[6];
    assert_eq!(prepared["protocol"], "extended");
    assert_eq!(prepared["prepared"], true);
    assert_eq!(prepared["params"], 1);
    assert_eq!(prepared["rows"], 1);
    assert_eq!(prepared["route"], "replica");
    assert_eq!(prepared["outcome"], "ok");

    // One client, one id; every event its own.
    assert!(events.iter().all(|e| e["client_id"] == select["client_id"]));
    let mut ids: Vec<&str> = events
        .iter()
        .map(|e| e["event_id"].as_str().unwrap())
        .collect();
    ids.dedup();
    assert_eq!(ids.len(), 7);
}

#[tokio::test]
async fn test_a_statement_cut_by_the_query_timeout_is_an_event() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.ndjson");
    let mut client = client(&path).await;

    let mut config = config().deref().clone();
    config.config.general.query_timeout = 100;
    set(config).unwrap();

    client.send(Query::new("SELECT pg_sleep(1)")).await;
    assert!(client.try_process().await.is_err());

    let events = events(&path, 1).await;
    assert_eq!(events[0]["outcome"], "timeout");
    assert_eq!(events[0]["command"], "SELECT");
    assert!(events[0]["duration_ms"].as_f64().unwrap() >= 100.0);
}

#[tokio::test]
async fn test_a_copy_is_one_event_with_its_data() {
    use crate::net::{CopyData, CopyDone};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.ndjson");
    let mut client = client(&path).await;
    run(
        &mut client,
        "CREATE TABLE IF NOT EXISTS test_query_events_copy (id BIGINT)",
    )
    .await;

    let query = "COPY test_query_events_copy FROM STDIN";
    client.send(Query::new(query)).await;
    client.try_process().await.unwrap();
    client.read_until('G').await.unwrap();
    let data = b"10\n11\n12\n";
    client.send(CopyData::new(data)).await;
    client.send(CopyDone).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();

    let events = events(&path, 2).await;
    assert_eq!(events.len(), 2, "{events:#?}");
    let copy = &events[1];
    assert_eq!(copy["command"], "COPY");
    assert_eq!(copy["route"], "primary");
    assert_eq!(copy["rows"], 3);
    assert_eq!(copy["outcome"], "ok");
    assert!(copy["bytes_in"].as_u64().unwrap() as usize >= query.len() + data.len());
}
