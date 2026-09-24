//! A read on a replica whose connection breaks before any of its answer
//! reached the client runs again on another server (ganjban lab H-4).

use std::{ops::Deref, time::Duration};

use pgdog_config::ReadWriteSplit;

use crate::{
    backend::databases::reload_from_existing,
    config::{config, load_test_replicas, set},
    expect_message,
    net::{DataRow, Parameters},
};

use super::prelude::*;

/// Reads go to the (read-only) replica only.
fn replicas() {
    load_test_replicas();
    let mut config = config().deref().clone();
    config.config.general.read_write_split = ReadWriteSplit::ExcludePrimary;
    set(config).unwrap();
    reload_from_existing().unwrap();
}

/// Terminate the backend running `query` (the replica dying under a read).
async fn terminate(killer: &mut TestClient, query: &str) {
    for _ in 0..50 {
        killer
            .send_simple(Query::new(format!(
                "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity \
                 WHERE query = '{query}' AND pid <> pg_backend_pid()"
            )))
            .await;
        let messages = killer.read_until('Z').await.unwrap();
        let killed = messages
            .into_iter()
            .find(|m| m.code() == 'D')
            .and_then(|m| expect_message!(m, DataRow).get_text(0));
        if killed.as_deref() == Some("1") {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{query} never ran");
}

#[tokio::test]
async fn test_read_runs_again_when_its_replica_fails() {
    replicas();
    let mut killer = TestClient::new(Parameters::default()).await.leak_pool();
    let mut reader = TestClient::new(Parameters::default()).await.leak_pool();

    let query = "SELECT pg_sleep(0.5), 4242";
    let read = tokio::spawn(async move {
        reader.send_simple(Query::new(query)).await;
        reader.read_until('Z').await
    });

    terminate(&mut killer, query).await;

    let messages = read
        .await
        .unwrap()
        .expect("the read must succeed on another server");
    let row = messages
        .into_iter()
        .find(|m| m.code() == 'D')
        .expect("a row");
    assert_eq!(
        expect_message!(row, DataRow).get_text(1).as_deref(),
        Some("4242")
    );
}

#[tokio::test]
async fn test_read_in_a_transaction_fails_as_before() {
    replicas();
    let mut killer = TestClient::new(Parameters::default()).await.leak_pool();
    let mut reader = TestClient::new(Parameters::default()).await.leak_pool();

    reader.send_simple(Query::new("BEGIN READ ONLY")).await;
    reader.read_until('Z').await.unwrap();

    let query = "SELECT pg_sleep(0.5), 4343";
    let read = tokio::spawn(async move {
        let sent = reader.try_send_simple(Query::new(query)).await;
        (sent.is_err(), reader)
    });

    terminate(&mut killer, query).await;

    let (failed, mut reader) = read.await.unwrap();
    // The client sees the failure: as an engine error, or the server's FATAL.
    let error_sent = failed
        || tokio::time::timeout(Duration::from_secs(2), reader.read_until('Z'))
            .await
            .map(|result| result.is_err())
            .unwrap_or(true);
    assert!(error_sent, "a read inside a transaction is not run again");
}

/// The extended protocol: the client already has ParseComplete,
/// BindComplete and RowDescription from the failed replica; the other
/// server's copies of them are not sent again.
#[tokio::test]
async fn test_extended_read_runs_again_without_repeating_its_header() {
    replicas();
    let mut killer = TestClient::new(Parameters::default()).await.leak_pool();
    let mut reader = TestClient::new(Parameters::default()).await.leak_pool();

    let query = "SELECT pg_sleep(0.5), 4444";
    let read = tokio::spawn(async move {
        reader.send(Parse::named("h4_extended", query)).await;
        reader.send(Bind::new_statement("h4_extended")).await;
        reader.send(Describe::new_portal("")).await;
        reader.send(Execute::new()).await;
        reader.send(Sync).await;
        reader.try_process().await.unwrap();
        reader.read_until('Z').await
    });

    terminate(&mut killer, query).await;

    let messages = read
        .await
        .unwrap()
        .expect("the read must succeed on another server");
    let codes = messages.iter().map(|m| m.code()).collect::<String>();
    assert_eq!(codes, "12TDCZ");
    let row = messages
        .into_iter()
        .find(|m| m.code() == 'D')
        .expect("a row");
    assert_eq!(
        expect_message!(row, DataRow).get_text(1).as_deref(),
        Some("4444")
    );
}
