//! A statement is only ever replaced on another server connection before any
//! of it was sent. Once sent, a failure reaches the client: never run twice.

use crate::{
    expect_message,
    net::{DataRow, Parameters, RowDescription},
};

use super::prelude::*;

async fn value(client: &mut TestClient, query: &str) -> i64 {
    client.send_simple(Query::new(query)).await;
    expect_message!(client.read().await, RowDescription);
    let row = expect_message!(client.read().await, DataRow);
    let value = row.get_text(0).expect("a value").parse().expect("a number");
    client.read_until('Z').await.unwrap();
    value
}

#[tokio::test]
async fn test_statement_that_ran_is_not_retried() {
    let mut setup = TestClient::new_replicas(Parameters::default())
        .await
        .leak_pool();
    setup
        .send_simple(Query::new(
            "CREATE SEQUENCE IF NOT EXISTS test_no_retry_after_send",
        ))
        .await;
    setup.read_until('Z').await.unwrap();
    let before = value(&mut setup, "SELECT nextval('test_no_retry_after_send')").await;

    // The statement runs (nextval is not rolled back), then its server
    // connection dies with 57P01, like a server shutting down mid-query.
    let mut client = TestClient::new_replicas(Parameters::default())
        .await
        .leak_pool();
    client
        .send(Query::new(
            "SELECT nextval('test_no_retry_after_send'), pg_terminate_backend(pg_backend_pid())",
        ))
        .await;
    let processed = client.try_process().await;
    // The failure reaches the client, as an error message or as the
    // client's session ending with the engine error.
    let error_sent =
        tokio::time::timeout(std::time::Duration::from_secs(2), client.read_until('Z'))
            .await
            .map(|result| result.is_err())
            .unwrap_or(false);
    assert!(
        processed.is_err() || error_sent,
        "the client must see the failure"
    );

    let after = value(&mut setup, "SELECT nextval('test_no_retry_after_send')").await;
    assert_eq!(after, before + 2, "the statement ran exactly once");
}
