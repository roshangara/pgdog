//! Session state that transaction pooling must keep for the client
//! (ganjban lab P-5): cursors WITH HOLD and SET SESSION CHARACTERISTICS.

use crate::{
    expect_message,
    net::{CommandComplete, DataRow, Parameters, parameter::ParameterValue},
};

use super::prelude::*;

/// The first column of the last row the query returns.
async fn value(client: &mut TestClient, query: &str) -> String {
    client.send_simple(Query::new(query)).await;
    let mut value = None;
    loop {
        let message = client.read().await;
        match message.code() {
            'D' => value = expect_message!(message, DataRow).get_text(0),
            'E' => panic!("{query}: {message:?}"),
            'Z' => return value.expect("a row"),
            _ => (),
        }
    }
}

async fn run(client: &mut TestClient, query: &str) {
    client.send_simple(Query::new(query)).await;
    client.read_until('Z').await.unwrap();
}

/// pgjdbc's setTransactionIsolation: every later transaction of this client
/// runs at that level, on any server connection, and no other client's does.
#[tokio::test]
async fn test_session_characteristics_follow_the_client_only() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;
    let mut other = TestClient::new_replicas(Parameters::default()).await;

    client
        .send_simple(Query::new(
            "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
        ))
        .await;
    assert_eq!(
        expect_message!(client.read().await, CommandComplete).command(),
        "SET"
    );
    client.read_until('Z').await.unwrap();
    assert_eq!(
        client.client().params.get("default_transaction_isolation"),
        Some(&ParameterValue::String("serializable".into()))
    );

    for _ in 0..10 {
        run(&mut client, "BEGIN").await;
        assert_eq!(
            value(
                &mut client,
                "SELECT current_setting('transaction_isolation')"
            )
            .await,
            "serializable"
        );
        run(&mut client, "COMMIT").await;

        assert_eq!(
            value(&mut other, "SHOW transaction_isolation").await,
            "read committed"
        );
        run(&mut other, "BEGIN").await;
        assert_eq!(
            value(&mut other, "SHOW transaction_isolation").await,
            "read committed"
        );
        run(&mut other, "COMMIT").await;
    }

    run(
        &mut client,
        "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY, NOT DEFERRABLE",
    )
    .await;
    assert_eq!(
        value(&mut client, "SHOW default_transaction_read_only").await,
        "on"
    );
    assert_eq!(
        value(&mut client, "SHOW default_transaction_isolation").await,
        "serializable"
    );
    assert!(!client.backend_locked());
}

/// `DECLARE ... WITH HOLD` then FETCH as separate statements: the cursor
/// lives on one server connection, so the client stays there until it
/// closes it (psycopg's named cursor withhold=True, Django's .iterator()).
#[tokio::test]
async fn test_cursor_with_hold_pins_until_closed() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;
    let mut other = TestClient::new_replicas(Parameters::default()).await;

    run(
        &mut client,
        "DECLARE test_hc CURSOR WITH HOLD FOR SELECT generate_series(1, 9)",
    )
    .await;
    assert!(client.backend_locked());

    for expected in ["3", "6", "9"] {
        assert_eq!(value(&mut other, "SELECT 1").await, "1");
        assert_eq!(value(&mut client, "FETCH 3 FROM test_hc").await, expected);
    }

    run(&mut client, "CLOSE test_hc").await;
    assert!(!client.backend_locked());

    // Declared in a transaction: gone with a rollback, kept by a commit.
    run(&mut client, "BEGIN").await;
    run(
        &mut client,
        "DECLARE test_hc2 CURSOR WITH HOLD FOR SELECT 1",
    )
    .await;
    run(&mut client, "ROLLBACK").await;
    assert!(!client.backend_locked());

    run(&mut client, "BEGIN").await;
    run(
        &mut client,
        "DECLARE test_hc3 CURSOR WITH HOLD FOR SELECT 1",
    )
    .await;
    run(&mut client, "COMMIT").await;
    assert!(client.backend_locked());
    assert_eq!(value(&mut client, "FETCH 1 FROM test_hc3").await, "1");
    run(&mut client, "CLOSE ALL").await;
    assert!(!client.backend_locked());

    // DISCARD ALL closes them too.
    run(
        &mut client,
        "DECLARE test_hc4 CURSOR WITH HOLD FOR SELECT 1",
    )
    .await;
    assert!(client.backend_locked());
    run(&mut client, "DISCARD ALL").await;
    assert!(!client.backend_locked());
}

/// psycopg declares the cursor with the extended protocol.
#[tokio::test]
async fn test_cursor_with_hold_declared_extended() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;

    client
        .send(Parse::new_anonymous(
            "DECLARE \"test_held_cursor\" CURSOR WITH HOLD FOR SELECT generate_series(1, $1::int)",
        ))
        .await;
    client
        .send(Bind::new_params(
            "",
            &[Parameter {
                len: 1,
                data: "4".into(),
            }],
        ))
        .await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();
    assert!(client.backend_locked());

    assert_eq!(
        value(&mut client, "FETCH FORWARD 4 FROM \"test_held_cursor\"").await,
        "4"
    );
    run(&mut client, "CLOSE \"test_held_cursor\"").await;
    assert!(!client.backend_locked());
}
