use super::prelude::*;

#[tokio::test]
async fn test_session_lock_tracked_outside_transaction() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new("SELECT pg_advisory_lock(101)"))
        .await;
    client.read_until('Z').await.unwrap();

    {
        let locks = client.engine.advisory_locks();
        assert!(locks.contains(101));
        assert_eq!(locks.len(), 1);
    }

    assert!(client.backend_connected());
    assert!(client.backend_locked());

    // A follow-up query must not release the pinned backend — otherwise the
    // session-scoped lock would be invisible on a different connection.
    client.send_simple(Query::new("SELECT 1")).await;
    client.read_until('Z').await.unwrap();

    assert!(client.backend_connected());
    assert!(client.backend_locked());
    assert!(client.engine.advisory_locks().contains(101));
}

#[tokio::test]
async fn test_session_lock_inside_transaction_survives_commit() {
    // A plain pg_advisory_lock taken inside a transaction lives past COMMIT
    // because it's session-scoped — we record it in `locks` right away.
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new("SELECT pg_advisory_lock(202)"))
        .await;
    client.read_until('Z').await.unwrap();

    assert!(client.engine.advisory_locks().contains(202));
    assert!(client.backend_connected());
    assert!(client.backend_locked());

    client.send_simple(Query::new("COMMIT")).await;
    client.read_until('Z').await.unwrap();

    assert!(
        client.engine.advisory_locks().contains(202),
        "session-scoped lock must survive COMMIT"
    );
    assert!(client.backend_connected());
    assert!(
        client.backend_locked(),
        "backend must stay pinned while the session lock is held"
    );
}

#[tokio::test]
async fn test_session_lock_inside_transaction_survives_rollback() {
    // Session-scoped locks aren't unwound by ROLLBACK — only xact locks are.
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new("SELECT pg_advisory_lock(303)"))
        .await;
    client.read_until('Z').await.unwrap();

    assert!(client.engine.advisory_locks().contains(303));
    assert!(client.backend_connected());
    assert!(client.backend_locked());

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();

    assert!(
        client.engine.advisory_locks().contains(303),
        "session-scoped lock must survive ROLLBACK"
    );
    assert!(client.backend_connected());
    assert!(client.backend_locked());
}

#[tokio::test]
async fn test_unlock_removes_session_lock() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new("SELECT pg_advisory_lock(404)"))
        .await;
    client.read_until('Z').await.unwrap();

    assert!(client.engine.advisory_locks().contains(404));
    assert!(client.backend_connected());
    assert!(client.backend_locked());

    client
        .send_simple(Query::new("SELECT pg_advisory_unlock(404)"))
        .await;
    client.read_until('Z').await.unwrap();

    let locks = client.engine.advisory_locks();
    assert!(!locks.contains(404));
    assert_eq!(locks.len(), 0);
    assert!(
        !client.backend_locked(),
        "backend must be released once the last session lock is dropped"
    );
}

#[tokio::test]
async fn test_unlock_all_clears_session_locks() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new("SELECT pg_advisory_lock(1)"))
        .await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new("SELECT pg_advisory_lock(2)"))
        .await;
    client.read_until('Z').await.unwrap();

    assert_eq!(client.engine.advisory_locks().len(), 2);
    assert!(client.backend_connected());
    assert!(client.backend_locked());

    client
        .send_simple(Query::new("SELECT pg_advisory_unlock_all()"))
        .await;
    client.read_until('Z').await.unwrap();

    let locks = client.engine.advisory_locks();
    assert_eq!(locks.len(), 0);
    assert!(
        !client.backend_locked(),
        "backend must be released after pg_advisory_unlock_all()"
    );
}

#[tokio::test]
async fn test_discard_all_clears_session_locks() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new("SELECT pg_advisory_lock(1)"))
        .await;
    client.read_until('Z').await.unwrap();

    assert!(client.engine.advisory_locks().contains(1));
    assert!(client.backend_locked());

    client.send_simple(Query::new("DISCARD ALL")).await;
    client.read_until('Z').await.unwrap();

    assert_eq!(client.engine.advisory_locks().len(), 0);
    assert!(
        !client.backend_locked(),
        "backend must be released after DISCARD ALL"
    );
}

#[tokio::test]
async fn test_non_all_discard_keeps_session_locks() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client
        .send_simple(Query::new("SELECT pg_advisory_lock(1)"))
        .await;
    client.read_until('Z').await.unwrap();

    for query in ["DISCARD PLANS", "DISCARD SEQUENCES", "DISCARD TEMP"] {
        client.send_simple(Query::new(query)).await;
        client.read_until('Z').await.unwrap();

        assert!(
            client.engine.advisory_locks().contains(1),
            "{query} must not release advisory locks",
        );
        assert!(client.backend_locked());
    }
}

#[tokio::test]
async fn test_xact_lock_does_not_pin_backend_and_releases_on_commit() {
    // pg_advisory_xact_lock isn't tracked.
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new("SELECT pg_advisory_xact_lock(999)"))
        .await;
    client.read_until('Z').await.unwrap();

    let locks = client.engine.advisory_locks();
    assert_eq!(locks.len(), 0);
    assert!(client.backend_connected());

    client.send_simple(Query::new("COMMIT")).await;
    client.read_until('Z').await.unwrap();

    let locks = client.engine.advisory_locks();
    assert_eq!(locks.len(), 0);
    assert!(
        !client.backend_locked(),
        "backend must be released after xact lock is dropped"
    );
}

#[tokio::test]
async fn test_xact_lock_released_on_rollback() {
    let mut client = TestClient::new_sharded(Parameters::default()).await;

    client.send_simple(Query::new("BEGIN")).await;
    client.read_until('Z').await.unwrap();

    client
        .send_simple(Query::new("SELECT pg_advisory_xact_lock(777)"))
        .await;
    client.read_until('Z').await.unwrap();

    assert_eq!(client.engine.advisory_locks().len(), 0);
    assert!(client.backend_connected());

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();

    let locks = client.engine.advisory_locks();
    assert_eq!(locks.len(), 0);
    assert!(!client.backend_locked());
}

// ganjban lab P-8: locks the door did not track.

/// The first column of the only row the query returns.
async fn scalar(client: &mut TestClient, query: &str) -> String {
    use crate::{expect_message, net::DataRow};

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

#[tokio::test]
async fn test_expression_key_pins_until_released() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;
    let mut other = TestClient::new_replicas(Parameters::default()).await;

    for (lock, unlock) in [
        (
            "SELECT pg_advisory_lock(hashtext('p8-expression'))",
            "SELECT pg_advisory_unlock(hashtext('p8-expression'))",
        ),
        (
            "SELECT pg_catalog.pg_advisory_lock(4343431)",
            "SELECT pg_catalog.pg_advisory_unlock(4343431)",
        ),
    ] {
        let try_lock = lock.replace("pg_advisory_lock", "pg_try_advisory_lock");
        let try_unlock = unlock.to_owned();

        scalar(&mut client, lock).await;
        assert!(client.backend_locked(), "{lock}");
        let pid = client.backend_pid().await;
        assert_eq!(client.backend_pid().await, pid, "{lock}: moved backends");

        // Mutual exclusion holds: another client can't take it.
        assert_eq!(scalar(&mut other, &try_lock).await, "f", "{lock}");

        assert_eq!(scalar(&mut client, unlock).await, "t", "{unlock}");
        assert!(!client.backend_locked(), "{unlock}: still pinned");

        // Released on the server, not left on a pooled connection.
        assert_eq!(scalar(&mut other, &try_lock).await, "t", "{lock}");
        assert_eq!(scalar(&mut other, &try_unlock).await, "t", "{unlock}");
    }
}

#[tokio::test]
async fn test_lock_taken_twice_needs_two_releases() {
    let mut client = TestClient::new_replicas(Parameters::default()).await;

    scalar(&mut client, "SELECT pg_advisory_lock(4401)").await;
    scalar(&mut client, "SELECT pg_advisory_lock(4401)").await;
    assert_eq!(
        scalar(&mut client, "SELECT pg_advisory_unlock(4401)").await,
        "t"
    );
    assert!(client.backend_locked(), "the server still holds one level");
    assert_eq!(
        scalar(&mut client, "SELECT pg_advisory_unlock(4401)").await,
        "t"
    );
    assert!(!client.backend_locked());
}

/// pgx's statement cache and lib/pq with arguments parse a statement in a
/// request of its own: the unlock must run on the pinned backend.
#[tokio::test]
async fn test_unlock_parsed_in_its_own_round_trip() {
    use crate::{expect_message, net::DataRow};

    let mut client = TestClient::new_replicas(Parameters::default()).await;

    scalar(&mut client, "SELECT pg_advisory_lock(4402)").await;
    assert!(client.backend_locked());

    client
        .send(Parse::named("p8_unlock", "SELECT pg_advisory_unlock(4402)"))
        .await;
    client.send(Describe::new_statement("p8_unlock")).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    client.read_until('Z').await.unwrap();
    assert!(client.backend_locked(), "parsing released nothing");

    client.send(Bind::new_statement("p8_unlock")).await;
    client.send(Execute::new()).await;
    client.send(Sync).await;
    client.try_process().await.unwrap();
    let messages = client.read_until('Z').await.unwrap();
    let row = messages
        .into_iter()
        .find(|message| message.code() == 'D')
        .expect("a row");
    assert_eq!(
        expect_message!(row, DataRow).get_text(0).as_deref(),
        Some("t")
    );
    assert!(!client.backend_locked());
}
