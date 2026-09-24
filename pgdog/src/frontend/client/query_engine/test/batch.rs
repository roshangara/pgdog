//! Several statements in one simple query on a database with one shard:
//! sent whole, to the primary if any statement writes, their session state
//! recorded (ganjban lab P-6, P-7).

use std::ops::Deref;

use itertools::Itertools;
use pgdog_config::ReadWriteSplit;

use crate::{
    backend::databases::{databases, reload_from_existing},
    config::{Role, config, load_test_replicas, set},
    expect_message,
    net::{
        CommandComplete, DataRow, ErrorResponse, Parameters, ReadyForQuery,
        parameter::ParameterValue,
    },
};

use super::prelude::*;

/// A primary and a read-only replica on the local server; reads go to
/// the replica only.
async fn client() -> TestClient {
    load_test_replicas();

    let mut config = config().deref().clone();
    config.config.general.read_write_split = ReadWriteSplit::ExcludePrimary;
    set(config).unwrap();
    reload_from_existing().unwrap();

    let mut setup = TestClient::new(Parameters::default()).await.leak_pool();
    setup
        .send_simple(Query::new(
            "CREATE TABLE IF NOT EXISTS test_batch_kv (k BIGINT, v TEXT)",
        ))
        .await;
    setup.read_until('Z').await.unwrap();

    TestClient::new(Parameters::default()).await
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

/// Message codes of the whole response to a simple query, notices left out.
async fn codes(client: &mut TestClient, query: &str) -> Vec<char> {
    client.send_simple(Query::new(query)).await;
    let mut codes = vec![];
    loop {
        let message = client.read().await;
        if message.code() != 'N' {
            codes.push(message.code());
        }
        if message.code() == 'Z' {
            return codes;
        }
    }
}

/// The first column of the last row the query returns.
async fn value(client: &mut TestClient, query: &str) -> String {
    client.send_simple(Query::new(query)).await;
    let mut value = None;
    loop {
        let message = client.read().await;
        match message.code() {
            'D' => {
                let row = expect_message!(message, DataRow);
                value = row.get_text(0);
            }
            'E' => panic!("{query}: {:?}", ErrorResponse::try_from(message).unwrap()),
            'Z' => return value.expect("a row"),
            _ => (),
        }
    }
}

#[tokio::test]
async fn test_reset_all_among_other_statements_does_not_panic() {
    let mut client = client().await;

    assert_eq!(
        codes(&mut client, "SET application_name = 'x'; RESET ALL").await,
        ['C', 'C', 'Z']
    );
    assert_eq!(
        codes(&mut client, "RESET ALL; SELECT 1").await,
        ['C', 'T', 'D', 'C', 'Z']
    );
    assert_eq!(value(&mut client, "SELECT 1").await, "1");
}

/// Npgsql resets a pooled connection that has prepared statements with this
/// query, and skips exactly as many response messages as PostgreSQL sends.
#[tokio::test]
async fn test_npgsql_reset_answers_like_postgres() {
    let mut client = client().await;

    client
        .send_simple(Query::new("SET work_mem TO '9MB'"))
        .await;
    client.read_until('Z').await.unwrap();

    let reset = "SET SESSION AUTHORIZATION DEFAULT;RESET ALL;CLOSE ALL;UNLISTEN *;\
                 SELECT pg_advisory_unlock_all();DISCARD SEQUENCES;DISCARD TEMP";
    client.send_simple(Query::new(reset)).await;
    let mut commands = vec![];
    let mut codes = vec![];
    loop {
        let message = client.read().await;
        codes.push(message.code());
        if message.code() == 'C' {
            commands.push(
                expect_message!(message.clone(), CommandComplete)
                    .command()
                    .to_owned(),
            );
        }
        if message.code() == 'Z' {
            assert_eq!(expect_message!(message, ReadyForQuery).status, 'I');
            break;
        }
    }

    assert_eq!(
        codes,
        ['C', 'C', 'C', 'C', 'T', 'D', 'C', 'C', 'C', 'Z'],
        "{commands:?}"
    );
    assert_eq!(
        commands,
        [
            "SET",
            "RESET",
            "CLOSE CURSOR ALL",
            "UNLISTEN",
            "SELECT 1",
            "DISCARD SEQUENCES",
            "DISCARD TEMP"
        ]
    );

    // RESET ALL reset the client's work_mem, wherever it runs next.
    assert!(client.client().params.get("work_mem").is_none());
    assert_eq!(value(&mut client, "SHOW work_mem").await, "4MB");
    assert!(!client.backend_locked());
}

#[tokio::test]
async fn test_select_then_write_goes_to_the_primary() {
    let mut client = client().await;

    for query in [
        "SELECT 1; INSERT INTO test_batch_kv VALUES (1, 'a')",
        "SELECT count(*) FROM test_batch_kv; UPDATE test_batch_kv SET v = 'b' WHERE k = 1",
        "SELECT 1; SELECT * INTO TEMP test_batch_into FROM test_batch_kv; DROP TABLE test_batch_into",
    ] {
        let before = replica_requests();
        let codes = codes(&mut client, query).await;
        assert!(!codes.contains(&'E'), "{query}: {codes:?}");
        assert_eq!(replica_requests(), before, "{query} ran on the replica");
    }
}

#[tokio::test]
async fn test_reads_alone_stay_on_the_replica() {
    let mut client = client().await;

    let before = replica_requests();
    assert_eq!(
        value(
            &mut client,
            "SELECT 1; SELECT count(*) FROM test_batch_kv; SHOW transaction_read_only"
        )
        .await,
        "on"
    );
    assert_eq!(replica_requests(), before + 1);
}

#[tokio::test]
async fn test_set_with_several_statements_is_not_refused() {
    let mut client = client().await;

    // A migration file that sets a timeout first.
    let codes = codes(
        &mut client,
        "SET lock_timeout = '5s'; CREATE TABLE IF NOT EXISTS test_batch_migration (id int); \
         INSERT INTO test_batch_migration VALUES (1)",
    )
    .await;
    assert_eq!(codes, ['C', 'C', 'C', 'Z']);

    // The SET applied to the statements after it, and to the client's
    // next statements on any server connection.
    assert_eq!(
        client.client().params.get("lock_timeout"),
        Some(&ParameterValue::String("5s".into()))
    );
    assert_eq!(value(&mut client, "SHOW lock_timeout").await, "5s");
    assert_eq!(
        value(&mut client, "SET lock_timeout = '7s'; SHOW lock_timeout").await,
        "7s"
    );
    assert_eq!(value(&mut client, "SHOW lock_timeout").await, "7s");
}

#[tokio::test]
async fn test_failed_query_records_nothing() {
    let mut client = client().await;

    client
        .send_simple(Query::new("SET work_mem = '9MB'; SELECT 1/0"))
        .await;
    let error = client.read_until('Z').await.unwrap_err();
    assert_eq!(error.code, "22012");
    client.read_until('Z').await.unwrap();

    assert!(client.client().params.get("work_mem").is_none());
    assert_eq!(value(&mut client, "SHOW work_mem").await, "4MB");
}

#[tokio::test]
async fn test_rolled_back_set_is_not_recorded() {
    let mut client = client().await;

    let codes = codes(
        &mut client,
        "BEGIN; SET work_mem = '9MB'; ROLLBACK; BEGIN; SET statement_timeout = '8s'; COMMIT",
    )
    .await;
    assert_eq!(codes.iter().filter(|c| **c == 'E').count(), 0);

    let params = &client.client().params;
    assert!(params.get("work_mem").is_none());
    assert_eq!(
        params.get("statement_timeout"),
        Some(&ParameterValue::String("8s".into()))
    );
    assert!(!client.backend_locked());
}

#[tokio::test]
async fn test_set_in_a_transaction_left_open_commits_with_it() {
    let mut client = client().await;

    let first = codes(&mut client, "BEGIN; SET work_mem = '9MB'").await;
    assert_eq!(first, ['C', 'C', 'Z']);
    assert!(client.backend_connected());

    client.send_simple(Query::new("ROLLBACK")).await;
    client.read_until('Z').await.unwrap();
    assert!(client.client().params.get("work_mem").is_none());

    codes(&mut client, "BEGIN; SET work_mem = '9MB'").await;
    client.send_simple(Query::new("COMMIT")).await;
    client.read_until('Z').await.unwrap();
    assert_eq!(
        client.client().params.get("work_mem"),
        Some(&ParameterValue::String("9MB".into()))
    );
}

#[tokio::test]
async fn test_temp_table_in_a_batch_pins() {
    let mut client = client().await;

    let codes = codes(
        &mut client,
        "CREATE TEMP TABLE test_batch_temp (id int); INSERT INTO test_batch_temp VALUES (1)",
    )
    .await;
    assert_eq!(codes, ['C', 'C', 'Z']);
    assert!(client.backend_locked());
    assert_eq!(
        value(&mut client, "SELECT count(*) FROM test_batch_temp").await,
        "1"
    );

    client
        .send_simple(Query::new("DROP TABLE test_batch_temp"))
        .await;
    client.read_until('Z').await.unwrap();
    assert!(!client.backend_locked());
}

#[tokio::test]
async fn test_listen_in_a_batch_is_refused() {
    let mut client = client().await;

    client
        .send_simple(Query::new("LISTEN test_batch_channel; SELECT 1"))
        .await;
    let error = client.read_until('Z').await.unwrap_err();
    assert!(error.message.contains("LISTEN"), "{error:?}");
    client.read_until('Z').await.unwrap();
    assert_eq!(value(&mut client, "SELECT 1").await, "1");
}

#[tokio::test]
async fn test_advisory_lock_and_unlock_in_one_batch() {
    let mut client = client().await;

    let codes = codes(
        &mut client,
        "SELECT pg_advisory_lock(1234567); SELECT pg_advisory_unlock(1234567)",
    )
    .await;
    assert_eq!(codes.iter().filter(|c| **c == 'E').count(), 0);
    assert!(!client.backend_locked());

    codes_ok(
        &mut client,
        "SELECT pg_advisory_unlock_all(); SELECT pg_advisory_lock(1234568)",
    )
    .await;
    assert!(client.backend_locked());
    codes_ok(&mut client, "SELECT pg_advisory_unlock(1234568)").await;
    assert!(!client.backend_locked());
}

async fn codes_ok(client: &mut TestClient, query: &str) {
    let codes = codes(client, query).await;
    assert!(!codes.contains(&'E'), "{query}: {}", codes.iter().join(""));
}
