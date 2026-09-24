//! A configuration reload while a write waits for a connection, with
//! automatic roles (ganjban lab P-3).

use std::time::{Duration, Instant};

use crate::{
    backend::databases::{databases, init, reload_from_existing},
    config::{ConfigAndUsers, Database, Role, User, set},
    net::Parameters,
};

use super::prelude::*;

const CHECKOUT_TIMEOUT: u64 = 5_000;

/// The local server with `role = "auto"`: the door elects it. (Two entries
/// for one server would race each other's LSN and flip the election.)
/// Every pool holds one connection.
fn load_auto() {
    let mut config = ConfigAndUsers::default();
    let database = |host: &str| Database {
        name: "pgdog".into(),
        host: host.into(),
        port: 5432,
        role: Role::Auto,
        ..Default::default()
    };
    config.config.databases = vec![database("127.0.0.1")];
    config.config.general.lsn_check_delay = 0;
    config.config.general.lsn_check_interval = 100;
    config.config.general.checkout_timeout = CHECKOUT_TIMEOUT;
    config.config.general.default_pool_size = 1;
    config.users.users = vec![User {
        name: "pgdog".into(),
        database: "pgdog".into(),
        password: Some("pgdog".into()),
        pool_size: Some(1),
        ..Default::default()
    }];

    set(config).unwrap();
    init().unwrap();
}

async fn elected() {
    for _ in 0..100 {
        let cluster = databases().cluster(("pgdog", "pgdog")).unwrap();
        if cluster.shards()[0]
            .pools_with_roles()
            .iter()
            .any(|(role, _)| *role == Role::Primary)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("no primary elected");
}

async fn run(client: &mut TestClient, query: &str) {
    client.send_simple(Query::new(query)).await;
    client.read_until('Z').await.unwrap();
}

/// A new passthrough user, a changed password or SIGHUP rebuilds every pool
/// while a write waits for the primary's only connection. The write moves to
/// the new pools and runs as soon as the connection is free, instead of
/// failing after checkout_timeout with "checkout timeout".
#[tokio::test]
async fn test_write_waiting_through_a_reload_moves_to_the_new_pools() {
    load_auto();
    elected().await;

    let mut holder = TestClient::new(Parameters::default()).await.leak_pool();
    run(
        &mut holder,
        "CREATE TABLE IF NOT EXISTS test_reload_waiting_write (id BIGINT)",
    )
    .await;

    // The holder takes the primary's only connection.
    run(&mut holder, "BEGIN").await;
    run(
        &mut holder,
        "INSERT INTO test_reload_waiting_write VALUES (1)",
    )
    .await;

    let mut writer = TestClient::new(Parameters::default()).await.leak_pool();
    let write = tokio::spawn(async move {
        writer
            .send_simple(Query::new(
                "INSERT INTO test_reload_waiting_write VALUES (2)",
            ))
            .await;
        let result = writer.read_until('Z').await;
        (Instant::now(), result.map(|_| ()))
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    if write.is_finished() {
        let (_, result) = write.await.unwrap();
        panic!("the write did not wait for the connection: {result:?}");
    }

    // A new passthrough user's first login: every pool is rebuilt.
    reload_from_existing().unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !write.is_finished(),
        "the write still waits for the connection"
    );

    run(&mut holder, "COMMIT").await;
    let released = Instant::now();

    let (done, result) = tokio::time::timeout(Duration::from_millis(CHECKOUT_TIMEOUT), write)
        .await
        .expect("the write must not wait out checkout_timeout")
        .unwrap();
    result.expect("the write succeeds");
    assert!(
        done.duration_since(released) < Duration::from_secs(2),
        "the write ran {:?} after the connection was free",
        done.duration_since(released)
    );

    drop(holder);
    crate::backend::databases::shutdown();
}
