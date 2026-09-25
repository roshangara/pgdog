//! A write in flight on a primary that stops answering ends when the door
//! elects another primary, not at the client's own timeout: the client gets
//! FATAL 57P01, as from a PostgreSQL that was demoted, and its retry runs on
//! the new primary (ganjban lab B-16).

use std::time::{Duration, Instant, SystemTime};

use tokio::{io::AsyncReadExt, time::timeout};

use crate::{
    backend::{
        DisconnectReason,
        databases::{databases, init},
        pool::Pool,
        replication::publisher::Lsn,
    },
    config::{ConfigAndUsers, Database, Role, User, set},
    net::{ErrorResponse, Parameters},
};

use super::prelude::*;
use super::silent_proxy::Proxy;

/// Two writers with `role = "auto"`: W1 behind the proxy, W2 the local
/// server. The test is the door's LSN check: it says who is primary.
fn load(proxy: &Proxy) {
    let mut config = ConfigAndUsers::default();
    let writer = |port: u16| Database {
        name: "pgdog".into(),
        host: "127.0.0.1".into(),
        port,
        role: Role::Auto,
        ..Default::default()
    };
    config.config.databases = vec![writer(proxy.port), writer(5432)];
    let general = &mut config.config.general;
    general.lsn_check_delay = 3_600_000;
    general.lsn_check_interval = 3_600_000;
    general.connect_timeout = 500;
    config.users.users = vec![User {
        name: "pgdog".into(),
        database: "pgdog".into(),
        password: Some("pgdog".into()),
        ..Default::default()
    }];

    set(config).unwrap();
    init().unwrap();
}

fn pool(port: u16) -> Pool {
    databases().cluster(("pgdog", "pgdog")).unwrap().shards()[0]
        .pools()
        .into_iter()
        .find(|pool| pool.addr().port == port)
        .unwrap()
}

fn primary_port() -> Option<u16> {
    databases().cluster(("pgdog", "pgdog")).unwrap().shards()[0]
        .pools_with_roles()
        .into_iter()
        .find(|(role, _)| *role == Role::Primary)
        .map(|(_, pool)| pool.addr().port)
}

/// What the door's LSN check stores for a server.
fn probe(port: u16, replica: bool, timeline: i64, lsn: i64) {
    pool(port).store_lsn_stats(
        pgdog_stats::LsnStats {
            replica,
            timeline,
            lsn: Lsn::from_i64(lsn),
            offset_bytes: lsn,
            fetched: SystemTime::now(),
            ..Default::default()
        }
        .into(),
    );
}

async fn elected(port: u16) {
    timeout(Duration::from_secs(2), async {
        while primary_port() != Some(port) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the election follows the check");
}

async fn write(client: &mut SpawnedClient) {
    client
        .send(Query::new("INSERT INTO test_primary_changed VALUES (1)"))
        .await;
    client.read_until('Z').await;
}

#[tokio::test]
async fn test_write_in_flight_on_a_lost_primary_ends_at_the_election() {
    let proxy = Proxy::start().await;
    load(&proxy);
    let w1 = proxy.port;
    let w2 = 5432;

    // W1 is the primary on timeline 1, W2 its replica.
    probe(w1, false, 1, 1_000);
    probe(w2, true, 0, 1_000);
    elected(w1).await;

    let mut client = SpawnedClient::new(Parameters::default()).await;
    client
        .send(Query::new(
            "CREATE TABLE IF NOT EXISTS test_primary_changed (id BIGINT)",
        ))
        .await;
    client.read_until('Z').await;
    write(&mut client).await;

    // W1 freezes with a write in flight: the statement reached its kernel
    // and is never answered, and nothing resets the connection.
    proxy.go_silent();
    client
        .send(Query::new("INSERT INTO test_primary_changed VALUES (2)"))
        .await;
    assert!(
        timeout(Duration::from_secs(1), client.read())
            .await
            .is_err(),
        "a frozen primary answers nothing"
    );

    // The check reports W2 promoted, on timeline 2: the door elects it,
    // and the write on W1 ends at once.
    let closed = crate::stats::connections::closed(DisconnectReason::PrimaryChanged);
    let started = Instant::now();
    probe(w2, false, 2, 2_000);

    let error = timeout(Duration::from_secs(3), client.read())
        .await
        .expect("the write in flight ends at the election");
    let took = started.elapsed();
    let error = ErrorResponse::try_from(error).expect("an error");
    assert_eq!(error.severity, "FATAL");
    assert_eq!(error.code, "57P01");
    assert!(took < Duration::from_secs(1), "took {took:?}");
    assert_eq!(primary_port(), Some(w2));

    // Its session ends, as PostgreSQL's does when it is demoted.
    let mut byte = [0u8; 1];
    let eof = timeout(Duration::from_secs(1), client.conn.read(&mut byte))
        .await
        .expect("the connection is closed");
    assert_eq!(eof.unwrap(), 0);

    // W1's connections are closed, and counted.
    timeout(Duration::from_secs(1), async {
        loop {
            let state = pool(w1).state();
            if state.idle == 0 && state.checked_out == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("no connection to the old primary is left");
    assert!(crate::stats::connections::closed(DisconnectReason::PrimaryChanged) > closed);

    // The client's retry runs on the new primary.
    let assigned = pool(w2).state().stats.counts.server_assignment_count;
    let mut retry = SpawnedClient::new(Parameters::default()).await;
    let started = Instant::now();
    timeout(Duration::from_secs(2), write(&mut retry))
        .await
        .expect("the retry is served by the new primary");
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(pool(w2).state().stats.counts.server_assignment_count > assigned);
}
