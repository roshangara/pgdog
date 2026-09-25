//! A replica that stops answering is taken out of reads within its LSN
//! check, and the reads waiting on it run elsewhere (ganjban lab H-3).

use std::time::{Duration, Instant};

use crate::{
    backend::databases::{databases, init},
    config::{ConfigAndUsers, Database, Role, User, set},
    expect_message,
    net::{DataRow, Parameters},
};
use pgdog_config::ReadWriteSplit;

use super::prelude::*;
use super::silent_proxy::Proxy;

/// The primary is the local server; the only replica is behind the proxy.
fn load(proxy: &Proxy) {
    let mut config = ConfigAndUsers::default();
    config.config.databases = vec![
        Database {
            name: "pgdog".into(),
            host: "127.0.0.1".into(),
            port: 5432,
            role: Role::Primary,
            ..Default::default()
        },
        Database {
            name: "pgdog".into(),
            host: "127.0.0.1".into(),
            port: proxy.port,
            role: Role::Replica,
            read_only: Some(true),
            ..Default::default()
        },
    ];
    let general = &mut config.config.general;
    general.read_write_split = ReadWriteSplit::ExcludePrimary;
    general.replica_down_detection = true;
    general.lsn_check_delay = 0;
    general.lsn_check_interval = 100;
    general.lsn_check_timeout = 300;
    general.connect_timeout = 500;
    general.ban_timeout = 10_000;
    config.users.users = vec![User {
        name: "pgdog".into(),
        database: "pgdog".into(),
        password: Some("pgdog".into()),
        ..Default::default()
    }];

    set(config).unwrap();
    init().unwrap();
}

async fn read(client: &mut TestClient) -> String {
    client.send_simple(Query::new("SELECT 4545")).await;
    let messages = client.read_until('Z').await.unwrap();
    let row = messages
        .into_iter()
        .find(|m| m.code() == 'D')
        .expect("a row");
    expect_message!(row, DataRow).get_text(0).unwrap()
}

fn replica_reads() -> usize {
    databases().cluster(("pgdog", "pgdog")).unwrap().shards()[0]
        .pools_with_roles()
        .into_iter()
        .find(|(role, _)| *role == Role::Replica)
        .map(|(_, pool)| pool.state().stats.counts.server_assignment_count)
        .unwrap()
}

fn replica_healthy() -> bool {
    databases().cluster(("pgdog", "pgdog")).unwrap().shards()[0]
        .pools_with_roles()
        .into_iter()
        .find(|(role, _)| *role == Role::Replica)
        .map(|(_, pool)| pool.healthy())
        .unwrap()
}

#[tokio::test]
async fn test_silent_replica_is_taken_out_of_reads_at_its_check() {
    let proxy = Proxy::start().await;
    load(&proxy);

    let mut client = TestClient::new(Parameters::default()).await;
    // The replica serves reads.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let before = replica_reads();
    for _ in 0..5 {
        assert_eq!(read(&mut client).await, "4545");
    }
    assert!(replica_reads() >= before + 5);
    assert!(replica_healthy());

    proxy.go_silent();

    // The read goes to the silent replica and would wait for it forever;
    // the replica's LSN check doesn't answer in 300 ms, the replica is down,
    // and the read runs again on the primary.
    let started = Instant::now();
    let answer = tokio::time::timeout(Duration::from_secs(3), read(&mut client))
        .await
        .expect("the read must not wait for the silent replica");
    assert_eq!(answer, "4545");
    let took = started.elapsed();
    assert!(took < Duration::from_millis(1_500), "took {took:?}");
    assert!(
        took > Duration::from_millis(50),
        "the read never waited for the replica: {took:?}"
    );
    assert!(!replica_healthy());

    // Reads go on, from the primary.
    let started = Instant::now();
    assert_eq!(read(&mut client).await, "4545");
    assert!(started.elapsed() < Duration::from_millis(200));
}
