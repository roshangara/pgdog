//! A database's own read_write_split (ganjban lab P-4): every read of one
//! database on the primary, the others' on replicas.

use pgdog_config::ReadWriteSplit;

use crate::{
    backend::databases::{databases, init},
    config::{ConfigAndUsers, Database, Role, User, set},
    net::Parameters,
};

use super::prelude::*;

/// `pgdog` reads from the primary; `shard_0` from the replica.
fn load() {
    let mut config = ConfigAndUsers::default();
    let entries = |name: &str, split: Option<ReadWriteSplit>| {
        vec![
            Database {
                name: name.into(),
                host: "127.0.0.1".into(),
                port: 5432,
                role: Role::Primary,
                read_write_split: split,
                ..Default::default()
            },
            Database {
                name: name.into(),
                host: "127.0.0.1".into(),
                port: 5432,
                role: Role::Replica,
                read_only: Some(true),
                read_write_split: split,
                ..Default::default()
            },
        ]
    };
    config.config.databases = entries("pgdog", Some(ReadWriteSplit::PreferPrimary));
    config.config.databases.extend(entries("shard_0", None));
    config.config.general.read_write_split = ReadWriteSplit::ExcludePrimary;
    config.users.users = ["pgdog", "shard_0"]
        .into_iter()
        .map(|database| User {
            name: "pgdog".into(),
            database: database.into(),
            password: Some("pgdog".into()),
            ..Default::default()
        })
        .collect();

    set(config).unwrap();
    init().unwrap();
}

fn replica_requests(database: &str) -> usize {
    databases().cluster(("pgdog", database)).unwrap().shards()[0]
        .pools_with_roles()
        .into_iter()
        .filter(|(role, _)| *role == Role::Replica)
        .map(|(_, pool)| pool.state().stats.counts.server_assignment_count)
        .sum()
}

async fn read(client: &mut TestClient) {
    client
        .send_simple(Query::new("SELECT count(*) FROM pg_class"))
        .await;
    client.read_until('Z').await.unwrap();
}

#[tokio::test]
async fn test_one_database_reads_from_the_primary() {
    load();

    let params = |database: &str| {
        let mut params = Parameters::default();
        params.insert("user", "pgdog");
        params.insert("database", database);
        params
    };
    let mut on_primary = TestClient::new(params("pgdog")).await.leak_pool();
    let mut on_replica = TestClient::new(params("shard_0")).await.leak_pool();

    let (before_primary, before_replica) = (replica_requests("pgdog"), replica_requests("shard_0"));
    for _ in 0..5 {
        read(&mut on_primary).await;
        read(&mut on_replica).await;
    }

    assert_eq!(
        replica_requests("pgdog"),
        before_primary,
        "pgdog read a replica"
    );
    assert_eq!(replica_requests("shard_0"), before_replica + 5);

    // A query can still ask for a replica.
    on_primary
        .send_simple(Query::new(
            "/* pgdog_role: replica */ SELECT count(*) FROM pg_class",
        ))
        .await;
    on_primary.read_until('Z').await.unwrap();
    assert_eq!(replica_requests("pgdog"), before_primary + 1);

    crate::backend::databases::shutdown();
}
