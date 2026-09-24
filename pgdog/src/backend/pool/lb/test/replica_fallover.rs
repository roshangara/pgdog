//! Reads fall over quickly from a replica that stopped answering.

use tokio::net::TcpListener;

use super::*;

/// A server that accepts connections and never answers,
/// like a replica whose host froze.
async fn silent_server() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let mut held = vec![];
        while let Ok((socket, _)) = listener.accept().await {
            held.push(socket);
        }
    });

    port
}

fn replica(host: &str, port: u16) -> PoolConfig {
    let mut config = create_test_pool_config(host, port);
    config.config.checkout_timeout = Duration::from_millis(3_000);
    config.config.replica_checkout_timeout = Duration::from_millis(300);
    config.config.connect_timeout = Duration::from_millis(3_000);
    config.config.ban_timeout = Duration::from_secs(60);
    config
}

fn replicas(configs: &[PoolConfig]) -> LoadBalancer {
    let lb = LoadBalancer::new(
        &None,
        configs,
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::ExcludePrimary,
        Default::default(),
    );
    lb.launch();
    lb
}

fn read() -> Request {
    Request::new(Default::default(), true, false)
}

#[tokio::test]
async fn test_silent_replica_costs_replica_checkout_timeout() {
    let silent = silent_server().await;
    let lb = replicas(&[replica("127.0.0.1", silent), replica("127.0.0.1", 5432)]);

    let started = Instant::now();
    let conn = lb
        .get(&read())
        .await
        .expect("read served by the other replica");
    let waited = started.elapsed();

    assert_eq!(conn.pool.addr().port, 5432);
    assert!(waited < Duration::from_millis(1_000), "waited {waited:?}");
    assert!(lb.targets[0].ban.banned());
    drop(conn);

    lb.shutdown();
}

#[tokio::test]
async fn test_last_replica_waits_checkout_timeout() {
    let silent = silent_server().await;
    let lb = replicas(&[replica("127.0.0.1", silent)]);

    let started = Instant::now();
    let result = lb.get(&read()).await;
    let waited = started.elapsed();

    // Nowhere else to go: wait the full checkout_timeout, as before.
    assert!(result.is_err());
    assert!(waited >= Duration::from_millis(3_000), "waited {waited:?}");

    lb.shutdown();
}

#[tokio::test]
async fn test_unhealthy_replica_is_skipped() {
    let lb = replicas(&[replica("127.0.0.1", 5432), replica("localhost", 5432)]);
    lb.targets[0].health().toggle(false);

    let conn = lb.get(&read()).await.unwrap();

    assert_eq!(conn.pool.addr().host, "localhost");
    assert!(lb.targets[0].ban.banned());
    drop(conn);

    lb.shutdown();
}

#[tokio::test]
async fn test_all_unhealthy_replicas_still_serve_reads() {
    let lb = replicas(&[replica("127.0.0.1", 5432), replica("localhost", 5432)]);
    lb.targets[0].health().toggle(false);
    lb.targets[1].health().toggle(false);

    // Health flags can lag: with no healthy alternative, try them anyway.
    assert!(lb.get(&read()).await.is_ok());

    lb.shutdown();
}
