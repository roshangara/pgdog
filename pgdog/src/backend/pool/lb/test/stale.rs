//! Stale replicas when the primary can't be asked (ganjban lab H-5): a
//! stale replica leaves reads only while a fresh source answers; with none,
//! the freshest replica serves them and the door says how stale they are.

use super::*;

const BOUND: Duration = Duration::from_secs(30);

fn threshold() -> ReplicaLag {
    ReplicaLag {
        duration: BOUND,
        bytes: i64::MAX,
    }
}

/// A primary and two replicas, weights favouring the second replica.
fn lb() -> LoadBalancer {
    let primary = {
        let mut config = create_test_pool_config("127.0.0.1", 5432);
        config.address.configured_role = Role::Primary;
        Pool::new(&config)
    };
    let mut first = create_test_pool_config("127.0.0.1", 5432);
    first.config.lb_weight = 0;
    let mut second = create_test_pool_config("localhost", 5432);
    second.config.lb_weight = 255;

    LoadBalancer::new(
        &Some(primary),
        &[first, second],
        LoadBalancingStrategy::WeightedRoundRobin,
        ReadWriteSplit::ExcludePrimary,
        Default::default(),
    )
}

fn lag(target: &Target, seconds: u64) {
    target.pool.lock().replica_lag = ReplicaLag {
        duration: Duration::from_secs(seconds),
        bytes: 0,
    };
}

fn primary(lb: &LoadBalancer) -> &Target {
    lb.targets
        .iter()
        .find(|t| t.role() == Role::Primary)
        .unwrap()
}

#[test]
fn test_stale_replica_leaves_reads_while_a_fresh_one_answers() {
    let lb = lb();
    lag(&lb.targets[0], 45);
    lag(&lb.targets[1], 1);

    Monitor::new_test(&lb).ban_check(&threshold());

    assert!(lb.targets[0].ban.banned());
    assert!(!lb.targets[1].ban.banned());
    assert_eq!(lb.stale_ms.load(Ordering::Relaxed), 0);
}

#[test]
fn test_all_replicas_stale_primary_answers_reads_go_to_the_primary() {
    let lb = lb();
    lag(&lb.targets[0], 45);
    lag(&lb.targets[1], 60);

    Monitor::new_test(&lb).ban_check(&threshold());

    assert!(lb.targets[0].ban.banned());
    assert!(lb.targets[1].ban.banned());
    assert!(!primary(&lb).ban.banned());
    assert_eq!(lb.stale_ms.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn test_no_fresh_source_the_freshest_replica_serves_reads() {
    let lb = lb();
    // The pools, not the load balancer's own monitor: the test runs the
    // monitor's check itself, with its threshold.
    lb.targets.iter().for_each(|target| target.pool.launch());
    lag(&lb.targets[0], 45);
    lag(&lb.targets[1], 60);
    // The primary is cut off (N02: damavand <-> zagros).
    primary(&lb).health().toggle(false);

    Monitor::new_test(&lb).ban_check(&threshold());

    assert!(!lb.targets[0].ban.banned(), "reads never fail");
    assert!(!lb.targets[1].ban.banned(), "reads never fail");
    assert_eq!(lb.stale_ms.load(Ordering::Relaxed), 45_000);

    // The weights favour the second replica; the first is fresher.
    for _ in 0..5 {
        let conn = lb.get(&Request::default()).await.unwrap();
        assert_eq!(conn.pool.addr().host, "127.0.0.1");
    }

    // Fresh again: the weights decide.
    lag(&lb.targets[0], 0);
    lag(&lb.targets[1], 0);
    primary(&lb).health().toggle(true);
    Monitor::new_test(&lb).ban_check(&threshold());
    assert_eq!(lb.stale_ms.load(Ordering::Relaxed), 0);
    let conn = lb.get(&Request::default()).await.unwrap();
    assert_eq!(conn.pool.addr().host, "localhost");
    drop(conn);

    lb.shutdown();
}

/// Every replica is stale and the primary answers, so reads go to the
/// primary; then the primary stops answering (it froze, or died while reads
/// waited for it).
async fn primary_serving_reads(stale_replica_healthy: bool) -> (LoadBalancer, u16) {
    let silent = super::replica_fallover::silent_server().await;
    let primary = {
        let mut config = create_test_pool_config("127.0.0.1", silent);
        config.address.configured_role = Role::Primary;
        config.config.checkout_timeout = Duration::from_millis(3_000);
        config.config.replica_checkout_timeout = Duration::from_millis(300);
        config.config.connect_timeout = Duration::from_millis(3_000);
        config.config.ban_timeout = Duration::from_secs(60);
        Pool::new(&config)
    };
    let mut replica = create_test_pool_config("127.0.0.1", 5432);
    replica.config.ban_timeout = Duration::from_secs(60);
    let lb = LoadBalancer::new(
        &Some(primary),
        &[replica],
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::ExcludePrimary,
        Default::default(),
    );
    lb.targets.iter().for_each(|target| target.pool.launch());

    // The monitor took the replica out of reads for its lag while the
    // primary answered (or for being down).
    let replica = &lb.targets[0];
    assert_eq!(replica.role(), Role::Replica);
    if stale_replica_healthy {
        replica.ban.ban(Error::ReplicaLag, Duration::from_secs(60));
    } else {
        replica.health().toggle(false);
        replica
            .ban
            .ban(Error::PoolUnhealthy, Duration::from_secs(60));
    }
    (lb, silent)
}

fn read() -> Request {
    Request::new(Default::default(), true, false)
}

/// A read on the primary gives up on it after replica_checkout_timeout, not
/// checkout_timeout (35 s on a door): the stale replica is somewhere to go,
/// and the next attempt (Connection::connect tries once more) gets it.
#[tokio::test]
async fn test_read_on_a_silent_primary_falls_back_to_the_stale_replica() {
    let (lb, _) = primary_serving_reads(true).await;

    let started = Instant::now();
    let result = lb.get(&read()).await;
    let waited = started.elapsed();
    assert_eq!(result.err(), Some(Error::AllReplicasDown));
    assert!(waited < Duration::from_millis(1_500), "waited {waited:?}");

    // Nothing fresh answers: the stale replica is back in reads.
    assert!(!lb.targets[0].ban.banned());
    let conn = lb.get(&read()).await.expect("the stale replica serves it");
    assert_eq!(conn.pool.addr().port, 5432);
    drop(conn);

    lb.shutdown();
}

/// A replica out of reads because it is down is nowhere to go: the read
/// waits for the primary as before.
#[tokio::test]
async fn test_read_on_a_silent_primary_waits_when_the_replica_is_down() {
    let (lb, _) = primary_serving_reads(false).await;

    let started = Instant::now();
    let result = lb.get(&read()).await;
    let waited = started.elapsed();
    assert!(result.is_err());
    assert!(waited >= Duration::from_millis(3_000), "waited {waited:?}");

    lb.shutdown();
}
