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
