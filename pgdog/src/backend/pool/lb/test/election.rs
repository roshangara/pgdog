//! Primary election with automatic roles, and writes during a failover.

use std::time::SystemTime;

use crate::backend::pool::lsn_monitor::LsnStats;

use super::*;

const CHECKOUT_TIMEOUT: Duration = Duration::from_millis(2_000);

/// Auto targets; a closed port stands for a dead server.
fn auto_lb(targets: &[(&str, u16)]) -> LoadBalancer {
    let configs = targets
        .iter()
        .map(|(host, port)| {
            let mut config = create_auto_test_pool_config(host, *port);
            config.config.checkout_timeout = CHECKOUT_TIMEOUT;
            config.config.connect_timeout = Duration::from_millis(100);
            config
        })
        .collect::<Vec<_>>();

    LoadBalancer::new(
        &None,
        &configs,
        LoadBalancingStrategy::RoundRobin,
        ReadWriteSplit::ExcludePrimary,
        Default::default(),
    )
}

fn stats(target: &Target, replica: bool, timeline: i64, lsn: i64, age: Duration) {
    let stats: LsnStats = StatsLsnStats {
        replica,
        timeline,
        lsn: Lsn::from_i64(lsn),
        offset_bytes: lsn,
        fetched: SystemTime::now() - age,
        ..Default::default()
    }
    .into();
    *target.pool.inner().lsn_stats.write() = stats;
}

fn primary_port(lb: &LoadBalancer) -> Option<u16> {
    lb.primary().map(|pool| pool.addr().port)
}

const FRESH: Duration = Duration::ZERO;

#[test]
fn test_election_prefers_latest_timeline() {
    let lb = auto_lb(&[("127.0.0.1", 5001), ("127.0.0.1", 5002)]);
    stats(&lb.targets[0], false, 7, 900, FRESH);
    stats(&lb.targets[1], false, 8, 100, FRESH);

    assert!(lb.redetect_roles());
    assert_eq!(primary_port(&lb), Some(5002));
}

#[test]
fn test_election_same_timeline_most_wal() {
    let lb = auto_lb(&[("127.0.0.1", 5001), ("127.0.0.1", 5002)]);
    stats(&lb.targets[0], false, 8, 900, FRESH);
    stats(&lb.targets[1], false, 8, 100, FRESH);

    assert!(lb.redetect_roles());
    assert_eq!(primary_port(&lb), Some(5001));
}

#[test]
fn test_crashed_primary_loses_to_promoted_replica() {
    let lb = auto_lb(&[("127.0.0.1", 5001), ("127.0.0.1", 5002)]);
    stats(&lb.targets[0], false, 7, 500, FRESH);
    stats(&lb.targets[1], true, 0, 500, FRESH);
    assert!(lb.redetect_roles());
    assert_eq!(primary_port(&lb), Some(5001));

    // 5001 stops answering: its last stats stay. 5002 is promoted.
    stats(&lb.targets[0], false, 7, 500, Duration::from_secs(60));
    stats(&lb.targets[1], false, 8, 520, FRESH);
    assert!(lb.redetect_roles());
    assert_eq!(primary_port(&lb), Some(5002));
}

/// GH#1255: the old primary comes back before it is made a replica. It
/// reports fresh stats and more WAL, on the old timeline: it must not win.
#[test]
fn test_old_primary_back_as_primary_does_not_win() {
    let lb = auto_lb(&[("127.0.0.1", 5001), ("127.0.0.1", 5002)]);
    stats(&lb.targets[0], true, 0, 500, FRESH);
    stats(&lb.targets[1], false, 8, 520, Duration::from_secs(2));
    assert!(lb.redetect_roles());

    stats(&lb.targets[0], false, 7, 9_000, FRESH);
    assert!(!lb.redetect_roles());
    assert_eq!(primary_port(&lb), Some(5002));
}

/// GH#1255: the elected primary reports it's in recovery (it was demoted,
/// or came back as a replica). Nobody else is primary yet: drop it at once.
#[test]
fn test_primary_in_recovery_is_dropped() {
    let lb = auto_lb(&[("127.0.0.1", 5001), ("127.0.0.1", 5002)]);
    stats(&lb.targets[0], false, 7, 500, FRESH);
    stats(&lb.targets[1], true, 0, 500, FRESH);
    assert!(lb.redetect_roles());
    assert_eq!(primary_port(&lb), Some(5001));

    stats(&lb.targets[0], true, 0, 510, FRESH);
    assert!(lb.redetect_roles());
    assert_eq!(primary_port(&lb), None);
    assert!(
        lb.targets
            .iter()
            .all(|target| target.role() == Role::Replica)
    );
}

#[tokio::test]
async fn test_write_waits_for_election() {
    let lb = auto_lb(&[("127.0.0.1", 5432), ("127.0.0.1", 1)]);
    lb.launch();
    stats(&lb.targets[0], true, 0, 500, FRESH);
    stats(&lb.targets[1], true, 0, 500, FRESH);
    lb.redetect_roles();

    let started = Instant::now();
    let write = {
        let lb = lb.clone();
        tokio::spawn(async move { lb.get_primary(&Request::default()).await })
    };

    sleep(Duration::from_millis(300)).await;
    assert!(!write.is_finished(), "no primary: the write must wait");

    stats(&lb.targets[0], false, 8, 600, FRESH);
    assert!(lb.redetect_roles());

    let conn = timeout(Duration::from_secs(1), write)
        .await
        .expect("the write must resume right after the election")
        .unwrap()
        .expect("connection to the new primary");
    assert_eq!(conn.pool.addr().port, 5432);
    assert!(started.elapsed() < CHECKOUT_TIMEOUT);
    drop(conn);

    lb.shutdown();
}

#[tokio::test]
async fn test_write_moves_to_new_primary() {
    let lb = auto_lb(&[("127.0.0.1", 1), ("127.0.0.1", 5432)]);
    lb.launch();
    // The elected primary is dead: the write waits on its pool.
    stats(&lb.targets[0], false, 7, 500, FRESH);
    stats(&lb.targets[1], true, 0, 500, FRESH);
    assert!(lb.redetect_roles());

    let started = Instant::now();
    let write = {
        let lb = lb.clone();
        tokio::spawn(async move { lb.get_primary(&Request::default()).await })
    };

    sleep(Duration::from_millis(300)).await;
    assert!(!write.is_finished());

    // A replica is promoted.
    stats(&lb.targets[1], false, 8, 520, FRESH);
    assert!(lb.redetect_roles());

    let conn = timeout(Duration::from_secs(1), write)
        .await
        .expect("the write must move to the new primary")
        .unwrap()
        .expect("connection to the new primary");
    assert_eq!(conn.pool.addr().port, 5432);
    assert!(started.elapsed() < CHECKOUT_TIMEOUT);
    drop(conn);

    lb.shutdown();
}

#[tokio::test]
async fn test_write_without_primary_waits_checkout_timeout() {
    let lb = auto_lb(&[("127.0.0.1", 5432), ("127.0.0.1", 1)]);
    lb.launch();
    stats(&lb.targets[0], true, 0, 500, FRESH);
    stats(&lb.targets[1], true, 0, 500, FRESH);
    lb.redetect_roles();

    let started = Instant::now();
    let result = lb.get_primary(&Request::default()).await;
    let waited = started.elapsed();

    assert!(matches!(result, Err(Error::CheckoutTimeout)));
    assert!(
        waited >= CHECKOUT_TIMEOUT,
        "must not fail early: {waited:?}"
    );
    assert!(waited < CHECKOUT_TIMEOUT + Duration::from_millis(500));

    lb.shutdown();
}
