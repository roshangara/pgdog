//! Reads served stale because no server within `ban_replica_lag` answers.

use std::collections::BTreeMap;
use std::time::Duration;

use once_cell::sync::Lazy;
use parking_lot::Mutex;

use super::{Measurement, Metric, OpenMetric};

/// (user, database) -> how stale their reads are.
static STALE: Lazy<Mutex<BTreeMap<(String, String), Duration>>> =
    Lazy::new(|| Mutex::new(BTreeMap::new()));

/// Reads of this user and database are this stale: no fresher server
/// answers.
pub(crate) fn set(user: &str, database: &str, staleness: Duration) {
    STALE
        .lock()
        .insert((user.to_owned(), database.to_owned()), staleness);
}

/// Reads of this user and database are fresh again.
pub(crate) fn clear(user: &str, database: &str) {
    STALE.lock().remove(&(user.to_owned(), database.to_owned()));
}

pub(crate) struct StaleReads {
    stale: BTreeMap<(String, String), Duration>,
}

impl StaleReads {
    pub(crate) fn load() -> Metric {
        Metric::new(Self {
            stale: STALE.lock().clone(),
        })
    }
}

impl OpenMetric for StaleReads {
    fn name(&self) -> String {
        "reads_stale_seconds".into()
    }

    fn metric_type(&self) -> String {
        "gauge".into()
    }

    fn help(&self) -> Option<String> {
        Some(
            "How old the data of reads is while no server within ban_replica_lag answers \
             (the freshest that does serves them); 0 when reads are fresh. Unlabelled: the \
             largest of all users and databases."
                .into(),
        )
    }

    fn measurements(&self) -> Vec<Measurement> {
        let largest = self
            .stale
            .values()
            .max()
            .copied()
            .unwrap_or_default()
            .as_secs_f64();

        let mut measurements = vec![Measurement {
            labels: vec![],
            measurement: largest.into(),
        }];
        measurements.extend(
            self.stale
                .iter()
                .map(|((user, database), staleness)| Measurement {
                    labels: vec![
                        ("user".into(), user.clone()),
                        ("database".into(), database.clone()),
                    ],
                    measurement: staleness.as_secs_f64().into(),
                }),
        );
        measurements
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_stale_reads_metric() {
        set("stale_user", "stale_db", Duration::from_secs(42));
        let render = StaleReads::load().to_string();
        assert!(
            render.contains("# TYPE reads_stale_seconds gauge"),
            "{render}"
        );
        assert!(
            render.contains(r#"reads_stale_seconds{user="stale_user",database="stale_db"} 42"#),
            "{render}"
        );

        clear("stale_user", "stale_db");
        let render = StaleReads::load().to_string();
        assert!(!render.contains("stale_user"), "{render}");
    }
}
