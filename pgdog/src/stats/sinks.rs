//! What the log and the statement events lost, and wrote.
//!
//! Both are written by threads of their own behind bounded queues, so a
//! slow sink costs lines or events, never a client's time: these counters
//! are how that loss is seen.

use super::pools::PoolMetric;
use super::{Measurement, Metric};

pub(crate) struct Sinks;

impl Sinks {
    pub(crate) fn load() -> Vec<Metric> {
        let counter = |name: &str, help: &str, value: u64| {
            Metric::new(PoolMetric {
                name: name.into(),
                measurements: vec![Measurement {
                    labels: vec![],
                    measurement: value.into(),
                }],
                help: help.into(),
                unit: None,
                metric_type: Some("counter".into()),
            })
        };

        vec![
            counter(
                "log_lines_dropped_total",
                "Log lines dropped because the log sink was behind.",
                crate::log_sink::dropped(),
            ),
            counter(
                "query_events_total",
                "Statement events written to query_events.",
                crate::query_events::written(),
            ),
            counter(
                "query_events_dropped_total",
                "Statement events dropped because their writer was behind.",
                crate::query_events::dropped(),
            ),
        ]
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_sinks_render_as_counters() {
        let rendered = Sinks::load()
            .iter()
            .map(|m| m.to_string())
            .collect::<String>();
        for name in [
            "log_lines_dropped_total",
            "query_events_total",
            "query_events_dropped_total",
        ] {
            assert!(
                rendered.contains(&format!("# TYPE {name} counter")),
                "{rendered}"
            );
        }
    }
}
