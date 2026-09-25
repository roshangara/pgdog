//! What the log lost.
//!
//! The log is written by a thread of its own behind a bounded queue, so a
//! slow sink costs lines, never a client's time: this counter is how that
//! loss is seen.

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

        vec![counter(
            "log_lines_dropped_total",
            "Log lines dropped because the log sink was behind.",
            crate::log_sink::dropped(),
        )]
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
        for name in ["log_lines_dropped_total"] {
            assert!(
                rendered.contains(&format!("# TYPE {name} counter")),
                "{rendered}"
            );
        }
    }
}
