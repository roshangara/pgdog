//! Read-after-write metrics.

use std::sync::atomic::{AtomicU64, Ordering};

use super::{Measurement, Metric, OpenMetric};

static READS_ON_PRIMARY: AtomicU64 = AtomicU64::new(0);

/// Count a read sent to the primary because its client wrote recently.
pub(crate) fn read_on_primary() {
    READS_ON_PRIMARY.fetch_add(1, Ordering::Relaxed);
}

pub(crate) struct ReadAfterWrite {
    reads_on_primary: u64,
}

impl ReadAfterWrite {
    pub(crate) fn load() -> Metric {
        Metric::new(Self {
            reads_on_primary: READS_ON_PRIMARY.load(Ordering::Relaxed),
        })
    }
}

impl OpenMetric for ReadAfterWrite {
    fn name(&self) -> String {
        "read_after_write_primary_reads_total".into()
    }

    fn metric_type(&self) -> String {
        "counter".into()
    }

    fn help(&self) -> Option<String> {
        Some(
            "Reads sent to the primary because their client wrote within read_after_write_ms."
                .into(),
        )
    }

    fn measurements(&self) -> Vec<Measurement> {
        vec![Measurement {
            labels: vec![],
            measurement: self.reads_on_primary.into(),
        }]
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_read_after_write_metric_counts() {
        let before = READS_ON_PRIMARY.load(Ordering::Relaxed);
        read_on_primary();

        let render = ReadAfterWrite::load().to_string();
        let lines: Vec<&str> = render.lines().collect();
        assert_eq!(
            lines[0],
            "# TYPE read_after_write_primary_reads_total counter"
        );
        // Other tests may count reads concurrently.
        let value: u64 = lines
            .iter()
            .find(|line| line.starts_with("read_after_write_primary_reads_total "))
            .and_then(|line| line.split(' ').nth(1))
            .and_then(|value| value.parse().ok())
            .expect("counter sample");
        assert!(value > before);
    }
}
