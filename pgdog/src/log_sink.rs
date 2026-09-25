//! The log sink: logging never blocks serving.
//!
//! Log lines are formatted on the thread that logs them and handed to a
//! thread of their own over a bounded queue; that thread writes them to
//! stderr. When the queue is full (stderr is a pipe nobody is reading fast
//! enough: journald, a container runtime rotating its log on a slow disk),
//! the line is dropped and counted instead of blocking the thread that
//! serves clients. The count is `log_lines_dropped_total` on the metrics
//! port, and the sink writes how many lines it lost once it catches up.
//!
//! Per-connection lines (a client connected or disconnected, a server
//! connection opened or closed) use the [`CONNECTIONS`] target, so
//! `log_level = "info,pgdog::connections=warn"` turns them down while
//! keeping their errors: their volume follows connection churn, and the
//! counts are on the metrics port.

use std::io::{self, Write};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::thread;
use std::time::{Duration, Instant};

use tracing_subscriber::fmt::MakeWriter;

/// Target of every per-connection log line.
pub(crate) const CONNECTIONS: &str = "pgdog::connections";

/// Lines the queue holds while the sink is behind.
pub(crate) const QUEUE_LINES: usize = 16_384;

static DROPPED: AtomicU64 = AtomicU64::new(0);
static SINK: OnceLock<LogSink> = OnceLock::new();

/// Log lines dropped because the sink was behind.
pub(crate) fn dropped() -> u64 {
    DROPPED.load(Ordering::Relaxed)
}

enum Message {
    Line(Vec<u8>),
    Flush(SyncSender<()>),
}

/// A writer for `tracing_subscriber::fmt` that never blocks.
#[derive(Clone)]
pub(crate) struct LogSink {
    tx: SyncSender<Message>,
    dropped: &'static AtomicU64,
}

impl LogSink {
    /// The process' sink, writing to stderr. Made once; later calls
    /// return the same sink.
    pub(crate) fn stderr() -> Self {
        SINK.get_or_init(|| Self::spawn(io::stderr(), QUEUE_LINES, &DROPPED))
            .clone()
    }

    /// A sink writing to `out` from a thread of its own.
    fn spawn<W: Write + Send + 'static>(
        out: W,
        capacity: usize,
        dropped: &'static AtomicU64,
    ) -> Self {
        let (tx, rx) = sync_channel(capacity);
        let spawned = thread::Builder::new()
            .name("pgdog-log".into())
            .spawn(move || write_lines(rx, out, dropped));
        if spawned.is_err() {
            // No thread: every line is dropped and counted, nothing blocks.
            let (tx, _) = sync_channel(0);
            return Self { tx, dropped };
        }
        Self { tx, dropped }
    }

    /// Queue a line; a full queue drops it.
    fn send(&self, line: &[u8]) {
        match self.tx.try_send(Message::Line(line.to_vec())) {
            Ok(()) => (),
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Wait up to `timeout` for the queued lines to be written. Called
    /// before the process exits, so its last lines are not lost.
    pub(crate) fn flush(&self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        let (ack, done) = sync_channel(1);
        let mut message = Message::Flush(ack);
        loop {
            match self.tx.try_send(message) {
                Ok(()) => break,
                Err(TrySendError::Full(returned)) if Instant::now() < deadline => {
                    message = returned;
                    thread::sleep(Duration::from_millis(1));
                }
                Err(_) => return,
            }
        }
        let _ = done.recv_timeout(deadline.saturating_duration_since(Instant::now()));
    }
}

/// Flush the process' sink, if it was made.
pub(crate) fn flush() {
    if let Some(sink) = SINK.get() {
        sink.flush(Duration::from_secs(2));
    }
}

fn write_lines<W: Write>(rx: Receiver<Message>, out: W, dropped: &AtomicU64) {
    let mut out = io::BufWriter::with_capacity(64 * 1024, out);
    let mut reported = 0;
    let mut acks = vec![];

    while let Ok(message) = rx.recv() {
        let mut next = Some(message);
        // Write everything queued, then flush once.
        while let Some(message) = next.take() {
            match message {
                Message::Line(line) => {
                    let _ = out.write_all(&line);
                }
                Message::Flush(ack) => acks.push(ack),
            }
            next = rx.try_recv().ok();
        }

        let lost = dropped.load(Ordering::Relaxed);
        if lost > reported {
            let _ = writeln!(
                out,
                "pgdog: {} log lines dropped: the log sink was behind",
                lost - reported
            );
            reported = lost;
        }

        let _ = out.flush();
        for ack in acks.drain(..) {
            let _ = ack.try_send(());
        }
    }
}

/// One line at a time: the formatter writes each event with one
/// `write_all`.
pub(crate) struct LogLine<'a> {
    sink: &'a LogSink,
}

impl Write for LogLine<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.sink.send(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for LogSink {
    type Writer = LogLine<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        LogLine { sink: self }
    }
}

#[cfg(test)]
mod test {
    use std::sync::{Arc, Barrier, Mutex};

    use super::*;

    /// A writer that, once, blocks until the test lets it go, like a pipe
    /// nobody reads.
    struct Stalled {
        entered: Option<SyncSender<()>>,
        release: Arc<Barrier>,
        written: Arc<Mutex<Vec<u8>>>,
    }

    impl Write for Stalled {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if let Some(entered) = self.entered.take() {
                entered.send(()).unwrap();
                self.release.wait();
            }
            self.written.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_a_stalled_sink_drops_and_counts_instead_of_blocking() {
        static TEST_DROPPED: AtomicU64 = AtomicU64::new(0);
        let release = Arc::new(Barrier::new(2));
        let written = Arc::new(Mutex::new(vec![]));
        let (entered, stalled) = sync_channel(1);
        let sink = LogSink::spawn(
            Stalled {
                entered: Some(entered),
                release: release.clone(),
                written: written.clone(),
            },
            4,
            &TEST_DROPPED,
        );

        // The sink's thread takes the first line and blocks writing it.
        sink.make_writer().write_all(b"line 0\n").unwrap();
        stalled.recv_timeout(Duration::from_secs(5)).unwrap();

        // Four more fill the queue; the rest must neither block nor be kept.
        let start = Instant::now();
        for i in 1..=100 {
            let mut line = sink.make_writer();
            line.write_all(format!("line {i}\n").as_bytes()).unwrap();
        }
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "logging waited for a stalled sink"
        );
        assert_eq!(TEST_DROPPED.load(Ordering::Relaxed), 96);

        // The pipe drains: what was queued is written, and the loss is said.
        release.wait();
        sink.flush(Duration::from_secs(5));
        let written = String::from_utf8(written.lock().unwrap().clone()).unwrap();
        assert_eq!(
            written,
            "line 0\nline 1\nline 2\nline 3\nline 4\npgdog: 96 log lines dropped: the log sink was behind\n"
        );
    }

    #[test]
    fn test_flush_writes_every_queued_line() {
        static TEST_DROPPED: AtomicU64 = AtomicU64::new(0);
        let written = Arc::new(Mutex::new(vec![]));
        let sink = LogSink::spawn(
            Stalled {
                entered: None,
                release: Arc::new(Barrier::new(1)),
                written: written.clone(),
            },
            1024,
            &TEST_DROPPED,
        );
        for i in 0..500 {
            sink.make_writer()
                .write_all(format!("{i}\n").as_bytes())
                .unwrap();
        }
        sink.flush(Duration::from_secs(5));
        let written = String::from_utf8(written.lock().unwrap().clone()).unwrap();
        assert_eq!(written.lines().count(), 500);
        assert_eq!(TEST_DROPPED.load(Ordering::Relaxed), 0);
    }
}
