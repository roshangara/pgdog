//! The thread that writes statement events to the file and rotates it.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel};
use std::thread;
use std::time::{Duration, Instant, UNIX_EPOCH};

use tracing::{error, info};

use super::fingerprint::{command, fingerprint};
use super::{DROPPED, GENERATION, QueryEvent, ROTATED, TARGET, Target, Text, WRITTEN};
use crate::util::instance_id;

/// How long an error message may be in an event.
const MAX_ERROR_MESSAGE: usize = 8 * 1024;

/// While events flow, the writer drains the queue this often instead of
/// waiting on it: a producer then never has to wake it (a syscall and a
/// context switch per event), and a batch is one write.
const DRAIN_EVERY: Duration = Duration::from_millis(10);

/// Fingerprints kept by text, for the statements the parser shares.
const FINGERPRINTS: usize = 4096;

/// Start the writer; the queue holds `capacity` events.
pub(super) fn spawn(capacity: usize) -> SyncSender<Box<QueryEvent>> {
    let (tx, rx) = sync_channel(capacity);
    let spawned = thread::Builder::new()
        .name("pgdog-query-events".into())
        .spawn(move || Writer::new(instance_id()).run(rx));
    if let Err(err) = spawned {
        error!("query events: no writer thread: {err}; every event is dropped");
    }
    tx
}

pub(super) struct Writer {
    instance: String,
    generation: u64,
    target: Option<Target>,
    file: Option<BufWriter<File>>,
    size: u64,
    seq: u64,
    failed: Option<Instant>,
    line: Vec<u8>,
    /// Fingerprint and command of the parser's shared texts, by the text's
    /// address; the entry holds the text, so the address can't be reused.
    fingerprints: HashMap<usize, Fingerprinted>,
}

/// A parser's shared text, its fingerprint, and where its command is in it.
type Fingerprinted = (Arc<str>, u64, Option<(usize, usize)>);

impl Writer {
    pub(super) fn new(instance: &str) -> Self {
        Self {
            instance: instance.to_owned(),
            generation: u64::MAX,
            target: None,
            file: None,
            size: 0,
            seq: 0,
            failed: None,
            line: Vec::with_capacity(1024),
            fingerprints: HashMap::new(),
        }
    }

    fn run(mut self, rx: Receiver<Box<QueryEvent>>) {
        loop {
            // Quiet: wait for an event; its producer wakes this thread once.
            match rx.recv_timeout(Duration::from_secs(1)) {
                Ok(event) => {
                    self.refresh();
                    self.write(&event);
                }
                Err(RecvTimeoutError::Timeout) => {
                    self.refresh();
                    self.flush();
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
            // Flowing: drain, write the batch, sleep, again, until a drain
            // finds nothing.
            loop {
                let mut drained = 0;
                for event in rx.try_iter() {
                    self.write(&event);
                    drained += 1;
                }
                self.flush();
                if drained == 0 {
                    break;
                }
                thread::sleep(DRAIN_EVERY);
            }
        }
        self.flush();
    }

    /// Follow a configuration change: another file, or none.
    fn refresh(&mut self) {
        let generation = GENERATION.load(Ordering::Acquire);
        if generation == self.generation {
            return;
        }
        self.generation = generation;
        let target = TARGET.lock().clone();
        if target != self.target {
            self.flush();
            self.file = None;
            self.failed = None;
            if let Some(ref target) = target {
                info!(
                    "query events to {} (rotated at {} bytes)",
                    target.path.display(),
                    target.max_bytes
                );
            }
            self.target = target;
        }
    }

    fn flush(&mut self) {
        if let Some(file) = self.file.as_mut()
            && let Err(err) = file.flush()
        {
            self.fail(&err);
        }
    }

    fn fail(&mut self, err: &std::io::Error) {
        if let Some(ref target) = self.target {
            error!(
                "query events: {}: {err}; events are dropped until it can be written",
                target.path.display()
            );
        }
        self.file = None;
        self.failed = Some(Instant::now());
    }

    /// The file, opened if it isn't; `None` after a failure, for a while.
    fn file(&mut self) -> Option<&mut BufWriter<File>> {
        if self.file.is_none() {
            let target = self.target.as_ref()?;
            if self
                .failed
                .is_some_and(|at| at.elapsed() < Duration::from_secs(5))
            {
                return None;
            }
            match open(&target.path) {
                Ok((file, size)) => {
                    self.file = Some(BufWriter::with_capacity(256 * 1024, file));
                    self.size = size;
                    self.failed = None;
                }
                Err(err) => {
                    self.fail(&err);
                    return None;
                }
            }
        }
        self.file.as_mut()
    }

    pub(super) fn write(&mut self, event: &QueryEvent) {
        if self.target.is_none() {
            DROPPED.fetch_add(1, Ordering::Relaxed);
            return;
        }

        self.seq += 1;
        let mut line = std::mem::take(&mut self.line);
        line.clear();
        let text = event.text.as_ref().map(|text| self.fingerprint(text));
        encode(&mut line, event, &self.instance, self.seq, text);

        let written = match self.file() {
            Some(file) => file.write_all(&line),
            None => {
                DROPPED.fetch_add(1, Ordering::Relaxed);
                self.line = line;
                return;
            }
        };
        match written {
            Ok(()) => {
                WRITTEN.fetch_add(1, Ordering::Relaxed);
                self.size += line.len() as u64;
            }
            Err(err) => {
                DROPPED.fetch_add(1, Ordering::Relaxed);
                self.fail(&err);
            }
        }
        self.line = line;

        if let Some(max) = self.target.as_ref().map(|target| target.max_bytes)
            && self.size >= max
        {
            self.rotate();
        }
    }

    /// The fingerprint and the command of a statement's text. The parser's
    /// shared text of a prepared statement is the same allocation for every
    /// execution: its fingerprint is computed once.
    fn fingerprint(&mut self, text: &Text) -> (u64, Option<String>) {
        let computed = |text: &str| {
            let command = command(text).map(|(start, end)| text[start..end].to_ascii_uppercase());
            (fingerprint(text), command)
        };
        let Text::Shared(shared) = text else {
            return text.with_str(computed);
        };
        let key = shared.as_ptr() as usize;
        if let Some((kept, fingerprint, command)) = self.fingerprints.get(&key)
            && kept.len() == shared.len()
        {
            let command = command.map(|(start, end)| shared[start..end].to_ascii_uppercase());
            return (*fingerprint, command);
        }
        if self.fingerprints.len() >= FINGERPRINTS {
            self.fingerprints.clear();
        }
        let fingerprint = fingerprint(shared);
        let range = command(shared);
        self.fingerprints
            .insert(key, (shared.clone(), fingerprint, range));
        (
            fingerprint,
            range.map(|(start, end)| shared[start..end].to_ascii_uppercase()),
        )
    }

    /// `path` becomes `path.1`, `path.1` becomes `path.2`, ... `path.7` goes.
    fn rotate(&mut self) {
        self.flush();
        self.file = None;
        let Some(path) = self.target.as_ref().map(|target| target.path.clone()) else {
            return;
        };
        for n in (1..ROTATED).rev() {
            let from = rotated(&path, n);
            if from.exists()
                && let Err(err) = fs::rename(&from, rotated(&path, n + 1))
            {
                error!("query events: rotating {}: {err}", from.display());
            }
        }
        if let Err(err) = fs::rename(&path, rotated(&path, 1)) {
            error!("query events: rotating {}: {err}", path.display());
        }
        self.size = 0;
    }
}

/// `path.n`.
pub(crate) fn rotated(path: &Path, n: usize) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(format!(".{n}"));
    PathBuf::from(name)
}

fn open(path: &Path) -> std::io::Result<(File, u64)> {
    if let Some(dir) = path.parent()
        && !dir.as_os_str().is_empty()
    {
        fs::create_dir_all(dir)?;
    }
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    let size = file.metadata()?.len();
    Ok((file, size))
}

/// Milliseconds with a fraction, rounded once (0.496396, not 0.49639599999999995).
fn millis(duration: Duration) -> f64 {
    duration.as_nanos() as f64 / 1_000_000.0
}

fn string(line: &mut Vec<u8>, key: &str, value: &str) {
    line.push(b',');
    push_key(line, key);
    // A str always serializes.
    let _ = serde_json::to_writer(&mut *line, value);
}

fn push_key(line: &mut Vec<u8>, key: &str) {
    line.push(b'"');
    line.extend_from_slice(key.as_bytes());
    line.extend_from_slice(b"\":");
}

fn int(line: &mut Vec<u8>, key: &str, value: u64) {
    line.push(b',');
    push_key(line, key);
    line.extend_from_slice(itoa::Buffer::new().format(value).as_bytes());
}

fn float(line: &mut Vec<u8>, key: &str, value: f64) {
    line.push(b',');
    push_key(line, key);
    if value.is_finite() {
        line.extend_from_slice(ryu::Buffer::new().format_finite(value).as_bytes());
    } else {
        line.extend_from_slice(b"null");
    }
}

fn boolean(line: &mut Vec<u8>, key: &str, value: bool) {
    line.push(b',');
    push_key(line, key);
    line.extend_from_slice(if value { b"true" } else { b"false" });
}

/// One JSON object and a newline. Names are the columns of ganjban's
/// `ganjban_door_query`; a field with no value is left out.
pub(super) fn encode(
    line: &mut Vec<u8>,
    event: &QueryEvent,
    instance: &str,
    seq: u64,
    text: Option<(u64, Option<String>)>,
) {
    let at = event
        .at
        .duration_since(UNIX_EPOCH)
        .map(millis)
        .unwrap_or_default();
    line.extend_from_slice(b"{\"at\":");
    line.extend_from_slice(ryu::Buffer::new().format_finite(at).as_bytes());
    line.extend_from_slice(b",\"event_id\":\"");
    line.extend_from_slice(instance.as_bytes());
    line.push(b'-');
    line.extend_from_slice(itoa::Buffer::new().format(seq).as_bytes());
    line.push(b'"');

    let client = &event.client;
    string(line, "client_id", &client.id);
    string(line, "client_addr", &client.addr);
    string(line, "client_port", &client.port);
    string(line, "application", &client.application);
    if let Some(tls) = client.tls_version {
        string(line, "tls_version", tls);
    }
    string(line, "database", &client.database);
    string(line, "user", &client.user);

    string(line, "route", event.route.as_str());
    if let Some(reason) = event.route_reason {
        string(line, "route_reason", reason);
    }
    if let Some(ref server) = event.server {
        string(line, "server", server);
    }
    if let Some((fingerprint, command)) = text {
        line.extend_from_slice(b",\"fingerprint\":\"");
        for shift in (0..16).rev() {
            line.push(b"0123456789abcdef"[((fingerprint >> (shift * 4)) & 0xf) as usize]);
        }
        line.push(b'"');
        if let Some(command) = command {
            string(line, "command", &command);
        }
    }
    string(
        line,
        "protocol",
        if event.extended { "extended" } else { "simple" },
    );
    boolean(line, "prepared", event.prepared);
    int(line, "params", event.params as u64);
    boolean(line, "in_transaction", event.in_transaction);
    int(line, "xact_seq", event.xact_seq);
    int(line, "stmt_seq", event.stmt_seq);

    let accounted = event.parse + event.wait + event.server_time;
    float(line, "duration_ms", millis(event.duration));
    float(line, "parse_ms", millis(event.parse));
    float(line, "wait_ms", millis(event.wait));
    float(line, "server_ms", millis(event.server_time));
    float(
        line,
        "transfer_ms",
        millis(event.duration.saturating_sub(accounted)),
    );

    int(line, "rows", event.rows);
    int(line, "bytes_in", event.bytes_in);
    int(line, "bytes_out", event.bytes_out);
    string(line, "outcome", event.outcome.as_str());
    if let Some(ref error) = event.error {
        string(line, "sqlstate", &error.sqlstate);
        string(line, "error_severity", &error.severity);
        let message = match error.message.char_indices().nth(MAX_ERROR_MESSAGE) {
            Some((end, _)) => &error.message[..end],
            None => &error.message,
        };
        string(line, "error_message", message);
    }
    boolean(line, "retried", event.retried);
    line.extend_from_slice(b"}\n");
}

#[cfg(test)]
mod test {
    use serde_json::Value;

    use super::super::Text;

    use super::super::test_support::event;
    use super::super::{EventError, Outcome};
    use super::*;

    fn parse(line: &[u8]) -> Value {
        serde_json::from_slice(line).expect("one JSON object")
    }

    #[test]
    fn test_an_event_is_one_json_line_with_the_table_columns() {
        let mut line = vec![];
        let mut event = event("SELECT * FROM t WHERE id = 42");
        event.error = Some(EventError {
            sqlstate: "40001".into(),
            severity: "ERROR".into(),
            message: "could not serialize \"access\"".into(),
        });
        event.outcome = Outcome::Error;
        let text = Writer::new("a1b2c3d4").fingerprint(event.text.as_ref().unwrap());
        encode(&mut line, &event, "a1b2c3d4", 7, Some(text));

        assert_eq!(line.iter().filter(|b| **b == b'\n').count(), 1);
        assert!(line.ends_with(b"}\n"));
        let json = parse(&line);
        assert_eq!(json["event_id"], "a1b2c3d4-7");
        assert_eq!(json["client_id"], "test-1");
        assert_eq!(json["client_port"], "5555");
        assert_eq!(json["route"], "replica");
        assert_eq!(json["route_reason"], "read");
        assert_eq!(json["server"], "10.0.0.2:5432");
        assert_eq!(json["command"], "SELECT");
        assert_eq!(json["protocol"], "extended");
        assert_eq!(json["params"], 1);
        assert_eq!(json["rows"], 1);
        assert_eq!(json["outcome"], "error");
        assert_eq!(json["sqlstate"], "40001");
        assert_eq!(json["error_message"], "could not serialize \"access\"");
        assert!(json["duration_ms"].as_f64().unwrap() > 1.49);
        // duration - parse - wait - server
        let transfer = json["transfer_ms"].as_f64().unwrap();
        assert!((transfer - 0.25).abs() < 0.001, "{transfer}");
        // Never the text or its values.
        assert!(!String::from_utf8_lossy(&line).contains("FROM t"));
        assert!(json.get("text").is_none());
        let at = json["at"].as_f64().unwrap();
        assert!(at > 1.7e12, "{at}");
        // Floats keep a point, so a whole number is typed as a double, and
        // carry no rounding noise.
        assert!(String::from_utf8_lossy(&line).contains("\"wait_ms\":0.03,"));
        assert!(String::from_utf8_lossy(&line).contains("\"server_ms\":1.2,"));
    }

    #[test]
    fn test_statements_that_differ_only_in_values_share_a_fingerprint() {
        let encode_one = |text: &str| {
            let mut line = vec![];
            let event = event(text);
            let text = Writer::new("i").fingerprint(event.text.as_ref().unwrap());
            encode(&mut line, &event, "i", 1, Some(text));
            parse(&line)["fingerprint"].as_str().unwrap().to_owned()
        };
        assert_eq!(
            encode_one("SELECT * FROM t WHERE id = 1 AND name = 'a'"),
            encode_one("select *  from t where id = 99 and name = 'it''s'")
        );
        assert_ne!(
            encode_one("SELECT * FROM t WHERE id = 1"),
            encode_one("SELECT * FROM u WHERE id = 1")
        );
    }

    #[test]
    fn test_a_shared_text_is_fingerprinted_once() {
        let mut writer = Writer::new("i");
        let shared: Arc<str> = Arc::from("select * from t where id = $1");
        let first = writer.fingerprint(&Text::Shared(shared.clone()));
        assert_eq!(writer.fingerprints.len(), 1);
        let again = writer.fingerprint(&Text::Shared(shared.clone()));
        assert_eq!(first, again);
        assert_eq!(first.1.as_deref(), Some("SELECT"));
        // The same text in another allocation gives the same answer.
        let other = writer.fingerprint(&Text::Bytes("select * from t where id = $1".into()));
        assert_eq!(first, other);
    }

    #[test]
    fn test_rotation_keeps_seven_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.ndjson");
        let mut writer = Writer::new("i");
        writer.target = Some(Target {
            path: path.clone(),
            max_bytes: 1,
        });
        writer.generation = GENERATION.load(Ordering::Acquire);

        // Every event rotates the file: ten events, the current file is
        // empty, .1 has the newest, .7 the oldest kept.
        for n in 1..=10 {
            let mut event = event("SELECT 1");
            event.stmt_seq = n;
            writer.write(&event);
        }
        assert!(!rotated(&path, 8).exists());
        let seq = |n: usize| {
            let raw = fs::read(rotated(&path, n)).unwrap();
            parse(&raw)["stmt_seq"].as_u64().unwrap()
        };
        assert_eq!(seq(1), 10);
        assert_eq!(seq(7), 4);
        // The next event opens a new file.
        writer.write(&event("SELECT 2"));
        assert_eq!(seq(1), 1);
        assert_eq!(seq(7), 5);
    }
}
