//! A client's statement events (`[general] query_events`).
//!
//! A request's statements (each simple Query, each Execute) are noted when
//! the request arrives, timed through parsing, the pool checkout and the
//! server, and written when the last message of each has gone to the
//! client: its CommandComplete, EmptyQueryResponse, PortalSuspended or
//! ErrorResponse (extended protocol), or the ReadyForQuery that ends it
//! (simple protocol). A statement PgDog answers itself (BEGIN before the
//! first statement of a transaction, SET, an error of its own) is written
//! with the route `door` when the request is done. A statement cut short by
//! the connection ending is written with the outcome `timeout` or
//! `disconnected`. Nothing here waits: `query_events::send` drops an event
//! the writer has no room for.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use chrono::Utc;
use tokio::time::Instant;

use crate::backend::pool::Connection;
use crate::frontend::Error;
use crate::frontend::router::Route as RouterRoute;
use crate::net::{ErrorResponse, FromBytes, Message, Protocol, ProtocolMessage, ToBytes};
use crate::query_events::{self, ClientInfo, EventError, Outcome, QueryEvent, Route, Text};
use crate::util::{instance_id, user_database_from_params};

use super::QueryEngineContext;

#[derive(Debug, Default)]
pub(crate) struct Events {
    client: Option<Arc<ClientInfo>>,
    xact_seq: u64,
    stmt_seq: u64,
    /// Statements not written yet, in the order the client sent them.
    pending: VecDeque<Pending>,
    /// The last server a statement went to, and its `host:port`.
    server: Option<(String, u16, Arc<str>)>,
}

#[derive(Debug)]
struct Pending {
    at: SystemTime,
    start: Instant,
    /// Filled while the request that carried it is handled.
    open: bool,
    text: Option<Text>,
    extended: bool,
    prepared: bool,
    params: u16,
    in_transaction: bool,
    xact_seq: u64,
    stmt_seq: u64,
    parse: Duration,
    wait: Duration,
    sent: Option<Instant>,
    answered: Option<Instant>,
    route: Route,
    route_reason: Option<&'static str>,
    server: Option<Arc<str>>,
    rows: u64,
    bytes_in: u64,
    bytes_out: u64,
    error: Option<EventError>,
    retried: bool,
    complete: bool,
}

impl Pending {
    fn event(self, client: &Arc<ClientInfo>, now: Instant, outcome: Option<Outcome>) -> QueryEvent {
        let outcome = outcome.unwrap_or(match &self.error {
            Some(error) if error.sqlstate == "57014" => Outcome::Cancelled,
            Some(_) => Outcome::Error,
            None => Outcome::Ok,
        });
        let server_time = match (self.sent, self.answered) {
            (Some(sent), Some(answered)) => answered.saturating_duration_since(sent),
            (Some(sent), None) => now.saturating_duration_since(sent),
            _ => Duration::ZERO,
        };
        QueryEvent {
            at: self.at,
            client: client.clone(),
            route: self.route,
            route_reason: self.route_reason,
            server: self.server,
            text: self.text,
            extended: self.extended,
            prepared: self.prepared,
            params: self.params,
            in_transaction: self.in_transaction,
            xact_seq: self.xact_seq,
            stmt_seq: self.stmt_seq,
            duration: now.saturating_duration_since(self.start),
            parse: self.parse,
            wait: self.wait,
            server_time,
            rows: self.rows,
            bytes_in: self.bytes_in,
            bytes_out: self.bytes_out,
            outcome,
            error: self.error,
            retried: self.retried,
        }
    }
}

fn event_error(error: &ErrorResponse) -> EventError {
    EventError {
        sqlstate: error.code.clone(),
        severity: error.severity.clone(),
        message: error.message.clone(),
    }
}

/// Rows in a CommandComplete tag: its last word, when a number.
fn rows(message: &Message) -> u64 {
    let bytes = message.to_bytes();
    // 'C', length, tag, NUL.
    let tag = bytes
        .get(5..bytes.len().saturating_sub(1))
        .unwrap_or_default();
    let start = tag.iter().rposition(|b| *b == b' ').map_or(0, |p| p + 1);
    std::str::from_utf8(&tag[start..])
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or_default()
}

impl Events {
    /// A request arrived: note its statements.
    pub(super) fn begin(&mut self, context: &QueryEngineContext<'_>) {
        if !query_events::enabled() {
            return;
        }

        if self.client.is_none() {
            let peer = *context.stream.peer_addr();
            let (user, database) = user_database_from_params(context.params);
            self.client = Some(Arc::new(ClientInfo {
                id: format!("{}-{}", instance_id(), context.id.pid()),
                addr: peer.map(|addr| addr.ip().to_string()).unwrap_or_default(),
                port: peer.map(|addr| addr.port().to_string()).unwrap_or_default(),
                application: context
                    .params
                    .get_default("application_name", "")
                    .to_owned(),
                tls_version: context.stream.tls_version(),
                user: user.to_owned(),
                database: database.to_owned(),
            }));
        }

        let now = Instant::now();
        let waited = Utc::now()
            .signed_duration_since(context.statement_start)
            .to_std()
            .unwrap_or_default();
        let start = now.checked_sub(waited).unwrap_or(now);
        let at = SystemTime::from(context.statement_start);
        let in_transaction = context.in_transaction();

        let (mut bytes, mut params, mut prepared) = (0u64, 0u16, false);
        let mut parsed = None;
        let mut first = true;
        let pending = self.pending.len();
        for message in context.client_request.messages.iter() {
            bytes += message.len() as u64;
            let (extended, text) = match message {
                ProtocolMessage::Query(query) => {
                    let payload = &query.payload;
                    let text = (payload.len() > 5).then(|| payload.slice(5..payload.len() - 1));
                    (false, text)
                }
                ProtocolMessage::Execute(_) => (true, parsed.take()),
                ProtocolMessage::Parse(parse) => {
                    let query = parse.query_ref();
                    parsed = (!query.is_empty()).then(|| query.slice(0..query.len() - 1));
                    continue;
                }
                ProtocolMessage::Bind(bind) => {
                    params = bind.params_raw().len().min(u16::MAX as usize) as u16;
                    prepared = !bind.anonymous();
                    continue;
                }
                _ => continue,
            };

            // A request outside a transaction begins one; its other
            // statements share it.
            if first && !in_transaction {
                self.xact_seq += 1;
            }
            first = false;
            self.stmt_seq += 1;
            self.pending.push_back(Pending {
                at,
                start,
                open: true,
                text: text.map(Text::Bytes),
                extended,
                prepared: extended && prepared,
                params: if extended { params } else { 0 },
                in_transaction,
                xact_seq: self.xact_seq,
                stmt_seq: self.stmt_seq,
                parse: Duration::ZERO,
                wait: Duration::ZERO,
                sent: None,
                answered: None,
                route: Route::Door,
                route_reason: None,
                server: None,
                rows: 0,
                bytes_in: std::mem::take(&mut bytes),
                bytes_out: 0,
                error: None,
                retried: false,
                complete: false,
            });
        }
        if self.pending.len() > pending {
            // Sync, Flush and the rest count with the last statement.
            if let Some(last) = self.pending.back_mut() {
                last.bytes_in += bytes;
            }
        } else if let Some(copy) = self
            .pending
            .iter_mut()
            .rev()
            .find(|p| p.sent.is_some() && !p.complete)
        {
            // The data of a COPY in progress.
            copy.bytes_in += bytes;
        }
    }

    /// The request was split into requests of its own: they are the
    /// statements, not it.
    pub(super) fn split(&mut self) {
        let before = self.pending.len();
        let began = self
            .pending
            .iter()
            .find(|p| p.open)
            .is_some_and(|p| !p.in_transaction);
        self.pending.retain(|p| !p.open);
        self.stmt_seq -= (before - self.pending.len()) as u64;
        if began {
            self.xact_seq -= 1;
        }
    }

    fn open(&mut self) -> impl Iterator<Item = &mut Pending> {
        self.pending.iter_mut().filter(|p| p.open)
    }

    /// The request was parsed (and routed): its text and the time it took.
    pub(super) fn parsed(&mut self, context: &QueryEngineContext<'_>, took: Duration) {
        if self.pending.is_empty() {
            return;
        }
        // A Bind of a statement prepared earlier: the parser's copy.
        let text = if self.open().any(|p| p.text.is_none()) {
            context
                .client_request
                .ast
                .as_ref()
                .map(|ast| ast.query_without_comment.clone())
                .or_else(|| {
                    context
                        .client_request
                        .query()
                        .ok()
                        .flatten()
                        .map(|query| Arc::from(query.query()))
                })
                .map(Text::Shared)
        } else {
            None
        };
        for pending in self.open() {
            if pending.text.is_none() {
                pending.text = text.clone();
            }
            pending.parse += took;
        }
    }

    /// Where the router sent the request.
    pub(super) fn routed(&mut self, route: &RouterRoute) {
        let reason = if route.is_read_after_write() {
            "read_after_write"
        } else if route.is_read() {
            "read"
        } else {
            "write"
        };
        for pending in self.open() {
            pending.route_reason.get_or_insert(reason);
        }
    }

    /// The request got a server connection after waiting `wait` for it
    /// (`None`: it had one already).
    pub(super) fn connected(&mut self, wait: Option<Duration>) {
        for pending in self.open().filter(|p| p.sent.is_none()) {
            if let Some(wait) = wait {
                pending.wait += wait;
            }
        }
    }

    /// The request goes to the server now.
    pub(super) fn sending(&mut self, backend: &Connection, pinned: bool, in_transaction: bool) {
        if self.pending.is_empty() {
            return;
        }
        let serving = backend.serving();
        let route = match serving {
            Some((_, crate::config::Role::Replica)) => Route::Replica,
            _ => Route::Primary,
        };
        let server = serving.map(|(addr, _)| match &self.server {
            Some((host, port, name)) if *host == addr.host && *port == addr.port => name.clone(),
            _ => {
                let name: Arc<str> = Arc::from(format!("{}:{}", addr.host, addr.port));
                self.server = Some((addr.host.clone(), addr.port, name.clone()));
                name
            }
        });
        let now = Instant::now();
        for pending in self
            .pending
            .iter_mut()
            .filter(|p| p.open && p.sent.is_none())
        {
            pending.sent = Some(now);
            pending.route = route;
            pending.server = server.clone();
            if pinned {
                pending.route_reason = Some("pinned");
            } else if in_transaction && pending.in_transaction {
                pending.route_reason = Some("transaction");
            }
        }
    }

    /// A read runs again on another server.
    pub(super) fn retried(&mut self) {
        for pending in self.open() {
            pending.retried = true;
        }
    }

    /// PgDog answered with an error of its own.
    pub(super) fn door_error(&mut self, error: &ErrorResponse) {
        if let Some(pending) = self.pending.iter_mut().find(|p| p.open && !p.complete) {
            pending.error = Some(event_error(error));
            pending.complete = true;
        }
    }

    /// A server message went to the client (`forwarded`), or ended the
    /// request without going (a pipeline's ReadyForQuery).
    pub(super) fn server_message(&mut self, message: &Message, forwarded: bool) {
        if self.pending.is_empty() {
            return;
        }
        let code = message.code();
        let now = Instant::now();

        if let Some(head) = self.pending.iter_mut().find(|p| !p.complete) {
            if forwarded {
                head.bytes_out += message.len() as u64;
            }
            match code {
                'C' => {
                    head.rows += rows(message);
                    if head.extended {
                        head.answered = Some(now);
                        head.complete = true;
                    }
                }
                'I' | 's' if head.extended => {
                    head.answered = Some(now);
                    head.complete = true;
                }
                'E' => {
                    if let Ok(error) = ErrorResponse::from_bytes(message.to_bytes()) {
                        head.error = Some(event_error(&error));
                    }
                    if head.extended {
                        head.answered = Some(now);
                        head.complete = true;
                    }
                }
                _ => (),
            }
        }

        if code == 'Z' {
            // The simple Query, or the COPY, is done; an Execute left
            // unanswered was skipped by the server after an error: it never
            // ran. What no server saw yet is left for `finish`.
            self.pending.retain_mut(|p| {
                if p.sent.is_none() || p.complete {
                    return true;
                }
                if p.extended {
                    return false;
                }
                p.answered.get_or_insert(now);
                p.complete = true;
                true
            });
        }

        self.write_complete(now);
    }

    fn write_complete(&mut self, now: Instant) {
        let Some(client) = self.client.clone() else {
            return;
        };
        while self.pending.front().is_some_and(|p| p.complete) {
            if let Some(pending) = self.pending.pop_front() {
                query_events::send(pending.event(&client, now, None));
            }
        }
    }

    /// The request was handled. `done`: nothing more comes from the server
    /// for it (not a COPY in progress, not a streamed answer).
    pub(super) fn finish(&mut self, done: bool) {
        if self.pending.is_empty() {
            return;
        }
        let now = Instant::now();
        for pending in self.pending.iter_mut() {
            // What no server answered, PgDog did.
            if done && pending.open {
                pending.complete = true;
            }
            pending.open = false;
        }
        self.write_complete(now);
    }

    /// The client's connection ends with `err`: its statements end with it.
    pub(super) fn abort(&mut self, err: &Error) {
        let Some(client) = self.client.clone() else {
            return;
        };
        if self.pending.is_empty() {
            return;
        }
        let outcome = if is_timeout(err) {
            Outcome::Timeout
        } else {
            Outcome::Disconnected
        };
        let error = event_error(&ErrorResponse::from_client_err(err));
        let now = Instant::now();
        for mut pending in std::mem::take(&mut self.pending) {
            if pending.complete {
                query_events::send(pending.event(&client, now, None));
            } else {
                pending.error.get_or_insert_with(|| error.clone());
                query_events::send(pending.event(&client, now, Some(outcome)));
            }
        }
    }
}

fn is_timeout(err: &Error) -> bool {
    use crate::backend::Error as BackendError;
    use crate::backend::pool::Error as PoolError;

    matches!(
        err,
        Error::Timeout(_) | Error::Backend(BackendError::Pool(PoolError::CheckoutTimeout))
    )
}
