use fnv::FnvHashSet;

use crate::frontend::router::parser::statement::{AdvisoryLocks as ParserAdvisoryLocks, LockScope};

/// The query that tells how many session advisory locks the server
/// connection holds. Run outside a transaction, where no transaction-level
/// advisory lock can exist.
pub(crate) const HELD_ADVISORY_LOCKS: &str = "SELECT pg_catalog.count(*) FROM pg_catalog.pg_locks \
     WHERE locktype = 'advisory' AND pid = pg_catalog.pg_backend_pid() AND granted";

/// Tracks the session advisory locks the current client holds, which pin it
/// to its server connection.
///
/// A key we can read (a literal, a bound parameter) is tracked by value. A
/// lock whose key we can't read (`pg_advisory_lock(hashtext('x'))`) pins the
/// client too. A release that may leave the client holding nothing (the last
/// known key, any key we can't read, or a key taken twice, since advisory
/// locks stack) is confirmed with the server before the client is unpinned.
#[derive(Default, Debug)]
pub(crate) struct AdvisoryLocks {
    locks: FnvHashSet<i64>,
    /// A session lock with a key we couldn't read was taken.
    unknown: bool,
    /// A lock was released that may have been the last one: ask the server
    /// before unpinning.
    verify: bool,
}

impl AdvisoryLocks {
    /// Apply the lock calls of a statement: releases first, then
    /// acquisitions (a multi-statement query's calls come netted so).
    pub(crate) fn merge(&mut self, locks: &ParserAdvisoryLocks) {
        let held = self.locked();

        for lock in locks.iter().filter(|lock| lock.unlock) {
            if lock.all {
                // pg_advisory_unlock_all() releases every session lock.
                self.clear();
            } else {
                if let Some(id) = lock.id {
                    self.locks.remove(&id);
                }
                // Maybe the last lock, maybe one of several of the same key,
                // maybe a key we couldn't read: the server knows.
                if held {
                    self.verify = true;
                }
            }
        }

        for lock in locks
            .iter()
            .filter(|lock| !lock.unlock && lock.scope == LockScope::Session)
        {
            match lock.id {
                Some(id) => {
                    self.locks.insert(id);
                }
                None => self.unknown = true,
            }
        }
    }

    /// The client holds, or may hold, a session advisory lock.
    pub(crate) fn locked(&self) -> bool {
        !self.locks.is_empty() || self.unknown || self.verify
    }

    /// The server must be asked whether the client still holds a lock.
    pub(crate) fn needs_verification(&self) -> bool {
        self.verify
    }

    /// The server said how many session advisory locks the connection holds.
    pub(crate) fn verified(&mut self, held: i64) {
        if held == 0 {
            self.clear();
        } else {
            self.verify = false;
            if self.locks.is_empty() {
                // It holds locks we can't name.
                self.unknown = true;
            }
        }
    }

    pub(crate) fn clear(&mut self) {
        self.locks.clear();
        self.unknown = false;
        self.verify = false;
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, id: i64) -> bool {
        self.locks.contains(&id)
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.locks.len()
    }
}
