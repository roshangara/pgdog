//! Cursors `WITH HOLD` the client has open.
//!
//! Such a cursor outlives the transaction that declared it, on that server
//! connection only: while one is open the client stays pinned to it.

use fnv::FnvHashMap;

#[derive(Debug, Default)]
pub(super) struct HoldCursors {
    /// Name -> declared by a committed transaction.
    cursors: FnvHashMap<String, bool>,
}

impl HoldCursors {
    /// `DECLARE <name> CURSOR WITH HOLD` ran.
    pub(super) fn declare(&mut self, name: &str, in_transaction: bool) {
        self.cursors.insert(name.to_owned(), !in_transaction);
    }

    /// `CLOSE <name>`, or `CLOSE ALL` (`None`), ran.
    pub(super) fn close(&mut self, name: Option<&str>) {
        match name {
            Some(name) => {
                self.cursors.remove(name);
            }
            None => self.cursors.clear(),
        }
    }

    /// A rolled-back transaction takes the cursors it declared with it.
    pub(super) fn finish_transaction(&mut self, rollback: bool) {
        if rollback {
            self.cursors.retain(|_, committed| *committed);
        } else {
            self.cursors
                .values_mut()
                .for_each(|committed| *committed = true);
        }
    }

    pub(super) fn clear(&mut self) {
        self.cursors.clear();
    }

    pub(super) fn is_empty(&self) -> bool {
        self.cursors.is_empty()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn rollback_drops_uncommitted_cursors() {
        let mut cursors = HoldCursors::default();
        cursors.declare("a", false);
        cursors.declare("b", true);
        cursors.finish_transaction(true);
        assert!(!cursors.is_empty());
        cursors.close(Some("a"));
        assert!(cursors.is_empty());

        cursors.declare("c", true);
        cursors.finish_transaction(false);
        cursors.finish_transaction(true);
        assert!(!cursors.is_empty());
        cursors.close(None);
        assert!(cursors.is_empty());
    }
}
