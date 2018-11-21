use crate::{LogIndex, StateMachine, Term};
use log::error;
use serde_json;
use std::collections::BTreeMap;

pub(crate) trait Storage {
    /// Returns whether an entry with index `log_index` and term `log_term`
    /// exists in the storage.
    fn has_entry(&self, log_index: LogIndex, log_term: Term) -> bool;

    /// Returns the term of the last entry in the log (not necessarily applied).
    fn last_log_term(&self) -> Term;

    /// Returns the index of the last entry in the log (not necessarily applied).
    fn last_log_index(&self) -> LogIndex;

    /// Returns the index of the last applied entry.
    fn last_applied_index(&self) -> LogIndex;

    /// Returns the number of entries in the log.
    fn len(&self) -> usize;

    /// Appends to the log starting at index `start_index`.
    ///
    /// We should already have all logs up to `start_index - 1`.
    ///
    /// If `start_index` or further entries already exist, erase them and
    /// replace with the supplied entries. This cannot be used to erase entries
    /// that are already applied.
    fn append(&mut self, entries: Vec<Vec<u8>>, start_index: LogIndex, term: Term);

    /// Applies all entries up to and including `last_commit`.
    ///
    /// Entries cannot be rolled back after being applied.
    fn apply_up_to(&mut self, last_commit: LogIndex);
}

#[derive(Debug)]
struct LogEntry<Entry> {
    term: Term,
    entry: Entry,
}

/// The log. This is persisted by each server.
///
/// It contains the term of each entry in order to ensure consistency in the
/// case of leader failure with uncommitted logs.
///
/// Committed logs don't need this - we can optimize later.
type Log<Entry> = BTreeMap<LogIndex, LogEntry<Entry>>;

pub struct MemoryStorage<S: StateMachine> {
    /// The log of commands issued.
    log: Log<Vec<u8>>,

    /// The current state, with all commands up to `last_applied` applied.
    pub(crate) state: S,

    /// The index of the last log applied by us to our state.
    last_applied: LogIndex,
}

impl<S: StateMachine> Default for MemoryStorage<S> {
    fn default() -> Self {
        MemoryStorage {
            log: Default::default(),
            state: Default::default(),
            last_applied: 0,
        }
    }
}

impl<S: StateMachine> Storage for MemoryStorage<S> {
    fn has_entry(&self, log_index: LogIndex, log_term: Term) -> bool {
        match self.log.get(&log_index) {
            None => false,
            Some(LogEntry { term, .. }) => *term == log_term,
        }
    }

    fn last_log_term(&self) -> Term {
        self.log.values().next_back().map(|x| x.term).unwrap_or(0)
    }

    fn last_log_index(&self) -> LogIndex {
        *self.log.keys().next_back().unwrap_or(&0)
    }

    fn last_applied_index(&self) -> LogIndex {
        self.last_applied
    }

    fn len(&self) -> usize {
        self.log.len()
    }

    fn append(&mut self, entries: Vec<Vec<u8>>, start_index: LogIndex, term: Term) {
        if start_index > 1 && self.last_log_index() < (start_index - 1) {
            panic!(
                "MemoryStorage::append called with start_index {} and last_log_index {}",
                start_index,
                self.last_log_index()
            );
        }

        // Remove everything that might conflict.
        self.log.split_off(&start_index);

        let mut index = start_index;
        for entry in entries.into_iter() {
            self.log.insert(index, LogEntry { term, entry });
            index += 1;
        }
    }

    fn apply_up_to(&mut self, last_commit: LogIndex) {
        use std::ops::Bound::{Excluded, Included};

        if last_commit <= self.last_applied {
            return;
        }

        let apply_range = (Excluded(self.last_applied), Included(last_commit));
        for entry in self.log.range(apply_range) {
            let log_entry = &(entry.1).entry;
            match do_apply(&mut self.state, &log_entry) {
                Ok(_) => {}
                Err(e) => {
                    error!("could not interpret command bytes: {:?}", log_entry);
                    panic!("could not interpret command: {:?}", e);
                }
            }
        }

        self.last_applied = last_commit;
    }
}

fn do_apply<S: StateMachine>(state: &mut S, entry: &[u8]) -> Result<(), serde_json::Error> {
    // FIXME: Currently we assume all commands and responses are assumed to
    // be encoded in JSON, eventually a Protocol trait is needed.
    let command: S::Command = serde_json::from_slice(&entry)?;
    state.apply(&command);
    Ok(())
}

#[allow(unused)]
fn do_apply_with_response<S: StateMachine>(
    state: &mut S,
    entry: &[u8],
) -> Result<Vec<u8>, serde_json::Error> {
    let command: S::Command = serde_json::from_slice(&entry)?;
    let response = state.apply(&command);
    Ok(serde_json::to_vec(&response)?)
}
