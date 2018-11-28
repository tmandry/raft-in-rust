pub(crate) mod protos;
pub mod server;
pub mod storage;

use crate::storage::Storage;
use serde::{de::DeserializeOwned, Serialize};
use std::cmp::min;
use std::fmt::Debug;
use std::sync::{Arc, RwLock};

pub trait StateMachine: Default + Send + Sync {
    type Command: Serialize + DeserializeOwned + Clone + Debug;
    type Response: Serialize + DeserializeOwned + Clone + Debug;

    fn apply(&mut self, command: &Self::Command) -> Self::Response;
}

type ServerId = i32;
type Term = i32;
type LogIndex = i64;

/// The Peer state that is common to all servers in the cluster.
pub struct Peer {
    /// What we think the current term is. Never goes down.
    current_term: Term,

    /// Who we voted for in the last election.
    voted_for: Option<ServerId>,

    /// The index of the last known committed log.
    last_commit: LogIndex,

    /// Storage of our state and log entries.
    // TODO adopt more fine-grained concurrency
    storage: Arc<RwLock<dyn Storage + Send + Sync>>,
}

/// Invoked by leader to replicate log entries.
///
/// Also used as heartbeat (with `entries` empty.)
#[derive(Clone, Debug)]
pub(crate) struct AppendEntries {
    term: Term,
    leader_id: ServerId,
    prev_log_index: Option<LogIndex>,
    prev_log_term: Term,
    entries: Vec<Vec<u8>>,
    leader_commit: LogIndex,
}

/// Reasons why append_entries can fail.
#[derive(Debug, PartialEq)]
pub(crate) enum AppendEntriesError {
    BadTerm,
    NeedBackfill,
}

/// Invoked by candidates seeking election.
#[derive(Debug)]
pub(crate) struct VoteRequest {
    term: Term,
    candidate_id: ServerId,
    last_log_index: LogIndex,
    last_log_term: Term,
}

impl Peer {
    pub(crate) fn new(storage: Arc<RwLock<dyn Storage + Send + Sync>>) -> Peer {
        Peer {
            current_term: 0,
            voted_for: None,
            last_commit: 0,
            storage,
        }
    }

    /// Attempt to append the supplied entries to the log (see
    /// [`AppendEntries`].) Returns true on success.
    pub(crate) fn append_entries(
        &mut self,
        request: AppendEntries,
    ) -> Result<(), AppendEntriesError> {
        if request.term < self.current_term {
            return Err(AppendEntriesError::BadTerm);
        }
        self.saw_term(request.term);

        {
            let mut storage = self.storage.write().unwrap();
            if let Some(prev_log_index) = request.prev_log_index {
                if !storage.has_entry(prev_log_index, request.prev_log_term) {
                    return Err(AppendEntriesError::NeedBackfill);
                }
            }

            storage.append(
                request.entries,
                request.prev_log_index.unwrap_or(0) + 1,
                request.term,
            );
        }

        self.update_commit(request.leader_commit);
        Ok(())
    }

    /// Process a vote request. Returns the whether or not the vote was granted.
    /// Also returns the current term.
    pub(crate) fn request_vote(&mut self, req: &VoteRequest) -> (bool, Term) {
        let current_term = self.current_term;

        if req.term < current_term {
            return (false, current_term);
        }
        self.saw_term(req.term);

        let last_log_term = self.storage.read().unwrap().last_log_term();
        if self.voted_for.is_some()
            || req.last_log_term < last_log_term
            || req.last_log_index < self.last_commit
        {
            return (false, current_term);
        }

        // TODO persist on stable storage before responding
        self.voted_for = Some(req.candidate_id);

        return (true, current_term);
    }

    fn update_commit(&mut self, leader_commit: LogIndex) {
        if self.last_commit >= leader_commit {
            return;
        }

        let mut storage = self.storage.write().unwrap();

        // Update last commit.
        self.last_commit = min(leader_commit, storage.last_log_index());

        // Apply all entries up to and including the last commit.
        storage.apply_up_to(self.last_commit);
    }

    fn saw_term(&mut self, term: Term) {
        if term > self.current_term {
            // TODO convert to follower
            // TODO save current_term to persistent storage
            self.current_term = term;
            self.voted_for = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryStorage;
    use serde_derive::{Deserialize, Serialize};
    use serde_json;
    use std::sync::Arc;

    struct TestService(i64);

    impl Default for TestService {
        fn default() -> TestService {
            TestService(0)
        }
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    enum Command {
        Increment,
        Double,
    }
    use self::Command::{Double, Increment};

    impl StateMachine for TestService {
        type Command = Command;
        type Response = i64;

        fn apply(&mut self, command: &Self::Command) -> Self::Response {
            match command {
                Command::Increment => {
                    self.0 += 1;
                    self.0
                }
                Command::Double => {
                    self.0 *= 2;
                    self.0
                }
            }
        }
    }

    const START_TERM: Term = 2;

    fn valid_peer() -> (Peer, Arc<RwLock<MemoryStorage<TestService>>>) {
        let storage = Arc::new(RwLock::new(MemoryStorage::<TestService>::default()));
        let mut peer = Peer::new(storage.clone());
        peer.current_term = START_TERM;
        (peer, storage)
    }

    fn valid_heartbeat() -> AppendEntries {
        AppendEntries {
            term: START_TERM,
            leader_id: 1,
            prev_log_index: None,
            prev_log_term: START_TERM,
            entries: vec![],
            leader_commit: 0,
        }
    }

    #[test]
    fn append_entries_succeeds_with_valid_request() {
        let (mut peer, _) = valid_peer();
        assert!(peer.append_entries(valid_heartbeat()).is_ok());
    }

    #[test]
    fn append_entries_fails_with_unknown_log_index() {
        let (mut peer, _) = valid_peer();
        assert_eq!(
            Err(AppendEntriesError::NeedBackfill),
            peer.append_entries(AppendEntries {
                prev_log_index: Some(5),
                ..valid_heartbeat()
            })
        );
    }

    #[test]
    fn append_entries_fails_with_old_term() {
        let (mut peer, _) = valid_peer();
        assert_eq!(
            Err(AppendEntriesError::BadTerm),
            peer.append_entries(AppendEntries {
                term: 1,
                ..valid_heartbeat()
            })
        );
    }

    fn valid_append(c: Command) -> AppendEntries {
        AppendEntries {
            entries: entries(vec![c]),
            ..valid_heartbeat()
        }
    }

    fn entries(cs: Vec<Command>) -> Vec<Vec<u8>> {
        cs.into_iter()
            .map(|c| serde_json::to_vec(&c).expect("could not serialize"))
            .collect()
    }

    fn try_append(request: AppendEntries, peer: &mut Peer) -> bool {
        if !peer.append_entries(request.clone()).is_ok() {
            println!("Request failed: {:#?}", request);
            return false;
        }
        true
    }

    #[test]
    fn append_entries_appends_to_log_with_valid_request() {
        let (mut peer, storage) = valid_peer();
        assert_eq!(0, storage.read().unwrap().len());
        assert!(peer.append_entries(valid_append(Increment)).is_ok());
        assert_eq!(1, storage.read().unwrap().len());
    }

    #[test]
    fn append_entries_follows_leader_commit() {
        fn send_with_commit(
            leader_commit: LogIndex,
            request: AppendEntries,
            peer: &mut Peer,
            storage: &mut Arc<RwLock<MemoryStorage<TestService>>>,
        ) {
            let last_log_index = storage.read().unwrap().last_log_index();
            let last_log_index = match last_log_index {
                0 => None,
                _ => Some(last_log_index),
            };
            try_append(
                AppendEntries {
                    leader_commit,
                    prev_log_index: last_log_index,
                    ..request
                },
                peer,
            );
        }

        let (ref mut peer, ref mut storage) = valid_peer();

        send_with_commit(0, valid_append(Increment), peer, storage);
        send_with_commit(0, valid_append(Increment), peer, storage);

        // No entries have been committed yet.
        assert_eq!(0, storage.read().unwrap().state.0);

        send_with_commit(1, valid_append(Increment), peer, storage);
        assert_eq!(1, storage.read().unwrap().state.0);
        send_with_commit(1, valid_heartbeat(), peer, storage);
        assert_eq!(1, storage.read().unwrap().state.0);
        send_with_commit(2, valid_heartbeat(), peer, storage);
        assert_eq!(2, storage.read().unwrap().state.0);
        send_with_commit(4, valid_append(Increment), peer, storage);
        assert_eq!(4, storage.read().unwrap().state.0);
    }

    #[test]
    fn append_entries_removes_conflicting_entries_from_old_leader() {
        let (ref mut peer, ref mut storage) = valid_peer();

        for i in 0..8 {
            try_append(
                AppendEntries {
                    term: START_TERM,
                    leader_commit: 0,
                    prev_log_index: if i > 0 { Some(i) } else { None },
                    ..valid_append(Increment)
                },
                peer,
            );
        }

        // First leader commits the first 5 entries.
        try_append(
            AppendEntries {
                term: START_TERM,
                leader_commit: 5,
                prev_log_index: Some(8),
                ..valid_heartbeat()
            },
            peer,
        );
        assert_eq!(5, storage.read().unwrap().state.0);

        // New leader comes up 2 terms later, but did not see the last two uncommitted
        // entries. It did see the first uncommitted entry 6, though (still uncommitted).
        try_append(
            AppendEntries {
                term: START_TERM + 2,
                leader_commit: 5,
                prev_log_index: Some(6),
                ..valid_heartbeat()
            },
            peer,
        );
        assert_eq!(5, storage.read().unwrap().state.0);

        // New leader tells us about a new committed entry. At this point we can finally commit
        // entry 6 from the previous term, then apply entry 7 (a Double operation). This leaves us
        // with a state of 12.
        try_append(
            AppendEntries {
                term: START_TERM + 2,
                leader_commit: 7,
                prev_log_index: Some(6),
                ..valid_append(Double)
            },
            peer,
        );
        assert_eq!(12, storage.read().unwrap().state.0);
    }

    #[test]
    fn append_entries_works_for_multiple_entries() {
        let (ref mut peer, ref mut storage) = valid_peer();

        try_append(
            AppendEntries {
                prev_log_index: None,
                leader_commit: 1,
                entries: entries(vec![Increment, Increment, Increment]),
                ..valid_heartbeat()
            },
            peer,
        );
        assert_eq!(1, storage.read().unwrap().state.0);

        try_append(
            AppendEntries {
                prev_log_index: Some(3),
                leader_commit: 3,
                ..valid_heartbeat()
            },
            peer,
        );
        assert_eq!(3, storage.read().unwrap().state.0);
    }
}
