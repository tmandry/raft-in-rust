pub mod election;
pub(crate) mod protos;
pub mod server;
pub mod storage;

use crate::storage::Storage;
use serde::{de::DeserializeOwned, Serialize};
use serde_derive::{Deserialize, Serialize};
use std::cmp::min;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::rc::Rc;
use std::cell::RefCell;

pub trait StateMachine: Default {
    type Command: Serialize + DeserializeOwned + Clone + Debug;
    type Response: Serialize + DeserializeOwned + Clone + Debug;

    fn apply(&mut self, command: &Self::Command) -> Self::Response;
}

type ServerId = i32;
type Term = i32;
type LogIndex = i64;

/// The entire state of a Raft instance.
pub struct Raft {
    /// The generic state of the Raft server.
    server: Arc<Mutex<Server>>,

    storage: Rc<RefCell<dyn Storage>>,
}

/// The generic Raft server state, i.e. everything that does not involve
/// the StateMachine directly.
#[allow(dead_code)]
pub(crate) struct Server {
    /// What we think the current term is. Never goes down.
    current_term: Term,

    /// Who we voted for in the last election.
    voted_for: Option<ServerId>,

    /// The index of the last known committed log.
    last_commit: LogIndex,

    /// If we are the leader, this contains our leader state.
    leader_state: Option<Leader>,
}

impl Default for Server {
    fn default() -> Server {
        Server {
            current_term: 0,
            voted_for: None,
            last_commit: 0,
            leader_state: None,
        }
    }
}

impl Server {
    fn saw_term(&mut self, term: Term) {
        if term > self.current_term {
            // TODO convert to follower
            // TODO save current_term to persistent storage
            self.current_term = term;
            self.voted_for = None;
        }
    }
}

struct Leader;

/// AppendEntries RPC, invoked by leader to replicate log entries.
///
/// Also used as heartbeat (with `entries` empty.)
#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct AppendEntries {
    term: Term,
    leader_id: ServerId,
    prev_log_index: Option<LogIndex>,
    prev_log_term: Term,
    entries: Vec<Vec<u8>>,
    leader_commit: LogIndex,
}

pub(crate) struct VoteRequest {
    term: Term,
    candidate_id: ServerId,
    last_log_index: LogIndex,
    last_log_term: Term,
}

#[allow(dead_code)]
impl Raft {
    pub(crate) fn new(storage: Rc<RefCell<dyn Storage>>) -> Raft {
        Raft {
            server: Default::default(),
            storage
        }
    }

    /// See [`AppendEntries`].
    pub(crate) fn append_entries(&mut self, request: AppendEntries) -> bool {
        if request.term < self.server.lock().unwrap().current_term {
            return false;
        }
        self.server.lock().unwrap().saw_term(request.term);

        if let Some(prev_log_index) = request.prev_log_index {
            if !self
                .storage
                .borrow()
                .has_entry(prev_log_index, request.prev_log_term)
            {
                return false;
            }
        }

        self.storage.borrow_mut().append(
            request.entries,
            request.prev_log_index.unwrap_or(0) + 1,
            request.term,
        );
        self.update_commit(request.leader_commit);

        true
    }

    pub(crate) fn request_vote(&mut self, req: &VoteRequest) -> (Term, bool) {
        let mut server = self.server.lock().unwrap();
        let current_term = server.current_term;

        if req.term < current_term {
            return (current_term, false);
        }
        server.saw_term(req.term);

        if server.voted_for.is_some()
            || req.last_log_term < self.storage.borrow().last_log_term()
            || req.last_log_index < server.last_commit
        {
            return (current_term, false);
        }

        // TODO persist on stable storage before responding
        server.voted_for = Some(req.candidate_id);

        return (current_term, true);
    }

    fn update_commit(&mut self, leader_commit: LogIndex) {
        let srv = &mut self.server.lock().unwrap();
        if srv.last_commit >= leader_commit {
            return;
        }

        let mut storage = self.storage.borrow_mut();

        // Update last commit.
        srv.last_commit = min(leader_commit, storage.last_log_index());

        // Apply all entries up to and including the last commit.
        storage.apply_up_to(srv.last_commit);
    }
}

/*
type Error = ();

#[allow(dead_code)]
impl Raft {
    fn handle_request(&mut self, _request: &S::Command) -> Result<S::Response, Error> {
        unimplemented!()
    }
}
*/

#[cfg(test)]
mod tests {
    use super::*;
    use std::rc::Rc;
    use crate::storage::MemoryStorage;
    use serde_json;

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

    fn valid_raft() -> (Raft, Rc<RefCell<MemoryStorage<TestService>>>) {
        let storage = Rc::new(RefCell::new(MemoryStorage::<TestService>::default()));
        let raft = Raft::new(storage.clone());
        raft.server.lock().unwrap().current_term = START_TERM;
        (raft, storage)
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
        let (mut raft, _) = valid_raft();
        assert_eq!(true, raft.append_entries(valid_heartbeat()));
    }

    #[test]
    fn append_entries_fails_with_unknown_log_index() {
        let (mut raft, _) = valid_raft();
        assert_eq!(
            false,
            raft.append_entries(AppendEntries {
                prev_log_index: Some(5),
                ..valid_heartbeat()
            })
        );
    }

    #[test]
    fn append_entries_fails_with_old_term() {
        let (mut raft, _) = valid_raft();
        assert_eq!(
            false,
            raft.append_entries(AppendEntries {
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
        cs.into_iter().map(|c| serde_json::to_vec(&c).expect("could not serialize")).collect()
    }

    fn try_append(request: AppendEntries, raft: &mut Raft) -> bool {
        if !raft.append_entries(request.clone()) {
            println!("Request failed: {:#?}", request);
            return false;
        }
        true
    }

    #[test]
    fn append_entries_appends_to_log_with_valid_request() {
        let (mut raft, storage) = valid_raft();
        assert_eq!(0, storage.borrow().len());
        assert_eq!(true, raft.append_entries(valid_append(Increment)));
        assert_eq!(1, storage.borrow().len());
    }

    #[test]
    fn append_entries_follows_leader_commit() {
        fn send_with_commit(
            leader_commit: LogIndex,
            request: AppendEntries,
            raft: &mut Raft,
            storage: &mut Rc<RefCell<MemoryStorage<TestService>>>,
        ) {
            let last_log_index = storage.borrow().last_log_index();
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
                raft,
            );
        }

        let (ref mut raft, ref mut storage) = valid_raft();

        send_with_commit(0, valid_append(Increment), raft, storage);
        send_with_commit(0, valid_append(Increment), raft, storage);

        // No entries have been committed yet.
        assert_eq!(0, storage.borrow().state.0);

        send_with_commit(1, valid_append(Increment), raft, storage);
        assert_eq!(1, storage.borrow().state.0);
        send_with_commit(1, valid_heartbeat(), raft, storage);
        assert_eq!(1, storage.borrow().state.0);
        send_with_commit(2, valid_heartbeat(), raft, storage);
        assert_eq!(2, storage.borrow().state.0);
        send_with_commit(4, valid_append(Increment), raft, storage);
        assert_eq!(4, storage.borrow().state.0);
    }

    #[test]
    fn append_entries_removes_conflicting_entries_from_old_leader() {
        let (ref mut raft, ref mut storage) = valid_raft();

        for i in 0..8 {
            try_append(
                AppendEntries {
                    term: START_TERM,
                    leader_commit: 0,
                    prev_log_index: if i > 0 { Some(i) } else { None },
                    ..valid_append(Increment)
                },
                raft,
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
            raft,
        );
        assert_eq!(5, storage.borrow().state.0);

        // New leader comes up 2 terms later, but did not see the last two uncommitted
        // entries. It did see the first uncommitted entry 6, though (still uncommitted).
        try_append(
            AppendEntries {
                term: START_TERM + 2,
                leader_commit: 5,
                prev_log_index: Some(6),
                ..valid_heartbeat()
            },
            raft,
        );
        assert_eq!(5, storage.borrow().state.0);

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
            raft,
        );
        assert_eq!(12, storage.borrow().state.0);
    }

    #[test]
    fn append_entries_works_for_multiple_entries() {
        let (ref mut raft, ref mut storage) = valid_raft();

        try_append(
            AppendEntries {
                prev_log_index: None,
                leader_commit: 1,
                entries: entries(vec![Increment, Increment, Increment]),
                ..valid_heartbeat()
            },
            raft,
        );
        assert_eq!(1, storage.borrow().state.0);

        try_append(
            AppendEntries {
                prev_log_index: Some(3),
                leader_commit: 3,
                ..valid_heartbeat()
            },
            raft,
        );
        assert_eq!(3, storage.borrow().state.0);
    }
}
