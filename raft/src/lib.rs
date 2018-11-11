use serde::{de::DeserializeOwned, Serialize};
use serde_derive::{Deserialize, Serialize};
use std::cmp::min;
use std::collections::BTreeMap;
use std::fmt::Debug;

pub trait StateMachine: Default {
    type Command: Serialize + DeserializeOwned + Clone + Debug;
    type Response: Serialize + DeserializeOwned + Clone + Debug;

    fn apply(&mut self, command: &Self::Command) -> Self::Response;
}

type ServerId = u16;
type Term = u32;
type LogIndex = u64;

/// The log. This is persisted by each server.
///
/// It contains the term of each entry in order to ensure consistency in the
/// case of leader failure with uncommitted logs.
///
/// Committed logs don't need this - we can optimize later.
type Log<Entry> = BTreeMap<LogIndex, LogEntry<Entry>>;

#[derive(Debug)]
struct LogEntry<Entry> {
    term: Term,
    entry: Entry,
}

/// The entire state of a Raft instance.
pub(crate) struct Raft<S: StateMachine> {
    /// The log of commands issued.
    log: Log<S::Command>,

    /// The current state, with all commands up to `last_applied` applied.
    state: S,

    /// The generic state of the Raft server.
    server: Server,
}

impl<S: StateMachine> Default for Raft<S> {
    fn default() -> Raft<S> {
        Raft {
            log: Default::default(),
            state: Default::default(),
            server: Default::default(),
        }
    }
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

    /// The index of the last log applied by us to our state.
    last_applied: LogIndex,

    /// If we are the leader, this contains our leader state.
    leader_state: Option<Leader>,
}

struct Leader;

/// AppendEntries RPC, invoked by leader to replicate log entries.
///
/// Also used as heartbeat (with `entries` empty.)
#[derive(Serialize, Deserialize, Clone, Debug)]
struct AppendEntries<Command> {
    term: Term,
    leader_id: ServerId,
    prev_log_index: Option<LogIndex>,
    prev_log_term: Term,
    entries: Vec<Command>,
    leader_commit: LogIndex,
}

impl Default for Server {
    fn default() -> Server {
        Server {
            current_term: 0,
            voted_for: None,
            last_commit: 0,
            last_applied: 0,
            leader_state: None,
        }
    }
}

impl Server {}

#[allow(dead_code)]
impl<S: StateMachine> Raft<S> {
    pub(crate) fn new() -> Raft<S> {
        Raft {
            log: Default::default(),
            state: Default::default(),
            server: Default::default(),
        }
    }

    /// See [`AppendEntries`].
    pub(crate) fn append_entries(&mut self, request: AppendEntries<S::Command>) -> bool {
        if request.term < self.server.current_term {
            return false;
        }

        if let Some(prev_log_index) = request.prev_log_index {
            if !self.has_entry(prev_log_index, request.prev_log_term) {
                return false;
            }
        }

        self.do_append(
            request.entries,
            request.prev_log_index.unwrap_or(0) + 1,
            request.term,
        );
        self.update_commit(request.leader_commit);

        true
    }

    fn has_entry(&self, log_index: LogIndex, log_term: Term) -> bool {
        match self.log.get(&log_index) {
            None => false,
            Some(LogEntry { term, .. }) => *term == log_term,
        }
    }

    fn do_append(&mut self, entries: Vec<S::Command>, start_index: LogIndex, term: Term) {
        // Remove everything that might conflict.
        self.log.split_off(&start_index);

        let mut index = start_index;
        for entry in entries.into_iter() {
            self.log.insert(index, LogEntry { term, entry });
            index += 1;
        }
    }

    fn update_commit(&mut self, leader_commit: LogIndex) {
        let srv = &mut self.server;
        if srv.last_commit >= leader_commit {
            return;
        }

        // Update last commit.
        let last_key = *self.log.keys().last().unwrap_or(&0);
        srv.last_commit = min(leader_commit, last_key);

        // Apply all entries up to and including the last commit.
        if srv.last_commit > srv.last_applied {
            use std::ops::Bound::{Excluded, Included};

            let apply_range = (Excluded(srv.last_applied), Included(srv.last_commit));
            for entry in self.log.range(apply_range) {
                self.state.apply(&(entry.1).entry);
            }

            srv.last_applied = srv.last_commit;
        }
    }
}

type Error = ();

#[allow(dead_code)]
impl<S: StateMachine> Raft<S> {
    fn handle_request(&mut self, _request: &S::Command) -> Result<S::Response, Error> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn valid_raft() -> Raft<TestService> {
        let mut raft: Raft<TestService> = Default::default();
        raft.server.current_term = START_TERM;
        raft
    }

    fn valid_heartbeat<C>() -> AppendEntries<C> {
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
        assert_eq!(true, valid_raft().append_entries(valid_heartbeat()));
    }

    #[test]
    fn append_entries_fails_with_unknown_log_index() {
        assert_eq!(
            false,
            valid_raft().append_entries(AppendEntries {
                prev_log_index: Some(5),
                ..valid_heartbeat()
            })
        );
    }

    #[test]
    fn append_entries_fails_with_old_term() {
        assert_eq!(
            false,
            valid_raft().append_entries(AppendEntries {
                term: 1,
                ..valid_heartbeat()
            })
        );
    }

    fn valid_append(c: Command) -> AppendEntries<Command> {
        AppendEntries {
            entries: vec![c],
            ..valid_heartbeat()
        }
    }

    fn try_append(request: AppendEntries<Command>, raft: &mut Raft<TestService>) -> bool {
        if !raft.append_entries(request.clone()) {
            println!("Request failed: {:#?}", request);
            return false;
        }
        true
    }

    #[test]
    fn append_entries_appends_to_log_with_valid_request() {
        let mut raft = valid_raft();
        assert_eq!(0, raft.log.len());
        assert_eq!(true, raft.append_entries(valid_append(Increment)));
        assert_eq!(1, raft.log.len());
    }

    #[test]
    fn append_entries_follows_leader_commit() {
        fn send_with_commit(
            leader_commit: LogIndex,
            request: AppendEntries<Command>,
            raft: &mut Raft<TestService>,
        ) {
            try_append(
                AppendEntries {
                    leader_commit,
                    prev_log_index: raft.log.keys().last().map(|x| *x),
                    ..request
                },
                raft,
            );
        }

        let raft = &mut valid_raft();

        send_with_commit(0, valid_append(Increment), raft);
        send_with_commit(0, valid_append(Increment), raft);

        // No entries have been committed yet.
        assert_eq!(0, raft.state.0);

        send_with_commit(1, valid_append(Increment), raft);
        assert_eq!(1, raft.state.0);
        send_with_commit(1, valid_heartbeat(), raft);
        assert_eq!(1, raft.state.0);
        send_with_commit(2, valid_heartbeat(), raft);
        assert_eq!(2, raft.state.0);
        send_with_commit(4, valid_append(Increment), raft);
        assert_eq!(4, raft.state.0);
    }

    #[test]
    fn append_entries_removes_conflicting_entries_from_old_leader() {
        let raft = &mut valid_raft();

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
        assert_eq!(5, raft.state.0);

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
        assert_eq!(5, raft.state.0);

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
        assert_eq!(12, raft.state.0);
    }

    #[test]
    fn append_entries_works_for_multiple_entries() {
        let raft = &mut valid_raft();

        try_append(
            AppendEntries {
                prev_log_index: None,
                leader_commit: 1,
                entries: vec![Increment, Increment, Increment],
                ..valid_heartbeat()
            },
            raft,
        );
        assert_eq!(1, raft.state.0);

        try_append(
            AppendEntries {
                prev_log_index: Some(3),
                leader_commit: 3,
                ..valid_heartbeat()
            },
            raft,
        );
        assert_eq!(3, raft.state.0);
    }
}
