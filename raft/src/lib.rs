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

#[allow(dead_code)]
pub(crate) struct Server<S: StateMachine> {
    /// The log of commands issued.
    log: Log<S::Command>,

    /// The current state, with all commands up to `last_applied` applied.
    state: S,

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

#[allow(dead_code)]
impl<S: StateMachine> Server<S> {
    pub(crate) fn new() -> Server<S> {
        Server {
            log: Default::default(),
            state: Default::default(),
            current_term: 0,
            voted_for: None,
            last_commit: 0,
            last_applied: 0,
            leader_state: None,
        }
    }

    /// See [`AppendEntries`].
    pub(crate) fn append_entries(&mut self, request: AppendEntries<S::Command>) -> bool {
        if request.term < self.current_term {
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
        if self.last_commit >= leader_commit {
            return;
        }

        // Update last commit.
        let last_key = *self.log.keys().last().unwrap_or(&0);
        self.last_commit = min(leader_commit, last_key);

        // Apply all entries up to and including the last commit.
        if self.last_commit > self.last_applied {
            use std::ops::Bound::{Excluded, Included};

            let apply_range = (Excluded(self.last_applied), Included(self.last_commit));
            for entry in self.log.range(apply_range) {
                self.state.apply(&(entry.1).entry);
            }

            self.last_applied = self.last_commit;
        }
    }
}

type Error = ();

#[allow(dead_code)]
impl<S: StateMachine> Server<S> {
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

    fn valid_server() -> Server<TestService> {
        let mut server = Server::<TestService>::new();
        server.current_term = START_TERM;
        server
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
        assert_eq!(true, valid_server().append_entries(valid_heartbeat()));
    }

    #[test]
    fn append_entries_fails_with_unknown_log_index() {
        assert_eq!(
            false,
            valid_server().append_entries(AppendEntries {
                prev_log_index: Some(5),
                ..valid_heartbeat()
            })
        );
    }

    #[test]
    fn append_entries_fails_with_old_term() {
        assert_eq!(
            false,
            valid_server().append_entries(AppendEntries {
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

    fn try_append(request: AppendEntries<Command>, server: &mut Server<TestService>) -> bool {
        if !server.append_entries(request.clone()) {
            println!("Request failed: {:#?}", request);
            return false;
        }
        true
    }

    #[test]
    fn append_entries_appends_to_log_with_valid_request() {
        let mut server = valid_server();
        assert_eq!(0, server.log.len());
        assert_eq!(true, server.append_entries(valid_append(Increment)));
        assert_eq!(1, server.log.len());
    }

    #[test]
    fn append_entries_follows_leader_commit() {
        fn send_with_commit(
            leader_commit: LogIndex,
            request: AppendEntries<Command>,
            server: &mut Server<TestService>,
        ) {
            try_append(
                AppendEntries {
                    leader_commit,
                    prev_log_index: server.log.keys().last().map(|x| *x),
                    ..request
                },
                server,
            );
        }

        let server = &mut valid_server();

        send_with_commit(0, valid_append(Increment), server);
        send_with_commit(0, valid_append(Increment), server);

        // No entries have been committed yet.
        assert_eq!(0, server.state.0);

        send_with_commit(1, valid_append(Increment), server);
        assert_eq!(1, server.state.0);
        send_with_commit(1, valid_heartbeat(), server);
        assert_eq!(1, server.state.0);
        send_with_commit(2, valid_heartbeat(), server);
        assert_eq!(2, server.state.0);
        send_with_commit(4, valid_append(Increment), server);
        assert_eq!(4, server.state.0);
    }

    #[test]
    fn append_entries_removes_conflicting_entries_from_old_leader() {
        let server = &mut valid_server();

        for i in 0..8 {
            try_append(
                AppendEntries {
                    term: START_TERM,
                    leader_commit: 0,
                    prev_log_index: if i > 0 { Some(i) } else { None },
                    ..valid_append(Increment)
                },
                server,
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
            server,
        );
        assert_eq!(5, server.state.0);

        // New leader comes up 2 terms later, but did not see the last two uncommitted
        // entries. It did see the first uncommitted entry 6, though (still uncommitted).
        try_append(
            AppendEntries {
                term: START_TERM + 2,
                leader_commit: 5,
                prev_log_index: Some(6),
                ..valid_heartbeat()
            },
            server,
        );
        assert_eq!(5, server.state.0);

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
            server,
        );
        assert_eq!(12, server.state.0);
    }

    #[test]
    fn append_entries_works_for_multiple_entries() {
        let server = &mut valid_server();

        try_append(
            AppendEntries {
                prev_log_index: None,
                leader_commit: 1,
                entries: vec![Increment, Increment, Increment],
                ..valid_heartbeat()
            },
            server,
        );
        assert_eq!(1, server.state.0);

        try_append(
            AppendEntries {
                prev_log_index: Some(3),
                leader_commit: 3,
                ..valid_heartbeat()
            },
            server,
        );
        assert_eq!(3, server.state.0);
    }
}
