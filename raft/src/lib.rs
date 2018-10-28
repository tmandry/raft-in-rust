use serde::{de::DeserializeOwned, Serialize};
use serde_derive::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub trait StateMachine: Default {
    type Command: Serialize + DeserializeOwned;
    type Response: Serialize + DeserializeOwned;

    fn apply(&mut self, command: &Self::Command) -> Self::Response;
}

type CandidateId = u16;
type Term = u32;
type LogIndex = u64;

/// The log. This is persisted by each server.
///
/// It contains the term of each entry in order to ensure consistency in the
/// case of leader failure with uncommitted logs.
///
/// Committed logs don't need this - we can optimize later.
type Log<Entry> = BTreeMap<LogIndex, (Term, Entry)>;

#[allow(dead_code)]
pub(crate) struct Server<S: StateMachine> {
    /// The log of commands issued.
    log: Log<S::Command>,

    /// The current state, with all commands up to `last_applied` applied.
    state: S,

    /// What we think the current term is. Never goes down.
    current_term: Term,

    /// Who we voted for in the last election.
    voted_for: Option<CandidateId>,

    /// The index of the last known committed log.
    last_commit: LogIndex,

    /// The index of the last log applied by us to our state.
    last_applied: LogIndex,

    /// If we are the leader, this contains our leader state.
    leader_state: Option<Leader>,
}

struct Leader;

#[derive(Serialize, Deserialize)]
struct AppendEntries {}

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

    pub(crate) fn append_entries(&mut self, _request: &AppendEntries) -> bool {
        false
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

    #[derive(Serialize, Deserialize)]
    enum Command {
        Increment,
    }

    impl StateMachine for TestService {
        type Command = Command;
        type Response = bool;

        fn apply(&mut self, command: &Self::Command) -> Self::Response {
            match command {
                Command::Increment => {
                    self.0 += 1;
                    true
                }
            }
        }
    }

    #[test]
    fn it_works() {
        let mut server = Server::<TestService>::new();
        assert_eq!(false, server.append_entries(&AppendEntries {}));
    }
}
