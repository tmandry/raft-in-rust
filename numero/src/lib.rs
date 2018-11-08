use raft::StateMachine;
use serde_derive::{Deserialize, Serialize};
use std::cmp::{max, min};

struct NumberService {
    last: Option<u64>,
}

#[allow(dead_code)]
impl NumberService {
    const MAX_LOWER_BOUND: u64 = 1_000_000;

    fn new() -> NumberService {
        NumberService { last: None }
    }

    fn next_number(&mut self, lower_bound: Option<u64>) -> u64 {
        let lower_bound = min(lower_bound.unwrap_or(0), Self::MAX_LOWER_BOUND);
        let next = max(lower_bound, self.last.unwrap_or(0) + 1);
        self.last = Some(next);
        next
    }

    fn last_issued(&self) -> Option<u64> {
        self.last
    }
}

impl Default for NumberService {
    fn default() -> NumberService {
        Self::new()
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Command {
    GiveMeANumber(Option<u64>),
    LastNumberIssued,
}

impl StateMachine for NumberService {
    type Command = Command;
    type Response = Option<u64>;

    fn apply(&mut self, command: &Self::Command) -> Self::Response {
        match command {
            Command::GiveMeANumber(lower_bound) => Some(self.next_number(*lower_bound)),
            Command::LastNumberIssued => self.last_issued(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_number() {
        let mut svc = NumberService::new();
        assert_eq!(1, svc.next_number(None));
        assert_eq!(2, svc.next_number(None));
        assert_eq!(3, svc.next_number(None));
    }

    #[test]
    fn apply() {
        let mut svc = NumberService::new();
        assert_eq!(None, svc.apply(&Command::LastNumberIssued));
        assert_eq!(Some(1), svc.apply(&Command::GiveMeANumber(None)));
        assert_eq!(Some(2), svc.apply(&Command::GiveMeANumber(None)));
        assert_eq!(Some(3), svc.apply(&Command::GiveMeANumber(None)));
        assert_eq!(Some(3), svc.apply(&Command::LastNumberIssued));
        assert_eq!(Some(4), svc.apply(&Command::GiveMeANumber(None)));
        assert_eq!(Some(100), svc.apply(&Command::GiveMeANumber(Some(100))));
        assert_eq!(Some(100), svc.apply(&Command::LastNumberIssued));
    }
}
