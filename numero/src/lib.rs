use raft::{StateMachine};

struct NumberService {
    last: Option<u64>,
}

#[allow(dead_code)]
impl NumberService {
    fn new() -> NumberService {
        NumberService {
            last: None,
        }
    }

    fn next_number(&mut self) -> u64 {
        let next = match self.last {
            Some(last) => last + 1,
            None       => 1,
        };
        self.last = Some(next);
        next
    }
}

pub enum Command {
    GiveMeANumber,
    LastNumberIssued,
}

impl StateMachine for NumberService {
    type Command = Command;
    type Response = Option<u64>;

    fn apply(&mut self, command: &Self::Command) -> Self::Response {
        match command {
            Command::GiveMeANumber => Some(self.next_number()),
            Command::LastNumberIssued => self.last,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_number() {
        let mut svc = NumberService::new();
        assert_eq!(1, svc.next_number());
        assert_eq!(2, svc.next_number());
        assert_eq!(3, svc.next_number());
    }

    #[test]
    fn apply() {
        let mut svc = NumberService::new();
        assert_eq!(None, svc.apply(&Command::LastNumberIssued));
        assert_eq!(Some(1), svc.apply(&Command::GiveMeANumber));
        assert_eq!(Some(2), svc.apply(&Command::GiveMeANumber));
        assert_eq!(Some(3), svc.apply(&Command::GiveMeANumber));
        assert_eq!(Some(3), svc.apply(&Command::LastNumberIssued));
        assert_eq!(Some(4), svc.apply(&Command::GiveMeANumber));
    }
}
