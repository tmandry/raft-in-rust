#![allow(dead_code)]

use raft::{StateMachine};

struct NumberService {
    last: Option<u64>,
}

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

enum Command {
    GiveMeANumber,
}

impl StateMachine for NumberService {
    type Command = Command;
    type Response = u64;

    fn apply(&mut self, command: &Self::Command) -> Self::Response {
        match command {
            Command::GiveMeANumber => self.next_number()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut svc = NumberService::new();
        assert_eq!(1, svc.next_number());
        assert_eq!(2, svc.next_number());
        assert_eq!(3, svc.next_number());
    }
}
