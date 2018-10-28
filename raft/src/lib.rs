#![allow(dead_code)]

use serde::{de::DeserializeOwned, Serialize};

pub trait StateMachine {
    type Command: Serialize + DeserializeOwned;
    type Response: Serialize + DeserializeOwned;

    fn apply(&mut self, command: &Self::Command) -> Self::Response;
}

struct Server<S: StateMachine> {
    log: Vec<S::Command>,
}

trait Apply<T> {
    type Result;
    fn apply(&mut self, command: &T) -> Self::Result;
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
