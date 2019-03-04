//use futures_new::prelude::*;
use raft::storage::MemoryStorage;
use raft::{BasicServerBuilder, Config, TarpcRaftServer as MyRaftServer};

use log::*;
use serde_json;
use std::env;
use std::fs::File;
use std::io;
use std::str;

mod sm {
    use log::*;
    use raft::StateMachine;
    use serde::{Deserialize, Serialize};

    #[derive(Debug)]
    pub struct TestService(i64);

    impl Default for TestService {
        fn default() -> TestService {
            TestService(0)
        }
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub enum Command {
        Increment,
        Double,
    }

    impl StateMachine for TestService {
        type Command = Command;
        type Response = i64;

        fn apply(&mut self, command: &Self::Command) -> Self::Response {
            match command {
                Command::Increment => {
                    self.0 += 1;
                }
                Command::Double => {
                    self.0 *= 2;
                }
            }
            debug!("state = {}", self.0);
            self.0
        }
    }
}

fn main() -> std::io::Result<()> {
    env_logger::Builder::from_default_env()
        .default_format_timestamp_nanos(true)
        .init();

    let id = env::args()
        .nth(1)
        .expect("please supply server id in args")
        .parse::<i32>()
        .expect("server id must be an integer");
    run::<MyRaftServer>(id)
}

fn run<Server: BasicServerBuilder>(id: i32) -> std::io::Result<()> {
    let config = Config::new(File::open("servers.txt")?, id);
    let storage = MemoryStorage::<sm::TestService>::new();
    let server = Server::new(storage, config);

    loop {
        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(_) => {
                let mut server = server.lock().unwrap();
                let command = serde_json::to_vec(&sm::Command::Increment).unwrap();
                server.apply_then(
                    command,
                    Box::new(|result| {
                        let result = result.map(|reply| str::from_utf8(&reply).unwrap().to_owned());
                        info!("Result after apply: {:?}", result);
                    }),
                );
            }
            Err(e) => {
                warn!("Error while reading from stdin: {:?}", e);
            }
        }
    }
}
