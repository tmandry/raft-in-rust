use raft::server::{Config, RaftServer};
use std::env;
use std::fs::File;
use std::thread::sleep;
use std::time::Duration;

mod sm {
    use raft::StateMachine;
    use serde_derive::{Deserialize, Serialize};

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
                    self.0
                }
                Command::Double => {
                    self.0 *= 2;
                    self.0
                }
            }
        }
    }
}

fn main() -> std::io::Result<()> {
    let id = env::args()
        .nth(1)
        .expect("please supply server id in args")
        .parse::<i32>()
        .expect("server id must be an integer");
    let config = Config::new(File::open("servers.txt")?, id);
    let mut server: RaftServer<sm::TestService> = RaftServer::new(config);

    sleep(Duration::from_millis((id as u64 + 2) * 100));
    server.rpc.timeout(&mut server.raft);
    Ok(())
}
