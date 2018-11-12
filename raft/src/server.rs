use crate::election::RpcState;
use crate::{Raft, ServerId, StateMachine};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

#[derive(Clone, Debug)]
pub struct Config {
    pub id: ServerId,
    pub endpoints: BTreeMap<ServerId, String>,
}

impl Config {
    pub fn new(servers: File, id: ServerId) -> Config {
        let mut conf = Config {
            id,
            endpoints: Default::default(),
        };
        let reader = BufReader::new(servers);
        for (index, endpoint) in reader.lines().enumerate() {
            conf.endpoints
                .insert(index as i32, endpoint.expect("error while reading config"));
        }
        conf
    }
}

// FIXME: Find a name that's less confusing

pub struct RaftServer<S: StateMachine> {
    pub rpc: RpcState,
    pub raft: Raft<S>,
}

impl<S: StateMachine> RaftServer<S> {
    pub fn new(config: Config) -> Self {
        let raft: Raft<S> = Default::default();
        RaftServer {
            rpc: RpcState::new(config, raft.server.clone()),
            raft,
        }
    }
}
