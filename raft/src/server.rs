use crate::election::RpcState;
use crate::{Raft, ServerId, StateMachine};
use crate::storage::MemoryStorage;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::rc::Rc;
use std::cell::RefCell;

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
    pub raft: Raft,
    pub storage: Rc<RefCell<MemoryStorage<S>>>,
}

impl<S: StateMachine + 'static> RaftServer<S> {
    pub fn new(config: Config) -> Self {
        let storage = Rc::new(RefCell::new(MemoryStorage::default()));
        let raft: Raft = Raft::new(storage.clone());
        RaftServer {
            rpc: RpcState::new(config, raft.server.clone()),
            raft,
            storage,
        }
    }
}
