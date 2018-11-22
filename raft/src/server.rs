use crate::election::RpcState;
use crate::{Peer, ServerId, StateMachine};
use crate::storage::MemoryStorage;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex};

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

pub struct RaftServer<S: StateMachine + Send + Sync> {
    pub rpc: RpcState,
    pub peer: Arc<Mutex<Peer>>,
    pub storage: Arc<Mutex<MemoryStorage<S>>>,
}

impl<S: StateMachine + 'static> RaftServer<S> {
    pub fn new(config: Config) -> Self {
        let storage = Arc::new(Mutex::new(MemoryStorage::default()));
        let peer = Arc::new(Mutex::new(Peer::new(storage.clone())));
        RaftServer {
            rpc: RpcState::new(config, peer.clone()),
            peer,
            storage,
        }
    }
}
