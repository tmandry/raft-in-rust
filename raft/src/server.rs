use crate::{ApplyError, ServerId, Storage};

use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex, RwLock};

pub type Endpoints = BTreeMap<ServerId, String>;

#[derive(Clone, Debug)]
pub struct Config {
    pub id: ServerId,
    pub endpoints: Endpoints,
    pub min_timeout_ms: i64,
    pub max_timeout_ms: i64,
    pub heartbeat_frequency_ms: i64,
}

impl Config {
    pub fn new(servers: File, id: ServerId) -> Config {
        let force_timeout = env::var("RAFT_FORCE_TIMEOUT").map(|x| x.parse::<i64>().unwrap());
        let mut conf = Config {
            id,
            endpoints: Default::default(),
            // TODO read from config file
            min_timeout_ms: force_timeout.clone().unwrap_or(600),
            max_timeout_ms: force_timeout.clone().unwrap_or(800),
            heartbeat_frequency_ms: 500,
        };
        let reader = BufReader::new(servers);
        for (index, endpoint) in reader.lines().enumerate() {
            conf.endpoints
                .insert(index as i32, endpoint.expect("error while reading config"));
        }
        conf
    }
}

pub trait BasicServerBuilder {
    fn new(
        storage: Arc<RwLock<dyn Storage + Send + Sync>>,
        config: Config,
    ) -> Arc<Mutex<dyn BasicServer>>;
}

pub trait BasicServer {
    fn apply_then(
        &mut self,
        entry: Vec<u8>,
        f: Box<dyn Fn(Result<Vec<u8>, ApplyError>) -> () + Send + Sync>,
    );
}
