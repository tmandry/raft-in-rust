use crate::storage::Storage;
use crate::{Peer, ServerId};

use chrono::Duration;
use log::*;
use rand::Rng;
use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::{Arc, Mutex, RwLock, Weak};
use timer::{self, Timer};
use tokio::runtime::Runtime;

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

pub trait RpcDriver: Send + 'static
where
    Self: std::marker::Sized,
{
    type LeaderState: Send;

    fn new(id: ServerId, endpoints: Endpoints, runtime: Arc<Mutex<Runtime>>) -> Self;
    fn connect_all(&mut self);
    fn create_rpc_server(&mut self, endpoint: String, raft_server: Weak<Mutex<RaftServer<Self>>>);
    fn timeout(raft_server: &mut RaftServer<Self>);
}

pub struct RaftServer<R: RpcDriver> {
    pub rpc: R,
    pub peer: Peer,
    pub storage: Arc<RwLock<dyn Storage + Send + Sync>>,

    pub(crate) timer: Timer,

    timeout: Duration,
    pub(crate) scheduled_timeout: Option<timer::Guard>,

    /// The configured heartbeat frequency for when this server is the leader.
    pub(crate) heartbeat_frequency: Duration,

    /// Used for scheduling callbacks.
    pub(crate) weak_self: Weak<Mutex<RaftServer<R>>>,
    pub(crate) runtime: Arc<Mutex<Runtime>>,

    pub(crate) state: RaftState<R::LeaderState>,
}

pub(crate) enum RaftState<L: Send> {
    Follower,
    Candidate { num_votes: i32 },
    Leader(L),
}

impl<R: RpcDriver> RaftServer<R> {
    pub fn new(
        storage: Arc<RwLock<dyn Storage + Send + Sync>>,
        config: Config,
    ) -> Arc<Mutex<Self>> {
        let mut endpoints = config.endpoints;
        let my_endpoint = endpoints
            .remove(&config.id)
            .expect("no server endpoint configured for my id!");

        let timeout = Duration::milliseconds(if config.min_timeout_ms == config.max_timeout_ms {
            config.min_timeout_ms
        } else {
            rand::thread_rng().gen_range(config.min_timeout_ms, config.max_timeout_ms)
        });
        info!("Setting timeout to {}", timeout);

        let runtime = Arc::new(Mutex::new(Runtime::new().unwrap()));
        let server = Arc::new(Mutex::new(RaftServer {
            rpc: R::new(config.id, endpoints, runtime.clone()),
            peer: Peer::new(storage.clone()),
            storage,

            timer: Timer::new(),

            scheduled_timeout: None,
            timeout,

            heartbeat_frequency: Duration::milliseconds(config.heartbeat_frequency_ms),

            weak_self: Weak::new(),
            runtime,

            state: RaftState::Follower,
        }));

        server
            .lock()
            .map(|mut this| {
                this.rpc.connect_all();
                let weak_server = Arc::downgrade(&server);
                this.rpc.create_rpc_server(my_endpoint, weak_server.clone());

                this.weak_self = weak_server;
                debug!("calling reset_timeout");
                this.reset_timeout();
            })
            .unwrap();
        server
    }
}

impl<R: RpcDriver> RaftServer<R> {
    pub(crate) fn reset_timeout(&mut self) {
        if let RaftState::Leader(_) = self.state {
            // It would be silly to schedule timeouts when we're the leader.
            return;
        }

        let weak_self = self.weak_self.clone();
        let guard = self.timer.schedule_with_delay(self.timeout, move || {
            debug!("delay");
            upgrade_or_return!(weak_self);
            R::timeout(&mut weak_self);
        });
        self.scheduled_timeout = Some(guard);
    }
}
