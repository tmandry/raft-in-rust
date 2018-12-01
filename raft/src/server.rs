use crate::leader::LeaderState;
use crate::protos::raft as protos;
use crate::protos::raft_grpc::{self, RaftService, RaftServiceClient};
use crate::storage::Storage;
use crate::{AppendEntries, Peer, ServerId, Term, VoteRequest};
//use crate::macros::upgrade_or_return;

use chrono::Duration;
use futures::Future;
use grpcio::{self, ChannelBuilder, EnvBuilder, Environment, RpcContext, ServerBuilder, UnarySink};
use log::{debug, error, info, warn};
use rand::Rng;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock, Weak};
use timer::{self, Timer};

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
        let mut conf = Config {
            id,
            endpoints: Default::default(),
            // TODO read from config file
            min_timeout_ms: 1000,
            max_timeout_ms: 3000,
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

pub struct RaftServer {
    pub rpc: RpcState,
    pub peer: Peer,
    pub storage: Arc<RwLock<dyn Storage + Send + Sync>>,

    pub(crate) timer: Timer,

    timeout: Duration,
    scheduled_timeout: Option<timer::Guard>,

    /// The configured heartbeat frequency for when this server is the leader.
    pub(crate) heartbeat_frequency: Duration,

    /// Used for scheduling callbacks.
    pub(crate) weak_self: Weak<Mutex<RaftServer>>,

    pub(crate) state: RaftState,
}

pub(crate) enum RaftState {
    Follower,
    Candidate { num_votes: i32 },
    Leader(LeaderState),
}

impl RaftServer {
    pub fn new(
        storage: Arc<RwLock<dyn Storage + Send + Sync>>,
        config: Config,
    ) -> Arc<Mutex<Self>> {
        let mut endpoints = config.endpoints;
        let my_endpoint = endpoints
            .remove(&config.id)
            .expect("no server endpoint configured for my id!");

        let timeout = Duration::milliseconds(
            rand::thread_rng().gen_range(config.min_timeout_ms, config.max_timeout_ms),
        );
        info!("Setting timeout to {}", timeout);

        let server = Arc::new(Mutex::new(RaftServer {
            rpc: RpcState::new(config.id),
            peer: Peer::new(storage.clone()),
            storage,

            timer: Timer::new(),

            scheduled_timeout: None,
            timeout,

            heartbeat_frequency: Duration::milliseconds(config.heartbeat_frequency_ms),

            weak_self: Weak::new(),

            state: RaftState::Follower,
        }));

        server
            .lock()
            .map(|mut this| {
                this.rpc.connect(endpoints);
                let weak_server = Arc::downgrade(&server);
                this.rpc.create_rpc_server(my_endpoint, weak_server.clone());

                this.weak_self = weak_server;
                this.reset_timeout();
            })
            .unwrap();
        server
    }

    #[allow(unused)]
    pub(crate) fn id(&self) -> ServerId {
        self.rpc.id
    }

    pub(crate) fn other_server_ids(&self) -> impl Iterator<Item = &ServerId> {
        self.rpc.clients.keys()
    }
}

pub struct RpcState {
    id: ServerId,
    pub(crate) clients: BTreeMap<ServerId, RaftServiceClient>,
    env: Arc<Environment>,
    #[allow(unused)]
    pub(crate) server: Option<grpcio::Server>,
}

impl RpcState {
    pub(crate) fn new(id: ServerId) -> Self {
        let env = Arc::new(EnvBuilder::new().build());
        RpcState {
            id: id,
            clients: Default::default(),
            env,
            server: None, //Self::create_server(my_endpoint, env, peer),
        }
    }

    fn connect(&mut self, endpoints: Endpoints) {
        for (id, endpoint) in endpoints {
            let ch = ChannelBuilder::new(self.env.clone()).connect(&endpoint);
            let client = RaftServiceClient::new(ch);
            self.clients.insert(id, client);
        }
    }

    fn create_rpc_server(&mut self, endpoint: String, raft_server: Weak<Mutex<RaftServer>>) {
        assert!(self.server.is_none());
        let endpoint: SocketAddr = endpoint.parse().unwrap();
        let service = raft_grpc::create_raft_service(raft_server);
        let mut server = ServerBuilder::new(self.env.clone())
            .register_service(service)
            .bind(endpoint.ip().to_string(), endpoint.port())
            .build()
            .unwrap();
        server.start();
        self.server = Some(server);
    }
}

impl RaftServer {
    fn reset_timeout(&mut self) {
        if let RaftState::Leader(_) = self.state {
            // It would be silly to schedule timeouts when we're the leader.
            return;
        }

        let weak_self = self.weak_self.clone();
        let guard = self.timer.schedule_with_delay(self.timeout, move || {
            upgrade_or_return!(weak_self);
            weak_self.timeout();
        });
        self.scheduled_timeout = Some(guard);
    }

    /// Starts an election to become the next leader.
    fn timeout(&mut self) {
        self.state = RaftState::Candidate { num_votes: 1 }; // we're going to vote for ourselves.
        self.reset_timeout();

        let mut req = protos::VoteRequest::new();

        let peer = &mut self.peer;
        let new_term = peer.current_term + 1;
        req.set_term(new_term);
        req.set_candidate(self.rpc.id);
        peer.storage
            .read()
            .map(|storage| {
                req.set_last_log_index(storage.last_log_index());
                req.set_last_log_term(storage.last_log_term());
            })
            .unwrap();

        // Update current term and vote for ourselves.
        peer.voted_for = Some(self.rpc.id);
        peer.current_term = new_term;

        info!(
            "Timeout occurred; starting new election with term {}",
            new_term
        );

        for client in self.rpc.clients.values() {
            let weak_self = self.weak_self.clone();
            let request = match client.request_vote_async(&req) {
                Ok(x) => x,
                Err(e) => {
                    warn!("Error while sending vote request: {}", e);
                    return;
                }
            };
            let task = request
                .map(move |resp| {
                    if resp.vote_granted {
                        upgrade_or_return!(weak_self);
                        weak_self.received_vote(resp.term);
                    }
                })
                .map_err(|e| {
                    error!("Error received during vote request: {:?}", e);
                });
            client.spawn(task);
        }
    }

    fn saw_term(&mut self, term: Term) {
        if term > self.peer.term() {
            info!("Saw term {}, now a follower", term);
            self.state = RaftState::Follower;
            self.reset_timeout();
        }
        self.peer.saw_term(term);
    }

    fn received_vote(&mut self, term: Term) {
        let current_term = self.peer.term();
        self.saw_term(term);
        if term != current_term {
            debug!("Received vote, but for an old term");
            return;
        }

        let num_votes = match self.state {
            RaftState::Candidate { num_votes } => num_votes + 1,
            _ => {
                debug!("Received vote, but no longer a candidate");
                return;
            }
        };

        debug!("Received vote");
        self.state = RaftState::Candidate { num_votes };

        if num_votes >= self.majority() {
            info!("Elected leader");
            self.scheduled_timeout = None;
            self.become_leader();
        }
    }

    pub(crate) fn majority(&self) -> i32 {
        (self.rpc.clients.len() as i32 + 2) / 2
    }
}

impl RaftService for Weak<Mutex<RaftServer>> {
    fn request_vote(
        &mut self,
        ctx: RpcContext,
        req: protos::VoteRequest,
        sink: UnarySink<protos::VoteResponse>,
    ) {
        info!("Got vote request from {}", req.candidate);

        // Introduce some artificial delay to make things more interesting.
        let delay = std::env::args().nth(1).unwrap().parse::<u64>().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10 * delay));

        let lock = self.upgrade();
        let mut this = match lock {
            Some(ref x) => x.lock().unwrap(),
            None => {
                warn!("Shutting down; ignoring RequestVote");
                return;
            }
        };

        this.saw_term(req.term);
        let (granted, term) = this.peer.request_vote(&VoteRequest {
            term: req.term,
            candidate_id: req.candidate,
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
        });

        if granted {
            this.reset_timeout();
        }

        let mut resp = protos::VoteResponse::new();
        resp.term = term;
        resp.vote_granted = granted;

        let f = sink
            .success(resp)
            .map_err(move |e| warn!("Failed to reply to {:?}: {:?}", req, e));
        ctx.spawn(f)
    }

    fn append_entries(
        &mut self,
        ctx: RpcContext,
        req: protos::AppendRequest,
        sink: UnarySink<protos::AppendResponse>,
    ) {
        debug!("Got append request from {}", req.leader_id);

        let lock = self.upgrade();
        let mut this = match lock {
            Some(ref x) => x.lock().unwrap(),
            None => {
                warn!("Shutting down; ignoring AppendEntries");
                return;
            }
        };

        this.saw_term(req.term);
        let result = this.peer.append_entries(AppendEntries {
            term: req.term,
            leader_id: req.leader_id,
            prev_log_index: if req.prev_log_index == 0 {
                None
            } else {
                Some(req.prev_log_index)
            },
            prev_log_term: req.prev_log_term,
            leader_commit: req.leader_commit,
            entries: req.entries.into_vec(),
        });

        use crate::AppendEntriesError::*;
        match result {
            Ok(_) => this.reset_timeout(),
            Err(NeedBackfill) => this.reset_timeout(),
            // Don't reset timeout for BadTerm; we haven't seen a valid request.
            Err(BadTerm) => {}
        };

        let mut resp = protos::AppendResponse::new();
        resp.term = this.peer.term();
        resp.success = result.is_ok();

        let f = sink
            .success(resp)
            .map_err(move |e| warn!("Failed to reply to AppendEntries: {:?}", e));
        ctx.spawn(f)
    }
}
