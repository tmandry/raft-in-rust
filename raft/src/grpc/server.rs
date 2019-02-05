use super::leader::LeaderState;
use super::protos::raft as protos;
use super::protos::raft_grpc::{self, RaftService, RaftServiceClient};
use crate::server::{Endpoints, RaftServer, RaftState, RpcDriver};
use crate::{AppendEntries, ServerId, Term, VoteRequest};

use futures::Future;
use grpcio::{self, ChannelBuilder, EnvBuilder, Environment, RpcContext, ServerBuilder, UnarySink};
use log::*;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, Weak};

impl RaftServer<GrpcDriver> {
    #[allow(unused)]
    pub(crate) fn id(&self) -> ServerId {
        self.rpc.id
    }

    pub(crate) fn other_server_ids(&self) -> impl Iterator<Item = &ServerId> {
        self.rpc.clients.keys()
    }

    pub fn spawn<F>(&self, f: F)
    where
        F: Future<Item = (), Error = ()> + Send + 'static,
    {
        self.rpc.clients.values().next().unwrap().spawn(f);
    }
}

pub struct GrpcDriver {
    id: ServerId,
    endpoints: Endpoints,
    pub(crate) clients: BTreeMap<ServerId, RaftServiceClient>,
    env: Arc<Environment>,
    pub(crate) server: Option<grpcio::Server>,
}

impl RpcDriver for GrpcDriver {
    type LeaderState = LeaderState;

    fn new(id: ServerId, endpoints: Endpoints) -> Self {
        let env = Arc::new(EnvBuilder::new().build());
        GrpcDriver {
            id,
            endpoints,
            clients: Default::default(),
            env,
            server: None, //Self::create_server(my_endpoint, env, peer),
        }
    }

    fn connect_all(&mut self) {
        for (id, endpoint) in &self.endpoints {
            self.clients.insert(*id, self.connect_endpoint(endpoint));
        }
    }

    fn create_rpc_server(
        &mut self,
        endpoint: String,
        raft_server: Weak<Mutex<RaftServer<GrpcDriver>>>,
    ) {
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

    fn timeout(raft_server: &mut RaftServer<GrpcDriver>) {
        raft_server.timeout();
    }
}

impl GrpcDriver {
    fn connect_endpoint(&self, endpoint: &str) -> RaftServiceClient {
        let ch = ChannelBuilder::new(self.env.clone())
            .load_balancing_policy(grpcio::LbPolicy::PickFirst)
            .max_reconnect_backoff(core::time::Duration::from_millis(3000)) // TODO config
            .connect(&endpoint);
        RaftServiceClient::new(ch)
    }

    pub(crate) fn reconnect_client(&mut self, id: ServerId) {
        self.clients.remove(&id);
        self.clients
            .insert(id, self.connect_endpoint(&self.endpoints[&id]));
    }
}

impl RaftServer<GrpcDriver> {
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
            let server = self.weak_self.clone();
            let request = match client.request_vote_async(&req) {
                Ok(x) => x,
                Err(e) => {
                    warn!("Error while sending vote request: {}", e);
                    return;
                }
            };
            let task = request
                .map(move |resp| {
                    trace!("request_vote response: {:?} {}", resp, resp.vote_granted);
                    upgrade_or_return!(server);
                    if resp.vote_granted {
                        server.received_vote(resp.term);
                    }
                    server.saw_term(resp.term);
                })
                .map_err(|e| {
                    error!("Error received during vote request: {:?}", e);
                });
            client.spawn(task);
        }
    }

    pub(crate) fn saw_term(&mut self, term: Term) {
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

impl RaftService for Weak<Mutex<RaftServer<GrpcDriver>>> {
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

        let (granted, term) = this.peer.request_vote(&VoteRequest {
            term: req.term,
            candidate_id: req.candidate,
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
        });
        trace!("request_vote() => ({}, {})", granted, term);

        if granted {
            this.state = RaftState::Follower;
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
        trace!("Got append request from {}", req.leader_id);

        let lock = self.upgrade();
        let mut this = match lock {
            Some(ref x) => x.lock().unwrap(),
            None => {
                warn!("Shutting down; ignoring AppendEntries");
                return;
            }
        };

        this.saw_term(req.term);
        let num_entries = req.entries.len();
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
            entries: req.entries.into_iter().map(|e| (e.term, e.data)).collect(),
        });
        debug!(
            "Append request from {} with {} entries returning {:?}",
            req.leader_id, num_entries, result
        );

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