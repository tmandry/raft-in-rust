use crate::protos::raft as protos;
use crate::protos::raft_grpc::{self, RaftService, RaftServiceClient};
use crate::storage::Storage;
use crate::{Peer, ServerId, StateMachine, VoteRequest};
use futures::Future;
use grpcio::{self, ChannelBuilder, EnvBuilder, Environment, RpcContext, ServerBuilder, UnarySink};
use log::{debug, error, info, warn};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock, Weak};

pub type Endpoints = BTreeMap<ServerId, String>;

#[derive(Clone, Debug)]
pub struct Config {
    pub id: ServerId,
    pub endpoints: Endpoints,
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

pub struct RaftServer {
    pub rpc: RpcState,
    pub peer: Peer,
    pub storage: Arc<RwLock<dyn Storage + Send + Sync>>,
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

        let server = Arc::new(Mutex::new(RaftServer {
            rpc: RpcState::new(config.id),
            peer: Peer::new(storage.clone()),
            storage,
        }));

        server
            .lock()
            .map(|mut this| {
                this.rpc.connect(endpoints);
                let weak_server = Arc::downgrade(&server);
                this.rpc.create_rpc_server(my_endpoint, weak_server);
            })
            .unwrap();
        server
    }
}

pub struct RpcState {
    id: ServerId,
    clients: BTreeMap<ServerId, RaftServiceClient>,
    env: Arc<Environment>,
    #[allow(unused)]
    server: Option<grpcio::Server>,
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
    pub fn timeout(&mut self) {
        let mut req = protos::VoteRequest::new();

        req.set_term(self.peer.current_term);
        req.set_candidate(self.rpc.id);

        let peer = &mut self.peer;
        peer.storage
            .read()
            .map(|storage| {
                req.set_last_log_index(storage.last_log_index());
                req.set_last_log_term(storage.last_log_term());
            })
            .unwrap();

        // Update current term and vote for ourselves.
        peer.voted_for = Some(self.rpc.id);
        peer.current_term += 1;

        let votes_required = (self.rpc.clients.len() as i32 + 2) / 2;
        let votes_received: i32 = self
            .rpc
            .clients
            .values()
            .map(|client| client.request_vote(&req)) // FIXME async
            .filter_map(|resp| match resp {
                Ok(reply) => {
                    peer.saw_term(reply.term);
                    if reply.vote_granted {
                        debug!("Received vote");
                        Some(1)
                    } else {
                        None
                    }
                }
                Err(e) => {
                    error!("Error received during vote request: {:?}", e);
                    None
                }
            })
            .take(votes_required as usize)
            .sum();

        if votes_received >= votes_required {
            info!("Received {} votes and elected!", votes_received);
        } else {
            info!("Received {} votes, not elected.", votes_received);
        }
    }
}

impl RaftService for Weak<Mutex<RaftServer>> {
    fn request_vote(
        &mut self,
        ctx: RpcContext,
        req: protos::VoteRequest,
        sink: UnarySink<protos::VoteResponse>,
    ) {
        info!("Got vote request from {}", req.get_candidate());

        // Introduce some artificial delay to make things more interesting.
        let delay = std::env::args().nth(1).unwrap().parse::<u64>().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10 * delay));

        let lock = self.upgrade();
        let mut this = match lock {
            Some(ref x) => x.lock().unwrap(),
            None => {
                warn!("Shutting down; ignoring VoteRequest");
                return;
            }
        };

        let mut resp = protos::VoteResponse::new();
        resp.set_term(this.peer.current_term);
        let (granted, term) = this.peer.request_vote(&VoteRequest {
            term: req.term,
            candidate_id: req.candidate,
            last_log_index: req.last_log_index,
            last_log_term: req.last_log_term,
        });
        resp.term = term;
        resp.vote_granted = granted;

        let f = sink
            .success(resp)
            .map_err(move |e| warn!("Failed to reply to {:?}: {:?}", req, e));
        ctx.spawn(f)
    }
}
