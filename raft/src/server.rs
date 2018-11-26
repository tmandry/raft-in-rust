use crate::protos::raft::{VoteRequest, VoteResponse};
use crate::protos::raft_grpc::{self, RaftService, RaftServiceClient};
use crate::storage::Storage;
use crate::{Peer, ServerId, StateMachine};
use futures::Future;
use grpcio::{self, ChannelBuilder, EnvBuilder, Environment, RpcContext, ServerBuilder, UnarySink};
use log::{debug, error, info, warn};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};

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

pub struct RaftServer {
    pub rpc: RpcState,
    pub peer: Arc<Mutex<Peer>>,
    pub storage: Arc<RwLock<dyn Storage + Send + Sync>>,
}

impl RaftServer {
    pub fn new(storage: Arc<RwLock<dyn Storage + Send + Sync>>, config: Config) -> Self {
        let peer = Arc::new(Mutex::new(Peer::new(storage.clone())));
        RaftServer {
            rpc: RpcState::new(config, peer.clone()),
            peer,
            storage,
        }
    }
}

pub struct RpcState {
    id: ServerId,
    clients: BTreeMap<ServerId, RaftServiceClient>,
    #[allow(unused)]
    server: grpcio::Server,
}

impl RpcState {
    pub(crate) fn new(config: Config, peer: Arc<Mutex<Peer>>) -> Self {
        let mut endpoints = config.endpoints;
        let my_endpoint = endpoints
            .remove(&config.id)
            .expect("no server endpoint configured for my id!");

        let env = Arc::new(EnvBuilder::new().build());
        RpcState {
            id: config.id,
            clients: Self::connect(endpoints, env.clone()),
            server: Self::create_server(my_endpoint, env, peer),
        }
    }

    fn connect(
        endpoints: BTreeMap<ServerId, String>,
        env: Arc<Environment>,
    ) -> BTreeMap<ServerId, RaftServiceClient> {
        let mut clients = BTreeMap::new();
        for (id, endpoint) in endpoints {
            let ch = ChannelBuilder::new(env.clone()).connect(&endpoint);
            let client = RaftServiceClient::new(ch);
            clients.insert(id, client);
        }
        clients
    }

    fn create_server(
        endpoint: String,
        env: Arc<Environment>,
        peer: Arc<Mutex<Peer>>,
    ) -> grpcio::Server {
        let endpoint: SocketAddr = endpoint.parse().unwrap();
        let service = raft_grpc::create_raft_service(peer);
        let mut server = ServerBuilder::new(env)
            .register_service(service)
            .bind(endpoint.ip().to_string(), endpoint.port())
            .build()
            .unwrap();
        server.start();
        server
    }

    pub fn timeout(&self, peer: &mut Arc<Mutex<Peer>>) {
        let mut req = VoteRequest::new();

        peer.lock()
            .map(|mut peer| {
                req.set_term(peer.current_term);
                req.set_candidate(self.id);

                peer.storage
                    .read()
                    .map(|storage| {
                        req.set_last_log_index(storage.last_log_index());
                        req.set_last_log_term(storage.last_log_term());
                    })
                    .unwrap();

                // Update current term and vote for ourselves.
                peer.voted_for = Some(self.id);
                peer.current_term += 1;
            })
            .unwrap();

        let votes_required = (self.clients.len() as i32 + 2) / 2;
        let votes_received: i32 = self
            .clients
            .values()
            .map(|client| client.request_vote(&req)) // FIXME async
            .filter_map(|resp| match resp {
                Ok(reply) => {
                    peer.lock().unwrap().saw_term(reply.term);
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

impl RaftService for Arc<Mutex<Peer>> {
    fn request_vote(&mut self, ctx: RpcContext, req: VoteRequest, sink: UnarySink<VoteResponse>) {
        info!("Got vote request from {}", req.get_candidate());

        // Introduce some artificial delay to make things more interesting.
        let delay = std::env::args().nth(1).unwrap().parse::<u64>().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10 * delay));

        let mut this = self.lock().unwrap();

        let mut resp = VoteResponse::new();
        resp.set_term(this.current_term);
        let granted = match this.voted_for {
            Some(_) => false,
            None => {
                this.voted_for = Some(req.get_candidate());
                true
            }
        };
        resp.set_vote_granted(granted);

        let f = sink
            .success(resp)
            .map_err(move |e| warn!("Failed to reply to {:?}: {:?}", req, e));
        ctx.spawn(f)
    }
}
