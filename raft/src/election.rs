use crate::protos::raft::{VoteRequest, VoteResponse};
use crate::protos::raft_grpc::{self, RaftService, RaftServiceClient};
use crate::server::Config;
use crate::{Raft, Server, ServerId, StateMachine};
use futures::Future;
use grpcio::{self, ChannelBuilder, EnvBuilder, Environment, RpcContext, ServerBuilder, UnarySink};
use log::{debug, error, info, warn};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

impl RaftService for Arc<Mutex<Server>> {
    fn request_vote(&mut self, ctx: RpcContext, req: VoteRequest, sink: UnarySink<VoteResponse>) {
        info!("Got vote request from {}", req.get_candidate());

        // Introduce some artificial delay to make things more interesting.
        let delay = std::env::args().nth(1).unwrap().parse::<u64>().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(100 * delay));

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

pub struct RpcState {
    id: ServerId,
    clients: BTreeMap<ServerId, RaftServiceClient>,
    #[allow(unused)]
    server: grpcio::Server,
}

impl RpcState {
    pub(crate) fn new(config: Config, server: Arc<Mutex<Server>>) -> Self {
        let mut endpoints = config.endpoints;
        let my_endpoint = endpoints
            .remove(&config.id)
            .expect("no server endpoint configured for my id!");

        let env = Arc::new(EnvBuilder::new().build());
        RpcState {
            id: config.id,
            clients: Self::connect(endpoints, env.clone()),
            server: Self::create_server(my_endpoint, env, server),
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
        server: Arc<Mutex<Server>>,
    ) -> grpcio::Server {
        let endpoint: SocketAddr = endpoint.parse().unwrap();
        let service = raft_grpc::create_raft_service(server);
        let mut server = ServerBuilder::new(env)
            .register_service(service)
            .bind(endpoint.ip().to_string(), endpoint.port())
            .build()
            .unwrap();
        server.start();
        server
    }

    pub fn timeout<S: StateMachine>(&self, raft: &mut Raft<S>) {
        let mut req = VoteRequest::new();
        req.set_term(raft.server.lock().unwrap().current_term);
        req.set_candidate(self.id);

        let last_log = raft.log.iter().next_back();
        req.set_last_log_index(last_log.map(|x| *x.0).unwrap_or(0));
        req.set_last_log_term(last_log.map(|x| x.1.term).unwrap_or(0));

        let votes_required = (self.clients.len() as i32 + 2) / 2;
        let votes_received: i32 = self
            .clients
            .values()
            .map(|client| client.request_vote(&req))
            .filter_map(|resp| match resp {
                Ok(reply) => {
                    raft.saw_term(reply.term);
                    if reply.vote_granted {
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
