use crate::server::{BasicServer, BasicServerBuilder, Config, Endpoints};
use crate::storage::Storage;
use crate::{AppendEntries, ApplyError, Peer, ServerId, Term, VoteRequest};
use futures_new::{
    compat::{Compat, TokioDefaultSpawner, Future01CompatExt},
    future::{self, Ready},
    prelude::*,
};
use log::*;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex, RwLock, Weak};
use tarpc::server::Handler;
use tarpc::{self, client, context};
use std::time::{Duration, Instant};
use tokio::timer::Delay;

pub type TarpcRaftServer = RaftServer;

#[derive(Serialize, Deserialize, Debug)]
pub struct AppendResponse {
    term: Term,
    success: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct VoteResponse {
    term: Term,
    vote_granted: bool,
}

mod service {
    use super::*;

    tarpc::service! {
        rpc append_entries(req: AppendEntries) -> AppendResponse;
        rpc request_vote(req: VoteRequest) -> VoteResponse;
    }
}

pub struct RaftServer {
    pub(crate) id: ServerId,

    pub rpc: TarpcDriver,
    pub peer: Peer,
    pub storage: Arc<RwLock<dyn Storage + Send + Sync>>,

    timeout: Duration,

    /// The configured heartbeat frequency for when this server is the leader.
    //pub(crate) heartbeat_frequency: Duration,

    /// Used for scheduling callbacks.
    pub(crate) weak_self: Weak<Mutex<RaftServer>>,
    //pub(crate) runtime: Arc<Mutex<Runtime>>,
    pub(crate) state: RaftState<LeaderState>,
}

pub(crate) enum RaftState<L: Send> {
    Follower,
    #[allow(unused)]
    Candidate {
        num_votes: i32,
    },
    #[allow(unused)]
    Leader(L),
}

impl BasicServerBuilder for RaftServer {
    fn new(
        storage: Arc<RwLock<dyn Storage + Send + Sync>>,
        config: Config,
    ) -> Arc<Mutex<dyn BasicServer>> {
        let mut endpoints = config.endpoints;
        let my_endpoint = endpoints
            .remove(&config.id)
            .expect("no server endpoint configured for my id!");

        let timeout = Duration::from_millis(if config.min_timeout_ms == config.max_timeout_ms {
            config.min_timeout_ms as u64
        } else {
            rand::thread_rng().gen_range(config.min_timeout_ms, config.max_timeout_ms) as u64
        });
        info!("Setting timeout to {:?}", timeout);

        let server = Arc::new(Mutex::new(RaftServer {
            id: config.id,

            rpc: TarpcDriver::new(config.id, endpoints),
            peer: Peer::new(storage.clone()),
            storage,

            timeout,

            //heartbeat_frequency: Duration::milliseconds(config.heartbeat_frequency_ms),
            weak_self: Weak::new(),
            state: RaftState::Follower,
        }));

        let task = server
            .lock()
            .map(|mut this| {
                let weak_server = Arc::downgrade(&server);
                this.weak_self = weak_server.clone();
                run(my_endpoint, weak_server).boxed()
            })
            .unwrap();

        std::thread::spawn(move || {
            let task = Compat::new(task.map(|()| Ok(())));
            tokio::run(task);
        });

        server
    }
}

async fn run(my_endpoint: String, weak_server: Weak<Mutex<RaftServer>>) {
    let this = weak_server.clone();
    let task = {
        upgrade_or_return!(this);
        this.rpc.connect_all(weak_server.clone());
        this.reset_timeout();
        this.rpc.create_rpc_server(my_endpoint, weak_server)
    };
    await!(task);
}

impl BasicServer for RaftServer {
    fn apply_then(
        &mut self,
        _entry: Vec<u8>,
        _f: Box<dyn Fn(Result<Vec<u8>, ApplyError>) -> () + Send + Sync>,
    ) {
        panic!("apply_then unimplemented");
    }
}

impl RaftServer {
    pub(crate) fn reset_timeout(&mut self) {
        if let RaftState::Leader(_) = self.state {
            // It would be silly to schedule timeouts when we're the leader.
            return;
        }

        let weak_self = self.weak_self.clone();
        let when = Instant::now() + self.timeout;
        let task = Delay::new(when)
            .compat()
            .map(move |d| {
                if let Err(e) = d {
                    panic!("delay errored; err={:?}", e);
                }
                debug!("delay");
                upgrade_or_return!(weak_self, Ok(()));
                TarpcDriver::timeout(&mut weak_self);
                Ok(())
            });
        tokio::spawn(Compat::new(task));
    }
}
impl service::Service for Arc<Mutex<RaftServer>> {
    type AppendEntriesFut = Ready<AppendResponse>;

    fn append_entries(
        self,
        _: tarpc::context::Context,
        _req: AppendEntries,
    ) -> Self::AppendEntriesFut {
        future::ready(AppendResponse {
            term: 0,
            success: false,
        })
    }

    type RequestVoteFut = Ready<VoteResponse>;

    fn request_vote(self, _: tarpc::context::Context, _req: VoteRequest) -> Self::RequestVoteFut {
        future::ready(VoteResponse {
            term: 0,
            vote_granted: false,
        })
    }
}

pub struct TarpcDriver {
    //rpc_server: Option<tarpc::server::Server>,
    endpoints: Endpoints,
    clients: BTreeMap<ServerId, service::Client>,
}

type LeaderState = ();

#[allow(unused)]
impl TarpcDriver {
    fn new(id: ServerId, endpoints: Endpoints) -> Self {
        tarpc::init(TokioDefaultSpawner);
        TarpcDriver {
            endpoints,
            clients: Default::default(),
        }
    }

    fn connect_all(&mut self, weak_server: Weak<Mutex<RaftServer>>) {
        let mut clients = Arc::new(Mutex::new(BTreeMap::new()));

        let all = self
            .endpoints
            .iter()
            .inspect(|(id, _)| debug!("connecting to client {}", id))
            .map(|(id, endpoint)| (*id, connect_endpoint(endpoint.clone())))
            .map(|(id, conn)| {
                let clients = clients.clone();
                conn.boxed().map(move |client| match client {
                    Ok(client) => {
                        debug!("Connected to client {}", id);
                        clients.lock().unwrap().insert(id, client);
                    }
                    Err(e) => error!("Error connecting to client {}: {}", id, e),
                })
            });
        let task = future::join_all(all).map(move |results| {
            upgrade_or_return!(weak_server, Ok(()));
            let this = &mut weak_server.rpc;
            std::mem::swap(&mut this.clients, &mut *clients.lock().unwrap());
            debug!("Finished connect_all; clients={}", this.clients.len());
            Ok(())
        });
        tokio::spawn(Compat::new(task));
    }

    fn create_rpc_server<'a>(
        &self,
        endpoint: String,
        raft_server: Weak<Mutex<RaftServer>>,
    ) -> impl Future<Output = ()> {
        let raft_server: Arc<Mutex<RaftServer>> = raft_server.upgrade().expect("asdf");
        let transport =
            bincode_transport::listen(&endpoint.parse().unwrap()).expect("could not listen");
        tarpc::server::new(tarpc::server::Config::default())
            .incoming(transport)
            .respond_with(service::serve(raft_server))
    }

    fn timeout(raft_server: &mut RaftServer) {
        raft_server.timeout();
    }
}

async fn connect_endpoint(endpoint: String) -> io::Result<service::Client> {
    let addr = endpoint.parse().expect("Could not parse server address");
    let transport = await!(bincode_transport::connect(&addr))?;

    // new_stub is generated by the service! macro. Like Server, it takes a config and any
    // Transport as input, and returns a Client, also generated by the macro.
    // by the service mcro.
    let client = await!(service::new_stub(client::Config::default(), transport))?;
    Ok(client)
}

//type ApplyError = ();
impl RaftServer {
    fn timeout(&mut self) {
        self.reset_timeout();
        warn!("timeout");

        // Vote for ourselves.
        self.state = RaftState::Candidate { num_votes: 1 };
        self.peer.voted_for = Some(self.id);
        self.peer.current_term += 1;

        let (last_log_index, last_log_term) =
            self.peer.storage.read().map(|s| (s.last_log_index(), s.last_log_term())).unwrap();

        let req = VoteRequest {
            term: self.peer.term(),
            candidate_id: self.id,
            last_log_index,
            last_log_term
        };

        for (id, client) in &self.rpc.clients {
            let req = req.clone();
            let mut client = client.clone();
            let id = *id;
            debug!("spawning vote request to {}", id);
            tokio::spawn(Compat::new(async move {
                debug!("Sending vote request to {}", id);
                let resp = await!(client.request_vote(context::current(), req));
                debug!("Vote response: {:#?}", resp);
                Ok(())
            }.boxed()));
        }
    }

    pub fn apply_one(&mut self, _entry: Vec<u8>) -> impl Future<Output = Result<Vec<u8>, ()>> {
        future::ready(Ok(vec![]))
    }

    pub fn spawn<F>(&self, f: F)
    where
        F: Future<Output = Result<(), ()>> + Send + Unpin + 'static,
    {
        tokio::run(Compat::new(f));
    }

    #[allow(unused)]
    pub(crate) fn majority(&self) -> i32 {
        (self.rpc.clients.len() as i32 + 2) / 2
    }
}
