use crate::server::{Endpoints, RaftServer, RpcDriver};
use crate::ServerId;
use crate::{AppendEntries, Term, VoteRequest};
use futures_new::{
    compat::{Compat, TokioDefaultSpawner},
    future::{self, Ready},
    prelude::*,
};
use log::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex, Weak};
use tarpc;
use tarpc::server::Handler;

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

tarpc::service! {
    rpc append_entries(req: AppendEntries) -> AppendResponse;
    rpc request_vote(req: VoteRequest) -> VoteResponse;
}

impl Service for Arc<Mutex<RaftServer<TarpcDriver>>> {
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

type RpcClient = ();

pub struct TarpcDriver {
    //rpc_server: Option<tarpc::server::Server>,
    endpoints: Endpoints,
    clients: BTreeMap<ServerId, RpcClient>,
}

#[allow(unused)]
impl RpcDriver for TarpcDriver {
    type LeaderState = ();

    fn new(id: ServerId, endpoints: Endpoints) -> Self {
        TarpcDriver {
            endpoints,
            clients: Default::default(),
        }
    }

    fn connect_all(&mut self) {
        warn!("connect_all unimplemented");
    }

    fn create_rpc_server(&mut self, endpoint: String, raft_server: Weak<Mutex<RaftServer<Self>>>) {
        tarpc::init(TokioDefaultSpawner);
        tokio::run(
            run(endpoint, raft_server)
                .map_err(|e| error!("Error while running server: {}", e))
                .boxed()
                .compat(),
        );
    }

    fn timeout(raft_server: &mut RaftServer<Self>) {
        warn!("timeout unimplemented");
    }
}

async fn connect_endpoint(endpoint: String) -> RpcClient {
    panic!("unimplemented")
}

pub type ApplyError = ();
impl RaftServer<TarpcDriver> {
    pub fn apply_one(
        &mut self,
        entry: Vec<u8>,
    ) -> impl Future<Output = Result<Vec<u8>, ApplyError>> {
        future::ready(Ok(vec![]))
    }

    pub fn spawn<F>(&self, f: F)
    where
        F: Future<Output = Result<(), ()>> + Send + Unpin + 'static,
    {
        tokio::run(Compat::new(f));
    }
}

async fn run(
    endpoint: String,
    raft_server: Weak<Mutex<RaftServer<TarpcDriver>>>,
) -> io::Result<()> {
    let raft_server: Arc<Mutex<RaftServer<TarpcDriver>>> = raft_server.upgrade().expect("asdf");
    let transport =
        bincode_transport::listen(&endpoint.parse().unwrap()).expect("could not listen");
    let server = tarpc::server::new(tarpc::server::Config::default())
        .incoming(transport)
        .respond_with(serve(raft_server));

    await!(server);

    Ok(())
}
