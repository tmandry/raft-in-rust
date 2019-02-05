use crate::server::{Endpoints, RaftServer, RpcDriver};
use crate::ServerId;
use std::sync::{Mutex, Weak};

pub struct TarpcDriver {}

#[allow(unused)]
impl RpcDriver for TarpcDriver {
    type LeaderState = ();

    fn new(id: ServerId, endpoints: Endpoints) -> Self {
        TarpcDriver {}
    }

    fn connect_all(&mut self) {
        panic!("unimplemented");
    }

    fn create_rpc_server(&mut self, endpoint: String, raft_server: Weak<Mutex<RaftServer<Self>>>) {
        panic!("unimplemented");
    }

    fn timeout(raft_server: &mut RaftServer<Self>) {
        panic!("unimplemented");
    }
}
