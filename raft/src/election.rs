use jsonrpc_core::Result;
use serde_derive::{Deserialize, Serialize};
//use jsonrpc_macros::build_rpc_trait;

use crate::{Server, Term};

#[derive(Serialize, Deserialize)]
pub struct RequestVote;

type RVResult = (Term, bool);

build_rpc_trait! {
    pub trait Rpc {
        #[rpc(name = "request_vote")]
        fn request_vote(&self, RequestVote) -> Result<RVResult>;
    }
}

pub struct RpcImpl {
    _s: Server,
}

impl RpcImpl {
    pub fn new() -> RpcImpl {
        RpcImpl {
            _s: Server::default(),
        }
    }
}

impl Rpc for RpcImpl {
    fn request_vote(&self, _msg: RequestVote) -> Result<RVResult> {
        Ok((0, false))
    }
}
