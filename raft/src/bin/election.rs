extern crate jsonrpc_core;
extern crate jsonrpc_http_server;
extern crate raft;

use jsonrpc_core::{IoHandler, Params, Value};
use jsonrpc_http_server::{AccessControlAllowOrigin, DomainsValidation, ServerBuilder};

use raft::election::{Rpc, RpcImpl};

fn main() {
    let mut io = IoHandler::default();
    io.add_method("say_hello", |_params: Params| {
        Ok(Value::String("hello".into()))
    });
    let raft_rpc = RpcImpl::new();
    io.extend_with(raft_rpc.to_delegate());

    let server = ServerBuilder::new(io)
        .cors(DomainsValidation::AllowOnly(vec![
            AccessControlAllowOrigin::Null,
        ]))
        .start_http(&"127.0.0.1:3030".parse().unwrap())
        .expect("Unable to start RPC server");

    server.wait();
}
