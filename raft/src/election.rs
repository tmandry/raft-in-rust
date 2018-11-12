use crate::protos::raft::{VoteRequest, VoteResponse};
use crate::protos::raft_grpc::{RaftService, RaftServiceClient};
use crate::{Raft, Server, ServerId, StateMachine};
use futures::Future;
use grpcio::{RpcContext, UnarySink};
use std::collections::BTreeMap;

impl RaftService for Server {
    fn request_vote(&mut self, ctx: RpcContext, req: VoteRequest, sink: UnarySink<VoteResponse>) {
        println!("Got vote request from {}", req.get_candidate());

        let mut resp = VoteResponse::new();
        resp.set_term(self.current_term);
        let granted = match self.voted_for {
            Some(_) => false,
            None => {
                self.voted_for = Some(req.get_candidate());
                true
            }
        };
        resp.set_vote_granted(granted);

        let f = sink
            .success(resp)
            .map_err(move |e| println!("Failed to reply to {:?}: {:?}", req, e));
        ctx.spawn(f)
    }
}

pub struct RpcState {
    pub id: ServerId,
    pub clients: BTreeMap<ServerId, RaftServiceClient>,
}

impl RpcState {
    pub fn timeout<S: StateMachine>(&self, raft: &mut Raft<S>) {
        let mut req = VoteRequest::new();
        req.set_term(raft.server.current_term);
        req.set_candidate(self.id);

        let last_log = raft.log.iter().next_back();
        req.set_last_log_index(last_log.map(|x| *x.0).unwrap_or(0));
        req.set_last_log_term(last_log.map(|x| x.1.term).unwrap_or(0));

        let votes_received: i32 = self
            .clients
            .values()
            .map(|client| client.request_vote(&req))
            .map(|resp| match resp {
                Ok(reply) => {
                    raft.saw_term(reply.term);
                    if reply.vote_granted {
                        1
                    } else {
                        0
                    }
                }
                Err(e) => {
                    println!("Error received during vote request: {:?}", e);
                    0
                }
            })
            .sum();
        println!("Received {} votes!", votes_received);
    }
}
