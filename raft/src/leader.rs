//! Defines the behavior of the leader.

use crate::protos::raft as protos;
use crate::server::{RaftServer, RaftState};
use crate::{LogIndex, ServerId};

use futures::future::{self, Future};
use log::*;
use std::collections::BTreeMap;
use timer;

pub(crate) struct LeaderState {
    next_index: BTreeMap<ServerId, LogIndex>,
    match_index: BTreeMap<ServerId, LogIndex>,
    next_heartbeat: Option<timer::Guard>,
}

pub enum ApplyError {
    NotLeader,
}

impl RaftServer {
    pub(crate) fn become_leader(&mut self) {
        info!("Becoming leader");

        let last_log_index = self.peer.storage.read().unwrap().last_log_index();

        let mut next_index: BTreeMap<ServerId, LogIndex> = BTreeMap::new();
        let mut match_index: BTreeMap<ServerId, LogIndex> = BTreeMap::new();
        for id in self.other_server_ids() {
            next_index.insert(*id, last_log_index + 1);
            match_index.insert(*id, 0);
        }

        self.state = RaftState::Leader(LeaderState {
            next_index,
            match_index,
            next_heartbeat: None,
        });

        self.heartbeat();
    }

    fn heartbeat(&mut self) {
        let _state = match &mut self.state {
            RaftState::Leader(inner) => inner,
            _ => return,
        };

        debug!("Sending heartbeat");

        let mut req = protos::AppendRequest::new();
        req.set_term(self.peer.current_term);
        req.set_leader_id(self.id());
        self.peer
            .storage
            .read()
            .map(|storage| {
                req.set_prev_log_index(storage.last_log_index());
                req.set_prev_log_term(storage.last_log_term());
            })
            .unwrap();
        req.set_leader_commit(self.peer.last_commit);

        for client in self.rpc.clients.values() {
            //let weak_self = self.weak_self.clone();
            let request = match client.append_entries_async(&req) {
                Ok(x) => x,
                Err(e) => {
                    warn!("Error while sending heartbeat: {}", e);
                    return;
                }
            };
            let task = request
                .map(|resp| {
                    if !resp.success {
                        // TODO: retry failed requests due to log inconsistency
                        warn!("Log inconsistency detected, TODO retry");
                    }
                })
                .map_err(|e| {
                    warn!("Error received from heartbeat: {:?}", e);
                });
            client.spawn(task);
        }

        self.schedule_heartbeat();
    }

    fn apply(
        &mut self,
        _entries: Vec<Vec<u8>>,
    ) -> impl Future<Item = Vec<Vec<u8>>, Error = ApplyError> {
        let _state = match &mut self.state {
            RaftState::Leader(inner) => inner,
            _ => return future::err(ApplyError::NotLeader),
        };

        self.schedule_heartbeat();

        future::err(ApplyError::NotLeader)
    }

    fn schedule_heartbeat(&mut self) {
        let state = match &mut self.state {
            RaftState::Leader(inner) => inner,
            _ => return,
        };
        let server = self.weak_self.clone();
        state.next_heartbeat = Some(self.timer.schedule_with_delay(
            self.heartbeat_frequency,
            move || {
                upgrade_or_return!(server);
                server.heartbeat();
            },
        ));
    }
}
