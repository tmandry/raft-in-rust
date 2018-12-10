//! Defines the behavior of the leader.

use crate::protos::raft as protos;
use crate::server::{RaftServer, RaftState};
use crate::storage;
use crate::{LogIndex, ServerId};

use futures::future::{self, Future};
use log::*;
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use timer;

pub(crate) struct LeaderState {
    #[allow(unused)]
    next_index: BTreeMap<ServerId, LogIndex>,
    #[allow(unused)]
    match_index: BTreeMap<ServerId, LogIndex>,
    next_heartbeat: Option<timer::Guard>,
}

#[derive(Debug)]
pub enum ApplyError {
    NotLeader,
    StorageError(storage::Error),
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

        let req = self.append_request(vec![]);
        for client in self.rpc.clients.values() {
            //let weak_self = self.weak_self.clone();
            let request = match client.append_entries_async(&req) {
                Ok(x) => x,
                Err(e) => {
                    warn!("Error while sending heartbeat: {}", e);
                    continue;
                }
            };
            let task = request
                .map(move |resp| {
                    upgrade_or_return!(server);
                    let current_term = server.peer.current_term;
                    server.saw_term(resp.term);
                    if !resp.success && resp.term == current_term {
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

    pub fn apply_one(
        &mut self,
        entry: Vec<u8>,
    ) -> Box<dyn Future<Item = Vec<u8>, Error = ApplyError> + Send> {
        self.schedule_heartbeat();

        match self.state {
            RaftState::Leader(_) => (),
            _ => return Box::new(future::err(ApplyError::NotLeader)),
        };

        // Commit everything up to now.
        let last_log_index = self.peer.storage.read().unwrap().last_log_index();
        self.peer.update_commit(last_log_index);

        let req = self.append_request(vec![entry.clone()]);
        let requests =
            self.rpc
                .clients
                .values()
                .flat_map(|client| match client.append_entries_async(&req) {
                    Ok(request) => Some(request),
                    Err(e) => {
                        warn!("Error while sending append request: {}", e);
                        None
                    }
                });

        self.peer.append_local(vec![entry]);
        let appends = Arc::new(AtomicUsize::new(1)); // count ourselves
        self.combine_requests(requests, appends)
    }

    fn combine_requests<
        I: IntoIterator<Item = F>,
        F: Future<Item = protos::AppendResponse> + Send + 'static,
    >(
        &self,
        requests: I,
        appends: Arc<AtomicUsize>,
    ) -> Box<dyn Future<Item = Vec<u8>, Error = ApplyError> + Send>
    where
        F::Error: std::fmt::Debug,
    {
        trace!(
            "combine_requests(appends={})",
            appends.load(Ordering::SeqCst)
        );
        let server = self.weak_self.clone();
        Box::new(futures::select_all(requests).then(
            move |result| -> Box<dyn Future<Item = Vec<u8>, Error = ApplyError> + Send> {
                match &result {
                    Ok((r, _, _)) => trace!("AppendEntries response: {:?}", r),
                    Err((e, _, _)) => trace!("AppendEntries error: {:?}", e),
                };
                upgrade_or_return!(server, Box::new(future::ok(vec![])));

                let rest: Vec<_> = match result {
                    Ok((response, _index, rest)) => {
                        let should_commit = if response.success {
                            let total = appends.fetch_add(1, Ordering::SeqCst) + 1;
                            total as i32 == server.majority()
                        } else {
                            // TODO: retry failed requests due to log inconsistency
                            warn!("Log inconsistency detected, TODO retry");
                            false
                        };

                        if should_commit {
                            // Commit the response.
                            trace!("committing");
                            let result = match server.peer.apply_one() {
                                Ok(r) => Box::new(future::ok(r)),
                                Err(e) => Box::new(future::err(ApplyError::StorageError(e))),
                            };
                            trace!("  result = {:?}", result);

                            // Can immediately return the result, but have to keep the
                            // remaining futures alive.
                            let remaining = future::join_all(rest).map(|_| ()).map_err(|e| {
                                warn!("Error while sending append request (applied): {:?}", e);
                            });
                            server.rpc.clients.values().next().unwrap().spawn(remaining);

                            return result;
                        }

                        rest
                    }
                    Err((e, _index, rest)) => {
                        warn!("Error while sending append request: {:?}", e);
                        rest
                    }
                };

                if rest.is_empty() {
                    Box::new(future::ok(vec![]))
                } else {
                    Box::new(server.combine_requests(rest, appends.clone()))
                }
            },
        ))
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

    fn append_request(&self, entries: Vec<Vec<u8>>) -> protos::AppendRequest {
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
        req.set_entries(entries.into());
        req
    }
}
