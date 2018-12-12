//! Defines the behavior of the leader.

use crate::protos::raft as protos;
use crate::protos::raft_grpc;
use crate::server::{RaftServer, RaftState};
use crate::storage::{self, Storage};
use crate::{LogIndex, ServerId, Term};

use futures::future::{self, Future};
use log::*;
use std::collections::BTreeMap;
use std::ops::Deref;
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
    ticks_since_response: BTreeMap<ServerId, i32>,
}

#[derive(Debug)]
pub enum ApplyError {
    NotLeader,
    StorageError(storage::Error),
}

#[derive(Debug)]
enum AppendRequestError {
    RpcError(grpcio::Error),
    RetryUnsuccessful,
}

type AppendFuture = Box<
    dyn Future<Item = (ServerId, protos::AppendResponse), Error = AppendRequestError>
        + Send
        + 'static,
>;

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
            ticks_since_response: Default::default(),
        });

        self.heartbeat();
    }

    fn heartbeat(&mut self) {
        debug!("Sending heartbeat");
        let req = self.append_request(vec![]);

        let state = match &mut self.state {
            RaftState::Leader(inner) => inner,
            _ => return,
        };

        let mut reconnect = vec![];
        for (id, client) in self.rpc.clients.iter() {
            *state.ticks_since_response.entry(*id).or_insert(0) += 1;
            // TODO: config
            if state.ticks_since_response[id] >= 5 {
                error!("Reconnecting to {}", id);
                reconnect.push(*id);
                state.ticks_since_response.insert(*id, 0);
            }

            let server = self.weak_self.clone();
            let id = *id;
            let prev_log_index = req.prev_log_index;
            let request = match client.append_entries_async(&req) {
                Ok(x) => x,
                Err(e) => {
                    warn!("Error while sending heartbeat: {}", e);
                    continue;
                }
            };
            let task = request
                .map_err(|e| AppendRequestError::RpcError(e))
                .and_then(
                    move |resp| -> Box<Future<Item = (), Error = AppendRequestError> + Send> {
                        upgrade_or_return!(server, Box::new(future::empty()));

                        let state = match &mut server.state {
                            RaftState::Leader(inner) => inner,
                            _ => return Box::new(future::empty()),
                        };
                        state.ticks_since_response.insert(id, 0);

                        let current_term = server.peer.current_term;
                        server.saw_term(resp.term);
                        if !resp.success && resp.term == current_term {
                            info!("Catching up server {}", id);
                            return Box::new(
                                server.retry_failed_append(prev_log_index, id).map(|_| ()),
                            );
                        }

                        Box::new(future::ok(()))
                    },
                )
                .map_err(|e| {
                    warn!("Error received from heartbeat: {:?}", e);
                });

            // FIXME This is a bug waiting to happen.
            //
            // Technically it won't deadlock because we're calling this from the
            // timer thread, but really we should just move everything to use
            // ReentrantLock.
            client.spawn(task);
        }

        for id in reconnect {
            self.rpc.reconnect_client(id);
        }

        self.schedule_heartbeat();
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

    pub fn apply_one(
        &mut self,
        entry: Vec<u8>,
    ) -> Box<dyn Future<Item = Vec<u8>, Error = ApplyError> + Send> {
        let entry = (self.peer.current_term, entry);

        self.schedule_heartbeat();

        match self.state {
            RaftState::Leader(_) => (),
            _ => return Box::new(future::err(ApplyError::NotLeader)),
        };

        // Commit everything up to now.
        let last_log_index = self.peer.storage.read().unwrap().last_log_index();
        self.peer.update_commit(last_log_index);

        let log_index = last_log_index + 1;

        let req = self.append_request(vec![entry.clone()]);
        let requests: Vec<AppendFuture> = self
            .rpc
            .clients
            .iter()
            .flat_map(|(id, client)| self.issue_append_request(&req, client, *id, log_index, true))
            .collect();

        self.peer.append_local(vec![entry]);
        let appends = Arc::new(AtomicUsize::new(1)); // count ourselves
        self.combine_requests(requests, appends, log_index)
    }

    fn issue_append_request(
        &self,
        request: &protos::AppendRequest,
        client: &raft_grpc::RaftServiceClient,
        server_id: ServerId,
        log_index: LogIndex,
        retry: bool,
    ) -> Option<AppendFuture> {
        let req = match client.append_entries_async(request) {
            Ok(req) => req,
            Err(e) => {
                warn!("Error while sending append request: {}", e);
                return None;
            }
        };
        let req = req
            .map(move |resp| (server_id, resp))
            .map_err(|e| AppendRequestError::RpcError(e));

        let req: AppendFuture = if retry {
            let server = self.weak_self.clone();
            Box::new(req.and_then(move |(server_id, resp)| -> AppendFuture {
                if !resp.success {
                    debug!(
                        "Failed append for server_id={} log_index={}",
                        server_id, log_index
                    );
                    upgrade_or_return!(server, Box::new(future::empty()));
                    return server.retry_failed_append(log_index, server_id);
                }
                Box::new(future::ok((server_id, resp)))
            }))
        } else {
            Box::new(req)
        };
        Some(req)
    }

    fn combine_requests<I: IntoIterator<Item = AppendFuture>>(
        &self,
        requests: I,
        appends: Arc<AtomicUsize>,
        log_index: LogIndex,
    ) -> Box<dyn Future<Item = Vec<u8>, Error = ApplyError> + Send> {
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
                    Ok(((_server_id, response), _, rest)) => {
                        assert!(
                            response.success,
                            "Should have retried on unsuccessful append request, or returned error"
                        );

                        let should_commit = {
                            let total = appends.fetch_add(1, Ordering::SeqCst) + 1;
                            total as i32 == server.majority()
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

                            let runtime = server.runtime.clone();
                            std::mem::drop(server);
                            runtime.lock().unwrap().spawn(remaining);

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
                    Box::new(server.combine_requests(rest, appends.clone(), log_index))
                }
            },
        ))
    }

    fn get_entry_for_retry<S: Deref<Target = dyn Storage + Send + Sync>>(
        storage: &S,
        index: LogIndex,
    ) -> Result<(Term, Vec<u8>), AppendRequestError> {
        match storage.get_entry(index) {
            Some(x) => Ok(x),
            None => {
                error!(
                    "Failed append retry because entry index {} was not available",
                    index
                );
                Err(AppendRequestError::RetryUnsuccessful)
            }
        }
    }

    fn build_retry_request(
        &self,
        index: LogIndex,
    ) -> Result<protos::AppendRequest, AppendRequestError> {
        let storage = self.peer.storage.read().unwrap();
        let (term, entry) = Self::get_entry_for_retry(&storage, index)?;

        let mut request = self.append_request(vec![(term, entry)]);
        if index == 1 {
            request.prev_log_index = 0;
            request.prev_log_term = 0;
        } else {
            request.prev_log_index = index - 1;
            request.prev_log_term = Self::get_entry_for_retry(&storage, index - 1)?.0;
        }
        Ok(request)
    }

    fn retry_failed_append(&self, target_index: LogIndex, server_id: ServerId) -> AppendFuture {
        if target_index <= 1 {
            error!(
                "Server {} returned error on first entry! Giving up.",
                server_id
            );
            return Box::new(future::err(AppendRequestError::RetryUnsuccessful));
        }
        let index = target_index - 1;
        trace!("retry_failed_append: index={}", index);

        let request = match self.build_retry_request(index) {
            Ok(r) => r,
            Err(e) => return Box::new(future::err(e)),
        };

        let call = self.issue_append_request(
            &request,
            self.rpc.clients.get(&server_id).unwrap(),
            server_id,
            index,
            false,
        );
        if call.is_none() {
            return Box::new(future::err(AppendRequestError::RetryUnsuccessful));
        }

        let server = self.weak_self.clone();
        let serverr = self.weak_self.clone();
        Box::new(
            call.unwrap()
                .and_then(move |(server_id, resp)| -> AppendFuture {
                    upgrade_or_return!(server, Box::new(future::empty()));
                    if !resp.success {
                        debug!(
                            "Retrying failed append for server_id={} log_index={}",
                            server_id, index
                        );
                        return server.retry_failed_append(index, server_id);
                    }
                    Box::new(future::ok((server_id, resp)))
                })
                .and_then(move |(server_id, resp)| -> AppendFuture {
                    if !resp.success {
                        error!(
                            "Failed retry to append server_id={} log_index={}",
                            server_id, index
                        );
                        return Box::new(future::err(AppendRequestError::RetryUnsuccessful));
                    }
                    trace!(
                        "retry_failed_append: success for index={}, moving to {}",
                        index,
                        target_index
                    );

                    upgrade_or_return!(serverr, Box::new(future::empty()));
                    let request = match serverr.build_retry_request(target_index) {
                        Ok(r) => r,
                        Err(e) => return Box::new(future::err(e)),
                    };

                    match serverr.issue_append_request(
                        &request,
                        serverr.rpc.clients.get(&server_id).unwrap(),
                        server_id,
                        target_index,
                        false,
                    ) {
                        Some(f) => f,
                        None => Box::new(future::err(AppendRequestError::RetryUnsuccessful)),
                    }
                })
                .then(move |r| {
                    let (server_id, resp) = r?;
                    if !resp.success {
                        error!(
                            "Failed retry to append server_id={} log_index={}",
                            server_id, target_index
                        );
                        return Err(AppendRequestError::RetryUnsuccessful);
                    }
                    Ok((server_id, resp))
                }),
        )
    }

    fn append_request(&self, entries: Vec<(Term, Vec<u8>)>) -> protos::AppendRequest {
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
        req.set_entries(
            entries
                .into_iter()
                .map(|e| {
                    let mut entry = protos::LogEntry::new();
                    entry.term = e.0;
                    entry.data = e.1;
                    entry
                })
                .collect(),
        );
        req
    }
}
