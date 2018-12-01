//! Defines the behavior of the leader.

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

        // TODO
        debug!("TODO: Send heartbeat");

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
