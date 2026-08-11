//! LiveTaskRegistry: daemon-memory runtime handles with bounded replay.

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use agentmesh_core::TaskStatus;
use agentmesh_tasks::{TaskError, TaskManager};
use tokio::sync::{RwLock, broadcast};
use tracing::instrument;
use uuid::Uuid;

use crate::protocol::DaemonStreamEvent;

/// Bounded replay buffer of stream events (daemon memory only).
pub struct ReplayBuffer {
    events: VecDeque<(u64, DaemonStreamEvent)>,
    next_seq: u64,
    max_events: usize,
    max_bytes: usize,
    bytes: usize,
}

impl ReplayBuffer {
    pub fn new(max_events: usize, max_bytes: usize) -> Self {
        Self {
            events: VecDeque::new(),
            next_seq: 1,
            max_events,
            max_bytes,
            bytes: 0,
        }
    }

    /// Push an event, returning its sequence number. Drops oldest when full.
    pub fn push(&mut self, event: DaemonStreamEvent) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        let size = serde_json::to_string(&event).map(|s| s.len()).unwrap_or(64);
        self.events.push_back((seq, event));
        self.bytes += size;
        while self.events.len() > self.max_events || self.bytes > self.max_bytes {
            if let Some((_, dropped)) = self.events.pop_front() {
                self.bytes = self.bytes.saturating_sub(
                    serde_json::to_string(&dropped)
                        .map(|s| s.len())
                        .unwrap_or(64),
                );
            }
        }
        seq
    }

    /// Oldest available sequence number.
    pub fn oldest_available(&self) -> u64 {
        self.events
            .front()
            .map(|(seq, _)| *seq)
            .unwrap_or(self.next_seq)
    }

    /// Events with sequence strictly greater than `after`, in order.
    pub fn replay_after(&self, after: u64) -> Vec<(u64, DaemonStreamEvent)> {
        self.events
            .iter()
            .filter(|(seq, _)| *seq > after)
            .cloned()
            .collect()
    }

    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }
}

/// A live task owned by the daemon.
pub struct LiveTask {
    pub task_id: Uuid,
    pub context_id: Uuid,
    pub agent_session_id: Uuid,
    pub agent_id: String,
    pub status: RwLock<TaskStatus>,
    pub replay: RwLock<ReplayBuffer>,
    pub broadcaster: broadcast::Sender<(u64, DaemonStreamEvent)>,
    pub manager: TaskManager,
    pub run_id: Uuid,
}

impl LiveTask {
    pub fn subscribe(&self) -> broadcast::Receiver<(u64, DaemonStreamEvent)> {
        self.broadcaster.subscribe()
    }

    pub async fn replay_after(&self, after: u64) -> Vec<(u64, DaemonStreamEvent)> {
        self.replay.read().await.replay_after(after)
    }

    pub async fn oldest_available(&self) -> u64 {
        self.replay.read().await.oldest_available()
    }

    /// Push an event into the replay buffer and broadcast it.
    pub async fn push(&self, event: DaemonStreamEvent) {
        let seq = {
            let mut replay = self.replay.write().await;
            replay.push(event.clone())
        };
        let _ = self.broadcaster.send((seq, event));
    }

    /// Real cancel: kills the underlying agent process via the TaskManager.
    #[instrument(skip_all, fields(task_id = %self.task_id))]
    pub async fn cancel(&self) -> Result<(), TaskError> {
        self.manager.cancel_run(&self.agent_id, &self.run_id).await
    }
}

/// Daemon-memory registry of live tasks.
#[derive(Clone, Default)]
pub struct LiveTaskRegistry {
    tasks: Arc<RwLock<HashMap<Uuid, Arc<LiveTask>>>>,
}

impl LiveTaskRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn insert(&self, task: Arc<LiveTask>) {
        self.tasks.write().await.insert(task.task_id, task);
    }

    pub async fn get(&self, task_id: Uuid) -> Option<Arc<LiveTask>> {
        self.tasks.read().await.get(&task_id).cloned()
    }

    pub async fn remove(&self, task_id: Uuid) {
        self.tasks.write().await.remove(&task_id);
    }

    /// Live tasks with their current status.
    pub async fn list(&self) -> Vec<(Uuid, String, Uuid, TaskStatus)> {
        let mut out = Vec::new();
        for task in self.tasks.read().await.values() {
            let status = *task.status.read().await;
            out.push((
                task.task_id,
                task.agent_id.clone(),
                task.agent_session_id,
                status,
            ));
        }
        out
    }

    pub async fn live_count(&self) -> usize {
        self.tasks.read().await.len()
    }
}
