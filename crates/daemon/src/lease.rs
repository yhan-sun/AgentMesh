//! SessionLeaseManager: one live task per agent session.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum LeaseError {
    #[error("agent session {session_id} is busy with task {active_task_id}")]
    SessionBusy {
        session_id: Uuid,
        active_task_id: Uuid,
    },
}

/// RAII lease guard: releases the lease on drop, terminal or not.
pub struct SessionLease {
    manager: Arc<SessionLeaseManager>,
    session_id: Uuid,
}

impl Drop for SessionLease {
    fn drop(&mut self) {
        self.manager.release(self.session_id);
    }
}

/// Guards concurrent use of the same agent session (and therefore its
/// worktree and native session).
#[derive(Default)]
pub struct SessionLeaseManager {
    leases: Mutex<HashMap<Uuid, Uuid>>,
}

impl SessionLeaseManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Acquire the lease for `session_id` bound to `task_id`.
    pub fn acquire(
        self: &Arc<Self>,
        session_id: Uuid,
        task_id: Uuid,
    ) -> Result<SessionLease, LeaseError> {
        let mut leases = self.leases.lock().unwrap();

        match leases.get(&session_id) {
            Some(active) if *active != task_id => Err(LeaseError::SessionBusy {
                session_id,
                active_task_id: *active,
            }),
            _ => {
                leases.insert(session_id, task_id);
                Ok(SessionLease {
                    manager: self.clone(),
                    session_id,
                })
            }
        }
    }

    pub fn release(&self, session_id: Uuid) {
        self.leases.lock().unwrap().remove(&session_id);
    }

    /// Whether a session currently holds an active lease (Phase 14 cleanup
    /// guard).
    pub fn is_leased(&self, session_id: Uuid) -> bool {
        self.leases.lock().unwrap().contains_key(&session_id)
    }
}
