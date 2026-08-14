//! Phase 14 cleanup: the daemon/application layer owns safe removal.
//!
//! The CLI never touches git worktrees or branches directly. Everything flows
//! through the daemon, which first asserts there is no live owner — a live
//! task in [`crate::registry::LiveTaskRegistry`], an active
//! [`crate::lease::SessionLeaseManager`] lease, or a running/interrupted
//! workflow dependency — and then delegates to
//! [`agentmesh_workspace::WorkspaceManager::plan_cleanup`] /
//! [`agentmesh_workspace::WorkspaceManager::cleanup`].

use std::collections::HashSet;

use agentmesh_orchestrator::WorkflowStatus;
use agentmesh_workspace::{CleanupContext, CleanupOutcome, CleanupPlan, Workspace, WorkspaceError};
use uuid::Uuid;

use crate::server::SharedState;

/// Errors produced by the cleanup layer.
#[derive(Debug, thiserror::Error)]
pub enum CleanupError {
    #[error("task `{0}` not found")]
    TaskNotFound(Uuid),

    #[error("task `{0}` has no agent session / workspace to clean up")]
    TaskHasNoWorkspace(Uuid),

    #[error("workflow `{0}` not found")]
    WorkflowNotFound(Uuid),

    #[error("workflow `{0}` is still active (status `{1}`); cancel or wait before cleanup")]
    WorkflowStillActive(Uuid, String),

    #[error("workspace error: {0}")]
    Workspace(#[from] WorkspaceError),

    #[error("storage error: {0}")]
    Storage(#[from] agentmesh_storage::StorageError),

    #[error("task error: {0}")]
    Task(#[from] agentmesh_tasks::TaskError),

    #[error("internal error: {0}")]
    Internal(String),
}

/// Resolve a task's isolated workspace: `Task → AgentSession → Workspace`.
pub async fn resolve_workspace_for_task(
    state: &SharedState,
    task_id: Uuid,
) -> Result<Workspace, CleanupError> {
    let task = state
        .task_manager
        .get_task(task_id)
        .await?
        .ok_or(CleanupError::TaskNotFound(task_id))?;
    let session_id = task
        .agent_session_id
        .ok_or(CleanupError::TaskHasNoWorkspace(task_id))?;
    match state.workspaces.workspace_for_session(session_id).await {
        Ok(workspace) => Ok(workspace),
        Err(WorkspaceError::WorkspaceNotFound(_)) => Err(CleanupError::TaskHasNoWorkspace(task_id)),
        Err(err) => Err(err.into()),
    }
}

/// The external facts the daemon owns: live task, session lease, workflow
/// dependency — all required to be absent before a cleanup.
async fn build_cleanup_context(
    state: &SharedState,
    session_id: Uuid,
) -> Result<CleanupContext, CleanupError> {
    let live = state.registry.list().await;
    let has_live_task = live
        .iter()
        .any(|(_, _, session, status)| session == &session_id && !status.is_terminal());
    let has_session_lease = state.leases.is_leased(session_id);
    let has_workflow_dependency = state
        .workflows_repo
        .has_active_dependency_on_session(session_id)
        .await?;
    Ok(CleanupContext {
        has_live_task,
        has_session_lease,
        has_workflow_dependency,
        archive_only: false,
    })
}

// ---------- task-level ----------

/// Preflight (never deletes) a cleanup of a task's workspace.
pub async fn plan_cleanup_task(
    state: &SharedState,
    task_id: Uuid,
) -> Result<CleanupPlan, CleanupError> {
    let workspace = resolve_workspace_for_task(state, task_id).await?;
    let context = build_cleanup_context(state, workspace.agent_session_id).await?;
    Ok(state
        .workspaces
        .plan_cleanup(workspace.id, &state.applies, &context)
        .await?)
}

/// Archive a task's workspace: `state → Archived`, files and branch kept.
pub async fn archive_task(state: &SharedState, task_id: Uuid) -> Result<(), CleanupError> {
    let workspace = resolve_workspace_for_task(state, task_id).await?;
    Ok(state.workspaces.archive(workspace.id).await?)
}

/// Clean up a task's workspace after the full preflight.
pub async fn cleanup_task(
    state: &SharedState,
    task_id: Uuid,
) -> Result<CleanupOutcome, CleanupError> {
    let workspace = resolve_workspace_for_task(state, task_id).await?;
    let context = build_cleanup_context(state, workspace.agent_session_id).await?;
    Ok(state
        .workspaces
        .cleanup(workspace.id, &state.applies, &context)
        .await?)
}

// ---------- workflow-level ----------

/// All distinct workspaces used by a completed/failed/cancelled workflow's
/// steps. A workflow that is still running or interrupted refuses cleanup.
async fn resolve_workflow_workspaces(
    state: &SharedState,
    workflow_id: Uuid,
) -> Result<Vec<Workspace>, CleanupError> {
    let row = state
        .workflows_repo
        .get(workflow_id)
        .await?
        .ok_or(CleanupError::WorkflowNotFound(workflow_id))?;
    let status = WorkflowStatus::from_str(&row.status).ok_or_else(|| {
        CleanupError::Internal(format!("unknown workflow status `{}`", row.status))
    })?;
    if matches!(
        status,
        WorkflowStatus::Running | WorkflowStatus::Interrupted
    ) {
        return Err(CleanupError::WorkflowStillActive(workflow_id, row.status));
    }

    let steps = state.steps.list_for(workflow_id).await?;
    let mut seen = HashSet::new();
    let mut workspaces = Vec::new();
    for step in steps {
        let Some(task_id) = step.task_id else {
            continue;
        };
        let Ok(Some(task)) = state.task_manager.get_task(task_id).await else {
            continue;
        };
        let Some(session_id) = task.agent_session_id else {
            continue;
        };
        if !seen.insert(session_id) {
            continue;
        }
        if let Ok(workspace) = state.workspaces.workspace_for_session(session_id).await {
            workspaces.push(workspace);
        }
    }
    Ok(workspaces)
}

/// Preflight a cleanup of every workspace a workflow used.
pub async fn plan_cleanup_workflow(
    state: &SharedState,
    workflow_id: Uuid,
) -> Result<Vec<CleanupPlan>, CleanupError> {
    let workspaces = resolve_workflow_workspaces(state, workflow_id).await?;
    let mut plans = Vec::new();
    for workspace in workspaces {
        let context = build_cleanup_context(state, workspace.agent_session_id).await?;
        plans.push(
            state
                .workspaces
                .plan_cleanup(workspace.id, &state.applies, &context)
                .await?,
        );
    }
    Ok(plans)
}

/// Clean up every workspace a workflow used. The full preflight runs first:
/// if any workspace is unsafe, nothing is removed.
pub async fn cleanup_workflow(
    state: &SharedState,
    workflow_id: Uuid,
) -> Result<Vec<CleanupOutcome>, CleanupError> {
    let workspaces = resolve_workflow_workspaces(state, workflow_id).await?;
    // Complete preflight before deleting anything.
    for workspace in &workspaces {
        let context = build_cleanup_context(state, workspace.agent_session_id).await?;
        state
            .workspaces
            .plan_cleanup(workspace.id, &state.applies, &context)
            .await?;
    }
    let mut outcomes = Vec::new();
    for workspace in workspaces {
        let context = build_cleanup_context(state, workspace.agent_session_id).await?;
        outcomes.push(
            state
                .workspaces
                .cleanup(workspace.id, &state.applies, &context)
                .await?,
        );
    }
    Ok(outcomes)
}
