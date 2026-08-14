//! ApplyManager (Phase 13): safely apply a workspace's reviewed changes back
//! to the user's source repository.
//!
//! The manager is the only layer that writes agent results into the source
//! repository. The CLI and orchestrator never run git themselves; everything
//! flows through here:
//!
//! ```text
//! CLI / Daemon
//!   ↓
//! ApplyManager::plan / ::apply
//!   ↓
//! WorkspaceManager + git abstraction
//!   ↓
//! source repository
//! ```
//!
//! Apply is not commit: the source working tree becomes `base + agent
//! changes` while its HEAD and the agent worktree stay untouched. The user
//! reviews and commits.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use agentmesh_core::provenance::{ApplyCompletedPayload, actor_type, entity_type, event_type};
use agentmesh_orchestrator::{
    PersistedStepResult, ReviewVerdict, WorkflowRole, WorkflowStatus, WorkflowStepStatus,
};
use agentmesh_storage::{
    ApplyRepository, ApplyRow, ApplyStatus, ClaimResult, TaskRepository, WorkflowRepository,
    WorkflowStepRepository, WorkflowStepRow, WorkspaceState,
};
use agentmesh_workspace::git::git;
use agentmesh_workspace::{
    Workspace, WorkspaceDiff, WorkspaceError, WorkspaceManager, workspace_snapshot_hash,
};
use uuid::Uuid;

use crate::error::ApplyError;
use crate::model::{ApplyOutcome, ApplyPlan, PlannedFile};
use crate::path::{expand_untracked, validate_untracked_file};

/// A resolved apply source: which workspace holds the agent changes and what
/// to record on the persisted apply row.
#[derive(Debug, Clone)]
struct ResolvedSource {
    workspace: Workspace,
    task_id: Option<Uuid>,
    workflow_id: Option<Uuid>,
}

/// Applies workspace results to source repositories.
#[derive(Clone)]
pub struct ApplyManager {
    tasks: TaskRepository,
    workspaces: Arc<WorkspaceManager>,
    workflows: WorkflowRepository,
    steps: WorkflowStepRepository,
    applies: ApplyRepository,
    competitions: Option<agentmesh_storage::CompetitionRepository>,
    provenance: Option<agentmesh_storage::ProvenanceRepository>,
}

impl ApplyManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tasks: TaskRepository,
        workspaces: Arc<WorkspaceManager>,
        workflows: WorkflowRepository,
        steps: WorkflowStepRepository,
        applies: ApplyRepository,
    ) -> Self {
        Self {
            tasks,
            workspaces,
            workflows,
            steps,
            applies,
            competitions: None,
            provenance: None,
        }
    }

    /// Attach competition repository for Best-of-N winner resolution (Phase 23).
    pub fn with_competitions(
        mut self,
        competitions: agentmesh_storage::CompetitionRepository,
    ) -> Self {
        self.competitions = Some(competitions);
        self
    }

    /// Attach provenance repository for audit trail (Phase 24).
    pub fn with_provenance(mut self, provenance: agentmesh_storage::ProvenanceRepository) -> Self {
        self.provenance = Some(provenance);
        self
    }

    // ---------- plan (preflight only, never writes to the source) ----------

    /// Plan an apply for a task's workspace: `Task → AgentSession → Workspace`.
    pub async fn plan_task(&self, task_id: Uuid) -> Result<ApplyPlan, ApplyError> {
        let source = self.resolve_task_source(task_id).await?;
        self.build_plan(&source).await
    }

    /// Plan an apply for a completed workflow's implementer/fixer workspace.
    pub async fn plan_workflow(&self, workflow_id: Uuid) -> Result<ApplyPlan, ApplyError> {
        let source = self.resolve_workflow_source(workflow_id).await?;
        self.build_plan(&source).await
    }

    // ---------- apply (preflight + write) ----------

    /// Preflight + apply a task's workspace result to the source repository.
    pub async fn apply_task(&self, task_id: Uuid) -> Result<ApplyOutcome, ApplyError> {
        let source = self.resolve_task_source(task_id).await?;
        self.execute(&source).await
    }

    /// Preflight + apply a completed workflow's implementer/fixer workspace.
    pub async fn apply_workflow(&self, workflow_id: Uuid) -> Result<ApplyOutcome, ApplyError> {
        let source = self.resolve_workflow_source(workflow_id).await?;
        self.execute(&source).await
    }

    // ---------- source resolution ----------

    async fn resolve_task_source(&self, task_id: Uuid) -> Result<ResolvedSource, ApplyError> {
        let task = self
            .tasks
            .get(task_id)
            .await?
            .ok_or(ApplyError::TaskNotFound(task_id))?;
        let session_id = task
            .agent_session_id
            .ok_or(ApplyError::TaskHasNoSession(task_id))?;
        let workspace = match self.workspaces.workspace_for_session(session_id).await {
            Ok(workspace) => workspace,
            Err(WorkspaceError::WorkspaceNotFound(_)) => {
                return Err(ApplyError::TaskHasNoWorkspace(task_id));
            }
            Err(err) => return Err(err.into()),
        };
        Ok(ResolvedSource {
            workspace,
            task_id: Some(task_id),
            workflow_id: None,
        })
    }

    /// Resolve a workflow's apply source by role, never by agent brand.
    ///
    /// Role-based selection (Phase 13 section 14): the last *Completed*
    /// `Fixer` step's workspace, falling back to the last *Completed*
    /// `Implementer` step's workspace. Reviewer and FinalReviewer workspaces
    /// are never selected.
    ///
    /// Phase 16: for DAG workflows (those with dependency edges), the "last"
    /// code node is the unique *maximal* completed code node — one that no
    /// other completed code node transitively depends on. Parallel code nodes
    /// with no unique source are rejected as [`ApplyError::AmbiguousApplySource`].
    async fn resolve_workflow_source(
        &self,
        workflow_id: Uuid,
    ) -> Result<ResolvedSource, ApplyError> {
        let row = self
            .workflows
            .get(workflow_id)
            .await?
            .ok_or(ApplyError::WorkflowNotFound(workflow_id))?;
        let status = WorkflowStatus::from_str(&row.status).ok_or_else(|| {
            ApplyError::Internal(format!("unknown workflow status `{}`", row.status))
        })?;
        if status != WorkflowStatus::Completed {
            return Err(ApplyError::WorkflowNotCompleted(workflow_id, row.status));
        }

        // Phase 23: for Best-of-N competition workflows, the unique apply source
        // is strictly the persisted Winner workspace. Evaluator and loser
        // workspaces are never applied.
        if let Some(competitions) = &self.competitions
            && let Ok(Some(group)) = competitions.get_group_for_workflow(workflow_id).await
        {
            if let Some(winner_task_id) = group.winner_task_id {
                let task = self
                    .tasks
                    .get(winner_task_id)
                    .await
                    .map_err(ApplyError::Storage)?
                    .ok_or(ApplyError::AmbiguousApplySource(workflow_id))?;
                let session_id = task
                    .agent_session_id
                    .ok_or(ApplyError::AmbiguousApplySource(workflow_id))?;
                let workspace = match self.workspaces.workspace_for_session(session_id).await {
                    Ok(workspace) => workspace,
                    Err(WorkspaceError::WorkspaceNotFound(_)) => {
                        return Err(ApplyError::TaskHasNoWorkspace(winner_task_id));
                    }
                    Err(err) => return Err(err.into()),
                };
                return Ok(ResolvedSource {
                    workspace,
                    task_id: Some(winner_task_id),
                    workflow_id: Some(workflow_id),
                });
            } else {
                return Err(ApplyError::AmbiguousApplySource(workflow_id));
            }
        }

        let steps = self.steps.list_for(workflow_id).await?;

        // A completed workflow must not carry an unapproved review.
        self.verify_review_approved(workflow_id, &steps)?;

        // Completed code nodes (Fixer preferred, then Implementer), in the
        // persisted order (topological for DAGs).
        let code_nodes: Vec<&WorkflowStepRow> = steps
            .iter()
            .filter(|s| s.status == WorkflowStepStatus::Completed.as_str())
            .filter(|s| {
                WorkflowRole::from_str(&s.role) == Some(WorkflowRole::Fixer)
                    || WorkflowRole::from_str(&s.role) == Some(WorkflowRole::Implementer)
            })
            .collect();
        if code_nodes.is_empty() {
            return Err(ApplyError::AmbiguousApplySource(workflow_id));
        }

        // DAG workflow (identified by node_id rows): the unique source is the
        // unique maximal code node. Parallel code nodes → AmbiguousApplySource.
        let is_dag = steps.iter().any(|s| s.node_id.is_some());
        if is_dag {
            let dependencies = self.steps.list_dependencies(workflow_id).await?;
            let source_row =
                self.unique_dag_code_source(workflow_id, &dependencies, &code_nodes)?;
            return self.resolve_source_from_step(workflow_id, source_row).await;
        }

        // Sequential workflow: the last completed code node wins.
        let source_step = code_nodes.last().expect("non-empty");
        self.resolve_source_from_step(workflow_id, source_step)
            .await
    }

    /// The unique maximal completed code node of a DAG, or `AmbiguousApplySource`
    /// when more than one parallel code node has no successor.
    fn unique_dag_code_source<'a>(
        &self,
        workflow_id: Uuid,
        dependencies: &[agentmesh_storage::WorkflowStepDependencyRow],
        code_nodes: &[&'a WorkflowStepRow],
    ) -> Result<&'a WorkflowStepRow, ApplyError> {
        // Adjacency: node -> set of nodes that depend on it.
        let mut dependents: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for dep in dependencies {
            dependents
                .entry(dep.depends_on_node_id.clone())
                .or_default()
                .push(dep.node_id.clone());
        }
        // A code node X is a "prerequisite" of code node Y if Y transitively
        // depends on X. The maximal code nodes are those that are not a
        // transitive dependency of any *other* completed code node.
        let code_ids: std::collections::HashSet<String> = code_nodes
            .iter()
            .filter_map(|s| s.node_id.clone())
            .collect();
        let maximal: Vec<&WorkflowStepRow> = code_nodes
            .iter()
            .filter(|node| {
                let id = match &node.node_id {
                    Some(id) => id.clone(),
                    None => return true, // legacy row: no edges, keep ordinal fallback
                };
                !transitively_depends_on(&dependents, &id, &code_ids)
            })
            .copied()
            .collect();
        match maximal.len() {
            1 => Ok(maximal[0]),
            0 => Err(ApplyError::AmbiguousApplySource(workflow_id)),
            _ => Err(ApplyError::AmbiguousApplySource(workflow_id)),
        }
    }

    /// Resolve a workspace from a chosen code node's task + session.
    async fn resolve_source_from_step(
        &self,
        workflow_id: Uuid,
        source_step: &WorkflowStepRow,
    ) -> Result<ResolvedSource, ApplyError> {
        let task_id = source_step
            .task_id
            .ok_or(ApplyError::AmbiguousApplySource(workflow_id))?;
        let task = self
            .tasks
            .get(task_id)
            .await?
            .ok_or(ApplyError::TaskNotFound(task_id))?;
        let session_id = task
            .agent_session_id
            .ok_or(ApplyError::TaskHasNoSession(task_id))?;
        let workspace = match self.workspaces.workspace_for_session(session_id).await {
            Ok(workspace) => workspace,
            Err(WorkspaceError::WorkspaceNotFound(_)) => {
                return Err(ApplyError::AmbiguousApplySource(workflow_id));
            }
            Err(err) => return Err(err.into()),
        };
        Ok(ResolvedSource {
            workspace,
            task_id: None,
            workflow_id: Some(workflow_id),
        })
    }

    /// Workflow policy (Phase 13 section 11): if a review ran, the last review
    /// verdict must be `Approved`; `ChangesRequested` (or a missing verdict on
    /// a completed review step) rejects the apply.
    ///
    /// Phase 22: evaluator member votes are NOT the group's verdict — a
    /// consensus-review's final authority is its ConsensusGate. In node-id
    /// order the evaluators sort after the gate, so they are skipped here;
    /// otherwise a `changes_requested` evaluator vote would wrongly block an
    /// apply that the final gate approved.
    fn verify_review_approved(
        &self,
        workflow_id: Uuid,
        steps: &[WorkflowStepRow],
    ) -> Result<(), ApplyError> {
        for step in steps.iter().rev() {
            if step.status != WorkflowStepStatus::Completed.as_str() {
                continue;
            }
            let Some(role) = WorkflowRole::from_str(&step.role) else {
                continue;
            };
            if !role.is_reviewer() || role == WorkflowRole::Evaluator {
                continue;
            }
            let verdict = step
                .result_json
                .as_deref()
                .and_then(|json| serde_json::from_str::<PersistedStepResult>(json).ok())
                .and_then(|r| r.review_result)
                .map(|r| r.verdict)
                .ok_or_else(|| {
                    ApplyError::Internal(format!(
                        "completed review step has no verdict (workflow {workflow_id})"
                    ))
                })?;
            if verdict != ReviewVerdict::Approved {
                return Err(ApplyError::ReviewNotApproved(workflow_id));
            }
            return Ok(());
        }
        // No review step ran; nothing to approve.
        Ok(())
    }

    // ---------- preflight ----------

    /// Full preflight, in order (Phase 13 section 7): idempotency, source
    /// exists, source clean, HEAD == workspace base, patch `--check`, every
    /// untracked path safe, every destination absent, every source present.
    ///
    /// Never writes to the source repository.
    async fn build_plan(&self, source: &ResolvedSource) -> Result<ApplyPlan, ApplyError> {
        let workspace = &source.workspace;
        let source_root = &workspace.repository_root;

        // Idempotency: a completed apply of this workspace is viewable via
        // --check but never re-applied (and the source legitimately contains
        // the changes, so the normal clean-at-base validation would misreport
        // it as dirty).
        let already_applied = self
            .applies
            .has_completed_for_workspace(workspace.id)
            .await?;
        let mut warnings = Vec::new();
        let mut applicable = true;
        if already_applied {
            warnings.push("this workspace's result has already been applied".to_string());
            applicable = false;
        }

        let source_revision;
        if !already_applied {
            if !source_root.is_dir() {
                return Err(ApplyError::SourceRepositoryMissing(
                    source_root.display().to_string(),
                ));
            }
            if !self.workspaces.is_clean(source_root).await? {
                return Err(ApplyError::SourceRepositoryDirty);
            }
            source_revision = self.workspaces.base_revision(source_root).await?;
            if source_revision != workspace.base_revision {
                return Err(ApplyError::SourceRevisionChanged {
                    base: workspace.base_revision.clone(),
                    current: source_revision,
                });
            }
        } else {
            source_revision = self.workspaces.base_revision(source_root).await?;
        }

        let diff = self.workspaces.diff(workspace).await?;
        if diff.is_empty() {
            warnings.push("no changes since the base revision".to_string());
            applicable = false;
        }

        let changed_files = diff
            .changed_files
            .iter()
            .map(|file| PlannedFile {
                status: file.status.as_str().to_string(),
                path: file.path.display().to_string(),
            })
            .collect::<Vec<_>>();

        // Expand untracked directories, then validate every file: relative
        // path, source exists inside the workspace, destination absent inside
        // the source, no symlink escape on either side.
        let mut untracked_files = Vec::new();
        for entry in &diff.untracked_files {
            for file in expand_untracked(entry, &workspace.path) {
                let rel = file
                    .strip_prefix(&workspace.path)
                    .map_err(|_| ApplyError::UnsafeApplyPath(file.display().to_string()))?;
                validate_untracked_file(rel, &workspace.path, source_root)?;
                untracked_files.push(rel.display().to_string());
            }
        }

        // Tracked patch validation: `git apply --check` on a temp file.
        let has_patch = !diff.patch.trim().is_empty();
        if has_patch && applicable {
            let temp = write_patch(&diff.patch)?;
            let check = git(
                source_root,
                &["apply", "--check", temp.path().to_str().unwrap_or_default()],
            )
            .await?;
            if !check.success() {
                return Err(ApplyError::ApplyCheckFailed(
                    check.stderr.trim().to_string(),
                ));
            }
        }

        Ok(ApplyPlan {
            source_repository: source_root.clone(),
            workspace: workspace.path.clone(),
            base_revision: workspace.base_revision.clone(),
            source_revision,
            changed_files,
            untracked_files,
            patch_size: diff.patch.len() as u64,
            applicable,
            warnings,
            already_applied,
        })
    }

    // ---------- execution ----------

    /// Run the preflight, then write the tracked patch and untracked files,
    /// rolling back to the base on any failure.
    async fn execute(&self, source: &ResolvedSource) -> Result<ApplyOutcome, ApplyError> {
        let plan = self.build_plan(source).await?;
        if plan.already_applied {
            return Err(ApplyError::AlreadyApplied);
        }
        if !plan.applicable {
            return Err(ApplyError::NoChanges);
        }

        let workspace = &source.workspace;
        let source_root = &workspace.repository_root;
        let diff = self.workspaces.diff(workspace).await?;
        let snapshot_hash = workspace_snapshot_hash(&workspace.path, &diff);

        let row = ApplyRow {
            id: Uuid::new_v4(),
            task_id: source.task_id,
            workflow_id: source.workflow_id,
            workspace_id: workspace.id,
            source_repository: source_root.clone(),
            base_revision: workspace.base_revision.clone(),
            status: ApplyStatus::Applying,
            error: None,
            created_at: chrono::Utc::now().to_rfc3339(),
            completed_at: None,
            workspace_snapshot_hash: Some(snapshot_hash),
        };
        // Phase 14 P0: atomically claim the workspace. The partial UNIQUE
        // index guarantees one applying/completed apply per workspace; a
        // concurrent request is rejected, never raced through.
        match self.applies.claim_workspace(&row).await? {
            ClaimResult::Claimed => {}
            ClaimResult::AlreadyCompleted => return Err(ApplyError::AlreadyApplied),
            ClaimResult::InProgress => return Err(ApplyError::ApplyInProgress),
        }

        // 1. Tracked patch (git apply --check + git apply on a temp file).
        let mut tracked_applied = false;
        if !diff.patch.trim().is_empty() {
            if let Err(err) = self.apply_patch(source_root, &diff.patch).await {
                let _ = self
                    .applies
                    .mark_failed(row.id, &bound(&err.to_string()))
                    .await;
                return Err(err);
            }
            tracked_applied = true;
        }

        // 2. Untracked copies, rolling back on a partial failure.
        let untracked: Vec<PathBuf> = plan.untracked_files.iter().map(PathBuf::from).collect();
        let mut copied: Vec<PathBuf> = Vec::new();
        if let Err(err) = self
            .copy_untracked(&untracked, &workspace.path, source_root, &mut copied)
            .await
        {
            match self.rollback(source_root, &diff, &copied).await {
                Ok(()) => {
                    let _ = self
                        .applies
                        .mark_failed(row.id, &bound(&err.to_string()))
                        .await;
                    return Err(err);
                }
                Err(rollback_err) => {
                    let loud = format!(
                        "{err}; rollback also failed ({rollback_err}); the source repository may need manual recovery — inspect and revert it before continuing"
                    );
                    let _ = self.applies.mark_failed(row.id, &bound(&loud)).await;
                    return Err(ApplyError::ApplyRollbackFailed(loud));
                }
            }
        }

        self.applies.mark_completed(row.id).await?;
        // Phase 14: a successful apply moves the workspace to `Applied`. The
        // worktree, branch and artifacts all stay — apply is not cleanup.
        self.workspaces
            .repository()
            .set_state(workspace.id, WorkspaceState::Applied)
            .await?;

        if let Some(prov) = &self.provenance {
            let payload = serde_json::to_value(ApplyCompletedPayload {
                apply_id: row.id,
                workflow_id: row.workflow_id,
                source_workspace_id: Some(row.workspace_id),
                applied_commit: None,
                applied_files_count: plan.changed_files.len(),
                snapshot_hash: row.workspace_snapshot_hash.clone(),
            })
            .unwrap_or_default();

            let _ = prov
                .append_event(
                    row.workflow_id,
                    event_type::APPLY_COMPLETED,
                    entity_type::APPLY,
                    &row.id.to_string(),
                    None,
                    actor_type::SYSTEM,
                    Some("ApplyManager"),
                    &payload,
                )
                .await;
        }

        Ok(ApplyOutcome {
            apply_id: row.id,
            plan,
            tracked_applied,
            untracked_copied: copied.len(),
            workspace_snapshot_hash: row.workspace_snapshot_hash.unwrap_or_default(),
        })
    }

    /// Low-level tracked-patch apply: `git apply --check` then `git apply`.
    ///
    /// The patch is written to a temp file and never passed as a command-line
    /// argument (Phase 13 section 4). Exposed so callers and tests can drive a
    /// single raw patch through the same conflict detection.
    pub async fn apply_patch(&self, source_root: &Path, patch: &str) -> Result<(), ApplyError> {
        let temp = write_patch(patch)?;
        let path = temp.path().to_str().unwrap_or_default();
        let check = git(source_root, &["apply", "--check", path]).await?;
        if !check.success() {
            return Err(ApplyError::ApplyCheckFailed(
                check.stderr.trim().to_string(),
            ));
        }
        let apply = git(source_root, &["apply", path]).await?;
        if !apply.success() {
            return Err(ApplyError::ApplyFailed(apply.stderr.trim().to_string()));
        }
        Ok(())
    }

    /// Copy untracked files from the workspace into the source repository.
    /// Files copied so far are tracked in `copied` for rollback.
    async fn copy_untracked(
        &self,
        untracked: &[PathBuf],
        workspace_root: &Path,
        source_root: &Path,
        copied: &mut Vec<PathBuf>,
    ) -> Result<(), ApplyError> {
        for rel in untracked {
            // Re-validate immediately before the write (TOCTOU guard).
            validate_untracked_file(rel, workspace_root, source_root)?;
            let dst = source_root.join(rel);
            if let Some(parent) = dst.parent() {
                std::fs::create_dir_all(parent).map_err(|err| {
                    ApplyError::CopyFailed(rel.display().to_string(), err.to_string())
                })?;
            }
            std::fs::copy(workspace_root.join(rel), &dst).map_err(|err| {
                ApplyError::CopyFailed(rel.display().to_string(), err.to_string())
            })?;
            copied.push(rel.clone());
        }
        Ok(())
    }

    /// Roll back an interrupted apply: delete copied files, then reverse the
    /// tracked patch. A failure here means the source may need manual repair.
    async fn rollback(
        &self,
        source_root: &Path,
        diff: &WorkspaceDiff,
        copied: &[PathBuf],
    ) -> Result<(), ApplyError> {
        for rel in copied {
            std::fs::remove_file(source_root.join(rel)).map_err(|err| {
                ApplyError::Internal(format!(
                    "failed to remove copied file {}: {err}",
                    rel.display()
                ))
            })?;
        }
        if !diff.patch.trim().is_empty() {
            let temp = write_patch(&diff.patch)?;
            let out = git(
                source_root,
                &["apply", "-R", temp.path().to_str().unwrap_or_default()],
            )
            .await?;
            if !out.success() {
                return Err(ApplyError::Internal(format!(
                    "reverse apply failed: {}",
                    out.stderr.trim()
                )));
            }
        }
        Ok(())
    }
}

/// Whether `id` is a transitive dependency of any *other* code node — i.e. a
/// node reachable from `id` via the `dependents` map is a code node that is
/// not `id` itself. A maximal code node (not a prerequisite of another code
/// node) is the unique apply source.
fn transitively_depends_on(
    dependents: &std::collections::HashMap<String, Vec<String>>,
    id: &str,
    code_ids: &std::collections::HashSet<String>,
) -> bool {
    let mut seen = std::collections::HashSet::new();
    let mut stack = vec![id.to_string()];
    while let Some(cur) = stack.pop() {
        if !seen.insert(cur.clone()) {
            continue;
        }
        if cur != id && code_ids.contains(&cur) {
            return true;
        }
        if let Some(nexts) = dependents.get(&cur) {
            for next in nexts {
                stack.push(next.clone());
            }
        }
    }
    false
}

/// Write a patch to a temporary file so `git apply` never receives it through
/// the command line (large patches stay off argv).
fn write_patch(patch: &str) -> Result<tempfile::NamedTempFile, ApplyError> {
    let mut temp = tempfile::NamedTempFile::new()
        .map_err(|err| ApplyError::Internal(format!("cannot create temp patch file: {err}")))?;
    temp.write_all(patch.as_bytes())
        .map_err(|err| ApplyError::Internal(format!("cannot write temp patch file: {err}")))?;
    temp.flush()
        .map_err(|err| ApplyError::Internal(format!("cannot flush temp patch file: {err}")))?;
    Ok(temp)
}

/// Bound an error message persisted to the database.
fn bound(message: &str) -> String {
    const MAX: usize = 16 * 1024;
    if message.len() <= MAX {
        message.to_string()
    } else {
        format!("{}… (truncated)", &message[..MAX])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use std::process::Command as StdCommand;

    use agentmesh_storage::{Database, WorkspaceRepository};

    fn git(dir: &Path, args: &[&str]) {
        let status = StdCommand::new("git")
            .args(args)
            .current_dir(dir)
            .status()
            .expect("git runs");
        assert!(status.success(), "git {args:?} failed");
    }

    fn clean_repo() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().canonicalize().expect("canonicalize");
        git(&root, &["init", "-q"]);
        git(&root, &["config", "user.name", "AgentMesh Test"]);
        git(
            &root,
            &["config", "user.email", "agentmesh@example.invalid"],
        );
        std::fs::write(root.join("tracked.txt"), "base\n").expect("write");
        git(&root, &["add", "."]);
        git(&root, &["commit", "-q", "-m", "initial"]);
        (dir, root)
    }

    async fn test_manager() -> (ApplyManager, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Database::open(dir.path().join("agentmesh.db"))
            .await
            .expect("db");
        let workspaces = Arc::new(WorkspaceManager::new(
            WorkspaceRepository::new(db.clone()),
            dir.path().join("worktrees"),
        ));
        let manager = ApplyManager::new(
            TaskRepository::new(db.clone()),
            workspaces,
            WorkflowRepository::new(db.clone()),
            WorkflowStepRepository::new(db.clone()),
            ApplyRepository::new(db.clone()),
        );
        (manager, dir)
    }

    /// Rolling back an unappliable patch must surface an error: a patch that
    /// was never applied cannot be reversed, and the failure must not be
    /// silent (the caller reports that the source may need manual recovery).
    #[tokio::test]
    async fn rollback_failure_returns_error() {
        let (_repo, root) = clean_repo();
        let (manager, _dir) = test_manager().await;
        let add_ghost = "\
diff --git a/ghost.txt b/ghost.txt
new file mode 100644
index 0000000..e69de29
--- /dev/null
+++ b/ghost.txt
@@ -0,0 +1 @@
+boo
";
        let diff = WorkspaceDiff {
            patch: add_ghost.to_string(),
            changed_files: vec![],
            untracked_files: vec![],
        };
        // `ghost.txt` was never created, so reversing "add ghost.txt" fails.
        let err = manager.rollback(&root, &diff, &[]).await;
        let message = err.expect_err("rollback must fail").to_string();
        assert!(!message.is_empty(), "a failed rollback must not be silent");
    }
}
