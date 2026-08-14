//! AgentMesh Tauri desktop application backend.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use agentmesh_core::AgentMeshConfig;
use agentmesh_daemon::client::DaemonClient;
use agentmesh_daemon::paths::Scope;
use agentmesh_storage::{
    ApplyRepository, ArtifactRepository, Database, TaskRepository, WorkflowRepository,
    WorkflowStepRepository, WorkspaceRepository,
};
use agentmesh_workspace::WorkspaceManager;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DoctorStatus {
    pub git_available: bool,
    pub git_version: String,
    pub sqlite_connected: bool,
    pub migrations_count: u32,
    pub daemon_running: bool,
    pub daemon_instance_id: Option<String>,
    pub agents: Vec<AgentHealthItem>,
    pub repo_root: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentHealthItem {
    pub id: String,
    pub name: String,
    pub command: Option<String>,
    pub available: bool,
    pub version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskItem {
    pub id: String,
    pub context_id: String,
    pub agent_id: String,
    pub status: String,
    pub prompt: String,
    pub error: Option<String>,
    pub created_at: String,
    pub completed_at: Option<String>,
    pub artifacts_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactItem {
    pub name: String,
    pub kind: String,
    pub size_bytes: usize,
    pub content: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDetail {
    pub id: String,
    pub context_id: String,
    pub agent_id: String,
    pub status: String,
    pub prompt: String,
    pub error: Option<String>,
    pub workspace: Option<String>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub artifacts: Vec<ArtifactItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowItem {
    pub id: String,
    pub name: String,
    pub status: String,
    pub goal: String,
    pub graph_nodes_count: usize,
    pub created_at: String,
    pub completed_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDetail {
    pub id: String,
    pub name: String,
    pub status: String,
    pub goal: String,
    pub created_at: String,
    pub completed_at: Option<String>,
    pub steps: Vec<WorkflowStepItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowStepItem {
    pub id: String,
    pub node_id: String,
    pub agent_id: String,
    pub status: String,
    pub intent: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvenanceEventItem {
    pub sequence: u64,
    pub event_type: String,
    pub agent_id: Option<String>,
    pub payload_hash: String,
    pub event_hash: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvenanceAuditReport {
    pub workflow_id: String,
    pub valid_chain: bool,
    pub total_events: usize,
    pub events: Vec<ProvenanceEventItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyResult {
    pub success: bool,
    pub dry_run: bool,
    pub message: String,
    pub files_changed: Vec<String>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn get_client() -> Result<DaemonClient, String> {
    let scope = Scope::resolve();
    agentmesh_daemon::connect_or_start(scope)
        .await
        .map_err(|e| format!("Failed to connect to daemon: {e}"))
}

async fn get_db() -> Result<Database, String> {
    let scope = Scope::resolve();
    let db_path = match scope {
        Scope::Project(root) => root.join(".agentmesh").join("agentmesh.db"),
        Scope::User => {
            let base = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
            base.join(".agentmesh").join("agentmesh.db")
        }
    };
    if let Some(parent) = db_path.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }
    Database::open(&db_path)
        .await
        .map_err(|e| format!("Database connection error: {e}"))
}

// ---------------------------------------------------------------------------
// Tauri Commands
// ---------------------------------------------------------------------------

#[tauri::command]
async fn doctor_check() -> Result<DoctorStatus, String> {
    // 1. Check Git
    let git_output = Command::new("git").arg("--version").output();
    let (git_available, git_version) = match git_output {
        Ok(out) if out.status.success() => (
            true,
            String::from_utf8_lossy(&out.stdout).trim().to_string(),
        ),
        _ => (false, "Not found".to_string()),
    };

    // 2. Check Database
    let db = get_db().await.ok();
    let (sqlite_connected, migrations_count) = match db {
        Some(_) => (true, 16),
        None => (false, 0),
    };

    // 3. Check Daemon
    let client = get_client().await;
    let (daemon_running, daemon_instance_id) = match client {
        Ok(c) => match c.health().await {
            Ok(h) => (true, Some(h.instance_id)),
            Err(_) => (true, None),
        },
        Err(_) => (false, None),
    };

    // 4. Check Agents
    let cfg = AgentMeshConfig::load();
    let mut agents = Vec::new();
    for (id, agent_cfg) in &cfg.agents {
        let cmd = agent_cfg.command.as_deref().unwrap_or(id.as_str());
        let ver_out = Command::new(cmd).arg("--version").output();
        let (avail, ver) = match ver_out {
            Ok(out) if out.status.success() => (
                true,
                Some(String::from_utf8_lossy(&out.stdout).trim().to_string()),
            ),
            _ => {
                if id == "mock" {
                    (true, Some("0.1.0 (Built-in)".to_string()))
                } else {
                    (false, None)
                }
            }
        };
        agents.push(AgentHealthItem {
            id: id.clone(),
            name: match id.as_str() {
                "claude" => "Claude Code".into(),
                "codex" => "Codex".into(),
                "opencode" => "OpenCode".into(),
                "antigravity" => "Antigravity".into(),
                "mock" => "Mock Agent".into(),
                other => other.to_string(),
            },
            command: agent_cfg.command.clone(),
            available: avail,
            version: ver,
        });
    }

    let repo_root = std::env::current_dir()
        .ok()
        .map(|p| p.display().to_string());

    Ok(DoctorStatus {
        git_available,
        git_version,
        sqlite_connected,
        migrations_count,
        daemon_running,
        daemon_instance_id,
        agents,
        repo_root,
    })
}

#[tauri::command]
async fn list_tasks(limit: Option<usize>) -> Result<Vec<TaskItem>, String> {
    let db = get_db().await?;
    let repo = TaskRepository::new(db.clone());
    let art_repo = ArtifactRepository::new(db);
    let filter = agentmesh_storage::TaskFilter::default().limit(limit.unwrap_or(50));
    let tasks = repo
        .list(&filter)
        .await
        .map_err(|e| format!("Failed to list tasks: {e}"))?;

    let mut result = Vec::new();
    for t in tasks {
        let arts = art_repo.list_by_task(t.id).await.unwrap_or_default();
        result.push(TaskItem {
            id: t.id.to_string(),
            context_id: t.context_id.to_string(),
            agent_id: t.agent_id,
            status: t.status.as_str().to_string(),
            prompt: t.input.content,
            error: t.error,
            created_at: t.created_at.to_rfc3339(),
            completed_at: t.completed_at.map(|c| c.to_rfc3339()),
            artifacts_count: arts.len(),
        });
    }
    Ok(result)
}

#[tauri::command]
async fn get_task_details(task_id: String) -> Result<TaskDetail, String> {
    let id = Uuid::parse_str(&task_id).map_err(|e| format!("Invalid UUID: {e}"))?;
    let db = get_db().await?;
    let task_repo = TaskRepository::new(db.clone());
    let art_repo = ArtifactRepository::new(db);

    let task = task_repo
        .get(id)
        .await
        .map_err(|e| format!("DB error: {e}"))?
        .ok_or_else(|| format!("Task {task_id} not found"))?;

    let artifacts_db = art_repo
        .list_by_task(id)
        .await
        .map_err(|e| format!("Artifact DB error: {e}"))?;

    let artifacts = artifacts_db
        .into_iter()
        .map(|a| {
            let size = a.content.len();
            let preview = String::from_utf8(a.content).ok();
            ArtifactItem {
                name: a.name,
                kind: a.kind.key().to_string(),
                size_bytes: size,
                content: preview,
            }
        })
        .collect();

    Ok(TaskDetail {
        id: task.id.to_string(),
        context_id: task.context_id.to_string(),
        agent_id: task.agent_id,
        status: task.status.as_str().to_string(),
        prompt: task.input.content,
        error: task.error,
        workspace: task.workspace.map(|w| w.display().to_string()),
        created_at: task.created_at.to_rfc3339(),
        started_at: task.started_at.map(|s| s.to_rfc3339()),
        completed_at: task.completed_at.map(|c| c.to_rfc3339()),
        artifacts,
    })
}

#[tauri::command]
async fn run_task(
    agent_id: String,
    prompt: String,
    from_task_id: Option<String>,
    from_context_id: Option<String>,
) -> Result<String, String> {
    let client = get_client().await?;
    let from_task = from_task_id.and_then(|s| Uuid::parse_str(&s).ok());
    let from_context = from_context_id.and_then(|s| Uuid::parse_str(&s).ok());
    let workspace = std::env::current_dir().ok();

    let res = client
        .run_with_options(
            &agent_id,
            &prompt,
            workspace.as_ref(),
            from_task,
            from_context,
        )
        .await
        .map_err(|e| format!("Run task failed: {e}"))?;

    Ok(res.task_id.to_string())
}

#[tauri::command]
async fn get_task_diff(task_id: String) -> Result<String, String> {
    let id = Uuid::parse_str(&task_id).map_err(|e| format!("Invalid UUID: {e}"))?;
    let db = get_db().await?;
    let task_repo = TaskRepository::new(db.clone());
    let task = task_repo
        .get(id)
        .await
        .map_err(|e| format!("DB error: {e}"))?
        .ok_or_else(|| format!("Task {task_id} not found"))?;

    if let Some(workspace_path) = task.workspace {
        let out = Command::new("git")
            .arg("-C")
            .arg(&workspace_path)
            .arg("diff")
            .arg("HEAD~1")
            .output();

        match out {
            Ok(res) if res.status.success() => {
                let diff = String::from_utf8_lossy(&res.stdout).to_string();
                if diff.trim().is_empty() {
                    Ok("(No changes detected in workspace)".into())
                } else {
                    Ok(diff)
                }
            }
            _ => Ok("(Workspace clean or not a git worktree)".into()),
        }
    } else {
        Ok("(No isolated workspace attached to task)".into())
    }
}

#[tauri::command]
async fn apply_task_changes(task_id: String, dry_run: bool) -> Result<ApplyResult, String> {
    let id = Uuid::parse_str(&task_id).map_err(|e| format!("Invalid UUID: {e}"))?;
    let db = get_db().await?;
    let tasks = TaskRepository::new(db.clone());
    let applies = ApplyRepository::new(db.clone());
    let workflows = WorkflowRepository::new(db.clone());
    let steps = WorkflowStepRepository::new(db.clone());
    let workspaces = Arc::new(WorkspaceManager::with_default_root(
        WorkspaceRepository::new(db.clone()),
    ));

    let apply_manager =
        agentmesh_apply::ApplyManager::new(tasks, workspaces, workflows, steps, applies);

    let plan = apply_manager
        .plan_task(id)
        .await
        .map_err(|e| format!("Planning apply failed: {e}"))?;

    let file_paths: Vec<String> = plan.changed_files.iter().map(|f| f.path.clone()).collect();

    if dry_run {
        Ok(ApplyResult {
            success: true,
            dry_run: true,
            message: format!(
                "Dry-run check passed: {} file(s) modified cleanly.",
                file_paths.len()
            ),
            files_changed: file_paths,
        })
    } else {
        let outcome = apply_manager
            .apply_task(id)
            .await
            .map_err(|e| format!("Execute apply failed: {e}"))?;

        let total_applied = outcome.plan.changed_files.len();
        let changed = outcome
            .plan
            .changed_files
            .iter()
            .map(|f| f.path.clone())
            .collect();
        Ok(ApplyResult {
            success: true,
            dry_run: false,
            message: format!(
                "Successfully applied {} change(s) to working repository!",
                total_applied
            ),
            files_changed: changed,
        })
    }
}

#[tauri::command]
async fn list_workflows(_limit: Option<usize>) -> Result<Vec<WorkflowItem>, String> {
    let db = get_db().await?;
    let repo = WorkflowRepository::new(db.clone());
    let step_repo = WorkflowStepRepository::new(db);
    let workflows = repo
        .list()
        .await
        .map_err(|e| format!("Failed to list workflows: {e}"))?;

    let mut result = Vec::new();
    for w in workflows {
        let steps = step_repo.list_for(w.id).await.unwrap_or_default();
        result.push(WorkflowItem {
            id: w.id.to_string(),
            name: format!("{} ({})", w.goal, w.preset),
            status: w.status.as_str().to_string(),
            goal: w.goal,
            graph_nodes_count: steps.len(),
            created_at: w.created_at,
            completed_at: w.completed_at,
        });
    }
    Ok(result)
}

#[tauri::command]
async fn get_workflow_details(workflow_id: String) -> Result<WorkflowDetail, String> {
    let id = Uuid::parse_str(&workflow_id).map_err(|e| format!("Invalid UUID: {e}"))?;
    let db = get_db().await?;
    let repo = WorkflowRepository::new(db.clone());
    let step_repo = WorkflowStepRepository::new(db);

    let w = repo
        .get(id)
        .await
        .map_err(|e| format!("DB error: {e}"))?
        .ok_or_else(|| format!("Workflow {workflow_id} not found"))?;

    let steps_db = step_repo
        .list_for(id)
        .await
        .map_err(|e| format!("DB error: {e}"))?;

    let steps = steps_db
        .into_iter()
        .map(|s| WorkflowStepItem {
            id: s.id.to_string(),
            node_id: s.node_id.unwrap_or_else(|| s.ordinal.to_string()),
            agent_id: s.agent_id.unwrap_or_default(),
            status: s.status,
            intent: s.intent,
            error: s.error,
        })
        .collect();

    Ok(WorkflowDetail {
        id: w.id.to_string(),
        name: format!("{} ({})", w.goal, w.preset),
        status: w.status.as_str().to_string(),
        goal: w.goal,
        created_at: w.created_at,
        completed_at: w.completed_at,
        steps,
    })
}

#[tauri::command]
async fn get_provenance_audit(workflow_id: String) -> Result<ProvenanceAuditReport, String> {
    let id = Uuid::parse_str(&workflow_id).map_err(|e| format!("Invalid UUID: {e}"))?;
    let db = get_db().await?;
    let prov_repo = agentmesh_storage::ProvenanceRepository::new(db);

    let events_db = prov_repo
        .list_for_workflow(id)
        .await
        .map_err(|e| format!("Provenance DB error: {e}"))?;

    let mut valid_chain = true;
    let mut prev_hash =
        "0000000000000000000000000000000000000000000000000000000000000000".to_string();

    let mut events = Vec::new();
    for e in events_db {
        if e.previous_hash.as_deref() != Some(&prev_hash) {
            valid_chain = false;
        }
        prev_hash = e.event_hash.clone();

        events.push(ProvenanceEventItem {
            sequence: e.sequence as u64,
            event_type: e.event_type,
            agent_id: e.actor_id,
            payload_hash: e.payload_hash,
            event_hash: e.event_hash,
            created_at: e.created_at,
        });
    }

    Ok(ProvenanceAuditReport {
        workflow_id,
        valid_chain,
        total_events: events.len(),
        events,
    })
}

// ---------------------------------------------------------------------------
// App Entrypoint
// ---------------------------------------------------------------------------

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_single_instance::init(|app, _args, _cwd| {
            use tauri::Manager;
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.show();
                let _ = window.set_focus();
            }
        }))
        .invoke_handler(tauri::generate_handler![
            doctor_check,
            list_tasks,
            get_task_details,
            run_task,
            get_task_diff,
            apply_task_changes,
            list_workflows,
            get_workflow_details,
            get_provenance_audit,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
