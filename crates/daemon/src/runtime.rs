//! Daemon runtime lifecycle: lock, stale recovery, heartbeat, metadata.

use std::path::PathBuf;
use std::sync::Arc;

use agentmesh_storage::{
    AgentSessionRepository, ArtifactRepository, ContextRepository, Database, TaskRepository,
    WorkspaceRepository,
};
use agentmesh_tasks::TaskManager;
use agentmesh_workspace::WorkspaceManager;
use chrono::Utc;
use tokio::sync::Notify;
use tracing::instrument;
use uuid::Uuid;

use crate::auth;
use crate::lease::SessionLeaseManager;
use crate::lock::ScopeLock;
use crate::paths::{self, Scope};
use crate::protocol::{DAEMON_PROTOCOL_VERSION, DaemonMeta};
use crate::registry::LiveTaskRegistry;
use crate::server::{self, DaemonState, SharedState};

/// How often live tasks report a heartbeat.
pub const HEARTBEAT_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Everything a running daemon holds.
pub struct DaemonRuntime {
    pub scope: Scope,
    pub instance_id: Uuid,
    pub token: String,
    pub state: SharedState,
    pub _lock: ScopeLock,
}

/// Build the shared state (repositories + TaskManager) for a scope.
pub async fn build_state(
    scope: &Scope,
    instance_id: Uuid,
    token: String,
) -> anyhow::Result<SharedState> {
    let database_path = match scope {
        Scope::Project(root) => root.join(".agentmesh").join("agentmesh.db"),
        Scope::User => agentmesh_storage::database::default_database_path(),
    };
    let database = Database::open(&database_path).await?;
    let tasks = TaskRepository::new(database.clone());
    let artifacts = ArtifactRepository::new(database.clone());
    let contexts = ContextRepository::new(database.clone());
    let sessions = AgentSessionRepository::new(database.clone());
    let workspaces = Arc::new(WorkspaceManager::with_default_root(
        WorkspaceRepository::new(database.clone()),
    ));
    let task_manager = TaskManager::new(
        Arc::new(agentmesh_adapters::AgentRegistry::from_config(
            &agentmesh_core::AgentMeshConfig::load(),
        )),
        tasks.clone(),
        artifacts,
        contexts,
        sessions,
        workspaces,
    );

    Ok(Arc::new(DaemonState {
        instance_id,
        token,
        task_manager,
        registry: LiveTaskRegistry::new(),
        leases: Arc::new(SessionLeaseManager::new()),
        scope: scope.clone(),
        started_at: Utc::now(),
        shutdown: Arc::new(Notify::new()),
        shutting_down: std::sync::atomic::AtomicBool::new(false),
        task_repo: tasks,
    }))
}

/// Start a daemon: acquire the scope lock, recover stale tasks, bind the
/// server, write metadata, and run until shutdown.
#[instrument(skip_all, fields(scope = %scope.label()))]
pub async fn serve(scope: Scope) -> anyhow::Result<()> {
    let lock = ScopeLock::acquire(&paths::daemon_lock_path(&scope)).map_err(|err| match err {
        crate::lock::LockError::Held { .. } => anyhow::anyhow!(
            "AgentMesh daemon is already running for this scope ({}).",
            scope.label()
        ),
        other => anyhow::anyhow!("{other}"),
    })?;

    let instance_id = Uuid::new_v4();
    let token = auth::generate_token();

    // Recover tasks owned by dead daemon instances (we hold the exclusive
    // lock, so no other live daemon owns this scope).
    let state = build_state(&scope, instance_id, token.clone()).await?;
    let recovered = state
        .task_repo
        .recover_stale_owned_tasks(&instance_id.to_string())
        .await?;
    if recovered > 0 {
        tracing::warn!(count = recovered, "recovered stale owned tasks");
    }

    let (addr, router, listener) = server::bind(state.clone()).await?;

    // Write metadata (atomic via temp file + rename) and token.
    write_metadata(&scope, &instance_id, std::process::id(), &addr.to_string()).await?;
    auth::write_token(&paths::daemon_token_path(&scope), &token)?;
    tracing::info!(instance = %instance_id, address = %addr, "daemon ready");

    // Heartbeat loop.
    let heartbeat_state = state.clone();
    let heartbeat = tokio::spawn(async move {
        let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            for (task_id, ..) in heartbeat_state.registry.list().await {
                if let Err(err) = heartbeat_state.task_repo.heartbeat(task_id).await {
                    tracing::warn!(task_id = %task_id, error = %err, "heartbeat failed; will retry");
                }
            }
        }
    });

    // Serve until shutdown is requested (signal handler or shutdown API).
    let shutdown = state.shutdown.clone();
    tokio::select! {
        _ = server::serve(listener, router, shutdown.clone()) => {}
        _ = shutdown.notified() => {}
    }
    heartbeat.abort();

    // Graceful teardown: remove metadata; the lock file may remain.
    let _ = std::fs::remove_file(paths::daemon_json_path(&scope));
    let _ = std::fs::remove_file(paths::daemon_token_path(&scope));
    drop(lock);
    tracing::info!(instance = %instance_id, "daemon stopped");
    Ok(())
}

/// Write daemon.json atomically (temp file + rename).
pub async fn write_metadata(
    scope: &Scope,
    instance_id: &Uuid,
    pid: u32,
    address: &str,
) -> anyhow::Result<()> {
    let meta = DaemonMeta {
        protocol_version: DAEMON_PROTOCOL_VERSION,
        instance_id: instance_id.to_string(),
        pid,
        address: address.to_string(),
        started_at: Utc::now().to_rfc3339(),
    };
    let path = paths::daemon_json_path(scope);
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_string_pretty(&meta)?)?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

/// Read daemon metadata (None when not present or unreadable).
pub fn read_metadata(scope: &Scope) -> Option<DaemonMeta> {
    let path = paths::daemon_json_path(scope);
    let content = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&content).ok()
}

/// Background daemon process entry point.
pub fn spawn_daemon_process(scope: &Scope) -> anyhow::Result<std::process::Child> {
    let exe = std::env::current_exe()?;
    let log_path = paths::daemon_log_path(scope);
    if let Some(parent) = log_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    let mut scope_arg = "user".to_string();
    if let Scope::Project(root) = scope {
        scope_arg = root.display().to_string();
    }
    let child = std::process::Command::new(exe)
        .args(["daemon", "serve", "--scope", &scope_arg])
        .stdin(std::process::Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log)
        .spawn()?;
    Ok(child)
}

/// Resolve a `--scope` CLI argument into a Scope.
pub fn parse_scope_arg(arg: &str) -> anyhow::Result<Scope> {
    if arg == "user" {
        Ok(Scope::User)
    } else {
        let root = PathBuf::from(arg);
        if !root.is_dir() {
            anyhow::bail!("scope directory does not exist: {}", root.display());
        }
        Ok(Scope::Project(root))
    }
}
