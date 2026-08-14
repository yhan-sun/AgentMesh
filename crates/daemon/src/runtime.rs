//! Daemon runtime lifecycle: lock, stale recovery, heartbeat, metadata.

use std::path::PathBuf;
use std::sync::Arc;

use agentmesh_apply::ApplyManager;
use agentmesh_orchestrator::directory::{AgentAuth, AgentDirectory, DiscoveredEndpoint};
use agentmesh_orchestrator::router::RuleRouter;
use agentmesh_storage::{
    AgentSessionRepository, ApplyRepository, ArtifactRepository, ContextRepository, Database,
    TaskRepository, WorkflowPlanRepository, WorkflowRecoveryRepository, WorkflowReplanRepository,
    WorkflowRepository, WorkflowStepRepository, WorkspaceRepository,
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
use crate::workflow_service::WorkflowService;

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
    let config = agentmesh_core::AgentMeshConfig::load();
    let tasks = TaskRepository::new(database.clone());
    let artifacts = ArtifactRepository::new(database.clone());
    let contexts = ContextRepository::new(database.clone());
    let sessions = AgentSessionRepository::new(database.clone());
    let workspaces = Arc::new(WorkspaceManager::with_default_root(
        WorkspaceRepository::new(database.clone()),
    ));
    let task_manager = TaskManager::new(
        Arc::new(agentmesh_adapters::AgentRegistry::from_config(&config)),
        tasks.clone(),
        artifacts,
        contexts,
        sessions,
        workspaces.clone(),
    );
    let competitions_repo = agentmesh_storage::CompetitionRepository::new(database.clone());
    let workflows = WorkflowService::new(
        instance_id,
        task_manager.clone(),
        WorkflowRepository::new(database.clone()),
        WorkflowStepRepository::new(database.clone()),
        WorkflowPlanRepository::new(database.clone()),
        WorkflowReplanRepository::new(database.clone()),
        agentmesh_storage::EvaluationRepository::new(database.clone()),
        competitions_repo.clone(),
        workspaces.clone(),
        RuleRouter::new(config.routing_config()),
    );
    let policy = match &config.planner {
        Some(planner) => planner
            .policy
            .as_ref()
            .map(agentmesh_orchestrator::PlanPolicy::from_config)
            .unwrap_or_default(),
        None => agentmesh_orchestrator::PlanPolicy::default(),
    };
    let plans = crate::planner::PlanService::with_policy(
        workflows.clone(),
        WorkflowPlanRepository::new(database.clone()),
        policy.clone(),
    );
    let replans = crate::replan::ReplanService::with_policy(
        workflows.clone(),
        WorkflowReplanRepository::new(database.clone()),
        policy.clone(),
    );
    let recovery_config = config.recovery.clone().unwrap_or_default();
    let recovery_policy = crate::recovery::RecoveryPolicy::from_config(&recovery_config);
    let recoveries = crate::recovery::RecoveryService::with_policy(
        workflows.clone(),
        WorkflowRecoveryRepository::new(database.clone()),
        workspaces.clone(),
        policy,
        recovery_policy.clone(),
    );
    // Failure sink → auto-generate (and optionally auto-execute) recovery
    // proposals when a workflow reaches Failed (Phase 20 §8/§14).
    let (failure_tx, mut failure_rx) = tokio::sync::mpsc::channel(64);
    workflows.set_failure_sink(failure_tx).await;
    let auto_generate = recovery_policy.auto_generate;
    let auto_execute = recovery_policy.auto_execute;
    let recoveries_consumer = recoveries.clone();
    tokio::spawn(async move {
        while let Some(workflow_id) = failure_rx.recv().await {
            if !auto_generate {
                continue;
            }
            match recoveries_consumer.propose(workflow_id, None).await {
                Ok(recovery_id) => {
                    if auto_execute
                        && recoveries_consumer
                            .get(recovery_id)
                            .await
                            .ok()
                            .flatten()
                            .map(|r| r.status == agentmesh_storage::recovery_status::READY)
                            .unwrap_or(false)
                        && let Err(err) = recoveries_consumer.execute(recovery_id).await
                    {
                        tracing::warn!(
                            workflow_id = %workflow_id,
                            error = %err,
                            "auto recovery execution failed"
                        );
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        workflow_id = %workflow_id,
                        error = %err,
                        "auto recovery proposal failed"
                    );
                }
            }
        }
    });
    let applies = ApplyRepository::new(database.clone());
    let workflows_repo = WorkflowRepository::new(database.clone());
    let steps = WorkflowStepRepository::new(database.clone());
    let artifacts = ArtifactRepository::new(database.clone());
    let provenance_repo = agentmesh_storage::ProvenanceRepository::new(database.clone());
    let provenance = Arc::new(crate::provenance_service::ProvenanceService::new(
        provenance_repo.clone(),
        workflows_repo.clone(),
        steps.clone(),
        agentmesh_storage::EvaluationRepository::new(database.clone()),
        competitions_repo.clone(),
        applies.clone(),
        WorkflowPlanRepository::new(database.clone()),
        WorkflowReplanRepository::new(database.clone()),
        WorkflowRecoveryRepository::new(database.clone()),
        tasks.clone(),
        agentmesh_storage::WorkspaceRepository::new(database.clone()),
    ));
    let apply = Arc::new(
        ApplyManager::new(
            tasks.clone(),
            workspaces.clone(),
            workflows_repo.clone(),
            steps.clone(),
            applies.clone(),
        )
        .with_competitions(competitions_repo.clone())
        .with_provenance(provenance_repo.clone()),
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
        workflows,
        plans,
        replans,
        recoveries,
        apply,
        workspaces,
        applies,
        workflows_repo,
        steps,
        competitions: competitions_repo,
        artifacts,
        provenance_repo,
        provenance,
        a2a_agents: std::sync::Mutex::new(serde_json::json!({})),
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

    // Recover workflows interrupted by a dead daemon (Phase 12). They stay
    // `Interrupted` until an explicit `workflow resume`.
    let interrupted = state.workflows.recover_interrupted().await?;
    if interrupted > 0 {
        tracing::warn!(count = interrupted, "recovered interrupted workflows");
    }

    // Recover plans stuck in `executing` after a daemon crash (Phase 19 §1):
    // a claim that never produced a workflow is `failed`; one that did is
    // corrected to `executed` by its real workflow, never mislabeled.
    let (plans_failed, plans_executed) = state.plans.recover_stale_executing().await?;
    if plans_failed > 0 || plans_executed > 0 {
        tracing::warn!(
            failed = plans_failed,
            corrected = plans_executed,
            "recovered stale executing plans"
        );
    }

    // Recover replans stuck in `applying` (Phase 20 §2): the atomic apply
    // transaction makes the workflow's graph_revision prove the outcome.
    let (replans_ready, replans_applied, replans_failed) =
        state.replans.recover_stale_applying().await?;
    if replans_ready > 0 || replans_applied > 0 || replans_failed > 0 {
        tracing::warn!(
            ready = replans_ready,
            applied = replans_applied,
            failed = replans_failed,
            "recovered stale applying replans"
        );
    }

    // Recover recovery proposals stuck mid-flight (Phase 20 §20).
    let (recoveries_generating, recoveries_ready, recoveries_executed) =
        state.recoveries.recover_stale_executing().await?;
    if recoveries_generating > 0 || recoveries_ready > 0 || recoveries_executed > 0 {
        tracing::warn!(
            analyzer_failed = recoveries_generating,
            retryable = recoveries_ready,
            corrected = recoveries_executed,
            "recovered stale executing recoveries"
        );
    }

    let (addr, router, listener) = server::bind(state.clone()).await?;

    // Write metadata (atomic via temp file + rename) and token.
    write_metadata(&scope, &instance_id, std::process::id(), &addr.to_string()).await?;
    auth::write_token(&paths::daemon_token_path(&scope), &token)?;
    tracing::info!(instance = %instance_id, address = %addr, "daemon ready");

    // A2A listeners: one per enabled + online agent.
    let a2a_listeners = crate::a2a::start_listeners(&state, &scope).await;

    // The workflow service routes through the A2A agents we just started.
    if let Ok(directory) = build_workflow_directory(&state, &scope).await {
        state.workflows.set_directory(directory);
    } else {
        tracing::warn!("no A2A agents discovered; workflows will be unavailable");
    }

    // Heartbeat loop: live tasks + running workflows.
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
            heartbeat_state.workflows.heartbeat_live().await;
        }
    });

    // Serve until shutdown is requested (signal handler or shutdown API).
    let shutdown = state.shutdown.clone();
    tokio::select! {
        _ = server::serve(listener, router, shutdown.clone()) => {}
        _ = shutdown.notified() => {}
    }
    heartbeat.abort();

    // Graceful teardown: stop A2A listeners, remove metadata; the lock file
    // may remain.
    crate::a2a::stop_listeners(a2a_listeners);
    let _ = std::fs::remove_file(paths::daemon_json_path(&scope));
    let _ = std::fs::remove_file(paths::daemon_token_path(&scope));
    let _ = std::fs::remove_file(paths::a2a_token_path(&scope));
    drop(lock);
    tracing::info!(instance = %instance_id, "daemon stopped");
    Ok(())
}

/// Build the AgentDirectory from the daemon's A2A listeners (Phase 12).
async fn build_workflow_directory(
    state: &SharedState,
    scope: &Scope,
) -> anyhow::Result<AgentDirectory> {
    let a2a_agents = state.a2a_agents.lock().unwrap().clone();
    let token = auth::read_token(&paths::a2a_token_path(scope)).ok();
    let mut discovered = Vec::new();
    if let Some(agents) = a2a_agents.as_object() {
        for (agent_id, info) in agents {
            let url = info
                .get("url")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let card_url = info
                .get("card_url")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if url.is_empty() || card_url.is_empty() {
                continue;
            }
            discovered.push(DiscoveredEndpoint {
                agent_id: agent_id.clone(),
                url,
                card_url,
            });
        }
    }
    let mut directory = AgentDirectory::new();
    directory.refresh(&discovered, &AgentAuth { token }).await?;
    Ok(directory)
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
