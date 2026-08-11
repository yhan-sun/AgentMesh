//! Application wiring: config → registry → database → task manager.

use std::path::PathBuf;
use std::sync::Arc;

use agentmesh_adapters::AgentRegistry;
use agentmesh_core::AgentMeshConfig;
use agentmesh_storage::{
    AgentSessionRepository, ArtifactRepository, ContextRepository, Database, TaskRepository,
    WorkspaceRepository,
};
use agentmesh_tasks::TaskManager;
use agentmesh_workspace::WorkspaceManager;

/// Shared application context, initialized once per process.
///
/// Owns the agent registry, the task manager and the repository handles.
/// Kept out of `core`: the daemon may build its own context later.
pub struct AppContext {
    pub registry: Arc<AgentRegistry>,
    pub database_path: PathBuf,
    pub tasks: TaskRepository,
    pub artifacts: ArtifactRepository,
    pub sessions: AgentSessionRepository,
    pub workspaces: WorkspaceManager,
}

impl AppContext {
    pub async fn init() -> anyhow::Result<Self> {
        let config = AgentMeshConfig::load();
        let registry = Arc::new(AgentRegistry::from_config(&config));

        let path = agentmesh_storage::database::default_database_path();
        tracing::debug!(database = %path.display(), "opening agentmesh database");
        let database = Database::open(&path).await?;

        let tasks = TaskRepository::new(database.clone());
        let artifacts = ArtifactRepository::new(database.clone());
        let contexts = ContextRepository::new(database.clone());
        let sessions = AgentSessionRepository::new(database.clone());
        let workspace_repo = WorkspaceRepository::new(database.clone());
        let workspaces = Arc::new(WorkspaceManager::with_default_root(workspace_repo));
        let _task_manager = TaskManager::new(
            registry.clone(),
            tasks.clone(),
            artifacts.clone(),
            contexts,
            sessions.clone(),
            workspaces.clone(),
        );

        Ok(Self {
            registry,
            database_path: path,
            tasks,
            artifacts,
            sessions,
            workspaces: (*workspaces).clone(),
        })
    }
}
