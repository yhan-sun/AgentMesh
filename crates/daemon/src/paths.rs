//! Project-scoped daemon paths.

use std::path::{Path, PathBuf};

/// Scope of a daemon: a project (git root) or the user-level fallback.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    Project(PathBuf),
    User,
}

impl Scope {
    /// Resolve the scope for the current directory, mirroring the database
    /// scope semantics (git root → project, otherwise user fallback).
    pub fn resolve() -> Scope {
        match agentmesh_storage::database::project_root() {
            Some(root) => Scope::Project(root),
            None => Scope::User,
        }
    }

    /// Runtime directory holding daemon metadata.
    pub fn runtime_dir(&self) -> PathBuf {
        match self {
            Scope::Project(root) => root.join(".agentmesh").join("runtime"),
            Scope::User => agentmesh_storage::database::user_data_dir().join("runtime"),
        }
    }

    /// Human-readable scope label.
    pub fn label(&self) -> String {
        match self {
            Scope::Project(root) => root.display().to_string(),
            Scope::User => agentmesh_storage::database::user_data_dir()
                .display()
                .to_string(),
        }
    }

    /// Directory used to locate the project database (for diagnostics).
    pub fn project_root(&self) -> Option<&Path> {
        match self {
            Scope::Project(root) => Some(root),
            Scope::User => None,
        }
    }
}

pub fn daemon_lock_path(scope: &Scope) -> PathBuf {
    scope.runtime_dir().join("daemon.lock")
}

pub fn daemon_json_path(scope: &Scope) -> PathBuf {
    scope.runtime_dir().join("daemon.json")
}

pub fn daemon_token_path(scope: &Scope) -> PathBuf {
    scope.runtime_dir().join("daemon.token")
}

pub fn daemon_log_path(scope: &Scope) -> PathBuf {
    match scope {
        Scope::Project(root) => root.join(".agentmesh").join("logs").join("daemon.log"),
        Scope::User => agentmesh_storage::database::user_data_dir()
            .join("logs")
            .join("daemon.log"),
    }
}
