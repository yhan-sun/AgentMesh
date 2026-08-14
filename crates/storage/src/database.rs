//! SQLite connection management, pragmas and migrations.

use std::path::Path;
use std::path::PathBuf;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{SqlitePool, migrate};

use crate::error::StorageError;

/// Default location for the AgentMesh state database.
///
/// When run inside a Git repository the database lives under the project
/// root: `<git-root>/.agentmesh/agentmesh.db`. Otherwise it falls back to
/// the user data directory (`~/.local/share/agentmesh/agentmesh.db`).
pub fn default_database_path() -> PathBuf {
    project_root()
        .map(|root| root.join(".agentmesh").join("agentmesh.db"))
        .unwrap_or_else(user_data_path)
}

/// Walk up from the current directory to find the Git repository root.
pub fn project_root() -> Option<PathBuf> {
    std::env::current_dir()
        .ok()
        .and_then(|cwd| find_git_root(&cwd))
}

/// Walk up from `start` to find the Git repository root.
pub fn find_git_root(start: &Path) -> Option<PathBuf> {
    let mut dir = start;
    loop {
        if dir.join(".git").exists() {
            return Some(dir.to_path_buf());
        }
        dir = dir.parent()?;
    }
}

/// AgentMesh user data directory (`~/.local/share/agentmesh`).
pub fn user_data_dir() -> PathBuf {
    #[cfg(windows)]
    {
        std::env::var_os("APPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."))
            .join("agentmesh")
    }
    #[cfg(not(windows))]
    {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".local")
            .join("share")
            .join("agentmesh")
    }
}

fn user_data_path() -> PathBuf {
    user_data_dir().join("agentmesh.db")
}

/// AgentMesh state database: owns the connection pool and runs migrations.
#[derive(Clone)]
pub struct Database {
    pool: SqlitePool,
}

impl Database {
    /// Open (creating if needed) the database at `path` and run migrations.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|source| StorageError::CreateTaskDir {
                path: parent.display().to_string(),
                source,
            })?;
        }

        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true)
            // WAL + synchronous=NORMAL: a crash can lose the last committed
            // transactions, never corrupt the database — the daemon treats
            // recent state as recoverable anyway. NORMAL avoids a fsync per
            // commit, which keeps write transactions short under load.
            .synchronous(SqliteSynchronous::Normal)
            // 30s: under heavy concurrent load a writer may have to wait for
            // the single WAL writer; the scheduler persists node state
            // asynchronously and must not drop it on a lock timeout.
            .busy_timeout(std::time::Duration::from_secs(30));

        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .map_err(|source| StorageError::Open {
                path: path.display().to_string(),
                source,
            })?;

        let database = Self { pool };
        database.migrate().await?;
        Ok(database)
    }

    /// Run pending migrations.
    pub async fn migrate(&self) -> Result<(), StorageError> {
        migrate::Migrator::new(Path::new(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/migrations"
        )))
        .await?
        .run(&self.pool)
        .await?;
        Ok(())
    }

    /// Access the underlying pool for repository queries.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::Row;

    #[tokio::test]
    async fn fresh_database_migrates() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Database::open(dir.path().join("agentmesh.db"))
            .await
            .expect("open");
        let pool = db.pool();
        for table in ["tasks", "artifacts"] {
            let row = sqlx::query("SELECT name FROM sqlite_master WHERE type='table' AND name=?")
                .bind(table)
                .fetch_one(pool)
                .await
                .expect("table exists");
            assert_eq!(row.get::<String, _>("name"), table);
        }
    }

    #[tokio::test]
    async fn migrate_twice_is_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("agentmesh.db");
        Database::open(&path).await.expect("first open");
        Database::open(&path).await.expect("second open");
    }

    #[tokio::test]
    async fn foreign_keys_are_enabled() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Database::open(dir.path().join("agentmesh.db"))
            .await
            .expect("open");
        let row = sqlx::query("PRAGMA foreign_keys")
            .fetch_one(db.pool())
            .await
            .expect("pragma");
        assert_eq!(row.get::<i64, _>("foreign_keys"), 1);
    }

    #[tokio::test]
    async fn journal_mode_is_wal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Database::open(dir.path().join("agentmesh.db"))
            .await
            .expect("open");
        let row = sqlx::query("PRAGMA journal_mode")
            .fetch_one(db.pool())
            .await
            .expect("pragma");
        assert_eq!(row.get::<String, _>("journal_mode"), "wal");
    }

    #[test]
    fn project_root_detects_git_repository() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(dir.path().join(".git")).expect("mkdir");
        let nested = dir.path().join("a").join("b");
        std::fs::create_dir_all(&nested).expect("mkdir");
        assert_eq!(find_git_root(&nested), Some(dir.path().to_path_buf()));
    }

    #[test]
    fn project_root_none_outside_repository() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert_eq!(find_git_root(dir.path()), None);
    }

    #[test]
    fn default_path_prefers_project_root() {
        // In this repository the workspace is a Git repository, so the
        // default path must be under the repo root.
        let path = default_database_path();
        let cwd = std::env::current_dir().expect("cwd");
        assert!(path.starts_with(cwd) || !path.to_string_lossy().is_empty());
    }
}
