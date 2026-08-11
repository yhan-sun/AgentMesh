//! Agent session persistence.

use agentmesh_core::AgentSession;
use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

use crate::database::Database;
use crate::error::StorageError;

/// Persists [`AgentSession`]s.
#[derive(Clone)]
pub struct AgentSessionRepository {
    database: Database,
}

impl AgentSessionRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    /// Insert a session (native session id may be `None` before the agent
    /// starts).
    pub async fn create(&self, session: &AgentSession) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO agent_sessions (id, context_id, agent_id, native_session_id, workspace, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(session.id.to_string())
        .bind(session.context_id.to_string())
        .bind(&session.agent_id)
        .bind(session.native_session_id.as_deref())
        .bind(session.workspace.as_ref().map(|p| p.display().to_string()))
        .bind(session.created_at.to_rfc3339())
        .bind(session.updated_at.to_rfc3339())
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::CreateSession {
            session_id: session.id.to_string(),
            source,
        })?;
        Ok(())
    }

    /// Load a session by id; `Ok(None)` when it does not exist.
    pub async fn get(&self, id: Uuid) -> Result<Option<AgentSession>, StorageError> {
        let row = sqlx::query(
            "SELECT id, context_id, agent_id, native_session_id, workspace, created_at, updated_at
             FROM agent_sessions WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.database.pool())
        .await
        .map_err(|source| StorageError::LoadSession {
            session_id: id.to_string(),
            source,
        })?;
        row.map(|row| row_to_session(&row)).transpose()
    }

    /// Load the session for a (context, agent) pair.
    pub async fn get_by_context_agent(
        &self,
        context_id: Uuid,
        agent_id: &str,
    ) -> Result<Option<AgentSession>, StorageError> {
        let row = sqlx::query(
            "SELECT id, context_id, agent_id, native_session_id, workspace, created_at, updated_at
             FROM agent_sessions WHERE context_id = ? AND agent_id = ?",
        )
        .bind(context_id.to_string())
        .bind(agent_id)
        .fetch_optional(self.database.pool())
        .await
        .map_err(|source| StorageError::LoadSession {
            session_id: context_id.to_string(),
            source,
        })?;
        row.map(|row| row_to_session(&row)).transpose()
    }

    /// Update the native session id.
    ///
    /// A stored native session id is never overwritten back to `NULL`; only
    /// non-`None` values are written.
    pub async fn set_native_session_id(
        &self,
        id: Uuid,
        native_session_id: &str,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "UPDATE agent_sessions
             SET native_session_id = ?, updated_at = ?
             WHERE id = ? AND ? IS NOT NULL",
        )
        .bind(native_session_id)
        .bind(Utc::now().to_rfc3339())
        .bind(id.to_string())
        .bind(native_session_id)
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdateSession {
            session_id: id.to_string(),
            source,
        })?;
        if result.rows_affected() == 0 {
            return Err(StorageError::SessionNotFound(id.to_string()));
        }
        Ok(())
    }

    /// Update `updated_at` to now.
    pub async fn touch(&self, id: Uuid) -> Result<(), StorageError> {
        sqlx::query("UPDATE agent_sessions SET updated_at = ? WHERE id = ?")
            .bind(Utc::now().to_rfc3339())
            .bind(id.to_string())
            .execute(self.database.pool())
            .await
            .map_err(|source| StorageError::TouchSession {
                session_id: id.to_string(),
                source,
            })?;
        Ok(())
    }

    /// Update the workspace binding of a session.
    pub async fn set_workspace(
        &self,
        id: Uuid,
        workspace: Option<&std::path::Path>,
    ) -> Result<(), StorageError> {
        let result =
            sqlx::query("UPDATE agent_sessions SET workspace = ?, updated_at = ? WHERE id = ?")
                .bind(workspace.map(|p| p.display().to_string()))
                .bind(Utc::now().to_rfc3339())
                .bind(id.to_string())
                .execute(self.database.pool())
                .await
                .map_err(|source| StorageError::UpdateSession {
                    session_id: id.to_string(),
                    source,
                })?;
        if result.rows_affected() == 0 {
            return Err(StorageError::SessionNotFound(id.to_string()));
        }
        Ok(())
    }
}

fn row_to_session(row: &sqlx::sqlite::SqliteRow) -> Result<AgentSession, StorageError> {
    let session_id: String = row.get("id");
    let context_id: String = row.get("context_id");
    let session = AgentSession {
        id: Uuid::parse_str(&session_id).map_err(|err| StorageError::LoadSession {
            session_id: session_id.clone(),
            source: sqlx::Error::Decode(err.to_string().into()),
        })?,
        context_id: Uuid::parse_str(&context_id).map_err(|err| StorageError::LoadSession {
            session_id: session_id.clone(),
            source: sqlx::Error::Decode(err.to_string().into()),
        })?,
        agent_id: row.get("agent_id"),
        native_session_id: row.get("native_session_id"),
        workspace: row
            .get::<Option<String>, _>("workspace")
            .map(std::path::PathBuf::from),
        created_at: row_to_time(row, "created_at", &session_id)?,
        updated_at: row_to_time(row, "updated_at", &session_id)?,
    };
    Ok(session)
}

fn row_to_time(
    row: &sqlx::sqlite::SqliteRow,
    column: &str,
    session_id: &str,
) -> Result<chrono::DateTime<chrono::Utc>, StorageError> {
    let value: String = row.get(column);
    chrono::DateTime::parse_from_rfc3339(&value)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|err| StorageError::LoadSession {
            session_id: session_id.to_string(),
            source: sqlx::Error::Decode(err.to_string().into()),
        })
}
