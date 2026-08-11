//! Context persistence.

use agentmesh_core::{AgentSession, AgentTask, Context};
use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

use crate::database::Database;
use crate::error::StorageError;

/// Persists [`Context`]s.
#[derive(Clone)]
pub struct ContextRepository {
    database: Database,
}

impl ContextRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    /// Insert a context.
    pub async fn create(&self, context: &Context) -> Result<(), StorageError> {
        sqlx::query("INSERT INTO contexts (id, created_at, updated_at) VALUES (?, ?, ?)")
            .bind(context.id.to_string())
            .bind(context.created_at.to_rfc3339())
            .bind(context.updated_at.to_rfc3339())
            .execute(self.database.pool())
            .await
            .map_err(|source| StorageError::CreateContext {
                context_id: context.id.to_string(),
                source,
            })?;
        Ok(())
    }

    /// Load a context by id; `Ok(None)` when it does not exist.
    pub async fn get(&self, id: Uuid) -> Result<Option<Context>, StorageError> {
        let row = sqlx::query("SELECT id, created_at, updated_at FROM contexts WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(self.database.pool())
            .await
            .map_err(|source| StorageError::LoadContext {
                context_id: id.to_string(),
                source,
            })?;
        row.map(|row| row_to_context(&row)).transpose()
    }

    /// Update `updated_at` to now.
    pub async fn touch(&self, id: Uuid) -> Result<(), StorageError> {
        sqlx::query("UPDATE contexts SET updated_at = ? WHERE id = ?")
            .bind(Utc::now().to_rfc3339())
            .bind(id.to_string())
            .execute(self.database.pool())
            .await
            .map_err(|source| StorageError::TouchContext {
                context_id: id.to_string(),
                source,
            })?;
        Ok(())
    }

    /// Create a context, its agent session and its first task in a single
    /// transaction so the three can never be partially persisted.
    pub async fn create_run_setup(
        &self,
        context: &Context,
        session: &AgentSession,
        task: &AgentTask,
    ) -> Result<(), StorageError> {
        let mut tx =
            self.database
                .pool()
                .begin()
                .await
                .map_err(|source| StorageError::CreateContext {
                    context_id: context.id.to_string(),
                    source,
                })?;

        sqlx::query("INSERT INTO contexts (id, created_at, updated_at) VALUES (?, ?, ?)")
            .bind(context.id.to_string())
            .bind(context.created_at.to_rfc3339())
            .bind(context.updated_at.to_rfc3339())
            .execute(&mut *tx)
            .await
            .map_err(|source| StorageError::CreateContext {
                context_id: context.id.to_string(),
                source,
            })?;

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
        .execute(&mut *tx)
        .await
        .map_err(|source| StorageError::CreateSession {
            session_id: session.id.to_string(),
            source,
        })?;

        insert_task(&mut tx, task).await?;

        tx.commit()
            .await
            .map_err(|source| StorageError::CreateContext {
                context_id: context.id.to_string(),
                source,
            })
    }

    /// Create a new task in an existing context (resume flow).
    pub async fn create_task_for_context(
        &self,
        context: &Context,
        session: &AgentSession,
        task: &AgentTask,
    ) -> Result<(), StorageError> {
        let mut tx =
            self.database
                .pool()
                .begin()
                .await
                .map_err(|source| StorageError::CreateTask {
                    task_id: task.id.to_string(),
                    source,
                })?;

        insert_task(&mut tx, task).await?;
        touch_rows(&mut tx, context.id, session.id).await?;

        tx.commit()
            .await
            .map_err(|source| StorageError::CreateTask {
                task_id: task.id.to_string(),
                source,
            })
    }
}

async fn insert_task(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    task: &AgentTask,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO tasks (id, agent_id, status, prompt, workspace, error, created_at, started_at, completed_at, context_id, agent_session_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(task.id.to_string())
    .bind(&task.agent_id)
    .bind(task.status.as_str())
    .bind(&task.input.content)
    .bind(task.workspace.as_ref().map(|p| p.display().to_string()))
    .bind(task.error.as_deref())
    .bind(task.created_at.to_rfc3339())
    .bind(task.started_at.map(|t| t.to_rfc3339()))
    .bind(task.completed_at.map(|t| t.to_rfc3339()))
    .bind(task.context_id.to_string())
    .bind(task.agent_session_id.map(|id| id.to_string()))
    .execute(&mut **tx)
    .await
    .map_err(|source| StorageError::CreateTask {
        task_id: task.id.to_string(),
        source,
    })?;
    Ok(())
}

async fn touch_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    context_id: Uuid,
    session_id: Uuid,
) -> Result<(), StorageError> {
    let now = Utc::now().to_rfc3339();
    sqlx::query("UPDATE contexts SET updated_at = ? WHERE id = ?")
        .bind(&now)
        .bind(context_id.to_string())
        .execute(&mut **tx)
        .await
        .map_err(|source| StorageError::TouchContext {
            context_id: context_id.to_string(),
            source,
        })?;
    sqlx::query("UPDATE agent_sessions SET updated_at = ? WHERE id = ?")
        .bind(&now)
        .bind(session_id.to_string())
        .execute(&mut **tx)
        .await
        .map_err(|source| StorageError::TouchSession {
            session_id: session_id.to_string(),
            source,
        })?;
    Ok(())
}

fn row_to_context(row: &sqlx::sqlite::SqliteRow) -> Result<Context, StorageError> {
    let context_id: String = row.get("id");
    Ok(Context {
        id: Uuid::parse_str(&context_id).map_err(|err| StorageError::LoadContext {
            context_id: context_id.clone(),
            source: sqlx::Error::Decode(err.to_string().into()),
        })?,
        created_at: parse_time(row.get("created_at"), &context_id)
            .map_err(|e| decode_ctx_err(&context_id, e))?,
        updated_at: parse_time(row.get("updated_at"), &context_id)
            .map_err(|e| decode_ctx_err(&context_id, e))?,
    })
}

fn parse_time(value: String, id: &str) -> Result<chrono::DateTime<chrono::Utc>, String> {
    chrono::DateTime::parse_from_rfc3339(&value)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .map_err(|err| format!("{id}: {err}"))
}

fn decode_ctx_err(context_id: &str, message: String) -> StorageError {
    StorageError::LoadContext {
        context_id: context_id.to_string(),
        source: sqlx::Error::Decode(message.into()),
    }
}
