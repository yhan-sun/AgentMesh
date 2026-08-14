//! Task persistence: SQL rows in, domain objects out.

use agentmesh_core::{AgentMessage, AgentTask, MessageRole, TaskStatus};
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use crate::database::Database;
use crate::error::StorageError;

/// Filters for listing tasks.
#[derive(Debug, Clone, Default)]
pub struct TaskFilter {
    pub agent_id: Option<String>,
    pub status: Option<TaskStatus>,
    pub context_id: Option<Uuid>,
    pub limit: usize,
}

impl TaskFilter {
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    pub fn agent(mut self, agent_id: impl Into<String>) -> Self {
        self.agent_id = Some(agent_id.into());
        self
    }

    pub fn status(mut self, status: TaskStatus) -> Self {
        self.status = Some(status);
        self
    }

    pub fn context(mut self, context_id: Uuid) -> Self {
        self.context_id = Some(context_id);
        self
    }
}

/// SQL row shape of the `tasks` table.
struct TaskRow {
    id: String,
    agent_id: String,
    status: String,
    prompt: String,
    workspace: Option<String>,
    error: Option<String>,
    created_at: String,
    started_at: Option<String>,
    completed_at: Option<String>,
    context_id: Option<String>,
    agent_session_id: Option<String>,
}

impl TaskRow {
    fn into_domain(self) -> Result<AgentTask, StorageError> {
        let status = TaskStatus::from_str(&self.status).ok_or_else(|| StorageError::LoadTask {
            task_id: self.id.clone(),
            source: sqlx::Error::Decode(format!("unknown task status `{}`", self.status).into()),
        })?;
        Ok(AgentTask {
            id: Uuid::parse_str(&self.id).map_err(|err| StorageError::LoadTask {
                task_id: self.id.clone(),
                source: sqlx::Error::Decode(err.to_string().into()),
            })?,
            context_id: parse_uuid_opt(&self.context_id, &self.id)?.unwrap_or_else(Uuid::new_v4),
            agent_id: self.agent_id,
            status,
            input: AgentMessage {
                role: MessageRole::User,
                content: self.prompt,
            },
            artifacts: Vec::new(),
            created_at: parse_time(&self.created_at, &self.id)?,
            started_at: parse_optional_time(self.started_at.as_deref(), &self.id)?,
            completed_at: parse_optional_time(self.completed_at.as_deref(), &self.id)?,
            error: self.error,
            workspace: self.workspace.map(std::path::PathBuf::from),
            agent_session_id: self
                .agent_session_id
                .map(|id| Uuid::parse_str(&id))
                .transpose()
                .map_err(|err| StorageError::LoadTask {
                    task_id: self.id.clone(),
                    source: sqlx::Error::Decode(err.to_string().into()),
                })?,
        })
    }
}

fn parse_uuid_opt(value: &Option<String>, task_id: &str) -> Result<Option<Uuid>, StorageError> {
    value
        .as_deref()
        .map(|id| {
            Uuid::parse_str(id).map_err(|err| StorageError::LoadTask {
                task_id: task_id.to_string(),
                source: sqlx::Error::Decode(err.to_string().into()),
            })
        })
        .transpose()
}

fn parse_time(value: &str, task_id: &str) -> Result<DateTime<Utc>, StorageError> {
    DateTime::parse_from_rfc3339(value)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|err| StorageError::LoadTask {
            task_id: task_id.to_string(),
            source: sqlx::Error::Decode(err.to_string().into()),
        })
}

fn parse_optional_time(
    value: Option<&str>,
    task_id: &str,
) -> Result<Option<DateTime<Utc>>, StorageError> {
    value.map(|v| parse_time(v, task_id)).transpose()
}

/// Persists [`AgentTask`] state.
#[derive(Clone)]
pub struct TaskRepository {
    database: Database,
}

impl TaskRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    /// Insert a new task with its current status.
    pub async fn create(&self, task: &AgentTask) -> Result<(), StorageError> {
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
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::CreateTask {
            task_id: task.id.to_string(),
            source,
        })?;
        Ok(())
    }

    /// Load a task by id; `Ok(None)` when it does not exist.
    pub async fn get(&self, id: Uuid) -> Result<Option<AgentTask>, StorageError> {
        let row = sqlx::query(
            "SELECT id, agent_id, status, prompt, workspace, error, created_at, started_at, completed_at, context_id, agent_session_id
             FROM tasks WHERE id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(self.database.pool())
        .await
        .map_err(|source| StorageError::LoadTask {
            task_id: id.to_string(),
            source,
        })?;
        row.map(|row| row_to_task(&row)).transpose()
    }

    /// List tasks, newest first, honoring the filter.
    pub async fn list(&self, filter: &TaskFilter) -> Result<Vec<AgentTask>, StorageError> {
        let limit = filter.limit.clamp(1, 200);
        let mut query = String::from(
            "SELECT id, agent_id, status, prompt, workspace, error, created_at, started_at, completed_at, context_id, agent_session_id
             FROM tasks WHERE 1 = 1",
        );
        if filter.agent_id.is_some() {
            query.push_str(" AND agent_id = ?");
        }
        if filter.status.is_some() {
            query.push_str(" AND status = ?");
        }
        if filter.context_id.is_some() {
            query.push_str(" AND context_id = ?");
        }
        query.push_str(" ORDER BY created_at DESC LIMIT ?");

        let mut q = sqlx::query(&query);
        if let Some(agent_id) = &filter.agent_id {
            q = q.bind(agent_id);
        }
        if let Some(status) = &filter.status {
            q = q.bind(status.as_str());
        }
        if let Some(context_id) = &filter.context_id {
            q = q.bind(context_id.to_string());
        }
        let rows = q
            .bind(limit as i64)
            .fetch_all(self.database.pool())
            .await
            .map_err(StorageError::ListTasks)?;

        rows.iter().map(row_to_task).collect()
    }

    /// Update the task status, guarding against leaving terminal states.
    ///
    /// Returns `Ok(true)` when a row was updated, `Ok(false)` when the task
    /// does not exist or is already terminal.
    pub async fn set_status(&self, id: Uuid, status: TaskStatus) -> Result<bool, StorageError> {
        let now = Utc::now().to_rfc3339();
        let result = sqlx::query(
            "UPDATE tasks
             SET status = ?,
                 completed_at = CASE WHEN ? THEN ? ELSE completed_at END
             WHERE id = ?
               AND status NOT IN ('completed', 'failed', 'cancelled')",
        )
        .bind(status.as_str())
        .bind(status.is_terminal())
        .bind(status.is_terminal().then_some(now))
        .bind(id.to_string())
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdateTaskStatus {
            task_id: id.to_string(),
            status: status.as_str().to_string(),
            source,
        })?;
        Ok(result.rows_affected() > 0)
    }

    /// Store the failure message and move the task to `Failed`.
    pub async fn set_error(&self, id: Uuid, error: &str) -> Result<bool, StorageError> {
        let result = sqlx::query(
            "UPDATE tasks
             SET status = 'failed', error = ?, completed_at = ?
             WHERE id = ?
               AND status NOT IN ('completed', 'failed', 'cancelled')",
        )
        .bind(error)
        .bind(Utc::now().to_rfc3339())
        .bind(id.to_string())
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdateTaskStatus {
            task_id: id.to_string(),
            status: TaskStatus::Failed.as_str().to_string(),
            source,
        })?;
        Ok(result.rows_affected() > 0)
    }

    /// Mark a task as started (Working) with `started_at` set.
    pub async fn mark_started(&self, id: Uuid) -> Result<bool, StorageError> {
        let result = sqlx::query(
            "UPDATE tasks
             SET status = 'working', started_at = ?
             WHERE id = ? AND status = 'submitted'",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(id.to_string())
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdateTaskStatus {
            task_id: id.to_string(),
            status: TaskStatus::Working.as_str().to_string(),
            source,
        })?;
        Ok(result.rows_affected() > 0)
    }

    /// Move the task to `Completed` with `completed_at` set.
    pub async fn mark_completed(&self, id: Uuid) -> Result<bool, StorageError> {
        self.set_status(id, TaskStatus::Completed).await
    }

    /// Record which daemon instance owns the task's live runtime.
    pub async fn set_runtime_owner(&self, id: Uuid, instance_id: &str) -> Result<(), StorageError> {
        sqlx::query("UPDATE tasks SET runtime_owner = ? WHERE id = ?")
            .bind(instance_id)
            .bind(id.to_string())
            .execute(self.database.pool())
            .await
            .map_err(|source| StorageError::UpdateTaskStatus {
                task_id: id.to_string(),
                status: "runtime_owner".to_string(),
                source,
            })?;
        Ok(())
    }

    /// Update the runtime heartbeat timestamp.
    pub async fn heartbeat(&self, id: Uuid) -> Result<(), StorageError> {
        sqlx::query("UPDATE tasks SET runtime_heartbeat_at = ? WHERE id = ?")
            .bind(Utc::now().to_rfc3339())
            .bind(id.to_string())
            .execute(self.database.pool())
            .await
            .map_err(|source| StorageError::UpdateTaskStatus {
                task_id: id.to_string(),
                status: "runtime_heartbeat_at".to_string(),
                source,
            })?;
        Ok(())
    }

    /// Fail all non-terminal tasks owned by a dead daemon instance.
    ///
    /// Only tasks whose `runtime_owner` is set and differs from
    /// `current_instance_id` are touched; unowned (legacy) tasks are kept
    /// as-is because nobody can prove they are stale.
    pub async fn recover_stale_owned_tasks(
        &self,
        current_instance_id: &str,
    ) -> Result<u64, StorageError> {
        let result = sqlx::query(
            "UPDATE tasks
             SET status = 'failed',
                 error = 'AgentMesh daemon terminated before task completion.',
                 completed_at = ?
             WHERE status IN ('submitted', 'working', 'input_required')
               AND runtime_owner IS NOT NULL
               AND runtime_owner != ?",
        )
        .bind(Utc::now().to_rfc3339())
        .bind(current_instance_id)
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::UpdateTaskStatus {
            task_id: "*".to_string(),
            status: "recover_stale".to_string(),
            source,
        })?;
        Ok(result.rows_affected())
    }

    /// Update the workspace the task ran in.
    pub async fn set_workspace(
        &self,
        id: Uuid,
        workspace: Option<&std::path::Path>,
    ) -> Result<(), StorageError> {
        sqlx::query("UPDATE tasks SET workspace = ? WHERE id = ?")
            .bind(workspace.map(|p| p.display().to_string()))
            .bind(id.to_string())
            .execute(self.database.pool())
            .await
            .map_err(|source| StorageError::UpdateTaskStatus {
                task_id: id.to_string(),
                status: "workspace".to_string(),
                source,
            })?;
        Ok(())
    }
}

fn row_to_task(row: &sqlx::sqlite::SqliteRow) -> Result<AgentTask, StorageError> {
    let task_id: String = row.get("id");
    TaskRow {
        id: task_id.clone(),
        agent_id: row.get("agent_id"),
        status: row.get("status"),
        prompt: row.get("prompt"),
        workspace: row.get("workspace"),
        error: row.get("error"),
        created_at: row.get("created_at"),
        started_at: row.get("started_at"),
        completed_at: row.get("completed_at"),
        context_id: row.get("context_id"),
        agent_session_id: row.get("agent_session_id"),
    }
    .into_domain()
}
