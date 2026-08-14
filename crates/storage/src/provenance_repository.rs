//! Provenance repository (Phase 24).
//!
//! Provides strictly append-only, tamper-evident audit logging for AgentMesh.
//! Updates and deletes are prohibited by design.

use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

use crate::database::Database;
use crate::error::StorageError;
use agentmesh_core::provenance::{
    PROVENANCE_SCHEMA_VERSION, ProvenanceEvent, compute_event_hash, compute_payload_hash,
};

/// SQL row shape of `provenance_events`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProvenanceEventRow {
    pub id: Uuid,
    pub workflow_id: Option<Uuid>,
    pub sequence: i64,
    pub event_type: String,
    pub entity_type: String,
    pub entity_id: String,
    pub parent_event_id: Option<Uuid>,
    pub actor_type: String,
    pub actor_id: Option<String>,
    pub payload_json: String,
    pub payload_hash: String,
    pub previous_hash: Option<String>,
    pub event_hash: String,
    pub created_at: String,
}

impl ProvenanceEventRow {
    pub fn to_dto(&self) -> ProvenanceEvent {
        let payload: serde_json::Value =
            serde_json::from_str(&self.payload_json).unwrap_or(serde_json::Value::Null);
        ProvenanceEvent {
            id: self.id,
            workflow_id: self.workflow_id,
            sequence: self.sequence,
            event_type: self.event_type.clone(),
            entity_type: self.entity_type.clone(),
            entity_id: self.entity_id.clone(),
            parent_event_id: self.parent_event_id,
            actor_type: self.actor_type.clone(),
            actor_id: self.actor_id.clone(),
            payload,
            payload_hash: self.payload_hash.clone(),
            previous_hash: self.previous_hash.clone(),
            event_hash: self.event_hash.clone(),
            created_at: self.created_at.clone(),
            schema_version: PROVENANCE_SCHEMA_VERSION,
        }
    }
}

/// Append-only repository for provenance events.
#[derive(Clone)]
pub struct ProvenanceRepository {
    database: Database,
}

impl ProvenanceRepository {
    pub fn new(database: Database) -> Self {
        Self { database }
    }

    /// Atomically appends a provenance event to the workflow's hash chain.
    ///
    /// Automatically resolves the next sequential integer and previous event hash
    /// inside an immediate SQLite transaction, guaranteeing continuous sequences
    /// without duplicate collisions under concurrency.
    #[allow(clippy::too_many_arguments)]
    pub async fn append_event(
        &self,
        workflow_id: Option<Uuid>,
        event_type: &str,
        entity_type: &str,
        entity_id: &str,
        parent_event_id: Option<Uuid>,
        actor_type: &str,
        actor_id: Option<&str>,
        payload: &serde_json::Value,
    ) -> Result<ProvenanceEventRow, StorageError> {
        let (canonical_json, payload_hash) = compute_payload_hash(payload);

        let mut attempts = 0;
        loop {
            attempts += 1;
            let mut tx = self
                .database
                .pool()
                .begin()
                .await
                .map_err(StorageError::ListProvenanceEvents)?;

            // Fetch the latest event for this workflow to determine sequence and previous hash
            let last_row = match workflow_id {
                Some(wid) => {
                    let wid_str = wid.to_string();
                    sqlx::query(
                        "SELECT sequence, event_hash FROM provenance_events WHERE workflow_id = ? ORDER BY sequence DESC LIMIT 1",
                    )
                    .bind(wid_str)
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(StorageError::ListProvenanceEvents)?
                }
                None => {
                    sqlx::query(
                        "SELECT sequence, event_hash FROM provenance_events WHERE workflow_id IS NULL ORDER BY sequence DESC LIMIT 1",
                    )
                    .fetch_optional(&mut *tx)
                    .await
                    .map_err(StorageError::ListProvenanceEvents)?
                }
            };

            let (sequence, previous_hash) = match last_row {
                Some(row) => {
                    let seq: i64 = row.get("sequence");
                    let hash: String = row.get("event_hash");
                    (seq + 1, Some(hash))
                }
                None => (1, None),
            };

            let event_id = Uuid::new_v4();
            let wid_str = workflow_id.map(|w| w.to_string());
            let event_hash = compute_event_hash(
                previous_hash.as_deref(),
                wid_str.as_deref(),
                sequence,
                event_type,
                entity_type,
                entity_id,
                actor_type,
                actor_id,
                &payload_hash,
            );
            let created_at = Utc::now().to_rfc3339();

            let row = ProvenanceEventRow {
                id: event_id,
                workflow_id,
                sequence,
                event_type: event_type.to_string(),
                entity_type: entity_type.to_string(),
                entity_id: entity_id.to_string(),
                parent_event_id,
                actor_type: actor_type.to_string(),
                actor_id: actor_id.map(str::to_string),
                payload_json: canonical_json.clone(),
                payload_hash: payload_hash.clone(),
                previous_hash,
                event_hash,
                created_at,
            };

            let insert_res = sqlx::query(
                r#"
                INSERT INTO provenance_events (
                    id, workflow_id, sequence, event_type, entity_type, entity_id,
                    parent_event_id, actor_type, actor_id, payload_json, payload_hash,
                    previous_hash, event_hash, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
            )
            .bind(row.id.to_string())
            .bind(row.workflow_id.map(|w| w.to_string()))
            .bind(row.sequence)
            .bind(&row.event_type)
            .bind(&row.entity_type)
            .bind(&row.entity_id)
            .bind(row.parent_event_id.map(|p| p.to_string()))
            .bind(&row.actor_type)
            .bind(&row.actor_id)
            .bind(&row.payload_json)
            .bind(&row.payload_hash)
            .bind(&row.previous_hash)
            .bind(&row.event_hash)
            .bind(&row.created_at)
            .execute(&mut *tx)
            .await;

            match insert_res {
                Ok(_) => {
                    if let Err(err) = tx.commit().await {
                        if attempts < 20 {
                            tokio::time::sleep(std::time::Duration::from_millis(5 * attempts))
                                .await;
                            continue;
                        }
                        return Err(StorageError::ListProvenanceEvents(err));
                    }
                    return Ok(row);
                }
                Err(err) => {
                    let _ = tx.rollback().await;
                    if attempts < 20 {
                        tokio::time::sleep(std::time::Duration::from_millis(5 * attempts)).await;
                        continue;
                    }
                    return Err(StorageError::AppendProvenanceEvent {
                        event_id: row.id.to_string(),
                        source: err,
                    });
                }
            }
        }
    }

    /// Directly insert a prepared row (e.g. for synthetic snapshots or tests).
    pub async fn insert_raw_row(&self, row: &ProvenanceEventRow) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO provenance_events (
                id, workflow_id, sequence, event_type, entity_type, entity_id,
                parent_event_id, actor_type, actor_id, payload_json, payload_hash,
                previous_hash, event_hash, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(row.id.to_string())
        .bind(row.workflow_id.map(|w| w.to_string()))
        .bind(row.sequence)
        .bind(&row.event_type)
        .bind(&row.entity_type)
        .bind(&row.entity_id)
        .bind(row.parent_event_id.map(|p| p.to_string()))
        .bind(&row.actor_type)
        .bind(&row.actor_id)
        .bind(&row.payload_json)
        .bind(&row.payload_hash)
        .bind(&row.previous_hash)
        .bind(&row.event_hash)
        .bind(&row.created_at)
        .execute(self.database.pool())
        .await
        .map_err(|source| StorageError::AppendProvenanceEvent {
            event_id: row.id.to_string(),
            source,
        })?;
        Ok(())
    }

    /// Loads a single provenance event by ID.
    pub async fn get_event(&self, id: Uuid) -> Result<Option<ProvenanceEventRow>, StorageError> {
        let id_str = id.to_string();
        let row = sqlx::query(
            r#"
            SELECT id, workflow_id, sequence, event_type, entity_type, entity_id,
                   parent_event_id, actor_type, actor_id, payload_json, payload_hash,
                   previous_hash, event_hash, created_at
            FROM provenance_events
            WHERE id = ?
            "#,
        )
        .bind(id_str)
        .fetch_optional(self.database.pool())
        .await
        .map_err(|source| StorageError::LoadProvenanceEvent {
            event_id: id.to_string(),
            source,
        })?;

        Ok(row.map(|r| map_row(&r)))
    }

    /// Lists all provenance events for a workflow in ascending sequence order.
    pub async fn list_for_workflow(
        &self,
        workflow_id: Uuid,
    ) -> Result<Vec<ProvenanceEventRow>, StorageError> {
        let wid_str = workflow_id.to_string();
        let rows = sqlx::query(
            r#"
            SELECT id, workflow_id, sequence, event_type, entity_type, entity_id,
                   parent_event_id, actor_type, actor_id, payload_json, payload_hash,
                   previous_hash, event_hash, created_at
            FROM provenance_events
            WHERE workflow_id = ?
            ORDER BY sequence ASC
            "#,
        )
        .bind(wid_str)
        .fetch_all(self.database.pool())
        .await
        .map_err(StorageError::ListProvenanceEvents)?;

        Ok(rows.iter().map(map_row).collect())
    }

    /// Counts total provenance events recorded for a workflow.
    pub async fn count_for_workflow(&self, workflow_id: Uuid) -> Result<i64, StorageError> {
        let wid_str = workflow_id.to_string();
        let row =
            sqlx::query("SELECT COUNT(*) as count FROM provenance_events WHERE workflow_id = ?")
                .bind(wid_str)
                .fetch_one(self.database.pool())
                .await
                .map_err(StorageError::ListProvenanceEvents)?;

        Ok(row.get("count"))
    }

    /// Returns the most recent provenance event for a workflow.
    pub async fn last_for_workflow(
        &self,
        workflow_id: Uuid,
    ) -> Result<Option<ProvenanceEventRow>, StorageError> {
        let wid_str = workflow_id.to_string();
        let row = sqlx::query(
            r#"
            SELECT id, workflow_id, sequence, event_type, entity_type, entity_id,
                   parent_event_id, actor_type, actor_id, payload_json, payload_hash,
                   previous_hash, event_hash, created_at
            FROM provenance_events
            WHERE workflow_id = ?
            ORDER BY sequence DESC
            LIMIT 1
            "#,
        )
        .bind(wid_str)
        .fetch_optional(self.database.pool())
        .await
        .map_err(StorageError::ListProvenanceEvents)?;

        Ok(row.map(|r| map_row(&r)))
    }

    /// Lists provenance events globally with limit and offset.
    pub async fn list_all(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ProvenanceEventRow>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT id, workflow_id, sequence, event_type, entity_type, entity_id,
                   parent_event_id, actor_type, actor_id, payload_json, payload_hash,
                   previous_hash, event_hash, created_at
            FROM provenance_events
            ORDER BY created_at ASC, sequence ASC
            LIMIT ? OFFSET ?
            "#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(self.database.pool())
        .await
        .map_err(StorageError::ListProvenanceEvents)?;

        Ok(rows.iter().map(map_row).collect())
    }
}

fn map_row(r: &sqlx::sqlite::SqliteRow) -> ProvenanceEventRow {
    let id_str: String = r.get("id");
    let wid_str: Option<String> = r.get("workflow_id");
    let parent_str: Option<String> = r.get("parent_event_id");

    ProvenanceEventRow {
        id: Uuid::parse_str(&id_str).unwrap_or_default(),
        workflow_id: wid_str.and_then(|s| Uuid::parse_str(&s).ok()),
        sequence: r.get("sequence"),
        event_type: r.get("event_type"),
        entity_type: r.get("entity_type"),
        entity_id: r.get("entity_id"),
        parent_event_id: parent_str.and_then(|s| Uuid::parse_str(&s).ok()),
        actor_type: r.get("actor_type"),
        actor_id: r.get("actor_id"),
        payload_json: r.get("payload_json"),
        payload_hash: r.get("payload_hash"),
        previous_hash: r.get("previous_hash"),
        event_hash: r.get("event_hash"),
        created_at: r.get("created_at"),
    }
}
