-- 0016_provenance.sql
-- Immutable Provenance Ledger (Phase 24)

CREATE TABLE provenance_events (
    id TEXT PRIMARY KEY,
    workflow_id TEXT,
    sequence INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    parent_event_id TEXT,
    actor_type TEXT NOT NULL,
    actor_id TEXT,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    previous_hash TEXT,
    event_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY(workflow_id)
        REFERENCES workflows(id)
        ON DELETE SET NULL
);

CREATE UNIQUE INDEX idx_provenance_workflow_sequence ON provenance_events(workflow_id, sequence) WHERE workflow_id IS NOT NULL;
CREATE INDEX idx_provenance_events_workflow ON provenance_events(workflow_id);
CREATE INDEX idx_provenance_events_entity ON provenance_events(entity_type, entity_id);
CREATE INDEX idx_provenance_events_created_at ON provenance_events(created_at);
