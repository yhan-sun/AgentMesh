-- Workspace lifecycle + apply concurrency guard (Phase 14).
--
-- * `applies.workspace_snapshot_hash` records the SHA-256 fingerprint of the
--   workspace at apply time so cleanup can detect later changes.
-- * A partial UNIQUE index guarantees at most one `applying`/`completed`
--   apply per workspace, making concurrent applies atomic at the database
--   layer (no application-level race check). A second concurrent apply is
--   rejected as `ApplyInProgress` (another one is running) or `AlreadyApplied`
--   (one already completed).

ALTER TABLE applies ADD COLUMN workspace_snapshot_hash TEXT;

CREATE UNIQUE INDEX idx_applies_workspace_active_unique
    ON applies(workspace_id)
    WHERE status IN ('applying', 'completed');

CREATE INDEX idx_applies_status ON applies(status);
CREATE INDEX idx_applies_workspace ON applies(workspace_id);
