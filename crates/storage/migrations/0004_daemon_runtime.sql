-- Daemon runtime ownership: which daemon instance owns a task's live run,
-- plus the last heartbeat for diagnostics.

ALTER TABLE tasks ADD COLUMN runtime_owner TEXT;
ALTER TABLE tasks ADD COLUMN runtime_heartbeat_at TEXT;

CREATE INDEX idx_tasks_runtime_owner ON tasks(runtime_owner);
