-- Plan revision history, edit metadata and atomic execution claims (Phase 18).
--
-- * workflow_plan_revisions keeps every version of a plan's JSON. Revision 1
--   is always the original planner output; later revisions are user edits,
--   so an edit never destroys the planner's structure.
-- * workflow_plans gains current_revision (which revision is active),
--   execution_claimed_at (set atomically when an execute wins the claim) and
--   executed_revision (audit: which revision actually ran).

ALTER TABLE workflow_plans ADD COLUMN current_revision INTEGER;
ALTER TABLE workflow_plans ADD COLUMN execution_claimed_at TEXT;
ALTER TABLE workflow_plans ADD COLUMN executed_revision INTEGER;

CREATE TABLE workflow_plan_revisions (
    id TEXT PRIMARY KEY,
    plan_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    plan_json TEXT NOT NULL,
    source TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (plan_id, revision)
);

CREATE INDEX idx_plan_revisions_plan ON workflow_plan_revisions(plan_id, revision);

-- Backfill: every pre-Phase-18 plan that already had JSON becomes revision 1
-- (the original planner output) with current_revision = 1, so a `plan edit`
-- on an old plan appends revision 2 without losing the original.
INSERT INTO workflow_plan_revisions (id, plan_id, revision, plan_json, source, created_at)
SELECT lower(hex(randomblob(16))), id, 1, plan_json, 'planner', created_at
FROM workflow_plans
WHERE plan_json IS NOT NULL;

UPDATE workflow_plans
SET current_revision = 1
WHERE plan_json IS NOT NULL AND current_revision IS NULL;
