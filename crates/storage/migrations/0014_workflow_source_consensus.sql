-- Phase 22: explicit source workspace + consensus fix rounds.
--
-- * workflows.source_workspace: the explicit source project/repository the
--   user asked AgentMesh to operate on. It is an immutable runtime input —
--   never read from the daemon cwd, never taken from a planner objective or an
--   agent's output. NULL keeps the legacy (daemon-cwd) behavior for old rows.
--
-- * evaluation_groups.round: which consensus fix round a group belongs to.
--   Round 0 is the initial evaluation; round 1 the (single, bounded) fix
--   round. Old rows are backfilled to 0 by the DEFAULT.

ALTER TABLE workflows ADD COLUMN source_workspace TEXT NULL;

ALTER TABLE evaluation_groups ADD COLUMN round INTEGER NOT NULL DEFAULT 0;
