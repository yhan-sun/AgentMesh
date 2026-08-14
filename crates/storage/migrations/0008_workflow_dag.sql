-- DAG workflow persistence (Phase 16): node identity + dependency edges.
--
-- A DAG workflow stores one `workflow_steps` row per node, keyed by
-- (workflow_id, ordinal) exactly like a sequential step. `node_id` is the
-- stable node slug (e.g. `security_review`), NULL for legacy sequential steps.
--
-- The DAG itself lives in `workflow_step_dependencies`: one row per directed
-- edge (dependent node -> dependency node). A sequential workflow has no
-- dependency rows, so it resumes through the pre-Phase-16 path untouched.

ALTER TABLE workflow_steps ADD COLUMN node_id TEXT;

CREATE TABLE workflow_step_dependencies (
    id TEXT PRIMARY KEY,

    workflow_id TEXT NOT NULL,
    -- The dependent node (the node that waits).
    node_id TEXT NOT NULL,
    -- A node_id this node depends on (must complete first).
    depends_on_node_id TEXT NOT NULL,

    FOREIGN KEY(workflow_id)
        REFERENCES workflows(id)
        ON DELETE CASCADE,

    UNIQUE(workflow_id, node_id, depends_on_node_id)
);

CREATE INDEX idx_workflow_step_dependencies_workflow
    ON workflow_step_dependencies(workflow_id);
CREATE INDEX idx_workflow_step_dependencies_node
    ON workflow_step_dependencies(workflow_id, node_id);
