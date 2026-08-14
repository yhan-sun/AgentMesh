//! Cross-agent context sharing and handoff formatting.
//!
//! Enables heterogeneous agents (e.g. Claude Code, Codex, OpenCode, Antigravity)
//! to inherit conversation transcripts, decision summaries, and artifacts from
//! prior tasks in a safe, structured format.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ArtifactKind, TaskStatus};

/// Summary of an artifact produced by a prior task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriorArtifactSummary {
    pub name: String,
    pub kind: ArtifactKind,
    pub content_preview: Option<String>,
    pub size_bytes: usize,
}

/// Summary of a prior task executed by another agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriorTaskSummary {
    pub task_id: Uuid,
    pub agent_id: String,
    pub status: TaskStatus,
    pub prompt: String,
    pub error: Option<String>,
    pub artifacts: Vec<PriorArtifactSummary>,
    pub created_at: String,
    pub completed_at: Option<String>,
}

/// Format the structured cross-agent context block prepended to the new agent's prompt.
pub fn format_cross_agent_prompt(current_prompt: &str, prior_tasks: &[PriorTaskSummary]) -> String {
    if prior_tasks.is_empty() {
        return current_prompt.to_string();
    }

    let mut out = String::new();
    out.push_str("<prior_agent_context>\n");
    out.push_str(
        "The following previous task(s) were completed by other agents in this session:\n\n",
    );

    for (idx, task) in prior_tasks.iter().enumerate() {
        out.push_str(&format!(
            "### Task {} [ID: {} | Agent: {} | Status: {}]\n",
            idx + 1,
            task.task_id,
            task.agent_id,
            task.status.as_str()
        ));
        out.push_str("**User Request / Input:**\n");
        out.push_str(&task.prompt);
        out.push_str("\n\n");

        if let Some(err) = &task.error {
            out.push_str(&format!("**Execution Error:**\n```\n{err}\n```\n\n"));
        }

        if !task.artifacts.is_empty() {
            out.push_str("**Generated Artifacts & Output:**\n");
            for art in &task.artifacts {
                out.push_str(&format!(
                    "- **{}** (`{}` - {} bytes)\n",
                    art.name,
                    art.kind.key(),
                    art.size_bytes
                ));
                if let Some(preview) = &art.content_preview {
                    out.push_str("```\n");
                    out.push_str(preview);
                    out.push_str("\n```\n");
                }
            }
            out.push('\n');
        }
    }

    out.push_str("</prior_agent_context>\n\n");
    out.push_str("## Current Task Request\n");
    out.push_str(current_prompt);
    out
}

/// Format a comprehensive Markdown document for `.agentmesh/context.md` in the workspace.
pub fn format_workspace_context_md(prior_tasks: &[PriorTaskSummary]) -> String {
    let mut out = String::new();
    out.push_str("# AgentMesh Cross-Agent Session Context\n\n");
    out.push_str("> Auto-generated context snapshot for cross-agent collaboration.\n\n");

    for (idx, task) in prior_tasks.iter().enumerate() {
        out.push_str(&format!(
            "## Task {} — {} (`{}`)\n\n",
            idx + 1,
            task.agent_id,
            task.task_id
        ));
        out.push_str(&format!("- **Status**: `{}`\n", task.status.as_str()));
        out.push_str(&format!("- **Created**: `{}`\n", task.created_at));
        if let Some(completed) = &task.completed_at {
            out.push_str(&format!("- **Completed**: `{completed}`\n"));
        }
        out.push_str("\n### Prompt\n\n");
        out.push_str(&task.prompt);
        out.push_str("\n\n");

        if let Some(err) = &task.error {
            out.push_str(&format!("### Error\n\n```\n{err}\n```\n\n"));
        }

        if !task.artifacts.is_empty() {
            out.push_str("### Artifacts\n\n");
            for art in &task.artifacts {
                out.push_str(&format!(
                    "#### {} ({}, {} bytes)\n\n",
                    art.name,
                    art.kind.key(),
                    art.size_bytes
                ));
                if let Some(content) = &art.content_preview {
                    out.push_str("```\n");
                    out.push_str(content);
                    out.push_str("\n```\n\n");
                }
            }
        }
        out.push_str("---\n\n");
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_cross_agent_prompt_empty() {
        assert_eq!(format_cross_agent_prompt("hello", &[]), "hello");
    }

    #[test]
    fn test_format_cross_agent_prompt_with_prior_task() {
        let task = PriorTaskSummary {
            task_id: Uuid::nil(),
            agent_id: "codex".to_string(),
            status: TaskStatus::Completed,
            prompt: "Design API".to_string(),
            error: None,
            artifacts: vec![PriorArtifactSummary {
                name: "api.json".to_string(),
                kind: ArtifactKind::Json,
                content_preview: Some("{\"ok\":true}".to_string()),
                size_bytes: 11,
            }],
            created_at: "2026-08-14T00:00:00Z".to_string(),
            completed_at: Some("2026-08-14T00:00:01Z".to_string()),
        };

        let formatted = format_cross_agent_prompt("Implement API in Rust", &[task]);
        assert!(formatted.contains("<prior_agent_context>"));
        assert!(formatted.contains("codex"));
        assert!(formatted.contains("Design API"));
        assert!(formatted.contains("api.json"));
        assert!(formatted.contains("## Current Task Request\nImplement API in Rust"));
    }
}
