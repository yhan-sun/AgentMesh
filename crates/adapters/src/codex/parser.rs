//! Parser for Codex CLI's `exec --json` protocol.
//!
//! Codex emits newline-delimited JSON events on stdout (verified against
//! codex-cli 0.147.0). Only the fields AgentMesh needs are modeled; unknown
//! fields and event types are ignored so CLI upgrades do not break the task
//! pipeline.

use agentmesh_core::AgentEvent;
use serde::Deserialize;

/// Raw top-level JSON event envelope as emitted by `codex exec --json`.
///
/// All fields are optional: serde ignores unknown fields, and parsing must
/// never fail because of a field we do not use.
#[derive(Debug, Deserialize)]
struct CodexRawEvent {
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default)]
    thread_id: Option<String>,
    #[serde(default)]
    item: Option<CodexRawItem>,
    #[serde(default)]
    error: Option<CodexRawError>,
    /// Top-level message, observed on `error` events.
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CodexRawItem {
    #[serde(rename = "type")]
    item_type: String,
    #[serde(default)]
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CodexRawError {
    #[serde(default)]
    message: Option<String>,
}

/// Streaming parser for Codex's JSON-lines protocol.
///
/// Pure: line in, events out — unit testable against fixtures without a
/// Codex binary or network access.
#[derive(Debug, Default)]
pub struct CodexParser {
    session_id: Option<String>,
    saw_terminal: bool,
    /// Last non-terminal error seen (e.g. retry messages); used as failure
    /// detail when the turn ends without a proper completion.
    last_error: Option<String>,
}

impl CodexParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// The native Codex session id (thread id), once `thread.started` is seen.
    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    /// Whether a terminal event (Completed / Failed) was already emitted.
    pub fn saw_terminal(&self) -> bool {
        self.saw_terminal
    }

    /// The last non-terminal error message, if any.
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    /// Parse one line of Codex stdout into zero or more [`AgentEvent`]s.
    pub fn parse_line(&mut self, line: &str) -> Vec<AgentEvent> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Vec::new();
        }
        let raw: CodexRawEvent = match serde_json::from_str(trimmed) {
            Ok(event) => event,
            Err(err) => {
                // Codex is run in a machine-readable mode; non-JSON stdout is
                // not an agent answer, so warn and drop it.
                tracing::warn!(error = %err, line = %trimmed, "codex stream line is not valid JSON; ignoring");
                return Vec::new();
            }
        };

        if let Some(thread_id) = raw.thread_id.filter(|id| !id.is_empty()) {
            self.session_id = Some(thread_id);
        }

        match raw.event_type.as_str() {
            // Session identifier carrier.
            "thread.started" => Vec::new(),
            "turn.started" => Vec::new(),
            // Progress for individual items (tool calls, file changes, ...):
            // too noisy for user-facing output, keep as debug metadata.
            "item.started" => {
                let kind = raw.item.as_ref().map(|item| item.item_type.as_str());
                tracing::debug!(item_type = kind, "codex item started");
                Vec::new()
            }
            "item.completed" | "item.updated" => match raw.item {
                Some(CodexRawItem {
                    item_type,
                    text: Some(text),
                }) if item_type == "agent_message" => {
                    vec![AgentEvent::Message(text)]
                }
                Some(item) => {
                    tracing::debug!(item_type = %item.item_type, "codex item completed");
                    Vec::new()
                }
                None => Vec::new(),
            },
            // Turn finished successfully.
            "turn.completed" => {
                if self.saw_terminal {
                    Vec::new()
                } else {
                    self.saw_terminal = true;
                    vec![AgentEvent::Completed]
                }
            }
            // Turn failed: terminal.
            "turn.failed" => {
                if self.saw_terminal {
                    Vec::new()
                } else {
                    self.saw_terminal = true;
                    let detail = raw
                        .error
                        .and_then(|error| error.message)
                        .or_else(|| self.last_error.clone())
                        .unwrap_or_else(|| "Codex turn failed".to_string());
                    vec![AgentEvent::Failed(detail)]
                }
            }
            // Non-terminal errors (e.g. "Reconnecting..." retry messages):
            // remember the latest one for failure detail, but never fail the
            // task here — the turn may still complete.
            "error" => {
                let detail = raw
                    .error
                    .and_then(|error| error.message)
                    .or(raw.message)
                    .unwrap_or_else(|| "Codex error".to_string());
                tracing::warn!(error = %detail, "codex reported a non-terminal error");
                self.last_error = Some(detail);
                Vec::new()
            }
            other => {
                tracing::debug!(event_type = other, "ignoring unknown codex event type");
                Vec::new()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const THREAD_STARTED: &str =
        r#"{"type":"thread.started","thread_id":"019fef17-7ec9-76a0-855f-87bb9d399bfd"}"#;
    const TURN_STARTED: &str = r#"{"type":"turn.started"}"#;
    const AGENT_MESSAGE: &str =
        r#"{"type":"item.completed","item":{"id":"item_0","type":"agent_message","text":"hello"}}"#;
    const TURN_COMPLETED: &str = r#"{"type":"turn.completed","usage":{"input_tokens":14841,"cached_input_tokens":7936,"output_tokens":5}}"#;

    fn parse_all(lines: &[&str]) -> (Vec<AgentEvent>, CodexParser) {
        let mut parser = CodexParser::new();
        let mut events = Vec::new();
        for line in lines {
            events.extend(parser.parse_line(line));
        }
        (events, parser)
    }

    #[test]
    fn thread_started_extracts_thread_id() {
        let (events, parser) = parse_all(&[THREAD_STARTED]);
        assert!(events.is_empty());
        assert_eq!(
            parser.session_id(),
            Some("019fef17-7ec9-76a0-855f-87bb9d399bfd")
        );
    }

    #[test]
    fn turn_started_is_not_terminal() {
        let (events, parser) = parse_all(&[THREAD_STARTED, TURN_STARTED]);
        assert!(events.is_empty());
        assert!(!parser.saw_terminal());
    }

    #[test]
    fn agent_message_becomes_message() {
        let (events, _) = parse_all(&[THREAD_STARTED, TURN_STARTED, AGENT_MESSAGE]);
        assert_eq!(events, vec![AgentEvent::Message("hello".to_string())]);
    }

    #[test]
    fn turn_completed_emits_completed() {
        let (events, parser) = parse_all(&[THREAD_STARTED, AGENT_MESSAGE, TURN_COMPLETED]);
        assert!(events.contains(&AgentEvent::Completed));
        assert!(parser.saw_terminal());
    }

    #[test]
    fn turn_failed_emits_failed() {
        let (events, _) = parse_all(&[r#"{"type":"turn.failed"}"#]);
        assert!(events.contains(&AgentEvent::Failed("Codex turn failed".to_string())));
    }

    #[test]
    fn turn_failed_carries_error_message() {
        let (events, _) =
            parse_all(&[r#"{"type":"turn.failed","error":{"message":"model at capacity"}}"#]);
        assert!(events.contains(&AgentEvent::Failed("model at capacity".to_string())));
    }

    #[test]
    fn error_events_are_non_terminal() {
        let (events, parser) = parse_all(&[r#"{"type":"error","message":"model at capacity"}"#]);
        assert!(events.is_empty(), "non-terminal error must not emit events");
        assert!(!parser.saw_terminal());
        assert_eq!(parser.last_error(), Some("model at capacity"));
    }

    #[test]
    fn error_then_turn_failed_emits_failed_with_error_message() {
        let (events, _) = parse_all(&[
            r#"{"type":"error","message":"model at capacity"}"#,
            r#"{"type":"turn.failed"}"#,
        ]);
        assert!(events.contains(&AgentEvent::Failed("model at capacity".to_string())));
    }

    #[test]
    fn retry_errors_then_success_still_completes() {
        let (events, parser) = parse_all(&[
            r#"{"type":"error","message":"Reconnecting... 1/5"}"#,
            r#"{"type":"error","message":"Reconnecting... 2/5"}"#,
            AGENT_MESSAGE,
            TURN_COMPLETED,
        ]);
        assert!(!events.iter().any(|e| matches!(e, AgentEvent::Failed(_))));
        assert!(events.contains(&AgentEvent::Completed));
        assert!(parser.saw_terminal());
    }

    #[test]
    fn unknown_event_type_is_ignored() {
        let (events, _) = parse_all(&[r#"{"type":"telemetry","foo":"bar"}"#]);
        assert!(events.is_empty());
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let extra = r#"{"type":"item.completed","future_field":1,"item":{"id":"i0","type":"agent_message","text":"ok","extra":true}}"#;
        let (events, _) = parse_all(&[extra]);
        assert_eq!(events, vec![AgentEvent::Message("ok".to_string())]);
    }

    #[test]
    fn command_execution_items_do_not_leak() {
        let started = r#"{"type":"item.started","item":{"id":"i1","type":"command_execution","command":"/bin/zsh -lc 'echo hi'","status":"in_progress"}}"#;
        let completed = r#"{"type":"item.completed","item":{"id":"i1","type":"command_execution","command":"/bin/zsh -lc 'echo hi'","aggregated_output":"hello\n","exit_code":0,"status":"completed"}}"#;
        let (events, _) = parse_all(&[started, completed, TURN_COMPLETED]);
        assert!(!events.iter().any(|e| matches!(e, AgentEvent::Message(_))));
        assert!(events.contains(&AgentEvent::Completed));
    }

    #[test]
    fn reasoning_items_do_not_leak() {
        let reasoning = r#"{"type":"item.completed","item":{"id":"r0","type":"reasoning","summary":"thinking about the plan"}}"#;
        let (events, _) = parse_all(&[reasoning, TURN_COMPLETED]);
        assert!(!events.iter().any(|e| matches!(e, AgentEvent::Message(_))));
        assert!(events.contains(&AgentEvent::Completed));
    }

    #[test]
    fn malformed_line_is_warned_and_ignored() {
        let (events, _) = parse_all(&["this is not json {"]);
        assert!(events.is_empty());
    }

    #[test]
    fn duplicate_terminal_events_are_deduplicated() {
        let (events, parser) = parse_all(&[TURN_COMPLETED, TURN_COMPLETED, AGENT_MESSAGE]);
        assert_eq!(
            events
                .iter()
                .filter(|e| **e == AgentEvent::Completed)
                .count(),
            1
        );
        assert!(parser.saw_terminal());
    }

    #[test]
    fn terminal_after_failed_is_deduplicated() {
        let (events, _) = parse_all(&[r#"{"type":"turn.failed"}"#, TURN_COMPLETED]);
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, AgentEvent::Completed | AgentEvent::Failed(_)))
                .count(),
            1
        );
    }

    #[test]
    fn empty_lines_produce_nothing() {
        let (events, _) = parse_all(&["", "   "]);
        assert!(events.is_empty());
    }
}
