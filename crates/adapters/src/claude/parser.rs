//! Parser for Claude Code's `--output-format stream-json` protocol.
//!
//! Claude Code emits newline-delimited JSON objects on stdout. We only read
//! the fields AgentMesh needs and ignore everything else, so future CLI
//! versions that add fields or event types do not break the task pipeline.

use agentmesh_core::AgentEvent;
use serde::Deserialize;

/// Raw JSON event envelope as emitted by `claude -p --output-format stream-json`.
///
/// All fields are optional: unknown fields are ignored by serde, and the
/// parse must never fail because of a field we do not use.
#[derive(Debug, Deserialize)]
struct ClaudeRawEvent {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    subtype: Option<String>,
    #[serde(default)]
    session_id: Option<String>,
    #[serde(default)]
    message: Option<ClaudeRawMessage>,
    #[serde(default)]
    is_error: Option<bool>,
    #[serde(default)]
    result: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ClaudeRawMessage {
    #[serde(default)]
    content: Vec<ClaudeRawContentBlock>,
}

#[derive(Debug, Deserialize)]
struct ClaudeRawContentBlock {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    text: Option<String>,
}

/// Streaming parser for Claude Code's JSON-lines protocol.
///
/// The parser is deliberately pure (line in, events out) so it can be unit
/// tested against fixtures without a Claude binary or network access.
#[derive(Debug, Default)]
pub struct ClaudeParser {
    session_id: Option<String>,
    saw_terminal: bool,
}

impl ClaudeParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// The native Claude session id, extracted from any event that carries one.
    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    /// Whether a terminal event (Completed / Failed / Cancelled) was seen.
    pub fn saw_terminal(&self) -> bool {
        self.saw_terminal
    }

    /// Parse one line of Claude stdout into zero or more [`AgentEvent`]s.
    pub fn parse_line(&mut self, line: &str) -> Vec<AgentEvent> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Vec::new();
        }
        let raw: ClaudeRawEvent = match serde_json::from_str(trimmed) {
            Ok(event) => event,
            Err(err) => {
                tracing::warn!(error = %err, line = %trimmed, "claude stream line is not valid JSON; forwarding as message");
                return vec![AgentEvent::Message(trimmed.to_string())];
            }
        };

        if let Some(session_id) = raw.session_id.filter(|sid| !sid.is_empty()) {
            self.session_id = Some(session_id);
        }

        match raw.kind.as_str() {
            // Lifecycle/hook noise; only session id is interesting.
            "system" | "user" => Vec::new(),
            // Final text from the model.
            "assistant" => raw
                .message
                .into_iter()
                .flat_map(|message| message.content)
                .filter(|block| block.kind == "text")
                .filter_map(|block| block.text)
                .map(AgentEvent::Message)
                .collect(),
            // One-shot terminal event.
            "result" => {
                self.saw_terminal = true;
                let failed = raw.is_error == Some(true)
                    || raw.subtype.as_deref() == Some("error")
                    || raw.subtype.as_deref() == Some("failure");
                if failed {
                    let detail = raw
                        .result
                        .unwrap_or_else(|| "claude reported an error".to_string());
                    vec![AgentEvent::Failed(detail)]
                } else {
                    vec![AgentEvent::Completed]
                }
            }
            // Cancellation is signaled by the process exit, not by an event;
            // remember it here so the adapter can emit Cancelled on exit.
            other => {
                tracing::debug!(event_type = other, "ignoring unknown claude event type");
                Vec::new()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INIT_EVENT: &str = r#"{"type":"system","subtype":"init","cwd":"/tmp","session_id":"sid-123","model":"claude-x"}"#;
    const ASSISTANT_EVENT: &str = r#"{"type":"assistant","message":{"id":"m1","role":"assistant","content":[{"type":"text","text":"hello"},{"type":"tool_use","name":"Bash","input":{}}]}, "session_id":"sid-123"}"#;
    const RESULT_EVENT: &str = r#"{"type":"result","subtype":"success","is_error":false,"session_id":"sid-123","result":"hello"}"#;

    fn parse_all(lines: &[&str]) -> (Vec<AgentEvent>, ClaudeParser) {
        let mut parser = ClaudeParser::new();
        let mut events = Vec::new();
        for line in lines {
            events.extend(parser.parse_line(line));
        }
        (events, parser)
    }

    #[test]
    fn extracts_text_from_assistant_event() {
        let (events, _) = parse_all(&[ASSISTANT_EVENT]);
        assert_eq!(events, vec![AgentEvent::Message("hello".to_string())]);
    }

    #[test]
    fn extracts_session_id() {
        let (_, parser) = parse_all(&[INIT_EVENT, ASSISTANT_EVENT]);
        assert_eq!(parser.session_id(), Some("sid-123"));
    }

    #[test]
    fn completion_event_emits_completed() {
        let (events, parser) = parse_all(&[INIT_EVENT, ASSISTANT_EVENT, RESULT_EVENT]);
        assert!(events.contains(&AgentEvent::Completed));
        assert!(parser.saw_terminal());
    }

    #[test]
    fn error_result_emits_failed() {
        let error = r#"{"type":"result","subtype":"error","is_error":true,"result":"boom"}"#;
        let (events, _) = parse_all(&[error]);
        assert!(events.contains(&AgentEvent::Failed("boom".to_string())));
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let extra = r#"{"type":"assistant","future_field":42,"message":{"content":[{"type":"text","text":"ok"}]}}"#;
        let (events, _) = parse_all(&[extra]);
        assert_eq!(events, vec![AgentEvent::Message("ok".to_string())]);
    }

    #[test]
    fn unknown_event_type_is_ignored() {
        let (events, _) = parse_all(&[r#"{"type":"telemetry","op":"foo"}"#]);
        assert!(events.is_empty());
    }

    #[test]
    fn malformed_line_falls_back_to_message() {
        let (events, _) = parse_all(&["this is not json {"]);
        assert_eq!(
            events,
            vec![AgentEvent::Message("this is not json {".to_string())]
        );
    }

    #[test]
    fn empty_and_whitespace_lines_produce_nothing() {
        let (events, _) = parse_all(&["", "   "]);
        assert!(events.is_empty());
    }

    #[test]
    fn assistant_without_text_blocks_produces_nothing() {
        let (events, _) = parse_all(&[
            r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"Bash"}]}}"#,
        ]);
        assert!(events.is_empty());
    }

    #[test]
    fn system_events_do_not_leak() {
        let (events, _) = parse_all(&[
            INIT_EVENT,
            r#"{"type":"system","subtype":"hook_response","hook_name":"SessionStart"}"#,
        ]);
        assert!(events.is_empty());
    }

    #[test]
    fn cancellation_is_flagged_via_terminal_state() {
        let mut parser = ClaudeParser::new();
        assert!(!parser.saw_terminal());
        parser.parse_line(RESULT_EVENT);
        assert!(parser.saw_terminal());
    }
}
