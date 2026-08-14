//! Parser for Antigravity CLI's `--output-format stream-json` protocol.
//!
//! Antigravity emits newline-delimited JSON events on stdout (schema verified
//! against agy 1.1.12 and the official headless-mode documentation):
//!
//! ```text
//! {"event":"init","conversation_id":"...","init":{...}}
//! {"event":"step_update","step_update":{...}}
//! {"event":"result","result":{"conversation_id":"...","status":"SUCCESS","response":"..."}}
//! ```
//!
//! Only user-visible final text is surfaced as [`AgentEvent::Message`];
//! tool calls, subagents, checkpoints and reasoning trajectories are debug
//! metadata. Unknown events and fields are ignored.

use agentmesh_core::AgentEvent;
use serde::Deserialize;

/// Raw stream-json event envelope as emitted by `agy -p --output-format stream-json`.
#[derive(Debug, Deserialize)]
struct AntigravityRawEvent {
    #[serde(rename = "event")]
    event_type: String,
    /// Conversation id carried on the `init` event line.
    #[serde(default)]
    conversation_id: Option<String>,
    #[serde(default)]
    step_update: Option<AntigravityRawStep>,
    #[serde(default)]
    result: Option<AntigravityRawResult>,
}

#[derive(Debug, Deserialize)]
struct AntigravityRawStep {
    #[serde(default)]
    conversation_id: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    step_type: Option<String>,
    #[serde(default)]
    text_delta: Option<String>,
    #[serde(default)]
    tool_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AntigravityRawResult {
    #[serde(default)]
    conversation_id: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    response: Option<String>,
    #[serde(default)]
    error: Option<String>,
}

/// Streaming parser for Antigravity's NDJSON stream.
///
/// Pure: line in, events out — unit testable against fixtures without an agy
/// binary or network access.
#[derive(Debug, Default)]
pub struct AntigravityParser {
    conversation_id: Option<String>,
    saw_terminal: bool,
    /// Text of the last assistant message emitted, to deduplicate against the
    /// terminal `result.response`.
    last_sent_text: Option<String>,
}

impl AntigravityParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// The native Antigravity conversation id, once any event carries one.
    pub fn conversation_id(&self) -> Option<&str> {
        self.conversation_id.as_deref()
    }

    /// Whether a terminal event (Completed / Failed) was already emitted.
    pub fn saw_terminal(&self) -> bool {
        self.saw_terminal
    }

    /// Parse one line of Antigravity stdout into zero or more [`AgentEvent`]s.
    pub fn parse_line(&mut self, line: &str) -> Vec<AgentEvent> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Vec::new();
        }
        let raw: AntigravityRawEvent = match serde_json::from_str(trimmed) {
            Ok(event) => event,
            Err(err) => {
                // agy is run in `--output-format stream-json`; non-JSON stdout
                // is not an agent answer, so warn and drop it.
                tracing::warn!(error = %err, line = %trimmed, "antigravity stream line is not valid JSON; ignoring");
                return Vec::new();
            }
        };

        if let Some(conversation_id) = raw
            .conversation_id
            .as_deref()
            .or_else(|| {
                raw.step_update
                    .as_ref()
                    .and_then(|s| s.conversation_id.as_deref())
            })
            .or_else(|| {
                raw.result
                    .as_ref()
                    .and_then(|r| r.conversation_id.as_deref())
            })
            .filter(|id| !id.is_empty())
        {
            self.conversation_id = Some(conversation_id.to_string());
        }

        match raw.event_type.as_str() {
            // Run configuration; not user-visible.
            "init" => {
                tracing::debug!("antigravity run initialized");
                Vec::new()
            }
            // Step trajectory. Only the completed `agent_response` text is
            // user-visible; partial deltas, tools and checkpoints are not.
            "step_update" => match raw.step_update {
                Some(AntigravityRawStep {
                    state: Some(state),
                    step_type: Some(step_type),
                    text_delta: Some(text_delta),
                    ..
                }) if step_type == "agent_response"
                    && state == "DONE"
                    && !text_delta.is_empty() =>
                {
                    self.last_sent_text = Some(text_delta.clone());
                    vec![AgentEvent::Message(text_delta)]
                }
                Some(step) => {
                    tracing::debug!(
                        state = ?step.state,
                        step_type = ?step.step_type,
                        tool_name = ?step.tool_name,
                        "antigravity step update"
                    );
                    Vec::new()
                }
                None => Vec::new(),
            },
            // Terminal result: same shape as `--output-format json`.
            "result" => {
                if self.saw_terminal {
                    return Vec::new();
                }
                self.saw_terminal = true;
                let Some(result) = raw.result else {
                    return vec![AgentEvent::Failed(
                        "Antigravity run ended without a result".to_string(),
                    )];
                };
                match result.status.as_deref() {
                    Some("SUCCESS") => {
                        let mut events = Vec::new();
                        if let Some(response) = result
                            .response
                            .filter(|text| !text.is_empty())
                            .filter(|text| self.last_sent_text.as_deref() != Some(text.as_str()))
                        {
                            events.push(AgentEvent::Message(response));
                        }
                        events.push(AgentEvent::Completed);
                        events
                    }
                    _ => {
                        let detail = result
                            .error
                            .unwrap_or_else(|| "Antigravity run failed".to_string());
                        vec![AgentEvent::Failed(detail)]
                    }
                }
            }
            other => {
                tracing::debug!(
                    event_type = other,
                    "ignoring unknown antigravity event type"
                );
                Vec::new()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const INIT: &str = r#"{"event":"init","conversation_id":"c3b66b04-872b-4fbe-a3a4-058a026ef20a","init":{"cwd":"/tmp/project","tools":["run_command","write_to_file"],"permission_mode":"request-review"}}"#;
    const STEP_USER: &str = r#"{"event":"step_update","step_update":{"conversation_id":"c3b66b04-872b-4fbe-a3a4-058a026ef20a","step_index":0,"state":"DONE","step_type":"user_input"}}"#;
    const STEP_ACTIVE_DELTA: &str = r#"{"event":"step_update","step_update":{"conversation_id":"c3b66b04-872b-4fbe-a3a4-058a026ef20a","step_index":1,"state":"ACTIVE","step_type":"agent_response","text_delta":"partial chunk "}}"#;
    const STEP_DONE_DELTA: &str = r#"{"event":"step_update","step_update":{"conversation_id":"c3b66b04-872b-4fbe-a3a4-058a026ef20a","step_index":1,"state":"DONE","step_type":"agent_response","text_delta":"full answer","duration_seconds":2.1}}"#;
    const STEP_TOOL: &str = r#"{"event":"step_update","step_update":{"conversation_id":"c3b66b04-872b-4fbe-a3a4-058a026ef20a","step_index":2,"state":"DONE","step_type":"tool","tool_name":"run_command"}}"#;
    const RESULT_SUCCESS: &str = r#"{"event":"result","result":{"conversation_id":"c3b66b04-872b-4fbe-a3a4-058a026ef20a","status":"SUCCESS","response":"full answer","duration_seconds":3.0,"num_turns":1,"usage":{"input_tokens":10,"output_tokens":5,"thinking_tokens":0,"total_tokens":15}}}"#;

    fn parse_all(lines: &[&str]) -> (Vec<AgentEvent>, AntigravityParser) {
        let mut parser = AntigravityParser::new();
        let mut events = Vec::new();
        for line in lines {
            events.extend(parser.parse_line(line));
        }
        (events, parser)
    }

    #[test]
    fn init_extracts_conversation_id() {
        let (events, parser) = parse_all(&[INIT]);
        assert!(events.is_empty());
        assert_eq!(
            parser.conversation_id(),
            Some("c3b66b04-872b-4fbe-a3a4-058a026ef20a")
        );
        assert!(!parser.saw_terminal());
    }

    #[test]
    fn agent_response_done_becomes_message() {
        let (events, _) = parse_all(&[INIT, STEP_USER, STEP_DONE_DELTA, RESULT_SUCCESS]);
        let messages: Vec<&str> = events
            .iter()
            .filter_map(|e| match e {
                AgentEvent::Message(text) => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(messages, vec!["full answer"]);
        assert!(events.contains(&AgentEvent::Completed));
    }

    #[test]
    fn active_partial_deltas_do_not_leak() {
        let (events, _) = parse_all(&[INIT, STEP_ACTIVE_DELTA, STEP_DONE_DELTA, RESULT_SUCCESS]);
        let messages: Vec<&str> = events
            .iter()
            .filter_map(|e| match e {
                AgentEvent::Message(text) => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(messages, vec!["full answer"]);
    }

    #[test]
    fn tool_steps_do_not_leak() {
        let (events, _) = parse_all(&[INIT, STEP_TOOL, RESULT_SUCCESS]);
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, AgentEvent::Message(text) if text.contains("run_command"))),
            "tool payload leaked into messages: {events:?}"
        );
        // The terminal result's own response is still user-visible text.
        assert!(events.contains(&AgentEvent::Message("full answer".to_string())));
        assert!(events.contains(&AgentEvent::Completed));
    }

    #[test]
    fn result_carries_response_when_no_step_text() {
        let result = r#"{"event":"result","result":{"conversation_id":"c1","status":"SUCCESS","response":"direct answer","duration_seconds":1.0}}"#;
        let (events, parser) = parse_all(&[INIT, result]);
        assert!(events.contains(&AgentEvent::Message("direct answer".to_string())));
        assert!(events.contains(&AgentEvent::Completed));
        assert_eq!(parser.conversation_id(), Some("c1"));
    }

    #[test]
    fn duplicate_response_is_deduplicated() {
        let (events, _) = parse_all(&[INIT, STEP_DONE_DELTA, RESULT_SUCCESS]);
        let messages = events
            .iter()
            .filter(|e| matches!(e, AgentEvent::Message(_)))
            .count();
        assert_eq!(messages, 1);
    }

    #[test]
    fn error_status_emits_failed() {
        let result = r#"{"event":"result","result":{"conversation_id":"","status":"ERROR","response":"","error":"authentication failed or timed out","duration_seconds":0,"num_turns":0}}"#;
        let (events, parser) = parse_all(&[result]);
        assert!(events.contains(&AgentEvent::Failed(
            "authentication failed or timed out".to_string()
        )));
        assert!(parser.saw_terminal());
        assert!(parser.conversation_id().is_none());
    }

    #[test]
    fn error_status_without_detail_has_default() {
        let result = r#"{"event":"result","result":{"conversation_id":"c1","status":"ERROR"}}"#;
        let (events, _) = parse_all(&[result]);
        assert!(events.contains(&AgentEvent::Failed("Antigravity run failed".to_string())));
    }

    #[test]
    fn result_without_payload_fails() {
        let (events, _) = parse_all(&[r#"{"event":"result"}"#]);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, AgentEvent::Failed(msg) if msg.contains("without a result")))
        );
    }

    #[test]
    fn conversation_id_seen_from_step_update() {
        let line = r#"{"event":"step_update","step_update":{"conversation_id":"c-from-step","step_index":0,"state":"DONE","step_type":"user_input"}}"#;
        let (_, parser) = parse_all(&[line]);
        assert_eq!(parser.conversation_id(), Some("c-from-step"));
    }

    #[test]
    fn unknown_event_type_is_ignored() {
        let (events, _) = parse_all(&[r#"{"event":"telemetry","foo":"bar"}"#]);
        assert!(events.is_empty());
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let line = r#"{"event":"step_update","future_field":1,"step_update":{"conversation_id":"c1","state":"DONE","step_type":"agent_response","text_delta":"ok","extra":true}}"#;
        let (events, _) = parse_all(&[line]);
        assert_eq!(events, vec![AgentEvent::Message("ok".to_string())]);
    }

    #[test]
    fn duplicate_terminal_events_are_deduplicated() {
        let (events, _) = parse_all(&[RESULT_SUCCESS, RESULT_SUCCESS]);
        assert_eq!(
            events
                .iter()
                .filter(|e| **e == AgentEvent::Completed)
                .count(),
            1
        );
    }

    #[test]
    fn malformed_line_is_warned_and_ignored() {
        let (events, _) = parse_all(&["not json {"]);
        assert!(events.is_empty());
    }

    #[test]
    fn empty_lines_produce_nothing() {
        let (events, _) = parse_all(&["", "  "]);
        assert!(events.is_empty());
    }
}
