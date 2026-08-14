//! Parser for OpenCode's `run --format json` protocol.
//!
//! OpenCode emits newline-delimited JSON events on stdout (verified against
//! opencode 1.18.16). Only the fields AgentMesh needs are modeled; unknown
//! fields and event types are ignored so CLI upgrades do not break the task
//! pipeline. Reasoning, tool and other internal parts are never surfaced as
//! user-visible messages.

use agentmesh_core::AgentEvent;
use serde::Deserialize;

/// Raw top-level JSON event envelope as emitted by `opencode run --format json`.
///
/// All fields are optional: serde ignores unknown fields, and parsing must
/// never fail because of a field we do not use.
#[derive(Debug, Deserialize)]
struct OpenCodeRawEvent {
    #[serde(rename = "type")]
    event_type: String,
    /// Native session id, present on every event line.
    #[serde(default, rename = "sessionID")]
    session_id: Option<String>,
    #[serde(default)]
    part: Option<OpenCodeRawPart>,
    #[serde(default)]
    error: Option<OpenCodeRawError>,
}

#[derive(Debug, Deserialize)]
struct OpenCodeRawPart {
    #[serde(default)]
    text: Option<String>,
}

/// `error` events carry their detail under `error.data.message`.
#[derive(Debug, Deserialize)]
struct OpenCodeRawError {
    #[serde(default)]
    data: Option<OpenCodeRawErrorData>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OpenCodeRawErrorData {
    #[serde(default)]
    message: Option<String>,
}

/// Streaming parser for OpenCode's JSON-lines protocol.
///
/// Pure: line in, events out — unit testable against fixtures without an
/// OpenCode binary or network access.
#[derive(Debug, Default)]
pub struct OpenCodeParser {
    session_id: Option<String>,
    saw_terminal: bool,
    /// Last error message seen; used as failure detail when a run ends
    /// without a proper terminal event.
    last_error: Option<String>,
}

impl OpenCodeParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// The native OpenCode session id, once any event line carries one.
    pub fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    /// Whether a terminal event (Completed / Failed) was already emitted.
    pub fn saw_terminal(&self) -> bool {
        self.saw_terminal
    }

    /// The last error message seen, if any.
    pub fn last_error(&self) -> Option<&str> {
        self.last_error.as_deref()
    }

    /// Parse one line of OpenCode stdout into zero or more [`AgentEvent`]s.
    pub fn parse_line(&mut self, line: &str) -> Vec<AgentEvent> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Vec::new();
        }
        let raw: OpenCodeRawEvent = match serde_json::from_str(trimmed) {
            Ok(event) => event,
            Err(err) => {
                // OpenCode is run in `--format json`; non-JSON stdout is not
                // an agent answer, so warn and drop it.
                tracing::warn!(error = %err, line = %trimmed, "opencode stream line is not valid JSON; ignoring");
                return Vec::new();
            }
        };

        if let Some(session_id) = raw.session_id.filter(|id| !id.is_empty()) {
            self.session_id = Some(session_id);
        }

        match raw.event_type.as_str() {
            // Assistant text part: the user-visible answer.
            "text" => match raw.part {
                Some(OpenCodeRawPart {
                    text: Some(text), ..
                }) => vec![AgentEvent::Message(text)],
                _ => Vec::new(),
            },
            // A step finished (reason `stop`, `abort`, ...): treat the run as
            // complete on the first terminal event.
            "step_finish" => {
                if self.saw_terminal {
                    Vec::new()
                } else {
                    self.saw_terminal = true;
                    vec![AgentEvent::Completed]
                }
            }
            // Fatal error event: the run is over.
            "error" => {
                if self.saw_terminal {
                    Vec::new()
                } else {
                    self.saw_terminal = true;
                    let detail = raw
                        .error
                        .as_ref()
                        .and_then(|err| {
                            err.data
                                .as_ref()
                                .and_then(|data| data.message.clone())
                                .or_else(|| err.message.clone())
                        })
                        .or_else(|| self.last_error.clone())
                        .unwrap_or_else(|| "OpenCode run failed".to_string());
                    vec![AgentEvent::Failed(detail)]
                }
            }
            // Internal/session bookkeeping: not user-visible.
            "step_start" | "message" | "reasoning" | "tool" | "file" | "command" | "agent"
            | "patch" | "snapshot" | "auth" => {
                tracing::debug!(
                    event_type = raw.event_type.as_str(),
                    "opencode internal event"
                );
                Vec::new()
            }
            other => {
                tracing::debug!(event_type = other, "ignoring unknown opencode event type");
                Vec::new()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STEP_START: &str = r#"{"type":"step_start","timestamp":1786503665206,"sessionID":"ses_00c1578aeffeI57OZaxthCr4R4","part":{"id":"prt_1","messageID":"msg_1","sessionID":"ses_00c1578aeffeI57OZaxthCr4R4","type":"step-start"}}"#;
    const TEXT: &str = r#"{"type":"text","timestamp":1786503665548,"sessionID":"ses_00c1578aeffeI57OZaxthCr4R4","part":{"id":"prt_2","messageID":"msg_1","sessionID":"ses_00c1578aeffeI57OZaxthCr4R4","type":"text","text":"OPENCODE","time":{"start":1786503665493,"end":1786503665542}}}"#;
    const STEP_FINISH: &str = r#"{"type":"step_finish","timestamp":1786503665634,"sessionID":"ses_00c1578aeffeI57OZaxthCr4R4","part":{"id":"prt_3","reason":"stop","messageID":"msg_1","sessionID":"ses_00c1578aeffeI57OZaxthCr4R4","type":"step-finish","tokens":{"total":10438,"input":8642,"output":4}}}"#;
    const ERROR_EVENT: &str = r#"{"type":"error","timestamp":1786504012928,"sessionID":"ses_00c101caeffeOfiJ5Muc2qvdX4","error":{"name":"UnknownError","data":{"message":"Unexpected server error. Check server logs for details.","ref":"err_a96a05eb"}}}"#;

    fn parse_all(lines: &[&str]) -> (Vec<AgentEvent>, OpenCodeParser) {
        let mut parser = OpenCodeParser::new();
        let mut events = Vec::new();
        for line in lines {
            events.extend(parser.parse_line(line));
        }
        (events, parser)
    }

    #[test]
    fn step_start_extracts_session_id() {
        let (events, parser) = parse_all(&[STEP_START]);
        assert!(events.is_empty());
        assert_eq!(parser.session_id(), Some("ses_00c1578aeffeI57OZaxthCr4R4"));
        assert!(!parser.saw_terminal());
    }

    #[test]
    fn text_becomes_message() {
        let (events, _) = parse_all(&[STEP_START, TEXT]);
        assert_eq!(events, vec![AgentEvent::Message("OPENCODE".to_string())]);
    }

    #[test]
    fn step_finish_emits_completed() {
        let (events, parser) = parse_all(&[STEP_START, TEXT, STEP_FINISH]);
        assert!(events.contains(&AgentEvent::Completed));
        assert!(parser.saw_terminal());
    }

    #[test]
    fn error_event_emits_failed() {
        let (events, parser) = parse_all(&[ERROR_EVENT]);
        assert!(events.contains(&AgentEvent::Failed(
            "Unexpected server error. Check server logs for details.".to_string()
        )));
        assert!(parser.saw_terminal());
    }

    #[test]
    fn reasoning_events_do_not_leak() {
        let reasoning = r#"{"type":"reasoning","timestamp":1,"sessionID":"ses_1","part":{"id":"r1","sessionID":"ses_1","type":"reasoning","text":"thinking about the plan"}}"#;
        let (events, _) = parse_all(&[reasoning, TEXT, STEP_FINISH]);
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, AgentEvent::Message(m) if m.contains("thinking")))
        );
        assert!(events.contains(&AgentEvent::Message("OPENCODE".to_string())));
        assert!(events.contains(&AgentEvent::Completed));
    }

    #[test]
    fn tool_events_do_not_leak() {
        let tool = r#"{"type":"tool","timestamp":1,"sessionID":"ses_1","part":{"id":"t1","sessionID":"ses_1","type":"tool","tool":"bash","state":"running","input":{"command":"ls"}}}"#;
        let (events, _) = parse_all(&[tool, STEP_FINISH]);
        assert!(!events.iter().any(|e| matches!(e, AgentEvent::Message(_))));
        assert!(events.contains(&AgentEvent::Completed));
    }

    #[test]
    fn message_events_with_user_role_do_not_leak() {
        // `message` events carry role context including user echo.
        let user_msg = r#"{"type":"message","timestamp":1,"sessionID":"ses_1","part":{"id":"m1","sessionID":"ses_1","type":"message","role":"user","content":[{"type":"text","text":"my prompt"}]}}"#;
        let (events, _) = parse_all(&[user_msg, STEP_FINISH]);
        assert!(!events.iter().any(|e| matches!(e, AgentEvent::Message(_))));
        assert!(events.contains(&AgentEvent::Completed));
    }

    #[test]
    fn unknown_event_type_is_ignored() {
        let (events, _) = parse_all(&[r#"{"type":"telemetry","sessionID":"ses_1","foo":"bar"}"#]);
        assert!(events.is_empty());
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let extra = r#"{"type":"text","future_field":1,"sessionID":"ses_1","part":{"type":"text","text":"ok","extra":true}}"#;
        let (events, _) = parse_all(&[extra]);
        assert_eq!(events, vec![AgentEvent::Message("ok".to_string())]);
    }

    #[test]
    fn duplicate_terminal_events_are_deduplicated() {
        let (events, _) = parse_all(&[STEP_FINISH, STEP_FINISH]);
        assert_eq!(
            events
                .iter()
                .filter(|e| **e == AgentEvent::Completed)
                .count(),
            1
        );
    }

    #[test]
    fn terminal_after_failed_is_deduplicated() {
        let (events, _) = parse_all(&[ERROR_EVENT, STEP_FINISH]);
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, AgentEvent::Completed | AgentEvent::Failed(_)))
                .count(),
            1
        );
    }

    #[test]
    fn malformed_line_is_warned_and_ignored() {
        let (events, _) = parse_all(&["this is not json {"]);
        assert!(events.is_empty());
    }

    #[test]
    fn empty_lines_produce_nothing() {
        let (events, _) = parse_all(&["", "   "]);
        assert!(events.is_empty());
    }

    #[test]
    fn text_without_session_id_still_emits_message() {
        let line = r#"{"type":"text","part":{"type":"text","text":"lonely answer"}}"#;
        let (events, parser) = parse_all(&[line]);
        assert_eq!(
            events,
            vec![AgentEvent::Message("lonely answer".to_string())]
        );
        assert!(parser.session_id().is_none());
    }

    #[test]
    fn error_without_detail_falls_back() {
        let (events, _) = parse_all(&[r#"{"type":"error","sessionID":"ses_1"}"#]);
        assert!(events.contains(&AgentEvent::Failed("OpenCode run failed".to_string())));
    }
}
