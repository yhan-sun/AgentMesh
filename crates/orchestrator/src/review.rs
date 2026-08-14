//! Reviewer verdict parsing (Phase 11).
//!
//! Review steps must end with a machine-parseable verdict, not free text.
//! The reviewer is instructed to produce a JSON artifact (e.g. `review.json`):
//!
//! ```json
//! { "verdict": "approved", "summary": "...", "issues": [] }
//! { "verdict": "changes_requested", "summary": "...", "issues": [
//!     { "severity": "high", "title": "...", "description": "...", "file": "src/x.rs" }
//! ] }
//! ```
//!
//! Only structured JSON is accepted — a natural-language review is never
//! guessed at, and a missing or malformed verdict fails the review step.

use agentmesh_a2a::mapping::ARTIFACT_KIND_META_KEY;
use agentmesh_a2a::types::{A2AArtifact, Part};
use agentmesh_core::ArtifactKind;
use serde::Deserialize;

use crate::workflow_state::{ReviewIssue, ReviewResult, ReviewSeverity, ReviewVerdict};

/// Parse the structured review verdict from a review step's artifacts.
///
/// Looks at JSON-kind or review-named artifacts and returns the first that
/// parses as a review. Returns `Err(reason)` when the reviewer produced no
/// structured verdict or only malformed ones.
pub fn parse_review(artifacts: &[A2AArtifact]) -> Result<ReviewResult, String> {
    let candidates: Vec<&A2AArtifact> = artifacts
        .iter()
        .filter(|artifact| {
            is_json_kind(artifact) || artifact.name.to_lowercase().contains("review")
        })
        .collect();
    if candidates.is_empty() {
        return Err("reviewer produced no structured verdict artifact".to_string());
    }
    for artifact in candidates {
        let Some(text) = artifact_text(artifact) else {
            continue;
        };
        let Ok(raw) = serde_json::from_str::<RawReview>(&text) else {
            continue;
        };
        return raw_into_result(raw);
    }
    Err("reviewer produced a malformed structured verdict artifact".to_string())
}

/// Render review issues for a fixer / final-reviewer prompt.
pub fn render_issues(issues: &[ReviewIssue]) -> String {
    if issues.is_empty() {
        return "None".to_string();
    }
    let mut out = String::new();
    for (index, issue) in issues.iter().enumerate() {
        out.push_str(&format!(
            "{}. [{}] {}: {}\n",
            index + 1,
            issue.severity.key(),
            issue.title,
            issue.description
        ));
        if let Some(file) = &issue.file {
            out.push_str(&format!("   file: {file}\n"));
        }
    }
    out
}

#[derive(Debug, Deserialize)]
struct RawReview {
    #[serde(default)]
    verdict: Option<String>,
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    issues: Option<Vec<RawIssue>>,
    /// Evaluator confidence (Phase 21 §6), 0.0..=1.0; optional for plain
    /// reviews.
    #[serde(default)]
    confidence: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct RawIssue {
    #[serde(default)]
    severity: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    file: Option<String>,
}

fn raw_into_result(raw: RawReview) -> Result<ReviewResult, String> {
    let verdict = raw
        .verdict
        .as_deref()
        .and_then(ReviewVerdict::from_key)
        .ok_or_else(|| "review verdict field is missing or invalid".to_string())?;
    if let Some(confidence) = raw.confidence
        && !(0.0..=1.0).contains(&confidence)
    {
        return Err(format!(
            "review confidence {confidence} is out of range 0.0..=1.0"
        ));
    }
    Ok(ReviewResult {
        verdict,
        summary: raw.summary.unwrap_or_default(),
        issues: raw
            .issues
            .unwrap_or_default()
            .into_iter()
            .map(|issue| ReviewIssue {
                severity: issue
                    .severity
                    .as_deref()
                    .map(ReviewSeverity::from_key)
                    .unwrap_or(ReviewSeverity::Medium),
                title: issue.title.unwrap_or_default(),
                description: issue.description.unwrap_or_default(),
                file: issue.file,
            })
            .collect(),
        confidence: raw.confidence,
    })
}

fn is_json_kind(artifact: &A2AArtifact) -> bool {
    artifact
        .metadata
        .as_ref()
        .and_then(|m| m.get(ARTIFACT_KIND_META_KEY))
        .and_then(|v| v.as_str())
        .and_then(ArtifactKind::from_key)
        == Some(ArtifactKind::Json)
}

fn artifact_text(artifact: &A2AArtifact) -> Option<String> {
    for part in &artifact.parts {
        match part {
            Part::Text(part) => return Some(part.text.clone()),
            Part::Data(part) => return Some(part.data.to_string()),
            Part::File(_) => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use agentmesh_a2a::types::{DataPart, TextPart};
    use serde_json::json;

    fn json_artifact(name: &str, value: serde_json::Value) -> A2AArtifact {
        A2AArtifact {
            name: name.to_string(),
            parts: vec![Part::Data(DataPart { data: value })],
            metadata: Some(json!({ ARTIFACT_KIND_META_KEY: "json" })),
        }
    }

    fn text_artifact(name: &str, text: &str) -> A2AArtifact {
        A2AArtifact {
            name: name.to_string(),
            parts: vec![Part::Text(TextPart {
                text: text.to_string(),
            })],
            metadata: None,
        }
    }

    #[test]
    fn parses_approved_verdict() {
        let review = json_artifact(
            "review.json",
            json!({ "verdict": "approved", "summary": "looks good", "issues": [] }),
        );
        let result = parse_review(&[review]).expect("parse");
        assert_eq!(result.verdict, ReviewVerdict::Approved);
        assert_eq!(result.summary, "looks good");
        assert!(result.issues.is_empty());
    }

    #[test]
    fn parses_changes_requested_with_issues() {
        let review = json_artifact(
            "review.json",
            json!({
                "verdict": "changes_requested",
                "summary": "issues found",
                "issues": [
                    { "severity": "high", "title": "auth bypass", "description": "missing check", "file": "src/auth.rs" },
                    { "severity": "low", "title": "nit", "description": "rename var" }
                ]
            }),
        );
        let result = parse_review(&[review]).expect("parse");
        assert_eq!(result.verdict, ReviewVerdict::ChangesRequested);
        assert_eq!(result.issues.len(), 2);
        assert_eq!(result.issues[0].severity, ReviewSeverity::High);
        assert_eq!(result.issues[0].file.as_deref(), Some("src/auth.rs"));
        assert_eq!(result.issues[1].severity, ReviewSeverity::Low);
        assert_eq!(result.issues[1].file, None);
    }

    #[test]
    fn verdict_is_case_insensitive() {
        let review = text_artifact(
            "REVIEW.json",
            r#"{"verdict":"APPROVED","summary":"ok","issues":[]}"#,
        );
        let result = parse_review(&[review]).expect("parse");
        assert_eq!(result.verdict, ReviewVerdict::Approved);
    }

    #[test]
    fn no_verdict_artifact_fails() {
        let patch = text_artifact("changes.patch", "diff --git");
        assert!(parse_review(&[patch]).is_err());
        assert!(parse_review(&[]).is_err());
    }

    #[test]
    fn malformed_verdict_value_fails() {
        let review = json_artifact(
            "review.json",
            json!({ "verdict": "maybe", "summary": "x", "issues": [] }),
        );
        let err = parse_review(&[review]).expect_err("invalid verdict");
        assert!(err.contains("verdict"), "{err}");
    }

    #[test]
    fn malformed_json_fails() {
        let review = text_artifact("review.json", "not json at all");
        assert!(parse_review(&[review]).is_err());
    }

    #[test]
    fn unrelated_json_is_ignored_when_no_review() {
        let plan = json_artifact("plan.json", json!({ "modules": ["a"] }));
        assert!(parse_review(&[plan]).is_err());
    }

    #[test]
    fn render_issues_formats_all_fields() {
        let issues = vec![ReviewIssue {
            severity: ReviewSeverity::Critical,
            title: "crash".into(),
            description: "panics".into(),
            file: Some("src/x.rs".into()),
        }];
        let rendered = render_issues(&issues);
        assert!(rendered.contains("[critical] crash: panics"));
        assert!(rendered.contains("file: src/x.rs"));
        assert_eq!(render_issues(&[]), "None");
    }
}
