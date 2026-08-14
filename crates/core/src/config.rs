//! Configuration loading and merging for AgentMesh.
//!
//! Load precedence: project config > user config > defaults.
//!
//! * Project: `./.agentmesh/config.toml`
//! * User:    `~/.config/agentmesh/config.toml` (`%APPDATA%\agentmesh` on Windows)

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// Errors produced while loading configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file `{path}`: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("failed to parse config file `{path}`: {source}")]
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },
}

/// Preferred agent order for a routing skill (intent key -> agent ids).
///
/// The table is keyed by [`agentmesh_core::TaskIntent`] names so the example
/// config reads `implementation = ["codex", "claude"]` even though that
/// intent maps to the `code` skill. Agents are listed most-preferred first.
fn default_architecture() -> Vec<String> {
    vec![
        "claude".into(),
        "codex".into(),
        "opencode".into(),
        "antigravity".into(),
    ]
}
fn default_implementation() -> Vec<String> {
    vec![
        "codex".into(),
        "opencode".into(),
        "claude".into(),
        "antigravity".into(),
    ]
}
fn default_debug() -> Vec<String> {
    vec![
        "codex".into(),
        "opencode".into(),
        "claude".into(),
        "antigravity".into(),
    ]
}
fn default_review() -> Vec<String> {
    vec![
        "claude".into(),
        "codex".into(),
        "opencode".into(),
        "antigravity".into(),
    ]
}
fn default_testing() -> Vec<String> {
    vec![
        "codex".into(),
        "opencode".into(),
        "claude".into(),
        "antigravity".into(),
    ]
}
fn default_uiux() -> Vec<String> {
    vec!["antigravity".into(), "claude".into(), "opencode".into()]
}
fn default_general() -> Vec<String> {
    vec![
        "claude".into(),
        "codex".into(),
        "opencode".into(),
        "antigravity".into(),
    ]
}

/// Deterministic routing preferences, one ordered agent list per intent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingConfig {
    #[serde(default = "default_architecture")]
    pub architecture: Vec<String>,
    #[serde(default = "default_implementation")]
    pub implementation: Vec<String>,
    #[serde(default = "default_debug")]
    pub debug: Vec<String>,
    #[serde(default = "default_review")]
    pub review: Vec<String>,
    #[serde(default = "default_testing")]
    pub testing: Vec<String>,
    #[serde(default = "default_uiux")]
    pub uiux: Vec<String>,
    #[serde(default = "default_general")]
    pub general: Vec<String>,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            architecture: default_architecture(),
            implementation: default_implementation(),
            debug: default_debug(),
            review: default_review(),
            testing: default_testing(),
            uiux: default_uiux(),
            general: default_general(),
        }
    }
}

impl RoutingConfig {
    /// Preferred agent ids for an intent, most preferred first.
    pub fn preferred(&self, intent: crate::TaskIntent) -> &[String] {
        match intent {
            crate::TaskIntent::Architecture => &self.architecture,
            crate::TaskIntent::Implementation => &self.implementation,
            crate::TaskIntent::Debug => &self.debug,
            crate::TaskIntent::Review => &self.review,
            crate::TaskIntent::Testing => &self.testing,
            crate::TaskIntent::UIUX => &self.uiux,
            crate::TaskIntent::General => &self.general,
        }
    }
}

/// Planner policy limits (Phase 18), `[planner.policy]`.
///
/// Every field is optional; an absent field falls back to the safe default in
/// [`agentmesh_orchestrator::PlanPolicy`]. The policy is deterministic local
/// code — the planner can never change it, only the config can.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanPolicyConfig {
    /// Maximum plan nodes (and, for a fixed DAG, maximum agent calls).
    #[serde(default)]
    pub max_nodes: Option<usize>,
    /// Maximum estimated agent calls (= node count for a fixed DAG).
    #[serde(default)]
    pub max_agent_calls: Option<usize>,
    /// Maximum concurrent DAG nodes a `plan execute` may request.
    #[serde(default)]
    pub max_parallel: Option<usize>,
    /// Intents a plan node may use; absent = the full legal intent set.
    #[serde(default)]
    pub allowed_intents: Option<Vec<String>>,
    /// Roles a plan node may use; absent = the full legal role set.
    #[serde(default)]
    pub allowed_roles: Option<Vec<String>>,
}

impl PlanPolicyConfig {
    /// Merge `other` into `self`; `other` wins per-field on conflicts.
    fn merge(&mut self, other: PlanPolicyConfig) {
        if other.max_nodes.is_some() {
            self.max_nodes = other.max_nodes;
        }
        if other.max_agent_calls.is_some() {
            self.max_agent_calls = other.max_agent_calls;
        }
        if other.max_parallel.is_some() {
            self.max_parallel = other.max_parallel;
        }
        if other.allowed_intents.is_some() {
            self.allowed_intents = other.allowed_intents;
        }
        if other.allowed_roles.is_some() {
            self.allowed_roles = other.allowed_roles;
        }
    }
}

/// Planner configuration, `[planner]` (Phase 18).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlannerConfig {
    #[serde(default)]
    pub policy: Option<PlanPolicyConfig>,
}

impl PlannerConfig {
    /// Merge `other` into `self`; `other` wins per-field on conflicts.
    fn merge(&mut self, other: PlannerConfig) {
        if let Some(policy) = other.policy {
            match &mut self.policy {
                Some(existing) => existing.merge(policy),
                None => self.policy = Some(policy),
            }
        }
    }
}

/// Evaluation policy (Phase 21), `[evaluation]`.
///
/// The control plane decides evaluator count, quorum and consensus strategy —
/// the planner never sets them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvaluationConfig {
    /// Hard cap on evaluators in one group (default 5).
    #[serde(default)]
    pub max_evaluators: Option<usize>,
    /// Default evaluators when not specified (default 3).
    #[serde(default)]
    pub default_evaluators: Option<usize>,
    /// Default quorum (default 2).
    #[serde(default)]
    pub default_quorum: Option<usize>,
    /// Consensus strategy: `majority` | `unanimous` (default `majority`).
    #[serde(default)]
    pub strategy: Option<String>,
    /// Maximum total evaluator agent calls across ALL consensus fix rounds
    /// (Phase 22 §16). Default 6 covers 3 evaluators × 2 rounds. Checked
    /// before execution and again before a dynamic fix-loop graph extension.
    #[serde(default)]
    pub max_total_evaluator_calls: Option<usize>,
}

impl EvaluationConfig {
    /// The safe defaults.
    pub fn defaults() -> Self {
        Self {
            max_evaluators: Some(5),
            default_evaluators: Some(3),
            default_quorum: Some(2),
            strategy: Some("majority".to_string()),
            max_total_evaluator_calls: Some(6),
        }
    }

    /// Merge `other` into `self`; `other` wins per-field on conflicts.
    fn merge(&mut self, other: EvaluationConfig) {
        if other.max_evaluators.is_some() {
            self.max_evaluators = other.max_evaluators;
        }
        if other.default_evaluators.is_some() {
            self.default_evaluators = other.default_evaluators;
        }
        if other.default_quorum.is_some() {
            self.default_quorum = other.default_quorum;
        }
        if other.strategy.is_some() {
            self.strategy = other.strategy;
        }
        if other.max_total_evaluator_calls.is_some() {
            self.max_total_evaluator_calls = other.max_total_evaluator_calls;
        }
    }
}

/// Competition policy limits (Phase 23), `[competition]`.
///
/// Best-of-N competition runs multiple candidate implementations in parallel
/// with independent session lanes and worktrees, followed by blind evaluation
/// and deterministic winner selection.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompetitionConfig {
    /// Default number of candidates in a Best-of-N competition (default 2).
    #[serde(default)]
    pub default_candidates: Option<usize>,
    /// Hard cap on candidates in one competition (default 3, max 3).
    #[serde(default)]
    pub max_candidates: Option<usize>,
    /// Maximum total candidate calls allowed (default 3).
    #[serde(default)]
    pub max_total_candidate_calls: Option<usize>,
}

impl CompetitionConfig {
    /// The safe defaults.
    pub fn defaults() -> Self {
        Self {
            default_candidates: Some(2),
            max_candidates: Some(3),
            max_total_candidate_calls: Some(3),
        }
    }

    /// Merge `other` into `self`; `other` wins per-field on conflicts.
    fn merge(&mut self, other: CompetitionConfig) {
        if other.default_candidates.is_some() {
            self.default_candidates = other.default_candidates;
        }
        if other.max_candidates.is_some() {
            self.max_candidates = other.max_candidates;
        }
        if other.max_total_candidate_calls.is_some() {
            self.max_total_candidate_calls = other.max_total_candidate_calls;
        }
    }
}

/// Recovery policy limits (Phase 20), `[recovery]`.
///
/// Failure recovery is bounded: a fixed maximum number of attempts, an agent
/// call budget across the whole recovery chain, and — critically — a proposal
/// is never auto-executed unless `auto_execute` is explicitly `true` (default
/// `false`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryConfig {
    /// Max recovery attempts per failed workflow (default 1, hard cap 2).
    #[serde(default)]
    pub max_attempts: Option<usize>,
    /// Auto-generate a recovery proposal when a node fails (default true).
    #[serde(default)]
    pub auto_generate: Option<bool>,
    /// Auto-execute a ready recovery proposal (default false).
    #[serde(default)]
    pub auto_execute: Option<bool>,
    /// Total agent calls budgeted across the whole recovery chain (default 6).
    #[serde(default)]
    pub max_recovery_agent_calls: Option<usize>,
}

impl RecoveryConfig {
    /// The safe default policy.
    pub fn defaults() -> Self {
        Self {
            max_attempts: Some(1),
            auto_generate: Some(true),
            auto_execute: Some(false),
            max_recovery_agent_calls: Some(6),
        }
    }

    /// Merge `other` into `self`; `other` wins per-field on conflicts.
    fn merge(&mut self, other: RecoveryConfig) {
        if other.max_attempts.is_some() {
            self.max_attempts = other.max_attempts;
        }
        if other.auto_generate.is_some() {
            self.auto_generate = other.auto_generate;
        }
        if other.auto_execute.is_some() {
            self.auto_execute = other.auto_execute;
        }
        if other.max_recovery_agent_calls.is_some() {
            self.max_recovery_agent_calls = other.max_recovery_agent_calls;
        }
    }
}

/// Per-agent configuration, e.g. `[agents.claude]`.
///
/// When an agent section is present but `enabled` is not written, the agent
/// is enabled by default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Executable used to launch the agent; `None` means the adapter's
    /// built-in default (e.g. `claude`).
    #[serde(default)]
    pub command: Option<String>,
    /// Extra arguments prepended to the adapter's own arguments.
    #[serde(default)]
    pub args: Vec<String>,
    /// Extra environment variables passed to the agent process.
    #[serde(default)]
    pub env: HashMap<String, String>,
    /// Vendor-specific options, e.g. `[agents.codex] options.sandbox = "read-only"`.
    #[serde(default)]
    pub options: HashMap<String, String>,
}

fn default_enabled() -> bool {
    true
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            command: None,
            args: Vec::new(),
            env: HashMap::new(),
            options: HashMap::new(),
        }
    }
}

impl AgentConfig {
    /// Merge `other` into `self`; `other` wins on conflicts.
    /// Env and options maps are merged key-by-key.
    fn merge(&mut self, other: AgentConfig) {
        self.enabled = other.enabled;
        if other.command.is_some() {
            self.command = other.command;
        }
        self.args = other.args;
        for (key, value) in other.env {
            self.env.insert(key, value);
        }
        for (key, value) in other.options {
            self.options.insert(key, value);
        }
    }
}

/// Top-level AgentMesh configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentMeshConfig {
    #[serde(default)]
    pub agents: HashMap<String, AgentConfig>,
    /// Routing preferences for the rule router; `None` uses the defaults.
    #[serde(default)]
    pub routing: Option<RoutingConfig>,
    /// Planner configuration (Phase 18); `None` uses the safe defaults.
    #[serde(default)]
    pub planner: Option<PlannerConfig>,
    /// Recovery policy (Phase 20); `None` uses the safe defaults.
    #[serde(default)]
    pub recovery: Option<RecoveryConfig>,
    /// Evaluation policy (Phase 21); `None` uses the safe defaults.
    #[serde(default)]
    pub evaluation: Option<EvaluationConfig>,
    /// Competition policy (Phase 23); `None` uses the safe defaults.
    #[serde(default)]
    pub competition: Option<CompetitionConfig>,
}

impl AgentMeshConfig {
    /// Default configuration: mock, claude, codex, opencode and antigravity
    /// enabled with their standard commands.
    pub fn default_config() -> Self {
        let mut agents = HashMap::new();
        agents.insert("mock".to_string(), AgentConfig::default());
        agents.insert(
            "claude".to_string(),
            AgentConfig {
                command: Some("claude".to_string()),
                ..AgentConfig::default()
            },
        );
        agents.insert(
            "codex".to_string(),
            AgentConfig {
                command: Some("codex".to_string()),
                ..AgentConfig::default()
            },
        );
        agents.insert(
            "opencode".to_string(),
            AgentConfig {
                command: Some("opencode".to_string()),
                ..AgentConfig::default()
            },
        );
        agents.insert(
            "antigravity".to_string(),
            AgentConfig {
                command: Some("agy".to_string()),
                ..AgentConfig::default()
            },
        );
        Self {
            agents,
            routing: Some(RoutingConfig::default()),
            planner: None,
            recovery: None,
            evaluation: None,
            competition: None,
        }
    }

    /// Resolved routing configuration (defaults when not configured).
    pub fn routing_config(&self) -> RoutingConfig {
        self.routing.clone().unwrap_or_default()
    }

    /// Load configuration with precedence project > user > defaults.
    ///
    /// Missing files are not an error; invalid files are logged and skipped.
    pub fn load() -> Self {
        let mut config = Self::default_config();
        if let Some(user) = Self::load_optional(&user_config_path()) {
            config = config.overlay(user);
        }
        if let Some(project) = Self::load_optional(&project_config_path()) {
            config = config.overlay(project);
        }
        config
    }

    fn load_optional(path: &Path) -> Option<AgentMeshConfig> {
        match Self::load_from(path) {
            Ok(Some(config)) => {
                tracing::debug!(config_path = %path.display(), "loaded config");
                Some(config)
            }
            Ok(None) => None,
            Err(err) => {
                tracing::warn!(error = %err, "ignoring invalid config file");
                None
            }
        }
    }

    /// Load a single config file; `Ok(None)` when the file does not exist.
    pub fn load_from(path: &Path) -> Result<Option<AgentMeshConfig>, ConfigError> {
        let content = match std::fs::read_to_string(path) {
            Ok(content) => content,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(source) => {
                return Err(ConfigError::Read {
                    path: path.to_path_buf(),
                    source,
                });
            }
        };
        let config: AgentMeshConfig =
            toml::from_str(&content).map_err(|source| ConfigError::Parse {
                path: path.to_path_buf(),
                source,
            })?;
        Ok(Some(config))
    }

    /// Overlay `other` on top of `self`; per-agent entries in `other` win.
    pub fn overlay(mut self, other: AgentMeshConfig) -> AgentMeshConfig {
        for (id, agent_config) in other.agents {
            match self.agents.get_mut(&id) {
                Some(existing) => existing.merge(agent_config),
                None => {
                    self.agents.insert(id, agent_config);
                }
            }
        }
        if other.routing.is_some() {
            self.routing = other.routing;
        }
        if let Some(planner) = other.planner {
            match &mut self.planner {
                Some(existing) => existing.merge(planner),
                None => self.planner = Some(planner),
            }
        }
        if let Some(recovery) = other.recovery {
            match &mut self.recovery {
                Some(existing) => existing.merge(recovery),
                None => self.recovery = Some(recovery),
            }
        }
        if let Some(evaluation) = other.evaluation {
            match &mut self.evaluation {
                Some(existing) => existing.merge(evaluation),
                None => self.evaluation = Some(evaluation),
            }
        }
        if let Some(competition) = other.competition {
            match &mut self.competition {
                Some(existing) => existing.merge(competition),
                None => self.competition = Some(competition),
            }
        }
        self
    }

    /// Validates configuration and returns all structured errors with section, field, and reason.
    pub fn validate(&self, file_label: Option<&str>) -> Result<(), Vec<ConfigValidationError>> {
        let mut errors = Vec::new();
        let file = file_label.map(str::to_string);

        // 1. Agents
        for (agent_id, cfg) in &self.agents {
            if let Some(cmd) = &cfg.command
                && cmd.trim().is_empty()
            {
                errors.push(ConfigValidationError {
                    file: file.clone(),
                    section: format!("agents.{agent_id}"),
                    field: "command".to_string(),
                    reason: "command cannot be empty".to_string(),
                });
            }
        }

        // 2. Routing
        if let Some(routing) = &self.routing {
            for (intent_name, list) in [
                ("architecture", &routing.architecture),
                ("implementation", &routing.implementation),
                ("debug", &routing.debug),
                ("review", &routing.review),
                ("testing", &routing.testing),
                ("uiux", &routing.uiux),
                ("general", &routing.general),
            ] {
                if list.is_empty() {
                    errors.push(ConfigValidationError {
                        file: file.clone(),
                        section: "routing".to_string(),
                        field: intent_name.to_string(),
                        reason: "must specify at least one agent".to_string(),
                    });
                }
            }
        }

        // 3. Planner Policy
        if let Some(planner) = &self.planner
            && let Some(policy) = &planner.policy
        {
            if let Some(max_nodes) = policy.max_nodes
                && (max_nodes == 0 || max_nodes > 100)
            {
                errors.push(ConfigValidationError {
                    file: file.clone(),
                    section: "planner.policy".to_string(),
                    field: "max_nodes".to_string(),
                    reason: "must be between 1 and 100".to_string(),
                });
            }
            if let (Some(max_nodes), Some(max_calls)) = (policy.max_nodes, policy.max_agent_calls)
                && max_calls < max_nodes
            {
                errors.push(ConfigValidationError {
                    file: file.clone(),
                    section: "planner.policy".to_string(),
                    field: "max_agent_calls".to_string(),
                    reason: format!("must be >= max_nodes ({max_nodes})"),
                });
            }
            if let Some(max_parallel) = policy.max_parallel
                && (max_parallel == 0 || max_parallel > 8)
            {
                errors.push(ConfigValidationError {
                    file: file.clone(),
                    section: "planner.policy".to_string(),
                    field: "max_parallel".to_string(),
                    reason: "must be between 1 and 8".to_string(),
                });
            }
            if let Some(intents) = &policy.allowed_intents {
                for intent in intents {
                    if crate::TaskIntent::from_key(intent).is_none() {
                        errors.push(ConfigValidationError {
                            file: file.clone(),
                            section: "planner.policy".to_string(),
                            field: "allowed_intents".to_string(),
                            reason: format!("unknown intent `{intent}`"),
                        });
                    }
                }
            }
        }

        // 4. Recovery
        if let Some(recovery) = &self.recovery {
            if let Some(max_attempts) = recovery.max_attempts
                && (max_attempts == 0 || max_attempts > 2)
            {
                errors.push(ConfigValidationError {
                    file: file.clone(),
                    section: "recovery".to_string(),
                    field: "max_attempts".to_string(),
                    reason: "must be between 1 and 2".to_string(),
                });
            }
            if let Some(calls) = recovery.max_recovery_agent_calls
                && (calls == 0 || calls > 20)
            {
                errors.push(ConfigValidationError {
                    file: file.clone(),
                    section: "recovery".to_string(),
                    field: "max_recovery_agent_calls".to_string(),
                    reason: "must be between 1 and 20".to_string(),
                });
            }
        }

        // 5. Evaluation
        if let Some(evaluation) = &self.evaluation {
            if let Some(max_eval) = evaluation.max_evaluators
                && (max_eval == 0 || max_eval > 5)
            {
                errors.push(ConfigValidationError {
                    file: file.clone(),
                    section: "evaluation".to_string(),
                    field: "max_evaluators".to_string(),
                    reason: "must be between 1 and 5".to_string(),
                });
            }
            if let Some(def_eval) = evaluation.default_evaluators {
                let max_bound = evaluation.max_evaluators.unwrap_or(5);
                if def_eval == 0 || def_eval > max_bound {
                    errors.push(ConfigValidationError {
                        file: file.clone(),
                        section: "evaluation".to_string(),
                        field: "default_evaluators".to_string(),
                        reason: format!("must be between 1 and {max_bound}"),
                    });
                }
            }
            if let Some(quorum) = evaluation.default_quorum {
                let eval_bound = evaluation.default_evaluators.unwrap_or(3);
                if quorum == 0 || quorum > eval_bound {
                    errors.push(ConfigValidationError {
                        file: file.clone(),
                        section: "evaluation".to_string(),
                        field: "default_quorum".to_string(),
                        reason: format!("must be between 1 and default_evaluators ({eval_bound})"),
                    });
                }
            }
            if let Some(strategy) = &evaluation.strategy
                && strategy != "majority"
                && strategy != "unanimous"
            {
                errors.push(ConfigValidationError {
                    file: file.clone(),
                    section: "evaluation".to_string(),
                    field: "strategy".to_string(),
                    reason: "must be `majority` or `unanimous`".to_string(),
                });
            }
        }

        // 6. Competition
        if let Some(comp) = &self.competition {
            if let Some(max_cand) = comp.max_candidates
                && (max_cand == 0 || max_cand > 3)
            {
                errors.push(ConfigValidationError {
                    file: file.clone(),
                    section: "competition".to_string(),
                    field: "max_candidates".to_string(),
                    reason: "must be between 1 and 3".to_string(),
                });
            }
            if let Some(def_cand) = comp.default_candidates {
                let max_bound = comp.max_candidates.unwrap_or(3);
                if def_cand == 0 || def_cand > max_bound {
                    errors.push(ConfigValidationError {
                        file: file.clone(),
                        section: "competition".to_string(),
                        field: "default_candidates".to_string(),
                        reason: format!("must be between 1 and {max_bound}"),
                    });
                }
            }
            if let (Some(max_cand), Some(calls)) =
                (comp.max_candidates, comp.max_total_candidate_calls)
                && calls < max_cand
            {
                errors.push(ConfigValidationError {
                    file: file.clone(),
                    section: "competition".to_string(),
                    field: "max_total_candidate_calls".to_string(),
                    reason: format!("must be >= max_candidates ({max_cand})"),
                });
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

/// Detailed, actionable configuration validation error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConfigValidationError {
    pub file: Option<String>,
    pub section: String,
    pub field: String,
    pub reason: String,
}

impl std::fmt::Display for ConfigValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(file) = &self.file {
            writeln!(f, "{file}")?;
        }
        writeln!(f, "[{}]", self.section)?;
        writeln!(f, "{}", self.field)?;
        write!(f, "{}", self.reason)
    }
}

/// Project-local configuration path (`./.agentmesh/config.toml`).
pub fn project_config_path() -> PathBuf {
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join(".agentmesh")
        .join("config.toml")
}

/// User-level configuration path (`~/.config/agentmesh/config.toml`).
pub fn user_config_path() -> PathBuf {
    #[cfg(windows)]
    {
        std::env::var_os("APPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."))
            .join("agentmesh")
            .join("config.toml")
    }
    #[cfg(not(windows))]
    {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."))
            .join(".config")
            .join("agentmesh")
            .join("config.toml")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_all_builtin_agents() {
        let config = AgentMeshConfig::default_config();
        assert_eq!(config.agents.len(), 5);
        for agent in ["mock", "claude", "codex", "opencode", "antigravity"] {
            assert!(config.agents[agent].enabled, "{agent} should be enabled");
        }
        assert_eq!(config.agents["claude"].command.as_deref(), Some("claude"));
        assert_eq!(config.agents["codex"].command.as_deref(), Some("codex"));
        assert_eq!(
            config.agents["opencode"].command.as_deref(),
            Some("opencode")
        );
        assert_eq!(config.agents["antigravity"].command.as_deref(), Some("agy"));
    }

    #[test]
    fn parse_vendor_options() {
        let toml = r#"
            [agents.codex]
            enabled = true

            [agents.codex.options]
            sandbox = "workspace-write"
        "#;
        let config: AgentMeshConfig = toml::from_str(toml).expect("parse");
        assert_eq!(
            config.agents["codex"]
                .options
                .get("sandbox")
                .map(String::as_str),
            Some("workspace-write")
        );
    }

    #[test]
    fn parse_full_config() {
        let toml = r#"
            [agents.mock]
            enabled = false

            [agents.claude]
            enabled = true
            command = "/custom/claude"
            args = ["--model", "sonnet"]

            [agents.claude.env]
            KEY = "value"
        "#;
        let config: AgentMeshConfig = toml::from_str(toml).expect("parse");
        assert!(!config.agents["mock"].enabled);
        let claude = &config.agents["claude"];
        assert!(claude.enabled);
        assert_eq!(claude.command.as_deref(), Some("/custom/claude"));
        assert_eq!(claude.args, vec!["--model", "sonnet"]);
        assert_eq!(claude.env.get("KEY").map(String::as_str), Some("value"));
    }

    #[test]
    fn agent_section_without_enabled_defaults_to_enabled() {
        let toml = r#"
            [agents.claude]
            command = "claude"
        "#;
        let config: AgentMeshConfig = toml::from_str(toml).expect("parse");
        assert!(config.agents["claude"].enabled);
    }

    #[test]
    fn unknown_tables_are_tolerated() {
        let toml = r#"
            [agents.claude]
            enabled = true

            [routing]
            architecture = ["claude"]
        "#;
        let config: AgentMeshConfig = toml::from_str(toml).expect("parse");
        assert!(config.agents["claude"].enabled);
    }

    #[test]
    fn project_overrides_user() {
        let base = AgentMeshConfig {
            agents: HashMap::from([
                (
                    "claude".to_string(),
                    AgentConfig {
                        command: Some("claude".to_string()),
                        env: HashMap::from([("A".to_string(), "1".to_string())]),
                        ..AgentConfig::default()
                    },
                ),
                ("mock".to_string(), AgentConfig::default()),
            ]),
            routing: None,
            planner: None,
            recovery: None,
            evaluation: None,
            competition: None,
        };
        let overlay = AgentMeshConfig {
            agents: HashMap::from([(
                "claude".to_string(),
                AgentConfig {
                    command: Some("/opt/claude".to_string()),
                    env: HashMap::from([("B".to_string(), "2".to_string())]),
                    ..AgentConfig::default()
                },
            )]),
            routing: None,
            planner: None,
            recovery: None,
            evaluation: None,
            competition: None,
        };
        let merged = base.overlay(overlay);
        let claude = &merged.agents["claude"];
        assert_eq!(claude.command.as_deref(), Some("/opt/claude"));
        assert_eq!(claude.env.get("A").map(String::as_str), Some("1"));
        assert_eq!(claude.env.get("B").map(String::as_str), Some("2"));
        assert!(merged.agents.contains_key("mock"));
    }

    #[test]
    fn disabled_agent_stays_disabled() {
        let config = AgentMeshConfig {
            agents: HashMap::from([(
                "claude".to_string(),
                AgentConfig {
                    enabled: false,
                    ..AgentConfig::default()
                },
            )]),
            routing: None,
            planner: None,
            recovery: None,
            evaluation: None,
            competition: None,
        };
        assert!(!config.agents["claude"].enabled);
    }

    #[test]
    fn load_from_missing_file_returns_none() {
        let path = PathBuf::from("/nonexistent/agentmesh/config.toml");
        assert_eq!(AgentMeshConfig::load_from(&path).expect("no error"), None);
    }

    #[test]
    fn load_from_invalid_file_returns_error() {
        let dir =
            std::env::temp_dir().join(format!("agentmesh-test-invalid-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("bad.toml");
        std::fs::write(&path, "not [ valid toml {{{").expect("write");
        let result = AgentMeshConfig::load_from(&path);
        assert!(matches!(result, Err(ConfigError::Parse { .. })));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_from_valid_file() {
        let dir = std::env::temp_dir().join(format!("agentmesh-test-valid-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            "[agents.claude]\nenabled = true\ncommand = \"my-claude\"\n",
        )
        .expect("write");
        let config = AgentMeshConfig::load_from(&path)
            .expect("no error")
            .expect("config exists");
        assert_eq!(
            config.agents["claude"].command.as_deref(),
            Some("my-claude")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn routing_defaults_follow_spec() {
        let config = AgentMeshConfig::default_config();
        let routing = config.routing_config();
        assert_eq!(
            routing.architecture,
            vec!["claude", "codex", "opencode", "antigravity"]
        );
        assert_eq!(
            routing.implementation,
            vec!["codex", "opencode", "claude", "antigravity"]
        );
        assert_eq!(
            routing.debug,
            vec!["codex", "opencode", "claude", "antigravity"]
        );
        assert_eq!(
            routing.review,
            vec!["claude", "codex", "opencode", "antigravity"]
        );
        assert_eq!(
            routing.testing,
            vec!["codex", "opencode", "claude", "antigravity"]
        );
        assert_eq!(routing.uiux, vec!["antigravity", "claude", "opencode"]);
        assert_eq!(
            routing.general,
            vec!["claude", "codex", "opencode", "antigravity"]
        );
    }

    #[test]
    fn routing_parses_from_toml_with_partial_defaults() {
        let toml = r#"
            [routing]
            architecture = ["mock"]
            review = ["codex"]
        "#;
        let config: AgentMeshConfig = toml::from_str(toml).expect("parse");
        let routing = config.routing_config();
        assert_eq!(routing.architecture, vec!["mock"]);
        assert_eq!(routing.review, vec!["codex"]);
        // Unlisted intents keep their defaults.
        assert_eq!(
            routing.testing,
            vec!["codex", "opencode", "claude", "antigravity"]
        );
        assert_eq!(
            routing.general,
            vec!["claude", "codex", "opencode", "antigravity"]
        );
    }

    #[test]
    fn routing_overlay_replaces_only_when_present() {
        let base = AgentMeshConfig::default_config();
        let overlay: AgentMeshConfig =
            toml::from_str("[routing]\narchitecture = [\"mock\"]\n").expect("parse");
        let merged = base.overlay(overlay);
        let routing = merged.routing_config();
        assert_eq!(routing.architecture, vec!["mock"]);
        // Intents absent from the overlay keep the base defaults.
        assert_eq!(
            routing.review,
            vec!["claude", "codex", "opencode", "antigravity"]
        );
    }

    #[test]
    fn routing_config_missing_entirely_uses_defaults() {
        let config = AgentMeshConfig::load_from(&std::path::PathBuf::from(
            "/nonexistent/agentmesh/config.toml",
        ))
        .expect("no error");
        let routing = config.map(|c| c.routing_config()).unwrap_or_default();
        assert_eq!(
            routing.general,
            vec!["claude", "codex", "opencode", "antigravity"]
        );
    }

    // ---------- Phase 18: planner policy config ----------

    #[test]
    fn planner_policy_parses_from_toml() {
        let toml = r#"
            [planner.policy]
            max_nodes = 10
            max_parallel = 4
            allowed_intents = ["architecture", "implementation"]
        "#;
        let config: AgentMeshConfig = toml::from_str(toml).expect("parse");
        let policy = config
            .planner
            .expect("planner present")
            .policy
            .expect("policy present");
        assert_eq!(policy.max_nodes, Some(10));
        assert_eq!(policy.max_parallel, Some(4));
        assert_eq!(policy.max_agent_calls, None, "absent fields stay None");
        assert_eq!(
            policy.allowed_intents,
            Some(vec![
                "architecture".to_string(),
                "implementation".to_string()
            ])
        );
        assert_eq!(policy.allowed_roles, None);
    }

    #[test]
    fn planner_policy_absent_uses_none() {
        let config = AgentMeshConfig::default_config();
        assert!(config.planner.is_none());
    }

    #[test]
    fn planner_policy_overlay_merges_field_by_field() {
        let base: AgentMeshConfig =
            toml::from_str("[planner.policy]\nmax_nodes = 10\nmax_parallel = 4\n").expect("base");
        let overlay: AgentMeshConfig =
            toml::from_str("[planner.policy]\nmax_nodes = 6\n").expect("overlay");
        let merged = base.overlay(overlay);
        let policy = merged.planner.expect("planner").policy.expect("policy");
        assert_eq!(policy.max_nodes, Some(6), "overlay wins on max_nodes");
        assert_eq!(policy.max_parallel, Some(4), "unmentioned field is kept");
    }

    // ---------- Phase 20: recovery policy config ----------

    #[test]
    fn recovery_config_parses_and_defaults() {
        let toml = r#"
            [recovery]
            max_attempts = 2
            auto_execute = true
        "#;
        let config: AgentMeshConfig = toml::from_str(toml).expect("parse");
        let recovery = config.recovery.expect("recovery present");
        assert_eq!(recovery.max_attempts, Some(2));
        assert_eq!(recovery.auto_execute, Some(true));
        assert_eq!(recovery.auto_generate, None, "absent fields stay None");
        // Defaults are the safe ones.
        let defaults = RecoveryConfig::defaults();
        assert_eq!(defaults.max_attempts, Some(1));
        assert_eq!(defaults.auto_generate, Some(true));
        assert_eq!(defaults.auto_execute, Some(false));
        assert_eq!(defaults.max_recovery_agent_calls, Some(6));
    }

    #[test]
    fn recovery_overlay_merges_field_by_field() {
        let base: AgentMeshConfig =
            toml::from_str("[recovery]\nmax_attempts = 1\nauto_execute = false\n").expect("base");
        let overlay: AgentMeshConfig =
            toml::from_str("[recovery]\nmax_attempts = 2\n").expect("overlay");
        let merged = base.overlay(overlay);
        let recovery = merged.recovery.expect("recovery");
        assert_eq!(recovery.max_attempts, Some(2), "overlay wins");
        assert_eq!(recovery.auto_execute, Some(false), "unmentioned field kept");
    }

    // ---------- Phase 21: evaluation policy config ----------

    #[test]
    fn evaluation_config_parses_and_defaults() {
        let toml = r#"
            [evaluation]
            max_evaluators = 5
            default_evaluators = 3
            default_quorum = 2
            strategy = "majority"
            max_total_evaluator_calls = 6
        "#;
        let config: AgentMeshConfig = toml::from_str(toml).expect("parse");
        let evaluation = config.evaluation.expect("evaluation present");
        assert_eq!(evaluation.max_evaluators, Some(5));
        assert_eq!(evaluation.default_evaluators, Some(3));
        assert_eq!(evaluation.default_quorum, Some(2));
        assert_eq!(evaluation.strategy.as_deref(), Some("majority"));
        assert_eq!(evaluation.max_total_evaluator_calls, Some(6));
        let defaults = EvaluationConfig::defaults();
        assert_eq!(defaults.default_evaluators, Some(3));
        assert_eq!(defaults.default_quorum, Some(2));
        assert_eq!(defaults.max_total_evaluator_calls, Some(6));
    }

    #[test]
    fn config_validation_catches_invalid_competition_and_planner_fields() {
        let invalid_toml = r#"
            [competition]
            max_candidates = 0
            default_candidates = 5

            [planner.policy]
            max_nodes = 0
            max_parallel = 10
        "#;
        let config: AgentMeshConfig = toml::from_str(invalid_toml).expect("parse");
        let errs = config
            .validate(Some(".agentmesh/config.toml"))
            .expect_err("should have validation errors");
        assert!(
            errs.iter()
                .any(|e| e.section == "competition" && e.field == "max_candidates")
        );
        assert!(
            errs.iter()
                .any(|e| e.section == "competition" && e.field == "default_candidates")
        );
        assert!(
            errs.iter()
                .any(|e| e.section == "planner.policy" && e.field == "max_nodes")
        );
        assert!(
            errs.iter()
                .any(|e| e.section == "planner.policy" && e.field == "max_parallel")
        );
    }

    #[test]
    fn default_config_passes_validation() {
        let config = AgentMeshConfig::default_config();
        assert!(config.validate(None).is_ok());
    }
}
