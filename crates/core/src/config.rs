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
}

impl AgentMeshConfig {
    /// Default configuration: mock, claude and codex enabled with their
    /// standard commands.
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
        Self { agents }
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
        self
    }
}

fn project_config_path() -> PathBuf {
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join(".agentmesh")
        .join("config.toml")
}

fn user_config_path() -> PathBuf {
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
    fn default_config_has_mock_claude_and_codex() {
        let config = AgentMeshConfig::default_config();
        assert_eq!(config.agents.len(), 3);
        assert!(config.agents["mock"].enabled);
        assert!(config.agents["claude"].enabled);
        assert!(config.agents["codex"].enabled);
        assert_eq!(config.agents["claude"].command.as_deref(), Some("claude"));
        assert_eq!(config.agents["codex"].command.as_deref(), Some("codex"));
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
        let dir = std::env::temp_dir().join(format!("agentmesh-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("bad.toml");
        std::fs::write(&path, "not [ valid toml {{{").expect("write");
        let result = AgentMeshConfig::load_from(&path);
        assert!(matches!(result, Err(ConfigError::Parse { .. })));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_from_valid_file() {
        let dir = std::env::temp_dir().join(format!("agentmesh-test-{}", std::process::id()));
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
}
