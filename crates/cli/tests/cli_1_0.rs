//! Tests for AgentMesh 1.0 CLI UX: init, doctor, config validate, and exit codes.

use std::process::Command;
use tempfile::tempdir;

fn agentmesh_bin() -> &'static str {
    env!("CARGO_BIN_EXE_agentmesh")
}

#[test]
fn test_cli_init_creates_config_and_respects_force_flag() {
    let dir = tempdir().expect("tempdir");

    // 1. Initial init
    let output = Command::new(agentmesh_bin())
        .arg("init")
        .current_dir(dir.path())
        .output()
        .expect("exec agentmesh init");

    assert!(output.status.success(), "init should succeed: {:?}", output);
    let config_path = dir.path().join(".agentmesh").join("config.toml");
    assert!(config_path.exists(), "config.toml must be created");

    let content = std::fs::read_to_string(&config_path).expect("read config");
    assert!(content.contains("[agents.claude]"));
    assert!(content.contains("[routing]"));
    assert!(content.contains("[competition]"));

    // 2. Second init without --force must fail with exit code 2 (InvalidArgumentsOrConfig)
    let output_err = Command::new(agentmesh_bin())
        .arg("init")
        .current_dir(dir.path())
        .output()
        .expect("exec agentmesh init again");

    assert_eq!(
        output_err.status.code(),
        Some(2),
        "should exit with code 2 on existing file without --force"
    );
    let stderr = String::from_utf8_lossy(&output_err.stderr);
    assert!(
        stderr.contains("already exists"),
        "error must mention already exists: {stderr}"
    );

    // 3. Second init with --force must succeed
    let output_force = Command::new(agentmesh_bin())
        .args(["init", "--force"])
        .current_dir(dir.path())
        .output()
        .expect("exec agentmesh init --force");

    assert!(output_force.status.success(), "init --force should succeed");
}

#[test]
fn test_cli_config_validate_valid_and_invalid() {
    let dir = tempdir().expect("tempdir");

    // Init valid config
    let status = Command::new(agentmesh_bin())
        .arg("init")
        .current_dir(dir.path())
        .status()
        .expect("exec init");
    assert!(status.success());

    // Validate valid config
    let output_valid = Command::new(agentmesh_bin())
        .args(["config", "validate", "--json"])
        .current_dir(dir.path())
        .output()
        .expect("exec config validate");

    assert!(output_valid.status.success());
    let stdout = String::from_utf8_lossy(&output_valid.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("valid json output");
    assert_eq!(parsed["valid"], true);

    // Corrupt config with invalid competition bounds
    let config_path = dir.path().join(".agentmesh").join("config.toml");
    let corrupt_content = r#"
        [competition]
        max_candidates = 0
        default_candidates = 5
    "#;
    std::fs::write(&config_path, corrupt_content).expect("write corrupt config");

    // Validate invalid config
    let output_invalid = Command::new(agentmesh_bin())
        .args(["config", "validate", "--json"])
        .current_dir(dir.path())
        .output()
        .expect("exec config validate invalid");

    assert_eq!(
        output_invalid.status.code(),
        Some(2),
        "exit code must be 2 for invalid config"
    );
}

#[test]
fn test_cli_doctor_human_and_json_modes() {
    let dir = tempdir().expect("tempdir");

    // Doctor human output
    let output_human = Command::new(agentmesh_bin())
        .arg("doctor")
        .current_dir(dir.path())
        .output()
        .expect("exec doctor");

    assert!(output_human.status.success());
    let stdout_human = String::from_utf8_lossy(&output_human.stdout);
    assert!(stdout_human.contains("AgentMesh Doctor"));
    assert!(stdout_human.contains("Runtime"));
    assert!(stdout_human.contains("Agents"));
    assert!(stdout_human.contains("Workspace"));

    // Doctor json output
    let output_json = Command::new(agentmesh_bin())
        .args(["doctor", "--json"])
        .current_dir(dir.path())
        .output()
        .expect("exec doctor --json");

    assert!(output_json.status.success());
    let stdout_json = String::from_utf8_lossy(&output_json.stdout);
    let parsed: serde_json::Value =
        serde_json::from_str(&stdout_json).expect("valid json doctor output");
    assert!(parsed.get("runtime").is_some());
    assert!(parsed.get("agents").is_some());
    assert!(parsed.get("workspace").is_some());
    assert!(parsed.get("summary").is_some());
}

#[test]
fn test_cli_agents_json_mode() {
    let output = Command::new(agentmesh_bin())
        .args(["agents", "--json"])
        .output()
        .expect("exec agents --json");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("valid agents json");
    assert!(parsed.is_array());
    let arr = parsed.as_array().unwrap();
    assert!(arr.iter().any(|a| a["id"] == "mock"));
    assert!(arr.iter().any(|a| a["id"] == "claude"));
}
