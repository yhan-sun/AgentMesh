use std::io::Write;

mod app;

use agentmesh_adapters::{AgentRunRequest, HealthStatus};
use agentmesh_core::{AgentEvent, AgentMessage, ArtifactKind, TaskStatus};
use agentmesh_storage::TaskFilter;
use anyhow::anyhow;
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use uuid::Uuid;

use crate::app::AppContext;

#[derive(Parser)]
#[command(
    name = "agentmesh",
    version,
    about = "A2A runtime and orchestrator for coding agents"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// List registered agents and their health
    Agents,

    /// Run a task on an agent with a plain-text prompt
    Run {
        /// Agent id, e.g. `mock`, `claude`, `codex`
        agent: String,

        /// Prompt to send to the agent
        prompt: String,
    },

    /// Diagnose the environment: binaries, versions, health
    Doctor,

    /// List recent tasks (newest first)
    Tasks {
        /// Maximum number of tasks to show (default 20)
        #[arg(long, default_value_t = 20)]
        limit: usize,

        /// Filter by agent id
        #[arg(long)]
        agent: Option<String>,

        /// Filter by status
        #[arg(long)]
        status: Option<String>,
    },

    /// Show a single task by its full id
    Task {
        /// Full task id (UUID)
        task_id: Uuid,
    },

    /// Resume a previous task's native agent session
    Resume {
        /// Source task id (UUID); its agent and session are inferred
        task_id: Uuid,

        /// Prompt to send to the resumed session
        prompt: String,
    },

    /// Show the isolated workspace of a task and its changes
    Workspace {
        /// Task id (UUID)
        task_id: Uuid,
    },

    /// Print the cumulative git patch of a task's workspace
    Diff {
        /// Task id (UUID)
        task_id: Uuid,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let cli = Cli::parse();
    match cli.command {
        Command::Agents => {
            let context = AppContext::init().await?;
            cmd_agents(&context).await?
        }
        Command::Run { agent, prompt } => {
            let context = AppContext::init().await?;
            cmd_run(&context, &agent, &prompt).await?
        }
        Command::Doctor => {
            let context = AppContext::init().await?;
            cmd_doctor(&context).await?
        }
        Command::Tasks {
            limit,
            agent,
            status,
        } => {
            let context = AppContext::init().await?;
            cmd_tasks(&context, limit, agent.as_deref(), status.as_deref()).await?
        }
        Command::Task { task_id } => {
            let context = AppContext::init().await?;
            cmd_task(&context, task_id).await?
        }
        Command::Resume { task_id, prompt } => {
            let context = AppContext::init().await?;
            cmd_resume(&context, task_id, &prompt).await?
        }
        Command::Workspace { task_id } => {
            let context = AppContext::init().await?;
            cmd_workspace(&context, task_id).await?
        }
        Command::Diff { task_id } => {
            let context = AppContext::init().await?;
            cmd_diff(&context, task_id).await?
        }
    }
    Ok(())
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

async fn cmd_agents(context: &AppContext) -> anyhow::Result<()> {
    println!("{:<12} {:<10} SKILLS", "NAME", "STATUS");
    for agent in context.registry.list() {
        let health = agent.health_check().await?;
        let descriptor = agent.descriptor();
        let skills = descriptor
            .skills
            .iter()
            .map(|skill| skill.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        println!(
            "{:<12} {:<10} {}",
            agent.id(),
            health.status.as_str(),
            skills
        );
    }
    Ok(())
}

async fn cmd_doctor(context: &AppContext) -> anyhow::Result<()> {
    println!("AgentMesh Doctor");
    println!();
    for agent in context.registry.list() {
        println!("{}", agent.id());
        let health = agent.health_check().await?;
        match health.status {
            HealthStatus::Online => {
                if let Some(command) = &health.command {
                    println!("  command: {command}");
                    println!("  found: yes");
                }
                if let Some(version) = &health.version {
                    println!("  version: {version}");
                }
                println!("  status: ok");
            }
            HealthStatus::Offline => {
                if let Some(command) = &health.command {
                    println!("  command: {command}");
                }
                if health.version.is_some() {
                    println!("  found: yes");
                } else {
                    println!("  found: no");
                }
                if let Some(version) = &health.version {
                    println!("  version: {version}");
                }
                println!("  status: unavailable");
                if let Some(message) = &health.message {
                    println!("  message: {message}");
                }
                if let Some(details) = &health.details {
                    println!("  details: {details}");
                }
            }
        }
        println!();
    }
    println!("Database: {}", context.database_path.display());

    let (git_found, git_version) = context.workspaces.health_check().await;
    println!();
    println!("git");
    if git_found {
        println!("  found: yes");
        if let Some(version) = git_version {
            println!("  version: {version}");
        }
        println!("  worktree: supported");
        println!("  status: ok");
    } else {
        println!("  found: no");
        println!("  status: unavailable");
    }
    Ok(())
}

async fn cmd_run(context: &AppContext, agent_id: &str, prompt: &str) -> anyhow::Result<()> {
    let input = AgentMessage::user(prompt);
    let request = AgentRunRequest::new(Uuid::new_v4(), Uuid::new_v4(), input);

    let mut run = context
        .task_manager
        .start(agent_id, request)
        .await
        .map_err(|err| anyhow!("failed to start task: {err}"))?;
    let agent_label = agent_id.to_string();
    run_and_print(&mut run, &agent_label).await
}

async fn cmd_resume(
    context: &AppContext,
    source_task_id: Uuid,
    prompt: &str,
) -> anyhow::Result<()> {
    let input = AgentMessage::user(prompt);
    let request = AgentRunRequest::new(Uuid::new_v4(), Uuid::new_v4(), input);
    let mut run = context
        .task_manager
        .resume(source_task_id, request)
        .await
        .map_err(|err| anyhow!("failed to resume task `{source_task_id}`: {err}"))?;
    let agent_label = run.agent_id().to_string();
    run_and_print(&mut run, &agent_label).await
}

/// Shared run loop: stream events, handle Ctrl+C, print task summary.
async fn run_and_print(
    run: &mut agentmesh_tasks::ManagedTaskRun,
    agent_label: &str,
) -> anyhow::Result<()> {
    let mut artifacts = Vec::new();
    let mut task_ok = false;
    loop {
        tokio::select! {
            event = run.next_event() => {
                let Some(event) = event else { break };
                match event {
                    AgentEvent::Started => println!("[{agent_label}] task started"),
                    AgentEvent::Message(content) => println!("[{agent_label}] {content}"),
                    AgentEvent::ArtifactUpdated(artifact) => artifacts.push(artifact),
                    AgentEvent::StatusChanged(status) => {
                        println!("[{agent_label}] status: {}", status_label(status));
                    }
                    AgentEvent::Completed => {
                        println!("[{agent_label}] task completed");
                        task_ok = true;
                    }
                    AgentEvent::Failed(message) => {
                        println!("[{agent_label}] failed: {message}");
                    }
                }
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                eprintln!("[agentmesh] interrupted, cancelling task...");
                let _ = run.cancel().await;
                // Keep draining so the cancellation event is persisted.
                while let Some(event) = run.next_event().await {
                    match event {
                        AgentEvent::StatusChanged(TaskStatus::Cancelled) => {
                            eprintln!("[{agent_label}] status: cancelled");
                        }
                        AgentEvent::Completed => task_ok = true,
                        AgentEvent::Failed(message) => eprintln!("[{agent_label}] failed: {message}"),
                        _ => {}
                    }
                }
                break;
            }
        }
    }
    std::io::stdout().flush()?;

    println!();
    println!("Task:    {}", run.task_id());
    println!("Context: {}", run.context_id());
    if !artifacts.is_empty() {
        println!("Artifacts:");
        for artifact in artifacts {
            println!(
                "  {} ({})",
                artifact.name,
                artifact_kind_label(artifact.kind)
            );
        }
    }
    if !task_ok {
        return Err(anyhow!("task `{}` did not complete", run.task_id()));
    }
    Ok(())
}

async fn cmd_tasks(
    context: &AppContext,
    limit: usize,
    agent: Option<&str>,
    status: Option<&str>,
) -> anyhow::Result<()> {
    let status = match status {
        Some(raw) => Some(TaskStatus::from_str(raw).ok_or_else(|| {
            anyhow!(
                "invalid status `{raw}` (expected: submitted, working, input_required, completed, failed, cancelled)"
            )
        })?),
        None => None,
    };
    let filter = TaskFilter::default().limit(limit);
    let filter = match agent {
        Some(agent) => filter.agent(agent),
        None => filter,
    };
    let filter = match status {
        Some(status) => filter.status(status),
        None => filter,
    };

    let tasks = context.tasks.list(&filter).await?;
    println!("{:<10} {:<8} {:<14} CREATED", "ID", "AGENT", "STATUS");
    for task in tasks {
        println!(
            "{:<10} {:<8} {:<14} {}",
            short_id(&task.id),
            task.agent_id,
            status_label(task.status),
            format_time(task.created_at)
        );
    }
    Ok(())
}

async fn cmd_task(context: &AppContext, task_id: Uuid) -> anyhow::Result<()> {
    let task = context
        .tasks
        .get(task_id)
        .await?
        .ok_or_else(|| anyhow!("task `{task_id}` not found"))?;

    println!("Task");
    println!("  id: {}", task.id);
    println!("  agent: {}", task.agent_id);
    println!("  status: {}", status_label(task.status));
    println!("  created: {}", format_time(task.created_at));
    if let Some(started) = task.started_at {
        println!("  started: {}", format_time(started));
    }
    if let Some(completed) = task.completed_at {
        println!("  completed: {}", format_time(completed));
    }
    if let Some(workspace) = &task.workspace {
        println!("  workspace: {}", workspace.display());
    }
    println!("  context: {}", task.context_id);
    match task.agent_session_id {
        Some(session_id) => {
            println!("  agent session: {session_id}");
            if let Ok(Some(session)) = context.sessions.get(session_id).await
                && let Some(native) = &session.native_session_id
            {
                println!("  native session: {native}");
            }
        }
        None => println!("  agent session: legacy / unavailable"),
    }

    println!();
    println!("Prompt:");
    println!("  {}", task.input.content);

    let artifacts = context.artifacts.list_by_task(task.id).await?;
    if !artifacts.is_empty() {
        println!();
        println!("Artifacts:");
        for artifact in artifacts {
            println!(
                "  {} ({}){}",
                artifact.name,
                artifact_kind_label(artifact.kind),
                artifact
                    .path
                    .map(|p| format!(" -> {}", p.display()))
                    .unwrap_or_default()
            );
        }
    }

    if let Some(error) = &task.error {
        println!();
        println!("Error:");
        println!("  {error}");
    }
    Ok(())
}

fn status_label(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Submitted => "submitted",
        TaskStatus::Working => "working",
        TaskStatus::InputRequired => "input required",
        TaskStatus::Completed => "completed",
        TaskStatus::Failed => "failed",
        TaskStatus::Cancelled => "cancelled",
    }
}

fn artifact_kind_label(kind: ArtifactKind) -> &'static str {
    match kind {
        ArtifactKind::Text => "text",
        ArtifactKind::File => "file",
        ArtifactKind::Patch => "patch",
        ArtifactKind::Json => "json",
        ArtifactKind::Log => "log",
        ArtifactKind::TestResult => "test result",
    }
}

fn short_id(id: &Uuid) -> String {
    id.to_string()[..8].to_string()
}

fn format_time(time: DateTime<Utc>) -> String {
    time.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

/// Resolve a task to its workspace (if any), read-only.
async fn task_workspace(
    context: &AppContext,
    task_id: Uuid,
) -> anyhow::Result<Option<agentmesh_workspace::Workspace>> {
    let task = context
        .tasks
        .get(task_id)
        .await?
        .ok_or_else(|| anyhow!("task `{task_id}` not found"))?;
    let Some(session_id) = task.agent_session_id else {
        println!("Workspace: none (task predates workspace tracking)");
        return Ok(None);
    };
    match context.workspaces.workspace_for_session(session_id).await {
        Ok(workspace) => Ok(Some(workspace)),
        Err(agentmesh_workspace::WorkspaceError::WorkspaceNotFound(_)) => {
            println!("Workspace: none (no workspace for this session)");
            Ok(None)
        }
        Err(agentmesh_workspace::WorkspaceError::WorkspaceMissing(path)) => {
            println!("Workspace: missing (path no longer exists: {path})");
            Ok(None)
        }
        Err(err) => Err(anyhow!(err)),
    }
}

async fn cmd_workspace(context: &AppContext, task_id: Uuid) -> anyhow::Result<()> {
    let Some(workspace) = task_workspace(context, task_id).await? else {
        return Ok(());
    };
    println!("Workspace");
    println!("  id: {}", workspace.id);
    println!("  path: {}", workspace.path.display());
    println!("  repository: {}", workspace.repository_root.display());
    println!("  branch: {}", workspace.branch);
    println!("  base revision: {}", workspace.base_revision);
    println!("  state: active");

    let diff = context.workspaces.diff(&workspace).await?;
    println!();
    if diff.changed_files.is_empty() && diff.untracked_files.is_empty() {
        println!("Changes: none");
    } else {
        println!("Changes:");
        for file in &diff.changed_files {
            println!("  {} {}", file.status.as_str(), file.path.display());
        }
        for file in &diff.untracked_files {
            println!("  U {}", file.display());
        }
    }
    Ok(())
}

async fn cmd_diff(context: &AppContext, task_id: Uuid) -> anyhow::Result<()> {
    let Some(workspace) = task_workspace(context, task_id).await? else {
        return Ok(());
    };
    let diff = context.workspaces.diff(&workspace).await?;
    if diff.is_empty() {
        println!(
            "No changes since base revision {}.",
            workspace.base_revision
        );
        return Ok(());
    }
    println!(
        "# AgentMesh diff for task {task_id} (workspace scope, base {})",
        workspace.base_revision
    );
    println!("{}", diff.patch.trim_end());
    if !diff.untracked_files.is_empty() {
        println!();
        println!("# Untracked files (not part of the patch):");
        for file in &diff.untracked_files {
            println!("#   {}", file.display());
        }
    }
    Ok(())
}
