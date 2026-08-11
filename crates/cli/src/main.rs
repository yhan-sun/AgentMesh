mod app;

use futures::StreamExt;

use agentmesh_adapters::HealthStatus;
use agentmesh_core::{AgentEvent, ArtifactKind, TaskStatus};
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

    /// Attach to a live task and observe its stream (detach-only Ctrl+C)
    Attach {
        /// Task id (UUID)
        task_id: Uuid,
    },

    /// Cancel a live task (kills the real agent process via the daemon)
    Cancel {
        /// Task id (UUID)
        task_id: Uuid,
    },

    /// Manage the AgentMesh daemon
    #[command(subcommand)]
    Daemon(DaemonCommand),
}

#[derive(Subcommand)]
enum DaemonCommand {
    /// Start the background daemon (idempotent)
    Start,

    /// Show daemon status
    Status,

    /// Stop the daemon; refuses while tasks are running unless --force
    Stop {
        /// Cancel running tasks before shutting down
        #[arg(long)]
        force: bool,
    },

    /// Run the daemon in the foreground (advanced)
    #[command(hide = true)]
    Serve {
        /// Scope: a project directory or "user"
        #[arg(long)]
        scope: String,
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
        Command::Attach { task_id } => {
            let client = daemon_client_or_err().await?;
            cmd_attach(&client, task_id).await?
        }
        Command::Cancel { task_id } => {
            let client = daemon_client_or_err().await?;
            cmd_cancel(&client, task_id).await?
        }
        Command::Daemon(command) => match command {
            DaemonCommand::Start => cmd_daemon_start().await?,
            DaemonCommand::Status => cmd_daemon_status().await?,
            DaemonCommand::Stop { force } => cmd_daemon_stop(force).await?,
            DaemonCommand::Serve { scope } => {
                let scope = agentmesh_daemon::runtime::parse_scope_arg(&scope)?;
                agentmesh_daemon::serve(scope).await?
            }
        },
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

async fn cmd_run(_context: &AppContext, agent_id: &str, prompt: &str) -> anyhow::Result<()> {
    let scope = agentmesh_daemon::Scope::resolve();
    let client = agentmesh_daemon::connect_or_start(scope)
        .await
        .map_err(|err| anyhow!("unable to start AgentMesh daemon: {err}"))?;
    let workspace = std::env::current_dir().ok();
    let response = client
        .run(agent_id, prompt, workspace.as_ref())
        .await
        .map_err(|err| anyhow!("failed to run task through daemon: {err}"))?;
    stream_task_to_terminal(&client, &response, false).await
}

async fn cmd_resume(
    _context: &AppContext,
    source_task_id: Uuid,
    prompt: &str,
) -> anyhow::Result<()> {
    let scope = agentmesh_daemon::Scope::resolve();
    let client = agentmesh_daemon::connect_or_start(scope)
        .await
        .map_err(|err| anyhow!("unable to start AgentMesh daemon: {err}"))?;
    let response = client
        .resume(source_task_id, prompt)
        .await
        .map_err(|err| anyhow!("failed to resume task `{source_task_id}`: {err}"))?;
    stream_task_to_terminal(&client, &response, false).await
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

// ---------- daemon helpers ----------

async fn daemon_client_or_err() -> anyhow::Result<agentmesh_daemon::DaemonClient> {
    let scope = agentmesh_daemon::Scope::resolve();
    agentmesh_daemon::connect_or_start(scope)
        .await
        .map_err(|err| anyhow!("unable to start AgentMesh daemon: {err}"))
}

/// Stream a task's events from the daemon until terminal.
async fn stream_task_to_terminal(
    client: &agentmesh_daemon::DaemonClient,
    response: &agentmesh_daemon::protocol::RunResponse,
    detach_only: bool,
) -> anyhow::Result<()> {
    let task_id = response.task_id;
    let agent_id = response.agent_id.clone();
    let mut metadata_printed = false;
    let mut last_seq = 0u64;
    let mut task_ok = false;

    let mut stream = Box::pin(client.events(task_id, 0));
    loop {
        tokio::select! {
            event = stream.next() => {
                let Some(event) = event else { break };
                let event = event.map_err(|err| anyhow!("daemon stream error: {err}"))?;
                if let Some(id) = event.id {
                    last_seq = last_seq.max(id);
                }
                match event.data {
                    agentmesh_daemon::protocol::DaemonStreamEvent::TaskInfo {
                        task_id, context_id, ..
                    } => {
                        if !metadata_printed {
                            metadata_printed = true;
                            println!("[{}] task started", agent_id);
                            _ = task_id;
                            _ = context_id;
                        }
                    }
                    agentmesh_daemon::protocol::DaemonStreamEvent::Agent { event } => {
                        match &event {
                            AgentEvent::Message(content) => println!("[{agent_id}] {content}"),
                            AgentEvent::StatusChanged(status) => {
                                println!("[{agent_id}] status: {}", status_label(*status));
                            }
                            AgentEvent::Completed => {
                                println!("[{agent_id}] task completed");
                                task_ok = true;
                            }
                            AgentEvent::Failed(message) => {
                                println!("[{agent_id}] failed: {message}");
                            }
                            AgentEvent::Started => {
                                println!("[{agent_id}] task started");
                            }
                            AgentEvent::ArtifactUpdated(_) => {}
                        }
                        if matches!(event, AgentEvent::Completed | AgentEvent::Failed(_)) {
                            break;
                        }
                    }
                    agentmesh_daemon::protocol::DaemonStreamEvent::ReplayGap { .. } => {}
                }
            }
            signal = tokio::signal::ctrl_c(), if !detach_only => {
                signal?;
                eprintln!("[agentmesh] interrupting, cancelling task...");
                match client.cancel(task_id).await {
                    Ok(()) => {
                        // Wait for the daemon to land the terminal state.
                        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
                        loop {
                            if let Ok(Some(task)) = client.get_task(task_id).await {
                                let status = task.get("status").and_then(|v| v.as_str()).unwrap_or("");
                                if status == "cancelled" || status == "failed" || status == "completed" {
                                    break;
                                }
                            }
                            if std::time::Instant::now() > deadline {
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                        }
                        println!("[{}] status: cancelled", agent_id);
                    }
                    Err(err) => {
                        eprintln!("[agentmesh] failed to contact daemon; task cancellation could not be confirmed: {err}");
                    }
                }
                break;
            }
        }
    }

    println!();
    println!("Task:    {task_id}");
    if !task_ok {
        return Err(anyhow!("task `{task_id}` did not complete"));
    }
    Ok(())
}

async fn cmd_attach(client: &agentmesh_daemon::DaemonClient, task_id: Uuid) -> anyhow::Result<()> {
    // Terminal tasks cannot stream (messages are not persisted); report state.
    if let Ok(Some(task)) = client.get_task(task_id).await {
        let status = task.get("status").and_then(|v| v.as_str()).unwrap_or("");
        let is_terminal = matches!(status, "completed" | "failed" | "cancelled");
        if is_terminal {
            println!("Task is already {}.\nstatus: {status}", status);
            return Ok(());
        }
    }
    let mut stream = Box::pin(client.events(task_id, 0));
    let mut first = true;
    while let Some(event) = stream.next().await {
        let event = event.map_err(|err| anyhow!("daemon stream error: {err}"))?;
        match event.data {
            agentmesh_daemon::protocol::DaemonStreamEvent::TaskInfo { task_id, .. } => {
                if first {
                    println!("Attached to task {task_id} (Ctrl+C detaches)");
                    first = false;
                }
            }
            agentmesh_daemon::protocol::DaemonStreamEvent::Agent { event } => {
                let terminal = matches!(event, AgentEvent::Completed | AgentEvent::Failed(_));
                match &event {
                    AgentEvent::Message(content) => println!("{content}"),
                    AgentEvent::StatusChanged(status) => {
                        println!("status: {}", status_label(*status));
                    }
                    AgentEvent::Completed => println!("task completed"),
                    AgentEvent::Failed(message) => println!("failed: {message}"),
                    AgentEvent::Started => println!("task started"),
                    AgentEvent::ArtifactUpdated(_) => {}
                }
                if terminal {
                    break;
                }
            }
            agentmesh_daemon::protocol::DaemonStreamEvent::ReplayGap { .. } => {}
        }
    }
    Ok(())
}

async fn cmd_cancel(client: &agentmesh_daemon::DaemonClient, task_id: Uuid) -> anyhow::Result<()> {
    match client.cancel(task_id).await {
        Ok(()) => {
            // Wait for the daemon to confirm the terminal state.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            let mut final_status = String::from("cancelling");
            loop {
                if let Ok(Some(task)) = client.get_task(task_id).await
                    && let Some(status) = task.get("status").and_then(|v| v.as_str())
                {
                    final_status = status.to_string();
                    if matches!(status, "completed" | "failed" | "cancelled") {
                        break;
                    }
                }
                if std::time::Instant::now() > deadline {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            }
            println!("Task cancelled.");
            println!("status: {final_status}");
            Ok(())
        }
        Err(err) => Err(anyhow!(
            "failed to contact daemon; task cancellation could not be confirmed: {err}"
        )),
    }
}

async fn cmd_daemon_start() -> anyhow::Result<()> {
    let scope = agentmesh_daemon::Scope::resolve();
    if let Ok(client) = agentmesh_daemon::probe(&scope).await {
        let health = client.health().await?;
        println!(
            "AgentMesh daemon is already running (instance {}).",
            health.instance_id
        );
        return Ok(());
    }
    agentmesh_daemon::connect_or_start(scope)
        .await
        .map_err(|err| anyhow!("failed to start daemon: {err}"))?;
    println!("AgentMesh daemon started.");
    Ok(())
}

async fn cmd_daemon_status() -> anyhow::Result<()> {
    let scope = agentmesh_daemon::Scope::resolve();
    let client = match agentmesh_daemon::probe(&scope).await {
        Ok(client) => client,
        Err(_) => {
            println!(
                "AgentMesh Daemon\n\nstatus: not running\nscope: {}",
                scope.label()
            );
            return Ok(());
        }
    };
    let health = client.health().await?;
    let runtime = client.runtime().await?;
    println!("AgentMesh Daemon");
    println!();
    println!("status:   running");
    println!("instance: {}", &health.instance_id[..8]);
    let meta = agentmesh_daemon::runtime::read_metadata(&scope);
    if let Some(meta) = meta {
        println!("pid:      {}", meta.pid);
        println!("address:  {}", meta.address);
        println!("protocol: {}", meta.protocol_version);
        println!("started:  {}", meta.started_at);
    }
    println!("live tasks: {}", runtime.live_tasks.len());
    for task in runtime.live_tasks {
        println!("  {} ({})", task.task_id, task.status);
    }
    println!("scope:    {}", scope.label());
    Ok(())
}

async fn cmd_daemon_stop(force: bool) -> anyhow::Result<()> {
    let scope = agentmesh_daemon::Scope::resolve();
    let client = agentmesh_daemon::probe(&scope)
        .await
        .map_err(|err| anyhow!("daemon is not running: {err}"))?;
    match client.shutdown(force).await {
        Ok(response) => {
            if response.cancelled_tasks > 0 {
                println!(
                    "Cancelled {} running task(s) before shutdown.",
                    response.cancelled_tasks
                );
            }
            println!("AgentMesh daemon stopped.");
            Ok(())
        }
        Err(err) => Err(anyhow!("{err}")),
    }
}
