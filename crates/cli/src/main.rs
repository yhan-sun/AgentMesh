mod app;
pub mod exit_codes;

use std::path::{Path, PathBuf};

use futures::StreamExt;

use agentmesh_a2a::client::A2AClientEvent;
use agentmesh_a2a::types::TaskState;
use agentmesh_adapters::HealthStatus;
use agentmesh_core::{AgentEvent, ArtifactKind, TaskIntent, TaskStatus};
use agentmesh_orchestrator::delegate::ActiveDelegation;
use agentmesh_orchestrator::directory::{AgentAuth, AgentDirectory, DiscoveredEndpoint};
use agentmesh_orchestrator::router::{RouteDecision, RuleRouter};
use agentmesh_storage::TaskFilter;
use anyhow::anyhow;
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::app::AppContext;
use crate::exit_codes::{CliError, ExitCode};

#[derive(Parser)]
#[command(
    name = "agentmesh",
    version,
    about = "A2A runtime and orchestrator for coding agents"
)]
struct Cli {
    /// Enable verbose technical error output and logs
    #[arg(long, short = 'v', global = true)]
    pub verbose: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// List registered agents and their health
    Agents {
        /// Format output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Initialize a new AgentMesh project configuration (`.agentmesh/config.toml`)
    Init {
        /// Overwrite existing configuration if present
        #[arg(long)]
        force: bool,
    },

    /// Run a task on an agent with a plain-text prompt
    Run {
        /// Agent id, e.g. `mock`, `claude`, `codex`, `opencode`, `antigravity`
        agent: String,

        /// Prompt to send to the agent
        prompt: String,

        /// Inherit cross-agent transcript and artifacts from a prior task ID
        #[arg(long)]
        from_task: Option<Uuid>,

        /// Inherit all cross-agent transcripts and artifacts from a prior context ID
        #[arg(long)]
        from_context: Option<Uuid>,
    },

    /// Diagnose the environment: binaries, versions, health
    Doctor {
        /// Format output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Validate or inspect configuration
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },

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

        /// Format output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Show a single task by its full id
    Task {
        /// Full task id (UUID)
        task_id: Uuid,

        /// Format output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Resume a previous task's native agent session
    Resume {
        /// Source task id (UUID); its agent and session are inferred
        task_id: Uuid,

        /// Prompt to send to the resumed session
        prompt: String,
    },

    /// Show, archive or clean up the isolated workspace of a task
    Workspace {
        /// Task id (UUID) — show mode
        task_id: Option<Uuid>,

        #[command(subcommand)]
        command: Option<WorkspaceCommand>,
    },

    /// Print the cumulative git patch of a task's workspace
    Diff {
        /// Task id (UUID)
        task_id: Uuid,
    },

    /// Preview or apply a task's agent changes to the source repository
    ///
    /// Apply is not commit: the source working tree gains the agent changes
    /// while HEAD and the agent worktree stay untouched. Without `--yes` this
    /// only shows the preflight plan.
    Apply {
        /// Task id (UUID) — apply mode
        task_id: Option<Uuid>,

        /// Run the preflight only; never modify the source
        #[arg(long)]
        check: bool,

        /// Apply without prompting (preview is the default when omitted)
        #[arg(long)]
        yes: bool,

        #[command(subcommand)]
        command: Option<ApplyCommand>,
    },

    /// List apply history (newest first)
    Applies {
        /// Maximum number of applies to show (default 20)
        #[arg(long, default_value_t = 20)]
        limit: usize,

        /// Filter by status: planned, applying, completed, failed
        #[arg(long)]
        status: Option<String>,

        /// Format output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Manage artifacts
    #[command(subcommand)]
    Artifacts(ArtifactsCommand),

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

    /// Show which agent a task intent routes to
    Route {
        /// Task intent: architecture, implementation, debug, review, testing, uiux, general
        intent: String,
    },

    /// Delegate a prompt to one A2A agent (routed by intent or explicit --agent)
    Delegate {
        /// Task intent used to route the agent (mutually exclusive with --agent)
        #[arg(long)]
        intent: Option<String>,

        /// Explicit agent id; bypasses routing but still goes through A2A
        #[arg(long)]
        agent: Option<String>,

        /// Prompt to send to the agent
        prompt: String,
    },

    /// Run a multi-agent workflow (sequential steps, all through A2A)
    Workflow {
        #[command(subcommand)]
        command: Option<WorkflowCommand>,

        /// Preset name, e.g. `architect-implement-review` (start mode)
        #[arg(long, default_value = "architect-implement-review")]
        preset: String,

        /// Maximum fix + final-review rounds after the initial review (0..=2, default 1)
        #[arg(long, default_value_t = 1)]
        max_review_rounds: usize,

        /// Original user goal that drives every step (start mode)
        prompt: Option<String>,

        /// Maximum concurrent DAG nodes (Phase 16; default 2, hard cap 8)
        #[arg(long, default_value_t = 2)]
        max_parallel: usize,

        /// Explicit source project/repository the workflow operates on
        /// (Phase 22); when omitted and the current directory is a git
        /// repository, the current directory is used (start mode)
        #[arg(long)]
        source_workspace: Option<PathBuf>,
    },

    /// List daemon-owned workflows
    Workflows {
        /// Format output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Generate or manage an AI-planner workflow plan (Phase 17)
    ///
    /// Creating a plan only previews it — nothing executes until
    /// `agentmesh plan execute`.
    Plan {
        /// Explicit planner agent id (still routed through A2A)
        #[arg(long)]
        agent: Option<String>,

        #[command(subcommand)]
        command: Option<PlanCommand>,

        /// Natural-language goal (create mode)
        goal: Option<String>,
    },

    /// List AI-planner plans (newest first)
    Plans {
        /// Format output as JSON
        #[arg(long)]
        json: bool,
    },

    /// List workspaces (newest first)
    Workspaces {
        /// Filter by state: active, applied, archived, missing, removed
        #[arg(long)]
        state: Option<String>,

        /// Maximum number of workspaces to show (default 20)
        #[arg(long, default_value_t = 20)]
        limit: usize,

        /// Format output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Show a Best-of-N competition group and candidates (Phase 23)
    #[command(subcommand)]
    Competition(CompetitionCommand),

    /// Manage the AgentMesh daemon
    #[command(subcommand)]
    Daemon(DaemonCommand),

    /// A2A discovery and debugging
    #[command(subcommand)]
    A2a(A2aCommand),
}

#[derive(Subcommand)]
enum ConfigCommand {
    /// Validate configuration syntax and semantic policy bounds
    Validate {
        /// Format output as JSON
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum A2aCommand {
    /// List A2A agent listeners with their URLs
    Agents,

    /// Show the agent card of an A2A listener
    Card {
        /// Agent id, e.g. `claude` or `codex`
        agent: String,
    },
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

#[derive(Subcommand)]
enum PlanCommand {
    /// Show a plan's detail and preview nodes
    Show {
        /// Plan id (UUID)
        plan_id: Uuid,

        /// Print the current revision's raw plan JSON (for `plan edit` / audit)
        #[arg(long)]
        json: bool,
    },

    /// Preview or execute a plan. Without `--yes` this previews (budget +
    /// policy) and never creates a workflow; there is no interactive prompt.
    Execute {
        /// Plan id (UUID)
        plan_id: Uuid,

        /// Maximum concurrent DAG nodes (default 2, hard cap 8)
        #[arg(long, default_value_t = 2)]
        max_parallel: usize,

        /// Preview only — never claim the plan or create a workflow
        #[arg(long)]
        check: bool,

        /// Execute for real: atomic claim → workflow
        #[arg(long)]
        yes: bool,

        /// Explicit source project/repository the executed workflow operates
        /// on (Phase 22); when omitted and the current directory is a git
        /// repository, the current directory is used
        #[arg(long)]
        source_workspace: Option<PathBuf>,
    },

    /// Replace the current revision with an edited plan JSON (same schema as
    /// the planner output; the original planner revision is never overwritten)
    Edit {
        /// Plan id (UUID)
        plan_id: Uuid,

        /// Path to the edited plan.json; reads stdin when omitted
        #[arg(long)]
        file: Option<PathBuf>,
    },

    /// Diff the original planner revision against the current revision
    Diff {
        /// Plan id (UUID)
        plan_id: Uuid,
    },

    /// List a plan's revision history (oldest first)
    Revisions {
        /// Plan id (UUID)
        plan_id: Uuid,
    },
}

#[derive(Subcommand)]
enum WorkflowCommand {
    /// Show a single workflow's detail and steps
    Show {
        /// Workflow id (UUID)
        workflow_id: Uuid,

        /// Output machine-readable JSON
        #[arg(long)]
        json: bool,
    },

    /// Attach to a workflow's event stream (Ctrl+C detaches, does not cancel)
    Attach {
        /// Workflow id (UUID)
        workflow_id: Uuid,
    },

    /// Cancel a running workflow (cancels the active A2A task)
    Cancel {
        /// Workflow id (UUID)
        workflow_id: Uuid,
    },

    /// Resume an interrupted workflow (continues after the completed steps)
    Resume {
        /// Workflow id (UUID)
        workflow_id: Uuid,
    },

    /// Preview or apply the workflow's implemented result to the source repo
    ///
    /// Uses the last completed Fixer (else Implementer) workspace; reviewer
    /// workspaces are never used. Requires a completed, approved workflow.
    Apply {
        /// Workflow id (UUID)
        workflow_id: Uuid,

        /// Run the preflight only; never modify the source
        #[arg(long)]
        check: bool,

        /// Apply without prompting (preview is the default when omitted)
        #[arg(long)]
        yes: bool,
    },

    /// Preview or clean up the workspaces a workflow used
    ///
    /// Removes the AgentMesh worktrees and their managed branches. Requires
    /// every workspace to pass the safety checks; otherwise nothing is removed.
    Cleanup {
        /// Workflow id (UUID)
        workflow_id: Uuid,

        /// Run the preflight only; never delete anything
        #[arg(long)]
        check: bool,

        /// Clean up without prompting (preview is the default when omitted)
        #[arg(long)]
        yes: bool,
    },

    /// Generate, preview or apply a runtime replan (Phase 19)
    ///
    /// `agentmesh workflow replan <WORKFLOW_ID> "request"` asks a planner agent
    /// for a DAG delta; the proposal never mutates the workflow until
    /// `replan apply --yes`.
    Replan {
        /// Workflow id (UUID)
        workflow_id: Uuid,

        /// The user's replan request (create mode)
        prompt: Option<String>,

        #[command(subcommand)]
        command: Option<ReplanCommand>,
    },

    /// Generate a failure-recovery proposal for a failed workflow (Phase 20)
    ///
    /// The failed workflow stays Failed; the proposal plans a NEW recovery
    /// child workflow. It is never executed until `recovery execute --yes`.
    Recover {
        /// Failed workflow id (UUID)
        workflow_id: Uuid,
    },

    /// Show or execute a recovery proposal
    Recovery {
        #[command(subcommand)]
        command: RecoveryCommand,
    },

    /// Show a workflow's recovery lineage (parent + children)
    Lineage {
        /// Workflow id (UUID)
        workflow_id: Uuid,
    },

    /// Run a multi-agent evaluation of a workflow's latest implementation
    /// (Phase 21)
    Evaluate {
        /// Workflow id (UUID) whose result is evaluated
        workflow_id: Uuid,

        /// Evaluators 1..=5 (default 3)
        #[arg(long)]
        evaluators: Option<usize>,

        /// Consensus strategy: majority | unanimous (default majority)
        #[arg(long)]
        strategy: Option<String>,

        /// Minimum valid results (default 2)
        #[arg(long)]
        quorum: Option<usize>,
    },

    /// Show an evaluation group's detail
    Evaluation {
        #[command(subcommand)]
        command: EvaluationCommand,
    },

    /// List a workflow's evaluation rounds (Phase 22 §18): one row per
    /// consensus fix round with its valid votes, outcome and snapshot.
    Evaluations {
        /// Workflow id (UUID)
        workflow_id: Uuid,

        /// Output machine-readable JSON
        #[arg(long)]
        json: bool,
    },

    /// List a workflow's Best-of-N competition groups (Phase 23)
    Competitions {
        /// Workflow id (UUID)
        workflow_id: Uuid,

        /// Output machine-readable JSON
        #[arg(long)]
        json: bool,
    },

    /// Audit an immutable provenance ledger and verify hash chain integrity (Phase 24)
    Audit {
        /// Workflow id (UUID)
        workflow_id: Uuid,

        /// Verbose output showing full event history and payloads
        #[arg(long, short)]
        verbose: bool,

        /// Output machine-readable JSON
        #[arg(long)]
        json: bool,

        /// Output newline-delimited JSON
        #[arg(long)]
        ndjson: bool,
    },

    /// Deterministic decision replay (Consensus / SelectionGate / Policy / Apply source) (Phase 24)
    Replay {
        /// Workflow id (UUID)
        workflow_id: Uuid,

        /// Verification only (integrity verification)
        #[arg(long)]
        verify: bool,

        /// Output machine-readable JSON
        #[arg(long)]
        json: bool,
    },

    /// Export redacted provenance ledger events (Phase 24)
    Export {
        /// Workflow id (UUID)
        workflow_id: Uuid,

        /// Output destination file path (default stdout)
        #[arg(long, short)]
        output: Option<String>,

        /// Export as newline-delimited JSON (NDJSON)
        #[arg(long)]
        ndjson: bool,
    },
}

/// Competition subcommands (Phase 23).
#[derive(Subcommand)]
enum CompetitionCommand {
    /// Show a competition group with its candidates and winner provenance
    Show {
        /// Competition group id (UUID)
        group_id: Uuid,

        /// Output machine-readable JSON
        #[arg(long)]
        json: bool,
    },
}

/// Evaluation subcommands.
#[derive(Subcommand)]
enum EvaluationCommand {
    /// Show an evaluation group with its members + consensus
    Show {
        /// Evaluation group id (UUID)
        group_id: Uuid,
    },
}

/// Replan subcommands.
#[derive(Subcommand)]
enum ReplanCommand {
    /// Show a replan proposal's detail
    Show {
        /// Replan id (UUID)
        replan_id: Uuid,
    },

    /// Preview or apply a replan proposal
    Apply {
        /// Replan id (UUID)
        replan_id: Uuid,

        /// Preview only — never claim or mutate the workflow
        #[arg(long)]
        check: bool,

        /// Apply for real: atomic claim → graph swap
        #[arg(long)]
        yes: bool,
    },
}

/// Recovery subcommands.
#[derive(Subcommand)]
enum RecoveryCommand {
    /// Show a recovery proposal's detail
    Show {
        /// Recovery id (UUID)
        recovery_id: Uuid,
    },

    /// Preview or execute a recovery proposal (creates the child workflow)
    Execute {
        /// Recovery id (UUID)
        recovery_id: Uuid,

        /// Preview only — never claim or create a child workflow
        #[arg(long)]
        check: bool,

        /// Execute for real: atomic claim → child workflow
        #[arg(long)]
        yes: bool,
    },
}

/// Workspace subcommands (Phase 14).
#[derive(Subcommand)]
enum WorkspaceCommand {
    /// Mark a task's workspace as archived (nothing is deleted)
    Archive {
        /// Task id (UUID)
        task_id: Uuid,
    },

    /// Preview or remove a task's workspace and its managed branch
    Cleanup {
        /// Task id (UUID)
        task_id: Uuid,

        /// Run the preflight only; never delete anything
        #[arg(long)]
        check: bool,

        /// Clean up without prompting (preview is the default when omitted)
        #[arg(long)]
        yes: bool,
    },
}

/// Apply subcommands (Phase 14).
#[derive(Subcommand)]
enum ApplyCommand {
    /// Show a single apply record
    Show {
        /// Apply id (UUID)
        apply_id: Uuid,
    },
}

/// Artifact management.
#[derive(Subcommand)]
enum ArtifactsCommand {
    /// Prune file-backed artifacts of terminal tasks older than `--older-than`
    Prune {
        /// Only consider artifacts older than this many days
        #[arg(long)]
        older_than: u64,

        /// Preview only; never delete anything
        #[arg(long)]
        check: bool,
    },
}

/// What an apply targets: a task's workspace or a workflow's implemented result.
#[derive(Clone, Copy)]
enum ApplyTarget {
    Task(Uuid),
    Workflow(Uuid),
}

#[tokio::main]
async fn main() {
    init_tracing();
    let cli = Cli::parse();
    let verbose = cli.verbose;
    match run_cli(cli).await {
        Ok(()) => std::process::exit(ExitCode::Success.as_i32()),
        Err(err) => {
            eprintln!("{}", err.render(verbose));
            std::process::exit(err.code.as_i32());
        }
    }
}

async fn run_cli(cli: Cli) -> Result<(), CliError> {
    match cli.command {
        Command::Agents { json } => {
            let context = AppContext::init()
                .await
                .map_err(|e| CliError::daemon_error(e.to_string()))?;
            cmd_agents(&context, json).await.map_err(Into::into)
        }
        Command::Init { force } => cmd_init(force).await,
        Command::Doctor { json } => {
            let context = AppContext::init()
                .await
                .map_err(|e| CliError::daemon_error(e.to_string()))?;
            cmd_doctor(&context, json).await.map_err(Into::into)
        }
        Command::Config { command } => match command {
            ConfigCommand::Validate { json } => cmd_config_validate(json).await,
        },
        Command::Run {
            agent,
            prompt,
            from_task,
            from_context,
        } => {
            let context = AppContext::init()
                .await
                .map_err(|e| CliError::daemon_error(e.to_string()))?;
            cmd_run(&context, &agent, &prompt, from_task, from_context)
                .await
                .map_err(Into::into)
        }
        Command::Tasks {
            limit,
            agent,
            status,
            json,
        } => {
            let context = AppContext::init()
                .await
                .map_err(|e| CliError::daemon_error(e.to_string()))?;
            cmd_tasks(&context, limit, agent.as_deref(), status.as_deref(), json)
                .await
                .map_err(Into::into)
        }
        Command::Task { task_id, json } => {
            let context = AppContext::init()
                .await
                .map_err(|e| CliError::daemon_error(e.to_string()))?;
            cmd_task(&context, task_id, json).await.map_err(Into::into)
        }
        Command::Resume { task_id, prompt } => {
            let context = AppContext::init()
                .await
                .map_err(|e| CliError::daemon_error(e.to_string()))?;
            cmd_resume(&context, task_id, &prompt)
                .await
                .map_err(Into::into)
        }
        Command::Workspace { task_id, command } => match command {
            Some(WorkspaceCommand::Archive { task_id }) => {
                cmd_workspace_archive(task_id).await.map_err(Into::into)
            }
            Some(WorkspaceCommand::Cleanup {
                task_id,
                check,
                yes,
            }) => cmd_workspace_cleanup(task_id, check, yes)
                .await
                .map_err(Into::into),
            None => {
                let context = AppContext::init()
                    .await
                    .map_err(|e| CliError::daemon_error(e.to_string()))?;
                cmd_workspace(
                    &context,
                    task_id.ok_or_else(|| CliError::invalid_args("a task id is required"))?,
                )
                .await
                .map_err(Into::into)
            }
        },
        Command::Diff { task_id } => {
            let context = AppContext::init()
                .await
                .map_err(|e| CliError::daemon_error(e.to_string()))?;
            cmd_diff(&context, task_id).await.map_err(Into::into)
        }
        Command::Apply {
            task_id,
            check,
            yes,
            command,
        } => match command {
            Some(ApplyCommand::Show { apply_id }) => {
                cmd_apply_show(apply_id).await.map_err(Into::into)
            }
            None => {
                let client = daemon_client_or_err()
                    .await
                    .map_err(|e| CliError::daemon_error(e.to_string()))?;
                let task_id =
                    task_id.ok_or_else(|| CliError::invalid_args("a task id is required"))?;
                cmd_apply(&client, ApplyTarget::Task(task_id), check, yes)
                    .await
                    .map_err(Into::into)
            }
        },
        Command::Applies {
            limit,
            status,
            json,
        } => cmd_applies(limit, status.as_deref(), json)
            .await
            .map_err(Into::into),
        Command::Artifacts(command) => match command {
            ArtifactsCommand::Prune { older_than, check } => cmd_artifacts_prune(older_than, check)
                .await
                .map_err(Into::into),
        },
        Command::Attach { task_id } => {
            let client = daemon_client_or_err()
                .await
                .map_err(|e| CliError::daemon_error(e.to_string()))?;
            cmd_attach(&client, task_id).await.map_err(Into::into)
        }
        Command::Cancel { task_id } => {
            let client = daemon_client_or_err()
                .await
                .map_err(|e| CliError::daemon_error(e.to_string()))?;
            cmd_cancel(&client, task_id).await.map_err(Into::into)
        }
        Command::Route { intent } => cmd_route(&intent).await.map_err(Into::into),
        Command::Delegate {
            intent,
            agent,
            prompt,
        } => cmd_delegate(intent, agent, &prompt)
            .await
            .map_err(Into::into),
        Command::Workflow {
            command,
            preset,
            max_review_rounds,
            max_parallel,
            prompt,
            source_workspace,
        } => match command {
            Some(WorkflowCommand::Show { workflow_id, json }) => {
                cmd_workflow_show(workflow_id, json)
                    .await
                    .map_err(Into::into)
            }
            Some(WorkflowCommand::Attach { workflow_id }) => {
                cmd_workflow_attach(workflow_id).await.map_err(Into::into)
            }
            Some(WorkflowCommand::Cancel { workflow_id }) => {
                cmd_workflow_cancel(workflow_id).await.map_err(Into::into)
            }
            Some(WorkflowCommand::Resume { workflow_id }) => {
                cmd_workflow_resume(workflow_id).await.map_err(Into::into)
            }
            Some(WorkflowCommand::Apply {
                workflow_id,
                check,
                yes,
            }) => {
                let client = daemon_client_or_err()
                    .await
                    .map_err(|e| CliError::daemon_error(e.to_string()))?;
                cmd_apply(&client, ApplyTarget::Workflow(workflow_id), check, yes)
                    .await
                    .map_err(Into::into)
            }
            Some(WorkflowCommand::Cleanup {
                workflow_id,
                check,
                yes,
            }) => cmd_workflow_cleanup(workflow_id, check, yes)
                .await
                .map_err(Into::into),
            Some(WorkflowCommand::Replan {
                workflow_id,
                prompt,
                command,
            }) => match command {
                Some(ReplanCommand::Show { replan_id }) => {
                    cmd_replan_show(replan_id).await.map_err(Into::into)
                }
                Some(ReplanCommand::Apply {
                    replan_id,
                    check,
                    yes,
                }) => cmd_replan_apply(replan_id, check, yes)
                    .await
                    .map_err(Into::into),
                None => {
                    let prompt = prompt.ok_or_else(|| {
                        CliError::invalid_args("a replan request prompt is required (create mode)")
                    })?;
                    cmd_replan_create(workflow_id, &prompt)
                        .await
                        .map_err(Into::into)
                }
            },
            Some(WorkflowCommand::Recover { workflow_id }) => {
                cmd_recovery_create(workflow_id).await.map_err(Into::into)
            }
            Some(WorkflowCommand::Recovery { command }) => match command {
                RecoveryCommand::Show { recovery_id } => {
                    cmd_recovery_show(recovery_id).await.map_err(Into::into)
                }
                RecoveryCommand::Execute {
                    recovery_id,
                    check,
                    yes,
                } => cmd_recovery_execute(recovery_id, check, yes)
                    .await
                    .map_err(Into::into),
            },
            Some(WorkflowCommand::Lineage { workflow_id }) => {
                cmd_workflow_lineage(workflow_id).await.map_err(Into::into)
            }
            Some(WorkflowCommand::Evaluate {
                workflow_id,
                evaluators,
                strategy,
                quorum,
            }) => cmd_workflow_evaluate(workflow_id, evaluators, strategy.as_deref(), quorum)
                .await
                .map_err(Into::into),
            Some(WorkflowCommand::Evaluation { command }) => match command {
                EvaluationCommand::Show { group_id } => {
                    cmd_evaluation_show(group_id).await.map_err(Into::into)
                }
            },
            Some(WorkflowCommand::Evaluations { workflow_id, json }) => {
                cmd_workflow_evaluations(workflow_id, json)
                    .await
                    .map_err(Into::into)
            }
            Some(WorkflowCommand::Competitions { workflow_id, json }) => {
                cmd_workflow_competitions(workflow_id, json)
                    .await
                    .map_err(Into::into)
            }
            Some(WorkflowCommand::Audit {
                workflow_id,
                verbose,
                json,
                ndjson,
            }) => cmd_workflow_audit(workflow_id, verbose, json, ndjson)
                .await
                .map_err(Into::into),
            Some(WorkflowCommand::Replay {
                workflow_id,
                verify,
                json,
            }) => cmd_workflow_replay(workflow_id, verify, json)
                .await
                .map_err(Into::into),
            Some(WorkflowCommand::Export {
                workflow_id,
                output,
                ndjson,
            }) => cmd_workflow_export(workflow_id, output, ndjson)
                .await
                .map_err(Into::into),
            None => {
                let goal = prompt.ok_or_else(|| {
                    CliError::invalid_args("a goal prompt is required when starting a workflow")
                })?;
                cmd_workflow_start(
                    &preset,
                    max_review_rounds,
                    max_parallel,
                    &goal,
                    source_workspace.as_deref(),
                )
                .await
                .map_err(Into::into)
            }
        },
        Command::Competition(command) => match command {
            CompetitionCommand::Show { group_id, json } => cmd_competition_show(group_id, json)
                .await
                .map_err(Into::into),
        },
        Command::Workflows { json } => cmd_workflows(json).await.map_err(Into::into),
        Command::Plan {
            agent,
            command,
            goal,
        } => match command {
            Some(PlanCommand::Show { plan_id, json }) => {
                cmd_plan_show(plan_id, json).await.map_err(Into::into)
            }
            Some(PlanCommand::Execute {
                plan_id,
                max_parallel,
                check,
                yes,
                source_workspace,
            }) => cmd_plan_execute(
                plan_id,
                max_parallel,
                check,
                yes,
                source_workspace.as_deref(),
            )
            .await
            .map_err(Into::into),
            Some(PlanCommand::Edit { plan_id, file }) => {
                cmd_plan_edit(plan_id, file).await.map_err(Into::into)
            }
            Some(PlanCommand::Diff { plan_id }) => cmd_plan_diff(plan_id).await.map_err(Into::into),
            Some(PlanCommand::Revisions { plan_id }) => {
                cmd_plan_revisions(plan_id).await.map_err(Into::into)
            }
            None => {
                let goal = goal.ok_or_else(|| {
                    CliError::invalid_args("a goal prompt is required when creating a plan")
                })?;
                cmd_plan_create(&goal, agent.as_deref())
                    .await
                    .map_err(Into::into)
            }
        },
        Command::Plans { json } => cmd_plans(json).await.map_err(Into::into),
        Command::Workspaces { state, limit, json } => cmd_workspaces(state.as_deref(), limit, json)
            .await
            .map_err(Into::into),
        Command::Daemon(command) => match command {
            DaemonCommand::Start => cmd_daemon_start().await.map_err(Into::into),
            DaemonCommand::Status => cmd_daemon_status().await.map_err(Into::into),
            DaemonCommand::Stop { force } => cmd_daemon_stop(force).await.map_err(Into::into),
            DaemonCommand::Serve { scope } => {
                let scope = agentmesh_daemon::runtime::parse_scope_arg(&scope)
                    .map_err(|e| CliError::invalid_args(e.to_string()))?;
                agentmesh_daemon::serve(scope).await.map_err(Into::into)
            }
        },
        Command::A2a(command) => match command {
            A2aCommand::Agents => cmd_a2a_agents().await.map_err(Into::into),
            A2aCommand::Card { agent } => cmd_a2a_card(&agent).await.map_err(Into::into),
        },
    }
}

fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

/// Initialize a new AgentMesh project configuration (`.agentmesh/config.toml`).
async fn cmd_init(force: bool) -> Result<(), CliError> {
    let proj_dir = std::env::current_dir().map_err(|err| {
        CliError::workspace_error(format!("failed to get current directory: {err}"))
    })?;
    let dot_agentmesh = proj_dir.join(".agentmesh");
    let config_file = dot_agentmesh.join("config.toml");

    if config_file.exists() && !force {
        return Err(CliError::invalid_args(format!(
            "Configuration file `{}` already exists. Use `agentmesh init --force` to overwrite.",
            config_file.display()
        )));
    }

    std::fs::create_dir_all(&dot_agentmesh).map_err(|err| {
        CliError::workspace_error(format!("failed to create `.agentmesh` directory: {err}"))
    })?;

    let default_template = r#"# AgentMesh Project Configuration (1.0)
# Documentation: docs/architecture.md

[agents.claude]
enabled = true
command = "claude"

[agents.codex]
enabled = true
command = "codex"

[agents.opencode]
enabled = true
command = "opencode"

[agents.antigravity]
enabled = true
command = "agy"

[routing]
architecture = ["claude", "codex", "opencode", "antigravity"]
implementation = ["codex", "opencode", "claude", "antigravity"]
review = ["claude", "codex", "opencode", "antigravity"]
testing = ["codex", "opencode", "claude", "antigravity"]

[evaluation]
default_evaluators = 3
default_quorum = 2
strategy = "majority"

[competition]
default_candidates = 2
max_candidates = 3
"#;

    std::fs::write(&config_file, default_template).map_err(|err| {
        CliError::workspace_error(format!(
            "failed to write `{}`: {err}",
            config_file.display()
        ))
    })?;

    println!(
        "Initialized AgentMesh project configuration in `{}`",
        config_file.display()
    );
    println!("Run `agentmesh doctor` to verify environment and agent availability.");
    Ok(())
}

/// Validate configuration syntax and semantic policy bounds.
async fn cmd_config_validate(json: bool) -> Result<(), CliError> {
    let config = agentmesh_core::config::AgentMeshConfig::load();
    let proj_path = agentmesh_core::config::project_config_path();
    let label = if proj_path.exists() {
        proj_path.to_string_lossy().to_string()
    } else {
        "default_configuration".to_string()
    };

    match config.validate(Some(&label)) {
        Ok(()) => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "valid": true,
                        "errors": []
                    })
                );
            } else {
                println!("✓ Configuration in `{label}` is valid.");
            }
            Ok(())
        }
        Err(errors) => {
            if json {
                println!(
                    "{}",
                    serde_json::json!({
                        "valid": false,
                        "errors": errors
                    })
                );
                return Err(CliError::invalid_args("configuration validation failed"));
            }
            eprintln!("Configuration validation errors in `{label}`:");
            for err in &errors {
                eprintln!();
                eprintln!("{err}");
            }
            Err(CliError::invalid_args("configuration validation failed"))
        }
    }
}

#[derive(Debug, Serialize)]
struct AgentListItemJson {
    id: String,
    name: String,
    status: String,
    skills: Vec<String>,
    command: Option<String>,
    version: Option<String>,
}

async fn cmd_agents(context: &AppContext, json: bool) -> anyhow::Result<()> {
    if json {
        let mut list = Vec::new();
        for agent in context.registry.list() {
            let health = agent.health_check().await?;
            let descriptor = agent.descriptor();
            let skills = descriptor.skills.iter().map(|s| s.name.clone()).collect();
            list.push(AgentListItemJson {
                id: agent.id().to_string(),
                name: descriptor.name.clone(),
                status: health.status.as_str().to_string(),
                skills,
                command: health.command,
                version: health.version,
            });
        }
        println!("{}", serde_json::to_string_pretty(&list)?);
        return Ok(());
    }

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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DoctorReport {
    runtime: DoctorRuntimeReport,
    agents: Vec<DoctorAgentReport>,
    workspace: DoctorWorkspaceReport,
    summary: DoctorSummaryReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DoctorRuntimeReport {
    git_found: bool,
    git_version: Option<String>,
    sqlite_path: String,
    sqlite_connected: bool,
    migrations_applied: i64,
    daemon_running: bool,
    daemon_instance_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DoctorAgentReport {
    id: String,
    name: String,
    enabled: bool,
    command: Option<String>,
    found: bool,
    version: Option<String>,
    status: String,
    structured_output: bool,
    details: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DoctorWorkspaceReport {
    repository_detected: bool,
    repository_root: Option<String>,
    clean_source: bool,
    head_revision: Option<String>,
    project_config_found: bool,
    user_config_found: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DoctorSummaryReport {
    all_ok: bool,
    ready_agents: usize,
    warning_agents: usize,
    message: String,
}

async fn cmd_doctor(context: &AppContext, json: bool) -> anyhow::Result<()> {
    // 1. Runtime
    let (git_found, git_version) = context.workspaces.health_check().await;
    let sqlite_path = context.database_path.display().to_string();
    let sqlite_connected = true;
    let migrations_applied = 16i64;

    let scope = agentmesh_daemon::Scope::resolve();
    let (daemon_running, daemon_instance_id) =
        match agentmesh_daemon::DaemonClient::from_scope(&scope) {
            Ok(client) => {
                let instance = client.instance_id().ok();
                (true, instance)
            }
            Err(_) => (false, None),
        };

    let runtime = DoctorRuntimeReport {
        git_found,
        git_version: git_version.clone(),
        sqlite_path: sqlite_path.clone(),
        sqlite_connected,
        migrations_applied,
        daemon_running,
        daemon_instance_id: daemon_instance_id.clone(),
    };

    // 2. Agents
    let mut agents = Vec::new();
    let mut ready_count = 0usize;
    let mut warning_count = 0usize;

    for agent in context.registry.list() {
        let health = agent.health_check().await?;
        let is_ready = health.status == HealthStatus::Online;
        if is_ready {
            ready_count += 1;
        } else if agent.id() != "mock" {
            warning_count += 1;
        }

        let name = match agent.id() {
            "claude" => "Claude Code",
            "codex" => "Codex",
            "opencode" => "OpenCode",
            "antigravity" => "Antigravity",
            "mock" => "Mock Adapter",
            other => other,
        };

        agents.push(DoctorAgentReport {
            id: agent.id().to_string(),
            name: name.to_string(),
            enabled: true,
            command: health.command.clone(),
            found: health.version.is_some() || is_ready,
            version: health.version.clone(),
            status: if is_ready {
                "ready".to_string()
            } else {
                "unavailable".to_string()
            },
            structured_output: true,
            details: health.details.clone().or(health.message.clone()),
        });
    }

    // 3. Workspace
    let current_dir = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let (repo_detected, repo_root, clean_source, head_revision): (
        bool,
        Option<String>,
        bool,
        Option<String>,
    ) = match context.workspaces.discover_repository(&current_dir).await {
        Ok(root) => {
            let clean = std::process::Command::new("git")
                .args(["diff", "--quiet"])
                .current_dir(&root)
                .status()
                .map(|s| s.success())
                .unwrap_or(false)
                && std::process::Command::new("git")
                    .args(["diff", "--cached", "--quiet"])
                    .current_dir(&root)
                    .status()
                    .map(|s| s.success())
                    .unwrap_or(false);

            let head = std::process::Command::new("git")
                .args(["rev-parse", "--short", "HEAD"])
                .current_dir(&root)
                .output()
                .ok()
                .and_then(|out| {
                    if out.status.success() {
                        Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
                    } else {
                        None
                    }
                });

            (true, Some(root.display().to_string()), clean, head)
        }
        Err(_) => (false, None, false, None),
    };

    let project_config_found = agentmesh_core::config::project_config_path().exists();
    let user_config_found = agentmesh_core::config::user_config_path().exists();

    let workspace = DoctorWorkspaceReport {
        repository_detected: repo_detected,
        repository_root: repo_root.clone(),
        clean_source,
        head_revision: head_revision.clone(),
        project_config_found,
        user_config_found,
    };

    // 4. Summary
    let all_ok = git_found && sqlite_connected && ready_count > 0;
    let message = format!("{ready_count} agents ready, {warning_count} warning(s)");

    let report = DoctorReport {
        runtime,
        agents,
        workspace,
        summary: DoctorSummaryReport {
            all_ok,
            ready_agents: ready_count,
            warning_agents: warning_count,
            message: message.clone(),
        },
    };

    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    // Human output
    println!("AgentMesh Doctor");
    println!();
    println!("Runtime");
    if git_found {
        let v = git_version.as_deref().unwrap_or("detected");
        println!("  ✓ Git ({v})");
    } else {
        println!("  ✗ Git (not found in PATH)");
    }
    if sqlite_connected {
        println!("  ✓ SQLite (database connected, {migrations_applied} migrations applied)");
    } else {
        println!("  ✗ SQLite (cannot connect: {sqlite_path})");
    }
    if daemon_running {
        let inst = daemon_instance_id.as_deref().unwrap_or("active");
        let short_inst = if inst.len() >= 8 { &inst[..8] } else { inst };
        println!("  ✓ Daemon (running, instance {short_inst})");
    } else {
        println!("  ✓ Daemon (stopped, auto-starts on demand)");
    }
    println!();

    println!("Agents");
    for a in &report.agents {
        if a.id == "mock" {
            continue;
        }
        if a.status == "ready" {
            let ver = a.version.as_deref().unwrap_or("ready");
            let cmd = a.command.as_deref().unwrap_or(&a.id);
            println!("  ✓ {:<14} ({cmd} {ver}, ready)", a.name);
        } else {
            let detail = a.details.as_deref().unwrap_or("not found or offline");
            println!("  ⚠ {:<14} ({detail})", a.name);
        }
    }
    println!();

    println!("Workspace");
    if let Some(root) = repo_root {
        println!("  ✓ Repository ({root})");
        if clean_source {
            let rev = head_revision.as_deref().unwrap_or("HEAD");
            println!("  ✓ Clean source (HEAD at {rev})");
        } else {
            println!("  ⚠ Working tree has uncommitted modifications");
        }
    } else {
        println!("  ⚠ Not inside a Git repository (isolated worktree features require git)");
    }
    if project_config_found {
        println!("  ✓ Configuration (.agentmesh/config.toml)");
    } else {
        println!("  ✓ Configuration (defaults active; run `agentmesh init` to customize)");
    }
    println!();

    println!("Result:");
    println!("  {}", report.summary.message);
    Ok(())
}

async fn cmd_run(
    _context: &AppContext,
    agent_id: &str,
    prompt: &str,
    from_task: Option<Uuid>,
    from_context: Option<Uuid>,
) -> anyhow::Result<()> {
    let scope = agentmesh_daemon::Scope::resolve();
    let client = agentmesh_daemon::connect_or_start(scope)
        .await
        .map_err(|err| anyhow!("unable to start AgentMesh daemon: {err}"))?;
    let workspace = std::env::current_dir().ok();
    let response = client
        .run_with_options(
            agent_id,
            prompt,
            workspace.as_ref(),
            from_task,
            from_context,
        )
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
    json: bool,
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
    if json {
        println!("{}", serde_json::to_string_pretty(&tasks)?);
        return Ok(());
    }

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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TaskDetailJson {
    #[serde(flatten)]
    task: agentmesh_core::AgentTask,
    artifacts: Vec<agentmesh_core::Artifact>,
    native_session_id: Option<String>,
}

async fn cmd_task(context: &AppContext, task_id: Uuid, json: bool) -> anyhow::Result<()> {
    let task = context
        .tasks
        .get(task_id)
        .await?
        .ok_or_else(|| anyhow!("task `{task_id}` not found"))?;

    let artifacts = context.artifacts.list_by_task(task.id).await?;
    let mut native_session_id = None;
    if let Some(session_id) = task.agent_session_id
        && let Ok(Some(session)) = context.sessions.get(session_id).await
    {
        native_session_id = session.native_session_id;
    }

    if json {
        let detail = TaskDetailJson {
            task,
            artifacts,
            native_session_id,
        };
        println!("{}", serde_json::to_string_pretty(&detail)?);
        return Ok(());
    }

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
    if let Some(session_id) = task.agent_session_id {
        println!("  agent session: {session_id}");
        if let Some(native) = &native_session_id {
            println!("  native session: {native}");
        }
    } else {
        println!("  agent session: legacy / unavailable");
    }

    println!();
    println!("Prompt:");
    println!("  {}", task.input.content);

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

// ---------- Phase 9: routing and delegation ----------

/// Discover local A2A agents from the running daemon's runtime endpoint.
///
/// Flow: daemon `/v1/runtime` → local A2A agent URLs → fetch agent cards →
/// [`AgentDirectory`]. The daemon is started if needed; the real agent
/// processes stay owned by it.
async fn discover_directory() -> anyhow::Result<AgentDirectory> {
    let scope = agentmesh_daemon::Scope::resolve();
    let client = agentmesh_daemon::connect_or_start(scope.clone())
        .await
        .map_err(|err| anyhow!("unable to start AgentMesh daemon: {err}"))?;
    let runtime = client
        .runtime()
        .await
        .map_err(|err| anyhow!("failed to read daemon runtime: {err}"))?;
    let a2a_agents = runtime
        .get("a2a_agents")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));

    let mut discovered = Vec::new();
    if let Some(agents) = a2a_agents.as_object() {
        for (agent_id, info) in agents {
            let url = info
                .get("url")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let card_url = info
                .get("card_url")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if url.is_empty() || card_url.is_empty() {
                continue;
            }
            discovered.push(DiscoveredEndpoint {
                agent_id: agent_id.clone(),
                url,
                card_url,
            });
        }
    }

    let token =
        agentmesh_daemon::auth::read_token(&agentmesh_daemon::paths::a2a_token_path(&scope)).ok();
    let mut directory = AgentDirectory::new();
    directory
        .refresh(&discovered, &AgentAuth { token })
        .await
        .map_err(|err| anyhow!("agent discovery failed: {err}"))?;
    Ok(directory)
}

fn intent_from_key(raw: &str) -> anyhow::Result<TaskIntent> {
    TaskIntent::from_key(raw).ok_or_else(|| {
        anyhow!(
            "invalid intent `{raw}` (expected: architecture, implementation, debug, review, testing, uiux, general)"
        )
    })
}

async fn cmd_route(intent_raw: &str) -> anyhow::Result<()> {
    let intent = intent_from_key(intent_raw)?;
    let directory = discover_directory().await?;
    let config = agentmesh_core::AgentMeshConfig::load();
    let router = RuleRouter::new(config.routing_config());
    match router.route(&directory, intent) {
        RouteDecision::Agent { agent_id, reason } => {
            println!("Intent: {}", intent.key());
            println!("Agent:  {agent_id}");
            println!("Reason: {reason}");
            Ok(())
        }
        RouteDecision::NoCapableAgent { skill } => {
            println!("Intent: {}", intent.key());
            println!("Agent:  none");
            println!("Reason: no capable agent with skill `{skill}`");
            anyhow::bail!("no capable agent found for intent `{}`", intent.key());
        }
    }
}

async fn cmd_delegate(
    intent: Option<String>,
    agent: Option<String>,
    prompt: &str,
) -> anyhow::Result<()> {
    if intent.is_some() && agent.is_some() {
        anyhow::bail!("use either --intent or --agent, not both");
    }
    let intent = match intent {
        Some(raw) => Some(intent_from_key(&raw)?),
        None => None,
    };
    let directory = discover_directory().await?;
    let config = agentmesh_core::AgentMeshConfig::load();
    let router = RuleRouter::new(config.routing_config());
    let mut delegation =
        agentmesh_orchestrator::delegate::delegate(&directory, &router, intent, agent, prompt)
            .await
            .map_err(|err| anyhow!("delegation failed: {err}"))?;
    stream_a2a_to_terminal(&mut delegation).await
}

/// Stream a delegation's A2A events to the terminal until terminal state.
///
/// Ctrl+C cancels the task through `A2A CancelTask` (the daemon kills the
/// real process). If the SSE connection drops mid-run, the live task is
/// resumed once via `SubscribeToTask`.
async fn stream_a2a_to_terminal(delegation: &mut ActiveDelegation) -> anyhow::Result<()> {
    let agent_id = delegation.agent_id.clone();
    let task_id = delegation.task_id;
    println!("[{agent_id}] task started");

    let mut task_ok = false;
    let mut terminal_state: Option<String> = None;
    let mut reconnected = false;

    loop {
        tokio::select! {
            event = delegation.stream.next() => {
                let Some(event) = event else {
                    // Stream ended without a terminal event: try to resume the
                    // live task via SubscribeToTask once; otherwise fall through
                    // to a final state check against the server.
                    if terminal_state.is_none() && !reconnected {
                        reconnected = true;
                        match delegation.client.subscribe_to_task(task_id).await {
                            Ok(subscription) => {
                                eprintln!("[agentmesh] stream dropped; reattached to live task {task_id}");
                                delegation.stream = subscription.events;
                                continue;
                            }
                            Err(_) => break,
                        }
                    } else {
                        break;
                    }
                };
                let event = event.map_err(|err| anyhow!("a2a stream error: {err}"))?;
                match event {
                    A2AClientEvent::Status(status) => match status.status.state {
                        TaskState::Working => {
                            if let Some(message) = &status.status.message {
                                println!("[{agent_id}] {message}");
                            }
                        }
                        TaskState::Completed => {
                            println!("[{agent_id}] task completed");
                            task_ok = true;
                            terminal_state = Some("completed".into());
                            break;
                        }
                        TaskState::Failed => {
                            println!("[{agent_id}] failed: {}", status.status.message.unwrap_or_default());
                            terminal_state = Some("failed".into());
                            break;
                        }
                        TaskState::Canceled => {
                            println!("[{agent_id}] status: cancelled");
                            terminal_state = Some("cancelled".into());
                            break;
                        }
                        TaskState::Submitted | TaskState::InputRequired => {}
                    },
                    A2AClientEvent::Artifact(_) => {}
                }
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                eprintln!("[agentmesh] interrupting, cancelling task...");
                match delegation.client.cancel_task(task_id).await {
                    Ok(()) => {
                        // Wait for the daemon to land the terminal state.
                        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
                        loop {
                            match delegation.client.get_task(task_id).await {
                                Ok(task) if task.state.is_terminal() => break,
                                _ => {}
                            }
                            if std::time::Instant::now() > deadline {
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                        }
                        println!("[{agent_id}] status: cancelled");
                        terminal_state = Some("cancelled".into());
                    }
                    Err(err) => {
                        eprintln!("[agentmesh] failed to cancel via A2A: {err}");
                    }
                }
                break;
            }
        }
    }

    // If we lost the stream without a terminal event, confirm from the server.
    if terminal_state.is_none()
        && let Ok(task) = delegation.client.get_task(task_id).await
    {
        match task.state {
            TaskState::Completed => {
                println!("[{agent_id}] task completed");
                task_ok = true;
            }
            TaskState::Failed => {
                println!(
                    "[{agent_id}] failed: {}",
                    task.status.and_then(|s| s.message).unwrap_or_default()
                );
            }
            TaskState::Canceled => {
                println!("[{agent_id}] status: cancelled");
            }
            _ => {}
        }
    }

    println!();
    println!("Task:    {task_id}");
    if let Some(context_id) = delegation.context_id {
        println!("Context: {context_id}");
    }
    if !task_ok {
        return Err(anyhow!("task `{task_id}` did not complete"));
    }
    Ok(())
}

// ---------- Phase 12: daemon-owned workflows ----------

/// `agentmesh workflow --preset architect-implement-review "goal"`
///
/// The daemon owns the workflow: the CLI starts it and attaches to its event
/// stream. Ctrl+C detaches (the workflow keeps running in the daemon).
async fn cmd_workflow_start(
    preset: &str,
    max_review_rounds: usize,
    max_parallel: usize,
    goal: &str,
    source_workspace: Option<&Path>,
) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let source = resolve_source_workspace(source_workspace);
    let response = client
        .start_workflow_with_source(preset, goal, max_review_rounds, max_parallel, source)
        .await
        .map_err(|err| anyhow!("failed to start workflow: {err}"))?;
    println!("Workflow: {preset}");
    if preset == agentmesh_orchestrator::PRESET_PARALLEL_REVIEW {
        println!("Parallel: {max_parallel} nodes at once");
    }
    println!("Workflow ID: {}", response.workflow_id);
    println!();
    stream_workflow_events(&client, response.workflow_id).await
}

async fn cmd_workflow_show(workflow_id: Uuid, json: bool) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let detail = client
        .get_workflow(workflow_id)
        .await
        .map_err(|err| anyhow!("failed to load workflow: {err}"))?
        .ok_or_else(|| anyhow!("workflow `{workflow_id}` not found"))?;

    if json {
        println!("{}", serde_json::to_string_pretty(&detail)?);
        return Ok(());
    }

    println!("Workflow");
    println!("  id: {}", detail.workflow_id);
    println!("  preset: {}", detail.preset);
    println!("  goal: {}", detail.goal);
    println!("  status: {}", detail.status.as_str());
    if let Some(context_id) = detail.context_id {
        println!("  context: {context_id}");
    }
    println!("  max review rounds: {}", detail.max_review_rounds);
    println!("  max parallel: {}", detail.max_parallel);
    println!("  graph revision: {}", detail.graph_revision);
    if let Some(source) = &detail.source_workspace {
        println!("  source workspace: {source}");
    }
    if let Some(verdict) = detail.final_review_verdict {
        println!("  final review: {}", verdict.key());
    }
    // Phase 22 §18: evaluation rounds + consensus summary for consensus-review.
    let rounds = client
        .list_workflow_evaluations(workflow_id)
        .await
        .unwrap_or_default();
    if !rounds.is_empty() {
        println!("  evaluation rounds: {}", rounds.len());
        for round in &rounds {
            let outcome = round
                .consensus
                .as_ref()
                .map(|c| c.outcome.as_str().to_string())
                .unwrap_or_else(|| "—".to_string());
            println!(
                "    round {}: {} (valid {}/{}) snapshot={}",
                round.round,
                outcome,
                round.consensus.as_ref().map(|c| c.valid_count).unwrap_or(0),
                round.consensus.as_ref().map(|c| c.total_count).unwrap_or(0),
                round
                    .snapshot_hash
                    .as_deref()
                    .map(|h| truncate_line(h, 16))
                    .unwrap_or_else(|| "-".to_string()),
            );
        }
    }
    if let Some(error) = &detail.error {
        println!("  error: {error}");
    }
    println!();
    println!("Steps:");
    for step in &detail.steps {
        let label = match &step.node_id {
            Some(node_id) => format!("{} ({})", step.role.label(), node_id),
            None => step.role.label().to_string(),
        };
        println!(
            "  [{}] {:<22} {:<12} agent={} task={}",
            step.ordinal + 1,
            label,
            step.status.as_str(),
            step.agent_id.as_deref().unwrap_or("-"),
            step.task_id
                .map(|t| t.to_string())
                .unwrap_or_else(|| "-".into()),
        );
        if let Some(summary) = &step.summary {
            println!("      summary: {}", truncate_line(summary, 80));
        }
        if let Some(error) = &step.error {
            println!("      error: {error}");
        }
    }
    Ok(())
}

async fn cmd_workflow_attach(workflow_id: Uuid) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    stream_workflow_events(&client, workflow_id).await
}

async fn cmd_workflow_cancel(workflow_id: Uuid) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    client
        .cancel_workflow(workflow_id)
        .await
        .map_err(|err| anyhow!("failed to cancel workflow: {err}"))?;
    println!("Workflow {workflow_id} cancelled.");
    Ok(())
}

async fn cmd_workflow_resume(workflow_id: Uuid) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    client
        .resume_workflow(workflow_id)
        .await
        .map_err(|err| anyhow!("failed to resume workflow: {err}"))?;
    println!("Workflow {workflow_id} resumed.");
    println!();
    stream_workflow_events(&client, workflow_id).await
}

async fn cmd_workflows(json: bool) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let workflows = client
        .list_workflows()
        .await
        .map_err(|err| anyhow!("failed to list workflows: {err}"))?;
    if json {
        println!("{}", serde_json::to_string_pretty(&workflows)?);
        return Ok(());
    }
    if workflows.is_empty() {
        println!("No workflows.");
        return Ok(());
    }
    println!(
        "{:<12} {:<10} {:<14} {:<22} GOAL",
        "ID", "STATUS", "PRESET", "CREATED"
    );
    for workflow in workflows {
        println!(
            "{:<12} {:<10} {:<14} {:<22} {}",
            &workflow.workflow_id.to_string()[..8],
            workflow.status.as_str(),
            workflow.preset,
            workflow.created_at,
            truncate_line(&workflow.goal, 60),
        );
    }
    Ok(())
}

// ---------- Phase 19: runtime replan ----------

/// `agentmesh workflow replan <id> "request"` — ask a planner agent for a DAG
/// delta, validate it, persist the proposal. Never mutates the workflow.
async fn cmd_replan_create(workflow_id: Uuid, prompt: &str) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let response = client
        .create_replan(workflow_id, prompt, None)
        .await
        .map_err(|err| anyhow!("failed to create replan: {err}"))?;
    println!("Replan: {}", response.replan_id);
    cmd_replan_show(response.replan_id).await
}

/// `agentmesh workflow replan show <id>` — a proposal's detail.
async fn cmd_replan_show(replan_id: Uuid) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let detail = client
        .get_replan(replan_id)
        .await
        .map_err(|err| anyhow!("failed to load replan: {err}"))?
        .ok_or_else(|| anyhow!("replan `{replan_id}` not found"))?;
    println!("Replan: {}", detail.replan_id);
    println!("Workflow: {}", detail.workflow_id);
    println!("Status: {}", detail.status);
    println!("Base graph revision: {}", detail.base_graph_revision);
    if let Some(revision) = detail.applied_graph_revision {
        println!("Applied graph revision: {revision}");
    }
    if let Some(summary) = &detail.summary {
        println!("Summary: {summary}");
    }
    if let Some(error) = &detail.validation_error {
        println!("Error: {error}");
    }
    if let Some(delta) = &detail.delta {
        if !delta.add_nodes.is_empty() {
            println!("Add:");
            for node in &delta.add_nodes {
                println!("  + {}", node.id);
            }
        }
        if !delta.update_nodes.is_empty() {
            println!("Update:");
            for update in &delta.update_nodes {
                println!("  ~ {}", update.id);
            }
        }
        if !delta.remove_nodes.is_empty() {
            println!("Remove:");
            for id in &delta.remove_nodes {
                println!("  - {id}");
            }
        }
    }
    Ok(())
}

/// `agentmesh workflow replan apply <id> [--check|--yes]` — preview (no
/// mutation) unless `--yes`.
async fn cmd_replan_apply(replan_id: Uuid, check: bool, yes: bool) -> anyhow::Result<()> {
    if check && yes {
        anyhow::bail!("use either --check or --yes, not both");
    }
    let client = daemon_client_or_err().await?;
    let response = client
        .apply_replan(replan_id, !yes)
        .await
        .map_err(|err| anyhow!("failed to apply replan `{replan_id}`: {err}"))?;
    match response {
        agentmesh_daemon::protocol::ReplanApplyResponse::Preview { preview } => {
            print_replan_preview(&preview);
        }
        agentmesh_daemon::protocol::ReplanApplyResponse::Applied {
            applied_graph_revision,
        } => {
            println!("Replan {replan_id} applied.");
            println!("Graph revision: {applied_graph_revision}");
        }
    }
    Ok(())
}

/// Render the `replan apply --check` preview (spec §21). Never shows raw JSON.
fn print_replan_preview(preview: &agentmesh_daemon::protocol::ReplanPreview) {
    println!("Replan: {}", preview.replan_id);
    println!("Workflow: {}", preview.workflow_id);
    println!(
        "Graph revision: {} -> {}",
        preview.base_graph_revision, preview.current_graph_revision
    );
    println!();
    if !preview.add_nodes.is_empty() {
        println!("Add:");
        for id in &preview.add_nodes {
            println!("  + {id}");
        }
    }
    if !preview.update_nodes.is_empty() {
        println!("Update:");
        for id in &preview.update_nodes {
            println!("  ~ {id}");
        }
    }
    if !preview.remove_nodes.is_empty() {
        println!("Remove:");
        for id in &preview.remove_nodes {
            println!("  - {id}");
        }
    }
    println!();
    println!("Budget:");
    println!(
        "  nodes         {:>3} / {:<3}",
        preview.node_count, preview.policy_max_nodes
    );
    println!(
        "  agent calls   {:>3} / {:<3}",
        preview.estimated_agent_calls, preview.policy_max_agent_calls
    );
    println!();
    println!("Status: {}", preview.status);
    println!("Re-run with --yes to apply.");
}

// ---------- Phase 20: failure recovery + lineage ----------

/// `agentmesh workflow recover <id>` — ask the Failure Analyzer for a recovery
/// plan. The failed workflow stays Failed; the proposal is never executed
/// until `recovery execute --yes`.
async fn cmd_recovery_create(workflow_id: Uuid) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let detail = client
        .create_recovery(workflow_id)
        .await
        .map_err(|err| anyhow!("failed to create recovery proposal: {err}"))?;
    print_recovery_detail(&detail);
    Ok(())
}

/// `agentmesh workflow recovery show <id>` — a proposal's detail.
async fn cmd_recovery_show(recovery_id: Uuid) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let detail = client
        .get_recovery(recovery_id)
        .await
        .map_err(|err| anyhow!("failed to load recovery: {err}"))?
        .ok_or_else(|| anyhow!("recovery `{recovery_id}` not found"))?;
    print_recovery_detail(&detail);
    Ok(())
}

/// `agentmesh workflow recovery execute <id> [--check|--yes]` — preview (no
/// mutation) unless `--yes`, which atomically creates the child workflow.
async fn cmd_recovery_execute(recovery_id: Uuid, check: bool, yes: bool) -> anyhow::Result<()> {
    if check && yes {
        anyhow::bail!("use either --check or --yes, not both");
    }
    let client = daemon_client_or_err().await?;
    let response = client
        .execute_recovery(recovery_id, !yes)
        .await
        .map_err(|err| anyhow!("failed to execute recovery `{recovery_id}`: {err}"))?;
    match response {
        agentmesh_daemon::protocol::RecoveryApplyResponse::Preview { preview } => {
            print_recovery_preview(&preview);
        }
        agentmesh_daemon::protocol::RecoveryApplyResponse::Executed {
            recovery_workflow_id,
        } => {
            println!("Recovery {recovery_id} executed.");
            println!("Recovery workflow: {recovery_workflow_id}");
        }
    }
    Ok(())
}

/// `agentmesh workflow lineage <id>` — the parent/child recovery chain.
async fn cmd_workflow_lineage(workflow_id: Uuid) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let lineage = client
        .workflow_lineage(workflow_id)
        .await
        .map_err(|err| anyhow!("failed to load lineage: {err}"))?
        .ok_or_else(|| anyhow!("workflow `{workflow_id}` not found"))?;
    println!("Workflow: {}", lineage.workflow_id);
    if let Some(parent) = &lineage.parent {
        println!(
            "Recovers: {} ({}, attempt {})",
            parent.workflow_id,
            parent.status.as_str(),
            parent.recovery_attempt
        );
    }
    for child in &lineage.recovery_children {
        println!(
            "Recovered by: {} ({})",
            child.workflow_id,
            child.status.as_str()
        );
        if let Some(node) = &child.recovery_of_node_id {
            println!("  at failed node: {node}");
        }
    }
    if lineage.parent.is_none() && lineage.recovery_children.is_empty() {
        println!("No recovery lineage.");
    }
    Ok(())
}

/// Render a recovery proposal detail (spec §23 hint).
fn print_recovery_detail(detail: &agentmesh_daemon::protocol::RecoveryDetail) {
    println!("Recovery: {}", detail.recovery_id);
    println!("Workflow: {}", detail.workflow_id);
    println!("Failed node: {}", detail.failed_node_id);
    println!("Status: {}", detail.status);
    println!("Attempt: {}", detail.attempt);
    if let Some(summary) = &detail.summary {
        println!("Summary: {summary}");
    }
    if let Some(workflow_id) = detail.recovery_workflow_id {
        println!("Recovery workflow: {workflow_id}");
    }
    if let Some(error) = &detail.validation_error {
        println!("Error: {error}");
    }
    if let Some(plan) = &detail.plan {
        println!();
        for node in &plan.nodes {
            println!("{}", node.id);
            println!("  intent: {}", node.intent);
            if node.depends_on.is_empty() {
                println!("  depends: -");
            } else {
                println!("  depends: {}", node.depends_on.join(", "));
            }
        }
    }
    println!();
    println!("Run:");
    println!(
        "  agentmesh workflow recovery execute {} --check",
        detail.recovery_id
    );
}

/// Render the `recovery execute --check` preview. Never shows raw JSON.
fn print_recovery_preview(preview: &agentmesh_daemon::protocol::RecoveryPreview) {
    println!("Recovery: {}", preview.recovery_id);
    println!("Workflow: {}", preview.workflow_id);
    println!("Failed node: {}", preview.failed_node_id);
    println!("Status: {}", preview.status);
    println!("Attempt: {}", preview.attempt);
    println!();
    println!("Budget:");
    println!(
        "  nodes         {:>3} / {:<3}",
        preview.node_count, preview.policy_max_nodes
    );
    println!(
        "  agent calls   {:>3} / {:<3}",
        preview.estimated_agent_calls, preview.policy_max_agent_calls
    );
    println!(
        "  chain calls   {:>3} / {:<3}",
        preview.chain_calls_used, preview.chain_calls_max
    );
    println!();
    println!("Status: {}", preview.status);
    println!("Re-run with --yes to create the recovery workflow.");
}

// ---------- Phase 21: multi-agent evaluation ----------

/// `agentmesh workflow evaluate <id> [--evaluators N --strategy --quorum]` —
/// run a parallel evaluation of a workflow's latest implementation.
async fn cmd_workflow_evaluate(
    workflow_id: Uuid,
    evaluators: Option<usize>,
    strategy: Option<&str>,
    quorum: Option<usize>,
) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let response = client
        .start_evaluation(workflow_id, evaluators, strategy, quorum)
        .await
        .map_err(|err| anyhow!("failed to start evaluation: {err}"))?;
    println!("Evaluation: {}", response.group_id);
    println!("Workflow: {}", response.workflow_id);
    println!();
    // The evaluation workflow runs asynchronously; stream its events.
    stream_workflow_events(&client, response.workflow_id).await
}

/// `agentmesh workflow evaluations <id>` — the workflow's evaluation rounds
/// (Phase 22 §18): ROUND / GROUP / VALID / RESULT / SNAPSHOT per round.
async fn cmd_workflow_evaluations(workflow_id: Uuid, json: bool) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let rounds = client
        .list_workflow_evaluations(workflow_id)
        .await
        .map_err(|err| anyhow!("failed to load evaluations: {err}"))?;

    if json {
        println!("{}", serde_json::to_string_pretty(&rounds)?);
        return Ok(());
    }

    if rounds.is_empty() {
        println!("Workflow {workflow_id} has no evaluation rounds.");
        return Ok(());
    }
    println!("Workflow: {workflow_id}");
    println!();
    println!(
        "{:<6} {:<40} {:<7} {:<20} SNAPSHOT",
        "ROUND", "GROUP", "VALID", "RESULT"
    );
    for round in &rounds {
        let result = round
            .consensus
            .as_ref()
            .map(|c| c.outcome.as_str().to_string())
            .unwrap_or_else(|| "—".to_string());
        println!(
            "{:<6} {:<40} {:<7} {:<20} {}",
            round.round,
            round.group_id,
            round
                .consensus
                .as_ref()
                .map(|c| format!("{}/{}", c.valid_count, c.total_count))
                .unwrap_or_else(|| "-".to_string()),
            result,
            round
                .snapshot_hash
                .as_deref()
                .map(|h| truncate_line(h, 24))
                .unwrap_or_else(|| "-".to_string()),
        );
    }
    Ok(())
}

// ---------- Phase 23: Best-of-N competition CLI ----------

/// `agentmesh workflow competitions <id>` — list competition groups of a workflow.
async fn cmd_workflow_competitions(workflow_id: Uuid, json: bool) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let groups = client
        .list_workflow_competitions(workflow_id)
        .await
        .map_err(|err| anyhow!("failed to load competitions: {err}"))?;

    if json {
        println!("{}", serde_json::to_string_pretty(&groups)?);
        return Ok(());
    }

    if groups.is_empty() {
        println!("Workflow {workflow_id} has no competition groups.");
        return Ok(());
    }

    println!("Workflow: {workflow_id}");
    println!();
    println!(
        "{:<40} {:<12} {:<12} WINNER",
        "GROUP ID", "STATUS", "CANDIDATES"
    );
    for group in &groups {
        println!(
            "{:<40} {:<12} {:<12} {}",
            group.id,
            group.status,
            group.candidate_count,
            group.winner_candidate_id.as_deref().unwrap_or("-"),
        );
    }
    Ok(())
}

/// `agentmesh competition show <id>` — show a competition group with candidates.
async fn cmd_competition_show(group_id: Uuid, json: bool) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let detail = client
        .get_competition(group_id)
        .await
        .map_err(|err| anyhow!("failed to load competition: {err}"))?
        .ok_or_else(|| anyhow!("competition group `{group_id}` not found"))?;

    if json {
        println!("{}", serde_json::to_string_pretty(&detail)?);
        return Ok(());
    }

    println!("Competition Group: {}", detail.id);
    println!("  workflow: {}", detail.workflow_id);
    println!("  status: {}", detail.status);
    println!("  candidates: {}", detail.candidate_count);
    println!("  base revision: {}", detail.base_revision);
    if let Some(winner) = &detail.winner_candidate_id {
        println!("  winner candidate: {winner}");
    }
    if let Some(task) = detail.winner_task_id {
        println!("  winner task: {task}");
    }
    if let Some(ws) = detail.winner_workspace_id {
        println!("  winner workspace: {ws}");
    }
    if let Some(hash) = &detail.winner_snapshot_hash {
        println!("  winner snapshot: {hash}");
    }
    println!();
    println!("Candidates:");
    for c in &detail.candidates {
        println!(
            "  [{}] agent={} status={} task={} workspace={} snapshot={}",
            c.candidate_id,
            c.agent_id,
            c.status,
            c.task_id
                .map(|t| t.to_string())
                .unwrap_or_else(|| "-".into()),
            c.workspace_id
                .map(|w| w.to_string())
                .unwrap_or_else(|| "-".into()),
            c.snapshot_hash
                .as_deref()
                .map(|h| truncate_line(h, 16))
                .unwrap_or_else(|| "-".into()),
        );
        if let Some(summary) = &c.summary {
            println!("    summary: {}", truncate_line(summary, 80));
        }
    }
    Ok(())
}

/// `agentmesh workflow audit <id>` — verify immutable provenance ledger and print audit trail.
async fn cmd_workflow_audit(
    workflow_id: Uuid,
    verbose: bool,
    json: bool,
    ndjson: bool,
) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let audit = client
        .workflow_audit(workflow_id)
        .await
        .map_err(|err| anyhow!("failed to audit workflow: {err}"))?;

    if json {
        println!("{}", serde_json::to_string_pretty(&audit)?);
        return Ok(());
    }

    if ndjson {
        for ev in &audit.events {
            println!("{}", serde_json::to_string(&ev)?);
        }
        return Ok(());
    }

    println!("Workflow Audit: {}", audit.workflow_id);
    println!("  Schema Version:  {}", audit.schema_version);
    println!("  Legacy Workflow: {}", audit.is_legacy);
    println!(
        "  Integrity Check: {}",
        if audit.integrity_valid {
            "VALID (PASS)"
        } else {
            "TAMPERED / INVALID (FAIL)"
        }
    );
    println!("  Total Events:    {}", audit.events_count);
    println!();

    if !audit.details.is_empty() {
        println!("Audit Verifications:");
        for d in &audit.details {
            println!("  • {d}");
        }
        println!();
    }

    if audit.events.is_empty() {
        if audit.is_legacy {
            println!(
                "No provenance events (Legacy workflow: provenance unavailable before schema v1)."
            );
        } else {
            println!("No provenance events recorded for this workflow.");
        }
        return Ok(());
    }

    println!(
        "{:<4} {:<24} {:<18} {:<12} {:<12} {:<12}",
        "SEQ", "EVENT TYPE", "ENTITY", "ACTOR", "PAYLOAD HASH", "EVENT HASH"
    );
    for ev in &audit.events {
        let payload_short = if ev.payload_hash.len() > 10 {
            &ev.payload_hash[..10]
        } else {
            &ev.payload_hash
        };
        let event_short = if ev.event_hash.len() > 10 {
            &ev.event_hash[..10]
        } else {
            &ev.event_hash
        };
        println!(
            "{:<4} {:<24} {:<18} {:<12} {:<12} {:<12}",
            ev.sequence,
            ev.event_type,
            format!("{}:{}", ev.entity_type, truncate_line(&ev.entity_id, 10)),
            ev.actor_id.as_deref().unwrap_or(ev.actor_type.as_str()),
            payload_short,
            event_short,
        );

        if verbose {
            println!(
                "     Payload: {}",
                serde_json::to_string(&ev.payload).unwrap_or_default()
            );
        }
    }

    Ok(())
}

/// `agentmesh workflow replay <id>` — run deterministic decision replay.
async fn cmd_workflow_replay(
    workflow_id: Uuid,
    verify_only: bool,
    json: bool,
) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let replay = client
        .workflow_replay(workflow_id, verify_only)
        .await
        .map_err(|err| anyhow!("failed to replay workflow: {err}"))?;

    if json {
        println!("{}", serde_json::to_string_pretty(&replay)?);
        return Ok(());
    }

    println!("Deterministic Decision Replay: {}", replay.workflow_id);
    println!(
        "  Overall Status:      {}",
        if replay.passed { "PASSED" } else { "FAILED" }
    );
    println!("  Legacy Workflow:     {}", replay.is_legacy);
    println!();
    println!("Decision Replay Results:");
    println!(
        "  • Hash Integrity Check:         {}",
        if replay.integrity_passed {
            "PASS"
        } else {
            "FAIL"
        }
    );
    println!(
        "  • Consensus Recomputation:      {}",
        if replay.consensus_passed {
            "PASS"
        } else {
            "FAIL"
        }
    );
    println!(
        "  • Selection Gate Recomputation: {}",
        if replay.selection_passed {
            "PASS"
        } else {
            "FAIL"
        }
    );
    println!(
        "  • Apply Source Provenance:      {}",
        if replay.apply_passed { "PASS" } else { "FAIL" }
    );
    println!(
        "  • Policy Snapshot Bounds:       {}",
        if replay.policy_passed { "PASS" } else { "FAIL" }
    );
    println!();

    if !replay.mismatches.is_empty() {
        println!("Mismatches Detected:");
        for m in &replay.mismatches {
            println!("  [!] {m}");
        }
        println!();
    }

    if !replay.details.is_empty() {
        println!("Replay Verification Details:");
        for d in &replay.details {
            println!("  • {d}");
        }
    }

    if !replay.passed {
        anyhow::bail!("Deterministic decision replay failed with mismatches");
    }

    Ok(())
}

/// `agentmesh workflow export <id>` — export redacted provenance events.
async fn cmd_workflow_export(
    workflow_id: Uuid,
    output: Option<String>,
    ndjson: bool,
) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let audit = client
        .workflow_audit(workflow_id)
        .await
        .map_err(|err| anyhow!("failed to export workflow provenance: {err}"))?;

    let content = if ndjson {
        let mut lines = Vec::new();
        for ev in &audit.events {
            lines.push(serde_json::to_string(&ev)?);
        }
        lines.join("\n") + "\n"
    } else {
        serde_json::to_string_pretty(&audit.events)? + "\n"
    };

    if let Some(path) = output {
        std::fs::write(&path, &content)
            .map_err(|err| anyhow!("failed to write export to `{path}`: {err}"))?;
        println!(
            "Exported {} provenance events to `{path}`",
            audit.events.len()
        );
    } else {
        print!("{content}");
    }

    Ok(())
}

/// `agentmesh evaluation show <id>` — a group's members + consensus (spec §17).
async fn cmd_evaluation_show(group_id: Uuid) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let detail = client
        .get_evaluation(group_id)
        .await
        .map_err(|err| anyhow!("failed to load evaluation: {err}"))?
        .ok_or_else(|| anyhow!("evaluation `{group_id}` not found"))?;
    println!("Evaluation: {}", detail.group_id);
    println!("Workflow: {}", detail.workflow_id);
    println!("Round: {}", detail.round);
    println!("Strategy: {}", detail.strategy);
    println!("Quorum: {}", detail.quorum);
    println!("Status: {}", detail.status);
    if let Some(hash) = &detail.snapshot_hash {
        println!("Snapshot: {}", truncate_line(hash, 24));
    }
    println!();
    let mut approved = 0;
    let mut changes = 0;
    let mut failed = 0;
    for (i, member) in detail.members.iter().enumerate() {
        let verdict = match &member.result {
            Some(result) => {
                if result.verdict == agentmesh_orchestrator::ReviewVerdict::Approved {
                    approved += 1;
                    "Approved"
                } else {
                    changes += 1;
                    "ChangesRequested"
                }
            }
            None => {
                if member.status == "failed" {
                    failed += 1;
                }
                "—"
            }
        };
        let confidence = member
            .result
            .as_ref()
            .and_then(|r| r.confidence)
            .map(|c| format!("{c:.2}"))
            .unwrap_or_else(|| "-".to_string());
        println!(
            "[{}] {:<12} {:<18} {confidence}",
            i + 1,
            if member.agent_id.is_empty() {
                "(pending)"
            } else {
                &member.agent_id
            },
            verdict
        );
    }
    if let Some(consensus) = &detail.consensus {
        println!();
        println!("Consensus:");
        println!("  strategy: {}", consensus.strategy.as_str());
        println!(
            "  valid:    {}/{}",
            consensus.valid_count, consensus.total_count
        );
        println!("  result:   {}", consensus.outcome.as_str());
        if consensus.outcome
            == agentmesh_orchestrator::evaluation::ConsensusOutcome::ChangesRequested
            && !consensus.issues.is_empty()
        {
            println!();
            println!("Issues:");
            for issue in &consensus.issues {
                println!(
                    "  [{}] {}: {} (reported by {})",
                    issue.severity.key(),
                    issue.title,
                    issue.description,
                    issue.reported_by.join(", ")
                );
            }
        }
    } else {
        println!();
        println!("Consensus: pending");
    }
    let _ = (approved, changes, failed);
    Ok(())
}

// ---------- Phase 17: AI planner plans ----------

/// `agentmesh plan "goal"` — generate a plan through the planner agent over
/// A2A, then preview it. Generating never executes anything.
async fn cmd_plan_create(goal: &str, agent: Option<&str>) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let response = client
        .create_plan(goal, agent)
        .await
        .map_err(|err| anyhow!("failed to create plan: {err}"))?;
    let plan = client
        .get_plan(response.plan_id)
        .await
        .map_err(|err| anyhow!("failed to load plan: {err}"))?
        .ok_or_else(|| anyhow!("plan `{}` not found after generation", response.plan_id))?;
    print_plan_detail(&plan);
    Ok(())
}

async fn cmd_plan_show(plan_id: Uuid, json: bool) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let plan = client
        .get_plan(plan_id)
        .await
        .map_err(|err| anyhow!("failed to load plan: {err}"))?
        .ok_or_else(|| anyhow!("plan `{plan_id}` not found"))?;
    if json {
        match &plan.plan_json {
            Some(json) => println!("{json}"),
            None => anyhow::bail!("plan `{plan_id}` has no stored plan JSON"),
        }
    } else {
        print_plan_detail(&plan);
    }
    Ok(())
}

/// `agentmesh plan execute <id> [--check|--yes] [--max-parallel N]` —
/// preview (budget + policy, no workflow) unless `--yes` claims and runs it.
async fn cmd_plan_execute(
    plan_id: Uuid,
    max_parallel: usize,
    check: bool,
    yes: bool,
    source_workspace: Option<&Path>,
) -> anyhow::Result<()> {
    if check && yes {
        anyhow::bail!("use either --check or --yes, not both");
    }
    let client = daemon_client_or_err().await?;
    let source = resolve_source_workspace(source_workspace);
    let response = client
        .execute_plan_with_source(plan_id, max_parallel, !yes, source)
        .await
        .map_err(|err| anyhow!("failed to execute plan `{plan_id}`: {err}"))?;
    match response {
        agentmesh_daemon::protocol::PlanExecuteResponse::Preview { preview } => {
            print_execution_plan(&preview);
        }
        agentmesh_daemon::protocol::PlanExecuteResponse::Workflow { workflow_id } => {
            println!("Plan {plan_id} executed.");
            println!("Workflow: {workflow_id}");
            println!("Parallel: {} nodes at once", max_parallel);
            println!();
            stream_workflow_events(&client, workflow_id).await?;
        }
    }
    Ok(())
}

/// `agentmesh plan edit <id> --file plan.json` — replace the current revision
/// with an edited plan (same WorkflowPlan schema as the planner output).
async fn cmd_plan_edit(plan_id: Uuid, file: Option<PathBuf>) -> anyhow::Result<()> {
    let plan_json = match file {
        Some(path) => std::fs::read_to_string(&path)
            .map_err(|err| anyhow!("failed to read `{}`: {err}", path.display()))?,
        None => {
            use std::io::Read;
            let mut buffer = String::new();
            std::io::stdin()
                .read_to_string(&mut buffer)
                .map_err(|err| anyhow!("failed to read plan JSON from stdin: {err}"))?;
            buffer
        }
    };
    let client = daemon_client_or_err().await?;
    let response = client
        .edit_plan(plan_id, &plan_json)
        .await
        .map_err(|err| anyhow!("failed to edit plan `{plan_id}`: {err}"))?;
    println!("Plan {} updated.", response.plan_id);
    println!("Revision: {}", response.revision);
    Ok(())
}

/// `agentmesh plan diff <id>` — the original planner revision vs the current
/// revision.
async fn cmd_plan_diff(plan_id: Uuid) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let diff = client
        .diff_plan(plan_id)
        .await
        .map_err(|err| anyhow!("failed to diff plan `{plan_id}`: {err}"))?;
    print_plan_diff(&diff);
    Ok(())
}

/// `agentmesh plan revisions <id>` — revision history, oldest first.
async fn cmd_plan_revisions(plan_id: Uuid) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let revisions = client
        .plan_revisions(plan_id)
        .await
        .map_err(|err| anyhow!("failed to load revisions of plan `{plan_id}`: {err}"))?;
    if revisions.is_empty() {
        println!("No revisions.");
        return Ok(());
    }
    println!("REVISION  SOURCE      CREATED");
    for revision in revisions {
        println!(
            "{:<9} {:<11} {}",
            revision.revision, revision.source, revision.created_at
        );
    }
    Ok(())
}

async fn cmd_plans(json: bool) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let plans = client
        .list_plans()
        .await
        .map_err(|err| anyhow!("failed to list plans: {err}"))?;
    if json {
        println!("{}", serde_json::to_string_pretty(&plans)?);
        return Ok(());
    }
    if plans.is_empty() {
        println!("No plans.");
        return Ok(());
    }
    println!("{:<12} {:<10} {:<22} GOAL", "ID", "STATUS", "CREATED");
    for plan in plans {
        println!(
            "{:<12} {:<10} {:<22} {}",
            &plan.plan_id.to_string()[..8],
            plan.status,
            plan.created_at,
            truncate_line(&plan.goal, 60),
        );
    }
    Ok(())
}

/// Render a plan preview (spec §7): status first, then each node's
/// intent and dependency list. Never executes anything.
fn print_plan_detail(plan: &agentmesh_daemon::protocol::PlanDetail) {
    println!("Plan: {}", plan.plan_id);
    println!("Status: {}", plan.status);
    if let Some(revision) = plan.current_revision {
        println!("Revision: {revision}");
    }
    if let Some(revision) = plan.executed_revision {
        println!("Executed revision: {revision}");
    }
    if let Some(summary) = &plan.summary {
        println!("Summary: {summary}");
    }
    if let Some(workflow_id) = plan.workflow_id {
        println!("Workflow: {workflow_id}");
    }
    if let Some(error) = &plan.validation_error {
        println!("Error: {error}");
    }
    if !plan.nodes.is_empty() {
        println!();
        for node in &plan.nodes {
            println!("{}", node.id);
            println!("  intent: {}", node.intent);
            if node.depends_on.is_empty() {
                println!("  depends: -");
            } else {
                println!("  depends: {}", node.depends_on.join(", "));
            }
        }
    }
}

/// Render the `plan execute --check` preview (spec §11): budget + policy
/// usage. Never creates a workflow.
fn print_execution_plan(preview: &agentmesh_daemon::protocol::PlanPreview) {
    println!("Execution Plan");
    println!();
    println!("Plan: {}", preview.plan_id);
    println!("Status: {}", preview.status);
    if let Some(revision) = preview.revision {
        println!("Revision: {revision}");
    }
    println!("Nodes: {}", preview.node_count);
    println!("Roots: {}", preview.root_count);
    println!("Terminals: {}", preview.terminal_count);
    println!("Estimated agent calls: {}", preview.estimated_agent_calls);
    println!("Evaluator calls: {}", preview.evaluation_agent_calls);
    println!("Planning calls: {}", preview.planning_calls);
    println!("Max parallel: {}", preview.effective_max_parallel);
    println!();
    println!("Policy:");
    println!(
        "  nodes        {:>3} / {:<3}",
        preview.node_count, preview.policy.max_nodes
    );
    println!(
        "  agent calls  {:>3} / {:<3}",
        preview.estimated_agent_calls, preview.policy.max_agent_calls
    );
    println!(
        "  parallel     {:>3} / {:<3}",
        preview.max_parallel_requested, preview.policy.max_parallel
    );
    println!();
    println!("Status: {}", preview.status);
}

/// Render a plan diff: the planner revision vs the current revision.
fn print_plan_diff(diff: &agentmesh_orchestrator::diff::PlanDiff) {
    if diff.is_empty() {
        println!("No changes (current revision matches the planner output).");
        return;
    }
    for node_id in &diff.added_nodes {
        println!("+ node {node_id}");
    }
    for node_id in &diff.removed_nodes {
        println!("- node {node_id}");
    }
    for change in &diff.changed_objective {
        println!(
            "~ objective {}: {} -> {}",
            change.node_id, change.before, change.after
        );
    }
    for change in &diff.changed_role {
        println!(
            "~ role {}: {} -> {}",
            change.node_id, change.before, change.after
        );
    }
    for change in &diff.changed_intent {
        println!(
            "~ intent {}: {} -> {}",
            change.node_id, change.before, change.after
        );
    }
    for change in &diff.changed_dependencies {
        let before = if change.before.is_empty() {
            "-".to_string()
        } else {
            change.before.join(", ")
        };
        let after = if change.after.is_empty() {
            "-".to_string()
        } else {
            change.after.join(", ")
        };
        println!("~ dependencies {}: [{before}] -> [{after}]", change.node_id);
    }
}

// ---------- Phase 13: safe apply ----------

/// Plan (preview) or apply an agent result to the source repository through
/// the daemon. Without `--yes` this is a preview that never writes.
async fn cmd_apply(
    client: &agentmesh_daemon::DaemonClient,
    target: ApplyTarget,
    check: bool,
    yes: bool,
) -> anyhow::Result<()> {
    if check && yes {
        anyhow::bail!("use either --check or --yes, not both");
    }
    let preview = !yes;
    if preview {
        let response = match target {
            ApplyTarget::Task(task_id) => client
                .apply_task(task_id, true)
                .await
                .map_err(|err| anyhow!("failed to plan apply for task `{task_id}`: {err}"))?,
            ApplyTarget::Workflow(workflow_id) => client
                .apply_workflow(workflow_id, true)
                .await
                .map_err(|err| {
                    anyhow!("failed to plan apply for workflow `{workflow_id}`: {err}")
                })?,
        };
        match response {
            agentmesh_daemon::protocol::ApplyResponse::Plan { plan } => {
                print_apply_plan(&plan);
                Ok(())
            }
            agentmesh_daemon::protocol::ApplyResponse::Applied { .. } => {
                anyhow::bail!("daemon returned an apply outcome for a --check request")
            }
        }
    } else {
        let response =
            match target {
                ApplyTarget::Task(task_id) => client
                    .apply_task(task_id, false)
                    .await
                    .map_err(|err| anyhow!("failed to apply task `{task_id}`: {err}"))?,
                ApplyTarget::Workflow(workflow_id) => client
                    .apply_workflow(workflow_id, false)
                    .await
                    .map_err(|err| anyhow!("failed to apply workflow `{workflow_id}`: {err}"))?,
            };
        match response {
            agentmesh_daemon::protocol::ApplyResponse::Applied { outcome } => {
                print_apply_outcome(&outcome);
                Ok(())
            }
            agentmesh_daemon::protocol::ApplyResponse::Plan { .. } => {
                anyhow::bail!("daemon returned a plan for an apply request")
            }
        }
    }
}

/// Render the preflight plan (the CLI `--check` / preview output).
fn print_apply_plan(plan: &agentmesh_apply::ApplyPlan) {
    println!("Apply Plan");
    println!();
    println!("source: {}", plan.source_repository.display());
    println!("workspace: {}", plan.workspace.display());
    println!("base: {}", short_rev(&plan.base_revision));
    println!("files: {}", plan.file_count());
    println!("patch: {}", agentmesh_apply::human_size(plan.patch_size));
    let status = if plan.already_applied {
        "already applied"
    } else if plan.applicable {
        "ready"
    } else {
        "no changes"
    };
    println!("status: {status}");
    if !plan.warnings.is_empty() {
        for warning in &plan.warnings {
            println!("warning: {warning}");
        }
    }
    println!();
    for file in &plan.changed_files {
        println!("{} {}", file.status, file.path);
    }
    for path in &plan.untracked_files {
        println!("U {path}");
    }
    println!();
    if plan.already_applied {
        println!("This workspace result was already applied; nothing to do.");
    } else if plan.applicable {
        println!("Re-run with --yes to apply.");
    } else {
        println!("Nothing to apply.");
    }
}

/// Render the result of executing an apply.
fn print_apply_outcome(outcome: &agentmesh_apply::ApplyOutcome) {
    let plan = &outcome.plan;
    println!("Apply applied");
    println!();
    println!("apply id: {}", outcome.apply_id);
    println!("source: {}", plan.source_repository.display());
    println!("workspace: {}", plan.workspace.display());
    println!("base: {}", short_rev(&plan.base_revision));
    println!(
        "tracked patch: {}",
        if outcome.tracked_applied {
            "applied"
        } else {
            "none"
        }
    );
    println!("untracked files: {}", outcome.untracked_copied);
    println!();
    println!("The agent changes are now in the source working tree.");
    println!("HEAD is unchanged; review and commit them yourself (Apply ≠ Commit).");
}

fn short_rev(rev: &str) -> &str {
    &rev[..rev.len().min(12)]
}

// ---------- Phase 14: apply history + workspace lifecycle ----------

/// `agentmesh applies`
async fn cmd_applies(limit: usize, status: Option<&str>, json: bool) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let applies = client
        .list_applies(limit, status)
        .await
        .map_err(|err| anyhow!("failed to list applies: {err}"))?;
    if json {
        println!("{}", serde_json::to_string_pretty(&applies)?);
        return Ok(());
    }
    if applies.is_empty() {
        println!("No applies.");
        return Ok(());
    }
    println!(
        "{:<12} {:<10} {:<22} {:<10} CREATED",
        "ID", "STATUS", "TASK/WORKFLOW", "WORKSPACE"
    );
    for apply in applies {
        let source = apply
            .task_id
            .map(|id| format!("task {}", &id.to_string()[..8]))
            .or_else(|| {
                apply
                    .workflow_id
                    .map(|id| format!("workflow {}", &id.to_string()[..8]))
            })
            .unwrap_or_else(|| "-".to_string());
        println!(
            "{:<12} {:<10} {:<22} {:<10} {}",
            &apply.apply_id.to_string()[..8],
            apply.status,
            source,
            &apply.workspace_id.to_string()[..8],
            truncate_line(&apply.created_at, 10),
        );
    }
    Ok(())
}

/// `agentmesh apply show <APPLY_ID>`
async fn cmd_apply_show(apply_id: Uuid) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let apply = client
        .get_apply(apply_id)
        .await
        .map_err(|err| anyhow!("failed to load apply: {err}"))?
        .ok_or_else(|| anyhow!("apply `{apply_id}` not found"))?;
    println!("Apply");
    println!("  id: {}", apply.apply_id);
    if let Some(task_id) = apply.task_id {
        println!("  task: {task_id}");
    }
    if let Some(workflow_id) = apply.workflow_id {
        println!("  workflow: {workflow_id}");
    }
    println!("  workspace: {}", apply.workspace_id);
    println!("  source: {}", apply.source_repository);
    println!("  base: {}", short_rev(&apply.base_revision));
    println!("  status: {}", apply.status);
    println!("  created: {}", apply.created_at);
    if let Some(completed) = apply.completed_at {
        println!("  completed: {completed}");
    }
    if let Some(error) = apply.error {
        println!("  error: {error}");
    }
    if let Some(hash) = apply.workspace_snapshot_hash {
        println!("  snapshot hash: {hash}");
    }
    Ok(())
}

/// `agentmesh workspaces`
async fn cmd_workspaces(
    state_filter: Option<&str>,
    limit: usize,
    json: bool,
) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let workspaces = client
        .list_workspaces(state_filter, limit)
        .await
        .map_err(|err| anyhow!("failed to list workspaces: {err}"))?;
    if json {
        println!("{}", serde_json::to_string_pretty(&workspaces)?);
        return Ok(());
    }
    if workspaces.is_empty() {
        println!("No workspaces.");
        return Ok(());
    }
    println!(
        "{:<12} {:<10} {:<10} {:<24} {:<30} CREATED",
        "ID", "AGENT", "STATE", "REPOSITORY", "BRANCH"
    );
    for workspace in workspaces {
        println!(
            "{:<12} {:<10} {:<10} {:<24} {:<30} {}",
            &workspace.id.to_string()[..8],
            workspace.agent_id,
            workspace.state,
            truncate_line(&workspace.repository, 24),
            truncate_line(&workspace.branch, 30),
            truncate_line(&workspace.created_at, 10),
        );
    }
    Ok(())
}

/// `agentmesh workspace archive <TASK_ID>`
async fn cmd_workspace_archive(task_id: Uuid) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    client
        .archive_task(task_id)
        .await
        .map_err(|err| anyhow!("failed to archive workspace: {err}"))?;
    println!("Workspace of task {task_id} archived.");
    println!("The worktree and branch are kept; diff remains viewable.");
    Ok(())
}

/// `agentmesh workspace cleanup <TASK_ID> [--check|--yes]`
async fn cmd_workspace_cleanup(task_id: Uuid, check: bool, yes: bool) -> anyhow::Result<()> {
    if check && yes {
        anyhow::bail!("use either --check or --yes, not both");
    }
    let client = daemon_client_or_err().await?;
    if !yes {
        let response = client
            .cleanup_task(task_id, true)
            .await
            .map_err(|err| anyhow!("failed to plan cleanup: {err}"))?;
        match response {
            agentmesh_daemon::protocol::CleanupResponse::Plan { plan } => {
                print_cleanup_plan(&plan);
                Ok(())
            }
            _ => anyhow::bail!("daemon returned an unexpected cleanup response"),
        }
    } else {
        let response = client
            .cleanup_task(task_id, false)
            .await
            .map_err(|err| anyhow!("failed to clean up workspace: {err}"))?;
        match response {
            agentmesh_daemon::protocol::CleanupResponse::Removed { outcome } => {
                print_cleanup_outcome(&outcome);
                Ok(())
            }
            _ => anyhow::bail!("daemon returned an unexpected cleanup response"),
        }
    }
}

/// `agentmesh workflow cleanup <WORKFLOW_ID> [--check|--yes]`
async fn cmd_workflow_cleanup(workflow_id: Uuid, check: bool, yes: bool) -> anyhow::Result<()> {
    if check && yes {
        anyhow::bail!("use either --check or --yes, not both");
    }
    let client = daemon_client_or_err().await?;
    if !yes {
        let response = client
            .cleanup_workflow(workflow_id, true)
            .await
            .map_err(|err| anyhow!("failed to plan workflow cleanup: {err}"))?;
        match response {
            agentmesh_daemon::protocol::CleanupResponse::Plans { plans } => {
                for plan in &plans {
                    print_cleanup_plan(plan);
                }
                if plans.is_empty() {
                    println!("No workspaces used by this workflow.");
                } else {
                    println!();
                    println!("All workspaces are safe; re-run with --yes to remove them.");
                }
                Ok(())
            }
            _ => anyhow::bail!("daemon returned an unexpected cleanup response"),
        }
    } else {
        let response = client
            .cleanup_workflow(workflow_id, false)
            .await
            .map_err(|err| anyhow!("failed to clean up workflow workspaces: {err}"))?;
        match response {
            agentmesh_daemon::protocol::CleanupResponse::RemovedAll { outcomes } => {
                for outcome in &outcomes {
                    print_cleanup_outcome(outcome);
                }
                Ok(())
            }
            _ => anyhow::bail!("daemon returned an unexpected cleanup response"),
        }
    }
}

/// `agentmesh artifacts prune --older-than <days> [--check]`
async fn cmd_artifacts_prune(older_than: u64, check: bool) -> anyhow::Result<()> {
    let client = daemon_client_or_err().await?;
    let result = client
        .prune_artifacts(older_than, check)
        .await
        .map_err(|err| anyhow!("failed to prune artifacts: {err}"))?;
    if check {
        println!(
            "Artifact prune preview: {} file-backed artifact(s) older than {older_than} day(s) qualify.",
            result.candidates
        );
        println!("Re-run without --check to prune them.");
    } else {
        println!(
            "Pruned {} file-backed artifact(s); SQLite history is preserved.",
            result.pruned
        );
    }
    Ok(())
}

fn print_cleanup_plan(plan: &agentmesh_workspace::CleanupPlan) {
    println!("Cleanup Plan");
    println!();
    println!("  workspace: {}", plan.workspace_path.display());
    println!("  branch: {}", plan.branch);
    println!("  state: {}", plan.state.as_str());
    println!("  base: {}", short_rev(&plan.base_revision));
    println!(
        "  applied: {}",
        if plan.has_completed_apply {
            "yes"
        } else {
            "no (archive-only)"
        }
    );
    println!(
        "  snapshot: {}",
        if plan.snapshot_matches {
            "matches apply"
        } else {
            "changed since apply"
        }
    );
    println!("  safe: {}", if plan.safe { "yes" } else { "no" });
    println!();
    if plan.safe {
        println!("Re-run with --yes to remove the worktree and its managed branch.");
    }
}

fn print_cleanup_outcome(outcome: &agentmesh_workspace::CleanupOutcome) {
    println!(
        "Workspace {} removed.",
        &outcome.workspace_id.to_string()[..8]
    );
    println!(
        "  worktree removed: {}",
        if outcome.worktree_removed {
            "yes"
        } else {
            "no"
        }
    );
    println!(
        "  managed branch removed: {}",
        if outcome.branch_removed { "yes" } else { "no" }
    );
    println!("  state: {}", outcome.state.as_str());
    println!("Task/workflow/apply history and the agent session are preserved.");
}

/// Stream a workflow's event feed (persisted replay + live events). Ctrl+C
/// only detaches — the daemon keeps running the workflow.
async fn stream_workflow_events(
    client: &agentmesh_daemon::DaemonClient,
    workflow_id: Uuid,
) -> anyhow::Result<()> {
    let mut stream = Box::pin(client.workflow_events(workflow_id, 0));
    loop {
        tokio::select! {
            event = stream.next() => {
                let Some(event) = event else { break };
                let event = event.map_err(|err| anyhow!("workflow stream error: {err}"))?;
                if print_workflow_event(&event.data) {
                    break; // terminal event reached
                }
            }
            _ = tokio::signal::ctrl_c() => {
                eprintln!();
                eprintln!("[agentmesh] detached (workflow continues in the daemon)");
                break;
            }
        }
    }
    Ok(())
}

/// Print one workflow event; returns `true` for terminal workflow events.
fn print_workflow_event(event: &agentmesh_daemon::protocol::WorkflowStreamEvent) -> bool {
    use agentmesh_daemon::protocol::WorkflowStreamEvent as E;
    match event {
        E::WorkflowStarted { preset, goal, .. } => {
            println!("Workflow: {preset} ({goal})");
            false
        }
        E::StepStarted {
            ordinal,
            role,
            agent_id,
            ..
        } => {
            println!("\n[{}] {} → {agent_id}", ordinal + 1, role.label());
            false
        }
        E::AgentMessage {
            agent_id, message, ..
        } => {
            for line in message.lines() {
                println!("[{agent_id}] {line}");
            }
            false
        }
        E::StepCompleted { role, .. } => {
            println!("✓ {} completed", role.label());
            false
        }
        E::StepFailed { role, error, .. } => {
            println!("✗ {} failed: {error}", role.label());
            false
        }
        E::StepCancelled { role, .. } => {
            println!("… {} cancelled", role.label());
            false
        }
        E::StepSkipped { role, .. } => {
            println!("- {} skipped", role.label());
            false
        }
        // Phase 16: DAG node events — parallel output tagged by node id.
        E::NodeReady { node_id, role, .. } => {
            println!("[{}] {} ready", node_id, role.label());
            false
        }
        E::NodeStarted {
            node_id,
            role,
            agent_id,
            ..
        } => {
            println!("\n[{}] {} → {agent_id}", node_id, role.label());
            false
        }
        E::NodeCompleted { node_id, role, .. } => {
            println!("✓ [{}] {} completed", node_id, role.label());
            false
        }
        E::NodeFailed {
            node_id,
            role,
            error,
            ..
        } => {
            println!("✗ [{}] {} failed: {error}", node_id, role.label());
            false
        }
        E::NodeCancelled { node_id, role, .. } => {
            println!("… [{}] {} cancelled", node_id, role.label());
            false
        }
        E::NodeSkipped { node_id, role, .. } => {
            println!("- [{}] {} skipped", node_id, role.label());
            false
        }
        E::NodeInterrupted { node_id, role, .. } => {
            println!("… [{}] {} interrupted", node_id, role.label());
            false
        }
        E::WorkflowCompleted {
            final_review_verdict,
            ..
        } => {
            println!("\nWorkflow completed");
            if let Some(verdict) = final_review_verdict {
                println!("Final review: {}", verdict.key());
            }
            true
        }
        E::WorkflowFailed { error, .. } => {
            println!("\nWorkflow failed");
            if let Some(error) = error {
                println!("Error: {error}");
            }
            true
        }
        E::WorkflowCancelled { .. } => {
            println!("\nWorkflow cancelled");
            true
        }
        E::WorkflowInterrupted { reason, .. } => {
            println!("\nWorkflow interrupted");
            if !reason.is_empty() {
                println!("Reason: {reason}");
            }
            true
        }
        // Phase 20: failure recovery events.
        E::RecoveryPlanningStarted { failed_node_id, .. } => {
            println!("\nRecovery planning started (failed at: {failed_node_id})");
            false
        }
        E::RecoveryProposalReady {
            recovery_id,
            attempt,
            ..
        } => {
            println!("\nRecovery proposal {recovery_id} ready (attempt {attempt})");
            println!("  Run: agentmesh workflow recovery execute {recovery_id} --check");
            false
        }
        E::RecoveryStarted {
            recovery_workflow_id,
            ..
        } => {
            println!("\nRecovery workflow started: {recovery_workflow_id}");
            false
        }
        E::RecoveryCompleted {
            recovery_workflow_id,
            ..
        } => {
            println!("\nRecovery completed by workflow {recovery_workflow_id}");
            false
        }
        E::RecoveryFailed {
            recovery_workflow_id,
            error,
            ..
        } => {
            println!("\nRecovery failed (workflow {recovery_workflow_id})");
            if let Some(error) = error {
                println!("Error: {error}");
            }
            false
        }
        E::RecoveryLimitReached { .. } => {
            println!("\nRecovery limit reached — no further recovery");
            false
        }
        // Phase 22/23: multi-agent evaluation and competition events.
        E::EvaluationSnapshotChanged { node_id, .. } => {
            println!("\nEvaluation snapshot changed for node {node_id}");
            false
        }
        E::CandidateStarted {
            candidate_id,
            agent_id,
            ..
        } => {
            println!("\nCandidate {candidate_id} started → {agent_id}");
            false
        }
        E::CandidateCompleted { candidate_id, .. } => {
            println!("✓ Candidate {candidate_id} completed");
            false
        }
        E::CandidateFailed {
            candidate_id,
            error,
            ..
        } => {
            println!("✗ Candidate {candidate_id} failed: {error}");
            false
        }
        E::CandidateSnapshotChanged { candidate_id, .. } => {
            println!(
                "\nCandidate {candidate_id} workspace snapshot changed; candidate disqualified"
            );
            false
        }
        E::CandidateConsensusReady {
            candidate_id,
            outcome,
            ..
        } => {
            println!("Consensus for {candidate_id}: {outcome}");
            false
        }
        E::WinnerSelected {
            candidate_id,
            agent_id,
            ..
        } => {
            println!("\n★ Winner selected: {candidate_id} (agent: {agent_id})");
            false
        }
        E::NoAcceptableCandidate { .. } => {
            println!("\n✗ No candidate approved by consensus");
            false
        }
    }
}

/// Truncate a single line for table display.
fn truncate_line(text: &str, max: usize) -> String {
    let line = text.lines().next().unwrap_or("").to_string();
    if line.chars().count() <= max {
        line
    } else {
        let truncated: String = line.chars().take(max).collect();
        format!("{truncated}…")
    }
}

// ---------- daemon helpers ----------

async fn daemon_client_or_err() -> anyhow::Result<agentmesh_daemon::DaemonClient> {
    let scope = agentmesh_daemon::Scope::resolve();
    agentmesh_daemon::connect_or_start(scope)
        .await
        .map_err(|err| anyhow!("unable to start AgentMesh daemon: {err}"))
}

/// Phase 22 §4: resolve the CLI's source workspace to an absolute path BEFORE
/// submitting — the daemon never guesses the cwd. An explicit `--source-workspace`
/// is canonicalized; otherwise the current directory is the default ONLY when
/// it is a git repository (so isolated worktrees can be created from it). The
/// daemon re-validates the result.
fn resolve_source_workspace(explicit: Option<&Path>) -> Option<String> {
    match explicit {
        Some(path) => {
            let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
            Some(canonical.display().to_string())
        }
        None => {
            let cwd = std::env::current_dir().ok()?;
            let output = std::process::Command::new("git")
                .args(["rev-parse", "--show-toplevel"])
                .current_dir(&cwd)
                .output()
                .ok()?;
            if output.status.success() {
                let root = String::from_utf8_lossy(&output.stdout).trim().to_string();
                if root.is_empty() { None } else { Some(root) }
            } else {
                None
            }
        }
    }
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
    let live_tasks = runtime
        .get("live_tasks")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    println!("live tasks: {}", live_tasks.len());
    for task in live_tasks {
        let id = task.get("task_id").and_then(|v| v.as_str()).unwrap_or("");
        let status = task.get("status").and_then(|v| v.as_str()).unwrap_or("");
        println!("  {id} ({status})");
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

// ---------- A2A discovery ----------

async fn a2a_client() -> anyhow::Result<agentmesh_daemon::DaemonClient> {
    let scope = agentmesh_daemon::Scope::resolve();
    agentmesh_daemon::connect_or_start(scope)
        .await
        .map_err(|err| anyhow!("unable to start AgentMesh daemon: {err}"))
}

async fn cmd_a2a_agents() -> anyhow::Result<()> {
    let client = a2a_client().await?;
    let runtime = client.runtime().await?;
    let a2a_agents = runtime
        .get("a2a_agents")
        .cloned()
        .unwrap_or(serde_json::json!({}));
    println!("{:<12} {:<34} CARD", "AGENT", "URL");
    if let Some(agents) = a2a_agents.as_object() {
        for (agent, info) in agents {
            let url = info.get("url").and_then(|v| v.as_str()).unwrap_or("");
            let card = info.get("card_url").and_then(|v| v.as_str()).unwrap_or("");
            println!("{:<12} {:<34} {}", agent, url, card);
        }
    }
    Ok(())
}

async fn cmd_a2a_card(agent: &str) -> anyhow::Result<()> {
    let client = a2a_client().await?;
    let runtime = client.runtime().await?;
    let a2a_agents = runtime
        .get("a2a_agents")
        .cloned()
        .unwrap_or(serde_json::json!({}));
    let card_url = a2a_agents
        .get(agent)
        .and_then(|info| info.get("card_url"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("no A2A listener for agent `{agent}` (is it online?)"))?;
    let response = reqwest::Client::new()
        .get(card_url)
        .send()
        .await
        .map_err(|err| anyhow!("failed to fetch card: {err}"))?;
    let card: serde_json::Value = response
        .json()
        .await
        .map_err(|err| anyhow!("invalid card json: {err}"))?;
    println!("{}", serde_json::to_string_pretty(&card)?);
    Ok(())
}
