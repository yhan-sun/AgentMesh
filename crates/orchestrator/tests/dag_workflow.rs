//! Orchestrator DAG scheduler tests (Phase 16): parallel execution against a
//! controllable, concurrency-aware mock A2A backend.
//!
//! The backend measures real parallelism with a barrier (two nodes both parked
//! active before release) and enforces one active task per agent session — a
//! busy session returns `SessionBusy` so the scheduler can prove it waits
//! rather than fails. No sleeps are used to *measure* concurrency; the barrier
//! and the active counter are the assertions.

mod common;

use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};
use std::time::Duration;

use agentmesh_a2a::backend::{A2ABackend, A2ABackendError, A2ARun, A2AStreamEvent};
use agentmesh_a2a::server::A2AServerConfig;
use agentmesh_core::{
    AgentDescriptor, AgentEvent, AgentMessage, AgentSkill, AgentTask, Artifact, TaskStatus,
    WorkspaceRequirement,
};
use agentmesh_orchestrator::dag::{WorkflowGraph, WorkflowNode};
use agentmesh_orchestrator::dag_scheduler::{DagRun, NodeStatus};
use agentmesh_orchestrator::directory::{AgentAuth, AgentDirectory, DiscoveredEndpoint};
use agentmesh_orchestrator::router::RuleRouter;
use agentmesh_orchestrator::workflow::{NoopObserver, WorkflowEngine, WorkflowObserver};
use agentmesh_orchestrator::{
    PRESET_PARALLEL_REVIEW, WorkflowOptions, WorkflowResult, WorkflowRole, WorkflowStatus,
};
use async_trait::async_trait;
use futures::Stream;
use tokio::sync::{Barrier, mpsc};
use uuid::Uuid;

/// A machine-parseable review.json artifact.
fn review_artifact(verdict: &str, summary: &str, issues: serde_json::Value) -> Artifact {
    let mut review = Artifact::text(
        "review.json",
        serde_json::json!({ "verdict": verdict, "summary": summary, "issues": issues }).to_string(),
    );
    review.kind = agentmesh_core::ArtifactKind::Json;
    review
}

// ---------- concurrency-aware test backend ----------

#[derive(Debug, Clone)]
struct RecordedTask {
    task_id: Uuid,
    context_id: Uuid,
    agent_session_id: Option<Uuid>,
    agent_id: String,
    prompt: String,
}

struct LiveTask {
    tx: tokio::sync::broadcast::Sender<(u64, A2AStreamEvent)>,
    run_tx: mpsc::Sender<A2AStreamEvent>,
    state: StdRwLock<Option<TaskStatus>>,
    artifacts: StdRwLock<Vec<Artifact>>,
}

/// Controllable backend with:
/// * one active task per agent session (busy → `SessionBusy`),
/// * an active counter for concurrency assertions,
/// * an optional barrier every started task parks on (proves overlap),
/// * FIFO per-agent event scripts (empty script = task stays live).
#[derive(Clone, Default)]
struct ConcurrencyBackend {
    tasks: Arc<StdRwLock<HashMap<Uuid, Arc<LiveTask>>>>,
    recordings: Arc<StdRwLock<Vec<RecordedTask>>>,
    sessions: Arc<StdRwLock<HashMap<(Uuid, String), Uuid>>>,
    session_busy: Arc<StdRwLock<HashMap<Uuid, Uuid>>>,
    busy_hits: Arc<AtomicUsize>,
    scripts: Arc<StdRwLock<HashMap<String, VecDeque<Vec<AgentEvent>>>>>,
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
    barrier: Arc<tokio::sync::Mutex<Option<Arc<Barrier>>>>,
    barrier_skip: Arc<StdRwLock<Option<String>>>,
    step: Duration,
}

impl ConcurrencyBackend {
    fn new() -> Self {
        Self {
            step: Duration::from_millis(5),
            ..Self::default()
        }
    }

    fn push_script(&self, agent_id: &str, script: Vec<AgentEvent>) {
        self.scripts
            .write()
            .unwrap()
            .entry(agent_id.to_string())
            .or_default()
            .push_back(script);
    }

    /// Park every started task (except those for `skip_agent`) on this barrier
    /// until it trips (proving that the expected number of nodes are
    /// concurrently active). Skipping the first node's agent lets a single-root
    /// DAG reach its parallel fan-out before the barrier matters.
    async fn set_barrier(&self, parties: usize, skip_agent: &str) {
        *self.barrier.lock().await = Some(Arc::new(Barrier::new(parties)));
        *self.barrier_skip.write().unwrap() = Some(skip_agent.to_string());
    }

    fn recordings(&self) -> Vec<RecordedTask> {
        self.recordings.read().unwrap().clone()
    }

    fn sessions(&self) -> Vec<(Uuid, String)> {
        self.sessions
            .read()
            .unwrap()
            .iter()
            .map(|((ctx, agent), _)| (*ctx, agent.clone()))
            .collect()
    }

    fn active(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }

    fn max_active(&self) -> usize {
        self.max_active.load(Ordering::Relaxed)
    }

    fn busy_hits(&self) -> usize {
        self.busy_hits.load(Ordering::Relaxed)
    }

    fn resolve_or_create_session(&self, context_id: Uuid, agent_id: &str) -> Uuid {
        let key = (context_id, agent_id.to_string());
        if let Some(session_id) = self.sessions.read().unwrap().get(&key) {
            return *session_id;
        }
        let session_id = Uuid::new_v4();
        self.sessions.write().unwrap().insert(key, session_id);
        session_id
    }

    async fn spawn(
        &self,
        agent_id: &str,
        prompt: &str,
        context_id: Uuid,
        agent_session_id: Uuid,
    ) -> Result<A2ARun, A2ABackendError> {
        // Enforce one active task per session.
        {
            let busy = self.session_busy.read().unwrap();
            if busy.get(&agent_session_id).is_some() {
                self.busy_hits.fetch_add(1, Ordering::Relaxed);
                return Err(A2ABackendError::SessionBusy);
            }
        }
        let task_id = Uuid::new_v4();
        self.session_busy
            .write()
            .unwrap()
            .insert(agent_session_id, task_id);
        self.recordings.write().unwrap().push(RecordedTask {
            task_id,
            context_id,
            agent_session_id: Some(agent_session_id),
            agent_id: agent_id.to_string(),
            prompt: prompt.to_string(),
        });

        // Measure concurrency: mark active before parking on the barrier.
        let cur = self.active.fetch_add(1, Ordering::Relaxed) + 1;
        self.max_active.fetch_max(cur, Ordering::Relaxed);

        let script = self
            .scripts
            .write()
            .unwrap()
            .get_mut(agent_id)
            .and_then(|queue| queue.pop_front());

        let (tx, _) = tokio::sync::broadcast::channel(256);
        let (run_tx, run_rx) = mpsc::channel(256);
        let live = Arc::new(LiveTask {
            tx,
            run_tx: run_tx.clone(),
            state: StdRwLock::new(Some(TaskStatus::Submitted)),
            artifacts: StdRwLock::new(Vec::new()),
        });
        self.tasks.write().unwrap().insert(task_id, live.clone());
        let _ = run_tx
            .send(A2AStreamEvent::TaskInfo {
                task_id,
                context_id,
                agent_session_id: Some(agent_session_id),
                agent_id: agent_id.to_string(),
            })
            .await;

        // Optional barrier: prove the expected nodes are all active at once.
        // Cleared after tripping so later nodes (implementation/review) do not
        // re-enter it and deadlock.
        let skip = self.barrier_skip.read().unwrap().clone();
        let barrier = self.barrier.lock().await.clone();
        if let Some(barrier) = barrier
            && skip.as_deref() != Some(agent_id)
        {
            let _ = barrier.wait().await;
            *self.barrier.lock().await = None;
        }

        if let Some(script) = script
            && !script.is_empty()
        {
            let step = self.step;
            let seq = Arc::new(AtomicU64::new(1));
            let live_for_task = live.clone();
            let busy = self.session_busy.clone();
            let active = self.active.clone();
            let ses = agent_session_id;
            tokio::spawn(async move {
                let _ = live_for_task.tx.send((
                    seq.fetch_add(1, Ordering::Relaxed),
                    A2AStreamEvent::Agent(AgentEvent::Started),
                ));
                for event in script.iter() {
                    tokio::time::sleep(step).await;
                    let _ = run_tx.send(A2AStreamEvent::Agent(event.clone())).await;
                    let _ = live_for_task.tx.send((
                        seq.fetch_add(1, Ordering::Relaxed),
                        A2AStreamEvent::Agent(event.clone()),
                    ));
                    if let AgentEvent::ArtifactUpdated(artifact) = event {
                        live_for_task
                            .artifacts
                            .write()
                            .unwrap()
                            .push(artifact.clone());
                    }
                    if matches!(event, AgentEvent::Completed | AgentEvent::Failed(_)) {
                        let state = match event {
                            AgentEvent::Completed => TaskStatus::Completed,
                            _ => TaskStatus::Failed,
                        };
                        *live_for_task.state.write().unwrap() = Some(state);
                        busy.write().unwrap().remove(&ses);
                        active.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            });
        } else {
            // Empty script: task stays live (used for cancel tests).
            let live_for_task = live.clone();
            let busy = self.session_busy.clone();
            let active = self.active.clone();
            let ses = agent_session_id;
            tokio::spawn(async move {
                let _ = live_for_task
                    .tx
                    .send((1u64, A2AStreamEvent::Agent(AgentEvent::Started)));
                // Remains active until cancelled.
                let _ = busy;
                let _ = active;
                let _ = ses;
            });
        }
        Ok(A2ARun {
            task_id,
            context_id,
            agent_session_id: Some(agent_session_id),
            agent_id: agent_id.to_string(),
            events: run_rx,
        })
    }
}

#[async_trait]
impl A2ABackend for ConcurrencyBackend {
    async fn start(
        &self,
        agent_id: &str,
        prompt: &str,
        _workspace: Option<std::path::PathBuf>,
    ) -> Result<A2ARun, A2ABackendError> {
        let context_id = Uuid::new_v4();
        let session_id = self.resolve_or_create_session(context_id, agent_id);
        self.spawn(agent_id, prompt, context_id, session_id).await
    }

    async fn start_in_context(
        &self,
        context_id: Uuid,
        agent_id: &str,
        prompt: &str,
    ) -> Result<A2ARun, A2ABackendError> {
        let session_id = self.resolve_or_create_session(context_id, agent_id);
        self.spawn(agent_id, prompt, context_id, session_id).await
    }

    async fn get_task(
        &self,
        task_id: Uuid,
    ) -> Result<Option<(AgentTask, Vec<Artifact>)>, A2ABackendError> {
        let Some(live) = self.tasks.read().unwrap().get(&task_id).cloned() else {
            return Ok(None);
        };
        let recording = self
            .recordings
            .read()
            .unwrap()
            .iter()
            .find(|r| r.task_id == task_id)
            .cloned();
        let state = live.state.read().unwrap().unwrap_or(TaskStatus::Failed);
        let mut task = AgentTask::new(
            recording
                .as_ref()
                .map(|r| r.agent_id.clone())
                .unwrap_or_else(|| "mock".to_string()),
            AgentMessage::user(
                recording
                    .as_ref()
                    .map(|r| r.prompt.clone())
                    .unwrap_or_default(),
            ),
        );
        task.id = task_id;
        task.status = state;
        if let Some(recording) = &recording {
            task.context_id = recording.context_id;
        }
        Ok(Some((task, live.artifacts.read().unwrap().clone())))
    }

    async fn list_tasks(
        &self,
        _context_id: Option<Uuid>,
        _status: Option<TaskStatus>,
        _limit: usize,
    ) -> Result<Vec<(AgentTask, Vec<Artifact>)>, A2ABackendError> {
        Ok(Vec::new())
    }

    async fn cancel(&self, task_id: Uuid) -> Result<(), A2ABackendError> {
        let live = self
            .tasks
            .read()
            .unwrap()
            .get(&task_id)
            .cloned()
            .ok_or(A2ABackendError::TaskNotFound(task_id))?;
        if live
            .state
            .read()
            .unwrap()
            .map(|s| s.is_terminal())
            .unwrap_or(true)
        {
            return Ok(());
        }
        *live.state.write().unwrap() = Some(TaskStatus::Cancelled);
        // Release the session + active count so downstream assertions see the
        // task actually stopped (mirrors the daemon releasing the lease).
        let session_id = self
            .recordings
            .read()
            .unwrap()
            .iter()
            .find(|r| r.task_id == task_id)
            .and_then(|r| r.agent_session_id);
        if let Some(session_id) = session_id {
            self.session_busy.write().unwrap().remove(&session_id);
        }
        self.active.fetch_sub(1, Ordering::Relaxed);
        let event = A2AStreamEvent::Agent(AgentEvent::StatusChanged(TaskStatus::Cancelled));
        let _ = live.tx.send((0, event.clone()));
        let _ = live.run_tx.send(event).await;
        Ok(())
    }

    async fn subscribe(
        &self,
        task_id: Uuid,
        _after: u64,
    ) -> Result<Pin<Box<dyn Stream<Item = A2AStreamEvent> + Send>>, A2ABackendError> {
        let live = self
            .tasks
            .read()
            .unwrap()
            .get(&task_id)
            .cloned()
            .ok_or(A2ABackendError::TaskNotLive)?;
        let mut receiver = live.tx.subscribe();
        let stream = async_stream::stream! {
            loop {
                match receiver.recv().await {
                    Ok((_seq, event)) => yield event,
                    Err(_) => return,
                }
            }
        };
        Ok(Box::pin(stream))
    }
}

// ---------- test harness ----------

async fn start_agent(
    backend: ConcurrencyBackend,
    agent_id: &str,
    skills: &[&str],
) -> (String, String, String) {
    let token = "dag-test-token".to_string();
    let descriptor = AgentDescriptor {
        id: agent_id.to_string(),
        name: format!("Mock {agent_id}"),
        description: None,
        skills: skills.iter().map(|s| AgentSkill::new(*s, None)).collect(),
        endpoint: format!("agent://{agent_id}"),
        workspace_requirement: WorkspaceRequirement::None,
    };
    let config = Arc::new(A2AServerConfig::new(
        agent_id.to_string(),
        descriptor,
        token.clone(),
        Arc::new(backend),
    ));
    let (addr, router, listener) = agentmesh_a2a::server::bind(config.clone())
        .await
        .expect("bind");
    config.set_url(format!("http://{addr}/")).await;
    tokio::spawn(agentmesh_a2a::server::serve(listener, router));
    (
        format!("http://{addr}/"),
        format!("http://{addr}/.well-known/agent-card.json"),
        token,
    )
}

struct Env {
    backend: ConcurrencyBackend,
    directory: AgentDirectory,
}

/// alpha=architecture+code, beta=code, gamma=review+testing
async fn env_with() -> Env {
    let backend = ConcurrencyBackend::new();
    let mut discovered = Vec::new();
    for (id, skills) in [
        ("alpha", &["architecture", "code"][..]),
        ("beta", &["code"][..]),
        ("gamma", &["review", "testing"][..]),
    ] {
        let (url, card_url, _token) = start_agent(backend.clone(), id, skills).await;
        discovered.push(DiscoveredEndpoint {
            agent_id: id.to_string(),
            url,
            card_url,
        });
    }
    let mut directory = AgentDirectory::new();
    directory
        .refresh(
            &discovered,
            &AgentAuth {
                token: Some("dag-test-token".into()),
            },
        )
        .await
        .expect("refresh");
    Env { backend, directory }
}

fn router_for(config: agentmesh_core::RoutingConfig) -> RuleRouter {
    RuleRouter::new(config)
}

fn parallel_review_routing() -> agentmesh_core::RoutingConfig {
    agentmesh_core::RoutingConfig {
        architecture: vec!["alpha".into()],
        implementation: vec!["beta".into()],
        review: vec!["gamma".into()],
        testing: vec!["gamma".into()],
        ..agentmesh_core::RoutingConfig::default()
    }
}

fn plan_script() -> Vec<AgentEvent> {
    vec![
        AgentEvent::Message("architecture: split auth".into()),
        AgentEvent::Completed,
    ]
}

fn analysis_script(summary: &str) -> Vec<AgentEvent> {
    vec![
        AgentEvent::Message(summary.to_string()),
        AgentEvent::Completed,
    ]
}

/// A reviewer node must end with a machine-parseable review.json.
fn review_script(verdict: &str, summary: &str) -> Vec<AgentEvent> {
    vec![
        AgentEvent::Message(format!("review: {summary}")),
        AgentEvent::ArtifactUpdated(review_artifact(verdict, summary, serde_json::json!([]))),
        AgentEvent::Completed,
    ]
}

fn implement_script() -> Vec<AgentEvent> {
    vec![
        AgentEvent::Message("implemented the plan".into()),
        AgentEvent::Completed,
    ]
}

async fn start_parallel_review(env: &Env, max_parallel: usize) -> Arc<DagRun> {
    let engine = WorkflowEngine::new(env.directory.clone(), router_for(parallel_review_routing()));
    engine
        .start_dag(
            PRESET_PARALLEL_REVIEW,
            "Refactor the authentication subsystem",
            WorkflowOptions {
                max_review_rounds: 0,
                max_parallel,
            },
        )
        .expect("preset")
}

/// Run a DAG to completion under a timeout (a hung barrier → test failure).
async fn complete(run: Arc<DagRun>) -> WorkflowResult {
    let observer: Arc<dyn WorkflowObserver> = Arc::new(NoopObserver);
    tokio::time::timeout(Duration::from_secs(15), run.run_to_completion(observer))
        .await
        .expect("run timed out (parallel nodes did not overlap?)")
}

// ---------- tests ----------

#[tokio::test]
async fn parallel_review_preset_completes_all_nodes() {
    let env = env_with().await;
    env.backend.push_script("alpha", plan_script());
    env.backend
        .push_script("gamma", review_script("approved", "security: ok"));
    env.backend
        .push_script("beta", analysis_script("tests: planned"));
    env.backend.push_script("beta", implement_script());
    env.backend
        .push_script("gamma", review_script("approved", "approved"));
    let run = start_parallel_review(&env, 2).await;

    let result = complete(run).await;
    assert_eq!(result.status, WorkflowStatus::Completed);
    assert_eq!(result.steps.len(), 5);
    let recorded = env.backend.recordings();
    let agents: Vec<_> = recorded.iter().map(|r| r.agent_id.as_str()).collect();
    assert_eq!(agents[0], "alpha");
    assert_eq!(agents[3], "beta");
    assert_eq!(agents[4], "gamma");
    let mut middle = vec![agents[1], agents[2]];
    middle.sort();
    assert_eq!(middle, vec!["beta", "gamma"]);
}

#[tokio::test]
async fn fan_out_runs_in_parallel() {
    let env = env_with().await;
    env.backend.push_script("alpha", plan_script());
    env.backend
        .push_script("gamma", review_script("approved", "security: ok"));
    env.backend
        .push_script("beta", analysis_script("tests: planned"));
    env.backend.push_script("beta", implement_script());
    env.backend
        .push_script("gamma", review_script("approved", "approved"));
    // Barrier of 2 for the fan-out node pair. If the scheduler ran them one at
    // a time the barrier would never trip and the run would time out. The
    // root (alpha) is skipped so the parallel pair is the first to meet.
    env.backend.set_barrier(2, "alpha").await;
    let run = start_parallel_review(&env, 2).await;

    let result = complete(run).await;
    assert_eq!(result.status, WorkflowStatus::Completed);
    // The two parallel nodes were both active at once.
    assert!(env.backend.max_active() >= 2, "expected overlap");
}

#[tokio::test]
async fn max_parallel_1_serializes_parallel_nodes() {
    let env = env_with().await;
    env.backend.push_script("alpha", plan_script());
    env.backend
        .push_script("gamma", review_script("approved", "security: ok"));
    env.backend
        .push_script("beta", analysis_script("tests: planned"));
    env.backend.push_script("beta", implement_script());
    env.backend
        .push_script("gamma", review_script("approved", "approved"));
    let run = start_parallel_review(&env, 1).await;

    let result = complete(run).await;

    assert_eq!(result.status, WorkflowStatus::Completed);
    // max_parallel=1 → never more than one node executing concurrently.
    assert!(env.backend.max_active() <= 1, "expected full serialization");
}

#[tokio::test]
async fn max_parallel_clamped_to_hard_cap() {
    let env = env_with().await;
    let engine = WorkflowEngine::new(env.directory.clone(), router_for(parallel_review_routing()));
    let run = engine
        .start_dag(
            PRESET_PARALLEL_REVIEW,
            "goal",
            WorkflowOptions {
                max_review_rounds: 0,
                max_parallel: 99,
            },
        )
        .unwrap();
    assert_eq!(run.max_parallel(), 8, "hard cap is 8");
}

#[tokio::test]
async fn fan_in_waits_for_all_dependencies_in_order() {
    let env = env_with().await;
    env.backend.push_script("alpha", plan_script());
    env.backend
        .push_script("gamma", review_script("approved", "security review done"));
    env.backend
        .push_script("beta", analysis_script("test planning done"));
    env.backend.push_script("beta", implement_script());
    env.backend
        .push_script("gamma", review_script("approved", "approved"));
    let run = start_parallel_review(&env, 2).await;
    let result = complete(run.clone()).await;
    assert_eq!(result.status, WorkflowStatus::Completed);

    // The implementation node (index 3) received BOTH dependency outputs,
    // ordered by dependency node id.
    let recordings = env.backend.recordings();
    let implementer = &recordings[3];
    assert!(implementer.prompt.contains("Dependency Results"));
    assert!(implementer.prompt.contains("security_review"));
    assert!(implementer.prompt.contains("security review done"));
    assert!(implementer.prompt.contains("test_planning"));
    assert!(implementer.prompt.contains("test planning done"));
    // Deterministic ordering: security_review before test_planning.
    let sec_pos = implementer.prompt.find("security_review").unwrap();
    let test_pos = implementer.prompt.find("test_planning").unwrap();
    assert!(
        sec_pos < test_pos,
        "fan-in must be deterministically ordered"
    );
    // The full chat history is NOT concatenated (only bounded summaries).
    assert!(
        !implementer
            .prompt
            .contains("SYSTEM WORKFLOW INSTRUCTION\nUNTRUSTED")
    );
}

#[tokio::test]
async fn same_session_nodes_are_serialized_by_busy_retry() {
    // Three-node custom graph: a → b, a → c; b and c are both Implementer so
    // routing (implementation → beta) sends them to the SAME agent session.
    let graph = WorkflowGraph::new(vec![
        WorkflowNode::new("a", WorkflowRole::Architect),
        WorkflowNode::with_dependencies("b", WorkflowRole::Implementer, vec!["a".to_string()]),
        WorkflowNode::with_dependencies("c", WorkflowRole::Implementer, vec!["a".to_string()]),
    ])
    .expect("acyclic");

    let env = env_with().await;
    env.backend.push_script("alpha", plan_script());
    env.backend.push_script(
        "beta",
        vec![
            AgentEvent::Message("b started".into()),
            AgentEvent::Message("b working".into()),
            AgentEvent::Message("b done".into()),
            AgentEvent::Completed,
        ],
    );
    env.backend.push_script("beta", analysis_script("c done"));
    let engine = WorkflowEngine::new(env.directory.clone(), router_for(parallel_review_routing()));
    let run = engine
        .start_dag_with_graph(
            "custom",
            "goal",
            graph,
            WorkflowOptions {
                max_review_rounds: 0,
                max_parallel: 2,
            },
            Uuid::new_v4(),
        )
        .expect("start");
    let result = complete(run.clone()).await;
    assert_eq!(result.status, WorkflowStatus::Completed);

    // The second same-session node hit SessionBusy and waited (did not fail).
    assert!(env.backend.busy_hits() >= 1, "expected a session-busy wait");
    // max_parallel=2 would allow overlap, but the session forbids it.
    assert_eq!(
        env.backend.max_active(),
        1,
        "same session never runs in parallel"
    );
    assert_eq!(
        env.backend.sessions().len(),
        2,
        "one session for alpha + one for beta"
    );
    assert_eq!(env.backend.recordings().len(), 3);
}

#[tokio::test]
async fn node_failure_fail_fast_cancels_siblings_and_skips_downstream() {
    let env = env_with().await;
    env.backend.push_script("alpha", plan_script());
    env.backend.push_script(
        "gamma",
        vec![AgentEvent::Failed("security review failed".into())],
    );
    // test_planning (beta) would normally run in parallel — start it but keep
    // it live so the failure must cancel it. A barrier makes both parallel
    // nodes start together before the security review fails.
    env.backend.set_barrier(2, "alpha").await;
    env.backend.push_script("beta", Vec::new());
    env.backend.push_script("beta", Vec::new()); // implementation (must skip)
    env.backend.push_script("gamma", Vec::new()); // review (must skip)
    let run = start_parallel_review(&env, 2).await;
    let result = complete(run.clone()).await;
    assert_eq!(result.status, WorkflowStatus::Failed);

    let statuses = run.node_statuses();
    let by_id: HashMap<&str, NodeStatus> =
        statuses.iter().map(|(id, s)| (id.as_str(), *s)).collect();
    assert_eq!(by_id.get("architecture"), Some(&NodeStatus::Completed));
    assert_eq!(by_id.get("security_review"), Some(&NodeStatus::Failed));
    // test_planning was running when the failure landed — cancelled.
    assert_eq!(by_id.get("test_planning"), Some(&NodeStatus::Cancelled));
    // Downstream nodes never started.
    assert_eq!(by_id.get("implementation"), Some(&NodeStatus::Skipped));
    assert_eq!(by_id.get("review"), Some(&NodeStatus::Skipped));
    // Only 3 tasks contacted an agent (architecture, security_review,
    // test_planning); implementation and review never spawned.
    assert_eq!(env.backend.recordings().len(), 3);
    assert!(
        env.backend.active() == 0,
        "all live tasks must be cancelled"
    );
}

#[tokio::test]
async fn workflow_cancel_cancels_all_active_tasks_and_skips_rest() {
    let env = env_with().await;
    env.backend.push_script("alpha", plan_script());
    // Both parallel nodes stay live so cancel must pull them all.
    env.backend.push_script("gamma", Vec::new());
    env.backend.push_script("beta", Vec::new());
    env.backend.push_script("beta", Vec::new());
    env.backend.push_script("gamma", Vec::new());
    let run = start_parallel_review(&env, 2).await;
    let observer: Arc<dyn WorkflowObserver> = Arc::new(NoopObserver);

    let run_for_task = run.clone();
    let result_handle = tokio::spawn(async move { run_for_task.run_to_completion(observer).await });

    // Wait until the two parallel nodes are active AND the root has finished
    // (recordings = architecture + security_review + test_planning).
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while (env.backend.recordings().len() < 3 || env.backend.active() < 2)
        && std::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        env.backend.recordings().len() >= 3 && env.backend.active() == 2,
        "parallel nodes never became active"
    );

    run.cancel().await;
    let result = result_handle.await.expect("run task");
    assert_eq!(result.status, WorkflowStatus::Cancelled);

    let statuses = run.node_statuses();
    let by_id: HashMap<&str, NodeStatus> =
        statuses.iter().map(|(id, s)| (id.as_str(), *s)).collect();
    assert_eq!(by_id.get("architecture"), Some(&NodeStatus::Completed));
    // Both running nodes were cancelled; downstream skipped.
    assert_eq!(by_id.get("security_review"), Some(&NodeStatus::Cancelled));
    assert_eq!(by_id.get("test_planning"), Some(&NodeStatus::Cancelled));
    assert_eq!(by_id.get("implementation"), Some(&NodeStatus::Skipped));
    assert_eq!(by_id.get("review"), Some(&NodeStatus::Skipped));
    assert_eq!(env.backend.active(), 0, "cancel must kill every live task");
}

#[tokio::test]
async fn review_verdict_is_recorded() {
    let env = env_with().await;
    env.backend.push_script("alpha", plan_script());
    let mut review = review_artifact("approved", "all good", serde_json::json!([]));
    review.name = "review.json".into();
    env.backend.push_script(
        "gamma",
        vec![
            AgentEvent::Message("review ok".into()),
            AgentEvent::ArtifactUpdated(review),
            AgentEvent::Completed,
        ],
    );
    env.backend
        .push_script("beta", analysis_script("tests: planned"));
    env.backend.push_script("beta", implement_script());
    let mut final_review = review_artifact("approved", "approved", serde_json::json!([]));
    final_review.name = "review.json".into();
    env.backend.push_script(
        "gamma",
        vec![
            AgentEvent::Message("final ok".into()),
            AgentEvent::ArtifactUpdated(final_review),
            AgentEvent::Completed,
        ],
    );
    let run = start_parallel_review(&env, 2).await;
    let result = complete(run.clone()).await;
    assert_eq!(result.status, WorkflowStatus::Completed);
    assert_eq!(
        result.final_review_verdict,
        Some(agentmesh_orchestrator::ReviewVerdict::Approved)
    );
}
