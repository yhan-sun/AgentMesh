import { useState, useEffect } from "react";
import { invoke } from "@tauri-apps/api/core";
import {
  Bot,
  GitBranch,
  GitPullRequest,
  ShieldCheck,
  Activity,
  Play,
  RefreshCw,
} from "lucide-react";
import {
  DoctorStatus,
  TaskItem,
  TaskDetail,
  WorkflowItem,
  WorkflowDetail,
  ProvenanceAuditReport,
  ApplyResult,
} from "./types";
import "./index.css";

export default function App() {
  const [activeTab, setActiveTab] = useState<
    "tasks" | "workflows" | "diff" | "provenance" | "doctor"
  >("tasks");

  // State
  const [doctor, setDoctor] = useState<DoctorStatus | null>(null);
  const [tasks, setTasks] = useState<TaskItem[]>([]);
  const [selectedTaskId, setSelectedTaskId] = useState<string | null>(null);
  const [taskDetail, setTaskDetail] = useState<TaskDetail | null>(null);
  const [taskDiff, setTaskDiff] = useState<string>("");
  const [applyResult, setApplyResult] = useState<ApplyResult | null>(null);

  // Task Run Form
  const [selectedAgent, setSelectedAgent] = useState("claude");
  const [prompt, setPrompt] = useState("");
  const [fromTaskId, setFromTaskId] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);

  // Workflows
  const [workflows, setWorkflows] = useState<WorkflowItem[]>([]);
  const [selectedWorkflowId, setSelectedWorkflowId] = useState<string | null>(null);
  const [workflowDetail, setWorkflowDetail] = useState<WorkflowDetail | null>(null);
  const [auditReport, setAuditReport] = useState<ProvenanceAuditReport | null>(null);

  // Initial load
  useEffect(() => {
    refreshDoctor();
    refreshTasks();
    refreshWorkflows();
  }, []);

  const refreshDoctor = async () => {
    try {
      const res = await invoke<DoctorStatus>("doctor_check");
      setDoctor(res);
    } catch (e) {
      console.error(e);
    }
  };

  const refreshTasks = async () => {
    try {
      const res = await invoke<TaskItem[]>("list_tasks", { limit: 50 });
      setTasks(res);
    } catch (e) {
      console.error(e);
    }
  };

  const refreshWorkflows = async () => {
    try {
      const res = await invoke<WorkflowItem[]>("list_workflows", { limit: 20 });
      setWorkflows(res);
    } catch (e) {
      console.error(e);
    }
  };

  const handleSelectTask = async (id: string) => {
    setSelectedTaskId(id);
    try {
      const detail = await invoke<TaskDetail>("get_task_details", { taskId: id });
      setTaskDetail(detail);
      const diff = await invoke<string>("get_task_diff", { taskId: id });
      setTaskDiff(diff);
      setApplyResult(null);
    } catch (e) {
      console.error(e);
    }
  };

  const handleSelectWorkflow = async (id: string) => {
    setSelectedWorkflowId(id);
    try {
      const detail = await invoke<WorkflowDetail>("get_workflow_details", {
        workflowId: id,
      });
      setWorkflowDetail(detail);
      const audit = await invoke<ProvenanceAuditReport>("get_provenance_audit", {
        workflowId: id,
      });
      setAuditReport(audit);
    } catch (e) {
      console.error(e);
    }
  };

  const handleRunTask = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!prompt.trim()) return;
    setIsSubmitting(true);
    try {
      const taskId = await invoke<string>("run_task", {
        agentId: selectedAgent,
        prompt: prompt.trim(),
        fromTaskId: fromTaskId ? fromTaskId : null,
        fromContextId: null,
      });
      setPrompt("");
      await refreshTasks();
      handleSelectTask(taskId);
    } catch (err) {
      alert("Error starting task: " + err);
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleApply = async (dryRun: boolean) => {
    if (!selectedTaskId) return;
    try {
      const res = await invoke<ApplyResult>("apply_task_changes", {
        taskId: selectedTaskId,
        dryRun,
      });
      setApplyResult(res);
    } catch (e) {
      alert("Apply failed: " + e);
    }
  };

  return (
    <div className="app-container">
      {/* Sidebar Navigation */}
      <aside className="sidebar">
        <div className="sidebar-header">
          <div className="logo-badge">M</div>
          <div className="sidebar-title">
            <h1>AgentMesh</h1>
            <span>v1.0 Desktop</span>
          </div>
        </div>

        <nav className="sidebar-nav">
          <div
            className={`nav-item ${activeTab === "tasks" ? "active" : ""}`}
            onClick={() => setActiveTab("tasks")}
          >
            <Bot size={18} />
            <span>Agents & Tasks</span>
          </div>

          <div
            className={`nav-item ${activeTab === "workflows" ? "active" : ""}`}
            onClick={() => setActiveTab("workflows")}
          >
            <GitBranch size={18} />
            <span>Workflows & DAG</span>
          </div>

          <div
            className={`nav-item ${activeTab === "diff" ? "active" : ""}`}
            onClick={() => setActiveTab("diff")}
          >
            <GitPullRequest size={18} />
            <span>Diff & Safe Apply</span>
          </div>

          <div
            className={`nav-item ${activeTab === "provenance" ? "active" : ""}`}
            onClick={() => setActiveTab("provenance")}
          >
            <ShieldCheck size={18} />
            <span>Audit & Replay</span>
          </div>

          <div
            className={`nav-item ${activeTab === "doctor" ? "active" : ""}`}
            onClick={() => setActiveTab("doctor")}
          >
            <Activity size={18} />
            <span>Doctor & Health</span>
          </div>
        </nav>

        <div className="sidebar-footer">
          <div className="daemon-indicator">
            <div
              className={`status-dot ${
                doctor?.daemon_running ? "online" : "offline"
              }`}
            />
            <span>{doctor?.daemon_running ? "Daemon Live" : "Daemon Standby"}</span>
          </div>
          <button className="btn btn-secondary" onClick={refreshDoctor}>
            <RefreshCw size={13} />
          </button>
        </div>
      </aside>

      {/* Main Content Area */}
      <main className="main-content">
        {/* Tab 1: Agents & Tasks */}
        {activeTab === "tasks" && (
          <>
            <header className="content-header">
              <div>
                <h2>Agent Task Orchestration</h2>
                <p>Run isolated coding agents with cross-agent context handoff</p>
              </div>
              <div className="header-actions">
                <button className="btn btn-secondary" onClick={refreshTasks}>
                  <RefreshCw size={14} /> Refresh
                </button>
              </div>
            </header>

            <div className="content-body">
              {/* Task Dispatch Form */}
              <div className="card">
                <form onSubmit={handleRunTask}>
                  <div className="grid-2">
                    <div className="input-group">
                      <label className="input-label">Target Agent</label>
                      <select
                        className="select-input"
                        value={selectedAgent}
                        onChange={(e) => setSelectedAgent(e.target.value)}
                      >
                        <option value="claude">Claude Code</option>
                        <option value="codex">Codex (OpenAI)</option>
                        <option value="opencode">OpenCode</option>
                        <option value="antigravity">Antigravity (AGY)</option>
                        <option value="mock">Mock Agent</option>
                      </select>
                    </div>

                    <div className="input-group">
                      <label className="input-label">
                        Inherit Context From (Cross-Agent Handoff)
                      </label>
                      <select
                        className="select-input"
                        value={fromTaskId}
                        onChange={(e) => setFromTaskId(e.target.value)}
                      >
                        <option value="">-- Start Fresh Context --</option>
                        {tasks.map((t) => (
                          <option key={t.id} value={t.id}>
                            [{t.agent_id}] {t.prompt.substring(0, 40)}... ({t.id.substring(0, 8)})
                          </option>
                        ))}
                      </select>
                    </div>
                  </div>

                  <div className="input-group">
                    <label className="input-label">Prompt / Instruction</label>
                    <textarea
                      className="textarea-input"
                      placeholder="e.g. Implement WebSocket reconnection logic with exponential backoff..."
                      value={prompt}
                      onChange={(e) => setPrompt(e.target.value)}
                    />
                  </div>

                  <button
                    type="submit"
                    className="btn btn-primary"
                    disabled={isSubmitting || !prompt.trim()}
                  >
                    <Play size={14} /> {isSubmitting ? "Starting..." : "Run Task in Isolated Worktree"}
                  </button>
                </form>
              </div>

              {/* Tasks List and Detail */}
              <div className="grid-2">
                <div className="card">
                  <h3 style={{ marginBottom: 14, fontSize: 14 }}>Recent Tasks</h3>
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Agent</th>
                        <th>Prompt</th>
                        <th>Status</th>
                        <th>Artifacts</th>
                      </tr>
                    </thead>
                    <tbody>
                      {tasks.map((t) => (
                        <tr
                          key={t.id}
                          onClick={() => handleSelectTask(t.id)}
                          style={{
                            background:
                              selectedTaskId === t.id
                                ? "var(--bg-card-hover)"
                                : "transparent",
                          }}
                        >
                          <td>
                            <strong>{t.agent_id}</strong>
                          </td>
                          <td>{t.prompt.substring(0, 32)}...</td>
                          <td>
                            <span className={`badge badge-${t.status}`}>
                              {t.status}
                            </span>
                          </td>
                          <td>{t.artifacts_count}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                {/* Selected Task Details */}
                <div className="card">
                  <h3 style={{ marginBottom: 14, fontSize: 14 }}>Task Details</h3>
                  {taskDetail ? (
                    <div>
                      <div style={{ marginBottom: 12 }}>
                        <span className="input-label">Task ID</span>
                        <code style={{ fontSize: 12 }}>{taskDetail.id}</code>
                      </div>

                      <div style={{ marginBottom: 12 }}>
                        <span className="input-label">Prompt</span>
                        <div className="code-viewer">{taskDetail.prompt}</div>
                      </div>

                      {taskDetail.error && (
                        <div style={{ marginBottom: 12 }}>
                          <span className="input-label">Error</span>
                          <div
                            className="code-viewer"
                            style={{ color: "var(--accent-red)" }}
                          >
                            {taskDetail.error}
                          </div>
                        </div>
                      )}

                      {taskDetail.artifacts.length > 0 && (
                        <div>
                          <span className="input-label">Generated Artifacts</span>
                          {taskDetail.artifacts.map((a, i) => (
                            <div key={i} style={{ marginBottom: 8 }}>
                              <strong>
                                {a.name} ({a.kind}, {a.size_bytes}B)
                              </strong>
                              {a.content && (
                                <div className="code-viewer">{a.content}</div>
                              )}
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  ) : (
                    <p style={{ color: "var(--text-muted)", fontSize: 13 }}>
                      Select a task from the list to view full execution details and artifacts.
                    </p>
                  )}
                </div>
              </div>
            </div>
          </>
        )}

        {/* Tab 2: Workflows & DAG */}
        {activeTab === "workflows" && (
          <>
            <header className="content-header">
              <div>
                <h2>Workflows & Persistent DAG</h2>
                <p>Parallel multi-agent execution with consensus review & fix loops</p>
              </div>
              <div className="header-actions">
                <button className="btn btn-secondary" onClick={refreshWorkflows}>
                  <RefreshCw size={14} /> Refresh
                </button>
              </div>
            </header>

            <div className="content-body">
              <div className="grid-2">
                <div className="card">
                  <h3 style={{ marginBottom: 14, fontSize: 14 }}>Workflow Runs</h3>
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>Goal / Preset</th>
                        <th>Nodes</th>
                        <th>Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {workflows.map((w) => (
                        <tr
                          key={w.id}
                          onClick={() => handleSelectWorkflow(w.id)}
                          style={{
                            background:
                              selectedWorkflowId === w.id
                                ? "var(--bg-card-hover)"
                                : "transparent",
                          }}
                        >
                          <td>{w.name}</td>
                          <td>{w.graph_nodes_count}</td>
                          <td>
                            <span className={`badge badge-${w.status}`}>
                              {w.status}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>

                <div className="card">
                  <h3 style={{ marginBottom: 14, fontSize: 14 }}>DAG Execution Steps</h3>
                  {workflowDetail ? (
                    <div>
                      <p style={{ fontSize: 13, marginBottom: 14, color: "var(--text-secondary)" }}>
                        <strong>Goal:</strong> {workflowDetail.goal}
                      </p>

                      <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                        {workflowDetail.steps.map((step) => (
                          <div
                            key={step.id}
                            style={{
                              padding: 12,
                              background: "var(--bg-input)",
                              borderRadius: 8,
                              border: "1px solid var(--border-subtle)",
                            }}
                          >
                            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
                              <strong>
                                Node {step.node_id}: {step.intent}
                              </strong>
                              <span className={`badge badge-${step.status}`}>{step.status}</span>
                            </div>
                            <div style={{ fontSize: 12, color: "var(--text-muted)" }}>
                              Assigned Agent: <code>{step.agent_id}</code>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  ) : (
                    <p style={{ color: "var(--text-muted)", fontSize: 13 }}>
                      Select a workflow to visualize DAG node progression and dependencies.
                    </p>
                  )}
                </div>
              </div>
            </div>
          </>
        )}

        {/* Tab 3: Diff & Safe Apply */}
        {activeTab === "diff" && (
          <>
            <header className="content-header">
              <div>
                <h2>Git Diff & Two-Phase Safe Apply</h2>
                <p>Inspect isolated worktree changes and rollout atomically with snapshot validation</p>
              </div>
              <div className="header-actions">
                <button
                  className="btn btn-secondary"
                  onClick={() => handleApply(true)}
                  disabled={!selectedTaskId}
                >
                  Dry Run (--check)
                </button>
                <button
                  className="btn btn-success"
                  onClick={() => handleApply(false)}
                  disabled={!selectedTaskId}
                >
                  Safe Apply (--yes)
                </button>
              </div>
            </header>

            <div className="content-body">
              {applyResult && (
                <div
                  className="card"
                  style={{
                    borderColor: applyResult.success
                      ? "var(--accent-green)"
                      : "var(--accent-red)",
                  }}
                >
                  <p style={{ fontWeight: 600, fontSize: 14, marginBottom: 6 }}>
                    {applyResult.message}
                  </p>
                  {applyResult.files_changed.length > 0 && (
                    <ul style={{ paddingLeft: 20, fontSize: 12.5 }}>
                      {applyResult.files_changed.map((f, i) => (
                        <li key={i}>{f}</li>
                      ))}
                    </ul>
                  )}
                </div>
              )}

              <div className="card">
                <h3 style={{ marginBottom: 12, fontSize: 14 }}>
                  Workspace Unified Patch (Task: {selectedTaskId || "None selected"})
                </h3>
                <div className="code-viewer" style={{ maxHeight: 520, overflowY: "auto" }}>
                  {taskDiff || "(Select a task with an isolated workspace to inspect Git diff)"}
                </div>
              </div>
            </div>
          </>
        )}

        {/* Tab 4: Provenance & Audit Replay */}
        {activeTab === "provenance" && (
          <>
            <header className="content-header">
              <div>
                <h2>Immutable Provenance Ledger & Replay</h2>
                <p>Verifiable SHA-256 decision audit trail and deterministic decision replay</p>
              </div>
            </header>

            <div className="content-body">
              {auditReport ? (
                <div>
                  <div
                    className="card"
                    style={{
                      display: "flex",
                      alignItems: "center",
                      justifyContent: "space-between",
                    }}
                  >
                    <div>
                      <h3>Hash Chain Integrity Status</h3>
                      <p style={{ fontSize: 13, color: "var(--text-secondary)" }}>
                        Total Recorded Events: {auditReport.total_events}
                      </p>
                    </div>
                    <div>
                      {auditReport.valid_chain ? (
                        <span className="badge badge-completed" style={{ fontSize: 13, padding: "6px 12px" }}>
                          ✓ Cryptographic Chain Verified
                        </span>
                      ) : (
                        <span className="badge badge-failed" style={{ fontSize: 13, padding: "6px 12px" }}>
                          ⚠ Tampered Chain Detected
                        </span>
                      )}
                    </div>
                  </div>

                  <div className="card">
                    <h3 style={{ marginBottom: 14, fontSize: 14 }}>Event Sequence Ledger</h3>
                    <table className="data-table">
                      <thead>
                        <tr>
                          <th>Seq</th>
                          <th>Event Type</th>
                          <th>Actor</th>
                          <th>Event Hash</th>
                          <th>Timestamp</th>
                        </tr>
                      </thead>
                      <tbody>
                        {auditReport.events.map((e) => (
                          <tr key={e.sequence}>
                            <td>#{e.sequence}</td>
                            <td>
                              <strong>{e.event_type}</strong>
                            </td>
                            <td>{e.agent_id || "system"}</td>
                            <td>
                              <code>{e.event_hash.substring(0, 16)}...</code>
                            </td>
                            <td>{e.created_at}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              ) : (
                <div className="card">
                  <p style={{ color: "var(--text-muted)", fontSize: 13 }}>
                    Select a workflow in the "Workflows & DAG" tab to view and verify its cryptographic provenance ledger.
                  </p>
                </div>
              )}
            </div>
          </>
        )}

        {/* Tab 5: Doctor & Health */}
        {activeTab === "doctor" && (
          <>
            <header className="content-header">
              <div>
                <h2>Doctor & System Diagnostics</h2>
                <p>Verify environment, databases, background daemon, and agent availability</p>
              </div>
              <div className="header-actions">
                <button className="btn btn-secondary" onClick={refreshDoctor}>
                  <RefreshCw size={14} /> Re-run Diagnostics
                </button>
              </div>
            </header>

            <div className="content-body">
              {doctor && (
                <div>
                  <div className="grid-3" style={{ marginBottom: 20 }}>
                    <div className="card">
                      <span className="input-label">Git Subsystem</span>
                      <p style={{ fontSize: 15, fontWeight: 600 }}>{doctor.git_version}</p>
                      <span
                        className={`badge badge-${
                          doctor.git_available ? "ready" : "failed"
                        }`}
                        style={{ marginTop: 8 }}
                      >
                        {doctor.git_available ? "Available" : "Missing"}
                      </span>
                    </div>

                    <div className="card">
                      <span className="input-label">SQLite Storage</span>
                      <p style={{ fontSize: 15, fontWeight: 600 }}>
                        16 / 16 Migrations Applied
                      </p>
                      <span className="badge badge-ready" style={{ marginTop: 8 }}>
                        Connected
                      </span>
                    </div>

                    <div className="card">
                      <span className="input-label">AgentMesh Daemon</span>
                      <p style={{ fontSize: 15, fontWeight: 600 }}>
                        {doctor.daemon_running ? "Running" : "Auto-Starts on Demand"}
                      </p>
                      <span
                        className={`badge badge-${
                          doctor.daemon_running ? "running" : "pending"
                        }`}
                        style={{ marginTop: 8 }}
                      >
                        {doctor.daemon_running ? "Active" : "Standby"}
                      </span>
                    </div>
                  </div>

                  <div className="card">
                    <h3 style={{ marginBottom: 14, fontSize: 14 }}>
                      Registered Coding Agent Adapters
                    </h3>
                    <table className="data-table">
                      <thead>
                        <tr>
                          <th>Agent</th>
                          <th>Executable</th>
                          <th>Status</th>
                          <th>Detected Version</th>
                        </tr>
                      </thead>
                      <tbody>
                        {doctor.agents.map((ag) => (
                          <tr key={ag.id}>
                            <td>
                              <strong>{ag.name}</strong>
                            </td>
                            <td>
                              <code>{ag.command || ag.id}</code>
                            </td>
                            <td>
                              <span
                                className={`badge badge-${
                                  ag.available ? "ready" : "failed"
                                }`}
                              >
                                {ag.available ? "Ready" : "Offline"}
                              </span>
                            </td>
                            <td>{ag.version || "Not detected in PATH"}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          </>
        )}
      </main>
    </div>
  );
}
