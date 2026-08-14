# AgentMesh Backlog (Post-1.0 Roadmap)

This backlog contains architectural proposals and future ideas frozen during the 1.0 stabilization phase.

---

## Future Capabilities

### 1. Dynamic DAG Mutations (Runtime Graph Manipulation)
- **Concept**: Allow human operators or supervisor agents to dynamically insert, replace, or prune DAG nodes during active execution without triggering a full Planner replan cycle.
- **Milestone**: 1.1

### 2. Multi-Tenant Cloud Daemon & Remote Worker Execution
- **Concept**: Extend the Daemon architecture to support distributed execution across remote compute workers (e.g., Docker sandboxes, Kubernetes pods, AWS Firecracker microVMs) instead of local subprocesses.
- **Milestone**: 1.2

### 3. Interactive Web UI & Rich Terminal UI (TUI)
- **Concept**: Real-time visual graph viewer, live multi-lane terminal streaming, and interactive SelectionGate inspection via a local web interface (`agentmesh ui`) or Ratatui-based TUI (`agentmesh top`).
- **Milestone**: 1.1

### 4. Telemetry, Cost & Token Attribution
- **Concept**: Granular token usage tracking, API cost accounting per agent/model/task, and OpenTelemetry-compliant trace export.
- **Milestone**: 1.1

### 5. Plugin Ecosystem & Custom Agent Protocols
- **Concept**: Dynamic WASM or gRPC plugins for proprietary enterprise LLM endpoints and external CI/CD tool integrations.
- **Milestone**: 1.3
