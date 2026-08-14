export interface DoctorStatus {
  git_available: boolean;
  git_version: string;
  sqlite_connected: boolean;
  migrations_count: number;
  daemon_running: boolean;
  daemon_instance_id?: string;
  agents: AgentHealthItem[];
  repo_root?: string;
}

export interface AgentHealthItem {
  id: string;
  name: string;
  command?: string;
  available: boolean;
  version?: string;
}

export interface TaskItem {
  id: string;
  context_id: string;
  agent_id: string;
  status: string;
  prompt: string;
  error?: string;
  created_at: string;
  completed_at?: string;
  artifacts_count: number;
}

export interface ArtifactItem {
  name: string;
  kind: string;
  size_bytes: number;
  content?: string;
}

export interface TaskDetail {
  id: string;
  context_id: string;
  agent_id: string;
  status: string;
  prompt: string;
  error?: string;
  workspace?: string;
  created_at: string;
  started_at?: string;
  completed_at?: string;
  artifacts: ArtifactItem[];
}

export interface WorkflowItem {
  id: string;
  name: string;
  status: string;
  goal: string;
  graph_nodes_count: number;
  created_at: string;
  completed_at?: string;
}

export interface WorkflowStepItem {
  id: string;
  node_id: string;
  agent_id: string;
  status: string;
  intent: string;
  error?: string;
}

export interface WorkflowDetail {
  id: string;
  name: string;
  status: string;
  goal: string;
  created_at: string;
  completed_at?: string;
  steps: WorkflowStepItem[];
}

export interface ProvenanceEventItem {
  sequence: number;
  event_type: string;
  agent_id?: string;
  payload_hash: string;
  event_hash: string;
  created_at: string;
}

export interface ProvenanceAuditReport {
  workflow_id: string;
  valid_chain: boolean;
  total_events: number;
  events: ProvenanceEventItem[];
}

export interface ApplyResult {
  success: boolean;
  dry_run: boolean;
  message: string;
  files_changed: string[];
}
