# AgentMesh

> **面向异构 AI 编程 Agent 的 A2A 运行时与编排调度系统**

[中文](README.md) | [English](README_en.md)

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust: 1.80+](https://img.shields.io/badge/Rust-1.80%2B-orange.svg)](https://www.rust-lang.org)
[![Status: 1.0 Release Candidate](https://img.shields.io/badge/Status-1.0%20RC-brightgreen.svg)]()

AgentMesh 将多样化的 AI 编程智能体（**Claude Code**、**Codex**、**OpenCode**、**Antigravity** 以及 **Mock**）统一接入标准化 A2A 运行时。它提供隔离的 Git worktree 执行环境、持久化并行 DAG 调度、多 Evaluator 共识评审修复循环、Best-of-N 盲评竞技机制，以及具备可验证确定性决策重放的 SHA-256 溯源账本。

---

## 为什么选择 AgentMesh？

| 核心能力 | 单独使用 Agent CLI | AgentMesh 1.0 |
| :--- | :--- | :--- |
| **执行安全性** | 直接原地修改当前工作区代码 | **独立 Git Worktree** 执行；绝对不污染主工作树 |
| **多 Agent 协同** | 终端之间手动复制粘贴上下文 | **持久化 DAG 工作流**；支持并行调度与策略预算控制 |
| **代码评审与质量** | 盲目信任单一模型的单次输出 | **多 Evaluator 独立盲评审**与自动修复循环（最多 3 轮） |
| **方案探索 / 竞技** | 仅能串行试验单一方案 | **Best-of-N 并行盲评竞技**与确定性 `SelectionGate` 仲裁 |
| **变更应用控制** | 无版本管控的散乱改动 | **两阶段安全应用**（`--check` 预览，`--yes` 原子合入） |
| **审计与合规** | 仅有易丢失的终端滚动日志 | **不可篡改的 SHA-256 溯源账本**与确定性决策重放 |
| **隐私与凭据安全** | 凭据可能残留于执行日志 | **零密钥持久化**；严格脱敏推理细节与敏感凭据 |

---

## 10 分钟快速上手

### 1. 构建与安装

```bash
git clone https://github.com/yhan-sun/AgentMesh.git
cd AgentMesh
cargo build --release
cp target/release/agentmesh ~/.cargo/bin/
```

### 2. 初始化与环境诊断

在你的 Git 代码仓库根目录下执行：

```bash
# 1. 初始化项目配置 (.agentmesh/config.toml)
agentmesh init

# 2. 诊断守护进程、SQLite 数据库、Git Worktree 支持及各 Agent 状态
agentmesh doctor
```

诊断输出示例：

```text
AgentMesh Doctor

Runtime
  ✓ Git (git version 2.45.0)
  ✓ SQLite (database connected, 16 migrations applied)
  ✓ Daemon (stopped, auto-starts on demand)

Agents
  ✓ Claude Code    (claude 1.2.0, ready)
  ✓ Codex          (codex 0.8.4, ready)
  ✓ OpenCode       (opencode 0.5.1, ready)
  ✓ Antigravity    (agy 2.1.0, ready)

Workspace
  ✓ Repository (/path/to/my-repo)
  ✓ Clean source (HEAD at a1b2c3d)
  ✓ Configuration (.agentmesh/config.toml)

Result:
  4 agents ready, 0 warning(s)
```

---

## 3 个核心演示

### 演示 1：单 Agent 任务与独立 Worktree 安全应用

在完全隔离的 Git Worktree 中运行 Agent，零风险修改代码：

```bash
# 在隔离 worktree 中调度 Claude Code 执行开发任务
agentmesh run claude "为 src/client.rs 添加指数退避重试机制"

# 查看该任务生成的 Git diff
agentmesh diff <TASK_ID>

# 演练预检（Dry-run 检查补丁是否能干净应用）
agentmesh apply <TASK_ID> --check

# 安全合入主工作目录
agentmesh apply <TASK_ID> --yes
```

### 演示 2：并行 DAG 工作流与多评审共识修复循环

在持久化有向无环图中协同多个专业化 Agent：

```bash
# 启动标准 plan-build-review 工作流
agentmesh workflow --preset full "重构用户认证模块以支持 OAuth2 PKCE 流程"
```

1. **架构设计阶段**：Claude 负责设计接口规范与认证契约。
2. **并行实现阶段**：Codex 与 OpenCode 分别在专属 Worktree 中并发实现服务端与客户端逻辑。
3. **共识评审阶段**：Antigravity 与 Claude 独立审查代码变更与潜在缺陷。
4. **自动修复循环**：若评审发现严重问题，AgentMesh 自动将反馈派发给对应 Agent 进行定向修复（最多 3 轮）。
5. **安全应用**：检视整体补丁后一键合并。

```bash
# 查看工作流执行图状态
agentmesh workflow show <WORKFLOW_ID>

# 接入实时事件流
agentmesh workflow attach <WORKFLOW_ID>
```

### 演示 3：Best-of-N 盲评竞技与确定性决策门控

让多个候选实现并发竞争，基于无偏见盲评选出最优解：

```bash
# 启动 Best-of-N 算法竞赛工作流
agentmesh workflow --preset best-of-n "将 JSON 解析器的吞吐量提升 2 倍"
```

1. **并行实现**：候选 A（Claude）与候选 B（Codex）在独立 Session Lane（`lane_candidate_a`、`lane_candidate_b`）中互不干扰地编码。
2. **盲评打分**：独立 Evaluator（OpenCode 和 Antigravity）在匿名的前提下对补丁进行正确性与性能审查。
3. **确定性 SelectionGate**：基于多数共识准则与缺陷数最少原则，纯函数计算确定胜出方案。
4. **仅胜出者安全应用**：仅允许合并胜出者分支，未入选分支自动归档保留追溯。
5. **审计与决策重放**：

```bash
# 基于溯源账本离线重放决策判定过程（不调用 LLM，不重复执行代码）
agentmesh workflow replay <WORKFLOW_ID> --verify

# 导出防篡改审计报告（JSON / NDJSON 格式）
agentmesh workflow export <WORKFLOW_ID> --output audit.json
```

---

## CLI 命令参考

### 基础任务命令

| 命令 | 说明 |
| :--- | :--- |
| `agentmesh init [--force]` | 在当前目录初始化 `.agentmesh/config.toml` |
| `agentmesh doctor [--json]` | 全面诊断系统运行环境、数据库、Git 和 Agent 状态 |
| `agentmesh config validate [--json]` | 校验配置文件的语法与语义策略边界约束 |
| `agentmesh agents [--json]` | 列出所有注册的 Agent 及其在线状态与技能 |
| `agentmesh run <agent> <prompt>` | 针对指定 Agent 派发单次运行任务 |
| `agentmesh tasks [--status <s>] [--limit <n>]` | 分页列出历史任务记录 |
| `agentmesh task <task_id> [--json]` | 查看任务详细信息、会话及产物 |
| `agentmesh diff <task_id>` | 查看任务在独立 Worktree 中生成的完整 Git Diff |
| `agentmesh apply <task_id> [--check] [--yes]` | 安全演练检查或将变更合入当前工作区 |
| `agentmesh cancel <task_id>` | 取消正在执行的任务并清理子进程树 |
| `agentmesh resume <task_id> <prompt>` | 恢复已中断或继续现有的任务会话 |

### 工作流与 DAG 命令

| 命令 | 说明 |
| :--- | :--- |
| `agentmesh workflow "goal" [--preset <p>]` | 启动 DAG 工作流（`standard`, `full`, `quick`, `best-of-n`） |
| `agentmesh workflows [--json]` | 列出所有工作流运行实例 |
| `agentmesh workflow show <id> [--json]` | 展示工作流节点拓扑、依赖关系与执行状态 |
| `agentmesh workflow attach <id>` | 接入正在运行的工作流实时 SSE 事件流 |
| `agentmesh workflow cancel <id>` | 优雅取消工作流中所有活跃节点 |
| `agentmesh workflow resume <id>` | 崩溃或异常中断后从断点处恢复工作流 |
| `agentmesh workflow replan <id> "reason"` | 触发 Planner Agent 进行运行时 DAG 增量重规划 |
| `agentmesh workflow recover <id>` | 为失败步骤自动生成恢复子工作流 |
| `agentmesh workflow audit <id> [--ndjson]` | 查看工作流不可篡改的按时间序列决策日志 |
| `agentmesh workflow replay <id> [--verify]` | 纯离线确定性重放决策逻辑并校验哈希链完整性 |
| `agentmesh workflow export <id> -o file` | 导出包含溯源链的结构化审计账本 |

### 稳定进程退出码（Exit Codes）

AgentMesh 遵循标准且可预测的进程退出码契约：

| 退出码 | 状态标识 | 含义与处置建议 |
| :---: | :--- | :--- |
| `0` | **Success** | 命令执行成功完成 |
| `2` | **InvalidArgs / Config** | 命令行参数缺失、语法错误或 `.agentmesh/config.toml` 超出合法范围 |
| `3` | **AgentUnavailable** | 请求的 Agent 二进制可执行文件未在 `PATH` 中找到或处于离线状态 |
| `4` | **Task / Workflow Failed** | 任务执行失败、单元测试未通过或无候选者通过盲评 |
| `5` | **Cancelled** | 操作被用户主动取消或信号终止 |
| `6` | **PolicyViolation** | DAG 规模超出策略预算（如节点数超出 `max_nodes` 上限） |
| `7` | **Workspace / Git Error** | 当前工作区未提交有脏代码、合并冲突或 Worktree 创建失败 |
| `8` | **Daemon / Runtime Error**| Daemon 通信异常或 SQLite 数据库锁定 |
| `9` | **ProtocolError** | A2A 协议帧损坏或 JSON-RPC 格式不合规 |
| `10`| **IntegrityFailure** | 溯源哈希链校验失败或检测到账本被篡改 |

---

## 架构总览

```text
┌────────────────────────────────────────────────────────┐
│                      AgentMesh CLI                     │
└───────────────────────────┬────────────────────────────┘
                            │ Unix Domain Socket / HTTP (Bearer Token)
┌───────────────────────────▼────────────────────────────┐
│                    AgentMesh Daemon                    │
│   ┌────────────────────────────────────────────────┐   │
│   │             Workflow / DAG Scheduler           │   │
│   │    (Parallel Nodes, SelectionGate, Recovery)   │   │
│   └───────────────────────┬────────────────────────┘   │
│                           │ A2A Protocol (JSON-RPC)    │
│   ┌───────────────────────▼────────────────────────┐   │
│   │               Per-Agent A2A Servers            │   │
│   └───────────────────────┬────────────────────────┘   │
│                           │ Process Adapter            │
│   ┌───────────────────────▼────────────────────────┐   │
│   │  Claude Code  │  Codex  │  OpenCode  │  AGY    │   │
│   └────────────────────────────────────────────────┘   │
│                           │ Isolated Worktrees         │
│   ┌───────────────────────▼────────────────────────┐   │
│   │  .agentmesh/workspaces/<task-id> (Git Branches)│   │
│   └────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────┘
```

详细的架构不变式、状态机定义及核心实体概念（`Context != Session != Task != Workspace`），请参阅 [`docs/architecture.md`](docs/architecture.md)。

---

## 配置文件 (`.agentmesh/config.toml`)

项目级配置存储于仓库根目录下的 `.agentmesh/config.toml`：

```toml
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
```

可随时使用如下命令校验配置文件合规性：
```bash
agentmesh config validate
```

---

## 安全与 Git 规范保障

1. **零凭据持久化**：AgentMesh 绝不在 SQLite 数据库或日志文件中持久化 API Key、Token 或任何环境密钥。
2. **确定性脱敏**：在审计日志与导出账本中，自动剔除敏感推理内容与密钥字段。
3. **禁止非预期 Git 提交与推送**：绝不在未经开发者显式执行 `--yes` 确认的情况下直接修改工作区分支或执行 `git push`。
4. **独立 Worktree 隔离**：所有 Agent 代码编写与试验均在独立分支（`agentmesh/<task-id>`）与对应 Worktree 目录中发生。
5. **密码学溯源防篡改**：所有状态流转与裁决事件均以 SHA-256 形成哈希链（`payload_hash`, `previous_hash`, `event_hash`）。

---

## 开源协议

AgentMesh 基于 [MIT License](LICENSE) 开源。
