---
name: agentloom-collab
description: Collaborate autonomously with remote server agents connected to your AgentLoom Hub via MCP tools. Use when a task requires inspecting, building, or deploying code across remote VPS, container, or Kubernetes nodes.
---

# AgentLoom Multi-Agent Collaboration Workflow

You have access to remote worker nodes through the `agentloom` MCP tools.

## Core Tools

1. `agentloom_list_agents` — Check which remote worker nodes are online, idle, or currently executing a task.
2. `agentloom_send` — Dispatch a prompt containing `@node_name` mentions. Keep `wait_for_reply=true` for interactive tasks or pass an existing `thread_id` to continue a conversation.
3. `agentloom_list_threads` / `agentloom_get_thread` — Inspect collaboration history or fetch delayed replies if `agentloom_send` timed out.
4. `agentloom_control` — Restart an online worker node cleanly (admin token only).

## Operating Rules

- **Verify availability first**: Call `agentloom_list_agents` before delegating work so you know exact node names and online status.
- **Keep @mentions explicit and bounded**: Every task prompt must `@node_name` the intended worker(s). Instruct workers not to `@` other nodes unless a multi-hop handoff is explicitly required.
- **Audit before deploy**: When coordinating a local-to-remote deployment, confirm local build/tests pass first, then ask the remote worker to audit its current service state before applying changes and verifying live endpoints.
- **Fact-only reporting**: Report only outputs, PIDs, URLs, and HTTP checks actually returned by the remote worker or observed locally.
