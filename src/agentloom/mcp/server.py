#!/usr/bin/env python3
"""Zero-dependency Python stdlib MCP Server (JSON-RPC 2.0 over stdio) for AgentLoom."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Optional

from agentloom import __version__
from agentloom.protocol import to_http_url


def _load_dotenv_file(path: str) -> None:
    if not os.path.isfile(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v
    except OSError:
        pass


def resolve_settings(
    base_url: Optional[str] = None,
    token: Optional[str] = None,
    sender: Optional[str] = None,
) -> tuple[str, str, str]:
    """Resolve URL, token, and sender with CLI > env > .env > legacy env fallback."""
    _load_dotenv_file(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
    _load_dotenv_file(os.path.join(os.getcwd(), ".env"))

    resolved_url = (
        base_url
        or os.environ.get("AGENTLOOM_URL")
        or os.environ.get("AGENT_CHAT_URL")
        or "http://127.0.0.1:3000"
    )
    resolved_token = (
        token
        or os.environ.get("AGENTLOOM_TOKEN")
        or os.environ.get("AGENT_CHAT_SECRET")
        or ""
    ).strip()
    resolved_sender = (
        sender
        or os.environ.get("AGENTLOOM_SENDER")
        or os.environ.get("AGENT_CHAT_SENDER")
        or "local-agent"
    ).strip()
    return to_http_url(resolved_url), resolved_token, resolved_sender


TOOLS = [
    {
        "name": "agentloom_list_agents",
        "description": "List all remote worker nodes accessible by the current token along with online and execution status.",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "agentloom_send",
        "description": "Send a task or message containing @node_name mentions to remote worker nodes, optionally waiting for replies.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "content": {
                    "type": "string",
                    "description": "Message body including @node_name mention(s), e.g. '@prod-node check nginx status'",
                },
                "thread_id": {
                    "type": "string",
                    "description": "Optional existing thread ID to continue the conversation.",
                },
                "wait_for_reply": {
                    "type": "boolean",
                    "description": "Wait for remote worker replies via incremental polling (default true).",
                    "default": True,
                },
                "timeout": {
                    "type": "integer",
                    "description": "Reply wait timeout in seconds (10-600, default 180).",
                    "default": 180,
                },
            },
            "required": ["content"],
        },
    },
    {
        "name": "agentloom_list_threads",
        "description": "List collaboration threads accessible by the current token.",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": [],
        },
    },
    {
        "name": "agentloom_get_thread",
        "description": "Fetch messages and metadata for a specific collaboration thread.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "thread_id": {"type": "string", "description": "Target thread ID"},
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of recent messages to fetch (default 50).",
                    "default": 50,
                },
            },
            "required": ["thread_id"],
        },
    },
    {
        "name": "agentloom_control",
        "description": "Send a control command (restart) to an online worker node. Requires an admin token.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "agent": {"type": "string", "description": "Target worker node name"},
                "action": {
                    "type": "string",
                    "enum": ["restart"],
                    "description": "Control action to execute (restart).",
                },
            },
            "required": ["agent", "action"],
        },
    },
]


class AgentLoomMCPClient:
    def __init__(self, base_url: str, token: str, sender: str):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.sender = sender

    def api_request(self, method: str, path: str, body: Optional[dict] = None, timeout: int = 30) -> dict:
        if not self.token:
            return {"error": "AGENTLOOM_TOKEN is not configured. Set AGENTLOOM_TOKEN or pass --token."}
        url = f"{self.base_url}{path}"
        data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
        headers = {
            "X-Token": self.token,
            "Accept": "application/json",
            "User-Agent": f"agentloom-mcp/{__version__}",
        }
        if data is not None:
            headers["Content-Type"] = "application/json"

        last_err: object = None
        for attempt in range(3):
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    raw = resp.read().decode("utf-8", errors="replace")
                    return json.loads(raw) if raw else {"ok": True}
            except urllib.error.HTTPError as exc:
                err_body = exc.read().decode("utf-8", errors="replace")
                if exc.code in (502, 503, 504, 520, 522, 524) and attempt < 2:
                    last_err = f"HTTP {exc.code}: {err_body[:200]}"
                    time.sleep(1.5)
                    continue
                if exc.code == 403:
                    return {"error": f"HTTP 403 Forbidden: {err_body[:300]} (admin or ACL permission required)"}
                return {"error": f"HTTP {exc.code}: {err_body[:400]}"}
            except Exception as exc:
                last_err = exc
                if attempt < 2:
                    time.sleep(1.5)
        return {"error": f"Request failed after 3 attempts: {last_err}"}

    def handle_tool_call(self, name: str, args: dict) -> dict:
        if name == "agentloom_list_agents":
            return self.api_request("GET", "/api/agents", timeout=15)

        if name == "agentloom_list_threads":
            return self.api_request("GET", "/api/threads", timeout=15)

        if name == "agentloom_get_thread":
            tid = (args.get("thread_id") or "").strip()
            limit = int(args.get("limit", 50))
            if not tid:
                return {"error": "thread_id is required"}
            return self.api_request("GET", f"/api/threads/{tid}?limit={limit}", timeout=15)

        if name == "agentloom_send":
            content = (args.get("content") or "").strip()
            if not content:
                return {"error": "content is required"}
            wait_for_reply = bool(args.get("wait_for_reply", True))
            timeout = max(10, min(int(args.get("timeout", 180)), 600))
            thread_id = args.get("thread_id") or None

            send_res = self.api_request(
                "POST",
                "/api/send",
                body={
                    "sender": self.sender,
                    "role": "mcp",
                    "content": content,
                    "thread_id": thread_id,
                    "wait_for_reply": False,
                },
                timeout=20,
            )
            if not isinstance(send_res, dict) or "error" in send_res:
                return send_res

            tid = send_res.get("thread_id")
            delivered = send_res.get("delivered") or []
            denied = send_res.get("denied_mentions") or []
            if not wait_for_reply or not tid or not delivered:
                if denied and not delivered:
                    send_res["note"] = f"Target node(s) {denied} denied by token ACL."
                return send_res

            sent_idx = int(send_res.get("message_index") or 1)
            deadline = time.time() + timeout
            while time.time() < deadline:
                time.sleep(1.5)
                t_data = self.api_request("GET", f"/api/threads/{tid}?since_index={sent_idx}", timeout=15)
                if isinstance(t_data, dict) and "messages" in t_data:
                    new_replies = [m for m in t_data["messages"] if m.get("sender") in delivered]
                    if new_replies:
                        out = {
                            "ok": True,
                            "thread_id": tid,
                            "delivered": delivered,
                            "replies": new_replies,
                        }
                        if denied:
                            out["denied_mentions"] = denied
                        return out

            out = {
                "ok": True,
                "thread_id": tid,
                "delivered": delivered,
                "replies": [],
                "note": f"Dispatched to {delivered}, but no reply received within {timeout}s. Poll with agentloom_get_thread.",
            }
            if denied:
                out["denied_mentions"] = denied
            return out

        if name == "agentloom_control":
            agent = (args.get("agent") or "").strip()
            action = (args.get("action") or "").strip()
            if not agent or action not in ("restart",):
                return {"error": "agent and valid action ('restart') are required"}
            return self.api_request("POST", f"/api/agents/{agent}/control", body={"action": action}, timeout=20)

        return {"error": f"Unknown tool: {name}"}


def _send_jsonrpc(obj: dict) -> None:
    sys.stdout.write(json.dumps(obj, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def run_stdio_server(
    base_url: Optional[str] = None,
    token: Optional[str] = None,
    sender: Optional[str] = None,
) -> None:
    if hasattr(sys.stdin, "reconfigure"):
        sys.stdin.reconfigure(encoding="utf-8")
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    resolved_url, resolved_token, resolved_sender = resolve_settings(base_url, token, sender)
    client = AgentLoomMCPClient(resolved_url, resolved_token, resolved_sender)

    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            continue

        req_id = req.get("id")
        method = req.get("method", "")
        params = req.get("params") or {}

        if method == "initialize":
            _send_jsonrpc(
                {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "agentloom-mcp", "version": __version__},
                    },
                }
            )
        elif method == "notifications/initialized":
            continue
        elif method == "ping":
            _send_jsonrpc({"jsonrpc": "2.0", "id": req_id, "result": {}})
        elif method == "tools/list":
            _send_jsonrpc({"jsonrpc": "2.0", "id": req_id, "result": {"tools": TOOLS}})
        elif method == "tools/call":
            tool_name = params.get("name", "")
            tool_args = params.get("arguments") or {}
            res = client.handle_tool_call(tool_name, tool_args)
            is_err = isinstance(res, dict) and "error" in res
            _send_jsonrpc(
                {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": json.dumps(res, ensure_ascii=False, indent=2)}],
                        "isError": is_err,
                    },
                }
            )
        elif req_id is not None:
            _send_jsonrpc(
                {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32601, "message": f"Method not found: {method}"},
                }
            )


def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(prog="agentloom-mcp", description="AgentLoom stdio MCP Server")
    parser.add_argument("--url", default=None, help="Hub HTTP URL")
    parser.add_argument("--token", default=None, help="Operator or admin token")
    parser.add_argument("--sender", default=None, help="Local sender name")
    args = parser.parse_args(argv)
    run_stdio_server(base_url=args.url, token=args.token, sender=args.sender)


if __name__ == "__main__":
    main()
