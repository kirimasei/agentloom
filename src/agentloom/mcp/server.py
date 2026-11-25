#!/usr/bin/env python3
"""Zero-dependency Python stdlib MCP Server (JSON-RPC 2.0 over stdio) for AgentLoom."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import mimetypes
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
        "inputSchema": {"type": "object", "properties": {}, "required": []},
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
        "inputSchema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "agentloom_get_thread",
        "description": "Fetch messages, artifacts, and metadata for a specific collaboration thread.",
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
    {
        "name": "agentloom_upload_artifact",
        "description": "Upload a local file or text log/config artifact to the Hub (optionally attached to a thread).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "filename": {"type": "string", "description": "Artifact filename (defaults to basename of file_path)"},
                "file_path": {"type": "string", "description": "Path to a local file to upload"},
                "content_text": {"type": "string", "description": "UTF-8 text content if file_path is omitted"},
                "thread_id": {"type": "string", "description": "Optional thread ID to attach this artifact to"},
                "content_type": {"type": "string", "description": "Optional MIME type"},
            },
            "required": [],
        },
    },
    {
        "name": "agentloom_download_artifact",
        "description": "Download an artifact from the Hub to a local path or return UTF-8 text if small.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "artifact_id": {"type": "string", "description": "Artifact ID (e.g. art_179558_abcd1234)"},
                "save_path": {"type": "string", "description": "Optional local file path to save the downloaded bytes"},
            },
            "required": ["artifact_id"],
        },
    },
    {
        "name": "agentloom_request_approval",
        "description": "Create a human-in-the-loop approval request on the Hub and optionally wait for an admin decision.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "action": {"type": "string", "description": "Short action identifier (e.g. deploy-prod, db-migrate)"},
                "summary": {"type": "string", "description": "Human-readable description of the sensitive operation"},
                "thread_id": {"type": "string", "description": "Optional collaboration thread ID"},
                "target_agent": {"type": "string", "description": "Optional target worker node"},
                "wait_for_decision": {"type": "boolean", "default": True},
                "timeout": {"type": "integer", "default": 180},
            },
            "required": ["action", "summary"],
        },
    },
]


class AgentLoomMCPClient:
    def __init__(self, base_url: str, token: str, sender: str):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.sender = sender

    def _raw_request(
        self,
        method: str,
        path: str,
        body: Optional[dict] = None,
        timeout: int = 30,
    ) -> tuple[Optional[bytes], Optional[str]]:
        if not self.token:
            return None, "AGENTLOOM_TOKEN is not configured. Set AGENTLOOM_TOKEN or pass --token."
        url = f"{self.base_url}{path}"
        data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
        headers = {
            "X-Token": self.token,
            "Accept": "*/*",
            "User-Agent": f"agentloom-mcp/{__version__}",
        }
        if data is not None:
            headers["Content-Type"] = "application/json"

        last_err: object = None
        for attempt in range(3):
            req = urllib.request.Request(url, data=data, headers=headers, method=method)
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    return resp.read(), None
            except urllib.error.HTTPError as exc:
                err_body = exc.read().decode("utf-8", errors="replace")
                if exc.code in (502, 503, 504, 520, 522, 524) and attempt < 2:
                    last_err = f"HTTP {exc.code}: {err_body[:200]}"
                    time.sleep(1.5)
                    continue
                if exc.code == 403:
                    return None, f"HTTP 403 Forbidden: {err_body[:300]} (admin or ACL permission required)"
                return None, f"HTTP {exc.code}: {err_body[:400]}"
            except Exception as exc:
                last_err = exc
                if attempt < 2:
                    time.sleep(1.5)
        return None, f"Request failed after 3 attempts: {last_err}"

    def api_request(self, method: str, path: str, body: Optional[dict] = None, timeout: int = 30) -> dict:
        raw_bytes, err = self._raw_request(method, path, body=body, timeout=timeout)
        if err is not None:
            return {"error": err}
        if not raw_bytes:
            return {"ok": True}
        return json.loads(raw_bytes.decode("utf-8", errors="replace"))

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

        if name == "agentloom_upload_artifact":
            file_path = (args.get("file_path") or "").strip()
            content_text = args.get("content_text")
            filename = (args.get("filename") or "").strip()
            thread_id = (args.get("thread_id") or "").strip() or None
            content_type = (args.get("content_type") or "").strip()

            payload: dict = {"thread_id": thread_id}
            if file_path:
                if not os.path.isfile(file_path):
                    return {"error": f"Local file not found: {file_path}"}
                with open(file_path, "rb") as f:
                    raw = f.read()
                filename = filename or os.path.basename(file_path)
                guessed, _ = mimetypes.guess_type(filename)
                payload["filename"] = filename
                payload["content_base64"] = base64.b64encode(raw).decode("ascii")
                payload["content_type"] = content_type or guessed or "application/octet-stream"
            elif content_text is not None:
                payload["filename"] = filename or "snippet.txt"
                payload["content_text"] = str(content_text)
                payload["content_type"] = content_type or "text/plain; charset=utf-8"
            else:
                return {"error": "Either file_path or content_text is required"}

            return self.api_request("POST", "/api/artifacts", body=payload, timeout=60)

        if name == "agentloom_download_artifact":
            artifact_id = (args.get("artifact_id") or "").strip()
            save_path = (args.get("save_path") or "").strip()
            if not artifact_id:
                return {"error": "artifact_id is required"}
            meta = self.api_request("GET", f"/api/artifacts/{artifact_id}", timeout=15)
            if "error" in meta:
                return meta
            raw_bytes, err = self._raw_request("GET", f"/api/artifacts/{artifact_id}/download", timeout=60)
            if err is not None or raw_bytes is None:
                return {"error": err or "Download failed"}
            sha = hashlib.sha256(raw_bytes).hexdigest()
            if save_path:
                parent = os.path.dirname(os.path.abspath(save_path))
                if parent:
                    os.makedirs(parent, exist_ok=True)
                with open(save_path, "wb") as f:
                    f.write(raw_bytes)
                return {
                    "ok": True,
                    "artifact_id": artifact_id,
                    "filename": meta.get("filename"),
                    "save_path": os.path.abspath(save_path),
                    "size_bytes": len(raw_bytes),
                    "sha256": sha,
                    "sha256_match": sha == meta.get("sha256"),
                }
            if len(raw_bytes) <= 32768:
                try:
                    return {
                        "ok": True,
                        "artifact_id": artifact_id,
                        "filename": meta.get("filename"),
                        "size_bytes": len(raw_bytes),
                        "sha256": sha,
                        "text": raw_bytes.decode("utf-8"),
                    }
                except UnicodeDecodeError:
                    pass
            return {
                "ok": True,
                "artifact_id": artifact_id,
                "filename": meta.get("filename"),
                "size_bytes": len(raw_bytes),
                "sha256": sha,
                "note": "Binary or large artifact; pass save_path to write bytes to disk.",
            }

        if name == "agentloom_request_approval":
            action = (args.get("action") or "").strip()
            summary = (args.get("summary") or "").strip()
            if not action or not summary:
                return {"error": "action and summary are required"}
            wait_for_decision = bool(args.get("wait_for_decision", True))
            timeout = max(5, min(int(args.get("timeout", 180)), 600))
            res = self.api_request(
                "POST",
                "/api/approvals",
                body={
                    "action": action,
                    "summary": summary,
                    "thread_id": (args.get("thread_id") or "").strip() or None,
                    "target_agent": (args.get("target_agent") or "").strip(),
                },
                timeout=20,
            )
            if "error" in res or not wait_for_decision:
                return res
            approval = res.get("approval") or {}
            apr_id = approval.get("id")
            if not apr_id:
                return res

            deadline = time.time() + timeout
            while time.time() < deadline:
                time.sleep(1.5)
                cur = self.api_request("GET", f"/api/approvals/{apr_id}", timeout=15)
                if "error" in cur:
                    return cur
                if cur.get("status") != "pending":
                    return {"ok": True, "approval": cur}

            return {
                "ok": True,
                "approval": approval,
                "note": f"Approval {apr_id} still pending after {timeout}s.",
            }

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
