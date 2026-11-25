"""FastAPI Hub server for AgentLoom with SQLite persistence, RBAC, and node ACLs."""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import logging
import os
import re
import secrets
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from agentloom import __version__
from agentloom.hub.auth import AuthManager, Principal
from agentloom.hub.config import HubConfig
from agentloom.hub.db import HubDatabase
from agentloom.protocol import (
    build_mcp_config_snippet,
    build_worker_command,
    extract_mentions,
    infer_role,
    make_topic,
)

log = logging.getLogger("agentloom.hub")
STATIC_DIR = Path(__file__).resolve().parent / "static"


def sanitize_filename(name: str) -> str:
    base = os.path.basename((name or "").replace("\\", "/")).strip()
    cleaned = re.sub(r"[\x00-\x1f\x7f<>:\"/\\|?*]+", "_", base).strip(". ")
    return (cleaned or "artifact.bin")[:120]


class AgentStats:
    def __init__(self, max_replies_per_hour: int):
        self.max_replies_per_hour = max_replies_per_hour
        self.recent_replies: dict[int, int] = defaultdict(int)
        self.total_replies = 0
        self.total_messages = 0

    def can_reply(self) -> bool:
        hour_bucket = int(time.time()) // 3600
        return self.recent_replies.get(hour_bucket, 0) < self.max_replies_per_hour

    def record_reply(self) -> None:
        hour_bucket = int(time.time()) // 3600
        self.recent_replies[hour_bucket] = self.recent_replies.get(hour_bucket, 0) + 1
        self.total_replies += 1

    def cleanup(self) -> None:
        hour_bucket = int(time.time()) // 3600
        for bucket in [k for k in self.recent_replies if hour_bucket - k > 2]:
            del self.recent_replies[bucket]


class Thread:
    def __init__(self, thread_id: str, initiator: str, topic: str = "", max_rounds: int = 30):
        self.id = thread_id
        self.participants: set[str] = set()
        if initiator:
            self.participants.add(initiator)
        self.messages: list[dict] = []
        self.round_count = 0
        self.max_rounds = max_rounds
        self.manually_closed = False
        self.created_at = datetime.now(timezone.utc)
        self.topic = topic
        self.last_activity = datetime.now(timezone.utc)

    @property
    def is_closed(self) -> bool:
        return self.manually_closed or self.round_count >= self.max_rounds

    def add_message(self, sender: str, content: str, mentions: list[str], role: Optional[str] = None) -> dict:
        resolved_role = role or infer_role(sender)
        msg_obj = {
            "sender": sender,
            "role": resolved_role,
            "content": content,
            "mentions": mentions,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self.messages.append(msg_obj)
        self.round_count += 1
        self.last_activity = datetime.now(timezone.utc)
        if sender and sender != "system":
            self.participants.add(sender)
        return msg_obj

    def snapshot_tuple(self) -> tuple:
        return (
            self.id,
            self.topic,
            json.dumps(sorted(list(self.participants)), ensure_ascii=False),
            self.round_count,
            1 if self.manually_closed else 0,
            self.created_at.isoformat(),
            self.last_activity.isoformat(),
        )

    def to_dict(self) -> dict:
        last_msg = None
        if self.messages:
            m = self.messages[-1]
            last_msg = {
                "sender": m["sender"],
                "role": m.get("role", infer_role(m["sender"])),
                "preview": m["content"][:120],
                "timestamp": m["timestamp"],
            }
        return {
            "id": self.id,
            "participants": sorted(list(self.participants)),
            "round_count": self.round_count,
            "max_rounds": self.max_rounds,
            "topic": self.topic,
            "is_closed": self.is_closed,
            "created_at": self.created_at.isoformat(),
            "last_activity": self.last_activity.isoformat(),
            "message_count": len(self.messages),
            "last_message": last_msg,
        }


class SendRequest(BaseModel):
    sender: str = "local-agent"
    role: str = "mcp"
    content: str
    thread_id: Optional[str] = None
    mentions: Optional[list[str]] = None
    wait_for_reply: bool = False
    timeout: int = 120


class RenameThreadRequest(BaseModel):
    topic: str


class ControlRequest(BaseModel):
    action: str


class CreateTokenRequest(BaseModel):
    name: str
    role: str = "operator"
    allowed_agents: list[str] = ["*"]


class ArtifactUploadRequest(BaseModel):
    filename: str
    content_base64: Optional[str] = None
    content_text: Optional[str] = None
    thread_id: Optional[str] = None
    content_type: str = "application/octet-stream"


class CreateApprovalRequest(BaseModel):
    action: str
    summary: str
    thread_id: Optional[str] = None
    target_agent: str = ""
    payload: Optional[dict] = None


class DecideApprovalRequest(BaseModel):
    decision: str
    note: str = ""


class HubState:
    def __init__(self, cfg: HubConfig):
        self.cfg = cfg
        self.db = HubDatabase(cfg.db_path)
        self._auth: Optional[AuthManager] = None

        self.agents: dict[str, dict] = {}
        self.connected_agents: dict[WebSocket, str] = {}
        self.connected_humans: dict[WebSocket, tuple[str, Principal]] = {}
        self.ws_token_ids: dict[WebSocket, str] = {}
        self.threads: dict[str, Thread] = {}
        self.thread_waiters: dict[str, list[asyncio.Event]] = defaultdict(list)
        self.approval_waiters: dict[str, list[asyncio.Event]] = defaultdict(list)
        self.approval_worker_requests: dict[str, tuple[str, str]] = {}

    @property
    def auth(self) -> AuthManager:
        if self._auth is None:
            admin_token, generated = self.cfg.resolve_admin_token()
            if generated:
                log.info(f"Generated root admin token saved to {self.cfg.admin_token_path}")
            self._auth = AuthManager(admin_token=admin_token, db=self.db)
        return self._auth

    def ensure_agent(self, name: str, role: str = "agent", meta: Optional[dict] = None) -> dict:
        if name not in self.agents:
            self.agents[name] = {
                "name": name,
                "role": role,
                "meta": meta or {},
                "last_seen": time.time(),
                "stats": AgentStats(self.cfg.max_replies_per_hour),
                "working": None,
            }
        else:
            self.agents[name]["last_seen"] = time.time()
            if meta:
                self.agents[name]["meta"].update(meta)
        return self.agents[name]

    def get_agent_ws(self, agent_name: str) -> Optional[WebSocket]:
        for ws, name in list(self.connected_agents.items()):
            if name == agent_name:
                return ws
        return None

    def is_agent_online(self, name: str) -> bool:
        info = self.agents.get(name)
        if not info:
            return False
        ws = self.get_agent_ws(name)
        return ws is not None and (time.time() - info["last_seen"] < self.cfg.agent_alive_timeout)

    def build_agents_snapshot(self, principal: Optional[Principal] = None) -> dict:
        now = time.time()
        result = {}
        for name, info in self.agents.items():
            if principal and not principal.can_access_agent(name):
                continue
            online = self.is_agent_online(name)
            result[name] = {
                "online": online,
                "last_seen": info["last_seen"],
                "last_seen_ago": int(now - info["last_seen"]),
                "total_replies": info["stats"].total_replies,
                "total_messages": info["stats"].total_messages,
                "can_reply": info["stats"].can_reply(),
                "meta": info.get("meta", {}),
                "working": info.get("working") if online else None,
            }
        return result

    def can_view_thread(self, principal: Principal, thread: Thread) -> bool:
        if principal.is_admin or "*" in principal.allowed_agents:
            return True
        agent_participants = {m["sender"] for m in thread.messages if m.get("role") == "agent"}
        for p in thread.participants:
            if p in self.agents:
                agent_participants.add(p)
        for m in thread.messages:
            for men in m.get("mentions", []):
                if not principal.can_access_agent(men):
                    return False
        if any(not principal.can_access_agent(a) for a in agent_participants):
            return False
        return (
            any(principal.can_access_agent(a) for a in agent_participants)
            or principal.name in thread.participants
        )

    def notify_thread_waiters(self, thread_id: str) -> None:
        for ev in list(self.thread_waiters.get(thread_id, [])):
            ev.set()

    def notify_approval_waiters(self, approval_id: str) -> None:
        for ev in list(self.approval_waiters.get(approval_id, [])):
            ev.set()

    def can_view_artifact(self, principal: Principal, artifact: dict) -> bool:
        if principal.is_admin or "*" in principal.allowed_agents:
            return True
        tid = artifact.get("thread_id")
        if tid:
            t = self.threads.get(tid)
            return bool(t and self.can_view_thread(principal, t))
        return principal.name == artifact.get("uploader")

    def can_view_approval(self, principal: Principal, approval: dict) -> bool:
        if principal.is_admin or "*" in principal.allowed_agents:
            return True
        target_agent = (approval.get("target_agent") or "").strip()
        if target_agent and not principal.can_access_agent(target_agent):
            return False
        tid = approval.get("thread_id")
        if tid:
            t = self.threads.get(tid)
            return bool(t and self.can_view_thread(principal, t))
        return principal.name == approval.get("requester") or (
            bool(target_agent) and principal.can_access_agent(target_agent)
        )

    async def save_thread(self, thread: Thread) -> None:
        await self.db.run(self.db.save_thread_sync, thread.snapshot_tuple())

    async def save_thread_and_message(self, thread: Thread, msg_obj: dict) -> None:
        await self.db.run(self.db.save_thread_and_message_sync, thread.snapshot_tuple(), thread.id, msg_obj)

    def load_threads_from_db(self) -> None:
        rows = self.db.load_threads_sync()
        for row in rows:
            t = Thread(row["id"], "", row["topic"], max_rounds=self.cfg.max_rounds)
            t.participants = set(row["participants"])
            t.round_count = row["round_count"]
            t.manually_closed = row["is_closed"]
            t.created_at = datetime.fromisoformat(row["created_at"])
            t.last_activity = datetime.fromisoformat(row["last_activity"])
            t.messages = row["messages"]
            self.threads[t.id] = t
        log.info(f"Loaded {len(self.threads)} threads from SQLite")

    async def send_to_agents_and_humans(
        self,
        payload_obj: dict,
        thread: Optional[Thread] = None,
        agent_scope: Optional[str] = None,
    ) -> None:
        payload = json.dumps(payload_obj, ensure_ascii=False)
        dead_agents = []
        for ws in list(self.connected_agents.keys()):
            try:
                await ws.send_text(payload)
            except Exception:
                dead_agents.append(ws)
        for ws in dead_agents:
            self.connected_agents.pop(ws, None)
            self.ws_token_ids.pop(ws, None)

        dead_humans = []
        for ws, (_, principal) in list(self.connected_humans.items()):
            if agent_scope and not principal.can_access_agent(agent_scope):
                continue
            if thread and not self.can_view_thread(principal, thread):
                continue
            try:
                await ws.send_text(payload)
            except Exception:
                dead_humans.append(ws)
        for ws in dead_humans:
            self.connected_humans.pop(ws, None)
            self.ws_token_ids.pop(ws, None)

    async def broadcast_agents_update(self) -> None:
        dead_agents = []
        agent_payload = json.dumps(
            {"type": "agents_update", "agents": self.build_agents_snapshot()},
            ensure_ascii=False,
        )
        for ws in list(self.connected_agents.keys()):
            try:
                await ws.send_text(agent_payload)
            except Exception:
                dead_agents.append(ws)
        for ws in dead_agents:
            self.connected_agents.pop(ws, None)
            self.ws_token_ids.pop(ws, None)

        dead_humans = []
        for ws, (_, principal) in list(self.connected_humans.items()):
            human_payload = json.dumps(
                {"type": "agents_update", "agents": self.build_agents_snapshot(principal)},
                ensure_ascii=False,
            )
            try:
                await ws.send_text(human_payload)
            except Exception:
                dead_humans.append(ws)
        for ws in dead_humans:
            self.connected_humans.pop(ws, None)
            self.ws_token_ids.pop(ws, None)

    async def broadcast_message(
        self,
        sender: str,
        content: str,
        mentions: list[str],
        thread_id: Optional[str] = None,
        role: Optional[str] = None,
        thread: Optional[Thread] = None,
        stream_id: Optional[str] = None,
    ) -> None:
        target_thread = thread or (self.threads.get(thread_id) if thread_id else None)
        payload = {
            "type": "broadcast",
            "sender": sender,
            "role": role or infer_role(sender),
            "content": content,
            "mentions": mentions,
            "thread_id": thread_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if stream_id:
            payload["stream_id"] = stream_id
        await self.send_to_agents_and_humans(payload, thread=target_thread)

    async def broadcast_thread_update(self, thread: Thread) -> None:
        await self.send_to_agents_and_humans(
            {"type": "thread_update", "thread": thread.to_dict()},
            thread=thread,
        )

    async def deliver_to_agents(self, sender: str, content: str, mentions: list[str], thread: Thread) -> list[str]:
        delivered = []
        recent_msgs = thread.messages[-self.cfg.recent_context_limit :]
        for mentioned in mentions:
            if mentioned == sender:
                continue
            ws = self.get_agent_ws(mentioned)
            if ws and self.is_agent_online(mentioned):
                try:
                    await ws.send_text(
                        json.dumps(
                            {
                                "type": "message_for_you",
                                "thread_id": thread.id,
                                "sender": sender,
                                "content": content,
                                "mentions": mentions,
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "thread_info": thread.to_dict(),
                                "recent_messages": recent_msgs,
                            },
                            ensure_ascii=False,
                        )
                    )
                    if mentioned in self.agents:
                        self.agents[mentioned]["stats"].total_messages += 1
                    delivered.append(mentioned)
                except Exception as exc:
                    log.error(f"Failed delivering to {mentioned}: {exc}")
        return delivered

    async def handle_message(
        self,
        sender: str,
        msg: dict,
        sender_role: Optional[str] = None,
        principal: Optional[Principal] = None,
    ) -> dict:
        content = (msg.get("content") or "").strip()
        if not content:
            return {
                "thread_id": msg.get("thread_id"),
                "delivered": [],
                "denied_mentions": [],
                "message_index": 0,
            }

        resolved_role = sender_role or infer_role(sender)
        raw_mentions = msg.get("mentions") or extract_mentions(content)
        raw_mentions = [m for m in raw_mentions if m != sender]

        if principal:
            allowed_mentions = [m for m in raw_mentions if principal.can_access_agent(m)]
            denied_mentions = [m for m in raw_mentions if not principal.can_access_agent(m)]
        else:
            allowed_mentions = raw_mentions
            denied_mentions = []

        thread_id = msg.get("thread_id")
        if thread_id and thread_id in self.threads:
            thread = self.threads[thread_id]
            if principal and not self.can_view_thread(principal, thread):
                raise HTTPException(status_code=403, detail="Thread outside token ACL scope")
        else:
            thread_id = thread_id or f"thread-{int(time.time())}-{secrets.token_hex(3)}"
            thread = Thread(thread_id, sender, make_topic(content), max_rounds=self.cfg.max_rounds)
            self.threads[thread_id] = thread
            await self.save_thread(thread)
            log.info(f"New thread created: {thread_id} by {sender}")

        if thread.is_closed:
            await self.broadcast_message(
                "system",
                f"[线程已关闭] 请新建线程或重新打开 {thread_id}",
                [],
                thread_id,
                role="system",
                thread=thread,
            )
            return {
                "thread_id": thread_id,
                "delivered": [],
                "denied_mentions": denied_mentions,
                "message_index": len(thread.messages),
            }

        msg_obj = thread.add_message(sender, content, allowed_mentions, role=resolved_role)
        await self.save_thread_and_message(thread, msg_obj)
        message_index = len(thread.messages)

        await self.broadcast_message(sender, content, allowed_mentions, thread_id, role=resolved_role, thread=thread)
        await self.broadcast_thread_update(thread)

        delivered = await self.deliver_to_agents(sender, content, allowed_mentions, thread)
        self.notify_thread_waiters(thread_id)

        if denied_mentions:
            tip = f"[ACL] 无权访问节点 {', '.join('@' + m for m in denied_mentions)}，已跳过投递。"
            await self.broadcast_message("system", tip, [], thread_id, role="system", thread=thread)
        elif allowed_mentions and not delivered:
            online_list = [
                n
                for n in self.agents
                if self.is_agent_online(n) and (not principal or principal.can_access_agent(n))
            ]
            tip = (
                f"[提示] 目标 {', '.join('@' + m for m in allowed_mentions)} 当前不在线。"
                f"可用在线节点: {', '.join('@' + n for n in online_list) or '无'}"
            )
            await self.broadcast_message("system", tip, [], thread_id, role="system", thread=thread)

        return {
            "thread_id": thread_id,
            "delivered": delivered,
            "denied_mentions": denied_mentions,
            "message_index": message_index,
        }

    async def handle_agent_reply(self, sender: str, msg: dict) -> None:
        content = (msg.get("content") or "").strip()
        thread_id = msg.get("thread_id")
        mentions = [m for m in extract_mentions(content) if m != sender]

        if sender in self.agents:
            self.agents[sender]["working"] = None
            await self.send_to_agents_and_humans(
                {
                    "type": "agent_status",
                    "agent": sender,
                    "state": "idle",
                    "thread_id": thread_id,
                    "detail": "",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
                agent_scope=sender,
            )
            stats = self.agents[sender]["stats"]
            if not stats.can_reply():
                log.warning(f"Rate limited: {sender}")
                await self.broadcast_message(
                    "system",
                    f"[限流] @{sender} 超过每小时 {self.cfg.max_replies_per_hour} 条回复上限",
                    [],
                    thread_id,
                    role="system",
                )
                return

        thread = self.threads.get(thread_id) if thread_id else None
        if not thread:
            thread_id = thread_id or f"thread-{int(time.time())}-{secrets.token_hex(3)}"
            thread = Thread(thread_id, sender, make_topic(content), max_rounds=self.cfg.max_rounds)
            self.threads[thread_id] = thread

        msg_obj = thread.add_message(sender, content, mentions, role="agent")
        if sender in self.agents:
            self.agents[sender]["stats"].record_reply()
        await self.save_thread_and_message(thread, msg_obj)

        await self.broadcast_message(
            sender,
            content,
            mentions,
            thread_id,
            role="agent",
            thread=thread,
            stream_id=msg.get("stream_id"),
        )
        await self.broadcast_thread_update(thread)
        self.notify_thread_waiters(thread_id)

        if thread.is_closed:
            await self.broadcast_message(
                "system",
                f"[熔断] 线程 {thread_id} 已达最大轮数 ({self.cfg.max_rounds})，自动关闭",
                [],
                thread_id,
                role="system",
                thread=thread,
            )
            return

        await self.deliver_to_agents(sender, content, mentions, thread)

    async def revoke_ws_for_token(self, token_id: str) -> None:
        targets = [ws for ws, tid in list(self.ws_token_ids.items()) if tid == token_id]
        for ws in targets:
            try:
                await ws.send_text(json.dumps({"type": "error", "error": "Token revoked"}))
                await ws.close(code=4001, reason="Token revoked")
            except Exception:
                pass


def create_app(config: Optional[HubConfig] = None) -> FastAPI:
    cfg = config or HubConfig.from_env()
    state = HubState(cfg)

    async def heartbeat_checker():
        prev_online: set[str] = set()
        while True:
            await asyncio.sleep(20)
            cur_online = {name for name in state.agents if state.is_agent_online(name)}
            if cur_online != prev_online:
                for offline_name in prev_online - cur_online:
                    if offline_name in state.agents:
                        state.agents[offline_name]["working"] = None
                prev_online = cur_online
                await state.broadcast_agents_update()

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        state.db.init_sync()
        _ = state.auth
        state.load_threads_from_db()
        task = asyncio.create_task(heartbeat_checker())
        try:
            yield
        finally:
            task.cancel()

    app = FastAPI(title=f"AgentLoom Hub v{__version__}", lifespan=lifespan)
    app.state.hub = state

    os.makedirs(STATIC_DIR, exist_ok=True)
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    async def require_authenticated(request: Request) -> Principal:
        return await state.auth.require_authenticated(request)

    async def require_operator(request: Request) -> Principal:
        return await state.auth.require_operator(request)

    async def require_admin(request: Request) -> Principal:
        return await state.auth.require_admin(request)

    @app.get("/health")
    async def health():
        return {"ok": True, "name": "agentloom", "version": __version__}

    @app.get("/")
    async def root():
        return FileResponse(str(STATIC_DIR / "index.html"))

    @app.post("/api/auth/verify")
    @app.get("/api/auth/whoami")
    async def auth_whoami(principal: Principal = Depends(require_operator)):
        return {"ok": True, **principal.to_dict()}

    # ── Token Management (Admin Only) ─────────────────────────────────
    @app.get("/api/tokens")
    async def api_list_tokens(_admin: Principal = Depends(require_admin)):
        return {"tokens": await state.auth.list_tokens()}

    @app.post("/api/tokens")
    async def api_create_token(
        req: CreateTokenRequest,
        request: Request,
        _admin: Principal = Depends(require_admin),
    ):
        record = await state.auth.create_token(req.name, req.role, req.allowed_agents)
        base_url = str(request.base_url).rstrip("/")
        if req.role == "operator":
            record["mcp_config"] = build_mcp_config_snippet(base_url, record["token"], sender=req.name)
        elif req.role == "worker":
            node_hint = (
                req.allowed_agents[0]
                if req.allowed_agents and req.allowed_agents[0] != "*"
                else "my-node"
            )
            record["worker_command"] = build_worker_command(base_url, record["token"], name=node_hint)
        return record

    @app.delete("/api/tokens/{token_id}")
    async def api_revoke_token(token_id: str, _admin: Principal = Depends(require_admin)):
        revoked = await state.auth.revoke_token(token_id)
        if not revoked:
            raise HTTPException(status_code=404, detail="Token not found or already revoked")
        await state.revoke_ws_for_token(token_id)
        return {"ok": True, "token_id": token_id}

    # ── Agents & Control ──────────────────────────────────────────────
    @app.get("/api/agents")
    async def list_agents(principal: Principal = Depends(require_operator)):
        return state.build_agents_snapshot(principal)

    @app.get("/api/status")
    async def get_status(principal: Principal = Depends(require_operator)):
        visible_threads = [t for t in state.threads.values() if state.can_view_thread(principal, t)]
        return {
            "server_time": datetime.now(timezone.utc).isoformat(),
            "agents": state.build_agents_snapshot(principal),
            "active_threads": len([t for t in visible_threads if not t.is_closed]),
            "closed_threads": len([t for t in visible_threads if t.is_closed]),
        }

    @app.post("/api/agents/{agent_name}/control")
    async def control_agent(
        agent_name: str,
        req: ControlRequest,
        _admin: Principal = Depends(require_admin),
    ):
        action = (req.action or "").strip()
        if action not in ("restart",):
            raise HTTPException(status_code=400, detail=f"Unsupported action: {action}")
        if agent_name not in state.agents:
            raise HTTPException(status_code=404, detail=f"Agent not found: {agent_name}")
        ws = state.get_agent_ws(agent_name)
        if not ws or not state.is_agent_online(agent_name):
            raise HTTPException(status_code=409, detail=f"Agent is offline: {agent_name}")
        try:
            await ws.send_text(
                json.dumps(
                    {
                        "type": "control",
                        "action": action,
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    },
                    ensure_ascii=False,
                )
            )
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"Failed sending control to {agent_name}: {exc}")
        return {"ok": True, "agent": agent_name, "action": action}

    # ── Threads & Messaging ───────────────────────────────────────────
    @app.get("/api/threads")
    async def list_threads(principal: Principal = Depends(require_operator)):
        visible = [t for t in state.threads.values() if state.can_view_thread(principal, t)]
        visible.sort(key=lambda t: t.last_activity, reverse=True)
        return {t.id: t.to_dict() for t in visible}

    @app.get("/api/threads/{thread_id}")
    async def get_thread(
        thread_id: str,
        limit: int = 100,
        since_index: Optional[int] = None,
        principal: Principal = Depends(require_operator),
    ):
        t = state.threads.get(thread_id)
        if not t:
            raise HTTPException(status_code=404, detail="Thread not found")
        if not state.can_view_thread(principal, t):
            raise HTTPException(status_code=403, detail="Thread outside token ACL scope")
        if since_index is not None:
            msgs = t.messages[max(0, since_index) :]
        else:
            msgs = t.messages[-limit:]
        artifacts = await state.db.run(state.db.list_artifacts_sync, thread_id)
        return {**t.to_dict(), "messages": msgs, "artifacts": artifacts}

    @app.patch("/api/threads/{thread_id}")
    async def rename_thread(
        thread_id: str,
        req: RenameThreadRequest,
        principal: Principal = Depends(require_operator),
    ):
        t = state.threads.get(thread_id)
        if not t:
            raise HTTPException(status_code=404, detail="Thread not found")
        if not state.can_view_thread(principal, t):
            raise HTTPException(status_code=403, detail="Thread outside token ACL scope")
        new_topic = req.topic.strip()[:120]
        if not new_topic:
            raise HTTPException(status_code=400, detail="Topic cannot be empty")
        t.topic = new_topic
        t.last_activity = datetime.now(timezone.utc)
        await state.save_thread(t)
        await state.broadcast_thread_update(t)
        return {"ok": True, "thread": t.to_dict()}

    @app.post("/api/thread/{thread_id}/close")
    async def close_thread(
        thread_id: str,
        reason: str = "manual",
        principal: Principal = Depends(require_operator),
    ):
        t = state.threads.get(thread_id)
        if not t:
            raise HTTPException(status_code=404, detail="Thread not found")
        if not state.can_view_thread(principal, t):
            raise HTTPException(status_code=403, detail="Thread outside token ACL scope")
        t.manually_closed = True
        await state.save_thread(t)
        await state.broadcast_message("system", f"[线程已关闭] ({reason})", [], thread_id, role="system", thread=t)
        await state.broadcast_thread_update(t)
        return {"ok": True}

    @app.post("/api/thread/{thread_id}/reopen")
    async def reopen_thread(thread_id: str, principal: Principal = Depends(require_operator)):
        t = state.threads.get(thread_id)
        if not t:
            raise HTTPException(status_code=404, detail="Thread not found")
        if not state.can_view_thread(principal, t):
            raise HTTPException(status_code=403, detail="Thread outside token ACL scope")
        t.manually_closed = False
        if t.round_count >= state.cfg.max_rounds:
            t.round_count = max(0, state.cfg.max_rounds - 10)
        await state.save_thread(t)
        await state.broadcast_message("system", "[线程已重新打开]", [], thread_id, role="system", thread=t)
        await state.broadcast_thread_update(t)
        return {"ok": True}

    @app.post("/api/send")
    async def api_send(req: SendRequest, principal: Principal = Depends(require_operator)):
        sanitized_role = req.role if req.role in ("mcp", "human") else "mcp"
        result = await state.handle_message(
            req.sender,
            {"content": req.content, "thread_id": req.thread_id, "mentions": req.mentions or []},
            sender_role=sanitized_role,
            principal=principal,
        )
        thread_id = result.get("thread_id")
        delivered = result.get("delivered", [])
        message_index = result.get("message_index", 0)

        replies = []
        if req.wait_for_reply and thread_id and delivered:
            t = state.threads.get(thread_id)
            if t:
                deadline = time.time() + max(5, min(req.timeout, 600))
                ev = asyncio.Event()
                state.thread_waiters[thread_id].append(ev)
                try:
                    while time.time() < deadline:
                        new_msgs = [m for m in t.messages[message_index:] if m["sender"] in delivered]
                        if new_msgs:
                            replies = new_msgs
                            break
                        remaining = deadline - time.time()
                        if remaining <= 0:
                            break
                        ev.clear()
                        try:
                            await asyncio.wait_for(ev.wait(), timeout=min(remaining, 2.0))
                        except asyncio.TimeoutError:
                            pass
                finally:
                    if ev in state.thread_waiters[thread_id]:
                        state.thread_waiters[thread_id].remove(ev)

        return {
            "ok": True,
            "thread_id": thread_id,
            "delivered": delivered,
            "denied_mentions": result.get("denied_mentions", []),
            "message_index": message_index,
            "replies": replies,
        }

    # ── Artifacts Transfer ────────────────────────────────────────────
    @app.post("/api/artifacts")
    async def upload_artifact(
        req: ArtifactUploadRequest,
        principal: Principal = Depends(require_authenticated),
    ):
        clean_name = sanitize_filename(req.filename)
        tid = (req.thread_id or "").strip() or None
        target_thread: Optional[Thread] = None
        if tid:
            target_thread = state.threads.get(tid)
            if not target_thread:
                raise HTTPException(status_code=404, detail="Thread not found")
            if principal.is_operator and not state.can_view_thread(principal, target_thread):
                raise HTTPException(status_code=403, detail="Thread outside token ACL scope")

        if req.content_base64 is not None:
            try:
                raw_bytes = base64.b64decode(req.content_base64.encode("ascii"), validate=True)
            except (binascii.Error, ValueError) as exc:
                raise HTTPException(status_code=400, detail=f"Invalid base64 payload: {exc}")
        elif req.content_text is not None:
            raw_bytes = req.content_text.encode("utf-8")
        else:
            raise HTTPException(status_code=400, detail="Either content_base64 or content_text is required")

        max_bytes = state.cfg.max_artifact_mb * 1024 * 1024
        if len(raw_bytes) > max_bytes:
            raise HTTPException(status_code=413, detail=f"Artifact exceeds {state.cfg.max_artifact_mb} MB limit")

        artifact_id = f"art_{int(time.time())}_{secrets.token_hex(4)}"
        sha256 = hashlib.sha256(raw_bytes).hexdigest()
        os.makedirs(state.cfg.artifacts_dir, exist_ok=True)
        disk_path = os.path.join(state.cfg.artifacts_dir, artifact_id)
        with open(disk_path, "wb") as f:
            f.write(raw_bytes)

        meta = await state.db.run(
            state.db.insert_artifact_sync,
            artifact_id,
            tid,
            principal.name,
            clean_name,
            (req.content_type or "application/octet-stream")[:120],
            len(raw_bytes),
            sha256,
        )
        await state.send_to_agents_and_humans(
            {"type": "artifact_added", "artifact": meta},
            thread=target_thread,
        )
        if target_thread:
            await state.broadcast_message(
                "system",
                f"[附件] {principal.name} 上传了 {clean_name} ({len(raw_bytes)} B)",
                [],
                tid,
                role="system",
                thread=target_thread,
            )
        return {"ok": True, "artifact": meta}

    @app.get("/api/artifacts")
    async def list_artifacts(
        thread_id: Optional[str] = None,
        principal: Principal = Depends(require_operator),
    ):
        tid = (thread_id or "").strip() or None
        if tid:
            t = state.threads.get(tid)
            if not t:
                raise HTTPException(status_code=404, detail="Thread not found")
            if not state.can_view_thread(principal, t):
                raise HTTPException(status_code=403, detail="Thread outside token ACL scope")
        rows = await state.db.run(state.db.list_artifacts_sync, tid)
        visible = [a for a in rows if state.can_view_artifact(principal, a)]
        return {"artifacts": visible}

    @app.get("/api/artifacts/{artifact_id}")
    async def get_artifact_meta(
        artifact_id: str,
        principal: Principal = Depends(require_authenticated),
    ):
        art = await state.db.run(state.db.get_artifact_sync, artifact_id)
        if not art:
            raise HTTPException(status_code=404, detail="Artifact not found")
        if not principal.is_worker or principal.is_operator:
            if not state.can_view_artifact(principal, art):
                raise HTTPException(status_code=403, detail="Artifact outside token ACL scope")
        return art

    @app.get("/api/artifacts/{artifact_id}/download")
    async def download_artifact(
        artifact_id: str,
        principal: Principal = Depends(require_authenticated),
    ):
        art = await state.db.run(state.db.get_artifact_sync, artifact_id)
        if not art:
            raise HTTPException(status_code=404, detail="Artifact not found")
        if principal.role == "operator" and not state.can_view_artifact(principal, art):
            raise HTTPException(status_code=403, detail="Artifact outside token ACL scope")
        disk_path = os.path.join(state.cfg.artifacts_dir, artifact_id)
        if not os.path.isfile(disk_path):
            raise HTTPException(status_code=404, detail="Artifact file missing on disk")
        return FileResponse(
            disk_path,
            media_type="application/octet-stream",
            filename=art["filename"],
            headers={"X-Content-Type-Options": "nosniff"},
        )

    # ── Human-in-the-Loop Approvals ───────────────────────────────────
    @app.post("/api/approvals")
    async def create_approval(
        req: CreateApprovalRequest,
        principal: Principal = Depends(require_operator),
    ):
        tid = (req.thread_id or "").strip() or None
        target_thread: Optional[Thread] = None
        if tid:
            target_thread = state.threads.get(tid)
            if not target_thread:
                raise HTTPException(status_code=404, detail="Thread not found")
            if not state.can_view_thread(principal, target_thread):
                raise HTTPException(status_code=403, detail="Thread outside token ACL scope")
        target_agent = (req.target_agent or "").strip()
        if target_agent and not principal.can_access_agent(target_agent):
            raise HTTPException(status_code=403, detail=f"Node '{target_agent}' outside token ACL scope")

        approval_id = f"apr_{int(time.time())}_{secrets.token_hex(4)}"
        record = await state.db.run(
            state.db.insert_approval_sync,
            approval_id,
            tid,
            principal.name,
            target_agent,
            req.action.strip()[:80],
            req.summary.strip()[:500],
            req.payload or {},
        )
        await state.send_to_agents_and_humans(
            {"type": "approval_created", "approval": record},
            thread=target_thread,
            agent_scope=target_agent or None,
        )
        if target_thread:
            await state.broadcast_message(
                "system",
                f"[待审批 #{approval_id}] {principal.name} 请求执行 {record['action']}：{record['summary']}",
                [],
                tid,
                role="system",
                thread=target_thread,
            )
        return {"ok": True, "approval": record}

    @app.get("/api/approvals")
    async def list_approvals(
        status: Optional[str] = None,
        thread_id: Optional[str] = None,
        principal: Principal = Depends(require_operator),
    ):
        rows = await state.db.run(state.db.list_approvals_sync, status, thread_id)
        visible = [a for a in rows if state.can_view_approval(principal, a)]
        return {"approvals": visible}

    @app.get("/api/approvals/{approval_id}")
    async def get_approval(
        approval_id: str,
        principal: Principal = Depends(require_operator),
    ):
        record = await state.db.run(state.db.get_approval_sync, approval_id)
        if not record:
            raise HTTPException(status_code=404, detail="Approval not found")
        if not state.can_view_approval(principal, record):
            raise HTTPException(status_code=403, detail="Approval outside token ACL scope")
        return record

    @app.post("/api/approvals/{approval_id}/decide")
    async def decide_approval(
        approval_id: str,
        req: DecideApprovalRequest,
        admin: Principal = Depends(require_admin),
    ):
        decision = (req.decision or "").strip().lower()
        if decision not in ("approved", "rejected"):
            raise HTTPException(status_code=400, detail="decision must be 'approved' or 'rejected'")
        existing = await state.db.run(state.db.get_approval_sync, approval_id)
        if not existing:
            raise HTTPException(status_code=404, detail="Approval not found")
        if existing["status"] != "pending":
            raise HTTPException(status_code=409, detail=f"Approval already {existing['status']}")

        updated = await state.db.run(
            state.db.decide_approval_sync,
            approval_id,
            decision,
            admin.name,
            (req.note or "").strip()[:300],
        )
        if not updated:
            raise HTTPException(status_code=409, detail="Approval could not be updated")

        state.notify_approval_waiters(approval_id)
        worker_meta = state.approval_worker_requests.pop(approval_id, None)
        if worker_meta:
            target_worker, req_id = worker_meta
            ws = state.get_agent_ws(target_worker)
            if ws:
                try:
                    await ws.send_text(
                        json.dumps(
                            {
                                "type": "approval_decision",
                                "approval_id": approval_id,
                                "request_id": req_id,
                                "decision": decision,
                                "decided_by": admin.name,
                                "note": updated["decision_note"],
                            },
                            ensure_ascii=False,
                        )
                    )
                except Exception as exc:
                    log.warning(f"Failed notifying worker {target_worker} of approval {approval_id}: {exc}")

        tid = updated.get("thread_id")
        target_thread = state.threads.get(tid) if tid else None
        await state.send_to_agents_and_humans(
            {"type": "approval_updated", "approval": updated},
            thread=target_thread,
            agent_scope=(updated.get("target_agent") or None),
        )
        if target_thread:
            label = "已批准" if decision == "approved" else "已拒绝"
            note_suffix = f"（备注：{updated['decision_note']}）" if updated["decision_note"] else ""
            await state.broadcast_message(
                "system",
                f"[审批{label} #{approval_id}] {admin.name} {label} {updated['action']}{note_suffix}",
                [],
                tid,
                role="system",
                thread=target_thread,
            )
        return {"ok": True, "approval": updated}

    # ── WebSocket Endpoint ────────────────────────────────────────────
    @app.websocket("/ws")
    async def websocket_endpoint(ws: WebSocket):
        await ws.accept()
        agent_name: Optional[str] = None
        role: str = "agent"
        principal: Optional[Principal] = None

        try:
            raw = await asyncio.wait_for(ws.receive_text(), timeout=15)
            msg = json.loads(raw)
            if msg.get("type") != "register":
                await ws.send_text(json.dumps({"type": "error", "error": "First message must be register"}))
                await ws.close()
                return

            name = (msg.get("agent_name") or "").strip()
            token = msg.get("token") or msg.get("secret")
            role = (msg.get("role") or "agent").strip()
            meta = msg.get("meta") if isinstance(msg.get("meta"), dict) else {}

            principal = await state.auth.authenticate(token)
            if not principal or not name:
                await ws.send_text(json.dumps({"type": "error", "error": "Invalid agent_name or token"}))
                await ws.close()
                return

            if role == "human":
                if not principal.is_operator:
                    await ws.send_text(json.dumps({"type": "error", "error": "Insufficient role for human UI"}))
                    await ws.close()
                    return
                agent_name = name
                state.connected_humans[ws] = (name, principal)
                state.ws_token_ids[ws] = principal.token_id
                await ws.send_text(
                    json.dumps(
                        {
                            "type": "registered",
                            "agent": name,
                            "role": "human",
                            "principal": principal.to_dict(),
                            "ok": True,
                            "agents": state.build_agents_snapshot(principal),
                        },
                        ensure_ascii=False,
                    )
                )
            else:
                if not principal.is_worker or not principal.can_access_agent(name):
                    await ws.send_text(
                        json.dumps({"type": "error", "error": f"Token not authorized for node '{name}'"})
                    )
                    await ws.close()
                    return
                agent_name = name
                for old_ws, old_name in list(state.connected_agents.items()):
                    if old_name == name and old_ws != ws:
                        try:
                            await old_ws.send_text(
                                json.dumps({"type": "replaced", "message": f"{name} reconnected elsewhere"})
                            )
                            await old_ws.close()
                        except Exception:
                            pass
                        state.connected_agents.pop(old_ws, None)
                        state.ws_token_ids.pop(old_ws, None)

                state.ensure_agent(name, role="agent", meta=meta)
                state.connected_agents[ws] = name
                state.ws_token_ids[ws] = principal.token_id
                await ws.send_text(
                    json.dumps(
                        {"type": "registered", "agent": name, "role": "agent", "ok": True},
                        ensure_ascii=False,
                    )
                )
                await state.broadcast_agents_update()

            while True:
                data = await asyncio.wait_for(ws.receive_text(), timeout=300)
                msg = json.loads(data)
                msg_type = msg.get("type")

                if msg_type == "heartbeat":
                    if role != "human" and agent_name in state.agents:
                        state.agents[agent_name]["last_seen"] = time.time()
                        state.agents[agent_name]["stats"].cleanup()
                    await ws.send_text(json.dumps({"type": "heartbeat_ack", "ok": True}))
                    continue

                if msg_type == "status" and role != "human" and agent_name:
                    state_str = (msg.get("state") or "idle").strip()
                    tid = msg.get("thread_id")
                    detail = (msg.get("detail") or "")[:120]
                    if agent_name in state.agents:
                        if state_str == "working":
                            state.agents[agent_name]["working"] = {
                                "thread_id": tid,
                                "detail": detail,
                                "started_at": datetime.now(timezone.utc).isoformat(),
                            }
                        else:
                            state.agents[agent_name]["working"] = None
                    await state.send_to_agents_and_humans(
                        {
                            "type": "agent_status",
                            "agent": agent_name,
                            "state": state_str,
                            "thread_id": tid,
                            "detail": detail,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        },
                        agent_scope=agent_name,
                    )
                    continue

                if msg_type == "stream_chunk" and role != "human" and agent_name:
                    delta = str(msg.get("delta") or "")[:4000]
                    if not delta:
                        continue
                    tid = msg.get("thread_id")
                    stream_id = (msg.get("stream_id") or "").strip()
                    seq = int(msg.get("seq") or 0)
                    if agent_name in state.agents and state.agents[agent_name].get("working"):
                        prev = state.agents[agent_name]["working"].get("preview", "")
                        state.agents[agent_name]["working"]["preview"] = (prev + delta)[-2000:]
                    target_thread = state.threads.get(tid) if tid else None
                    await state.send_to_agents_and_humans(
                        {
                            "type": "agent_stream",
                            "agent": agent_name,
                            "stream_id": stream_id,
                            "thread_id": tid,
                            "delta": delta,
                            "seq": seq,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        },
                        thread=target_thread,
                        agent_scope=agent_name,
                    )
                    continue

                if msg_type == "approval_request" and role != "human" and agent_name:
                    tid = (msg.get("thread_id") or "").strip() or None
                    req_id = (msg.get("request_id") or "").strip()
                    action = (msg.get("action") or "high-risk-action").strip()[:80]
                    summary = (msg.get("summary") or "").strip()[:500]
                    payload_obj = msg.get("payload") if isinstance(msg.get("payload"), dict) else {}
                    approval_id = f"apr_{int(time.time())}_{secrets.token_hex(4)}"
                    record = await state.db.run(
                        state.db.insert_approval_sync,
                        approval_id,
                        tid,
                        agent_name,
                        agent_name,
                        action,
                        summary,
                        payload_obj,
                    )
                    state.approval_worker_requests[approval_id] = (agent_name, req_id)
                    await ws.send_text(
                        json.dumps(
                            {
                                "type": "approval_registered",
                                "approval_id": approval_id,
                                "request_id": req_id,
                                "status": "pending",
                            },
                            ensure_ascii=False,
                        )
                    )
                    target_thread = state.threads.get(tid) if tid else None
                    await state.send_to_agents_and_humans(
                        {"type": "approval_created", "approval": record},
                        thread=target_thread,
                        agent_scope=agent_name,
                    )
                    if target_thread:
                        await state.broadcast_message(
                            "system",
                            f"[需管理员审批 #{approval_id}] @{agent_name} 触发高危策略（{action}）：{summary}",
                            [],
                            tid,
                            role="system",
                            thread=target_thread,
                        )
                    continue

                if msg_type == "message":
                    if role == "human":
                        sender_name = (msg.get("sender") or agent_name or "user").strip()
                        await state.handle_message(
                            sender_name,
                            msg,
                            sender_role="human",
                            principal=principal,
                        )
                    else:
                        await state.handle_message(agent_name, msg, sender_role="agent")
                    continue

                if msg_type == "reply" and role != "human" and agent_name:
                    await state.handle_agent_reply(agent_name, msg)
                    continue

                if msg_type == "control_ack":
                    log.info(
                        f"Control ack from {agent_name}: action={msg.get('action')} "
                        f"ok={msg.get('ok')} detail={msg.get('detail', '')}"
                    )
                    continue

        except (WebSocketDisconnect, asyncio.TimeoutError):
            pass
        except Exception as exc:
            log.error(f"WebSocket error for {agent_name} ({role}): {exc}")
        finally:
            was_agent = ws in state.connected_agents
            state.connected_agents.pop(ws, None)
            state.connected_humans.pop(ws, None)
            state.ws_token_ids.pop(ws, None)
            if was_agent and agent_name and agent_name in state.agents:
                if state.get_agent_ws(agent_name) is None:
                    state.agents[agent_name]["working"] = None
                    await state.send_to_agents_and_humans(
                        {
                            "type": "agent_status",
                            "agent": agent_name,
                            "state": "idle",
                            "thread_id": None,
                            "detail": "",
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        },
                        agent_scope=agent_name,
                    )
                await state.broadcast_agents_update()

    return app


app = create_app()
