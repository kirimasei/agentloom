"""Unified outbound WebSocket WorkerRunner for AgentLoom with streaming and HITL approval gates."""

from __future__ import annotations

import asyncio
import logging
import re
import secrets
import signal
import time
from typing import Optional

import websockets

from agentloom import __version__
from agentloom.protocol import extract_mentions, to_ws_url
from agentloom.worker.adapters import BaseAdapter

log = logging.getLogger("agentloom.worker")


class WorkerRunner:
    """Maintains an outbound WebSocket connection to an AgentLoom Hub."""

    def __init__(
        self,
        name: str,
        server_url: str,
        token: str,
        adapter: BaseAdapter,
        region: str = "",
        color: str = "",
        mono: str = "",
        capabilities: Optional[list[str]] = None,
        approval_patterns: Optional[list[str]] = None,
        approval_timeout: int = 180,
        timeout: int = 300,
        max_concurrent: int = 1,
        dry_run: bool = False,
        verbose: bool = False,
    ):
        self.name = name.strip()
        self.server_url = to_ws_url(server_url)
        self.token = token.strip()
        self.adapter = adapter
        self.region = region.strip()
        self.color = color.strip()
        self.mono = mono.strip()[:2].upper()
        self.capabilities = [c.strip() for c in (capabilities or []) if c.strip()]
        self.approval_patterns = [p.strip() for p in (approval_patterns or []) if p and p.strip()]
        self.approval_timeout = approval_timeout
        self.timeout = timeout
        self.dry_run = dry_run

        if verbose:
            logging.getLogger("agentloom").setLevel(logging.DEBUG)

        self.ws = None
        self.connected = False
        self._running = True
        self._was_replaced = False
        self._restart_requested = False
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self._pending_approvals: dict[str, asyncio.Future] = {}
        self._approval_ids: dict[str, str] = {}

    def stop(self) -> None:
        self._running = False

    def _build_meta(self) -> dict:
        meta = self.adapter.build_meta()
        meta["version"] = __version__
        if self.region:
            meta["region"] = self.region
        if self.color:
            meta["color"] = self.color
        if self.mono:
            meta["mono"] = self.mono
        if self.capabilities:
            meta["capabilities"] = self.capabilities
        if self.approval_patterns:
            meta["approval_gated"] = True
        return meta

    def _match_approval_pattern(self, content: str) -> Optional[str]:
        text = content or ""
        for pat in self.approval_patterns:
            try:
                if re.search(pat, text, re.IGNORECASE):
                    return pat
            except re.error:
                if pat.lower() in text.lower():
                    return pat
        return None

    async def connect(self, register_signals: bool = True) -> None:
        if register_signals:
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    signal.signal(sig, lambda _s, _f: self.stop())
                except (ValueError, OSError):
                    pass

        backoff = 1
        while self._running:
            heartbeat_task = None
            self._was_replaced = False
            try:
                log.info(f"Connecting to {self.server_url} as '{self.name}' ({self.adapter.adapter_name})...")
                async with websockets.connect(
                    self.server_url,
                    ping_interval=30,
                    ping_timeout=20,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    self.ws = ws
                    self.connected = True
                    backoff = 1

                    import json

                    await ws.send(
                        json.dumps(
                            {
                                "type": "register",
                                "agent_name": self.name,
                                "role": "agent",
                                "token": self.token,
                                "meta": self._build_meta(),
                            },
                            ensure_ascii=False,
                        )
                    )

                    resp = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
                    if resp.get("type") == "registered":
                        log.info(f"Registered as: {resp.get('agent')}")
                    else:
                        log.error(f"Registration failed: {resp}")
                        await asyncio.sleep(backoff)
                        continue

                    heartbeat_task = asyncio.create_task(self.heartbeat_loop())
                    await self.message_loop()

            except websockets.exceptions.ConnectionClosed as exc:
                log.warning(f"Connection closed: {exc}")
            except Exception as exc:
                log.error(f"Connection error: {exc}")
            finally:
                if heartbeat_task:
                    heartbeat_task.cancel()
                self.connected = False
                self.ws = None

            if not self._running:
                break
            if self._was_replaced:
                log.warning("Replaced by another connection; waiting 15s before retry...")
                await asyncio.sleep(15)
            else:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def send_json(self, payload: dict) -> bool:
        import json

        if self.ws and self.connected:
            try:
                await self.ws.send(json.dumps(payload, ensure_ascii=False))
                return True
            except Exception:
                return False
        return False

    async def send_status(self, state: str, thread_id: str = "", detail: str = "") -> None:
        await self.send_json(
            {
                "type": "status",
                "state": state,
                "thread_id": thread_id,
                "detail": detail,
            }
        )

    async def heartbeat_loop(self) -> None:
        while self._running and self.ws and self.connected:
            try:
                await self.send_json({"type": "heartbeat"})
                await asyncio.sleep(25)
            except Exception:
                break

    async def trigger_graceful_restart(self, reason: str) -> None:
        async with self.semaphore:
            log.info(f"Graceful restart triggered ({reason}); stopping worker loop...")
            self._restart_requested = True
            self._running = False
            if self.ws:
                try:
                    await self.ws.close()
                except Exception:
                    pass

    async def handle_control(self, msg: dict) -> None:
        action = (msg.get("action") or "").strip()
        log.info(f"Received control command: {action}")
        if action == "restart":
            await self.send_json(
                {
                    "type": "control_ack",
                    "action": "restart",
                    "ok": True,
                    "detail": "restarting",
                }
            )
            await self.trigger_graceful_restart("control:restart")

    async def message_loop(self) -> None:
        import json

        while self._running and self.ws:
            try:
                data = await asyncio.wait_for(self.ws.recv(), timeout=300)
                msg = json.loads(data)
                msg_type = msg.get("type")

                if msg_type in (
                    "heartbeat_ack",
                    "agents_update",
                    "broadcast",
                    "thread_update",
                    "agent_status",
                    "agent_stream",
                    "artifact_added",
                    "approval_created",
                    "approval_updated",
                ):
                    continue
                elif msg_type == "approval_registered":
                    req_id = msg.get("request_id") or ""
                    apr_id = msg.get("approval_id") or ""
                    if req_id and apr_id:
                        self._approval_ids[req_id] = apr_id
                elif msg_type == "approval_decision":
                    req_id = msg.get("request_id") or ""
                    fut = self._pending_approvals.get(req_id)
                    if fut and not fut.done():
                        fut.set_result(msg)
                elif msg_type == "message_for_you":
                    asyncio.create_task(self.process_message(msg))
                elif msg_type == "control":
                    asyncio.create_task(self.handle_control(msg))
                elif msg_type == "replaced":
                    self._was_replaced = True
                    break
                elif msg_type == "error":
                    log.error(f"Server error: {msg.get('error')}")
            except asyncio.TimeoutError:
                continue
            except websockets.exceptions.ConnectionClosed:
                break
            except Exception as exc:
                log.error(f"Message loop error: {exc}")
                await asyncio.sleep(1)

    async def _request_approval_and_wait(
        self,
        thread_id: str,
        sender: str,
        matched_pattern: str,
        content: str,
    ) -> tuple[bool, str]:
        req_id = f"req_{secrets.token_hex(4)}"
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._pending_approvals[req_id] = fut
        try:
            sent = await self.send_json(
                {
                    "type": "approval_request",
                    "request_id": req_id,
                    "thread_id": thread_id,
                    "action": f"pattern:{matched_pattern}",
                    "summary": f"Message from {sender} matched approval rule '{matched_pattern}': {content[:240]}",
                    "payload": {"pattern": matched_pattern, "sender": sender},
                }
            )
            if not sent:
                return False, "Cannot submit approval request while disconnected."

            decision_msg = await asyncio.wait_for(fut, timeout=self.approval_timeout)
            apr_id = decision_msg.get("approval_id") or self._approval_ids.get(req_id, "")
            decision = decision_msg.get("decision")
            note = (decision_msg.get("note") or "").strip()
            decided_by = decision_msg.get("decided_by") or "admin"
            if decision == "approved":
                return True, f"Approved ({apr_id}) by {decided_by}"
            return False, (
                f"[审批被拒绝 #{apr_id}] 管理员 {decided_by} 拒绝执行命中高危规则 `{matched_pattern}` 的任务"
                + (f"：{note}" if note else "。")
            )
        except asyncio.TimeoutError:
            apr_id = self._approval_ids.get(req_id, "")
            return False, (
                f"[审批超时 #{apr_id}] 命中高危规则 `{matched_pattern}` 的任务在 {self.approval_timeout}s 内未获管理员批准，已取消执行。"
            )
        finally:
            self._pending_approvals.pop(req_id, None)
            self._approval_ids.pop(req_id, None)

    def build_prompt(self, msg: dict) -> str:
        sender = msg.get("sender", "user")
        content = msg.get("content", "")
        thread_id = msg.get("thread_id", "")
        thread_info = msg.get("thread_info") or {}
        recent = msg.get("recent_messages") or []

        history_lines = [
            f"[{m.get('sender')}]: {m.get('content', '')[:800]}"
            for m in recent[:-1]
        ]
        history_block = "\n".join(history_lines) if history_lines else "(no prior messages)"

        return (
            f"You are remote worker node '{self.name}' ({self.adapter.adapter_name}) on AgentLoom.\n"
            f"Thread: {thread_id} (participants: {', '.join(thread_info.get('participants', []))})\n\n"
            f"[Recent History]\n{history_block}\n\n"
            f"[Current Message from {sender}]\n{content}\n\n"
            f"[Rules]\n"
            f"1. Execute requested checks/tasks directly and report verified results.\n"
            f"2. Be concise and factual.\n"
            f"3. Only @mention another node when handoff is required; never @mention yourself (@{self.name})."
        )

    async def process_message(self, msg: dict) -> None:
        async with self.semaphore:
            thread_id = msg.get("thread_id", "")
            sender = msg.get("sender", "user")
            content = msg.get("content", "")
            prompt = self.build_prompt(msg)
            had_error = False
            stream_id = f"str_{secrets.token_hex(4)}"

            matched_pat = self._match_approval_pattern(content)
            if matched_pat:
                await self.send_status("working", thread_id, f"Waiting approval ({matched_pat})...")
                approved, approval_note = await self._request_approval_and_wait(
                    thread_id, sender, matched_pat, content
                )
                if not approved:
                    await self.send_json(
                        {
                            "type": "reply",
                            "thread_id": thread_id,
                            "content": approval_note,
                            "mentions": [],
                        }
                    )
                    await self.send_status("idle", thread_id)
                    return

            await self.send_status("working", thread_id, "Running...")

            stream_buf: list[str] = []
            last_flush = 0.0
            seq_num = 0

            async def _flush_stream() -> None:
                nonlocal last_flush, seq_num
                if not stream_buf:
                    return
                chunk_text = "".join(stream_buf)
                stream_buf.clear()
                last_flush = time.monotonic()
                seq_num += 1
                await self.send_json(
                    {
                        "type": "stream_chunk",
                        "stream_id": stream_id,
                        "thread_id": thread_id,
                        "delta": chunk_text,
                        "seq": seq_num,
                    }
                )

            async def on_chunk(delta: str) -> None:
                if not delta:
                    return
                stream_buf.append(delta)
                buffered_len = sum(len(x) for x in stream_buf)
                if buffered_len >= 32 or (time.monotonic() - last_flush) >= 0.05:
                    await _flush_stream()

            try:
                if self.dry_run:
                    reply = f"[DRY RUN @ {self.name}] received message from {sender}"
                else:
                    reply = await self.adapter.invoke(
                        prompt,
                        thread_id,
                        sender,
                        msg,
                        on_chunk=on_chunk,
                    )
                    await _flush_stream()
                    if not reply:
                        reply = f"[Error] {self.name} failed or timed out (>{self.timeout}s)"
                        had_error = True

                reply = re.sub(r"\n{3,}", "\n\n", reply.strip())
                if len(reply) > 8000:
                    reply = reply[:8000] + "\n...[truncated]"

                new_mentions = extract_mentions(reply, exclude=self.name)

                if not (self.ws and self.connected):
                    for _ in range(20):
                        if self.ws and self.connected:
                            break
                        await asyncio.sleep(0.5)

                sent = await self.send_json(
                    {
                        "type": "reply",
                        "thread_id": thread_id,
                        "stream_id": stream_id,
                        "content": reply,
                        "mentions": new_mentions,
                    }
                )
                if not sent:
                    had_error = True
                    log.error("Cannot send reply: WebSocket disconnected past grace window")
            except Exception as exc:
                had_error = True
                log.error(f"process_message error: {exc}")
            finally:
                await self.send_status("error" if had_error else "idle", thread_id)
