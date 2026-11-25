"""Unified outbound WebSocket WorkerRunner for AgentLoom with streaming and HITL approval gates."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import mimetypes
import os
import re
import secrets
import signal
import tempfile
import time
from typing import Optional

import httpx
import websockets

from agentloom import __version__
from agentloom.protocol import (
    ARTIFACT_ID_PATTERN,
    ARTIFACT_UPLOAD_PATTERN,
    extract_artifact_upload_directives,
    extract_mentions,
    is_sensitive_artifact_path,
    sanitize_artifact_filename,
    to_http_url,
    to_ws_url,
)
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
        max_artifact_mb: int = 10,
        artifact_inbox_dir: Optional[str] = None,
        auto_download_artifacts: bool = True,
    ):
        self.name = name.strip()
        self.server_url = to_ws_url(server_url)
        self.http_base_url = to_http_url(self.server_url)
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
        self.max_artifact_mb = max(1, int(max_artifact_mb))
        self.artifact_inbox_dir = (
            os.path.abspath(os.path.expanduser(artifact_inbox_dir))
            if artifact_inbox_dir
            else os.path.join(tempfile.gettempdir(), "agentloom-inbox")
        )
        self.auto_download_artifacts = auto_download_artifacts

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
        meta["artifact_bridge"] = True
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

    async def _prepare_incoming_artifacts(self, msg: dict) -> list[dict]:
        """Auto-download artifacts referenced in the message or attached to the thread."""
        if not self.auto_download_artifacts or not self.token or not self.http_base_url:
            return []

        thread_id = (msg.get("thread_id") or "").strip() or "default"
        content = msg.get("content") or ""
        candidate_ids: list[str] = []
        seen: set[str] = set()

        for aid in ARTIFACT_ID_PATTERN.findall(content):
            if aid not in seen:
                seen.add(aid)
                candidate_ids.append(aid)

        thread_arts = msg.get("thread_artifacts") or []
        remote_recent = [a for a in thread_arts if a.get("uploader") != self.name][-3:]
        for art in remote_recent:
            aid = (art.get("id") or "").strip()
            if aid and aid not in seen:
                seen.add(aid)
                candidate_ids.append(aid)

        if not candidate_ids:
            return []

        results: list[dict] = []
        max_bytes = self.max_artifact_mb * 1024 * 1024
        headers = {"X-Token": self.token, "Authorization": f"Bearer {self.token}"}
        inbox_dir = os.path.join(self.artifact_inbox_dir, sanitize_artifact_filename(thread_id))

        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                for aid in candidate_ids:
                    try:
                        meta_resp = await client.get(f"{self.http_base_url}/api/artifacts/{aid}", headers=headers)
                        if meta_resp.status_code != 200:
                            log.warning(f"Skip artifact {aid}: HTTP {meta_resp.status_code}")
                            continue
                        meta = meta_resp.json()
                        size_bytes = int(meta.get("size_bytes") or 0)
                        if size_bytes > max_bytes:
                            log.warning(f"Skip artifact {aid}: {size_bytes} B exceeds {self.max_artifact_mb} MB limit")
                            continue

                        safe_name = sanitize_artifact_filename(meta.get("filename") or f"{aid}.bin")
                        local_path = os.path.join(inbox_dir, f"{aid}_{safe_name}")
                        expected_sha = (meta.get("sha256") or "").lower()

                        if os.path.isfile(local_path) and expected_sha:
                            with open(local_path, "rb") as f:
                                cur_sha = hashlib.sha256(f.read()).hexdigest().lower()
                            if cur_sha == expected_sha:
                                results.append(
                                    {
                                        "id": aid,
                                        "filename": safe_name,
                                        "size": size_bytes,
                                        "local_path": local_path,
                                    }
                                )
                                continue

                        dl_resp = await client.get(
                            f"{self.http_base_url}/api/artifacts/{aid}/download",
                            headers=headers,
                        )
                        if dl_resp.status_code != 200:
                            log.warning(f"Download artifact {aid} failed: HTTP {dl_resp.status_code}")
                            continue
                        raw_bytes = dl_resp.content
                        if len(raw_bytes) > max_bytes:
                            log.warning(f"Downloaded artifact {aid} exceeds {self.max_artifact_mb} MB limit")
                            continue
                        actual_sha = hashlib.sha256(raw_bytes).hexdigest().lower()
                        if expected_sha and actual_sha != expected_sha:
                            log.warning(f"SHA-256 mismatch for artifact {aid}")
                            continue

                        os.makedirs(inbox_dir, exist_ok=True)
                        with open(local_path, "wb") as f:
                            f.write(raw_bytes)
                        results.append(
                            {
                                "id": aid,
                                "filename": safe_name,
                                "size": len(raw_bytes),
                                "local_path": local_path,
                            }
                        )
                    except Exception as exc:
                        log.warning(f"Failed preparing artifact {aid}: {exc}")
        except Exception as exc:
            log.warning(f"Artifact inbox client error: {exc}")

        return results

    async def _process_upload_directives(self, reply: str, thread_id: str) -> tuple[str, list[str]]:
        """Upload files referenced via [[ARTIFACT_UPLOAD: ...]] outside code blocks and replace with receipts."""
        directives = extract_artifact_upload_directives(reply)
        if not directives:
            return reply, []

        max_bytes = self.max_artifact_mb * 1024 * 1024
        headers = {
            "X-Token": self.token,
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }
        outcomes: dict[tuple[str, Optional[str]], str] = {}
        receipt_lines: list[str] = []

        async with httpx.AsyncClient(timeout=120.0) as client:
            for raw_path, custom_name in directives:
                expanded = os.path.abspath(os.path.expanduser(raw_path))
                deny_reason = is_sensitive_artifact_path(expanded)
                if deny_reason:
                    line = f"⚠️ [Artifact blocked/failed] {raw_path}: {deny_reason}"
                    outcomes[(raw_path, custom_name)] = line
                    receipt_lines.append(line)
                    continue

                if not os.path.isfile(expanded):
                    line = f"⚠️ [Artifact blocked/failed] {raw_path}: file does not exist"
                    outcomes[(raw_path, custom_name)] = line
                    receipt_lines.append(line)
                    continue

                size_bytes = os.path.getsize(expanded)
                if size_bytes > max_bytes:
                    line = (
                        f"⚠️ [Artifact blocked/failed] {raw_path}: "
                        f"size {size_bytes} B exceeds {self.max_artifact_mb} MB limit"
                    )
                    outcomes[(raw_path, custom_name)] = line
                    receipt_lines.append(line)
                    continue

                filename = sanitize_artifact_filename(custom_name or os.path.basename(expanded))
                mime_type, _ = mimetypes.guess_type(filename)
                try:
                    with open(expanded, "rb") as f:
                        raw_bytes = f.read()
                    payload = {
                        "filename": filename,
                        "content_base64": base64.b64encode(raw_bytes).decode("ascii"),
                        "thread_id": thread_id or None,
                        "content_type": mime_type or "application/octet-stream",
                    }
                    resp = await client.post(
                        f"{self.http_base_url}/api/artifacts",
                        headers=headers,
                        json=payload,
                    )
                    if resp.status_code >= 400:
                        line = f"⚠️ [Artifact blocked/failed] {raw_path}: HTTP {resp.status_code} {resp.text[:160]}"
                    else:
                        art = (resp.json() or {}).get("artifact") or {}
                        aid = art.get("id", "unknown")
                        sha = (art.get("sha256") or "")[:12]
                        line = f"📎 [Artifact uploaded] {filename} ({len(raw_bytes)} B, id: {aid}, sha256: {sha})"
                except Exception as exc:
                    line = f"⚠️ [Artifact blocked/failed] {raw_path}: {exc}"

                outcomes[(raw_path, custom_name)] = line
                receipt_lines.append(line)

        def _replace_match(m: re.Match) -> str:
            inner = m.group(1)
            if "|" in inner:
                rp, _, rn = inner.partition("|")
                key = (rp.strip().strip('"').strip("'"), rn.strip().strip('"').strip("'") or None)
            else:
                key = (inner.strip().strip('"').strip("'"), None)
            return outcomes.get(key, m.group(0))

        parts = re.split(r"(```[\s\S]*?```)", reply)
        for idx in range(0, len(parts), 2):
            parts[idx] = ARTIFACT_UPLOAD_PATTERN.sub(_replace_match, parts[idx])

        return "".join(parts), receipt_lines

    def build_prompt(self, msg: dict, downloaded_artifacts: Optional[list[dict]] = None) -> str:
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

        artifacts_block = ""
        if downloaded_artifacts:
            art_lines = [
                f"- {a['id']} ({a['filename']}, {a['size']} B) -> {a['local_path']}"
                for a in downloaded_artifacts
            ]
            artifacts_block = "\n[Downloaded Thread Artifacts]\n" + "\n".join(art_lines) + "\n"

        return (
            f"You are remote worker node '{self.name}' ({self.adapter.adapter_name}) on AgentLoom.\n"
            f"Thread: {thread_id} (participants: {', '.join(thread_info.get('participants', []))})\n\n"
            f"[Recent History]\n{history_block}\n\n"
            f"[Current Message from {sender}]\n{content}\n"
            f"{artifacts_block}\n"
            f"[Rules]\n"
            f"1. Execute requested checks/tasks directly and report verified results.\n"
            f"2. Be concise and factual.\n"
            f"3. Only @mention another node when handoff is required; never @mention yourself (@{self.name}).\n"
            f"4. To send files/archives back (bypassing the 8000-char reply limit), write them to disk (e.g. /tmp/out.tar.xz) and put `[[ARTIFACT_UPLOAD: /path/to/file]]` (or `[[ARTIFACT_UPLOAD: /path/to/file | custom_name.ext]]`) on its own line in your reply; do NOT delete the file in the same command and never upload or read any token/secret files."
        )

    async def process_message(self, msg: dict) -> None:
        async with self.semaphore:
            thread_id = msg.get("thread_id", "")
            sender = msg.get("sender", "user")
            content = msg.get("content", "")
            downloaded_artifacts = await self._prepare_incoming_artifacts(msg)
            prompt = self.build_prompt(msg, downloaded_artifacts=downloaded_artifacts)
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

                receipt_lines: list[str] = []
                if not had_error:
                    reply, receipt_lines = await self._process_upload_directives(reply, thread_id)

                reply = re.sub(r"\n{3,}", "\n\n", reply.strip())
                if len(reply) > 8000:
                    receipt_footer = "\n\n" + "\n".join(receipt_lines) if receipt_lines else ""
                    max_body = max(1000, 8000 - len(receipt_footer) - 20)
                    reply = reply[:max_body] + "\n...[truncated]" + receipt_footer

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
