"""QwenPaw / AgentScope console SSE adapter."""

from __future__ import annotations

import json
import logging
from typing import Optional

import httpx

from agentloom.worker.adapters import BaseAdapter, StreamCallback

log = logging.getLogger("agentloom.worker.qwenpaw")


def extract_text_from_output(output_items: list) -> str:
    """Extract final assistant text from a completed QwenPaw response payload."""
    parts = []
    for item in output_items or []:
        if not isinstance(item, dict):
            continue
        if item.get("type", "") == "reasoning":
            continue
        role = item.get("role", "")
        if role and role != "assistant":
            continue
        content = item.get("content")
        if isinstance(content, str) and content.strip():
            parts.append(content.strip())
        elif isinstance(content, list):
            for block in content:
                if isinstance(block, dict) and block.get("type") in ("text", "output_text"):
                    txt = block.get("text") or ""
                    if txt:
                        parts.append(txt)
    return "\n".join(parts).strip()


class QwenPawAdapter(BaseAdapter):
    adapter_name = "qwenpaw-sse"

    def __init__(self, api_url: str = "http://127.0.0.1:8088", timeout: int = 300):
        self.api_url = (api_url or "http://127.0.0.1:8088").rstrip("/")
        self.timeout = timeout

    async def invoke(
        self,
        prompt: str,
        thread_id: str,
        sender: str,
        raw_msg: dict,
        on_chunk: Optional[StreamCallback] = None,
    ) -> Optional[str]:
        payload = {
            "input": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
            "session_id": f"agentloom-{thread_id}" if thread_id else "agentloom",
            "stream": True,
        }
        msg_types: dict[str, str] = {}
        msg_texts: dict[str, list[str]] = {}
        msg_order: list[str] = []
        completed_output_text = ""

        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(self.timeout, connect=15.0)) as client:
                async with client.stream(
                    "POST",
                    f"{self.api_url}/api/console/chat",
                    headers={"Content-Type": "application/json", "Accept": "text/event-stream"},
                    json=payload,
                ) as resp:
                    if resp.status_code != 200:
                        body = await resp.aread()
                        log.error(f"QwenPaw HTTP {resp.status_code}: {body.decode('utf-8', errors='replace')[:300]}")
                        return None

                    async for raw_line in resp.aiter_lines():
                        line = raw_line.strip()
                        if not line.startswith("data:"):
                            continue
                        data_str = line[5:].strip()
                        if not data_str or data_str == "[DONE]":
                            continue
                        try:
                            ev = json.loads(data_str)
                        except json.JSONDecodeError:
                            continue
                        if not isinstance(ev, dict):
                            continue

                        if ev.get("object") == "message" and ev.get("id"):
                            mid = ev["id"]
                            msg_types[mid] = ev.get("type", "message")
                            if mid not in msg_order:
                                msg_order.append(mid)

                        if ev.get("type") == "text" and ev.get("delta") and "text" in ev:
                            mid = ev.get("msg_id") or "_default"
                            if mid not in msg_order:
                                msg_order.append(mid)
                            delta_txt = ev.get("text") or ""
                            msg_texts.setdefault(mid, []).append(delta_txt)
                            if delta_txt and on_chunk and msg_types.get(mid) != "reasoning":
                                await on_chunk(delta_txt)

                        if ev.get("object") == "response" and ev.get("status") == "completed":
                            out_txt = extract_text_from_output(ev.get("output") or [])
                            if out_txt:
                                completed_output_text = out_txt

            if completed_output_text:
                return completed_output_text

            non_reasoning = [
                "".join(msg_texts[mid]).strip()
                for mid in msg_order
                if mid in msg_texts and msg_types.get(mid) != "reasoning"
            ]
            non_reasoning = [t for t in non_reasoning if t]
            if non_reasoning:
                return "\n".join(non_reasoning)

            all_parts = ["".join(msg_texts[mid]).strip() for mid in msg_order if mid in msg_texts]
            all_parts = [t for t in all_parts if t]
            return all_parts[-1] if all_parts else None
        except Exception as exc:
            log.error(f"QwenPaw API error: {exc}")
            return None
