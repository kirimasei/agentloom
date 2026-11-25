"""Built-in Echo adapter for connectivity smoke tests, streaming demos, and E2E verification."""

from __future__ import annotations

import asyncio
from typing import Optional

from agentloom.worker.adapters import BaseAdapter, StreamCallback


class EchoAdapter(BaseAdapter):
    adapter_name = "echo"

    async def invoke(
        self,
        prompt: str,
        thread_id: str,
        sender: str,
        raw_msg: dict,
        on_chunk: Optional[StreamCallback] = None,
    ) -> Optional[str]:
        content = (raw_msg.get("content") or "").strip()
        if "stream:" in content:
            parts = [p for p in content.split("stream:", 1)[1].split("|") if p]
            if parts:
                for idx, part in enumerate(parts):
                    if on_chunk:
                        await on_chunk(part)
                    if idx < len(parts) - 1:
                        await asyncio.sleep(0.06)
                return "".join(parts).strip()
        if "echo:" in content:
            text = content.split("echo:", 1)[1].strip()
            if on_chunk and text:
                await on_chunk(text)
            return text
        text = f"[echo] received from {sender} in {thread_id}: {content}"
        if on_chunk:
            await on_chunk(text)
        return text
