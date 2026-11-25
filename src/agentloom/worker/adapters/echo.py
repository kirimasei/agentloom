"""Built-in Echo adapter for connectivity smoke tests and E2E verification."""

from __future__ import annotations

from typing import Optional

from agentloom.worker.adapters import BaseAdapter


class EchoAdapter(BaseAdapter):
    adapter_name = "echo"

    async def invoke(self, prompt: str, thread_id: str, sender: str, raw_msg: dict) -> Optional[str]:
        content = (raw_msg.get("content") or "").strip()
        if "echo:" in content:
            return content.split("echo:", 1)[1].strip()
        return f"[echo] received from {sender} in {thread_id}: {content}"
