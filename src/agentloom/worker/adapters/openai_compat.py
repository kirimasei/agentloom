"""Generic OpenAI-compatible Chat Completions adapter with optional SSE streaming."""

from __future__ import annotations

import json
import logging
import os
from typing import Optional

import httpx

from agentloom.worker.adapters import BaseAdapter, StreamCallback

log = logging.getLogger("agentloom.worker.openai")


async def call_openai_chat(
    endpoint: str,
    headers: dict[str, str],
    payload: dict,
    timeout: int,
    on_chunk: Optional[StreamCallback] = None,
) -> Optional[str]:
    """Call an OpenAI-compatible /chat/completions endpoint with optional SSE streaming."""
    if on_chunk is None:
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                r = await client.post(endpoint, headers=headers, json={**payload, "stream": False})
                if r.status_code != 200:
                    log.error(f"OpenAI API HTTP {r.status_code}: {r.text[:300]}")
                    return None
                data = r.json()
                choices = data.get("choices") or []
                if choices:
                    return ((choices[0].get("message") or {}).get("content") or "").strip()
                return None
        except Exception as exc:
            log.error(f"OpenAI API call failed: {exc}")
            return None

    chunks: list[str] = []
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(timeout, connect=15.0)) as client:
            async with client.stream("POST", endpoint, headers=headers, json={**payload, "stream": True}) as resp:
                if resp.status_code != 200:
                    body = await resp.aread()
                    log.error(f"OpenAI Stream HTTP {resp.status_code}: {body.decode('utf-8', errors='replace')[:300]}")
                    return None
                ctype = (resp.headers.get("content-type") or "").lower()
                if "text/event-stream" not in ctype:
                    raw = await resp.aread()
                    data = json.loads(raw.decode("utf-8", errors="replace"))
                    choices = data.get("choices") or []
                    text = ((choices[0].get("message") or {}).get("content") or "").strip() if choices else ""
                    if text:
                        await on_chunk(text)
                    return text or None

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
                    choices = ev.get("choices") or []
                    if not choices:
                        continue
                    delta = ((choices[0].get("delta") or {}).get("content")) or ""
                    if delta:
                        chunks.append(delta)
                        await on_chunk(delta)
        full = "".join(chunks).strip()
        return full or None
    except Exception as exc:
        log.error(f"OpenAI stream call failed: {exc}")
        return None


class OpenAICompatAdapter(BaseAdapter):
    adapter_name = "openai-compat"

    def __init__(
        self,
        api_url: str = "http://127.0.0.1:8000",
        api_key: Optional[str] = None,
        model: str = "gpt-4o",
        system_prompt: str = "",
        timeout: int = 300,
    ):
        self.api_url = (api_url or "http://127.0.0.1:8000").rstrip("/")
        self.api_key = (api_key or os.environ.get("OPENAI_API_KEY") or "").strip() or None
        self.model = model
        self.system_prompt = system_prompt
        self.timeout = timeout

    async def invoke(
        self,
        prompt: str,
        thread_id: str,
        sender: str,
        raw_msg: dict,
        on_chunk: Optional[StreamCallback] = None,
    ) -> Optional[str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        messages = []
        if self.system_prompt:
            messages.append({"role": "system", "content": self.system_prompt})
        messages.append({"role": "user", "content": prompt})

        endpoint = self.api_url if self.api_url.endswith("/chat/completions") else f"{self.api_url}/v1/chat/completions"
        if self.api_url.endswith("/v1"):
            endpoint = f"{self.api_url}/chat/completions"

        payload = {
            "model": self.model,
            "messages": messages,
            "user": f"agentloom:{thread_id}",
        }
        return await call_openai_chat(endpoint, headers, payload, self.timeout, on_chunk=on_chunk)
