"""Generic OpenAI-compatible Chat Completions adapter."""

from __future__ import annotations

import logging
import os
from typing import Optional

import httpx

from agentloom.worker.adapters import BaseAdapter

log = logging.getLogger("agentloom.worker.openai")


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

    async def invoke(self, prompt: str, thread_id: str, sender: str, raw_msg: dict) -> Optional[str]:
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
            "stream": False,
        }
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                r = await client.post(endpoint, headers=headers, json=payload)
                if r.status_code != 200:
                    log.error(f"OpenAI-compat HTTP {r.status_code}: {r.text[:300]}")
                    return None
                data = r.json()
                choices = data.get("choices") or []
                if choices:
                    return ((choices[0].get("message") or {}).get("content") or "").strip()
                return None
        except Exception as exc:
            log.error(f"OpenAI-compat call failed: {exc}")
            return None
