"""Hermes Agent adapter (HTTP OpenAI-compatible API with CLI fallback)."""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

from agentloom.worker.adapters import BaseAdapter, StreamCallback
from agentloom.worker.adapters.openai_compat import call_openai_chat

log = logging.getLogger("agentloom.worker.hermes")


def load_hermes_env(env_path: str = "~/.hermes/.env") -> dict[str, str]:
    result: dict[str, str] = {}
    expanded = os.path.expanduser(env_path)
    if not os.path.isfile(expanded):
        return result
    try:
        with open(expanded, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                result[k.strip()] = v.strip().strip('"').strip("'")
    except OSError:
        pass
    return result


class HermesAdapter(BaseAdapter):
    adapter_name = "hermes-api"

    def __init__(
        self,
        api_url: Optional[str] = None,
        api_key: Optional[str] = None,
        hermes_env: str = "~/.hermes/.env",
        hermes_bin: str = "hermes",
        timeout: int = 300,
    ):
        env_vars = load_hermes_env(hermes_env)
        resolved_url = (api_url or os.environ.get("HERMES_API_URL") or "").strip()
        if not resolved_url and env_vars.get("API_SERVER_ENABLED", "").lower() in ("true", "1", "yes"):
            port = env_vars.get("API_SERVER_PORT", "8642")
            resolved_url = f"http://127.0.0.1:{port}"

        self.api_url = resolved_url.rstrip("/") if resolved_url else None
        self.api_key = (api_key or os.environ.get("HERMES_API_KEY") or env_vars.get("API_SERVER_KEY") or "").strip() or None
        self.hermes_bin = hermes_bin
        self.timeout = timeout
        self.adapter_name = "hermes-api" if self.api_url else "hermes-cli"

    async def _call_api(
        self,
        prompt: str,
        thread_id: str,
        on_chunk: Optional[StreamCallback] = None,
    ) -> Optional[str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        payload = {
            "model": "hermes-agent",
            "messages": [{"role": "user", "content": prompt}],
            "user": f"agentloom:{thread_id}",
        }
        return await call_openai_chat(
            f"{self.api_url}/v1/chat/completions",
            headers,
            payload,
            self.timeout,
            on_chunk=on_chunk,
        )

    async def _call_cli(self, prompt: str, on_chunk: Optional[StreamCallback] = None) -> Optional[str]:
        cmd = [self.hermes_bin, "chat", "-q", prompt, "-Q"]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=self.timeout)
            out = stdout.decode("utf-8", errors="replace").strip()
            if proc.returncode != 0:
                log.warning(f"Hermes CLI exited {proc.returncode}: {stderr.decode('utf-8', errors='replace')[:200]}")
            if out and on_chunk:
                await on_chunk(out)
            return out or None
        except asyncio.TimeoutError:
            log.warning(f"Hermes CLI timed out after {self.timeout}s")
            try:
                proc.kill()
            except Exception:
                pass
            return None
        except Exception as exc:
            log.error(f"Hermes CLI error: {exc}")
            return None

    async def invoke(
        self,
        prompt: str,
        thread_id: str,
        sender: str,
        raw_msg: dict,
        on_chunk: Optional[StreamCallback] = None,
    ) -> Optional[str]:
        if self.api_url:
            res = await self._call_api(prompt, thread_id, on_chunk=on_chunk)
            if res is not None:
                return res
            log.info("Hermes HTTP API unavailable, falling back to CLI...")
        return await self._call_cli(prompt, on_chunk=on_chunk)
