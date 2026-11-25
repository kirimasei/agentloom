"""Base adapter interface and registry for AgentLoom workers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Awaitable, Callable, Optional

StreamCallback = Callable[[str], Awaitable[None]]


class BaseAdapter(ABC):
    """Abstract interface implemented by every worker backend adapter."""

    adapter_name: str = "base"

    @abstractmethod
    async def invoke(
        self,
        prompt: str,
        thread_id: str,
        sender: str,
        raw_msg: dict,
        on_chunk: Optional[StreamCallback] = None,
    ) -> Optional[str]:
        """Execute a prompt against the underlying agent and return its reply text."""

    def build_meta(self) -> dict:
        """Return adapter metadata reported to the Hub upon registration."""
        return {"backend": self.adapter_name}


def create_adapter(kind: str, **kwargs) -> BaseAdapter:
    """Instantiate a built-in adapter by name."""
    from agentloom.worker.adapters.cli_exec import CLIExecAdapter
    from agentloom.worker.adapters.echo import EchoAdapter
    from agentloom.worker.adapters.hermes import HermesAdapter
    from agentloom.worker.adapters.openai_compat import OpenAICompatAdapter
    from agentloom.worker.adapters.qwenpaw import QwenPawAdapter

    normalized = (kind or "hermes").strip().lower()
    timeout = int(kwargs.get("timeout") or 300)

    if normalized == "hermes":
        return HermesAdapter(
            api_url=kwargs.get("api_url"),
            api_key=kwargs.get("api_key"),
            hermes_env=kwargs.get("hermes_env") or "~/.hermes/.env",
            hermes_bin=kwargs.get("hermes_bin") or "hermes",
            timeout=timeout,
        )
    if normalized == "openai":
        return OpenAICompatAdapter(
            api_url=kwargs.get("api_url") or "http://127.0.0.1:8000",
            api_key=kwargs.get("api_key"),
            model=kwargs.get("model") or "gpt-4o",
            system_prompt=kwargs.get("system_prompt") or "",
            timeout=timeout,
        )
    if normalized == "qwenpaw":
        return QwenPawAdapter(
            api_url=kwargs.get("api_url") or "http://127.0.0.1:8088",
            timeout=timeout,
        )
    if normalized == "cli":
        return CLIExecAdapter(
            command=kwargs.get("cli_command"),
            preset=kwargs.get("preset") or "claude",
            timeout=timeout,
        )
    if normalized == "echo":
        return EchoAdapter()

    raise ValueError(f"Unknown adapter '{kind}'. Expected one of: hermes, openai, qwenpaw, cli, echo")
