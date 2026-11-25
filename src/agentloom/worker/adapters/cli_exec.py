"""Generic CLI coding-agent adapter (Claude Code, Codex, OpenCode, or custom)."""

from __future__ import annotations

import asyncio
import logging
import shlex
from typing import Optional

from agentloom.worker.adapters import BaseAdapter

log = logging.getLogger("agentloom.worker.cli")

CLI_PRESETS: dict[str, list[str]] = {
    "claude": ["claude", "-p", "{prompt}"],
    "codex": ["codex", "exec", "{prompt}"],
    "opencode": ["opencode", "run", "{prompt}"],
}


class CLIExecAdapter(BaseAdapter):
    def __init__(
        self,
        command: Optional[str] = None,
        preset: str = "claude",
        timeout: int = 300,
    ):
        self.timeout = timeout
        if command and command.strip():
            self.argv_template = shlex.split(command.strip())
            self.adapter_name = f"cli:{self.argv_template[0]}"
        else:
            preset_key = (preset or "claude").strip().lower()
            if preset_key not in CLI_PRESETS:
                raise ValueError(f"Unknown CLI preset '{preset}' and no --cli-command provided")
            self.argv_template = list(CLI_PRESETS[preset_key])
            self.adapter_name = f"cli:{preset_key}"

    async def invoke(self, prompt: str, thread_id: str, sender: str, raw_msg: dict) -> Optional[str]:
        has_placeholder = any("{prompt}" in tok for tok in self.argv_template)
        argv = [
            tok.replace("{prompt}", prompt).replace("{thread_id}", thread_id)
            for tok in self.argv_template
        ]
        stdin_bytes = None if has_placeholder else prompt.encode("utf-8")

        try:
            proc = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.PIPE if stdin_bytes is not None else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(input=stdin_bytes),
                timeout=self.timeout,
            )
            out = stdout.decode("utf-8", errors="replace").strip()
            if proc.returncode != 0:
                err_txt = stderr.decode("utf-8", errors="replace").strip()
                log.warning(f"CLI '{argv[0]}' exited {proc.returncode}: {err_txt[:300]}")
                return out or f"[CLI exited {proc.returncode}] {err_txt[:500]}"
            return out or "(empty output)"
        except asyncio.TimeoutError:
            log.warning(f"CLI command timed out after {self.timeout}s")
            try:
                proc.kill()
            except Exception:
                pass
            return None
        except Exception as exc:
            log.error(f"CLI execution error: {exc}")
            return None
