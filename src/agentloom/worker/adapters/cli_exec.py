"""Generic CLI coding-agent adapter (Claude Code, Codex, OpenCode, or custom)."""

from __future__ import annotations

import asyncio
import logging
import shlex
from typing import Optional

from agentloom.worker.adapters import BaseAdapter, StreamCallback

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

    async def invoke(
        self,
        prompt: str,
        thread_id: str,
        sender: str,
        raw_msg: dict,
        on_chunk: Optional[StreamCallback] = None,
    ) -> Optional[str]:
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
            if on_chunk is None:
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(input=stdin_bytes),
                    timeout=self.timeout,
                )
                out = stdout.decode("utf-8", errors="replace").strip()
                err_txt = stderr.decode("utf-8", errors="replace").strip()
            else:
                if proc.stdin and stdin_bytes is not None:
                    proc.stdin.write(stdin_bytes)
                    await proc.stdin.drain()
                    proc.stdin.close()

                chunks: list[str] = []

                async def _drain() -> str:
                    assert proc.stdout is not None and proc.stderr is not None
                    stderr_task = asyncio.create_task(proc.stderr.read())
                    while True:
                        buf = await proc.stdout.read(1024)
                        if not buf:
                            break
                        text_part = buf.decode("utf-8", errors="replace")
                        chunks.append(text_part)
                        await on_chunk(text_part)
                    err_raw = await stderr_task
                    await proc.wait()
                    return err_raw.decode("utf-8", errors="replace").strip()

                err_txt = await asyncio.wait_for(_drain(), timeout=self.timeout)
                out = "".join(chunks).strip()

            if proc.returncode != 0:
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
