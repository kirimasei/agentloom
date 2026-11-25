"""Environment-driven configuration for AgentLoom Hub."""

from __future__ import annotations

import os
import secrets
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class HubConfig:
    host: str = "0.0.0.0"
    port: int = 3000
    data_dir: str = "./data"
    admin_token: str = ""
    max_rounds: int = 30
    max_replies_per_hour: int = 60
    agent_alive_timeout: int = 90
    recent_context_limit: int = 15
    max_artifact_mb: int = 20

    @property
    def db_path(self) -> str:
        return str(Path(self.data_dir) / "agentloom.db")

    @property
    def admin_token_path(self) -> str:
        return str(Path(self.data_dir) / "admin.token")

    @property
    def artifacts_dir(self) -> str:
        return str(Path(self.data_dir) / "artifacts")

    @classmethod
    def from_env(cls) -> "HubConfig":
        return cls(
            host=os.environ.get("AGENTLOOM_HOST", "0.0.0.0").strip() or "0.0.0.0",
            port=int(os.environ.get("AGENTLOOM_PORT", "3000")),
            data_dir=os.environ.get("AGENTLOOM_DATA_DIR", "./data").strip() or "./data",
            admin_token=(
                os.environ.get("AGENTLOOM_ADMIN_TOKEN")
                or os.environ.get("AGENT_CHAT_SECRET")
                or ""
            ).strip(),
            max_rounds=int(os.environ.get("AGENTLOOM_MAX_ROUNDS", "30")),
            max_replies_per_hour=int(os.environ.get("AGENTLOOM_MAX_REPLIES_PER_HOUR", "60")),
            agent_alive_timeout=int(os.environ.get("AGENTLOOM_AGENT_TIMEOUT", "90")),
            recent_context_limit=int(os.environ.get("AGENTLOOM_CONTEXT_LIMIT", "15")),
            max_artifact_mb=int(os.environ.get("AGENTLOOM_MAX_ARTIFACT_MB", "20")),
        )

    def resolve_admin_token(self) -> tuple[str, bool]:
        """Return (admin_token, was_generated). Persists generated tokens under data_dir."""
        if self.admin_token:
            return self.admin_token, False

        os.makedirs(self.data_dir, exist_ok=True)
        tok_path = self.admin_token_path
        if os.path.isfile(tok_path):
            with open(tok_path, "r", encoding="utf-8") as f:
                existing = f.read().strip()
            if existing:
                return existing, False

        generated = f"al_adm_{secrets.token_hex(24)}"
        with open(tok_path, "w", encoding="utf-8") as f:
            f.write(generated)
        try:
            os.chmod(tok_path, 0o600)
        except OSError:
            pass
        return generated, True
