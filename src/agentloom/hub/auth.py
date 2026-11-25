"""RBAC and node ACL authentication for AgentLoom Hub."""

from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass, field
from typing import Optional

from fastapi import HTTPException, Request

from agentloom.hub.db import HubDatabase

VALID_ROLES = ("admin", "operator", "worker")
ROLE_PREFIX = {
    "admin": "al_adm_",
    "operator": "al_op_",
    "worker": "al_wk_",
}


@dataclass(frozen=True)
class Principal:
    token_id: str
    name: str
    role: str
    allowed_agents: list[str] = field(default_factory=lambda: ["*"])

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"

    @property
    def is_operator(self) -> bool:
        return self.role in ("admin", "operator")

    @property
    def is_worker(self) -> bool:
        return self.role in ("admin", "worker")

    def can_access_agent(self, agent_name: str) -> bool:
        if self.is_admin or "*" in self.allowed_agents:
            return True
        return agent_name in self.allowed_agents

    def can_control_agent(self, agent_name: str) -> bool:
        return self.is_admin

    def to_dict(self) -> dict:
        return {
            "token_id": self.token_id,
            "name": self.name,
            "role": self.role,
            "allowed_agents": self.allowed_agents,
            "can_control": self.is_admin,
        }


def hash_token(raw: str) -> str:
    """Compute UTF-8 safe SHA-256 hex digest."""
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def safe_compare(a: str, b: str) -> bool:
    """UTF-8 byte-safe constant-time comparison (supports non-ASCII safely)."""
    return secrets.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


def extract_request_token(request: Request) -> Optional[str]:
    candidate = (
        request.headers.get("X-Token")
        or request.headers.get("X-Secret")
        or request.query_params.get("token")
        or request.query_params.get("secret")
    )
    if not candidate:
        auth_header = request.headers.get("Authorization", "")
        if auth_header.lower().startswith("bearer "):
            candidate = auth_header[7:].strip()
    return candidate.strip() if candidate else None


class AuthManager:
    def __init__(self, admin_token: str, db: HubDatabase):
        self.admin_token = admin_token.strip()
        self.db = db

    async def authenticate(self, candidate: Optional[str]) -> Optional[Principal]:
        if not candidate:
            return None
        clean = candidate.strip()
        if not clean:
            return None
        if safe_compare(clean, self.admin_token):
            return Principal(
                token_id="root-admin",
                name="admin",
                role="admin",
                allowed_agents=["*"],
            )
        row = await self.db.run(self.db.find_token_by_hash_sync, hash_token(clean))
        if not row:
            return None
        return Principal(
            token_id=row["id"],
            name=row["name"],
            role=row["role"],
            allowed_agents=row["allowed_agents"] or ["*"],
        )

    async def require_authenticated(self, request: Request) -> Principal:
        principal = await self.authenticate(extract_request_token(request))
        if not principal:
            raise HTTPException(status_code=401, detail="Invalid or missing token")
        return principal

    async def require_operator(self, request: Request) -> Principal:
        principal = await self.authenticate(extract_request_token(request))
        if not principal:
            raise HTTPException(status_code=401, detail="Invalid or missing token")
        if not principal.is_operator:
            raise HTTPException(status_code=403, detail="Worker tokens cannot access operator endpoints")
        return principal

    async def require_admin(self, request: Request) -> Principal:
        principal = await self.authenticate(extract_request_token(request))
        if not principal:
            raise HTTPException(status_code=401, detail="Invalid or missing token")
        if not principal.is_admin:
            raise HTTPException(status_code=403, detail="Admin privileges required")
        return principal

    async def create_token(
        self,
        name: str,
        role: str,
        allowed_agents: Optional[list[str]] = None,
    ) -> dict:
        clean_role = (role or "operator").strip().lower()
        if clean_role not in VALID_ROLES:
            raise HTTPException(status_code=400, detail=f"Invalid role: {role}")
        clean_name = (name or "").strip()[:48]
        if not clean_name:
            raise HTTPException(status_code=400, detail="Token name cannot be empty")

        agents_list = [a.strip() for a in (allowed_agents or ["*"]) if a and a.strip()]
        if not agents_list:
            agents_list = ["*"]

        raw_token = f"{ROLE_PREFIX[clean_role]}{secrets.token_hex(20)}"
        token_id = f"tok_{secrets.token_hex(6)}"
        token_prefix = raw_token[:14] + "..."

        record = await self.db.run(
            self.db.insert_token_sync,
            token_id,
            clean_name,
            clean_role,
            hash_token(raw_token),
            token_prefix,
            agents_list,
        )
        record["token"] = raw_token
        return record

    async def list_tokens(self) -> list[dict]:
        return await self.db.run(self.db.list_tokens_sync)

    async def revoke_token(self, token_id: str) -> bool:
        if token_id == "root-admin":
            raise HTTPException(status_code=400, detail="Cannot revoke root admin token")
        return await self.db.run(self.db.revoke_token_sync, token_id)
