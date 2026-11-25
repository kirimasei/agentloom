"""Non-blocking SQLite WAL persistence for AgentLoom Hub."""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Optional

from agentloom.protocol import infer_role


class HubDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = asyncio.Lock()

    async def run(self, fn, *args) -> Any:
        async with self._lock:
            return await asyncio.to_thread(fn, *args)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_sync(self) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        conn = self._connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS threads (
                    id TEXT PRIMARY KEY,
                    topic TEXT NOT NULL DEFAULT '',
                    participants TEXT NOT NULL DEFAULT '[]',
                    round_count INTEGER NOT NULL DEFAULT 0,
                    is_closed INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    last_activity TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    thread_id TEXT NOT NULL,
                    sender TEXT NOT NULL,
                    role TEXT NOT NULL DEFAULT '',
                    content TEXT NOT NULL,
                    mentions TEXT NOT NULL DEFAULT '[]',
                    timestamp TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tokens (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    role TEXT NOT NULL,
                    token_hash TEXT UNIQUE NOT NULL,
                    token_prefix TEXT NOT NULL,
                    allowed_agents TEXT NOT NULL DEFAULT '["*"]',
                    created_at TEXT NOT NULL,
                    revoked INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            cols = {row[1] for row in conn.execute("PRAGMA table_info(messages)").fetchall()}
            if "role" not in cols:
                conn.execute("ALTER TABLE messages ADD COLUMN role TEXT NOT NULL DEFAULT ''")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_thread ON messages(thread_id, id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tokens_hash ON tokens(token_hash)")
            conn.commit()
        finally:
            conn.close()

    def load_threads_sync(self, limit: int = 200) -> list[dict]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM threads ORDER BY last_activity DESC LIMIT ?", (limit,)
            ).fetchall()
            result = []
            for row in rows:
                msg_rows = conn.execute(
                    "SELECT sender, role, content, mentions, timestamp FROM messages WHERE thread_id=? ORDER BY id ASC",
                    (row["id"],),
                ).fetchall()
                msgs = []
                for mr in msg_rows:
                    sender = mr["sender"]
                    msgs.append(
                        {
                            "sender": sender,
                            "role": (mr["role"] or "").strip() or infer_role(sender),
                            "content": mr["content"],
                            "mentions": json.loads(mr["mentions"] or "[]"),
                            "timestamp": mr["timestamp"],
                        }
                    )
                result.append(
                    {
                        "id": row["id"],
                        "topic": row["topic"],
                        "participants": json.loads(row["participants"] or "[]"),
                        "round_count": row["round_count"],
                        "is_closed": bool(row["is_closed"]),
                        "created_at": row["created_at"],
                        "last_activity": row["last_activity"],
                        "messages": msgs,
                    }
                )
            return result
        finally:
            conn.close()

    def save_thread_sync(self, thread_snap: tuple) -> None:
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO threads (id, topic, participants, round_count, is_closed, created_at, last_activity)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    topic=excluded.topic,
                    participants=excluded.participants,
                    round_count=excluded.round_count,
                    is_closed=excluded.is_closed,
                    last_activity=excluded.last_activity
                """,
                thread_snap,
            )
            conn.commit()
        finally:
            conn.close()

    def save_thread_and_message_sync(self, thread_snap: tuple, thread_id: str, msg_obj: dict) -> None:
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO threads (id, topic, participants, round_count, is_closed, created_at, last_activity)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    topic=excluded.topic,
                    participants=excluded.participants,
                    round_count=excluded.round_count,
                    is_closed=excluded.is_closed,
                    last_activity=excluded.last_activity
                """,
                thread_snap,
            )
            conn.execute(
                """
                INSERT INTO messages (thread_id, sender, role, content, mentions, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    thread_id,
                    msg_obj["sender"],
                    msg_obj.get("role", infer_role(msg_obj["sender"])),
                    msg_obj["content"],
                    json.dumps(msg_obj["mentions"], ensure_ascii=False),
                    msg_obj["timestamp"],
                ),
            )
            conn.commit()
        finally:
            conn.close()

    # ── Token Persistence ─────────────────────────────────────────────
    def insert_token_sync(
        self,
        token_id: str,
        name: str,
        role: str,
        token_hash: str,
        token_prefix: str,
        allowed_agents: list[str],
    ) -> dict:
        created_at = datetime.now(timezone.utc).isoformat()
        conn = self._connect()
        try:
            conn.execute(
                """
                INSERT INTO tokens (id, name, role, token_hash, token_prefix, allowed_agents, created_at, revoked)
                VALUES (?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    token_id,
                    name,
                    role,
                    token_hash,
                    token_prefix,
                    json.dumps(allowed_agents, ensure_ascii=False),
                    created_at,
                ),
            )
            conn.commit()
        finally:
            conn.close()
        return {
            "id": token_id,
            "name": name,
            "role": role,
            "token_prefix": token_prefix,
            "allowed_agents": allowed_agents,
            "created_at": created_at,
            "revoked": False,
        }

    def list_tokens_sync(self) -> list[dict]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT id, name, role, token_prefix, allowed_agents, created_at, revoked FROM tokens ORDER BY created_at DESC"
            ).fetchall()
            return [
                {
                    "id": r["id"],
                    "name": r["name"],
                    "role": r["role"],
                    "token_prefix": r["token_prefix"],
                    "allowed_agents": json.loads(r["allowed_agents"] or '["*"]'),
                    "created_at": r["created_at"],
                    "revoked": bool(r["revoked"]),
                }
                for r in rows
            ]
        finally:
            conn.close()

    def find_token_by_hash_sync(self, token_hash: str) -> Optional[dict]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT id, name, role, token_prefix, allowed_agents, created_at, revoked FROM tokens WHERE token_hash=?",
                (token_hash,),
            ).fetchone()
            if not row or row["revoked"]:
                return None
            return {
                "id": row["id"],
                "name": row["name"],
                "role": row["role"],
                "token_prefix": row["token_prefix"],
                "allowed_agents": json.loads(row["allowed_agents"] or '["*"]'),
                "created_at": row["created_at"],
                "revoked": bool(row["revoked"]),
            }
        finally:
            conn.close()

    def revoke_token_sync(self, token_id: str) -> bool:
        conn = self._connect()
        try:
            cur = conn.execute("UPDATE tokens SET revoked=1 WHERE id=? AND revoked=0", (token_id,))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()
