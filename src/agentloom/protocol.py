"""Shared zero-dependency protocol helpers for AgentLoom."""

from __future__ import annotations

import os
import re
import shlex
from typing import Optional

MENTION_PATTERN = re.compile(r"(?<![A-Za-z0-9_.-])@([A-Za-z0-9_][\w-]*)")
ARTIFACT_UPLOAD_PATTERN = re.compile(r"^\s*\[\[ARTIFACT_UPLOAD:\s*(.+?)\s*\]\]\s*$", re.MULTILINE)
ARTIFACT_ID_PATTERN = re.compile(r"\b(art_\d+_[0-9a-fA-F]{6,16})\b")

_SENSITIVE_FILENAMES = {
    ".env",
    "auth.json",
    "credentials.json",
    "id_rsa",
    "id_ed25519",
    "authorized_keys",
    "known_hosts",
    "shadow",
    "gshadow",
    "sudoers",
}
_SENSITIVE_EXTENSIONS = (".token", ".pem", ".key", ".p12", ".pfx")
_SENSITIVE_DIR_FRAGMENTS = (
    "/.secrets/",
    "/.ssh/",
    "/.agentloom-worker/",
    "/etc/shadow",
    "/etc/gshadow",
    "/etc/sudoers",
)


def extract_artifact_upload_directives(content: str) -> list[tuple[str, Optional[str]]]:
    """Extract deduplicated [[ARTIFACT_UPLOAD: path | custom_name]] directives outside code blocks."""
    text = re.sub(r"```[\s\S]*?```", " ", content or "")
    seen: set[tuple[str, Optional[str]]] = set()
    directives: list[tuple[str, Optional[str]]] = []
    for raw_inner in ARTIFACT_UPLOAD_PATTERN.findall(text):
        if "|" in raw_inner:
            raw_path, _, raw_name = raw_inner.partition("|")
            path_str = raw_path.strip().strip('"').strip("'")
            custom_name: Optional[str] = raw_name.strip().strip('"').strip("'") or None
        else:
            path_str = raw_inner.strip().strip('"').strip("'")
            custom_name = None
        if not path_str:
            continue
        item = (path_str, custom_name)
        if item not in seen:
            seen.add(item)
            directives.append(item)
    return directives


def sanitize_artifact_filename(filename: str) -> str:
    """Keep basename only and replace unsafe characters for artifact filenames."""
    base = os.path.basename((filename or "").replace("\\", "/")).strip()
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", base).strip("._")
    return cleaned[:120] or "artifact.bin"


def is_sensitive_artifact_path(path: str) -> Optional[str]:
    """Return a denial reason if path points to a credential/secret file, else None."""
    try:
        resolved = os.path.realpath(os.path.expanduser(path))
    except OSError:
        resolved = os.path.abspath(os.path.expanduser(path))
    norm = resolved.replace("\\", "/").lower()
    fname = os.path.basename(norm)

    if fname in _SENSITIVE_FILENAMES or fname.startswith(".env."):
        return f"Refused sensitive file '{fname}'"
    if fname.startswith("id_rsa.") or fname.startswith("id_ed25519."):
        return f"Refused SSH key file '{fname}'"
    if any(fname.endswith(ext) for ext in _SENSITIVE_EXTENSIONS):
        return f"Refused credential extension '{fname}'"
    for frag in _SENSITIVE_DIR_FRAGMENTS:
        if frag in norm or norm.endswith(frag.rstrip("/")):
            return f"Refused sensitive path matching '{frag}'"
    return None


def extract_mentions(content: str, exclude: Optional[str] = None) -> list[str]:
    """Extract deduplicated @mentions while ignoring code blocks, inline code, and emails."""
    text = content or ""
    text = re.sub(r"```[\s\S]*?```", " ", text)
    text = re.sub(r"`[^`\n]*`", " ", text)
    found = MENTION_PATTERN.findall(text)
    seen: set[str] = set()
    res: list[str] = []
    for item in found:
        if exclude and item == exclude:
            continue
        if item not in seen:
            seen.add(item)
            res.append(item)
    return res


def make_topic(content: str) -> str:
    """Derive a concise thread topic by stripping leading @mentions."""
    cleaned = re.sub(r"^(?:@[A-Za-z0-9_][\w-]*[\s,，:：]*)+", "", (content or "").strip()).strip()
    first_line = next((ln.strip() for ln in cleaned.splitlines() if ln.strip()), "")
    if not first_line:
        first_line = next((ln.strip() for ln in (content or "").strip().splitlines() if ln.strip()), "")
    return first_line[:60]


def infer_role(sender: str) -> str:
    """Infer message sender role from well-known prefixes."""
    if sender == "system":
        return "system"
    low = (sender or "").lower()
    if sender in ("user", "human") or low.startswith("web-"):
        return "human"
    if any(k in low for k in ("antigravity", "codex", "claude", "cursor", "local", "mcp")):
        return "mcp"
    return "agent"


def to_ws_url(hub_url: str) -> str:
    """Convert an HTTP(S) or WS(S) hub base URL into a /ws endpoint URL."""
    url = (hub_url or "").strip().rstrip("/")
    if url.startswith("https://"):
        url = "wss://" + url[8:]
    elif url.startswith("http://"):
        url = "ws://" + url[7:]
    if not url.endswith("/ws"):
        url = f"{url}/ws"
    return url


def to_http_url(server_url: str) -> str:
    """Convert a WS(S) or HTTP(S) server URL into an HTTP(S) base URL."""
    url = (server_url or "").strip().rstrip("/")
    if url.startswith("wss://"):
        url = "https://" + url[6:]
    elif url.startswith("ws://"):
        url = "http://" + url[5:]
    if url.endswith("/ws"):
        url = url[:-3]
    return url.rstrip("/")


def build_mcp_config_snippet(hub_url: str, token: str, sender: str = "local-agent") -> dict:
    """Generate a ready-to-paste MCP client configuration snippet."""
    base_url = to_http_url(hub_url)
    return {
        "mcpServers": {
            "agentloom": {
                "command": "uvx",
                "args": ["--from", "agentloom", "agentloom-mcp"],
                "env": {
                    "AGENTLOOM_URL": base_url,
                    "AGENTLOOM_TOKEN": token,
                    "AGENTLOOM_SENDER": sender,
                },
            }
        },
        "_pythonFallback": {
            "command": "python3",
            "args": ["-m", "agentloom.mcp"],
            "env": {
                "AGENTLOOM_URL": base_url,
                "AGENTLOOM_TOKEN": token,
                "AGENTLOOM_SENDER": sender,
            },
        },
    }


def build_worker_command(
    ws_url: str,
    token: str,
    name: str,
    adapter: str = "hermes",
) -> str:
    """Generate a shell-safe worker startup command."""
    endpoint = to_ws_url(ws_url)
    parts = [
        "agentloom",
        "worker",
        "--name",
        shlex.quote(name),
        "--server",
        shlex.quote(endpoint),
        "--adapter",
        shlex.quote(adapter),
        "--token",
        shlex.quote(token),
    ]
    return " ".join(parts)
