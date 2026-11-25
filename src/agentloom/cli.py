"""Unified CLI entry point for AgentLoom (lazy-imports optional subpackages)."""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import logging
import mimetypes
import os
import sys
import urllib.error
import urllib.request
from typing import Optional

from agentloom import __version__
from agentloom.protocol import build_mcp_config_snippet, sanitize_artifact_filename, to_http_url


def _cmd_hub(args: argparse.Namespace) -> None:
    try:
        import uvicorn
        from agentloom.hub.app import create_app
        from agentloom.hub.config import HubConfig
    except ImportError as exc:
        print(
            f"[agentloom] Hub dependencies not installed ({exc}). "
            'Install with: pip install "agentloom[hub]"',
            file=sys.stderr,
        )
        sys.exit(1)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    cfg = HubConfig.from_env()
    if args.host:
        cfg.host = args.host
    if args.port:
        cfg.port = args.port
    if args.data_dir:
        cfg.data_dir = args.data_dir
    if args.admin_token:
        cfg.admin_token = args.admin_token

    app = create_app(cfg)
    print(f"AgentLoom Hub v{__version__} listening on http://{cfg.host}:{cfg.port}")
    uvicorn.run(app, host=cfg.host, port=cfg.port, log_level=args.log_level)


def _cmd_worker(args: argparse.Namespace) -> None:
    try:
        from agentloom.worker.adapters import create_adapter
        from agentloom.worker.runner import WorkerRunner
    except ImportError as exc:
        print(
            f"[agentloom] Worker dependencies not installed ({exc}). "
            'Install with: pip install "agentloom[worker]"',
            file=sys.stderr,
        )
        sys.exit(1)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    token = (args.token or os.environ.get("AGENTLOOM_TOKEN") or "").strip()
    if not token and args.token_file and os.path.isfile(args.token_file):
        with open(args.token_file, "r", encoding="utf-8") as f:
            token = f.read().strip()
    if not token:
        print("[agentloom] Token required! Pass --token, --token-file, or AGENTLOOM_TOKEN.", file=sys.stderr)
        sys.exit(1)

    capabilities = [c.strip() for c in (args.capabilities or "").split(",") if c.strip()]
    approval_patterns = [p.strip() for p in (args.require_approval_patterns or "").split(",") if p.strip()]
    adapter = create_adapter(
        args.adapter,
        api_url=args.api_url,
        api_key=args.api_key,
        model=args.model,
        system_prompt=args.system_prompt,
        hermes_env=args.hermes_env,
        hermes_bin=args.hermes_bin,
        cli_command=args.cli_command,
        preset=args.cli_preset,
        timeout=args.timeout,
    )

    runner = WorkerRunner(
        name=args.name,
        server_url=args.server,
        token=token,
        adapter=adapter,
        region=args.region,
        color=args.color,
        mono=args.mono,
        capabilities=capabilities,
        approval_patterns=approval_patterns,
        approval_timeout=args.approval_timeout,
        timeout=args.timeout,
        max_concurrent=args.max_concurrent,
        dry_run=args.dry_run,
        verbose=args.verbose,
        max_artifact_mb=args.max_artifact_mb,
        artifact_inbox_dir=args.artifact_inbox_dir,
    )
    asyncio.run(runner.connect())


def _cmd_mcp(args: argparse.Namespace) -> None:
    from agentloom.mcp.server import run_stdio_server

    run_stdio_server(
        base_url=args.url,
        token=args.token,
        sender=args.sender,
    )


def _http_json(method: str, url: str, token: str, body: Optional[dict] = None) -> dict:
    data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
    headers = {
        "X-Token": token,
        "Accept": "application/json",
        "User-Agent": f"agentloom-cli/{__version__}",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw) if raw else {"ok": True}
    except urllib.error.HTTPError as exc:
        err_text = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {err_text}") from exc


def _cmd_init(args: argparse.Namespace) -> None:
    base_url = to_http_url(args.url)
    token = args.token.strip()
    if args.verify:
        info = _http_json("GET", f"{base_url}/api/auth/whoami", token)
        print(
            f"# Verified principal: name={info.get('name')} role={info.get('role')} "
            f"allowed_agents={info.get('allowed_agents')}",
            file=sys.stderr,
        )
    snippet = build_mcp_config_snippet(base_url, token, sender=args.sender)
    print(json.dumps(snippet, ensure_ascii=False, indent=2))


def _cmd_token(args: argparse.Namespace) -> None:
    base_url = to_http_url(args.url)
    admin_token = (args.admin_token or os.environ.get("AGENTLOOM_ADMIN_TOKEN") or "").strip()
    if not admin_token:
        print("[agentloom] Admin token required via --admin-token or AGENTLOOM_ADMIN_TOKEN", file=sys.stderr)
        sys.exit(1)

    if args.token_action == "list":
        res = _http_json("GET", f"{base_url}/api/tokens", admin_token)
    elif args.token_action == "create":
        allowed = [x.strip() for x in args.allowed_agents.split(",") if x.strip()] if args.allowed_agents else ["*"]
        res = _http_json(
            "POST",
            f"{base_url}/api/tokens",
            admin_token,
            body={"name": args.name, "role": args.role, "allowed_agents": allowed},
        )
    elif args.token_action == "revoke":
        res = _http_json("DELETE", f"{base_url}/api/tokens/{args.token_id}", admin_token)
    else:
        raise ValueError(f"Unsupported token action: {args.token_action}")

    print(json.dumps(res, ensure_ascii=False, indent=2))


def _resolve_cli_token(args: argparse.Namespace) -> str:
    tok = (getattr(args, "token", "") or os.environ.get("AGENTLOOM_TOKEN") or "").strip()
    token_file = getattr(args, "token_file", "") or ""
    if not tok and token_file and os.path.isfile(token_file):
        with open(token_file, "r", encoding="utf-8") as f:
            tok = f.read().strip()
    if not tok:
        print("[agentloom] Token required via --token, --token-file, or AGENTLOOM_TOKEN", file=sys.stderr)
        sys.exit(1)
    return tok


def _cmd_artifact(args: argparse.Namespace) -> None:
    raw_url = (getattr(args, "url", None) or os.environ.get("AGENTLOOM_URL") or "").strip()
    if not raw_url:
        print("[agentloom] Hub URL required via --url or AGENTLOOM_URL", file=sys.stderr)
        sys.exit(1)
    base_url = to_http_url(raw_url)
    token = _resolve_cli_token(args)

    if args.artifact_action == "upload":
        fpath = os.path.abspath(os.path.expanduser(args.file_path))
        if not os.path.isfile(fpath):
            print(f"[agentloom] File not found: {fpath}", file=sys.stderr)
            sys.exit(1)
        filename = sanitize_artifact_filename(args.filename or os.path.basename(fpath))
        mime_type, _ = mimetypes.guess_type(filename)
        with open(fpath, "rb") as f:
            raw_bytes = f.read()
        body = {
            "filename": filename,
            "content_base64": base64.b64encode(raw_bytes).decode("ascii"),
            "thread_id": (args.thread_id or "").strip() or None,
            "content_type": mime_type or "application/octet-stream",
        }
        res = _http_json("POST", f"{base_url}/api/artifacts", token, body=body)
        print(json.dumps(res, ensure_ascii=False, indent=2))
    elif args.artifact_action == "list":
        tid = (args.thread_id or "").strip()
        qs = f"?thread_id={urllib.request.quote(tid)}" if tid else ""
        res = _http_json("GET", f"{base_url}/api/artifacts{qs}", token)
        print(json.dumps(res, ensure_ascii=False, indent=2))
    elif args.artifact_action == "download":
        aid = args.artifact_id.strip()
        meta = _http_json("GET", f"{base_url}/api/artifacts/{aid}", token)
        req = urllib.request.Request(
            f"{base_url}/api/artifacts/{aid}/download",
            headers={"X-Token": token, "User-Agent": f"agentloom-cli/{__version__}"},
            method="GET",
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw_bytes = resp.read()
        sha = hashlib.sha256(raw_bytes).hexdigest()
        out_path = args.output or sanitize_artifact_filename(meta.get("filename") or f"{aid}.bin")
        out_abs = os.path.abspath(out_path)
        os.makedirs(os.path.dirname(out_abs), exist_ok=True)
        with open(out_abs, "wb") as f:
            f.write(raw_bytes)
        print(
            json.dumps(
                {
                    "ok": True,
                    "artifact_id": aid,
                    "filename": meta.get("filename"),
                    "save_path": out_abs,
                    "size_bytes": len(raw_bytes),
                    "sha256": sha,
                    "sha256_match": sha == meta.get("sha256"),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        raise ValueError(f"Unsupported artifact action: {args.artifact_action}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="agentloom", description="AgentLoom multi-agent collaboration suite")
    parser.add_argument("--version", action="version", version=f"agentloom {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    # hub
    p_hub = sub.add_parser("hub", help="Start the AgentLoom Hub server")
    p_hub.add_argument("--host", default=None, help="Bind host (default 0.0.0.0)")
    p_hub.add_argument("--port", type=int, default=None, help="Bind port (default 3000)")
    p_hub.add_argument("--data-dir", default=None, help="SQLite and token storage directory")
    p_hub.add_argument("--admin-token", default=None, help="Override admin token")
    p_hub.add_argument("--log-level", default="info", help="Uvicorn log level")
    p_hub.set_defaults(func=_cmd_hub)

    # worker
    p_worker = sub.add_parser("worker", help="Run an outbound remote worker node")
    p_worker.add_argument("--name", required=True, help="Unique node name (e.g. us-prod-1)")
    p_worker.add_argument("--server", required=True, help="Hub WS/HTTP URL (e.g. wss://hub.example.com/ws)")
    p_worker.add_argument(
        "--adapter",
        default="hermes",
        choices=["hermes", "openai", "qwenpaw", "cli", "echo"],
        help="Backend agent adapter",
    )
    p_worker.add_argument("--token", default="", help="Worker or admin token")
    p_worker.add_argument("--token-file", default="", help="Path to file containing token")
    p_worker.add_argument("--api-url", default=None, help="Backend HTTP API base URL")
    p_worker.add_argument("--api-key", default=None, help="Backend HTTP API key")
    p_worker.add_argument("--model", default=None, help="Model name for openai adapter")
    p_worker.add_argument("--system-prompt", default=None, help="System prompt for openai adapter")
    p_worker.add_argument("--hermes-env", default="~/.hermes/.env", help="Hermes .env file path")
    p_worker.add_argument("--hermes-bin", default="hermes", help="Hermes CLI binary")
    p_worker.add_argument("--cli-command", default=None, help="Custom CLI command template (supports {prompt})")
    p_worker.add_argument("--cli-preset", default="claude", choices=["claude", "codex", "opencode", "custom"])
    p_worker.add_argument("--region", default="", help="Human-readable region/host label")
    p_worker.add_argument("--color", default="", help="Preferred UI hex color (e.g. #818cf8)")
    p_worker.add_argument("--mono", default="", help="2-char monogram badge for UI")
    p_worker.add_argument("--capabilities", default="", help="Comma-separated capability tags")
    p_worker.add_argument(
        "--require-approval-patterns",
        default="",
        help="Comma-separated keywords/regex requiring admin approval before execution",
    )
    p_worker.add_argument("--approval-timeout", type=int, default=180, help="Approval wait timeout in seconds")
    p_worker.add_argument("--timeout", type=int, default=300, help="Execution timeout in seconds")
    p_worker.add_argument("--max-concurrent", type=int, default=1, help="Max concurrent tasks")
    p_worker.add_argument("--dry-run", action="store_true", help="Echo received prompts without invoking backend")
    p_worker.add_argument("--verbose", action="store_true", help="Enable debug logging")
    p_worker.add_argument("--max-artifact-mb", type=int, default=10, help="Max artifact size (MB) for worker bridge")
    p_worker.add_argument("--artifact-inbox-dir", default=None, help="Directory for auto-downloaded thread artifacts")
    p_worker.set_defaults(func=_cmd_worker)

    # mcp
    p_mcp = sub.add_parser("mcp", help="Run zero-dependency MCP stdio server")
    p_mcp.add_argument("--url", default=None, help="Hub HTTP URL")
    p_mcp.add_argument("--token", default=None, help="Operator or admin token")
    p_mcp.add_argument("--sender", default=None, help="Sender name shown in threads")
    p_mcp.set_defaults(func=_cmd_mcp)

    # init
    p_init = sub.add_parser("init", help="Verify connectivity and print MCP config JSON")
    p_init.add_argument("--url", required=True, help="Hub HTTP URL")
    p_init.add_argument("--token", required=True, help="Operator or admin token")
    p_init.add_argument("--sender", default="local-agent", help="Local agent display name")
    p_init.add_argument("--verify", action="store_true", help="Call /api/auth/whoami to verify token")
    p_init.set_defaults(func=_cmd_init)

    # token
    p_tok = sub.add_parser("token", help="Manage Hub access tokens (admin only)")
    p_tok.add_argument("--url", required=True, help="Hub HTTP URL")
    p_tok.add_argument("--admin-token", default="", help="Admin token")
    tok_sub = p_tok.add_subparsers(dest="token_action", required=True)

    tok_list = tok_sub.add_parser("list", help="List issued tokens")
    tok_list.set_defaults(func=_cmd_token)

    tok_create = tok_sub.add_parser("create", help="Create a new operator/worker/admin token")
    tok_create.add_argument("--name", required=True, help="Human-friendly label")
    tok_create.add_argument("--role", default="operator", choices=["admin", "operator", "worker"])
    tok_create.add_argument("--allowed-agents", default="*", help="Comma-separated node whitelist or *")
    tok_create.set_defaults(func=_cmd_token)

    tok_revoke = tok_sub.add_parser("revoke", help="Revoke a token by ID")
    tok_revoke.add_argument("token_id", help="Token ID to revoke")
    tok_revoke.set_defaults(func=_cmd_token)

    # artifact
    p_art = sub.add_parser("artifact", help="Upload, list, or download Hub artifacts")
    p_art.add_argument("--url", default=None, help="Hub HTTP URL (or AGENTLOOM_URL)")
    p_art.add_argument("--token", default="", help="Access token (or AGENTLOOM_TOKEN)")
    p_art.add_argument("--token-file", default="", help="Path to file containing token")
    art_sub = p_art.add_subparsers(dest="artifact_action", required=True)

    art_up = art_sub.add_parser("upload", help="Upload a local file as a Hub artifact")
    art_up.add_argument("file_path", help="Local file path to upload")
    art_up.add_argument("--thread-id", default="", help="Optional thread ID")
    art_up.add_argument("--filename", default="", help="Override artifact filename")
    art_up.set_defaults(func=_cmd_artifact)

    art_ls = art_sub.add_parser("list", help="List artifacts (optionally by thread)")
    art_ls.add_argument("--thread-id", default="", help="Optional thread ID")
    art_ls.set_defaults(func=_cmd_artifact)

    art_dl = art_sub.add_parser("download", help="Download an artifact by ID")
    art_dl.add_argument("artifact_id", help="Artifact ID")
    art_dl.add_argument("--output", "-o", default="", help="Destination file path")
    art_dl.set_defaults(func=_cmd_artifact)

    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
