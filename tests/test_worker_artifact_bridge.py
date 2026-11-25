import asyncio
import hashlib
import json
import os
import socket
import subprocess
import sys
from typing import Optional

import httpx
import pytest
import uvicorn

from agentloom.hub.app import create_app
from agentloom.hub.config import HubConfig
from agentloom.protocol import (
    ARTIFACT_ID_PATTERN,
    extract_artifact_upload_directives,
    is_sensitive_artifact_path,
    sanitize_artifact_filename,
)
from agentloom.worker.adapters import BaseAdapter, StreamCallback
from agentloom.worker.runner import WorkerRunner


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _ScriptedAdapter(BaseAdapter):
    adapter_name = "scripted"

    def __init__(self) -> None:
        self.prompts: list[str] = []
        self.next_reply: str = "ok"

    async def invoke(
        self,
        prompt: str,
        thread_id: str,
        sender: str,
        raw_msg: dict,
        on_chunk: Optional[StreamCallback] = None,
    ) -> Optional[str]:
        self.prompts.append(prompt)
        return self.next_reply


def test_protocol_artifact_directives_and_security():
    sample = (
        "Here is an example in code:\n"
        "```text\n"
        "[[ARTIFACT_UPLOAD: /tmp/ignored-example.txt]]\n"
        "```\n"
        "Real uploads:\n"
        "[[ARTIFACT_UPLOAD: /tmp/bundle.tar.xz]]\n"
        "[[ARTIFACT_UPLOAD: /tmp/raw.bin | custom-name.tar.gz]]\n"
        "[[ARTIFACT_UPLOAD: /tmp/bundle.tar.xz]]\n"
    )
    directives = extract_artifact_upload_directives(sample)
    assert directives == [
        ("/tmp/bundle.tar.xz", None),
        ("/tmp/raw.bin", "custom-name.tar.gz"),
    ]
    assert sanitize_artifact_filename("../../evil/path/my file?.tar.xz") == "my_file_.tar.xz"
    assert is_sensitive_artifact_path("/root/.secrets/worker.token") is not None
    assert is_sensitive_artifact_path("/tmp/.env.production") is not None
    assert is_sensitive_artifact_path("/home/user/.ssh/id_ed25519") is not None
    assert is_sensitive_artifact_path("/tmp/commercial-skills-raw.tar.xz") is None


@pytest.mark.asyncio
async def test_worker_artifact_bridge_and_cli_e2e(tmp_path):
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    ws_url = f"ws://127.0.0.1:{port}/ws"
    admin_token = "v3-admin-secret"

    cfg = HubConfig(data_dir=str(tmp_path / "hub_data"), admin_token=admin_token, max_artifact_mb=2)
    app = create_app(cfg)
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    server_task = asyncio.create_task(server.serve())

    async with httpx.AsyncClient() as client:
        for _ in range(40):
            try:
                if (await client.get(f"{base_url}/health")).status_code == 200:
                    break
            except Exception:
                pass
            await asyncio.sleep(0.1)
        else:
            server.should_exit = True
            await server_task
            pytest.fail("Hub failed to start")

        wk_tok = (
            await client.post(
                f"{base_url}/api/tokens",
                headers={"X-Token": admin_token},
                json={"name": "bridge-wk", "role": "worker", "allowed_agents": ["bridge-node"]},
            )
        ).json()["token"]
        op_tok = (
            await client.post(
                f"{base_url}/api/tokens",
                headers={"X-Token": admin_token},
                json={"name": "alice", "role": "operator", "allowed_agents": ["bridge-node"]},
            )
        ).json()["token"]

    adapter = _ScriptedAdapter()
    inbox_dir = tmp_path / "worker_inbox"
    worker = WorkerRunner(
        name="bridge-node",
        server_url=ws_url,
        token=wk_tok,
        adapter=adapter,
        artifact_inbox_dir=str(inbox_dir),
    )
    worker_task = asyncio.create_task(worker.connect(register_signals=False))

    try:
        async with httpx.AsyncClient() as client:
            for _ in range(40):
                r = await client.get(f"{base_url}/api/agents", headers={"X-Token": op_tok})
                if (
                    r.status_code == 200
                    and r.json().get("bridge-node", {}).get("online") is True
                    and r.json().get("bridge-node", {}).get("meta", {}).get("artifact_bridge") is True
                ):
                    break
                await asyncio.sleep(0.1)
            else:
                pytest.fail("Worker with artifact_bridge=True did not come online")

            # 1. Worker uploads an artifact via [[ARTIFACT_UPLOAD: ...]] (plus >8000 chars body)
            out_file = tmp_path / "bundle.bin"
            payload_bytes = b"AGENTLOOM-V3-BUNDLE-" + os.urandom(256)
            out_file.write_bytes(payload_bytes)
            expected_sha = hashlib.sha256(payload_bytes).hexdigest()

            long_prefix = "X" * 8200
            adapter.next_reply = (
                f"{long_prefix}\n"
                f"[[ARTIFACT_UPLOAD: {out_file} | renamed-bundle.bin]]"
            )
            r1 = await client.post(
                f"{base_url}/api/send",
                headers={"X-Token": op_tok},
                json={
                    "sender": "alice",
                    "content": "@bridge-node pack and upload",
                    "wait_for_reply": True,
                    "timeout": 15,
                },
            )
            assert r1.status_code == 200
            replies1 = r1.json()["replies"]
            assert len(replies1) == 1
            reply_text = replies1[0]["content"]
            assert "renamed-bundle.bin" in reply_text
            m = ARTIFACT_ID_PATTERN.search(reply_text)
            assert m is not None, f"Missing artifact_id in reply: {reply_text[-300:]}"
            uploaded_aid = m.group(1)

            dl1 = await client.get(f"{base_url}/api/artifacts/{uploaded_aid}/download", headers={"X-Token": op_tok})
            assert dl1.status_code == 200
            assert hashlib.sha256(dl1.content).hexdigest() == expected_sha

            # 2. Sensitive path upload is blocked
            secret_file = tmp_path / "worker.token"
            secret_file.write_text("al_wk_super_secret", encoding="utf-8")
            adapter.next_reply = f"Done.\n[[ARTIFACT_UPLOAD: {secret_file}]]"
            r2 = await client.post(
                f"{base_url}/api/send",
                headers={"X-Token": op_tok},
                json={
                    "sender": "alice",
                    "content": "@bridge-node upload token",
                    "wait_for_reply": True,
                    "timeout": 15,
                },
            )
            assert r2.status_code == 200
            reply2 = r2.json()["replies"][0]["content"]
            assert "Artifact blocked/failed" in reply2
            assert ARTIFACT_ID_PATTERN.search(reply2) is None

            # 3. CLI upload -> Worker auto-download into inbox -> CLI list & download
            cli_src = tmp_path / "incoming-spec.txt"
            cli_src.write_text("hello from operator cli", encoding="utf-8")
            up_proc = await asyncio.to_thread(
                subprocess.run,
                [
                    sys.executable,
                    "-m",
                    "agentloom.cli",
                    "artifact",
                    "--url",
                    base_url,
                    "--token",
                    op_tok,
                    "upload",
                    str(cli_src),
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            up_json = json.loads(up_proc.stdout)
            incoming_aid = up_json["artifact"]["id"]

            adapter.next_reply = "received"
            r3 = await client.post(
                f"{base_url}/api/send",
                headers={"X-Token": op_tok},
                json={
                    "sender": "alice",
                    "content": f"@bridge-node inspect {incoming_aid}",
                    "wait_for_reply": True,
                    "timeout": 15,
                },
            )
            assert r3.status_code == 200
            last_prompt = adapter.prompts[-1]
            assert "[Downloaded Thread Artifacts]" in last_prompt
            assert incoming_aid in last_prompt
            downloaded_files = list(inbox_dir.rglob(f"{incoming_aid}_incoming-spec.txt"))
            assert len(downloaded_files) == 1
            assert downloaded_files[0].read_text(encoding="utf-8") == "hello from operator cli"

            # CLI list & download
            ls_proc = await asyncio.to_thread(
                subprocess.run,
                [sys.executable, "-m", "agentloom.cli", "artifact", "--url", base_url, "--token", op_tok, "list"],
                capture_output=True,
                text=True,
                check=True,
            )
            ls_json = json.loads(ls_proc.stdout)
            assert any(a["id"] == incoming_aid for a in ls_json["artifacts"])

            cli_dl_dest = tmp_path / "cli_dl.bin"
            dl_proc = await asyncio.to_thread(
                subprocess.run,
                [
                    sys.executable,
                    "-m",
                    "agentloom.cli",
                    "artifact",
                    "--url",
                    base_url,
                    "--token",
                    op_tok,
                    "download",
                    uploaded_aid,
                    "-o",
                    str(cli_dl_dest),
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            dl_json = json.loads(dl_proc.stdout)
            assert dl_json["sha256_match"] is True
            assert cli_dl_dest.read_bytes() == payload_bytes
    finally:
        worker.stop()
        server.should_exit = True
        await asyncio.gather(worker_task, server_task, return_exceptions=True)
