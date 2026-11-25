import asyncio
import json
import socket
import subprocess
import sys

import httpx
import pytest
import uvicorn
import websockets

from agentloom.hub.app import create_app
from agentloom.hub.config import HubConfig
from agentloom.worker.adapters.echo import EchoAdapter
from agentloom.worker.runner import WorkerRunner


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _mcp_rpc(proc: subprocess.Popen, msg: dict) -> dict:
    assert proc.stdin is not None and proc.stdout is not None
    proc.stdin.write(json.dumps(msg, ensure_ascii=False) + "\n")
    proc.stdin.flush()
    line = proc.stdout.readline()
    assert line, "MCP server closed stdout unexpectedly"
    return json.loads(line)


@pytest.mark.asyncio
async def test_v2_streaming_artifacts_and_hitl_approvals(tmp_path):
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    ws_url = f"ws://127.0.0.1:{port}/ws"
    admin_token = "v2-admin-secret"

    cfg = HubConfig(data_dir=str(tmp_path), admin_token=admin_token, max_artifact_mb=1)
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

        op_a = (
            await client.post(
                f"{base_url}/api/tokens",
                headers={"X-Token": admin_token},
                json={"name": "alice", "role": "operator", "allowed_agents": ["stream-node"]},
            )
        ).json()["token"]
        op_b = (
            await client.post(
                f"{base_url}/api/tokens",
                headers={"X-Token": admin_token},
                json={"name": "bob", "role": "operator", "allowed_agents": ["other-node"]},
            )
        ).json()["token"]
        wk_tok = (
            await client.post(
                f"{base_url}/api/tokens",
                headers={"X-Token": admin_token},
                json={"name": "stream-worker", "role": "worker", "allowed_agents": ["stream-node"]},
            )
        ).json()["token"]

    # Worker configured with approval pattern matching "rm -rf"
    worker = WorkerRunner(
        name="stream-node",
        server_url=ws_url,
        token=wk_tok,
        adapter=EchoAdapter(),
        approval_patterns=["rm -rf"],
        approval_timeout=15,
    )
    worker_task = asyncio.create_task(worker.connect(register_signals=False))

    async with httpx.AsyncClient() as client:
        for _ in range(40):
            r = await client.get(f"{base_url}/api/agents", headers={"X-Token": op_a})
            if r.status_code == 200 and r.json().get("stream-node", {}).get("online"):
                break
            await asyncio.sleep(0.1)
        else:
            worker.stop()
            server.should_exit = True
            await asyncio.gather(worker_task, server_task, return_exceptions=True)
            pytest.fail("Worker did not come online")

    # ── 1. Streaming Test via WebSocket Human Observer ────────────────
    try:
        async with websockets.connect(ws_url) as human_ws:
            await human_ws.send(
                json.dumps({"type": "register", "agent_name": "ui-test", "role": "human", "token": admin_token})
            )
            reg = json.loads(await human_ws.recv())
            assert reg["type"] == "registered"

            async with httpx.AsyncClient() as client:
                send_r = await client.post(
                    f"{base_url}/api/send",
                    headers={"X-Token": op_a},
                    json={
                        "sender": "alice",
                        "content": "@stream-node stream:step1-|step2-|done",
                        "wait_for_reply": False,
                    },
                )
                assert send_r.status_code == 200
                stream_tid = send_r.json()["thread_id"]

            stream_events = []
            final_broadcast = None
            for _ in range(30):
                ev = json.loads(await asyncio.wait_for(human_ws.recv(), timeout=5))
                if ev.get("type") == "agent_stream" and ev.get("thread_id") == stream_tid:
                    stream_events.append(ev["delta"])
                elif ev.get("type") == "broadcast" and ev.get("sender") == "stream-node" and ev.get("thread_id") == stream_tid:
                    final_broadcast = ev
                    break

            assert len(stream_events) >= 2
            assert "step1-step2-done" in "".join(stream_events)
            assert final_broadcast is not None
            assert final_broadcast["content"] == "step1-step2-done"

        # ── 2. Artifacts Upload, Download, Traversal Guard, ACL, & MCP ────
        local_file = tmp_path / "build-log.txt"
        local_file.write_text("deploy ok\nline 2", encoding="utf-8")
        downloaded_file = tmp_path / "downloaded-log.txt"

        proc = subprocess.Popen(
            [sys.executable, "-m", "agentloom.mcp", "--url", base_url, "--token", op_a, "--sender", "alice-mcp"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )
        try:
            _mcp_rpc(proc, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
            up_res = await asyncio.to_thread(
                _mcp_rpc,
                proc,
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {
                        "name": "agentloom_upload_artifact",
                        "arguments": {
                            "filename": "../../etc/evil-build.txt",
                            "file_path": str(local_file),
                            "thread_id": stream_tid,
                        },
                    },
                },
            )
            up_payload = json.loads(up_res["result"]["content"][0]["text"])
            assert up_payload["ok"] is True
            art = up_payload["artifact"]
            assert art["filename"] == "evil-build.txt"
            art_id = art["id"]

            dl_res = await asyncio.to_thread(
                _mcp_rpc,
                proc,
                {
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "tools/call",
                    "params": {
                        "name": "agentloom_download_artifact",
                        "arguments": {"artifact_id": art_id, "save_path": str(downloaded_file)},
                    },
                },
            )
            dl_payload = json.loads(dl_res["result"]["content"][0]["text"])
            assert dl_payload["sha256_match"] is True
            assert downloaded_file.read_text(encoding="utf-8") == "deploy ok\nline 2"
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()

        async with httpx.AsyncClient() as client:
            # Cross-thread operator B must get 403 downloading op_a's thread artifact
            r_forbid = await client.get(f"{base_url}/api/artifacts/{art_id}/download", headers={"X-Token": op_b})
            assert r_forbid.status_code == 403

            # Security header check on authorized download
            r_ok = await client.get(f"{base_url}/api/artifacts/{art_id}/download", headers={"X-Token": op_a})
            assert r_ok.status_code == 200
            assert r_ok.headers.get("x-content-type-options") == "nosniff"

            # ── 3. HITL Approval Gate (Worker Pattern + Reject + Approve) ─
            # 3a. Trigger approval rule -> worker pauses waiting
            r_risk1 = await client.post(
                f"{base_url}/api/send",
                headers={"X-Token": op_a},
                json={"sender": "alice", "content": "@stream-node run rm -rf /tmp/cache echo: should-not-run", "wait_for_reply": False},
            )
            tid_reject = r_risk1.json()["thread_id"]

            pending_list = []
            for _ in range(30):
                r_aprs = await client.get(
                    f"{base_url}/api/approvals?status=pending&thread_id={tid_reject}",
                    headers={"X-Token": admin_token},
                )
                pending_list = r_aprs.json()["approvals"]
                if pending_list:
                    break
                await asyncio.sleep(0.1)
            assert len(pending_list) == 1
            apr_reject_id = pending_list[0]["id"]

            # Operator cannot decide approvals (403)
            r_op_dec = await client.post(
                f"{base_url}/api/approvals/{apr_reject_id}/decide",
                headers={"X-Token": op_a},
                json={"decision": "approved"},
            )
            assert r_op_dec.status_code == 403

            # Admin rejects -> worker does NOT execute EchoAdapter
            r_rej = await client.post(
                f"{base_url}/api/approvals/{apr_reject_id}/decide",
                headers={"X-Token": admin_token},
                json={"decision": "rejected", "note": "use safe cleanup"},
            )
            assert r_rej.status_code == 200

            reject_reply = None
            for _ in range(30):
                t_data = (await client.get(f"{base_url}/api/threads/{tid_reject}", headers={"X-Token": admin_token})).json()
                agent_msgs = [m for m in t_data["messages"] if m["sender"] == "stream-node"]
                if agent_msgs:
                    reject_reply = agent_msgs[-1]["content"]
                    break
                await asyncio.sleep(0.1)
            assert reject_reply is not None
            assert "审批被拒绝" in reject_reply
            assert "should-not-run" not in reject_reply

            # 3b. Trigger again and Admin approves -> worker executes EchoAdapter
            r_risk2 = await client.post(
                f"{base_url}/api/send",
                headers={"X-Token": op_a},
                json={"sender": "alice", "content": "@stream-node rm -rf echo: approved-execution-ok", "wait_for_reply": False},
            )
            tid_approve = r_risk2.json()["thread_id"]

            pending_list2 = []
            for _ in range(30):
                r_aprs2 = await client.get(
                    f"{base_url}/api/approvals?status=pending&thread_id={tid_approve}",
                    headers={"X-Token": admin_token},
                )
                pending_list2 = r_aprs2.json()["approvals"]
                if pending_list2:
                    break
                await asyncio.sleep(0.1)
            assert len(pending_list2) == 1
            apr_ok_id = pending_list2[0]["id"]

            r_ok_dec = await client.post(
                f"{base_url}/api/approvals/{apr_ok_id}/decide",
                headers={"X-Token": admin_token},
                json={"decision": "approved"},
            )
            assert r_ok_dec.status_code == 200

            approved_reply = None
            for _ in range(30):
                t_data2 = (await client.get(f"{base_url}/api/threads/{tid_approve}", headers={"X-Token": admin_token})).json()
                agent_msgs2 = [m for m in t_data2["messages"] if m["sender"] == "stream-node"]
                if agent_msgs2:
                    approved_reply = agent_msgs2[-1]["content"]
                    break
                await asyncio.sleep(0.1)
            assert approved_reply == "approved-execution-ok"
    finally:
        worker.stop()
        server.should_exit = True
        await asyncio.gather(worker_task, server_task, return_exceptions=True)
