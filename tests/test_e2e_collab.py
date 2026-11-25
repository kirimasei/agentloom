import asyncio
import json
import socket
import subprocess
import sys

import httpx
import pytest
import uvicorn

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
async def test_full_e2e_hub_worker_and_mcp(tmp_path):
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    ws_url = f"ws://127.0.0.1:{port}/ws"
    admin_token = "e2e-admin-secret"

    cfg = HubConfig(data_dir=str(tmp_path), admin_token=admin_token)
    app = create_app(cfg)
    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    server_task = asyncio.create_task(server.serve())

    # Wait for Hub /health
    async with httpx.AsyncClient() as client:
        for _ in range(40):
            try:
                r = await client.get(f"{base_url}/health")
                if r.status_code == 200:
                    break
            except Exception:
                pass
            await asyncio.sleep(0.1)
        else:
            server.should_exit = True
            await server_task
            pytest.fail("Hub failed to start")

        # Create scoped operator & worker tokens via admin API
        r_op = await client.post(
            f"{base_url}/api/tokens",
            headers={"X-Token": admin_token},
            json={"name": "friend-mcp", "role": "operator", "allowed_agents": ["echo-node"]},
        )
        assert r_op.status_code == 200
        op_token = r_op.json()["token"]

        r_wk = await client.post(
            f"{base_url}/api/tokens",
            headers={"X-Token": admin_token},
            json={"name": "echo-worker", "role": "worker", "allowed_agents": ["echo-node"]},
        )
        assert r_wk.status_code == 200
        wk_token = r_wk.json()["token"]

    # Launch WorkerRunner with EchoAdapter
    worker = WorkerRunner(
        name="echo-node",
        server_url=ws_url,
        token=wk_token,
        adapter=EchoAdapter(),
        region="Local · Echo",
        color="#38bdf8",
        capabilities=["smoke-test"],
    )
    worker_task = asyncio.create_task(worker.connect(register_signals=False))

    # Wait until echo-node shows online
    async with httpx.AsyncClient() as client:
        for _ in range(40):
            r = await client.get(f"{base_url}/api/agents", headers={"X-Token": op_token})
            if r.status_code == 200 and r.json().get("echo-node", {}).get("online"):
                break
            await asyncio.sleep(0.1)
        else:
            worker.stop()
            server.should_exit = True
            await asyncio.gather(worker_task, server_task, return_exceptions=True)
            pytest.fail("Worker failed to register online")

    # Launch zero-dep MCP stdio server using the scoped operator token
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "agentloom.mcp",
            "--url",
            base_url,
            "--token",
            op_token,
            "--sender",
            "friend-mcp",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )

    try:
        init_res = await asyncio.to_thread(
            _mcp_rpc, proc, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
        )
        assert init_res["result"]["serverInfo"]["name"] == "agentloom-mcp"

        list_res = await asyncio.to_thread(
            _mcp_rpc, proc, {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}}
        )
        tool_names = {t["name"] for t in list_res["result"]["tools"]}
        assert {"agentloom_list_agents", "agentloom_send", "agentloom_get_thread", "agentloom_control"} <= tool_names

        agents_res = await asyncio.to_thread(
            _mcp_rpc,
            proc,
            {
                "jsonrpc": "2.0",
                "id": 3,
                "method": "tools/call",
                "params": {"name": "agentloom_list_agents", "arguments": {}},
            },
        )
        agents_payload = json.loads(agents_res["result"]["content"][0]["text"])
        assert agents_payload["echo-node"]["online"] is True
        assert agents_payload["echo-node"]["meta"]["region"] == "Local · Echo"

        send_res = await asyncio.to_thread(
            _mcp_rpc,
            proc,
            {
                "jsonrpc": "2.0",
                "id": 4,
                "method": "tools/call",
                "params": {
                    "name": "agentloom_send",
                    "arguments": {
                        "content": "@echo-node echo: pong-from-e2e",
                        "wait_for_reply": True,
                        "timeout": 15,
                    },
                },
            },
        )
        send_payload = json.loads(send_res["result"]["content"][0]["text"])
        assert send_payload["ok"] is True
        assert send_payload["replies"][0]["sender"] == "echo-node"
        assert "pong-from-e2e" in send_payload["replies"][0]["content"]

        # Operator calling control must receive 403 error
        ctrl_res = await asyncio.to_thread(
            _mcp_rpc,
            proc,
            {
                "jsonrpc": "2.0",
                "id": 5,
                "method": "tools/call",
                "params": {
                    "name": "agentloom_control",
                    "arguments": {"agent": "echo-node", "action": "restart"},
                },
            },
        )
        assert ctrl_res["result"]["isError"] is True
        assert "403" in ctrl_res["result"]["content"][0]["text"]
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()

    # Admin triggers restart control; worker exits cleanly
    async with httpx.AsyncClient() as client:
        r_ctrl = await client.post(
            f"{base_url}/api/agents/echo-node/control",
            headers={"X-Token": admin_token},
            json={"action": "restart"},
        )
        assert r_ctrl.status_code == 200

    await asyncio.wait_for(worker_task, timeout=10)
    assert worker._restart_requested is True

    server.should_exit = True
    await server_task
