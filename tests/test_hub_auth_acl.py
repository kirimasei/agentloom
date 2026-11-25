from fastapi.testclient import TestClient

from agentloom.hub.app import create_app
from agentloom.hub.config import HubConfig


def test_hub_auth_rbac_and_acl(tmp_path):
    cfg = HubConfig(data_dir=str(tmp_path), admin_token="root-secret-123")
    app = create_app(cfg)

    with TestClient(app) as client:
        # 1. Health check is public
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json()["name"] == "agentloom"

        # 2. Non-ASCII token via query param returns 401 (never 500)
        r = client.get("/api/auth/whoami", params={"token": "非ASCII测试密钥"})
        assert r.status_code == 401

        # 3. Root admin login succeeds and can create scoped tokens
        r = client.get("/api/auth/whoami", headers={"X-Token": "root-secret-123"})
        assert r.status_code == 200
        assert r.json()["role"] == "admin"
        assert r.json()["can_control"] is True

        # Register two agents directly in state to test ACL filtering
        state = app.state.hub
        state.ensure_agent("public-staging", meta={"region": "SG"})
        state.ensure_agent("private-prod", meta={"region": "US"})

        # Create scoped operator token limited to public-staging
        r = client.post(
            "/api/tokens",
            headers={"X-Token": "root-secret-123"},
            json={"name": "alice", "role": "operator", "allowed_agents": ["public-staging"]},
        )
        assert r.status_code == 200
        op_data = r.json()
        op_token = op_data["token"]
        op_id = op_data["id"]
        assert op_token.startswith("al_op_")
        assert "mcp_config" in op_data

        # Create worker token
        r = client.post(
            "/api/tokens",
            headers={"X-Token": "root-secret-123"},
            json={"name": "staging-worker", "role": "worker", "allowed_agents": ["public-staging"]},
        )
        assert r.status_code == 200
        wk_token = r.json()["token"]
        assert wk_token.startswith("al_wk_")

        # 4. Worker token cannot access operator endpoints (403)
        r = client.get("/api/auth/whoami", headers={"X-Token": wk_token})
        assert r.status_code == 403

        # 5. Scoped operator only sees public-staging, not private-prod
        r = client.get("/api/agents", headers={"X-Token": op_token})
        assert r.status_code == 200
        visible_agents = r.json()
        assert "public-staging" in visible_agents
        assert "private-prod" not in visible_agents

        # 6. Scoped operator cannot call control or token management (403)
        r = client.post(
            "/api/agents/public-staging/control",
            headers={"X-Token": op_token},
            json={"action": "restart"},
        )
        assert r.status_code == 403

        r = client.get("/api/tokens", headers={"X-Token": op_token})
        assert r.status_code == 403

        # 7. Admin creates a private thread mentioning private-prod; scoped operator must not see it
        r = client.post(
            "/api/send",
            headers={"X-Token": "root-secret-123"},
            json={"sender": "admin", "content": "@private-prod secret audit", "wait_for_reply": False},
        )
        assert r.status_code == 200
        private_tid = r.json()["thread_id"]

        r = client.get("/api/threads", headers={"X-Token": op_token})
        assert r.status_code == 200
        assert private_tid not in r.json()

        r = client.get(f"/api/threads/{private_tid}", headers={"X-Token": op_token})
        assert r.status_code == 403

        # 8. Operator mentioning private-prod gets denied_mentions
        r = client.post(
            "/api/send",
            headers={"X-Token": op_token},
            json={"sender": "alice", "content": "@private-prod hello", "wait_for_reply": False},
        )
        assert r.status_code == 200
        assert r.json()["denied_mentions"] == ["private-prod"]

        # 9. Revoking operator token blocks subsequent calls immediately
        r = client.delete(f"/api/tokens/{op_id}", headers={"X-Token": "root-secret-123"})
        assert r.status_code == 200

        r = client.get("/api/agents", headers={"X-Token": op_token})
        assert r.status_code == 401
