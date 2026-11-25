from agentloom.protocol import (
    build_mcp_config_snippet,
    build_worker_command,
    extract_mentions,
    infer_role,
    make_topic,
    to_http_url,
    to_ws_url,
)


def test_extract_mentions_ignores_code_and_emails():
    content = """
    @alpha 请帮我看下这个问题
    邮箱联系 dev@example.com 不要触发
    行内代码 `npm i @types/node` 忽略
    ```bash
    echo "@gamma inside fence"
    ```
    顺便通知 @beta 和 @alpha（去重）以及我自己 @self-node
    """
    assert extract_mentions(content, exclude="self-node") == ["alpha", "beta"]


def test_make_topic_strips_leading_mentions():
    assert make_topic("@node-a @node-b, 请检查磁盘空间\n第二行") == "请检查磁盘空间"
    assert make_topic("@only-mention") == "@only-mention"


def test_url_and_role_helpers():
    assert to_ws_url("https://hub.example.com") == "wss://hub.example.com/ws"
    assert to_http_url("wss://hub.example.com/ws") == "https://hub.example.com"
    assert infer_role("alice-antigravity") == "mcp"
    assert infer_role("web-1234") == "human"
    assert infer_role("prod-us") == "agent"

    snippet = build_mcp_config_snippet("https://hub.example.com", "al_op_123", sender="alice")
    assert snippet["mcpServers"]["agentloom"]["env"]["AGENTLOOM_TOKEN"] == "al_op_123"
    cmd = build_worker_command("https://hub.example.com", "al_wk_456", name="node-1", adapter="cli")
    assert "wss://hub.example.com/ws" in cmd
    assert "node-1" in cmd
