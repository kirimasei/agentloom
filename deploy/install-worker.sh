#!/usr/bin/env bash
set -euo pipefail

# One-line outbound worker installer for VPS, containers, or K8s pods.
# Usage:
#   curl -fsSL <raw-url>/install-worker.sh | bash -s -- \
#     --name my-vps --server wss://hub.example.com/ws --token al_wk_... --adapter hermes

NAME=""
SERVER=""
TOKEN=""
ADAPTER="hermes"
REGION=""
COLOR=""
CAPABILITIES=""
INSTALL_DIR="${AGENTLOOM_WORKER_DIR:-$HOME/.agentloom-worker}"
PKG_SOURCE="${AGENTLOOM_PKG_SOURCE:-agentloom[worker]}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name) NAME="$2"; shift 2 ;;
    --server) SERVER="$2"; shift 2 ;;
    --token) TOKEN="$2"; shift 2 ;;
    --adapter) ADAPTER="$2"; shift 2 ;;
    --region) REGION="$2"; shift 2 ;;
    --color) COLOR="$2"; shift 2 ;;
    --capabilities) CAPABILITIES="$2"; shift 2 ;;
    --dir) INSTALL_DIR="$2"; shift 2 ;;
    --source) PKG_SOURCE="$2"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$NAME" || -z "$SERVER" || -z "$TOKEN" ]]; then
  echo "Required flags: --name <node_name> --server <wss://.../ws> --token <token>" >&2
  exit 1
fi

PYTHON_BIN="${PYTHON_BIN:-python3}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "python3 is required (Python >= 3.10)." >&2
  exit 1
fi

mkdir -p "$INSTALL_DIR"
if [[ ! -d "$INSTALL_DIR/.venv" ]]; then
  "$PYTHON_BIN" -m venv "$INSTALL_DIR/.venv"
fi

"$INSTALL_DIR/.venv/bin/pip" install --upgrade pip >/dev/null
"$INSTALL_DIR/.venv/bin/pip" install --upgrade "$PKG_SOURCE"

TOKEN_FILE="$INSTALL_DIR/worker.token"
umask 077
printf '%s' "$TOKEN" > "$TOKEN_FILE"
chmod 600 "$TOKEN_FILE"

RUN_SCRIPT="$INSTALL_DIR/start-worker.sh"
cat > "$RUN_SCRIPT" <<EOF
#!/usr/bin/env bash
set -euo pipefail
while true; do
  "$INSTALL_DIR/.venv/bin/agentloom" worker \\
    --name "$NAME" \\
    --server "$SERVER" \\
    --adapter "$ADAPTER" \\
    --token-file "$TOKEN_FILE" \\
    --region "$REGION" \\
    --color "$COLOR" \\
    --capabilities "$CAPABILITIES" || true
  sleep 5
done
EOF
chmod 700 "$RUN_SCRIPT"

if command -v systemctl >/dev/null 2>&1 && [[ "$(id -u)" -eq 0 ]] && [[ -d /run/systemd/system ]]; then
  UNIT_FILE="/etc/systemd/system/agentloom-worker-${NAME}.service"
  cat > "$UNIT_FILE" <<EOF
[Unit]
Description=AgentLoom Worker ($NAME)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=$INSTALL_DIR/.venv/bin/agentloom worker --name $NAME --server $SERVER --adapter $ADAPTER --token-file $TOKEN_FILE --region "$REGION" --color "$COLOR" --capabilities "$CAPABILITIES"
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
  systemctl daemon-reload
  systemctl enable --now "agentloom-worker-${NAME}.service"
  echo "✅ Started via systemd: agentloom-worker-${NAME}.service"
elif command -v tmux >/dev/null 2>&1; then
  tmux kill-session -t "agentloom-${NAME}" 2>/dev/null || true
  tmux new-session -d -s "agentloom-${NAME}" "$RUN_SCRIPT"
  echo "✅ Started in tmux session: agentloom-${NAME}"
else
  nohup "$RUN_SCRIPT" > "$INSTALL_DIR/worker.log" 2>&1 &
  echo "✅ Started in background via nohup (log: $INSTALL_DIR/worker.log)"
fi
