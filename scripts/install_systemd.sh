#!/usr/bin/env bash
set -euo pipefail

if [[ $# -gt 0 ]]; then
  echo "Usage: $0" >&2
  echo "Named plans are no longer configured by the scraper. Use .env/CLI limits only if you want explicit caps." >&2
  exit 2
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
USER_NAME="${SUDO_USER:-$(id -un)}"
SERVICE_NAME="dowirly-amazon-scraper.service"
VENV_BIN="$ROOT/.venv/bin/dowirly-scrape"

if [[ ! -x "$VENV_BIN" ]]; then
  echo "Missing $VENV_BIN. Create the venv and run: pip install -e ." >&2
  exit 1
fi
if [[ ! -f "$ROOT/.env" ]]; then
  echo "Missing $ROOT/.env with Oxylabs credentials." >&2
  exit 1
fi

TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT
cat > "$TMP" <<EOF
[Unit]
Description=Dowirly Amazon.sa catalog scraper
Wants=network-online.target
After=network-online.target
StartLimitIntervalSec=0

[Service]
Type=simple
User=$USER_NAME
WorkingDirectory=$ROOT
Environment=PYTHONUNBUFFERED=1
ExecStart=$VENV_BIN --mode production
Restart=on-failure
RestartPreventExitStatus=3
RestartSec=20
KillSignal=SIGTERM
TimeoutStopSec=300

[Install]
WantedBy=multi-user.target
EOF

sudo install -m 0644 "$TMP" "/etc/systemd/system/$SERVICE_NAME"
sudo systemctl daemon-reload
sudo systemctl reset-failed "$SERVICE_NAME" 2>/dev/null || true
sudo systemctl enable "$SERVICE_NAME"
sudo systemctl restart "$SERVICE_NAME"

echo "Installed and started $SERVICE_NAME"
echo "The service has no named plan configuration; provider/account limits are authoritative unless you set SCRAPER_MAX_RESULTS."
echo "Monitor:  sudo journalctl -u $SERVICE_NAME -f -o cat"
echo "Summary:  bash $ROOT/scripts/status.sh"
echo "Watch:    bash $ROOT/scripts/status.sh --watch 5"
