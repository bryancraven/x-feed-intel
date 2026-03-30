#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="${INSTALL_DIR:-$SCRIPT_DIR}"
RUN_AS_USER="${RUN_AS_USER:-${SUDO_USER:-$USER}}"
RUN_AS_GROUP="${RUN_AS_GROUP:-$RUN_AS_USER}"
PYTHON_BIN="${PYTHON_BIN:-$INSTALL_DIR/venv/bin/python}"
VENV_BIN_DIR="$(dirname "$PYTHON_BIN")"
ENV_FILE="${ENV_FILE:-/etc/x-feed-intel.env}"
SYSTEMD_DIR="${SYSTEMD_DIR:-/etc/systemd/system}"

render_unit() {
  local src="$1"
  local dest="$2"
  sed \
    -e "s|__INSTALL_DIR__|$INSTALL_DIR|g" \
    -e "s|__RUN_AS_USER__|$RUN_AS_USER|g" \
    -e "s|__RUN_AS_GROUP__|$RUN_AS_GROUP|g" \
    -e "s|__PYTHON_BIN__|$PYTHON_BIN|g" \
    -e "s|__VENV_BIN_DIR__|$VENV_BIN_DIR|g" \
    -e "s|__ENV_FILE__|$ENV_FILE|g" \
    "$src" | sudo tee "$SYSTEMD_DIR/$dest" >/dev/null
}

echo "Installing X Feed Intel systemd units"

render_unit "$SCRIPT_DIR/x-feed-intel-dashboard.service" "x-feed-intel-dashboard.service"
render_unit "$SCRIPT_DIR/x-feed-intel-weekly-rollover.service" "x-feed-intel-weekly-rollover.service"
sudo cp "$SCRIPT_DIR/x-feed-intel-weekly-rollover.timer" "$SYSTEMD_DIR/x-feed-intel-weekly-rollover.timer"

if [ ! -f "$ENV_FILE" ]; then
  sudo cp "$SCRIPT_DIR/.env.example" "$ENV_FILE"
  sudo chmod 600 "$ENV_FILE"
  echo "Created $ENV_FILE from .env.example; fill in real values before starting services."
else
  echo "Keeping existing $ENV_FILE"
fi

sudo systemctl daemon-reload
sudo systemctl enable x-feed-intel-dashboard.service
sudo systemctl enable --now x-feed-intel-weekly-rollover.timer

echo "Installed units to $SYSTEMD_DIR"
echo "Dashboard service: x-feed-intel-dashboard.service"
echo "Weekly timer: x-feed-intel-weekly-rollover.timer"
echo "Bootstrap the first admin before starting x-feed-intel-dashboard.service."
