#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
  echo "Run as root: sudo $0" >&2
  exit 1
fi

SOURCE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_ROOT=/opt/crypto-sentinel
CONFIG_ROOT=/etc/crypto-sentinel
DATA_ROOT=/var/lib/crypto-sentinel
SERVICE_USER=crypto-sentinel

if ! id "$SERVICE_USER" >/dev/null 2>&1; then
  useradd --system --home "$DATA_ROOT" --shell /usr/sbin/nologin "$SERVICE_USER"
fi
install -d -o root -g root -m 0755 "$INSTALL_ROOT" "$CONFIG_ROOT"
install -d -o "$SERVICE_USER" -g "$SERVICE_USER" -m 0700 "$DATA_ROOT"

# Copy source without local secrets, virtual environments, caches, or runtime data.
tar -C "$SOURCE_ROOT" \
  --exclude='.venv' --exclude='.env' --exclude='config.yaml' --exclude='data' \
  --exclude='dist' --exclude='build' --exclude='*.egg-info' --exclude='__pycache__' \
  -cf - . | tar -C "$INSTALL_ROOT" -xf -

python3 -m venv "$INSTALL_ROOT/.venv"
"$INSTALL_ROOT/.venv/bin/python" -m pip install --upgrade pip
"$INSTALL_ROOT/.venv/bin/python" -m pip install "$INSTALL_ROOT"

if [[ ! -f "$CONFIG_ROOT/config.yaml" ]]; then
  cp "$INSTALL_ROOT/config.vps.example.yaml" "$CONFIG_ROOT/config.yaml"
fi
if [[ ! -f "$CONFIG_ROOT/sentinel.env" ]]; then
  "$INSTALL_ROOT/.venv/bin/python" "$INSTALL_ROOT/scripts/generate_tokens.py" > "$CONFIG_ROOT/sentinel.env"
fi
chown root:"$SERVICE_USER" "$CONFIG_ROOT/config.yaml" "$CONFIG_ROOT/sentinel.env"
chmod 0640 "$CONFIG_ROOT/config.yaml" "$CONFIG_ROOT/sentinel.env"
chown -R root:root "$INSTALL_ROOT"

install -o root -g root -m 0644 "$INSTALL_ROOT/deploy/systemd/crypto-sentinel.service" /etc/systemd/system/crypto-sentinel.service
systemctl daemon-reload

echo "Installed but not started."
echo "1. Edit $CONFIG_ROOT/config.yaml and $CONFIG_ROOT/sentinel.env"
echo "2. Test: sudo -u $SERVICE_USER $INSTALL_ROOT/.venv/bin/crypto-sentinel --config $CONFIG_ROOT/config.yaml simulate"
echo "3. Start: systemctl enable --now crypto-sentinel"
