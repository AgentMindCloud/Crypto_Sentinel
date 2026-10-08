#!/usr/bin/env bash
set -euo pipefail
umask 027

if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
  echo "Run as root: sudo $0" >&2
  exit 1
fi

SOURCE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_ROOT=/opt/crypto-sentinel
CONFIG_ROOT=/etc/crypto-sentinel
DATA_ROOT=/var/lib/crypto-sentinel
SERVICE_USER=crypto-sentinel

SOURCE_FILES=(
  "LICENSE"
  "README.md"
  "pyproject.toml"
  "config.vps.example.yaml"
  "deploy/systemd/crypto-sentinel.service"
  "scripts/generate_tokens.py"
  "src/crypto_sentinel/__init__.py"
  "src/crypto_sentinel/__main__.py"
  "src/crypto_sentinel/alerting.py"
  "src/crypto_sentinel/app.py"
  "src/crypto_sentinel/checkpoint.py"
  "src/crypto_sentinel/cli.py"
  "src/crypto_sentinel/config.py"
  "src/crypto_sentinel/dashboard.py"
  "src/crypto_sentinel/detector.py"
  "src/crypto_sentinel/doctor.py"
  "src/crypto_sentinel/exchanges/__init__.py"
  "src/crypto_sentinel/exchanges/base.py"
  "src/crypto_sentinel/exchanges/binance.py"
  "src/crypto_sentinel/exchanges/bybit.py"
  "src/crypto_sentinel/exchanges/okx.py"
  "src/crypto_sentinel/health.py"
  "src/crypto_sentinel/models.py"
  "src/crypto_sentinel/persistence.py"
  "src/crypto_sentinel/simulation.py"
  "src/crypto_sentinel/state.py"
  "src/crypto_sentinel/stats.py"
  "src/crypto_sentinel/config.example.yaml"
  "src/crypto_sentinel/static/icon.svg"
  "src/crypto_sentinel/static/index.html"
  "src/crypto_sentinel/static/manifest.webmanifest"
)

for relative_path in "${SOURCE_FILES[@]}"; do
  if [[ ! -f "$SOURCE_ROOT/$relative_path" ]]; then
    echo "Required install source is missing: $relative_path" >&2
    exit 1
  fi
done

if ! id "$SERVICE_USER" >/dev/null 2>&1; then
  useradd --system --home "$DATA_ROOT" --shell /usr/sbin/nologin "$SERVICE_USER"
fi
if [[ -L "$INSTALL_ROOT" || ( -e "$INSTALL_ROOT" && ! -d "$INSTALL_ROOT" ) ]]; then
  echo "Refusing unsafe install root: $INSTALL_ROOT" >&2
  exit 1
fi
install -d -o root -g root -m 0755 "$INSTALL_ROOT"
install -d -o root -g "$SERVICE_USER" -m 0750 "$CONFIG_ROOT"
install -d -o "$SERVICE_USER" -g "$SERVICE_USER" -m 0700 "$DATA_ROOT"

# The install root contains code only; configuration and runtime state live under
# CONFIG_ROOT and DATA_ROOT. Clear an earlier broad-copy install, then install only
# the exact build/runtime files listed above.
find "$INSTALL_ROOT" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
for relative_path in "${SOURCE_FILES[@]}"; do
  install \
    -D \
    -o root \
    -g root \
    -m 0644 \
    "$SOURCE_ROOT/$relative_path" \
    "$INSTALL_ROOT/$relative_path"
done

python3 -m venv "$INSTALL_ROOT/.venv"
"$INSTALL_ROOT/.venv/bin/python" -m pip install --upgrade pip
"$INSTALL_ROOT/.venv/bin/python" -m pip install "$INSTALL_ROOT"
chown -R root:root "$INSTALL_ROOT"
chmod 0755 "$INSTALL_ROOT"
find "$INSTALL_ROOT" -xdev -type d -exec chmod go-w {} +
find "$INSTALL_ROOT" -xdev -type f -exec chmod go-w {} +

if [[ ! -f "$CONFIG_ROOT/config.yaml" ]]; then
  install \
    -o root \
    -g "$SERVICE_USER" \
    -m 0640 \
    "$INSTALL_ROOT/config.vps.example.yaml" \
    "$CONFIG_ROOT/config.yaml"
fi
if [[ ! -f "$CONFIG_ROOT/sentinel.env" ]]; then
  TOKEN_TEMP="$(mktemp "$CONFIG_ROOT/.sentinel.env.XXXXXX")"
  trap 'rm -f -- "$TOKEN_TEMP"' EXIT
  "$INSTALL_ROOT/.venv/bin/python" "$INSTALL_ROOT/scripts/generate_tokens.py" > "$TOKEN_TEMP"
  install \
    -o root \
    -g "$SERVICE_USER" \
    -m 0640 \
    "$TOKEN_TEMP" \
    "$CONFIG_ROOT/sentinel.env"
  rm -f -- "$TOKEN_TEMP"
  trap - EXIT
fi
chown root:"$SERVICE_USER" "$CONFIG_ROOT/config.yaml" "$CONFIG_ROOT/sentinel.env"
chmod 0640 "$CONFIG_ROOT/config.yaml" "$CONFIG_ROOT/sentinel.env"

install -o root -g root -m 0644 "$INSTALL_ROOT/deploy/systemd/crypto-sentinel.service" /etc/systemd/system/crypto-sentinel.service
systemctl daemon-reload

echo "Installed but not started."
echo "1. Edit $CONFIG_ROOT/config.yaml and $CONFIG_ROOT/sentinel.env"
echo "2. Test: sudo -u $SERVICE_USER $INSTALL_ROOT/.venv/bin/crypto-sentinel --config $CONFIG_ROOT/config.yaml simulate"
echo "3. Start: systemctl enable --now crypto-sentinel"
