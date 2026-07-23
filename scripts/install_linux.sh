#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PYTHON_BIN="${PYTHON_BIN:-python3}"

"$PYTHON_BIN" - <<'PY'
import sys
if sys.version_info < (3, 11):
    raise SystemExit(f"Python 3.11+ required; found {sys.version.split()[0]}")
PY

if [[ ! -x .venv/bin/python ]]; then
  "$PYTHON_BIN" -m venv .venv
fi
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -e .
[[ -f config.yaml ]] || cp config.example.yaml config.yaml
[[ -f .env ]] || .venv/bin/python scripts/generate_tokens.py > .env
mkdir -p data
chmod 700 data
chmod 600 .env config.yaml || true
printf '\nInstalled Crypto Sentinel Free.\nRun: ./.venv/bin/crypto-sentinel --config config.yaml run\n'
