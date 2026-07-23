#!/usr/bin/env bash
set -euo pipefail

URL="${1:-http://127.0.0.1:8787/api/ingest}"
TOKEN="${2:-${INGEST_TOKEN:-}}"
SEVERITY="${3:-critical}"
if [[ -z "$TOKEN" ]]; then
  echo "Usage: $0 [url] <ingest-token> [warning|critical]" >&2
  exit 2
fi

curl --fail --silent --show-error \
  -X POST "$URL" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  --data "{\"source\":\"shell-test\",\"severity\":\"$SEVERITY\",\"category\":\"external_test\",\"symbol\":\"BTCUSDT\",\"direction\":\"down\",\"title\":\"External webhook test\",\"message\":\"This is a test of authenticated external alert ingestion. No market event occurred.\",\"dedup_key\":\"external-test:BTCUSDT:$(date +%s)\"}"
printf '\n'
