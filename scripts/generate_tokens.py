from __future__ import annotations

import secrets

print(f"DASHBOARD_TOKEN={secrets.token_urlsafe(32)}")
print(f"INGEST_TOKEN={secrets.token_urlsafe(32)}")
print(f"NTFY_TOPIC=crypto-sentinel-{secrets.token_urlsafe(24)}")
print("NTFY_TOKEN=")
print("TELEGRAM_BOT_TOKEN=")
print("TELEGRAM_CHAT_ID=")
