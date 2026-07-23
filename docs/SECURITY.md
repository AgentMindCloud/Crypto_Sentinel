# Security and threat model

## Non-negotiable boundary

Crypto Sentinel Free uses public market data only. It does not need and should never receive:

- wallet seed phrases or private keys
- exchange passwords or session cookies
- withdrawal-enabled API keys
- trading API permissions
- browser wallet access
- remote-control access to an exchange account

If a future feature requires any of these, treat it as a different system and conduct a separate security review.

## Dashboard exposure

The safe default is `127.0.0.1`. Fresh Windows/Linux installs generate random dashboard and ingest tokens. When the dashboard binds to any non-loopback address, configuration validation requires an access token. A token provides authentication, not transport encryption.

For remote access, prefer one of:

1. SSH local port forwarding to a loopback-bound dashboard.
2. A private overlay network such as Tailscale with host firewall rules.
3. A reverse proxy with TLS, authentication, request-size limits, and no public directory listing.

Do not expose port 8787 directly to the public Internet. Do not put the access token in public screenshots or logs. Query-string tokens are supported for browser EventSource compatibility; the dashboard stores a supplied token locally and removes it from the visible URL, but browser history/process metadata can still briefly observe the launch URL. Use query tokens only over loopback or encrypted transport.

The server adds a restrictive content-security policy, frame denial, no-referrer policy, same-origin resource policy, and disabled camera/microphone/geolocation/payment permissions. These headers reduce browser attack surface but do not replace TLS or host firewalling.

## External ingest endpoint

Set a separate, random `INGEST_TOKEN`. The endpoint is intended for trusted local tools such as TabChart or a webhook relay. It is not a public webhook collector.

Controls implemented:

- bearer or `X-Access-Token` authentication
- 64 KiB request limit
- mandatory `application/json` content type and JSON-object body
- browser cross-origin and cross-site rejection for state-changing endpoints
- field-length and nesting caps
- non-finite metric sanitization
- external timestamp clamping
- cooldown/dedup handling
- no shell evaluation of incoming fields

A custom local notifier command receives alert values as separate process arguments, not through a shell. Nevertheless, configure only executables you trust.

## ntfy

On the public ntfy service, an unauthenticated topic name is effectively a shared secret. Generate a long random topic, for example:

```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

Authenticated/self-hosted ntfy is preferable for sensitive portfolio-related alert text. Keep messages minimal; market symbols and thresholds may reveal strategy information. Leave `click_url` empty unless it is a secured address reachable from the receiving device.

## Telegram

Bot tokens grant control of the bot. Store them only in `.env`, restrict filesystem permissions, and rotate immediately if exposed. A Telegram chat is convenient, not a confidential trading journal.

## Files and permissions

Protect:

- `.env`
- `config.yaml` if it embeds secrets
- `data/sentinel.db`
- `data/sentinel.log`

On Linux:

```bash
chmod 600 .env config.yaml
chmod 700 data
```

The included systemd unit uses a dedicated unprivileged account and hardening directives. Review paths before installation.

## Supply chain

Dependencies are intentionally few: aiohttp, Pydantic, and PyYAML. Install into an isolated virtual environment. For a controlled deployment, build and retain the wheel and hashes, scan dependencies, and periodically rebuild with security updates rather than allowing unattended arbitrary upgrades.

## AI integration

An LLM can summarize an accepted alert or help tune configuration, but should not:

- receive credentials
- control withdrawals
- place trades automatically
- silently edit live thresholds
- suppress deterministic critical alarms

Any AI analysis should display the raw metrics and venue evidence on which its explanation is based.
