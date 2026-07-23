# Crypto Sentinel Free

A zero-subscription, keyless crypto market anomaly alarm. It consumes **public** Binance, Bybit, and OKX futures data, applies deterministic rules, sounds a local alarm, serves a live browser dashboard, and can escalate through ntfy, Telegram, or a generic webhook.

It does **not** connect to a wallet, place orders, require exchange API keys, or ask an LLM whether an alarm should fire.

## What it detects

- Rapid price moves that are unusual relative to each venue's rolling baseline
- Abnormal quote-volume participation
- Directionally aligned taker buy/sell imbalance
- Cross-exchange confirmation of the same move
- Binance and Bybit liquidation bursts
- Cross-exchange price divergence, bad wicks, and possible venue/data problems
- Feed disconnections and stale market data
- External alerts from TabChart, TradingView, or another tool through an authenticated webhook

The detector uses median/MAD-based robust scores rather than assuming normally distributed crypto returns. A normal market anomaly needs absolute price displacement, statistical abnormality, minimum notional volume, and either volume or order-flow confirmation. Critical rules can require agreement across multiple exchanges.

## Reliability features

- Fixed five-second buckets with late/out-of-order trade handling
- Bounded event deduplication across reconnects
- Atomic gzip checkpoints of rolling baselines and recent liquidations
- Alert cooldown state restored from SQLite after restart
- Severity upgrades allowed during cooldown
- Separate local-alarm and remote-notifier execution, so a slow webhook does not stall detection
- Three delivery attempts for each enabled remote notifier
- Persistence errors logged without suppressing browser/local alarm delivery
- Bounded market-event and remote-notification queues
- Rotating logs, feed-health alarms, and a deployment `doctor`

This is still best-effort monitoring, not an exactly-once safety system. Read the limitations below before relying on it.

## Fastest Windows setup

Requirements: Windows 10/11, Python 3.11 or newer, and Chrome or another modern browser.

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\install_windows.ps1
.\scripts\run_windows.ps1
```

The installer creates `.venv`, installs the project, copies `config.example.yaml` to `config.yaml`, and generates random dashboard/ingest tokens in `.env`. Existing config and secrets are not overwritten. The app opens its loopback dashboard and supplies the token once; the page stores it locally and removes it from the address bar.

On the dashboard:

1. Click **Arm browser sound**.
2. Click **Enable browser notifications**.
3. Click **Test critical** and verify both the Python process alarm and browser alarm.
4. Keep the dashboard pinned. The process-level sound remains primary because browsers can suspend background tabs.

Run all three preflight checks before relying on live data:

```powershell
.\.venv\Scripts\crypto-sentinel.exe --config config.yaml simulate
.\.venv\Scripts\crypto-sentinel.exe --config config.yaml validate-config
.\.venv\Scripts\crypto-sentinel.exe --config config.yaml doctor --timeout 15
```

`doctor` performs real WebSocket probes on the target machine. Do not treat the deployment as live until Binance, Bybit, and OKX all pass.

After manual testing, prepare and verify the Docker shadow, then register
automatic startup:

```powershell
Copy-Item config.docker.example.yaml config.docker.yaml
.\.venv\Scripts\python.exe scripts\generate_tokens.py | Set-Content -Encoding ascii .env.docker
docker compose build
docker compose up -d --no-build
.\.venv\Scripts\python.exe scripts\soak_monitor.py --once
.\scripts\register_windows_startup.ps1
```

The registration script first tries three limited, interactive Scheduled Tasks.
If local Windows policy denies standard-user task registration, it installs a
per-user Startup shortcut instead. That shortcut runs a single-instance
supervisor which restarts the native audible primary and soak monitor, starts
Docker Desktop if needed, and recovers the Docker shadow. No administrator or
exchange credentials are required. Use `.\scripts\unregister_windows_startup.ps1`
to remove either startup method.

The primary stays on `127.0.0.1:8787`; the non-audible Docker shadow uses
`127.0.0.1:8788`. They never share tokens, databases, checkpoints, or logs.

## Linux or VPS setup

```bash
chmod +x scripts/install_linux.sh
./scripts/install_linux.sh
./.venv/bin/crypto-sentinel --config config.yaml simulate
./.venv/bin/crypto-sentinel --config config.yaml doctor --timeout 15
./.venv/bin/crypto-sentinel --config config.yaml run
```

The installer creates random secrets in `.env` when none exists. A VPS cannot play through your home speakers, so enable ntfy or Telegram. Keep the dashboard bound to loopback and reach it through an SSH tunnel:

```bash
ssh -L 8787:127.0.0.1:8787 user@your-vps
```

Then open `http://127.0.0.1:8787/` locally. A hardened systemd service and installer are included under `deploy/systemd/` and `scripts/install_systemd.sh`.

## Docker setup

Docker is intended here as an isolated shadow/soak witness. Container audio is
disabled, so it does not replace the verified native Windows alarm.

```bash
cp config.docker.example.yaml config.docker.yaml
python scripts/generate_tokens.py > .env.docker
docker compose up -d --build
docker compose logs -f crypto-sentinel
```

The Compose file publishes the shadow dashboard only on host loopback port
`8788`, uses a read-only root filesystem, bounded process/log settings, a
60-second shutdown grace period, and dropped Linux capabilities. Configuration
validation refuses a non-loopback bind without an access token.

See `docs/RELIABILITY_ROADMAP.md` for the soak/fault sequence and the
reliability gate that precedes the premium interactive dashboard redesign.

## Wheel-only install

The wheel contains the Python package, dashboard assets, and bundled starter config. Deployment scripts are in the source archive.

```bash
python -m venv .venv
. .venv/bin/activate                 # Windows: .venv\Scripts\Activate.ps1
pip install crypto_sentinel_free-0.1.0-py3-none-any.whl
crypto-sentinel init-config --output config.yaml
crypto-sentinel --config config.yaml simulate
```

## Mobile alarms with ntfy

The install scripts generate a long random `NTFY_TOPIC`. Enable ntfy in `config.yaml`:

```yaml
notifiers:
  ntfy:
    enabled: true
    server: https://ntfy.sh
    topic: ${NTFY_TOPIC:-}
    token: ${NTFY_TOKEN:-}
    minimum_severity: warning
    click_url: ''
```

Subscribe to the same topic in the ntfy app. On the public service, an unauthenticated topic name acts like a shared secret, so keep the generated value private. Use authenticated or self-hosted ntfy for sensitive alert text.

Leave `click_url` empty unless the dashboard has a deliberately secured address reachable from the receiving device. `127.0.0.1` on a phone refers to the phone, not the monitoring PC or VPS.

Test delivery:

```bash
crypto-sentinel --config config.yaml test-alert --severity critical
```

## Telegram

Create a Telegram bot, send it a message, obtain the chat ID, and place both values in `.env`:

```dotenv
TELEGRAM_BOT_TOKEN=123456:replace-me
TELEGRAM_CHAT_ID=123456789
```

Enable the Telegram notifier in `config.yaml`, then run `test-alert`. Use a dedicated bot/chat and never put exchange credentials, private keys, or seed phrases in this project.

## External alert ingestion

The dashboard exposes an authenticated JSON endpoint:

```text
POST /api/ingest
Authorization: Bearer <INGEST_TOKEN>
Content-Type: application/json
```

Example:

```json
{
  "source": "tabchart",
  "severity": "critical",
  "category": "price_cross",
  "symbol": "BTCUSDT",
  "direction": "down",
  "title": "BTC crossed emergency level",
  "message": "BTCUSDT crossed below the configured invalidation level.",
  "dedup_key": "tabchart:BTCUSDT:emergency-down"
}
```

Use `scripts/send_test_webhook.ps1` or `scripts/send_test_webhook.sh` to verify it. Controls include a 64 KiB request limit, mandatory JSON content type, bearer authentication, browser cross-origin rejection, field-length limits, finite-metric sanitization, timestamp clamping, and normal cooldown handling.

For TabChart's **run local application** delivery, point it at `powershell.exe` and map the installed version's alert fields into `scripts/tabchart_bridge.ps1`:

```text
-NoProfile -ExecutionPolicy Bypass -File "C:\path\crypto-sentinel-free\scripts\tabchart_bridge.ps1" -Symbol "BTCUSDT" -Direction "down" -Severity "critical" -Title "TabChart BTC alert" -Message "Configured level crossed"
```

The bridge reads `INGEST_TOKEN` from the project `.env` when it is not supplied explicitly. TabChart placeholder names vary by release, so the project deliberately does not invent them.

## Command reference

```text
crypto-sentinel --config config.yaml run
crypto-sentinel --config config.yaml validate-config
crypto-sentinel --config config.yaml doctor [--skip-network] [--json]
crypto-sentinel --config config.yaml simulate [--json]
crypto-sentinel --config config.yaml test-alert --severity warning|critical
crypto-sentinel init-config --output config.yaml
```

## Recommended initial policy

Start with the bundled balanced thresholds and monitor only liquid majors. Do not make the rules more sensitive immediately. Run for several days and record:

- alerts per day and symbol
- single-venue versus multi-venue confirmation
- event-to-notification delay
- duplicate or noisy categories
- whether the event was still useful when received
- expected events the detector missed

A useful target is a small number of warnings and fewer than a few critical alarms per week, but the right rate depends on symbols, regime, and purpose. See `docs/TUNING.md`.

## Important limitations

- This is an attention and risk-monitoring system, not a validated trading strategy.
- Binance's force-order stream is snapshot-based and can undercount dense liquidation activity.
- OKX quote-volume accuracy depends on successfully loading current contract metadata.
- Public schemas and endpoints can change; feed-health alarms and maintenance remain necessary.
- Local detection stops when the host sleeps, loses power, or the process exits.
- A hard crash can lose up to one checkpoint interval of baseline state.
- Remote retries can produce duplicate messages when a provider accepts a request but its response is lost.
- Remote notification queues are in memory; an abrupt process failure can lose unsent notifications.
- Cross-exchange confirmation reduces false positives but can delay or miss venue-specific incidents.
- No public retail feed guarantees complete, lossless, or zero-latency market data.
- The default thresholds are engineering starting points, not backtested alpha.

Read `docs/ARCHITECTURE.md`, `docs/SECURITY.md`, `docs/TUNING.md`, `docs/PROTOCOL_REFERENCES.md`, and `PROJECT_STATUS.md`.

## Development

```bash
python -m venv .venv
. .venv/bin/activate                 # Windows: .venv\Scripts\Activate.ps1
pip install -e ".[dev]"
pytest -q
python -m compileall -q src
```

The package is pure Python and builds as a platform-independent wheel.
`requirements.lock` records the complete tested runtime dependency closure;
`pyproject.toml` retains compatible ranges so normal installs can receive
maintenance updates. The Docker image additionally uses a digest-pinned Python
base and `requirements.docker.lock`, whose CPython 3.12 Linux x86-64 wheels are
individually SHA-256 pinned.

## License

MIT. See `LICENSE`.
