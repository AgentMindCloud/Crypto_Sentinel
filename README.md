# Crypto Sentinel Free

A zero-subscription, keyless crypto market anomaly alarm. It consumes **public** Binance, Bybit, and OKX futures data, applies deterministic rules, sounds a local alarm, serves a premium interactive browser dashboard, and can escalate through ntfy, Telegram, or a generic webhook.

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
- Feed-wide payload freshness plus per-symbol informational ages and
  subscription-acknowledgement tracking; a quiet symbol does not force a
  reconnect while another valid market stream proves the multiplexed socket is
  flowing
- Market-payload watchdogs that reconnect pong-only or wholly silent feeds
- Continuous-window coverage rules that keep post-outage price gaps out of
  short-window anomaly calculations until the window recovers
- Future-timestamp quarantine and monotonic liveness tracking
- Atomic gzip checkpoints of rolling baselines and recent liquidations
- Successful-checkpoint write age, last failure, and path state included in
  authenticated readiness
- Alert cooldown state restored from SQLite after restart
- Severity upgrades allowed during cooldown
- Supervised critical tasks with heartbeat state exposed through authenticated
  readiness telemetry
- Queue high-water, drop, and consumer-lag monitoring with health/recovery alarms
- Separate local-alarm and remote-notifier execution, so a slow webhook does not stall detection
- Checked, priority-aware local-alarm retries with persisted
  queued/attempt/success/failure receipts; a failed alarm does not retain its
  cooldown
- Three delivery attempts for each enabled remote notifier
- Persistence errors logged without suppressing browser/local alarm delivery
- Bounded market-event and remote-notification queues
- A credential-free `doctor` that probes every configured exchange-symbol
  mapping and validates Bybit/OKX subscription acknowledgements
- A single-instance Windows supervisor, isolated Docker shadow, bounded soak
  recorder, and fail-closed soak report
- Rotating logs and feed-health/recovery alarms

This is still best-effort monitoring, not an exactly-once safety system. Read the limitations below before relying on it.

## Fastest Windows setup

Requirements: Windows 10/11, Python 3.11 or newer, and Chrome or another modern browser.

```powershell
Set-ExecutionPolicy -Scope Process Bypass
.\scripts\install_windows.ps1
.\scripts\run_windows.ps1
```

The installer creates `.venv`, installs the project, copies `config.example.yaml` to `config.yaml`, and generates random dashboard/ingest tokens in `.env`. Existing config and secrets are not overwritten. An enabled dashboard now requires a nonempty access token even on loopback. The app opens its loopback dashboard and supplies the generated token once; the page stores it locally and removes it from the address bar.

On the dashboard:

1. Click **Arm browser sound**.
2. Click **Enable browser notifications**.
3. Click **Test critical alarm** and verify both the Python process alarm and browser alarm.
4. Keep the dashboard pinned. The process-level sound remains primary because browsers can suspend background tabs.

Run all three preflight checks before relying on live data:

```powershell
.\.venv\Scripts\crypto-sentinel.exe --config config.yaml simulate
.\.venv\Scripts\crypto-sentinel.exe --config config.yaml validate-config
.\.venv\Scripts\crypto-sentinel.exe --config config.yaml doctor --timeout 15
```

`doctor` performs real WebSocket probes on the target machine, observes a
parsed trade for every configured symbol, and validates Bybit/OKX subscription
acknowledgements. Do not treat the deployment as live until every enabled
exchange passes.

At runtime, `stale_after_seconds` is a feed-wide transport watchdog: the socket
reconnects only when no valid trade or liquidation arrives anywhere on that
multiplexed feed. Per-symbol ages remain visible for diagnosis, and detector
freshness prevents an old symbol price from producing a price alert, but a
single naturally quiet symbol does not by itself prove a broken subscription.
Public trade streams provide no per-symbol heartbeat, so a partial
subscription stall cannot be distinguished with certainty from a quiet market;
rerun `doctor` when a symbol remains silent unexpectedly.

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

The registration script first tries one limited, interactive guardian Scheduled Task.
If local Windows policy denies standard-user task registration, it installs a
per-user Startup shortcut instead. That shortcut runs a single-instance
guardian, which waits on and restarts the single-instance supervisor with a
bounded backoff after unexpected exits. The guardian rotates `data/guardian.log`
and sounds an independent critical PC alarm before restarting. The supervisor
restarts the native audible primary and soak monitor, starts Docker Desktop if
needed, and recovers the Docker shadow. Neither process terminates its child at
ordinary logoff. No administrator or exchange credentials are required. Use
`.\scripts\unregister_windows_startup.ps1` to remove either startup method.
Before using the shortcut path, including `-StartupShortcutOnly`, registration
must remove and verify the absence of the legacy main, Docker-shadow, and soak
tasks. It refuses to create the shortcut if Task Scheduler cannot be inspected
or cleaned.

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

Then open `http://127.0.0.1:8787/` locally. A hardened systemd service and
installer are included under `deploy/systemd/` and `scripts/install_systemd.sh`.
The systemd installer copies an explicit code/build allowlist into a root-owned
install tree; local configuration, VCS metadata, caches, reports, and runtime
data are not copied. Configuration/secrets are root-owned and service-group
readable, while runtime data alone is writable by the service account.

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
validation refuses any enabled dashboard without a nonempty access token,
including a loopback-only dashboard.

See `docs/RELIABILITY_ROADMAP.md` for the implemented reliability phases,
remaining host-loss limitations, and timed soak gates.

## Premium dashboard

Open the native audible primary at `http://127.0.0.1:8787/`. The Docker shadow,
when enabled, is at `http://127.0.0.1:8788/` and intentionally has no speaker
alarm.

The dashboard now includes:

- a truthful readiness rail for feed freshness, critical-task heartbeats,
  queue pressure, alarm receipts, and any supplied supervisor/checkpoint state;
- native-primary versus Docker-shadow awareness;
- venue and per-symbol health, subscription state, detector recovery reasons,
  and persisted metric timelines;
- persisted local-alarm receipt history;
- interactive symbol, venue, severity, category, time, search, and review
  filters;
- an alert evidence drawer with detector metrics;
- browser-local acknowledgements and notes, plus filtered CSV/JSON export;
- separate warning and critical end-to-end alarm controls;
- optional soak/recovery history when that evidence is supplied.

Acknowledgements and notes are local browser metadata only. They do not mute,
deduplicate, reconfigure, or otherwise change the detector. The dashboard has no
order, wallet, signing, trading, or withdrawal controls.

## Timed soak gates

Start a clean evidence interval after deliberate fault tests, keep the
supervisor running, and retain the UTC start time:

```powershell
$gateStart = (Get-Date).ToUniversalTime().ToString("o")
$gateStart | Set-Content -LiteralPath data\soak-gate-start.txt -Encoding ascii
```

Run each gate only after its full duration has elapsed:

```powershell
$gateStart = (Get-Content -LiteralPath data\soak-gate-start.txt -Raw).Trim()
.\.venv\Scripts\python.exe scripts\soak_report.py --input data\soak.jsonl --since $gateStart --hours 1 --interval 60 --min-coverage 0.98 --require-complete
.\.venv\Scripts\python.exe scripts\soak_report.py --input data\soak.jsonl --since $gateStart --hours 24 --interval 60 --min-coverage 0.98 --require-complete
.\.venv\Scripts\python.exe scripts\soak_report.py --input data\soak.jsonl --since $gateStart --hours 72 --interval 60 --min-coverage 0.98 --require-complete
```

`--require-complete` exits nonzero for insufficient time or coverage, invalid
rows or duplicate timestamps, missing/malformed native or shadow evidence,
instance failures, detector baseline/continuity/recovery failures, monitor
gaps, queue/feed drops, clock quarantine or adjustment, local-alarm failure,
or runtime/container restart. Every retained sample must explicitly include
runtime identity, readiness, nonempty feeds, all expected tasks, queue, clock,
alarm-count, checkpoint, and per-market/per-venue evidence. Docker must also
have been observed in that sample as present, running, and healthy; an absent
Docker CLI/engine/container or an unknown health state fails closed. A partial
detector-ready count caused only by `ready` plus `stale_data` is recorded as an
informational quiet-market sample when authenticated runtime readiness and
every feed are healthy; it does not fail the timed gate. The report retains
those samples/pairs and the minimum ready ratio. Zero-ready markets, mixed or
missing reasons, positive recovery time, and any other unready reason remain
fail-closed. A failed gate is evidence to investigate, not a result to edit
away.

## Wheel-only install

The wheel contains the Python package, dashboard assets, and bundled starter config. Deployment scripts are in the source archive.

```bash
python -m venv .venv
. .venv/bin/activate                 # Windows: .venv\Scripts\Activate.ps1
pip install crypto_sentinel_free-0.2.0-py3-none-any.whl
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

The 0.2.0 examples also make the continuity and clock boundaries explicit:

```yaml
detector:
  minimum_window_coverage: 0.8
  maximum_data_gap_seconds: 15
  max_future_skew_seconds: 5
```

These are detector integrity controls, not sensitivity shortcuts. Ordinary
trade silence is not treated as an outage. Continuity gaps come only from
explicit feed disconnect/stall, local queue loss, process/checkpoint downtime,
or clock/runtime interruption evidence. An active gap is always fail-closed. A closed gap is
eligible only when every individual break is no longer than
`maximum_data_gap_seconds` and the union of all breaks still leaves at least
`minimum_window_coverage` of the detector window observed. Reconnect and
restart gaps close independently per symbol only after a fresh trade is
accepted; subscription acknowledgements, pongs, liquidations, duplicates,
replays, and quarantined future events do not restore price continuity.
Exchange timestamps beyond the skew allowance are quarantined rather than
allowed to distort freshness or cooldown behavior.

## Important limitations

- This is an attention and risk-monitoring system, not a validated trading strategy.
- Binance's force-order stream is snapshot-based and can undercount dense liquidation activity.
- OKX quote-volume accuracy depends on successfully loading current contract metadata.
- Public schemas and endpoints can change; feed-health alarms and maintenance remain necessary.
- Local detection stops when the host sleeps, loses power, or the process exits.
- The native primary, Docker shadow, guardian, supervisor, and soak recorder
  are all on the same PC. They cannot provide independent coverage while that
  host is asleep, powered off, or disconnected; use a second always-on host for
  that.
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
