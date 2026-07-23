# Project status

Release build date: 2026-07-22
Target-host verification: 2026-07-23
Version: 0.1.0

## Implemented

- Keyless Binance USDⓈ-M aggregate-trade and force-order feed adapter using the current routed market endpoint
- Keyless Bybit linear public-trade and full-liquidation adapter
- Keyless OKX swap-trade adapter with public instrument-metadata conversion
- Reconnect/backoff with jitter, explicit heartbeat behavior, stale-feed health, and queue-drop counters
- Fixed-time buckets with late/out-of-order event handling
- Bounded trade and liquidation deduplication around reconnects
- Median/MAD robust return and volume scores
- Multi-window price/volume/order-flow anomaly rules
- Cross-exchange confirmation
- Liquidation burst detection and directionally consistent market-shock merging
- Cross-exchange price-divergence alarms
- Overlapping-window suppression, cooldowns, severity upgrades, and post-restart cooldown restoration
- SQLite alert and metric persistence with WAL
- Atomic gzip state checkpoint and baseline restoration
- Local OS alarm, browser sound/notification, ntfy, Telegram, and generic webhook delivery
- Non-blocking local/remote delivery, bounded remote queue, and three remote attempts
- Authenticated external alert ingestion with JSON-only and browser cross-origin controls
- Live dashboard with server-sent events, operational market cards, and browser security headers
- Rotating file logs
- Windows, Linux, Docker, and hardened systemd deployment material
- Deterministic offline crash/liquidation simulation
- Deployment `doctor` with local checks and live WebSocket probes

## Verified in the original build environment

- Python bytecode compilation
- 29 offline unit/integration tests in the original release gate
- Deterministic simulation emits exactly one critical three-exchange market shock with corroborating liquidations
- Local, VPS, and Docker example configuration validation
- Local `doctor --skip-network`
- Authenticated dashboard/API smoke test
- Rejection of unauthenticated, cross-site, and non-JSON mutation requests
- Security response headers
- External webhook ingestion and SQLite persistence
- Graceful shutdown, final checkpoint, checkpoint restoration, and cooldown restoration
- Browser UI rendering with mocked API data in headless Chromium
- Platform-independent wheel build and package-data inspection
- Installed-wheel smoke from an isolated site-packages path: bundled config, CLI, dashboard, webhook ingestion, SQLite, and checkpoint

Exact release commands and results are recorded in `RELEASE_CHECKS.txt`.

## Verified on the target Windows host

- SHA-256 verification of the supplied source archive and wheel
- Isolated Python 3.12 virtual environment and dependency installation
- Ruff lint and format gates, Python compilation, and 32/32 tests
- Native and Docker configuration validation and deterministic simulation
- Credential-free live doctors for Binance, Bybit, and OKX
- Audible Windows process alarm and browser sound/notification controls
- Native dashboard on `127.0.0.1:8787` and isolated Docker shadow on
  `127.0.0.1:8788`
- Digest-pinned Docker base, SHA-256-pinned Linux wheels, read-only root
  filesystem, dropped capabilities, bounded logs, and authenticated health
- Docker child crash/restart, network disconnect/reconnect, and pause/unpause
- Native child crash/recovery through the single-instance Windows supervisor
- Per-user automatic startup shortcut; Task Scheduler registration was
  attempted but denied by the host's standard-user Windows policy
- Continuous bounded primary/shadow soak samples
- Public Git staging checked against the actual local token values; local
  tokens, configs, database, logs, checkpoints, virtual environment, and setup
  report remain excluded

## Not verified in the original build environment

Outbound DNS/network access was unavailable, so these could not be exercised here:

- live Binance, Bybit, or OKX WebSocket handshakes
- current end-to-end event latency
- real ntfy, Telegram, or generic-webhook delivery
- retry behavior against real provider failure modes
- Windows speaker output and scheduled-task behavior
- Chrome notification permission/background behavior on the target machine
- long-duration memory/CPU behavior under production market load

The target-host checks above close the original live-connectivity, Docker,
Windows sound, and browser-interaction gaps. Multi-hour/multi-day evidence,
real host sleep/lock testing, and complete per-symbol subscription diagnostics
remain open as described in `docs/RELIABILITY_ROADMAP.md`.

## Operational readiness gate

Do not rely on the system until all of these pass on the target machine:

1. `simulate` emits one critical market shock.
2. `validate-config` succeeds.
3. `doctor` passes every configured live feed.
4. Dashboard critical test is audible with Chrome foregrounded and backgrounded.
5. Process-level sound is audible while Chrome is closed.
6. Every enabled remote notifier receives a critical test.
7. All feeds remain healthy for at least one hour.
8. Sleep, lock, network loss, and reconnect behavior are understood.
9. A deliberate network interruption produces feed-health alarms through surviving channels.
10. Alert volume is reviewed for several days before thresholds are trusted.

## Highest-value next engineering work

1. Capture and replay normalized raw events for reproducible backtests and latency measurements.
2. Add per-symbol or liquidity-tier thresholds.
3. Add order-book depth/spread deterioration and open-interest/funding data.
4. Add a durable notification outbox when delivery guarantees justify the added complexity.
5. Add an external dead-man heartbeat independent of this process.
6. Run multi-day soak/load tests and publish CPU, memory, queue, and latency measurements.
7. Add optional AI summaries outside the deterministic trigger path.
