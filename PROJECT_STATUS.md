# Project status

Release-candidate date: 2026-07-24
Target-host verification: 2026-07-24
Version: 0.2.0

## Implemented

- Keyless Binance USDⓈ-M aggregate-trade and force-order feed adapter using the current routed market endpoint
- Keyless Bybit linear public-trade and full-liquidation adapter
- Keyless OKX swap-trade adapter with public instrument-metadata conversion
- Reconnect/backoff with jitter, explicit heartbeat behavior, feed-wide valid
  payload freshness, subscription acknowledgement, whole-feed/pong-only
  watchdogs, per-symbol diagnostics, and queue-drop counters
- Fixed-time buckets with late/out-of-order event handling
- Continuous-window coverage and post-gap recovery; outage jumps cannot be
  labeled as short-window moves until the window is continuous again
- Future-timestamp quarantine, receive-time normalization, and monotonic
  liveness/clock-adjustment tracking
- Bounded trade and liquidation deduplication around reconnects
- Median/MAD robust return and volume scores
- Multi-window price/volume/order-flow anomaly rules
- Cross-exchange confirmation
- Liquidation burst detection and directionally consistent market-shock merging
- Cross-exchange price-divergence alarms
- Overlapping-window suppression, cooldowns, severity upgrades, and post-restart cooldown restoration
- SQLite alert and metric persistence with WAL
- Atomic gzip state checkpoint and baseline restoration
- Successful-checkpoint write age, last failure, and path state enforced by
  authenticated readiness after the startup allowance
- Local OS alarm, browser sound/notification, ntfy, Telegram, and generic webhook delivery
- Priority-aware local-alarm queue with checked return codes, retries,
  persisted queued/attempt/success/failure receipts, and cooldown clearing after
  final local-alarm failure
- Non-blocking local/remote delivery, bounded queues, and three remote attempts
- Authenticated external alert ingestion with JSON-only and browser cross-origin controls
- Supervised critical asyncio tasks with heartbeats and nonzero termination when
  a critical task exits unexpectedly
- Authenticated readiness covering expected critical tasks, task heartbeat
  state, feed-wide transport freshness, subscription acknowledgement, queue
  pressure, drops, and consumer lag
- Runtime queue-health/recovery alarms for new drops, sustained pressure, and
  excessive consumer lag
- Premium responsive dashboard with server-sent alerts, native/shadow
  awareness, readiness/operations rails, persisted metric timelines, alarm
  receipts, filters, evidence drawer, browser-local review notes, and CSV/JSON
  export
- Rotating file logs
- Windows, Linux, Docker, and hardened systemd deployment material
- Single-instance Windows guardian and supervisor, separate Docker shadow, bounded
  authenticated primary/shadow soak capture, and fail-closed soak reports
- Deterministic offline crash/liquidation simulation
- Deployment `doctor` with local checks and live WebSocket probes for every
  configured exchange-symbol mapping plus Bybit/OKX acknowledgement validation

## Final 0.2.0 target-host validation

- Package and runtime versions agree on `0.2.0`
- All four tracked local/Docker/VPS/bundled examples load through the real
  schema with `minimum_window_coverage: 0.8`,
  `maximum_data_gap_seconds: 15`, and `max_future_skew_seconds: 5`
- 143/143 unit and integration tests passed
- Python bytecode compilation passed
- Whole-tree Ruff lint and formatting passed
- Dependency integrity, PowerShell parsing, dashboard JavaScript parsing, and
  Git whitespace checks passed
- Native and Docker real configuration validation passed
- Deterministic simulation emitted exactly one critical three-exchange market
  shock with $2.7M corroborating liquidations
- Credential-free live doctor parsed every one of the 15 configured
  exchange-symbol mappings and verified all Bybit/OKX acknowledgements
- Native and Docker health endpoints are HTTP 200 ready; the Docker container
  is healthy with restart count 0
- Live Bybit socket closures produced successful local `monitoring_gap` alarms
  and subsequent `monitoring_recovery` events
- Premium dashboard desktop and 390×844 mobile browser acceptance passed,
  including filtering, evidence drawer, alarm receipts, local review notes,
  export controls, and responsive overflow checks

The exact commands, results, fixed issues, artifact hashes, live fault
evidence, and limitations are recorded in
`docs/RELEASE_CHECKS_0.2.0.md`.

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

## Target Windows host baseline carried forward from the previous release

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

These checks established that the host, public feeds, speaker, browser, Docker,
and supervisor can work. They are not a substitute for restarting and
revalidating the new 0.2.0 code or completing the timed gates below.

## Not verified in the original build environment

Outbound DNS/network access was unavailable, so these could not be exercised here:

- live Binance, Bybit, or OKX WebSocket handshakes
- current end-to-end event latency
- real ntfy, Telegram, or generic-webhook delivery
- retry behavior against real provider failure modes
- Windows speaker output and scheduled-task behavior
- Chrome notification permission/background behavior on the target machine
- long-duration memory/CPU behavior under production market load

The target-host validation closed the original live-connectivity, Docker,
Windows sound, browser-interaction, process-restart, and reconnect-observation
gaps. Version 0.2.0 exposes per-symbol freshness diagnostics and checks every
configured symbol in `doctor`; public streams still do not provide an ongoing
authoritative per-symbol heartbeat. The first strict one-hour report had enough
elapsed time but correctly remained incomplete after one detector-unready BNB
sample following a live Bybit reconnect. A new gate then caught a second real
Bybit socket closure. A clean one-hour gate and the 24-hour/72-hour gates, plus
physical reboot/login and lock/unlock tests, remain open as described in
`docs/RELIABILITY_ROADMAP.md`.

## Operational readiness gate

Do not rely on the system until all of these pass on the target machine:

1. `simulate` emits one critical market shock.
2. `validate-config` succeeds.
3. `doctor` acknowledges and observes market data for every configured
   exchange-symbol mapping.
4. Authenticated readiness is `ready`, every expected critical task and
   feed-wide transport watchdog is fresh, `doctor` has observed every configured
   symbol, and queue drop/lag counters are clear.
5. Dashboard critical test records a successful local-alarm receipt and is
   audible with Chrome foregrounded and backgrounded.
6. Process-level sound is audible while Chrome is closed.
7. Every enabled remote notifier receives a critical test.
8. The one-hour, 24-hour, and 72-hour soak reports each pass with
   `--require-complete`.
9. Lock, network loss, and reconnect behavior are understood; a deliberate
   interruption produces feed-health and recovery evidence.
10. Alert volume is reviewed for several days before thresholds are trusted.

## Highest-value next engineering work

1. Complete and publish the one-hour, 24-hour, and 72-hour 0.2.0 soak reports.
2. Add an external dead-man heartbeat on an independent host; the native,
   Docker, guardian, supervisor, and soak processes currently share one PC.
3. Capture and replay normalized raw events for reproducible backtests and
   latency measurements.
4. Add per-symbol or liquidity-tier thresholds.
5. Add order-book depth/spread deterioration and open-interest/funding data.
6. Add a durable remote-notification outbox when delivery guarantees justify
   the added complexity.
7. Add optional AI summaries outside the deterministic trigger path.
