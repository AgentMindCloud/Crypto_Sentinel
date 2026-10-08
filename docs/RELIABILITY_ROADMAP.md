# Reliability roadmap

Crypto Sentinel 0.2.0 remains monitoring-only. It uses public market feeds and
cannot connect to a wallet, place or sign an order, trade, or withdraw funds.
Reliability claims below distinguish implemented controls from elapsed-time and
independent-host evidence that still does not exist.

## Current deployment model

- **Native Windows primary:** the only instance allowed to produce the verified
  Windows speaker alarm. It listens on `127.0.0.1:8787`. The tested host's
  standard-user Task Scheduler registration was denied by Windows policy, so a
  verified per-user Startup shortcut launches a single-instance guardian.
- **Docker shadow:** an isolated detector and evidence source on
  `127.0.0.1:8788`. It has separate tokens, SQLite state, checkpoint, logs, and
  no local or remote notifier. It must never replace the audible primary.
- **Guardian:** owns a separate per-user mutex, waits on and restarts the
  supervisor after unexpected exits with bounded backoff, rotates a bounded
  log, and sounds an independent critical PC alarm before restart. It does not
  terminate the supervisor during ordinary logoff.
- **Supervisor:** restarts the native child and soak recorder, recovers the
  Docker shadow, polls authenticated readiness, writes bounded status, and
  sounds a watchdog alarm after repeated primary/shadow failures or a tracked
  native-process exit.
- **Soak recorder:** samples authenticated readiness from both instances every
  minute into bounded, ignored `data/soak.jsonl`. A long sampling gap records
  sleep, suspension, or recorder failure after the host resumes.

These components share one PC. No program on that PC can monitor or sound while
the PC is asleep, powered off, or disconnected. True host-loss coverage
requires an always-on second machine or VPS. Power-loss coverage also benefits
from a UPS and firmware configured to restore power automatically.

## Phase 1 — runtime truthfulness

Implemented in 0.2.0:

- Every expected critical background task is registered, heartbeated, exposed
  through authenticated status, and treated as fatal if it exits unexpectedly.
- Readiness includes expected-task presence and heartbeat, queue pressure,
  queue drops/lag, feed connectivity, subscription acknowledgement, and
  feed-wide valid-market-payload freshness.
- Successful-checkpoint write age, last failure, file presence, and path age
  are exposed and can make readiness fail after the startup allowance.
- Feed summary age represents the newest valid payload anywhere on the
  multiplexed socket. Per-symbol ages are exposed as informational diagnostics;
  absence of trades for one symbol is not proof of transport failure.
- Bybit and OKX validate subscription responses. Missing, rejected, duplicate,
  or unexpected acknowledgements prevent readiness.
- Feed watchdogs reconnect sockets when valid market payloads stop even if pong
  traffic continues.
- The public `/api/healthz` endpoint reports readiness and returns HTTP 503
  while starting or degraded instead of returning unconditional success.

Still open:

- Liveness and readiness are represented by readiness/status rather than two
  independently specified endpoints.

## Phase 2 — alarm correctness and delivery

Implemented in 0.2.0:

- Detector windows require configured coverage and a bounded largest data gap.
  After an outage, affected windows report `recovering_after_gap` and cannot
  emit a short-window move until a continuous window has elapsed.
- Future exchange timestamps beyond the configured skew allowance are
  quarantined. Small future skew is normalized to trusted receive time, and
  liveness uses monotonic time.
- Local alarms use a bounded critical-priority queue, checked player/command
  return values, three attempts, safe error details, and persisted
  `queued`/`attempt`/`success`/`failure` receipts.
- A final local-alarm failure clears the cooldown entry, so an unverified sound
  is not treated as delivered after restart.
- New market-event drops, sustained queue pressure, and excessive consumer lag
  produce runtime-health alarms and explicit recovery alerts.
- Feed incidents produce health and recovery alerts with missing subscription
  or whole-feed payload-stall evidence.

Partially implemented:

- Network/feed monitoring gaps generate feed health/recovery alerts while a
  surviving local process can run. Host sleep or power loss is recorded by the
  soak timestamp gap only after the PC resumes; the same PC cannot alarm during
  the outage.
- Local alarm receipts are durable. Optional remote notifiers still retry from
  an in-memory queue and do not have a durable outbox.

## Phase 3 — diagnostic and configuration coverage

Implemented in 0.2.0:

- `doctor` observes a parsed trade for every configured symbol on Binance,
  Bybit, and OKX.
- `doctor` validates the Bybit subscription response for all trade and
  liquidation topics and every individual OKX trade-topic acknowledgement.
- Runtime tests cover rejected/duplicate acknowledgements, pong-only payload
  stalls, whole-feed silence, quiet symbols beside active symbols, duplicate
  replay, queue saturation, future
  timestamps, post-gap recovery, local-alarm retry/failure, and critical-task
  exit.
- Cross-field configuration validation rejects empty/duplicate windows,
  impossible confirmation counts, weaker critical thresholds, invalid
  liquidation/spread ordering, duplicate venue mappings, and a maximum data gap
  that does not fit the shortest detector window.

Important boundary:

- Sparse liquidation streams can be subscription-acknowledged, but `doctor`
  cannot require a real liquidation event to occur during a short probe.
- Public exchange schema changes remain an operational risk even with fixture
  coverage and live probes.

## Phase 4 — elapsed-time and fault evidence

Completed on the tested host on 2026-07-23:

- Docker child-process crash/restart
- Docker network disconnect/reconnect
- Docker pause/unpause
- Native primary child-process crash/recovery
- Duplicate-supervisor exclusion
- Guardian mutex, restart, log-rotation, and alarm paths passed static and
  parser checks; live guardian duplicate/restart behavior remains to be repeated
- Continuous authenticated primary/shadow sampling

Still required for the 0.2.0 release candidate:

- clean one-hour supervised soak
- clean 24-hour normal soak
- clean 72-hour normal soak
- real Windows lock/unlock interval with a subsequent audible critical test
- reboot/login validation of the updated guardian, supervisor, and both updated
  instances
- review and retention of each generated report

The previous controlled fault samples are useful evidence but must not be mixed
into a clean timed gate. Record a new UTC starting point after fault testing:

```powershell
$gateStart = (Get-Date).ToUniversalTime().ToString("o")
$gateStart | Set-Content -LiteralPath data\soak-gate-start.txt -Encoding ascii
```

After enough wall-clock time has elapsed, run the exact fail-closed gates:

```powershell
$gateStart = (Get-Content -LiteralPath data\soak-gate-start.txt -Raw).Trim()
.\.venv\Scripts\python.exe scripts\soak_report.py --input data\soak.jsonl --since $gateStart --hours 1 --interval 60 --min-coverage 0.98 --require-complete
.\.venv\Scripts\python.exe scripts\soak_report.py --input data\soak.jsonl --since $gateStart --hours 24 --interval 60 --min-coverage 0.98 --require-complete
.\.venv\Scripts\python.exe scripts\soak_report.py --input data\soak.jsonl --since $gateStart --hours 72 --interval 60 --min-coverage 0.98 --require-complete
```

Add `--json` when a machine-readable report is needed. With
`--require-complete`, the command exits nonzero for insufficient elapsed time or
coverage, invalid JSONL rows, primary/shadow failures, detector
baseline/continuity/recovery failures, sampling gaps, event drops, future-event
quarantine, clock adjustment, local-alarm failure, or any runtime/container
restart.

A partially ready detector market is classified as an informational
quiet-market sample only when its exact reasons are `ready` plus `stale_data`,
at least one venue remains alert-eligible, authenticated instance readiness is
true, every feed is healthy, and no recovery time remains. The report retains
`quiet_detector_samples`, `quiet_detector_pairs`, and the minimum detector-ready
ratio. Zero-ready markets, missing or malformed evidence, mixed reasons, and
all baseline, continuity, or recovery states remain blocking. Public streams
still cannot prove that a single silent symbol is subscribed rather than merely
quiet, so the every-symbol `doctor` remains the validation gate.

Do not delete inconvenient rows to manufacture a pass; start a documented new
gate after investigating the cause.

## Premium interactive dashboard

Implemented in 0.2.0:

- Cinnabar Glass responsive identity with reduced-motion support and no external
  runtime assets
- native-primary/Docker-shadow and supervisor awareness when supplied
- readiness, expected critical tasks, queue, per-venue and per-symbol health
- persisted detector metric timelines and local-alarm delivery receipt history
- interactive symbol, venue, severity, category, time, search, and review
  filters
- alert evidence drawer with exact detector metrics
- browser-local acknowledgement/notes and filtered CSV/JSON exports
- separate warning and critical end-to-end alarm controls
- soak/recovery presentation when such evidence is supplied

The dashboard does not invent missing telemetry. Checkpoint or soak panels show
“not reported” until the backend supplies that evidence. Browser-local review
metadata does not suppress alarms or change configuration.

## Inputs that materially improve detection

The most valuable user-supplied dataset is not an API key. It is:

- timestamps and symbols for real events that should have been warning or
  critical;
- maximum acceptable detection delay for each event type;
- ordinary periods that should not alert;
- acceptable warnings per day and critical false positives per month;
- the exact symbol universe and whether venue-specific incidents matter.

Those labels enable public-feed capture/replay, per-symbol or liquidity-tier
thresholds, and honest false-positive/false-negative measurement without
exchange credentials or trading permissions.

## Highest-value remaining work

1. Complete the one-hour, 24-hour, and 72-hour gates.
2. Add an independent-host dead-man heartbeat and optional remote delivery for
   actual host-loss coverage.
3. Add a durable remote-notification outbox if stronger delivery guarantees
   justify its complexity.
4. Capture/replay normalized public events for repeatable false-positive,
   false-negative, and latency measurements.
