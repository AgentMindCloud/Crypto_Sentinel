# Reliability roadmap

Crypto Sentinel remains monitoring-only. Reliability work is sequenced ahead of
the premium dashboard so a more polished interface does not create false
confidence in incomplete health signals.

## Current deployment model

- **Native Windows primary:** the only instance allowed to produce the verified
  Windows speaker alarm. It listens on `127.0.0.1:8787`. The tested host's
  standard-user Task Scheduler registration was denied by Windows policy, so a
  verified per-user Startup shortcut launches a single-instance supervisor.
- **Docker shadow:** an isolated detector and evidence source on
  `127.0.0.1:8788`. It has separate tokens, SQLite state, checkpoint, logs, and
  no local or remote notifier. It must never replace the audible primary.
- **Soak monitor:** samples authenticated status from both instances every
  minute into a bounded ignored JSONL file. A long gap identifies sleep,
  suspension, or monitor failure after the host resumes.

No process on one PC can monitor or sound while that PC is asleep, powered off,
or disconnected. On the tested host, automatic sleep and hibernation were
disabled at setup time; recheck them after power-plan changes. True host-loss
coverage requires an always-on second device or VPS, and power-loss coverage
also benefits from a UPS.

## Reliability gate before dashboard redesign

### Phase 1 — runtime truthfulness

1. Supervise every critical background task and terminate nonzero if the event
   consumer, detector, health monitor, maintenance loop, or checkpoint loop
   exits unexpectedly.
2. Add separate liveness and readiness endpoints. Readiness must include task
   heartbeats, queue pressure, checkpoint age, feed freshness, and detector
   evaluation heartbeat.
3. Track freshness per `(exchange, symbol)`, not only per exchange.
4. Validate every Bybit/OKX subscription acknowledgement and reconnect if any
   configured topic is rejected.
5. Force reconnect when a socket answers pings but valid market payloads stop.

### Phase 2 — alarm correctness and delivery

1. Require continuous window coverage after a data gap; do not label a
   five-minute outage jump as a 60-second move.
2. Emit a distinct monitoring-gap/recovery alert after sleep or network loss.
3. Add local-alarm attempt/success/failure receipts, checked command return
   codes, retry, and critical-priority delivery.
4. Alarm on new queue drops, sustained high-water pressure, and excessive
   receive-to-consume lag.
5. Quarantine excessive exchange/local timestamp skew and use monotonic receive
   time for liveness.

### Phase 3 — full diagnostic coverage

1. Extend `doctor` from the first symbol to every configured exchange-symbol
   mapping.
2. Require acknowledgement for sparse liquidation topics.
3. Add deterministic fault tests for rejected subscriptions, pong-only stalls,
   one-symbol silence, schema changes, duplicate replay, queue saturation,
   future timestamps, and critical-task crashes.
4. Add stricter cross-field config validation so impossible or weaker critical
   rules fail before startup.

### Phase 4 — evidence

1. One-hour supervised fault soak.
2. Twenty-four-hour normal soak.
3. Seventy-two-hour normal soak.
4. Controlled Docker-shadow network disconnect/reconnect, pause/unpause, and
   PID crash/restart tests.
5. Native supervisor process-crash recovery and real Windows lock/unlock
   audible test.

Completed on the tested host on 2026-07-23: Docker child-process crash/restart,
Docker network disconnect/reconnect, Docker pause/unpause, native primary
child-process crash/recovery, duplicate-supervisor exclusion, and continuous
authenticated primary/shadow samples. Still required: the timed one-hour,
24-hour, and 72-hour soaks; a real lock/unlock interval; and review of the
recorded evidence after each gate.

The evidence set records feed ages, per-symbol readiness (after Phase 1), queue
high-water and drops, reconnect duration, detector latency, checkpoint age,
process/container restarts, CPU/RSS, and alarm-delivery receipts.

## Inputs that materially improve detection

The most valuable user-supplied dataset is not an API key. It is:

- timestamps and symbols for real events that should have been warning or
  critical;
- maximum acceptable detection delay for each event type;
- ordinary periods that should not alert;
- acceptable warnings per day and critical false positives per month;
- the exact symbol universe and whether venue-specific incidents matter.

Those labels enable raw public-feed capture/replay, per-symbol or liquidity-tier
thresholds, and honest false-positive/false-negative measurement.

## Premium interactive dashboard after the reliability gate

The redesign should expose evidence rather than decorative confidence:

- live system rail with native/Docker status, task heartbeat, checkpoint age,
  queue pressure, and last successful audible alarm;
- interactive symbol, venue, severity, category, and time-range filters;
- price, return-z, volume-z, imbalance, liquidation, latency, and feed-gap
  timelines;
- alert detail drawer with exact venue evidence and detector rule trace;
- acknowledgement, notes, export, and test controls separated from live alerts;
- soak/recovery history and explicit monitoring gaps;
- responsive premium dark visual system with sparklines, subtle motion, strong
  critical contrast, and accessible reduced-motion/high-contrast modes.

Configuration editing should remain a later, guarded feature with validation,
preview, explicit apply, and rollback—not a casual live form.
