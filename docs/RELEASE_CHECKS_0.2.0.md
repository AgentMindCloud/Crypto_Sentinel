# Crypto Sentinel Free 0.2.0 release checks

Target host: Windows 11, Python 3.12, Docker Desktop  
Verification window: 2026-07-23 UTC / 2026-07-24 Asia/Saigon  
Base commit: `3e544b3d52d64cc68339ebce89a1114a986ad437`  
Release branch: `codex/reliability-premium-dashboard`

## Verdict

Crypto Sentinel Free 0.2.0 is installed and running locally as a
monitoring-only system:

- Native audible primary: `http://127.0.0.1:8787/`
- Isolated Docker shadow: `http://127.0.0.1:8788/`
- Native and shadow `/api/healthz`: HTTP 200, `ready`
- Docker container: running, healthy, restart count 0
- Windows guardian, supervisor, native worker, and soak monitor: running
- Automatic per-user startup shortcut: installed and points to the guardian
- Exchange API keys, wallet access, order placement, trading, and withdrawal
  capabilities: not added
- ntfy, Telegram, and generic webhook delivery: intentionally disabled

The final automated code gate is green: 143 tests passed, with Ruff formatting
and lint, byte-compilation, dependency integrity, PowerShell parsing, dashboard
JavaScript parsing, and Git whitespace checks all passing.

The strict one-hour soak gate was not forced green. It accumulated 71 samples
over 4,234.385 seconds (1.176 hours) at 100% sample coverage, but correctly
reported `gate_passed: false` because one native BNB sample had only two of
three detector venues ready with `insufficient_window` after a live Bybit
reconnect. The socket closure produced an audible `monitoring_gap` warning with
a successful local-alarm receipt and a subsequent `monitoring_recovery` event.
A new gate marker was started without deleting the evidence; it then caught a
second real Bybit `websocket closed` event at 20:55 UTC and also remains
non-clean. Both runtimes recovered and are currently ready.

## Monitoring-only and secret boundary

- All exchange connections use public market-data endpoints.
- No private exchange endpoint, authenticated exchange client, order model,
  wallet operation, trade executor, or withdrawal path was added.
- A monitoring-only AST/dependency/endpoint regression test rejects those
  capability classes and dangerous dependency additions.
- The local dashboard token protects status/history APIs but is not an exchange
  credential.
- `.env`, `.env.docker`, `config.yaml`, `config.docker.yaml`, `data/`, and
  `.venv/` remain ignored and untracked.
- The public-source staging audit checks the staged tree against the actual
  ignored local token values without printing them.

## Supplied artifact SHA-256 verification

The retained 0.1.0 artifacts were rehashed on the target host:

| Artifact | SHA-256 |
|---|---|
| `crypto-sentinel-free-0.1.0-source.zip` | `8dd6b9eb3fdf7b07b6aa10e236cf12a159818aeefd1d39af25820df5c61a4398` |
| `crypto_sentinel_free-0.1.0-py3-none-any.whl` | `139102ee9f728023da6bfe5643a7a947f4126524d7e220ab25de00fb6912b725` |
| `crypto-sentinel-free-0.1.0-RELEASE_CHECKS.txt` | `c5043773106b29e2ed983be4ac720123a880e3a80414e31fc2fbec2f6d4b728d` |
| `crypto-sentinel-free-0.1.0-SHA256SUMS.txt` | `6827fe0208909291a284b6a55be55ab8105255a22f641864304580360ddd1ef0` |

The source ZIP and original wheel hashes match the supplied checksum manifest.
The retained release checks and manifest hashes above identify the exact local
copies used for the audit.

The final source tree also produced
`dist/crypto_sentinel_free-0.2.0-py3-none-any.whl` with SHA-256
`1388ed93bee73eb7a2bae40b7d734d852c430a4dcca7dbfb33ed899c5f25e1cf`.
It contains 31 archive entries; the bundled dashboard HTML, icon, web manifest,
and example configuration are present. A `--no-deps` install into a new
isolated temporary target imported version 0.2.0 and resolved the bundled
dashboard, configuration, and 20-second Bybit application heartbeat
successfully.

## Exact source changes

### Configuration and packaging

- `pyproject.toml` and `src/crypto_sentinel/__init__.py`: version 0.2.0 and
  monitoring-only package description.
- `config.example.yaml`, `config.docker.example.yaml`,
  `config.vps.example.yaml`, and `src/crypto_sentinel/config.example.yaml`:
  continuity-window, maximum-gap, and future-skew settings.
- The ignored target-host `config.yaml` and `config.docker.yaml` received the
  same schema fields; no notifier credentials or exchange credentials were
  added.
- `Dockerfile`: readiness-based health check with a startup allowance.

### Event, feed, and detector reliability

- `src/crypto_sentinel/models.py`: received-event envelopes, metric readiness
  evidence, and alarm-delivery receipt models.
- `src/crypto_sentinel/state.py`: checkpoint schema v2, monotonic receive-time
  handling, future-event quarantine, explicit continuity intervals, and
  post-gap recovery evidence.
- `src/crypto_sentinel/detector.py`: stale metrics cannot satisfy
  cross-exchange alarms; spread checks use only currently eligible venues.
- `src/crypto_sentinel/health.py`: feed connection generations,
  subscription acknowledgement, feed-wide freshness, per-symbol diagnostics,
  continuity state, task heartbeats, queue/drop/lag health, and clock health.
- `src/crypto_sentinel/exchanges/base.py`,
  `src/crypto_sentinel/exchanges/binance.py`,
  `src/crypto_sentinel/exchanges/bybit.py`, and
  `src/crypto_sentinel/exchanges/okx.py`: explicit connection lifecycles,
  current-generation receive metadata, public-subscription evidence, and
  whole-feed silence handling.
- `src/crypto_sentinel/app.py`: supervised critical tasks, fail-closed exits,
  queue/runtime/checkpoint alarms, continuity-gap/recovery aggregation, and
  readiness enforcement.
- `src/crypto_sentinel/doctor.py`: every configured symbol must produce a
  parsed trade; Bybit and OKX acknowledgements are verified.

The original false-reconnect behavior was removed: one naturally quiet symbol
is a per-symbol diagnostic and no longer causes a healthy, active whole feed to
reconnect. Whole-feed silence, pong-only traffic, missing acknowledgements,
drops, and stale valid payloads still fail readiness. After the live soak
captured two real Bybit socket closures, a fixed 20-second Bybit JSON
application heartbeat was added in addition to protocol-level ping/pong and
immediate reconnect.

### Alarm delivery and persistence

- `src/crypto_sentinel/alerting.py`: priority local queue, checked sound-process
  return codes, retries, queued/attempt/success/failure receipts, and cooldown
  rollback after final local-delivery failure.
- `src/crypto_sentinel/persistence.py`: delivery-receipt migration, storage,
  and bounded queries.
- Local OS sound remains primary; browser sound/notifications are a second
  local path. Remote notifiers remain disabled.

### Dashboard and local API

- `src/crypto_sentinel/dashboard.py`: authenticated bounded metric and receipt
  APIs, full readiness health endpoint, and restrictive local security headers.
- `src/crypto_sentinel/static/index.html`: premium “Cinnabar Glass” dashboard
  with system/readiness rails, native/shadow state, venue and symbol filters,
  persisted timelines, recovery history, alarm receipts, alert-evidence drawer,
  browser-local notes/acknowledgement, CSV/JSON export, test controls,
  responsive layout, and reduced-motion support.
- `src/crypto_sentinel/static/icon.svg` and
  `src/crypto_sentinel/static/manifest.webmanifest`: matching offline identity.

The dashboard has no CDN, remote font, analytics, or third-party runtime asset.
Desktop 1440×1000 and mobile 390×844 interactions were exercised. The final
inline JavaScript parser check passed. The in-app test browser blocks system
notification permission, while the user's normal browser previously showed
notifications enabled; the user confirmed the PC alarm is audible.

### Windows, Docker, startup, and soak reliability

- `scripts/run_windows_guardian.ps1` (new): one mutex-protected guardian adopts
  or restarts the supervisor, emits three critical sounds on unexpected
  supervisor exit, uses bounded exponential backoff, and rotates its log.
- `scripts/run_windows_supervisor.ps1`: one mutex-protected owner for the native
  primary, Docker shadow, and soak monitor; authenticated readiness watchdogs;
  direct critical sound on native-process death; separate cooldowns for native,
  primary-health, and shadow-health incidents.
- `scripts/register_windows_startup.ps1`: both Task Scheduler and fallback
  Startup paths target the guardian; legacy duplicate Docker/soak tasks are
  removed when registration is permitted.
- `scripts/soak_monitor.py`: bounded native/shadow/Docker/runtime/readiness,
  per-venue market, alarm, queue, task, checkpoint, clock, and gap evidence.
- `scripts/soak_report.py` (new): 1h/24h/72h fail-closed gates with duration,
  coverage, failure, gap, drop, quarantine, clock, alarm, checkpoint, detector,
  and restart checks. A strict quiet-market exemption applies only to healthy
  partial `{ready, stale_data}` evidence with zero recovery.
- `compose.yaml` already supplied loopback-only ports, read-only root,
  `no-new-privileges`, dropped capabilities, bounded logging, and
  `restart: unless-stopped`; the rebuilt shadow uses those controls.

Task Scheduler registration was attempted and Windows denied it under the
current standard-user policy. The active fallback is:

`%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup\Crypto Sentinel Free.lnk`

It targets `powershell.exe` with
`scripts\run_windows_guardian.ps1`. This provides automatic start after this
user logs in. The active Balanced power plan now has AC and DC sleep-after and
hibernate-after values set to 0 (Never).

### Documentation and tests

- Updated: `README.md`, `PROJECT_STATUS.md`, `docs/ARCHITECTURE.md`, and
  `docs/RELIABILITY_ROADMAP.md`.
- Expanded: `tests/test_alerting.py`, `tests/test_app.py`,
  `tests/test_checkpoint.py`, `tests/test_config.py`, `tests/test_doctor.py`,
  `tests/test_models.py`, `tests/test_parsers.py`,
  `tests/test_persistence.py`, and `tests/test_state.py`.
- Added: `tests/test_dashboard.py`, `tests/test_deployment_assets.py`,
  `tests/test_monitoring_only.py`, `tests/test_soak_monitor.py`, and
  `tests/test_soak_report.py`.

## Commands and results

The material release and validation commands are recorded below. Repeated
read-only inspection commands (`rg`, `Get-Content`, Git diff/status queries,
token-redacted localhost API queries, and process inspection) are omitted from
the literal list but their conclusions are included in this report.

```powershell
.\.venv\Scripts\python.exe -m ruff format --check .
.\.venv\Scripts\python.exe -m ruff check .
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m compileall -q src scripts tests
.\.venv\Scripts\python.exe -m pip check
git diff --check
```

Results:

- Ruff format: 42 files already formatted
- Ruff lint: all checks passed
- Pytest: 143 passed in 3.94 seconds
- Compileall: passed
- Pip check: no broken requirements
- Git diff check: passed; only Windows LF-to-CRLF advisory messages

```powershell
$errors = @()
Get-ChildItem -LiteralPath scripts -Filter *.ps1 | ForEach-Object {
    $tokens = $null
    $parseErrors = $null
    [void][System.Management.Automation.Language.Parser]::ParseFile(
        $_.FullName, [ref]$tokens, [ref]$parseErrors
    )
    if ($parseErrors) { $errors += $parseErrors }
}
if ($errors.Count -gt 0) { $errors | Format-List; exit 1 }
```

Result: all PowerShell files parsed successfully.

```powershell
node -e "const fs=require('fs'),vm=require('vm');const h=fs.readFileSync('src/crypto_sentinel/static/index.html','utf8');const s=[...h.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/gi)].map(m=>m[1]);if(!s.length)throw Error('no inline script');s.forEach((x,i)=>new vm.Script(x,{filename:'inline-'+i+'.js'}));console.log('Dashboard JavaScript parser: PASS ('+s.length+' block)')"
```

Result: one dashboard script block parsed successfully.

```powershell
.\.venv\Scripts\crypto-sentinel.exe --config config.yaml validate-config
.\.venv\Scripts\crypto-sentinel.exe --config config.docker.yaml validate-config
.\.venv\Scripts\crypto-sentinel.exe --config config.yaml simulate --json
.\.venv\Scripts\crypto-sentinel.exe --config config.yaml doctor --timeout 45 --json
```

Results:

- Both real configs passed for five symbols, Binance/Bybit/OKX, and 60/300s
  windows.
- Simulation generated 2,232 trades, two liquidations, and exactly one critical
  three-exchange `market_shock` with $2.7M corroborating liquidations.
- Live doctor passed all local checks and all 15 configured exchange-symbol
  mappings after the final heartbeat deployment. Binance parsed all five
  symbols in 1,094ms; Bybit acknowledged all ten topics and parsed all five
  symbols in 5,484ms; OKX acknowledged all five topics and parsed all five
  symbols in 2,000ms. Failed checks: 0.

```powershell
docker compose up -d --build
docker compose ps --format json
docker inspect --format '{{.Name}}|health={{.State.Health.Status}}|status={{.State.Status}}|restarts={{.RestartCount}}|image={{.Image}}' crypto-sentinel-shadow-crypto-sentinel-1
docker image inspect --format '{{index .RepoDigests 0}}|id={{.Id}}' crypto-sentinel-shadow-crypto-sentinel
```

Final Docker result:

- Container `crypto-sentinel-shadow-crypto-sentinel-1`
- State running, health healthy, restart count 0
- Image ID and local digest:
  `sha256:90fad1cd81d83ba76238f42989937231d49281256d223e64adb9765ac3b4a715`

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts\register_windows_startup.ps1
powercfg /change standby-timeout-ac 0
powercfg /change standby-timeout-dc 0
powercfg /change hibernate-timeout-ac 0
powercfg /change hibernate-timeout-dc 0
```

Result: Task Scheduler was denied, the per-user Startup fallback was created
and verified, and AC/DC sleep and hibernate timers are Never.

Controlled fault checks performed:

- A duplicate guardian invocation exited harmlessly; exactly one guardian
  remained.
- Supervisor PID 8204 was terminated; the guardian stayed alive, sounded the
  critical alarm, and started a new supervisor after five seconds.
- Native worker PID 28428 was terminated; the supervisor sounded the critical
  process-death alarm and started a replacement.
- A first native-crash trial exposed a shared-cooldown bug in which a recent
  Docker warning could suppress the native critical sound. The cooldowns were
  separated by incident class, the test was repeated, and the critical sound
  and replacement both succeeded.
- The Docker container was disconnected from and reconnected to
  `crypto-sentinel-shadow_default`; shadow HTTP failed during the interruption,
  then recovered to HTTP 200 ready without a container restart.
- Docker Desktop's Linux engine was stopped once and recovered; the supervisor
  logged the outage and the final shadow is healthy.

```powershell
$gateStart = (Get-Content -LiteralPath data\soak-gate-start.txt -Raw).Trim()
.\.venv\Scripts\python.exe scripts\soak_report.py `
  --input data\soak.jsonl `
  --since $gateStart `
  --hours 1 `
  --interval 60 `
  --min-coverage 0.98 `
  --require-complete
```

Result: exit 1, deliberately not hidden. The report had 71 samples, 1.176h
duration, 100% coverage, zero primary/shadow health failures, zero monitoring
sample gaps, zero drops, zero quarantined future events, zero clock
adjustments, zero local-alarm failures, zero checkpoint failures, and zero
runtime/container restarts. It failed on one detector-unready native BNB sample
(`2/3`, `insufficient_window`) following a Bybit reconnect. Fifteen healthy BNB
`{ready, stale_data}` instance samples were correctly reported as
informational quiet-market samples rather than outages.

The local warning alarm test alert was
`ca5e3428-63c1-4d3f-913e-4d8f1fb76ca2`. It has queued, attempt, and success
local receipts. The user confirmed the alarm is audible.

## Reproducible issues found and fixed

1. Quiet BNB periods caused false whole-feed reconnects. Feed-wide valid
   payload liveness is now distinct from per-symbol diagnostics.
2. Cross-exchange spread could consume a price made ineligible by a continuity
   gap. Spread calculations now use only ready shortest-window metrics.
3. A Docker-health warning cooldown could suppress a native-process-death
   critical alarm. Incident classes now use separate cooldown timestamps.
4. The Task Scheduler path bypassed the guardian and could duplicate ownership
   of Docker/soak processes. Every startup path now targets one guardian-owned
   lifecycle.
5. The soak report treated legitimate healthy `{ready, stale_data}` partial
   BNB readiness as a full outage. It now records this bounded case as
   informational, while malformed, missing, zero-ready, recovery, warmup,
   unhealthy-transport, drop, clock, checkpoint, gap, and restart evidence
   remains blocking.
6. Dashboard feed and native/shadow labels could say “stale” or “not reported”
   when the precise state was subscribing, recovering, offline, or ready.
   Labels now reflect the actual state.
7. The live soak captured two Bybit `websocket closed` events. Reconnect,
   fail-closed detector recovery, and audible warnings worked; a fixed
   20-second Bybit JSON heartbeat was added to reduce avoidable closures while
   preserving immediate reconnect.

## Failed or corrected checks

- A short live doctor initially timed out on OKX. The 45-second per-exchange
  gate then passed every symbol and acknowledgement; the transient failure was
  retained rather than omitted.
- An attempted PowerShell syntax check mistakenly routed `.ps1` files through
  Ruff and produced expected Python-syntax errors. It was replaced by the real
  PowerShell parser; all scripts passed.
- `pip wheel . --no-deps --no-build-isolation` failed because the runtime
  virtual environment intentionally lacked `setuptools`. An isolated PEP 517
  build succeeded. The final rebuilt wheel hash and package-data inspection are
  recorded above.
- One post-build inspection used the invalid PowerShell parameter
  `Select-Object -Single`; the wheel itself had already built. The inspection
  was rerun with valid PowerShell.
- Task Scheduler registration was denied by Windows policy. The verified
  per-user Startup guardian is the active automatic-start mechanism.
- The one-hour live soak was complete in elapsed time but not clean; its
  fail-closed result is described above.

## Remaining limitations

1. The clean 1h gate and the 24h/72h gates still require uninterrupted elapsed
   evidence. The monitor is running continuously and has already caught two
   real Bybit socket closures; no result will be called a pass unless
   `--require-complete` exits 0.
2. A physical reboot/login, lock/unlock, manual sleep, mains loss, and full
   network-loss test were not performed. Unit tests and controlled process,
   Docker-engine, and container-network faults cover the implemented logic.
3. The current PC cannot make a sound while powered off, asleep, muted, or
   physically disconnected. Native and Docker instances share the same machine
   and therefore are not independent failure domains.
4. The Startup shortcut runs only after this Windows user logs in. A
   pre-login/background scheduled task could not be registered under the
   current standard-user policy.
5. A successful local receipt proves that the OS sound invocation returned
   successfully; it cannot prove the speakers were unmuted or that a person
   heard it. The user's direct confirmation supplies the human acceptance test.
6. Browser notifications depend on browser permission and runtime policy.
   Local process sound is the primary path and does not require the dashboard
   tab.
7. ntfy, Telegram, and generic webhook delivery remain intentionally disabled
   and were not tested. This does not affect the local PC alarm path.
8. Public WebSocket streams provide no authoritative continuous per-symbol
   heartbeat. A silent symbol can be genuinely quiet or partially
   unsubscribed. Per-symbol diagnostics plus the every-symbol live doctor
   reduce, but cannot eliminate, that ambiguity.
9. Detection thresholds have not been calibrated against labeled historical
   important/non-important events, an agreed false-positive rate, or an agreed
   maximum alarm delay. The deterministic pipeline is tested; strategy quality
   still needs empirical calibration.

## Highest-value inputs for the next reliability step

No exchange keys or wallet access are needed. The most valuable user-provided
inputs would be:

- timestamps, symbols, and directions of historical events that should have
  alarmed;
- examples that should not have alarmed;
- acceptable alarm delay and false-positive frequency;
- optionally, an always-on second PC or small VPS for an external dead-man
  heartbeat using no exchange credentials.

Those inputs enable reproducible replay, threshold calibration, and a truly
independent “this PC stopped monitoring” alarm.
