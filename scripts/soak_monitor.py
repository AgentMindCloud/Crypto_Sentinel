from __future__ import annotations

import argparse
import json
import math
import subprocess
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_ALARM_COUNT_KEYS = frozenset({"queued", "attempt", "success", "failure"})
_BASE_EXPECTED_TASKS = frozenset(
    {"event-consumer", "detector", "health-monitor", "maintenance", "alert-workers"}
)
_MAX_EVIDENCE_ERRORS = 64
_MAX_FEEDS = 16
_MAX_MARKETS = 128
_MAX_REASONS = 32
_MAX_SYMBOLS_PER_FEED = 128
_MAX_TASKS = 64
_MAX_VENUES_PER_MARKET = 16


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Record bounded native and Docker-shadow health samples."
    )
    parser.add_argument("--primary-url", default="http://127.0.0.1:8787")
    parser.add_argument("--shadow-url", default="http://127.0.0.1:8788")
    parser.add_argument("--primary-env", default=".env")
    parser.add_argument("--shadow-env", default=".env.docker")
    parser.add_argument("--output", default="data/soak.jsonl")
    parser.add_argument("--interval", type=float, default=60.0)
    parser.add_argument("--max-bytes", type=int, default=25_000_000)
    parser.add_argument("--once", action="store_true")
    return parser


def _read_token(path: str | Path) -> str:
    env_path = Path(path)
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        key, separator, value = raw_line.partition("=")
        if separator and key.strip() == "DASHBOARD_TOKEN":
            token = value.strip()
            if token:
                return token
    raise ValueError(f"DASHBOARD_TOKEN is missing from {env_path}")


def _finite_number(value: Any, *, minimum: float = 0.0) -> bool:
    return bool(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) >= minimum
    )


def _nonnegative_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _expected_task_names(
    feeds: dict[str, Any],
    checkpoint_health: dict[str, Any],
) -> list[str]:
    names = set(_BASE_EXPECTED_TASKS)
    names.update(f"feed-{name}" for name in feeds)
    if checkpoint_health.get("enabled") is True:
        names.add("state-checkpoint")
    return sorted(names)


def _status_evidence_errors(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []

    def add(message: str) -> None:
        if len(errors) < _MAX_EVIDENCE_ERRORS:
            errors.append(message[:200])

    if not _finite_number(payload.get("started_ms"), minimum=1):
        add("runtime started_ms is missing or invalid")
    if not _finite_number(payload.get("uptime_seconds")):
        add("uptime_seconds is missing or invalid")
    if not _nonnegative_int(payload.get("queue_size")):
        add("queue_size is missing or invalid")
    if not _nonnegative_int(payload.get("queue_capacity")) or payload.get("queue_capacity") == 0:
        add("queue_capacity is missing or invalid")

    readiness = payload.get("readiness")
    if not isinstance(readiness, dict):
        add("readiness section is missing or invalid")
        readiness = {}
    if not isinstance(readiness.get("ready"), bool):
        add("readiness.ready is missing or invalid")
    if not isinstance(readiness.get("state"), str) or not readiness.get("state"):
        add("readiness.state is missing or invalid")
    readiness_reasons = readiness.get("reasons")
    if not isinstance(readiness_reasons, list) or any(
        not isinstance(reason, str) for reason in readiness_reasons
    ):
        add("readiness.reasons is missing or invalid")
    elif len(readiness_reasons) > _MAX_REASONS:
        add("readiness.reasons exceeds the evidence bound")

    feeds = payload.get("feeds")
    if not isinstance(feeds, dict) or not feeds:
        add("feeds section is missing, invalid, or empty")
        feeds = {}
    elif len(feeds) > _MAX_FEEDS:
        add("feeds section exceeds the evidence bound")
    stale_limits = payload.get("stale_after_seconds")
    if not isinstance(stale_limits, dict):
        add("stale_after_seconds is missing or invalid")
        stale_limits = {}
    symbol_freshness = payload.get("symbol_freshness_seconds")
    if not _finite_number(symbol_freshness, minimum=0.001):
        add("symbol_freshness_seconds is missing or invalid")
    for raw_name, raw_feed in feeds.items():
        name = str(raw_name)[:40]
        if not isinstance(raw_name, str) or not raw_name:
            add("feed name is invalid")
        if not isinstance(raw_feed, dict):
            add(f"feeds.{name} is invalid")
            continue
        if not isinstance(raw_feed.get("connected"), bool):
            add(f"feeds.{name}.connected is missing or invalid")
        if not isinstance(raw_feed.get("subscription_acknowledged"), bool):
            add(f"feeds.{name}.subscription_acknowledged is missing or invalid")
        age = raw_feed.get(
            "latest_message_age_seconds",
            raw_feed.get("message_age_seconds"),
        )
        if age is not None and not _finite_number(age):
            add(f"feeds.{name}.message age is invalid")
        if not _finite_number(stale_limits.get(raw_name), minimum=0.001):
            add(f"stale_after_seconds.{name} is missing or invalid")
        for counter in ("reconnects", "trades", "liquidations"):
            if not _nonnegative_int(raw_feed.get(counter)):
                add(f"feeds.{name}.{counter} is missing or invalid")
        dropped = raw_feed.get("dropped", raw_feed.get("dropped_events"))
        if not _nonnegative_int(dropped):
            add(f"feeds.{name}.dropped is missing or invalid")
        symbols = raw_feed.get("symbols")
        if not isinstance(symbols, dict) or not symbols:
            add(f"feeds.{name}.symbols is missing, invalid, or empty")
            continue
        if len(symbols) > _MAX_SYMBOLS_PER_FEED:
            add(f"feeds.{name}.symbols exceeds the evidence bound")
        for raw_symbol, raw_symbol_evidence in symbols.items():
            symbol = str(raw_symbol)[:40]
            if not isinstance(raw_symbol, str) or not raw_symbol:
                add(f"feeds.{name} contains an invalid symbol name")
            if not isinstance(raw_symbol_evidence, dict):
                add(f"feeds.{name}.symbols.{symbol} is invalid")
                continue
            if not isinstance(raw_symbol_evidence.get("seen_since_connect"), bool):
                add(f"feeds.{name}.symbols.{symbol}.seen_since_connect is invalid")
            symbol_age = raw_symbol_evidence.get("message_age_seconds")
            if symbol_age is not None and not _finite_number(symbol_age):
                add(f"feeds.{name}.symbols.{symbol}.message_age_seconds is invalid")
            for counter in ("messages", "trades", "liquidations"):
                if not _nonnegative_int(raw_symbol_evidence.get(counter)):
                    add(f"feeds.{name}.symbols.{symbol}.{counter} is invalid")

    checkpoint_health = payload.get("checkpoint_health")
    if not isinstance(checkpoint_health, dict):
        add("checkpoint_health section is missing or invalid")
        checkpoint_health = {}
    if not isinstance(checkpoint_health.get("enabled"), bool):
        add("checkpoint_health.enabled is missing or invalid")
    if not isinstance(checkpoint_health.get("healthy"), bool):
        add("checkpoint_health.healthy is missing or invalid")
    if not isinstance(checkpoint_health.get("reason"), str) or not checkpoint_health.get("reason"):
        add("checkpoint_health.reason is missing or invalid")
    if checkpoint_health.get("enabled") is True:
        if not isinstance(checkpoint_health.get("path_exists"), bool):
            add("checkpoint_health.path_exists is missing or invalid")
        if not isinstance(checkpoint_health.get("within_initial_allowance"), bool):
            add("checkpoint_health.within_initial_allowance is missing or invalid")
        for field in ("last_success_age_seconds", "path_age_seconds"):
            value = checkpoint_health.get(field)
            if value is not None and not _finite_number(value):
                add(f"checkpoint_health.{field} is invalid")
        last_failure_ms = checkpoint_health.get("last_failure_ms")
        if last_failure_ms is not None and not _finite_number(last_failure_ms, minimum=1):
            add("checkpoint_health.last_failure_ms is invalid")

    tasks = payload.get("tasks")
    if not isinstance(tasks, dict):
        add("tasks section is missing or invalid")
        tasks = {}
    elif len(tasks) > _MAX_TASKS:
        add("tasks section exceeds the evidence bound")
    expected_tasks = _expected_task_names(feeds, checkpoint_health)
    for name in expected_tasks:
        raw_task = tasks.get(name)
        if not isinstance(raw_task, dict):
            add(f"expected task {name} is missing or invalid")
            continue
        if not isinstance(raw_task.get("state"), str) or not raw_task.get("state"):
            add(f"tasks.{name}.state is missing or invalid")
        if not isinstance(raw_task.get("ready"), bool):
            add(f"tasks.{name}.ready is missing or invalid")
        if not _finite_number(raw_task.get("heartbeat_age_seconds")):
            add(f"tasks.{name}.heartbeat_age_seconds is missing or invalid")
        if not isinstance(raw_task.get("heartbeat_stale"), bool):
            add(f"tasks.{name}.heartbeat_stale is missing or invalid")
        if not isinstance(raw_task.get("last_error"), str):
            add(f"tasks.{name}.last_error is missing or invalid")

    queue_health = payload.get("queue_health")
    if not isinstance(queue_health, dict):
        add("queue_health section is missing or invalid")
        queue_health = {}
    for field in ("high_water", "drops_total", "drops_since_last_health_check"):
        if not _nonnegative_int(queue_health.get(field)):
            add(f"queue_health.{field} is missing or invalid")
    if not _finite_number(queue_health.get("consumer_lag_seconds")):
        add("queue_health.consumer_lag_seconds is missing or invalid")
    utilization = queue_health.get("utilization")
    if not _finite_number(utilization) or float(utilization or 0) > 1:
        add("queue_health.utilization is missing or invalid")

    clock_health = payload.get("clock_health")
    if not isinstance(clock_health, dict):
        add("clock_health section is missing or invalid")
        clock_health = {}
    for field in ("future_events_quarantined", "clock_adjustments"):
        if not _nonnegative_int(clock_health.get(field)):
            add(f"clock_health.{field} is missing or invalid")
    last_quarantine_ms = clock_health.get("last_quarantine_ms")
    if last_quarantine_ms is not None and not _finite_number(last_quarantine_ms, minimum=1):
        add("clock_health.last_quarantine_ms is invalid")

    alarm_delivery = payload.get("alarm_delivery")
    if not isinstance(alarm_delivery, dict):
        add("alarm_delivery section is missing or invalid")
        alarm_delivery = {}
    local_alarm = alarm_delivery.get("local_alarm")
    if not isinstance(local_alarm, dict):
        add("alarm_delivery.local_alarm is missing or invalid")
        local_alarm = {}
    if not isinstance(local_alarm.get("enabled"), bool):
        add("alarm_delivery.local_alarm.enabled is missing or invalid")
    if not _nonnegative_int(local_alarm.get("queue_depth")):
        add("alarm_delivery.local_alarm.queue_depth is missing or invalid")
    alarm_counts = local_alarm.get("counts")
    if not isinstance(alarm_counts, dict):
        add("alarm_delivery.local_alarm.counts is missing or invalid")
        alarm_counts = {}
    for name in sorted(_ALARM_COUNT_KEYS):
        if not _nonnegative_int(alarm_counts.get(name)):
            add(f"alarm_delivery.local_alarm.counts.{name} is missing or invalid")
    last_receipt = local_alarm.get("last_receipt")
    if last_receipt is not None and not isinstance(last_receipt, dict):
        add("alarm_delivery.local_alarm.last_receipt is invalid")

    markets = payload.get("markets")
    if not isinstance(markets, list) or not markets:
        add("markets section is missing, invalid, or empty")
        markets = []
    elif len(markets) > _MAX_MARKETS:
        add("markets section exceeds the evidence bound")
    for index, market in enumerate(markets):
        prefix = f"markets[{index}]"
        if not isinstance(market, dict):
            add(f"{prefix} is invalid")
            continue
        if not isinstance(market.get("symbol"), str) or not market.get("symbol"):
            add(f"{prefix}.symbol is missing or invalid")
        configured = market.get("configured_venues")
        ready = market.get("ready_venues")
        if not _nonnegative_int(configured) or configured == 0:
            add(f"{prefix}.configured_venues is missing or invalid")
        if not _nonnegative_int(ready) or (_nonnegative_int(configured) and ready > configured):
            add(f"{prefix}.ready_venues is missing or invalid")
        if not isinstance(market.get("readiness_reason"), str) or not market.get(
            "readiness_reason"
        ):
            add(f"{prefix}.readiness_reason is missing or invalid")
        reasons = market.get("readiness_reasons")
        if (
            not isinstance(reasons, list)
            or not reasons
            or any(not isinstance(reason, str) or not reason for reason in reasons)
        ):
            add(f"{prefix}.readiness_reasons is missing, empty, or invalid")
        elif len(reasons) > _MAX_REASONS:
            add(f"{prefix}.readiness_reasons exceeds the evidence bound")
        if not _finite_number(market.get("max_recovery_seconds_remaining")):
            add(f"{prefix}.max_recovery_seconds_remaining is missing or invalid")
        coverage = market.get("min_window_coverage_ratio")
        if not _finite_number(coverage) or float(coverage or 0) > 1:
            add(f"{prefix}.min_window_coverage_ratio is missing or invalid")
        largest_gap = market.get("max_largest_gap_seconds")
        if largest_gap is not None and not _finite_number(largest_gap):
            add(f"{prefix}.max_largest_gap_seconds is invalid")
        venues = market.get("venue_readiness")
        if not isinstance(venues, dict) or not venues:
            add(f"{prefix}.venue_readiness is missing, invalid, or empty")
            continue
        if len(venues) > _MAX_VENUES_PER_MARKET:
            add(f"{prefix}.venue_readiness exceeds the evidence bound")
        if _nonnegative_int(configured) and len(venues) != configured:
            add(f"{prefix}.venue_readiness count does not match configured_venues")
        observed_ready = 0
        for raw_name, venue in venues.items():
            name = str(raw_name)[:40]
            if not isinstance(raw_name, str) or not raw_name:
                add(f"{prefix}.venue_readiness contains an invalid venue name")
            if not isinstance(venue, dict):
                add(f"{prefix}.venue_readiness.{name} is invalid")
                continue
            for field in ("ready", "base_ready", "alert_eligible"):
                if not isinstance(venue.get(field), bool):
                    add(f"{prefix}.venue_readiness.{name}.{field} is invalid")
            if venue.get("alert_eligible") is True:
                observed_ready += 1
            if not isinstance(venue.get("reason"), str) or not venue.get("reason"):
                add(f"{prefix}.venue_readiness.{name}.reason is missing or invalid")
            age = venue.get("data_age_seconds")
            if age is not None and not _finite_number(age):
                add(f"{prefix}.venue_readiness.{name}.data_age_seconds is invalid")
        if _nonnegative_int(ready) and observed_ready != ready:
            add(f"{prefix}.ready_venues does not match venue readiness evidence")

    return errors


def _market_evidence(raw: dict[str, Any]) -> dict[str, Any]:
    raw_reasons = raw.get("readiness_reasons")
    reasons = (
        [str(reason)[:80] for reason in raw_reasons[:_MAX_REASONS]]
        if isinstance(raw_reasons, list)
        else []
    )
    raw_venues = raw.get("venue_readiness")
    venues: dict[str, dict[str, Any]] = {}
    if isinstance(raw_venues, dict):
        for name, venue in list(raw_venues.items())[:_MAX_VENUES_PER_MARKET]:
            if not isinstance(venue, dict):
                continue
            raw_missing_windows = venue.get("missing_windows_seconds")
            missing_windows = (
                list(raw_missing_windows)[:16] if isinstance(raw_missing_windows, list) else []
            )
            venues[str(name)[:40]] = {
                "ready": bool(venue.get("ready")),
                "base_ready": bool(venue.get("base_ready")),
                "alert_eligible": bool(venue.get("alert_eligible")),
                "reason": str(venue.get("reason") or "")[:80],
                "data_age_seconds": venue.get("data_age_seconds"),
                "window_coverage_ratio": venue.get("window_coverage_ratio"),
                "largest_gap_seconds": venue.get("largest_gap_seconds"),
                "recovery_seconds_remaining": venue.get("recovery_seconds_remaining"),
                "missing_windows_seconds": missing_windows,
            }
    return {
        "symbol": raw.get("symbol"),
        "ready_venues": raw.get("ready_venues"),
        "configured_venues": raw.get("configured_venues"),
        "readiness_reason": str(raw.get("readiness_reason") or "")[:160],
        "readiness_reasons": reasons,
        "venue_readiness": venues,
        "max_recovery_seconds_remaining": raw.get("max_recovery_seconds_remaining"),
        "min_window_coverage_ratio": raw.get("min_window_coverage_ratio"),
        "max_largest_gap_seconds": raw.get("max_largest_gap_seconds"),
    }


def _fetch_status(base_url: str, token: str) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/status",
        headers={"Authorization": f"Bearer {token}"},
    )
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            payload = json.load(response)
    except (OSError, ValueError, urllib.error.URLError) as exc:
        return {
            "ok": False,
            "latency_ms": round((time.monotonic() - started) * 1000),
            "error": f"{type(exc).__name__}: {str(exc)[:240]}",
            "evidence_complete": False,
            "evidence_errors": ["status request failed"],
            "runtime_started_ms": None,
        }
    if not isinstance(payload, dict):
        return {
            "ok": False,
            "latency_ms": round((time.monotonic() - started) * 1000),
            "error": "status payload is not an object",
            "evidence_complete": False,
            "evidence_errors": ["status payload is not an object"],
            "runtime_started_ms": None,
        }
    evidence_errors = _status_evidence_errors(payload)
    if evidence_errors:
        return {
            "ok": False,
            "latency_ms": round((time.monotonic() - started) * 1000),
            "error": "status payload evidence is incomplete or malformed",
            "evidence_complete": False,
            "evidence_errors": evidence_errors,
            "runtime_started_ms": (
                payload.get("started_ms")
                if _finite_number(payload.get("started_ms"), minimum=1)
                else None
            ),
        }

    feed_rows: dict[str, dict[str, Any]] = {}
    stale_limits = payload.get("stale_after_seconds", {})
    for name, raw in list(payload.get("feeds", {}).items())[:_MAX_FEEDS]:
        age = raw.get(
            "latest_message_age_seconds",
            raw.get("message_age_seconds"),
        )
        stale_after = stale_limits.get(name)
        symbol_freshness = payload.get("symbol_freshness_seconds", stale_after)
        symbols: dict[str, dict[str, Any]] = {}
        for symbol, symbol_raw in list((raw.get("symbols") or {}).items())[:_MAX_SYMBOLS_PER_FEED]:
            symbol_age = symbol_raw.get("message_age_seconds")
            symbol_healthy = bool(
                symbol_raw.get("seen_since_connect")
                and symbol_age is not None
                and symbol_freshness is not None
                and 0 <= float(symbol_age) <= float(symbol_freshness)
            )
            symbols[str(symbol)] = {
                "healthy": symbol_healthy,
                "message_age_seconds": symbol_age,
                "messages": int(symbol_raw.get("messages", 0)),
                "trades": int(symbol_raw.get("trades", 0)),
                "liquidations": int(symbol_raw.get("liquidations", 0)),
            }
        feed_healthy = bool(
            raw.get("connected")
            and raw.get("subscription_acknowledged", True)
            and age is not None
            and stale_after is not None
            and 0 <= float(age) <= float(stale_after)
        )
        feed_rows[name] = {
            "healthy": feed_healthy,
            "connected": bool(raw.get("connected")),
            "subscription_acknowledged": bool(raw.get("subscription_acknowledged", True)),
            "message_age_seconds": age,
            "diagnostic_symbol_age_seconds": raw.get(
                "oldest_symbol_message_age_seconds",
                raw.get("message_age_seconds"),
            ),
            "reconnects": int(raw.get("reconnects", 0)),
            "dropped_events": int(raw.get("dropped", raw.get("dropped_events", 0))),
            "trades": int(raw.get("trades", 0)),
            "liquidations": int(raw.get("liquidations", 0)),
            "symbols": symbols,
        }

    readiness = payload.get("readiness") or {}
    declared_ready = readiness.get("ready")
    tasks = payload.get("tasks") or {}
    expected_tasks = _expected_task_names(
        payload.get("feeds", {}),
        payload.get("checkpoint_health") or {},
    )
    task_rows: dict[str, dict[str, Any]] = {}
    for name in expected_tasks:
        raw = tasks[name]
        task_rows[name] = {
            "state": raw.get("state"),
            "ready": bool(raw.get("ready")),
            "heartbeat_age_seconds": raw.get("heartbeat_age_seconds"),
            "heartbeat_stale": bool(raw.get("heartbeat_stale")),
            "last_error": str(raw.get("last_error") or "")[:160],
        }
    queue_health = payload.get("queue_health") or {}
    clock_health = payload.get("clock_health") or {}
    alarm_delivery = payload.get("alarm_delivery") or {}
    local_alarm = alarm_delivery.get("local_alarm") or {}
    last_receipt = local_alarm.get("last_receipt") or {}
    checkpoint_health = payload.get("checkpoint_health") or {}

    return {
        "ok": declared_ready,
        "latency_ms": round((time.monotonic() - started) * 1000),
        "evidence_complete": True,
        "evidence_errors": [],
        "runtime_started_ms": payload.get("started_ms"),
        "uptime_seconds": payload.get("uptime_seconds"),
        "queue_size": payload.get("queue_size"),
        "queue_capacity": payload.get("queue_capacity"),
        "readiness": {
            "ready": declared_ready,
            "state": readiness.get("state"),
            "reasons": [
                str(reason)[:240] for reason in readiness.get("reasons", [])[:_MAX_REASONS]
            ],
        },
        "expected_tasks": expected_tasks,
        "tasks": task_rows,
        "queue_health": {
            "high_water": int(queue_health.get("high_water") or 0),
            "drops_total": int(queue_health.get("drops_total") or 0),
            "drops_since_last_health_check": int(
                queue_health.get("drops_since_last_health_check") or 0
            ),
            "consumer_lag_seconds": float(queue_health.get("consumer_lag_seconds") or 0),
            "utilization": float(queue_health.get("utilization") or 0),
        },
        "clock_health": {
            "future_events_quarantined": int(clock_health.get("future_events_quarantined") or 0),
            "clock_adjustments": int(clock_health.get("clock_adjustments") or 0),
            "last_quarantine_ms": clock_health.get("last_quarantine_ms"),
        },
        "alarm_delivery": {
            "enabled": bool(local_alarm.get("enabled")),
            "local_queue_depth": int(local_alarm.get("queue_depth") or 0),
            "counts": {
                str(key): int(value) for key, value in (local_alarm.get("counts") or {}).items()
            },
            "last_receipt": {
                "status": last_receipt.get("status"),
                "attempt": last_receipt.get("attempt"),
                "timestamp_ms": last_receipt.get("timestamp_ms"),
                "detail": str(last_receipt.get("detail") or "")[:80],
            }
            if last_receipt
            else None,
        },
        "checkpoint_health": {
            "enabled": bool(checkpoint_health.get("enabled")),
            "healthy": bool(checkpoint_health.get("healthy")),
            "reason": str(checkpoint_health.get("reason") or "")[:160],
            "last_success_age_seconds": checkpoint_health.get("last_success_age_seconds"),
            "last_failure_ms": checkpoint_health.get("last_failure_ms"),
            "path_exists": bool(checkpoint_health.get("path_exists")),
            "path_age_seconds": checkpoint_health.get("path_age_seconds"),
            "within_initial_allowance": bool(checkpoint_health.get("within_initial_allowance")),
        },
        "markets": [
            _market_evidence(market)
            for market in payload.get("markets", [])[:_MAX_MARKETS]
            if isinstance(market, dict)
        ],
        "feeds": feed_rows,
        "deployment": {
            str(key)[:40]: (str(value)[:160] if isinstance(value, str) else value)
            for key, value in list((payload.get("deployment") or {}).items())[:16]
            if isinstance(value, (str, int, float, bool)) or value is None
        }
        if isinstance(payload.get("deployment"), dict)
        else None,
    }


def _docker_state() -> dict[str, Any]:
    try:
        container_id = subprocess.run(
            ["docker", "compose", "ps", "-q", "crypto-sentinel"],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        ).stdout.strip()
        if not container_id:
            return {"observed": True, "present": False}
        raw = subprocess.run(
            [
                "docker",
                "inspect",
                container_id,
                "--format",
                "{{json .}}",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        ).stdout
        container = json.loads(raw)
        if not isinstance(container, dict):
            raise ValueError("docker inspect did not return an object")
        state = container.get("State") or {}
        return {
            "observed": True,
            "present": True,
            "container_id": str(container.get("Id") or container_id)[:64],
            "running": bool(state.get("Running")),
            "status": state.get("Status"),
            "started_at": state.get("StartedAt"),
            "restart_count": int(container.get("RestartCount", 0)),
            "health": (state.get("Health") or {}).get("Status"),
        }
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        return {
            "observed": False,
            "present": False,
            "error": f"{type(exc).__name__}: {str(exc)[:240]}",
        }


def _append_bounded(path: Path, row: dict[str, Any], max_bytes: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size >= max_bytes:
        rotated = path.with_suffix(path.suffix + ".1")
        rotated.unlink(missing_ok=True)
        path.replace(rotated)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(row, separators=(",", ":"), sort_keys=True))
        handle.write("\n")


def main() -> None:
    args = _parser().parse_args()
    if args.interval < 5:
        raise SystemExit("--interval must be at least 5 seconds")
    primary_token = _read_token(args.primary_env)
    shadow_token = _read_token(args.shadow_env)
    output = Path(args.output)
    previous_sample = time.monotonic()

    while True:
        now_monotonic = time.monotonic()
        gap_seconds = now_monotonic - previous_sample
        row = {
            "timestamp": datetime.now(UTC).isoformat(),
            "monitor_gap_seconds": (
                round(gap_seconds, 3) if gap_seconds >= args.interval * 1.5 else 0
            ),
            "primary": _fetch_status(args.primary_url, primary_token),
            "shadow": _fetch_status(args.shadow_url, shadow_token),
            "docker": _docker_state(),
        }
        _append_bounded(output, row, args.max_bytes)
        if args.once:
            print(json.dumps(row, indent=2, sort_keys=True))
            return
        previous_sample = now_monotonic
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
