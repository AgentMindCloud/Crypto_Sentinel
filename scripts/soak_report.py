from __future__ import annotations

import argparse
import json
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_ALARM_COUNT_KEYS = frozenset({"queued", "attempt", "success", "failure"})
_BASE_EXPECTED_TASKS = frozenset(
    {"event-consumer", "detector", "health-monitor", "maintenance", "alert-workers"}
)
_MAX_EVIDENCE_ISSUES = 64
_QUIET_MARKET_REASONS = frozenset({"ready", "stale_data"})


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Summarize Crypto Sentinel soak evidence and evaluate a timed gate."
    )
    parser.add_argument("--input", default="data/soak.jsonl")
    parser.add_argument("--hours", type=float, default=1.0)
    parser.add_argument("--interval", type=float, default=60.0)
    parser.add_argument("--min-coverage", type=float, default=0.98)
    parser.add_argument("--since", help="Only include samples at or after this ISO-8601 time.")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--require-complete", action="store_true")
    return parser


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _finite_number(value: Any, *, minimum: float = 0.0) -> bool:
    return bool(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) >= minimum
    )


def _nonnegative_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _number_or_zero(value: Any) -> float:
    return float(value) if _finite_number(value) else 0.0


def _int_or_zero(value: Any) -> int:
    return int(value) if _nonnegative_int(value) else 0


def load_rows(path: Path, since: datetime | None = None) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    invalid = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
            row_time = _timestamp(str(row["timestamp"]))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            invalid += 1
            continue
        if since is None or row_time >= since:
            row["_parsed_timestamp"] = row_time
            rows.append(row)
    rows.sort(key=lambda item: item["_parsed_timestamp"])
    return rows, invalid


def _longest_failure_run(rows: list[dict[str, Any]], key: str) -> int:
    longest = 0
    current = 0
    for row in rows:
        instance = row.get(key)
        if isinstance(instance, dict) and instance.get("ok") is True:
            current = 0
        else:
            current += 1
            longest = max(longest, current)
    return longest


def _runtime_restarts(rows: list[dict[str, Any]], key: str) -> int:
    restarts = 0
    previous_identity: float | None = None
    previous_uptime: float | None = None
    for row in rows:
        instance = row.get(key)
        if not isinstance(instance, dict):
            continue
        raw_identity = instance.get("runtime_started_ms")
        raw_uptime = instance.get("uptime_seconds")
        identity = float(raw_identity) if _finite_number(raw_identity, minimum=1) else None
        uptime = float(raw_uptime) if _finite_number(raw_uptime) else None
        if identity is None or uptime is None:
            continue
        if previous_identity is not None and (
            identity != previous_identity
            or (previous_uptime is not None and uptime + 1 < previous_uptime)
        ):
            restarts += 1
        previous_identity = identity
        previous_uptime = uptime
    return restarts


def _docker_runtime_restarts(rows: list[dict[str, Any]]) -> int:
    restarts = 0
    previous_identity: tuple[str, str] | None = None
    for row in rows:
        docker = row.get("docker")
        if not isinstance(docker, dict):
            continue
        container_id = docker.get("container_id")
        started_at = docker.get("started_at")
        if not isinstance(container_id, str) or not container_id:
            continue
        if not isinstance(started_at, str) or not started_at:
            continue
        identity = (container_id, started_at)
        if previous_identity is not None and identity != previous_identity:
            restarts += 1
        previous_identity = identity
    return restarts


def _expected_task_names(
    feeds: dict[str, Any],
    checkpoint_health: dict[str, Any],
) -> set[str]:
    names = set(_BASE_EXPECTED_TASKS)
    names.update(f"feed-{name}" for name in feeds)
    if checkpoint_health.get("enabled") is True:
        names.add("state-checkpoint")
    return names


def _instance_evidence_errors(instance: Any) -> list[str]:
    errors: list[str] = []

    def add(message: str) -> None:
        if len(errors) < _MAX_EVIDENCE_ISSUES:
            errors.append(message)

    if not isinstance(instance, dict):
        return ["instance section is missing or invalid"]
    if instance.get("evidence_complete") is not True:
        add("evidence_complete is not true")
    recorded_errors = instance.get("evidence_errors")
    if not isinstance(recorded_errors, list) or recorded_errors:
        add("evidence_errors is missing, invalid, or nonempty")
    if not isinstance(instance.get("ok"), bool):
        add("ok is missing or invalid")
    if not _finite_number(instance.get("runtime_started_ms"), minimum=1):
        add("runtime_started_ms is missing or invalid")
    if not _finite_number(instance.get("uptime_seconds")):
        add("uptime_seconds is missing or invalid")
    if not _nonnegative_int(instance.get("queue_size")):
        add("queue_size is missing or invalid")
    if not _nonnegative_int(instance.get("queue_capacity")) or instance.get("queue_capacity") == 0:
        add("queue_capacity is missing or invalid")

    readiness = instance.get("readiness")
    if not isinstance(readiness, dict):
        add("readiness is missing or invalid")
        readiness = {}
    if not isinstance(readiness.get("ready"), bool):
        add("readiness.ready is missing or invalid")
    elif isinstance(instance.get("ok"), bool) and instance["ok"] is not readiness["ready"]:
        add("ok does not match readiness.ready")
    if not isinstance(readiness.get("state"), str) or not readiness.get("state"):
        add("readiness.state is missing or invalid")
    reasons = readiness.get("reasons")
    if not isinstance(reasons, list) or any(not isinstance(reason, str) for reason in reasons):
        add("readiness.reasons is missing or invalid")

    feeds = instance.get("feeds")
    if not isinstance(feeds, dict) or not feeds:
        add("feeds is missing, invalid, or empty")
        feeds = {}
    for raw_name, feed in feeds.items():
        name = str(raw_name)
        if not isinstance(raw_name, str) or not raw_name:
            add("feed name is invalid")
        if not isinstance(feed, dict):
            add(f"feeds.{name} is invalid")
            continue
        for field in ("healthy", "connected", "subscription_acknowledged"):
            if not isinstance(feed.get(field), bool):
                add(f"feeds.{name}.{field} is missing or invalid")
        if instance.get("ok") is True and feed.get("healthy") is not True:
            add(f"ready instance contains unhealthy feed {name}")
        age = feed.get("message_age_seconds")
        if age is not None and not _finite_number(age):
            add(f"feeds.{name}.message_age_seconds is invalid")
        for field in ("reconnects", "dropped_events", "trades", "liquidations"):
            if not _nonnegative_int(feed.get(field)):
                add(f"feeds.{name}.{field} is missing or invalid")
        symbols = feed.get("symbols")
        if not isinstance(symbols, dict) or not symbols:
            add(f"feeds.{name}.symbols is missing, invalid, or empty")
            continue
        for raw_symbol, symbol in symbols.items():
            symbol_name = str(raw_symbol)
            if not isinstance(raw_symbol, str) or not raw_symbol:
                add(f"feeds.{name} contains an invalid symbol name")
            if not isinstance(symbol, dict):
                add(f"feeds.{name}.symbols.{symbol_name} is invalid")
                continue
            if not isinstance(symbol.get("healthy"), bool):
                add(f"feeds.{name}.symbols.{symbol_name}.healthy is missing or invalid")
            symbol_age = symbol.get("message_age_seconds")
            if symbol_age is not None and not _finite_number(symbol_age):
                add(f"feeds.{name}.symbols.{symbol_name}.message_age_seconds is invalid")
            for field in ("messages", "trades", "liquidations"):
                if not _nonnegative_int(symbol.get(field)):
                    add(f"feeds.{name}.symbols.{symbol_name}.{field} is invalid")

    checkpoint = instance.get("checkpoint_health")
    if not isinstance(checkpoint, dict):
        add("checkpoint_health is missing or invalid")
        checkpoint = {}
    if not isinstance(checkpoint.get("enabled"), bool):
        add("checkpoint_health.enabled is missing or invalid")
    if not isinstance(checkpoint.get("healthy"), bool):
        add("checkpoint_health.healthy is missing or invalid")
    if not isinstance(checkpoint.get("reason"), str) or not checkpoint.get("reason"):
        add("checkpoint_health.reason is missing or invalid")
    if checkpoint.get("enabled") is True:
        if not isinstance(checkpoint.get("path_exists"), bool):
            add("checkpoint_health.path_exists is missing or invalid")
        if not isinstance(checkpoint.get("within_initial_allowance"), bool):
            add("checkpoint_health.within_initial_allowance is missing or invalid")
        for field in ("last_success_age_seconds", "path_age_seconds"):
            value = checkpoint.get(field)
            if value is not None and not _finite_number(value):
                add(f"checkpoint_health.{field} is invalid")
        last_failure_ms = checkpoint.get("last_failure_ms")
        if last_failure_ms is not None and not _finite_number(last_failure_ms, minimum=1):
            add("checkpoint_health.last_failure_ms is invalid")
        if instance.get("ok") is True and checkpoint.get("healthy") is not True:
            add("ready instance contains an unhealthy checkpoint")

    expected_tasks = _expected_task_names(feeds, checkpoint)
    recorded_expected = instance.get("expected_tasks")
    if (
        not isinstance(recorded_expected, list)
        or any(not isinstance(name, str) or not name for name in recorded_expected)
        or len(recorded_expected) != len(set(recorded_expected))
    ):
        add("expected_tasks is missing or invalid")
    elif set(recorded_expected) != expected_tasks:
        add("expected_tasks does not match feeds/checkpoint evidence")
    tasks = instance.get("tasks")
    if not isinstance(tasks, dict):
        add("tasks is missing or invalid")
        tasks = {}
    for name in sorted(expected_tasks):
        task = tasks.get(name)
        if not isinstance(task, dict):
            add(f"expected task {name} is missing or invalid")
            continue
        if not isinstance(task.get("state"), str) or not task.get("state"):
            add(f"tasks.{name}.state is missing or invalid")
        if not isinstance(task.get("ready"), bool):
            add(f"tasks.{name}.ready is missing or invalid")
        if not _finite_number(task.get("heartbeat_age_seconds")):
            add(f"tasks.{name}.heartbeat_age_seconds is missing or invalid")
        if not isinstance(task.get("heartbeat_stale"), bool):
            add(f"tasks.{name}.heartbeat_stale is missing or invalid")
        if not isinstance(task.get("last_error"), str):
            add(f"tasks.{name}.last_error is missing or invalid")
        if instance.get("ok") is True and (
            task.get("state") not in {"starting", "running"}
            or task.get("ready") is not True
            or task.get("heartbeat_stale") is not False
        ):
            add(f"ready instance contains unhealthy expected task {name}")

    queue_health = instance.get("queue_health")
    if not isinstance(queue_health, dict):
        add("queue_health is missing or invalid")
        queue_health = {}
    for field in ("high_water", "drops_total", "drops_since_last_health_check"):
        if not _nonnegative_int(queue_health.get(field)):
            add(f"queue_health.{field} is missing or invalid")
    if not _finite_number(queue_health.get("consumer_lag_seconds")):
        add("queue_health.consumer_lag_seconds is missing or invalid")
    utilization = queue_health.get("utilization")
    if not _finite_number(utilization) or float(utilization or 0) > 1:
        add("queue_health.utilization is missing or invalid")

    clock_health = instance.get("clock_health")
    if not isinstance(clock_health, dict):
        add("clock_health is missing or invalid")
        clock_health = {}
    for field in ("future_events_quarantined", "clock_adjustments"):
        if not _nonnegative_int(clock_health.get(field)):
            add(f"clock_health.{field} is missing or invalid")
    last_quarantine_ms = clock_health.get("last_quarantine_ms")
    if last_quarantine_ms is not None and not _finite_number(last_quarantine_ms, minimum=1):
        add("clock_health.last_quarantine_ms is invalid")

    alarm_delivery = instance.get("alarm_delivery")
    if not isinstance(alarm_delivery, dict):
        add("alarm_delivery is missing or invalid")
        alarm_delivery = {}
    if not isinstance(alarm_delivery.get("enabled"), bool):
        add("alarm_delivery.enabled is missing or invalid")
    if not _nonnegative_int(alarm_delivery.get("local_queue_depth")):
        add("alarm_delivery.local_queue_depth is missing or invalid")
    counts = alarm_delivery.get("counts")
    if not isinstance(counts, dict):
        add("alarm_delivery.counts is missing or invalid")
        counts = {}
    for name in sorted(_ALARM_COUNT_KEYS):
        if not _nonnegative_int(counts.get(name)):
            add(f"alarm_delivery.counts.{name} is missing or invalid")

    markets = instance.get("markets")
    if not isinstance(markets, list) or not markets:
        add("markets is missing, invalid, or empty")
        markets = []
    for index, market in enumerate(markets):
        prefix = f"markets[{index}]"
        if not isinstance(market, dict):
            add(f"{prefix} is invalid")
            continue
        if not isinstance(market.get("symbol"), str) or not market.get("symbol"):
            add(f"{prefix}.symbol is missing or invalid")
        configured, ready, counts_valid = _venue_counts(market)
        if not counts_valid:
            add(f"{prefix} venue counts are missing or invalid")
        reasons = market.get("readiness_reasons")
        if (
            not isinstance(reasons, list)
            or not reasons
            or any(not isinstance(reason, str) or not reason for reason in reasons)
        ):
            add(f"{prefix}.readiness_reasons is missing, empty, or invalid")
        if not isinstance(market.get("readiness_reason"), str) or not market.get(
            "readiness_reason"
        ):
            add(f"{prefix}.readiness_reason is missing or invalid")
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
        if counts_valid and len(venues) != configured:
            add(f"{prefix}.venue_readiness count does not match configured_venues")
        observed_ready = 0
        for raw_name, venue in venues.items():
            name = str(raw_name)
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
        if counts_valid and observed_ready != ready:
            add(f"{prefix}.ready_venues does not match venue readiness evidence")
    return errors


def _docker_evidence_errors(docker: Any) -> list[str]:
    if not isinstance(docker, dict):
        return ["docker section is missing or invalid"]
    errors: list[str] = []
    if docker.get("observed") is not True:
        errors.append("docker state was not explicitly observed")
    if docker.get("present") is not True:
        errors.append("docker container is absent")
    if docker.get("running") is not True or docker.get("status") != "running":
        errors.append("docker container is not running")
    if docker.get("health") != "healthy":
        errors.append("docker container is not healthy")
    if not _nonnegative_int(docker.get("restart_count")):
        errors.append("docker restart_count is missing or invalid")
    if not isinstance(docker.get("container_id"), str) or not docker.get("container_id"):
        errors.append("docker container_id is missing or invalid")
    if not isinstance(docker.get("started_at"), str) or not docker.get("started_at"):
        errors.append("docker started_at is missing or invalid")
    return errors


def _normalized_reasons(raw: Any) -> set[str]:
    if not isinstance(raw, list):
        return set()
    reasons: set[str] = set()
    for reason in raw:
        if not isinstance(reason, str):
            return {"<invalid>"}
        normalized = reason.strip().lower()
        if normalized:
            reasons.add(normalized)
    return reasons


def _runtime_transport_healthy(instance: dict[str, Any]) -> bool:
    readiness = instance.get("readiness")
    feeds = instance.get("feeds")
    return bool(
        instance.get("ok") is True
        and isinstance(readiness, dict)
        and readiness.get("ready") is True
        and isinstance(feeds, dict)
        and feeds
        and all(isinstance(feed, dict) and feed.get("healthy") is True for feed in feeds.values())
    )


def _venue_evidence_supports_quiet_market(market: dict[str, Any]) -> bool:
    venues = market.get("venue_readiness")
    if not isinstance(venues, dict) or not venues:
        return False
    for venue in venues.values():
        if not isinstance(venue, dict):
            return False
        eligible = venue.get("alert_eligible")
        if eligible is True:
            if venue.get("ready") is not True or venue.get("reason") != "ready":
                return False
        elif eligible is False:
            if venue.get("ready") is not False or venue.get("reason") != "stale_data":
                return False
        else:
            return False
    return True


def _recovery_clear(market: dict[str, Any]) -> bool:
    value = market.get("max_recovery_seconds_remaining")
    return bool(
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value == 0
    )


def _quiet_market_stale_only(
    instance: dict[str, Any],
    market: dict[str, Any],
    *,
    configured: int,
    ready: int,
) -> bool:
    if (
        ready <= 0
        or configured <= 0
        or ready >= configured
        or not _runtime_transport_healthy(instance)
        or not _venue_evidence_supports_quiet_market(market)
        or not _recovery_clear(market)
    ):
        return False
    reasons = _normalized_reasons(market.get("readiness_reasons"))
    return reasons == _QUIET_MARKET_REASONS


def _venue_counts(market: dict[str, Any]) -> tuple[int, int, bool]:
    raw_configured = market.get("configured_venues")
    raw_ready = market.get("ready_venues")
    configured = (
        raw_configured
        if isinstance(raw_configured, int) and not isinstance(raw_configured, bool)
        else 0
    )
    ready = raw_ready if isinstance(raw_ready, int) and not isinstance(raw_ready, bool) else 0
    valid = configured > 0 and 0 <= ready <= configured
    return configured, ready, valid


def summarize(
    rows: list[dict[str, Any]],
    *,
    invalid_rows: int,
    interval_seconds: float,
    gate_hours: float,
    min_coverage: float,
) -> dict[str, Any]:
    ordered_rows = sorted(rows, key=lambda item: item["_parsed_timestamp"])
    unique_rows: list[dict[str, Any]] = []
    seen_timestamps: set[datetime] = set()
    duplicate_samples = 0
    for row in ordered_rows:
        timestamp = row["_parsed_timestamp"]
        if timestamp in seen_timestamps:
            duplicate_samples += 1
            continue
        seen_timestamps.add(timestamp)
        unique_rows.append(row)
    rows = unique_rows

    if not rows:
        return {
            "gate_passed": False,
            "gate_reasons": ["no valid soak samples"],
            "samples": 0,
            "invalid_rows": invalid_rows,
        }

    first = rows[0]["_parsed_timestamp"]
    last = rows[-1]["_parsed_timestamp"]
    duration_seconds = max(0.0, (last - first).total_seconds())
    expected_samples = max(1, math.floor(duration_seconds / interval_seconds) + 1)
    coverage = min(1.0, len(rows) / expected_samples)
    primary_failures = sum(
        not (isinstance(row.get("primary"), dict) and row["primary"].get("ok") is True)
        for row in rows
    )
    shadow_failures = sum(
        not (isinstance(row.get("shadow"), dict) and row["shadow"].get("ok") is True)
        for row in rows
    )
    explicit_gaps = [
        float(row["monitor_gap_seconds"])
        for row in rows
        if _finite_number(row.get("monitor_gap_seconds")) and float(row["monitor_gap_seconds"]) > 0
    ]
    timestamp_gaps = [
        (right["_parsed_timestamp"] - left["_parsed_timestamp"]).total_seconds()
        for left, right in zip(rows, rows[1:], strict=False)
        if (right["_parsed_timestamp"] - left["_parsed_timestamp"]).total_seconds()
        >= interval_seconds * 1.5
    ]

    max_feed_age = 0.0
    max_symbol_age = 0.0
    max_queue_size = 0
    max_dropped_events = 0
    max_queue_drops = 0
    max_reconnects = 0
    max_docker_restarts = 0
    max_consumer_lag = 0.0
    max_queue_utilization = 0.0
    max_future_quarantined = 0
    max_clock_adjustments = 0
    max_local_alarm_failures = 0
    max_task_heartbeat_age = 0.0
    max_checkpoint_age = 0.0
    checkpoint_unhealthy_samples = 0
    detector_unready_samples = 0
    detector_unready_pairs: set[str] = set()
    quiet_detector_samples = 0
    quiet_detector_pairs: set[str] = set()
    minimum_detector_ready_ratio = 1.0
    primary_evidence_failure_samples = 0
    shadow_evidence_failure_samples = 0
    docker_evidence_failure_samples = 0
    malformed_gap_samples = 0
    evidence_issues: set[str] = set()
    for row in rows:
        if not _finite_number(row.get("monitor_gap_seconds")):
            malformed_gap_samples += 1
            evidence_issues.add("sample: monitor_gap_seconds is missing or invalid")
        for instance_key in ("primary", "shadow"):
            instance_errors = _instance_evidence_errors(row.get(instance_key))
            if not instance_errors:
                continue
            if instance_key == "primary":
                primary_evidence_failure_samples += 1
            else:
                shadow_evidence_failure_samples += 1
            for error in instance_errors:
                if len(evidence_issues) < _MAX_EVIDENCE_ISSUES:
                    evidence_issues.add(f"{instance_key}: {error}")
        docker_errors = _docker_evidence_errors(row.get("docker"))
        if docker_errors:
            docker_evidence_failure_samples += 1
            for error in docker_errors:
                if len(evidence_issues) < _MAX_EVIDENCE_ISSUES:
                    evidence_issues.add(f"docker: {error}")
        docker = row.get("docker")
        max_docker_restarts = max(
            max_docker_restarts,
            _int_or_zero(docker.get("restart_count")) if isinstance(docker, dict) else 0,
        )
        for instance_key in ("primary", "shadow"):
            raw_instance = row.get(instance_key)
            instance = raw_instance if isinstance(raw_instance, dict) else {}
            instance_detector_unready = False
            instance_quiet_detector = False
            markets = instance.get("markets")
            if not isinstance(markets, list) or not markets:
                instance_detector_unready = True
                detector_unready_pairs.add(f"{instance_key}:unknown:missing")
                minimum_detector_ready_ratio = 0.0
                markets = []
            for market in markets:
                if not isinstance(market, dict):
                    instance_detector_unready = True
                    detector_unready_pairs.add(f"{instance_key}:unknown:invalid")
                    minimum_detector_ready_ratio = 0.0
                    continue
                symbol = str(market.get("symbol") or "unknown")
                configured, ready, counts_valid = _venue_counts(market)
                ratio = ready / configured if counts_valid else 0.0
                minimum_detector_ready_ratio = min(minimum_detector_ready_ratio, ratio)
                pair = f"{instance_key}:{symbol}:{ready}/{configured}"
                if not counts_valid:
                    instance_detector_unready = True
                    detector_unready_pairs.add(pair)
                elif ready < configured:
                    if _quiet_market_stale_only(
                        instance,
                        market,
                        configured=configured,
                        ready=ready,
                    ):
                        instance_quiet_detector = True
                        quiet_detector_pairs.add(pair)
                    else:
                        instance_detector_unready = True
                        detector_unready_pairs.add(pair)
                else:
                    reasons = _normalized_reasons(market.get("readiness_reasons"))
                    if reasons - {"ready"} or not _recovery_clear(market):
                        instance_detector_unready = True
                        detector_unready_pairs.add(pair)
            if instance_detector_unready:
                detector_unready_samples += 1
            if instance_quiet_detector:
                quiet_detector_samples += 1
            max_queue_size = max(max_queue_size, _int_or_zero(instance.get("queue_size")))
            raw_queue_health = instance.get("queue_health")
            queue_health = raw_queue_health if isinstance(raw_queue_health, dict) else {}
            max_queue_drops = max(
                max_queue_drops,
                _int_or_zero(queue_health.get("drops_total")),
            )
            max_consumer_lag = max(
                max_consumer_lag,
                _number_or_zero(queue_health.get("consumer_lag_seconds")),
            )
            max_queue_utilization = max(
                max_queue_utilization,
                _number_or_zero(queue_health.get("utilization")),
            )
            raw_clock_health = instance.get("clock_health")
            clock_health = raw_clock_health if isinstance(raw_clock_health, dict) else {}
            max_future_quarantined = max(
                max_future_quarantined,
                _int_or_zero(clock_health.get("future_events_quarantined")),
            )
            max_clock_adjustments = max(
                max_clock_adjustments,
                _int_or_zero(clock_health.get("clock_adjustments")),
            )
            raw_alarm_delivery = instance.get("alarm_delivery")
            alarm_delivery = raw_alarm_delivery if isinstance(raw_alarm_delivery, dict) else {}
            raw_alarm_counts = alarm_delivery.get("counts")
            alarm_counts = raw_alarm_counts if isinstance(raw_alarm_counts, dict) else {}
            max_local_alarm_failures = max(
                max_local_alarm_failures,
                _int_or_zero(alarm_counts.get("failure")),
            )
            raw_checkpoint = instance.get("checkpoint_health")
            checkpoint = raw_checkpoint if isinstance(raw_checkpoint, dict) else {}
            if checkpoint.get("enabled") and not checkpoint.get("healthy"):
                checkpoint_unhealthy_samples += 1
            checkpoint_age = checkpoint.get("last_success_age_seconds")
            if _finite_number(checkpoint_age):
                max_checkpoint_age = max(
                    max_checkpoint_age,
                    float(checkpoint_age),
                )
            raw_tasks = instance.get("tasks")
            tasks = raw_tasks if isinstance(raw_tasks, dict) else {}
            for task in tasks.values():
                if not isinstance(task, dict):
                    continue
                heartbeat_age = task.get("heartbeat_age_seconds")
                if _finite_number(heartbeat_age):
                    max_task_heartbeat_age = max(
                        max_task_heartbeat_age,
                        float(heartbeat_age),
                    )
            raw_feeds = instance.get("feeds")
            feeds = raw_feeds if isinstance(raw_feeds, dict) else {}
            for feed in feeds.values():
                if not isinstance(feed, dict):
                    continue
                age = feed.get("message_age_seconds")
                if _finite_number(age):
                    max_feed_age = max(max_feed_age, float(age))
                raw_symbols = feed.get("symbols")
                symbols = raw_symbols if isinstance(raw_symbols, dict) else {}
                for symbol in symbols.values():
                    if not isinstance(symbol, dict):
                        continue
                    symbol_age = symbol.get("message_age_seconds")
                    if _finite_number(symbol_age):
                        max_symbol_age = max(
                            max_symbol_age,
                            float(symbol_age),
                        )
                max_dropped_events = max(
                    max_dropped_events,
                    _int_or_zero(feed.get("dropped_events")),
                )
                max_reconnects = max(
                    max_reconnects,
                    _int_or_zero(feed.get("reconnects")),
                )

    gate_reasons: list[str] = []
    required_seconds = gate_hours * 3600
    if duration_seconds < required_seconds:
        gate_reasons.append(f"duration {duration_seconds / 3600:.2f}h is below {gate_hours:.2f}h")
    if coverage < min_coverage:
        gate_reasons.append(f"sample coverage {coverage:.1%} is below {min_coverage:.1%}")
    if duplicate_samples:
        gate_reasons.append(f"{duplicate_samples} duplicate timestamp sample(s)")
    if primary_failures:
        gate_reasons.append(f"{primary_failures} primary failure sample(s)")
    if shadow_failures:
        gate_reasons.append(f"{shadow_failures} shadow failure sample(s)")
    if primary_evidence_failure_samples:
        gate_reasons.append(
            f"{primary_evidence_failure_samples} incomplete primary evidence sample(s)"
        )
    if shadow_evidence_failure_samples:
        gate_reasons.append(
            f"{shadow_evidence_failure_samples} incomplete shadow evidence sample(s)"
        )
    if docker_evidence_failure_samples:
        gate_reasons.append(
            f"{docker_evidence_failure_samples} absent, unobserved, or unhealthy Docker sample(s)"
        )
    if malformed_gap_samples:
        gate_reasons.append(f"{malformed_gap_samples} malformed monitor-gap sample(s)")
    if explicit_gaps or timestamp_gaps:
        gate_reasons.append(f"{len(explicit_gaps) + len(timestamp_gaps)} monitoring gap(s)")
    if max_dropped_events or max_queue_drops:
        gate_reasons.append(
            f"drop counter reached {max_dropped_events} feed / {max_queue_drops} market queue"
        )
    if max_future_quarantined:
        gate_reasons.append(f"future-event quarantine counter reached {max_future_quarantined}")
    if max_clock_adjustments:
        gate_reasons.append(f"clock adjustment counter reached {max_clock_adjustments}")
    if max_local_alarm_failures:
        gate_reasons.append(f"local alarm failure counter reached {max_local_alarm_failures}")
    if checkpoint_unhealthy_samples:
        gate_reasons.append(f"{checkpoint_unhealthy_samples} unhealthy checkpoint sample(s)")
    if detector_unready_samples:
        gate_reasons.append(f"{detector_unready_samples} detector-unready instance sample(s)")
    primary_restarts = _runtime_restarts(rows, "primary")
    shadow_restarts = _runtime_restarts(rows, "shadow")
    docker_runtime_restarts = _docker_runtime_restarts(rows)
    if primary_restarts or shadow_restarts or max_docker_restarts or docker_runtime_restarts:
        gate_reasons.append(
            "runtime restarts observed: "
            f"primary {primary_restarts}, shadow {shadow_restarts}, "
            f"container counter {max_docker_restarts}, "
            f"container identity {docker_runtime_restarts}"
        )
    if invalid_rows:
        gate_reasons.append(f"{invalid_rows} invalid JSONL row(s)")

    return {
        "gate_passed": not gate_reasons,
        "gate_hours": gate_hours,
        "gate_reasons": gate_reasons,
        "samples": len(rows),
        "duplicate_timestamp_samples": duplicate_samples,
        "invalid_rows": invalid_rows,
        "first_timestamp": first.isoformat(),
        "last_timestamp": last.isoformat(),
        "duration_seconds": round(duration_seconds, 3),
        "coverage": round(coverage, 6),
        "primary_failure_samples": primary_failures,
        "shadow_failure_samples": shadow_failures,
        "longest_primary_failure_run": _longest_failure_run(rows, "primary"),
        "longest_shadow_failure_run": _longest_failure_run(rows, "shadow"),
        "monitoring_gaps": len(explicit_gaps) + len(timestamp_gaps),
        "largest_gap_seconds": round(max([0.0, *explicit_gaps, *timestamp_gaps]), 3),
        "max_feed_age_seconds": round(max_feed_age, 3),
        "max_symbol_age_seconds": round(max_symbol_age, 3),
        "max_queue_size": max_queue_size,
        "max_dropped_events": max_dropped_events,
        "max_queue_drops": max_queue_drops,
        "max_consumer_lag_seconds": round(max_consumer_lag, 3),
        "max_queue_utilization": round(max_queue_utilization, 6),
        "max_reconnects": max_reconnects,
        "max_docker_restarts": max_docker_restarts,
        "docker_runtime_restarts": docker_runtime_restarts,
        "docker_evidence_failure_samples": docker_evidence_failure_samples,
        "primary_runtime_restarts": primary_restarts,
        "shadow_runtime_restarts": shadow_restarts,
        "primary_evidence_failure_samples": primary_evidence_failure_samples,
        "shadow_evidence_failure_samples": shadow_evidence_failure_samples,
        "malformed_gap_samples": malformed_gap_samples,
        "evidence_issues": sorted(evidence_issues),
        "max_future_events_quarantined": max_future_quarantined,
        "max_clock_adjustments": max_clock_adjustments,
        "max_local_alarm_failures": max_local_alarm_failures,
        "max_task_heartbeat_age_seconds": round(max_task_heartbeat_age, 3),
        "max_checkpoint_age_seconds": round(max_checkpoint_age, 3),
        "checkpoint_unhealthy_samples": checkpoint_unhealthy_samples,
        "detector_unready_samples": detector_unready_samples,
        "detector_unready_pairs": sorted(detector_unready_pairs),
        "quiet_detector_samples": quiet_detector_samples,
        "quiet_detector_pairs": sorted(quiet_detector_pairs),
        "minimum_detector_ready_ratio": round(minimum_detector_ready_ratio, 6),
    }


def main() -> None:
    args = _parser().parse_args()
    if args.hours <= 0:
        raise SystemExit("--hours must be positive")
    if args.interval < 5:
        raise SystemExit("--interval must be at least 5 seconds")
    if not 0 < args.min_coverage <= 1:
        raise SystemExit("--min-coverage must be in (0, 1]")
    since = _timestamp(args.since) if args.since else None
    rows, invalid = load_rows(Path(args.input), since)
    summary = summarize(
        rows,
        invalid_rows=invalid,
        interval_seconds=args.interval,
        gate_hours=args.hours,
        min_coverage=args.min_coverage,
    )
    if args.json:
        print(json.dumps(summary, indent=2, sort_keys=True))
    else:
        verdict = "PASS" if summary["gate_passed"] else "INCOMPLETE"
        print(f"Soak gate: {verdict}")
        print(
            f"Samples: {summary.get('samples', 0)} | "
            f"Duration: {summary.get('duration_seconds', 0) / 3600:.2f}h | "
            f"Coverage: {summary.get('coverage', 0):.1%}"
        )
        if summary.get("quiet_detector_samples"):
            print(
                "Quiet-market stale detector samples: "
                f"{summary['quiet_detector_samples']} (informational)"
            )
        for reason in summary.get("gate_reasons", []):
            print(f"- {reason}")
    if args.require_complete and not summary["gate_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
