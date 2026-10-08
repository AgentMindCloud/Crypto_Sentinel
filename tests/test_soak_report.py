from __future__ import annotations

import copy
import importlib.util
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest


def _load_module():
    script = Path(__file__).parents[1] / "scripts" / "soak_report.py"
    spec = importlib.util.spec_from_file_location("soak_report", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


SOAK_REPORT = _load_module()


def _task_evidence() -> dict:
    return {
        "state": "running",
        "ready": True,
        "heartbeat_age_seconds": 0.5,
        "heartbeat_stale": False,
        "last_error": "",
    }


def _market(
    *,
    symbol: str = "BTCUSDT",
    ready_venues: int = 3,
    configured_venues: int = 3,
    reasons: list[str] | None = None,
    recovery_seconds: float = 0,
) -> dict:
    readiness_reasons = ["ready"] if reasons is None else reasons
    venue_names = ("binance", "bybit", "okx")
    venues = {}
    for index, name in enumerate(venue_names):
        ready = index < max(0, min(ready_venues, len(venue_names)))
        venues[name] = {
            "ready": ready,
            "base_ready": ready,
            "alert_eligible": ready,
            "reason": "ready" if ready else "stale_data",
            "data_age_seconds": 0.2,
        }
    return {
        "symbol": symbol,
        "ready_venues": ready_venues,
        "configured_venues": configured_venues,
        "readiness_reason": readiness_reasons[-1] if readiness_reasons else "invalid",
        "readiness_reasons": readiness_reasons,
        "max_recovery_seconds_remaining": recovery_seconds,
        "min_window_coverage_ratio": 1.0,
        "max_largest_gap_seconds": 1.0,
        "venue_readiness": venues,
    }


def _set_market_readiness(
    instance: dict,
    *,
    ready_venues: int,
    reasons: list[str],
    recovery_seconds: float = 0,
    symbol: str | None = None,
) -> None:
    current = instance["markets"][0]
    replacement = _market(
        symbol=symbol or current["symbol"],
        ready_venues=ready_venues,
        configured_venues=current["configured_venues"],
        reasons=reasons,
        recovery_seconds=recovery_seconds,
    )
    instance["markets"][0] = replacement


def _row(timestamp: datetime, *, primary: bool = True, shadow: bool = True) -> dict:
    feed = {
        "healthy": True,
        "connected": True,
        "subscription_acknowledged": True,
        "message_age_seconds": 0.2,
        "dropped_events": 0,
        "reconnects": 0,
        "trades": 10,
        "liquidations": 0,
        "symbols": {
            "BTCUSDT": {
                "healthy": True,
                "message_age_seconds": 0.2,
                "messages": 10,
                "trades": 10,
                "liquidations": 0,
            }
        },
    }
    expected_tasks = sorted(
        {
            "event-consumer",
            "detector",
            "health-monitor",
            "maintenance",
            "alert-workers",
            "state-checkpoint",
            "feed-binance",
            "feed-bybit",
            "feed-okx",
        }
    )
    instance = {
        "ok": True,
        "evidence_complete": True,
        "evidence_errors": [],
        "runtime_started_ms": 1_700_000_000_000,
        "uptime_seconds": 120,
        "readiness": {"ready": True, "state": "ready", "reasons": []},
        "queue_size": 0,
        "queue_capacity": 100,
        "feeds": {
            "binance": copy.deepcopy(feed),
            "bybit": copy.deepcopy(feed),
            "okx": copy.deepcopy(feed),
        },
        "expected_tasks": expected_tasks,
        "tasks": {name: _task_evidence() for name in expected_tasks},
        "queue_health": {
            "high_water": 0,
            "drops_total": 0,
            "drops_since_last_health_check": 0,
            "consumer_lag_seconds": 0,
            "utilization": 0,
        },
        "clock_health": {
            "future_events_quarantined": 0,
            "clock_adjustments": 0,
            "last_quarantine_ms": None,
        },
        "alarm_delivery": {
            "enabled": True,
            "local_queue_depth": 0,
            "counts": {
                "queued": 0,
                "attempt": 0,
                "success": 0,
                "failure": 0,
            },
        },
        "checkpoint_health": {
            "enabled": True,
            "healthy": True,
            "reason": "current",
            "last_success_age_seconds": 2,
            "last_failure_ms": None,
            "path_exists": True,
            "path_age_seconds": 2,
            "within_initial_allowance": False,
        },
        "markets": [_market()],
    }
    primary_row = copy.deepcopy(instance)
    primary_row["ok"] = primary
    shadow_row = copy.deepcopy(instance)
    shadow_row["ok"] = shadow
    return {
        "timestamp": timestamp.isoformat(),
        "_parsed_timestamp": timestamp,
        "monitor_gap_seconds": 0,
        "primary": primary_row,
        "shadow": shadow_row,
        "docker": {
            "observed": True,
            "present": True,
            "running": True,
            "status": "running",
            "health": "healthy",
            "restart_count": 0,
            "container_id": "a" * 64,
            "started_at": "2026-01-01T00:00:00Z",
        },
    }


def test_complete_gate_passes() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row(start + timedelta(minutes=index)) for index in range(61)]

    summary = SOAK_REPORT.summarize(
        rows,
        invalid_rows=0,
        interval_seconds=60,
        gate_hours=1,
        min_coverage=0.98,
    )

    assert summary["gate_passed"] is True
    assert summary["duration_seconds"] == 3600
    assert summary["coverage"] == 1


def test_docker_absent_fails_complete_gate() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row(start), _row(start + timedelta(minutes=1))]
    rows[1]["docker"] = {
        "observed": True,
        "present": False,
    }

    summary = SOAK_REPORT.summarize(
        rows,
        invalid_rows=0,
        interval_seconds=60,
        gate_hours=1 / 60,
        min_coverage=0.98,
    )

    assert summary["gate_passed"] is False
    assert summary["docker_evidence_failure_samples"] == 1
    assert any("Docker" in reason for reason in summary["gate_reasons"])


@pytest.mark.parametrize(
    ("instance_key", "section"),
    [
        ("primary", "readiness"),
        ("primary", "feeds"),
        ("primary", "expected_tasks"),
        ("primary", "tasks"),
        ("primary", "queue_health"),
        ("shadow", "clock_health"),
        ("shadow", "alarm_delivery"),
        ("shadow", "checkpoint_health"),
        ("shadow", "markets"),
        ("shadow", "runtime_started_ms"),
    ],
)
def test_missing_required_instance_evidence_fails_closed(
    instance_key,
    section,
) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row(start), _row(start + timedelta(minutes=1))]
    rows[1][instance_key].pop(section)

    summary = SOAK_REPORT.summarize(
        rows,
        invalid_rows=0,
        interval_seconds=60,
        gate_hours=1 / 60,
        min_coverage=0.98,
    )

    assert summary["gate_passed"] is False
    assert summary[f"{instance_key}_evidence_failure_samples"] == 1
    assert any(
        f"incomplete {instance_key} evidence" in reason for reason in summary["gate_reasons"]
    )


def test_malformed_alarm_counts_fail_closed() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row(start), _row(start + timedelta(minutes=1))]
    rows[1]["primary"]["alarm_delivery"]["counts"]["failure"] = "0"

    summary = SOAK_REPORT.summarize(
        rows,
        invalid_rows=0,
        interval_seconds=60,
        gate_hours=1 / 60,
        min_coverage=0.98,
    )

    assert summary["gate_passed"] is False
    assert summary["primary_evidence_failure_samples"] == 1
    assert (
        "primary: alarm_delivery.counts.failure is missing or invalid" in summary["evidence_issues"]
    )


def test_duplicate_timestamps_do_not_inflate_coverage() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row(start + timedelta(minutes=index)) for index in range(61) if index not in {20, 40}]
    rows.extend([copy.deepcopy(rows[10]), copy.deepcopy(rows[30])])

    summary = SOAK_REPORT.summarize(
        rows,
        invalid_rows=0,
        interval_seconds=60,
        gate_hours=1,
        min_coverage=0.98,
    )

    assert summary["gate_passed"] is False
    assert summary["samples"] == 59
    assert summary["duplicate_timestamp_samples"] == 2
    assert summary["coverage"] == pytest.approx(59 / 61, abs=1e-6)


def test_ninety_second_sampling_is_a_gap_for_sixty_second_gate() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row(start + timedelta(seconds=index * 90)) for index in range(41)]

    summary = SOAK_REPORT.summarize(
        rows,
        invalid_rows=0,
        interval_seconds=60,
        gate_hours=1,
        min_coverage=0.98,
    )

    assert summary["gate_passed"] is False
    assert summary["monitoring_gaps"] == 40
    assert summary["coverage"] == pytest.approx(41 / 61, abs=1e-6)


def test_runtime_identity_change_detects_restart_between_low_uptime_samples() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row(start), _row(start + timedelta(minutes=1))]
    rows[0]["primary"]["uptime_seconds"] = 10
    rows[1]["primary"]["uptime_seconds"] = 11
    rows[1]["primary"]["runtime_started_ms"] += 50_000

    summary = SOAK_REPORT.summarize(
        rows,
        invalid_rows=0,
        interval_seconds=60,
        gate_hours=1 / 60,
        min_coverage=0.98,
    )

    assert summary["gate_passed"] is False
    assert summary["primary_runtime_restarts"] == 1


def test_feed_and_symbol_ages_remain_separate_diagnostics() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row(start), _row(start + timedelta(minutes=1))]
    rows[1]["primary"]["feeds"]["bybit"].update(
        {
            "message_age_seconds": 0.25,
            "symbols": {
                "BTCUSDT": {"message_age_seconds": 0.1},
                "BNBUSDT": {"message_age_seconds": 120},
            },
        }
    )

    summary = SOAK_REPORT.summarize(
        rows,
        invalid_rows=0,
        interval_seconds=60,
        gate_hours=1 / 60,
        min_coverage=0.98,
    )

    assert summary["max_feed_age_seconds"] == 0.25
    assert summary["max_symbol_age_seconds"] == 120


def test_failure_and_gap_keep_gate_incomplete() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [
        _row(start),
        _row(start + timedelta(minutes=1), shadow=False),
        _row(start + timedelta(minutes=4)),
    ]

    summary = SOAK_REPORT.summarize(
        rows,
        invalid_rows=1,
        interval_seconds=60,
        gate_hours=0.05,
        min_coverage=0.98,
    )

    assert summary["gate_passed"] is False
    assert summary["shadow_failure_samples"] == 1
    assert summary["monitoring_gaps"] == 1
    assert any("invalid JSONL" in reason for reason in summary["gate_reasons"])


def test_load_rows_filters_since_and_counts_invalid(tmp_path) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    path = tmp_path / "soak.jsonl"
    first = _row(start)
    second = _row(start + timedelta(minutes=1))
    first.pop("_parsed_timestamp")
    second.pop("_parsed_timestamp")
    path.write_text(
        "\n".join([json.dumps(first), "{bad json", json.dumps(second)]),
        encoding="utf-8",
    )

    rows, invalid = SOAK_REPORT.load_rows(path, start + timedelta(seconds=30))

    assert invalid == 1
    assert len(rows) == 1
    assert rows[0]["timestamp"] == second["timestamp"]


def test_runtime_counters_and_uptime_regression_fail_gate() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row(start), _row(start + timedelta(minutes=1))]
    rows[0]["primary"].update({"uptime_seconds": 120})
    rows[1]["primary"].update(
        {
            "uptime_seconds": 10,
            "queue_health": {
                "drops_total": 2,
                "consumer_lag_seconds": 3.25,
                "utilization": 0.4,
            },
            "clock_health": {
                "future_events_quarantined": 1,
                "clock_adjustments": 1,
            },
            "alarm_delivery": {"counts": {"failure": 1}},
            "checkpoint_health": {
                "enabled": True,
                "healthy": False,
                "last_success_age_seconds": 190,
            },
            "tasks": {
                "detector": {
                    "heartbeat_age_seconds": 2.5,
                }
            },
        }
    )
    _set_market_readiness(
        rows[1]["primary"],
        ready_venues=2,
        reasons=["ready", "stale_data"],
    )

    summary = SOAK_REPORT.summarize(
        rows,
        invalid_rows=0,
        interval_seconds=60,
        gate_hours=1 / 60,
        min_coverage=0.98,
    )

    assert summary["gate_passed"] is False
    assert summary["max_queue_drops"] == 2
    assert summary["max_future_events_quarantined"] == 1
    assert summary["max_clock_adjustments"] == 1
    assert summary["max_local_alarm_failures"] == 1
    assert summary["primary_runtime_restarts"] == 1
    assert summary["max_consumer_lag_seconds"] == 3.25
    assert summary["max_task_heartbeat_age_seconds"] == 2.5
    assert summary["max_checkpoint_age_seconds"] == 190
    assert summary["checkpoint_unhealthy_samples"] == 1
    assert summary["quiet_detector_samples"] == 1


def test_detector_unready_venue_fails_gate() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row(start), _row(start + timedelta(minutes=1))]
    _set_market_readiness(
        rows[1]["shadow"],
        ready_venues=2,
        reasons=["ready", "recovering_after_gap"],
    )

    summary = SOAK_REPORT.summarize(
        rows,
        invalid_rows=0,
        interval_seconds=60,
        gate_hours=1 / 60,
        min_coverage=0.98,
    )

    assert summary["gate_passed"] is False
    assert summary["detector_unready_samples"] == 1
    assert summary["detector_unready_pairs"] == ["shadow:BTCUSDT:2/3"]
    assert summary["minimum_detector_ready_ratio"] == 0.666667
    assert any("detector-unready" in reason for reason in summary["gate_reasons"])


def test_quiet_market_stale_data_is_informational_but_ratio_is_retained() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row(start), _row(start + timedelta(minutes=1))]
    _set_market_readiness(
        rows[1]["shadow"],
        symbol="BNBUSDT",
        ready_venues=1,
        reasons=["ready", "stale_data"],
    )

    summary = SOAK_REPORT.summarize(
        rows,
        invalid_rows=0,
        interval_seconds=60,
        gate_hours=1 / 60,
        min_coverage=0.98,
    )

    assert summary["gate_passed"] is True
    assert summary["detector_unready_samples"] == 0
    assert summary["detector_unready_pairs"] == []
    assert summary["quiet_detector_samples"] == 1
    assert summary["quiet_detector_pairs"] == ["shadow:BNBUSDT:1/3"]
    assert summary["minimum_detector_ready_ratio"] == 0.333333
    assert not any("detector-unready" in reason for reason in summary["gate_reasons"])


def test_quiet_market_exemption_rejects_nonstale_venue_evidence() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row(start), _row(start + timedelta(minutes=1))]
    _set_market_readiness(
        rows[1]["shadow"],
        ready_venues=2,
        reasons=["ready", "stale_data"],
    )
    okx_evidence = rows[1]["shadow"]["markets"][0]["venue_readiness"]["okx"]
    okx_evidence["reason"] = "recovering_after_gap"

    summary = SOAK_REPORT.summarize(
        rows,
        invalid_rows=0,
        interval_seconds=60,
        gate_hours=1 / 60,
        min_coverage=0.98,
    )

    assert summary["gate_passed"] is False
    assert summary["detector_unready_samples"] == 1
    assert summary["quiet_detector_samples"] == 0


@pytest.mark.parametrize(
    "reasons",
    [
        [],
        ["ready"],
        ["stale_data"],
        ["recovering_after_gap"],
        ["ready", "recovering_after_gap"],
        ["stale_data", "recovering_after_gap"],
        ["insufficient_baseline"],
        ["warming_up"],
        ["continuity_break"],
    ],
)
def test_non_quiet_detector_deficits_remain_fail_closed(reasons) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row(start), _row(start + timedelta(minutes=1))]
    _set_market_readiness(
        rows[1]["primary"],
        ready_venues=2,
        reasons=reasons,
    )

    summary = SOAK_REPORT.summarize(
        rows,
        invalid_rows=0,
        interval_seconds=60,
        gate_hours=1 / 60,
        min_coverage=0.98,
    )

    assert summary["gate_passed"] is False
    assert summary["detector_unready_samples"] == 1
    assert summary["quiet_detector_samples"] == 0
    assert summary["detector_unready_pairs"] == ["primary:BTCUSDT:2/3"]


def test_stale_data_does_not_bypass_unhealthy_transport() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row(start), _row(start + timedelta(minutes=1))]
    rows[1]["primary"]["feeds"]["bybit"]["healthy"] = False
    _set_market_readiness(
        rows[1]["primary"],
        ready_venues=2,
        reasons=["ready", "stale_data"],
    )

    summary = SOAK_REPORT.summarize(
        rows,
        invalid_rows=0,
        interval_seconds=60,
        gate_hours=1 / 60,
        min_coverage=0.98,
    )

    assert summary["gate_passed"] is False
    assert summary["detector_unready_samples"] == 1
    assert summary["quiet_detector_samples"] == 0


def test_quiet_and_blocking_markets_are_counted_independently() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row(start), _row(start + timedelta(minutes=1))]
    rows[1]["primary"]["markets"] = [
        _market(
            symbol="BNBUSDT",
            ready_venues=2,
            reasons=["ready", "stale_data"],
        ),
        _market(
            symbol="BTCUSDT",
            ready_venues=2,
            reasons=["ready", "recovering_after_gap"],
            recovery_seconds=30,
        ),
    ]

    summary = SOAK_REPORT.summarize(
        rows,
        invalid_rows=0,
        interval_seconds=60,
        gate_hours=1 / 60,
        min_coverage=0.98,
    )

    assert summary["gate_passed"] is False
    assert summary["quiet_detector_samples"] == 1
    assert summary["quiet_detector_pairs"] == ["primary:BNBUSDT:2/3"]
    assert summary["detector_unready_samples"] == 1
    assert summary["detector_unready_pairs"] == ["primary:BTCUSDT:2/3"]


def test_positive_recovery_timer_blocks_quiet_exemption() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row(start), _row(start + timedelta(minutes=1))]
    _set_market_readiness(
        rows[1]["shadow"],
        ready_venues=2,
        reasons=["ready", "stale_data"],
        recovery_seconds=10,
    )

    summary = SOAK_REPORT.summarize(
        rows,
        invalid_rows=0,
        interval_seconds=60,
        gate_hours=1 / 60,
        min_coverage=0.98,
    )

    assert summary["gate_passed"] is False
    assert summary["detector_unready_samples"] == 1
    assert summary["quiet_detector_samples"] == 0


def test_missing_market_evidence_fails_closed() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row(start), _row(start + timedelta(minutes=1))]
    rows[1]["primary"]["markets"] = []

    summary = SOAK_REPORT.summarize(
        rows,
        invalid_rows=0,
        interval_seconds=60,
        gate_hours=1 / 60,
        min_coverage=0.98,
    )

    assert summary["gate_passed"] is False
    assert summary["detector_unready_samples"] == 1
    assert summary["detector_unready_pairs"] == ["primary:unknown:missing"]
    assert summary["minimum_detector_ready_ratio"] == 0


@pytest.mark.parametrize(
    ("ready", "configured", "reasons"),
    [
        (3, 3, ["stale_data"]),
        (3, 3, ["ready", "recovering_after_gap"]),
        (4, 3, ["ready"]),
        (-1, 3, ["stale_data"]),
        (0, 3, ["ready", "stale_data"]),
        (0, 0, ["stale_data"]),
    ],
)
def test_inconsistent_detector_evidence_remains_fail_closed(
    ready,
    configured,
    reasons,
) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [_row(start), _row(start + timedelta(minutes=1))]
    rows[1]["shadow"]["markets"][0].update(
        {
            "ready_venues": ready,
            "configured_venues": configured,
            "readiness_reasons": reasons,
        }
    )

    summary = SOAK_REPORT.summarize(
        rows,
        invalid_rows=0,
        interval_seconds=60,
        gate_hours=1 / 60,
        min_coverage=0.98,
    )

    assert summary["gate_passed"] is False
    assert summary["detector_unready_samples"] == 1
    assert summary["quiet_detector_samples"] == 0
