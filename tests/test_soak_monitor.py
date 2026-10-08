from __future__ import annotations

import importlib.util
import io
import json
from pathlib import Path


def _load_module():
    script = Path(__file__).parents[1] / "scripts" / "soak_monitor.py"
    spec = importlib.util.spec_from_file_location("soak_monitor", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


SOAK_MONITOR = _load_module()


class _Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _task_row() -> dict:
    return {
        "state": "running",
        "ready": True,
        "heartbeat_age_seconds": 1.0,
        "heartbeat_stale": False,
        "last_error": "",
    }


def _complete_payload(exchange: str = "binance") -> dict:
    task_names = {
        "event-consumer",
        "detector",
        "health-monitor",
        "maintenance",
        "alert-workers",
        "state-checkpoint",
        f"feed-{exchange}",
    }
    return {
        "started_ms": 1_700_000_000_000,
        "uptime_seconds": 120,
        "queue_size": 2,
        "queue_capacity": 100,
        "stale_after_seconds": {exchange: 45},
        "symbol_freshness_seconds": 30,
        "readiness": {"ready": True, "state": "ready", "reasons": []},
        "feeds": {
            exchange: {
                "connected": True,
                "subscription_acknowledged": True,
                "message_age_seconds": 0.5,
                "latest_message_age_seconds": 0.5,
                "oldest_symbol_message_age_seconds": 0.5,
                "reconnects": 1,
                "dropped": 0,
                "trades": 10,
                "liquidations": 1,
                "symbols": {
                    "BTCUSDT": {
                        "seen_since_connect": True,
                        "message_age_seconds": 0.4,
                        "messages": 10,
                        "trades": 10,
                        "liquidations": 0,
                    }
                },
            }
        },
        "tasks": {name: _task_row() for name in task_names},
        "queue_health": {
            "high_water": 5,
            "drops_total": 0,
            "drops_since_last_health_check": 0,
            "consumer_lag_seconds": 0.25,
            "utilization": 0.02,
        },
        "clock_health": {
            "future_events_quarantined": 0,
            "clock_adjustments": 0,
            "last_quarantine_ms": None,
        },
        "alarm_delivery": {
            "local_alarm": {
                "enabled": True,
                "queue_depth": 0,
                "counts": {
                    "queued": 3,
                    "attempt": 3,
                    "success": 3,
                    "failure": 0,
                },
                "last_receipt": {
                    "alert_id": "not-needed-in-soak",
                    "status": "success",
                    "attempt": 1,
                    "timestamp_ms": 456,
                    "detail": "",
                },
            }
        },
        "checkpoint_health": {
            "enabled": True,
            "healthy": True,
            "reason": "current",
            "last_success_age_seconds": 2.5,
            "last_failure_ms": None,
            "path_exists": True,
            "path_age_seconds": 2.4,
            "within_initial_allowance": False,
        },
        "markets": [
            {
                "symbol": "BTCUSDT",
                "ready_venues": 1,
                "configured_venues": 1,
                "readiness_reason": "ready",
                "readiness_reasons": ["ready"],
                "venue_readiness": {
                    exchange: {
                        "ready": True,
                        "base_ready": True,
                        "alert_eligible": True,
                        "reason": "ready",
                        "data_age_seconds": 0.2,
                        "window_coverage_ratio": 1.0,
                        "largest_gap_seconds": 1,
                        "recovery_seconds_remaining": 0,
                        "missing_windows_seconds": [],
                    }
                },
                "max_recovery_seconds_remaining": 0,
                "min_window_coverage_ratio": 1,
                "max_largest_gap_seconds": 1,
            }
        ],
        "deployment": {"startup": "per-user-supervisor"},
    }


def test_market_evidence_preserves_quiet_stale_reasons() -> None:
    evidence = SOAK_MONITOR._market_evidence(
        {
            "symbol": "BNBUSDT",
            "ready_venues": 2,
            "configured_venues": 3,
            "readiness_reason": "stale_data",
            "readiness_reasons": ["ready", "stale_data"],
            "max_recovery_seconds_remaining": 0,
        }
    )

    assert evidence["readiness_reason"] == "stale_data"
    assert evidence["readiness_reasons"] == ["ready", "stale_data"]
    assert evidence["ready_venues"] == 2
    assert evidence["configured_venues"] == 3


def test_fetch_status_records_truthful_reliability_evidence(monkeypatch) -> None:
    payload = _complete_payload()
    payload["feeds"]["binance"]["dropped"] = 2
    payload["queue_health"]["drops_total"] = 2
    payload["clock_health"]["future_events_quarantined"] = 1
    payload["clock_health"]["last_quarantine_ms"] = 123

    monkeypatch.setattr(
        SOAK_MONITOR.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _Response(json.dumps(payload).encode()),
    )

    result = SOAK_MONITOR._fetch_status("http://127.0.0.1:8787", "secret")

    assert result["ok"] is True
    assert result["evidence_complete"] is True
    assert result["runtime_started_ms"] == 1_700_000_000_000
    assert result["feeds"]["binance"]["dropped_events"] == 2
    assert result["feeds"]["binance"]["symbols"]["BTCUSDT"]["healthy"] is True
    assert result["queue_health"]["drops_total"] == 2
    assert result["clock_health"]["future_events_quarantined"] == 1
    assert result["alarm_delivery"]["counts"]["success"] == 3
    assert result["alarm_delivery"]["last_receipt"]["status"] == "success"
    assert "alert_id" not in result["alarm_delivery"]["last_receipt"]
    assert result["checkpoint_health"]["healthy"] is True
    assert result["checkpoint_health"]["last_success_age_seconds"] == 2.5
    assert result["markets"][0]["min_window_coverage_ratio"] == 1
    assert result["markets"][0]["max_largest_gap_seconds"] == 1
    assert result["markets"][0]["readiness_reason"] == "ready"
    assert result["markets"][0]["venue_readiness"]["binance"] == {
        "ready": True,
        "base_ready": True,
        "alert_eligible": True,
        "reason": "ready",
        "data_age_seconds": 0.2,
        "window_coverage_ratio": 1.0,
        "largest_gap_seconds": 1,
        "recovery_seconds_remaining": 0,
        "missing_windows_seconds": [],
    }


def test_declared_degraded_readiness_overrides_healthy_feeds(monkeypatch) -> None:
    payload = _complete_payload("okx")
    payload["readiness"] = {
        "ready": False,
        "state": "degraded",
        "reasons": ["task detector heartbeat stale"],
    }
    monkeypatch.setattr(
        SOAK_MONITOR.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _Response(json.dumps(payload).encode()),
    )

    result = SOAK_MONITOR._fetch_status("http://127.0.0.1:8788", "secret")

    assert result["ok"] is False
    assert result["evidence_complete"] is True
    assert result["readiness"]["reasons"] == ["task detector heartbeat stale"]


def test_missing_readiness_fails_closed_instead_of_using_feed_fallback(monkeypatch) -> None:
    payload = _complete_payload("bybit")
    payload.pop("readiness")
    monkeypatch.setattr(
        SOAK_MONITOR.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _Response(json.dumps(payload).encode()),
    )

    result = SOAK_MONITOR._fetch_status("http://127.0.0.1:8787", "secret")

    assert result["ok"] is False
    assert result["evidence_complete"] is False
    assert "readiness section is missing or invalid" in result["evidence_errors"]


def test_feed_age_and_quiet_symbol_age_remain_separate(monkeypatch) -> None:
    payload = _complete_payload("bybit")
    feed = payload["feeds"]["bybit"]
    feed["message_age_seconds"] = 100
    feed["latest_message_age_seconds"] = 0.2
    feed["oldest_symbol_message_age_seconds"] = 100
    feed["symbols"]["BTCUSDT"]["message_age_seconds"] = 0.1
    feed["symbols"]["BNBUSDT"] = {
        "seen_since_connect": True,
        "message_age_seconds": 100,
        "messages": 2,
        "trades": 2,
        "liquidations": 0,
    }
    monkeypatch.setattr(
        SOAK_MONITOR.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _Response(json.dumps(payload).encode()),
    )

    result = SOAK_MONITOR._fetch_status("http://127.0.0.1:8787", "secret")

    assert result["evidence_complete"] is True
    assert result["feeds"]["bybit"]["healthy"] is True
    assert result["feeds"]["bybit"]["message_age_seconds"] == 0.2
    assert result["feeds"]["bybit"]["diagnostic_symbol_age_seconds"] == 100
    assert result["feeds"]["bybit"]["symbols"]["BTCUSDT"]["healthy"] is True
    assert result["feeds"]["bybit"]["symbols"]["BNBUSDT"]["healthy"] is False


def test_missing_expected_task_is_rejected(monkeypatch) -> None:
    payload = _complete_payload()
    payload["tasks"].pop("health-monitor")
    monkeypatch.setattr(
        SOAK_MONITOR.urllib.request,
        "urlopen",
        lambda *_args, **_kwargs: _Response(json.dumps(payload).encode()),
    )

    result = SOAK_MONITOR._fetch_status("http://127.0.0.1:8787", "secret")

    assert result["ok"] is False
    assert result["evidence_complete"] is False
    assert "expected task health-monitor is missing or invalid" in result["evidence_errors"]


def test_docker_absence_is_explicitly_observed(monkeypatch) -> None:
    class _Result:
        stdout = ""

    monkeypatch.setattr(
        SOAK_MONITOR.subprocess,
        "run",
        lambda *_args, **_kwargs: _Result(),
    )

    state = SOAK_MONITOR._docker_state()

    assert state == {"observed": True, "present": False}
