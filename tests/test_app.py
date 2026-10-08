from __future__ import annotations

import asyncio
import time
from pathlib import Path

import pytest

from crypto_sentinel.app import SentinelApp
from crypto_sentinel.dashboard import AlarmDeliveryUnavailable
from crypto_sentinel.models import (
    Liquidation,
    MetricSnapshot,
    ReceivedMarketEvent,
    Severity,
    Trade,
)
from crypto_sentinel.state import MarketState


async def _consume_one(app: SentinelApp, event: ReceivedMarketEvent) -> None:
    consumer = asyncio.create_task(app._consume_events())
    app.queue.put_nowait(event)
    try:
        await asyncio.wait_for(app.queue.join(), timeout=1)
    finally:
        consumer.cancel()
        await asyncio.gather(consumer, return_exceptions=True)


def test_dashboard_reports_notification_permission_state() -> None:
    html = (
        Path(__file__).parents[1] / "src" / "crypto_sentinel" / "static" / "index.html"
    ).read_text(encoding="utf-8")
    assert "Browser notifications enabled" in html
    assert "Browser notifications blocked" in html
    assert "Browser notifications unavailable" in html


async def test_feed_health_realarms_after_recovery(example_config) -> None:
    example_config.exchanges.bybit.enabled = False
    example_config.exchanges.okx.enabled = False
    app = SentinelApp(example_config)
    emitted: list[tuple[str, bool]] = []

    class FakeDispatcher:
        async def emit(self, alert, *, bypass_cooldown=False):
            emitted.append((alert.category, bypass_cooldown))
            return True

    app.dispatcher = FakeDispatcher()  # type: ignore[assignment]

    await app._check_feed_health(1_000_000)
    await app._check_feed_health(1_001_000)
    assert emitted[:2] == [("feed_health", True), ("feed_health", False)]

    app.health.connected("binance")
    for symbol in dict.fromkeys(example_config.symbol_map("binance").values()):
        app.health.message("binance", "trade", symbol)
    await app._check_feed_health(1_002_000)
    assert emitted[-1] == ("feed_recovery", True)

    app.health.disconnected("binance", "test disconnect")
    await app._check_feed_health(1_003_000)
    assert emitted[-1] == ("feed_health", True)


def test_status_keeps_quiet_symbol_informational_when_feed_is_flowing(
    example_config,
) -> None:
    example_config.exchanges.bybit.enabled = False
    example_config.exchanges.okx.enabled = False
    app = SentinelApp(example_config)
    app.health.connected("binance")
    symbols = list(dict.fromkeys(example_config.symbol_map("binance").values()))
    for symbol in symbols[:-1]:
        app.health.message("binance", "trade", symbol)
    for task_name in app._expected_task_names:
        app.health.register_task(task_name, 60)
        app.health.task_heartbeat(task_name, ready=True)

    status = app.status()

    assert status["readiness"]["ready"]
    assert status["feeds"]["binance"]["unhealthy_symbols"] == [symbols[-1]]
    assert symbols[-1] in status["feeds"]["binance"]["unseen_symbols"]
    assert status["feeds"]["binance"]["message_age_seconds"] is None
    assert status["feeds"]["binance"]["latest_message_age_seconds"] is not None

    app.health.message("binance", "trade", symbols[-1])
    app.health._symbols["binance"][symbols[-1]].last_message_monotonic = (
        time.monotonic() - example_config.exchanges.binance.stale_after_seconds - 1
    )
    status = app.status()
    assert status["readiness"]["ready"]
    assert status["readiness"]["reasons"] == []
    assert symbols[-1] in status["feeds"]["binance"]["quiet_symbols"]
    assert (
        status["feeds"]["binance"]["message_age_seconds"]
        > example_config.exchanges.binance.stale_after_seconds
    )
    assert (
        status["feeds"]["binance"]["latest_message_age_seconds"]
        <= example_config.exchanges.binance.stale_after_seconds
    )


def test_reconnect_requires_current_socket_payload_for_readiness(example_config) -> None:
    example_config.exchanges.bybit.enabled = False
    example_config.exchanges.okx.enabled = False
    app = SentinelApp(example_config)
    symbols = list(dict.fromkeys(example_config.symbol_map("binance").values()))
    app.health.connected("binance")
    for symbol in symbols:
        app.health.message("binance", "trade", symbol)
    for task_name in app._expected_task_names:
        app.health.register_task(task_name, 60)
        app.health.task_heartbeat(task_name, ready=True)
    assert app.status()["readiness"]["ready"]

    # A new connection cannot inherit freshness from the old socket.
    app.health.connected("binance")
    reconnected = app.status()
    assert not reconnected["readiness"]["ready"]
    assert "binance: no fresh market payload across feed" in reconnected["readiness"]["reasons"]
    assert reconnected["feeds"]["binance"]["latest_message_age_seconds"] is None

    # Any valid payload proves multiplexed-feed transport liveness. Other
    # symbols remain diagnostic until they are observed on this connection.
    app.health.message("binance", "trade", symbols[0])
    recovered = app.status()
    assert recovered["readiness"]["ready"]
    assert recovered["feeds"]["binance"]["unseen_symbols"] == symbols[1:]


async def test_quiet_symbol_does_not_emit_feed_health_alarm(
    example_config,
) -> None:
    example_config.exchanges.bybit.enabled = False
    example_config.exchanges.okx.enabled = False
    app = SentinelApp(example_config)
    emitted = []

    class FakeDispatcher:
        async def emit(self, alert, *, bypass_cooldown=False):
            emitted.append((alert, bypass_cooldown))
            return True

    app.dispatcher = FakeDispatcher()  # type: ignore[assignment]
    app.health.connected("binance")
    symbols = list(dict.fromkeys(example_config.symbol_map("binance").values()))
    for symbol in symbols:
        app.health.message("binance", "trade", symbol)
    quiet_symbol = symbols[-1]
    app.health._symbols["binance"][quiet_symbol].last_message_monotonic = (
        time.monotonic() - example_config.exchanges.binance.stale_after_seconds - 1
    )
    app.health.message("binance", "trade", symbols[0])

    await app._check_feed_health(1_000_000)

    assert emitted == []


async def test_whole_feed_staleness_still_emits_health_alarm(
    example_config,
) -> None:
    example_config.exchanges.bybit.enabled = False
    example_config.exchanges.okx.enabled = False
    app = SentinelApp(example_config)
    emitted = []

    class FakeDispatcher:
        async def emit(self, alert, *, bypass_cooldown=False):
            emitted.append((alert, bypass_cooldown))
            return True

    app.dispatcher = FakeDispatcher()  # type: ignore[assignment]
    app.health.connected("binance")
    app.health.expect_subscriptions("binance", ["trades"])
    app.health.acknowledge_subscriptions("binance", ["trades"])
    for symbol in dict.fromkeys(example_config.symbol_map("binance").values()):
        app.health.message("binance", "trade", symbol)
    app.health.get("binance").last_message_monotonic = (
        time.monotonic() - example_config.exchanges.binance.stale_after_seconds - 1
    )

    await app._check_feed_health(1_000_000)

    assert len(emitted) == 1
    alert, bypass = emitted[0]
    assert alert.category == "feed_health"
    assert "across the feed" in alert.message
    assert bypass


async def test_critical_task_exit_stops_app(example_config) -> None:
    app = SentinelApp(example_config)

    async def fail() -> None:
        await asyncio.sleep(0)
        raise ValueError("deterministic failure")

    app._tasks = [app._create_critical_task(fail(), name="detector", heartbeat_timeout_seconds=10)]

    with pytest.raises(RuntimeError, match="critical task detector.*ValueError"):
        await app._wait_for_stop_or_task_failure()

    assert app.stop_event.is_set()
    assert app.health.task_snapshot()["detector"]["state"] == "failed"


async def test_alert_worker_exit_is_detected(example_config) -> None:
    app = SentinelApp(example_config)

    async def exit_normally() -> None:
        return None

    async def remain_running() -> None:
        await asyncio.Event().wait()

    exited = asyncio.create_task(exit_normally(), name="notifier-0")
    remote_live = asyncio.create_task(remain_running(), name="notifier-1")
    local_live = asyncio.create_task(remain_running(), name="local-alarm-worker")
    await asyncio.sleep(0)

    class FakeDispatcher:
        def worker_tasks(self):
            return exited, remote_live, local_live

    app.dispatcher = FakeDispatcher()  # type: ignore[assignment]
    try:
        with pytest.raises(RuntimeError, match="alert worker notifier-0 exited unexpectedly"):
            await app._alert_worker_watchdog()
    finally:
        remote_live.cancel()
        local_live.cancel()
        await asyncio.gather(remote_live, local_live, return_exceptions=True)


async def test_checkpoint_failure_degrades_and_success_recovers(
    example_config, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkpoint_path = tmp_path / "state.json.gz"
    example_config.storage.checkpoint = str(checkpoint_path)
    app = SentinelApp(example_config)
    app.health.register_task("state-checkpoint", 60)

    async def fail_save(_state, _now_ms):
        raise OSError("disk unavailable")

    monkeypatch.setattr(app.checkpoint, "save", fail_save)
    assert not await app._save_checkpoint()
    failed = app._checkpoint_status()
    assert not failed["healthy"]
    assert failed["reason"] == "last checkpoint save failed"
    assert failed["last_failure_error"] == "OSError: disk unavailable"
    assert not app.health.task_snapshot()["state-checkpoint"]["ready"]
    assert "checkpoint: last checkpoint save failed" in app.status()["readiness"]["reasons"]

    async def successful_save(_state, _now_ms):
        checkpoint_path.write_bytes(b"checkpoint")
        return True

    monkeypatch.setattr(app.checkpoint, "save", successful_save)
    assert await app._save_checkpoint()
    recovered = app._checkpoint_status()
    assert recovered["healthy"]
    assert recovered["reason"] == "current"
    assert app.health.task_snapshot()["state-checkpoint"]["ready"]

    app._checkpoint_last_success_monotonic = time.monotonic() - recovered["stale_after_seconds"] - 1
    stale = app._checkpoint_status()
    assert not stale["healthy"]
    assert "old" in stale["reason"]


async def test_event_consumer_fails_closed_on_state_exception(
    example_config,
) -> None:
    app = SentinelApp(example_config)

    class BrokenState:
        def record_trade(self, _event):
            raise RuntimeError("state invariant failed")

    app.state = BrokenState()  # type: ignore[assignment]
    app.queue.put_nowait(
        Trade(
            exchange="binance",
            symbol="BTCUSDT",
            timestamp_ms=1_720_000_000_000,
            price=100_000,
            quantity=0.1,
            taker_side="buy",
            event_id="broken-state",
        )
    )

    with pytest.raises(RuntimeError, match="state invariant failed"):
        await app._consume_events()

    await asyncio.wait_for(app.queue.join(), timeout=0.1)


async def test_maintenance_prunes_metrics_and_delivery_receipts(
    example_config,
) -> None:
    app = SentinelApp(example_config)
    calls = []

    class FakeDatabase:
        async def prune_metrics(self, cutoff_ms):
            calls.append(("metrics", cutoff_ms))
            return 7

        async def prune_delivery_receipts(self, cutoff_ms):
            calls.append(("receipts", cutoff_ms))
            return 11

    app.database = FakeDatabase()  # type: ignore[assignment]

    assert await app._prune_storage(123_000) == (7, 11)
    assert calls == [("metrics", 123_000), ("receipts", 123_000)]


async def test_queue_drop_is_alarm_worthy_and_reported(example_config) -> None:
    app = SentinelApp(example_config)
    emitted = []

    class FakeDispatcher:
        async def emit(self, alert, *, bypass_cooldown=False):
            emitted.append((alert, bypass_cooldown))
            return True

        def status(self):
            return {"local_alarm": {"enabled": True}}

    app.dispatcher = FakeDispatcher()  # type: ignore[assignment]
    generation_before_drop = app.health.continuity_generation("binance")
    app.health.dropped("binance", 1, example_config.runtime.queue_size)

    await app._check_runtime_health(1_000_000)

    alert, bypass_cooldown = emitted[-1]
    assert alert.category == "runtime_health"
    assert alert.severity.value == "critical"
    assert bypass_cooldown
    assert app.status()["queue_health"]["drops_since_last_health_check"] == 1
    assert app.health.continuity_generation("binance") == generation_before_drop + 1
    assert app.health.snapshot()["binance"]["continuity_break_active"]


async def test_alarm_api_handlers_reject_failed_required_local_admission(
    example_config,
) -> None:
    app = SentinelApp(example_config)

    class RejectingDispatcher:
        async def emit(self, _alert, *, bypass_cooldown=False):
            return False

    app.dispatcher = RejectingDispatcher()  # type: ignore[assignment]

    with pytest.raises(AlarmDeliveryUnavailable):
        await app.ingest_external(
            {
                "severity": "critical",
                "category": "external_test",
                "symbol": "TEST",
                "title": "External critical test",
                "message": "Admission must fail closed.",
            }
        )
    with pytest.raises(AlarmDeliveryUnavailable):
        await app.test_alert("critical")


async def test_continuity_gap_emits_once_then_recovers(
    example_config,
) -> None:
    app = SentinelApp(example_config)
    emitted = []
    windows = sorted(item.seconds for item in example_config.detector.windows)
    shortest = windows[0]

    class FakeDispatcher:
        async def emit(self, alert, *, bypass_cooldown=False):
            emitted.append((alert, bypass_cooldown))
            return True

        def status(self):
            return {"local_alarm": {"enabled": True}}

    app.dispatcher = FakeDispatcher()  # type: ignore[assignment]
    binance_gap = MetricSnapshot(
        exchange="binance",
        symbol="BTCUSDT",
        timestamp_ms=1_000_000,
        window_seconds=shortest,
        ready=False,
        readiness_reason="recovering_after_gap",
        window_coverage_ratio=0.55,
        largest_gap_seconds=24.0,
        recovery_seconds_remaining=45.0,
    )
    bybit_gap = MetricSnapshot(
        exchange="bybit",
        symbol="ETHUSDT",
        timestamp_ms=1_001_000,
        window_seconds=shortest,
        ready=False,
        readiness_reason="recovering_after_gap",
        window_coverage_ratio=0.7,
        largest_gap_seconds=18.0,
        recovery_seconds_remaining=30.0,
    )

    await app._check_continuity_health([binance_gap], 1_000_000)
    assert len(emitted) == 1
    warning, bypass = emitted[0]
    assert warning.category == "monitoring_gap"
    assert warning.severity is Severity.WARNING
    assert warning.symbol == "SYSTEM"
    assert bypass
    assert warning.metrics["affected_pairs"] == ["binance:BTCUSDT"]
    assert warning.metrics["affected_windows"] == sorted(
        f"binance:BTCUSDT@{window}s" for window in windows
    )
    assert warning.metrics["min_window_coverage_ratio"] == 0.55
    assert warning.metrics["max_largest_gap_seconds"] == 24.0
    assert warning.metrics["max_recovery_seconds_remaining"] == 45.0

    # The active episode absorbs newly affected pairs without another alarm.
    await app._check_continuity_health([binance_gap, bybit_gap], 1_010_000)
    assert len(emitted) == 1
    assert app._continuity_episode_pairs == {
        "binance:BTCUSDT",
        "bybit:ETHUSDT",
    }

    # No-data is neither a new continuity alarm nor a valid recovery.
    await app._check_continuity_health(
        [
            MetricSnapshot(
                exchange="binance",
                symbol="BTCUSDT",
                timestamp_ms=1_020_000,
                window_seconds=shortest,
                ready=False,
                readiness_reason="no_data",
            ),
            MetricSnapshot(
                exchange="bybit",
                symbol="ETHUSDT",
                timestamp_ms=1_020_000,
                window_seconds=shortest,
                ready=False,
                readiness_reason="baseline_warmup",
            ),
        ],
        1_020_000,
    )
    assert len(emitted) == 1

    shortest_ready = [
        MetricSnapshot(
            exchange=exchange,
            symbol=symbol,
            timestamp_ms=1_030_000,
            window_seconds=windows[0],
            ready=True,
            data_age_seconds=0.1,
            readiness_reason="ready",
            window_coverage_ratio=1.0,
        )
        for exchange, symbol in (
            ("binance", "BTCUSDT"),
            ("bybit", "ETHUSDT"),
        )
    ]
    await app._check_continuity_health(shortest_ready, 1_025_000)
    assert len(emitted) == 1

    ready = [
        MetricSnapshot(
            exchange=exchange,
            symbol=symbol,
            timestamp_ms=1_030_000,
            window_seconds=window,
            ready=True,
            data_age_seconds=0.1,
            readiness_reason="ready",
            window_coverage_ratio=1.0,
        )
        for exchange, symbol in (
            ("binance", "BTCUSDT"),
            ("bybit", "ETHUSDT"),
        )
        for window in windows
    ]
    await app._check_continuity_health(ready, 1_030_000)

    assert len(emitted) == 2
    recovery, recovery_bypass = emitted[1]
    assert recovery.category == "monitoring_recovery"
    assert recovery.severity is Severity.INFO
    assert not recovery_bypass
    assert recovery.metrics["affected_pairs"] == [
        "binance:BTCUSDT",
        "bybit:ETHUSDT",
    ]
    assert recovery.metrics["affected_windows"] == sorted(
        f"{exchange}:{symbol}@{window}s"
        for exchange, symbol in (
            ("binance", "BTCUSDT"),
            ("bybit", "ETHUSDT"),
        )
        for window in windows
    )
    assert recovery.metrics["episode_duration_seconds"] == 30.0
    assert app._continuity_episode_started_ms is None


async def test_continuity_ignores_warmup_but_tracks_longer_window(
    example_config,
) -> None:
    app = SentinelApp(example_config)
    emitted = []
    windows = sorted(item.seconds for item in example_config.detector.windows)

    class FakeDispatcher:
        async def emit(self, alert, *, bypass_cooldown=False):
            emitted.append((alert, bypass_cooldown))
            return True

    app.dispatcher = FakeDispatcher()  # type: ignore[assignment]
    await app._check_continuity_health(
        [
            MetricSnapshot(
                exchange="binance",
                symbol="BTCUSDT",
                timestamp_ms=1_000_000,
                window_seconds=windows[0],
                ready=False,
                readiness_reason="baseline_warmup",
            ),
            MetricSnapshot(
                exchange="bybit",
                symbol="BTCUSDT",
                timestamp_ms=1_000_000,
                window_seconds=windows[-1],
                ready=False,
                readiness_reason="recovering_after_gap",
            ),
        ],
        1_000_000,
    )

    assert len(emitted) == 1
    assert emitted[0][0].metrics["affected_pairs"] == ["bybit:BTCUSDT"]
    assert emitted[0][0].metrics["affected_windows"] == sorted(
        f"bybit:BTCUSDT@{window}s" for window in windows
    )
    assert app._continuity_episode_metrics == {f"bybit:BTCUSDT@{window}s" for window in windows}


def test_market_status_exposes_post_gap_recovery_truth(example_config) -> None:
    example_config.exchanges.okx.enabled = False
    app = SentinelApp(example_config)
    symbol = example_config.canonical_symbols()[0]
    windows = sorted(item.seconds for item in example_config.detector.windows)
    app.detector.last_metrics = []
    for exchange in ("binance", "bybit"):
        for window in windows:
            recovering = exchange == "binance" and window == windows[0]
            app.detector.last_metrics.append(
                MetricSnapshot(
                    exchange=exchange,
                    symbol=symbol,
                    timestamp_ms=1_000_000,
                    window_seconds=window,
                    ready=not recovering,
                    data_age_seconds=1.5 if recovering else 0.2,
                    readiness_reason="recovering_after_gap" if recovering else "ready",
                    window_coverage_ratio=0.62 if recovering else 1.0,
                    largest_gap_seconds=18.0 if recovering else 1.0,
                    recovery_seconds_remaining=42.0 if recovering else 0.0,
                )
            )

    market = next(item for item in app.status()["markets"] if item["symbol"] == symbol)

    assert market["readiness_reason"] == "recovering_after_gap"
    assert market["readiness_reasons"] == ["ready", "recovering_after_gap"]
    assert market["max_recovery_seconds_remaining"] == 42.0
    assert market["min_window_coverage_ratio"] == 0.62
    assert market["max_largest_gap_seconds"] == 18.0
    binance = market["venue_readiness"]["binance"]
    assert not binance["ready"]
    assert not binance["base_ready"]
    assert not binance["alert_eligible"]
    assert binance["reason"] == "recovering_after_gap"
    assert binance["data_age_seconds"] == 1.5
    assert binance["window_coverage_ratio"] == 0.62
    assert binance["largest_gap_seconds"] == 18.0
    assert binance["recovery_seconds_remaining"] == 42.0
    assert binance["missing_windows_seconds"] == []
    assert binance["windows"][str(windows[0])]["reason"] == "recovering_after_gap"
    assert binance["windows"][str(windows[-1])]["ready"]


def test_market_status_excludes_stale_base_ready_metric(example_config) -> None:
    example_config.exchanges.bybit.enabled = False
    example_config.exchanges.okx.enabled = False
    app = SentinelApp(example_config)
    symbol = example_config.canonical_symbols()[0]
    windows = sorted(item.seconds for item in example_config.detector.windows)
    app.detector.last_metrics = [
        MetricSnapshot(
            exchange="binance",
            symbol=symbol,
            timestamp_ms=1_000_000,
            window_seconds=window,
            ready=True,
            data_age_seconds=example_config.detector.freshness_seconds + 0.1,
            readiness_reason="ready",
            window_coverage_ratio=1.0,
            largest_gap_seconds=1.0,
        )
        for window in windows
    ]

    status = app.status()
    market = next(item for item in status["markets"] if item["symbol"] == symbol)

    assert status["symbol_freshness_seconds"] == example_config.detector.freshness_seconds
    assert market["ready_venues"] == 0
    assert market["readiness_reason"] == "stale_data"
    assert market["readiness_reasons"] == ["stale_data"]
    binance = market["venue_readiness"]["binance"]
    assert not binance["ready"]
    assert binance["base_ready"]
    assert not binance["alert_eligible"]
    assert binance["reason"] == "stale_data"
    assert binance["data_age_seconds"] == example_config.detector.freshness_seconds + 0.1
    assert binance["window_coverage_ratio"] == 1.0
    assert binance["largest_gap_seconds"] == 1.0
    assert binance["recovery_seconds_remaining"] == 0.0
    assert binance["missing_windows_seconds"] == []


def test_market_status_requires_every_detector_window(example_config) -> None:
    example_config.exchanges.bybit.enabled = False
    example_config.exchanges.okx.enabled = False
    app = SentinelApp(example_config)
    symbol = example_config.canonical_symbols()[0]
    windows = sorted(item.seconds for item in example_config.detector.windows)
    app.detector.last_metrics = [
        MetricSnapshot(
            exchange="binance",
            symbol=symbol,
            timestamp_ms=1_000_000,
            window_seconds=windows[0],
            ready=True,
            data_age_seconds=0.1,
            readiness_reason="ready",
            window_coverage_ratio=1.0,
            largest_gap_seconds=1.0,
        ),
        MetricSnapshot(
            exchange="binance",
            symbol=symbol,
            timestamp_ms=1_000_000,
            window_seconds=windows[-1],
            ready=False,
            data_age_seconds=0.1,
            readiness_reason="recovering_after_gap",
            window_coverage_ratio=0.8,
            largest_gap_seconds=20.0,
            recovery_seconds_remaining=240.0,
        ),
    ]

    market = next(item for item in app.status()["markets"] if item["symbol"] == symbol)

    assert market["ready_venues"] == 0
    assert market["readiness_reason"] == "recovering_after_gap"
    assert not market["venue_readiness"]["binance"]["ready"]
    assert market["venue_readiness"]["binance"]["windows"][str(windows[0])]["ready"]
    assert not market["venue_readiness"]["binance"]["windows"][str(windows[-1])]["ready"]


def test_readiness_degrades_after_unresolved_local_alarm_failure(example_config) -> None:
    app = SentinelApp(example_config)

    class FakeDispatcher:
        outcome = {
            "status": "failure",
            "detail": "speaker unavailable",
            "timestamp_ms": 1_000_000,
        }

        def status(self):
            return {
                "local_alarm": {
                    "enabled": True,
                    "last_outcome": self.outcome,
                }
            }

    dispatcher = FakeDispatcher()
    app.dispatcher = dispatcher  # type: ignore[assignment]

    assert "local alarm: speaker unavailable" in app.status()["readiness"]["reasons"]

    dispatcher.outcome = {
        "status": "success",
        "detail": "",
        "timestamp_ms": 1_001_000,
    }
    assert not any(
        reason.startswith("local alarm:") for reason in app.status()["readiness"]["reasons"]
    )


async def test_disconnect_closes_per_pair_only_on_accepted_current_trade(
    example_config,
) -> None:
    example_config.exchanges.bybit.enabled = False
    example_config.exchanges.okx.enabled = False
    example_config.symbols = example_config.symbols[:2]
    app = SentinelApp(example_config)
    symbols = list(dict.fromkeys(example_config.symbol_map("binance").values()))
    first_symbol, second_symbol = symbols
    base_ms = int(time.time() * 1000) - 5_000
    base_monotonic = time.monotonic()
    duplicate = Trade(
        "binance",
        first_symbol,
        base_ms + 5,
        100,
        1,
        "buy",
        "known-duplicate",
    )
    assert app.state.record_trade(
        duplicate,
        received_wall_ms=base_ms + 5,
        received_monotonic_s=base_monotonic,
    )

    app.health.begin_continuity_break(
        "binance",
        "process_start",
        timestamp_ms=base_ms,
    )
    app._sync_continuity_events()
    generation_one = app.health.continuity_generation("binance")
    app.health.connected("binance", timestamp_ms=base_ms + 1)
    app.health.expect_subscriptions("binance", ["trades"])
    app.health.acknowledge_subscriptions(
        "binance",
        ["trades"],
        timestamp_ms=base_ms + 2,
    )
    assert app.health.snapshot()["binance"]["continuity_break_active"]
    assert all(
        app.state._series[("binance", symbol)].continuity_breaks[-1].end_ms is None
        for symbol in symbols
    )

    await _consume_one(
        app,
        ReceivedMarketEvent(
            duplicate,
            base_ms + 6,
            base_monotonic + 0.006,
            generation_one,
        ),
    )
    await _consume_one(
        app,
        ReceivedMarketEvent(
            Trade(
                "binance",
                first_symbol,
                base_ms + 7,
                -1,
                1,
                "buy",
                "invalid-trade",
            ),
            base_ms + 7,
            base_monotonic + 0.007,
            generation_one,
        ),
    )
    assert app.state._series[("binance", first_symbol)].continuity_breaks[-1].end_ms is None

    # A replay received on the current connection is accepted as historical
    # state but cannot prove post-break continuity because its event time
    # predates the break.
    await _consume_one(
        app,
        ReceivedMarketEvent(
            Trade(
                "binance",
                first_symbol,
                base_ms - 1,
                100,
                1,
                "buy",
                "pre-break-replay",
            ),
            base_ms + 10,
            base_monotonic + 0.01,
            generation_one,
        ),
    )
    assert app.state._series[("binance", first_symbol)].continuity_breaks[-1].end_ms is None

    # Quarantined future trades and valid liquidations cannot restore price
    # continuity.
    await _consume_one(
        app,
        ReceivedMarketEvent(
            Trade(
                "binance",
                first_symbol,
                base_ms + 20_000,
                100,
                1,
                "buy",
                "future-trade",
            ),
            base_ms + 20,
            base_monotonic + 0.02,
            generation_one,
        ),
    )
    await _consume_one(
        app,
        ReceivedMarketEvent(
            Liquidation(
                "binance",
                first_symbol,
                base_ms + 30,
                100,
                1,
                "long",
                "liquidation",
            ),
            base_ms + 30,
            base_monotonic + 0.03,
            generation_one,
        ),
    )
    assert app.state._series[("binance", first_symbol)].continuity_breaks[-1].end_ms is None

    await _consume_one(
        app,
        ReceivedMarketEvent(
            Trade(
                "binance",
                first_symbol,
                base_ms + 100,
                101,
                1,
                "buy",
                "fresh-first-symbol",
            ),
            base_ms + 100,
            base_monotonic + 0.1,
            generation_one,
        ),
    )
    assert (
        app.state._series[("binance", first_symbol)].continuity_breaks[-1].end_ms == base_ms + 100
    )
    assert app.state._series[("binance", second_symbol)].continuity_breaks[-1].end_ms is None

    # A second disconnect after partial recovery must reopen every symbol and
    # invalidate all queued events from the previous connection generation.
    app.health.disconnected(
        "binance",
        "second disconnect",
        timestamp_ms=base_ms + 200,
    )
    app._sync_continuity_events()
    generation_two = app.health.continuity_generation("binance")
    assert generation_two == generation_one + 1
    assert app.state._series[("binance", first_symbol)].continuity_breaks[-1].end_ms is None

    await _consume_one(
        app,
        ReceivedMarketEvent(
            Trade(
                "binance",
                second_symbol,
                base_ms + 250,
                102,
                1,
                "buy",
                "queued-old-generation",
            ),
            base_ms + 250,
            base_monotonic + 0.25,
            generation_one,
        ),
    )
    assert app.state._series[("binance", second_symbol)].continuity_breaks[-1].end_ms is None

    for offset_ms, symbol in ((300, first_symbol), (350, second_symbol)):
        await _consume_one(
            app,
            ReceivedMarketEvent(
                Trade(
                    "binance",
                    symbol,
                    base_ms + offset_ms,
                    103,
                    1,
                    "buy",
                    f"fresh-generation-two:{symbol}",
                ),
                base_ms + offset_ms,
                base_monotonic + offset_ms / 1000,
                generation_two,
            ),
        )

    assert not app.health.snapshot()["binance"]["continuity_break_active"]
    assert all(
        app.state._series[("binance", symbol)].continuity_breaks[-1].end_ms is not None
        for symbol in symbols
    )


async def test_checkpoint_restart_merges_process_gap_until_current_trade(
    example_config,
    tmp_path: Path,
) -> None:
    example_config.exchanges.bybit.enabled = False
    example_config.exchanges.okx.enabled = False
    example_config.symbols = example_config.symbols[:1]
    example_config.storage.checkpoint = str(tmp_path / "restart-state.json.gz")
    app = SentinelApp(example_config)
    symbol = example_config.symbols[0].canonical
    saved_ms = int(time.time() * 1000) - 10_000
    load_ms = saved_ms + 5_000
    base_monotonic = time.monotonic()
    original = MarketState(
        example_config.detector.bucket_seconds,
        app.state.retention_seconds,
    )
    assert original.record_trade(
        Trade(
            "binance",
            symbol,
            saved_ms - 1_000,
            100,
            1,
            "buy",
            "checkpoint-seed",
        )
    )
    assert await app.checkpoint.save(original, saved_ms)
    assert await app.checkpoint.load(
        app.state,
        load_ms,
        {("binance", symbol)},
    )

    app.health.begin_continuity_break(
        "binance",
        "process_start",
        timestamp_ms=load_ms,
    )
    app._sync_continuity_events()
    generation = app.health.continuity_generation("binance")
    gap = app.state._series[("binance", symbol)].continuity_breaks[-1]
    assert gap.start_ms == saved_ms
    assert gap.end_ms is None
    assert gap.reason == "checkpoint_downtime,process_start"

    # A queued event from before the process-start generation can update state
    # but cannot close restored downtime.
    await _consume_one(
        app,
        ReceivedMarketEvent(
            Trade(
                "binance",
                symbol,
                load_ms + 100,
                101,
                1,
                "buy",
                "old-process-generation",
            ),
            load_ms + 100,
            base_monotonic + 0.1,
            generation - 1,
        ),
    )
    assert app.state._series[("binance", symbol)].continuity_breaks[-1].end_ms is None

    await _consume_one(
        app,
        ReceivedMarketEvent(
            Trade(
                "binance",
                symbol,
                load_ms + 200,
                102,
                1,
                "buy",
                "first-current-process-trade",
            ),
            load_ms + 200,
            base_monotonic + 0.2,
            generation,
        ),
    )
    closed_gap = app.state._series[("binance", symbol)].continuity_breaks[-1]
    assert closed_gap.start_ms == saved_ms
    assert closed_gap.end_ms == load_ms + 200


def test_runtime_pause_records_only_interruptions_above_tolerance(
    example_config,
) -> None:
    example_config.exchanges.bybit.enabled = False
    example_config.exchanges.okx.enabled = False
    app = SentinelApp(example_config)
    base_ms = int(time.time() * 1000) - 1_000
    app.state.record_trade(Trade("binance", "BTCUSDT", base_ms, 100, 1, "buy", "seed"))
    interval_ms = example_config.detector.evaluate_every_seconds * 1000
    tolerance_ms = example_config.detector.maximum_data_gap_seconds * 1000

    app._record_runtime_pause(
        base_ms,
        100,
        base_ms + interval_ms + tolerance_ms,
        100 + (interval_ms + tolerance_ms) / 1000,
    )
    assert not app.state._series[("binance", "BTCUSDT")].continuity_breaks

    app._record_runtime_pause(
        base_ms,
        100,
        base_ms + interval_ms + tolerance_ms + 1,
        100 + (interval_ms + tolerance_ms + 1) / 1000,
    )
    gap = app.state._series[("binance", "BTCUSDT")].continuity_breaks[-1]
    assert gap.start_ms == base_ms + interval_ms
    assert gap.end_ms is None
    assert gap.reason == "runtime_pause"
