from __future__ import annotations

import math
import random

import pytest

from crypto_sentinel.detector import AnomalyDetector
from crypto_sentinel.models import Liquidation, Trade
from crypto_sentinel.state import MarketSeries, MarketState


def _seed_ready_series(
    state: MarketState,
    now_ms: int,
    *,
    start_seconds: int = 400,
    end_seconds: int = 0,
    exchange: str = "binance",
    price_multiplier: float = 1.0,
) -> None:
    for index, seconds_ago in enumerate(range(start_seconds, end_seconds - 1, -5)):
        timestamp = now_ms - seconds_ago * 1000
        price = (100 + math.sin(index / 3) * 0.2 + index * 0.002) * price_multiplier
        quantity = 10 + index % 5
        assert state.record_trade(
            Trade(
                exchange,
                "BTCUSDT",
                timestamp,
                price,
                quantity,
                "buy" if index % 2 else "sell",
                f"seed:{exchange}:{timestamp}",
            )
        )


def test_state_calculates_ready_anomaly_metrics() -> None:
    random.seed(4)
    state = MarketState(bucket_seconds=5, retention_seconds=4000)
    now_ms = 1_800_000_000_000
    price = 100_000.0
    start = now_ms - 3_700_000
    timestamp = start
    while timestamp < now_ms - 65_000:
        price *= 1 + random.gauss(0, 0.00003)
        quote_volume = random.gauss(300_000, 40_000)
        state.record_trade(
            Trade(
                exchange="binance",
                symbol="BTCUSDT",
                timestamp_ms=timestamp,
                price=price,
                quantity=quote_volume / price,
                taker_side="buy" if random.random() > 0.5 else "sell",
                event_id=f"base:{timestamp}",
            )
        )
        timestamp += 5_000

    for step in range(13):
        timestamp = now_ms - 60_000 + step * 5_000
        price *= 1 - 0.001
        state.record_trade(
            Trade(
                exchange="binance",
                symbol="BTCUSDT",
                timestamp_ms=timestamp,
                price=price,
                quantity=2_000_000 / price,
                taker_side="sell",
                event_id=f"shock:{timestamp}",
            )
        )

    metric = state.metric("binance", "BTCUSDT", 60, 3600, 12, now_ms)
    assert metric.ready
    assert metric.return_bps is not None and metric.return_bps < -100
    assert metric.return_z is not None and metric.return_z < -4
    assert metric.volume_z is not None and metric.volume_z > 4
    assert metric.taker_imbalance is not None and metric.taker_imbalance < -0.9


def test_liquidation_snapshot_deduplicates_events() -> None:
    state = MarketState(bucket_seconds=5, retention_seconds=600)
    now_ms = 1_800_000_000_000
    event = Liquidation(
        exchange="binance",
        symbol="BTCUSDT",
        timestamp_ms=now_ms - 1000,
        price=100_000,
        quantity=10,
        liquidated_side="long",
        event_id="same",
    )
    assert state.record_liquidation(event)
    assert not state.record_liquidation(event)
    snapshot = state.liquidation_snapshot("BTCUSDT", 60, now_ms)
    assert snapshot.total_usd == 1_000_000
    assert snapshot.events == 1


def test_out_of_order_liquidations_remain_time_sorted() -> None:
    state = MarketState(bucket_seconds=5, retention_seconds=600)
    now_ms = 1_800_000_000_000
    newer = Liquidation("bybit", "BTCUSDT", now_ms - 1_000, 100, 1, "long", "new")
    older = Liquidation("bybit", "BTCUSDT", now_ms - 5_000, 100, 2, "long", "old")
    state.record_liquidation(newer)
    state.record_liquidation(older)
    queue = state._liquidations[("bybit", "BTCUSDT")]
    assert [item.event_id for item in queue] == ["old", "new"]
    assert state.liquidation_snapshot("BTCUSDT", 60, now_ms).events == 2


def test_liquidation_snapshot_excludes_venue_with_overlapping_continuity_gap() -> None:
    state = MarketState(bucket_seconds=5, retention_seconds=600)
    now_ms = 1_800_000_000_000
    for exchange in ("binance", "bybit"):
        assert state.record_trade(
            Trade(
                exchange,
                "BTCUSDT",
                now_ms - 1_000,
                100,
                1,
                "buy",
                f"trade:{exchange}",
            )
        )
        assert state.record_liquidation(
            Liquidation(
                exchange,
                "BTCUSDT",
                now_ms - 1_000,
                100,
                10 if exchange == "binance" else 5,
                "long",
                f"liquidation:{exchange}",
            )
        )

    state.record_continuity_break(
        "binance",
        "BTCUSDT",
        now_ms - 30_000,
        now_ms - 5_000,
        "websocket_closed",
    )

    snapshot = state.liquidation_snapshot("BTCUSDT", 60, now_ms)

    assert snapshot.exchanges == {"bybit": 500.0}
    assert snapshot.total_usd == 500.0
    assert snapshot.long_usd == 500.0
    assert snapshot.events == 1


def test_out_of_order_trade_is_inserted_without_corrupting_close() -> None:
    state = MarketState(bucket_seconds=5, retention_seconds=120)
    base_ms = 1_800_000_000_000
    state.record_trade(Trade("binance", "BTCUSDT", base_ms + 12_000, 103.0, 1.0, "buy", "newest"))
    state.record_trade(
        Trade("binance", "BTCUSDT", base_ms + 2_000, 100.0, 1.0, "buy", "old-bucket")
    )
    state.record_trade(
        Trade("binance", "BTCUSDT", base_ms + 10_500, 101.0, 1.0, "sell", "late-same")
    )

    series = state._series[("binance", "BTCUSDT")]
    assert [bucket.start_ms for bucket in series.buckets] == [base_ms, base_ms + 10_000]
    assert series.buckets[-1].open == 101.0
    assert series.buckets[-1].close == 103.0
    assert series.buckets[-1].trades == 2


def test_trade_dedup_cache_is_bounded() -> None:
    state = MarketState(bucket_seconds=5, retention_seconds=10_000)
    series = state._series.setdefault(
        ("binance", "BTCUSDT"),
        MarketSeries(5, 10_000),
    )
    series.max_seen_ids = 100
    base_ms = 1_800_000_000_000
    for index in range(250):
        assert series.add_trade(
            Trade(
                "binance",
                "BTCUSDT",
                base_ms + index,
                100.0,
                1.0,
                "buy",
                f"trade-{index}",
            )
        )
    assert len(series._seen_ids) <= 100
    assert len(series._seen_set) <= 100


def test_invalid_nonfinite_events_are_rejected() -> None:
    state = MarketState(bucket_seconds=5, retention_seconds=600)
    assert not state.record_trade(
        Trade("binance", "BTCUSDT", 1_800_000_000_000, math.inf, 1, "buy", "bad")
    )
    assert not state.record_liquidation(
        Liquidation("bybit", "BTCUSDT", 1_800_000_000_000, 100, -1, "long", "bad")
    )


def test_post_outage_jump_remains_in_recovery_and_cannot_alert(example_config) -> None:
    state = MarketState(bucket_seconds=5, retention_seconds=4000)
    now_ms = 1_800_000_000_000
    price = 100_000.0
    timestamp = now_ms - 3_700_000
    while timestamp <= now_ms - 120_000:
        state.record_trade(
            Trade(
                "binance",
                "BTCUSDT",
                timestamp,
                price,
                5.0,
                "buy",
                f"base:{timestamp}",
            )
        )
        timestamp += 5_000

    # A known feed outage lasts 90 seconds and resumes at a very different
    # price. The move did not occur inside a continuously observed interval.
    state.record_continuity_break(
        "binance",
        "BTCUSDT",
        now_ms - 120_000,
        now_ms - 30_000,
        "feed_disconnect",
    )
    price *= 0.90
    for timestamp in range(now_ms - 30_000, now_ms + 1, 5_000):
        state.record_trade(
            Trade(
                "binance",
                "BTCUSDT",
                timestamp,
                price,
                25.0,
                "sell",
                f"resume:{timestamp}",
            )
        )

    metric = state.metric(
        "binance",
        "BTCUSDT",
        60,
        3600,
        12,
        now_ms,
        maximum_data_gap_seconds=15,
        minimum_window_coverage=0.8,
    )
    assert not metric.ready
    assert metric.readiness_reason == "recovering_after_gap"
    assert metric.window_coverage_ratio < 0.8
    assert metric.largest_gap_seconds is not None and metric.largest_gap_seconds >= 90
    assert metric.recovery_seconds_remaining == 30
    assert metric.return_bps is not None and metric.return_bps < -900

    detector = AnomalyDetector(example_config, state)
    alerts, _ = detector.evaluate(now_ms)
    assert not any(alert.category in {"market_anomaly", "market_shock"} for alert in alerts)


def test_future_events_are_quarantined_and_liveness_uses_monotonic_time() -> None:
    wall_ms = 1_800_000_000_000
    clocks = {"wall": wall_ms, "monotonic": 100.0}
    state = MarketState(
        bucket_seconds=5,
        retention_seconds=600,
        max_future_skew_seconds=5,
        wall_clock_ms=lambda: int(clocks["wall"]),
        monotonic_clock=lambda: clocks["monotonic"],
    )

    assert not state.record_trade(
        Trade("binance", "BTCUSDT", wall_ms + 5_001, 100, 1, "buy", "future")
    )
    assert not state.record_liquidation(
        Liquidation("bybit", "BTCUSDT", wall_ms + 5_001, 100, 1, "long", "future")
    )
    assert state.record_trade(
        Trade("binance", "BTCUSDT", wall_ms + 3_000, 100, 1, "buy", "normalized")
    )
    assert state.record_trade(Trade("binance", "BTCUSDT", wall_ms, 100, 1, "buy", "valid"))

    # A wall-clock rollback must neither produce negative age nor make live data immortal.
    clocks["wall"] = wall_ms - 60_000
    clocks["monotonic"] = 110.0
    assert state.record_trade(
        Trade("binance", "BTCUSDT", wall_ms - 1_000, 99, 1, "sell", "late-replay")
    )
    metric = state.metric("binance", "BTCUSDT", 60, 300, 5)
    assert metric.timestamp_ms == wall_ms + 10_000
    assert metric.data_age_seconds == 10
    assert state.latest_prices("BTCUSDT", 5, int(clocks["wall"])) == {}
    status = state.clock_status()
    assert status["future_events_quarantined"] == 2
    assert status["future_events_normalized"] == 1
    assert status["clock_adjustments"] >= 1


def test_forward_wall_jump_reanchors_and_enters_gap_recovery() -> None:
    wall_ms = 1_800_000_000_000
    clocks = {"wall": wall_ms, "monotonic": 100.0}
    state = MarketState(
        bucket_seconds=5,
        retention_seconds=600,
        max_future_skew_seconds=5,
        wall_clock_ms=lambda: int(clocks["wall"]),
        monotonic_clock=lambda: clocks["monotonic"],
    )
    assert state.record_trade(Trade("binance", "BTCUSDT", wall_ms, 100, 1, "buy", "before-suspend"))

    clocks["wall"] = wall_ms + 60_000
    clocks["monotonic"] = 110.0
    assert state.record_trade(
        Trade(
            "binance",
            "BTCUSDT",
            wall_ms + 60_000,
            90,
            1,
            "sell",
            "after-suspend",
        )
    )
    metric = state.metric(
        "binance",
        "BTCUSDT",
        60,
        300,
        5,
        maximum_data_gap_seconds=15,
    )
    assert metric.timestamp_ms == wall_ms + 60_000
    assert metric.readiness_reason == "recovering_after_gap"
    assert state.clock_status()["clock_forward_reanchors"] == 1


def test_gap_recovery_uses_latest_relevant_gap_not_only_largest() -> None:
    state = MarketState(bucket_seconds=5, retention_seconds=300)
    now_ms = 1_800_000_000_000
    timestamps = (
        now_ms - 60_000,
        now_ms - 30_000,  # Largest gap ends here.
        now_ms - 25_000,
        now_ms - 10_000,  # Later, smaller disqualifying gap ends here.
        now_ms - 5_000,
        now_ms,
    )
    for index, timestamp in enumerate(timestamps):
        assert state.record_trade(
            Trade(
                "binance",
                "BTCUSDT",
                timestamp,
                100 + index,
                1,
                "buy",
                f"gap-{index}",
            )
        )

    state.record_continuity_break(
        "binance",
        "BTCUSDT",
        now_ms - 60_000,
        now_ms - 30_000,
        "first_disconnect",
    )
    state.record_continuity_break(
        "binance",
        "BTCUSDT",
        now_ms - 25_000,
        now_ms - 10_000,
        "second_disconnect",
    )

    metric = state.metric(
        "binance",
        "BTCUSDT",
        60,
        300,
        5,
        now_ms,
        maximum_data_gap_seconds=10,
        minimum_window_coverage=0.8,
    )
    assert metric.readiness_reason == "recovering_after_gap"
    assert metric.largest_gap_seconds == 30
    assert metric.recovery_seconds_remaining == 50


def test_normal_quiet_market_is_not_inferred_to_be_an_outage() -> None:
    state = MarketState(bucket_seconds=5, retention_seconds=600)
    now_ms = 1_800_000_000_000
    _seed_ready_series(state, now_ms, end_seconds=25)

    metric = state.metric(
        "binance",
        "BTCUSDT",
        60,
        300,
        5,
        now_ms,
        maximum_data_gap_seconds=15,
        minimum_window_coverage=0.8,
    )

    assert metric.ready
    assert metric.readiness_reason == "ready"
    assert metric.data_age_seconds == 25
    assert metric.window_coverage_ratio == 1.0
    assert metric.largest_gap_seconds == 0


def test_explicit_gap_contract_tolerates_short_closed_but_never_open() -> None:
    now_ms = 1_800_000_000_000
    tolerated = MarketState(bucket_seconds=5, retention_seconds=600)
    _seed_ready_series(tolerated, now_ms)
    tolerated.record_continuity_break(
        "binance",
        "BTCUSDT",
        now_ms - 40_000,
        now_ms - 30_000,
        "brief_reconnect",
    )

    tolerated_metric = tolerated.metric(
        "binance",
        "BTCUSDT",
        60,
        300,
        5,
        now_ms,
        maximum_data_gap_seconds=15,
        minimum_window_coverage=0.8,
    )
    assert tolerated_metric.ready
    assert tolerated_metric.window_coverage_ratio == pytest.approx(5 / 6)
    assert tolerated_metric.largest_gap_seconds == 10

    active = MarketState(bucket_seconds=5, retention_seconds=600)
    _seed_ready_series(active, now_ms)
    active.begin_continuity_break(
        "binance",
        "BTCUSDT",
        now_ms,
        "active_disconnect",
    )
    active_metric = active.metric(
        "binance",
        "BTCUSDT",
        60,
        300,
        5,
        now_ms,
        maximum_data_gap_seconds=15,
        minimum_window_coverage=0.8,
    )
    assert not active_metric.ready
    assert active_metric.readiness_reason == "recovering_after_gap"


def test_explicit_gap_contract_applies_duration_and_aggregate_coverage() -> None:
    now_ms = 1_800_000_000_000
    oversized = MarketState(bucket_seconds=5, retention_seconds=600)
    _seed_ready_series(oversized, now_ms)
    oversized.record_continuity_break(
        "binance",
        "BTCUSDT",
        now_ms - 50_000,
        now_ms - 34_000,
        "oversized_disconnect",
    )
    oversized_metric = oversized.metric(
        "binance",
        "BTCUSDT",
        60,
        300,
        5,
        now_ms,
        maximum_data_gap_seconds=15,
        minimum_window_coverage=0.5,
    )
    assert oversized_metric.readiness_reason == "recovering_after_gap"

    cumulative = MarketState(bucket_seconds=5, retention_seconds=600)
    _seed_ready_series(cumulative, now_ms)
    for start_seconds, end_seconds in ((55, 45), (35, 25)):
        cumulative.record_continuity_break(
            "binance",
            "BTCUSDT",
            now_ms - start_seconds * 1000,
            now_ms - end_seconds * 1000,
            "brief_disconnect",
        )
    cumulative_metric = cumulative.metric(
        "binance",
        "BTCUSDT",
        60,
        300,
        5,
        now_ms,
        maximum_data_gap_seconds=15,
        minimum_window_coverage=0.8,
    )
    assert cumulative_metric.largest_gap_seconds == 10
    assert cumulative_metric.window_coverage_ratio == pytest.approx(2 / 3)
    assert cumulative_metric.readiness_reason == "recovering_after_gap"


def test_break_crossed_by_selected_start_price_is_not_missed() -> None:
    now_ms = 1_800_000_000_000
    state = MarketState(bucket_seconds=5, retention_seconds=600)
    _seed_ready_series(state, now_ms)
    # There is no trade exactly at the nominal -60s boundary, so the selected
    # start price comes from -65s. This break lies before the nominal window
    # but is still crossed by the calculated return.
    series = state._series[("binance", "BTCUSDT")]
    series.buckets = type(series.buckets)(
        bucket for bucket in series.buckets if bucket.last_trade_ms != now_ms - 60_000
    )
    state.record_continuity_break(
        "binance",
        "BTCUSDT",
        now_ms - 64_000,
        now_ms - 61_000,
        "pre_window_disconnect",
    )

    metric = state.metric(
        "binance",
        "BTCUSDT",
        60,
        300,
        5,
        now_ms,
        maximum_data_gap_seconds=2,
        minimum_window_coverage=0.8,
    )

    assert metric.window_coverage_ratio == 1.0
    assert metric.readiness_reason == "recovering_after_gap"


def test_overlapping_continuity_breaks_are_unioned_and_reasons_retained() -> None:
    now_ms = 1_800_000_000_000
    state = MarketState(bucket_seconds=5, retention_seconds=600)
    _seed_ready_series(state, now_ms)
    state.record_continuity_break(
        "binance",
        "BTCUSDT",
        now_ms - 50_000,
        now_ms - 30_000,
        "feed_disconnect",
    )
    state.record_continuity_break(
        "binance",
        "BTCUSDT",
        now_ms - 40_000,
        now_ms - 20_000,
        "runtime_pause",
    )

    metric = state.metric(
        "binance",
        "BTCUSDT",
        60,
        300,
        5,
        now_ms,
        maximum_data_gap_seconds=40,
        minimum_window_coverage=0.5,
    )
    series = state._series[("binance", "BTCUSDT")]

    assert metric.window_coverage_ratio == 0.5
    assert metric.largest_gap_seconds == 30
    assert len(series.continuity_breaks) == 1
    assert series.continuity_breaks[0].reason == "feed_disconnect,runtime_pause"


def test_spread_alarm_excludes_venues_without_continuous_ready_price(
    example_config,
) -> None:
    now_ms = 1_800_000_000_000
    state = MarketState(bucket_seconds=5, retention_seconds=600)
    _seed_ready_series(
        state,
        now_ms,
        exchange="binance",
        price_multiplier=1.02,
    )
    _seed_ready_series(state, now_ms, exchange="bybit")
    _seed_ready_series(
        state,
        now_ms,
        exchange="okx",
        price_multiplier=1.0001,
    )
    state.begin_continuity_break(
        "binance",
        "BTCUSDT",
        now_ms,
        "feed_disconnect",
    )

    alerts, metrics = AnomalyDetector(example_config, state).evaluate(now_ms)

    assert any(
        metric.exchange == "binance" and metric.readiness_reason == "recovering_after_gap"
        for metric in metrics
    )
    assert not any(
        alert.category == "cross_exchange_spread" and alert.symbol == "BTCUSDT" for alert in alerts
    )
