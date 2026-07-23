from __future__ import annotations

import math
import random

from crypto_sentinel.models import Liquidation, Trade
from crypto_sentinel.state import MarketSeries, MarketState


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
