from __future__ import annotations

import math

import pytest

from crypto_sentinel.checkpoint import StateCheckpoint
from crypto_sentinel.models import Liquidation, Trade
from crypto_sentinel.state import MarketState


def _seed_checkpoint_baseline(state: MarketState, now_ms: int) -> None:
    for index, seconds_ago in enumerate(range(400, -1, -5)):
        timestamp_ms = now_ms - seconds_ago * 1000
        state.record_trade(
            Trade(
                "binance",
                "BTCUSDT",
                timestamp_ms,
                100 + math.sin(index / 4) * 0.2 + index * 0.002,
                10 + index % 3,
                "buy" if index % 2 else "sell",
                f"checkpoint:{timestamp_ms}",
            )
        )


async def test_checkpoint_roundtrip_restores_buckets_and_liquidations(tmp_path) -> None:
    now_ms = 1_800_000_000_000
    original = MarketState(bucket_seconds=5, retention_seconds=600)
    for index in range(20):
        original.record_trade(
            Trade(
                exchange="binance",
                symbol="BTCUSDT",
                timestamp_ms=now_ms - 100_000 + index * 5_000,
                price=100_000 + index,
                quantity=0.1,
                taker_side="buy" if index % 2 else "sell",
                event_id=f"trade-{index}",
            )
        )
    original.record_liquidation(
        Liquidation(
            exchange="binance",
            symbol="BTCUSDT",
            timestamp_ms=now_ms - 2_000,
            price=100_019,
            quantity=2,
            liquidated_side="long",
            event_id="liq-1",
        )
    )

    checkpoint = StateCheckpoint(str(tmp_path / "state.json.gz"), max_age_seconds=3600)
    assert await checkpoint.save(original, now_ms)

    restored = MarketState(bucket_seconds=5, retention_seconds=600)
    result = await checkpoint.load(
        restored,
        now_ms + 1_000,
        {("binance", "BTCUSDT")},
    )

    assert result is not None
    assert result["buckets"] == 20
    assert result["liquidations"] == 1
    assert restored.latest_prices("BTCUSDT", 120, now_ms + 1_000)["binance"] == 100_019
    assert len(restored._liquidations[("binance", "BTCUSDT")]) == 1
    snapshot = restored.liquidation_snapshot("BTCUSDT", 60, now_ms + 1_000)
    # The event is restored for audit/state continuity, but the checkpoint
    # downtime overlaps this alarm window. It must not count as burst evidence.
    assert snapshot.total_usd == 0
    assert snapshot.events == 0


async def test_checkpoint_ignores_stale_file(tmp_path) -> None:
    now_ms = 1_800_000_000_000
    original = MarketState(bucket_seconds=5, retention_seconds=600)
    original.record_trade(Trade("binance", "BTCUSDT", now_ms, 100_000, 1, "buy", "trade"))
    checkpoint = StateCheckpoint(str(tmp_path / "state.json.gz"), max_age_seconds=300)
    await checkpoint.save(original, now_ms)

    restored = MarketState(bucket_seconds=5, retention_seconds=600)
    result = await checkpoint.load(
        restored,
        now_ms + 301_000,
        {("binance", "BTCUSDT")},
    )
    assert result is None
    assert restored.latest_prices("BTCUSDT", 600, now_ms + 301_000) == {}


async def test_checkpoint_downtime_stays_open_until_first_accepted_trade(
    tmp_path,
) -> None:
    saved_ms = 1_800_000_000_000
    original = MarketState(bucket_seconds=5, retention_seconds=600)
    _seed_checkpoint_baseline(original, saved_ms)
    checkpoint = StateCheckpoint(str(tmp_path / "state.json.gz"), max_age_seconds=3600)
    assert await checkpoint.save(original, saved_ms)

    load_ms = saved_ms + 20_000
    restored = MarketState(bucket_seconds=5, retention_seconds=600)
    assert await checkpoint.load(
        restored,
        load_ms,
        {("binance", "BTCUSDT")},
    )
    before_trade = restored.metric(
        "binance",
        "BTCUSDT",
        60,
        300,
        5,
        load_ms,
        maximum_data_gap_seconds=15,
        minimum_window_coverage=0.8,
    )
    assert before_trade.readiness_reason == "recovering_after_gap"
    assert restored._series[("binance", "BTCUSDT")].continuity_breaks[-1].end_ms is None

    first_trade_ms = load_ms + 5_000
    assert restored.record_trade(
        Trade(
            "binance",
            "BTCUSDT",
            first_trade_ms,
            101,
            10,
            "buy",
            "first-live-trade",
        ),
        received_wall_ms=first_trade_ms,
        received_monotonic_s=100,
    )
    restored.end_continuity_break("binance", "BTCUSDT", first_trade_ms)
    still_recovering = restored.metric(
        "binance",
        "BTCUSDT",
        60,
        300,
        5,
        first_trade_ms,
        maximum_data_gap_seconds=15,
        minimum_window_coverage=0.8,
        now_monotonic_s=100,
    )
    assert still_recovering.readiness_reason == "recovering_after_gap"
    assert still_recovering.recovery_seconds_remaining == 60

    recovered_ms = first_trade_ms + 60_000
    for timestamp_ms in range(first_trade_ms + 5_000, recovered_ms + 1, 5_000):
        assert restored.record_trade(
            Trade(
                "binance",
                "BTCUSDT",
                timestamp_ms,
                101 + (timestamp_ms % 15_000) / 100_000,
                10,
                "buy",
                f"recovered:{timestamp_ms}",
            ),
            received_wall_ms=timestamp_ms,
            received_monotonic_s=100 + (timestamp_ms - first_trade_ms) / 1000,
        )
    recovered = restored.metric(
        "binance",
        "BTCUSDT",
        60,
        300,
        5,
        recovered_ms,
        maximum_data_gap_seconds=15,
        minimum_window_coverage=0.8,
        now_monotonic_s=160,
    )
    assert recovered.readiness_reason == "ready"


def test_legacy_v1_checkpoint_is_rejected_instead_of_losing_gap_truth() -> None:
    now_ms = 1_800_000_000_000
    original = MarketState(bucket_seconds=5, retention_seconds=600)
    _seed_checkpoint_baseline(original, now_ms)
    legacy_payload = original.export_checkpoint(now_ms)
    legacy_payload["version"] = 1
    for row in legacy_payload["series"]:
        row.pop("continuity_breaks", None)

    restored = MarketState(bucket_seconds=5, retention_seconds=600)
    with pytest.raises(ValueError, match="unsupported checkpoint version"):
        restored.restore_checkpoint(
            legacy_payload,
            now_ms,
            {("binance", "BTCUSDT")},
        )


def test_open_and_closed_breaks_survive_checkpoint_roundtrip() -> None:
    now_ms = 1_800_000_000_000
    original = MarketState(bucket_seconds=5, retention_seconds=600)
    _seed_checkpoint_baseline(original, now_ms)
    original.record_continuity_break(
        "binance",
        "BTCUSDT",
        now_ms - 50_000,
        now_ms - 40_000,
        "closed_disconnect",
    )
    original.begin_continuity_break(
        "binance",
        "BTCUSDT",
        now_ms - 10_000,
        "open_disconnect",
    )

    restored = MarketState(bucket_seconds=5, retention_seconds=600)
    restored.restore_checkpoint(
        original.export_checkpoint(now_ms),
        now_ms,
        {("binance", "BTCUSDT")},
    )
    breaks = list(restored._series[("binance", "BTCUSDT")].continuity_breaks)

    assert [(item.start_ms, item.end_ms, item.reason) for item in breaks] == [
        (now_ms - 50_000, now_ms - 40_000, "closed_disconnect"),
        (now_ms - 10_000, None, "open_disconnect"),
    ]
