from __future__ import annotations

from crypto_sentinel.checkpoint import StateCheckpoint
from crypto_sentinel.models import Liquidation, Trade
from crypto_sentinel.state import MarketState


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
    snapshot = restored.liquidation_snapshot("BTCUSDT", 60, now_ms + 1_000)
    assert snapshot.total_usd == 200_038


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
