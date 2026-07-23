from __future__ import annotations

import random
import time
from dataclasses import dataclass

from crypto_sentinel.config import AppConfig
from crypto_sentinel.detector import AnomalyDetector
from crypto_sentinel.models import Alert, Liquidation, Trade
from crypto_sentinel.state import MarketState


@dataclass(slots=True)
class SimulationResult:
    alerts: list[Alert]
    trades_generated: int
    liquidations_generated: int
    now_ms: int


async def run_simulation(config: AppConfig) -> SimulationResult:
    """Create deterministic baseline data followed by a multi-exchange downside shock."""
    rng = random.Random(7)
    detector_config = config.detector
    retention = (
        detector_config.baseline_seconds + max(item.seconds for item in detector_config.windows) * 3
    )
    state = MarketState(detector_config.bucket_seconds, retention)
    detector = AnomalyDetector(config, state)
    now_ms = int(time.time() * 1000)
    step_ms = detector_config.bucket_seconds * 1000
    baseline_ms = detector_config.baseline_seconds * 1000
    start_ms = now_ms - baseline_ms - 120_000
    exchanges = [
        name
        for name in ("binance", "bybit", "okx")
        if getattr(config.exchanges, name).enabled and config.symbol_map(name)
    ]
    if len(exchanges) < 2:
        exchanges = ["binance", "bybit"]

    venue_basis = {exchange: index * 0.00004 for index, exchange in enumerate(exchanges)}
    reference_price = 100_000.0
    prices = {exchange: reference_price * (1 + venue_basis[exchange]) for exchange in exchanges}
    trades = 0
    timestamp = start_ms
    while timestamp < now_ms - 65_000:
        # Keep venues tightly aligned so this focused simulation exercises the
        # market-shock path without also manufacturing a spread-divergence alert.
        reference_price *= 1 + rng.gauss(0, 0.000035)
        for exchange in exchanges:
            prices[exchange] = reference_price * (1 + venue_basis[exchange])
            quote_volume = max(150_000, rng.gauss(330_000, 55_000))
            side = "buy" if rng.random() >= 0.5 else "sell"
            state.record_trade(
                Trade(
                    exchange=exchange,
                    symbol="BTCUSDT",
                    timestamp_ms=timestamp,
                    price=prices[exchange],
                    quantity=quote_volume / prices[exchange],
                    taker_side=side,
                    event_id=f"sim:base:{exchange}:{timestamp}",
                )
            )
            trades += 1
        timestamp += step_ms

    shock_steps = max(12, 60_000 // step_ms)
    for step in range(shock_steps + 1):
        timestamp = now_ms - 60_000 + step * step_ms
        if timestamp > now_ms:
            timestamp = now_ms
        reference_price *= 1 - (0.0135 / shock_steps)
        for exchange in exchanges:
            prices[exchange] = reference_price * (1 + venue_basis[exchange])
            quote_volume = 1_900_000 + rng.random() * 500_000
            state.record_trade(
                Trade(
                    exchange=exchange,
                    symbol="BTCUSDT",
                    timestamp_ms=timestamp,
                    price=prices[exchange],
                    quantity=quote_volume / prices[exchange],
                    taker_side="sell",
                    event_id=f"sim:shock:{exchange}:{timestamp}",
                )
            )
            trades += 1

    liquidation_events = [
        ("binance", 1_450_000.0),
        ("bybit", 1_250_000.0),
    ]
    liquidations = 0
    for index, (exchange, notional) in enumerate(liquidation_events):
        if exchange not in exchanges:
            continue
        price = prices[exchange]
        state.record_liquidation(
            Liquidation(
                exchange=exchange,
                symbol="BTCUSDT",
                timestamp_ms=now_ms - 10_000 + index * 1000,
                price=price,
                quantity=notional / price,
                liquidated_side="long",
                event_id=f"sim:liq:{exchange}:{index}",
            )
        )
        liquidations += 1

    alerts, _ = detector.evaluate(now_ms)
    return SimulationResult(
        alerts=alerts,
        trades_generated=trades,
        liquidations_generated=liquidations,
        now_ms=now_ms,
    )
