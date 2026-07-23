from __future__ import annotations

import bisect
import math
import time
from collections import defaultdict, deque
from dataclasses import dataclass

from crypto_sentinel.models import (
    Bucket,
    Liquidation,
    LiquidationSnapshot,
    MetricSnapshot,
    Trade,
)
from crypto_sentinel.stats import robust_zscore


@dataclass(slots=True)
class _SeenEvent:
    event_id: str
    timestamp_ms: int


class MarketSeries:
    def __init__(self, bucket_seconds: int, retention_seconds: int) -> None:
        self.bucket_ms = bucket_seconds * 1000
        self.retention_ms = retention_seconds * 1000
        # Trade IDs are useful only around reconnect boundaries. Keeping every ID for the full
        # statistical baseline would consume unbounded memory on active markets.
        self.dedup_retention_ms = min(self.retention_ms, 120_000)
        self.max_seen_ids = 20_000
        self.buckets: deque[Bucket] = deque()
        self.last_trade_ms: int | None = None
        self._max_seen_timestamp_ms = 0
        self._seen_ids: deque[_SeenEvent] = deque()
        self._seen_set: set[str] = set()

    def _is_duplicate(self, event_id: str | None, timestamp_ms: int) -> bool:
        if not event_id:
            return False
        self._max_seen_timestamp_ms = max(self._max_seen_timestamp_ms, timestamp_ms)
        cutoff = self._max_seen_timestamp_ms - self.dedup_retention_ms
        while self._seen_ids and (
            self._seen_ids[0].timestamp_ms < cutoff or len(self._seen_ids) >= self.max_seen_ids
        ):
            old = self._seen_ids.popleft()
            self._seen_set.discard(old.event_id)
        if event_id in self._seen_set:
            return True
        if timestamp_ms < cutoff:
            return False
        self._seen_set.add(event_id)
        self._seen_ids.append(_SeenEvent(event_id, timestamp_ms))
        return False

    def add_trade(self, trade: Trade) -> bool:
        if self._is_duplicate(trade.event_id, trade.timestamp_ms):
            return False
        bucket_start = trade.timestamp_ms - (trade.timestamp_ms % self.bucket_ms)
        self.last_trade_ms = max(self.last_trade_ms or trade.timestamp_ms, trade.timestamp_ms)

        if not self.buckets or bucket_start > self.buckets[-1].start_ms:
            self.buckets.append(Bucket.from_trade(bucket_start, trade))
        elif bucket_start == self.buckets[-1].start_ms:
            self.buckets[-1].add(trade)
        else:
            found = False
            for bucket in reversed(self.buckets):
                if bucket.start_ms == bucket_start:
                    bucket.add(trade)
                    found = True
                    break
                if bucket.start_ms < bucket_start:
                    break
            if not found:
                starts = [bucket.start_ms for bucket in self.buckets]
                insert_at = bisect.bisect_left(starts, bucket_start)
                # Ignore data older than retained state. A delayed event inside retention is
                # inserted in time order so it contributes volume without corrupting close.
                if (
                    insert_at == 0
                    and self.buckets
                    and bucket_start < self.buckets[0].start_ms
                    and (self.last_trade_ms or trade.timestamp_ms) - bucket_start
                    > self.retention_ms
                ):
                    return False
                self.buckets.insert(insert_at, Bucket.from_trade(bucket_start, trade))
        self.prune(self.last_trade_ms or trade.timestamp_ms)
        return True

    def prune(self, now_ms: int) -> None:
        cutoff = now_ms - self.retention_ms
        while self.buckets and self.buckets[0].start_ms < cutoff:
            self.buckets.popleft()

    def latest_price(self) -> float | None:
        return self.buckets[-1].close if self.buckets else None

    def snapshot(
        self,
        exchange: str,
        symbol: str,
        window_seconds: int,
        baseline_seconds: int,
        min_baseline_points: int,
        now_ms: int,
    ) -> MetricSnapshot:
        if not self.buckets or not self.last_trade_ms:
            return MetricSnapshot(
                exchange=exchange,
                symbol=symbol,
                timestamp_ms=now_ms,
                window_seconds=window_seconds,
                ready=False,
            )

        buckets = list(self.buckets)
        times = [bucket.start_ms for bucket in buckets]
        closes = [bucket.close for bucket in buckets]
        prefix_volume = [0.0]
        prefix_buy = [0.0]
        prefix_sell = [0.0]
        prefix_trades = [0]
        for bucket in buckets:
            prefix_volume.append(prefix_volume[-1] + bucket.quote_volume)
            prefix_buy.append(prefix_buy[-1] + bucket.buy_quote_volume)
            prefix_sell.append(prefix_sell[-1] + bucket.sell_quote_volume)
            prefix_trades.append(prefix_trades[-1] + bucket.trades)

        def segment(end_ms: int, seconds: int) -> tuple[float, float, float, float, int] | None:
            start_ms = end_ms - seconds * 1000
            end_idx = bisect.bisect_right(times, end_ms) - 1
            start_price_idx = bisect.bisect_right(times, start_ms) - 1
            volume_start_idx = bisect.bisect_left(times, start_ms)
            if end_idx < 0 or start_price_idx < 0 or end_idx <= start_price_idx:
                return None
            start_price = closes[start_price_idx]
            end_price = closes[end_idx]
            if start_price <= 0:
                return None
            end_exclusive = end_idx + 1
            volume = prefix_volume[end_exclusive] - prefix_volume[volume_start_idx]
            buy = prefix_buy[end_exclusive] - prefix_buy[volume_start_idx]
            sell = prefix_sell[end_exclusive] - prefix_sell[volume_start_idx]
            trades = prefix_trades[end_exclusive] - prefix_trades[volume_start_idx]
            return ((end_price / start_price) - 1.0, volume, buy, sell, trades)

        current = segment(now_ms, window_seconds)
        if current is None:
            return MetricSnapshot(
                exchange=exchange,
                symbol=symbol,
                timestamp_ms=now_ms,
                window_seconds=window_seconds,
                ready=False,
                price=buckets[-1].close,
                data_age_seconds=(now_ms - self.last_trade_ms) / 1000,
            )

        current_return, current_volume, current_buy, current_sell, current_trades = current
        stride_ms = max(self.bucket_ms, window_seconds * 500)
        baseline_end = now_ms - window_seconds * 1000
        baseline_floor = now_ms - baseline_seconds * 1000
        baseline_returns: list[float] = []
        baseline_volumes: list[float] = []
        cursor = baseline_end
        while cursor >= baseline_floor and len(baseline_returns) < 240:
            historical = segment(cursor, window_seconds)
            if historical is not None:
                historical_return, historical_volume, _, _, _ = historical
                baseline_returns.append(historical_return)
                baseline_volumes.append(historical_volume)
            cursor -= stride_ms

        return_z = robust_zscore(current_return, baseline_returns)
        volume_z = robust_zscore(current_volume, baseline_volumes)
        total_taker = current_buy + current_sell
        imbalance = (current_buy - current_sell) / total_taker if total_taker > 0 else 0.0
        ready = (
            len(baseline_returns) >= min_baseline_points
            and return_z is not None
            and volume_z is not None
        )
        return MetricSnapshot(
            exchange=exchange,
            symbol=symbol,
            timestamp_ms=now_ms,
            window_seconds=window_seconds,
            ready=ready,
            price=buckets[-1].close,
            return_bps=current_return * 10_000,
            return_z=return_z,
            quote_volume=current_volume,
            volume_z=volume_z,
            taker_imbalance=imbalance,
            trades=current_trades,
            baseline_points=min(len(baseline_returns), len(baseline_volumes)),
            data_age_seconds=(now_ms - self.last_trade_ms) / 1000,
        )


class MarketState:
    def __init__(self, bucket_seconds: int, retention_seconds: int) -> None:
        self.bucket_seconds = bucket_seconds
        self.retention_seconds = retention_seconds
        self._series: dict[tuple[str, str], MarketSeries] = {}
        self._liquidations: dict[tuple[str, str], deque[Liquidation]] = defaultdict(deque)
        self._liquidation_seen: set[str] = set()
        self._liquidation_seen_order: deque[_SeenEvent] = deque()
        self._liquidation_max_seen_ms = 0
        self._liquidation_max_seen_ids = 50_000

    def record_trade(self, trade: Trade) -> bool:
        if (
            trade.timestamp_ms <= 0
            or not math.isfinite(trade.price)
            or not math.isfinite(trade.quantity)
            or trade.price <= 0
            or trade.quantity <= 0
            or trade.taker_side not in {"buy", "sell"}
        ):
            return False
        key = (trade.exchange, trade.symbol)
        series = self._series.setdefault(
            key, MarketSeries(self.bucket_seconds, self.retention_seconds)
        )
        return series.add_trade(trade)

    def record_liquidation(self, event: Liquidation) -> bool:
        if (
            event.timestamp_ms <= 0
            or not math.isfinite(event.price)
            or not math.isfinite(event.quantity)
            or event.price <= 0
            or event.quantity <= 0
            or event.liquidated_side not in {"long", "short"}
        ):
            return False
        if event.event_id:
            self._liquidation_max_seen_ms = max(self._liquidation_max_seen_ms, event.timestamp_ms)
            cutoff = self._liquidation_max_seen_ms - self.retention_seconds * 1000
            while self._liquidation_seen_order and (
                self._liquidation_seen_order[0].timestamp_ms < cutoff
                or len(self._liquidation_seen_order) >= self._liquidation_max_seen_ids
            ):
                old = self._liquidation_seen_order.popleft()
                self._liquidation_seen.discard(old.event_id)
            if event.event_id in self._liquidation_seen:
                return False
            if event.timestamp_ms >= cutoff:
                self._liquidation_seen.add(event.event_id)
                self._liquidation_seen_order.append(_SeenEvent(event.event_id, event.timestamp_ms))
        queue = self._liquidations[(event.exchange, event.symbol)]
        if queue and event.timestamp_ms < queue[-1].timestamp_ms:
            timestamps = [item.timestamp_ms for item in queue]
            queue.insert(bisect.bisect_right(timestamps, event.timestamp_ms), event)
        else:
            queue.append(event)
        self._prune_liquidations(self._liquidation_max_seen_ms or event.timestamp_ms)
        return True

    def _prune_liquidations(self, now_ms: int) -> None:
        cutoff = now_ms - self.retention_seconds * 1000
        for queue in self._liquidations.values():
            while queue and queue[0].timestamp_ms < cutoff:
                queue.popleft()

    def metric(
        self,
        exchange: str,
        symbol: str,
        window_seconds: int,
        baseline_seconds: int,
        min_baseline_points: int,
        now_ms: int | None = None,
    ) -> MetricSnapshot:
        timestamp = now_ms or int(time.time() * 1000)
        series = self._series.get((exchange, symbol))
        if not series:
            return MetricSnapshot(
                exchange=exchange,
                symbol=symbol,
                timestamp_ms=timestamp,
                window_seconds=window_seconds,
                ready=False,
            )
        return series.snapshot(
            exchange,
            symbol,
            window_seconds,
            baseline_seconds,
            min_baseline_points,
            timestamp,
        )

    def latest_prices(self, symbol: str, freshness_seconds: int, now_ms: int) -> dict[str, float]:
        prices: dict[str, float] = {}
        for (exchange, candidate), series in self._series.items():
            if candidate != symbol or series.last_trade_ms is None:
                continue
            if now_ms - series.last_trade_ms > freshness_seconds * 1000:
                continue
            price = series.latest_price()
            if price is not None:
                prices[exchange] = price
        return prices

    def liquidation_snapshot(
        self, symbol: str, window_seconds: int, now_ms: int | None = None
    ) -> LiquidationSnapshot:
        timestamp = now_ms or int(time.time() * 1000)
        cutoff = timestamp - window_seconds * 1000
        total = long_total = short_total = 0.0
        exchanges: dict[str, float] = {}
        events = 0
        for (exchange, candidate), queue in self._liquidations.items():
            if candidate != symbol:
                continue
            exchange_total = 0.0
            for event in reversed(queue):
                if event.timestamp_ms < cutoff:
                    break
                notional = event.quote_notional
                total += notional
                exchange_total += notional
                events += 1
                if event.liquidated_side == "long":
                    long_total += notional
                elif event.liquidated_side == "short":
                    short_total += notional
            if exchange_total > 0:
                exchanges[exchange] = exchange_total
        return LiquidationSnapshot(
            symbol=symbol,
            timestamp_ms=timestamp,
            window_seconds=window_seconds,
            total_usd=total,
            long_usd=long_total,
            short_usd=short_total,
            exchanges=exchanges,
            events=events,
        )

    def export_checkpoint(self, now_ms: int) -> dict[str, object]:
        self._prune_liquidations(now_ms)
        series_payload: list[dict[str, object]] = []
        for (exchange, symbol), series in self._series.items():
            series.prune(now_ms)
            if not series.buckets:
                continue
            series_payload.append(
                {
                    "exchange": exchange,
                    "symbol": symbol,
                    "last_trade_ms": series.last_trade_ms,
                    "buckets": [
                        {
                            "start_ms": bucket.start_ms,
                            "open": bucket.open,
                            "high": bucket.high,
                            "low": bucket.low,
                            "close": bucket.close,
                            "quote_volume": bucket.quote_volume,
                            "buy_quote_volume": bucket.buy_quote_volume,
                            "sell_quote_volume": bucket.sell_quote_volume,
                            "trades": bucket.trades,
                            "first_trade_ms": bucket.first_trade_ms,
                            "last_trade_ms": bucket.last_trade_ms,
                        }
                        for bucket in series.buckets
                    ],
                }
            )
        liquidation_payload = [
            {
                "exchange": event.exchange,
                "symbol": event.symbol,
                "timestamp_ms": event.timestamp_ms,
                "price": event.price,
                "quantity": event.quantity,
                "liquidated_side": event.liquidated_side,
                "event_id": event.event_id,
            }
            for queue in self._liquidations.values()
            for event in queue
        ]
        return {
            "version": 1,
            "saved_ms": now_ms,
            "bucket_seconds": self.bucket_seconds,
            "retention_seconds": self.retention_seconds,
            "series": series_payload,
            "liquidations": liquidation_payload,
        }

    def restore_checkpoint(
        self,
        payload: dict[str, object],
        now_ms: int,
        allowed_pairs: set[tuple[str, str]],
    ) -> dict[str, int]:
        if int(payload.get("version", 0)) != 1:
            raise ValueError("unsupported checkpoint version")
        if int(payload.get("bucket_seconds", 0)) != self.bucket_seconds:
            raise ValueError("checkpoint bucket size does not match configuration")
        cutoff_ms = now_ms - self.retention_seconds * 1000
        future_limit_ms = now_ms + 300_000
        max_buckets = self.retention_seconds // self.bucket_seconds + 4
        restored_buckets = 0
        restored_liquidations = 0

        raw_series = payload.get("series", [])
        if not isinstance(raw_series, list):
            raise ValueError("checkpoint series must be a list")
        for row in raw_series:
            if not isinstance(row, dict):
                continue
            exchange = str(row.get("exchange", ""))
            symbol = str(row.get("symbol", ""))
            key = (exchange, symbol)
            if key not in allowed_pairs:
                continue
            raw_buckets = row.get("buckets", [])
            if not isinstance(raw_buckets, list):
                continue
            buckets: list[Bucket] = []
            for raw in raw_buckets[-max_buckets:]:
                if not isinstance(raw, dict):
                    continue
                try:
                    bucket = Bucket(
                        start_ms=int(raw["start_ms"]),
                        open=float(raw["open"]),
                        high=float(raw["high"]),
                        low=float(raw["low"]),
                        close=float(raw["close"]),
                        quote_volume=float(raw.get("quote_volume", 0)),
                        buy_quote_volume=float(raw.get("buy_quote_volume", 0)),
                        sell_quote_volume=float(raw.get("sell_quote_volume", 0)),
                        trades=int(raw.get("trades", 0)),
                        first_trade_ms=int(raw.get("first_trade_ms", raw["start_ms"])),
                        last_trade_ms=int(raw.get("last_trade_ms", raw["start_ms"])),
                    )
                except (KeyError, TypeError, ValueError):
                    continue
                numeric = (
                    bucket.open,
                    bucket.high,
                    bucket.low,
                    bucket.close,
                    bucket.quote_volume,
                    bucket.buy_quote_volume,
                    bucket.sell_quote_volume,
                )
                if not all(math.isfinite(value) for value in numeric):
                    continue
                if not (cutoff_ms <= bucket.start_ms <= future_limit_ms):
                    continue
                if min(bucket.open, bucket.high, bucket.low, bucket.close) <= 0:
                    continue
                buckets.append(bucket)
            if not buckets:
                continue
            buckets.sort(key=lambda item: item.start_ms)
            series = MarketSeries(self.bucket_seconds, self.retention_seconds)
            series.buckets = deque(buckets)
            series.last_trade_ms = min(
                max(
                    int(row.get("last_trade_ms") or buckets[-1].last_trade_ms),
                    buckets[-1].last_trade_ms,
                ),
                future_limit_ms,
            )
            self._series[key] = series
            restored_buckets += len(buckets)

        raw_liquidations = payload.get("liquidations", [])
        if isinstance(raw_liquidations, list):
            for raw in raw_liquidations:
                if not isinstance(raw, dict):
                    continue
                try:
                    event = Liquidation(
                        exchange=str(raw["exchange"]),
                        symbol=str(raw["symbol"]),
                        timestamp_ms=int(raw["timestamp_ms"]),
                        price=float(raw["price"]),
                        quantity=float(raw["quantity"]),
                        liquidated_side=str(raw["liquidated_side"]),
                        event_id=str(raw["event_id"]) if raw.get("event_id") else None,
                    )
                except (KeyError, TypeError, ValueError):
                    continue
                if (event.exchange, event.symbol) not in allowed_pairs:
                    continue
                if not (cutoff_ms <= event.timestamp_ms <= future_limit_ms):
                    continue
                if not all(
                    math.isfinite(value) and value > 0 for value in (event.price, event.quantity)
                ):
                    continue
                if self.record_liquidation(event):
                    restored_liquidations += 1
        return {"buckets": restored_buckets, "liquidations": restored_liquidations}
