from __future__ import annotations

import bisect
import math
import time
from collections import defaultdict, deque
from collections.abc import Callable
from dataclasses import dataclass, replace

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


@dataclass(slots=True)
class _ContinuityBreak:
    start_ms: int
    end_ms: int | None
    reason: str


@dataclass(slots=True, frozen=True)
class _Segment:
    return_value: float
    volume: float
    buy: float
    sell: float
    trades: int
    coverage_ratio: float
    largest_gap_seconds: float
    continuous: bool
    last_gap_end_ms: int | None


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
        self.last_receive_monotonic_s: float | None = None
        self._max_seen_timestamp_ms = 0
        self._seen_ids: deque[_SeenEvent] = deque()
        self._seen_set: set[str] = set()
        self.continuity_breaks: deque[_ContinuityBreak] = deque()

    def _normalize_continuity_breaks(self) -> None:
        ordered = sorted(
            self.continuity_breaks,
            key=lambda item: (
                item.start_ms,
                item.end_ms if item.end_ms is not None else math.inf,
            ),
        )
        merged: deque[_ContinuityBreak] = deque()
        for item in ordered:
            if not merged:
                merged.append(item)
                continue
            previous = merged[-1]
            if previous.end_ms is None:
                continue
            if item.start_ms > previous.end_ms:
                merged.append(item)
                continue
            reasons = sorted(set(previous.reason.split(",")) | set(item.reason.split(",")))
            previous.reason = ",".join(reason for reason in reasons if reason)
            if item.end_ms is None:
                previous.end_ms = None
            else:
                previous.end_ms = max(previous.end_ms, item.end_ms)
        self.continuity_breaks = merged

    def begin_continuity_break(self, start_ms: int, reason: str) -> None:
        for item in reversed(self.continuity_breaks):
            if item.end_ms is None:
                item.start_ms = min(item.start_ms, max(1, int(start_ms)))
                reasons = sorted(set(item.reason.split(",")) | {reason[:120]})
                item.reason = ",".join(value for value in reasons if value)
                self._normalize_continuity_breaks()
                return
        self.continuity_breaks.append(_ContinuityBreak(max(1, int(start_ms)), None, reason[:120]))
        self._normalize_continuity_breaks()

    def end_continuity_break(self, end_ms: int) -> None:
        for item in reversed(self.continuity_breaks):
            if item.end_ms is None:
                item.end_ms = max(item.start_ms + 1, int(end_ms))
                self._normalize_continuity_breaks()
                return

    def record_continuity_break(
        self,
        start_ms: int,
        end_ms: int,
        reason: str,
    ) -> None:
        start = max(1, int(start_ms))
        end = max(start + 1, int(end_ms))
        self.continuity_breaks.append(_ContinuityBreak(start, end, reason[:120]))
        self._normalize_continuity_breaks()

    def continuity_break_overlaps(self, start_ms: int, end_ms: int) -> bool:
        """Return whether a known monitoring gap touches the requested interval."""
        start = int(start_ms)
        end = max(start, int(end_ms))
        return any(
            item.start_ms <= end and (item.end_ms is None or item.end_ms >= start)
            for item in self.continuity_breaks
        )

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

    def add_trade(self, trade: Trade, received_monotonic_s: float | None = None) -> bool:
        if self._is_duplicate(trade.event_id, trade.timestamp_ms):
            return False
        previous_last_trade_ms = self.last_trade_ms
        bucket_start = trade.timestamp_ms - (trade.timestamp_ms % self.bucket_ms)
        self.last_trade_ms = max(self.last_trade_ms or trade.timestamp_ms, trade.timestamp_ms)
        if received_monotonic_s is not None and (
            previous_last_trade_ms is None or trade.timestamp_ms >= previous_last_trade_ms
        ):
            self.last_receive_monotonic_s = max(
                self.last_receive_monotonic_s or received_monotonic_s,
                received_monotonic_s,
            )

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
        while (
            self.continuity_breaks
            and self.continuity_breaks[0].end_ms is not None
            and self.continuity_breaks[0].end_ms < cutoff
        ):
            self.continuity_breaks.popleft()

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
        *,
        maximum_data_gap_seconds: int,
        minimum_window_coverage: float,
        now_monotonic_s: float | None = None,
    ) -> MetricSnapshot:
        event_age_seconds = (
            max(0.0, (now_ms - self.last_trade_ms) / 1000)
            if self.last_trade_ms is not None
            else None
        )
        receive_age_seconds = (
            max(0.0, now_monotonic_s - self.last_receive_monotonic_s)
            if now_monotonic_s is not None and self.last_receive_monotonic_s is not None
            else None
        )
        ages = [age for age in (event_age_seconds, receive_age_seconds) if age is not None]
        data_age_seconds = max(ages) if ages else None
        if not self.buckets or not self.last_trade_ms:
            return MetricSnapshot(
                exchange=exchange,
                symbol=symbol,
                timestamp_ms=now_ms,
                window_seconds=window_seconds,
                ready=False,
                readiness_reason="no_data",
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

        last_trade_times = [bucket.last_trade_ms for bucket in buckets]

        def segment(end_ms: int, seconds: int) -> _Segment | None:
            start_ms = end_ms - seconds * 1000
            end_idx = bisect.bisect_right(last_trade_times, end_ms) - 1
            start_price_idx = bisect.bisect_right(last_trade_times, start_ms) - 1
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
            return_start_ms = last_trade_times[start_price_idx]
            explicit_breaks: list[tuple[int, int, bool]] = []
            coverage_intervals: list[tuple[int, int]] = []
            for continuity_break in self.continuity_breaks:
                break_is_open = continuity_break.end_ms is None
                break_end_ms = continuity_break.end_ms if not break_is_open else end_ms
                starts_after_segment = (
                    continuity_break.start_ms > end_ms
                    if break_is_open
                    else continuity_break.start_ms >= end_ms
                )
                if starts_after_segment or break_end_ms <= return_start_ms:
                    continue
                explicit_breaks.append(
                    (
                        max(0, break_end_ms - continuity_break.start_ms),
                        break_end_ms,
                        break_is_open,
                    )
                )
                overlap_start = max(start_ms, continuity_break.start_ms)
                overlap_end = min(end_ms, break_end_ms)
                if overlap_end > overlap_start:
                    coverage_intervals.append((overlap_start, overlap_end))
            maximum_gap_ms = maximum_data_gap_seconds * 1000
            merged_intervals: list[list[int]] = []
            for overlap_start, overlap_end in sorted(coverage_intervals):
                if not merged_intervals or overlap_start > merged_intervals[-1][1]:
                    merged_intervals.append([overlap_start, overlap_end])
                else:
                    merged_intervals[-1][1] = max(
                        merged_intervals[-1][1],
                        overlap_end,
                    )
            uncovered_ms = sum(end - start for start, end in merged_intervals)
            window_ms = seconds * 1000
            coverage_ratio = max(0.0, 1.0 - min(uncovered_ms, window_ms) / window_ms)
            largest_gap_ms = max(
                (duration_ms for duration_ms, _end_ms, _open in explicit_breaks),
                default=0,
            )
            has_open_break = any(is_open for _duration, _end, is_open in explicit_breaks)
            has_oversized_break = any(
                duration_ms > maximum_gap_ms for duration_ms, _end, _open in explicit_breaks
            )
            # Ordinary silence between valid trades is not transport loss.
            # Explicit closed breaks are tolerated only while each is within
            # maximum_data_gap_seconds and their aggregate union still meets
            # minimum_window_coverage. An active break is always fail-closed.
            continuous = (
                not has_open_break
                and not has_oversized_break
                and coverage_ratio >= minimum_window_coverage
            )
            latest_break_end_ms = max(
                (end for _duration, end, _open in explicit_breaks),
                default=end_ms,
            )
            return _Segment(
                return_value=(end_price / start_price) - 1.0,
                volume=volume,
                buy=buy,
                sell=sell,
                trades=trades,
                coverage_ratio=coverage_ratio,
                largest_gap_seconds=largest_gap_ms / 1000,
                continuous=continuous,
                last_gap_end_ms=latest_break_end_ms if not continuous else None,
            )

        current = segment(now_ms, window_seconds)
        if current is None:
            return MetricSnapshot(
                exchange=exchange,
                symbol=symbol,
                timestamp_ms=now_ms,
                window_seconds=window_seconds,
                ready=False,
                price=buckets[-1].close,
                data_age_seconds=data_age_seconds,
                readiness_reason="insufficient_window",
                recovery_seconds_remaining=float(window_seconds),
            )

        stride_ms = max(self.bucket_ms, window_seconds * 500)
        baseline_end = now_ms - window_seconds * 1000
        baseline_floor = now_ms - baseline_seconds * 1000
        baseline_returns: list[float] = []
        baseline_volumes: list[float] = []
        cursor = baseline_end
        while cursor >= baseline_floor and len(baseline_returns) < 240:
            historical = segment(cursor, window_seconds)
            if historical is not None and historical.continuous:
                baseline_returns.append(historical.return_value)
                baseline_volumes.append(historical.volume)
            cursor -= stride_ms

        return_z = robust_zscore(current.return_value, baseline_returns)
        volume_z = robust_zscore(current.volume, baseline_volumes)
        total_taker = current.buy + current.sell
        imbalance = (current.buy - current.sell) / total_taker if total_taker > 0 else 0.0
        ready = (
            current.continuous
            and len(baseline_returns) >= min_baseline_points
            and return_z is not None
            and volume_z is not None
        )
        if not current.continuous:
            readiness_reason = "recovering_after_gap"
        elif not ready:
            readiness_reason = "baseline_warmup"
        else:
            readiness_reason = "ready"
        recovery_seconds_remaining = 0.0
        if current.last_gap_end_ms is not None:
            recovery_seconds_remaining = max(
                0.0,
                (current.last_gap_end_ms + window_seconds * 1000 - now_ms) / 1000,
            )
        return MetricSnapshot(
            exchange=exchange,
            symbol=symbol,
            timestamp_ms=now_ms,
            window_seconds=window_seconds,
            ready=ready,
            price=buckets[-1].close,
            return_bps=current.return_value * 10_000,
            return_z=return_z,
            quote_volume=current.volume,
            volume_z=volume_z,
            taker_imbalance=imbalance,
            trades=current.trades,
            baseline_points=min(len(baseline_returns), len(baseline_volumes)),
            data_age_seconds=data_age_seconds,
            readiness_reason=readiness_reason,
            window_coverage_ratio=current.coverage_ratio,
            largest_gap_seconds=current.largest_gap_seconds,
            recovery_seconds_remaining=recovery_seconds_remaining,
        )


class MarketState:
    def __init__(
        self,
        bucket_seconds: int,
        retention_seconds: int,
        *,
        max_future_skew_seconds: int | None = None,
        wall_clock_ms: Callable[[], int] | None = None,
        monotonic_clock: Callable[[], float] | None = None,
    ) -> None:
        self.bucket_seconds = bucket_seconds
        self.retention_seconds = retention_seconds
        self.max_future_skew_ms = (
            max_future_skew_seconds * 1000 if max_future_skew_seconds is not None else None
        )
        self._wall_clock_ms = wall_clock_ms or (lambda: int(time.time() * 1000))
        self._monotonic_clock = monotonic_clock or time.monotonic
        self._clock_anchor_wall_ms: int | None = None
        self._clock_anchor_monotonic_s: float | None = None
        self._clock_adjustments = 0
        self._clock_forward_reanchors = 0
        self._clock_skew_active = False
        self._future_events_quarantined = 0
        self._future_events_normalized = 0
        self._future_events_by_pair: dict[str, int] = defaultdict(int)
        self._last_quarantine_ms: int | None = None
        self._series: dict[tuple[str, str], MarketSeries] = {}
        self._liquidations: dict[tuple[str, str], deque[Liquidation]] = defaultdict(deque)
        self._liquidation_seen: set[str] = set()
        self._liquidation_seen_order: deque[_SeenEvent] = deque()
        self._liquidation_max_seen_ms = 0
        self._liquidation_max_seen_ids = 50_000

    def begin_continuity_break(
        self,
        exchange: str,
        symbol: str,
        start_ms: int,
        reason: str,
    ) -> None:
        series = self._series.setdefault(
            (exchange, symbol),
            MarketSeries(self.bucket_seconds, self.retention_seconds),
        )
        series.begin_continuity_break(start_ms, reason)

    def end_continuity_break(
        self,
        exchange: str,
        symbol: str,
        end_ms: int,
    ) -> None:
        series = self._series.get((exchange, symbol))
        if series is not None:
            series.end_continuity_break(end_ms)

    def record_continuity_break(
        self,
        exchange: str,
        symbol: str,
        start_ms: int,
        end_ms: int,
        reason: str,
    ) -> None:
        series = self._series.setdefault(
            (exchange, symbol),
            MarketSeries(self.bucket_seconds, self.retention_seconds),
        )
        series.record_continuity_break(start_ms, end_ms, reason)

    def record_global_continuity_break(
        self,
        start_ms: int,
        end_ms: int,
        reason: str,
    ) -> None:
        for series in self._series.values():
            series.record_continuity_break(start_ms, end_ms, reason)

    def _trusted_wall_ms(self, wall_ms: int, monotonic_s: float) -> int:
        if self.max_future_skew_ms is None:
            return wall_ms
        if self._clock_anchor_wall_ms is None or self._clock_anchor_monotonic_s is None:
            self._clock_anchor_wall_ms = wall_ms
            self._clock_anchor_monotonic_s = monotonic_s
            return wall_ms
        projected = self._clock_anchor_wall_ms + int(
            (monotonic_s - self._clock_anchor_monotonic_s) * 1000
        )
        deviation_ms = wall_ms - projected
        if deviation_ms > self.max_future_skew_ms:
            # CLOCK_MONOTONIC may pause during system suspend on some platforms while wall time
            # advances. Re-anchor forward jumps so wake-up traffic is accepted; the resulting
            # market-data gap remains visible to the continuity gate.
            self._clock_adjustments += 1
            self._clock_forward_reanchors += 1
            self.record_global_continuity_break(
                projected,
                wall_ms,
                "clock_forward_jump",
            )
            self._clock_anchor_wall_ms = wall_ms
            self._clock_anchor_monotonic_s = monotonic_s
            self._clock_skew_active = False
            return wall_ms
        if deviation_ms < -self.max_future_skew_ms:
            if not self._clock_skew_active:
                self._clock_adjustments += 1
            self._clock_skew_active = True
            return projected
        self._clock_skew_active = False
        return wall_ms

    def _received_times(
        self,
        received_wall_ms: int | None,
        received_monotonic_s: float | None,
    ) -> tuple[int, float]:
        monotonic_s = (
            self._monotonic_clock() if received_monotonic_s is None else received_monotonic_s
        )
        wall_ms = self._wall_clock_ms() if received_wall_ms is None else received_wall_ms
        return self._trusted_wall_ms(wall_ms, monotonic_s), monotonic_s

    def _quarantine_future_event(
        self,
        event_type: str,
        exchange: str,
        symbol: str,
        timestamp_ms: int,
        received_wall_ms: int,
    ) -> bool:
        if (
            self.max_future_skew_ms is None
            or timestamp_ms <= received_wall_ms + self.max_future_skew_ms
        ):
            return False
        self._future_events_quarantined += 1
        self._future_events_by_pair[f"{exchange}:{symbol}:{event_type}"] += 1
        self._last_quarantine_ms = received_wall_ms
        return True

    def clock_status(self) -> dict[str, object]:
        return {
            "future_events_quarantined": self._future_events_quarantined,
            "future_events_normalized": self._future_events_normalized,
            "future_events_by_pair": dict(sorted(self._future_events_by_pair.items())),
            "clock_adjustments": self._clock_adjustments,
            "clock_forward_reanchors": self._clock_forward_reanchors,
            "clock_skew_active": self._clock_skew_active,
            "last_quarantine_ms": self._last_quarantine_ms,
        }

    def record_trade(
        self,
        trade: Trade,
        *,
        received_wall_ms: int | None = None,
        received_monotonic_s: float | None = None,
    ) -> bool:
        if (
            trade.timestamp_ms <= 0
            or not math.isfinite(trade.price)
            or not math.isfinite(trade.quantity)
            or trade.price <= 0
            or trade.quantity <= 0
            or trade.taker_side not in {"buy", "sell"}
        ):
            return False
        trusted_wall_ms, monotonic_s = self._received_times(received_wall_ms, received_monotonic_s)
        if self._quarantine_future_event(
            "trade",
            trade.exchange,
            trade.symbol,
            trade.timestamp_ms,
            trusted_wall_ms,
        ):
            return False
        if self.max_future_skew_ms is not None and trade.timestamp_ms > trusted_wall_ms:
            trade = replace(trade, timestamp_ms=trusted_wall_ms)
            self._future_events_normalized += 1
        key = (trade.exchange, trade.symbol)
        series = self._series.setdefault(
            key, MarketSeries(self.bucket_seconds, self.retention_seconds)
        )
        return series.add_trade(trade, monotonic_s)

    def record_liquidation(
        self,
        event: Liquidation,
        *,
        received_wall_ms: int | None = None,
        received_monotonic_s: float | None = None,
    ) -> bool:
        if (
            event.timestamp_ms <= 0
            or not math.isfinite(event.price)
            or not math.isfinite(event.quantity)
            or event.price <= 0
            or event.quantity <= 0
            or event.liquidated_side not in {"long", "short"}
        ):
            return False
        trusted_wall_ms, _ = self._received_times(received_wall_ms, received_monotonic_s)
        if self._quarantine_future_event(
            "liquidation",
            event.exchange,
            event.symbol,
            event.timestamp_ms,
            trusted_wall_ms,
        ):
            return False
        if self.max_future_skew_ms is not None and event.timestamp_ms > trusted_wall_ms:
            event = replace(event, timestamp_ms=trusted_wall_ms)
            self._future_events_normalized += 1
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
        *,
        maximum_data_gap_seconds: int | None = None,
        minimum_window_coverage: float = 0.8,
        now_monotonic_s: float | None = None,
    ) -> MetricSnapshot:
        monotonic_s = self._monotonic_clock() if now_monotonic_s is None else now_monotonic_s
        wall_ms = self._wall_clock_ms() if now_ms is None else now_ms
        timestamp = self._trusted_wall_ms(wall_ms, monotonic_s)
        max_gap_seconds = maximum_data_gap_seconds or max(self.bucket_seconds * 3, 5)
        series = self._series.get((exchange, symbol))
        if not series:
            return MetricSnapshot(
                exchange=exchange,
                symbol=symbol,
                timestamp_ms=timestamp,
                window_seconds=window_seconds,
                ready=False,
                readiness_reason="no_data",
            )
        return series.snapshot(
            exchange,
            symbol,
            window_seconds,
            baseline_seconds,
            min_baseline_points,
            timestamp,
            maximum_data_gap_seconds=max_gap_seconds,
            minimum_window_coverage=minimum_window_coverage,
            now_monotonic_s=monotonic_s,
        )

    def latest_prices(self, symbol: str, freshness_seconds: int, now_ms: int) -> dict[str, float]:
        monotonic_s = self._monotonic_clock()
        trusted_now_ms = self._trusted_wall_ms(now_ms, monotonic_s)
        prices: dict[str, float] = {}
        for (exchange, candidate), series in self._series.items():
            if candidate != symbol or series.last_trade_ms is None:
                continue
            receive_age_ms = (
                max(0.0, (monotonic_s - series.last_receive_monotonic_s) * 1000)
                if series.last_receive_monotonic_s is not None
                else 0.0
            )
            event_age_ms = max(0, trusted_now_ms - series.last_trade_ms)
            age_ms = max(receive_age_ms, event_age_ms)
            if age_ms > freshness_seconds * 1000:
                continue
            price = series.latest_price()
            if price is not None:
                prices[exchange] = price
        return prices

    def liquidation_snapshot(
        self, symbol: str, window_seconds: int, now_ms: int | None = None
    ) -> LiquidationSnapshot:
        monotonic_s = self._monotonic_clock()
        wall_ms = self._wall_clock_ms() if now_ms is None else now_ms
        timestamp = self._trusted_wall_ms(wall_ms, monotonic_s)
        cutoff = timestamp - window_seconds * 1000
        total = long_total = short_total = 0.0
        exchanges: dict[str, float] = {}
        events = 0
        for (exchange, candidate), queue in self._liquidations.items():
            if candidate != symbol:
                continue
            series = self._series.get((exchange, candidate))
            if series is not None and series.continuity_break_overlaps(cutoff, timestamp):
                # A burst calculation is unsafe when any part of its source
                # window crossed a known feed/process gap. Healthy venues may
                # still corroborate independently, but incomplete venue data
                # must never help trigger a liquidation alarm.
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
                    "continuity_breaks": [
                        {
                            "start_ms": item.start_ms,
                            "end_ms": item.end_ms,
                            "reason": item.reason,
                        }
                        for item in series.continuity_breaks
                    ],
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
            "version": 2,
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
        if int(payload.get("version", 0)) != 2:
            raise ValueError("unsupported checkpoint version")
        if int(payload.get("bucket_seconds", 0)) != self.bucket_seconds:
            raise ValueError("checkpoint bucket size does not match configuration")
        saved_ms = int(payload.get("saved_ms", now_ms))
        cutoff_ms = now_ms - self.retention_seconds * 1000
        future_limit_ms = now_ms + (
            self.max_future_skew_ms if self.max_future_skew_ms is not None else 300_000
        )
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
            raw_breaks = row.get("continuity_breaks", [])
            if isinstance(raw_breaks, list):
                for raw_break in raw_breaks:
                    if not isinstance(raw_break, dict):
                        continue
                    try:
                        start_ms = int(raw_break["start_ms"])
                        raw_end_ms = raw_break.get("end_ms")
                        end_ms = int(raw_end_ms) if raw_end_ms is not None else None
                        reason = str(raw_break.get("reason", "restored_gap"))
                    except (KeyError, TypeError, ValueError):
                        continue
                    if start_ms <= 0 or start_ms > future_limit_ms:
                        continue
                    if end_ms is None:
                        series.begin_continuity_break(start_ms, reason)
                    elif end_ms >= cutoff_ms:
                        series.record_continuity_break(
                            start_ms,
                            min(end_ms, future_limit_ms),
                            reason,
                        )
            if saved_ms < now_ms:
                series.begin_continuity_break(
                    max(cutoff_ms, saved_ms),
                    "checkpoint_downtime",
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
