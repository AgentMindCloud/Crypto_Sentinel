from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from typing import Any

from crypto_sentinel.config import AppConfig, ThresholdConfig, WindowConfig
from crypto_sentinel.models import Alert, LiquidationSnapshot, MetricSnapshot, Severity
from crypto_sentinel.state import MarketState


@dataclass(slots=True, frozen=True)
class _Candidate:
    metric: MetricSnapshot
    severity: Severity
    direction: str
    emergency: bool = False


class AnomalyDetector:
    def __init__(self, config: AppConfig, state: MarketState) -> None:
        self.config = config
        self.detector = config.detector
        self.state = state
        self.exchanges = [
            name
            for name in ("binance", "bybit", "okx")
            if getattr(config.exchanges, name).enabled and config.symbol_map(name)
        ]
        self.last_metrics: list[MetricSnapshot] = []

    @staticmethod
    def _aligned(metric: MetricSnapshot, threshold: ThresholdConfig) -> bool:
        if metric.return_bps is None or metric.taker_imbalance is None:
            return False
        return (
            abs(metric.taker_imbalance) >= threshold.min_abs_imbalance
            and metric.return_bps * metric.taker_imbalance > 0
        )

    def _passes(self, metric: MetricSnapshot, threshold: ThresholdConfig) -> bool:
        if (
            not metric.ready
            or metric.return_bps is None
            or metric.return_z is None
            or metric.volume_z is None
            or metric.quote_volume is None
        ):
            return False
        if metric.quote_volume < self.detector.minimum_window_quote_volume:
            return False
        price_displacement = abs(metric.return_bps) >= threshold.min_abs_return_bps
        statistically_unusual = abs(metric.return_z) >= threshold.min_abs_return_z
        participation = metric.volume_z >= threshold.min_volume_z
        order_flow = self._aligned(metric, threshold)
        return price_displacement and statistically_unusual and (participation or order_flow)

    def _candidate(self, metric: MetricSnapshot, window: WindowConfig) -> _Candidate | None:
        if metric.return_bps is None or metric.data_age_seconds is None:
            return None
        if metric.data_age_seconds < 0 or metric.data_age_seconds > self.detector.freshness_seconds:
            return None
        direction = "up" if metric.return_bps > 0 else "down"
        if self._passes(metric, window.critical):
            return _Candidate(metric=metric, severity=Severity.CRITICAL, direction=direction)
        if self._passes(metric, window.warning):
            return _Candidate(metric=metric, severity=Severity.WARNING, direction=direction)

        # During baseline warm-up, an extreme absolute move is surfaced as a warning rather
        # than silently ignored. A discontinuous window is deliberately excluded: otherwise a
        # post-outage price gap could be mislabeled as a move that occurred inside this window.
        if metric.readiness_reason != "baseline_warmup":
            return None
        emergency_threshold = (
            window.critical.min_abs_return_bps * self.detector.emergency_absolute_multiplier
        )
        enough_volume = (metric.quote_volume or 0) >= self.detector.minimum_window_quote_volume
        if abs(metric.return_bps) >= emergency_threshold and enough_volume:
            return _Candidate(
                metric=metric,
                severity=Severity.WARNING,
                direction=direction,
                emergency=True,
            )
        return None

    def _price_alert(
        self,
        symbol: str,
        window: WindowConfig,
        metrics: list[MetricSnapshot],
        now_ms: int,
    ) -> Alert | None:
        candidates = [
            candidate for metric in metrics if (candidate := self._candidate(metric, window))
        ]
        if not candidates:
            return None

        grouped: dict[str, list[_Candidate]] = {"up": [], "down": []}
        for candidate in candidates:
            grouped[candidate.direction].append(candidate)
        direction, selected = max(grouped.items(), key=lambda item: len(item[1]))
        if not selected:
            return None

        critical_candidates = [item for item in selected if item.severity == Severity.CRITICAL]
        confirmations = len({item.metric.exchange for item in selected})
        severity = Severity.WARNING
        if (
            critical_candidates
            and confirmations >= window.critical.min_confirmations
            or (
                confirmations >= window.critical.min_confirmations
                and all(
                    abs(item.metric.return_bps or 0) >= window.critical.min_abs_return_bps
                    for item in selected
                )
            )
        ):
            severity = Severity.CRITICAL
        elif confirmations < window.warning.min_confirmations:
            return None

        selected_metrics = [item.metric for item in selected]
        returns = [item.return_bps for item in selected_metrics if item.return_bps is not None]
        return_z = [abs(item.return_z) for item in selected_metrics if item.return_z is not None]
        volume_z = [item.volume_z for item in selected_metrics if item.volume_z is not None]
        imbalances = [
            item.taker_imbalance for item in selected_metrics if item.taker_imbalance is not None
        ]
        exchanges = sorted({item.exchange for item in selected_metrics})
        median_return = statistics.median(returns) if returns else 0.0
        max_return_z = max(return_z, default=0.0)
        max_volume_z = max(volume_z, default=0.0)
        median_imbalance = statistics.median(imbalances) if imbalances else 0.0
        arrow = "▲" if direction == "up" else "▼"
        title = f"{arrow} {symbol} {severity.value} {window.seconds}s market anomaly"
        message = (
            f"{symbol} moved {median_return / 100:+.2f}% over {window.seconds}s across "
            f"{confirmations} exchange(s). Max return z={max_return_z:.1f}, "
            f"max volume z={max_volume_z:.1f}, median taker imbalance={median_imbalance:+.0%}."
        )
        return Alert(
            severity=severity,
            category="market_anomaly",
            symbol=symbol,
            title=title,
            message=message,
            timestamp_ms=now_ms,
            direction=direction,
            exchanges=exchanges,
            metrics={
                "window_seconds": window.seconds,
                "confirmations": confirmations,
                "median_return_bps": median_return,
                "max_abs_return_z": max_return_z,
                "max_volume_z": max_volume_z,
                "median_taker_imbalance": median_imbalance,
                "emergency_without_baseline": any(item.emergency for item in selected),
                "by_exchange": {item.exchange: item.to_dict() for item in selected_metrics},
            },
            dedup_key=f"market:{symbol}:{direction}:{window.seconds}",
        )

    def _liquidation_alert(self, snapshot: LiquidationSnapshot, now_ms: int) -> Alert | None:
        config = self.detector.liquidation
        if not config.enabled or snapshot.total_usd < config.warning_usd:
            return None
        exchange_count = len(snapshot.exchanges)
        severity = Severity.WARNING
        if (
            snapshot.total_usd >= config.critical_usd
            or exchange_count >= config.min_exchanges_for_critical
            and snapshot.total_usd >= config.warning_usd
        ):
            severity = Severity.CRITICAL
        direction = snapshot.direction
        title = f"Liquidation burst: {snapshot.symbol} ${snapshot.total_usd:,.0f}"
        message = (
            f"${snapshot.total_usd:,.0f} liquidated in {snapshot.window_seconds}s across "
            f"{exchange_count} exchange(s): longs ${snapshot.long_usd:,.0f}, "
            f"shorts ${snapshot.short_usd:,.0f}, {snapshot.events} event(s)."
        )
        return Alert(
            severity=severity,
            category="liquidation_burst",
            symbol=snapshot.symbol,
            title=title,
            message=message,
            timestamp_ms=now_ms,
            direction=direction,
            exchanges=sorted(snapshot.exchanges),
            metrics=snapshot.to_dict(),
            dedup_key=f"liquidation:{snapshot.symbol}:{direction}",
        )

    def _spread_alert(
        self,
        symbol: str,
        now_ms: int,
        ready_exchanges: set[str],
    ) -> Alert | None:
        config = self.detector.spread
        if not config.enabled:
            return None
        prices = self.state.latest_prices(symbol, self.detector.freshness_seconds, now_ms)
        prices = {
            exchange: price for exchange, price in prices.items() if exchange in ready_exchanges
        }
        if len(prices) < config.min_exchanges:
            return None
        median_price = statistics.median(prices.values())
        if median_price <= 0:
            return None
        low_exchange, low_price = min(prices.items(), key=lambda item: item[1])
        high_exchange, high_price = max(prices.items(), key=lambda item: item[1])
        spread_bps = (high_price - low_price) / median_price * 10_000
        if spread_bps < config.warning_bps:
            return None
        severity = Severity.CRITICAL if spread_bps >= config.critical_bps else Severity.WARNING
        title = f"Cross-exchange divergence: {symbol} {spread_bps:.1f} bps"
        message = (
            f"{symbol} differs by {spread_bps:.1f} bps: {low_exchange} {low_price:,.8g} vs "
            f"{high_exchange} {high_price:,.8g}. Confirm venue health before acting."
        )
        return Alert(
            severity=severity,
            category="cross_exchange_spread",
            symbol=symbol,
            title=title,
            message=message,
            timestamp_ms=now_ms,
            direction="mixed",
            exchanges=sorted(prices),
            metrics={"spread_bps": spread_bps, "prices": prices},
            dedup_key=f"spread:{symbol}",
        )

    @staticmethod
    def _merge_price_and_liquidation(price: Alert, liquidation: Alert) -> Alert:
        if price.direction != liquidation.direction or liquidation.direction == "mixed":
            return price
        metrics = dict(price.metrics)
        metrics["liquidation"] = liquidation.metrics
        severity = price.severity
        if (
            price.severity == Severity.WARNING
            and liquidation.severity.rank >= Severity.WARNING.rank
        ):
            severity = Severity.CRITICAL
        message = f"{price.message} Corroborating liquidations: {liquidation.message}"
        window_seconds = int(metrics.get("window_seconds", 0))
        arrow = "▲" if price.direction == "up" else "▼"
        return Alert(
            severity=severity,
            category="market_shock",
            symbol=price.symbol,
            title=f"{arrow} {price.symbol} {severity.value} {window_seconds}s market shock",
            message=message,
            timestamp_ms=price.timestamp_ms,
            direction=price.direction,
            exchanges=sorted(set(price.exchanges + liquidation.exchanges)),
            metrics=metrics,
            dedup_key=price.dedup_key,
        )

    def evaluate(self, now_ms: int) -> tuple[list[Alert], list[MetricSnapshot]]:
        alerts: list[Alert] = []
        snapshots: list[MetricSnapshot] = []

        for symbol in self.config.canonical_symbols():
            liquidation_snapshot = self.state.liquidation_snapshot(
                symbol, self.detector.liquidation.window_seconds, now_ms
            )
            liquidation_alert = self._liquidation_alert(liquidation_snapshot, now_ms)
            liquidation_consumed = False
            spread_ready_exchanges: set[str] = set()

            price_alerts: list[Alert] = []
            for window in self.detector.windows:
                metrics = [
                    self.state.metric(
                        exchange,
                        symbol,
                        window.seconds,
                        self.detector.baseline_seconds,
                        self.detector.min_baseline_points,
                        now_ms,
                        maximum_data_gap_seconds=self.detector.maximum_data_gap_seconds,
                        minimum_window_coverage=self.detector.minimum_window_coverage,
                    )
                    for exchange in self.exchanges
                ]
                snapshots.extend(metrics)
                if window.seconds == self.detector.windows[0].seconds:
                    spread_ready_exchanges = {metric.exchange for metric in metrics if metric.ready}
                price_alert = self._price_alert(symbol, window, metrics, now_ms)
                if price_alert:
                    price_alerts.append(price_alert)

            if price_alerts:
                # Overlapping windows commonly trigger together during the same shock. Keep the
                # strongest alert and prefer the shorter window on ties to prevent double alarms.
                strongest = max(
                    price_alerts,
                    key=lambda item: (
                        item.severity.rank,
                        float(item.metrics.get("max_abs_return_z", 0.0)),
                        -int(item.metrics.get("window_seconds", 0)),
                    ),
                )
                if liquidation_alert:
                    strongest = self._merge_price_and_liquidation(strongest, liquidation_alert)
                    if strongest.category == "market_shock":
                        liquidation_consumed = True
                alerts.append(strongest)

            if liquidation_alert and not liquidation_consumed:
                alerts.append(liquidation_alert)
            spread_alert = self._spread_alert(
                symbol,
                now_ms,
                spread_ready_exchanges,
            )
            if spread_alert:
                alerts.append(spread_alert)

        # Defensive cleanup of any non-finite metric payloads before serialization.
        for alert in alerts:
            alert.metrics = _finite_payload(alert.metrics)
        self.last_metrics = snapshots
        return alerts, snapshots


def _finite_payload(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _finite_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_finite_payload(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value
