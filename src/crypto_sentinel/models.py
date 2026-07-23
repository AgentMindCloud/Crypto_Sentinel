from __future__ import annotations

import math
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any
from uuid import uuid4


class Severity(StrEnum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"

    @property
    def rank(self) -> int:
        return {Severity.INFO: 0, Severity.WARNING: 1, Severity.CRITICAL: 2}[self]


@dataclass(slots=True, frozen=True)
class Trade:
    exchange: str
    symbol: str
    timestamp_ms: int
    price: float
    quantity: float
    taker_side: str
    event_id: str | None = None

    @property
    def quote_notional(self) -> float:
        return self.price * self.quantity


@dataclass(slots=True, frozen=True)
class Liquidation:
    exchange: str
    symbol: str
    timestamp_ms: int
    price: float
    quantity: float
    liquidated_side: str
    event_id: str | None = None

    @property
    def quote_notional(self) -> float:
        return self.price * self.quantity


MarketEvent = Trade | Liquidation


@dataclass(slots=True)
class Bucket:
    start_ms: int
    open: float
    high: float
    low: float
    close: float
    quote_volume: float = 0.0
    buy_quote_volume: float = 0.0
    sell_quote_volume: float = 0.0
    trades: int = 0
    first_trade_ms: int = 0
    last_trade_ms: int = 0

    @classmethod
    def from_trade(cls, start_ms: int, trade: Trade) -> Bucket:
        notional = trade.quote_notional
        return cls(
            start_ms=start_ms,
            open=trade.price,
            high=trade.price,
            low=trade.price,
            close=trade.price,
            quote_volume=notional,
            buy_quote_volume=notional if trade.taker_side == "buy" else 0.0,
            sell_quote_volume=notional if trade.taker_side == "sell" else 0.0,
            trades=1,
            first_trade_ms=trade.timestamp_ms,
            last_trade_ms=trade.timestamp_ms,
        )

    def add(self, trade: Trade) -> None:
        notional = trade.quote_notional
        self.high = max(self.high, trade.price)
        self.low = min(self.low, trade.price)
        if self.first_trade_ms == 0 or trade.timestamp_ms < self.first_trade_ms:
            self.open = trade.price
            self.first_trade_ms = trade.timestamp_ms
        if self.last_trade_ms == 0 or trade.timestamp_ms >= self.last_trade_ms:
            self.close = trade.price
            self.last_trade_ms = trade.timestamp_ms
        self.quote_volume += notional
        if trade.taker_side == "buy":
            self.buy_quote_volume += notional
        else:
            self.sell_quote_volume += notional
        self.trades += 1


@dataclass(slots=True, frozen=True)
class MetricSnapshot:
    exchange: str
    symbol: str
    timestamp_ms: int
    window_seconds: int
    ready: bool
    price: float | None = None
    return_bps: float | None = None
    return_z: float | None = None
    quote_volume: float | None = None
    volume_z: float | None = None
    taker_imbalance: float | None = None
    trades: int = 0
    baseline_points: int = 0
    data_age_seconds: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True, frozen=True)
class LiquidationSnapshot:
    symbol: str
    timestamp_ms: int
    window_seconds: int
    total_usd: float
    long_usd: float
    short_usd: float
    exchanges: dict[str, float]
    events: int

    @property
    def direction(self) -> str:
        if self.long_usd > self.short_usd:
            return "down"
        if self.short_usd > self.long_usd:
            return "up"
        return "mixed"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Alert:
    severity: Severity
    category: str
    symbol: str
    title: str
    message: str
    timestamp_ms: int
    direction: str = "mixed"
    exchanges: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    dedup_key: str = ""
    source: str = "crypto-sentinel"
    id: str = field(default_factory=lambda: str(uuid4()))

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["severity"] = self.severity.value
        return payload

    @classmethod
    def from_external(cls, payload: dict[str, Any], now_ms: int) -> Alert:
        severity_raw = str(payload.get("severity", "warning")).lower()
        severity = (
            Severity(severity_raw)
            if severity_raw in Severity._value2member_map_
            else Severity.WARNING
        )
        source = str(payload.get("source", "external"))[:80]
        symbol = str(payload.get("symbol", "MARKET")).upper()[:40]
        direction_raw = str(payload.get("direction", "mixed")).lower()
        direction = direction_raw if direction_raw in {"up", "down", "mixed"} else "mixed"
        category = str(payload.get("category", "external"))[:80]
        title = str(payload.get("title", f"External alert: {symbol}"))[:200]
        message = str(payload.get("message", "External alert received."))[:4000]
        metrics_raw = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
        metrics = _safe_json_value(metrics_raw)
        try:
            timestamp_ms = int(payload.get("timestamp_ms") or now_ms)
        except (TypeError, ValueError):
            timestamp_ms = now_ms
        # A malformed external clock should not suppress legitimate future alerts through the
        # cooldown mechanism. Accept up to five minutes of clock skew in either direction.
        timestamp_ms = max(now_ms - 300_000, min(timestamp_ms, now_ms + 300_000))
        return cls(
            severity=severity,
            category=category,
            symbol=symbol,
            title=title,
            message=message,
            timestamp_ms=timestamp_ms,
            direction=direction,
            exchanges=[source],
            metrics=metrics,
            dedup_key=str(
                payload.get("dedup_key", f"external:{source}:{symbol}:{category}:{direction}")
            )[:300],
            source=source,
        )


def _safe_json_value(value: Any, depth: int = 0) -> Any:
    if depth >= 12:
        return "<maximum nesting reached>"
    if isinstance(value, dict):
        return {
            str(key)[:100]: _safe_json_value(item, depth + 1)
            for key, item in list(value.items())[:200]
        }
    if isinstance(value, list):
        return [_safe_json_value(item, depth + 1) for item in value[:500]]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)[:1000]
