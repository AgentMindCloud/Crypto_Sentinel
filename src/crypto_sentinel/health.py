from __future__ import annotations

import time
from collections import deque
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any


def _elapsed_seconds(now_monotonic: float, last_monotonic: float | None) -> float | None:
    if last_monotonic is None:
        return None
    return round(max(0.0, now_monotonic - last_monotonic), 2)


@dataclass(slots=True)
class SymbolHealth:
    exchange: str
    symbol: str
    last_message_ms: int | None = None
    last_message_monotonic: float | None = None
    messages: int = 0
    trades: int = 0
    liquidations: int = 0
    seen_since_connect: bool = False

    def to_dict(self, now_monotonic: float) -> dict[str, Any]:
        return {
            "exchange": self.exchange,
            "symbol": self.symbol,
            "last_message_ms": self.last_message_ms,
            "message_age_seconds": _elapsed_seconds(now_monotonic, self.last_message_monotonic),
            "messages": self.messages,
            "trades": self.trades,
            "liquidations": self.liquidations,
            "seen_since_connect": self.seen_since_connect,
        }


@dataclass(slots=True)
class FeedHealth:
    exchange: str
    connected: bool = False
    started_ms: int = 0
    connected_at_ms: int | None = None
    disconnected_at_ms: int | None = None
    last_message_ms: int | None = None
    last_message_monotonic: float | None = None
    last_error: str = ""
    reconnects: int = 0
    messages: int = 0
    trades: int = 0
    liquidations: int = 0
    dropped: int = 0
    expected_topics: list[str] = field(default_factory=list)
    acknowledged_topics: list[str] = field(default_factory=list)
    subscription_acknowledged: bool = False
    continuity_break_started_ms: int | None = None
    continuity_break_reason: str = ""

    def to_dict(
        self,
        now_monotonic: float,
        symbols: Mapping[str, SymbolHealth],
    ) -> dict[str, Any]:
        symbol_payload = {
            symbol: health.to_dict(now_monotonic) for symbol, health in symbols.items()
        }
        symbol_ages = [item["message_age_seconds"] for item in symbol_payload.values()]
        all_symbols_seen = bool(symbol_payload) and all(
            item["seen_since_connect"] for item in symbol_payload.values()
        )
        # A multiplexed public feed has transport-level liveness, while an
        # individual symbol may simply be quiet. Keep symbol ages visible, but
        # retain message_age_seconds as the conservative per-symbol diagnostic.
        # latest_message_age_seconds is the feed-wide transport-liveness age.
        if symbol_payload:
            message_age = (
                max(symbol_ages)
                if all_symbols_seen and all(age is not None for age in symbol_ages)
                else None
            )
        else:
            message_age = _elapsed_seconds(now_monotonic, self.last_message_monotonic)
        observed_symbol_ages = [age for age in symbol_ages if age is not None]
        return {
            "exchange": self.exchange,
            "connected": self.connected,
            "started_ms": self.started_ms,
            "connected_at_ms": self.connected_at_ms,
            "disconnected_at_ms": self.disconnected_at_ms,
            "last_message_ms": self.last_message_ms,
            "last_error": self.last_error,
            "reconnects": self.reconnects,
            "messages": self.messages,
            "trades": self.trades,
            "liquidations": self.liquidations,
            "dropped": self.dropped,
            "message_age_seconds": message_age,
            "latest_message_age_seconds": _elapsed_seconds(
                now_monotonic, self.last_message_monotonic
            ),
            "oldest_symbol_message_age_seconds": (
                max(observed_symbol_ages) if observed_symbol_ages else None
            ),
            "all_symbols_seen_since_connect": all_symbols_seen,
            "expected_topics": list(self.expected_topics),
            "acknowledged_topics": list(self.acknowledged_topics),
            "subscription_acknowledged": self.subscription_acknowledged,
            "continuity_break_active": self.continuity_break_started_ms is not None,
            "continuity_break_started_ms": self.continuity_break_started_ms,
            "continuity_break_reason": self.continuity_break_reason,
            "symbols": symbol_payload,
        }


@dataclass(slots=True, frozen=True)
class ContinuityEvent:
    exchange: str
    phase: str
    timestamp_ms: int
    reason: str
    symbol: str | None = None


@dataclass(slots=True)
class TaskHealth:
    name: str
    heartbeat_timeout_seconds: float
    started_ms: int
    started_monotonic: float
    last_heartbeat_ms: int
    last_heartbeat_monotonic: float
    state: str = "starting"
    ready: bool = False
    last_error: str = ""

    def to_dict(self, now_ms: int, now_monotonic: float) -> dict[str, Any]:
        heartbeat_age = max(0.0, now_monotonic - self.last_heartbeat_monotonic)
        heartbeat_stale = (
            self.state in {"starting", "running"} and heartbeat_age > self.heartbeat_timeout_seconds
        )
        return {
            "name": self.name,
            "state": self.state,
            "ready": self.ready,
            "started_ms": self.started_ms,
            "last_heartbeat_ms": self.last_heartbeat_ms,
            "heartbeat_age_seconds": round(heartbeat_age, 2),
            "heartbeat_timeout_seconds": self.heartbeat_timeout_seconds,
            "heartbeat_stale": heartbeat_stale,
            "last_error": self.last_error,
        }


class HealthRegistry:
    def __init__(
        self,
        exchanges: Iterable[str],
        symbols_by_exchange: Mapping[str, Iterable[str]] | None = None,
    ) -> None:
        now_ms = int(time.time() * 1000)
        names = list(exchanges)
        self._health = {name: FeedHealth(exchange=name, started_ms=now_ms) for name in names}
        symbol_mapping = symbols_by_exchange or {}
        self._symbols: dict[str, dict[str, SymbolHealth]] = {
            name: {
                symbol: SymbolHealth(exchange=name, symbol=symbol)
                for symbol in dict.fromkeys(symbol_mapping.get(name, ()))
            }
            for name in names
        }
        self._tasks: dict[str, TaskHealth] = {}
        self._queue_enqueued_at: deque[float] = deque()
        self._queue_size = 0
        self._queue_capacity = 0
        self._queue_high_water = 0
        self._queue_drops = 0
        self._queue_last_drop_ms: int | None = None
        self._last_consumer_ms: int | None = None
        self._last_consumer_lag_seconds = 0.0
        self._max_consumer_lag_seconds = 0.0
        self._continuity_events: deque[ContinuityEvent] = deque()
        self._continuity_generation = {name: 0 for name in names}
        self._continuity_generation_started_ms = {name: 0 for name in names}
        self._continuity_pending_symbols: dict[str, set[str]] = {name: set() for name in names}

    @staticmethod
    def _timestamp_ms(timestamp_ms: int | None) -> int:
        return int(time.time() * 1000) if timestamp_ms is None else int(timestamp_ms)

    def begin_continuity_break(
        self,
        exchange: str,
        reason: str,
        *,
        timestamp_ms: int | None = None,
    ) -> None:
        health = self._health[exchange]
        started_ms = self._timestamp_ms(timestamp_ms)
        self._continuity_generation[exchange] += 1
        self._continuity_generation_started_ms[exchange] = started_ms
        self._continuity_pending_symbols[exchange] = set(self._symbols[exchange])
        if health.continuity_break_started_ms is None:
            health.continuity_break_started_ms = started_ms
            health.continuity_break_reason = reason[:120]
        else:
            reasons = sorted(set(health.continuity_break_reason.split(",")) | {reason[:120]})
            health.continuity_break_reason = ",".join(value for value in reasons if value)[:120]
        self._continuity_events.append(
            ContinuityEvent(
                exchange=exchange,
                phase="start",
                timestamp_ms=started_ms,
                reason=health.continuity_break_reason,
            )
        )

    def end_continuity_break(
        self,
        exchange: str,
        *,
        timestamp_ms: int | None = None,
    ) -> None:
        health = self._health[exchange]
        if health.continuity_break_started_ms is None:
            return
        ended_ms = max(
            health.continuity_break_started_ms + 1,
            self._timestamp_ms(timestamp_ms),
        )
        for symbol in sorted(self._continuity_pending_symbols[exchange]):
            self._continuity_events.append(
                ContinuityEvent(
                    exchange=exchange,
                    phase="end",
                    timestamp_ms=ended_ms,
                    reason=health.continuity_break_reason,
                    symbol=symbol,
                )
            )
        self._continuity_pending_symbols[exchange].clear()
        health.continuity_break_started_ms = None
        health.continuity_break_reason = ""

    def continuity_generation(self, exchange: str) -> int:
        return self._continuity_generation[exchange]

    def accepted_trade(
        self,
        exchange: str,
        symbol: str,
        *,
        continuity_generation: int,
        event_timestamp_ms: int,
        received_wall_ms: int,
    ) -> bool:
        health = self._health[exchange]
        started_ms = self._continuity_generation_started_ms[exchange]
        if (
            health.continuity_break_started_ms is None
            or continuity_generation != self._continuity_generation[exchange]
            or symbol not in self._continuity_pending_symbols[exchange]
            or event_timestamp_ms < started_ms
        ):
            return False
        self._continuity_pending_symbols[exchange].remove(symbol)
        self._continuity_events.append(
            ContinuityEvent(
                exchange=exchange,
                phase="end",
                timestamp_ms=max(started_ms + 1, int(received_wall_ms)),
                reason=health.continuity_break_reason,
                symbol=symbol,
            )
        )
        if not self._continuity_pending_symbols[exchange]:
            health.continuity_break_started_ms = None
            health.continuity_break_reason = ""
        return True

    def drain_continuity_events(self) -> list[ContinuityEvent]:
        events = list(self._continuity_events)
        self._continuity_events.clear()
        return events

    def connected(self, exchange: str, *, timestamp_ms: int | None = None) -> None:
        health = self._health[exchange]
        now_ms = self._timestamp_ms(timestamp_ms)
        health.connected = True
        health.connected_at_ms = now_ms
        health.last_error = ""
        # Feed-level freshness is connection-scoped. A payload from the old
        # socket must never make a new, not-yet-observed connection look live.
        health.last_message_ms = None
        health.last_message_monotonic = None
        health.acknowledged_topics = []
        health.subscription_acknowledged = not health.expected_topics
        for symbol in self._symbols[exchange].values():
            # Symbol freshness is connection-scoped too. Retain cumulative
            # counters, but do not expose an old socket's timestamp beside
            # seen_since_connect=False after reconnect.
            symbol.last_message_ms = None
            symbol.last_message_monotonic = None
            symbol.seen_since_connect = False
        self.task_heartbeat(f"feed-{exchange}")

    def disconnected(
        self,
        exchange: str,
        error: str = "",
        *,
        timestamp_ms: int | None = None,
    ) -> None:
        health = self._health[exchange]
        now_ms = self._timestamp_ms(timestamp_ms)
        health.connected = False
        health.subscription_acknowledged = False
        health.disconnected_at_ms = now_ms
        if error:
            health.last_error = error[:500]
        health.reconnects += 1
        self.begin_continuity_break(
            exchange,
            "feed_disconnect",
            timestamp_ms=now_ms,
        )
        self.task_heartbeat(f"feed-{exchange}")

    def expect_subscriptions(self, exchange: str, topics: Iterable[str]) -> None:
        health = self._health[exchange]
        health.expected_topics = list(dict.fromkeys(topics))
        health.acknowledged_topics = []
        health.subscription_acknowledged = not health.expected_topics

    def acknowledge_subscriptions(
        self,
        exchange: str,
        topics: Iterable[str] | None = None,
        *,
        timestamp_ms: int | None = None,
    ) -> None:
        health = self._health[exchange]
        acknowledged = list(topics) if topics is not None else health.expected_topics
        known = dict.fromkeys(health.acknowledged_topics)
        for topic in acknowledged:
            if topic in health.expected_topics:
                known[topic] = None
        health.acknowledged_topics = list(known)
        health.subscription_acknowledged = set(health.expected_topics).issubset(
            health.acknowledged_topics
        )
        self.task_heartbeat(f"feed-{exchange}", ready=health.subscription_acknowledged)

    def message(
        self,
        exchange: str,
        event_type: str | None = None,
        symbol: str | None = None,
    ) -> None:
        health = self._health[exchange]
        now_ms = int(time.time() * 1000)
        now_monotonic = time.monotonic()
        health.last_message_ms = now_ms
        health.last_message_monotonic = now_monotonic
        health.messages += 1
        if event_type == "trade":
            health.trades += 1
        elif event_type == "liquidation":
            health.liquidations += 1
        if symbol is not None and symbol in self._symbols[exchange]:
            symbol_health = self._symbols[exchange][symbol]
            symbol_health.last_message_ms = now_ms
            symbol_health.last_message_monotonic = now_monotonic
            symbol_health.messages += 1
            symbol_health.seen_since_connect = True
            if event_type == "trade":
                symbol_health.trades += 1
            elif event_type == "liquidation":
                symbol_health.liquidations += 1
        self.task_heartbeat(f"feed-{exchange}")

    def dropped(
        self,
        exchange: str,
        queue_size: int | None = None,
        queue_capacity: int | None = None,
    ) -> None:
        now_ms = int(time.time() * 1000)
        self._health[exchange].dropped += 1
        self._queue_drops += 1
        self._queue_last_drop_ms = now_ms
        # A full queue is confirmed local data loss even if the exchange socket
        # remains healthy. Reopen every pair on that multiplexed feed and wait
        # for accepted current-generation trades before trusting returns again.
        self.begin_continuity_break(
            exchange,
            "queue_drop",
            timestamp_ms=now_ms,
        )
        self.observe_queue(queue_size, queue_capacity)

    def enqueued(self, queue_size: int, queue_capacity: int) -> None:
        self._queue_enqueued_at.append(time.monotonic())
        self.observe_queue(queue_size, queue_capacity)

    def consumed(self, queue_size: int, queue_capacity: int) -> None:
        now_monotonic = time.monotonic()
        lag = 0.0
        if self._queue_enqueued_at:
            lag = max(0.0, now_monotonic - self._queue_enqueued_at.popleft())
        self._last_consumer_lag_seconds = lag
        self._max_consumer_lag_seconds = max(self._max_consumer_lag_seconds, lag)
        self._last_consumer_ms = int(time.time() * 1000)
        self.observe_queue(queue_size, queue_capacity)
        self.task_heartbeat("event-consumer", ready=True)

    def observe_queue(
        self,
        queue_size: int | None,
        queue_capacity: int | None,
    ) -> None:
        if queue_size is not None:
            self._queue_size = max(0, queue_size)
            self._queue_high_water = max(self._queue_high_water, self._queue_size)
        if queue_capacity is not None:
            self._queue_capacity = max(0, queue_capacity)

    def queue_snapshot(
        self,
        queue_size: int | None = None,
        queue_capacity: int | None = None,
    ) -> dict[str, Any]:
        self.observe_queue(queue_size, queue_capacity)
        capacity = self._queue_capacity
        utilization = self._queue_size / capacity if capacity else 0.0
        high_water_utilization = self._queue_high_water / capacity if capacity else 0.0
        oldest_lag = (
            max(0.0, time.monotonic() - self._queue_enqueued_at[0])
            if self._queue_enqueued_at
            else 0.0
        )
        return {
            "size": self._queue_size,
            "capacity": capacity,
            "utilization": round(utilization, 4),
            "high_water": self._queue_high_water,
            "high_water_utilization": round(high_water_utilization, 4),
            "drops_total": self._queue_drops,
            "last_drop_ms": self._queue_last_drop_ms,
            "consumer_lag_seconds": round(oldest_lag, 3),
            "last_consumer_lag_seconds": round(self._last_consumer_lag_seconds, 3),
            "max_consumer_lag_seconds": round(self._max_consumer_lag_seconds, 3),
            "last_consumer_ms": self._last_consumer_ms,
        }

    def register_task(self, name: str, heartbeat_timeout_seconds: float) -> None:
        now_ms = int(time.time() * 1000)
        now_monotonic = time.monotonic()
        self._tasks[name] = TaskHealth(
            name=name,
            heartbeat_timeout_seconds=max(1.0, heartbeat_timeout_seconds),
            started_ms=now_ms,
            started_monotonic=now_monotonic,
            last_heartbeat_ms=now_ms,
            last_heartbeat_monotonic=now_monotonic,
        )

    def task_heartbeat(self, name: str, *, ready: bool | None = None) -> None:
        task = self._tasks.get(name)
        if task is None:
            return
        task.last_heartbeat_ms = int(time.time() * 1000)
        task.last_heartbeat_monotonic = time.monotonic()
        task.state = "running"
        if ready is not None:
            task.ready = ready

    def task_failed(self, name: str, error: str) -> None:
        task = self._tasks.get(name)
        if task is None:
            return
        task.state = "failed"
        task.ready = False
        task.last_error = error[:500]

    def task_stopped(self, name: str) -> None:
        task = self._tasks.get(name)
        if task is None or task.state == "failed":
            return
        task.state = "stopped"
        task.ready = False

    def task_snapshot(self) -> dict[str, dict[str, Any]]:
        now_ms = int(time.time() * 1000)
        now_monotonic = time.monotonic()
        return {name: task.to_dict(now_ms, now_monotonic) for name, task in self._tasks.items()}

    def snapshot(self) -> dict[str, dict[str, Any]]:
        now_monotonic = time.monotonic()
        return {
            name: health.to_dict(now_monotonic, self._symbols[name])
            for name, health in self._health.items()
        }

    def get(self, exchange: str) -> FeedHealth:
        return self._health[exchange]
