from __future__ import annotations

import abc
import asyncio
import contextlib
import logging
import math
import random
import time
from collections.abc import Mapping

from aiohttp import ClientSession, ClientTimeout

from crypto_sentinel.config import ExchangeConfig
from crypto_sentinel.health import HealthRegistry
from crypto_sentinel.models import Liquidation, MarketEvent, ReceivedMarketEvent, Trade


class BaseFeed(abc.ABC):
    name: str

    def __init__(
        self,
        config: ExchangeConfig,
        symbol_map: Mapping[str, str],
        output: asyncio.Queue[MarketEvent | ReceivedMarketEvent],
        health: HealthRegistry,
        stop_event: asyncio.Event,
    ) -> None:
        self.config = config
        self.symbol_map = dict(symbol_map)
        self.output = output
        self.health = health
        self.stop_event = stop_event
        self.log = logging.getLogger(f"crypto_sentinel.exchange.{self.name}")
        self._last_market_payload: dict[str, float] = {}
        self._last_feed_market_payload: float | None = None
        self._monotonic = time.monotonic

    async def run(self) -> None:
        backoff = self.config.reconnect_min_seconds
        timeout = ClientTimeout(total=None, connect=20, sock_connect=20, sock_read=None)
        async with ClientSession(timeout=timeout) as session:
            while not self.stop_event.is_set():
                self.health.task_heartbeat(f"feed-{self.name}")
                connected_started = time.monotonic()
                try:
                    await self.stream(session)
                    if not self.stop_event.is_set():
                        raise ConnectionError("websocket closed")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # reconnect boundary
                    self.health.disconnected(self.name, f"{type(exc).__name__}: {exc}")
                    self.log.warning("feed disconnected: %s", exc)
                    # Do not carry an old exponential backoff penalty across a connection that
                    # was healthy for a meaningful period. This keeps recovery fast after an
                    # exchange's routine 24-hour disconnect or a brief network interruption.
                    stable_seconds = max(30.0, self.config.stale_after_seconds * 2.0)
                    if time.monotonic() - connected_started >= stable_seconds:
                        backoff = self.config.reconnect_min_seconds
                    delay = min(backoff, self.config.reconnect_max_seconds)
                    delay *= random.uniform(0.85, 1.15)
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(self.stop_event.wait(), timeout=delay)
                    backoff = min(backoff * 2, self.config.reconnect_max_seconds)
                else:
                    backoff = self.config.reconnect_min_seconds

    def start_market_watchdog(self) -> None:
        now = self._monotonic()
        self._last_market_payload = {
            symbol: now for symbol in dict.fromkeys(self.symbol_map.values())
        }
        self._last_feed_market_payload = now

    def assert_market_flow(self) -> None:
        now = self._monotonic()
        if self._last_feed_market_payload is None:
            raise RuntimeError("market payload watchdog was not started")
        if now - self._last_feed_market_payload > self.config.stale_after_seconds:
            raise TimeoutError("valid market payload stalled for entire feed")

    @property
    def receive_timeout_seconds(self) -> float:
        return max(1.0, min(20.0, self.config.stale_after_seconds / 3))

    @staticmethod
    def _positive_finite(value: object) -> bool:
        return (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(value)
            and value > 0
        )

    def _valid_event(self, event: MarketEvent) -> bool:
        if (
            event.exchange != self.name
            or event.symbol not in self.symbol_map.values()
            or not isinstance(event.timestamp_ms, int)
            or isinstance(event.timestamp_ms, bool)
            or event.timestamp_ms <= 0
            or not self._positive_finite(event.price)
            or not self._positive_finite(event.quantity)
        ):
            return False
        if isinstance(event, Trade):
            return event.taker_side in {"buy", "sell"}
        if isinstance(event, Liquidation):
            return event.liquidated_side in {"long", "short"}
        return False

    def emit(self, event: MarketEvent) -> None:
        if not self._valid_event(event):
            self.log.warning(
                "discarding semantically invalid %s payload for %s",
                type(event).__name__,
                getattr(event, "symbol", "unknown"),
            )
            return
        event_type = "trade" if isinstance(event, Trade) else "liquidation"
        received_wall_ms = int(time.time() * 1000)
        received_monotonic_s = self._monotonic()
        queued_event = ReceivedMarketEvent(
            event=event,
            received_wall_ms=received_wall_ms,
            received_monotonic_s=received_monotonic_s,
            continuity_generation=self.health.continuity_generation(self.name),
        )
        self._last_market_payload[event.symbol] = received_monotonic_s
        self._last_feed_market_payload = received_monotonic_s
        # Record feed freshness even if the downstream queue is full. A queue
        # drop is a separate, alarm-worthy loss condition; it must not be
        # misreported as an exchange socket outage.
        self.health.message(self.name, event_type, event.symbol)
        try:
            self.output.put_nowait(queued_event)
        except asyncio.QueueFull:
            self.health.dropped(self.name, self.output.qsize(), self.output.maxsize)
            self.log.error("market-event queue full; dropping %s", type(event).__name__)
            return
        self.health.enqueued(self.output.qsize(), self.output.maxsize)

    @abc.abstractmethod
    async def stream(self, session: ClientSession) -> None:
        raise NotImplementedError
