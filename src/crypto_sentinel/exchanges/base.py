from __future__ import annotations

import abc
import asyncio
import contextlib
import logging
import random
import time
from collections.abc import Mapping

from aiohttp import ClientSession, ClientTimeout

from crypto_sentinel.config import ExchangeConfig
from crypto_sentinel.health import HealthRegistry
from crypto_sentinel.models import MarketEvent


class BaseFeed(abc.ABC):
    name: str

    def __init__(
        self,
        config: ExchangeConfig,
        symbol_map: Mapping[str, str],
        output: asyncio.Queue[MarketEvent],
        health: HealthRegistry,
        stop_event: asyncio.Event,
    ) -> None:
        self.config = config
        self.symbol_map = dict(symbol_map)
        self.output = output
        self.health = health
        self.stop_event = stop_event
        self.log = logging.getLogger(f"crypto_sentinel.exchange.{self.name}")

    async def run(self) -> None:
        backoff = self.config.reconnect_min_seconds
        timeout = ClientTimeout(total=None, connect=20, sock_connect=20, sock_read=None)
        async with ClientSession(timeout=timeout) as session:
            while not self.stop_event.is_set():
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

    def emit(self, event: MarketEvent) -> None:
        try:
            self.output.put_nowait(event)
        except asyncio.QueueFull:
            self.health.dropped(self.name)
            self.log.error("market-event queue full; dropping %s", type(event).__name__)
            return
        event_type = "trade" if event.__class__.__name__ == "Trade" else "liquidation"
        self.health.message(self.name, event_type)

    @abc.abstractmethod
    async def stream(self, session: ClientSession) -> None:
        raise NotImplementedError
