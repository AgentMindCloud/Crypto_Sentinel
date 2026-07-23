from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(slots=True)
class FeedHealth:
    exchange: str
    connected: bool = False
    started_ms: int = 0
    connected_at_ms: int | None = None
    disconnected_at_ms: int | None = None
    last_message_ms: int | None = None
    last_error: str = ""
    reconnects: int = 0
    messages: int = 0
    trades: int = 0
    liquidations: int = 0
    dropped: int = 0

    def to_dict(self, now_ms: int | None = None) -> dict[str, Any]:
        now = now_ms or int(time.time() * 1000)
        payload = asdict(self)
        payload["message_age_seconds"] = (
            round((now - self.last_message_ms) / 1000, 2) if self.last_message_ms else None
        )
        return payload


class HealthRegistry:
    def __init__(self, exchanges: list[str]) -> None:
        now_ms = int(time.time() * 1000)
        self._health = {name: FeedHealth(exchange=name, started_ms=now_ms) for name in exchanges}

    def connected(self, exchange: str) -> None:
        health = self._health[exchange]
        now_ms = int(time.time() * 1000)
        health.connected = True
        health.connected_at_ms = now_ms
        health.last_error = ""

    def disconnected(self, exchange: str, error: str = "") -> None:
        health = self._health[exchange]
        health.connected = False
        health.disconnected_at_ms = int(time.time() * 1000)
        if error:
            health.last_error = error[:500]
        health.reconnects += 1

    def message(self, exchange: str, event_type: str | None = None) -> None:
        health = self._health[exchange]
        health.last_message_ms = int(time.time() * 1000)
        health.messages += 1
        if event_type == "trade":
            health.trades += 1
        elif event_type == "liquidation":
            health.liquidations += 1

    def dropped(self, exchange: str) -> None:
        self._health[exchange].dropped += 1

    def snapshot(self) -> dict[str, dict[str, Any]]:
        now_ms = int(time.time() * 1000)
        return {name: health.to_dict(now_ms) for name, health in self._health.items()}

    def get(self, exchange: str) -> FeedHealth:
        return self._health[exchange]
