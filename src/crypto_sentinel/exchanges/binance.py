from __future__ import annotations

import asyncio
import json
from typing import Any

from aiohttp import ClientSession, WSMsgType

from crypto_sentinel.exchanges.base import BaseFeed
from crypto_sentinel.models import Liquidation, Trade


class BinanceFeed(BaseFeed):
    name = "binance"
    base_url = "wss://fstream.binance.com/market/stream?streams="

    def _url(self) -> str:
        streams: list[str] = []
        for symbol in self.symbol_map:
            lower = symbol.lower()
            streams.extend((f"{lower}@aggTrade", f"{lower}@forceOrder"))
        return self.base_url + "/".join(streams)

    async def stream(self, session: ClientSession) -> None:
        if not self.symbol_map:
            raise ValueError("no Binance symbols configured")
        topics = [
            topic
            for symbol in self.symbol_map
            for topic in (
                f"{symbol.lower()}@aggTrade",
                f"{symbol.lower()}@forceOrder",
            )
        ]
        async with session.ws_connect(self._url(), heartbeat=120, autoclose=True) as ws:
            self.health.connected(self.name)
            # Binance combined-stream subscriptions are encoded in the URL and
            # do not have a separate acknowledgement frame.
            self.health.expect_subscriptions(self.name, topics)
            self.health.acknowledge_subscriptions(self.name)
            self.start_market_watchdog()
            self.log.info("connected with %d symbols", len(self.symbol_map))
            while not self.stop_event.is_set():
                try:
                    message = await asyncio.wait_for(
                        ws.receive(), timeout=self.receive_timeout_seconds
                    )
                except TimeoutError:
                    self.assert_market_flow()
                    continue
                if message.type == WSMsgType.TEXT:
                    payload = json.loads(message.data)
                    event = self.parse_message(payload)
                    if event is not None:
                        self.emit(event)
                    self.assert_market_flow()
                elif message.type in {WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR}:
                    break

    def parse_message(self, payload: dict[str, Any]) -> Trade | Liquidation | None:
        data = payload.get("data", payload)
        if not isinstance(data, dict):
            return None
        event_type = data.get("e")
        exchange_symbol = str(data.get("s") or data.get("o", {}).get("s") or "").upper()
        canonical = self.symbol_map.get(exchange_symbol)
        if not canonical:
            return None

        if event_type == "aggTrade":
            price = float(data["p"])
            quantity = float(data["q"])
            timestamp_ms = int(data.get("T") or data.get("E"))
            buyer_is_maker = data.get("m")
            if not isinstance(buyer_is_maker, bool):
                return None
            event_id = f"binance:trade:{exchange_symbol}:{data.get('a', timestamp_ms)}"
            return Trade(
                exchange=self.name,
                symbol=canonical,
                timestamp_ms=timestamp_ms,
                price=price,
                quantity=quantity,
                taker_side="sell" if buyer_is_maker else "buy",
                event_id=event_id,
            )

        if event_type == "forceOrder":
            order = data.get("o")
            if not isinstance(order, dict):
                return None
            timestamp_ms = int(order.get("T") or data.get("E"))
            side = str(order.get("S", "")).upper()
            average_price = float(order.get("ap") or 0)
            order_price = float(order.get("p") or 0)
            price = average_price if average_price > 0 else order_price
            accumulated = float(order.get("z") or 0)
            last_filled = float(order.get("l") or 0)
            original = float(order.get("q") or 0)
            # The force-order stream is a snapshot. Prefer the last filled quantity so repeated
            # snapshots do not sum an order's cumulative fill more than once. Fall back to the
            # accumulated/original quantities for payloads where Binance omits the last fill.
            quantity = (
                last_filled if last_filled > 0 else (accumulated if accumulated > 0 else original)
            )
            if price <= 0 or quantity <= 0:
                return None
            liquidated_side = "long" if side == "SELL" else "short"
            event_id = (
                f"binance:liq:{exchange_symbol}:{timestamp_ms}:{side}:"
                f"{order.get('l', order.get('z', order.get('q', '0')))}:"
                f"{order.get('ap', order.get('p', '0'))}"
            )
            return Liquidation(
                exchange=self.name,
                symbol=canonical,
                timestamp_ms=timestamp_ms,
                price=price,
                quantity=quantity,
                liquidated_side=liquidated_side,
                event_id=event_id,
            )
        return None
