from __future__ import annotations

import asyncio
import json
from typing import Any

from aiohttp import ClientSession, WSMsgType

from crypto_sentinel.exchanges.base import BaseFeed
from crypto_sentinel.models import Liquidation, Trade


class BybitFeed(BaseFeed):
    name = "bybit"
    url = "wss://stream.bybit.com/v5/public/linear"

    async def stream(self, session: ClientSession) -> None:
        if not self.symbol_map:
            raise ValueError("no Bybit symbols configured")
        args: list[str] = []
        for symbol in self.symbol_map:
            args.extend((f"publicTrade.{symbol}", f"allLiquidation.{symbol}"))
        async with session.ws_connect(self.url, heartbeat=30, autoclose=True) as ws:
            await ws.send_json({"op": "subscribe", "args": args})
            self.health.connected(self.name)
            self.log.info("connected with %d symbols", len(self.symbol_map))
            while not self.stop_event.is_set():
                try:
                    message = await asyncio.wait_for(ws.receive(), timeout=20)
                except TimeoutError:
                    await ws.send_json({"op": "ping"})
                    continue
                if message.type == WSMsgType.TEXT:
                    payload = json.loads(message.data)
                    for event in self.parse_message(payload):
                        self.emit(event)
                elif message.type in {WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR}:
                    break

    def parse_message(self, payload: dict[str, Any]) -> list[Trade | Liquidation]:
        topic = str(payload.get("topic", ""))
        data = payload.get("data")
        if not isinstance(data, list):
            return []
        events: list[Trade | Liquidation] = []

        if topic.startswith("publicTrade."):
            for item in data:
                if not isinstance(item, dict):
                    continue
                exchange_symbol = str(item.get("s", "")).upper()
                canonical = self.symbol_map.get(exchange_symbol)
                if not canonical:
                    continue
                timestamp_ms = int(item["T"])
                side = str(item.get("S", "")).lower()
                trade_id = (
                    item.get("i")
                    or item.get("seq")
                    or f"{timestamp_ms}:{item.get('p')}:{item.get('v')}"
                )
                events.append(
                    Trade(
                        exchange=self.name,
                        symbol=canonical,
                        timestamp_ms=timestamp_ms,
                        price=float(item["p"]),
                        quantity=float(item["v"]),
                        taker_side="buy" if side == "buy" else "sell",
                        event_id=f"bybit:trade:{exchange_symbol}:{trade_id}",
                    )
                )
            return events

        if topic.startswith("allLiquidation."):
            for index, item in enumerate(data):
                if not isinstance(item, dict):
                    continue
                exchange_symbol = str(item.get("s", "")).upper()
                canonical = self.symbol_map.get(exchange_symbol)
                if not canonical:
                    continue
                timestamp_ms = int(item["T"])
                position_side = str(item.get("S", "")).lower()
                # Bybit documents S as the liquidated position side: Buy means a long was liquidated.
                liquidated_side = "long" if position_side == "buy" else "short"
                event_id = (
                    f"bybit:liq:{exchange_symbol}:{timestamp_ms}:{position_side}:"
                    f"{item.get('p')}:{item.get('v')}:{index}"
                )
                events.append(
                    Liquidation(
                        exchange=self.name,
                        symbol=canonical,
                        timestamp_ms=timestamp_ms,
                        price=float(item["p"]),
                        quantity=float(item["v"]),
                        liquidated_side=liquidated_side,
                        event_id=event_id,
                    )
                )
        return events
