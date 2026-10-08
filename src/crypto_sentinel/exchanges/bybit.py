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
    application_heartbeat_seconds = 20.0

    @staticmethod
    def validate_subscription_ack(payload: dict[str, Any]) -> bool:
        if payload.get("op") != "subscribe":
            return False
        if payload.get("success") is not True:
            detail = str(payload.get("ret_msg") or "subscription rejected")
            raise ConnectionError(f"Bybit subscription rejected: {detail}")
        return True

    async def _await_subscription_ack(self, ws: Any, topics: list[str]) -> None:
        deadline = asyncio.get_running_loop().time() + min(
            15.0, float(self.config.stale_after_seconds)
        )
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise TimeoutError("Bybit subscription acknowledgement timed out")
            message = await asyncio.wait_for(ws.receive(), timeout=remaining)
            if message.type == WSMsgType.TEXT:
                payload = json.loads(message.data)
                if self.validate_subscription_ack(payload):
                    self.health.acknowledge_subscriptions(self.name, topics)
                    return
                # Preserve a market payload that raced ahead of the ack.
                for event in self.parse_message(payload):
                    self.emit(event)
            elif message.type in {WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR}:
                raise ConnectionError(
                    f"websocket closed before Bybit subscription ack ({message.type})"
                )

    async def _send_application_heartbeat_if_due(
        self,
        ws: Any,
        deadline: float,
    ) -> float:
        now = self._monotonic()
        if now < deadline:
            return deadline
        # Bybit documents this JSON heartbeat for public V5 connections.
        # Send it on a fixed schedule even while active market data prevents a
        # receive timeout; protocol-level WebSocket ping/pong remains enabled.
        await ws.send_json({"op": "ping"})
        return now + self.application_heartbeat_seconds

    async def stream(self, session: ClientSession) -> None:
        if not self.symbol_map:
            raise ValueError("no Bybit symbols configured")
        args: list[str] = []
        for symbol in self.symbol_map:
            args.extend((f"publicTrade.{symbol}", f"allLiquidation.{symbol}"))
        async with session.ws_connect(self.url, heartbeat=30, autoclose=True) as ws:
            self.health.connected(self.name)
            self.health.expect_subscriptions(self.name, args)
            await ws.send_json({"op": "subscribe", "args": args})
            await self._await_subscription_ack(ws, args)
            self.start_market_watchdog()
            next_application_heartbeat = self._monotonic() + self.application_heartbeat_seconds
            self.log.info("connected with %d symbols", len(self.symbol_map))
            while not self.stop_event.is_set():
                heartbeat_wait = max(
                    0.05,
                    next_application_heartbeat - self._monotonic(),
                )
                try:
                    message = await asyncio.wait_for(
                        ws.receive(),
                        timeout=min(self.receive_timeout_seconds, heartbeat_wait),
                    )
                except TimeoutError:
                    next_application_heartbeat = await self._send_application_heartbeat_if_due(
                        ws,
                        next_application_heartbeat,
                    )
                    self.assert_market_flow()
                    continue
                if message.type == WSMsgType.TEXT:
                    payload = json.loads(message.data)
                    self.validate_subscription_ack(payload)
                    for event in self.parse_message(payload):
                        self.emit(event)
                    self.assert_market_flow()
                elif message.type in {WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR}:
                    break
                next_application_heartbeat = await self._send_application_heartbeat_if_due(
                    ws,
                    next_application_heartbeat,
                )

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
                if side not in {"buy", "sell"}:
                    continue
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
                if position_side not in {"buy", "sell"}:
                    continue
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
