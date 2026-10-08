from __future__ import annotations

import asyncio
import json
from typing import Any

from aiohttp import ClientSession, WSMsgType

from crypto_sentinel.exchanges.base import BaseFeed
from crypto_sentinel.models import Trade


class OkxFeed(BaseFeed):
    name = "okx"
    url = "wss://ws.okx.com:8443/ws/v5/public"
    instruments_url = "https://www.okx.com/api/v5/public/instruments"
    subscription_id = "cryptosentinel"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.contract_values: dict[str, float] = {symbol: 1.0 for symbol in self.symbol_map}

    async def _load_contract_values(self, session: ClientSession) -> None:
        for symbol in self.symbol_map:
            try:
                async with session.get(
                    self.instruments_url, params={"instType": "SWAP", "instId": symbol}
                ) as response:
                    response.raise_for_status()
                    payload = await response.json()
                rows = payload.get("data", [])
                if not rows:
                    raise ValueError("instrument metadata missing")
                row = rows[0]
                contract_value = float(row.get("ctVal") or 1) * float(row.get("ctMult") or 1)
                if contract_value <= 0:
                    raise ValueError("invalid contract value")
                self.contract_values[symbol] = contract_value
            except Exception as exc:
                # Relative volume statistics still work with a constant unit scale, but the
                # displayed quote notional can be wrong until metadata becomes available.
                self.log.warning("could not load %s contract value: %s", symbol, exc)

    @staticmethod
    def topic_key(arg: dict[str, Any]) -> str:
        return f"{arg.get('channel', '')}:{str(arg.get('instId', '')).upper()}"

    def validate_subscription_ack(self, payload: dict[str, Any]) -> str | None:
        self.raise_for_error(payload)
        if payload.get("event") != "subscribe":
            return None
        response_id = payload.get("id")
        if response_id is not None and response_id != self.subscription_id:
            raise ConnectionError(f"OKX subscription acknowledgement id mismatch: {response_id}")
        arg = payload.get("arg")
        if not isinstance(arg, dict):
            raise ConnectionError("OKX subscription acknowledgement missing arg")
        if arg.get("channel") != "trades" or not arg.get("instId"):
            raise ConnectionError(f"unexpected OKX subscription acknowledgement: {arg}")
        return self.topic_key(arg)

    async def _await_subscription_acks(self, ws: Any, expected_topics: list[str]) -> None:
        pending = set(expected_topics)
        deadline = asyncio.get_running_loop().time() + min(
            15.0, float(self.config.stale_after_seconds)
        )
        while pending:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                missing = ", ".join(sorted(pending))
                raise TimeoutError(f"OKX subscription acknowledgement timed out for: {missing}")
            message = await asyncio.wait_for(ws.receive(), timeout=remaining)
            if message.type == WSMsgType.TEXT:
                if message.data == "pong":
                    continue
                payload = json.loads(message.data)
                topic = self.validate_subscription_ack(payload)
                if topic is not None:
                    if topic not in pending:
                        raise ConnectionError(
                            f"unexpected or duplicate OKX subscription ack: {topic}"
                        )
                    pending.remove(topic)
                    self.health.acknowledge_subscriptions(self.name, [topic])
                    continue
                # Preserve a market payload that raced ahead of its ack.
                for event in self.parse_message(payload):
                    self.emit(event)
            elif message.type in {WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR}:
                raise ConnectionError(
                    f"websocket closed before OKX subscription acks ({message.type})"
                )

    async def stream(self, session: ClientSession) -> None:
        if not self.symbol_map:
            raise ValueError("no OKX symbols configured")
        await self._load_contract_values(session)
        args = [{"channel": "trades", "instId": symbol} for symbol in self.symbol_map]
        topics = [self.topic_key(arg) for arg in args]
        async with session.ws_connect(self.url, heartbeat=None, autoclose=True) as ws:
            self.health.connected(self.name)
            self.health.expect_subscriptions(self.name, topics)
            await ws.send_json({"id": self.subscription_id, "op": "subscribe", "args": args})
            await self._await_subscription_acks(ws, topics)
            self.start_market_watchdog()
            self.log.info("connected with %d symbols", len(self.symbol_map))
            while not self.stop_event.is_set():
                try:
                    message = await asyncio.wait_for(
                        ws.receive(), timeout=self.receive_timeout_seconds
                    )
                except TimeoutError:
                    await ws.send_str("ping")
                    self.assert_market_flow()
                    continue
                if message.type == WSMsgType.TEXT:
                    if message.data == "pong":
                        self.assert_market_flow()
                        continue
                    payload = json.loads(message.data)
                    self.validate_subscription_ack(payload)
                    for event in self.parse_message(payload):
                        self.emit(event)
                    self.assert_market_flow()
                elif message.type in {WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR}:
                    break

    @staticmethod
    def raise_for_error(payload: dict[str, Any]) -> None:
        if payload.get("event") != "error":
            return
        code = str(payload.get("code", "unknown"))
        message = str(payload.get("msg", "subscription rejected"))
        raise ConnectionError(f"OKX error {code}: {message}")

    def parse_message(self, payload: dict[str, Any]) -> list[Trade]:
        arg = payload.get("arg")
        data = payload.get("data")
        if (
            not isinstance(arg, dict)
            or arg.get("channel") != "trades"
            or not isinstance(data, list)
        ):
            return []
        exchange_symbol = str(arg.get("instId", "")).upper()
        canonical = self.symbol_map.get(exchange_symbol)
        if not canonical:
            return []
        events: list[Trade] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            timestamp_ms = int(item["ts"])
            side = str(item.get("side", "")).lower()
            if side not in {"buy", "sell"}:
                continue
            trade_id = item.get("tradeId") or f"{timestamp_ms}:{item.get('px')}:{item.get('sz')}"
            events.append(
                Trade(
                    exchange=self.name,
                    symbol=canonical,
                    timestamp_ms=timestamp_ms,
                    price=float(item["px"]),
                    quantity=float(item["sz"]) * self.contract_values.get(exchange_symbol, 1.0),
                    taker_side="buy" if side == "buy" else "sell",
                    event_id=f"okx:trade:{exchange_symbol}:{trade_id}",
                )
            )
        return events
