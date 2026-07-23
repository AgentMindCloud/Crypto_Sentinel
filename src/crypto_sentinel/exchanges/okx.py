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

    async def stream(self, session: ClientSession) -> None:
        if not self.symbol_map:
            raise ValueError("no OKX symbols configured")
        await self._load_contract_values(session)
        args = [{"channel": "trades", "instId": symbol} for symbol in self.symbol_map]
        async with session.ws_connect(self.url, heartbeat=None, autoclose=True) as ws:
            await ws.send_json({"id": self.subscription_id, "op": "subscribe", "args": args})
            self.health.connected(self.name)
            self.log.info("connected with %d symbols", len(self.symbol_map))
            while not self.stop_event.is_set():
                try:
                    message = await asyncio.wait_for(ws.receive(), timeout=20)
                except TimeoutError:
                    await ws.send_str("ping")
                    continue
                if message.type == WSMsgType.TEXT:
                    if message.data == "pong":
                        continue
                    payload = json.loads(message.data)
                    self.raise_for_error(payload)
                    for event in self.parse_message(payload):
                        self.emit(event)
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
