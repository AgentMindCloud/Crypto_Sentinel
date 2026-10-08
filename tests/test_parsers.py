from __future__ import annotations

import asyncio
import json
import math
from types import SimpleNamespace
from typing import Any

import pytest
from aiohttp import WSMsgType

from crypto_sentinel.config import ExchangeConfig
from crypto_sentinel.exchanges.binance import BinanceFeed
from crypto_sentinel.exchanges.bybit import BybitFeed
from crypto_sentinel.exchanges.okx import OkxFeed
from crypto_sentinel.health import HealthRegistry
from crypto_sentinel.models import Liquidation, ReceivedMarketEvent, Trade


def _feed(feed_type):
    name = feed_type.name
    return feed_type(
        ExchangeConfig(),
        {"BTCUSDT" if name != "okx" else "BTC-USDT-SWAP": "BTCUSDT"},
        asyncio.Queue(),
        HealthRegistry([name]),
        asyncio.Event(),
    )


class _FakeWebSocket:
    def __init__(
        self,
        messages: list[Any],
        *,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        self.messages = list(messages)
        self.stop_event = stop_event
        self.sent_json: list[dict[str, Any]] = []
        self.sent_text: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def receive(self):
        if not self.messages:
            raise AssertionError("fake websocket message sequence exhausted")
        message = self.messages.pop(0)
        if self.stop_event is not None and not self.messages:
            self.stop_event.set()
        return message

    async def send_json(self, payload):
        self.sent_json.append(payload)

    async def send_str(self, payload):
        self.sent_text.append(payload)


class _FakeSession:
    def __init__(self, ws: _FakeWebSocket) -> None:
        self.ws = ws

    def ws_connect(self, *_args, **_kwargs):
        return self.ws


def _text(payload: dict[str, Any] | str):
    data = payload if isinstance(payload, str) else json.dumps(payload)
    return SimpleNamespace(type=WSMsgType.TEXT, data=data)


def test_binance_aggregate_trade_parser() -> None:
    feed = _feed(BinanceFeed)
    event = feed.parse_message(
        {
            "stream": "btcusdt@aggTrade",
            "data": {
                "e": "aggTrade",
                "E": 1720000000001,
                "s": "BTCUSDT",
                "a": 12345,
                "p": "100000.0",
                "q": "0.20",
                "T": 1720000000000,
                "m": True,
            },
        }
    )
    assert isinstance(event, Trade)
    assert event.taker_side == "sell"
    assert event.quote_notional == 20_000


def test_base_feed_stamps_receipt_clock_and_current_continuity_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = asyncio.Queue()
    health = HealthRegistry(["binance"], {"binance": ["BTCUSDT"]})
    feed = BinanceFeed(
        ExchangeConfig(),
        {"BTCUSDT": "BTCUSDT"},
        output,
        health,
        asyncio.Event(),
    )
    health.begin_continuity_break(
        "binance",
        "feed_disconnect",
        timestamp_ms=1_700_000_000_000,
    )
    expected_generation = health.continuity_generation("binance")
    monkeypatch.setattr("crypto_sentinel.exchanges.base.time.time", lambda: 1_700_000_001.25)
    feed._monotonic = lambda: 123.5
    trade = Trade(
        "binance",
        "BTCUSDT",
        1_700_000_001_000,
        100,
        1,
        "buy",
        "generation-seam",
    )

    feed.emit(trade)
    queued = output.get_nowait()

    assert isinstance(queued, ReceivedMarketEvent)
    assert queued.event is trade
    assert queued.received_wall_ms == 1_700_000_001_250
    assert queued.received_monotonic_s == 123.5
    assert queued.continuity_generation == expected_generation


def test_feed_rejects_invalid_events_before_freshness_or_queue() -> None:
    feed = _feed(BinanceFeed)
    feed._last_market_payload = {"BTCUSDT": 10.0}
    feed._monotonic = lambda: 20.0
    invalid_events = [
        Trade(
            exchange="binance",
            symbol="BTCUSDT",
            timestamp_ms=1,
            price=math.nan,
            quantity=1,
            taker_side="buy",
        ),
        Trade(
            exchange="binance",
            symbol="BTCUSDT",
            timestamp_ms=1,
            price=100,
            quantity=-1,
            taker_side="buy",
        ),
        Trade(
            exchange="binance",
            symbol="BTCUSDT",
            timestamp_ms=1,
            price=100,
            quantity=1,
            taker_side="unknown",
        ),
        Trade(
            exchange="binance",
            symbol="BTCUSDT",
            timestamp_ms=0,
            price=100,
            quantity=1,
            taker_side="buy",
        ),
    ]

    for event in invalid_events:
        feed.emit(event)

    assert feed.output.empty()
    assert feed._last_market_payload == {"BTCUSDT": 10.0}
    assert feed._last_feed_market_payload is None
    health = feed.health.snapshot()["binance"]
    assert health["messages"] == 0
    assert health["message_age_seconds"] is None


def test_one_flowing_symbol_keeps_multiplexed_feed_live_when_another_is_quiet() -> None:
    feed = BinanceFeed(
        ExchangeConfig(stale_after_seconds=10),
        {"BTCUSDT": "BTCUSDT", "ETHUSDT": "ETHUSDT"},
        asyncio.Queue(),
        HealthRegistry(["binance"], {"binance": ["BTCUSDT", "ETHUSDT"]}),
        asyncio.Event(),
    )
    clock = {"monotonic": 0.0}
    feed._monotonic = lambda: clock["monotonic"]
    feed.start_market_watchdog()
    clock["monotonic"] = 20.0
    feed.emit(
        Trade(
            exchange="binance",
            symbol="BTCUSDT",
            timestamp_ms=1,
            price=100,
            quantity=1,
            taker_side="buy",
        )
    )
    clock["monotonic"] = 25.0

    feed.assert_market_flow()
    assert feed._last_market_payload["ETHUSDT"] == 0.0
    assert feed._last_feed_market_payload == 20.0


def test_whole_feed_without_valid_market_payload_still_fails_closed() -> None:
    feed = BinanceFeed(
        ExchangeConfig(stale_after_seconds=10),
        {"BTCUSDT": "BTCUSDT", "ETHUSDT": "ETHUSDT"},
        asyncio.Queue(),
        HealthRegistry(["binance"], {"binance": ["BTCUSDT", "ETHUSDT"]}),
        asyncio.Event(),
    )
    clock = {"monotonic": 0.0}
    feed._monotonic = lambda: clock["monotonic"]
    feed.start_market_watchdog()
    clock["monotonic"] = 10.001

    with pytest.raises(TimeoutError, match="stalled for entire feed"):
        feed.assert_market_flow()


def test_binance_liquidation_parser_uses_executed_quantity() -> None:
    feed = _feed(BinanceFeed)
    event = feed.parse_message(
        {
            "stream": "btcusdt@forceOrder",
            "data": {
                "e": "forceOrder",
                "E": 1720000000001,
                "o": {
                    "s": "BTCUSDT",
                    "S": "SELL",
                    "q": "1.0",
                    "p": "99000",
                    "ap": "98950",
                    "z": "0.5",
                    "l": "0.5",
                    "T": 1720000000000,
                },
            },
        }
    )
    assert isinstance(event, Liquidation)
    assert event.liquidated_side == "long"
    assert event.quantity == 0.5
    assert event.quote_notional == 49_475


def test_binance_liquidation_prefers_last_fill_over_cumulative_fill() -> None:
    feed = _feed(BinanceFeed)
    event = feed.parse_message(
        {
            "data": {
                "e": "forceOrder",
                "E": 1720000000001,
                "o": {
                    "s": "BTCUSDT",
                    "S": "SELL",
                    "q": "2.0",
                    "p": "99000",
                    "ap": "98950",
                    "z": "1.5",
                    "l": "0.25",
                    "T": 1720000000000,
                },
            }
        }
    )
    assert isinstance(event, Liquidation)
    assert event.quantity == 0.25


def test_bybit_trade_and_liquidation_parsers() -> None:
    feed = _feed(BybitFeed)
    trades = feed.parse_message(
        {
            "topic": "publicTrade.BTCUSDT",
            "data": [
                {
                    "T": 1720000000000,
                    "s": "BTCUSDT",
                    "S": "Buy",
                    "v": "0.1",
                    "p": "100000",
                    "i": "trade-1",
                }
            ],
        }
    )
    assert len(trades) == 1
    assert isinstance(trades[0], Trade)
    assert trades[0].taker_side == "buy"

    liquidations = feed.parse_message(
        {
            "topic": "allLiquidation.BTCUSDT",
            "data": [
                {
                    "T": 1720000000100,
                    "s": "BTCUSDT",
                    "S": "Buy",
                    "v": "0.25",
                    "p": "99500",
                }
            ],
        }
    )
    assert len(liquidations) == 1
    assert isinstance(liquidations[0], Liquidation)
    assert liquidations[0].liquidated_side == "long"


def test_okx_trade_parser_applies_contract_value() -> None:
    feed = _feed(OkxFeed)
    feed.contract_values["BTC-USDT-SWAP"] = 0.01
    events = feed.parse_message(
        {
            "arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"},
            "data": [
                {
                    "instId": "BTC-USDT-SWAP",
                    "tradeId": "42",
                    "px": "100000",
                    "sz": "5",
                    "side": "sell",
                    "ts": "1720000000000",
                }
            ],
        }
    )
    assert len(events) == 1
    assert events[0].quantity == 0.05
    assert events[0].quote_notional == 5_000


def test_okx_subscription_ids_and_error_frames() -> None:
    feed = _feed(OkxFeed)
    doctor_id = f"{feed.subscription_id}doctor"
    assert feed.subscription_id.isalnum() and len(feed.subscription_id) <= 32
    assert doctor_id.isalnum() and len(doctor_id) <= 32

    with pytest.raises(ConnectionError, match="60033.*Parameter id error"):
        feed.raise_for_error({"event": "error", "code": "60033", "msg": "Parameter id error"})


def test_bybit_subscription_ack_is_fail_closed() -> None:
    feed = _feed(BybitFeed)

    assert feed.validate_subscription_ack({"op": "subscribe", "success": True, "ret_msg": ""})
    assert not feed.validate_subscription_ack({"op": "ping", "success": True, "ret_msg": "pong"})
    with pytest.raises(ConnectionError, match="permission denied"):
        feed.validate_subscription_ack(
            {
                "op": "subscribe",
                "success": False,
                "ret_msg": "permission denied",
            }
        )


def test_feed_freshness_uses_monotonic_time_not_exchange_or_wall_timestamps() -> None:
    health = HealthRegistry(["binance"], {"binance": ["BTCUSDT"]})
    health.connected("binance")
    health.message("binance", "trade", "BTCUSDT")
    # A wall-clock jump or future exchange timestamp must not create a negative
    # age that can remain falsely healthy until wall time catches up.
    health.get("binance").last_message_ms = 9_999_999_999_999
    health._symbols["binance"]["BTCUSDT"].last_message_ms = 9_999_999_999_999

    snapshot = health.snapshot()["binance"]

    assert 0 <= snapshot["message_age_seconds"] < 1
    assert 0 <= snapshot["symbols"]["BTCUSDT"]["message_age_seconds"] < 1


def test_reconnect_clears_prior_socket_freshness_until_new_payload() -> None:
    health = HealthRegistry(["binance"], {"binance": ["BTCUSDT"]})
    health.connected("binance")
    health.expect_subscriptions("binance", ["trades"])
    health.acknowledge_subscriptions("binance", ["trades"])
    health.message("binance", "trade", "BTCUSDT")
    assert health.snapshot()["binance"]["latest_message_age_seconds"] is not None

    health.disconnected("binance", "test reconnect")
    health.connected("binance")
    health.acknowledge_subscriptions("binance", ["trades"])
    reconnected = health.snapshot()["binance"]

    assert reconnected["latest_message_age_seconds"] is None
    assert reconnected["message_age_seconds"] is None
    assert reconnected["oldest_symbol_message_age_seconds"] is None
    assert reconnected["symbols"]["BTCUSDT"]["message_age_seconds"] is None
    assert not reconnected["symbols"]["BTCUSDT"]["seen_since_connect"]

    health.message("binance", "trade", "BTCUSDT")
    recovered = health.snapshot()["binance"]
    assert recovered["latest_message_age_seconds"] is not None
    assert recovered["symbols"]["BTCUSDT"]["seen_since_connect"]


async def test_okx_requires_ack_for_every_requested_topic() -> None:
    feed = OkxFeed(
        ExchangeConfig(),
        {
            "BTC-USDT-SWAP": "BTCUSDT",
            "ETH-USDT-SWAP": "ETHUSDT",
        },
        asyncio.Queue(),
        HealthRegistry(["okx"], {"okx": ["BTCUSDT", "ETHUSDT"]}),
        asyncio.Event(),
    )
    topics = ["trades:BTC-USDT-SWAP", "trades:ETH-USDT-SWAP"]
    feed.health.connected("okx")
    feed.health.expect_subscriptions("okx", topics)
    ws = _FakeWebSocket(
        [
            _text(
                {
                    "id": feed.subscription_id,
                    "event": "subscribe",
                    "arg": {
                        "channel": "trades",
                        "instId": "BTC-USDT-SWAP",
                    },
                }
            ),
            SimpleNamespace(type=WSMsgType.CLOSED, data=""),
        ]
    )

    with pytest.raises(ConnectionError, match="before OKX subscription acks"):
        await feed._await_subscription_acks(ws, topics)

    health = feed.health.snapshot()["okx"]
    assert not health["subscription_acknowledged"]
    assert health["acknowledged_topics"] == ["trades:BTC-USDT-SWAP"]


async def test_okx_pong_does_not_mask_stalled_market_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop_event = asyncio.Event()
    config = ExchangeConfig(stale_after_seconds=10)
    feed = OkxFeed(
        config,
        {"BTC-USDT-SWAP": "BTCUSDT"},
        asyncio.Queue(),
        HealthRegistry(["okx"], {"okx": ["BTCUSDT"]}),
        stop_event,
    )
    ws = _FakeWebSocket(
        [
            _text(
                {
                    "id": feed.subscription_id,
                    "event": "subscribe",
                    "arg": {
                        "channel": "trades",
                        "instId": "BTC-USDT-SWAP",
                    },
                }
            ),
            _text("pong"),
        ]
    )
    ticks = iter((0.0, 20.0))
    monkeypatch.setattr(feed, "_monotonic", lambda: next(ticks))

    async def no_metadata(_session) -> None:
        return None

    monkeypatch.setattr(feed, "_load_contract_values", no_metadata)

    with pytest.raises(TimeoutError, match="valid market payload stalled for entire feed"):
        await feed.stream(_FakeSession(ws))  # type: ignore[arg-type]
    assert feed._last_feed_market_payload == 0.0


async def test_bybit_ack_marks_all_topics_ready() -> None:
    stop_event = asyncio.Event()
    feed = BybitFeed(
        ExchangeConfig(),
        {"BTCUSDT": "BTCUSDT"},
        asyncio.Queue(),
        HealthRegistry(["bybit"], {"bybit": ["BTCUSDT"]}),
        stop_event,
    )
    topics = ["publicTrade.BTCUSDT", "allLiquidation.BTCUSDT"]
    feed.health.connected("bybit")
    feed.health.expect_subscriptions("bybit", topics)
    ws = _FakeWebSocket([_text({"op": "subscribe", "success": True, "ret_msg": ""})])

    await feed._await_subscription_ack(ws, topics)

    health = feed.health.snapshot()["bybit"]
    assert health["subscription_acknowledged"]
    assert health["acknowledged_topics"] == topics


async def test_bybit_application_heartbeat_is_sent_on_fixed_schedule() -> None:
    feed = _feed(BybitFeed)
    ws = _FakeWebSocket([])
    now = 100.0
    feed._monotonic = lambda: now

    unchanged = await feed._send_application_heartbeat_if_due(ws, 101.0)
    assert unchanged == 101.0
    assert ws.sent_json == []

    next_deadline = await feed._send_application_heartbeat_if_due(ws, 100.0)
    assert ws.sent_json == [{"op": "ping"}]
    assert next_deadline == 120.0
