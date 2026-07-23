from __future__ import annotations

import asyncio

import pytest

from crypto_sentinel.config import ExchangeConfig
from crypto_sentinel.exchanges.binance import BinanceFeed
from crypto_sentinel.exchanges.bybit import BybitFeed
from crypto_sentinel.exchanges.okx import OkxFeed
from crypto_sentinel.health import HealthRegistry
from crypto_sentinel.models import Liquidation, Trade


def _feed(feed_type):
    name = feed_type.name
    return feed_type(
        ExchangeConfig(),
        {"BTCUSDT" if name != "okx" else "BTC-USDT-SWAP": "BTCUSDT"},
        asyncio.Queue(),
        HealthRegistry([name]),
        asyncio.Event(),
    )


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
