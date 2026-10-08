from __future__ import annotations

import asyncio
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from aiohttp import ClientSession, ClientTimeout, WSMsgType

from crypto_sentinel.config import AppConfig
from crypto_sentinel.exchanges.binance import BinanceFeed
from crypto_sentinel.exchanges.bybit import BybitFeed
from crypto_sentinel.exchanges.okx import OkxFeed
from crypto_sentinel.health import HealthRegistry


def _check(name: str, ok: bool, detail: str, **extra: Any) -> dict[str, Any]:
    return {"name": name, "ok": ok, "detail": detail, **extra}


def _writable_parent(path_raw: str, label: str) -> dict[str, Any]:
    path = Path(path_raw)
    parent = path.parent
    try:
        parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(prefix=".sentinel-write-test-", dir=parent, delete=True):
            pass
    except OSError as exc:
        return _check(label, False, f"not writable: {exc}", path=str(path))
    return _check(label, True, "writable", path=str(path))


def local_checks(config: AppConfig) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    checks.append(
        _check(
            "python",
            sys.version_info >= (3, 11),
            sys.version.split()[0],
        )
    )
    checks.append(_writable_parent(config.storage.database, "database_path"))
    if config.storage.checkpoint:
        checks.append(_writable_parent(config.storage.checkpoint, "checkpoint_path"))
    if config.runtime.log_file:
        checks.append(_writable_parent(config.runtime.log_file, "log_path"))

    enabled_exchanges = [
        name
        for name in ("binance", "bybit", "okx")
        if getattr(config.exchanges, name).enabled and config.symbol_map(name)
    ]
    checks.append(
        _check(
            "exchange_configuration",
            bool(enabled_exchanges),
            ", ".join(enabled_exchanges)
            if enabled_exchanges
            else "no enabled exchange has symbols",
        )
    )

    notifier_names: list[str] = []
    if config.notifiers.local.enabled:
        notifier_names.append("local")
    if config.notifiers.ntfy.enabled:
        notifier_names.append("ntfy")
    if config.notifiers.telegram.enabled:
        notifier_names.append("telegram")
    if config.notifiers.webhook.enabled:
        notifier_names.append("webhook")
    checks.append(
        _check(
            "alert_delivery",
            bool(notifier_names),
            ", ".join(notifier_names) if notifier_names else "no delivery channel enabled",
        )
    )

    ntfy = config.notifiers.ntfy
    if ntfy.enabled:
        valid_topic = bool(ntfy.topic) and "replace" not in ntfy.topic.lower()
        checks.append(
            _check(
                "ntfy_configuration",
                valid_topic,
                "topic set" if valid_topic else "topic missing/placeholder",
            )
        )
    telegram = config.notifiers.telegram
    if telegram.enabled:
        valid_telegram = bool(telegram.bot_token and telegram.chat_id)
        checks.append(
            _check(
                "telegram_configuration",
                valid_telegram,
                "credentials set" if valid_telegram else "bot token or chat ID missing",
            )
        )
    webhook = config.notifiers.webhook
    if webhook.enabled:
        checks.append(
            _check(
                "webhook_configuration",
                bool(webhook.url),
                "URL set" if webhook.url else "URL missing",
            )
        )
    return checks


async def _receive_json(ws: Any, timeout: float) -> dict[str, Any]:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise TimeoutError("no market payload received before timeout")
        message = await asyncio.wait_for(ws.receive(), timeout=remaining)
        if message.type == WSMsgType.TEXT:
            if message.data == "pong":
                continue
            payload = json.loads(message.data)
            if isinstance(payload, dict):
                return payload
        elif message.type in {WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR}:
            raise ConnectionError(f"websocket closed with type {message.type}")


async def _probe_binance(
    config: AppConfig, session: ClientSession, timeout: float
) -> dict[str, Any]:
    mapping = config.symbol_map("binance")
    if not mapping:
        return _check("binance_live", False, "no Binance symbol configured")
    feed = BinanceFeed(
        config.exchanges.binance,
        mapping,
        asyncio.Queue(),
        HealthRegistry(["binance"], {"binance": mapping.values()}),
        asyncio.Event(),
    )
    started = time.monotonic()
    deadline = asyncio.get_running_loop().time() + timeout
    expected_symbols = set(mapping.values())
    observed_symbols: set[str] = set()
    try:
        async with session.ws_connect(feed._url(), heartbeat=120, autoclose=True) as ws:
            while observed_symbols != expected_symbols:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    missing = ", ".join(sorted(expected_symbols - observed_symbols))
                    raise TimeoutError(f"no Binance trade payload for: {missing}")
                payload = await _receive_json(ws, remaining)
                event = feed.parse_message(payload)
                if event is not None and event.__class__.__name__ == "Trade":
                    observed_symbols.add(event.symbol)
            return _check(
                "binance_live",
                True,
                f"parsed trades for all {len(expected_symbols)} symbol(s)",
                latency_ms=round((time.monotonic() - started) * 1000),
                symbols=sorted(observed_symbols),
                topics_encoded=len(mapping) * 2,
                subscription_mode="combined-stream URL",
            )
    except Exception as exc:
        return _check("binance_live", False, f"{type(exc).__name__}: {exc}")


async def _probe_bybit(config: AppConfig, session: ClientSession, timeout: float) -> dict[str, Any]:
    mapping = config.symbol_map("bybit")
    if not mapping:
        return _check("bybit_live", False, "no Bybit symbol configured")
    feed = BybitFeed(
        config.exchanges.bybit,
        mapping,
        asyncio.Queue(),
        HealthRegistry(["bybit"], {"bybit": mapping.values()}),
        asyncio.Event(),
    )
    started = time.monotonic()
    deadline = asyncio.get_running_loop().time() + timeout
    expected_symbols = set(mapping.values())
    observed_symbols: set[str] = set()
    topics = [
        topic
        for exchange_symbol in mapping
        for topic in (
            f"publicTrade.{exchange_symbol}",
            f"allLiquidation.{exchange_symbol}",
        )
    ]
    acknowledged = False
    try:
        async with session.ws_connect(feed.url, heartbeat=30, autoclose=True) as ws:
            await ws.send_json({"op": "subscribe", "args": topics})
            while not acknowledged or observed_symbols != expected_symbols:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    missing = ", ".join(sorted(expected_symbols - observed_symbols))
                    if not acknowledged:
                        raise TimeoutError("Bybit subscription acknowledgement timed out")
                    raise TimeoutError(f"no Bybit trade payload for: {missing}")
                payload = await _receive_json(ws, remaining)
                if feed.validate_subscription_ack(payload):
                    acknowledged = True
                    continue
                events = feed.parse_message(payload)
                observed_symbols.update(
                    event.symbol for event in events if event.__class__.__name__ == "Trade"
                )
            return _check(
                "bybit_live",
                True,
                (
                    f"acknowledged {len(topics)} topic(s) and parsed trades "
                    f"for all {len(expected_symbols)} symbol(s)"
                ),
                latency_ms=round((time.monotonic() - started) * 1000),
                symbols=sorted(observed_symbols),
                acknowledged_topics=topics,
            )
    except Exception as exc:
        return _check("bybit_live", False, f"{type(exc).__name__}: {exc}")


async def _probe_okx(config: AppConfig, session: ClientSession, timeout: float) -> dict[str, Any]:
    mapping = config.symbol_map("okx")
    if not mapping:
        return _check("okx_live", False, "no OKX symbol configured")
    feed = OkxFeed(
        config.exchanges.okx,
        mapping,
        asyncio.Queue(),
        HealthRegistry(["okx"], {"okx": mapping.values()}),
        asyncio.Event(),
    )
    started = time.monotonic()
    deadline = asyncio.get_running_loop().time() + timeout
    expected_symbols = set(mapping.values())
    observed_symbols: set[str] = set()
    args = [{"channel": "trades", "instId": symbol} for symbol in mapping]
    pending_topics = {feed.topic_key(arg) for arg in args}
    try:
        await feed._load_contract_values(session)
        async with session.ws_connect(feed.url, heartbeat=None, autoclose=True) as ws:
            await ws.send_json(
                {
                    "id": feed.subscription_id,
                    "op": "subscribe",
                    "args": args,
                }
            )
            while pending_topics or observed_symbols != expected_symbols:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    if pending_topics:
                        missing = ", ".join(sorted(pending_topics))
                        raise TimeoutError(
                            f"OKX subscription acknowledgement timed out for: {missing}"
                        )
                    missing = ", ".join(sorted(expected_symbols - observed_symbols))
                    raise TimeoutError(f"no OKX trade payload for: {missing}")
                payload = await _receive_json(ws, remaining)
                topic = feed.validate_subscription_ack(payload)
                if topic is not None:
                    if topic not in pending_topics:
                        raise ConnectionError(
                            f"unexpected or duplicate OKX subscription ack: {topic}"
                        )
                    pending_topics.remove(topic)
                    continue
                events = feed.parse_message(payload)
                observed_symbols.update(event.symbol for event in events)
            return _check(
                "okx_live",
                True,
                (
                    f"acknowledged {len(args)} topic(s) and parsed trades "
                    f"for all {len(expected_symbols)} symbol(s)"
                ),
                latency_ms=round((time.monotonic() - started) * 1000),
                symbols=sorted(observed_symbols),
                acknowledged_topics=[feed.topic_key(arg) for arg in args],
                contract_values=dict(feed.contract_values),
            )
    except Exception as exc:
        return _check("okx_live", False, f"{type(exc).__name__}: {exc}")


async def network_checks(config: AppConfig, timeout: float) -> list[dict[str, Any]]:
    client_timeout = ClientTimeout(total=timeout + 5, connect=min(8, timeout))
    async with ClientSession(timeout=client_timeout) as session:
        probes = []
        if config.exchanges.binance.enabled and config.symbol_map("binance"):
            probes.append(_probe_binance(config, session, timeout))
        if config.exchanges.bybit.enabled and config.symbol_map("bybit"):
            probes.append(_probe_bybit(config, session, timeout))
        if config.exchanges.okx.enabled and config.symbol_map("okx"):
            probes.append(_probe_okx(config, session, timeout))
        if not probes:
            return []
        return list(await asyncio.gather(*probes))


async def run_doctor(
    config: AppConfig,
    *,
    timeout: float = 12.0,
    include_network: bool = True,
) -> dict[str, Any]:
    checks = local_checks(config)
    if include_network:
        checks.extend(await network_checks(config, timeout))
    failed = [item for item in checks if not item["ok"]]
    return {
        "ok": not failed,
        "checks": checks,
        "failed": len(failed),
        "pid": os.getpid(),
    }
