from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import statistics
import time
import webbrowser
from typing import Any

from crypto_sentinel.alerting import AlertDispatcher
from crypto_sentinel.checkpoint import StateCheckpoint
from crypto_sentinel.config import AppConfig
from crypto_sentinel.dashboard import DashboardServer, EventBroker
from crypto_sentinel.detector import AnomalyDetector
from crypto_sentinel.exchanges import BinanceFeed, BybitFeed, OkxFeed
from crypto_sentinel.health import HealthRegistry
from crypto_sentinel.models import Alert, Liquidation, MarketEvent, Severity, Trade
from crypto_sentinel.persistence import Database
from crypto_sentinel.state import MarketState


class SentinelApp:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.log = logging.getLogger("crypto_sentinel.app")
        self.started_ms = int(time.time() * 1000)
        self.stop_event = asyncio.Event()
        self.queue: asyncio.Queue[MarketEvent] = asyncio.Queue(maxsize=config.runtime.queue_size)
        self.enabled_exchanges = [
            name
            for name in ("binance", "bybit", "okx")
            if getattr(config.exchanges, name).enabled and config.symbol_map(name)
        ]
        self.health = HealthRegistry(self.enabled_exchanges)
        max_window = max(item.seconds for item in config.detector.windows)
        retention = config.detector.baseline_seconds + max_window * 3
        self.state = MarketState(config.detector.bucket_seconds, retention)
        self.detector = AnomalyDetector(config, self.state)
        self.database = Database(config.storage.database)
        self.checkpoint = StateCheckpoint(
            config.storage.checkpoint,
            config.storage.checkpoint_max_age_seconds,
        )
        self.broker = EventBroker()
        self.dispatcher = AlertDispatcher(config, self.database, self.broker.publish)
        self.dashboard: DashboardServer | None = None
        if config.dashboard.enabled:
            self.dashboard = DashboardServer(
                config.dashboard,
                self.database,
                self.broker,
                self.status,
                self.ingest_external,
                self.test_alert,
            )
        self._tasks: list[asyncio.Task[Any]] = []
        self._feed_problem: dict[str, bool] = {name: False for name in self.enabled_exchanges}

    def status(self) -> dict[str, Any]:
        now_ms = int(time.time() * 1000)
        shortest_window = min(item.seconds for item in self.config.detector.windows)
        markets: list[dict[str, Any]] = []
        for symbol in self.config.canonical_symbols():
            configured_venues = sum(
                symbol in self.config.symbol_map(exchange).values()
                for exchange in self.enabled_exchanges
            )
            metrics = [
                item
                for item in self.detector.last_metrics
                if item.symbol == symbol and item.window_seconds == shortest_window
            ]
            prices = self.state.latest_prices(
                symbol, self.config.detector.freshness_seconds, now_ms
            )
            returns = [item.return_bps for item in metrics if item.return_bps is not None]
            return_scores = [abs(item.return_z) for item in metrics if item.return_z is not None]
            volume_scores = [item.volume_z for item in metrics if item.volume_z is not None]
            imbalances = [
                item.taker_imbalance for item in metrics if item.taker_imbalance is not None
            ]
            markets.append(
                {
                    "symbol": symbol,
                    "window_seconds": shortest_window,
                    "price": statistics.median(prices.values()) if prices else None,
                    "venues_with_price": len(prices),
                    "ready_venues": sum(item.ready for item in metrics),
                    "configured_venues": configured_venues,
                    "median_return_bps": statistics.median(returns) if returns else None,
                    "max_abs_return_z": max(return_scores, default=None),
                    "max_volume_z": max(volume_scores, default=None),
                    "median_taker_imbalance": (
                        statistics.median(imbalances) if imbalances else None
                    ),
                }
            )
        return {
            "started_ms": self.started_ms,
            "uptime_seconds": round((now_ms - self.started_ms) / 1000, 1),
            "queue_size": self.queue.qsize(),
            "queue_capacity": self.config.runtime.queue_size,
            "symbols": self.config.canonical_symbols(),
            "windows_seconds": [item.seconds for item in self.config.detector.windows],
            "feeds": self.health.snapshot(),
            "stale_after_seconds": {
                name: getattr(self.config.exchanges, name).stale_after_seconds
                for name in self.enabled_exchanges
            },
            "markets": markets,
        }

    async def ingest_external(self, payload: dict[str, Any]) -> Alert:
        alert = Alert.from_external(payload, int(time.time() * 1000))
        await self.dispatcher.emit(alert)
        return alert

    async def test_alert(self, severity_raw: str) -> Alert:
        severity = Severity.CRITICAL if severity_raw == "critical" else Severity.WARNING
        now_ms = int(time.time() * 1000)
        alert = Alert(
            severity=severity,
            category="system_test",
            symbol="TEST",
            title=f"{severity.value.title()} alarm test",
            message="This is a user-triggered end-to-end alarm test. No market event occurred.",
            timestamp_ms=now_ms,
            direction="mixed",
            exchanges=["local"],
            metrics={"test": True},
            dedup_key=f"test:{now_ms}",
            source="dashboard",
        )
        await self.dispatcher.emit(alert, bypass_cooldown=True)
        return alert

    def request_stop(self) -> None:
        self.stop_event.set()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):
                loop.add_signal_handler(sig, self.request_stop)

    def _feeds(self) -> list[Any]:
        feeds: list[Any] = []
        classes = {"binance": BinanceFeed, "bybit": BybitFeed, "okx": OkxFeed}
        for name in self.enabled_exchanges:
            feeds.append(
                classes[name](
                    getattr(self.config.exchanges, name),
                    self.config.symbol_map(name),
                    self.queue,
                    self.health,
                    self.stop_event,
                )
            )
        return feeds

    async def run(self) -> None:
        self._install_signal_handlers()
        await self.database.initialize()
        allowed_pairs = {
            (exchange, symbol)
            for exchange in self.enabled_exchanges
            for symbol in self.config.canonical_symbols()
            if symbol in self.config.symbol_map(exchange).values()
        }
        if self.checkpoint.enabled:
            await self.checkpoint.load(
                self.state,
                int(time.time() * 1000),
                allowed_pairs,
            )
        await self.dispatcher.start()
        if self.dashboard:
            if (
                self.config.dashboard.host not in {"127.0.0.1", "localhost", "::1"}
                and not self.config.dashboard.access_token
            ):
                self.log.warning(
                    "dashboard is non-local with no access token; do not expose it publicly"
                )
            await self.dashboard.start()
            if self.config.dashboard.open_browser and self.config.dashboard.host in {
                "127.0.0.1",
                "localhost",
                "::1",
            }:
                url = f"http://127.0.0.1:{self.config.dashboard.port}/"
                if self.config.dashboard.access_token:
                    url += f"?token={self.config.dashboard.access_token}"
                asyncio.create_task(asyncio.to_thread(webbrowser.open, url))

        self._tasks = [
            asyncio.create_task(self._consume_events(), name="event-consumer"),
            asyncio.create_task(self._detector_loop(), name="detector"),
            asyncio.create_task(self._health_loop(), name="health-monitor"),
            asyncio.create_task(self._maintenance_loop(), name="maintenance"),
            asyncio.create_task(self._checkpoint_loop(), name="state-checkpoint"),
        ]
        self._tasks.extend(
            asyncio.create_task(feed.run(), name=f"feed-{feed.name}") for feed in self._feeds()
        )
        self.log.info(
            "started with exchanges=%s symbols=%s",
            ",".join(self.enabled_exchanges),
            ",".join(self.config.canonical_symbols()),
        )
        try:
            await self.stop_event.wait()
        finally:
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            if self.checkpoint.enabled:
                try:
                    await self.checkpoint.save(self.state, int(time.time() * 1000))
                except Exception as exc:
                    self.log.error("final state checkpoint failed: %s", exc)
            if self.dashboard:
                await self.dashboard.stop()
            await self.dispatcher.close()
            self.log.info("stopped")

    async def _consume_events(self) -> None:
        while True:
            event = await self.queue.get()
            try:
                if isinstance(event, Trade):
                    self.state.record_trade(event)
                elif isinstance(event, Liquidation):
                    self.state.record_liquidation(event)
            except Exception:
                self.log.exception("market event processing failed for %s", type(event).__name__)
            finally:
                self.queue.task_done()

    async def _detector_loop(self) -> None:
        interval = self.config.detector.evaluate_every_seconds
        last_metric_save = 0.0
        while True:
            await asyncio.sleep(interval)
            now_ms = int(time.time() * 1000)
            try:
                alerts, snapshots = self.detector.evaluate(now_ms)
                for alert in alerts:
                    await self.dispatcher.emit(alert)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.log.exception("detector evaluation failed")
                continue
            now = time.monotonic()
            if now - last_metric_save >= self.config.storage.metric_sample_seconds:
                try:
                    await self.database.save_metrics(snapshots)
                except Exception:
                    self.log.exception("metric persistence failed")
                else:
                    last_metric_save = now

    async def _health_loop(self) -> None:
        await asyncio.sleep(self.config.runtime.startup_grace_seconds)
        while True:
            await self._check_feed_health(int(time.time() * 1000))
            await asyncio.sleep(self.config.runtime.health_check_seconds)

    async def _check_feed_health(self, now_ms: int) -> None:
        snapshot = self.health.snapshot()
        for exchange in self.enabled_exchanges:
            health = snapshot[exchange]
            stale_after = getattr(self.config.exchanges, exchange).stale_after_seconds
            age = health["message_age_seconds"]
            unhealthy = not health["connected"] or age is None or age > stale_after
            if unhealthy:
                severe = age is not None and age > stale_after * 3
                severity = Severity.CRITICAL if severe else Severity.WARNING
                reason = (
                    "disconnected"
                    if not health["connected"]
                    else f"no market message for {age:.0f}s"
                )
                alert = Alert(
                    severity=severity,
                    category="feed_health",
                    symbol="SYSTEM",
                    title=f"{exchange.title()} feed unhealthy",
                    message=f"{exchange} is {reason}. Market alarms may be incomplete.",
                    timestamp_ms=now_ms,
                    direction="mixed",
                    exchanges=[exchange],
                    metrics=health,
                    dedup_key=f"health:{exchange}:unhealthy",
                )
                # The first failure after a recovery is a new incident and must not be
                # suppressed by the previous incident's cooldown. Repeated checks during the
                # same incident remain deduplicated; a warning-to-critical upgrade still passes.
                first_problem_in_episode = not self._feed_problem[exchange]
                await self.dispatcher.emit(alert, bypass_cooldown=first_problem_in_episode)
                self._feed_problem[exchange] = True
            elif self._feed_problem[exchange]:
                recovery = Alert(
                    severity=Severity.INFO,
                    category="feed_recovery",
                    symbol="SYSTEM",
                    title=f"{exchange.title()} feed recovered",
                    message=f"{exchange} market data is flowing again.",
                    timestamp_ms=now_ms,
                    direction="mixed",
                    exchanges=[exchange],
                    metrics=health,
                    dedup_key=f"health:{exchange}:recovered:{now_ms}",
                )
                await self.dispatcher.emit(recovery, bypass_cooldown=True)
                self._feed_problem[exchange] = False

    async def _maintenance_loop(self) -> None:
        while True:
            await asyncio.sleep(6 * 3600)
            cutoff_ms = (
                int(time.time() * 1000) - self.config.storage.retain_metric_days * 86_400_000
            )
            try:
                deleted = await self.database.prune_metrics(cutoff_ms)
            except Exception:
                self.log.exception("metric pruning failed")
            else:
                self.log.info("pruned %d old metric rows", deleted)

    async def _checkpoint_loop(self) -> None:
        if not self.checkpoint.enabled:
            return
        while True:
            await asyncio.sleep(self.config.storage.checkpoint_seconds)
            try:
                await self.checkpoint.save(self.state, int(time.time() * 1000))
            except Exception as exc:
                self.log.error("state checkpoint failed: %s", exc)
