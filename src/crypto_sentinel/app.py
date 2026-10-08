from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import signal
import statistics
import time
import webbrowser
from pathlib import Path
from typing import Any

from crypto_sentinel.alerting import AlertDispatcher
from crypto_sentinel.checkpoint import StateCheckpoint
from crypto_sentinel.config import AppConfig
from crypto_sentinel.dashboard import AlarmDeliveryUnavailable, DashboardServer, EventBroker
from crypto_sentinel.detector import AnomalyDetector
from crypto_sentinel.exchanges import BinanceFeed, BybitFeed, OkxFeed
from crypto_sentinel.health import HealthRegistry
from crypto_sentinel.models import (
    Alert,
    Liquidation,
    MarketEvent,
    MetricSnapshot,
    ReceivedMarketEvent,
    Severity,
    Trade,
)
from crypto_sentinel.persistence import Database
from crypto_sentinel.state import MarketState


class SentinelApp:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.log = logging.getLogger("crypto_sentinel.app")
        self.started_ms = int(time.time() * 1000)
        self._integration_last_evaluation_ms: int | None = None
        self._started_monotonic = time.monotonic()
        self.stop_event = asyncio.Event()
        self.queue: asyncio.Queue[MarketEvent | ReceivedMarketEvent] = asyncio.Queue(
            maxsize=config.runtime.queue_size
        )
        self.enabled_exchanges = [
            name
            for name in ("binance", "bybit", "okx")
            if getattr(config.exchanges, name).enabled and config.symbol_map(name)
        ]
        symbols_by_exchange = {
            exchange: list(dict.fromkeys(config.symbol_map(exchange).values()))
            for exchange in self.enabled_exchanges
        }
        self.health = HealthRegistry(self.enabled_exchanges, symbols_by_exchange)
        max_window = max(item.seconds for item in config.detector.windows)
        retention = config.detector.baseline_seconds + max_window * 3
        self.state = MarketState(
            config.detector.bucket_seconds,
            retention,
            max_future_skew_seconds=config.detector.max_future_skew_seconds,
        )
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
        self._queue_problem = False
        self._queue_drops_reported = 0
        self._last_queue_drop_delta = 0
        self._fatal_task_error = ""
        self._checkpoint_last_success_ms: int | None = None
        self._checkpoint_last_success_monotonic: float | None = None
        self._checkpoint_last_failure_ms: int | None = None
        self._checkpoint_last_failure_monotonic: float | None = None
        self._checkpoint_last_error = ""
        self._checkpoint_latest_attempt_succeeded: bool | None = None
        self._continuity_episode_started_ms: int | None = None
        self._continuity_episode_pairs: set[str] = set()
        self._continuity_episode_metrics: set[str] = set()
        self._continuity_episode_min_coverage = 1.0
        self._continuity_episode_max_gap = 0.0
        self._continuity_episode_max_recovery = 0.0
        self._expected_task_names = {
            "event-consumer",
            "detector",
            "health-monitor",
            "maintenance",
            "alert-workers",
            *(f"feed-{name}" for name in self.enabled_exchanges),
        }
        if self.checkpoint.enabled:
            self._expected_task_names.add("state-checkpoint")

    def _deployment_status(self) -> dict[str, Any] | None:
        database_path = Path(self.config.storage.database)
        status_path = database_path.with_name("supervisor-status.json")
        try:
            stat = status_path.stat()
            age_seconds = max(0.0, time.time() - stat.st_mtime)
            stale_after = max(120.0, float(self.config.runtime.health_check_seconds * 4))
            if age_seconds > stale_after or stat.st_size > 64 * 1024:
                return None
            payload = json.loads(status_path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                return None
            # Only expose the documented operational fields. The supervisor
            # file is local, but status is still an authenticated web response.
            deployment = {
                key: payload.get(key)
                for key in (
                    "timestamp_utc",
                    "supervisor_pid",
                    "startup",
                    "primary",
                    "shadow",
                    "last_alarm_utc",
                )
                if key in payload
            }
            deployment["status_age_seconds"] = round(age_seconds, 1)
            return deployment
        except (OSError, ValueError, json.JSONDecodeError):
            return None

    def _checkpoint_status(self) -> dict[str, Any]:
        if not self.checkpoint.enabled or self.checkpoint.path is None:
            return {
                "enabled": False,
                "healthy": True,
                "reason": "disabled",
            }
        now_monotonic = time.monotonic()
        elapsed = max(0.0, now_monotonic - self._started_monotonic)
        stale_after = max(
            float(self.config.storage.checkpoint_seconds * 3),
            float(self.config.runtime.startup_grace_seconds),
        )
        success_age = (
            max(
                0.0,
                now_monotonic - self._checkpoint_last_success_monotonic,
            )
            if self._checkpoint_last_success_monotonic is not None
            else None
        )
        try:
            stat = self.checkpoint.path.stat()
            path_exists = True
            path_age = max(0.0, time.time() - stat.st_mtime)
        except OSError:
            path_exists = False
            path_age = None

        latest_attempt_failed = self._checkpoint_latest_attempt_succeeded is False
        if latest_attempt_failed:
            healthy = False
            reason = "last checkpoint save failed"
        elif success_age is None:
            healthy = elapsed <= stale_after
            reason = (
                "awaiting first checkpoint"
                if healthy
                else "no successful checkpoint within startup allowance"
            )
        elif success_age > stale_after:
            healthy = False
            reason = f"last checkpoint is {success_age:.0f}s old"
        elif not path_exists:
            healthy = False
            reason = "checkpoint file is missing"
        else:
            healthy = True
            reason = "current"
        return {
            "enabled": True,
            "healthy": healthy,
            "reason": reason,
            "path": str(self.checkpoint.path),
            "stale_after_seconds": stale_after,
            "last_success_ms": self._checkpoint_last_success_ms,
            "last_success_age_seconds": (
                round(success_age, 2) if success_age is not None else None
            ),
            "last_failure_ms": self._checkpoint_last_failure_ms,
            "last_failure_error": self._checkpoint_last_error or None,
            "path_exists": path_exists,
            "path_age_seconds": round(path_age, 2) if path_age is not None else None,
            "within_initial_allowance": (
                self._checkpoint_last_success_ms is None and elapsed <= stale_after
            ),
        }

    def _readiness(
        self,
        feeds: dict[str, dict[str, Any]],
        tasks: dict[str, dict[str, Any]],
        queue_health: dict[str, Any],
        checkpoint_health: dict[str, Any],
        alarm_delivery: dict[str, Any],
    ) -> dict[str, Any]:
        reasons: list[str] = []
        for exchange in self.enabled_exchanges:
            feed = feeds[exchange]
            stale_after = getattr(self.config.exchanges, exchange).stale_after_seconds
            feed_reasons: list[str] = []
            if not feed["connected"]:
                feed_reasons.append("disconnected")
            if not feed["subscription_acknowledged"]:
                feed_reasons.append("subscriptions not acknowledged")
            if feed["continuity_break_active"]:
                feed_reasons.append("waiting for fresh post-break trades")
            feed_age = feed["latest_message_age_seconds"]
            if (
                feed["connected"]
                and feed["subscription_acknowledged"]
                and (feed_age is None or feed_age > stale_after)
            ):
                feed_reasons.append("no fresh market payload across feed")
            unseen_symbols: list[str] = []
            quiet_symbols: list[str] = []
            for symbol, symbol_health in feed["symbols"].items():
                age = symbol_health["message_age_seconds"]
                if not symbol_health["seen_since_connect"]:
                    unseen_symbols.append(symbol)
                elif age is None or age > stale_after:
                    quiet_symbols.append(symbol)
            feed["ready"] = not feed_reasons
            feed["readiness_reasons"] = feed_reasons
            feed["unhealthy_symbols"] = sorted(set(unseen_symbols) | set(quiet_symbols))
            feed["unseen_symbols"] = unseen_symbols
            feed["quiet_symbols"] = quiet_symbols
            reasons.extend(f"{exchange}: {reason}" for reason in feed_reasons)

        for name in sorted(self._expected_task_names - tasks.keys()):
            reasons.append(f"task {name} not started")
        for name, task in tasks.items():
            if task["state"] not in {"starting", "running"}:
                reasons.append(f"task {name} is {task['state']}")
            elif task["heartbeat_stale"]:
                reasons.append(f"task {name} heartbeat stale")
            elif not task["ready"]:
                reasons.append(f"task {name} not ready")

        lag_limit = max(5.0, self.config.runtime.health_check_seconds * 2.0)
        utilization = float(queue_health["utilization"])
        lag = float(queue_health["consumer_lag_seconds"])
        if utilization >= 0.8:
            reasons.append(f"market queue {utilization:.0%} full")
        if lag > lag_limit:
            reasons.append(f"market queue consumer lag {lag:.1f}s")
        if self._last_queue_drop_delta:
            reasons.append(f"{self._last_queue_drop_delta} market event(s) dropped")
        if self._fatal_task_error:
            reasons.append(self._fatal_task_error)
        if checkpoint_health["enabled"] and not checkpoint_health["healthy"]:
            reasons.append(f"checkpoint: {checkpoint_health['reason']}")
        local_alarm = alarm_delivery.get("local_alarm", {})
        last_outcome = local_alarm.get("last_outcome")
        if last_outcome is None:
            candidate = local_alarm.get("last_receipt")
            if isinstance(candidate, dict) and candidate.get("status") in {"success", "failure"}:
                last_outcome = candidate
        if (
            self.config.notifiers.local.enabled
            and isinstance(last_outcome, dict)
            and last_outcome.get("status") == "failure"
        ):
            detail = str(last_outcome.get("detail") or "delivery failed")
            reasons.append(f"local alarm: {detail}")

        unique_reasons = list(dict.fromkeys(reasons))
        ready = not unique_reasons
        return {
            "ready": ready,
            # `overall` is retained for the public health endpoint introduced
            # alongside this contract; the supervisor consumes `ready`.
            "overall": ready,
            "state": "ready" if ready else "degraded",
            "reasons": unique_reasons,
        }

    def status(self) -> dict[str, Any]:
        now_ms = int(time.time() * 1000)
        shortest_window = min(item.seconds for item in self.config.detector.windows)
        symbol_freshness_seconds = self.config.detector.freshness_seconds

        def alert_eligible(metric: MetricSnapshot) -> bool:
            age = metric.data_age_seconds
            return bool(metric.ready and age is not None and 0 <= age <= symbol_freshness_seconds)

        def effective_reason(metric: MetricSnapshot) -> str:
            if metric.ready and not alert_eligible(metric):
                return "stale_data"
            return metric.readiness_reason

        markets: list[dict[str, Any]] = []
        configured_windows = sorted(item.seconds for item in self.config.detector.windows)
        for symbol in self.config.canonical_symbols():
            configured_venue_names = [
                exchange
                for exchange in self.enabled_exchanges
                if symbol in self.config.symbol_map(exchange).values()
            ]
            configured_venues = len(configured_venue_names)
            all_metrics = [
                item
                for item in self.detector.last_metrics
                if item.symbol == symbol and item.exchange in configured_venue_names
            ]
            metrics = [item for item in all_metrics if item.window_seconds == shortest_window]
            prices = self.state.latest_prices(
                symbol, self.config.detector.freshness_seconds, now_ms
            )
            returns = [item.return_bps for item in metrics if item.return_bps is not None]
            return_scores = [abs(item.return_z) for item in metrics if item.return_z is not None]
            volume_scores = [item.volume_z for item in metrics if item.volume_z is not None]
            imbalances = [
                item.taker_imbalance for item in metrics if item.taker_imbalance is not None
            ]
            readiness_reasons = sorted(
                {effective_reason(item) for item in all_metrics if effective_reason(item)}
            )
            blocking_readiness_reasons = sorted(
                {
                    effective_reason(item)
                    for item in all_metrics
                    if not alert_eligible(item) and effective_reason(item)
                }
            )
            recovery_seconds = [item.recovery_seconds_remaining for item in all_metrics]
            coverage_ratios = [item.window_coverage_ratio for item in all_metrics]
            largest_gaps = [
                item.largest_gap_seconds
                for item in all_metrics
                if item.largest_gap_seconds is not None
            ]
            venue_readiness: dict[str, dict[str, Any]] = {}
            for exchange in configured_venue_names:
                by_window = {
                    item.window_seconds: item for item in all_metrics if item.exchange == exchange
                }
                missing_windows = [
                    window for window in configured_windows if window not in by_window
                ]
                window_details = {
                    str(window): (
                        {
                            "ready": alert_eligible(by_window[window]),
                            "base_ready": by_window[window].ready,
                            "alert_eligible": alert_eligible(by_window[window]),
                            "reason": effective_reason(by_window[window]),
                            "data_age_seconds": by_window[window].data_age_seconds,
                            "window_coverage_ratio": by_window[window].window_coverage_ratio,
                            "largest_gap_seconds": by_window[window].largest_gap_seconds,
                            "recovery_seconds_remaining": (
                                by_window[window].recovery_seconds_remaining
                            ),
                        }
                        if window in by_window
                        else {
                            "ready": False,
                            "base_ready": False,
                            "alert_eligible": False,
                            "reason": "missing_window_metric",
                            "data_age_seconds": None,
                            "window_coverage_ratio": 0.0,
                            "largest_gap_seconds": None,
                            "recovery_seconds_remaining": 0.0,
                        }
                    )
                    for window in configured_windows
                }
                venue_metrics = list(by_window.values())
                venue_ready = not missing_windows and all(
                    alert_eligible(item) for item in venue_metrics
                )
                base_ready = not missing_windows and all(item.ready for item in venue_metrics)
                venue_blocking_reasons = sorted(
                    {
                        effective_reason(item)
                        for item in venue_metrics
                        if not alert_eligible(item) and effective_reason(item)
                    }
                )
                if missing_windows:
                    venue_blocking_reasons.append("missing_window_metric")
                    blocking_readiness_reasons.append("missing_window_metric")
                    readiness_reasons.append("missing_window_metric")
                venue_ages = [
                    item.data_age_seconds
                    for item in venue_metrics
                    if item.data_age_seconds is not None
                ]
                venue_coverages = [item.window_coverage_ratio for item in venue_metrics]
                venue_gaps = [
                    item.largest_gap_seconds
                    for item in venue_metrics
                    if item.largest_gap_seconds is not None
                ]
                venue_recoveries = [item.recovery_seconds_remaining for item in venue_metrics]
                venue_readiness[exchange] = {
                    "ready": venue_ready,
                    "base_ready": base_ready,
                    "alert_eligible": venue_ready,
                    "reason": (
                        "ready"
                        if venue_ready
                        else ", ".join(dict.fromkeys(venue_blocking_reasons)) or "warming_up"
                    ),
                    "data_age_seconds": max(venue_ages, default=None),
                    "window_coverage_ratio": (
                        min(venue_coverages) if not missing_windows and venue_coverages else 0.0
                    ),
                    "largest_gap_seconds": max(venue_gaps, default=None),
                    "recovery_seconds_remaining": max(venue_recoveries, default=0.0),
                    "missing_windows_seconds": missing_windows,
                    "windows": window_details,
                }
            readiness_reasons = sorted(set(readiness_reasons))
            blocking_readiness_reasons = sorted(set(blocking_readiness_reasons))
            ready_venues = sum(bool(item["ready"]) for item in venue_readiness.values())
            markets.append(
                {
                    "symbol": symbol,
                    "window_seconds": shortest_window,
                    "windows_seconds": configured_windows,
                    "price": statistics.median(prices.values()) if prices else None,
                    "venues_with_price": len(prices),
                    "ready_venues": ready_venues,
                    "configured_venues": configured_venues,
                    "median_return_bps": statistics.median(returns) if returns else None,
                    "max_abs_return_z": max(return_scores, default=None),
                    "max_volume_z": max(volume_scores, default=None),
                    "median_taker_imbalance": (
                        statistics.median(imbalances) if imbalances else None
                    ),
                    "readiness_reason": (
                        "ready"
                        if configured_venues and ready_venues == configured_venues
                        else (
                            ", ".join(blocking_readiness_reasons)
                            if blocking_readiness_reasons
                            else "warming_up"
                        )
                    ),
                    "readiness_reasons": readiness_reasons,
                    "max_recovery_seconds_remaining": max(recovery_seconds, default=0.0),
                    "min_window_coverage_ratio": min(coverage_ratios, default=0.0),
                    "max_largest_gap_seconds": max(largest_gaps, default=None),
                    "venue_readiness": venue_readiness,
                }
            )
        feeds = self.health.snapshot()
        tasks = self.health.task_snapshot()
        queue_health = self.health.queue_snapshot(
            self.queue.qsize(), self.config.runtime.queue_size
        )
        queue_health["drops_since_last_health_check"] = self._last_queue_drop_delta
        checkpoint_health = self._checkpoint_status()
        alarm_delivery = self.dispatcher.status()
        readiness = self._readiness(
            feeds,
            tasks,
            queue_health,
            checkpoint_health,
            alarm_delivery,
        )
        deployment = self._deployment_status()
        clock_health = self.state.clock_status()
        payload = {
            "started_ms": self.started_ms,
            "integration_last_evaluation_ms": self._integration_last_evaluation_ms,
            "integration_evaluation_interval_seconds": self.config.detector.evaluate_every_seconds,
            "uptime_seconds": round((now_ms - self.started_ms) / 1000, 1),
            "queue_size": self.queue.qsize(),
            "queue_capacity": self.config.runtime.queue_size,
            "queue_high_water": queue_health["high_water"],
            "queue_drops": queue_health["drops_total"],
            "consumer_lag_seconds": queue_health["consumer_lag_seconds"],
            "symbols": self.config.canonical_symbols(),
            "windows_seconds": [item.seconds for item in self.config.detector.windows],
            "feeds": feeds,
            "symbol_health": {exchange: feed["symbols"] for exchange, feed in feeds.items()},
            "stale_after_seconds": {
                name: getattr(self.config.exchanges, name).stale_after_seconds
                for name in self.enabled_exchanges
            },
            "symbol_freshness_seconds": symbol_freshness_seconds,
            "tasks": tasks,
            "queue_health": queue_health,
            "readiness": readiness,
            "runtime_health": {
                "healthy": readiness["ready"],
                "alarm_worthy": not readiness["ready"],
                "issues": readiness["reasons"],
                "fatal_task_error": self._fatal_task_error or None,
            },
            "clock_health": clock_health,
            "alarm_delivery": alarm_delivery,
            "checkpoint_health": checkpoint_health,
            "continuity_health": {
                "recovering_after_gap": (self._continuity_episode_started_ms is not None),
                "episode_started_ms": self._continuity_episode_started_ms,
                "affected_pairs": sorted(self._continuity_episode_pairs),
                "affected_windows": sorted(self._continuity_episode_metrics),
                "min_window_coverage_ratio": (
                    self._continuity_episode_min_coverage
                    if self._continuity_episode_started_ms is not None
                    else None
                ),
                "max_largest_gap_seconds": (
                    self._continuity_episode_max_gap
                    if self._continuity_episode_started_ms is not None
                    else None
                ),
                "max_recovery_seconds_remaining": (
                    self._continuity_episode_max_recovery
                    if self._continuity_episode_started_ms is not None
                    else None
                ),
            },
            "markets": markets,
        }
        if deployment is not None:
            payload["deployment"] = deployment
        return payload

    async def ingest_external(self, payload: dict[str, Any]) -> Alert:
        alert = Alert.from_external(payload, int(time.time() * 1000))
        if not await self.dispatcher.emit(alert):
            raise AlarmDeliveryUnavailable("required local alarm admission failed")
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
        if not await self.dispatcher.emit(alert, bypass_cooldown=True):
            raise AlarmDeliveryUnavailable("required local alarm admission failed")
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

    def _create_critical_task(
        self,
        coroutine: Any,
        *,
        name: str,
        heartbeat_timeout_seconds: float,
    ) -> asyncio.Task[Any]:
        self.health.register_task(name, heartbeat_timeout_seconds)
        return asyncio.create_task(coroutine, name=name)

    async def _wait_for_stop_or_task_failure(self) -> None:
        stop_waiter = asyncio.create_task(self.stop_event.wait(), name="stop-event-waiter")
        try:
            done, _pending = await asyncio.wait(
                [stop_waiter, *self._tasks],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_waiter in done or self.stop_event.is_set():
                return
            completed = sorted(
                (task for task in done if task is not stop_waiter),
                key=lambda task: task.get_name(),
            )
            task = completed[0]
            if task.cancelled():
                detail = f"critical task {task.get_name()} was cancelled unexpectedly"
                cause: BaseException | None = None
            else:
                cause = task.exception()
                detail = f"critical task {task.get_name()} exited unexpectedly"
                if cause is not None:
                    detail += f": {type(cause).__name__}: {cause}"
            self._fatal_task_error = detail[:500]
            self.health.task_failed(task.get_name(), detail)
            self.log.critical(detail)
            self.stop_event.set()
            if cause is not None:
                raise RuntimeError(detail) from cause
            raise RuntimeError(detail)
        finally:
            stop_waiter.cancel()
            await asyncio.gather(stop_waiter, return_exceptions=True)

    async def _alert_worker_watchdog(self) -> None:
        interval = max(1.0, min(5.0, self.config.runtime.health_check_seconds / 2))
        while True:
            workers = list(self.dispatcher.worker_tasks())
            expected_names = {
                "notifier-0",
                "notifier-1",
                "local-alarm-worker",
            }
            worker_names = {worker.get_name() for worker in workers}
            missing = sorted(expected_names - worker_names)
            if missing:
                raise RuntimeError(
                    "alert dispatcher workers were not started: " + ", ".join(missing)
                )
            for worker in workers:
                if not worker.done():
                    continue
                if worker.cancelled():
                    raise RuntimeError(
                        f"alert worker {worker.get_name()} was cancelled unexpectedly"
                    )
                error = worker.exception()
                if error is None:
                    raise RuntimeError(f"alert worker {worker.get_name()} exited unexpectedly")
                raise RuntimeError(
                    f"alert worker {worker.get_name()} failed: {type(error).__name__}: {error}"
                ) from error
            self.health.task_heartbeat("alert-workers", ready=True)
            await asyncio.sleep(interval)

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
        continuity_start_ms = int(time.time() * 1000)
        for exchange in self.enabled_exchanges:
            self.health.begin_continuity_break(
                exchange,
                "process_start",
                timestamp_ms=continuity_start_ms,
            )
        self._sync_continuity_events()
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

        health_interval = self.config.runtime.health_check_seconds
        self._tasks = [
            self._create_critical_task(
                self._consume_events(),
                name="event-consumer",
                heartbeat_timeout_seconds=max(5, health_interval * 2),
            ),
            self._create_critical_task(
                self._detector_loop(),
                name="detector",
                heartbeat_timeout_seconds=max(5, self.config.detector.evaluate_every_seconds * 3),
            ),
            self._create_critical_task(
                self._health_loop(),
                name="health-monitor",
                heartbeat_timeout_seconds=(
                    self.config.runtime.startup_grace_seconds + health_interval * 2
                ),
            ),
            self._create_critical_task(
                self._maintenance_loop(),
                name="maintenance",
                heartbeat_timeout_seconds=7 * 3600,
            ),
            self._create_critical_task(
                self._alert_worker_watchdog(),
                name="alert-workers",
                heartbeat_timeout_seconds=max(10, health_interval * 2),
            ),
        ]
        if self.checkpoint.enabled:
            self._tasks.append(
                self._create_critical_task(
                    self._checkpoint_loop(),
                    name="state-checkpoint",
                    heartbeat_timeout_seconds=max(30, self.config.storage.checkpoint_seconds * 3),
                )
            )
        for feed in self._feeds():
            exchange_config = getattr(self.config.exchanges, feed.name)
            self._tasks.append(
                self._create_critical_task(
                    feed.run(),
                    name=f"feed-{feed.name}",
                    heartbeat_timeout_seconds=max(
                        exchange_config.stale_after_seconds * 2,
                        exchange_config.reconnect_max_seconds * 2 + 5,
                    ),
                )
            )
        self.log.info(
            "started with exchanges=%s symbols=%s",
            ",".join(self.enabled_exchanges),
            ",".join(self.config.canonical_symbols()),
        )
        try:
            await self._wait_for_stop_or_task_failure()
        finally:
            for task in self._tasks:
                task.cancel()
            await asyncio.gather(*self._tasks, return_exceptions=True)
            for task in self._tasks:
                self.health.task_stopped(task.get_name())
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
        heartbeat_interval = max(1.0, min(5.0, self.config.runtime.health_check_seconds / 2))
        while True:
            self.health.task_heartbeat("event-consumer", ready=True)
            try:
                queued_event = await asyncio.wait_for(
                    self.queue.get(),
                    timeout=heartbeat_interval,
                )
            except TimeoutError:
                continue
            self.health.consumed(self.queue.qsize(), self.config.runtime.queue_size)
            try:
                self._sync_continuity_events()
                if isinstance(queued_event, ReceivedMarketEvent):
                    event = queued_event.event
                else:
                    event = queued_event
                if isinstance(event, Trade):
                    if isinstance(queued_event, ReceivedMarketEvent):
                        accepted = self.state.record_trade(
                            event,
                            received_wall_ms=queued_event.received_wall_ms,
                            received_monotonic_s=(queued_event.received_monotonic_s),
                        )
                        if accepted:
                            self.health.accepted_trade(
                                event.exchange,
                                event.symbol,
                                continuity_generation=(queued_event.continuity_generation),
                                event_timestamp_ms=event.timestamp_ms,
                                received_wall_ms=queued_event.received_wall_ms,
                            )
                            self._sync_continuity_events()
                    else:
                        self.state.record_trade(event)
                elif isinstance(event, Liquidation):
                    if isinstance(queued_event, ReceivedMarketEvent):
                        self.state.record_liquidation(
                            event,
                            received_wall_ms=queued_event.received_wall_ms,
                            received_monotonic_s=(queued_event.received_monotonic_s),
                        )
                    else:
                        self.state.record_liquidation(event)
            except Exception:
                self.log.exception("market event processing failed for %s", type(event).__name__)
                self.health.task_heartbeat("event-consumer", ready=False)
                raise
            finally:
                self.queue.task_done()

    async def _detector_loop(self) -> None:
        interval = self.config.detector.evaluate_every_seconds
        last_metric_save = 0.0
        previous_wall_ms = int(time.time() * 1000)
        previous_monotonic_s = time.monotonic()
        while True:
            self.health.task_heartbeat("detector")
            await asyncio.sleep(interval)
            now_ms = int(time.time() * 1000)
            now_monotonic_s = time.monotonic()
            self._record_runtime_pause(
                previous_wall_ms,
                previous_monotonic_s,
                now_ms,
                now_monotonic_s,
            )
            previous_wall_ms = now_ms
            previous_monotonic_s = now_monotonic_s
            self._sync_continuity_events()
            try:
                alerts, snapshots = self.detector.evaluate(now_ms)
                for alert in alerts:
                    await self.dispatcher.emit(alert)
                await self._check_continuity_health(snapshots, now_ms)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.health.task_heartbeat("detector", ready=False)
                self.log.exception("detector evaluation failed")
                continue
            self._integration_last_evaluation_ms = now_ms
            self.health.task_heartbeat("detector", ready=True)
            now = time.monotonic()
            if now - last_metric_save >= self.config.storage.metric_sample_seconds:
                try:
                    await self.database.save_metrics(snapshots)
                except Exception:
                    self.log.exception("metric persistence failed")
                else:
                    last_metric_save = now

    def _sync_continuity_events(self) -> None:
        symbols_by_exchange = {
            exchange: tuple(dict.fromkeys(self.config.symbol_map(exchange).values()))
            for exchange in self.enabled_exchanges
        }
        for event in self.health.drain_continuity_events():
            symbols = (
                (event.symbol,)
                if event.symbol is not None
                else symbols_by_exchange.get(event.exchange, ())
            )
            for symbol in symbols:
                if event.phase == "start":
                    self.state.begin_continuity_break(
                        event.exchange,
                        symbol,
                        event.timestamp_ms,
                        event.reason,
                    )
                else:
                    self.state.end_continuity_break(
                        event.exchange,
                        symbol,
                        event.timestamp_ms,
                    )

    def _record_runtime_pause(
        self,
        previous_wall_ms: int,
        previous_monotonic_s: float,
        now_wall_ms: int,
        now_monotonic_s: float,
    ) -> None:
        expected_ms = self.config.detector.evaluate_every_seconds * 1000
        wall_elapsed_ms = max(0, now_wall_ms - previous_wall_ms)
        monotonic_elapsed_ms = max(
            0,
            int((now_monotonic_s - previous_monotonic_s) * 1000),
        )
        elapsed_ms = max(wall_elapsed_ms, monotonic_elapsed_ms)
        interruption_ms = max(0, elapsed_ms - expected_ms)
        maximum_gap_ms = self.config.detector.maximum_data_gap_seconds * 1000
        if interruption_ms <= maximum_gap_ms:
            return
        end_ms = max(now_wall_ms, previous_wall_ms + elapsed_ms)
        start_ms = end_ms - interruption_ms
        for exchange in self.enabled_exchanges:
            self.health.begin_continuity_break(
                exchange,
                "runtime_pause",
                timestamp_ms=start_ms,
            )
        self._sync_continuity_events()

    async def _check_continuity_health(
        self,
        snapshots: list[MetricSnapshot],
        now_ms: int,
    ) -> None:
        configured_windows = sorted(item.seconds for item in self.config.detector.windows)

        def metric_key(item: MetricSnapshot) -> str:
            return f"{item.exchange}:{item.symbol}@{item.window_seconds}s"

        current_by_metric = {metric_key(item): item for item in snapshots}
        affected = [item for item in snapshots if item.readiness_reason == "recovering_after_gap"]

        if affected:
            pairs = {f"{item.exchange}:{item.symbol}" for item in affected}
            # A continuity break contaminates every configured detector window
            # for the affected venue/symbol, including a window whose snapshot
            # is temporarily absent. Seed the complete expected set so recovery
            # can never be inferred from only the windows we happened to observe.
            metric_keys = {f"{pair}@{window}s" for pair in pairs for window in configured_windows}
            affected_windows = configured_windows
            min_coverage = min(item.window_coverage_ratio for item in affected)
            max_gap = max(
                (
                    item.largest_gap_seconds
                    for item in affected
                    if item.largest_gap_seconds is not None
                ),
                default=0.0,
            )
            max_recovery = max(item.recovery_seconds_remaining for item in affected)
            if self._continuity_episode_started_ms is None:
                details = [
                    {
                        "exchange": item.exchange,
                        "symbol": item.symbol,
                        "window_seconds": item.window_seconds,
                        "window_coverage_ratio": item.window_coverage_ratio,
                        "largest_gap_seconds": item.largest_gap_seconds,
                        "recovery_seconds_remaining": (item.recovery_seconds_remaining),
                    }
                    for item in sorted(
                        affected,
                        key=lambda item: (item.exchange, item.symbol, item.window_seconds),
                    )
                ]
                pair_text = ", ".join(sorted(pairs))
                alert = Alert(
                    severity=Severity.WARNING,
                    category="monitoring_gap",
                    symbol="SYSTEM",
                    title="Monitoring continuity gap detected",
                    message=(
                        f"Post-outage recovery is active for {pair_text}. "
                        "Market alarms may be incomplete until the "
                        "affected detector windows refill."
                    ),
                    timestamp_ms=now_ms,
                    direction="mixed",
                    exchanges=sorted({item.exchange for item in affected}),
                    metrics={
                        "affected_pairs": sorted(pairs),
                        "affected_count": len(pairs),
                        "affected_windows": sorted(metric_keys),
                        "affected_windows_seconds": affected_windows,
                        "min_window_coverage_ratio": min_coverage,
                        "max_largest_gap_seconds": max_gap,
                        "max_recovery_seconds_remaining": max_recovery,
                        "pairs": details,
                    },
                    dedup_key="monitoring:continuity-gap:active",
                )
                emitted = await self.dispatcher.emit(alert, bypass_cooldown=True)
                if not emitted:
                    return
                self._continuity_episode_started_ms = now_ms
                self._continuity_episode_pairs = set(pairs)
                self._continuity_episode_metrics = set(metric_keys)
                self._continuity_episode_min_coverage = min_coverage
                self._continuity_episode_max_gap = max_gap
                self._continuity_episode_max_recovery = max_recovery
                return

            self._continuity_episode_pairs.update(pairs)
            self._continuity_episode_metrics.update(metric_keys)
            self._continuity_episode_min_coverage = min(
                self._continuity_episode_min_coverage,
                min_coverage,
            )
            self._continuity_episode_max_gap = max(
                self._continuity_episode_max_gap,
                max_gap,
            )
            self._continuity_episode_max_recovery = max(
                self._continuity_episode_max_recovery,
                max_recovery,
            )
            return

        if self._continuity_episode_started_ms is None:
            return
        # Baseline warmup, no-data, stale data, or a missing metric is not
        # recovery. The episode clears only after every exact detector window
        # it touched is explicitly ready and fresh.
        if not all(
            key in current_by_metric
            and current_by_metric[key].ready
            and current_by_metric[key].data_age_seconds is not None
            and 0
            <= current_by_metric[key].data_age_seconds
            <= self.config.detector.freshness_seconds
            for key in self._continuity_episode_metrics
        ):
            return

        duration_seconds = max(
            0.0,
            (now_ms - self._continuity_episode_started_ms) / 1000,
        )
        recovery = Alert(
            severity=Severity.INFO,
            category="monitoring_recovery",
            symbol="SYSTEM",
            title="Monitoring continuity recovered",
            message=(
                "All post-outage detector windows are ready again for "
                + ", ".join(sorted(self._continuity_episode_pairs))
                + "."
            ),
            timestamp_ms=now_ms,
            direction="mixed",
            exchanges=sorted({pair.partition(":")[0] for pair in self._continuity_episode_pairs}),
            metrics={
                "affected_pairs": sorted(self._continuity_episode_pairs),
                "affected_count": len(self._continuity_episode_pairs),
                "affected_windows": sorted(self._continuity_episode_metrics),
                "windows_seconds": configured_windows,
                "episode_duration_seconds": round(duration_seconds, 1),
                "min_window_coverage_ratio": (self._continuity_episode_min_coverage),
                "max_largest_gap_seconds": (self._continuity_episode_max_gap),
                "max_recovery_seconds_remaining": (self._continuity_episode_max_recovery),
            },
            dedup_key=f"monitoring:continuity-gap:recovered:{now_ms}",
        )
        emitted = await self.dispatcher.emit(recovery)
        if not emitted:
            return
        self._continuity_episode_started_ms = None
        self._continuity_episode_pairs.clear()
        self._continuity_episode_metrics.clear()
        self._continuity_episode_min_coverage = 1.0
        self._continuity_episode_max_gap = 0.0
        self._continuity_episode_max_recovery = 0.0

    async def _health_loop(self) -> None:
        await asyncio.sleep(self.config.runtime.startup_grace_seconds)
        self.health.task_heartbeat("health-monitor", ready=True)
        while True:
            await self._check_feed_health(int(time.time() * 1000))
            await self._check_runtime_health(int(time.time() * 1000))
            self.health.task_heartbeat("health-monitor", ready=True)
            await asyncio.sleep(self.config.runtime.health_check_seconds)

    async def _check_feed_health(self, now_ms: int) -> None:
        snapshot = self.health.snapshot()
        for exchange in self.enabled_exchanges:
            health = snapshot[exchange]
            stale_after = getattr(self.config.exchanges, exchange).stale_after_seconds
            unseen_symbols = [
                symbol
                for symbol, symbol_health in health["symbols"].items()
                if not symbol_health["seen_since_connect"]
            ]
            quiet_symbols = [
                symbol
                for symbol, symbol_health in health["symbols"].items()
                if (
                    symbol_health["seen_since_connect"]
                    and (
                        symbol_health["message_age_seconds"] is None
                        or symbol_health["message_age_seconds"] > stale_after
                    )
                )
            ]
            feed_age = health["latest_message_age_seconds"]
            unhealthy = (
                not health["connected"]
                or not health["subscription_acknowledged"]
                or feed_age is None
                or feed_age > stale_after
            )
            if unhealthy:
                severe = feed_age is not None and feed_age > stale_after * 3
                severity = Severity.CRITICAL if severe else Severity.WARNING
                if not health["connected"]:
                    reason = "disconnected"
                elif not health["subscription_acknowledged"]:
                    missing_topics = sorted(
                        set(health["expected_topics"]) - set(health["acknowledged_topics"])
                    )
                    detail = ", ".join(missing_topics[:5])
                    suffix = f": {detail}" if detail else ""
                    reason = f"missing subscription acknowledgement{suffix}"
                else:
                    reason = "no valid market payload across the feed"
                metrics = dict(health)
                metrics["unhealthy_symbols"] = sorted(set(unseen_symbols) | set(quiet_symbols))
                metrics["unseen_symbols"] = unseen_symbols
                metrics["quiet_symbols"] = quiet_symbols
                alert = Alert(
                    severity=severity,
                    category="feed_health",
                    symbol="SYSTEM",
                    title=f"{exchange.title()} feed unhealthy",
                    message=f"{exchange} is {reason}. Market alarms may be incomplete.",
                    timestamp_ms=now_ms,
                    direction="mixed",
                    exchanges=[exchange],
                    metrics=metrics,
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

    async def _check_runtime_health(self, now_ms: int) -> None:
        queue_health = self.health.queue_snapshot(
            self.queue.qsize(), self.config.runtime.queue_size
        )
        drops_total = int(queue_health["drops_total"])
        drop_delta = max(0, drops_total - self._queue_drops_reported)
        self._queue_drops_reported = drops_total
        self._last_queue_drop_delta = drop_delta

        utilization = float(queue_health["utilization"])
        consumer_lag = float(queue_health["consumer_lag_seconds"])
        lag_limit = max(5.0, self.config.runtime.health_check_seconds * 2.0)
        problems: list[str] = []
        if drop_delta:
            problems.append(f"{drop_delta} new market event(s) dropped")
        if utilization >= 0.8:
            problems.append(f"market queue is {utilization:.0%} full")
        if consumer_lag > lag_limit:
            problems.append(f"consumer lag is {consumer_lag:.1f}s")

        if problems:
            critical = drop_delta > 0 or utilization >= 0.95 or consumer_lag > lag_limit * 3
            metrics = dict(queue_health)
            metrics["new_drops"] = drop_delta
            metrics["consumer_lag_limit_seconds"] = lag_limit
            alert = Alert(
                severity=Severity.CRITICAL if critical else Severity.WARNING,
                category="runtime_health",
                symbol="SYSTEM",
                title="Market event pipeline unhealthy",
                message=("; ".join(problems) + ". Market alarms may be delayed or incomplete."),
                timestamp_ms=now_ms,
                direction="mixed",
                exchanges=[],
                metrics=metrics,
                dedup_key="runtime:market-queue:unhealthy",
            )
            await self.dispatcher.emit(
                alert,
                bypass_cooldown=(not self._queue_problem or drop_delta > 0),
            )
            self._queue_problem = True
        elif self._queue_problem:
            recovery = Alert(
                severity=Severity.INFO,
                category="runtime_recovery",
                symbol="SYSTEM",
                title="Market event pipeline recovered",
                message="Queue pressure and consumer lag returned to normal.",
                timestamp_ms=now_ms,
                direction="mixed",
                exchanges=[],
                metrics=queue_health,
                dedup_key=f"runtime:market-queue:recovered:{now_ms}",
            )
            await self.dispatcher.emit(recovery, bypass_cooldown=True)
            self._queue_problem = False

    async def _maintenance_loop(self) -> None:
        while True:
            self.health.task_heartbeat("maintenance", ready=True)
            await asyncio.sleep(6 * 3600)
            cutoff_ms = (
                int(time.time() * 1000) - self.config.storage.retain_metric_days * 86_400_000
            )
            await self._prune_storage(cutoff_ms)
            self.health.task_heartbeat("maintenance", ready=True)

    async def _prune_storage(self, cutoff_ms: int) -> tuple[int, int]:
        metric_rows = await self.database.prune_metrics(cutoff_ms)
        receipt_rows = await self.database.prune_delivery_receipts(cutoff_ms)
        self.log.info(
            "pruned %d old metric rows and %d old delivery receipt rows",
            metric_rows,
            receipt_rows,
        )
        return metric_rows, receipt_rows

    async def _checkpoint_loop(self) -> None:
        while True:
            await self._save_checkpoint()
            await asyncio.sleep(self.config.storage.checkpoint_seconds)

    async def _save_checkpoint(self) -> bool:
        now_ms = int(time.time() * 1000)
        try:
            saved = await self.checkpoint.save(self.state, now_ms)
            if not saved:
                raise RuntimeError("checkpoint writer returned false")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._checkpoint_last_failure_ms = now_ms
            self._checkpoint_last_failure_monotonic = time.monotonic()
            self._checkpoint_last_error = f"{type(exc).__name__}: {exc}"[:500]
            self._checkpoint_latest_attempt_succeeded = False
            self.health.task_heartbeat("state-checkpoint", ready=False)
            self.log.error("state checkpoint failed: %s", exc)
            return False
        self._checkpoint_last_success_ms = now_ms
        self._checkpoint_last_success_monotonic = time.monotonic()
        self._checkpoint_latest_attempt_succeeded = True
        self.health.task_heartbeat("state-checkpoint", ready=True)
        return True
