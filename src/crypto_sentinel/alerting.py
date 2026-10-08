from __future__ import annotations

import asyncio
import json
import logging
import platform
import shutil
import subprocess
import sys
import time
from collections import OrderedDict, deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from pathlib import Path
from urllib.parse import quote

from aiohttp import ClientSession, ClientTimeout

from crypto_sentinel.config import AppConfig
from crypto_sentinel.models import Alert, DeliveryReceipt, Severity
from crypto_sentinel.persistence import Database


@dataclass(slots=True, frozen=True)
class _CooldownEntry:
    wall_ms: int
    monotonic_s: float
    severity_rank: int


@dataclass(slots=True, frozen=True)
class _LocalWork:
    priority: int
    sequence: int
    alert: Alert
    original_dedup_key: str
    cooldown_entry: _CooldownEntry | None


class AlertDispatcher:
    """Persist and fan out alerts without letting remote delivery stall detection."""

    def __init__(
        self,
        config: AppConfig,
        database: Database,
        publish_browser: Callable[[Alert], Awaitable[None]],
    ) -> None:
        self.config = config
        self.database = database
        self.publish_browser = publish_browser
        self.log = logging.getLogger("crypto_sentinel.alerting")
        self._last_sent: OrderedDict[str, _CooldownEntry] = OrderedDict()
        self._max_dedup_entries = 10_000
        self._session: ClientSession | None = None
        self._remote_queue: asyncio.Queue[Alert] = asyncio.Queue(maxsize=2048)
        self._remote_workers: list[asyncio.Task[None]] = []
        self._local_queue: asyncio.PriorityQueue[tuple[int, int, _LocalWork]] = (
            asyncio.PriorityQueue(maxsize=256)
        )
        self._local_worker_task: asyncio.Task[None] | None = None
        self._local_sequence = 0
        self._local_in_progress: str | None = None
        self._local_active_ids: set[str] = set()
        self._local_receipts: deque[DeliveryReceipt] = deque(maxlen=1000)
        self._local_last_outcome: DeliveryReceipt | None = None
        self._local_receipt_counts: dict[str, int] = {
            "queued": 0,
            "attempt": 0,
            "success": 0,
            "failure": 0,
        }
        self._retry_delays = (1.0, 3.0)
        self._local_retry_delays = (0.25, 1.0)

    async def start(self) -> None:
        if self._session is not None:
            return
        self._session = ClientSession(timeout=ClientTimeout(total=10, connect=5))
        await self._restore_recent_dedup()
        await self._restore_local_outcome()
        await self._restore_pending_local_alarms()
        self._remote_workers = [
            asyncio.create_task(self._remote_worker(), name=f"notifier-{index}")
            for index in range(2)
        ]
        self._local_worker_task = asyncio.create_task(
            self._local_worker(), name="local-alarm-worker"
        )

    def worker_tasks(self) -> tuple[asyncio.Task[None], ...]:
        """Return a snapshot of registered notifier workers for runtime supervision."""
        tasks = list(self._remote_workers)
        if self._local_worker_task is not None:
            tasks.append(self._local_worker_task)
        return tuple(tasks)

    async def close(self) -> None:
        if self._remote_workers:
            try:
                await asyncio.wait_for(self._remote_queue.join(), timeout=20)
            except TimeoutError:
                self.log.error(
                    "timed out draining %d queued remote alert(s)", self._remote_queue.qsize()
                )
        if self._local_worker_task:
            try:
                await asyncio.wait_for(self._local_queue.join(), timeout=30)
            except TimeoutError:
                self.log.error(
                    "timed out draining %d queued local alarm(s)",
                    self._local_queue.qsize(),
                )
        for task in self._remote_workers:
            task.cancel()
        if self._remote_workers:
            await asyncio.gather(*self._remote_workers, return_exceptions=True)
        self._remote_workers.clear()
        if self._local_worker_task:
            self._local_worker_task.cancel()
            await asyncio.gather(self._local_worker_task, return_exceptions=True)
            self._local_worker_task = None
        if self._session:
            await self._session.close()
            self._session = None

    async def _restore_recent_dedup(self) -> None:
        cooldown_ms = self.config.detector.cooldown_seconds * 1000
        if cooldown_ms <= 0:
            return
        now_ms = int(time.time() * 1000)
        now_monotonic_s = time.monotonic()
        try:
            rows = await self.database.list_recent_dedup(
                now_ms - cooldown_ms,
                self._max_dedup_entries,
            )
        except Exception as exc:
            self.log.error("could not restore alert cooldown state: %s", exc)
            return
        for row in reversed(rows):
            try:
                severity = Severity(str(row["severity"]))
                row_ms = min(int(row["timestamp_ms"]), now_ms)
                age_s = min(cooldown_ms / 1000, max(0.0, (now_ms - row_ms) / 1000))
                self._last_sent[str(row["dedup_key"])] = _CooldownEntry(
                    wall_ms=row_ms,
                    monotonic_s=now_monotonic_s - age_s,
                    severity_rank=severity.rank,
                )
            except (KeyError, TypeError, ValueError):
                continue
        while len(self._last_sent) > self._max_dedup_entries:
            self._last_sent.popitem(last=False)
        if rows:
            self.log.info("restored %d recent alert cooldown key(s)", len(self._last_sent))

    async def _restore_local_outcome(self) -> None:
        row = None
        get_durable = getattr(self.database, "get_latest_local_alarm_outcome", None)
        if get_durable is not None:
            try:
                row = await get_durable()
            except Exception as exc:
                self.log.error("could not restore durable local alarm outcome: %s", exc)
        if row is None:
            get_receipt = getattr(self.database, "get_latest_delivery_outcome", None)
            if get_receipt is not None:
                try:
                    row = await get_receipt("local")
                except Exception as exc:
                    self.log.error("could not restore latest local alarm receipt: %s", exc)
        if row is None:
            return
        try:
            self._local_last_outcome = DeliveryReceipt(
                alert_id=str(row["alert_id"]),
                channel="local",
                status=str(row["status"]),
                attempt=max(0, int(row["attempt"])),
                timestamp_ms=int(row["timestamp_ms"]),
                detail=str(row.get("detail", ""))[:160],
            )
        except (KeyError, TypeError, ValueError):
            self.log.error("ignored malformed persisted local alarm outcome")

    async def _restore_pending_local_alarms(
        self,
        *,
        extra_exclude_ids: tuple[str, ...] = (),
    ) -> None:
        if not self.config.notifiers.local.enabled:
            return
        list_pending = getattr(self.database, "list_pending_local_alarms", None)
        if list_pending is None:
            return
        available = self._local_queue.maxsize - self._local_queue.qsize()
        if available <= 0:
            return
        excluded_ids = tuple(self._local_active_ids) + extra_exclude_ids
        try:
            rows = await list_pending(
                limit=available,
                exclude_alert_ids=excluded_ids,
            )
        except Exception as exc:
            self.log.error("could not restore pending local alarms: %s", exc)
            return
        restored = 0
        for row in rows:
            try:
                alert = Alert(
                    severity=Severity(str(row["severity"])),
                    category=str(row["category"]),
                    symbol=str(row["symbol"]),
                    title=str(row["title"]),
                    message=str(row["message"]),
                    timestamp_ms=int(row["timestamp_ms"]),
                    direction=str(row["direction"]),
                    exchanges=list(row["exchanges"]),
                    metrics=dict(row["metrics"]),
                    dedup_key=str(row["original_dedup_key"]),
                    source=str(row["source"]),
                    id=str(row["id"]),
                )
                priority = 0 if alert.severity == Severity.CRITICAL else 1
                self._local_sequence += 1
                work = _LocalWork(
                    priority=priority,
                    sequence=self._local_sequence,
                    alert=alert,
                    original_dedup_key=alert.dedup_key,
                    cooldown_entry=None,
                )
            except (KeyError, TypeError, ValueError):
                self.log.error("ignored malformed pending local alarm row")
                continue
            admitted, evicted = self._try_admit_local_work(work)
            if not admitted:
                self.log.error(
                    "pending local alarm queue saturated while restoring %s",
                    alert.id,
                )
                break
            if evicted is not None:
                await self._finalize_local_failure(
                    evicted,
                    0,
                    "evicted_during_restart_replay",
                )
            await self._record_local_receipt(
                alert,
                "queued",
                0,
                "restart_replay",
            )
            restored += 1
        if restored:
            self.log.warning("restored %d pending local alarm(s)", restored)

    def _allowed(self, alert: Alert, minimum: str) -> bool:
        return alert.severity.rank >= Severity(minimum).rank

    def _deduplicated(self, alert: Alert, bypass: bool, now_monotonic_s: float) -> bool:
        if bypass or not alert.dedup_key:
            return False
        previous = self._last_sent.get(alert.dedup_key)
        if not previous:
            return False
        elapsed = max(0.0, now_monotonic_s - previous.monotonic_s)
        if alert.severity.rank > previous.severity_rank:
            return False
        return elapsed < self.config.detector.cooldown_seconds

    async def emit(self, alert: Alert, *, bypass_cooldown: bool = False) -> bool:
        now_ms = int(time.time() * 1000)
        now_monotonic_s = time.monotonic()
        if self._deduplicated(alert, bypass_cooldown, now_monotonic_s):
            self.log.debug("deduplicated alert %s", alert.dedup_key)
            return False
        cooldown_entry: _CooldownEntry | None = None
        if alert.dedup_key:
            cooldown_entry = _CooldownEntry(
                wall_ms=now_ms,
                monotonic_s=now_monotonic_s,
                severity_rank=alert.severity.rank,
            )
            self._last_sent[alert.dedup_key] = cooldown_entry
            self._last_sent.move_to_end(alert.dedup_key)
            while len(self._last_sent) > self._max_dedup_entries:
                self._last_sent.popitem(last=False)

        local = self.config.notifiers.local
        local_required = local.enabled and self._allowed(alert, local.minimum_severity)
        local_work: _LocalWork | None = None
        if local_required:
            alert.metrics = {
                **alert.metrics,
                "local_alarm": {
                    "status": "queued",
                    "attempt": 0,
                    "timestamp_ms": now_ms,
                },
            }
            self._local_sequence += 1
            priority = 0 if alert.severity == Severity.CRITICAL else 1
            local_work = _LocalWork(
                priority=priority,
                sequence=self._local_sequence,
                alert=alert,
                original_dedup_key=alert.dedup_key,
                cooldown_entry=cooldown_entry,
            )
        # Until the required PC alarm succeeds, persist the alert without a cooldown key and
        # retain a durable pending job. A crash after this commit will replay the local alarm.
        # An alarm must still reach the operator when optional persistence is unavailable.
        if local_work is not None:
            await self._persist_local_state(
                local_work,
                delivered=False,
                status="queued",
                attempt=0,
                timestamp_ms=now_ms,
            )
        else:
            try:
                await self.database.save_alert(alert)
            except Exception as exc:
                self.log.error("could not persist alert %s: %s", alert.id, exc)
        try:
            await self.publish_browser(alert)
        except Exception as exc:
            self.log.error("could not publish browser alert %s: %s", alert.id, exc)
        self.log.warning("%s | %s", alert.title, alert.message)

        local_admitted = True
        if local_work is not None:
            await self._record_local_receipt(alert, "queued", 0)
            local_admitted, evicted = self._try_admit_local_work(local_work)
            if evicted is not None:
                await self._finalize_local_failure(
                    evicted,
                    0,
                    "evicted_for_critical",
                )
            if not local_admitted:
                await self._finalize_local_failure(local_work, 0, "queue_full")

        if self._has_remote_delivery(alert):
            try:
                self._remote_queue.put_nowait(alert)
            except asyncio.QueueFull:
                # Do not create unbounded tasks during an alert storm. The persisted/browser/local
                # paths have already run, and this failure is made loud in the operational log.
                self.log.error("remote notifier queue full; delivery dropped for %s", alert.id)
        return local_admitted

    def _try_admit_local_work(
        self,
        work: _LocalWork,
    ) -> tuple[bool, _LocalWork | None]:
        item = (work.priority, work.sequence, work)
        try:
            self._local_queue.put_nowait(item)
            self._local_active_ids.add(work.alert.id)
            return True, None
        except asyncio.QueueFull:
            if work.priority != 0:
                return False, None

        # Critical alarms may displace one lower-priority queued alarm. Drain only the
        # priority prefix needed to find a warning, balancing unfinished-task accounting
        # before restoring the prefix so join() remains correct.
        displaced_prefix: list[tuple[int, int, _LocalWork]] = []
        evicted: _LocalWork | None = None
        while True:
            try:
                queued_item = self._local_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            self._local_queue.task_done()
            if queued_item[0] > work.priority:
                evicted = queued_item[2]
                break
            displaced_prefix.append(queued_item)

        for queued_item in displaced_prefix:
            self._local_queue.put_nowait(queued_item)
        if evicted is None:
            return False, None
        self._local_queue.put_nowait(item)
        self._local_active_ids.discard(evicted.alert.id)
        self._local_active_ids.add(work.alert.id)
        return True, evicted

    def _has_remote_delivery(self, alert: Alert) -> bool:
        return any(
            config.enabled and self._allowed(alert, config.minimum_severity)
            for config in (
                self.config.notifiers.ntfy,
                self.config.notifiers.telegram,
                self.config.notifiers.webhook,
            )
        )

    async def _remote_worker(self) -> None:
        while True:
            alert = await self._remote_queue.get()
            try:
                await self._deliver_remote(alert)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.error("remote notifier worker failed: %s", exc)
            finally:
                self._remote_queue.task_done()

    async def _deliver_remote(self, alert: Alert) -> None:
        operations: list[tuple[str, Callable[[], Awaitable[None]]]] = []
        ntfy = self.config.notifiers.ntfy
        if ntfy.enabled and self._allowed(alert, ntfy.minimum_severity):
            operations.append(("ntfy", lambda: self._ntfy(alert)))
        telegram = self.config.notifiers.telegram
        if telegram.enabled and self._allowed(alert, telegram.minimum_severity):
            operations.append(("telegram", lambda: self._telegram(alert)))
        webhook = self.config.notifiers.webhook
        if webhook.enabled and self._allowed(alert, webhook.minimum_severity):
            operations.append(("webhook", lambda: self._webhook(alert)))
        if not operations:
            return
        results = await asyncio.gather(
            *(self._deliver_with_retry(name, operation) for name, operation in operations),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, Exception):
                self.log.error("notifier failed: %s", result)

    async def _deliver_with_retry(
        self, name: str, operation: Callable[[], Awaitable[None]]
    ) -> None:
        attempts = len(self._retry_delays) + 1
        for attempt in range(1, attempts + 1):
            try:
                await operation()
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if attempt >= attempts:
                    raise RuntimeError(f"{name} failed after {attempts} attempt(s): {exc}") from exc
                delay = self._retry_delays[attempt - 1]
                self.log.warning(
                    "%s delivery attempt %d/%d failed; retrying in %.1fs: %s",
                    name,
                    attempt,
                    attempts,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)

    def status(self) -> dict[str, object]:
        last_receipt = self._local_receipts[-1].to_dict() if self._local_receipts else None
        last_outcome = (
            self._local_last_outcome.to_dict() if self._local_last_outcome is not None else None
        )
        return {
            "remote_queue_depth": self._remote_queue.qsize(),
            "local_alarm": {
                "enabled": self.config.notifiers.local.enabled,
                "queue_depth": self._local_queue.qsize(),
                "in_progress_alert_id": self._local_in_progress,
                "counts": dict(self._local_receipt_counts),
                "last_receipt": last_receipt,
                "last_outcome": last_outcome,
            },
        }

    async def _record_local_receipt(
        self,
        alert: Alert,
        status: str,
        attempt: int,
        detail: str = "",
    ) -> DeliveryReceipt:
        timestamp_ms = int(time.time() * 1000)
        receipt = DeliveryReceipt(
            alert_id=alert.id,
            channel="local",
            status=status,
            attempt=attempt,
            timestamp_ms=timestamp_ms,
            detail=detail[:160],
        )
        self._local_receipts.append(receipt)
        if status in {"success", "failure"}:
            self._local_last_outcome = receipt
        if status in self._local_receipt_counts:
            self._local_receipt_counts[status] += 1
        save_receipt = getattr(self.database, "save_delivery_receipt", None)
        if save_receipt is not None:
            try:
                await save_receipt(
                    alert.id,
                    "local",
                    status,
                    attempt,
                    detail=receipt.detail,
                    timestamp_ms=timestamp_ms,
                )
            except Exception as exc:
                self.log.error(
                    "could not persist local alarm receipt %s: %s",
                    alert.id,
                    type(exc).__name__,
                )
        return receipt

    @staticmethod
    def _safe_local_error(exc: Exception) -> str:
        if isinstance(exc, subprocess.TimeoutExpired):
            return "timeout"
        if isinstance(exc, subprocess.CalledProcessError):
            return f"exit_status_{exc.returncode}"
        if isinstance(exc, FileNotFoundError):
            return "executable_not_found"
        return type(exc).__name__[:80]

    @staticmethod
    def _set_local_metric(
        alert: Alert,
        status: str,
        attempt: int,
        timestamp_ms: int,
        detail: str = "",
    ) -> None:
        local_metric: dict[str, object] = {
            "status": status,
            "attempt": attempt,
            "timestamp_ms": timestamp_ms,
        }
        if detail:
            local_metric["detail"] = detail
        alert.metrics = {**alert.metrics, "local_alarm": local_metric}

    def _clear_failed_cooldown(self, work: _LocalWork) -> None:
        if not work.original_dedup_key or work.cooldown_entry is None:
            return
        if self._last_sent.get(work.original_dedup_key) == work.cooldown_entry:
            self._last_sent.pop(work.original_dedup_key, None)

    def _mark_successful_cooldown(self, work: _LocalWork, timestamp_ms: int) -> None:
        if not work.original_dedup_key:
            return
        entry = work.cooldown_entry
        if entry is None:
            entry = _CooldownEntry(
                wall_ms=timestamp_ms,
                monotonic_s=time.monotonic(),
                severity_rank=work.alert.severity.rank,
            )
        self._last_sent[work.original_dedup_key] = entry
        self._last_sent.move_to_end(work.original_dedup_key)
        while len(self._last_sent) > self._max_dedup_entries:
            self._last_sent.popitem(last=False)

    async def _persist_local_state(
        self,
        work: _LocalWork,
        *,
        delivered: bool,
        status: str,
        attempt: int,
        timestamp_ms: int,
        detail: str = "",
    ) -> None:
        stored_alert = work.alert if delivered else replace(work.alert, dedup_key="")
        try:
            save_state = getattr(self.database, "save_local_alarm_state", None)
            if save_state is None:
                await self.database.save_alert(stored_alert)
            else:
                await save_state(
                    stored_alert,
                    original_dedup_key=work.original_dedup_key,
                    priority=work.priority,
                    status=status,
                    attempt=attempt,
                    updated_ms=timestamp_ms,
                    detail=detail,
                )
        except Exception as exc:
            self.log.error(
                "could not persist local alarm state for %s: %s",
                work.alert.id,
                type(exc).__name__,
            )

    async def _finalize_local_failure(
        self,
        work: _LocalWork,
        attempt: int,
        detail: str,
    ) -> None:
        self._clear_failed_cooldown(work)
        timestamp_ms = int(time.time() * 1000)
        self._set_local_metric(work.alert, "failure", attempt, timestamp_ms, detail)
        await self._persist_local_state(
            work,
            delivered=False,
            status="failure",
            attempt=attempt,
            timestamp_ms=timestamp_ms,
            detail=detail,
        )
        await self._record_local_receipt(work.alert, "failure", attempt, detail)
        self.log.error(
            "local alarm failed for %s after %d attempt(s): %s",
            work.alert.id,
            attempt,
            detail,
        )

    async def _process_local_work(self, work: _LocalWork) -> None:
        attempts = len(self._local_retry_delays) + 1
        for attempt in range(1, attempts + 1):
            timestamp_ms = int(time.time() * 1000)
            self._set_local_metric(work.alert, "attempt", attempt, timestamp_ms)
            await self._persist_local_state(
                work,
                delivered=False,
                status="attempt",
                attempt=attempt,
                timestamp_ms=timestamp_ms,
            )
            await self._record_local_receipt(work.alert, "attempt", attempt)
            try:
                await self._local_sound(work.alert)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                detail = self._safe_local_error(exc)
                if attempt >= attempts:
                    await self._finalize_local_failure(work, attempt, detail)
                    return
                delay = self._local_retry_delays[attempt - 1]
                self.log.warning(
                    "local alarm attempt %d/%d failed for %s; retrying in %.2fs: %s",
                    attempt,
                    attempts,
                    work.alert.id,
                    delay,
                    detail,
                )
                await asyncio.sleep(delay)
            else:
                timestamp_ms = int(time.time() * 1000)
                self._set_local_metric(work.alert, "success", attempt, timestamp_ms)
                self._mark_successful_cooldown(work, timestamp_ms)
                await self._persist_local_state(
                    work,
                    delivered=True,
                    status="success",
                    attempt=attempt,
                    timestamp_ms=timestamp_ms,
                )
                await self._record_local_receipt(work.alert, "success", attempt)
                return

    async def _local_worker(self) -> None:
        while True:
            _, _, work = await self._local_queue.get()
            self._local_in_progress = work.alert.id
            refill = True
            try:
                await self._process_local_work(work)
            except asyncio.CancelledError:
                refill = False
                raise
            except Exception as exc:
                detail = self._safe_local_error(exc)
                await self._finalize_local_failure(work, 0, detail)
            finally:
                self._local_in_progress = None
                self._local_active_ids.discard(work.alert.id)
                try:
                    if refill:
                        await self._restore_pending_local_alarms(
                            extra_exclude_ids=(work.alert.id,),
                        )
                finally:
                    self._local_queue.task_done()

    async def _local_sound(self, alert: Alert) -> None:
        config = self.config.notifiers.local
        values = {
            "severity": alert.severity.value,
            "symbol": alert.symbol,
            "direction": alert.direction,
            "title": alert.title,
            "message": alert.message,
        }
        if config.custom_command:
            command = [part.format_map(values) for part in config.custom_command]
            await asyncio.to_thread(
                subprocess.run,
                command,
                check=True,
                timeout=30,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            return
        await asyncio.to_thread(self._play_sound_sync, alert)

    def _play_sound_sync(self, alert: Alert) -> None:
        config = self.config.notifiers.local
        repeats = config.repeat_critical if alert.severity == Severity.CRITICAL else 1
        sound_file = Path(config.sound_file) if config.sound_file else None
        system = platform.system().lower()

        if sound_file and sound_file.exists():
            if system == "windows":
                import winsound

                for _ in range(repeats):
                    winsound.PlaySound(str(sound_file), winsound.SND_FILENAME)
                return
            player = (
                "afplay"
                if system == "darwin"
                else (shutil.which("paplay") or shutil.which("aplay"))
            )
            if player:
                for _ in range(repeats):
                    subprocess.run(
                        [str(player), str(sound_file)],
                        check=True,
                        timeout=30,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                return

        if system == "windows":
            import winsound

            pattern = (
                [(1200, 250), (800, 250), (1200, 400)]
                if alert.severity == Severity.CRITICAL
                else [(900, 350)]
            )
            for _ in range(repeats):
                for frequency, duration in pattern:
                    winsound.Beep(frequency, duration)
            return
        for _ in range(repeats):
            sys.stdout.write("\a")
            sys.stdout.flush()

    async def _ntfy(self, alert: Alert) -> None:
        config = self.config.notifiers.ntfy
        if not config.topic or "replace" in config.topic.lower():
            raise ValueError("ntfy is enabled but topic is empty or still a placeholder")
        if not self._session:
            raise RuntimeError("dispatcher not started")
        url = f"{config.server.rstrip('/')}/{quote(config.topic, safe='')}"
        tags = "rotating_light"
        if alert.direction == "down":
            tags += ",chart_with_downwards_trend"
        elif alert.direction == "up":
            tags += ",chart_with_upwards_trend"
        headers = {
            "Title": alert.title,
            "Priority": "urgent" if alert.severity == Severity.CRITICAL else "high",
            "Tags": tags,
        }
        if config.token:
            headers["Authorization"] = f"Bearer {config.token}"
        if config.click_url:
            headers["Click"] = config.click_url
        async with self._session.post(
            url, data=alert.message.encode(), headers=headers
        ) as response:
            if response.status >= 300:
                raise RuntimeError(f"ntfy returned HTTP {response.status}: {await response.text()}")

    async def _telegram(self, alert: Alert) -> None:
        config = self.config.notifiers.telegram
        if not config.bot_token or not config.chat_id:
            raise ValueError("Telegram is enabled but bot_token/chat_id is missing")
        if not self._session:
            raise RuntimeError("dispatcher not started")
        url = f"https://api.telegram.org/bot{config.bot_token}/sendMessage"
        text = f"<b>{_escape_html(alert.title)}</b>\n{_escape_html(alert.message)}"
        payload = {
            "chat_id": config.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        async with self._session.post(url, json=payload) as response:
            body = await response.text()
            if response.status >= 300:
                raise RuntimeError(f"Telegram returned HTTP {response.status}: {body}")
            decoded = json.loads(body)
            if not decoded.get("ok"):
                raise RuntimeError(f"Telegram rejected message: {decoded.get('description', body)}")

    async def _webhook(self, alert: Alert) -> None:
        config = self.config.notifiers.webhook
        if not config.url:
            raise ValueError("webhook is enabled but URL is empty")
        if not self._session:
            raise RuntimeError("dispatcher not started")
        headers = {"Content-Type": "application/json"}
        if config.bearer_token:
            headers["Authorization"] = f"Bearer {config.bearer_token}"
        async with self._session.post(
            config.url, json=alert.to_dict(), headers=headers
        ) as response:
            if response.status >= 300:
                raise RuntimeError(
                    f"webhook returned HTTP {response.status}: {await response.text()}"
                )


def _escape_html(value: str) -> str:
    return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
