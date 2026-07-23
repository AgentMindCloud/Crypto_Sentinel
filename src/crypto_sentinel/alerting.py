from __future__ import annotations

import asyncio
import json
import logging
import platform
import shutil
import subprocess
import sys
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from pathlib import Path
from urllib.parse import quote

from aiohttp import ClientSession, ClientTimeout

from crypto_sentinel.config import AppConfig
from crypto_sentinel.models import Alert, Severity
from crypto_sentinel.persistence import Database


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
        self._last_sent: OrderedDict[str, tuple[int, int]] = OrderedDict()
        self._max_dedup_entries = 10_000
        self._session: ClientSession | None = None
        self._remote_queue: asyncio.Queue[Alert] = asyncio.Queue(maxsize=2048)
        self._remote_workers: list[asyncio.Task[None]] = []
        self._local_tasks: set[asyncio.Task[None]] = set()
        self._local_lock = asyncio.Lock()
        self._retry_delays = (1.0, 3.0)

    async def start(self) -> None:
        if self._session is not None:
            return
        self._session = ClientSession(timeout=ClientTimeout(total=10, connect=5))
        await self._restore_recent_dedup()
        self._remote_workers = [
            asyncio.create_task(self._remote_worker(), name=f"notifier-{index}")
            for index in range(2)
        ]

    async def close(self) -> None:
        if self._remote_workers:
            try:
                await asyncio.wait_for(self._remote_queue.join(), timeout=20)
            except TimeoutError:
                self.log.error(
                    "timed out draining %d queued remote alert(s)", self._remote_queue.qsize()
                )
        if self._local_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*list(self._local_tasks), return_exceptions=True),
                    timeout=20,
                )
            except TimeoutError:
                self.log.error("timed out waiting for local alarm task(s)")
        for task in self._remote_workers:
            task.cancel()
        if self._remote_workers:
            await asyncio.gather(*self._remote_workers, return_exceptions=True)
        self._remote_workers.clear()
        for task in list(self._local_tasks):
            if not task.done():
                task.cancel()
        self._local_tasks.clear()
        if self._session:
            await self._session.close()
            self._session = None

    async def _restore_recent_dedup(self) -> None:
        cooldown_ms = self.config.detector.cooldown_seconds * 1000
        if cooldown_ms <= 0:
            return
        try:
            rows = await self.database.list_recent_dedup(
                int(time.time() * 1000) - cooldown_ms,
                self._max_dedup_entries,
            )
        except Exception as exc:
            self.log.error("could not restore alert cooldown state: %s", exc)
            return
        for row in reversed(rows):
            try:
                severity = Severity(str(row["severity"]))
                self._last_sent[str(row["dedup_key"])] = (
                    int(row["timestamp_ms"]),
                    severity.rank,
                )
            except (KeyError, TypeError, ValueError):
                continue
        while len(self._last_sent) > self._max_dedup_entries:
            self._last_sent.popitem(last=False)
        if rows:
            self.log.info("restored %d recent alert cooldown key(s)", len(self._last_sent))

    def _allowed(self, alert: Alert, minimum: str) -> bool:
        return alert.severity.rank >= Severity(minimum).rank

    def _deduplicated(self, alert: Alert, bypass: bool, now_ms: int) -> bool:
        if bypass or not alert.dedup_key:
            return False
        previous = self._last_sent.get(alert.dedup_key)
        if not previous:
            return False
        previous_ms, previous_rank = previous
        elapsed = (now_ms - previous_ms) / 1000
        if alert.severity.rank > previous_rank:
            return False
        return elapsed < self.config.detector.cooldown_seconds

    async def emit(self, alert: Alert, *, bypass_cooldown: bool = False) -> bool:
        now_ms = int(time.time() * 1000)
        if self._deduplicated(alert, bypass_cooldown, now_ms):
            self.log.debug("deduplicated alert %s", alert.dedup_key)
            return False
        if alert.dedup_key:
            self._last_sent[alert.dedup_key] = (now_ms, alert.severity.rank)
            self._last_sent.move_to_end(alert.dedup_key)
            while len(self._last_sent) > self._max_dedup_entries:
                self._last_sent.popitem(last=False)

        # An alarm must still reach the operator when optional persistence is unavailable.
        try:
            await self.database.save_alert(alert)
        except Exception as exc:
            self.log.error("could not persist alert %s: %s", alert.id, exc)
        try:
            await self.publish_browser(alert)
        except Exception as exc:
            self.log.error("could not publish browser alert %s: %s", alert.id, exc)
        self.log.warning("%s | %s", alert.title, alert.message)

        local = self.config.notifiers.local
        if local.enabled and self._allowed(alert, local.minimum_severity):
            task = asyncio.create_task(
                self._local_sound_serial(alert), name=f"local-alarm-{alert.id}"
            )
            self._local_tasks.add(task)
            task.add_done_callback(self._local_tasks.discard)

        if self._has_remote_delivery(alert):
            try:
                self._remote_queue.put_nowait(alert)
            except asyncio.QueueFull:
                # Do not create unbounded tasks during an alert storm. The persisted/browser/local
                # paths have already run, and this failure is made loud in the operational log.
                self.log.error("remote notifier queue full; delivery dropped for %s", alert.id)
        return True

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

    async def _local_sound_serial(self, alert: Alert) -> None:
        async with self._local_lock:
            try:
                await self._local_sound(alert)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log.error("local alarm failed: %s", exc)

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
                check=False,
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
                        check=False,
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
