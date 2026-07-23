from __future__ import annotations

import asyncio
import time

from crypto_sentinel.alerting import AlertDispatcher
from crypto_sentinel.models import Alert, Severity
from crypto_sentinel.persistence import Database


async def test_dispatcher_deduplicates_but_allows_severity_upgrade(
    example_config, tmp_path
) -> None:
    example_config.notifiers.local.enabled = False
    example_config.notifiers.ntfy.enabled = False
    example_config.notifiers.telegram.enabled = False
    example_config.notifiers.webhook.enabled = False
    database = Database(str(tmp_path / "alerts.db"))
    await database.initialize()
    published: list[Alert] = []

    async def publish(alert: Alert) -> None:
        published.append(alert)

    dispatcher = AlertDispatcher(example_config, database, publish)
    await dispatcher.start()
    try:
        now_ms = int(time.time() * 1000)
        warning = Alert(
            Severity.WARNING,
            "test",
            "BTCUSDT",
            "Warning",
            "first",
            now_ms,
            dedup_key="same",
        )
        duplicate = Alert(
            Severity.WARNING,
            "test",
            "BTCUSDT",
            "Warning again",
            "duplicate",
            now_ms + 1,
            dedup_key="same",
        )
        critical = Alert(
            Severity.CRITICAL,
            "test",
            "BTCUSDT",
            "Critical",
            "upgrade",
            now_ms + 2,
            dedup_key="same",
        )
        assert await dispatcher.emit(warning)
        assert not await dispatcher.emit(duplicate)
        assert await dispatcher.emit(critical)
    finally:
        await dispatcher.close()

    assert [item.severity for item in published] == [Severity.WARNING, Severity.CRITICAL]
    assert len(await database.list_alerts()) == 2


async def test_dispatcher_dedup_memory_is_bounded(example_config, tmp_path) -> None:
    example_config.notifiers.local.enabled = False
    database = Database(str(tmp_path / "alerts.db"))
    await database.initialize()

    async def publish(_alert: Alert) -> None:
        return None

    dispatcher = AlertDispatcher(example_config, database, publish)
    dispatcher._max_dedup_entries = 3
    await dispatcher.start()
    try:
        now_ms = int(time.time() * 1000)
        for index in range(8):
            await dispatcher.emit(
                Alert(
                    Severity.WARNING,
                    "test",
                    "BTCUSDT",
                    f"Alert {index}",
                    "test",
                    now_ms + index,
                    dedup_key=f"key-{index}",
                )
            )
    finally:
        await dispatcher.close()

    assert len(dispatcher._last_sent) == 3


async def test_dispatcher_restores_cooldown_after_restart(example_config, tmp_path) -> None:
    example_config.notifiers.local.enabled = False
    example_config.notifiers.ntfy.enabled = False
    example_config.notifiers.telegram.enabled = False
    example_config.notifiers.webhook.enabled = False
    database = Database(str(tmp_path / "alerts.db"))
    await database.initialize()

    async def publish(_alert: Alert) -> None:
        return None

    now_ms = int(time.time() * 1000)
    first = Alert(
        Severity.WARNING,
        "test",
        "BTCUSDT",
        "First",
        "persist cooldown",
        now_ms,
        dedup_key="restart-key",
    )
    dispatcher = AlertDispatcher(example_config, database, publish)
    await dispatcher.start()
    assert await dispatcher.emit(first)
    await dispatcher.close()

    restarted = AlertDispatcher(example_config, database, publish)
    await restarted.start()
    try:
        duplicate = Alert(
            Severity.WARNING,
            "test",
            "BTCUSDT",
            "Duplicate",
            "should remain suppressed after restart",
            now_ms + 1,
            dedup_key="restart-key",
        )
        upgrade = Alert(
            Severity.CRITICAL,
            "test",
            "BTCUSDT",
            "Upgrade",
            "critical upgrades remain allowed",
            now_ms + 2,
            dedup_key="restart-key",
        )
        assert not await restarted.emit(duplicate)
        assert await restarted.emit(upgrade)
    finally:
        await restarted.close()


async def test_dispatcher_persistence_failure_does_not_block_alarm(
    example_config, tmp_path
) -> None:
    example_config.notifiers.local.enabled = False
    example_config.notifiers.ntfy.enabled = False
    example_config.notifiers.telegram.enabled = False
    example_config.notifiers.webhook.enabled = False
    database = Database(str(tmp_path / "alerts.db"))
    await database.initialize()
    published: list[Alert] = []

    async def broken_save(_alert: Alert) -> None:
        raise OSError("simulated full disk")

    async def publish(alert: Alert) -> None:
        published.append(alert)

    database.save_alert = broken_save  # type: ignore[method-assign]
    dispatcher = AlertDispatcher(example_config, database, publish)
    await dispatcher.start()
    try:
        alert = Alert(
            Severity.CRITICAL,
            "test",
            "BTCUSDT",
            "Fail open",
            "browser/local delivery should survive optional storage failure",
            int(time.time() * 1000),
            dedup_key="fail-open",
        )
        assert await dispatcher.emit(alert)
    finally:
        await dispatcher.close()
    assert published == [alert]


async def test_slow_remote_notifier_does_not_stall_emit(example_config, tmp_path) -> None:
    example_config.notifiers.local.enabled = False
    example_config.notifiers.ntfy.enabled = False
    example_config.notifiers.telegram.enabled = False
    example_config.notifiers.webhook.enabled = True
    example_config.notifiers.webhook.url = "https://example.invalid/alert"
    database = Database(str(tmp_path / "alerts.db"))
    await database.initialize()

    async def publish(_alert: Alert) -> None:
        return None

    started = asyncio.Event()
    release = asyncio.Event()
    dispatcher = AlertDispatcher(example_config, database, publish)

    async def slow_delivery(_alert: Alert) -> None:
        started.set()
        await release.wait()

    dispatcher._deliver_remote = slow_delivery  # type: ignore[method-assign]
    await dispatcher.start()
    try:
        alert = Alert(
            Severity.CRITICAL,
            "test",
            "BTCUSDT",
            "Non-blocking",
            "remote delivery is intentionally paused",
            int(time.time() * 1000),
            dedup_key="slow-remote",
        )
        assert await asyncio.wait_for(dispatcher.emit(alert), timeout=0.2)
        await asyncio.wait_for(started.wait(), timeout=0.2)
        release.set()
    finally:
        release.set()
        await dispatcher.close()


async def test_remote_delivery_retries_transient_failure(example_config, tmp_path) -> None:
    example_config.notifiers.local.enabled = False
    example_config.notifiers.ntfy.enabled = True
    example_config.notifiers.ntfy.topic = "test-topic"
    example_config.notifiers.telegram.enabled = False
    example_config.notifiers.webhook.enabled = False
    database = Database(str(tmp_path / "alerts.db"))
    await database.initialize()

    async def publish(_alert: Alert) -> None:
        return None

    dispatcher = AlertDispatcher(example_config, database, publish)
    dispatcher._retry_delays = (0.0, 0.0)
    attempts = 0

    async def flaky_ntfy(_alert: Alert) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise TimeoutError("transient test failure")

    dispatcher._ntfy = flaky_ntfy  # type: ignore[method-assign]
    alert = Alert(
        Severity.CRITICAL,
        "test",
        "BTCUSDT",
        "Retry",
        "remote notifier should retry",
        int(time.time() * 1000),
        dedup_key="retry-test",
    )
    await dispatcher._deliver_remote(alert)
    assert attempts == 3
