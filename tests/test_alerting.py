from __future__ import annotations

import asyncio
import subprocess
import sys
import time

import pytest

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


async def test_failed_local_alarm_retries_records_failure_and_reopens_cooldown(
    example_config, tmp_path
) -> None:
    example_config.notifiers.local.enabled = True
    example_config.notifiers.ntfy.enabled = False
    example_config.notifiers.telegram.enabled = False
    example_config.notifiers.webhook.enabled = False
    database = Database(str(tmp_path / "alerts.db"))
    await database.initialize()

    async def publish(_alert: Alert) -> None:
        return None

    dispatcher = AlertDispatcher(example_config, database, publish)
    dispatcher._local_retry_delays = (0.0, 0.0)
    attempts = 0

    async def failed_sound(_alert: Alert) -> None:
        nonlocal attempts
        attempts += 1
        raise OSError("secret-bearing detail must not be persisted")

    dispatcher._local_sound = failed_sound  # type: ignore[method-assign]
    await dispatcher.start()
    try:
        first = Alert(
            Severity.CRITICAL,
            "test",
            "BTCUSDT",
            "Alarm failure",
            "retry the PC alarm",
            int(time.time() * 1000),
            dedup_key="local-failure",
        )
        assert await dispatcher.emit(first)
        await asyncio.wait_for(dispatcher._local_queue.join(), timeout=1)
        assert attempts == 3
        assert "local-failure" not in dispatcher._last_sent
        status = dispatcher.status()["local_alarm"]
        assert isinstance(status, dict)
        assert status["counts"] == {
            "queued": 1,
            "attempt": 3,
            "success": 0,
            "failure": 1,
        }
        last_receipt = status["last_receipt"]
        assert isinstance(last_receipt, dict)
        assert last_receipt["status"] == "failure"
        assert last_receipt["detail"] == "OSError"
        assert "secret" not in last_receipt["detail"]
        rows = await database.list_alerts()
        assert rows[0]["dedup_key"] == ""
        assert rows[0]["metrics"]["local_alarm"]["status"] == "failure"

        async def successful_sound(_alert: Alert) -> None:
            return None

        dispatcher._local_sound = successful_sound  # type: ignore[method-assign]
        retry = Alert(
            Severity.CRITICAL,
            "test",
            "BTCUSDT",
            "Alarm retry",
            "failure must not consume cooldown",
            int(time.time() * 1000),
            dedup_key="local-failure",
        )
        assert await dispatcher.emit(retry)
        await asyncio.wait_for(dispatcher._local_queue.join(), timeout=1)
        assert dispatcher.status()["local_alarm"]["counts"]["success"] == 1
    finally:
        await dispatcher.close()


async def test_local_alarm_queue_prioritizes_critical_alerts(example_config, tmp_path) -> None:
    example_config.notifiers.local.enabled = True
    database = Database(str(tmp_path / "alerts.db"))
    await database.initialize()

    async def publish(_alert: Alert) -> None:
        return None

    dispatcher = AlertDispatcher(example_config, database, publish)
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    order: list[str] = []

    async def ordered_sound(alert: Alert) -> None:
        order.append(alert.title)
        if alert.title == "first warning":
            first_started.set()
            await release_first.wait()

    dispatcher._local_sound = ordered_sound  # type: ignore[method-assign]
    await dispatcher.start()
    try:
        now_ms = int(time.time() * 1000)
        first = Alert(
            Severity.WARNING,
            "test",
            "BTCUSDT",
            "first warning",
            "blocks worker briefly",
            now_ms,
            dedup_key="priority-1",
        )
        second = Alert(
            Severity.WARNING,
            "test",
            "BTCUSDT",
            "second warning",
            "must wait behind critical",
            now_ms + 1,
            dedup_key="priority-2",
        )
        critical = Alert(
            Severity.CRITICAL,
            "test",
            "BTCUSDT",
            "critical",
            "must be next",
            now_ms + 2,
            dedup_key="priority-critical",
        )
        assert await dispatcher.emit(first)
        await asyncio.wait_for(first_started.wait(), timeout=1)
        assert await dispatcher.emit(second)
        assert await dispatcher.emit(critical)
        release_first.set()
        await asyncio.wait_for(dispatcher._local_queue.join(), timeout=1)
    finally:
        release_first.set()
        await dispatcher.close()
    assert order == ["first warning", "critical", "second warning"]


async def test_custom_local_command_requires_zero_exit_status(example_config, tmp_path) -> None:
    example_config.notifiers.local.custom_command = [
        sys.executable,
        "-c",
        "raise SystemExit(7)",
    ]
    database = Database(str(tmp_path / "alerts.db"))

    async def publish(_alert: Alert) -> None:
        return None

    dispatcher = AlertDispatcher(example_config, database, publish)
    alert = Alert(
        Severity.WARNING,
        "test",
        "BTCUSDT",
        "command",
        "checked command",
        int(time.time() * 1000),
    )
    with pytest.raises(subprocess.CalledProcessError) as raised:
        await dispatcher._local_sound(alert)
    assert raised.value.returncode == 7


async def test_dispatcher_exposes_worker_tasks_for_supervision(example_config, tmp_path) -> None:
    database = Database(str(tmp_path / "alerts.db"))
    await database.initialize()

    async def publish(_alert: Alert) -> None:
        return None

    dispatcher = AlertDispatcher(example_config, database, publish)
    assert dispatcher.worker_tasks() == ()
    await dispatcher.start()
    try:
        tasks = dispatcher.worker_tasks()
        assert len(tasks) == 3
        assert {task.get_name() for task in tasks} == {
            "notifier-0",
            "notifier-1",
            "local-alarm-worker",
        }
    finally:
        await dispatcher.close()
    assert dispatcher.worker_tasks() == ()


async def test_full_warning_queue_evicts_warning_for_critical(example_config, tmp_path) -> None:
    example_config.notifiers.local.enabled = True
    example_config.notifiers.ntfy.enabled = False
    example_config.notifiers.telegram.enabled = False
    example_config.notifiers.webhook.enabled = False
    database = Database(str(tmp_path / "alerts.db"))
    await database.initialize()

    async def publish(_alert: Alert) -> None:
        return None

    dispatcher = AlertDispatcher(example_config, database, publish)
    dispatcher._local_queue = asyncio.PriorityQueue(maxsize=2)
    now_ms = int(time.time() * 1000)
    first_warning = Alert(
        Severity.WARNING,
        "test",
        "BTCUSDT",
        "first warning",
        "lower priority",
        now_ms,
        dedup_key="warning-1",
    )
    second_warning = Alert(
        Severity.WARNING,
        "test",
        "BTCUSDT",
        "second warning",
        "lower priority",
        now_ms + 1,
        dedup_key="warning-2",
    )
    critical = Alert(
        Severity.CRITICAL,
        "test",
        "BTCUSDT",
        "critical",
        "must be admitted",
        now_ms + 2,
        dedup_key="critical-admission",
    )

    assert await dispatcher.emit(first_warning)
    assert await dispatcher.emit(second_warning)
    assert await dispatcher.emit(critical)

    queued_titles: list[str] = []
    while not dispatcher._local_queue.empty():
        _, _, work = dispatcher._local_queue.get_nowait()
        queued_titles.append(work.alert.title)
        dispatcher._local_queue.task_done()
    await asyncio.wait_for(dispatcher._local_queue.join(), timeout=0.1)

    assert queued_titles == ["critical", "second warning"]
    rows = {row["title"]: row for row in await database.list_alerts()}
    assert rows["first warning"]["dedup_key"] == ""
    assert rows["first warning"]["metrics"]["local_alarm"] == {
        "status": "failure",
        "attempt": 0,
        "timestamp_ms": rows["first warning"]["metrics"]["local_alarm"]["timestamp_ms"],
        "detail": "evicted_for_critical",
    }
    assert rows["critical"]["metrics"]["local_alarm"]["status"] == "queued"
    assert "warning-1" not in dispatcher._last_sent
    assert "critical-admission" in dispatcher._last_sent


async def test_local_alarm_emit_returns_false_when_all_critical_queue_is_full(
    example_config, tmp_path
) -> None:
    example_config.notifiers.local.enabled = True
    example_config.notifiers.ntfy.enabled = False
    example_config.notifiers.telegram.enabled = False
    example_config.notifiers.webhook.enabled = False
    database = Database(str(tmp_path / "alerts.db"))
    await database.initialize()

    async def publish(_alert: Alert) -> None:
        return None

    dispatcher = AlertDispatcher(example_config, database, publish)
    dispatcher._local_queue = asyncio.PriorityQueue(maxsize=1)
    now_ms = int(time.time() * 1000)
    admitted = Alert(
        Severity.CRITICAL,
        "test",
        "BTCUSDT",
        "admitted critical",
        "occupies the queue",
        now_ms,
        dedup_key="critical-full-1",
    )
    rejected = Alert(
        Severity.CRITICAL,
        "test",
        "BTCUSDT",
        "rejected critical",
        "caller must retry",
        now_ms + 1,
        dedup_key="critical-full-2",
    )

    assert await dispatcher.emit(admitted)
    assert not await dispatcher.emit(rejected)
    assert "critical-full-1" in dispatcher._last_sent
    assert "critical-full-2" not in dispatcher._last_sent

    _, _, queued_work = dispatcher._local_queue.get_nowait()
    dispatcher._local_queue.task_done()
    assert queued_work.alert.id == admitted.id
    rows = {row["id"]: row for row in await database.list_alerts()}
    assert rows[rejected.id]["metrics"]["local_alarm"]["status"] == "failure"
    assert rows[rejected.id]["metrics"]["local_alarm"]["detail"] == "queue_full"


async def test_queued_local_alarm_replays_after_restart_and_restores_cooldown(
    example_config, tmp_path
) -> None:
    example_config.notifiers.local.enabled = True
    example_config.notifiers.ntfy.enabled = False
    example_config.notifiers.telegram.enabled = False
    example_config.notifiers.webhook.enabled = False
    database = Database(str(tmp_path / "alerts.db"))
    await database.initialize()

    async def publish(_alert: Alert) -> None:
        return None

    crashed = AlertDispatcher(example_config, database, publish)
    now_ms = int(time.time() * 1000)
    alert = Alert(
        Severity.CRITICAL,
        "test",
        "BTCUSDT",
        "persisted before crash",
        "must replay after restart",
        now_ms,
        dedup_key="restart-local-queued",
    )
    assert await crashed.emit(alert)
    pending = await database.list_pending_local_alarms()
    assert [(row["id"], row["local_status"]) for row in pending] == [(alert.id, "queued")]

    replayed: list[str] = []
    restarted = AlertDispatcher(example_config, database, publish)

    async def successful_sound(replayed_alert: Alert) -> None:
        replayed.append(replayed_alert.id)

    restarted._local_sound = successful_sound  # type: ignore[method-assign]
    await restarted.start()
    try:
        await asyncio.wait_for(restarted._local_queue.join(), timeout=1)
        assert replayed == [alert.id]
        assert await database.list_pending_local_alarms() == []
        stored = {row["id"]: row for row in await database.list_alerts()}[alert.id]
        assert stored["dedup_key"] == "restart-local-queued"
        assert stored["metrics"]["local_alarm"]["status"] == "success"
        assert "restart-local-queued" in restarted._last_sent
    finally:
        await restarted.close()

    after_success = AlertDispatcher(example_config, database, publish)
    await after_success.start()
    try:
        duplicate = Alert(
            Severity.CRITICAL,
            "test",
            "BTCUSDT",
            "duplicate",
            "successful replay restored cooldown",
            now_ms + 1,
            dedup_key="restart-local-queued",
        )
        assert not await after_success.emit(duplicate)
    finally:
        await after_success.close()


async def test_restart_replay_refills_bounded_queue_for_attempt_and_queued_jobs(
    example_config, tmp_path
) -> None:
    example_config.notifiers.local.enabled = True
    example_config.notifiers.ntfy.enabled = False
    example_config.notifiers.telegram.enabled = False
    example_config.notifiers.webhook.enabled = False
    database = Database(str(tmp_path / "alerts.db"))
    await database.initialize()
    now_ms = int(time.time() * 1000)
    for index, status in enumerate(("attempt", "queued"), start=1):
        alert = Alert(
            Severity.WARNING,
            "test",
            "BTCUSDT",
            f"{status} alarm",
            "must survive restart",
            now_ms + index,
            metrics={
                "local_alarm": {
                    "status": status,
                    "attempt": index if status == "attempt" else 0,
                    "timestamp_ms": now_ms + index,
                }
            },
            dedup_key="",
        )
        await database.save_local_alarm_state(
            alert,
            original_dedup_key=f"pending-{status}",
            priority=1,
            status=status,
            attempt=index if status == "attempt" else 0,
            updated_ms=now_ms + index,
        )

    async def publish(_alert: Alert) -> None:
        return None

    replayed: list[str] = []
    restarted = AlertDispatcher(example_config, database, publish)
    restarted._local_queue = asyncio.PriorityQueue(maxsize=1)

    async def successful_sound(alert: Alert) -> None:
        replayed.append(alert.title)

    restarted._local_sound = successful_sound  # type: ignore[method-assign]
    await restarted.start()
    try:
        await asyncio.wait_for(restarted._local_queue.join(), timeout=1)
    finally:
        await restarted.close()

    assert replayed == ["attempt alarm", "queued alarm"]
    assert await database.list_pending_local_alarms() == []


async def test_local_alarm_status_keeps_latest_terminal_outcome(example_config, tmp_path) -> None:
    example_config.notifiers.local.enabled = False
    database = Database(str(tmp_path / "alerts.db"))
    await database.initialize()

    async def publish(_alert: Alert) -> None:
        return None

    dispatcher = AlertDispatcher(example_config, database, publish)
    failed = Alert(
        Severity.CRITICAL,
        "test",
        "BTCUSDT",
        "failed",
        "terminal outcome",
        int(time.time() * 1000),
    )
    queued = Alert(
        Severity.WARNING,
        "test",
        "ETHUSDT",
        "queued",
        "newer non-terminal receipt",
        failed.timestamp_ms + 1,
    )
    await dispatcher._record_local_receipt(failed, "failure", 3, "OSError")
    await dispatcher._record_local_receipt(queued, "queued", 0)

    local_status = dispatcher.status()["local_alarm"]
    assert isinstance(local_status, dict)
    assert local_status["last_receipt"]["status"] == "queued"
    assert local_status["last_outcome"]["status"] == "failure"
    assert local_status["last_outcome"]["alert_id"] == failed.id

    restarted = AlertDispatcher(example_config, database, publish)
    await restarted.start()
    try:
        restored_status = restarted.status()["local_alarm"]
        assert isinstance(restored_status, dict)
        assert restored_status["last_receipt"] is None
        assert restored_status["last_outcome"]["status"] == "failure"
        assert restored_status["last_outcome"]["alert_id"] == failed.id
    finally:
        await restarted.close()


async def test_local_alarm_outcome_survives_receipt_pruning_until_success(
    example_config, tmp_path
) -> None:
    example_config.notifiers.local.enabled = True
    example_config.notifiers.ntfy.enabled = False
    example_config.notifiers.telegram.enabled = False
    example_config.notifiers.webhook.enabled = False
    database = Database(str(tmp_path / "alerts.db"))
    await database.initialize()

    async def publish(_alert: Alert) -> None:
        return None

    failed_dispatcher = AlertDispatcher(example_config, database, publish)
    failed_dispatcher._local_retry_delays = (0.0, 0.0)

    async def failed_sound(_alert: Alert) -> None:
        raise OSError("durable failure")

    failed_dispatcher._local_sound = failed_sound  # type: ignore[method-assign]
    await failed_dispatcher.start()
    failed_alert = Alert(
        Severity.CRITICAL,
        "test",
        "BTCUSDT",
        "failed alarm",
        "must remain unresolved across receipt pruning",
        int(time.time() * 1000),
        dedup_key="durable-outcome-failure",
    )
    try:
        assert await failed_dispatcher.emit(failed_alert)
        await asyncio.wait_for(failed_dispatcher._local_queue.join(), timeout=1)
        assert failed_dispatcher.status()["local_alarm"]["last_outcome"]["status"] == "failure"
    finally:
        await failed_dispatcher.close()

    await database.prune_delivery_receipts(int(time.time() * 1000) + 10_000)
    assert await database.list_delivery_receipts() == []

    recovered_dispatcher = AlertDispatcher(example_config, database, publish)

    async def successful_sound(_alert: Alert) -> None:
        return None

    recovered_dispatcher._local_sound = successful_sound  # type: ignore[method-assign]
    await recovered_dispatcher.start()
    successful_alert = Alert(
        Severity.CRITICAL,
        "test",
        "ETHUSDT",
        "successful alarm",
        "later terminal success clears the failure",
        int(time.time() * 1000),
        dedup_key="durable-outcome-success",
    )
    try:
        restored = recovered_dispatcher.status()["local_alarm"]["last_outcome"]
        assert restored["status"] == "failure"
        assert restored["alert_id"] == failed_alert.id
        assert await recovered_dispatcher.emit(successful_alert)
        await asyncio.wait_for(recovered_dispatcher._local_queue.join(), timeout=1)
        cleared = recovered_dispatcher.status()["local_alarm"]["last_outcome"]
        assert cleared["status"] == "success"
        assert cleared["alert_id"] == successful_alert.id
    finally:
        await recovered_dispatcher.close()

    await database.prune_delivery_receipts(int(time.time() * 1000) + 10_000)
    final_restart = AlertDispatcher(example_config, database, publish)
    await final_restart.start()
    try:
        durable_success = final_restart.status()["local_alarm"]["last_outcome"]
        assert durable_success["status"] == "success"
        assert durable_success["alert_id"] == successful_alert.id
    finally:
        await final_restart.close()
