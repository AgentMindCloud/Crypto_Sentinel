from __future__ import annotations

import sqlite3

from crypto_sentinel.models import Alert, MetricSnapshot, Severity
from crypto_sentinel.persistence import Database


async def test_database_roundtrip(tmp_path) -> None:
    database = Database(str(tmp_path / "sentinel.db"))
    await database.initialize()
    alert = Alert(
        severity=Severity.WARNING,
        category="test",
        symbol="BTCUSDT",
        title="Test",
        message="Round trip",
        timestamp_ms=123456,
        dedup_key="test:btc",
    )
    await database.save_alert(alert)
    rows = await database.list_alerts()
    assert len(rows) == 1
    assert rows[0]["id"] == alert.id
    assert rows[0]["severity"] == "warning"


async def test_metric_history_filters_and_orders_chronologically(tmp_path) -> None:
    database = Database(str(tmp_path / "sentinel.db"))
    await database.initialize()
    await database.save_metrics(
        [
            MetricSnapshot(
                exchange="binance",
                symbol="BTCUSDT",
                timestamp_ms=2000,
                window_seconds=60,
                ready=True,
                price=102.0,
            ),
            MetricSnapshot(
                exchange="binance",
                symbol="BTCUSDT",
                timestamp_ms=1000,
                window_seconds=60,
                ready=False,
                price=101.0,
            ),
            MetricSnapshot(
                exchange="okx",
                symbol="ETHUSDT",
                timestamp_ms=3000,
                window_seconds=300,
                ready=True,
                price=200.0,
            ),
        ]
    )

    rows = await database.list_metrics(
        symbol="btcusdt",
        exchange="BINANCE",
        window_seconds=60,
        since_ms=500,
    )

    assert [row["timestamp_ms"] for row in rows] == [1000, 2000]
    assert [row["ready"] for row in rows] == [False, True]
    assert [row["price"] for row in rows] == [101.0, 102.0]


async def test_delivery_receipt_roundtrip_and_filter(tmp_path) -> None:
    database = Database(str(tmp_path / "sentinel.db"))
    await database.initialize()
    await database.save_delivery_receipt(
        "alert-1",
        "local",
        "attempt",
        1,
        "starting local alarm",
        timestamp_ms=1000,
    )
    await database.save_delivery_receipt(
        "alert-1",
        "local",
        "success",
        1,
        timestamp_ms=1100,
    )
    await database.save_delivery_receipt(
        "alert-2",
        "browser",
        "published",
        1,
        timestamp_ms=1200,
    )

    rows = await database.list_delivery_receipts(alert_id="alert-1")

    assert [row["status"] for row in rows] == ["success", "attempt"]
    assert all(row["alert_id"] == "alert-1" for row in rows)

    deleted = await database.prune_delivery_receipts(1050)
    remaining = await database.list_delivery_receipts()

    assert deleted == 1
    assert [row["status"] for row in remaining] == ["published", "success"]


async def test_initialize_upgrades_existing_metrics_schema(tmp_path) -> None:
    path = tmp_path / "sentinel.db"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE metrics (
                timestamp_ms INTEGER NOT NULL,
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                window_seconds INTEGER NOT NULL,
                ready INTEGER NOT NULL,
                price REAL,
                return_bps REAL,
                return_z REAL,
                quote_volume REAL,
                volume_z REAL,
                taker_imbalance REAL,
                trades INTEGER NOT NULL,
                baseline_points INTEGER NOT NULL,
                data_age_seconds REAL,
                PRIMARY KEY(timestamp_ms, exchange, symbol, window_seconds)
            )
            """
        )

    database = Database(str(path))
    await database.initialize()

    with sqlite3.connect(path) as connection:
        columns = {
            str(row[1]) for row in connection.execute("PRAGMA table_info(metrics)").fetchall()
        }
    assert {
        "readiness_reason",
        "window_coverage_ratio",
        "largest_gap_seconds",
        "recovery_seconds_remaining",
    }.issubset(columns)


async def test_local_alarm_state_roundtrip_tracks_pending_and_terminal_cooldown(
    tmp_path,
) -> None:
    database = Database(str(tmp_path / "sentinel.db"))
    await database.initialize()
    alert = Alert(
        severity=Severity.CRITICAL,
        category="test",
        symbol="BTCUSDT",
        title="Durable alarm",
        message="Replay until terminal",
        timestamp_ms=1000,
        metrics={
            "local_alarm": {
                "status": "attempt",
                "attempt": 1,
                "timestamp_ms": 1500,
            }
        },
        dedup_key="",
    )

    await database.save_local_alarm_state(
        alert,
        original_dedup_key="durable-key",
        priority=0,
        status="attempt",
        attempt=1,
        updated_ms=1500,
    )

    pending = await database.list_pending_local_alarms()
    assert len(pending) == 1
    assert pending[0]["id"] == alert.id
    assert pending[0]["original_dedup_key"] == "durable-key"
    assert pending[0]["local_status"] == "attempt"
    assert await database.list_recent_dedup(0) == []

    alert.dedup_key = "durable-key"
    alert.metrics["local_alarm"] = {
        "status": "success",
        "attempt": 2,
        "timestamp_ms": 2500,
    }
    await database.save_local_alarm_state(
        alert,
        original_dedup_key="durable-key",
        priority=0,
        status="success",
        attempt=2,
        updated_ms=2500,
    )

    assert await database.list_pending_local_alarms() == []
    assert await database.list_recent_dedup(2000) == [
        {
            "dedup_key": "durable-key",
            "timestamp_ms": 2500,
            "severity": "critical",
        }
    ]


async def test_latest_delivery_outcome_ignores_newer_non_terminal_receipt(
    tmp_path,
) -> None:
    database = Database(str(tmp_path / "sentinel.db"))
    await database.initialize()
    await database.save_delivery_receipt(
        "failed-alert",
        "local",
        "failure",
        3,
        "OSError",
        timestamp_ms=1000,
    )
    await database.save_delivery_receipt(
        "queued-alert",
        "local",
        "queued",
        0,
        timestamp_ms=2000,
    )

    outcome = await database.get_latest_delivery_outcome("local")

    assert outcome is not None
    assert outcome["alert_id"] == "failed-alert"
    assert outcome["status"] == "failure"


async def test_newer_local_failure_invalidates_older_successful_cooldown(
    tmp_path,
) -> None:
    database = Database(str(tmp_path / "sentinel.db"))
    await database.initialize()
    successful = Alert(
        severity=Severity.WARNING,
        category="test",
        symbol="BTCUSDT",
        title="Successful alarm",
        message="Older delivery",
        timestamp_ms=1000,
        dedup_key="shared-key",
    )
    await database.save_local_alarm_state(
        successful,
        original_dedup_key="shared-key",
        priority=1,
        status="success",
        attempt=1,
        updated_ms=1000,
    )
    failed = Alert(
        severity=Severity.CRITICAL,
        category="test",
        symbol="BTCUSDT",
        title="Failed upgrade",
        message="Must reopen the cooldown",
        timestamp_ms=1500,
        dedup_key="",
    )
    await database.save_local_alarm_state(
        failed,
        original_dedup_key="shared-key",
        priority=0,
        status="failure",
        attempt=3,
        updated_ms=1500,
    )

    assert await database.list_recent_dedup(0) == []
