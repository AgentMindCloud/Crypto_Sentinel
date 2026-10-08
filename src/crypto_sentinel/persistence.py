from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from crypto_sentinel.models import Alert, MetricSnapshot


class Database:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._lock = asyncio.Lock()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    async def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)

        def create() -> None:
            with self._connect() as connection:
                connection.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS alerts (
                        id TEXT PRIMARY KEY,
                        timestamp_ms INTEGER NOT NULL,
                        severity TEXT NOT NULL,
                        category TEXT NOT NULL,
                        symbol TEXT NOT NULL,
                        direction TEXT NOT NULL,
                        title TEXT NOT NULL,
                        message TEXT NOT NULL,
                        exchanges_json TEXT NOT NULL,
                        metrics_json TEXT NOT NULL,
                        dedup_key TEXT NOT NULL,
                        source TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON alerts(timestamp_ms DESC);
                    CREATE INDEX IF NOT EXISTS idx_alerts_symbol ON alerts(symbol, timestamp_ms DESC);

                    CREATE TABLE IF NOT EXISTS metrics (
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
                        readiness_reason TEXT NOT NULL DEFAULT 'unavailable',
                        window_coverage_ratio REAL NOT NULL DEFAULT 0,
                        largest_gap_seconds REAL,
                        recovery_seconds_remaining REAL NOT NULL DEFAULT 0,
                        PRIMARY KEY(timestamp_ms, exchange, symbol, window_seconds)
                    );
                    CREATE INDEX IF NOT EXISTS idx_metrics_lookup
                        ON metrics(symbol, window_seconds, timestamp_ms DESC);

                    CREATE TABLE IF NOT EXISTS delivery_receipts (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        alert_id TEXT NOT NULL,
                        timestamp_ms INTEGER NOT NULL,
                        channel TEXT NOT NULL,
                        status TEXT NOT NULL,
                        attempt INTEGER NOT NULL,
                        detail TEXT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_delivery_receipts_alert
                        ON delivery_receipts(alert_id, timestamp_ms DESC);
                    CREATE INDEX IF NOT EXISTS idx_delivery_receipts_timestamp
                        ON delivery_receipts(timestamp_ms DESC);

                    CREATE TABLE IF NOT EXISTS local_alarm_jobs (
                        alert_id TEXT PRIMARY KEY,
                        priority INTEGER NOT NULL,
                        original_dedup_key TEXT NOT NULL,
                        status TEXT NOT NULL,
                        attempt INTEGER NOT NULL,
                        updated_ms INTEGER NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_local_alarm_jobs_pending
                        ON local_alarm_jobs(status, priority, updated_ms);

                    CREATE TABLE IF NOT EXISTS local_alarm_outcome (
                        channel TEXT PRIMARY KEY,
                        alert_id TEXT NOT NULL,
                        status TEXT NOT NULL,
                        attempt INTEGER NOT NULL,
                        updated_ms INTEGER NOT NULL,
                        detail TEXT NOT NULL
                    );
                    """
                )
                metric_columns = {
                    str(row["name"])
                    for row in connection.execute("PRAGMA table_info(metrics)").fetchall()
                }
                for name, definition in (
                    ("readiness_reason", "TEXT NOT NULL DEFAULT 'unavailable'"),
                    ("window_coverage_ratio", "REAL NOT NULL DEFAULT 0"),
                    ("largest_gap_seconds", "REAL"),
                    ("recovery_seconds_remaining", "REAL NOT NULL DEFAULT 0"),
                ):
                    if name not in metric_columns:
                        connection.execute(f"ALTER TABLE metrics ADD COLUMN {name} {definition}")

        await asyncio.to_thread(create)

    @staticmethod
    def _write_alert(connection: sqlite3.Connection, alert: Alert) -> None:
        payload = alert.to_dict()
        connection.execute(
            """
            INSERT OR REPLACE INTO alerts (
                id, timestamp_ms, severity, category, symbol, direction, title, message,
                exchanges_json, metrics_json, dedup_key, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                alert.id,
                alert.timestamp_ms,
                alert.severity.value,
                alert.category,
                alert.symbol,
                alert.direction,
                alert.title,
                alert.message,
                json.dumps(payload["exchanges"], separators=(",", ":")),
                json.dumps(payload["metrics"], separators=(",", ":")),
                alert.dedup_key,
                alert.source,
            ),
        )

    @staticmethod
    def _alert_row(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"],
            "timestamp_ms": row["timestamp_ms"],
            "severity": row["severity"],
            "category": row["category"],
            "symbol": row["symbol"],
            "direction": row["direction"],
            "title": row["title"],
            "message": row["message"],
            "exchanges": json.loads(row["exchanges_json"]),
            "metrics": json.loads(row["metrics_json"]),
            "dedup_key": row["dedup_key"],
            "source": row["source"],
        }

    async def save_alert(self, alert: Alert) -> None:
        def write() -> None:
            with self._connect() as connection:
                self._write_alert(connection, alert)

        async with self._lock:
            await asyncio.to_thread(write)

    async def save_local_alarm_state(
        self,
        alert: Alert,
        *,
        original_dedup_key: str,
        priority: int,
        status: str,
        attempt: int,
        updated_ms: int | None = None,
        detail: str = "",
    ) -> None:
        """Atomically persist an alert and its durable local-alarm lifecycle."""
        if status not in {"queued", "attempt", "success", "failure"}:
            raise ValueError(f"unsupported local alarm status: {status}")
        safe_priority = max(0, min(int(priority), 100))
        safe_attempt = max(0, int(attempt))
        safe_updated_ms = int(time.time() * 1000) if updated_ms is None else int(updated_ms)
        safe_dedup_key = str(original_dedup_key)[:300]
        safe_detail = str(detail)[:160]

        def write() -> None:
            with self._connect() as connection:
                self._write_alert(connection, alert)
                connection.execute(
                    """
                    INSERT INTO local_alarm_jobs (
                        alert_id, priority, original_dedup_key, status, attempt, updated_ms
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(alert_id) DO UPDATE SET
                        priority = excluded.priority,
                        original_dedup_key = excluded.original_dedup_key,
                        status = excluded.status,
                        attempt = excluded.attempt,
                        updated_ms = excluded.updated_ms
                    """,
                    (
                        alert.id,
                        safe_priority,
                        safe_dedup_key,
                        status,
                        safe_attempt,
                        safe_updated_ms,
                    ),
                )
                if status in {"success", "failure"}:
                    connection.execute(
                        """
                        INSERT INTO local_alarm_outcome (
                            channel, alert_id, status, attempt, updated_ms, detail
                        ) VALUES ('local', ?, ?, ?, ?, ?)
                        ON CONFLICT(channel) DO UPDATE SET
                            alert_id = excluded.alert_id,
                            status = excluded.status,
                            attempt = excluded.attempt,
                            updated_ms = excluded.updated_ms,
                            detail = excluded.detail
                        """,
                        (
                            alert.id,
                            status,
                            safe_attempt,
                            safe_updated_ms,
                            safe_detail,
                        ),
                    )

        async with self._lock:
            await asyncio.to_thread(write)

    async def save_metrics(self, snapshots: list[MetricSnapshot]) -> None:
        if not snapshots:
            return
        rows = [
            (
                item.timestamp_ms,
                item.exchange,
                item.symbol,
                item.window_seconds,
                int(item.ready),
                item.price,
                item.return_bps,
                item.return_z,
                item.quote_volume,
                item.volume_z,
                item.taker_imbalance,
                item.trades,
                item.baseline_points,
                item.data_age_seconds,
                item.readiness_reason,
                item.window_coverage_ratio,
                item.largest_gap_seconds,
                item.recovery_seconds_remaining,
            )
            for item in snapshots
        ]

        def write() -> None:
            with self._connect() as connection:
                connection.executemany(
                    """
                    INSERT OR REPLACE INTO metrics (
                        timestamp_ms, exchange, symbol, window_seconds, ready, price, return_bps,
                        return_z, quote_volume, volume_z, taker_imbalance, trades,
                        baseline_points, data_age_seconds, readiness_reason,
                        window_coverage_ratio, largest_gap_seconds, recovery_seconds_remaining
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )

        async with self._lock:
            await asyncio.to_thread(write)

    async def list_alerts(self, limit: int = 100) -> list[dict[str, Any]]:
        safe_limit = max(1, min(limit, 1000))

        def read() -> list[dict[str, Any]]:
            with self._connect() as connection:
                rows = connection.execute(
                    "SELECT * FROM alerts ORDER BY timestamp_ms DESC LIMIT ?", (safe_limit,)
                ).fetchall()
            return [self._alert_row(row) for row in rows]

        return await asyncio.to_thread(read)

    async def list_recent_dedup(self, cutoff_ms: int, limit: int = 10_000) -> list[dict[str, Any]]:
        safe_limit = max(1, min(limit, 50_000))

        def read() -> list[dict[str, Any]]:
            with self._connect() as connection:
                rows = connection.execute(
                    """
                    SELECT
                        dedup_key,
                        timestamp_ms,
                        severity,
                        restores_cooldown
                    FROM (
                        SELECT
                            alerts.dedup_key AS dedup_key,
                            alerts.timestamp_ms AS timestamp_ms,
                            alerts.severity AS severity,
                            1 AS restores_cooldown
                        FROM alerts
                        WHERE alerts.dedup_key <> '' AND alerts.timestamp_ms >= ?

                        UNION ALL

                        SELECT
                            local_alarm_jobs.original_dedup_key AS dedup_key,
                            local_alarm_jobs.updated_ms AS timestamp_ms,
                            alerts.severity AS severity,
                            CASE
                                WHEN local_alarm_jobs.status = 'success' THEN 1
                                ELSE 0
                            END AS restores_cooldown
                        FROM local_alarm_jobs
                        JOIN alerts ON alerts.id = local_alarm_jobs.alert_id
                        WHERE local_alarm_jobs.original_dedup_key <> ''
                          AND local_alarm_jobs.updated_ms >= ?
                    )
                    ORDER BY timestamp_ms DESC, restores_cooldown ASC
                    LIMIT ?
                    """,
                    (cutoff_ms, cutoff_ms, safe_limit),
                ).fetchall()
            result: list[dict[str, Any]] = []
            seen: set[str] = set()
            for row in rows:
                key = str(row["dedup_key"])
                if key in seen:
                    continue
                seen.add(key)
                if not bool(row["restores_cooldown"]):
                    continue
                result.append(
                    {
                        "dedup_key": key,
                        "timestamp_ms": int(row["timestamp_ms"]),
                        "severity": str(row["severity"]),
                    }
                )
            return result

        return await asyncio.to_thread(read)

    async def list_pending_local_alarms(
        self,
        limit: int = 256,
        *,
        exclude_alert_ids: tuple[str, ...] = (),
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, min(limit, 10_000))
        safe_excluded = tuple(str(alert_id)[:100] for alert_id in exclude_alert_ids[:1000])
        exclusion = ""
        parameters: list[Any] = []
        if safe_excluded:
            placeholders = ", ".join("?" for _ in safe_excluded)
            exclusion = f"AND local_alarm_jobs.alert_id NOT IN ({placeholders})"
            parameters.extend(safe_excluded)
        parameters.append(safe_limit)

        def read() -> list[dict[str, Any]]:
            with self._connect() as connection:
                rows = connection.execute(
                    f"""
                    SELECT
                        alerts.*,
                        local_alarm_jobs.priority AS local_priority,
                        local_alarm_jobs.original_dedup_key AS original_dedup_key,
                        local_alarm_jobs.status AS local_status,
                        local_alarm_jobs.attempt AS local_attempt,
                        local_alarm_jobs.updated_ms AS local_updated_ms
                    FROM local_alarm_jobs
                    JOIN alerts ON alerts.id = local_alarm_jobs.alert_id
                    WHERE local_alarm_jobs.status IN ('queued', 'attempt')
                    {exclusion}
                    ORDER BY
                        local_alarm_jobs.priority ASC,
                        local_alarm_jobs.updated_ms ASC,
                        alerts.timestamp_ms ASC
                    LIMIT ?
                    """,
                    parameters,
                ).fetchall()
            result: list[dict[str, Any]] = []
            for row in rows:
                item = self._alert_row(row)
                item.update(
                    {
                        "local_priority": int(row["local_priority"]),
                        "original_dedup_key": str(row["original_dedup_key"]),
                        "local_status": str(row["local_status"]),
                        "local_attempt": int(row["local_attempt"]),
                        "local_updated_ms": int(row["local_updated_ms"]),
                    }
                )
                result.append(item)
            return result

        return await asyncio.to_thread(read)

    async def list_metrics(
        self,
        *,
        symbol: str = "",
        exchange: str = "",
        window_seconds: int | None = None,
        since_ms: int | None = None,
        limit: int = 2000,
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, min(limit, 5000))
        clauses: list[str] = []
        parameters: list[Any] = []
        if symbol:
            clauses.append("symbol = ?")
            parameters.append(symbol.upper()[:40])
        if exchange:
            clauses.append("exchange = ?")
            parameters.append(exchange.lower()[:40])
        if window_seconds is not None:
            clauses.append("window_seconds = ?")
            parameters.append(max(1, int(window_seconds)))
        if since_ms is not None:
            clauses.append("timestamp_ms >= ?")
            parameters.append(max(0, int(since_ms)))
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        def read() -> list[dict[str, Any]]:
            with self._connect() as connection:
                rows = connection.execute(
                    f"""
                    SELECT timestamp_ms, exchange, symbol, window_seconds, ready, price,
                           return_bps, return_z, quote_volume, volume_z, taker_imbalance,
                           trades, baseline_points, data_age_seconds, readiness_reason,
                           window_coverage_ratio, largest_gap_seconds,
                           recovery_seconds_remaining
                    FROM metrics
                    {where}
                    ORDER BY timestamp_ms DESC
                    LIMIT ?
                    """,
                    (*parameters, safe_limit),
                ).fetchall()
            result = [dict(row) for row in rows]
            result.reverse()
            for row in result:
                row["ready"] = bool(row["ready"])
            return result

        return await asyncio.to_thread(read)

    async def save_delivery_receipt(
        self,
        alert_id: str,
        channel: str,
        status: str,
        attempt: int,
        detail: str = "",
        timestamp_ms: int | None = None,
    ) -> None:
        safe_alert_id = str(alert_id)[:100]
        safe_channel = str(channel)[:40]
        safe_status = str(status)[:40]
        safe_attempt = max(0, int(attempt))
        safe_detail = str(detail)[:500]
        safe_timestamp = int(timestamp_ms or time.time() * 1000)

        def write() -> None:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO delivery_receipts (
                        alert_id, timestamp_ms, channel, status, attempt, detail
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        safe_alert_id,
                        safe_timestamp,
                        safe_channel,
                        safe_status,
                        safe_attempt,
                        safe_detail,
                    ),
                )

        async with self._lock:
            await asyncio.to_thread(write)

    async def list_delivery_receipts(
        self,
        *,
        alert_id: str = "",
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, min(limit, 2000))
        safe_alert_id = str(alert_id)[:100]

        def read() -> list[dict[str, Any]]:
            with self._connect() as connection:
                if safe_alert_id:
                    rows = connection.execute(
                        """
                        SELECT alert_id, timestamp_ms, channel, status, attempt, detail
                        FROM delivery_receipts
                        WHERE alert_id = ?
                        ORDER BY timestamp_ms DESC, id DESC
                        LIMIT ?
                        """,
                        (safe_alert_id, safe_limit),
                    ).fetchall()
                else:
                    rows = connection.execute(
                        """
                        SELECT alert_id, timestamp_ms, channel, status, attempt, detail
                        FROM delivery_receipts
                        ORDER BY timestamp_ms DESC, id DESC
                        LIMIT ?
                        """,
                        (safe_limit,),
                    ).fetchall()
            return [dict(row) for row in rows]

        return await asyncio.to_thread(read)

    async def get_latest_delivery_outcome(
        self,
        channel: str,
    ) -> dict[str, Any] | None:
        safe_channel = str(channel)[:40]

        def read() -> dict[str, Any] | None:
            with self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT alert_id, timestamp_ms, channel, status, attempt, detail
                    FROM delivery_receipts
                    WHERE channel = ? AND status IN ('success', 'failure')
                    ORDER BY timestamp_ms DESC, id DESC
                    LIMIT 1
                    """,
                    (safe_channel,),
                ).fetchone()
            return dict(row) if row is not None else None

        return await asyncio.to_thread(read)

    async def get_latest_local_alarm_outcome(self) -> dict[str, Any] | None:
        """Return terminal local-alarm state that is independent of receipt retention."""

        def read() -> dict[str, Any] | None:
            with self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT
                        alert_id,
                        updated_ms AS timestamp_ms,
                        channel,
                        status,
                        attempt,
                        detail
                    FROM local_alarm_outcome
                    WHERE channel = 'local'
                    """
                ).fetchone()
                if row is None:
                    # Upgrade fallback for terminal jobs written before the singleton outcome
                    # table existed. New terminal writes populate local_alarm_outcome atomically.
                    row = connection.execute(
                        """
                        SELECT
                            alert_id,
                            updated_ms AS timestamp_ms,
                            'local' AS channel,
                            status,
                            attempt,
                            '' AS detail
                        FROM local_alarm_jobs
                        WHERE status IN ('success', 'failure')
                        ORDER BY updated_ms DESC, rowid DESC
                        LIMIT 1
                        """
                    ).fetchone()
            return dict(row) if row is not None else None

        return await asyncio.to_thread(read)

    async def prune_metrics(self, cutoff_ms: int) -> int:
        def prune() -> int:
            with self._connect() as connection:
                cursor = connection.execute(
                    "DELETE FROM metrics WHERE timestamp_ms < ?", (cutoff_ms,)
                )
                return cursor.rowcount

        async with self._lock:
            return await asyncio.to_thread(prune)

    async def prune_delivery_receipts(self, cutoff_ms: int) -> int:
        def prune() -> int:
            with self._connect() as connection:
                cursor = connection.execute(
                    "DELETE FROM delivery_receipts WHERE timestamp_ms < ?",
                    (cutoff_ms,),
                )
                return cursor.rowcount

        async with self._lock:
            return await asyncio.to_thread(prune)
