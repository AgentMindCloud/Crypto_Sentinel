from __future__ import annotations

import asyncio
import json
import sqlite3
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
                        PRIMARY KEY(timestamp_ms, exchange, symbol, window_seconds)
                    );
                    CREATE INDEX IF NOT EXISTS idx_metrics_lookup
                        ON metrics(symbol, window_seconds, timestamp_ms DESC);
                    """
                )

        await asyncio.to_thread(create)

    async def save_alert(self, alert: Alert) -> None:
        payload = alert.to_dict()

        def write() -> None:
            with self._connect() as connection:
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
                        baseline_points, data_age_seconds
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            result: list[dict[str, Any]] = []
            for row in rows:
                result.append(
                    {
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
                )
            return result

        return await asyncio.to_thread(read)

    async def list_recent_dedup(self, cutoff_ms: int, limit: int = 10_000) -> list[dict[str, Any]]:
        safe_limit = max(1, min(limit, 50_000))

        def read() -> list[dict[str, Any]]:
            with self._connect() as connection:
                rows = connection.execute(
                    """
                    SELECT dedup_key, timestamp_ms, severity
                    FROM alerts
                    WHERE dedup_key <> '' AND timestamp_ms >= ?
                    ORDER BY timestamp_ms DESC
                    LIMIT ?
                    """,
                    (cutoff_ms, safe_limit),
                ).fetchall()
            result: list[dict[str, Any]] = []
            seen: set[str] = set()
            for row in rows:
                key = str(row["dedup_key"])
                if key in seen:
                    continue
                seen.add(key)
                result.append(
                    {
                        "dedup_key": key,
                        "timestamp_ms": int(row["timestamp_ms"]),
                        "severity": str(row["severity"]),
                    }
                )
            return result

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
