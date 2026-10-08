"""Bounded read-only market feed. Detector and sound remain owned by Sentinel."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import sqlite3
import time
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from crypto_sentinel import __version__


def _iso(ms: Any) -> str | None:
    if not isinstance(ms, (int, float)) or not math.isfinite(ms) or ms <= 0:
        return None
    return (
        datetime.fromtimestamp(ms / 1000, UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _number(value: Any) -> float | None:
    return (
        float(value)
        if isinstance(value, (int, float)) and math.isfinite(value) and value >= 0
        else None
    )


def source_revision() -> str:
    digest = hashlib.sha256()
    package = Path(__file__).parent
    for path in sorted(package.rglob("*")):
        if path.is_file() and path.suffix in {".py", ".html", ".svg", ".webmanifest"}:
            digest.update(path.relative_to(package).as_posix().encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()


class MarketFeed:
    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path
        self.epoch = str(uuid4())
        self.revision = source_revision()

    async def export(
        self, status: dict[str, Any], cursor: str | None, limit: int = 100
    ) -> dict[str, Any]:
        if not 1 <= limit <= 200:
            raise ValueError("limit must be between 1 and 200")
        seq, reset = 0, None
        if cursor:
            match = re.fullmatch(r"([a-f0-9-]{36}):([0-9]{1,16})", cursor)
            if not match:
                raise ValueError("invalid cursor")
            if match[1] == self.epoch:
                seq = int(match[2])
            else:
                reset = "source_restarted"

        def read() -> tuple[list[dict[str, Any]], int, int]:
            with closing(
                sqlite3.connect(
                    self.database_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=2
                )
            ) as db:
                db.row_factory = sqlite3.Row
                db.execute("PRAGMA query_only=ON")
                low, high = db.execute(
                    "SELECT COALESCE(MIN(rowid),0),COALESCE(MAX(rowid),0) FROM alerts"
                ).fetchone()
                start = seq if 0 <= seq <= high and reset is None else 0
                rows = db.execute(
                    "SELECT rowid AS sequence,* FROM alerts WHERE rowid>? ORDER BY rowid LIMIT ?",
                    (start, limit + 1),
                ).fetchall()
                return [dict(row) for row in rows], low, high

        rows, low, high = await asyncio.to_thread(read)
        if seq > high:
            reset = "source_history_reset"
        elif seq and seq < low - 1 and reset is None:
            reset = "source_history_gap"
        more = len(rows) > limit
        rows = rows[:limit]
        events = []
        for row in rows:
            category = str(row["category"])[:80]
            kind = (
                "test"
                if category == "system_test"
                else "monitoring"
                if category in {"monitoring_gap", "feed_health", "runtime_health"}
                or row["symbol"] == "SYSTEM"
                else "external"
                if row["source"] != "crypto-sentinel"
                else "market"
            )
            events.append(
                {
                    "eventId": str(row["id"])[:80],
                    "sequence": str(row["sequence"]),
                    "observedAt": _iso(row["timestamp_ms"]),
                    "instrument": str(row["symbol"])[:40],
                    "venues": [str(value)[:40] for value in json.loads(row["exchanges_json"])[:10]],
                    "severity": str(row["severity"]),
                    "kind": kind,
                    "category": category,
                    "direction": str(row["direction"]),
                    "title": str(row["title"])[:200],
                    "message": str(row["message"])[:4000],
                    "source": str(row["source"])[:80],
                }
            )
        return {
            **self.metadata(status),
            "coverage": {
                "resetReason": reset,
                "hasMore": more,
                "historyStartSequence": str(low),
                "historyEndSequence": str(high),
                "guaranteedWhilePcAwakeOnly": True,
            },
            "events": events,
            "nextCursor": f"{self.epoch}:{rows[-1]['sequence'] if rows else (seq if not reset else 0)}",
        }

    def metadata(self, status: dict[str, Any]) -> dict[str, Any]:
        feeds = []
        for name, value in list(status.get("feeds", {}).items())[:10]:
            feeds.append(
                {
                    "venue": str(name)[:40],
                    "connected": value.get("connected") is True,
                    "subscriptionAcknowledged": value.get("subscription_acknowledged") is True,
                    "lastDataAt": _iso(value.get("last_message_ms")),
                    "dataAgeSeconds": _number(value.get("latest_message_age_seconds")),
                    "staleAfterSeconds": _number(status.get("stale_after_seconds", {}).get(name))
                    or 45,
                    "recoveringAfterGap": value.get("continuity_break_active") is True,
                }
            )
        task = status.get("tasks", {}).get("detector", {})
        sound = status.get("alarm_delivery", {}).get("local_alarm", {})
        last = sound.get("last_outcome") or {}
        return {
            "schemaVersion": 1,
            "application": "crypto-sentinel-free",
            "version": __version__,
            "sourceRevision": self.revision,
            "instanceId": self.epoch,
            "generatedAt": _iso(int(time.time() * 1000)),
            "mode": "live_observation",
            "detector": {
                "lastSuccessfulEvaluationAt": _iso(status.get("integration_last_evaluation_ms")),
                "evaluationIntervalSeconds": status.get(
                    "integration_evaluation_interval_seconds", 5
                ),
                "state": str(task.get("state", "unknown"))[:40],
                "ready": task.get("ready") is True and task.get("heartbeat_stale") is False,
                "recoveringAfterGap": status.get("continuity_health", {}).get(
                    "recovering_after_gap"
                )
                is True,
            },
            "feeds": feeds,
            "sound": {
                "enabled": sound.get("enabled") is True,
                "lastOutcome": str(last.get("status", "unverified"))[:40],
                "lastOutcomeAt": _iso(last.get("timestamp_ms")),
            },
        }
