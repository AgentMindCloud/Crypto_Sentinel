from __future__ import annotations

import json
import sqlite3
import time
from contextlib import closing
from types import SimpleNamespace
from uuid import uuid4

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request
from pydantic import ValidationError

from crypto_sentinel.config import DashboardConfig
from crypto_sentinel.dashboard import DashboardServer, EventBroker
from crypto_sentinel.integration import MarketFeed
from crypto_sentinel.models import Alert, Severity
from crypto_sentinel.persistence import Database

TOKEN = "read-only-fixture-token-" + "x" * 20


def source_status():
    now = int(time.time() * 1000)
    return {
        "integration_last_evaluation_ms": now,
        "integration_evaluation_interval_seconds": 5,
        "tasks": {"detector": {"state": "running", "ready": True, "heartbeat_stale": False}},
        "feeds": {
            "binance": {
                "connected": True,
                "subscription_acknowledged": True,
                "last_message_ms": now,
                "latest_message_age_seconds": 0,
            }
        },
        "stale_after_seconds": {"binance": 45},
        "alarm_delivery": {"local_alarm": {"enabled": True}},
    }


async def alert(db: Database, ident: str, category="momentum", timestamp=1_790_000_000_000):
    await db.save_alert(
        Alert(
            id=ident,
            severity=Severity.WARNING,
            category=category,
            symbol="BTCUSDT",
            title="Synthetic test",
            message="Synthetic only",
            timestamp_ms=timestamp,
            exchanges=["binance"],
        )
    )


async def test_feed_has_paged_cursor_with_backdated_events_and_restart_gap(tmp_path):
    db = Database(str(tmp_path / "fixture.db"))
    await db.initialize()
    first, second = str(uuid4()), str(uuid4())
    await alert(db, first)
    await alert(db, second, timestamp=1_780_000_000_000)
    feed = MarketFeed(db.path)
    page1 = await feed.export(source_status(), None, 1)
    assert page1["events"][0]["eventId"] == first
    assert page1["coverage"]["hasMore"]
    page2 = await feed.export(source_status(), page1["nextCursor"], 1)
    assert page2["events"][0]["eventId"] == second
    assert not page2["coverage"]["hasMore"]
    assert (await feed.export(source_status(), page2["nextCursor"]))["events"] == []
    restarted = MarketFeed(db.path)
    assert (await restarted.export(source_status(), page2["nextCursor"]))["coverage"][
        "resetReason"
    ] == "source_restarted"


async def test_feed_separates_successful_evaluation_and_test_sound(tmp_path):
    db = Database(str(tmp_path / "fixture.db"))
    await db.initialize()
    await alert(db, str(uuid4()), category="system_test")
    value = source_status()
    value["integration_last_evaluation_ms"] = None
    result = await MarketFeed(db.path).export(value, None)
    assert result["detector"]["lastSuccessfulEvaluationAt"] is None
    assert result["events"][0]["kind"] == "test"
    assert result["sound"]["lastOutcome"] == "unverified"
    assert result["coverage"]["guaranteedWhilePcAwakeOnly"]
    assert len(result["sourceRevision"]) == 64
    assert "access_token" not in json.dumps(result)


async def test_export_never_writes_or_acknowledges_source(tmp_path):
    db = Database(str(tmp_path / "fixture.db"))
    await db.initialize()
    await alert(db, str(uuid4()))
    # WAL checkpointing may change file bytes when a prior writer is collected.
    # Compare durable logical records, including delivery state, instead.
    with closing(sqlite3.connect(db.path)) as inspection:
        before = list(inspection.iterdump())
        await MarketFeed(db.path).export(source_status(), None)
        assert list(inspection.iterdump()) == before


@pytest.mark.parametrize(
    "cursor,limit", [("bad", 10), (None, 0), (None, 201), ("a" * 36 + ":-1", 10)]
)
async def test_invalid_cursor_or_limit_rejects(tmp_path, cursor, limit):
    with pytest.raises(ValueError):
        await MarketFeed(tmp_path / "unopened.db").export({}, cursor, limit)


def test_dedicated_token_cannot_equal_write_token():
    with pytest.raises(ValidationError):
        DashboardConfig(access_token=TOKEN, integration_token=TOKEN)
    with pytest.raises(ValidationError):
        DashboardConfig(access_token="dashboard", integration_token="short")


async def test_endpoint_rejects_dashboard_token_origin_foreign_host_query_secret_and_write(
    tmp_path,
):
    db = Database(str(tmp_path / "fixture.db"))
    await db.initialize()

    async def forbidden(_value):
        raise AssertionError("write invoked")

    server = DashboardServer(
        DashboardConfig(access_token="dashboard", integration_token=TOKEN),
        db,
        EventBroker(),
        source_status,
        forbidden,
        forbidden,
    )

    def req(path="/api/integration/market-feed", **headers):
        return make_mocked_request(
            "GET",
            path,
            headers={"Host": "127.0.0.1:8787", "Authorization": "Bearer " + TOKEN, **headers},
        )

    assert (await server.integration_feed(req())).status == 200
    with pytest.raises(web.HTTPUnauthorized):
        await server.integration_feed(req(Authorization="Bearer dashboard"))
    with pytest.raises(web.HTTPForbidden):
        await server.integration_feed(req(Origin="http://127.0.0.1:8787"))
    with pytest.raises(web.HTTPForbidden):
        await server.integration_feed(req(Host="evil.example:8787"))
    with pytest.raises(web.HTTPBadRequest):
        await server.integration_feed(req("/api/integration/market-feed?token=ignored"))
    with pytest.raises(web.HTTPUnauthorized):
        await server.test_alert(req())
    with pytest.raises(web.HTTPUnauthorized):
        await server.status(req())


async def test_disabled_integration_rejects_without_instantiating_feed():
    async def forbidden(_value):
        raise AssertionError("write invoked")

    server = DashboardServer(
        DashboardConfig(access_token="dashboard"),
        SimpleNamespace(),
        EventBroker(),
        source_status,
        forbidden,
        forbidden,
    )
    request = make_mocked_request(
        "GET",
        "/api/integration/market-feed",
        headers={"Host": "127.0.0.1:8787", "Authorization": "Bearer " + TOKEN},
    )
    with pytest.raises(web.HTTPUnauthorized):
        await server.integration_feed(request)
