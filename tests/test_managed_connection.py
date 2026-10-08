from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request

from crypto_sentinel.app import SentinelApp
from crypto_sentinel.config import DashboardConfig, load_config
from crypto_sentinel.connection import (
    connection_path,
    managed_token,
    read_connection,
    setup,
)
from crypto_sentinel.dashboard import DashboardServer, EventBroker
from crypto_sentinel.integration import MarketFeed, source_revision
from crypto_sentinel.persistence import Database


@pytest.mark.skipif(os.name != "nt", reason="Windows DPAPI credential boundary")
def test_managed_setup_uses_dpapi_preserves_config_and_reuses_token(tmp_path, monkeypatch):
    monkeypatch.setenv("DASHBOARD_TOKEN", "fixture-dashboard-authority")
    monkeypatch.setenv("INGEST_TOKEN", "fixture-ingest-authority")
    path = tmp_path / "config.yaml"
    path.write_bytes((Path(__file__).parents[1] / "config.example.yaml").read_bytes())
    before = path.read_bytes()
    connection_path(path).parent.mkdir(parents=True)
    revision = source_revision()
    first = setup(path, revision)
    value = read_connection(path)
    assert first["reused"] is False
    assert value["readToken"] not in connection_path(path).read_text()
    assert setup(path, revision)["reused"] is True
    assert read_connection(path) == value
    assert load_config(path).dashboard.integration_token == value["readToken"]
    assert path.read_bytes() == before
    assert value["readToken"] not in json.dumps(first)
    stored = json.loads(connection_path(path).read_text())
    stored["sourceRevision"] = "0" * 64
    connection_path(path).write_text(json.dumps(stored))
    with pytest.raises(ValueError, match="source changed"):
        managed_token(path)
    assert load_config(path).dashboard.integration_token == ""
    assert load_config(path).dashboard.enabled


@pytest.mark.skipif(os.name != "nt", reason="Windows DPAPI credential boundary")
def test_explicit_reselection_preserves_authority_and_rejects_stale_selection(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("DASHBOARD_TOKEN", "fixture-dashboard-authority")
    monkeypatch.setenv("INGEST_TOKEN", "fixture-ingest-authority")
    path = tmp_path / "config.yaml"
    path.write_bytes((Path(__file__).parents[1] / "config.example.yaml").read_bytes())
    config_bytes = path.read_bytes()
    connection_path(path).parent.mkdir(parents=True)
    old, new = "1" * 64, "2" * 64
    monkeypatch.setattr("crypto_sentinel.integration.source_revision", lambda: old)
    setup(path, old)
    before = connection_path(path).read_bytes()
    authority = read_connection(path)["readToken"]
    monkeypatch.setattr("crypto_sentinel.integration.source_revision", lambda: new)
    with pytest.raises(ValueError, match="explicit replacement"):
        setup(path, new)
    with pytest.raises(ValueError, match="Previous selected"):
        setup(path, new, "3" * 64)
    with pytest.raises(ValueError, match="Reviewed Sentinel"):
        setup(path, old, old)
    assert connection_path(path).read_bytes() == before
    result = setup(path, new, old)
    after = json.loads(connection_path(path).read_text())
    previous = json.loads(before)
    assert {**previous, "sourceRevision": new} == after
    assert read_connection(path)["readToken"] == authority
    assert path.read_bytes() == config_bytes
    assert result["reselected"] and result["reused"]
    assert authority not in json.dumps(result)
    assert load_config(path).dashboard.integration_token == authority
    assert setup(path, new)["reused"]
    with pytest.raises(ValueError, match="Previous selected"):
        setup(path, new, old)


@pytest.mark.skipif(os.name != "nt", reason="Windows DPAPI credential boundary")
def test_failed_reselection_keeps_original_and_never_creates_missing_selection(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("DASHBOARD_TOKEN", "fixture-dashboard-authority")
    monkeypatch.setenv("INGEST_TOKEN", "fixture-ingest-authority")
    path = tmp_path / "config.yaml"
    path.write_bytes((Path(__file__).parents[1] / "config.example.yaml").read_bytes())
    connection_path(path).parent.mkdir(parents=True)
    old, new = "1" * 64, "2" * 64
    monkeypatch.setattr("crypto_sentinel.integration.source_revision", lambda: old)
    with pytest.raises(ValueError, match="Previous managed selection is missing"):
        setup(path, old, old)
    assert not connection_path(path).exists()
    setup(path, old)
    before = connection_path(path).read_bytes()
    monkeypatch.setattr("crypto_sentinel.integration.source_revision", lambda: new)

    def failed_replace(*_args):
        raise OSError("isolated replacement failure")

    monkeypatch.setattr("crypto_sentinel.connection.os.replace", failed_replace)
    with pytest.raises(OSError, match="isolated replacement failure"):
        setup(path, new, old)
    assert connection_path(path).read_bytes() == before
    assert list(connection_path(path).parent.iterdir()) == [connection_path(path)]


async def test_identity_and_metadata_never_open_alert_database(tmp_path):
    async def forbidden(_value):
        raise AssertionError("write invoked")

    token = "fixture-integration-" + "x" * 30
    server = DashboardServer(
        DashboardConfig(access_token="dashboard", integration_token=token),
        Database(str(tmp_path / "never-created.db")),
        EventBroker(),
        lambda: {},
        forbidden,
        forbidden,
    )

    def req(path, **headers):
        return make_mocked_request("GET", path, headers={"Host": "127.0.0.1:8787", **headers})

    nonce = "a" * 64
    response = await server.integration_identity(req("/api/integration/identity?nonce=" + nonce))
    value = json.loads(response.text)
    assert (
        value["proof"]
        == hmac.new(
            token.encode(),
            f"{nonce}:{value['sourceRevision']}:{value['instanceId']}".encode(),
            hashlib.sha256,
        ).hexdigest()
    )
    status = await server.integration_status(
        req("/api/integration/status", Authorization="Bearer " + token)
    )
    assert "events" not in json.loads(status.text)
    assert not (tmp_path / "never-created.db").exists()
    with pytest.raises(web.HTTPForbidden):
        await server.integration_identity(
            req("/api/integration/identity?nonce=" + nonce, Origin="http://evil.example")
        )
    with pytest.raises(web.HTTPUnauthorized):
        await server.integration_status(
            req("/api/integration/status", Authorization="Bearer dashboard")
        )


async def test_detector_evaluates_without_dashboard_browser_or_network(
    example_config, tmp_path, monkeypatch
):
    example_config.dashboard.enabled = False
    example_config.dashboard.open_browser = False
    example_config.storage.database = str(tmp_path / "isolated.db")
    example_config.storage.checkpoint = ""
    example_config.runtime.log_file = ""
    example_config.detector.evaluate_every_seconds = 1
    app = SentinelApp(example_config)
    await app.database.initialize()
    ticks = []
    original = app.detector.evaluate

    def evaluate(now):
        ticks.append(now)
        return original(now)

    monkeypatch.setattr(app.detector, "evaluate", evaluate)
    task = asyncio.create_task(app._detector_loop())
    try:
        for _ in range(30):
            if len(ticks) >= 2:
                break
            await asyncio.sleep(0.1)
        assert len(ticks) >= 2
        assert app.dashboard is None
        assert app.status()["integration_last_evaluation_ms"] is not None
        assert (
            MarketFeed(app.database.path).metadata(app.status())["detector"][
                "lastSuccessfulEvaluationAt"
            ]
            is not None
        )
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
