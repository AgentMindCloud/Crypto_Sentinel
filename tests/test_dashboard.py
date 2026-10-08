from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp import web
from aiohttp.test_utils import make_mocked_request

from crypto_sentinel.config import DashboardConfig
from crypto_sentinel.dashboard import (
    AlarmDeliveryUnavailable,
    DashboardServer,
    EventBroker,
    _on_response_prepare,
)

STATIC = Path(__file__).parents[1] / "src" / "crypto_sentinel" / "static"


def _server(
    *,
    token: str = "secret",
    enabled: bool = True,
    status: dict[str, object] | None = None,
) -> tuple[DashboardServer, SimpleNamespace]:
    database = SimpleNamespace(
        list_alerts=AsyncMock(return_value=[]),
        list_metrics=AsyncMock(return_value=[]),
        list_delivery_receipts=AsyncMock(return_value=[]),
    )

    async def ingest_handler(_payload):
        raise AssertionError("not used")

    async def test_handler(_severity):
        raise AssertionError("not used")

    server = DashboardServer(
        DashboardConfig(enabled=enabled, access_token=token),
        database,  # type: ignore[arg-type]
        EventBroker(),
        lambda: status or {},
        ingest_handler,
        test_handler,
    )
    return server, database


async def test_empty_expected_token_never_authorizes_protected_apis() -> None:
    server, _database = _server(token="", enabled=False)
    supplied = {"Authorization": "Bearer caller-chosen-value"}

    with pytest.raises(web.HTTPUnauthorized):
        await server.status(make_mocked_request("GET", "/api/status", headers=supplied))
    with pytest.raises(web.HTTPUnauthorized):
        await server.alerts(make_mocked_request("GET", "/api/alerts", headers=supplied))
    with pytest.raises(web.HTTPUnauthorized):
        await server.test_alert(make_mocked_request("POST", "/api/test-alert", headers=supplied))
    with pytest.raises(web.HTTPUnauthorized):
        await server.ingest(make_mocked_request("POST", "/api/ingest", headers=supplied))


@pytest.mark.parametrize(
    ("endpoint", "payload"),
    [
        ("test_alert", {"severity": "critical"}),
        ("ingest", {"severity": "critical", "title": "external"}),
    ],
)
async def test_write_api_returns_503_when_local_alarm_admission_fails(
    endpoint: str,
    payload: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    server, _database = _server()
    handler = AsyncMock(
        side_effect=AlarmDeliveryUnavailable("required local alarm admission failed")
    )
    if endpoint == "test_alert":
        server.test_handler = handler
        path = "/api/test-alert"
    else:
        server.ingest_handler = handler
        path = "/api/ingest"
    async_json = AsyncMock(return_value=payload)
    monkeypatch.setattr("crypto_sentinel.dashboard._safe_json", async_json)

    request = make_mocked_request(
        "POST",
        path,
        headers={"Authorization": "Bearer secret"},
    )
    with pytest.raises(web.HTTPServiceUnavailable) as exc_info:
        await getattr(server, endpoint)(request)

    assert exc_info.value.status == 503
    assert exc_info.value.text == "required local alarm admission failed"
    handler.assert_awaited_once()


def test_premium_dashboard_is_local_interactive_and_accessible() -> None:
    html = (STATIC / "index.html").read_text(encoding="utf-8")

    for marker in (
        "Market anomaly",
        "Cinnabar",
        "Signal timeline",
        "Recovery history",
        "Alarm delivery",
        "Alert evidence",
        "filter-symbol",
        "filter-venue",
        "filter-severity",
        "filter-category",
        "filter-time",
        "alert-drawer",
        "export-csv",
        "export-json",
        "prefers-reduced-motion",
        "Skip to dashboard",
        "Monitoring only",
        "Browser notifications enabled",
        "Browser notifications blocked",
        "Browser notifications unavailable",
        "status.checkpoint_health",
    ):
        assert marker.lower() in html.lower()

    assert "#e64a2e" in html.lower()
    assert "#35a481" in html.lower()
    assert "Instrument Serif" in html
    assert "IBM Plex Mono" in html
    assert "bootstrap" not in html.lower()
    assert "lorem ipsum" not in html.lower()
    assert "http://" not in html
    assert "https://" not in html
    assert 'src="http' not in html.lower()
    assert 'href="http' not in html.lower()
    assert "@import url" not in html.lower()
    assert 'raw === "queued"' in html
    assert 'raw === "attempt"' in html
    assert 'raw === "failure"' in html
    assert "feed.subscription_acknowledged === false" in html
    assert '? "subscribing"' in html
    assert "feed.continuity_break_active" in html
    assert '? "recovering"' in html
    assert 'primaryHealthy ? "ready" : "reason not reported"' in html
    assert 'shadowHealthy ? "ready" : "reason not reported"' in html


def test_manifest_and_icon_use_cinnabar_glass_identity() -> None:
    manifest = json.loads((STATIC / "manifest.webmanifest").read_text(encoding="utf-8"))
    icon = (STATIC / "icon.svg").read_text(encoding="utf-8")

    assert manifest["theme_color"] == "#0E0C0B"
    assert manifest["background_color"] == "#0E0C0B"
    assert "monitoring-only" in manifest["description"]
    assert "#E64A2E" in icon
    assert "#35A481" in icon


async def test_security_headers_restrict_dashboard_resources() -> None:
    request = make_mocked_request("GET", "/")
    response = web.Response()

    await _on_response_prepare(request, response)

    assert response.headers["X-Frame-Options"] == "DENY"
    assert response.headers["Cross-Origin-Opener-Policy"] == "same-origin"
    assert response.headers["X-Robots-Tag"] == "noindex, nofollow"
    assert response.headers["Cache-Control"] == "no-store"
    policy = response.headers["Content-Security-Policy"]
    assert "default-src 'self'" in policy
    assert "connect-src 'self'" in policy
    assert "object-src 'none'" in policy
    assert "frame-ancestors 'none'" in policy
    assert "worker-src 'none'" in policy


@pytest.mark.parametrize(
    ("status", "expected_status", "expected_state"),
    [
        (
            {
                "feeds": {"binance": {"connected": True, "message_age_seconds": 0.5}},
                "stale_after_seconds": {"binance": 15},
            },
            200,
            "ready",
        ),
        (
            {
                "feeds": {"binance": {"connected": True, "message_age_seconds": 30}},
                "stale_after_seconds": {"binance": 15},
            },
            503,
            "degraded",
        ),
        (
            {
                "feeds": {
                    "binance": {
                        "connected": True,
                        "message_age_seconds": 100,
                        "latest_message_age_seconds": 0.5,
                    }
                },
                "stale_after_seconds": {"binance": 15},
            },
            200,
            "ready",
        ),
        (
            {
                "feeds": {
                    "binance": {
                        "connected": True,
                        "message_age_seconds": 0.5,
                        "latest_message_age_seconds": 30,
                    }
                },
                "stale_after_seconds": {"binance": 15},
            },
            503,
            "degraded",
        ),
        (
            {
                "feeds": {
                    "binance": {
                        "connected": True,
                        "subscription_acknowledged": False,
                        "latest_message_age_seconds": 0.5,
                    }
                },
                "stale_after_seconds": {"binance": 15},
            },
            503,
            "degraded",
        ),
        ({"readiness": {"overall": False}, "feeds": {}}, 503, "degraded"),
        ({}, 503, "starting"),
    ],
)
async def test_healthz_reports_actual_readiness(
    status: dict[str, object],
    expected_status: int,
    expected_state: str,
) -> None:
    server, _database = _server(status=status)

    response = await server.healthz(make_mocked_request("GET", "/api/healthz"))

    assert response.status == expected_status
    assert json.loads(response.text) == {
        "ok": expected_status == 200,
        "state": expected_state,
    }


async def test_metrics_endpoint_is_authenticated_and_bounds_queries() -> None:
    server, database = _server(token="secret")

    with pytest.raises(web.HTTPUnauthorized):
        await server.metrics(make_mocked_request("GET", "/api/metrics"))

    request = make_mocked_request(
        "GET",
        "/api/metrics?symbol=btcusdt&exchange=BINANCE&window_seconds=60&since_ms=1234&limit=2000",
        headers={"Authorization": "Bearer secret"},
    )
    response = await server.metrics(request)

    assert json.loads(response.text) == {"metrics": []}
    database.list_metrics.assert_awaited_once_with(
        symbol="BTCUSDT",
        exchange="binance",
        window_seconds=60,
        since_ms=1234,
        limit=2000,
    )

    invalid = make_mocked_request(
        "GET",
        "/api/metrics?limit=2001",
        headers={"Authorization": "Bearer secret"},
    )
    with pytest.raises(web.HTTPBadRequest):
        await server.metrics(invalid)


async def test_delivery_receipts_endpoint_is_authenticated_and_bounded() -> None:
    server, database = _server(token="secret")
    request = make_mocked_request(
        "GET",
        "/api/delivery-receipts?alert_id=alert-123&limit=250",
        headers={"X-Access-Token": "secret"},
    )

    response = await server.delivery_receipts(request)

    assert json.loads(response.text) == {"receipts": []}
    database.list_delivery_receipts.assert_awaited_once_with(
        alert_id="alert-123",
        limit=250,
    )

    invalid = make_mocked_request(
        "GET",
        "/api/delivery-receipts?limit=501",
        headers={"X-Access-Token": "secret"},
    )
    with pytest.raises(web.HTTPBadRequest):
        await server.delivery_receipts(invalid)
