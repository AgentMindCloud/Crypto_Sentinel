from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import re
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from aiohttp import web

from crypto_sentinel.config import DashboardConfig
from crypto_sentinel.integration import MarketFeed
from crypto_sentinel.models import Alert
from crypto_sentinel.persistence import Database


class AlarmDeliveryUnavailable(RuntimeError):
    """Required local-alarm admission failed for a dashboard write request."""


async def _on_response_prepare(request: web.Request, response: web.StreamResponse) -> None:
    # on_response_prepare runs before headers are sent for ordinary responses and long-lived SSE.
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("X-Robots-Tag", "noindex, nofollow")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault("Cross-Origin-Resource-Policy", "same-origin")
    response.headers.setdefault("Cross-Origin-Opener-Policy", "same-origin")
    response.headers.setdefault(
        "Permissions-Policy",
        "camera=(), microphone=(), geolocation=(), payment=()",
    )
    response.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; img-src 'self' data:; "
        "font-src 'self' data:; connect-src 'self'; manifest-src 'self'; "
        "object-src 'none'; base-uri 'none'; frame-ancestors 'none'; "
        "form-action 'none'; worker-src 'none'",
    )
    if request.path == "/" or request.path.startswith("/api/"):
        response.headers.setdefault("Cache-Control", "no-store")


class EventBroker:
    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()

    async def publish(self, alert: Alert) -> None:
        payload = alert.to_dict()
        for queue in list(self._subscribers):
            if queue.full():
                with contextlib.suppress(asyncio.QueueEmpty):
                    queue.get_nowait()
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait(payload)

    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=100)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self._subscribers.discard(queue)


class DashboardServer:
    def __init__(
        self,
        config: DashboardConfig,
        database: Database,
        broker: EventBroker,
        status_provider: Callable[[], dict[str, Any]],
        ingest_handler: Callable[[dict[str, Any]], Awaitable[Alert]],
        test_handler: Callable[[str], Awaitable[Alert]],
    ) -> None:
        self.config = config
        self.database = database
        self.market_feed = MarketFeed(database.path) if config.integration_token else None
        self.broker = broker
        self.status_provider = status_provider
        self.ingest_handler = ingest_handler
        self.test_handler = test_handler
        self.log = logging.getLogger("crypto_sentinel.dashboard")
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self.static_dir = Path(__file__).with_name("static")

    async def start(self) -> None:
        app = web.Application(client_max_size=64 * 1024)
        app.on_response_prepare.append(_on_response_prepare)
        app.add_routes(
            [
                web.get("/", self.index),
                web.get("/manifest.webmanifest", self.manifest),
                web.get("/icon.svg", self.icon),
                web.get("/api/healthz", self.healthz),
                web.get("/api/status", self.status),
                web.get("/api/integration/market-feed", self.integration_feed),
                web.get("/api/integration/identity", self.integration_identity),
                web.get("/api/integration/status", self.integration_status),
                web.get("/api/alerts", self.alerts),
                web.get("/api/metrics", self.metrics),
                web.get("/api/delivery-receipts", self.delivery_receipts),
                web.get("/api/events", self.events),
                web.post("/api/test-alert", self.test_alert),
                web.post("/api/ingest", self.ingest),
            ]
        )
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.config.host, self.config.port)
        await self._site.start()
        self.log.info("dashboard listening on http://%s:%d", self.config.host, self.config.port)

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
            self._site = None

    def _provided_token(self, request: web.Request) -> str:
        authorization = request.headers.get("Authorization", "")
        if authorization.lower().startswith("bearer "):
            return authorization[7:]
        return request.headers.get("X-Access-Token", "") or request.query.get("token", "")

    def _authorized(self, request: web.Request, expected: str) -> bool:
        if not expected or not expected.strip():
            return False
        provided = self._provided_token(request)
        return bool(provided) and hmac.compare_digest(provided, expected)

    def _require_dashboard_auth(self, request: web.Request) -> None:
        if not self._authorized(request, self.config.access_token):
            raise web.HTTPUnauthorized(text="dashboard token required")

    @staticmethod
    def _require_same_origin(request: web.Request) -> None:
        # Native integrations normally omit browser fetch headers. Browser-originated state
        # changes must come from this dashboard in addition to presenting a valid token.
        if request.headers.get("Sec-Fetch-Site", "").lower() == "cross-site":
            raise web.HTTPForbidden(text="cross-site request rejected")
        origin = request.headers.get("Origin")
        if origin:
            try:
                origin_host = urlsplit(origin).netloc
            except ValueError as exc:
                raise web.HTTPForbidden(text="invalid origin") from exc
            if not origin_host or not hmac.compare_digest(
                origin_host.lower(), request.host.lower()
            ):
                raise web.HTTPForbidden(text="cross-origin request rejected")

    async def index(self, _request: web.Request) -> web.FileResponse:
        return web.FileResponse(self.static_dir / "index.html")

    async def manifest(self, _request: web.Request) -> web.FileResponse:
        return web.FileResponse(self.static_dir / "manifest.webmanifest")

    async def icon(self, _request: web.Request) -> web.FileResponse:
        return web.FileResponse(self.static_dir / "icon.svg")

    async def healthz(self, _request: web.Request) -> web.Response:
        ready, state = _status_readiness(self.status_provider())
        return web.json_response(
            {"ok": ready, "state": state},
            status=200 if ready else 503,
        )

    def _require_integration_auth(self, request: web.Request) -> None:
        # Native local connector only: fixed Host, no browser origin or query tokens.
        expected_host = f"{self.config.host}:{self.config.port}"
        if request.host.lower() != expected_host.lower() or request.headers.get("Origin"):
            raise web.HTTPForbidden(text="integration origin rejected")
        authorization = request.headers.get("Authorization", "")
        provided = authorization[7:] if authorization.startswith("Bearer ") else ""
        expected = self.config.integration_token
        if not expected or not provided or not hmac.compare_digest(provided, expected):
            raise web.HTTPUnauthorized(text="read-only integration token required")
        if self.market_feed is None:
            raise web.HTTPServiceUnavailable(text="integration unavailable")

    async def integration_identity(self, request: web.Request) -> web.Response:
        if (
            request.host.lower() != f"{self.config.host}:{self.config.port}".lower()
            or request.headers.get("Origin")
        ):
            raise web.HTTPForbidden(text="integration origin rejected")
        nonce = request.query.get("nonce", "")
        if set(request.query) != {"nonce"} or not re.fullmatch(r"[a-f0-9]{64}", nonce):
            raise web.HTTPBadRequest(text="invalid nonce")
        if self.market_feed is None or not self.config.integration_token:
            raise web.HTTPServiceUnavailable(text="integration unavailable")
        feed = self.market_feed
        proof = hmac.new(
            self.config.integration_token.encode(),
            f"{nonce}:{feed.revision}:{feed.epoch}".encode(),
            hashlib.sha256,
        ).hexdigest()
        return web.json_response(
            {
                "application": "crypto-sentinel-free",
                "sourceRevision": feed.revision,
                "instanceId": feed.epoch,
                "proof": proof,
            }
        )

    async def integration_status(self, request: web.Request) -> web.Response:
        self._require_integration_auth(request)
        if request.query:
            raise web.HTTPBadRequest(text="unknown status query")
        return web.json_response(self.market_feed.metadata(self.status_provider()))

    async def integration_feed(self, request: web.Request) -> web.Response:
        self._require_integration_auth(request)
        if set(request.query) - {"cursor", "limit"}:
            raise web.HTTPBadRequest(text="unknown integration query")
        try:
            payload = await self.market_feed.export(
                self.status_provider(),
                request.query.get("cursor"),
                int(request.query.get("limit", "100")),
            )
        except ValueError as exc:
            raise web.HTTPBadRequest(text="invalid integration cursor or limit") from exc
        return web.json_response(payload)

    async def status(self, request: web.Request) -> web.Response:
        self._require_dashboard_auth(request)
        return web.json_response(self.status_provider())

    async def alerts(self, request: web.Request) -> web.Response:
        self._require_dashboard_auth(request)
        try:
            limit = int(request.query.get("limit", "100"))
        except ValueError:
            limit = 100
        return web.json_response({"alerts": await self.database.list_alerts(limit)})

    async def metrics(self, request: web.Request) -> web.Response:
        self._require_dashboard_auth(request)
        symbol = _bounded_query_text(request, "symbol", 40)
        exchange = _bounded_query_text(request, "exchange", 20)
        window_seconds = _bounded_query_int(request, "window_seconds", minimum=1, maximum=604_800)
        since_ms = _bounded_query_int(request, "since_ms", minimum=0, maximum=253_402_300_799_999)
        limit = _bounded_query_int(request, "limit", minimum=1, maximum=2_000) or 1_000
        rows = await self.database.list_metrics(
            symbol=symbol.upper() if symbol else None,
            exchange=exchange.lower() if exchange else None,
            window_seconds=window_seconds,
            since_ms=since_ms,
            limit=limit,
        )
        return web.json_response({"metrics": rows})

    async def delivery_receipts(self, request: web.Request) -> web.Response:
        self._require_dashboard_auth(request)
        alert_id = _bounded_query_text(request, "alert_id", 80) or ""
        limit = _bounded_query_int(request, "limit", minimum=1, maximum=500) or 200
        rows = await self.database.list_delivery_receipts(alert_id=alert_id, limit=limit)
        return web.json_response({"receipts": rows})

    async def events(self, request: web.Request) -> web.StreamResponse:
        self._require_dashboard_auth(request)
        response = web.StreamResponse(
            status=200,
            headers={
                "Content-Type": "text/event-stream",
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )
        await response.prepare(request)
        queue = self.broker.subscribe()
        try:
            await response.write(b": connected\n\n")
            while True:
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=15)
                except TimeoutError:
                    await response.write(b": keepalive\n\n")
                    continue
                data = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
                await response.write(f"event: alert\ndata: {data}\n\n".encode())
        except (ConnectionResetError, asyncio.CancelledError):
            pass
        finally:
            self.broker.unsubscribe(queue)
        return response

    async def test_alert(self, request: web.Request) -> web.Response:
        self._require_same_origin(request)
        self._require_dashboard_auth(request)
        payload = await _safe_json(request)
        severity = str(payload.get("severity", "warning")).lower()
        try:
            alert = await self.test_handler(severity)
        except AlarmDeliveryUnavailable as exc:
            raise web.HTTPServiceUnavailable(
                text=str(exc) or "required local alarm delivery unavailable"
            ) from exc
        return web.json_response(alert.to_dict(), status=201)

    async def ingest(self, request: web.Request) -> web.Response:
        self._require_same_origin(request)
        expected = self.config.ingest_token or self.config.access_token
        if not self._authorized(request, expected):
            raise web.HTTPUnauthorized(text="ingest token required")
        payload = await _safe_json(request)
        if not payload:
            raise web.HTTPBadRequest(text="JSON object required")
        try:
            alert = await self.ingest_handler(payload)
        except AlarmDeliveryUnavailable as exc:
            raise web.HTTPServiceUnavailable(
                text=str(exc) or "required local alarm delivery unavailable"
            ) from exc
        return web.json_response(alert.to_dict(), status=202)


async def _safe_json(request: web.Request) -> dict[str, Any]:
    if request.content_type != "application/json":
        raise web.HTTPUnsupportedMediaType(text="application/json required")
    try:
        payload = await request.json()
    except (json.JSONDecodeError, web.HTTPBadRequest) as exc:
        raise web.HTTPBadRequest(text="valid JSON required") from exc
    if not isinstance(payload, dict):
        raise web.HTTPBadRequest(text="JSON object required")
    return payload


def _status_readiness(status: dict[str, Any]) -> tuple[bool, str]:
    """Return a conservative public readiness result without exposing status details."""
    readiness = status.get("readiness")
    if isinstance(readiness, dict):
        explicit = readiness.get("overall")
        if isinstance(explicit, bool):
            return explicit, "ready" if explicit else "degraded"
        if isinstance(explicit, str):
            normalized = explicit.lower()
            ready = normalized in {"ok", "ready", "healthy", "live"}
            return ready, "ready" if ready else "degraded"

    feeds = status.get("feeds")
    if not isinstance(feeds, dict) or not feeds:
        return False, "starting"
    stale_limits = status.get("stale_after_seconds")
    if not isinstance(stale_limits, dict):
        stale_limits = {}

    for name, raw_feed in feeds.items():
        if not isinstance(raw_feed, dict) or not raw_feed.get("connected", False):
            return False, "degraded"
        if raw_feed.get("subscription_acknowledged") is False:
            return False, "degraded"
        age = raw_feed.get(
            "latest_message_age_seconds",
            raw_feed.get("message_age_seconds"),
        )
        stale_after = stale_limits.get(name)
        if not isinstance(age, (int, float)) or age < 0:
            return False, "degraded"
        if isinstance(stale_after, (int, float)) and age > stale_after:
            return False, "degraded"
        if raw_feed.get("ready") is False:
            return False, "degraded"
    return True, "ready"


def _bounded_query_text(request: web.Request, name: str, maximum: int) -> str | None:
    value = request.query.get(name)
    if value is None or not value.strip():
        return None
    normalized = value.strip()
    if len(normalized) > maximum or any(ord(character) < 32 for character in normalized):
        raise web.HTTPBadRequest(text=f"invalid {name}")
    return normalized


def _bounded_query_int(
    request: web.Request,
    name: str,
    *,
    minimum: int,
    maximum: int,
) -> int | None:
    value = request.query.get(name)
    if value is None or not value.strip():
        return None
    try:
        parsed = int(value)
    except ValueError as exc:
        raise web.HTTPBadRequest(text=f"invalid {name}") from exc
    if parsed < minimum or parsed > maximum:
        raise web.HTTPBadRequest(text=f"invalid {name}")
    return parsed
