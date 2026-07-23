from __future__ import annotations

from pathlib import Path

from crypto_sentinel.app import SentinelApp


def test_dashboard_reports_notification_permission_state() -> None:
    html = (
        Path(__file__).parents[1] / "src" / "crypto_sentinel" / "static" / "index.html"
    ).read_text(encoding="utf-8")
    assert "Browser notifications enabled" in html
    assert "Browser notifications blocked" in html
    assert "Browser notifications unavailable" in html


async def test_feed_health_realarms_after_recovery(example_config) -> None:
    example_config.exchanges.bybit.enabled = False
    example_config.exchanges.okx.enabled = False
    app = SentinelApp(example_config)
    emitted: list[tuple[str, bool]] = []

    class FakeDispatcher:
        async def emit(self, alert, *, bypass_cooldown=False):
            emitted.append((alert.category, bypass_cooldown))
            return True

    app.dispatcher = FakeDispatcher()  # type: ignore[assignment]

    await app._check_feed_health(1_000_000)
    await app._check_feed_health(1_001_000)
    assert emitted[:2] == [("feed_health", True), ("feed_health", False)]

    app.health.connected("binance")
    app.health.message("binance", "trade")
    await app._check_feed_health(1_002_000)
    assert emitted[-1] == ("feed_recovery", True)

    app.health.disconnected("binance", "test disconnect")
    await app._check_feed_health(1_003_000)
    assert emitted[-1] == ("feed_health", True)
