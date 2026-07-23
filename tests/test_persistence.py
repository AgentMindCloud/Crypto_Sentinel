from __future__ import annotations

from crypto_sentinel.models import Alert, Severity
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
