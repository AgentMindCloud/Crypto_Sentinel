from __future__ import annotations

import math

from crypto_sentinel.models import Alert


def test_external_alert_timestamp_is_clamped() -> None:
    now_ms = 1_800_000_000_000
    future = Alert.from_external({"timestamp_ms": now_ms + 99_000_000}, now_ms)
    past = Alert.from_external({"timestamp_ms": now_ms - 99_000_000}, now_ms)
    malformed = Alert.from_external({"timestamp_ms": "not-a-number"}, now_ms)

    assert future.timestamp_ms == now_ms + 300_000
    assert past.timestamp_ms == now_ms - 300_000
    assert malformed.timestamp_ms == now_ms


def test_external_alert_normalizes_direction_and_nonfinite_metrics() -> None:
    alert = Alert.from_external(
        {
            "direction": "sideways",
            "metrics": {"bad": math.nan, "nested": [math.inf, 1]},
            "dedup_key": "x" * 1000,
        },
        1_800_000_000_000,
    )
    assert alert.direction == "mixed"
    assert alert.metrics == {"bad": None, "nested": [None, 1]}
    assert len(alert.dedup_key) == 300
