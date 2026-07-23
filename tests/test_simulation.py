from __future__ import annotations

import pytest

from crypto_sentinel.models import Severity
from crypto_sentinel.simulation import run_simulation


@pytest.mark.asyncio
async def test_offline_simulation_fires_critical_market_shock(example_config) -> None:
    result = await run_simulation(example_config)
    assert len(result.alerts) == 1
    shocks = [alert for alert in result.alerts if alert.category == "market_shock"]
    assert len(shocks) == 1
    assert shocks[0].severity == Severity.CRITICAL
    assert shocks[0].symbol == "BTCUSDT"
    assert shocks[0].direction == "down"
    assert "liquidation" in shocks[0].metrics
