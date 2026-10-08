from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from crypto_sentinel.config import AppConfig, load_config


def _example_payload() -> dict:
    source = Path(__file__).resolve().parents[1] / "config.example.yaml"
    return yaml.safe_load(source.read_text(encoding="utf-8"))


def test_dotenv_and_relative_database_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("DASHBOARD_TOKEN", raising=False)
    source = Path(__file__).resolve().parents[1] / "config.example.yaml"
    config_path = tmp_path / "config.yaml"
    config_path.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    (tmp_path / ".env").write_text("DASHBOARD_TOKEN=secret-token\n", encoding="utf-8")
    config = load_config(config_path)
    assert config.dashboard.access_token == "secret-token"
    assert config.storage.database == str((tmp_path / "data/sentinel.db").resolve())
    assert config.storage.checkpoint == str((tmp_path / "data/state.json.gz").resolve())


@pytest.mark.parametrize("host", ["127.0.0.1", "localhost", "::1", "0.0.0.0"])
def test_enabled_dashboard_requires_access_token_on_every_bind(host: str) -> None:
    payload = _example_payload()
    payload["dashboard"]["host"] = host
    payload["dashboard"]["access_token"] = " \t"

    with pytest.raises(ValidationError, match="access_token"):
        AppConfig.model_validate(payload)


def test_disabled_dashboard_may_omit_access_token() -> None:
    payload = _example_payload()
    payload["dashboard"]["enabled"] = False
    payload["dashboard"]["access_token"] = ""

    assert AppConfig.model_validate(payload).dashboard.access_token == ""


def test_detector_requires_at_least_one_window() -> None:
    payload = _example_payload()
    payload["detector"]["windows"] = []
    with pytest.raises(ValidationError, match="at least one detector window"):
        AppConfig.model_validate(payload)


@pytest.mark.parametrize(
    ("field", "warning", "critical"),
    [
        ("min_abs_imbalance", 0.8, 0.7),
        ("min_confirmations", 2, 1),
    ],
)
def test_critical_window_thresholds_cannot_be_weaker(
    field: str,
    warning: float,
    critical: float,
) -> None:
    payload = _example_payload()
    window = payload["detector"]["windows"][0]
    window["warning"][field] = warning
    window["critical"][field] = critical
    with pytest.raises(ValidationError, match=f"critical {field}"):
        AppConfig.model_validate(payload)


@pytest.mark.parametrize(
    ("section", "warning_field", "critical_field"),
    [
        ("liquidation", "warning_usd", "critical_usd"),
        ("spread", "warning_bps", "critical_bps"),
    ],
)
def test_critical_aggregate_thresholds_cannot_be_weaker(
    section: str,
    warning_field: str,
    critical_field: str,
) -> None:
    payload = _example_payload()
    payload["detector"][section][warning_field] = 100
    payload["detector"][section][critical_field] = 99
    with pytest.raises(ValidationError, match=section):
        AppConfig.model_validate(payload)


def test_duplicate_exchange_symbol_is_rejected() -> None:
    payload = _example_payload()
    payload["symbols"][1]["binance"] = payload["symbols"][0]["binance"]
    with pytest.raises(ValidationError, match="binance symbols must be unique"):
        AppConfig.model_validate(payload)


def test_confirmations_must_be_possible_for_every_symbol() -> None:
    payload = _example_payload()
    payload["symbols"][0]["bybit"] = None
    payload["symbols"][0]["okx"] = None
    with pytest.raises(ValidationError, match="min_confirmations=2 exceeds 1"):
        AppConfig.model_validate(payload)


def test_baseline_point_count_must_be_possible() -> None:
    payload = _example_payload()
    payload["detector"]["baseline_seconds"] = 300
    with pytest.raises(ValidationError, match="can produce at most"):
        AppConfig.model_validate(payload)


def test_blank_symbol_is_rejected() -> None:
    payload = _example_payload()
    payload["symbols"][0]["binance"] = "   "
    with pytest.raises(ValidationError, match="must not be blank"):
        AppConfig.model_validate(payload)


def test_data_gap_limit_must_fit_every_window() -> None:
    payload = _example_payload()
    payload["detector"]["maximum_data_gap_seconds"] = 60
    with pytest.raises(ValidationError, match="shorter than every detector window"):
        AppConfig.model_validate(payload)


def test_reconnect_range_must_be_ordered() -> None:
    payload = _example_payload()
    payload["exchanges"]["binance"]["reconnect_min_seconds"] = 10
    payload["exchanges"]["binance"]["reconnect_max_seconds"] = 5
    with pytest.raises(ValidationError, match="reconnect_max_seconds"):
        AppConfig.model_validate(payload)


@pytest.mark.parametrize("aggregate", ["liquidation", "spread"])
def test_aggregate_exchange_confirmation_must_be_possible(aggregate: str) -> None:
    payload = _example_payload()
    payload["symbols"][0]["bybit"] = None
    payload["symbols"][0]["okx"] = None
    for window in payload["detector"]["windows"]:
        window["warning"]["min_confirmations"] = 1
        window["critical"]["min_confirmations"] = 1
    payload["detector"]["liquidation"]["enabled"] = aggregate == "liquidation"
    payload["detector"]["spread"]["enabled"] = aggregate == "spread"
    with pytest.raises(ValidationError, match=aggregate):
        AppConfig.model_validate(payload)


def test_liquidation_confirmations_exclude_okx_without_liquidation_feed() -> None:
    payload = _example_payload()
    payload["symbols"][0]["binance"] = None
    payload["symbols"][0]["bybit"] = "BTCUSDT"
    payload["symbols"][0]["okx"] = "BTC-USDT-SWAP"
    with pytest.raises(
        ValidationError,
        match="1 configured liquidation-capable exchange",
    ):
        AppConfig.model_validate(payload)
