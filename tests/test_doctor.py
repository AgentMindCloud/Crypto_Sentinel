from __future__ import annotations

import asyncio
from pathlib import Path

import yaml

from crypto_sentinel.config import load_config
from crypto_sentinel.doctor import run_doctor


def test_doctor_local_checks_pass(tmp_path: Path) -> None:
    source = Path(__file__).parents[1] / "config.example.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["storage"]["database"] = str(tmp_path / "sentinel.db")
    payload["runtime"]["log_file"] = str(tmp_path / "sentinel.log")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    config = load_config(config_path)

    result = asyncio.run(run_doctor(config, include_network=False))

    assert result["ok"]
    assert result["failed"] == 0
    names = {item["name"] for item in result["checks"]}
    assert {
        "python",
        "database_path",
        "checkpoint_path",
        "log_path",
        "exchange_configuration",
    } <= names
