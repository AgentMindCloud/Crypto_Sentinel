from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import yaml
from aiohttp import WSMsgType

from crypto_sentinel.config import load_config
from crypto_sentinel.doctor import _probe_bybit, run_doctor


def test_doctor_local_checks_pass(tmp_path: Path) -> None:
    source = Path(__file__).parents[1] / "config.example.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["storage"]["database"] = str(tmp_path / "sentinel.db")
    payload["runtime"]["log_file"] = str(tmp_path / "sentinel.log")
    payload["dashboard"]["access_token"] = "doctor-test-access-token"
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


def test_doctor_does_not_count_dashboard_as_alarm_delivery(tmp_path: Path) -> None:
    source = Path(__file__).parents[1] / "config.example.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["storage"]["database"] = str(tmp_path / "sentinel.db")
    payload["storage"]["checkpoint"] = str(tmp_path / "sentinel-state.json.gz")
    payload["runtime"]["log_file"] = str(tmp_path / "sentinel.log")
    payload["dashboard"]["access_token"] = "doctor-test-access-token"
    payload["notifiers"]["local"]["enabled"] = False
    payload["notifiers"]["ntfy"]["enabled"] = False
    payload["notifiers"]["telegram"]["enabled"] = False
    payload["notifiers"]["webhook"]["enabled"] = False
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    config = load_config(config_path)

    result = asyncio.run(run_doctor(config, include_network=False))
    delivery = next(item for item in result["checks"] if item["name"] == "alert_delivery")

    assert not result["ok"]
    assert not delivery["ok"]
    assert delivery["detail"] == "no delivery channel enabled"


def test_bybit_doctor_probes_every_configured_symbol(example_config) -> None:
    mapping = example_config.symbol_map("bybit")
    messages = [
        SimpleNamespace(
            type=WSMsgType.TEXT,
            data=json.dumps({"op": "subscribe", "success": True, "ret_msg": ""}),
        )
    ]
    for index, (exchange_symbol, canonical) in enumerate(mapping.items()):
        messages.append(
            SimpleNamespace(
                type=WSMsgType.TEXT,
                data=json.dumps(
                    {
                        "topic": f"publicTrade.{exchange_symbol}",
                        "data": [
                            {
                                "T": 1_720_000_000_000 + index,
                                "s": exchange_symbol,
                                "S": "Buy",
                                "v": "0.1",
                                "p": "100000",
                                "i": f"doctor-{canonical}",
                            }
                        ],
                    }
                ),
            )
        )

    class FakeWebSocket:
        def __init__(self) -> None:
            self.sent = None

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def send_json(self, payload):
            self.sent = payload

        async def receive(self):
            return messages.pop(0)

    class FakeSession:
        def __init__(self) -> None:
            self.ws = FakeWebSocket()

        def ws_connect(self, *_args, **_kwargs):
            return self.ws

    session = FakeSession()
    result = asyncio.run(
        _probe_bybit(example_config, session, 2.0)  # type: ignore[arg-type]
    )

    assert result["ok"]
    assert set(result["symbols"]) == set(mapping.values())
    assert len(result["acknowledged_topics"]) == len(mapping) * 2
    assert len(session.ws.sent["args"]) == len(mapping) * 2
