from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from crypto_sentinel.config import load_config


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


def test_non_loopback_dashboard_requires_access_token(tmp_path: Path) -> None:
    source = Path(__file__).resolve().parents[1] / "config.example.yaml"
    payload = yaml.safe_load(source.read_text(encoding="utf-8"))
    payload["dashboard"]["host"] = "0.0.0.0"
    payload["dashboard"]["access_token"] = ""
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    with pytest.raises(ValidationError, match="access_token"):
        load_config(path)
