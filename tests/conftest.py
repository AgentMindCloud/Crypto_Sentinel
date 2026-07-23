from __future__ import annotations

from pathlib import Path

import pytest

from crypto_sentinel.config import AppConfig, load_config


@pytest.fixture
def example_config() -> AppConfig:
    path = Path(__file__).resolve().parents[1] / "config.example.yaml"
    return load_config(path)
