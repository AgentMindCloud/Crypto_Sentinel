from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def _expand_env(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _expand_env(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_expand_env(item) for item in value]
    if not isinstance(value, str):
        return value

    def replace(match: re.Match[str]) -> str:
        name, default = match.group(1), match.group(2)
        return os.environ.get(name, default or "")

    return _ENV_PATTERN.sub(replace, value)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SymbolConfig(StrictModel):
    canonical: str
    binance: str | None = None
    bybit: str | None = None
    okx: str | None = None

    @field_validator("canonical", "binance", "bybit", "okx")
    @classmethod
    def normalize_symbol(cls, value: str | None) -> str | None:
        return value.upper() if value else value


class ExchangeConfig(StrictModel):
    enabled: bool = True
    stale_after_seconds: int = Field(default=45, ge=10, le=600)
    reconnect_min_seconds: float = Field(default=1.0, ge=0.2, le=30)
    reconnect_max_seconds: float = Field(default=60.0, ge=1, le=600)


class ExchangesConfig(StrictModel):
    binance: ExchangeConfig = Field(default_factory=ExchangeConfig)
    bybit: ExchangeConfig = Field(default_factory=ExchangeConfig)
    okx: ExchangeConfig = Field(default_factory=ExchangeConfig)


class ThresholdConfig(StrictModel):
    min_abs_return_bps: float = Field(ge=0)
    min_abs_return_z: float = Field(ge=0)
    min_volume_z: float = Field(ge=0)
    min_abs_imbalance: float = Field(ge=0, le=1)
    min_confirmations: int = Field(default=1, ge=1, le=3)


class WindowConfig(StrictModel):
    seconds: int = Field(ge=15, le=3600)
    warning: ThresholdConfig
    critical: ThresholdConfig

    @model_validator(mode="after")
    def critical_not_weaker(self) -> WindowConfig:
        if self.critical.min_abs_return_bps < self.warning.min_abs_return_bps:
            raise ValueError("critical min_abs_return_bps must be >= warning")
        if self.critical.min_abs_return_z < self.warning.min_abs_return_z:
            raise ValueError("critical min_abs_return_z must be >= warning")
        if self.critical.min_volume_z < self.warning.min_volume_z:
            raise ValueError("critical min_volume_z must be >= warning")
        return self


class LiquidationConfig(StrictModel):
    enabled: bool = True
    window_seconds: int = Field(default=60, ge=10, le=3600)
    warning_usd: float = Field(default=500_000, gt=0)
    critical_usd: float = Field(default=2_000_000, gt=0)
    min_exchanges_for_critical: int = Field(default=2, ge=1, le=3)


class SpreadConfig(StrictModel):
    enabled: bool = True
    warning_bps: float = Field(default=35, gt=0)
    critical_bps: float = Field(default=75, gt=0)
    min_exchanges: int = Field(default=2, ge=2, le=3)


class DetectorConfig(StrictModel):
    bucket_seconds: int = Field(default=5, ge=1, le=60)
    baseline_seconds: int = Field(default=3600, ge=300, le=86_400)
    min_baseline_points: int = Field(default=12, ge=5, le=500)
    evaluate_every_seconds: int = Field(default=5, ge=1, le=60)
    cooldown_seconds: int = Field(default=600, ge=0, le=86_400)
    freshness_seconds: int = Field(default=30, ge=5, le=600)
    minimum_window_quote_volume: float = Field(default=100_000, ge=0)
    emergency_absolute_multiplier: float = Field(default=1.75, ge=1)
    windows: list[WindowConfig]
    liquidation: LiquidationConfig = Field(default_factory=LiquidationConfig)
    spread: SpreadConfig = Field(default_factory=SpreadConfig)

    @field_validator("windows")
    @classmethod
    def unique_windows(cls, value: list[WindowConfig]) -> list[WindowConfig]:
        seconds = [item.seconds for item in value]
        if len(seconds) != len(set(seconds)):
            raise ValueError("detector windows must be unique")
        return sorted(value, key=lambda item: item.seconds)


class DashboardConfig(StrictModel):
    enabled: bool = True
    host: str = "127.0.0.1"
    port: int = Field(default=8787, ge=1, le=65535)
    open_browser: bool = True
    access_token: str = ""
    ingest_token: str = ""

    @model_validator(mode="after")
    def require_token_off_loopback(self) -> DashboardConfig:
        loopback = {"127.0.0.1", "localhost", "::1"}
        if self.enabled and self.host not in loopback and not self.access_token:
            raise ValueError("dashboard access_token is required when binding outside loopback")
        return self


class LocalNotifierConfig(StrictModel):
    enabled: bool = True
    minimum_severity: Literal["info", "warning", "critical"] = "warning"
    sound_file: str = ""
    repeat_critical: int = Field(default=3, ge=1, le=10)
    custom_command: list[str] = Field(default_factory=list)


class NtfyNotifierConfig(StrictModel):
    enabled: bool = False
    server: str = "https://ntfy.sh"
    topic: str = ""
    token: str = ""
    minimum_severity: Literal["info", "warning", "critical"] = "warning"
    click_url: str = ""


class TelegramNotifierConfig(StrictModel):
    enabled: bool = False
    bot_token: str = ""
    chat_id: str = ""
    minimum_severity: Literal["info", "warning", "critical"] = "warning"


class WebhookNotifierConfig(StrictModel):
    enabled: bool = False
    url: str = ""
    bearer_token: str = ""
    minimum_severity: Literal["info", "warning", "critical"] = "warning"


class NotifiersConfig(StrictModel):
    local: LocalNotifierConfig = Field(default_factory=LocalNotifierConfig)
    ntfy: NtfyNotifierConfig = Field(default_factory=NtfyNotifierConfig)
    telegram: TelegramNotifierConfig = Field(default_factory=TelegramNotifierConfig)
    webhook: WebhookNotifierConfig = Field(default_factory=WebhookNotifierConfig)


class StorageConfig(StrictModel):
    database: str = "data/sentinel.db"
    checkpoint: str = "data/state.json.gz"
    checkpoint_seconds: int = Field(default=60, ge=10, le=3600)
    checkpoint_max_age_seconds: int = Field(default=21_600, ge=300, le=604_800)
    metric_sample_seconds: int = Field(default=30, ge=5, le=3600)
    retain_metric_days: int = Field(default=7, ge=1, le=365)


class RuntimeConfig(StrictModel):
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    log_file: str = "data/sentinel.log"
    log_max_bytes: int = Field(default=5_000_000, ge=100_000, le=1_000_000_000)
    log_backups: int = Field(default=5, ge=1, le=50)
    queue_size: int = Field(default=100_000, ge=1000, le=2_000_000)
    startup_grace_seconds: int = Field(default=60, ge=5, le=600)
    health_check_seconds: int = Field(default=15, ge=5, le=300)


class AppConfig(StrictModel):
    symbols: list[SymbolConfig]
    exchanges: ExchangesConfig = Field(default_factory=ExchangesConfig)
    detector: DetectorConfig
    dashboard: DashboardConfig = Field(default_factory=DashboardConfig)
    notifiers: NotifiersConfig = Field(default_factory=NotifiersConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)

    @field_validator("symbols")
    @classmethod
    def validate_symbols(cls, value: list[SymbolConfig]) -> list[SymbolConfig]:
        canonical = [item.canonical for item in value]
        if len(canonical) != len(set(canonical)):
            raise ValueError("canonical symbols must be unique")
        if not value:
            raise ValueError("at least one symbol is required")
        return value

    def symbol_map(self, exchange: str) -> dict[str, str]:
        mapping: dict[str, str] = {}
        for item in self.symbols:
            exchange_symbol = getattr(item, exchange)
            if exchange_symbol:
                mapping[exchange_symbol] = item.canonical
        return mapping

    def canonical_symbols(self) -> list[str]:
        return [item.canonical for item in self.symbols]


def load_config(path: str | Path) -> AppConfig:
    config_path = Path(path).expanduser().resolve()
    _load_dotenv(config_path.parent / ".env")
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("configuration root must be a mapping")
    expanded = _expand_env(raw)
    config = AppConfig.model_validate(expanded)

    database = Path(config.storage.database)
    if not database.is_absolute():
        config.storage.database = str((config_path.parent / database).resolve())
    checkpoint = Path(config.storage.checkpoint) if config.storage.checkpoint else None
    if checkpoint and not checkpoint.is_absolute():
        config.storage.checkpoint = str((config_path.parent / checkpoint).resolve())
    sound = Path(config.notifiers.local.sound_file) if config.notifiers.local.sound_file else None
    if sound and not sound.is_absolute():
        config.notifiers.local.sound_file = str((config_path.parent / sound).resolve())
    log_file = Path(config.runtime.log_file) if config.runtime.log_file else None
    if log_file and not log_file.is_absolute():
        config.runtime.log_file = str((config_path.parent / log_file).resolve())
    return config
