from __future__ import annotations

import logging
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
        if value is None:
            return None
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("symbols must not be blank")
        return normalized


class ExchangeConfig(StrictModel):
    enabled: bool = True
    stale_after_seconds: int = Field(default=45, ge=10, le=600)
    reconnect_min_seconds: float = Field(default=1.0, ge=0.2, le=30)
    reconnect_max_seconds: float = Field(default=60.0, ge=1, le=600)

    @model_validator(mode="after")
    def valid_reconnect_range(self) -> ExchangeConfig:
        if self.reconnect_max_seconds < self.reconnect_min_seconds:
            raise ValueError("reconnect_max_seconds must be >= reconnect_min_seconds")
        return self


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
        if self.critical.min_abs_imbalance < self.warning.min_abs_imbalance:
            raise ValueError("critical min_abs_imbalance must be >= warning")
        if self.critical.min_confirmations < self.warning.min_confirmations:
            raise ValueError("critical min_confirmations must be >= warning")
        return self


class LiquidationConfig(StrictModel):
    enabled: bool = True
    window_seconds: int = Field(default=60, ge=10, le=3600)
    warning_usd: float = Field(default=500_000, gt=0)
    critical_usd: float = Field(default=2_000_000, gt=0)
    min_exchanges_for_critical: int = Field(default=2, ge=1, le=3)

    @model_validator(mode="after")
    def critical_not_weaker(self) -> LiquidationConfig:
        if self.critical_usd < self.warning_usd:
            raise ValueError("liquidation critical_usd must be >= warning_usd")
        return self


class SpreadConfig(StrictModel):
    enabled: bool = True
    warning_bps: float = Field(default=35, gt=0)
    critical_bps: float = Field(default=75, gt=0)
    min_exchanges: int = Field(default=2, ge=2, le=3)

    @model_validator(mode="after")
    def critical_not_weaker(self) -> SpreadConfig:
        if self.critical_bps < self.warning_bps:
            raise ValueError("spread critical_bps must be >= warning_bps")
        return self


class DetectorConfig(StrictModel):
    bucket_seconds: int = Field(default=5, ge=1, le=60)
    baseline_seconds: int = Field(default=3600, ge=300, le=86_400)
    min_baseline_points: int = Field(default=12, ge=5, le=500)
    evaluate_every_seconds: int = Field(default=5, ge=1, le=60)
    cooldown_seconds: int = Field(default=600, ge=0, le=86_400)
    freshness_seconds: int = Field(default=30, ge=5, le=600)
    minimum_window_quote_volume: float = Field(default=100_000, ge=0)
    emergency_absolute_multiplier: float = Field(default=1.75, ge=1)
    minimum_window_coverage: float = Field(default=0.8, ge=0.5, le=1)
    maximum_data_gap_seconds: int = Field(default=15, ge=1, le=300)
    max_future_skew_seconds: int = Field(default=5, ge=0, le=300)
    windows: list[WindowConfig]
    liquidation: LiquidationConfig = Field(default_factory=LiquidationConfig)
    spread: SpreadConfig = Field(default_factory=SpreadConfig)

    @field_validator("windows")
    @classmethod
    def unique_windows(cls, value: list[WindowConfig]) -> list[WindowConfig]:
        if not value:
            raise ValueError("at least one detector window is required")
        seconds = [item.seconds for item in value]
        if len(seconds) != len(set(seconds)):
            raise ValueError("detector windows must be unique")
        return sorted(value, key=lambda item: item.seconds)

    @model_validator(mode="after")
    def gap_must_fit_shortest_window(self) -> DetectorConfig:
        if self.windows and self.maximum_data_gap_seconds >= self.windows[0].seconds:
            raise ValueError("maximum_data_gap_seconds must be shorter than every detector window")
        for window in self.windows:
            stride_seconds = max(self.bucket_seconds, window.seconds / 2)
            available_baseline_points = (
                int((self.baseline_seconds - window.seconds) // stride_seconds) + 1
                if self.baseline_seconds >= window.seconds
                else 0
            )
            available_baseline_points = min(240, available_baseline_points)
            if self.min_baseline_points > available_baseline_points:
                raise ValueError(
                    f"{window.seconds}s window can produce at most "
                    f"{available_baseline_points} baseline point(s), below "
                    f"min_baseline_points={self.min_baseline_points}"
                )
        return self


class DashboardConfig(StrictModel):
    enabled: bool = True
    host: str = "127.0.0.1"
    port: int = Field(default=8787, ge=1, le=65535)
    open_browser: bool = True
    access_token: str = ""
    ingest_token: str = ""
    integration_token: str = ""

    @model_validator(mode="after")
    def require_access_token_when_enabled(self) -> DashboardConfig:
        if self.enabled and not self.access_token.strip():
            raise ValueError("dashboard access_token is required whenever the dashboard is enabled")
        if self.integration_token:
            if len(self.integration_token) < 32 or len(self.integration_token) > 256:
                raise ValueError("integration_token must contain 32 to 256 characters")
            if self.integration_token in {self.access_token, self.ingest_token}:
                raise ValueError("integration_token must be distinct from dashboard/ingest tokens")
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
        for exchange in ("binance", "bybit", "okx"):
            venue_symbols = [
                venue_symbol
                for item in value
                if (venue_symbol := getattr(item, exchange)) is not None
            ]
            if len(venue_symbols) != len(set(venue_symbols)):
                raise ValueError(f"{exchange} symbols must be unique")
        return value

    @model_validator(mode="after")
    def confirmations_must_be_possible(self) -> AppConfig:
        enabled = {
            exchange
            for exchange in ("binance", "bybit", "okx")
            if getattr(self.exchanges, exchange).enabled
        }
        for symbol in self.symbols:
            available = sum(1 for exchange in enabled if getattr(symbol, exchange) is not None)
            liquidation_available = sum(
                1
                for exchange in enabled.intersection({"binance", "bybit"})
                if getattr(symbol, exchange) is not None
            )
            if available == 0:
                raise ValueError(
                    f"{symbol.canonical} must be configured on at least one enabled exchange"
                )
            for window in self.detector.windows:
                for level, threshold in (
                    ("warning", window.warning),
                    ("critical", window.critical),
                ):
                    if threshold.min_confirmations > available:
                        raise ValueError(
                            f"{symbol.canonical} {window.seconds}s {level} "
                            f"min_confirmations={threshold.min_confirmations} exceeds "
                            f"{available} configured enabled exchange(s)"
                        )
            if (
                self.detector.liquidation.enabled
                and self.detector.liquidation.min_exchanges_for_critical > liquidation_available
            ):
                raise ValueError(
                    f"{symbol.canonical} liquidation min_exchanges_for_critical exceeds "
                    f"{liquidation_available} configured liquidation-capable exchange(s)"
                )
            if self.detector.spread.enabled and self.detector.spread.min_exchanges > available:
                raise ValueError(
                    f"{symbol.canonical} spread min_exchanges exceeds "
                    f"{available} configured enabled exchange(s)"
                )
        return self

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
    from crypto_sentinel.connection import managed_token

    try:
        integration_token = managed_token(config_path)
    except Exception:
        # Catch all adapter/configuration decoding failures at this optional boundary.
        # Connection failure must never stop the independent detector or local sound.
        logging.getLogger(__name__).warning(
            "Managed read-only connection disabled: verify its selected installation"
        )
        integration_token = ""
    if integration_token is not None:
        expanded.setdefault("dashboard", {})["integration_token"] = integration_token
    if os.environ.get("CRYPTO_SENTINEL_SUPPRESS_BROWSER") == "1":
        expanded.setdefault("dashboard", {})["open_browser"] = False
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
