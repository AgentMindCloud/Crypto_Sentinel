from __future__ import annotations

import argparse
import asyncio
import json
import logging
import shutil
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from crypto_sentinel.alerting import AlertDispatcher
from crypto_sentinel.app import SentinelApp
from crypto_sentinel.config import AppConfig, load_config
from crypto_sentinel.dashboard import EventBroker
from crypto_sentinel.doctor import run_doctor
from crypto_sentinel.models import Alert, Severity
from crypto_sentinel.persistence import Database
from crypto_sentinel.simulation import run_simulation


def _configure_logging(config: AppConfig) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if config.runtime.log_file:
        log_path = Path(config.runtime.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(
            RotatingFileHandler(
                log_path,
                maxBytes=config.runtime.log_max_bytes,
                backupCount=config.runtime.log_backups,
                encoding="utf-8",
            )
        )
    logging.basicConfig(
        level=getattr(logging, config.runtime.log_level),
        format="%(asctime)s %(levelname)s %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )


def _configure_console_output() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(encoding="utf-8", errors="backslashreplace")
        except (OSError, ValueError):
            # Embedded/test streams may not support reconfiguration.
            continue


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="crypto-sentinel",
        description="Free multi-exchange crypto anomaly alarms using public market data.",
    )
    parser.add_argument("--config", default="config.yaml", help="YAML configuration path")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("run", help="run live feeds, detector, notifiers, and dashboard")
    subparsers.add_parser("validate-config", help="validate and summarize the configuration")
    doctor = subparsers.add_parser(
        "doctor", help="check paths, delivery configuration, and live feeds"
    )
    doctor.add_argument("--timeout", type=float, default=12.0, help="seconds per live feed probe")
    doctor.add_argument("--skip-network", action="store_true", help="run local checks only")
    doctor.add_argument("--json", action="store_true", help="print machine-readable output")
    sim = subparsers.add_parser(
        "simulate", help="run a deterministic offline market-shock simulation"
    )
    sim.add_argument("--json", action="store_true", help="print machine-readable output")
    test = subparsers.add_parser("test-alert", help="send a notifier test without starting feeds")
    test.add_argument("--severity", choices=["warning", "critical"], default="critical")
    init = subparsers.add_parser(
        "init-config", help="copy the bundled example to a new config file"
    )
    init.add_argument("--output", default="config.yaml")
    return parser


def _load(path: str) -> AppConfig:
    try:
        return load_config(path)
    except FileNotFoundError as exc:
        raise SystemExit(
            f"Configuration not found: {path}. Run 'crypto-sentinel init-config' first."
        ) from exc
    except Exception as exc:
        raise SystemExit(f"Invalid configuration: {exc}") from exc


async def _test_alert(config: AppConfig, severity: str) -> None:
    database = Database(config.storage.database)
    await database.initialize()
    broker = EventBroker()
    dispatcher = AlertDispatcher(config, database, broker.publish)
    await dispatcher.start()
    try:
        now_ms = int(__import__("time").time() * 1000)
        alert = Alert(
            severity=Severity(severity),
            category="cli_test",
            symbol="TEST",
            title=f"{severity.title()} notifier test",
            message="This is a CLI-triggered notifier test. No market event occurred.",
            timestamp_ms=now_ms,
            exchanges=["local"],
            dedup_key=f"cli-test:{now_ms}",
            source="cli",
        )
        await dispatcher.emit(alert, bypass_cooldown=True)
    finally:
        await dispatcher.close()


def _init_config(output: str) -> None:
    destination = Path(output).expanduser().resolve()
    if destination.exists():
        raise SystemExit(f"Refusing to overwrite existing file: {destination}")
    source = Path(__file__).with_name("config.example.yaml")
    if not source.exists():
        source = Path.cwd() / "config.example.yaml"
    if not source.exists():
        raise SystemExit("config.example.yaml was not found")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)
    print(f"Created {destination}")


def main() -> None:
    _configure_console_output()
    args = _parser().parse_args()
    command = args.command or "run"
    if command == "init-config":
        _init_config(args.output)
        return
    config = _load(args.config)
    _configure_logging(config)

    if command == "validate-config":
        summary = {
            "symbols": config.canonical_symbols(),
            "exchanges": [
                name
                for name in ("binance", "bybit", "okx")
                if getattr(config.exchanges, name).enabled and config.symbol_map(name)
            ],
            "windows_seconds": [item.seconds for item in config.detector.windows],
            "dashboard": f"http://{config.dashboard.host}:{config.dashboard.port}/"
            if config.dashboard.enabled
            else None,
            "database": config.storage.database,
        }
        print(json.dumps(summary, indent=2))
        return

    if command == "doctor":
        result = asyncio.run(
            run_doctor(
                config,
                timeout=max(2.0, min(float(args.timeout), 60.0)),
                include_network=not args.skip_network,
            )
        )
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            for check in result["checks"]:
                marker = "PASS" if check["ok"] else "FAIL"
                latency = f" ({check['latency_ms']} ms)" if "latency_ms" in check else ""
                print(f"[{marker}] {check['name']}: {check['detail']}{latency}")
            print(
                f"Doctor {'passed' if result['ok'] else 'failed'} with {result['failed']} failure(s)."
            )
        if not result["ok"]:
            raise SystemExit(1)
        return

    if command == "simulate":
        result = asyncio.run(run_simulation(config))
        payload = {
            "trades_generated": result.trades_generated,
            "liquidations_generated": result.liquidations_generated,
            "alerts": [alert.to_dict() for alert in result.alerts],
        }
        if args.json:
            print(json.dumps(payload, indent=2))
        else:
            print(
                f"Generated {result.trades_generated} trades and "
                f"{result.liquidations_generated} liquidations."
            )
            if not result.alerts:
                print("No alerts fired; thresholds may be too strict.")
            for alert in result.alerts:
                print(f"[{alert.severity.value.upper()}] {alert.title}\n  {alert.message}")
        return

    if command == "test-alert":
        asyncio.run(_test_alert(config, args.severity))
        return

    try:
        asyncio.run(SentinelApp(config).run())
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        logging.getLogger("crypto_sentinel").exception("fatal error")
        sys.exit(f"Fatal error: {exc}")


if __name__ == "__main__":
    main()
