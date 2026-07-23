from __future__ import annotations

import argparse
import json
import subprocess
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Record bounded native and Docker-shadow health samples."
    )
    parser.add_argument("--primary-url", default="http://127.0.0.1:8787")
    parser.add_argument("--shadow-url", default="http://127.0.0.1:8788")
    parser.add_argument("--primary-env", default=".env")
    parser.add_argument("--shadow-env", default=".env.docker")
    parser.add_argument("--output", default="data/soak.jsonl")
    parser.add_argument("--interval", type=float, default=60.0)
    parser.add_argument("--max-bytes", type=int, default=25_000_000)
    parser.add_argument("--once", action="store_true")
    return parser


def _read_token(path: str | Path) -> str:
    env_path = Path(path)
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        key, separator, value = raw_line.partition("=")
        if separator and key.strip() == "DASHBOARD_TOKEN":
            token = value.strip()
            if token:
                return token
    raise ValueError(f"DASHBOARD_TOKEN is missing from {env_path}")


def _fetch_status(base_url: str, token: str) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/status",
        headers={"Authorization": f"Bearer {token}"},
    )
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            payload = json.load(response)
    except (OSError, ValueError, urllib.error.URLError) as exc:
        return {
            "ok": False,
            "latency_ms": round((time.monotonic() - started) * 1000),
            "error": f"{type(exc).__name__}: {str(exc)[:240]}",
        }

    feed_rows: dict[str, dict[str, Any]] = {}
    stale_limits = payload.get("stale_after_seconds", {})
    for name, raw in payload.get("feeds", {}).items():
        age = raw.get("message_age_seconds")
        stale_after = stale_limits.get(name)
        healthy = bool(
            raw.get("connected")
            and age is not None
            and stale_after is not None
            and 0 <= float(age) <= float(stale_after)
        )
        feed_rows[name] = {
            "healthy": healthy,
            "connected": bool(raw.get("connected")),
            "message_age_seconds": age,
            "reconnects": int(raw.get("reconnects", 0)),
            "dropped_events": int(raw.get("dropped_events", 0)),
            "trades": int(raw.get("trades", 0)),
            "liquidations": int(raw.get("liquidations", 0)),
        }

    return {
        "ok": bool(feed_rows) and all(row["healthy"] for row in feed_rows.values()),
        "latency_ms": round((time.monotonic() - started) * 1000),
        "uptime_seconds": payload.get("uptime_seconds"),
        "queue_size": payload.get("queue_size"),
        "queue_capacity": payload.get("queue_capacity"),
        "feeds": feed_rows,
    }


def _docker_state() -> dict[str, Any]:
    try:
        container_id = subprocess.run(
            ["docker", "compose", "ps", "-q", "crypto-sentinel"],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        ).stdout.strip()
        if not container_id:
            return {"present": False}
        raw = subprocess.run(
            [
                "docker",
                "inspect",
                container_id,
                "--format",
                "{{json .}}",
            ],
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        ).stdout
        container = json.loads(raw)
        state = container.get("State") or {}
        return {
            "present": True,
            "running": bool(state.get("Running")),
            "status": state.get("Status"),
            "started_at": state.get("StartedAt"),
            "restart_count": int(container.get("RestartCount", 0)),
            "health": (state.get("Health") or {}).get("Status"),
        }
    except (OSError, ValueError, subprocess.SubprocessError) as exc:
        return {"present": False, "error": f"{type(exc).__name__}: {str(exc)[:240]}"}


def _append_bounded(path: Path, row: dict[str, Any], max_bytes: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size >= max_bytes:
        rotated = path.with_suffix(path.suffix + ".1")
        rotated.unlink(missing_ok=True)
        path.replace(rotated)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(row, separators=(",", ":"), sort_keys=True))
        handle.write("\n")


def main() -> None:
    args = _parser().parse_args()
    if args.interval < 5:
        raise SystemExit("--interval must be at least 5 seconds")
    primary_token = _read_token(args.primary_env)
    shadow_token = _read_token(args.shadow_env)
    output = Path(args.output)
    previous_sample = time.monotonic()

    while True:
        now_monotonic = time.monotonic()
        gap_seconds = now_monotonic - previous_sample
        row = {
            "timestamp": datetime.now(UTC).isoformat(),
            "monitor_gap_seconds": (
                round(gap_seconds, 3) if gap_seconds > args.interval * 3 else 0
            ),
            "primary": _fetch_status(args.primary_url, primary_token),
            "shadow": _fetch_status(args.shadow_url, shadow_token),
            "docker": _docker_state(),
        }
        _append_bounded(output, row, args.max_bytes)
        if args.once:
            print(json.dumps(row, indent=2, sort_keys=True))
            return
        previous_sample = now_monotonic
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
