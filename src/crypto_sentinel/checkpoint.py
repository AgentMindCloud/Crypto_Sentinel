from __future__ import annotations

import asyncio
import gzip
import json
import logging
import os
from pathlib import Path
from typing import Any

from crypto_sentinel.state import MarketState


class StateCheckpoint:
    def __init__(self, path: str, max_age_seconds: int) -> None:
        self.path = Path(path) if path else None
        self.max_age_seconds = max_age_seconds
        self.log = logging.getLogger("crypto_sentinel.checkpoint")

    @property
    def enabled(self) -> bool:
        return self.path is not None

    async def save(self, state: MarketState, now_ms: int) -> bool:
        if not self.path:
            return False
        payload = state.export_checkpoint(now_ms)
        target = self.path

        def write() -> None:
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
            try:
                with gzip.open(temporary, "wt", encoding="utf-8", compresslevel=6) as handle:
                    json.dump(payload, handle, separators=(",", ":"), allow_nan=False)
                os.replace(temporary, target)
            finally:
                temporary.unlink(missing_ok=True)

        await asyncio.to_thread(write)
        return True

    async def load(
        self,
        state: MarketState,
        now_ms: int,
        allowed_pairs: set[tuple[str, str]],
    ) -> dict[str, int] | None:
        if not self.path or not self.path.exists():
            return None
        target = self.path

        def read() -> dict[str, Any]:
            with gzip.open(target, "rt", encoding="utf-8") as handle:
                payload = json.load(handle)
            if not isinstance(payload, dict):
                raise ValueError("checkpoint root must be an object")
            return payload

        try:
            payload = await asyncio.to_thread(read)
            saved_ms = int(payload.get("saved_ms", 0))
            age_ms = now_ms - saved_ms
            if saved_ms <= 0 or age_ms < -300_000:
                raise ValueError("checkpoint timestamp is invalid or too far in the future")
            if age_ms > self.max_age_seconds * 1000:
                self.log.warning("ignoring checkpoint older than %ds", self.max_age_seconds)
                return None
            restored = state.restore_checkpoint(payload, now_ms, allowed_pairs)
            self.log.info(
                "restored checkpoint with %d bucket(s) and %d liquidation(s)",
                restored["buckets"],
                restored["liquidations"],
            )
            return restored
        except Exception as exc:
            self.log.warning("could not restore checkpoint %s: %s", target, exc)
            return None
