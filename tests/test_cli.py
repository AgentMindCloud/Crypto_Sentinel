from __future__ import annotations

import sys
from typing import Any

from crypto_sentinel.cli import _configure_console_output


def test_configure_console_output_requests_utf8(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    class Stream:
        def reconfigure(self, **kwargs: Any) -> None:
            calls.append(kwargs)

    monkeypatch.setattr(sys, "stdout", Stream())
    monkeypatch.setattr(sys, "stderr", Stream())

    _configure_console_output()

    assert calls == [
        {"encoding": "utf-8", "errors": "backslashreplace"},
        {"encoding": "utf-8", "errors": "backslashreplace"},
    ]
