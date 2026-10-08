"""Owner-only Windows connection metadata; never exposes dashboard or ingest authority."""

from __future__ import annotations

import argparse
import base64
import ctypes
import hmac
import json
import os
import re
import secrets
from ctypes import wintypes
from pathlib import Path


def connection_path(config_path: Path) -> Path:
    return config_path.parent / "data" / "ceos-connection" / (config_path.name + ".connector.json")


def _dpapi(data: bytes, protect: bool) -> bytes:
    if os.name != "nt":
        raise ValueError("Managed Sentinel connection requires Windows DPAPI")

    class Blob(ctypes.Structure):
        _fields_ = [("cbData", wintypes.DWORD), ("pbData", ctypes.POINTER(ctypes.c_byte))]

    buffer = ctypes.create_string_buffer(data)
    source = Blob(len(data), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_byte)))
    result = Blob()
    crypt = ctypes.WinDLL("crypt32", use_last_error=True)
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.LocalFree.argtypes = [ctypes.c_void_p]
    kernel.LocalFree.restype = ctypes.c_void_p
    function = crypt.CryptProtectData if protect else crypt.CryptUnprotectData
    function.argtypes = [
        ctypes.POINTER(Blob),
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
        wintypes.DWORD,
        ctypes.POINTER(Blob),
    ]
    function.restype = wintypes.BOOL
    if not function(ctypes.byref(source), None, None, None, None, 1, ctypes.byref(result)):
        raise ValueError("Managed Sentinel credential is unavailable for this Windows owner")
    try:
        return ctypes.string_at(result.pbData, result.cbData)
    finally:
        kernel.LocalFree(result.pbData)


def read_connection(config_path: Path) -> dict:
    path = connection_path(config_path)
    if path.is_symlink() or path.parent.is_symlink() or path.stat().st_size > 16_384:
        raise ValueError("Invalid managed connection file")
    value = json.loads(path.read_text(encoding="utf-8-sig"))
    if (
        set(value) != {"schemaVersion", "origin", "sourceRevision", "protectedToken"}
        or value["schemaVersion"] != 1
    ):
        raise ValueError("Incompatible managed connection")
    if value["origin"] != "http://127.0.0.1:8787" or not re.fullmatch(
        r"[a-f0-9]{64}", value["sourceRevision"]
    ):
        raise ValueError("Unregistered managed connection")
    token = _dpapi(base64.b64decode(value["protectedToken"], validate=True), False).decode("ascii")
    if not re.fullmatch(r"[A-Za-z0-9_-]{43}", token):
        raise ValueError("Invalid managed credential")
    return {
        "schemaVersion": 1,
        "origin": value["origin"],
        "sourceRevision": value["sourceRevision"],
        "readToken": token,
    }


def managed_token(config_path: Path) -> str | None:
    if not connection_path(config_path).exists():
        return None
    from crypto_sentinel.integration import source_revision

    value = read_connection(config_path)
    if not hmac.compare_digest(value["sourceRevision"], source_revision()):
        raise ValueError("Managed Sentinel source changed; verify and reselect the candidate")
    return value["readToken"]


def setup(config_path: Path, expected_revision: str, expected_previous_revision: str = "") -> dict:
    from crypto_sentinel.config import load_config
    from crypto_sentinel.integration import source_revision

    revision = source_revision()
    if not hmac.compare_digest(revision, expected_revision):
        raise ValueError("Reviewed Sentinel source revision does not match")
    # Validate an existing installation without emitting its configuration or tokens.
    config = load_config(config_path)
    if (
        not config.dashboard.enabled
        or config.dashboard.host != "127.0.0.1"
        or config.dashboard.port != 8787
    ):
        raise ValueError("Managed connection requires the existing loopback primary on port 8787")
    path = connection_path(config_path)
    if path.exists():
        existing = read_connection(config_path)
        if expected_previous_revision and not hmac.compare_digest(
            existing["sourceRevision"], expected_previous_revision
        ):
            raise ValueError("Previous selected source revision does not match")
        if existing["sourceRevision"] != revision:
            if not expected_previous_revision:
                raise ValueError("Existing selection differs; explicit replacement is required")
            # Keep the original DPAPI envelope byte-for-byte; replace only reviewed identity.
            # The caller owns the lifecycle mutex and restarts the cached detector afterward.
            previous = path.read_bytes()
            value = json.loads(previous.decode("utf-8-sig"))
            if value["sourceRevision"] != expected_previous_revision:
                raise ValueError("Managed selection changed during replacement")
            value["sourceRevision"] = revision
            temporary = path.with_name(path.name + "." + secrets.token_hex(12) + ".tmp")
            try:
                with temporary.open("x", encoding="utf-8") as handle:
                    json.dump(value, handle)
                    handle.flush()
                    os.fsync(handle.fileno())
                if path.read_bytes() != previous:
                    raise ValueError("Managed selection changed during replacement")
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
            return {
                "configured": True,
                "origin": existing["origin"],
                "sourceRevision": revision,
                "previousSourceRevision": expected_previous_revision,
                "reused": True,
                "reselected": True,
            }
        return {
            "configured": True,
            "origin": existing["origin"],
            "sourceRevision": revision,
            "reused": True,
        }
    if expected_previous_revision:
        raise ValueError("Previous managed selection is missing")
    token = secrets.token_urlsafe(32)
    value = {
        "schemaVersion": 1,
        "origin": "http://127.0.0.1:8787",
        "sourceRevision": revision,
        "protectedToken": base64.b64encode(_dpapi(token.encode("ascii"), True)).decode("ascii"),
    }
    # Parent directory is created with an owner-only Windows ACL by the installer.
    with path.open("x", encoding="utf-8") as handle:
        json.dump(value, handle)
        handle.flush()
        os.fsync(handle.fileno())
    return {
        "configured": True,
        "origin": value["origin"],
        "sourceRevision": revision,
        "reused": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["setup", "connector", "revision"])
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--revision", default="")
    parser.add_argument("--previous-revision", default="")
    args = parser.parse_args()
    try:
        config_path = Path(args.config).resolve()
        if args.action == "revision":
            from crypto_sentinel.integration import source_revision

            result = {"sourceRevision": source_revision()}
        elif args.action == "setup":
            result = setup(config_path, args.revision, args.previous_revision)
        else:
            value = read_connection(config_path)
            from crypto_sentinel.integration import source_revision

            if not hmac.compare_digest(value["sourceRevision"], source_revision()):
                raise ValueError("Selected Sentinel source revision differs")
            result = value
        print(json.dumps(result))
    except Exception:
        # No configuration, secrets, response body or provider exception leaves this boundary.
        print(json.dumps({"error": "sentinel_managed_connection_unavailable"}))
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
