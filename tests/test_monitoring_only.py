import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = ROOT / "src" / "crypto_sentinel"


def test_release_surface_has_no_trading_wallet_or_exchange_key_capability() -> None:
    forbidden_identifiers = {
        "api_key",
        "api_secret",
        "exchange_api_key",
        "exchange_api_secret",
        "private_key",
        "seed_phrase",
        "mnemonic",
        "wallet",
        "wallet_address",
        "place_order",
        "create_order",
        "submit_order",
        "cancel_order",
        "sign_order",
        "execute_trade",
        "withdraw",
        "withdrawal_address",
    }
    forbidden_literals = {
        "/api/v3/order",
        "/fapi/v1/order",
        "/v5/order/create",
        "/api/v5/trade/order",
        "/api/v5/asset/withdrawal",
        "x-mbx-apikey",
        "x-bapi-api-key",
        "ok-access-key",
        "walletconnect",
    }
    found_identifiers: set[str] = set()
    string_literals: list[str] = []

    for path in SOURCE_ROOT.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            candidates: list[str] = []
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                candidates.append(node.name)
            elif isinstance(node, ast.Name):
                candidates.append(node.id)
            elif isinstance(node, ast.Attribute):
                candidates.append(node.attr)
            elif isinstance(node, ast.arg):
                candidates.append(node.arg)
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                string_literals.append(node.value.lower())
            found_identifiers.update(
                candidate.lower()
                for candidate in candidates
                if candidate.lower() in forbidden_identifiers
            )

    assert found_identifiers == set()
    joined_literals = "\n".join(string_literals)
    assert not any(value in joined_literals for value in forbidden_literals)

    dependencies = "\n".join(
        (ROOT / filename).read_text(encoding="utf-8").lower()
        for filename in ("pyproject.toml", "requirements.txt", "requirements.lock")
    )
    forbidden_execution_packages = {
        "ccxt",
        "web3",
        "binance-connector",
        "python-binance",
        "pybit",
        "okx-sdk",
    }
    assert not any(package in dependencies for package in forbidden_execution_packages)
