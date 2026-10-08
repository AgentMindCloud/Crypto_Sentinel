"""Execute the shipped launch expressions with synthetic modules and hostile paths."""

from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path

import pytest


def _ps_literal(value: Path | str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


@pytest.mark.skipif(os.name != "nt", reason="Windows managed launch boundary")
@pytest.mark.parametrize(
    "ambient_override", [False, True], ids=["hostile-cwd", "hostile-python-env"]
)
@pytest.mark.parametrize(
    "script_name,module_name,operation",
    [
        ("Manage-Integration.ps1", "connection", "connector"),
        ("Setup-Integration.ps1", "connection", "setup"),
        ("run_windows.ps1", "__main__", "run"),
    ],
)
def test_selected_python_bootstrap_ignores_unselected_imports(
    tmp_path: Path, script_name: str, module_name: str, operation: str, ambient_override: bool
) -> None:
    repository = Path(__file__).resolve().parents[1]
    python = repository / ".venv" / "Scripts" / "python.exe"
    powershell = Path(os.environ["SYSTEMROOT"]) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    if not python.exists():
        pytest.skip("Pinned local virtual environment is unavailable")

    # Read source scripts only. Never invoke their setup/start/configuration bodies.
    script = (repository / "scripts" / script_name).read_text(encoding="utf-8-sig")
    launches = [
        line.strip()
        for line in script.splitlines()
        if re.search(r"(?:^|=)\s*&\s*\$Python\s", line.strip())
    ]
    assert len(launches) == 1, "Review a changed launch seam before extending this check"
    launch = launches[0]

    selected = tmp_path / "selected source"
    package = selected / "src" / "crypto_sentinel"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("", encoding="utf-8")
    result = tmp_path / "selected-result.json"
    module = package / (module_name + ".py")
    module.write_text(
        "import json, os, sys\nfrom pathlib import Path\n"
        "value={'isolated':sys.flags.isolated,'module':str(Path(__file__).resolve()),'argv':sys.argv[1:]}\n"
        "Path(os.environ['CEOS_BOOTSTRAP_TEST_RESULT']).write_text(json.dumps(value),encoding='utf-8')\n"
        "print(json.dumps({'synthetic_selected_module':True}))\n",
        encoding="utf-8",
    )
    hostile = tmp_path / "hostile caller"
    shadow = hostile / "crypto_sentinel"
    shadow.mkdir(parents=True)
    marker = tmp_path / "unselected-executed.txt"
    (shadow / "__init__.py").write_text("", encoding="utf-8")
    (shadow / (module_name + ".py")).write_text(
        "import os\nfrom pathlib import Path\n"
        "Path(os.environ['CEOS_BOOTSTRAP_TEST_SHADOW']).write_text('unselected',encoding='utf-8')\n",
        encoding="utf-8",
    )
    config = selected / "synthetic-never-read.yaml"
    revision = "0" * 64
    harness = tmp_path / "launch-expression.ps1"
    harness.write_text(
        "$ErrorActionPreference='Stop'\n"
        + "$Root="
        + _ps_literal(selected)
        + "\n"
        + "$Python="
        + _ps_literal(python)
        + "\n"
        + "$Config="
        + _ps_literal(config)
        + "\n"
        + "$ReviewedSourceRevision="
        + _ps_literal(revision)
        + "\n"
        + "$ExpectedPreviousRevision=''\n"
        + launch
        + "\nif($LASTEXITCODE -ne 0){exit $LASTEXITCODE}\n",
        encoding="utf-8",
    )
    env = {
        key: os.environ[key]
        for key in (
            "SystemRoot",
            "WINDIR",
            "USERPROFILE",
            "APPDATA",
            "LOCALAPPDATA",
            "TEMP",
            "TMP",
            "SystemDrive",
        )
        if key in os.environ
    }
    env["PATH"] = str(Path(os.environ["SYSTEMROOT"]) / "System32")
    env["PATHEXT"] = ".COM;.EXE;.BAT;.CMD"
    env["CEOS_BOOTSTRAP_TEST_RESULT"] = str(result)
    env["CEOS_BOOTSTRAP_TEST_SHADOW"] = str(marker)
    if ambient_override:
        env.update(
            PYTHONHOME=str(hostile / "missing-python-home"),
            PYTHONPATH=str(hostile),
            PYTHONUSERBASE=str(hostile),
        )
    completed = subprocess.run(
        [str(powershell), "-NoProfile", "-NonInteractive", "-File", str(harness)],
        cwd=hostile,
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
        creationflags=subprocess.CREATE_NO_WINDOW,
        check=False,
    )
    assert completed.returncode == 0, "Selected Python launch failed with untrusted ambient paths"
    assert not marker.exists(), "Unselected caller-directory module was executed"
    assert result.exists(), "The selected synthetic source did not execute"
    value = json.loads(result.read_text(encoding="utf-8"))
    assert value["isolated"] == 1
    assert Path(value["module"]) == module.resolve()
    expected = (
        ["--config", str(config), "run"]
        if operation == "run"
        else [operation, "--config", str(config)]
    )
    if operation == "setup":
        expected += ["--revision", revision, "--previous-revision="]
    assert value["argv"] == expected
    assert not config.exists(), "No application configuration should be opened or created"
