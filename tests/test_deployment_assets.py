from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[1]


def test_docker_healthcheck_uses_full_runtime_readiness() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "/api/healthz" in dockerfile
    assert "--start-period=90s" in dockerfile
    assert "/api/status" not in dockerfile
    assert "DASHBOARD_TOKEN" not in dockerfile


def test_docker_shadow_is_loopback_isolated_and_non_notifying() -> None:
    compose = (ROOT / "compose.yaml").read_text(encoding="utf-8")
    config = yaml.safe_load((ROOT / "config.docker.example.yaml").read_text(encoding="utf-8"))

    assert "127.0.0.1:8788:8787" in compose
    assert "read_only: true" in compose
    assert "no-new-privileges:true" in compose
    assert "cap_drop:" in compose and "- ALL" in compose
    assert config["dashboard"]["open_browser"] is False
    assert all(
        config["notifiers"][name]["enabled"] is False
        for name in ("local", "ntfy", "telegram", "webhook")
    )


def test_all_startup_paths_target_one_guardian_owned_lifecycle() -> None:
    registration = (ROOT / "scripts" / "register_windows_startup.ps1").read_text(encoding="utf-8")

    assert '$GuardianScript = Join-Path $PSScriptRoot "run_windows_guardian.ps1"' in (registration)
    assert (
        '$Arguments = "-NoProfile -ExecutionPolicy Bypass '
        '-WindowStyle Hidden -File `"$GuardianScript`""'
    ) in registration
    assert (
        '$Shortcut.Arguments = "-NoProfile -ExecutionPolicy Bypass '
        '-WindowStyle Hidden -File `"$GuardianScript`""'
    ) in registration
    assert "-Argument $Arguments" in registration
    assert "$RunScript" not in registration
    assert "$DockerScript" not in registration
    assert "$SoakScript" not in registration
    assert "Unregister-ScheduledTask" in registration
    assert registration.count("Register-ScheduledTask `") == 1
    fallback_cleanup = registration.index(
        "Remove-ScheduledLifecycleTasks -Names $LifecycleTaskNames"
    )
    shortcut_creation = registration.index("$Shell.CreateShortcut($ShortcutPath)")
    assert fallback_cleanup < shortcut_creation
    assert "duplicate lifecycle ownership cannot be excluded" in registration


def test_systemd_installer_uses_an_exact_code_allowlist_and_restrictive_modes() -> None:
    installer = (ROOT / "scripts" / "install_systemd.sh").read_text(encoding="utf-8")
    allowlist_match = re.search(r"SOURCE_FILES=\(\n(?P<body>.*?)\n\)", installer, re.DOTALL)

    assert allowlist_match is not None
    source_files = re.findall(r'"([^"]+)"', allowlist_match.group("body"))
    assert source_files
    assert all((ROOT / relative_path).is_file() for relative_path in source_files)
    assert {
        "pyproject.toml",
        "README.md",
        "config.vps.example.yaml",
        "scripts/generate_tokens.py",
        "deploy/systemd/crypto-sentinel.service",
        "src/crypto_sentinel/app.py",
        "src/crypto_sentinel/static/index.html",
    }.issubset(source_files)
    assert not any(
        relative_path.startswith((".git", ".env", "data/", "tests/", "docs/"))
        or relative_path in {"config.yaml", "config.docker.yaml"}
        or "RELEASE_CHECKS" in relative_path
        or "PROJECT_STATUS" in relative_path
        for relative_path in source_files
    )
    assert "tar -C" not in installer
    assert "cp -a" not in installer
    assert "cp -r" not in installer
    assert 'install -d -o root -g "$SERVICE_USER" -m 0750 "$CONFIG_ROOT"' in installer
    assert 'install -d -o "$SERVICE_USER" -g "$SERVICE_USER" -m 0700 "$DATA_ROOT"' in installer
    assert 'chmod 0640 "$CONFIG_ROOT/config.yaml" "$CONFIG_ROOT/sentinel.env"' in installer
    assert 'chown -R root:root "$INSTALL_ROOT"' in installer


def test_guardian_restarts_supervisor_with_bounded_alarm_and_log() -> None:
    guardian = (ROOT / "scripts" / "run_windows_guardian.ps1").read_text(encoding="utf-8")

    assert '"Local\\CryptoSentinelFreeGuardian"' in guardian
    assert '"run_windows_supervisor.ps1"' in guardian
    assert "Start-Process" in guardian
    assert "Get-ExistingSupervisorProcess" in guardian
    assert "Adopted existing supervisor PID" in guardian
    assert "$supervisor.WaitForExit()" in guardian
    assert '"data\\guardian.log"' in guardian
    assert "$LogMaxBytes" in guardian
    assert "Remove-Item -LiteralPath $rotatedLogPath -Force" in guardian
    assert "Move-Item -LiteralPath $LogPath -Destination $rotatedLogPath" in guardian
    assert "[System.Media.SystemSounds]::Hand.Play()" in guardian
    assert "[console]::Beep(1400, 700)" in guardian
    assert "$MaximumBackoffSeconds" in guardian
    assert "[Math]::Min(" in guardian
    assert "while ($true)" in guardian
    assert "Stop-Process" not in guardian
    assert "Invoke-RestMethod" not in guardian
    assert "Invoke-WebRequest" not in guardian
    assert "DASHBOARD_TOKEN" not in guardian
    assert ".env" not in guardian
    assert "api_key" not in guardian.lower()
    assert "wallet" not in guardian.lower()
    assert "withdraw" not in guardian.lower()


def test_supervisor_alarms_on_tracked_native_exit_before_restart() -> None:
    supervisor = (ROOT / "scripts" / "run_windows_supervisor.ps1").read_text(encoding="utf-8")

    exit_branch = supervisor.index("if ($null -ne $native -and $native.HasExited)")
    restart_branch = supervisor.index("if ($null -eq $native)", exit_branch)
    exit_handling = supervisor[exit_branch:restart_branch]

    assert "Native primary exited unexpectedly: exit_code=" in exit_handling
    assert "$lastNativeExitAlarm.AddSeconds($AlarmCooldownSeconds)" in exit_handling
    assert "Invoke-WatchdogAlarm -Critical $true" in exit_handling
    assert "$native = $null" in exit_handling
    assert "$lastPrimaryHealthAlarm" in supervisor
    assert "$lastShadowHealthAlarm" in supervisor
