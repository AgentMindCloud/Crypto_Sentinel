[CmdletBinding()]
param(
    [string]$TaskName = "Crypto Sentinel Free",
    [string]$DockerTaskName = "Crypto Sentinel Docker Shadow",
    [string]$SoakTaskName = "Crypto Sentinel Soak Monitor",
    [switch]$StartupShortcutOnly
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$RunScript = Join-Path $PSScriptRoot "run_windows.ps1"
$DockerScript = Join-Path $PSScriptRoot "run_docker_shadow.ps1"
$SoakScript = Join-Path $PSScriptRoot "run_soak_monitor.ps1"
$SupervisorScript = Join-Path $PSScriptRoot "run_windows_supervisor.ps1"
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    throw "Install the project and test it manually before registering startup."
}

$PowerShell = (Get-Command powershell.exe).Source
$Arguments = "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$RunScript`""
$Action = New-ScheduledTaskAction -Execute $PowerShell -Argument $Arguments -WorkingDirectory $Root
$Trigger = New-ScheduledTaskTrigger -AtLogOn
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 999 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -MultipleInstances IgnoreNew
$UserId = if ($env:USERDOMAIN) { "$env:USERDOMAIN\$env:USERNAME" } else { $env:USERNAME }
$Principal = New-ScheduledTaskPrincipal -UserId $UserId -LogonType Interactive -RunLevel Limited

if (-not $StartupShortcutOnly) {
    try {
        Register-ScheduledTask `
            -TaskName $TaskName `
            -Action $Action `
            -Trigger $Trigger `
            -Settings $Settings `
            -Principal $Principal `
            -Description "Runs the keyless Crypto Sentinel market alarm after user logon." `
            -Force `
            -ErrorAction Stop | Out-Null

        $DockerAction = New-ScheduledTaskAction `
            -Execute $PowerShell `
            -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$DockerScript`"" `
            -WorkingDirectory $Root
        $DockerSettings = New-ScheduledTaskSettingsSet `
            -AllowStartIfOnBatteries `
            -DontStopIfGoingOnBatteries `
            -StartWhenAvailable `
            -RestartCount 5 `
            -RestartInterval (New-TimeSpan -Minutes 2) `
            -ExecutionTimeLimit (New-TimeSpan -Minutes 10) `
            -MultipleInstances IgnoreNew
        Register-ScheduledTask `
            -TaskName $DockerTaskName `
            -Action $DockerAction `
            -Trigger $Trigger `
            -Settings $DockerSettings `
            -Principal $Principal `
            -Description "Ensures the isolated Crypto Sentinel Docker shadow is running after logon." `
            -Force `
            -ErrorAction Stop | Out-Null

        $SoakAction = New-ScheduledTaskAction `
            -Execute $PowerShell `
            -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$SoakScript`"" `
            -WorkingDirectory $Root
        Register-ScheduledTask `
            -TaskName $SoakTaskName `
            -Action $SoakAction `
            -Trigger $Trigger `
            -Settings $Settings `
            -Principal $Principal `
            -Description "Records bounded native and Docker-shadow reliability evidence." `
            -Force `
            -ErrorAction Stop | Out-Null

        Write-Host "Registered scheduled tasks:" -ForegroundColor Green
        Write-Host " - $TaskName (native audible primary)"
        Write-Host " - $DockerTaskName (isolated Docker shadow)"
        Write-Host " - $SoakTaskName (bounded reliability evidence)"
        Write-Host "The audible primary runs only while this user is logged in."
        exit 0
    } catch {
        Write-Warning "Task Scheduler registration was denied; installing the per-user Startup fallback."
    }
}

$StartupDirectory = [Environment]::GetFolderPath("Startup")
$ShortcutPath = Join-Path $StartupDirectory "Crypto Sentinel Free.lnk"
$Shell = New-Object -ComObject WScript.Shell
$Shortcut = $Shell.CreateShortcut($ShortcutPath)
$Shortcut.TargetPath = $PowerShell
$Shortcut.Arguments = "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$SupervisorScript`""
$Shortcut.WorkingDirectory = $Root
$Shortcut.WindowStyle = 7
$Shortcut.Description = "Supervises the audible Crypto Sentinel primary, Docker shadow, and soak monitor."
$Shortcut.Save()

if (-not (Test-Path -LiteralPath $ShortcutPath)) {
    throw "The per-user Startup shortcut was not created."
}
Write-Host "Installed per-user automatic startup:" -ForegroundColor Green
Write-Host " - $ShortcutPath"
Write-Host " - Native primary and soak monitor are restarted by the supervisor."
Write-Host " - Docker Desktop and the isolated shadow are recovered when needed."
