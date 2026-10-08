[CmdletBinding()]
param(
    [string]$TaskName = "Crypto Sentinel Free",
    [string]$DockerTaskName = "Crypto Sentinel Docker Shadow",
    [string]$SoakTaskName = "Crypto Sentinel Soak Monitor",
    [switch]$StartupShortcutOnly
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$SupervisorScript = Join-Path $PSScriptRoot "run_windows_supervisor.ps1"
$GuardianScript = Join-Path $PSScriptRoot "run_windows_guardian.ps1"
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    throw "Install the project and test it manually before registering startup."
}
foreach ($required in @($SupervisorScript, $GuardianScript)) {
    if (-not (Test-Path -LiteralPath $required)) {
        throw "Required Windows reliability script is missing: $required"
    }
}

$PowerShell = (Get-Command powershell.exe).Source
$Arguments = "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$GuardianScript`""
$StartupDirectory = [Environment]::GetFolderPath("Startup")
$ShortcutPath = Join-Path $StartupDirectory "Crypto Sentinel Free.lnk"
$LifecycleTaskNames = @($TaskName, $DockerTaskName, $SoakTaskName)

function Get-ScheduledLifecycleTasks {
    param([Parameter(Mandatory = $true)][string[]]$Names)

    $allTasks = @(Get-ScheduledTask -ErrorAction Stop)
    return @(
        $allTasks | Where-Object {
            $candidateName = $_.TaskName
            $Names -contains $candidateName
        }
    )
}

function Remove-ScheduledLifecycleTasks {
    param([Parameter(Mandatory = $true)][string[]]$Names)

    try {
        $existingTasks = @(Get-ScheduledLifecycleTasks -Names $Names)
        foreach ($existingTask in $existingTasks) {
            Unregister-ScheduledTask `
                -TaskName $existingTask.TaskName `
                -TaskPath $existingTask.TaskPath `
                -Confirm:$false `
                -ErrorAction Stop
        }
        $remainingTasks = @(Get-ScheduledLifecycleTasks -Names $Names)
    } catch {
        throw "Could not verify and remove competing Crypto Sentinel scheduled tasks: $($_.Exception.Message)"
    }

    if ($remainingTasks.Count -ne 0) {
        $remainingNames = ($remainingTasks | ForEach-Object { $_.TaskName }) -join ", "
        throw "Competing Crypto Sentinel scheduled tasks remain: $remainingNames"
    }
}

if (-not $StartupShortcutOnly) {
    try {
        # Older releases split Docker and soak ownership into separate tasks. Remove those
        # owners before registering the one guardian task.
        Remove-ScheduledLifecycleTasks -Names @($DockerTaskName, $SoakTaskName)
        if (Test-Path -LiteralPath $ShortcutPath) {
            Remove-Item -LiteralPath $ShortcutPath -Force -ErrorAction Stop
        }

        $Action = New-ScheduledTaskAction `
            -Execute $PowerShell `
            -Argument $Arguments `
            -WorkingDirectory $Root
        $Trigger = New-ScheduledTaskTrigger -AtLogOn
        $Settings = New-ScheduledTaskSettingsSet `
            -AllowStartIfOnBatteries `
            -DontStopIfGoingOnBatteries `
            -StartWhenAvailable `
            -RestartCount 999 `
            -RestartInterval (New-TimeSpan -Minutes 1) `
            -ExecutionTimeLimit ([TimeSpan]::Zero) `
            -MultipleInstances IgnoreNew
        $UserId = if ($env:USERDOMAIN) {
            "$env:USERDOMAIN\$env:USERNAME"
        } else {
            $env:USERNAME
        }
        $Principal = New-ScheduledTaskPrincipal `
            -UserId $UserId `
            -LogonType Interactive `
            -RunLevel Limited
        Register-ScheduledTask `
            -TaskName $TaskName `
            -Action $Action `
            -Trigger $Trigger `
            -Settings $Settings `
            -Principal $Principal `
            -Description "Guards and restarts the keyless Crypto Sentinel alarm after user logon." `
            -Force `
            -ErrorAction Stop | Out-Null

        Write-Host "Registered scheduled task:" -ForegroundColor Green
        Write-Host " - $TaskName (single-instance guardian)"
        Write-Host " - The guardian owns the supervisor, native alarm, Docker shadow, and soak monitor."
        Write-Host "The audible primary runs only while this user is logged in."
        exit 0
    } catch {
        Write-Warning "Task Scheduler registration was unavailable; evaluating the per-user Startup fallback."
    }
}

# Whether explicitly requested or reached as a fallback, the shortcut is created only after
# proving the old main, Docker, and soak tasks are all gone. If Task Scheduler cannot be
# inspected or cleaned, stop instead of knowingly assigning the lifecycle to two owners.
try {
    Remove-ScheduledLifecycleTasks -Names $LifecycleTaskNames
} catch {
    throw "Refusing to create the Startup shortcut because duplicate lifecycle ownership cannot be excluded. $($_.Exception.Message)"
}

$Shell = New-Object -ComObject WScript.Shell
$Shortcut = $Shell.CreateShortcut($ShortcutPath)
$Shortcut.TargetPath = $PowerShell
$Shortcut.Arguments = "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$GuardianScript`""
$Shortcut.WorkingDirectory = $Root
$Shortcut.WindowStyle = 7
$Shortcut.Description = "Guards the Crypto Sentinel supervisor and its monitoring processes."
$Shortcut.Save()

if (-not (Test-Path -LiteralPath $ShortcutPath)) {
    throw "The per-user Startup shortcut was not created."
}
Write-Host "Installed per-user automatic startup:" -ForegroundColor Green
Write-Host " - $ShortcutPath"
Write-Host " - A single-instance guardian restarts the supervisor after an unexpected exit."
Write-Host " - Native primary and soak monitor are restarted by the supervisor."
Write-Host " - Docker Desktop and the isolated shadow are recovered when needed."
