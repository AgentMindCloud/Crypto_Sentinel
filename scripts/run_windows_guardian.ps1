[CmdletBinding()]
param(
    [int]$InitialBackoffSeconds = 5,
    [int]$MaximumBackoffSeconds = 60,
    [int]$StableRunSeconds = 300,
    [long]$LogMaxBytes = 2MB
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$SupervisorScript = Join-Path $PSScriptRoot "run_windows_supervisor.ps1"
$LogPath = Join-Path $Root "data\guardian.log"

if ($InitialBackoffSeconds -lt 1) {
    throw "InitialBackoffSeconds must be at least 1."
}
if ($MaximumBackoffSeconds -lt $InitialBackoffSeconds) {
    throw "MaximumBackoffSeconds must be at least InitialBackoffSeconds."
}
if ($StableRunSeconds -lt 1) {
    throw "StableRunSeconds must be at least 1."
}
if ($LogMaxBytes -lt 1024) {
    throw "LogMaxBytes must be at least 1024."
}
if (-not (Test-Path -LiteralPath $SupervisorScript)) {
    throw "Required supervisor script is missing: $SupervisorScript"
}

$PowerShell = (Get-Command powershell.exe -ErrorAction Stop).Source

function Write-GuardianLog {
    param([string]$Message)

    try {
        $logDirectory = Split-Path -Parent $LogPath
        New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null
        if ((Test-Path -LiteralPath $LogPath) -and
            (Get-Item -LiteralPath $LogPath).Length -ge $LogMaxBytes) {
            $rotatedLogPath = "$LogPath.1"
            if (Test-Path -LiteralPath $rotatedLogPath) {
                Remove-Item -LiteralPath $rotatedLogPath -Force
            }
            Move-Item -LiteralPath $LogPath -Destination $rotatedLogPath
        }
        $timestamp = (Get-Date).ToUniversalTime().ToString("o")
        Add-Content -LiteralPath $LogPath -Value "$timestamp $Message" -Encoding utf8
    } catch {
        # Logging must never disable the guardian's restart or audible-alarm path.
    }
}

function Get-ExistingSupervisorProcess {
    try {
        $candidate = Get-CimInstance Win32_Process |
            Where-Object {
                $_.ProcessId -ne $PID -and
                $_.CommandLine -and
                $_.CommandLine.IndexOf(
                    $SupervisorScript,
                    [System.StringComparison]::OrdinalIgnoreCase
                ) -ge 0
            } |
            Sort-Object CreationDate |
            Select-Object -First 1
        if ($candidate) {
            return Get-Process -Id $candidate.ProcessId -ErrorAction SilentlyContinue
        }
    } catch {
        Write-GuardianLog (
            "Existing-supervisor discovery failed: " +
            "$($_.Exception.GetType().Name): $($_.Exception.Message)"
        )
    }
    return $null
}

function Invoke-GuardianCriticalAlarm {
    for ($index = 0; $index -lt 3; $index++) {
        try {
            [System.Media.SystemSounds]::Hand.Play()
        } catch {
            try {
                [console]::Beep(1400, 700)
            } catch {
                # Some non-interactive audio devices expose neither fallback.
            }
        }
        if ($index -lt 2) {
            Start-Sleep -Milliseconds 700
        }
    }
}

$createdNew = $false
$mutex = [System.Threading.Mutex]::new(
    $true,
    "Local\CryptoSentinelFreeGuardian",
    [ref]$createdNew
)
if (-not $createdNew) {
    $mutex.Dispose()
    exit 0
}

$backoffSeconds = $InitialBackoffSeconds

try {
    Write-GuardianLog "Guardian started."
    while ($true) {
        $startedAt = Get-Date
        $exitCode = "not-started"

        try {
            $supervisor = Get-ExistingSupervisorProcess
            if ($supervisor) {
                Write-GuardianLog "Adopted existing supervisor PID $($supervisor.Id)."
            } else {
                $supervisorArguments = (
                    "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden " +
                    "-File `"$SupervisorScript`""
                )
                $supervisor = Start-Process `
                    -FilePath $PowerShell `
                    -ArgumentList $supervisorArguments `
                    -WorkingDirectory $Root `
                    -WindowStyle Hidden `
                    -PassThru
                Write-GuardianLog "Started supervisor PID $($supervisor.Id)."
            }
            $supervisor.WaitForExit()
            $exitCode = [string]$supervisor.ExitCode
        } catch {
            $exitCode = "launch-or-wait-error"
            Write-GuardianLog (
                "Supervisor launch/wait failed: " +
                "$($_.Exception.GetType().Name): $($_.Exception.Message)"
            )
        }

        $runSeconds = [Math]::Max(
            0,
            [int]((Get-Date) - $startedAt).TotalSeconds
        )
        Write-GuardianLog (
            "Supervisor exited unexpectedly: exit_code=$exitCode " +
            "runtime_seconds=$runSeconds."
        )
        Invoke-GuardianCriticalAlarm

        if ($runSeconds -ge $StableRunSeconds) {
            $backoffSeconds = $InitialBackoffSeconds
        }
        $delaySeconds = [Math]::Min(
            $MaximumBackoffSeconds,
            [Math]::Max($InitialBackoffSeconds, $backoffSeconds)
        )
        Write-GuardianLog "Restarting supervisor in $delaySeconds seconds."
        Start-Sleep -Seconds $delaySeconds
        $backoffSeconds = [Math]::Min(
            $MaximumBackoffSeconds,
            [Math]::Max($InitialBackoffSeconds, $delaySeconds * 2)
        )
    }
} finally {
    # Do not stop the supervisor here. At logoff, Windows may end the guardian
    # first; leaving the child untouched avoids turning an ordinary session
    # transition into a guardian-caused monitoring outage.
    Write-GuardianLog "Guardian stopped; supervisor was not terminated."
    if ($createdNew) {
        $mutex.ReleaseMutex()
    }
    $mutex.Dispose()
}
