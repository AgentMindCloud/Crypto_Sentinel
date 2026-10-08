[CmdletBinding()]
param(
    [int]$CheckSeconds = 10,
    [int]$DockerRetrySeconds = 60,
    [int]$HealthCheckSeconds = 30,
    [int]$FailuresBeforeAlarm = 2,
    [int]$AlarmCooldownSeconds = 300,
    [int]$WatchdogWarmupSeconds = 90
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
$Python = Join-Path $Root ".venv\Scripts\python.exe"
$DockerScript = Join-Path $PSScriptRoot "run_docker_shadow.ps1"
$LogPath = Join-Path $Root "data\supervisor.log"
$StatusPath = Join-Path $Root "data\supervisor-status.json"
$LogMaxBytes = 2MB

foreach ($required in @($Python, $DockerScript, (Join-Path $Root "config.yaml"))) {
    if (-not (Test-Path -LiteralPath $required)) {
        throw "Required supervisor file is missing: $required"
    }
}

function Write-SupervisorLog {
    param([string]$Message)
    $logDirectory = Split-Path -Parent $LogPath
    New-Item -ItemType Directory -Path $logDirectory -Force | Out-Null
    if ((Test-Path -LiteralPath $LogPath) -and
        (Get-Item -LiteralPath $LogPath).Length -ge $LogMaxBytes) {
        Move-Item -LiteralPath $LogPath -Destination "$LogPath.1" -Force
    }
    $timestamp = (Get-Date).ToUniversalTime().ToString("o")
    Add-Content -LiteralPath $LogPath -Value "$timestamp $Message" -Encoding utf8
}

function Read-DotEnvValue {
    param(
        [string]$Path,
        [string]$Name
    )
    if (-not (Test-Path -LiteralPath $Path)) {
        return ""
    }
    foreach ($line in Get-Content -LiteralPath $Path) {
        $key, $value = $line -split "=", 2
        if ($key.Trim() -eq $Name) {
            return $value.Trim()
        }
    }
    return ""
}

function Get-EndpointReadiness {
    param(
        [string]$BaseUrl,
        [string]$Token
    )
    try {
        $headers = @{}
        if ($Token) {
            $headers["Authorization"] = "Bearer $Token"
        }
        $status = Invoke-RestMethod `
            -Uri "$($BaseUrl.TrimEnd('/'))/api/status" `
            -Headers $headers `
            -TimeoutSec 5

        if ($status.PSObject.Properties.Name -contains "readiness") {
            $ready = [bool]$status.readiness.ready
            $reasons = @($status.readiness.reasons) -join "; "
            return [pscustomobject]@{
                healthy = $ready
                reason = $(if ($ready) { "" } elseif ($reasons) { $reasons } else { "not ready" })
            }
        }

        foreach ($feedProperty in $status.feeds.PSObject.Properties) {
            $feed = $feedProperty.Value
            $staleAfter = $status.stale_after_seconds.($feedProperty.Name)
            $feedAge = $feed.message_age_seconds
            if ($feed.PSObject.Properties.Name -contains "latest_message_age_seconds") {
                $feedAge = $feed.latest_message_age_seconds
            }
            if (-not $feed.connected) {
                return [pscustomobject]@{
                    healthy = $false
                    reason = "$($feedProperty.Name) disconnected"
                }
            }
            if (($feed.PSObject.Properties.Name -contains "subscription_acknowledged") -and
                -not $feed.subscription_acknowledged) {
                return [pscustomobject]@{
                    healthy = $false
                    reason = "$($feedProperty.Name) subscriptions not acknowledged"
                }
            }
            if ($null -eq $feedAge -or
                $null -eq $staleAfter -or
                [double]$feedAge -lt 0 -or
                [double]$feedAge -gt [double]$staleAfter) {
                return [pscustomobject]@{
                    healthy = $false
                    reason = "$($feedProperty.Name) stale"
                }
            }
        }
        return [pscustomobject]@{ healthy = $true; reason = "" }
    } catch {
        return [pscustomobject]@{
            healthy = $false
            reason = "$($_.Exception.GetType().Name): $($_.Exception.Message)"
        }
    }
}

function Invoke-WatchdogAlarm {
    param([bool]$Critical)
    $repeat = if ($Critical) { 3 } else { 1 }
    for ($index = 0; $index -lt $repeat; $index++) {
        try {
            if ($Critical) {
                [System.Media.SystemSounds]::Hand.Play()
            } else {
                [System.Media.SystemSounds]::Exclamation.Play()
            }
        } catch {
            [console]::Beep($(if ($Critical) { 1400 } else { 900 }), 700)
        }
        if ($index + 1 -lt $repeat) {
            Start-Sleep -Milliseconds 700
        }
    }
}

function Write-SupervisorStatus {
    param(
        [object]$Primary,
        [object]$Shadow,
        [int]$PrimaryFailures,
        [int]$ShadowFailures,
        [datetime]$PrimaryArmedAt,
        [datetime]$ShadowArmedAt,
        [datetime]$LastAlarm
    )
    $statusDirectory = Split-Path -Parent $StatusPath
    New-Item -ItemType Directory -Path $statusDirectory -Force | Out-Null
    $payload = [ordered]@{
        timestamp_utc = (Get-Date).ToUniversalTime().ToString("o")
        supervisor_pid = $PID
        startup = "per-user-supervisor"
        primary = [ordered]@{
            healthy = [bool]$Primary.healthy
            reason = [string]$Primary.reason
            consecutive_failures = $PrimaryFailures
            watchdog_armed = (Get-Date) -ge $PrimaryArmedAt
            watchdog_arms_utc = $PrimaryArmedAt.ToUniversalTime().ToString("o")
        }
        shadow = [ordered]@{
            healthy = [bool]$Shadow.healthy
            reason = [string]$Shadow.reason
            consecutive_failures = $ShadowFailures
            watchdog_armed = (Get-Date) -ge $ShadowArmedAt
            watchdog_arms_utc = $ShadowArmedAt.ToUniversalTime().ToString("o")
        }
        last_alarm_utc = $(if ($LastAlarm -gt [datetime]::MinValue) {
            $LastAlarm.ToUniversalTime().ToString("o")
        } else {
            $null
        })
    }
    $temporary = "$StatusPath.$PID.tmp"
    $json = $payload | ConvertTo-Json -Depth 6 -Compress
    [System.IO.File]::WriteAllText(
        $temporary,
        $json,
        [System.Text.UTF8Encoding]::new($false)
    )
    Move-Item -LiteralPath $temporary -Destination $StatusPath -Force
}

function Get-ExistingProcess {
    param([string]$CommandMarker)
    $candidate = Get-CimInstance Win32_Process |
        Where-Object {
            $_.CommandLine -and
            $_.CommandLine.Contains($CommandMarker) -and
            $_.CommandLine.Contains($Root)
        } |
        Select-Object -First 1
    if ($candidate) {
        return Get-Process -Id $candidate.ProcessId -ErrorAction SilentlyContinue
    }
    return $null
}

function Start-NativePrimary {
    $existing = Get-ExistingProcess -CommandMarker "-m crypto_sentinel"
    if ($existing) {
        Write-SupervisorLog "Adopted existing native primary PID $($existing.Id)."
        return $existing
    }
    $process = Start-Process `
        -FilePath $Python `
        -ArgumentList @("-m", "crypto_sentinel", "--config", "config.yaml", "run") `
        -WorkingDirectory $Root `
        -WindowStyle Hidden `
        -PassThru
    Write-SupervisorLog "Started native audible primary PID $($process.Id)."
    return $process
}

function Start-SoakMonitor {
    $existing = Get-ExistingProcess -CommandMarker "soak_monitor.py"
    if ($existing) {
        Write-SupervisorLog "Adopted existing soak monitor PID $($existing.Id)."
        return $existing
    }
    $process = Start-Process `
        -FilePath $Python `
        -ArgumentList @(
            "-u",
            "scripts\soak_monitor.py",
            "--interval",
            "60",
            "--output",
            "data\soak.jsonl"
        ) `
        -WorkingDirectory $Root `
        -WindowStyle Hidden `
        -PassThru
    Write-SupervisorLog "Started soak monitor PID $($process.Id)."
    return $process
}

function Test-DockerShadow {
    $containerId = & docker compose ps --status running -q crypto-sentinel 2>$null
    return ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($containerId))
}

function Ensure-DockerShadow {
    if (Test-DockerShadow) {
        return $false
    }
    & powershell.exe `
        -NoProfile `
        -ExecutionPolicy Bypass `
        -WindowStyle Hidden `
        -File $DockerScript *> $null
    if ($LASTEXITCODE -ne 0 -or -not (Test-DockerShadow)) {
        throw "Docker shadow helper exited without a running container."
    }
    Write-SupervisorLog "Recovered the Docker shadow."
    return $true
}

$createdNew = $false
$mutex = [System.Threading.Mutex]::new(
    $true,
    "Local\CryptoSentinelFreeSupervisor",
    [ref]$createdNew
)
if (-not $createdNew) {
    exit 0
}

$native = $null
$soak = $null
$nextDockerCheck = [datetime]::MinValue
$nextHealthCheck = (Get-Date).AddSeconds(10)
$primaryFailures = 0
$shadowFailures = 0
$lastPrimaryHealthy = $null
$lastShadowHealthy = $null
$lastWatchdogAlarm = [datetime]::MinValue
$lastNativeExitAlarm = [datetime]::MinValue
$lastPrimaryHealthAlarm = [datetime]::MinValue
$lastShadowHealthAlarm = [datetime]::MinValue
$primaryWatchdogArmedAt = (Get-Date).AddSeconds($WatchdogWarmupSeconds)
$shadowWatchdogArmedAt = (Get-Date).AddSeconds($WatchdogWarmupSeconds)
$primaryToken = Read-DotEnvValue -Path (Join-Path $Root ".env") -Name "DASHBOARD_TOKEN"
$shadowToken = Read-DotEnvValue -Path (Join-Path $Root ".env.docker") -Name "DASHBOARD_TOKEN"

try {
    Write-SupervisorLog "Supervisor started."
    while ($true) {
        if ($null -ne $native -and $native.HasExited) {
            $nativeExitCode = "unknown"
            try {
                $nativeExitCode = [string]$native.ExitCode
            } catch {
                # An adopted process may disappear before Windows exposes its code.
            }
            Write-SupervisorLog (
                "Native primary exited unexpectedly: exit_code=$nativeExitCode."
            )
            # Native process loss is a distinct critical incident. A recent
            # Docker-shadow warning or endpoint-health alarm must never silence
            # this process-death alarm.
            $alarmDue = (Get-Date) -ge $lastNativeExitAlarm.AddSeconds($AlarmCooldownSeconds)
            if ($alarmDue) {
                Invoke-WatchdogAlarm -Critical $true
                $lastNativeExitAlarm = Get-Date
                $lastWatchdogAlarm = $lastNativeExitAlarm
                Write-SupervisorLog (
                    "Watchdog CRITICAL alarm: native primary process exited unexpectedly."
                )
            } else {
                Write-SupervisorLog (
                    "Native-exit CRITICAL alarm suppressed by watchdog cooldown."
                )
            }
            $native = $null
        }

        if ($null -eq $native) {
            try {
                $native = Start-NativePrimary
                $primaryFailures = 0
                $primaryWatchdogArmedAt = (Get-Date).AddSeconds($WatchdogWarmupSeconds)
            } catch {
                Write-SupervisorLog "Native primary start failed: $($_.Exception.Message)"
            }
        }

        if ($null -eq $soak -or $soak.HasExited) {
            try {
                $soak = Start-SoakMonitor
            } catch {
                Write-SupervisorLog "Soak monitor start failed: $($_.Exception.Message)"
            }
        }

        if ((Get-Date) -ge $nextDockerCheck) {
            try {
                $shadowRecovered = Ensure-DockerShadow
                if ($shadowRecovered) {
                    $shadowFailures = 0
                    $shadowWatchdogArmedAt = (Get-Date).AddSeconds($WatchdogWarmupSeconds)
                }
            } catch {
                Write-SupervisorLog "Docker shadow check failed: $($_.Exception.Message)"
            }
            $nextDockerCheck = (Get-Date).AddSeconds($DockerRetrySeconds)
        }

        if ((Get-Date) -ge $nextHealthCheck) {
            $primaryReadiness = Get-EndpointReadiness `
                -BaseUrl "http://127.0.0.1:8787" `
                -Token $primaryToken
            $shadowReadiness = Get-EndpointReadiness `
                -BaseUrl "http://127.0.0.1:8788" `
                -Token $shadowToken
            $primaryArmed = (Get-Date) -ge $primaryWatchdogArmedAt
            $shadowArmed = (Get-Date) -ge $shadowWatchdogArmedAt
            $primaryFailures = if ($primaryReadiness.healthy -or -not $primaryArmed) {
                0
            } else {
                $primaryFailures + 1
            }
            $shadowFailures = if ($shadowReadiness.healthy -or -not $shadowArmed) {
                0
            } else {
                $shadowFailures + 1
            }

            if ($null -ne $lastPrimaryHealthy -and
                $lastPrimaryHealthy -ne $primaryReadiness.healthy) {
                Write-SupervisorLog "Primary readiness changed to $($primaryReadiness.healthy): $($primaryReadiness.reason)"
            }
            if ($null -ne $lastShadowHealthy -and
                $lastShadowHealthy -ne $shadowReadiness.healthy) {
                Write-SupervisorLog "Shadow readiness changed to $($shadowReadiness.healthy): $($shadowReadiness.reason)"
            }
            $lastPrimaryHealthy = $primaryReadiness.healthy
            $lastShadowHealthy = $shadowReadiness.healthy

            $primaryAlarm = $primaryFailures -ge $FailuresBeforeAlarm
            $shadowAlarm = $shadowFailures -ge $FailuresBeforeAlarm
            $critical = $primaryAlarm
            $alarmClassLast = if ($critical) {
                $lastPrimaryHealthAlarm
            } else {
                $lastShadowHealthAlarm
            }
            $alarmDue = (Get-Date) -ge $alarmClassLast.AddSeconds($AlarmCooldownSeconds)
            if ($alarmDue -and ($primaryAlarm -or $shadowAlarm)) {
                Invoke-WatchdogAlarm -Critical $critical
                if ($critical) {
                    $lastPrimaryHealthAlarm = Get-Date
                    $lastWatchdogAlarm = $lastPrimaryHealthAlarm
                } else {
                    $lastShadowHealthAlarm = Get-Date
                    $lastWatchdogAlarm = $lastShadowHealthAlarm
                }
                Write-SupervisorLog (
                    "Watchdog alarm: primary=$($primaryReadiness.healthy) " +
                    "shadow=$($shadowReadiness.healthy) " +
                    "primary_reason=$($primaryReadiness.reason) " +
                    "shadow_reason=$($shadowReadiness.reason)"
                )
            }

            Write-SupervisorStatus `
                -Primary $primaryReadiness `
                -Shadow $shadowReadiness `
                -PrimaryFailures $primaryFailures `
                -ShadowFailures $shadowFailures `
                -PrimaryArmedAt $primaryWatchdogArmedAt `
                -ShadowArmedAt $shadowWatchdogArmedAt `
                -LastAlarm $lastWatchdogAlarm
            $nextHealthCheck = (Get-Date).AddSeconds($HealthCheckSeconds)
        }

        Start-Sleep -Seconds $CheckSeconds
    }
} finally {
    Write-SupervisorLog "Supervisor stopped."
    if ($createdNew) {
        $mutex.ReleaseMutex()
    }
    $mutex.Dispose()
}
