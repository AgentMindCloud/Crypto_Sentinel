[CmdletBinding()]
param(
    [int]$CheckSeconds = 10,
    [int]$DockerRetrySeconds = 60
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
$Python = Join-Path $Root ".venv\Scripts\python.exe"
$DockerScript = Join-Path $PSScriptRoot "run_docker_shadow.ps1"
$LogPath = Join-Path $Root "data\supervisor.log"
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
        return
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

try {
    Write-SupervisorLog "Supervisor started."
    while ($true) {
        if ($null -eq $native -or $native.HasExited) {
            try {
                $native = Start-NativePrimary
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
                Ensure-DockerShadow
            } catch {
                Write-SupervisorLog "Docker shadow check failed: $($_.Exception.Message)"
            }
            $nextDockerCheck = (Get-Date).AddSeconds($DockerRetrySeconds)
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
