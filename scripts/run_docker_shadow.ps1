[CmdletBinding()]
param(
    [int]$DockerWaitSeconds = 120
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

foreach ($required in @("config.docker.yaml", ".env.docker")) {
    if (-not (Test-Path -LiteralPath $required)) {
        throw "Required Docker shadow file is missing: $required"
    }
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "Docker CLI was not found."
}

& docker info *> $null
if ($LASTEXITCODE -ne 0) {
    & docker desktop start | Out-Null
    $deadline = (Get-Date).AddSeconds($DockerWaitSeconds)
    do {
        Start-Sleep -Seconds 3
        & docker info *> $null
        if ($LASTEXITCODE -eq 0) { break }
    } while ((Get-Date) -lt $deadline)
}

& docker info *> $null
if ($LASTEXITCODE -ne 0) {
    throw "Docker Desktop did not become ready within $DockerWaitSeconds seconds."
}

& docker compose up -d --no-build
if ($LASTEXITCODE -ne 0) {
    throw "Docker shadow startup failed with exit code $LASTEXITCODE."
}

Write-Host "Crypto Sentinel Docker shadow is running on http://127.0.0.1:8788/." -ForegroundColor Green
