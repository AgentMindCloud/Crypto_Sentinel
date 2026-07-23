[CmdletBinding()]
param(
    [switch]$SkipPipUpgrade
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

function Invoke-PythonLauncher {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Arguments)
    if (Get-Command py -ErrorAction SilentlyContinue) {
        & py -3 @Arguments
    } elseif (Get-Command python -ErrorAction SilentlyContinue) {
        & python @Arguments
    } else {
        throw "Python 3.11 or newer was not found. Install Python, then rerun this script."
    }
    if ($LASTEXITCODE -ne 0) { throw "Python command failed with exit code $LASTEXITCODE" }
}

Invoke-PythonLauncher -Arguments @("-c", "import sys; assert sys.version_info >= (3, 11), sys.version")
if (-not (Test-Path ".venv\Scripts\python.exe")) {
    Invoke-PythonLauncher -Arguments @("-m", "venv", ".venv")
}

$VenvPython = Join-Path $Root ".venv\Scripts\python.exe"
if (-not $SkipPipUpgrade) {
    & $VenvPython -m pip install --upgrade pip
    if ($LASTEXITCODE -ne 0) { throw "pip upgrade failed" }
}
& $VenvPython -m pip install -e .
if ($LASTEXITCODE -ne 0) { throw "Project installation failed" }

if (-not (Test-Path "config.yaml")) { Copy-Item "config.example.yaml" "config.yaml" }
if (-not (Test-Path ".env")) {
    & $VenvPython "scripts\generate_tokens.py" | Set-Content -Encoding ascii ".env"
    if ($LASTEXITCODE -ne 0) { throw "Token generation failed" }
}
New-Item -ItemType Directory -Force -Path "data" | Out-Null

Write-Host ""
Write-Host "Installed Crypto Sentinel Free." -ForegroundColor Green
Write-Host "Next: .\scripts\run_windows.ps1"
Write-Host "Dashboard: http://127.0.0.1:8787/"
