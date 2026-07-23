[CmdletBinding()]
param(
    [string]$Config = "config.yaml"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    throw "Virtual environment not found. Run .\scripts\install_windows.ps1 first."
}
if (-not (Test-Path $Config)) {
    throw "Configuration not found: $Config"
}
& $Python -m crypto_sentinel --config $Config run
exit $LASTEXITCODE
