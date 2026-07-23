[CmdletBinding()]
param(
    [int]$IntervalSeconds = 60
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $Python)) {
    throw "Virtual environment not found. Run .\scripts\install_windows.ps1 first."
}

& $Python -u ".\scripts\soak_monitor.py" `
    --interval $IntervalSeconds `
    --output ".\data\soak.jsonl"
exit $LASTEXITCODE
