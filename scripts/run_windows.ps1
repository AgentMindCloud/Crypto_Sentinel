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
# Isolated bootstrap excludes caller CWD, user-site and ambient PYTHON* settings.
& $Python -I -c 'import runpy,sys;sys.path.insert(0,sys.argv.pop(1));runpy.run_module(sys.argv.pop(1),run_name=sys.argv.pop(1))' (Join-Path $Root 'src') crypto_sentinel __main__ --config $Config run
exit $LASTEXITCODE
