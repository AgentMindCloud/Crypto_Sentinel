[CmdletBinding()]
param(
    [string]$Symbol = "MARKET",
    [ValidateSet("up", "down", "mixed")][string]$Direction = "mixed",
    [ValidateSet("warning", "critical")][string]$Severity = "warning",
    [string]$Title = "TabChart alert",
    [string]$Message = "TabChart triggered a configured alert.",
    [string]$DedupKey = "",
    [string]$Url = "http://127.0.0.1:8787/api/ingest",
    [string]$Token = $env:INGEST_TOKEN
)

$ErrorActionPreference = "Stop"
if (-not $Token) {
    $Root = Split-Path -Parent $PSScriptRoot
    $EnvFile = Join-Path $Root ".env"
    if (Test-Path $EnvFile) {
        $Line = Get-Content $EnvFile | Where-Object { $_ -match '^INGEST_TOKEN=' } | Select-Object -First 1
        if ($Line) { $Token = ($Line -split '=', 2)[1].Trim().Trim('"').Trim("'") }
    }
}
if (-not $Token) { throw "INGEST_TOKEN is required in .env, as a parameter, or as an environment variable." }
if (-not $DedupKey) { $DedupKey = "tabchart:${Symbol}:${Direction}:$Title" }

$Payload = @{
    source = "tabchart"
    severity = $Severity
    category = "tabchart_alert"
    symbol = $Symbol
    direction = $Direction
    title = $Title
    message = $Message
    dedup_key = $DedupKey
} | ConvertTo-Json -Compress

Invoke-RestMethod `
    -Method Post `
    -Uri $Url `
    -Headers @{ Authorization = "Bearer $Token" } `
    -ContentType "application/json" `
    -Body $Payload | Out-Null
