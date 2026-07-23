[CmdletBinding()]
param(
    [string]$Url = "http://127.0.0.1:8787/api/ingest",
    [Parameter(Mandatory = $true)][string]$Token,
    [ValidateSet("warning", "critical")][string]$Severity = "critical"
)

$Headers = @{ Authorization = "Bearer $Token" }
$Body = @{
    source = "powershell-test"
    severity = $Severity
    category = "external_test"
    symbol = "BTCUSDT"
    direction = "down"
    title = "External webhook test"
    message = "This is a test of authenticated external alert ingestion. No market event occurred."
    dedup_key = "external-test:BTCUSDT:$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())"
} | ConvertTo-Json

Invoke-RestMethod -Method Post -Uri $Url -Headers $Headers -ContentType "application/json" -Body $Body
