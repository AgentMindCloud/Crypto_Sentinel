[CmdletBinding()]
param(
    [string]$TaskName = "Crypto Sentinel Free",
    [string]$DockerTaskName = "Crypto Sentinel Docker Shadow",
    [string]$SoakTaskName = "Crypto Sentinel Soak Monitor"
)
$ErrorActionPreference = "Stop"
foreach ($name in @($TaskName, $DockerTaskName, $SoakTaskName)) {
    if (Get-ScheduledTask -TaskName $name -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $name -Confirm:$false
        Write-Host "Removed scheduled task '$name'." -ForegroundColor Green
    }
}
$StartupDirectory = [Environment]::GetFolderPath("Startup")
$ShortcutPath = Join-Path $StartupDirectory "Crypto Sentinel Free.lnk"
if (Test-Path -LiteralPath $ShortcutPath) {
    Remove-Item -LiteralPath $ShortcutPath -Force
    Write-Host "Removed Startup shortcut '$ShortcutPath'." -ForegroundColor Green
}
