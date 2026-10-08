function Assert-ManagedIntegrity {
    param([Parameter(Mandatory=$true)][string]$SourceRoot)
    $manifestPath=Join-Path $SourceRoot 'data\ceos-connection\launcher-manifest.json'
    $item=Get-Item -LiteralPath $manifestPath -ErrorAction Stop
    if($item.Length -gt 262144 -or ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)){throw 'Invalid managed manifest'}
    $manifest=Get-Content -Raw -LiteralPath $manifestPath | ConvertFrom-Json
    if($manifest.schemaVersion -ne 1 -or $manifest.sourceRevision -cnotmatch '^[a-f0-9]{64}$'){throw 'Incompatible managed manifest'}
    $files=@($manifest.files.PSObject.Properties)
    if($files.Count -lt 8 -or $files.Count -gt 300){throw 'Invalid manifest bounds'}
    foreach($entry in $files){
        if($entry.Name -cnotmatch '^(src/crypto_sentinel/[A-Za-z0-9_./-]+|scripts/(Manage-Integration|Assert-ManagedIntegrity|run_managed_alarm|run_windows)\.ps1|\.venv/Scripts/python\.exe)$' -or $entry.Name.Contains('..') -or $entry.Value -cnotmatch '^[a-f0-9]{64}$'){throw 'Unregistered manifest entry'}
        $target=Join-Path $SourceRoot $entry.Name
        $targetItem=Get-Item -LiteralPath $target -ErrorAction Stop
        if($targetItem.Attributes -band [IO.FileAttributes]::ReparsePoint){throw 'Linked managed file rejected'}
        if((Get-FileHash -LiteralPath $target -Algorithm SHA256).Hash.ToLowerInvariant() -cne $entry.Value){throw 'Managed source changed; verify and reselect candidate'}
    }
    foreach($required in @('scripts/Manage-Integration.ps1','scripts/Assert-ManagedIntegrity.ps1','scripts/run_managed_alarm.ps1','scripts/run_windows.ps1','.venv/Scripts/python.exe')){
        if($required -notin $files.Name){throw 'Incomplete managed manifest'}
    }
}
