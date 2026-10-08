[CmdletBinding()]
param()
$ErrorActionPreference='Stop'
$env:PSModulePath=(Join-Path $PSHOME 'Modules')+';'+(Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\Modules')
$Root=Split-Path -Parent $PSScriptRoot
$directory=Join-Path $Root 'data\ceos-connection'
$helper=Join-Path $PSScriptRoot 'Manage-Integration.ps1'
$shell=Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
$digest=([BitConverter]::ToString([Security.Cryptography.SHA256]::Create().ComputeHash([Text.Encoding]::UTF8.GetBytes($Root.ToLowerInvariant())))).Replace('-','').Substring(0,24)
$mutex=New-Object Threading.Mutex($false,('Local\SentinelManagedGuardian-'+$digest))
$held=$false
try{
    try{$held=$mutex.WaitOne(0)}catch [Threading.AbandonedMutexException]{$held=$true}
    if(-not $held){exit 0}
    $failures=0
    while($true){
        $state='unavailable';$reason='verification_or_start_failed';$instance=$null;$failureClass=$null;$failureLine=$null;$failureCommand=$null
        try{
            . (Join-Path $PSScriptRoot 'Assert-ManagedIntegrity.ps1')
            Assert-ManagedIntegrity -SourceRoot $Root
            $raw=& $shell -NoProfile -File $helper -Action start 2>$null
            $status=$raw | ConvertFrom-Json
            if($LASTEXITCODE -ne 0 -or $status.status -ne 'running'){throw 'primary_unavailable'}
            $instance=$status.metadata.instanceId
            $state=if($status.metadata.detector.ready){'running'}else{'degraded'}
            $reason=if($state -eq 'running'){'primary_verified'}else{'detector_warming_or_unavailable'}
            $failures=0
        }catch{$failures=[Math]::Min($failures+1,20);$failureClass=$_.Exception.GetType().Name;$failureLine=$_.InvocationInfo.ScriptLineNumber;if($_.Exception.CommandName -match '^[A-Za-z0-9-]{1,80}$'){$failureCommand=$_.Exception.CommandName}}
        $delay=if($failures -eq 0){30}else{[Math]::Min(60,5*[Math]::Pow(2,[Math]::Min($failures-1,4)))}
        try{
            $receipt=@{schemaVersion=1;application='crypto-sentinel-native-guardian';observedAt=[DateTime]::UtcNow.ToString('o');state=$state;reason=$reason;instanceId=$instance;consecutiveFailures=$failures;nextCheckSeconds=$delay;requiresAwakePc=$true;failureClass=$failureClass;failureLine=$failureLine;failureCommand=$failureCommand}
            $target=Join-Path $directory 'guardian-status.json'
            $temp=Join-Path $directory ('guardian-status.'+$PID+'.tmp')
            [IO.File]::WriteAllText($temp,($receipt | ConvertTo-Json -Compress))
            Move-Item -LiteralPath $temp -Destination $target -Force
        }catch{}
        Start-Sleep -Seconds $delay
    }
}finally{if($held){$mutex.ReleaseMutex()};$mutex.Dispose()}
