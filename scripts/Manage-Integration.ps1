[CmdletBinding()]
param([ValidateSet('status','open','start','connector')][string]$Action='status')
$ErrorActionPreference='Stop'
$env:PSModulePath=(Join-Path $PSHOME 'Modules')+';'+(Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\Modules')
$Root=Split-Path -Parent $PSScriptRoot
$Python=Join-Path $Root '.venv\Scripts\python.exe'
$Config=Join-Path $Root 'config.yaml'
# Isolated Python ignores ambient paths, user-site packages and caller CWD.
# Only the selected, integrity-checked source root is inserted by the fixed loader.
$env:CRYPTO_SENTINEL_SUPPRESS_BROWSER='1'
$lockName='Local\SentinelManagedOpen-'+([BitConverter]::ToString([Security.Cryptography.SHA256]::Create().ComputeHash([Text.Encoding]::UTF8.GetBytes($Root.ToLowerInvariant())))).Replace('-','').Substring(0,24)
$mutex=New-Object Threading.Mutex($false,$lockName)
$held=$false
$stage='lock'
function Get-Connection {
    $raw=& $Python -I -c 'import runpy,sys;sys.path.insert(0,sys.argv.pop(1));runpy.run_module(sys.argv.pop(1),run_name=sys.argv.pop(1))' (Join-Path $Root 'src') crypto_sentinel.connection __main__ connector --config $Config 2>$null
    if($LASTEXITCODE -ne 0){throw 'connection_unavailable'}
    $value=$raw | ConvertFrom-Json
    if($value.schemaVersion -ne 1 -or $value.origin -cne 'http://127.0.0.1:8787' -or $value.sourceRevision -cnotmatch '^[a-f0-9]{64}$' -or $value.readToken -cnotmatch '^[A-Za-z0-9_-]{43}$'){throw 'connection_invalid'}
    return $value
}
function Get-VerifiedStatus($connection){
    $random=New-Object byte[] 32
    $rng=[Security.Cryptography.RandomNumberGenerator]::Create();$rng.GetBytes($random);$rng.Dispose()
    $nonce=([BitConverter]::ToString($random)).Replace('-','').ToLowerInvariant()
    $identity=Invoke-RestMethod -Uri ($connection.origin+'/api/integration/identity?nonce='+$nonce) -TimeoutSec 3 -MaximumRedirection 0
    if($identity.application -cne 'crypto-sentinel-free' -or $identity.sourceRevision -cne $connection.sourceRevision -or $identity.instanceId -notmatch '^[a-f0-9-]{36}$'){throw 'instance_mismatch'}
    $key=[Text.Encoding]::UTF8.GetBytes($connection.readToken)
    $hmac=New-Object Security.Cryptography.HMACSHA256(,$key)
    $expected=([BitConverter]::ToString($hmac.ComputeHash([Text.Encoding]::UTF8.GetBytes($nonce+':'+$identity.sourceRevision+':'+$identity.instanceId)))).Replace('-','').ToLowerInvariant();$hmac.Dispose()
    if($identity.proof -cne $expected){throw 'instance_mismatch'}
    $status=Invoke-RestMethod -Uri ($connection.origin+'/api/integration/status') -Headers @{Authorization=('Bearer '+$connection.readToken)} -TimeoutSec 3 -MaximumRedirection 0
    if($status.sourceRevision -cne $connection.sourceRevision -or $status.instanceId -cne $identity.instanceId){throw 'instance_changed'}
    return $status
}
try{
    try{$held=$mutex.WaitOne(20000)}catch [Threading.AbandonedMutexException]{$held=$true}
    if(-not $held){throw 'workspace_busy'}
    $stage='integrity'
    . (Join-Path $PSScriptRoot 'Assert-ManagedIntegrity.ps1')
    Assert-ManagedIntegrity -SourceRoot $Root
    $stage='credential'
    $connection=Get-Connection
    $stage='identity'
    if($Action -eq 'connector'){
        # Only the local owner backend invokes this; never send this response to a browser.
        $connection | ConvertTo-Json -Compress
        exit 0
    }
    $status=$null
    try{$status=Get-VerifiedStatus $connection}catch{}
    if(-not $status -and $Action -in @('open','start')){
        $listener=Get-NetTCPConnection -LocalPort 8787 -State Listen -ErrorAction SilentlyContinue
        if($listener){throw 'port_conflict_or_unverified_instance'}
        # The existing native launcher owns detector execution. No Docker, provider or shell input is accepted.
        $launcher=Join-Path $PSScriptRoot 'run_windows.ps1'
        $running=Get-CimInstance Win32_Process | Where-Object {$_.CommandLine -and ($_.CommandLine.Contains($launcher) -or ($_.ExecutablePath -eq $Python -and $_.CommandLine -match 'crypto_sentinel.*run'))}
        if(-not $running){Start-Process -FilePath (Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe') -ArgumentList @('-NoProfile','-File',('"'+$launcher+'"')) -WorkingDirectory $Root -WindowStyle Hidden | Out-Null}
        for($attempt=0;$attempt -lt 15 -and -not $status;$attempt++){Start-Sleep -Milliseconds 750;try{$status=Get-VerifiedStatus $connection}catch{}}
        if(-not $status){throw 'startup_failed_or_incompatible'}
    }
    if(-not $status){@{schemaVersion=1;application='crypto-sentinel-free';status='unavailable';sourceRevision=$connection.sourceRevision;origin=$connection.origin;observedAt=[DateTime]::UtcNow.ToString('o')} | ConvertTo-Json -Compress;exit 0}
    if($Action -eq 'open'){Start-Process -FilePath ($connection.origin+'/') -WindowStyle Hidden | Out-Null}
    @{schemaVersion=1;application='crypto-sentinel-free';status='running';sourceRevision=$connection.sourceRevision;origin=$connection.origin;observedAt=[DateTime]::UtcNow.ToString('o');metadata=$status;opened=($Action -eq 'open')} | ConvertTo-Json -Depth 10 -Compress
}catch{
    @{schemaVersion=1;application='crypto-sentinel-free';status='unavailable';reason=('managed_'+$stage+'_unavailable');observedAt=[DateTime]::UtcNow.ToString('o')} | ConvertTo-Json -Compress
    exit 1
}

finally{if($held){$mutex.ReleaseMutex()};$mutex.Dispose()}
