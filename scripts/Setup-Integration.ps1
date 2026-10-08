[CmdletBinding()]
param([Parameter(Mandatory=$true)][ValidatePattern('^[a-f0-9]{64}$')][string]$ReviewedSourceRevision,[ValidatePattern('^$|^[a-f0-9]{64}$')][string]$ExpectedPreviousRevision='',[switch]$EnableStartup)
$ErrorActionPreference='Stop'
$Root=Split-Path -Parent $PSScriptRoot
$lockName='Local\SentinelManagedOpen-'+([BitConverter]::ToString([Security.Cryptography.SHA256]::Create().ComputeHash([Text.Encoding]::UTF8.GetBytes($Root.ToLowerInvariant())))).Replace('-','').Substring(0,24)
$mutex=New-Object Threading.Mutex($false,$lockName)
$held=$false
try {
try{$held=$mutex.WaitOne(20000)}catch [Threading.AbandonedMutexException]{$held=$true}
if(-not $held){throw 'Managed lifecycle is busy'}
$Python=Join-Path $Root '.venv\Scripts\python.exe'
$Config=Join-Path $Root 'config.yaml'
$directory=Join-Path $Root 'data\ceos-connection'
if(Test-Path -LiteralPath $directory){if((Get-Item -LiteralPath $directory).Attributes -band [IO.FileAttributes]::ReparsePoint){throw 'Connection directory must not be a link'}}
New-Item -ItemType Directory -Path $directory -Force | Out-Null
$sid=[Security.Principal.WindowsIdentity]::GetCurrent().User
$acl=New-Object Security.AccessControl.DirectorySecurity
$acl.SetOwner($sid);$acl.SetAccessRuleProtection($true,$false)
$rule=New-Object Security.AccessControl.FileSystemAccessRule($sid,'FullControl','ContainerInherit,ObjectInherit','None','Allow')
$acl.AddAccessRule($rule)
$currentAcl=Get-Acl -LiteralPath $directory
$ownerSid=$currentAcl.GetOwner([Security.Principal.SecurityIdentifier])
$allow=@($currentAcl.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier]) | Where-Object {$_.AccessControlType -eq 'Allow'})
$alreadyPrivate=$currentAcl.AreAccessRulesProtected -and $ownerSid.Value -eq $sid.Value -and $allow.Count -eq 1 -and $allow[0].IdentityReference.Value -eq $sid.Value -and (($allow[0].FileSystemRights -band [Security.AccessControl.FileSystemRights]::FullControl) -eq [Security.AccessControl.FileSystemRights]::FullControl)
if(-not $alreadyPrivate){Set-Acl -LiteralPath $directory -AclObject $acl}
# Isolated Python ignores ambient paths, user-site packages and caller CWD.
# Only the selected, integrity-checked source root is inserted by the fixed loader.
$setupRaw=& $Python -I -c 'import runpy,sys;sys.path.insert(0,sys.argv.pop(1));runpy.run_module(sys.argv.pop(1),run_name=sys.argv.pop(1))' (Join-Path $Root 'src') crypto_sentinel.connection __main__ setup --config $Config --revision $ReviewedSourceRevision --previous-revision=$ExpectedPreviousRevision
if($LASTEXITCODE -ne 0){throw 'Managed connector setup failed'}
$setup=$setupRaw | ConvertFrom-Json
$files=@{}
$package=Join-Path $Root 'src/crypto_sentinel'
foreach($item in Get-ChildItem -LiteralPath $package -Recurse -File | Where-Object {$_.Extension -in @('.py','.html','.svg','.webmanifest')}){
    if($item.Attributes -band [IO.FileAttributes]::ReparsePoint){throw 'Linked package file rejected'}
    $relative=$item.FullName.Substring($Root.Length+1).Replace('\','/')
    $files[$relative]=(Get-FileHash -LiteralPath $item.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
}
foreach($relative in @('scripts/Manage-Integration.ps1','scripts/Assert-ManagedIntegrity.ps1','scripts/run_managed_alarm.ps1','scripts/run_windows.ps1','.venv/Scripts/python.exe')){
    $files[$relative]=(Get-FileHash -LiteralPath (Join-Path $Root $relative) -Algorithm SHA256).Hash.ToLowerInvariant()
}
$manifest=@{schemaVersion=1;sourceRevision=$ReviewedSourceRevision;files=$files}
$temp=Join-Path $directory ('manifest.'+[Guid]::NewGuid().ToString('N')+'.tmp')
[IO.File]::WriteAllText($temp,($manifest | ConvertTo-Json -Depth 4))
Move-Item -LiteralPath $temp -Destination (Join-Path $directory 'launcher-manifest.json') -Force
. (Join-Path $PSScriptRoot 'Assert-ManagedIntegrity.ps1')
Assert-ManagedIntegrity -SourceRoot $Root
if($EnableStartup){
    $startup=[Environment]::GetFolderPath('Startup')
    $legacy=Join-Path $startup 'Crypto Sentinel Free.lnk'
    if(Test-Path -LiteralPath $legacy){throw 'Existing legacy startup preserved; reconcile before enabling managed startup'}
    $tasks=@(Get-ScheduledTask -ErrorAction Stop | Where-Object {$_.TaskName -in @('Crypto Sentinel Free','Crypto Sentinel Docker Shadow','Crypto Sentinel Soak Monitor')})
    if($tasks.Count){throw 'Existing lifecycle tasks preserved; reconcile before enabling managed startup'}
    $shortcutPath=Join-Path $startup 'Crypto Sentinel Managed Alarm.lnk'
    $shell=New-Object -ComObject WScript.Shell
    $shortcut=$shell.CreateShortcut($shortcutPath)
    $guardian=Join-Path $PSScriptRoot 'run_managed_alarm.ps1'
    if((Test-Path -LiteralPath $shortcutPath) -and -not $shortcut.Arguments.Contains($guardian)){throw 'Different managed startup preserved'}
    $powershell=Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
    $shortcut.TargetPath=$powershell
    $shortcut.Arguments='-NoProfile -WindowStyle Hidden -File "'+$guardian+'"'
    $shortcut.WorkingDirectory=$Root;$shortcut.WindowStyle=7;$shortcut.Description='Independent native Sentinel alarms; no Docker or remote notifications added.';$shortcut.Save()
    Start-Process -FilePath $powershell -ArgumentList @('-NoProfile','-File',('"'+$guardian+'"')) -WorkingDirectory $Root -WindowStyle Hidden | Out-Null
}
@{configured=$true;origin=$setup.origin;sourceRevision=$setup.sourceRevision;reused=$setup.reused;managedStartupEnabled=[bool]$EnableStartup} | ConvertTo-Json -Compress
} finally {if($held){$mutex.ReleaseMutex()};$mutex.Dispose()}
