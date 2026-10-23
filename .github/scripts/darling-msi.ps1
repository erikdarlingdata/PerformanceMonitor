<#
.SYNOPSIS
  Runs ONE leg of the Darling MSI check (#6480) on an elevated hosted Windows runner and fails the leg, with a
  one-line reason, when the outcome is not the expected one.

.DESCRIPTION
  Called by .github/workflows/darling-msi.yml, one leg per job. The workflow has already published the payload (the
  finished Darling tree: service, viewer\, the scripts, pg-runtime.zip) and built the MSI from it. The package is
  installed and uninstalled ONLY on the runner this script runs on, never anywhere else.

  Legs:
    M1  A silent install of the MSI must leave the machine as install-darling.ps1 does. The script is the oracle: the
        leg first installs the same payload with install-darling.ps1 and takes a snapshot (service record, service
        security, the install root's, darling.json's and the store folder's security descriptors, the operator group,
        the firewall rules, the viewer shortcuts), removes that install, then installs the MSI with
        'msiexec /i ... /qn' and takes the same snapshot. The two must be equal. The service must be Running, the
        store must come up (PostgreSQL accepting connections, data directory initialised), and the MSI log must
        carry the scripts' own output (it is how an install that rolled back is diagnosed).
#>
param(
    [Parameter(Mandatory)][ValidateSet('M1')][string]$Leg,
    [Parameter(Mandatory)][string]$Workspace,
    [Parameter(Mandatory)][string]$Payload,
    [Parameter(Mandatory)][string]$Msi
)
$ErrorActionPreference = 'Continue'
$svc = 'PerformanceMonitor Darling'
$groupName = 'PerformanceMonitor Darling Operators'
$installRoot = 'C:\Program Files\PerformanceMonitorDarling'
$storeFolder = 'C:\ProgramData\PerformanceMonitorDarling'
$work = 'C:\darling-msi-work'
$clock = [System.Diagnostics.Stopwatch]::StartNew()
New-Item -ItemType Directory -Force -Path $work | Out-Null

function Say([string]$t) { Write-Host ''; Write-Host "##### [$([int]$clock.Elapsed.TotalSeconds)s] $t" }

function Fail-Leg([string]$reason) {
    Write-Host "::error title=$Leg::$reason"
    Write-Host "RESULT ${Leg}: FAIL - $reason"
    if ($env:GITHUB_STEP_SUMMARY) { "- $Leg - FAIL - $reason" | Out-File -FilePath $env:GITHUB_STEP_SUMMARY -Append -Encoding utf8 }
    Show-ServiceLogTail
    exit 1
}

function Show-ServiceLogTail {
    Get-ChildItem (Join-Path $storeFolder 'logs') -ErrorAction SilentlyContinue | Select-Object -Last 1 | ForEach-Object { Write-Host "----- tail of $($_.FullName)"; Get-Content $_.FullName -Tail 40 }
}

# Runs a script in Windows PowerShell 5.1 (the engine an operator's elevated prompt starts, and the one the MSI's custom
# actions use) and returns its exit code and stdout. A child started from pwsh inherits pwsh's PSModulePath and then
# cannot load Get-Acl, so it is given the machine value.
function Run-Script([string]$script, [string[]]$scriptArgs, [string]$tag, [int]$timeoutSec = 900) {
    $out = Join-Path $work "$tag.out.txt"; $err = Join-Path $work "$tag.err.txt"
    $argList = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', "`"$script`"") + $scriptArgs
    Write-Host "RUN powershell.exe $($argList -join ' ')"
    $savedModulePath = $env:PSModulePath
    $env:PSModulePath = [Environment]::GetEnvironmentVariable('PSModulePath', 'Machine')
    $p = Start-Process -FilePath 'powershell.exe' -ArgumentList $argList -RedirectStandardOutput $out -RedirectStandardError $err -PassThru -NoNewWindow
    $null = $p.Handle
    $env:PSModulePath = $savedModulePath
    if (-not $p.WaitForExit($timeoutSec * 1000)) { try { $p.Kill() } catch { }; $code = 'TIMEOUT' }
    else { $p.WaitForExit(); $code = [string]$p.ExitCode }
    Write-Host "----- stdout of $tag (exit code: $code) -----"
    if (Test-Path $out) { Get-Content $out -Tail 60 | ForEach-Object { Write-Host $_ } }
    if ((Test-Path $err) -and (Get-Item $err).Length -gt 0) { Write-Host "----- stderr of $tag -----"; Get-Content $err -TotalCount 30 | ForEach-Object { Write-Host $_ } }
    return [string]$code
}

# The service Running and the store up: PostgreSQL accepting connections on the managed port and its data directory
# initialised. The first start extracts pg-runtime.zip, runs initdb and migrates, which measured minutes on a runner.
function Wait-StoreUp([string]$label, [int]$minutes = 12) {
    $deadline = (Get-Date).AddMinutes($minutes)
    $dataDir = Join-Path $storeFolder 'pg'
    while ((Get-Date) -lt $deadline) {
        $s = Get-Service -Name $svc -ErrorAction SilentlyContinue
        if (-not $s) { Fail-Leg "$label`: the service '$svc' does not exist" }
        if ($s.Status -eq 'Stopped') { Fail-Leg "$label`: the service stopped by itself while the store was coming up" }
        $listening = $false
        if ($s.Status -eq 'Running' -and (Test-Path -LiteralPath (Join-Path $dataDir 'PG_VERSION'))) {
            $c = New-Object System.Net.Sockets.TcpClient
            try { $c.Connect('127.0.0.1', 5641); $listening = $c.Connected } catch { } finally { $c.Dispose() }
        }
        if ($listening) { Write-Host "$label`: service Running, store accepting connections on 5641"; return }
        Start-Sleep -Seconds 5
    }
    Fail-Leg "$label`: the store did not come up within $minutes minutes"
}

function Remove-Tree([string]$path) {
    if (-not (Test-Path -LiteralPath $path)) { return }
    Remove-Item -LiteralPath $path -Recurse -Force -ErrorAction SilentlyContinue
    if (Test-Path -LiteralPath $path) {
        takeown.exe /f $path /r /d y 2>&1 | Out-Null
        icacls.exe $path /grant 'Administrators:(OI)(CI)F' /t /c /q 2>&1 | Out-Null
        Remove-Item -LiteralPath $path -Recurse -Force -ErrorAction SilentlyContinue
    }
    if (Test-Path -LiteralPath $path) { Fail-Leg "could not remove $path" }
}

function Get-Sddl([string]$path) {
    if (-not (Test-Path -LiteralPath $path)) { return "(missing: $path)" }
    return (Get-Acl -LiteralPath $path).Sddl
}

# Everything the leg compares, as named sections of text lines.
function Get-Snapshot {
    $snap = [ordered]@{}
    $snap['install root SDDL'] = @(Get-Sddl $installRoot)
    $snap['darling.json SDDL'] = @(Get-Sddl (Join-Path $installRoot 'darling.json'))
    $snap['store folder SDDL'] = @(Get-Sddl $storeFolder)
    $sid = try { (New-Object System.Security.Principal.NTAccount($groupName)).Translate([System.Security.Principal.SecurityIdentifier]).Value } catch { '(no such group)' }
    $snap['operator group'] = @("sid $sid") + @(& net.exe localgroup $groupName 2>&1 | ForEach-Object { "$_".Trim() } | Where-Object { $_ })
    $rules = foreach ($r in @(Get-NetFirewallRule -DisplayName 'PerformanceMonitor Darling *' -ErrorAction SilentlyContinue | Sort-Object DisplayName)) {
        $app = $r | Get-NetFirewallApplicationFilter; $port = $r | Get-NetFirewallPortFilter; $addr = $r | Get-NetFirewallAddressFilter
        "$($r.DisplayName)|$($r.Direction)|$($r.Action)|$($r.Enabled)|$($r.Profile)|$($app.Program)|$($port.Protocol)|$($port.LocalPort)|$($addr.RemoteAddress)"
    }
    $snap['firewall rules'] = @($rules)
    foreach ($verb in 'qc', 'sdshow', 'qdescription', 'qfailure') {
        $snap["sc $verb"] = @(& sc.exe $verb $svc 2>&1 | ForEach-Object { "$_".Trim() } | Where-Object { $_ })
    }
    $shell = New-Object -ComObject WScript.Shell
    $links = foreach ($folder in 'CommonDesktopDirectory', 'CommonPrograms') {
        $lnk = Join-Path ([Environment]::GetFolderPath($folder)) 'Darling Viewer.lnk'
        if (Test-Path -LiteralPath $lnk) { $l = $shell.CreateShortcut($lnk); "$folder|$($l.TargetPath)|$($l.WorkingDirectory)|$($l.Description)" } else { "$folder|(missing)" }
    }
    $snap['shortcuts'] = @($links)
    return $snap
}

function Save-Snapshot($snap, [string]$name) {
    $lines = foreach ($k in $snap.Keys) { "== $k"; $snap[$k] }
    $lines | Set-Content -LiteralPath (Join-Path $work $name) -Encoding utf8
}

function Get-Marker {
    $v = Get-ItemProperty -Path 'HKLM:\SOFTWARE\PerformanceMonitor Darling' -ErrorAction SilentlyContinue
    return $v
}

if (-not [Security.Principal.WindowsPrincipal]([Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { Fail-Leg 'the runner is not elevated' }
if (Get-Service -Name $svc -ErrorAction SilentlyContinue) { Fail-Leg "the runner already has a '$svc' service, so the leg would not start from a clean machine" }
foreach ($p in $Payload, $Msi) { if (-not (Test-Path -LiteralPath $p)) { Fail-Leg "missing input: $p" } }
if (-not (Test-Path -LiteralPath (Join-Path $Payload 'pg-runtime.zip'))) { Fail-Leg "the payload has no pg-runtime.zip, so the store could not come up" }

# ---------------------------------------------------------------------------------------------------------------------
Say 'ORACLE: install the payload with install-darling.ps1'
New-Item -ItemType Directory -Force -Path $installRoot | Out-Null
Copy-Item -Path (Join-Path $Payload '*') -Destination $installRoot -Recurse -Force
$code = Run-Script (Join-Path $installRoot 'install-darling.ps1') @('-NoOperatorPrompt') 'script-install' 900
if ($code -ne '0') { Fail-Leg "install-darling.ps1 exited with $code" }
Wait-StoreUp 'script install'
$scriptSnap = Get-Snapshot
Save-Snapshot $scriptSnap 'snapshot-script.txt'
if ($null -ne (Get-Marker)) { Fail-Leg 'a script install wrote the MSI marker' }

Say 'ORACLE: remove the script install (uninstall-darling.ps1 keeps the store and darling.json; the leg deletes both, the group stays)'
$code = Run-Script (Join-Path $installRoot 'uninstall-darling.ps1') @() 'script-uninstall' 600
if ($code -ne '0') { Fail-Leg "uninstall-darling.ps1 exited with $code" }
if (Get-Service -Name $svc -ErrorAction SilentlyContinue) { Fail-Leg 'the service is still registered after uninstall-darling.ps1' }
Remove-Tree $installRoot
Remove-Tree $storeFolder

# ---------------------------------------------------------------------------------------------------------------------
Say 'MSI: silent install'
$log = Join-Path $work 'msi.log'
$sw = [System.Diagnostics.Stopwatch]::StartNew()
$p = Start-Process -FilePath 'msiexec.exe' -ArgumentList @('/i', "`"$Msi`"", '/qn', '/norestart', '/l*v', "`"$log`"") -PassThru -NoNewWindow
$null = $p.Handle
if (-not $p.WaitForExit(20 * 60 * 1000)) { try { $p.Kill() } catch { }; Fail-Leg 'msiexec /i did not finish within 20 minutes' }
$p.WaitForExit()
Write-Host "msiexec /i exit code $($p.ExitCode) after $([int]$sw.Elapsed.TotalSeconds)s"
if ($p.ExitCode -ne 0 -and $p.ExitCode -ne 3010) {
    Select-String -LiteralPath $log -Pattern 'Return value 3|Error \d+\.|install-darling|WixQuietExec' | Select-Object -Last 25 | ForEach-Object { Write-Host $_.Line }
    Fail-Leg "msiexec /i exited with $($p.ExitCode)"
}
Wait-StoreUp 'MSI install'

$marker = Get-Marker
if (-not $marker -or $marker.InstalledBy -ne 'msi') { Fail-Leg 'the MSI install did not write HKLM\SOFTWARE\PerformanceMonitor Darling InstalledBy=msi' }
if (-not (Select-String -LiteralPath $log -SimpleMatch -Pattern 'PreStart done' -Quiet)) { Fail-Leg 'the MSI log does not carry the PreStart script output (WixQuietExec logging)' }
$msiSnap = Get-Snapshot
Save-Snapshot $msiSnap 'snapshot-msi.txt'

# ---------------------------------------------------------------------------------------------------------------------
Say 'COMPARE: the MSI install against the script install'
$bad = @()
foreach ($k in $scriptSnap.Keys) {
    $d = Compare-Object -ReferenceObject (@("(section)") + @($scriptSnap[$k])) -DifferenceObject (@("(section)") + @($msiSnap[$k]))
    if ($d) {
        $bad += $k
        Write-Host "DIFFERENT: $k   (<= script install, => MSI install)"
        $d | ForEach-Object { Write-Host "  $($_.SideIndicator) $($_.InputObject)" }
    }
    else { Write-Host "same: $k ($(@($scriptSnap[$k]).Count) lines)" }
}
if ($bad.Count -gt 0) { Fail-Leg "the MSI install differs from the script install in: $($bad -join ', ')" }

Write-Host "RESULT ${Leg}: PASS"
if ($env:GITHUB_STEP_SUMMARY) { "- $Leg - PASS ($([int]$clock.Elapsed.TotalSeconds)s)" | Out-File -FilePath $env:GITHUB_STEP_SUMMARY -Append -Encoding utf8 }
exit 0
