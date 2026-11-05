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
    M2-M3  The older release's script install (the 3.10.0 zip) is not taken over: the MSI is refused with 1603 and the
        machine snapshot, darling.json and the Running service are unchanged (M2). The script's own uninstall, then the
        MSI, installs over the kept folder and store, and darling.json is byte-identical and the store is the same one
        (M3).
    M4-M6  A major upgrade from one MSI to a newer one (built on the runner with a higher version) keeps the service
        and darling.json, finds the install folder from the InstallFolder registry value and ignores an INSTALLFOLDER=
        given on the upgrade (M4). An uninstall keeps the store and darling.json and the uninstall script runs to its
        end (M5). An install and an uninstall with PURGEDATA=1 removes the store (M6).
    M7  An install the script refuses (a folder under a user profile) fails in the PreStart custom action and rolls
        back clean: no service, no marker, no files, no group, no firewall rules, no store.
    M8  An install into a folder BUILTIN\Users can modify is stopped by the folder check before install-darling.ps1
        runs (1603, the check's message in the log, PreStart never executed) and rolls back clean. M4 also repairs
        as an administrator, which runs the check and both script phases again.
#>
param(
    [Parameter(Mandatory)][ValidateSet('M1', 'M2-M3', 'M4-M6', 'M7', 'M8')][string]$Leg,
    [Parameter(Mandatory)][string]$Workspace,
    [Parameter(Mandatory)][string]$Payload,
    [Parameter(Mandatory)][string]$Msi,
    # M4-M6: the second, newer MSI (built from the same payload with a higher version).
    [string]$MsiNew,
    # M2-M3: the older release whose script install the MSI must refuse to take over.
    [string]$OldRelease = '3.10.0'
)
$ErrorActionPreference = 'Continue'
$svc = 'PerformanceMonitor Darling'
$groupName = 'PerformanceMonitor Darling Operators'
$installRoot = 'C:\Program Files\PerformanceMonitorDarling'
$storeFolder = 'C:\ProgramData\PerformanceMonitorDarling'
$work = 'C:\darling-msi-work'
$repo = if ($env:GITHUB_REPOSITORY) { $env:GITHUB_REPOSITORY } else { 'erikdarlingdata/PerformanceMonitor' }
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

# The security descriptor's primary-group field is the group of whoever created the object: the script install runs as the
# runner's administrator (primary group None) and the MSI's actions run as SYSTEM. Nothing authorizes through it, so it is
# masked; the owner and every DACL entry are compared.
function Get-Sddl([string]$path) {
    if (-not (Test-Path -LiteralPath $path)) { return "(missing: $path)" }
    return ((Get-Acl -LiteralPath $path).Sddl -replace 'G:.*?(?=D:)', 'G:(not compared)')
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
        if (Test-Path -LiteralPath $lnk) { $l = $shell.CreateShortcut($lnk); "$folder|$($l.TargetPath)|$($l.WorkingDirectory.TrimEnd('\'))|$($l.Description)" } else { "$folder|(missing)" }
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

# Compares two snapshots section by section and fails the leg on any difference.
function Compare-Snapshots($ref, $diff, [string]$what) {
    $bad = @()
    foreach ($k in $ref.Keys) {
        $d = Compare-Object -ReferenceObject (@("(section)") + @($ref[$k])) -DifferenceObject (@("(section)") + @($diff[$k]))
        if ($d) {
            $bad += $k
            Write-Host "DIFFERENT: $k   (<= before, => after)"
            $d | ForEach-Object { Write-Host "  $($_.SideIndicator) $($_.InputObject)" }
        }
        else { Write-Host "same: $k ($(@($ref[$k]).Count) lines)" }
    }
    if ($bad.Count -gt 0) { Fail-Leg "$what differs in: $($bad -join ', ')" }
}

# Runs msiexec, logging verbosely to <work>\<tag>.log, and returns its exit code. Each property is passed as NAME="value".
function Invoke-Msi([string]$verb, [string]$msi, [string[]]$props, [string]$tag, [int]$timeoutMin = 20) {
    $log = Join-Path $work "$tag.log"
    $argList = @($verb, "`"$msi`"", '/qn', '/norestart', '/l*v', "`"$log`"") + $props
    Write-Host "RUN msiexec.exe $($argList -join ' ')"
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $p = Start-Process -FilePath 'msiexec.exe' -ArgumentList $argList -PassThru -NoNewWindow
    $null = $p.Handle
    if (-not $p.WaitForExit($timeoutMin * 60 * 1000)) { try { $p.Kill() } catch { }; Fail-Leg "msiexec $verb ($tag) did not finish within $timeoutMin minutes" }
    $p.WaitForExit()
    Write-Host "msiexec $verb ($tag) exit code $($p.ExitCode) after $([int]$sw.Elapsed.TotalSeconds)s"
    return [int]$p.ExitCode
}

function Show-MsiLogErrors([string]$tag) {
    Select-String -LiteralPath (Join-Path $work "$tag.log") -Pattern 'Return value 3|Error \d+\.|install-darling|uninstall-darling|WixQuietExec|Launch condition' | Select-Object -Last 25 | ForEach-Object { Write-Host $_.Line }
}

function Test-LogHas([string]$tag, [string]$text) {
    return [bool](Select-String -LiteralPath (Join-Path $work "$tag.log") -SimpleMatch -Pattern $text -Quiet)
}

function Assert-MsiOk([int]$code, [string]$what, [string]$tag) {
    if ($code -ne 0 -and $code -ne 3010) { Show-MsiLogErrors $tag; Fail-Leg "$what exited with $code" }
}

# The Add/Remove Programs record of THIS package: its DisplayVersion, or $null.
function Get-ArpVersion {
    $k = Get-ChildItem 'HKLM:\SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall' -ErrorAction SilentlyContinue |
        Get-ItemProperty -ErrorAction SilentlyContinue | Where-Object { $_.DisplayName -eq 'PerformanceMonitor Darling' } | Select-Object -First 1
    if ($k) { return [string]$k.DisplayVersion } else { return $null }
}

function Get-ServicePath { return (@(& sc.exe qc $svc 2>&1 | Where-Object { $_ -match 'BINARY_PATH_NAME' }) -join '') }

function Get-FileSha([string]$path) { if (Test-Path -LiteralPath $path) { return (Get-FileHash -LiteralPath $path -Algorithm SHA256).Hash } else { return '(missing)' } }

# A release zip is run elevated, so its SHA-256 is compared with the release's own SHA256SUMS.txt before anything is
# extracted (the same check darling-upgrade-shapes.ps1 makes).
function Get-ReleaseZip([string]$version) {
    $zip = Join-Path $work "PerformanceMonitorDarling-$version.zip"
    if (-not (Test-Path $zip)) {
        gh release download "v$version" -R $repo -p "PerformanceMonitorDarling-$version.zip" -D $work --clobber
        if ($LASTEXITCODE -ne 0) { Fail-Leg "could not download the $version release zip" }
    }
    $sums = Join-Path $work "SHA256SUMS-$version.txt"
    if (-not (Test-Path $sums)) {
        gh release download "v$version" -R $repo -p 'SHA256SUMS.txt' -O $sums --clobber
        if ($LASTEXITCODE -ne 0) { Fail-Leg "the $version release ships no SHA256SUMS.txt to check its zip against" }
    }
    $name = Split-Path -Leaf $zip
    $expected = @()
    foreach ($line in (Get-Content -LiteralPath $sums)) {
        if ($line -match '^\s*([0-9a-fA-F]{64})\s+\*?(\S+)\s*$' -and $Matches[2] -eq $name) { $expected += $Matches[1] }
    }
    if ($expected.Count -ne 1) { Fail-Leg "SHA256SUMS.txt of the $version release lists $name $($expected.Count) times, expected once" }
    $actual = (Get-FileHash -LiteralPath $zip -Algorithm SHA256).Hash
    if ($actual -ne $expected[0]) { Remove-Item -LiteralPath $zip -Force -ErrorAction SilentlyContinue; Fail-Leg "$name does not match the SHA-256 in the $version release's SHA256SUMS.txt" }
    Write-Host "checksum ok: $name $actual"
    return $zip
}

if (-not [Security.Principal.WindowsPrincipal]([Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) { Fail-Leg 'the runner is not elevated' }
if (Get-Service -Name $svc -ErrorAction SilentlyContinue) { Fail-Leg "the runner already has a '$svc' service, so the leg would not start from a clean machine" }
foreach ($p in $Payload, $Msi) { if (-not (Test-Path -LiteralPath $p)) { Fail-Leg "missing input: $p" } }
if (-not (Test-Path -LiteralPath (Join-Path $Payload 'pg-runtime.zip'))) { Fail-Leg "the payload has no pg-runtime.zip, so the store could not come up" }

if ($Leg -eq 'M1') {
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
    if (-not (Select-String -LiteralPath $log -SimpleMatch -Pattern 'Install folder check passed' -Quiet)) { Fail-Leg 'the MSI log does not show the install folder check passing before PreStart' }
    $msiSnap = Get-Snapshot
    Save-Snapshot $msiSnap 'snapshot-msi.txt'

    # ---------------------------------------------------------------------------------------------------------------------
    Say 'COMPARE: the MSI install against the script install'
    Compare-Snapshots $scriptSnap $msiSnap 'the MSI install (against the script install)'
}

# ---------------------------------------------------------------------------------------------------------------------
if ($Leg -eq 'M2-M3') {
    Say "BASE: the $OldRelease release installed with its own install-darling.ps1 (the script install the MSI must not take over)"
    $zip = Get-ReleaseZip $OldRelease
    New-Item -ItemType Directory -Force -Path $installRoot | Out-Null
    tar -xf $zip -C $installRoot
    if ($LASTEXITCODE -ne 0) { Fail-Leg "could not extract $zip" }
    $cfg = '{ "postgres": { "managed": true, "port": 5641, "dataDirectory": null, "connectAs": "admin" }, "servers": [ { "name": "nosuch", "host": "nosuch.invalid", "auth": "integrated" } ] }'
    $configPath = Join-Path $installRoot 'darling.json'
    Set-Content -LiteralPath $configPath -Value $cfg
    $baseScript = Join-Path $installRoot 'install-darling.ps1'
    $declared = @(Select-String -LiteralPath $baseScript -Pattern '^\s*\[switch\]\$(\w+)' | ForEach-Object { $_.Matches[0].Groups[1].Value })
    $a = @(); foreach ($w in 'SkipPreflight', 'NoShortcuts', 'NoOperatorPrompt') { if ($declared -contains $w) { $a += "-$w" } }
    $code = Run-Script $baseScript $a 'base-install' 900
    if ($code -ne '0') { Fail-Leg "the $OldRelease install-darling.ps1 exited with $code" }
    Wait-StoreUp "$OldRelease script install"
    $pgVersion = Join-Path $storeFolder 'pg\PG_VERSION'
    $storeBorn = (Get-Item -LiteralPath $pgVersion).CreationTimeUtc
    $cfgSha = Get-FileSha $configPath
    $before = Get-Snapshot
    Save-Snapshot $before 'snapshot-m2-before.txt'

    # M2: refused, and nothing changed.
    Say 'M2: the MSI over the script install is refused with 1603 and changes nothing'
    $code = Invoke-Msi '/i' $Msi @() 'm2-refused'
    if ($code -ne 1603) { Show-MsiLogErrors 'm2-refused'; Fail-Leg "M2: msiexec /i over a script install exited with $code, expected 1603" }
    if (-not (Test-LogHas 'm2-refused' 'LaunchConditions. Return value 3')) { Fail-Leg 'M2: the refusal did not come from the LaunchConditions action' }
    if ($null -ne (Get-Marker)) { Fail-Leg 'M2: the refused install wrote the MSI marker' }
    if ($null -ne (Get-ArpVersion)) { Fail-Leg 'M2: the refused install left an Add/Remove Programs record' }
    if ((Get-Service -Name $svc).Status -ne 'Running') { Fail-Leg 'M2: the script-installed service is no longer Running' }
    if ((Get-FileSha $configPath) -ne $cfgSha) { Fail-Leg 'M2: darling.json changed' }
    $after = Get-Snapshot
    Save-Snapshot $after 'snapshot-m2-after.txt'
    Compare-Snapshots $before $after 'M2: the machine after the refused MSI'
    Write-Host 'M2: PASS'

    # M3: the script's own uninstall, then the MSI over the kept folder and store.
    Say 'M3: uninstall-darling.ps1, then the MSI installs; darling.json is byte-identical and the store is the same one'
    $code = Run-Script (Join-Path $installRoot 'uninstall-darling.ps1') @() 'old-uninstall' 600
    if ($code -ne '0') { Fail-Leg "M3: the $OldRelease uninstall-darling.ps1 exited with $code" }
    if (Get-Service -Name $svc -ErrorAction SilentlyContinue) { Fail-Leg 'M3: the service is still registered after uninstall-darling.ps1' }
    if (-not (Test-Path -LiteralPath $pgVersion) -or (Get-FileSha $configPath) -ne $cfgSha) { Fail-Leg 'M3: the script uninstall did not keep the store and darling.json' }
    $code = Invoke-Msi '/i' $Msi @() 'm3-install'
    Assert-MsiOk $code 'M3: msiexec /i over the kept install' 'm3-install'
    Wait-StoreUp 'M3 MSI install'
    $marker = Get-Marker
    if (-not $marker -or $marker.InstalledBy -ne 'msi') { Fail-Leg 'M3: the MSI install did not write the InstalledBy=msi marker' }
    if ((Get-FileSha $configPath) -ne $cfgSha) { Fail-Leg 'M3: darling.json is not byte-identical after the MSI install' }
    if ((Get-Item -LiteralPath $pgVersion).CreationTimeUtc -ne $storeBorn) { Fail-Leg 'M3: the store was re-created, not kept' }
    if ((Get-FileSha (Join-Path $installRoot 'install-darling.ps1')) -ne (Get-FileSha (Join-Path $Payload 'install-darling.ps1'))) { Fail-Leg 'M3: install-darling.ps1 in the install folder is not the MSI''s (the older release''s file was kept)' }
    Write-Host 'M3: PASS'
}

# ---------------------------------------------------------------------------------------------------------------------
if ($Leg -eq 'M4-M6') {
    if (-not $MsiNew -or -not (Test-Path -LiteralPath $MsiNew)) { Fail-Leg 'M4-M6 needs -MsiNew (the newer package)' }
    $alt = 'C:\Program Files\DarlingAlt'
    $other = 'C:\Program Files\DarlingOther'
    $altExe = Join-Path $alt 'PerformanceMonitor.Darling.Service.exe'
    $altConfig = Join-Path $alt 'darling.json'
    $pgVersion = Join-Path $storeFolder 'pg\PG_VERSION'

    Say 'M4: install the older package into a folder of the operator''s choosing'
    $code = Invoke-Msi '/i' $Msi @("INSTALLFOLDER=`"$alt`"") 'm4-install-old'
    Assert-MsiOk $code 'M4: msiexec /i (older package)' 'm4-install-old'
    Wait-StoreUp 'M4 first install'
    $marker = Get-Marker
    if (-not $marker -or $marker.InstalledBy -ne 'msi') { Fail-Leg 'M4: the install did not write the InstalledBy=msi marker' }
    if (([string]$marker.InstallFolder).TrimEnd('\') -ne $alt) { Fail-Leg "M4: InstallFolder is '$($marker.InstallFolder)', expected '$alt'" }
    if (-not (Test-Path -LiteralPath $altExe)) { Fail-Leg "M4: the service exe is not in $alt" }
    if (Test-Path -LiteralPath $installRoot) { Fail-Leg "M4: files went to the default folder $installRoot as well" }
    $oldVersion = Get-ArpVersion
    $cfgSha = Get-FileSha $altConfig
    if ($cfgSha -eq '(missing)') { Fail-Leg "M4: the install wrote no darling.json in $alt" }
    $storeBorn = (Get-Item -LiteralPath $pgVersion).CreationTimeUtc
    Write-Host "older package: version $oldVersion, darling.json $cfgSha"

    Say 'M4: a major upgrade to the newer package, with an INSTALLFOLDER= that must be ignored'
    $code = Invoke-Msi '/i' $MsiNew @("INSTALLFOLDER=`"$other`"") 'm4-upgrade'
    Assert-MsiOk $code 'M4: msiexec /i (newer package over the older)' 'm4-upgrade'
    Wait-StoreUp 'M4 upgrade'
    $newVersion = Get-ArpVersion
    if (-not $newVersion -or $newVersion -eq $oldVersion) { Fail-Leg "M4: Add/Remove Programs shows '$newVersion' after the upgrade (was '$oldVersion')" }
    $marker = Get-Marker
    if (-not $marker -or ([string]$marker.InstallFolder).TrimEnd('\') -ne $alt) { Fail-Leg "M4: InstallFolder after the upgrade is '$($marker.InstallFolder)', expected '$alt'" }
    if (Test-Path -LiteralPath $other) { Fail-Leg "M4: the INSTALLFOLDER= given on the upgrade was used ($other exists)" }
    if (-not (Test-Path -LiteralPath $altExe)) { Fail-Leg "M4: the service exe is gone from $alt after the upgrade" }
    if ((Get-ServicePath) -notmatch [regex]::Escape($alt)) { Fail-Leg "M4: the service record does not point into ${alt}: $(Get-ServicePath)" }
    if ((Get-FileSha $altConfig) -ne $cfgSha) { Fail-Leg 'M4: darling.json changed across the upgrade' }
    if ((Get-Item -LiteralPath $pgVersion).CreationTimeUtc -ne $storeBorn) { Fail-Leg 'M4: the store was re-created across the upgrade' }
    if (Test-LogHas 'm4-upgrade' 'Windows Installer removes it') { Fail-Leg 'M4: the uninstall script ran during the upgrade (it must not)' }
    if (-not (Test-LogHas 'm4-upgrade' 'Install folder check passed')) { Show-MsiLogErrors 'm4-upgrade'; Fail-Leg 'M4: the install folder check did not run (or did not pass) on the upgrade' }
    Write-Host "M4: PASS (upgraded $oldVersion to $newVersion)"

    # A repair an administrator starts runs the folder check and both script phases again (the condition's
    # MsiTrueAdminUser arm); a repair a standard user starts runs neither, which this runner, signed in as an
    # administrator, cannot show.
    Say 'M4: a repair (msiexec /fa) by an administrator runs the folder check and both script phases'
    $code = Invoke-Msi '/fa' $MsiNew @() 'm4-repair'
    Assert-MsiOk $code 'M4: msiexec /fa' 'm4-repair'
    Wait-StoreUp 'M4 repair'
    if (-not (Test-LogHas 'm4-repair' 'MsiTrueAdminUser = 1')) { Show-MsiLogErrors 'm4-repair'; Fail-Leg 'M4: the repair log does not show MsiTrueAdminUser = 1 for an administrator' }
    foreach ($ran in 'Install folder check passed', 'PreStart done') {
        if (-not (Test-LogHas 'm4-repair' $ran)) { Show-MsiLogErrors 'm4-repair'; Fail-Leg "M4: the administrator's repair did not log '$ran'" }
    }
    if ((Get-FileSha $altConfig) -ne $cfgSha) { Fail-Leg 'M4: darling.json changed across the repair' }
    Write-Host 'M4: PASS (repair)'

    Say 'M5: an uninstall keeps the store and darling.json'
    $code = Invoke-Msi '/x' $MsiNew @() 'm5-uninstall'
    Assert-MsiOk $code 'M5: msiexec /x' 'm5-uninstall'
    if (Get-Service -Name $svc -ErrorAction SilentlyContinue) { Fail-Leg 'M5: the service is still registered after the uninstall' }
    if ($null -ne (Get-Marker)) { Fail-Leg 'M5: the marker is still there after the uninstall' }
    if ($null -ne (Get-ArpVersion)) { Fail-Leg 'M5: the Add/Remove Programs record is still there after the uninstall' }
    if (Test-Path -LiteralPath $altExe) { Fail-Leg 'M5: the service exe is still in the install folder' }
    if (Test-Path -LiteralPath (Join-Path $alt 'pg-runtime')) { Fail-Leg 'M5: pg-runtime was left behind in the install folder' }
    if ((Get-FileSha $altConfig) -ne $cfgSha) { Fail-Leg 'M5: darling.json was changed or removed by the uninstall' }
    if (-not (Test-Path -LiteralPath $pgVersion) -or (Get-Item -LiteralPath $pgVersion).CreationTimeUtc -ne $storeBorn) { Fail-Leg 'M5: the store did not survive the uninstall' }
    if (@(Get-NetFirewallRule -DisplayName 'PerformanceMonitor Darling *' -ErrorAction SilentlyContinue).Count -ne 0) { Fail-Leg 'M5: firewall rules were left behind' }
    # The uninstall script is not cut off by the custom action's one-minute default: it ran to its last line.
    if (-not (Test-LogHas 'm5-uninstall' 'Windows Installer removes it')) { Fail-Leg 'M5: the MSI log has no stop line from uninstall-darling.ps1 -MsiUninstall' }
    if (-not (Test-LogHas 'm5-uninstall' 'Left in place')) { Fail-Leg 'M5: uninstall-darling.ps1 did not run to its last line (cut off?)' }
    Write-Host 'M5: PASS'

    Say 'M6: install again over the kept store, then uninstall with PURGEDATA=1'
    $code = Invoke-Msi '/i' $MsiNew @() 'm6-install'
    Assert-MsiOk $code 'M6: msiexec /i over the kept store' 'm6-install'
    Wait-StoreUp 'M6 reinstall'
    if ((Get-Item -LiteralPath $pgVersion).CreationTimeUtc -ne $storeBorn) { Fail-Leg 'M6: the reinstall did not reuse the kept store' }
    $code = Invoke-Msi '/x' $MsiNew @('PURGEDATA=1') 'm6-purge'
    Assert-MsiOk $code 'M6: msiexec /x PURGEDATA=1' 'm6-purge'
    if (Get-Service -Name $svc -ErrorAction SilentlyContinue) { Fail-Leg 'M6: the service is still registered after the uninstall' }
    if (Test-Path -LiteralPath $storeFolder) { Fail-Leg "M6: PURGEDATA=1 left $storeFolder behind" }
    if (-not (Test-LogHas 'm6-purge' 'Store, credentials, and logs deleted.')) { Fail-Leg 'M6: the MSI log has no purge line from uninstall-darling.ps1' }
    # The install made the operator group, so the purge removes it (#6953).
    $grp = try { (New-Object System.Security.Principal.NTAccount($groupName)).Translate([System.Security.Principal.SecurityIdentifier]).Value } catch { $null }
    if ($grp) { Fail-Leg "M6: PURGEDATA=1 left the operator group '$groupName' behind ($grp)" }
    Write-Host 'M6: PASS'
}

# ---------------------------------------------------------------------------------------------------------------------
if ($Leg -eq 'M7') {
    # A folder under a user profile is a real trigger the script refuses on a first install (the service account cannot read
    # it): the refusal happens in the PreStart custom action, after the files and the service record exist, so the rollback
    # has real work to undo.
    $profileDir = Join-Path $env:USERPROFILE 'DarlingMsiProfile'
    Say "M7: an install into $profileDir (under a user profile) fails in PreStart and rolls back clean"
    $code = Invoke-Msi '/i' $Msi @("INSTALLFOLDER=`"$profileDir`"") 'm7-install'
    if ($code -ne 1603) { Show-MsiLogErrors 'm7-install'; Fail-Leg "M7: msiexec /i into a profile folder exited with $code, expected 1603" }
    if (-not (Test-LogHas 'm7-install' 'This folder is under a user profile')) { Show-MsiLogErrors 'm7-install'; Fail-Leg 'M7: the install did not fail on the script''s profile-folder refusal' }
    if (Get-Service -Name $svc -ErrorAction SilentlyContinue) { Fail-Leg 'M7: the service record was left behind' }
    if ($null -ne (Get-Marker)) { Fail-Leg 'M7: the marker was left behind' }
    if ($null -ne (Get-ArpVersion)) { Fail-Leg 'M7: an Add/Remove Programs record was left behind' }
    foreach ($dir in $profileDir, $installRoot) {
        $left = @(Get-ChildItem -LiteralPath $dir -Recurse -Force -File -ErrorAction SilentlyContinue)
        if ($left.Count -gt 0) { Fail-Leg "M7: $($left.Count) file(s) were left in $dir (first: $($left[0].FullName))" }
    }
    if (Test-Path -LiteralPath $storeFolder) { Fail-Leg "M7: $storeFolder was created" }
    if (@(Get-NetFirewallRule -DisplayName 'PerformanceMonitor Darling *' -ErrorAction SilentlyContinue).Count -ne 0) { Fail-Leg 'M7: firewall rules were left behind' }
    $grp = try { (New-Object System.Security.Principal.NTAccount($groupName)).Translate([System.Security.Principal.SecurityIdentifier]).Value } catch { $null }
    if ($grp) { Fail-Leg "M7: the operator group '$groupName' was created ($grp)" }
    Write-Host 'M7: PASS'
}

# ---------------------------------------------------------------------------------------------------------------------
if ($Leg -eq 'M8') {
    # The folder is made here and BUILTIN\Users given Modify on it explicitly, so the leg does not depend on what the
    # runner's C:\ grants. The folder check must stop the install before install-darling.ps1 runs from it.
    $writable = 'C:\DarlingWritableTest'
    Remove-Tree $writable
    New-Item -ItemType Directory -Force -Path $writable | Out-Null
    & icacls.exe $writable /grant '*S-1-5-32-545:(OI)(CI)M' | Out-Null
    if ($LASTEXITCODE -ne 0) { Fail-Leg "M8: icacls could not grant BUILTIN\Users Modify on $writable" }
    Say "M8: an install into $writable (BUILTIN\Users can modify it) is stopped by the folder check and rolls back clean"
    $code = Invoke-Msi '/i' $Msi @("INSTALLFOLDER=`"$writable\`"") 'm8-install'
    if ($code -ne 1603) { Show-MsiLogErrors 'm8-install'; Fail-Leg "M8: msiexec /i into a writable folder exited with $code, expected 1603" }
    if (-not (Test-LogHas 'm8-install' 'Setup stopped before running install-darling.ps1')) { Show-MsiLogErrors 'm8-install'; Fail-Leg 'M8: the MSI log does not carry the folder check''s message' }
    if (-not (Test-LogHas 'm8-install' 'BUILTIN\Users')) { Show-MsiLogErrors 'm8-install'; Fail-Leg 'M8: the folder check''s message does not name BUILTIN\Users' }
    # The deferred PreStart is scheduled when the script is written; it must never have been executed.
    if (Test-LogHas 'm8-install' 'CustomActionSchedule(Action=DarlingPreStart') { Show-MsiLogErrors 'm8-install'; Fail-Leg 'M8: DarlingPreStart started (install-darling.ps1 ran from the writable folder)' }
    if (Test-LogHas 'm8-install' 'PreStart done') { Fail-Leg 'M8: install-darling.ps1 ran its PreStart phase' }
    if (Get-Service -Name $svc -ErrorAction SilentlyContinue) { Fail-Leg 'M8: the service record was left behind' }
    if ($null -ne (Get-Marker)) { Fail-Leg 'M8: the marker was left behind' }
    if ($null -ne (Get-ArpVersion)) { Fail-Leg 'M8: an Add/Remove Programs record was left behind' }
    $left = @(Get-ChildItem -LiteralPath $writable -Recurse -Force -File -ErrorAction SilentlyContinue)
    if ($left.Count -gt 0) { Fail-Leg "M8: $($left.Count) file(s) were left in $writable (first: $($left[0].FullName))" }
    if (Test-Path -LiteralPath $storeFolder) { Fail-Leg "M8: $storeFolder was created" }
    Write-Host 'M8: PASS'
}

Write-Host "RESULT ${Leg}: PASS"
if ($env:GITHUB_STEP_SUMMARY) { "- $Leg - PASS ($([int]$clock.Elapsed.TotalSeconds)s)" | Out-File -FilePath $env:GITHUB_STEP_SUMMARY -Append -Encoding utf8 }
exit 0
