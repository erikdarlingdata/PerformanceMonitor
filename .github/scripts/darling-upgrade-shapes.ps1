<#
.SYNOPSIS
  Runs ONE leg of the Darling upgrade-shape check (#5627): builds an older install the way an operator would
  have, then runs this checkout's install-darling.ps1 or upgrade-darling.ps1 over it and fails the leg, with a
  one-line reason, when the outcome is not the expected one.

.DESCRIPTION
  Called by .github/workflows/darling-upgrade-shapes.yml, one leg per job, on an elevated hosted Windows runner.
  The older install is the REAL release: its zip is downloaded, its own install-darling.ps1 runs, and its
  service is started until it has extracted pg-runtime (pg-runtime.stamp), then stopped. Nothing about the
  base is built by hand, because the owner of the extracted files is whatever account ran the service when it
  extracted them, and that is the thing the install/upgrade pre-lock check has to judge correctly.

  Shapes (the leg name is <shape>-<script>):
    U1  current release in C:\Program Files\PerformanceMonitorDarling, service stopped.      must PASS
    U2  as U1, service logon changed to a non-default local account and started once on it.  must PASS
    U3  as U1, service deleted. install-darling.ps1 must PASS; upgrade-darling.ps1 has no
        service to upgrade and must say so, without listing the product's own files.
    U4  pre-3.9 release in C:\PerformanceMonitorDarling. install-darling.ps1 must REFUSE, list only the
        folder's own write entry, and print the move steps.
#>
param(
    [Parameter(Mandatory)][ValidateSet('U1-install', 'U1-upgrade', 'U2-install', 'U2-upgrade', 'U3-install', 'U3-upgrade', 'U4-install')][string]$Leg,
    [Parameter(Mandatory)][string]$Workspace,
    [Parameter(Mandatory)][string]$CurrentVersion,
    [Parameter(Mandatory)][string]$OldVersion
)
$ErrorActionPreference = 'Continue'
$shape, $which = $Leg.Split('-')
$svc = 'PerformanceMonitor Darling'
$work = 'C:\darling-shapes-work'
$repo = if ($env:GITHUB_REPOSITORY) { $env:GITHUB_REPOSITORY } else { 'erikdarlingdata/PerformanceMonitor' }
$tools = Join-Path $Workspace 'Darling\tools'
$clock = [System.Diagnostics.Stopwatch]::StartNew()
New-Item -ItemType Directory -Force -Path $work | Out-Null

function Say([string]$t) { Write-Host ''; Write-Host "##### $t" }

function Fail-Leg([string]$reason) {
    Write-Host "::error title=$Leg::$reason"
    Write-Host "RESULT ${Leg}: FAIL - $reason"
    if ($env:GITHUB_STEP_SUMMARY) { "- $Leg - FAIL - $reason" | Out-File -FilePath $env:GITHUB_STEP_SUMMARY -Append -Encoding utf8 }
    exit 1
}

function Get-Zip([string]$version) {
    $zip = Join-Path $work "PerformanceMonitorDarling-$version.zip"
    if (-not (Test-Path $zip)) {
        gh release download "v$version" -R $repo -p "PerformanceMonitorDarling-$version.zip" -D $work --clobber
        if ($LASTEXITCODE -ne 0) { Fail-Leg "could not download the $version release zip" }
    }
    return $zip
}

# Runs a script in Windows PowerShell 5.1 (the engine an operator's elevated prompt starts) and returns its exit
# code and stdout. A child started from pwsh inherits pwsh's PSModulePath and then cannot load Get-Acl, so it is
# given the machine value. The code comes back as text so 'TIMEOUT' and a number compare the same way.
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
    $text = if (Test-Path $out) { (Get-Content $out -Raw) } else { '' }
    Write-Host "----- stdout of $tag (exit code: $code) -----"
    if (Test-Path $out) { Get-Content $out -TotalCount 80 | ForEach-Object { Write-Host $_ } }
    if ((Test-Path $err) -and (Get-Item $err).Length -gt 0) { Write-Host "----- stderr of $tag -----"; Get-Content $err -TotalCount 30 | ForEach-Object { Write-Host $_ } }
    return [pscustomobject]@{ Code = $code; Text = [string]$text }
}

function Stop-Darling {
    $s = Get-Service -Name $svc -ErrorAction SilentlyContinue
    if ($s -and $s.Status -ne 'Stopped') {
        try { Stop-Service -Name $svc -Force -ErrorAction Stop; $s.WaitForStatus('Stopped', [TimeSpan]::FromSeconds(180)) } catch { Write-Host "Stop-Service: $_" }
    }
    Get-Service -Name $svc -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "service now: $($_.Status)" }
}

# Installs <version> into <root> the way an operator does (extract, write darling.json, run its install script) and
# lets the SERVICE extract pg-runtime. A base without pg-runtime.stamp is not the shape under test, so it fails the leg.
function New-Base([string]$version, [string]$root, [bool]$acceptWritable) {
    Say "BASE: install $version into $root"
    $zip = Get-Zip $version
    New-Item -ItemType Directory -Force -Path $root | Out-Null
    tar -xf $zip -C $root
    $cfg = '{ "postgres": { "managed": true, "port": 5641, "dataDirectory": null, "connectAs": "admin" }, "servers": [ { "name": "nosuch", "host": "nosuch.invalid", "auth": "integrated" } ] }'
    Set-Content -LiteralPath (Join-Path $root 'darling.json') -Value $cfg
    $script = Join-Path $root 'install-darling.ps1'
    $declared = @(Select-String -LiteralPath $script -Pattern '^\s*\[switch\]\$(\w+)' | ForEach-Object { $_.Matches[0].Groups[1].Value })
    $a = @(); foreach ($w in 'SkipPreflight', 'NoShortcuts') { if ($declared -contains $w) { $a += "-$w" } }
    if ($acceptWritable -and $declared -contains 'AcceptWritableExtraction') { $a += '-AcceptWritableExtraction' }
    $r = Run-Script $script $a "base-$version-install" 600
    if ($r.Code -ne 0) { Fail-Leg "the $version base install exited with $($r.Code)" }

    $stamp = Join-Path $root 'pg-runtime\pg-runtime.stamp'
    $deadline = (Get-Date).AddMinutes(6)
    while (-not (Test-Path -LiteralPath $stamp) -and (Get-Date) -lt $deadline) { Start-Sleep -Seconds 5 }
    if (-not (Test-Path -LiteralPath $stamp)) {
        Get-ChildItem 'C:\ProgramData\PerformanceMonitorDarling\logs' -ErrorAction SilentlyContinue | Select-Object -Last 2 | ForEach-Object { Write-Host "log $($_.FullName)"; Get-Content $_.FullName -Tail 25 }
        Fail-Leg "the $version service did not extract pg-runtime within 6 minutes"
    }
    Start-Sleep -Seconds 10
    Stop-Darling
    Write-Host "BASE: $version installed in $root, pg-runtime extracted by its service, service stopped"
}

# The next-release scripts of THIS checkout, laid over the current release's files.
function Add-CurrentBuild([string]$dest) {
    $zip = Get-Zip $CurrentVersion
    tar -xf $zip -C $dest
    foreach ($f in 'install-darling.ps1', 'upgrade-darling.ps1', 'uninstall-darling.ps1') { Copy-Item -LiteralPath (Join-Path $tools $f) -Destination (Join-Path $dest $f) -Force }
}

# Gives a local account the "Log on as a service" right, which a service needs before it can start under it.
function Grant-ServiceLogonRight([string]$account) {
    $sid = (New-Object System.Security.Principal.NTAccount($account)).Translate([System.Security.Principal.SecurityIdentifier]).Value
    $inf = Join-Path $work 'userrights.inf'; $db = Join-Path $work 'userrights.sdb'
    secedit.exe /export /cfg $inf /areas USER_RIGHTS | Out-Null
    $out = New-Object System.Collections.Generic.List[string]
    $found = $false
    foreach ($line in (Get-Content -LiteralPath $inf)) {
        if ($line -match '^SeServiceLogonRight\s*=') { $out.Add("$line,*$sid"); $found = $true }
        else { $out.Add($line) }
        if (-not $found -and $line -match '^\[Privilege Rights\]') { $out.Add("SeServiceLogonRight = *$sid"); $found = $true }
    }
    Set-Content -LiteralPath $inf -Value $out -Encoding Unicode
    secedit.exe /configure /db $db /cfg $inf /areas USER_RIGHTS | Out-Null
}

function Show-State([string]$root) {
    Say "STATE root=$root"
    icacls $root | Select-Object -First 12
    sc.exe qc $svc
    foreach ($p in @("$root\pg-runtime", "$root\pg-runtime\pg-runtime.stamp", "$root\darling.json")) {
        if (Test-Path -LiteralPath $p) {
            $o = (Get-Acl -LiteralPath $p).GetOwner([System.Security.Principal.SecurityIdentifier])
            $n = try { $o.Translate([System.Security.Principal.NTAccount]).Value } catch { '(untranslatable)' }
            Write-Host "owner of ${p}: $n"
        } else { Write-Host "absent: $p" }
    }
}

function Expect-Success($r, [string]$what) {
    if ($r.Code -ne 0) { Fail-Leg "$what exited with $($r.Code), expected 0" }
    if ($r.Text -match 'Ordinary users can already write|already writable by ordinary users') { Fail-Leg "$what reported an ordinary-user write grant on a folder only administrators can write" }
}

Say "LEG $Leg on $([Environment]::OSVersion.VersionString) (current $CurrentVersion, older $OldVersion)"

# -- the base for the shape ----------------------------------------------------------------------------------------
$root = if ($shape -eq 'U4') { 'C:\PerformanceMonitorDarling' } else { 'C:\Program Files\PerformanceMonitorDarling' }
New-Base $(if ($shape -eq 'U4') { $OldVersion } else { $CurrentVersion }) $root $false

switch ($shape) {
    'U2' {
        Say 'U2: the service logon becomes a non-default local account and the service runs once on it'
        $account = 'darlingshape'
        $pw = 'Aa1!' + ([guid]::NewGuid().ToString('N'))
        net user $account $pw /add | Out-Null
        Grant-ServiceLogonRight ".\$account"
        sc.exe config $svc obj= ".\$account" password= $pw | Out-Host
        try { Start-Service -Name $svc -ErrorAction Stop; (Get-Service -Name $svc).WaitForStatus('Running', [TimeSpan]::FromSeconds(90)) } catch { Write-Host "start on ${account}: $_" }
        Write-Host "service on ${account} after the start attempt: $((Get-Service -Name $svc).Status)"
        Stop-Darling
    }
    'U3' {
        Say 'U3: the service is deleted'
        sc.exe delete $svc | Out-Host
        Start-Sleep -Seconds 3
        if (Get-Service -Name $svc -ErrorAction SilentlyContinue) { Fail-Leg 'the service is still registered after sc.exe delete' }
    }
}

# -- the current build over it, then the script under test ----------------------------------------------------------
if ($which -eq 'install') {
    Add-CurrentBuild $root
    Show-State $root
    Say "RUN this checkout's install-darling.ps1"
    $r = Run-Script (Join-Path $root 'install-darling.ps1') @('-SkipPreflight', '-NoShortcuts') "$Leg-run"
}
else {
    $stage = 'C:\Program Files\darling-shapes-stage'
    New-Item -ItemType Directory -Force -Path $stage | Out-Null
    Add-CurrentBuild $stage
    Show-State $root
    Say "RUN this checkout's upgrade-darling.ps1"
    $upgradeArgs = @('-SkipHashCheck')
    if ($shape -eq 'U3') { $upgradeArgs += @('-InstallRoot', "`"$root`"") }
    $r = Run-Script (Join-Path $stage 'upgrade-darling.ps1') $upgradeArgs "$Leg-run"
}

# -- the expectation of the leg ------------------------------------------------------------------------------------
switch ($Leg) {
    { $_ -in 'U1-install', 'U1-upgrade', 'U2-install', 'U2-upgrade' } { Expect-Success $r "$which-darling.ps1" }
    'U3-install' {
        Expect-Success $r 'install-darling.ps1'
        if (-not (Get-Service -Name $svc -ErrorAction SilentlyContinue)) { Fail-Leg 'install-darling.ps1 exited 0 but left no service registered' }
    }
    'U3-upgrade' {
        # With no service there is nothing to stop and start around the copy: the script says so and exits non-zero.
        if ($r.Code -eq 0 -or $r.Code -eq 'TIMEOUT') { Fail-Leg "upgrade-darling.ps1 with no service registered exited with $($r.Code), expected a refusal" }
        if ($r.Text -notmatch 'not registered on this machine|is not installed') { Fail-Leg 'upgrade-darling.ps1 with no service registered did not say the service is missing' }
        if ($r.Text -match 'owned by NT SERVICE|already writable by ordinary users|Ordinary users can already write') { Fail-Leg 'upgrade-darling.ps1 with no service registered listed the product''s own files or an ordinary-user write grant' }
    }
    'U4-install' {
        if ($r.Code -eq 0 -or $r.Code -eq 'TIMEOUT') { Fail-Leg "install-darling.ps1 over a pre-3.9 folder under C:\ exited with $($r.Code), expected a refusal" }
        if ($r.Text -match 'owned by NT SERVICE') { Fail-Leg 'the refusal lists files owned by the product''s own service account' }
        if ($r.Text -match 'and \d+ more') { Fail-Leg 'the refusal lists more than the folder''s own write entry' }
        $findings = @([regex]::Matches($r.Text, '(?m)^\s{2}\S.* on C:\\PerformanceMonitorDarling\s*$'))
        if ($findings.Count -ne 1) { Fail-Leg "the refusal lists $($findings.Count) write entries on the root folder, expected exactly 1" }
        $otherLines = @([regex]::Matches($r.Text, '(?m)^\s{2}\S.* on [A-Za-z]:\\.*$') | Where-Object { $_.Value -notmatch 'on C:\\PerformanceMonitorDarling\s*$' })
        if ($otherLines.Count -gt 0) { Fail-Leg "the refusal lists an entry other than the root folder: $($otherLines[0].Value.Trim())" }
        if ($r.Text -notmatch [regex]::Escape('C:\Program Files')) { Fail-Leg 'the refusal does not name C:\Program Files as the place to move to' }
        if ($r.Text -notmatch 'darling\.json') { Fail-Leg 'the refusal does not say to carry darling.json over' }
    }
}

$secs = [int]$clock.Elapsed.TotalSeconds
Write-Host "RESULT ${Leg}: PASS (${secs}s)"
if ($env:GITHUB_STEP_SUMMARY) { "- $Leg - PASS (${secs}s)" | Out-File -FilePath $env:GITHUB_STEP_SUMMARY -Append -Encoding utf8 }
