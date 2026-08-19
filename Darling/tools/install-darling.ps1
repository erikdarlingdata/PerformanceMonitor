<#
.SYNOPSIS
Installs (or upgrades) the PerformanceMonitor Darling service from the folder this script sits in,
and creates Desktop + Start Menu shortcuts for the bundled viewer.

.DESCRIPTION
Run from an ELEVATED PowerShell, from the folder you extracted the Darling zip into (the script
installs the service pointing at THAT folder — extract to the final location first, e.g.
C:\PerformanceMonitorDarling). What it does, in order:

  1. Verifies elevation, the service exe, and darling.json (offers to copy darling.sample.json
     and stops so you can edit it — the service is not installed with an unedited sample).
  1b. REFUSES an install directory the service account can never read: anywhere under a user profile
     (C:\Users\...), or a UNC / mapped-drive path. The service runs as an unprivileged virtual account
     that is neither you nor an administrator, and a profile folder grants nothing to it, so the
     service installs cleanly and then dies at the bundled PostgreSQL's first step (#2185, #2187).
     Extract to a machine-scoped local path such as C:\PerformanceMonitorDarling instead.
  2. Optional pre-flight: runs `--test-connection` and shows the per-server PASS/FAIL lines
     (continue-or-abort prompt on failure; -SkipPreflight to skip).
  3. Registers the Windows Event Log source 'PerformanceMonitor Darling' (requires elevation —
     the service's own virtual account cannot; without it Event Log diagnostics are silently
     dropped. The file log under %ProgramData%\PerformanceMonitorDarling\logs works regardless).
  4. Creates the service under the NT SERVICE virtual account (NEVER LocalSystem — the bundled
     PostgreSQL refuses to run with administrative privileges), start=auto. If the service
     already exists this is an UPGRADE: it is stopped and its binPath updated in place; your
     darling.json, store data, and credentials are untouched.
  4b. Restricts darling.json to SYSTEM / Administrators / the service account, plus read for
     INTERACTIVE (the Viewer). It holds encrypted SQL passwords and the MCP/web access tokens,
     and an install folder under C:\ otherwise inherits read access for BUILTIN\Users.
  4c. Creates (or removes) the scoped Windows Firewall rules to match darling.json, via the exe's
     --configure-firewall verb. This requires elevation, which is why it lives here: the service's
     own unprivileged account cannot create them, and only verifies them at runtime.
  5. Starts the service and confirms it reaches Running.
  6. Creates 'Darling Viewer' shortcuts on the Desktop and in the Start Menu pointing at
     viewer\PerformanceMonitor.Darling.Viewer.exe. (Taskbar pinning is deliberately not
     attempted — Windows blocks programmatic pinning by design; pin from the Start Menu entry.)

Uninstall with uninstall-darling.ps1 (same folder).

.PARAMETER SkipPreflight
Skip the --test-connection pre-flight gate.

.PARAMETER NoShortcuts
Do not create the viewer shortcuts.

.PARAMETER Network
After the service reaches Running, launch the interactive --configure-network wizard to opt into the
store / MCP / web-dashboard LAN endpoints (guided, delegated validation, comment-preserving darling.json
edit + backup). Off by default; the endpoints stay loopback-only unless you pass this or edit darling.json
by hand.
#>
[CmdletBinding()]
param(
    [switch]$SkipPreflight,
    [switch]$NoShortcuts,
    [switch]$Network
)

$ErrorActionPreference = 'Stop'
$serviceName = 'PerformanceMonitor Darling'
$root = $PSScriptRoot
$serviceExe = Join-Path $root 'PerformanceMonitor.Darling.Service.exe'
$viewerExe = Join-Path $root 'viewer\PerformanceMonitor.Darling.Viewer.exe'
$configPath = Join-Path $root 'darling.json'
$samplePath = Join-Path $root 'darling.sample.json'

function Fail([string]$message) { Write-Host "ERROR: $message" -ForegroundColor Red; exit 1 }

# True when $candidate IS $parent or sits underneath it.
#
# The separator is appended before the prefix test on purpose: a bare StartsWith reads C:\UsersData as
# living under C:\Users and would refuse a perfectly good install directory. A false refusal is a worse
# failure than the one this check exists to catch - it strands someone whose install would have worked -
# so the boundary is the one thing here worth being exact about. Equality counts as under: an install
# root sitting AT the profile root is exactly as unreadable as one below it.
function Test-PathIsAtOrUnder([string]$candidate, [string]$parent) {
    if ([string]::IsNullOrWhiteSpace($candidate) -or [string]::IsNullOrWhiteSpace($parent)) { return $false }

    try {
        # Normalizes separators, resolves . and .., and makes the comparison independent of how the path
        # was typed. Anything GetFullPath rejects is not a path we can reason about, so it is not matched.
        $c = [IO.Path]::GetFullPath($candidate).TrimEnd('\')
        $p = [IO.Path]::GetFullPath($parent).TrimEnd('\')
    }
    catch {
        return $false
    }

    if ($c.Equals($p, [StringComparison]::OrdinalIgnoreCase)) { return $true }
    return $c.StartsWith($p + '\', [StringComparison]::OrdinalIgnoreCase)
}

# The machine's profile root, read from where Windows actually keeps it rather than assumed to be
# C:\Users. ProfilesDirectory is relocatable, and a hardcoded literal would quietly stop matching on
# precisely the box that moved it - the one box where a missed check costs the most.
function Get-ProfilesDirectory {
    try {
        $configured = (Get-ItemProperty -Path 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\ProfileList' -Name 'ProfilesDirectory' -ErrorAction Stop).ProfilesDirectory
        $expanded = [Environment]::ExpandEnvironmentVariables($configured)
        if (-not [string]::IsNullOrWhiteSpace($expanded)) { return $expanded }
    }
    catch {
        # An unreadable ProfileList is not a reason to skip the check - fall back to the default location.
    }

    return (Join-Path $env:SystemDrive 'Users')
}

# Rewrites an extended-length path to its ordinary spelling (#2348), leaving anything else alone:
# \\?\UNC\server\share -> \\server\share, and \\?\C:\dir -> C:\dir. The C# twin is
# DarlingInstallLocation.StripExtendedLengthPrefix and the two must stay identical.
#
# The UNC form is tested FIRST because it is the longer, more specific prefix - checking \\?\ first would
# strip four characters off a share and leave the nonsense UNC\server\share, which is neither a share nor a
# local path. 'UNC' is matched case-insensitively because Windows accepts \\?\unc\ too, and a miss there
# would silently re-open the hole this closes.
function Convert-ExtendedLengthPath([string]$path) {
    if ([string]::IsNullOrEmpty($path)) { return $path }
    if ($path.StartsWith('\\?\UNC\', [StringComparison]::OrdinalIgnoreCase)) { return '\\' + $path.Substring(8) }
    if ($path.StartsWith('\\?\', [StringComparison]::Ordinal)) { return $path.Substring(4) }
    return $path
}

# 'UNC', 'mapped drive', or $null. Named separately from the profile case because the reason differs:
# a virtual service account reaches the network as the COMPUTER account rather than as the operator who
# typed the path, and a mapped drive letter belongs to one logon session, which a service never shares.
#
# Callers pass a path that Convert-ExtendedLengthPath has already normalized, so there is no \\?\ carve-out
# here any more: an extended-length LOCAL root has become C:\... and cannot reach the UNC test, while an
# extended-length SHARE has become \\server\share and correctly does (#2348 - the old wholesale exclusion
# waved real shares through, because skipping the check is not the same as passing it).
function Get-NetworkPathKind([string]$path) {
    if ([string]::IsNullOrWhiteSpace($path)) { return $null }

    if ($path.StartsWith('\\', [StringComparison]::Ordinal)) {
        return 'UNC'
    }

    $qualifier = $null
    try { $qualifier = Split-Path -Qualifier $path -ErrorAction Stop } catch { }
    if (-not $qualifier) { return $null }

    try {
        $drive = Get-CimInstance -ClassName Win32_LogicalDisk -Filter "DeviceID='$qualifier'" -ErrorAction Stop
        # DriveType 4 = network drive.
        if ($drive -and $drive.DriveType -eq 4) { return 'mapped drive' }
        # A DEFINITE answer is trusted, including "local" - so the fallback below is not consulted and
        # cannot second-guess WMI on a box where WMI works. DriveType 0 is "unknown", which is NOT an
        # answer: 0 is falsy here, so an unknown row falls through to the probe below instead of being
        # read as "local". That matters because a partial WMI response is likeliest on exactly the
        # restricted images this fallback exists for (review catch on #2248).
        if ($drive -and $drive.DriveType) { return $null }
    }
    catch {
        # WMI unavailable (locked-down or Server Core images). Fall through to the probe below rather
        # than answering "not network", which is the fail-open in #2201.
    }

    # No answer from WMI. DisplayRoot is populated ONLY for a drive letter mapped to a share, and comes
    # from .NET rather than WMI, so it survives a restricted image. That keeps the rule this function has
    # always followed - unknown is not network, and a refusal needs evidence - while no longer MISSING the
    # one case the guard exists to catch. Unknown still returns $null two lines down.
    $psDrive = Get-PSDrive -Name $qualifier.TrimEnd(':') -ErrorAction SilentlyContinue
    if ($psDrive -and -not [string]::IsNullOrWhiteSpace($psDrive.DisplayRoot)) { return 'mapped drive' }

    return $null
}

# -- 1. Environment checks ------------------------------------------------------------------------
$identity = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
if (-not $identity.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Fail 'Run this script from an ELEVATED PowerShell (service creation and Event Log registration require it).'
}

if (-not (Test-Path $serviceExe)) {
    Fail "PerformanceMonitor.Darling.Service.exe not found beside this script. Extract the full Darling zip and run install-darling.ps1 from the extracted folder."
}

# -- 1b. Refuse an install root the service account can never read (#2187) -------------------------
# Reported as #2185: a zip extracted to C:\Users\<someone>\Desktop\PerformanceMonitorDarling-3.2.0\ -
# a completely reasonable thing to do with a download - installed without a complaint and then failed,
# and nothing in the product said why.
#
# The mechanism is the deliberate account choice made at step 4. The service runs as the virtual account
# 'NT SERVICE\<serviceName>' and NEVER as LocalSystem, because the bundled PostgreSQL refuses to run with
# administrative privileges. That account is therefore not the installing user, not SYSTEM, and not
# Administrators - and a user profile grants access to approximately those three and nobody else. Measured
# on a clean Windows 11 box, a directory created under a profile inherits exactly:
#     NT AUTHORITY\SYSTEM:(I)(OI)(CI)(F)   BUILTIN\Administrators:(I)(OI)(CI)(F)   <user>:(I)(OI)(CI)(F)
# with no BUILTIN\Users, no Authenticated Users, and no CREATOR OWNER - so the account cannot read the
# program files it was pointed at, and cannot even read back what it writes there itself. The documented
# location inherits BUILTIN\Users:(I)(OI)(CI)(RX) from the volume root instead, which every service
# account is a member of, which is why C:\PerformanceMonitorDarling works and this does not.
#
# Step 4b is not a substitute: its ACL work is scoped to darling.json and its .bak-* copies, so pg-runtime
# keeps whatever the profile gave it. Fixing the tree's ACLs instead of refusing was considered and
# rejected in #2187 - it means the product starts silently ACLing directories inside somebody's profile.
#
# The residual, seen and accepted rather than discovered later: C:\Users\Public sits under the profile root
# and is refused, but an install there would actually WORK - it grants NT AUTHORITY\SERVICE:(OI)(CI)(IO)(M,DC),
# which every service account holds. It is deliberately not carved out. Nobody installs a Windows service
# into the shared documents profile, a carve-out would amount to documenting it as a reasonable place to
# install, and the refusal is not a dead end - it names C:\PerformanceMonitorDarling. One rule, no
# exceptions, and the one location it costs is one nobody wants.
#
# This runs BEFORE the pre-flight, the Event Log source, and service creation, so a doomed location costs
# nothing and leaves nothing behind.
$existing = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
# Classify the NORMALIZED spelling, but keep installing to $root exactly as given (#2348). The \\?\ prefix
# instructs the path parser and is not part of where the install lives, so stripping it for the decision
# changes which rules see the path and nothing about where files land.
$classifyRoot = Convert-ExtendedLengthPath $root

$networkKind = Get-NetworkPathKind $classifyRoot
# $env:USERPROFILE as well as the machine's profile root: a profile redirected outside ProfilesDirectory
# is still a profile, and it is the profile whose owner is most likely to be running this script.
$underProfile = (Test-PathIsAtOrUnder $classifyRoot (Get-ProfilesDirectory)) -or (Test-PathIsAtOrUnder $classifyRoot $env:USERPROFILE)

if ($underProfile -or $networkKind) {
    if ($underProfile) {
        $why = @"
This folder is under a user profile:

  $root

The service runs as the unprivileged virtual account 'NT SERVICE\$serviceName' - never LocalSystem,
because the bundled PostgreSQL refuses to run with administrative privileges. That account is not you,
not SYSTEM, and not Administrators, and a user profile grants access to about those three and nobody
else. So the service installs cleanly and then cannot read its own program files: the bundled
PostgreSQL's initdb.exe dies at exit code -1073741515 (0xC0000135, STATUS_DLL_NOT_FOUND) before it can
write a word of output, because the DLLs sitting beside it are unreadable (#2185).
"@
    }
    else {
        $why = @"
This folder is on a network location ($networkKind):

  $root

The service runs as the unprivileged virtual account 'NT SERVICE\$serviceName' - never LocalSystem,
because the bundled PostgreSQL refuses to run with administrative privileges. That account reaches the
network as the COMPUTER account rather than as you, so a share that opens for you is not open for it;
and a mapped drive letter belongs to YOUR logon session, which a service does not share and cannot see
at all. Either way the service installs cleanly and then cannot read its own program files.
"@
    }

    $fix = @"
Move the extracted folder to a machine-scoped local path and run this script again from there:

  C:\PerformanceMonitorDarling

That is the documented location, and a folder created there inherits read + execute for BUILTIN\Users,
which the service's virtual account is a member of. Your darling.json can move with it.
"@

    if (-not $existing) {
        Fail @"
$why

$fix

Nothing was installed or changed.
"@
    }

    # An UPGRADE is a question rather than a refusal, and only here. This script's upgrade path exists to
    # preserve installs operators have customized - it deliberately touches only binPath so a re-homed
    # logon account survives (#1802, #1823) - and #2187's rejected option 2 (grant the service account
    # read + execute on the tree) is exactly the thing an operator may already have done by hand here.
    # Refusing outright would strand a deployment that works. The diagnosis is identical and unmissable;
    # only the verdict is theirs. A fresh install in the same folder is still refused outright above.
    Write-Host ''
    Write-Host 'WARNING: a FRESH install would be refused in this location.' -ForegroundColor Red
    Write-Host $why -ForegroundColor Red
    Write-Host ''
    Write-Host $fix -ForegroundColor Yellow
    Write-Host ''
    Write-Host "Service '$serviceName' already exists, so this is an upgrade, and this folder may have been made" -ForegroundColor Yellow
    Write-Host 'to work by hand (granting the service account read + execute on the tree). That is the only reason' -ForegroundColor Yellow
    Write-Host 'this is a question. If it has NOT been, the service will stop working the moment it restarts.' -ForegroundColor Yellow
    $answer = Read-Host 'Point the service at this folder anyway? [y/N]'
    if ($answer -notmatch '^[Yy]') { exit 4 }
}

if (-not (Test-Path $configPath)) {
    if (Test-Path $samplePath) {
        Copy-Item $samplePath $configPath
        Write-Host ''
        Write-Host 'darling.json did not exist, so darling.sample.json was copied to darling.json.' -ForegroundColor Yellow
        Write-Host 'EDIT IT NOW (servers to monitor, auth) and re-run this script. The sample is heavily commented.' -ForegroundColor Yellow
        Write-Host "  notepad `"$configPath`"" -ForegroundColor Yellow
        exit 2
    }

    Fail 'Neither darling.json nor darling.sample.json found beside this script - the zip looks incomplete.'
}

# -- 2. Pre-flight --------------------------------------------------------------------------------
if (-not $SkipPreflight) {
    Write-Host 'Pre-flight: validating darling.json and probing every configured server (--test-connection)...'
    & $serviceExe --test-connection
    if ($LASTEXITCODE -ne 0) {
        Write-Host ''
        Write-Host "Pre-flight FAILED (exit $LASTEXITCODE) - the config is invalid or a server is unreachable." -ForegroundColor Yellow
        $answer = Read-Host 'Install anyway? A monitoring service may legitimately be installed while a target is down. [y/N]'
        if ($answer -notmatch '^[Yy]') { exit 3 }
    }
    else {
        Write-Host 'Pre-flight passed.' -ForegroundColor Green
    }
}

# -- 3. Event Log source --------------------------------------------------------------------------
try {
    New-EventLog -LogName Application -Source $serviceName -ErrorAction Stop
    Write-Host "Registered Event Log source '$serviceName'."
}
catch [System.InvalidOperationException] {
    Write-Host "Event Log source '$serviceName' already registered."
}

# -- 4. Create or upgrade the service -------------------------------------------------------------
# $existing was resolved back at 1b, which needs to know fresh-versus-upgrade before it decides whether a
# bad install location is a refusal or a question. Nothing between there and here creates or removes the
# service, so it is the same answer.
if ($existing) {
    Write-Host "Service already exists - upgrading its binPath in place (config, store data, and credentials untouched)."
    if ($existing.Status -ne 'Stopped') {
        Stop-Service -Name $serviceName -Force
        (Get-Service -Name $serviceName).WaitForStatus('Stopped', [TimeSpan]::FromSeconds(60))
    }

    & sc.exe config $serviceName binPath= "`"$serviceExe`"" | Out-Null
    if ($LASTEXITCODE -ne 0) { Fail "sc config failed ($LASTEXITCODE)." }
}
else {
    # The space after binPath=/start=/obj= is sc.exe syntax. The virtual service account is
    # password-less, per-service, unprivileged - and mandatory-in-practice for the bundled
    # PostgreSQL, which refuses to run with administrative privileges (LocalSystem bricks it).
    & sc.exe create $serviceName binPath= "`"$serviceExe`"" start= auto obj= "NT SERVICE\$serviceName" | Out-Null
    if ($LASTEXITCODE -ne 0) { Fail "sc create failed ($LASTEXITCODE)." }
    Write-Host "Created service '$serviceName' (NT SERVICE virtual account, automatic start)."
}

# The account the service ACTUALLY logs on as - NOT assumed to be the virtual account. Operators using
# integrated auth to monitored servers re-home the service to a domain account or gMSA (#1802, #1823),
# and the upgrade path above deliberately preserves that (sc config touches binPath only). The 4b
# hardening below must grant the account the service really runs as: rebuilding the DACL around the
# virtual account on a re-homed install would strip the operator's grant and lock the service out of
# its own config on the next start. Fresh installs resolve to the virtual account just created.
#
# Resolution failure is NOT a silent fallback. On the FRESH path the default is provably right - sc create
# above set that very account - so a WMI hiccup there costs nothing and only warns. On the UPGRADE path we
# do not know what the service was re-homed to, and guessing the virtual account is exactly the lockout
# this resolution exists to prevent, so it stops the install instead of quietly ACLing the wrong principal.
# sc.exe qc is tried first as a non-WMI second opinion, so a broken WMI repository alone cannot brick an
# upgrade; its field labels are localized, so a non-match there just falls through to the loud failure.
$serviceAccount = "NT SERVICE\$serviceName"
$startName = $null
try {
    $startName = (Get-CimInstance -ClassName Win32_Service -Filter "Name='$serviceName'" -ErrorAction Stop).StartName
}
catch {
    Write-Host "Get-CimInstance could not read the service's logon account ($($_.Exception.Message)) - falling back to sc.exe qc." -ForegroundColor Yellow
    $qc = & sc.exe qc $serviceName 2>$null
    if ($LASTEXITCODE -eq 0) {
        $match = $qc | Select-String -Pattern '^\s*SERVICE_START_NAME\s*:\s*(\S.*?)\s*$'
        if ($match) { $startName = $match.Matches[0].Groups[1].Value }
    }
}

if ($startName) {
    if ($startName -eq 'LocalSystem') { $serviceAccount = 'NT AUTHORITY\SYSTEM' }
    else { $serviceAccount = $startName -replace '^\.\\', "$env:COMPUTERNAME\" }
}
elseif ($existing) {
    Fail @"
Could not determine which account service '$serviceName' logs on as, and it already existed before this run.
Assuming the default virtual account would re-ACL darling.json around 'NT SERVICE\$serviceName' and strip the
grant of whatever domain account or gMSA the service was re-homed to (#1802, #1823), locking it out of its own
config on the next start - so this install stops here instead. Nothing was ACLed; the service is stopped with
its binPath already updated.

Check the account, then re-run this script:
  sc.exe qc $serviceName
"@
}
else {
    Write-Host "Could not determine the service's logon account; using the virtual account 'NT SERVICE\$serviceName' that sc create just set." -ForegroundColor Yellow
}

# -- 4b. Lock down darling.json (#1647) -----------------------------------------------------------
# The config holds every monitored server's encryptedPassword plus the MCP bearer and web access tokens.
# They are DPAPI LocalMachine scope with an entropy constant that ships in the open-source repo, so READ
# access to this file IS the secret - the ACL is the boundary, the same posture the PG credential files get.
# Extracting the zip to a folder created directly under C:\ (the documented location) inherits
# BUILTIN\Users: Read & Execute from the root DACL, which hands every local user the lot.
#
# Runs AFTER service creation on purpose: the NT SERVICE virtual account has no SID to grant until sc.exe
# create has made it. Mirrors DarlingFileSecurity.HardenFile exactly - SYSTEM / Administrators / the service
# account get full control on both, and INTERACTIVE gets read on the LIVE CONFIG ONLY (the Viewer and the CLI
# verbs run as the interactive operator and must still read darling.json; nothing reads a backup, #1769).
# Best-effort: a failure warns rather than aborting an otherwise good install, and the service re-asserts the
# live config's ACL at every startup.
#
# Covers darling.json AND its .bak-* siblings (#1721). The CLI's config-editing verbs back the file up
# before rewriting it, and File.Copy does NOT carry the source DACL - a backup takes the DIRECTORY's
# inheritable ACEs instead, so on an install under C:\ every past edit left a world-readable copy of the
# same secrets. The service hardens new backups itself now; these are the ones already on disk.
#
# The service account is also made the OWNER, best-effort and in a try of its own. Ownership carries
# WRITE_DAC implicitly, which is what lets the service re-assert this ACL at every start. Granting it
# FullControl below achieves the same thing today; owning the file means it still holds if someone later
# edits the DACL and drops that grant. Done AFTER the DACL and in its own try, so a SeRestorePrivilege
# failure cannot take the DACL down with it - the failure mode we are fixing came from a permissions call
# that silently did nothing.
$hardened = @()
$failed = @()
foreach ($secretFile in @($configPath) + @(Get-ChildItem -Path $root -Filter 'darling.json.bak-*' -File -ErrorAction SilentlyContinue | ForEach-Object { $_.FullName })) {
    try {
        $wk = [System.Security.Principal.WellKnownSidType]
        $systemSid      = New-Object System.Security.Principal.SecurityIdentifier($wk::LocalSystemSid, $null)
        $adminsSid      = New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinAdministratorsSid, $null)
        $interactiveSid = New-Object System.Security.Principal.SecurityIdentifier($wk::InteractiveSid, $null)
        $serviceSid     = (New-Object System.Security.Principal.NTAccount($serviceAccount)).Translate([System.Security.Principal.SecurityIdentifier])

        $acl = New-Object System.Security.AccessControl.FileSecurity
        # Protect the DACL and drop every inherited ACE: access is now EXACTLY the four rules below.
        $acl.SetAccessRuleProtection($true, $false)
        $full = [System.Security.AccessControl.FileSystemRights]::FullControl
        $read = [System.Security.AccessControl.FileSystemRights]::Read -bor [System.Security.AccessControl.FileSystemRights]::Synchronize
        $allow = [System.Security.AccessControl.AccessControlType]::Allow
        foreach ($sid in @($systemSid, $adminsSid, $serviceSid)) {
            $acl.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($sid, $full, $allow)))
        }
        # INTERACTIVE read goes to the LIVE config only. darling.json is read as the interactive operator by the
        # Viewer and the CLI verbs; a .bak-* copy is read by nothing (#1769) - the only code that knows the name
        # is the code that creates it, and restoring one already needs elevation because writing darling.json
        # does. So a backup granting INTERACTIVE was a second copy of every encrypted password and access token,
        # readable by any interactively-logged-on user, buying nothing. Mirrors
        # DarlingFileSecurity.HardenFile(path, allowInteractiveRead: false) for backups.
        if ($secretFile -eq $configPath) {
            $acl.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($interactiveSid, $read, $allow)))
        }
        Set-Acl -Path $secretFile -AclObject $acl

        # The owner goes onto the file's CURRENT descriptor, never a fresh FileSecurity (#1957). Set-Acl applies
        # the whole descriptor it is handed, so a bare object carrying nothing but an owner also wrote an empty,
        # UNPROTECTED DACL - which re-enabled inheritance and handed BUILTIN\Users read straight back on an
        # install under C:\. Measured on a scratch layout under C:\: immediately after this step the file read
        # protected=False with BUILTIN\Users present, all four inherited ACEs back and every hardened ACE gone.
        # So the verification below was failing HONESTLY - the file really was exposed at that instant - and the
        # SECURITY WARNING that fired on three consecutive field installs was right about a hole this script had
        # just opened itself. It stayed invisible because #1818's startup sweep re-hardens the config and its
        # backups seconds later at the first service start, so an operator's before/after ACL captures both
        # looked correct and only the installer disagreed. Re-reading first keeps the DACL written above in the
        # descriptor that gets applied, which is what makes the verified state the FINAL state.
        try {
            $owner = Get-Acl -Path $secretFile
            $owner.SetOwner($serviceSid)
            Set-Acl -Path $secretFile -AclObject $owner
        }
        catch {
            # Not fatal: the explicit FullControl grant above already carries WRITE_DAC.
        }

        # VERIFY rather than assume. A permissions call that appears to succeed and leaves the file
        # readable is exactly what went unnoticed for months on a field box.
        $after = Get-Acl -Path $secretFile
        $exposed = $after.Access | Where-Object {
            $_.AccessControlType -eq 'Allow' -and
            $_.IdentityReference.Translate([System.Security.Principal.SecurityIdentifier]).IsWellKnown($wk::BuiltinUsersSid)
        }
        if ($exposed -or -not $after.AreAccessRulesProtected) {
            $failed += $secretFile
        }
        else {
            $hardened += $secretFile
        }
    }
    catch {
        $failed += "$secretFile ($($_.Exception.Message))"
    }
}

if ($hardened.Count -gt 0) {
    Write-Host "Restricted $($hardened.Count) credential file(s) to SYSTEM, Administrators, and the service account (they hold encrypted passwords and access tokens). darling.json additionally allows INTERACTIVE read, which the Viewer and the CLI verbs need; its .bak-* copies do not."
    Write-Host 'The service re-verifies and re-applies these ACLs on every start (#1818), so this is a floor rather than a one-time act: a later edit that loosens one is repaired at the next start without re-running the installer.'
}
if ($failed.Count -gt 0) {
    Write-Host ''
    Write-Host 'SECURITY WARNING: these files still are not restricted:' -ForegroundColor Red
    foreach ($f in $failed) { Write-Host "  $f" -ForegroundColor Red }
    Write-Host '  They hold every monitored server''s encrypted SQL password plus the MCP and web access tokens.' -ForegroundColor Red
    Write-Host '  Those are DPAPI LocalMachine blobs, so READ ACCESS IS THE SECRET - any local user who can open' -ForegroundColor Red
    Write-Host '  the file can recover all of it. Fix each one from an elevated prompt:' -ForegroundColor Red
    Write-Host "    icacls `"<file>`" /inheritance:d" -ForegroundColor Yellow
    Write-Host "    icacls `"<file>`" /remove:g `"BUILTIN\Users`"" -ForegroundColor Yellow
    Write-Host "    icacls `"<file>`" /grant `"$serviceAccount`:(F)`"" -ForegroundColor Yellow
    Write-Host '  ...or move the install out of a world-readable folder such as one created directly under C:\.' -ForegroundColor Red
    Write-Host ''
}

# -- 4c. Firewall rules (#1771) --------------------------------------------------------------------
# The service runs as an unprivileged virtual account that CANNOT create firewall rules. It used to try on
# every start and fail "Access is denied" each time - harmless where the rules already existed from a manual
# setup, but on a FRESH networked install nothing ever created them, so remote MCP/web/store clients were
# simply blocked. Networked is the normal deployment mode, so rule management belongs here, in the elevated
# install, and the running service only verifies.
#
# The exe does the work (--configure-firewall) rather than this script building rules itself: the rule names,
# the ports, and above all WHETHER a surface is really exposed come from the same resolvers the service
# fail-closes on, so a config the service degrades to loopback never gets an open port, and the service's own
# start-up check always reaches the same verdict the installer just acted on. Idempotent, so an upgrade
# re-running it is a no-op, and it needs only darling.json - no store, no credentials - so it is safe here,
# BEFORE the first start.
#
# Best-effort: a firewall failure warns rather than aborting an otherwise good install. Re-run it by hand.
function Invoke-FirewallReconcile {
    & $serviceExe --configure-firewall
    if ($LASTEXITCODE -ne 0) {
        Write-Host ''
        Write-Host "Firewall rules were not fully configured (exit $LASTEXITCODE). The service will still run, and" -ForegroundColor Yellow
        Write-Host 'will log which rule is missing on each start. Re-run from an elevated prompt:' -ForegroundColor Yellow
        Write-Host "  & `"$serviceExe`" --configure-firewall" -ForegroundColor Yellow
        Write-Host ''
    }
}

Invoke-FirewallReconcile

# -- 5. Start + confirm ---------------------------------------------------------------------------
Start-Service -Name $serviceName
(Get-Service -Name $serviceName).WaitForStatus('Running', [TimeSpan]::FromSeconds(60))
Write-Host "Service is Running. First start does real work (unpack pg-runtime, initdb, store migration, first collection cycle) - give it ~2 minutes." -ForegroundColor Green
Write-Host "Primary log: %ProgramData%\PerformanceMonitorDarling\logs\darling-service_yyyyMMdd.log"

# -- 5b. Optional guided network setup ------------------------------------------------------------
# Runs elevated (this whole script is), so the wizard's restart-to-apply works, and its restart is what
# generates the store TLS cert on the first exposed start. Loopback-only stays the default without -Network.
if ($Network) {
    Write-Host ''
    Write-Host 'Launching the guided network-exposure wizard (--configure-network)...' -ForegroundColor Cyan
    & $serviceExe --configure-network

    # The wizard just rewrote the exposure, so the rules created above no longer match darling.json - a newly
    # exposed endpoint has no rule yet, and one turned back off has a stale one. Re-reconcile (idempotent).
    Write-Host ''
    Write-Host 'Re-applying the firewall rules to match the new exposure...' -ForegroundColor Cyan
    Invoke-FirewallReconcile
}

# -- 6. Viewer shortcuts --------------------------------------------------------------------------
if (-not $NoShortcuts) {
    if (Test-Path $viewerExe) {
        # GetFolderPath returns '' for Desktop/StartMenu whenever there is no loaded user profile - a
        # Windows service, a scheduled task, a CI runner, a remote/SSM session - and Join-Path then
        # throws. Guard it: keep only the targets whose folder resolves, and skip cleanly otherwise.
        $desktop   = [Environment]::GetFolderPath('Desktop')
        $startMenu = [Environment]::GetFolderPath('StartMenu')
        $targets = @()
        if ($desktop)   { $targets += (Join-Path $desktop 'Darling Viewer.lnk') }
        if ($startMenu) { $targets += (Join-Path $startMenu 'Programs\Darling Viewer.lnk') }
        if ($targets.Count -gt 0) {
            $shell = New-Object -ComObject WScript.Shell
            foreach ($lnkPath in $targets) {
                $lnk = $shell.CreateShortcut($lnkPath)
                $lnk.TargetPath = $viewerExe
                $lnk.WorkingDirectory = Split-Path $viewerExe
                $lnk.Description = 'PerformanceMonitor Darling Viewer'
                $lnk.Save()
            }
            Write-Host "Created 'Darling Viewer' shortcuts ($($targets.Count)). Pin to taskbar from the Start Menu entry if wanted."
        }
        else {
            Write-Host 'No interactive Desktop/Start Menu (non-interactive or SYSTEM context) - skipping viewer shortcuts.' -ForegroundColor Yellow
        }
    }
    else {
        Write-Host 'viewer\PerformanceMonitor.Darling.Viewer.exe not found - skipping shortcuts.' -ForegroundColor Yellow
    }
}

Write-Host ''
Write-Host 'Done.' -ForegroundColor Green
