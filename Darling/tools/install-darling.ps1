<#
.SYNOPSIS
Installs (or upgrades) the PerformanceMonitor Darling service from the folder this script sits in,
and creates Desktop + Start Menu shortcuts for the bundled viewer.

.DESCRIPTION
Run from an ELEVATED PowerShell, from the folder you extracted the Darling zip into (the script
installs the service pointing at THAT folder - extract to the final location first, e.g.
C:\Program Files\PerformanceMonitorDarling). What it does, in order:

  1. Verifies elevation, the service exe, and darling.json. When darling.json does not exist it writes a
     minimal one (the bundled PostgreSQL store, no servers) and carries on: the service starts with no
     monitored servers and you add the first one afterwards in the Viewer (#6468). darling.sample.json stays
     beside it as the full reference.
  1a. REFUSES an install tree an ordinary local user can ALREADY write to, before anything - including
     the lock at 1b2 - has run (#4043): the usual case is Authenticated Users: Modify, inherited from
     C:\. A lock closes FURTHER writes; it cannot undo one made before it existed, so this recursively
     checks every file and directory's owner and explicit ACEs and refuses outright, naming the
     principals, unless -AcceptWritableExtraction is passed (a dev loop that extracts somewhere
     deliberately writable).
  1b. REFUSES an install directory the service account can never read: anywhere under a user profile
     (C:\Users\...), or a UNC / mapped-drive path. The service runs as an unprivileged virtual account
     that is neither you nor an administrator, and a profile folder grants nothing to it, so the
     service installs cleanly and then dies at the bundled PostgreSQL's first step (#2185, #2187).
     Extract to a machine-scoped local path such as C:\Program Files\PerformanceMonitorDarling instead.
  1b2. Locks the install folder against ordinary users before anything runs from it (#4034): a folder made
     directly under C:\ otherwise lets any local user replace the service's binaries. Extract into a fresh
     folder and run this straight away; no lock can undo a file swapped before it.
  1c2. REFUSES a first install whose darling.json is an unedited copy of darling.sample.json, because the
     copy still names the sample's example servers (#6475). Runs before the lock at 1b2, so a refusal leaves
     the folder's permissions as they were.
  2. Optional pre-flight: runs `--test-connection` and shows the per-server PASS/FAIL lines
     (continue-or-abort prompt on failure; -SkipPreflight to skip). STOPS (exit 5) instead of prompting when
     the exe prints "doesn't fully support CET": Windows needs its updates (#5651).
  3. Registers the Windows Event Log source 'PerformanceMonitor Darling' (requires elevation -
     the service's own virtual account cannot; without it Event Log diagnostics are silently
     dropped. The file log under %ProgramData%\PerformanceMonitorDarling\logs works regardless).
  4. Creates the service under the NT SERVICE virtual account (NEVER LocalSystem - the bundled
     PostgreSQL refuses to run with administrative privileges), start=auto. If the service
     already exists this is an UPGRADE: it is stopped and its binPath updated in place; your
     darling.json, store data, and credentials are untouched.
  4b. Restricts darling.json to SYSTEM / Administrators / the service account, plus read for the
     local group 'PerformanceMonitor Darling Operators' (the Viewer and the CLI, run without
     elevation). The group is created here, empty: you add each person who should have that access.
     Step 7 offers to add the account signed in to this session (default No), or -AddOperator names one.
     darling.json holds encrypted SQL passwords and the MCP/web access tokens, and an install
     folder under C:\ otherwise inherits read access for BUILTIN\Users.
  4b2. Grants the service account read and execute on the locked install folder, and Modify only on the
     runtime folders it writes (pg-runtime and pg-runtime-prev, plus darling-keys with your own PostgreSQL),
     where it extracts its bundled PostgreSQL and keeps its keys, and re-verifies the lock.
  4b3. Creates the store folder (the parent of postgres.dataDirectory, by default
     %ProgramData%\PerformanceMonitorDarling) with Administrators as owner and a locked ACL before
     the service first runs. An ordinary user can otherwise create and own that folder first.
  4c. Creates (or removes) the scoped Windows Firewall rules to match darling.json, via the exe's
     --configure-firewall verb. This requires elevation, which is why it lives here: the service's
     own unprivileged account cannot create them, and only verifies them at runtime.
  5. Starts the service and confirms it reaches Running.
  6. Creates 'Darling Viewer' shortcuts on the public Desktop and in the all-users Start Menu (not the
     Desktop of whichever account elevated, #6475) pointing at
     viewer\PerformanceMonitor.Darling.Viewer.exe. (Taskbar pinning is deliberately not
     attempted - Windows blocks programmatic pinning by design; pin from the Start Menu entry.)
  7. Asks once whether to add the account signed in to this session to the operator group (default
     No; -AddOperator <account> adds one without asking, -NoOperatorPrompt never asks, and a host that
     cannot take an answer counts as No). A Yes also gives that account a direct grant of the group's
     access (read on darling.json and the store's admin and viewer credential files, traverse on the
     store folder) so the Viewer and the CLI work without a sign-out; the service drops that grant when
     it rewrites those permissions at a start, and uninstall-darling.ps1 removes it.

Uninstall with uninstall-darling.ps1 (same folder).

The MSI package (#6480) runs this same script in two phases through -MsiPhase, around the service the package creates and
starts itself: PreStart does steps 1-3 and 4b-4c, PostStart does 5a (an upgrade only) and step 7. The package owns steps 4, 5
and 6, so neither phase runs them, and neither phase asks a question. A machine the MSI installed is refused here without
-MsiPhase: update it with a newer MSI.

.PARAMETER SkipPreflight
Skip the --test-connection probe (2). The service and the viewer\ folder are self-contained, so there is no
.NET runtime check to skip (#6471).

.PARAMETER NoShortcuts
Do not create the viewer shortcuts.

.PARAMETER Network
After the service reaches Running, launch the interactive --configure-network wizard to opt into the
store / MCP / web-dashboard LAN endpoints (guided, delegated validation, comment-preserving darling.json
edit + backup). Off by default; the endpoints stay loopback-only unless you pass this or edit darling.json
by hand.

.PARAMETER AcceptWritableExtraction
Skip the 1a check that refuses an install tree ordinary local users can already write to. That check exists
because a lock (1b2) only stops writes from here on - it cannot undo one made before it ran, so a tree that
was EVER writable by someone besides SYSTEM, Administrators, TrustedInstaller and you may already hold a
swapped binary. Passing this accepts that risk. It exists for a dev loop that extracts into a folder you
know is writable on purpose (a repo working copy, a shared build share) - not for a real install. Extract
under C:\Program Files\<something>, or another folder only an administrator can write to, instead.

.PARAMETER AddOperator
Add this account (DOMAIN\user, or a local user's name) to the local group 'PerformanceMonitor Darling Operators' at the end of
the install without asking (#6478), and give it a direct grant of the same access so the Viewer and the CLI work in the
session that is already open. For a scripted install. Without this parameter and without -NoOperatorPrompt, an interactive
install asks "Add <the signed-in account> to the group? [y/N]" and the answer defaults to No. Ignored on a domain controller,
which has no local groups. Cannot be combined with -NoOperatorPrompt.

.PARAMETER NoOperatorPrompt
Never ask whether to add the signed-in account to the operator group (#6478). The install prints who to add, and that the
person signs out and back in, as it did before the question existed. A host that cannot take an answer (input redirected, or
started with -NonInteractive) is treated the same way without this switch.

.PARAMETER MsiPhase
PreStart or PostStart. For the MSI package's custom actions only (#6480); a person running this by hand does not pass it.
PreStart runs after the package has installed the files and created the service, before it starts it: steps 1 to 3 and 4b to 4c
(locks and permissions, the operator group, the store folder, the firewall rules). PostStart runs after the package has started
the service: 5a (the firewall re-reconcile) on an upgrade only, then step 7. Neither phase creates, stops or starts the service,
creates shortcuts, or asks a question: a question becomes "continue" (the CET refusal at step 2 still stops the install, exit 5).
Under -MsiPhase the script does not treat an existing service as an upgrade, because the package creates the service first;
-MsiUpgrade says which it is. Cannot be combined with -Network.

.PARAMETER MsiUpgrade
With -MsiPhase: the package is replacing an installed Darling (a major upgrade), so the store already exists. PostStart then
re-applies the firewall rules at 5a. Without it a -MsiPhase run is a fresh install, even though the service record exists by then.

.PARAMETER WebDashboard
When this script writes the minimal darling.json (none existed), set web.enabled to true in it, so the web dashboard starts on its
loopback address. Does nothing when darling.json already exists: that file is the operator's.
#>
[CmdletBinding()]
param(
    [switch]$SkipPreflight,
    [switch]$NoShortcuts,
    [switch]$Network,
    [switch]$AcceptWritableExtraction,
    [string]$AddOperator,
    [switch]$NoOperatorPrompt,
    [ValidateSet('PreStart', 'PostStart')]
    [string]$MsiPhase,
    [switch]$MsiUpgrade,
    [switch]$WebDashboard
)

$ErrorActionPreference = 'Stop'
$serviceName = 'PerformanceMonitor Darling'
$root = $PSScriptRoot
$serviceExe = Join-Path $root 'PerformanceMonitor.Darling.Service.exe'
$viewerExe = Join-Path $root 'viewer\PerformanceMonitor.Darling.Viewer.exe'
$configPath = Join-Path $root 'darling.json'
$samplePath = Join-Path $root 'darling.sample.json'

# The local group whose members read darling.json and the store's role credentials without elevation (the
# Viewer and the CLI verbs). It replaces NT AUTHORITY\INTERACTIVE, which put every logged-on user of a shared
# host inside the boundary of a secret that anyone who can read the file can decrypt. The name is the service
# account's name plus a role suffix, like Windows' own "Performance Log Users". It must match
# DarlingFileSecurity.OperatorGroupName in the service project.
$operatorGroupName = 'PerformanceMonitor Darling Operators'

function Fail([string]$message) { Write-Host "ERROR: $message" -ForegroundColor Red; exit 1 }

if ($AddOperator -and $NoOperatorPrompt) { Fail '-AddOperator and -NoOperatorPrompt cannot be used together: one adds the account without asking, the other never asks.' }

# Read-only counterpart to Lock-DarlingInstallTree, for #4043: is $path ALREADY writable by someone outside
# $trusted, right now, before anything is locked or a service account even exists? #4038's lock only stops
# FURTHER writes - it never inspects what is already on disk, so a binary swapped in the gap between
# extracting the zip and running this script survives the lock untouched. This is the check that catches
# that gap, called at 1a on the install root before the lock ever runs.
#
# -Recurse walks every file and directory below $path too (round-1 review, #4043, M2). Checking only the
# root and the service exe missed the case where the root was RE-PERMISSIONED after extraction and the lock
# then erases the evidence: Lock-DarlingInstallTree's own walk resets every child's owner and explicit write
# grants as it goes, so once a tree has been through it once, a child a stranger created or replaced no
# longer shows anything at the root to catch - re-doing that same post-lock verification here, before the
# lock has run, is not redundant, it is the only time this evidence still exists. A descendant's OWNER is
# always checked (ownership is never inherited, so a re-permissioned root cannot paper over who created a
# child), but only its EXPLICIT (non-inherited) write grants are - an inherited ACE just repeats whatever
# $path's own DACL already says, which this function checks on $path directly; re-checking the same fact on
# every descendant of a tree the size of a shipped pg-runtime would cost real time for nothing new to learn.
# A junction or link below $path is never descended into (same rule Lock-DarlingInstallTree's own walk
# applies) and is reported on sight instead - a real install never holds one.
#
# The write-rights bitmask mirrors Lock-DarlingInstallTree's: the named rights (write, append/create, the
# two attribute bits, delete, change-permissions, take-ownership) plus the two generic bits an inherited ACE
# can carry instead of the specific ones. The return is principal NAMES, not SIDs: this fires as a refusal
# an operator has to act on, and 'S-1-5-11' means nothing to most of them, so each is translated back to an
# account or group name where Windows can, and left as the raw SID only when it can't (an orphaned SID from
# a deleted account).
#
# CREATOR OWNER, CREATOR GROUP and their two SERVER twins (S-1-3-0..3) are excluded by SID, not by leaving
# them out of $trusted: they are inherit-only templates that grant nothing on an object that already
# exists, only rights a CHILD inherits when someone later CREATES one under this path - and creating that
# child needs write/create rights on THIS path, which the rest of this check already tests for every other
# principal. Flagging the template ACE itself refused C:\Program Files and C:\Program Files\dotnet on this
# machine (round-1 review, #4043) - the very fix the refusal below recommends, both carrying CREATOR OWNER
# by default.
#
# OWNER RIGHTS (S-1-3-4) is different and is NOT in that exclusion list: unlike the four above it can
# redefine what the OWNER may do instead of the implicit full control an owner otherwise gets, so the ACE
# itself never names the real risk either way - what matters is WHO the owner is. An owner outside $trusted
# holds WRITE_DAC and WRITE_OWNER implicitly and can grant itself anything regardless of what the DACL
# currently says, the same fact Lock-DarlingInstallTree's own post-lock walk already acts on ("owned by").
# So the owner is checked directly here too, rather than trying to read that ACE.
#
# -ServiceSids (#5627) are the service's own accounts (Get-DarlingServiceSids). They are trusted as owner and as
# a write grantee only in the folders and files the service writes (Test-DarlingServiceWrittenPath, one list
# shared with Lock-DarlingInstallTree; -ServiceDirectories are the extra folders Get-DarlingExtraServiceWriteDirectories
# returns for it). Everywhere else in the tree, $path itself included, they are named like
# any other account: the lock gives the service read and execute on $path, so a write right there is not one
# the lock put there. A caller that passes none (a source build, which the service never writes to) trusts
# them nowhere.
#
# The return is the finding TEXTS, or with -AsRecords one object per finding (Kind: Grant, Owner, Link, HardLink,
# Unreadable or Unlisted; Path; Text, the same line the text form returns; IsFile, from the item the walk itself
# listed) for the callers that decide something from what a finding IS, never from its text (#5649).
function Get-UntrustedWriteGrantees([string]$path, [array]$trusted, [switch]$Recurse, [array]$serviceSids = @(), [array]$serviceDirectories = @(), [switch]$AsRecords) {
    $rights = [System.Security.AccessControl.FileSystemRights]
    $allow = [System.Security.AccessControl.AccessControlType]::Allow
    $sidType = [System.Security.Principal.SecurityIdentifier]
    $wk = [System.Security.Principal.WellKnownSidType]
    $inheritOnlyTemplates = @(
        (New-Object System.Security.Principal.SecurityIdentifier($wk::CreatorOwnerSid, $null)),
        (New-Object System.Security.Principal.SecurityIdentifier($wk::CreatorGroupSid, $null)),
        (New-Object System.Security.Principal.SecurityIdentifier($wk::CreatorOwnerServerSid, $null)),
        (New-Object System.Security.Principal.SecurityIdentifier($wk::CreatorGroupServerSid, $null)))
    $write = [int64]($rights::WriteData -bor $rights::AppendData -bor $rights::WriteAttributes -bor $rights::WriteExtendedAttributes -bor $rights::Delete -bor $rights::DeleteSubdirectoriesAndFiles -bor $rights::ChangePermissions -bor $rights::TakeOwnership) -bor 0x10000000 -bor 0x40000000

    function New-DarlingWriteFinding([string]$kind, [string]$findingPath, [string]$text, [bool]$isFile = $false) {
        return [pscustomobject]@{ Kind = $kind; Path = $findingPath; Text = $text; IsFile = $isFile }
    }
    # $item is what the walk listed for $itemPath ($null for $path itself). The file-or-folder answer comes from it,
    # never from a second look at the disk, so the check and the listing cannot disagree (#5649).
    function Get-DarlingOneObjectWriteFindings([string]$itemPath, [bool]$includeInherited, $item = $null) {
        $isFile = [bool]($null -ne $item -and -not $item.PSIsContainer)
        try { $acl = Get-Acl -LiteralPath $itemPath -ErrorAction Stop }
        catch { return @(New-DarlingWriteFinding 'Unreadable' $itemPath "$itemPath (its permissions could not be read: $($_.Exception.Message))" $isFile) }
        $ownerTrusted = $trusted
        $granteeTrusted = $trusted
        if (@($serviceSids).Count -gt 0) {
            if (Test-DarlingServiceWrittenPath $path $itemPath $serviceDirectories $item) { $ownerTrusted = @($trusted) + @($serviceSids); $granteeTrusted = $ownerTrusted }
        }
        $bad = @($acl.GetAccessRules($true, $includeInherited, $sidType) | Where-Object {
            $_.AccessControlType -eq $allow -and (([int64]$_.FileSystemRights) -band $write) -ne 0 -and $granteeTrusted -notcontains $_.IdentityReference -and $inheritOnlyTemplates -notcontains $_.IdentityReference })
        $result = @($bad | ForEach-Object {
            $grantee = $_.IdentityReference
            $name = try { $grantee.Translate([System.Security.Principal.NTAccount]).Value } catch { $grantee.Value }
            New-DarlingWriteFinding 'Grant' $itemPath "$name on $itemPath" $isFile
        })
        $owner = $acl.GetOwner($sidType)
        if ($ownerTrusted -notcontains $owner) {
            $name = try { $owner.Translate([System.Security.Principal.NTAccount]).Value } catch { $owner.Value }
            $result += New-DarlingWriteFinding 'Owner' $itemPath "$itemPath (owned by $name)" $isFile
        }
        return @($result)
    }

    $result = @(Get-DarlingOneObjectWriteFindings $path $true)
    if ($Recurse) {
        # -ErrorVariable, so a folder the walk cannot list is a finding of its own (#4043 round-2 review).
        # Skipped silently, nothing under it reached the checks above and the tree read as clean.
        # Compared in one canonical long spelling (Get-DarlingCanonicalPath), so an install folder handed in as a short path
        # (C:\PROGRA~1\...), with a trailing backslash or in other case is the same folder as the one the listing returns. A
        # path that cannot be worked out is taken to be in the root, so the file is checked rather than passed over (#5649).
        $rootFull = Get-DarlingCanonicalPath $path
        foreach ($child in @(Get-ChildItem -LiteralPath $path -Recurse -Force -ErrorAction SilentlyContinue -ErrorVariable listFailures)) {
            if ($child.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
                $result += New-DarlingWriteFinding 'Link' $child.FullName "$($child.FullName) (a junction or link)"
                continue
            }
            # A file with more than one name (a hard link) shares its permissions and its owner under every name, so
            # one of them in the root with a name the service is trusted on would carry the other name's rights into
            # the lock and the install pass (#5649). Reported like a link, for the root files the service writes.
            # The names are counted through Get-DarlingFileNameCount, never from LinkType, and a count that cannot be read is a
            # hard link here too.
            $childFull = if (-not $child.PSIsContainer -and (Test-DarlingServiceWrittenFileName $child.Name)) { Get-DarlingCanonicalPath $child.FullName } else { $null }
            if (-not $child.PSIsContainer -and (Test-DarlingServiceWrittenFileName $child.Name) -and ($null -eq $rootFull -or $null -eq $childFull -or [IO.Path]::GetDirectoryName($childFull).TrimEnd('\') -ieq $rootFull.TrimEnd('\'))) {
                $nameCount = Get-DarlingFileNameCount $child.FullName
                if ($nameCount -gt 1) {
                    $result += New-DarlingWriteFinding 'HardLink' $child.FullName "$($child.FullName) (a hard link: this file has more than one name, and its permissions are the same under every one)" $true
                }
                elseif ($nameCount -lt 1) {
                    $result += New-DarlingWriteFinding 'HardLink' $child.FullName "$($child.FullName) (a hard link, as far as can be told: the number of names this file has could not be read, and a file with more than one name has the same permissions under every one)" $true
                }
            }
            $result += Get-DarlingOneObjectWriteFindings $child.FullName $false $child
        }
        foreach ($failure in @($listFailures)) {
            $unlisted = if ($failure.TargetObject) { "$($failure.TargetObject)" } else { $path }
            $result += New-DarlingWriteFinding 'Unlisted' $unlisted "$unlisted (its contents could not be listed: $($failure.CategoryInfo.Reason))"
        }
    }
    # Each finding once: a folder made under C:\ inherits BUILTIN\Users as two ACEs (one grants append, one
    # grants write), and without this every caller named that principal twice for the same path.
    # Compared without regard to case, the way Windows compares paths: the same path spelled in two cases is one finding.
    $seen = New-Object 'System.Collections.Generic.HashSet[string]' ([StringComparer]::OrdinalIgnoreCase)
    $unique = @(foreach ($found in $result) { if ($seen.Add($found.Text)) { $found } })
    if ($AsRecords) { return @($unique) }
    return @($unique | ForEach-Object { $_.Text })
}

# Every SID that is a DIRECT member of BUILTIN\Administrators right now (round-1 review, #4043, M2): an
# owner or explicit grantee that IS an administrator, just not the one running this script, is not a
# finding - that account could already do anything on this box, including replacing the very thing this
# check exists to protect, so refusing a tree because a DIFFERENT admin extracted it would only teach
# operators to reach for -AcceptWritableExtraction on sight. Nested and domain group membership are not
# resolved - an Administrators member that is itself a group, or a domain group granted membership by GPO,
# is not expanded - the refusal still NAMES the principal in that case, and -AcceptWritableExtraction is the
# accepted way past it.
#
# ADSI, not Get-LocalGroupMember: the latter throws on an orphaned SID (a member whose account was since
# deleted), which would take down this whole check over one stale membership. WinNT://./Administrators asks
# each member for its objectSid directly, with no name lookup, so an orphaned SID cannot throw here; a
# member that still fails to answer is skipped rather than failing the rest.
#
# $null (not an empty array) means the group itself could not be enumerated at all - a different failure
# from "enumerated fine, zero members" - and the caller trusts NOBODY through this path when that happens
# (fail closed): every principal it would have added stays untranslated by this path and is still named as
# usual by Get-UntrustedWriteGrantees, never silently waved through.
function Get-LocalAdministratorsDirectMemberSids {
    try {
        $group = [ADSI]"WinNT://./Administrators,group"
        $sids = New-Object System.Collections.Generic.List[System.Security.Principal.SecurityIdentifier]
        foreach ($member in @($group.Invoke('Members'))) {
            try {
                $bytes = $member.GetType().InvokeMember('objectSid', 'GetProperty', $null, $member, $null)
                $sids.Add((New-Object System.Security.Principal.SecurityIdentifier($bytes, 0)))
            }
            catch { }
        }
        return , @($sids)
    }
    catch { return $null }
}

# Resolves $account (a service logon name, exactly as Windows reports it) to its SID, or $null when it will
# not translate - a stale or unreachable account (a gMSA whose domain controller cannot be reached on this
# re-run, round-1 review, #4043, L4). Normalizes the two spellings that never translate as written:
# LocalSystem, and a leading .\ meaning this computer. Kept byte-identical in install-darling.ps1 and
# upgrade-darling.ps1.
function Resolve-DarlingServiceAccountSid([string]$account) {
    if (-not $account) { return $null }
    if ($account -eq 'LocalSystem') { $account = 'NT AUTHORITY\SYSTEM' }
    $account = $account -replace '^\.\\', "$env:COMPUTERNAME\"
    try { return (New-Object System.Security.Principal.NTAccount($account)).Translate([System.Security.Principal.SecurityIdentifier]) }
    catch { return $null }
}

# The folders and files the service itself writes inside the install root. ONE list, read by Lock-DarlingInstallTree
# (what it creates, grants and keeps the service's ownership of) and by the pre-lock walk (where the service's own
# account is trusted as owner and as a write grantee), so the two cannot drift apart. Directories are always
# created and granted; KeyDirectories only on a bring-your-own PostgreSQL install (Get-DarlingExtraServiceWriteDirectories
# returns them, and only then are they on the list). Files are matched in the install root only, and only files (a folder of the same name is not on the
# list): darling.json, and every file whose name starts with darling.json.bak- (BackupPrefix, #5649). That is the prefix the hardening covers: the
# install-time pass in install-darling.ps1, the service's TryHardenConfigBackups and the CLI's backup enumeration all
# take every darling.json.bak-* name, not only the darling.json.bak-yyyyMMdd-HHmmss(-N) names the config-editing verbs
# write, so a backup an operator named by hand (darling.json.bak-before-edit) becomes service-owned at the next start
# and must not read as a stranger's file. A backup is data the service reads and writes, never code that runs. This is
# the ONE spelling of that prefix in these scripts, and every PowerShell site tests it with
# StartsWith(prefix, OrdinalIgnoreCase): -like and -Filter fold case by the current culture (Turkish I) and, in Windows
# PowerShell 5.1, match some look-alike characters, so they can disagree with the service. DarlingConfigBackup.Prefix
# in the service is the one in C#, and a test pins the two to the same text. A file with more than one name (a hard
# link) is never one the service writes: the root files this list covers come from Get-DarlingServiceWrittenRootFiles,
# which leaves those out. Nor is a name ending in a dot or a space, which Windows folds into another name (the path test
# refuses those too, and the service's backup test does the same). Kept byte-identical in install-darling.ps1 and upgrade-darling.ps1.
function Get-DarlingServiceWrittenNames {
    return [pscustomobject]@{
        Directories    = @('pg-runtime', 'pg-runtime-prev')
        KeyDirectories = @('darling-keys')
        Files          = @('darling.json')
        BackupPrefix   = 'darling.json.bak-'
    }
}

# True when a file NAME is one of the root files on the list above.
function Test-DarlingServiceWrittenFileName([string]$name) {
    $names = Get-DarlingServiceWrittenNames
    # A name Windows folds into another (a trailing dot or space) is not one the service wrote: it resolves to a DIFFERENT
    # file, and Test-DarlingServiceWrittenPath already refuses it, so the two answer the same (#5649).
    if ([string]::IsNullOrEmpty($name) -or $name.EndsWith('.') -or $name.EndsWith(' ')) { return $false }
    foreach ($file in @($names.Files)) {
        if ([string]::Equals($name, $file, [StringComparison]::OrdinalIgnoreCase)) { return $true }
    }
    return [bool]$name.StartsWith($names.BackupPrefix, [StringComparison]::OrdinalIgnoreCase)
}

# The number of names a file has (1 for an ordinary file, more for a hard link), read through a handle opened for
# attributes only, with every sharing mode allowed and the file itself rather than anything it points to (the way
# DarlingFileSecurity.HardenWithoutFollowingLinks reads it in the service). $item.LinkType is no substitute: Windows
# PowerShell 5.1 leaves it empty when the file cannot be opened for read (another process holds it with no read
# sharing, or this account is denied read), so a hard link read as no link at all. Returns -1 when the count cannot be
# read, and every caller treats that as more than one name: a file whose names cannot be counted is never trusted, made
# service-owned, granted anything or reset (#5649). Kept byte-identical in install-darling.ps1 and upgrade-darling.ps1.
function Get-DarlingFileNameCount([string]$path) {
    try {
        if (-not ('DarlingNativeFileNames' -as [type])) {
            Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

public static class DarlingNativeFileNames
{
    [StructLayout(LayoutKind.Sequential)]
    private struct FileTimeValue { public uint Low; public uint High; }

    [StructLayout(LayoutKind.Sequential)]
    private struct ByHandleFileInformation
    {
        public uint FileAttributes;
        public FileTimeValue CreationTime;
        public FileTimeValue LastAccessTime;
        public FileTimeValue LastWriteTime;
        public uint VolumeSerialNumber;
        public uint FileSizeHigh;
        public uint FileSizeLow;
        public uint NumberOfLinks;
        public uint FileIndexHigh;
        public uint FileIndexLow;
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern SafeFileHandle CreateFileW(string fileName, uint desiredAccess, uint shareMode, IntPtr securityAttributes, uint creationDisposition, uint flagsAndAttributes, IntPtr templateFile);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool GetFileInformationByHandle(SafeFileHandle file, out ByHandleFileInformation information);

    // FILE_READ_ATTRIBUTES; share read, write and delete; OPEN_EXISTING; FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS.
    public static int CountNames(string path)
    {
        using (SafeFileHandle handle = CreateFileW(path, 0x80, 7, IntPtr.Zero, 3, 0x00200000 | 0x02000000, IntPtr.Zero))
        {
            ByHandleFileInformation information;
            if (handle.IsInvalid || !GetFileInformationByHandle(handle, out information)) { return -1; }
            return (int)information.NumberOfLinks;
        }
    }
}
'@
        }
        return [int][DarlingNativeFileNames]::CountNames($path)
    }
    catch { return -1 }
}

# Sets the owner of one file or folder to the SID given, only when it has exactly one name and is inside the tree being locked
# (#5677). OpenRoot opens the tree's root folder once, without sharing delete or rename, and keeps that handle open until
# CloseRoot, so no folder above the root can be replaced while the walk runs; the root's real path is read from that handle
# (GetFinalPathNameByHandleW, normalized, by volume GUID). SetOwner opens the object itself rather than anything it points to
# (FILE_FLAG_OPEN_REPARSE_POINT, the way Get-DarlingFileNameCount opens) through a \\?\ path, so a name ending in a dot or a space
# is not folded into another, reads the opened handle's real path and sets nothing unless that is the root's path or begins with
# it and a backslash (ordinal, case-sensitive: both come from the file system), reads the link count from the same handle, and
# only a count of exactly 1 is followed by SetSecurityInfo on that handle for the owner alone. A file with more than one name (a
# hard link) shares one owner under every name, so one whose count is not 1 is never touched. SetOwner returns 0 when the owner
# was set; 1 when the count is not 1 (detail is the count); 2 when it could not be opened and 3 when the count could not be read
# (detail is the Win32 error); 4 when the owner could not be set (detail is the error); 5 when the opened object is not inside
# the root, or no root is open, or its real path could not be read (nothing was changed). attributes is the object's attribute
# bits as read from the handle, so a caller can tell a folder from a link without opening it again. EnablePrivileges turns on
# SeTakeOwnershipPrivilege and SeBackupPrivilege (backup semantics let an object that denies Administrators read access open)
# for this process's token, keeps what AdjustTokenPrivileges reports as their previous state and returns false if either could
# not be turned on; RestorePrivileges puts back exactly that previous state. Kept byte-identical in install-darling.ps1 and
# upgrade-darling.ps1.
function Initialize-DarlingNativeOwner {
    if ('DarlingNativeOwner' -as [type]) { return }
    Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

public static class DarlingNativeOwner
{
    [StructLayout(LayoutKind.Sequential, Pack = 4)]
    private struct TokenPrivilege { public uint Count; public long Luid; public uint Attributes; }

    [StructLayout(LayoutKind.Sequential)]
    private struct FileTimeValue { public uint Low; public uint High; }

    [StructLayout(LayoutKind.Sequential)]
    private struct ByHandleFileInformation
    {
        public uint FileAttributes;
        public FileTimeValue CreationTime;
        public FileTimeValue LastAccessTime;
        public FileTimeValue LastWriteTime;
        public uint VolumeSerialNumber;
        public uint FileSizeHigh;
        public uint FileSizeLow;
        public uint NumberOfLinks;
        public uint FileIndexHigh;
        public uint FileIndexLow;
    }

    private static SafeFileHandle rootHandle;
    private static string rootPath;
    private static TokenPrivilege[] previousStates = new TokenPrivilege[0];

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern SafeFileHandle CreateFileW(string fileName, uint desiredAccess, uint shareMode, IntPtr securityAttributes, uint creationDisposition, uint flagsAndAttributes, IntPtr templateFile);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern uint GetFinalPathNameByHandleW(SafeFileHandle file, StringBuilder path, uint length, uint flags);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool GetFileInformationByHandle(SafeFileHandle file, out ByHandleFileInformation information);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern uint SetSecurityInfo(SafeFileHandle handle, int objectType, uint securityInfo, byte[] owner, IntPtr group, IntPtr dacl, IntPtr sacl);

    [DllImport("kernel32.dll")]
    private static extern IntPtr GetCurrentProcess();

    [DllImport("kernel32.dll")]
    private static extern bool CloseHandle(IntPtr handle);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern bool OpenProcessToken(IntPtr process, uint access, out IntPtr token);

    [DllImport("advapi32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern bool LookupPrivilegeValueW(string systemName, string name, out long luid);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern bool AdjustTokenPrivileges(IntPtr token, bool disableAll, ref TokenPrivilege state, uint length, out TokenPrivilege previous, out uint returned);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern bool AdjustTokenPrivileges(IntPtr token, bool disableAll, ref TokenPrivilege state, uint length, IntPtr previous, IntPtr returned);

    // TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY; SE_PRIVILEGE_ENABLED is 2. A privilege the token does not hold is reported as an
    // error. A privilege that was already on changes nothing, so its previous state comes back empty and is not touched again.
    public static bool EnablePrivileges()
    {
        IntPtr token;
        if (!OpenProcessToken(GetCurrentProcess(), 0x28, out token)) { return false; }
        try
        {
            bool all = true;
            System.Collections.Generic.List<TokenPrivilege> saved = new System.Collections.Generic.List<TokenPrivilege>();
            foreach (string name in new string[] { "SeTakeOwnershipPrivilege", "SeBackupPrivilege" })
            {
                TokenPrivilege state = new TokenPrivilege();
                state.Count = 1;
                state.Attributes = 2u;
                if (!LookupPrivilegeValueW(null, name, out state.Luid)) { all = false; continue; }
                TokenPrivilege previous;
                uint returned;
                if (!AdjustTokenPrivileges(token, false, ref state, (uint)Marshal.SizeOf(typeof(TokenPrivilege)), out previous, out returned) || Marshal.GetLastWin32Error() != 0) { all = false; }
                else if (previous.Count > 0) { saved.Add(previous); }
            }
            previousStates = saved.ToArray();
            return all;
        }
        finally { CloseHandle(token); }
    }

    public static bool RestorePrivileges()
    {
        IntPtr token;
        if (!OpenProcessToken(GetCurrentProcess(), 0x28, out token)) { return false; }
        try
        {
            bool all = true;
            foreach (TokenPrivilege previous in previousStates)
            {
                TokenPrivilege state = previous;
                if (!AdjustTokenPrivileges(token, false, ref state, 0, IntPtr.Zero, IntPtr.Zero) || Marshal.GetLastWin32Error() != 0) { all = false; }
            }
            previousStates = new TokenPrivilege[0];
            return all;
        }
        finally { CloseHandle(token); }
    }

    // The \\?\ spelling of an absolute path (\\?\UNC\ for a UNC path), which Windows does not trim or fold.
    private static string ExtendedPath(string path)
    {
        if (path.StartsWith(@"\\?\", StringComparison.Ordinal)) { return path; }
        if (path.StartsWith(@"\\", StringComparison.Ordinal)) { return @"\\?\UNC\" + path.Substring(2); }
        return @"\\?\" + path;
    }

    // FILE_NAME_NORMALIZED (0) | VOLUME_NAME_GUID (1). Null when the path could not be read.
    private static string RealPath(SafeFileHandle handle, out int error)
    {
        error = 0;
        StringBuilder buffer = new StringBuilder(1024);
        uint length = GetFinalPathNameByHandleW(handle, buffer, (uint)buffer.Capacity, 1);
        if (length >= buffer.Capacity)
        {
            buffer = new StringBuilder((int)length + 1);
            length = GetFinalPathNameByHandleW(handle, buffer, (uint)buffer.Capacity, 1);
        }
        if (length == 0 || length >= buffer.Capacity)
        {
            error = Marshal.GetLastWin32Error();
            if (error == 0) { error = 122; }
            return null;
        }
        return buffer.ToString();
    }

    // FILE_READ_ATTRIBUTES; share read and write but not delete; OPEN_EXISTING; FILE_FLAG_BACKUP_SEMANTICS.
    public static bool OpenRoot(string root, out int detail)
    {
        CloseRoot();
        detail = 0;
        SafeFileHandle handle = CreateFileW(ExtendedPath(root), 0x80, 3, IntPtr.Zero, 3, 0x02000000, IntPtr.Zero);
        if (handle.IsInvalid) { detail = Marshal.GetLastWin32Error(); handle.Dispose(); return false; }
        int error;
        string real = RealPath(handle, out error);
        if (real == null) { detail = error; handle.Dispose(); return false; }
        rootHandle = handle;
        rootPath = real;
        return true;
    }

    public static void CloseRoot()
    {
        if (rootHandle != null) { rootHandle.Dispose(); }
        rootHandle = null;
        rootPath = null;
    }

    // FILE_READ_ATTRIBUTES | WRITE_OWNER; share read, write and delete; OPEN_EXISTING;
    // FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS. SE_FILE_OBJECT is 1, OWNER_SECURITY_INFORMATION is 1.
    public static int SetOwner(string path, byte[] ownerSid, out int detail, out uint attributes)
    {
        detail = 0;
        attributes = 0;
        using (SafeFileHandle handle = CreateFileW(ExtendedPath(path), 0x80 | 0x00080000, 7, IntPtr.Zero, 3, 0x00200000 | 0x02000000, IntPtr.Zero))
        {
            if (handle.IsInvalid) { detail = Marshal.GetLastWin32Error(); return 2; }
            int pathError;
            string real = RealPath(handle, out pathError);
            string root = rootPath;
            if (real == null || root == null) { detail = pathError; return 5; }
            string prefix = root.EndsWith(@"\", StringComparison.Ordinal) ? root : root + @"\";
            if (!string.Equals(real, root, StringComparison.Ordinal) && !real.StartsWith(prefix, StringComparison.Ordinal)) { return 5; }
            ByHandleFileInformation information;
            if (!GetFileInformationByHandle(handle, out information)) { detail = Marshal.GetLastWin32Error(); return 3; }
            attributes = information.FileAttributes;
            detail = (int)information.NumberOfLinks;
            if (information.NumberOfLinks != 1) { return 1; }
            uint error = SetSecurityInfo(handle, 1, 1, ownerSid, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero);
            if (error != 0) { detail = (int)error; return 4; }
            return 0;
        }
    }
}
'@
}

# One long, absolute spelling of a path, to compare paths by (#5649): GetFullPath's answer (separators, '.' and '..'
# resolved) with every 8.3 short name in it (C:\PROGRA~1, C:\Users\RUNNER~1) spelled out and the trailing backslash dropped
# (a drive root keeps it). GetFullPath alone is not enough: it spells short names out differently from one runtime to the
# next (Windows PowerShell 5.1 leaves them when part of the path is missing or has a '..' step, PowerShell 7 spells out only
# the part that exists), so a root typed one way and a path listed or joined the other compared unequal, which is how a root
# under a GitHub Windows runner's temp folder (C:\Users\RUNNER~1\...) lost its hard links and its trusted folders. The part
# of the path that exists is spelled out by GetLongPathName and a name below it that does not exist stays as typed. Returns
# $null when no part of the path can be worked out, and every caller treats that as not trusted. The separator it trims and
# joins with is the runtime's, so a backslash is only a separator on Windows. Kept byte-identical in install-darling.ps1 and
# upgrade-darling.ps1.
function Get-DarlingCanonicalPath([string]$path) {
    if ([string]::IsNullOrWhiteSpace($path)) { return $null }
    try {
        $full = [IO.Path]::GetFullPath($path)
        $trimmed = $full.TrimEnd([IO.Path]::DirectorySeparatorChar)
        if ($trimmed.Length -le 2) { return $full }
        if ($trimmed.IndexOf('~') -lt 0) { return $trimmed }
        if (-not ('DarlingNativePaths' -as [type])) {
            Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
using System.Text;

public static class DarlingNativePaths
{
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern uint GetLongPathNameW(string shortPath, StringBuilder longPath, uint bufferLength);

    // The long spelling of a path that exists, or null.
    public static string LongName(string path)
    {
        StringBuilder buffer = new StringBuilder(path.Length + 260);
        uint length = GetLongPathNameW(path, buffer, (uint)buffer.Capacity);
        if (length > buffer.Capacity)
        {
            buffer = new StringBuilder((int)length + 1);
            length = GetLongPathNameW(path, buffer, (uint)buffer.Capacity);
        }
        if (length == 0 || length > buffer.Capacity) { return null; }
        return buffer.ToString();
    }
}
'@
        }
        $below = @()
        $existing = $trimmed
        while ($true) {
            $long = [DarlingNativePaths]::LongName($existing)
            if ($null -ne $long) { break }
            $parent = [IO.Path]::GetDirectoryName($existing)
            if ([string]::IsNullOrEmpty($parent)) { return $null }
            $below = @([IO.Path]::GetFileName($existing)) + $below
            $existing = $parent
        }
        return (@($long.TrimEnd([IO.Path]::DirectorySeparatorChar)) + $below) -join [IO.Path]::DirectorySeparatorChar
    }
    catch { return $null }
}

# The names on the way from $root down to $candidate, as an array, or $null when $candidate is not below $root, is not an
# absolute path, has a name on the way that Windows folds into another (a '.' or '..' step, an empty name, a trailing dot or
# space) or either path cannot be worked out. Both paths go through Get-DarlingCanonicalPath, so a root typed as a short path
# (C:\PROGRA~1\x), with a trailing backslash or in another case gives the same answer as its long spelling. The fold check
# runs on the candidate as it was handed in, because that is the spelling Windows would resolve (GetFullPath folds the
# names away). Kept byte-identical in install-darling.ps1 and upgrade-darling.ps1.
function Get-DarlingSegmentsBelowRoot([string]$root, [string]$candidate) {
    if ([string]::IsNullOrWhiteSpace($candidate) -or $candidate -notmatch '^([A-Za-z]:[\\/]|\\\\)') { return $null }
    $given = @($candidate.TrimEnd('\', '/') -split '[\\/]')
    $first = if ($candidate.StartsWith('\\')) { 2 } else { 1 }
    for ($i = $first; $i -lt $given.Count; $i++) {
        $name = $given[$i]
        if ($name -eq '' -or $name -eq '.' -or $name -eq '..' -or $name.EndsWith('.') -or $name.EndsWith(' ')) { return $null }
    }
    $rootText = Get-DarlingCanonicalPath $root
    $candidateText = Get-DarlingCanonicalPath $candidate
    if ($null -eq $rootText -or $null -eq $candidateText) { return $null }
    $prefix = $rootText.TrimEnd('\') + '\'
    if (-not $candidateText.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase) -or $candidateText.Length -eq $prefix.Length) { return $null }
    return , @($candidateText.Substring($prefix.Length) -split '\\')
}

# The files directly in $root that the service writes (the names on the list above): the ones the lock makes the
# service the owner of, and with -BackupsOnly the darling.json.bak-* ones the install pass hardens. A file with more
# than one name (a hard link) and a file that is a link are left out, so neither gets the service as its owner or a
# FullControl grant: those rights would land on the other names too (#5649). Hidden files are included. Kept
# byte-identical in install-darling.ps1 and upgrade-darling.ps1.
function Get-DarlingServiceWrittenRootFiles([string]$root, [switch]$BackupsOnly) {
    $names = Get-DarlingServiceWrittenNames
    return @(Get-ChildItem -LiteralPath $root -Force -File -ErrorAction SilentlyContinue | Where-Object {
        (Test-DarlingServiceWrittenFileName $_.Name) -and
        (-not $BackupsOnly -or $_.Name.StartsWith($names.BackupPrefix, [StringComparison]::OrdinalIgnoreCase)) -and
        -not ($_.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -and
        (Get-DarlingFileNameCount $_.FullName) -eq 1 })
}

# True when $candidate is a folder on the list above inside $root (or anything below it), or one of the listed
# files directly in $root. $extraDirectories are the key folders Get-DarlingExtraServiceWriteDirectories returns
# for this install, the same ones the lock is handed. The FIRST name below $root is compared exactly, and a name
# Windows would fold into another ('pg-runtime.', a '..' step) matches nothing. The install root itself and every
# other file or folder in it are not on the list. $item is what the caller's own walk listed for $candidate: a
# listed file is trusted by name only when it is a plain file (not a folder, a link or a file with more than one
# name), and with no item a root file is not trusted. Whether it is a file or a folder and whether it is a link are read from
# $item, the item the walk listed; the number of names is read live, because Get-DarlingFileNameCount opens the file (for
# attributes only) each time it is called.
function Test-DarlingServiceWrittenPath([string]$root, [string]$candidate, [string[]]$extraDirectories = @(), $item = $null) {
    $segments = Get-DarlingSegmentsBelowRoot $root $candidate
    if ($null -eq $segments) { return $false }
    $names = Get-DarlingServiceWrittenNames
    foreach ($directory in (@($names.Directories) + @($extraDirectories | Where-Object { $_ }))) {
        if ($segments[0] -ieq $directory) { return $true }
    }
    # Files only (#5649): a folder that carries a listed name is not a file the service writes, and neither is a link or
    # a file with more than one name. The answer comes from the item the walk listed.
    return [bool]($segments.Count -eq 1 -and (Test-DarlingServiceWrittenFileName $segments[0]) -and $null -ne $item -and -not $item.PSIsContainer -and -not ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -and (Get-DarlingFileNameCount $candidate) -eq 1)
}

# The trusted set for Get-UntrustedWriteGrantees BEFORE a service account exists to add to it (#4043):
# SYSTEM, Administrators, TrustedInstaller, whoever is running this script elevated right now, and every
# direct member of BUILTIN\Administrators (round-1 review, M2) - a folder the installing admin's own account
# owns and can write to, or one a DIFFERENT administrator's account owns, is exactly what a normal
# extraction looks like, not a finding. Kept as its own function because install-darling.ps1 and
# upgrade-darling.ps1 both need it and must agree on it. This is the whole set for a folder the service never
# writes to (a source build), and the base set for the install folder, where Get-DarlingServiceSids adds the
# service's own account for the folders on the Get-DarlingServiceWrittenNames list.
function Get-DarlingPreLockTrustedSids {
    $wk = [System.Security.Principal.WellKnownSidType]
    $sidType = [System.Security.Principal.SecurityIdentifier]
    $trusted = @(
        (New-Object System.Security.Principal.SecurityIdentifier($wk::LocalSystemSid, $null)),
        (New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinAdministratorsSid, $null)),
        (New-Object System.Security.Principal.NTAccount('NT SERVICE\TrustedInstaller')).Translate($sidType),
        [Security.Principal.WindowsIdentity]::GetCurrent().User)
    $adminMembers = Get-LocalAdministratorsDirectMemberSids
    if ($adminMembers) { $trusted += $adminMembers }
    return $trusted
}

# The accounts the service runs as, for Get-UntrustedWriteGrantees -ServiceSids (#5627): the SID this product's
# own service gets, 'NT SERVICE\<serviceName>', whether or not that service is registered right now and whatever
# account it logs on as today, plus $existingServiceAccount, the CURRENT logon account of an ALREADY-REGISTERED
# service, when the caller has one (round-1 review, #4043). A re-run of install-darling.ps1 over a tree #4038
# already locked - a repair, or this script used as its own upgrade path - is not a fresh extraction: the lock
# already granted that account Modify on its folders. The service extracted pg-runtime and owns it by design, and
# that owner stays whoever ran the service WHEN it extracted - which is not the current logon account once the
# service has been deleted, renamed or pointed at another account since. Only OUR service name is derived: no
# other NT SERVICE\* SID, and not the NT SERVICE authority as a whole, so a tree owned by a different service's
# SID is still a finding. The account is resolved through Resolve-DarlingServiceAccountSid; a name that will not
# translate is left out rather than thrown on here - see Get-DarlingPreLockTrustedSidsForRerun for the caller
# that refuses instead of silently dropping it (L4).
function Get-DarlingServiceSids([string]$existingServiceAccount, [string]$serviceName) {
    $sids = @()
    if ($existingServiceAccount) {
        $serviceSid = Resolve-DarlingServiceAccountSid $existingServiceAccount
        if ($serviceSid) { $sids += $serviceSid }
    }
    if ($serviceName) {
        # Computed, not looked up: Windows answers 'NT SERVICE\<name>' only while a service of that name is
        # registered, and the SID that owns pg-runtime outlives the registration. The SID is the one Windows
        # derives from the name (what sc.exe showsid prints): SHA-1 of the upper-cased name in UTF-16LE, its first
        # 20 bytes read as five little-endian integers under S-1-5-80. Windows folds case with its own table and
        # .NET with the invariant culture; they agree for plain ASCII, so a name outside it is not computed.
        try {
            if ($serviceName -notmatch '^[\x20-\x7E]+$') { throw 'the service name is not plain ASCII' }
            $nameHash = [System.Security.Cryptography.SHA1]::Create().ComputeHash([System.Text.Encoding]::Unicode.GetBytes($serviceName.ToUpperInvariant()))
            $nameParts = 0..4 | ForEach-Object { [BitConverter]::ToUInt32($nameHash, $_ * 4) }
            $sids += New-Object System.Security.Principal.SecurityIdentifier("S-1-5-80-$($nameParts -join '-')")
        }
        catch {
            Write-Host "WARNING: could not work out the Windows SID of NT SERVICE\$serviceName ($($_.Exception.Message)), so files that account owns will be listed." -ForegroundColor Yellow
        }
    }
    return $sids
}

# The pre-lock trusted sets for a RE-RUN over a possibly-already-registered service, failing closed with a
# clear message instead of silently mis-trusting or mis-refusing (round-1 review, #4043, L3/L4). Two
# distinct ways the existing service's account can defeat the pre-lock check if let through quietly:
# Windows cannot say what it is AT ALL (L3 - Get-DarlingServiceLogonName returns nothing; this is 1b2's own
# failure, reached here first and worded the same way so it is recognisable as the same problem), or it
# names an account that will not translate to a SID (L4 - most often a gMSA whose domain controller this
# box cannot reach right now). Either one, left unresolved, makes the lock's own grant to that account look
# exactly like a stranger's - a misleading "ordinary users can already write here" refusal that sends an
# operator to re-extract a perfectly good install, or worse, trains them to reach for
# -AcceptWritableExtraction on sight.
#
# Returns Trusted (the base set, for Get-UntrustedWriteGrantees) and ServiceSids (for its -ServiceSids).
# $existingService is the Get-Service result (or $null) the caller already has, not re-queried here, so a
# caller that already asked does not ask Windows the same question again on every call. Kept byte-identical
# in install-darling.ps1 and upgrade-darling.ps1.
function Get-DarlingPreLockTrustedSidsForRerun([string]$serviceName, $existingService) {
    if (-not $existingService) {
        return [pscustomobject]@{ Trusted = @(Get-DarlingPreLockTrustedSids); ServiceSids = @(Get-DarlingServiceSids $null $serviceName) }
    }

    $existingAccount = Get-DarlingServiceLogonName $serviceName
    if (-not $existingAccount) {
        Fail "Could not read which account the existing '$serviceName' service logs on as, so the install folder cannot be locked without taking that account's access away. Nothing was changed. Check it with: sc.exe qc `"$serviceName`", then re-run this script."
    }
    if (-not (Resolve-DarlingServiceAccountSid $existingAccount)) {
        Fail "The existing '$serviceName' service logs on as '$existingAccount', which could not be resolved to a SID right now (its domain may be unreachable, or the account may no longer exist). The install folder cannot be safely checked or locked without knowing whether its own grant belongs to that account. Verify the account is reachable, then re-run this script."
    }
    return [pscustomobject]@{ Trusted = @(Get-DarlingPreLockTrustedSids); ServiceSids = @(Get-DarlingServiceSids $existingAccount $serviceName) }
}

# The absolute paths darling.json names that sit inside $folder (#5627): the store (postgres.dataDirectory), a
# certificate or key (a tls pfxPath, certPath or keyPath) and any 'file:' reference. Returns Lines, each one
# '"path" (what names it)', and Incomplete, true when a value was left out or the list was cut. A regex over the
# comment-stripped text, as Get-DarlingExtraServiceWriteDirectories does, because Windows PowerShell 5.1's
# ConvertFrom-Json rejects comments and trailing commas. Each value is read on its own, so one that cannot be read as a path
# is left out and the rest still print: a value holding a control character, a line break or any of < > | " is
# left out, so is one over 260 characters, and a value that starts with two slashes or two backslashes (a network share
# or device path) is not looked up and counts as Incomplete, and at most 10 are listed. A darling.json that cannot be read counts
# as Incomplete; a missing one names nothing. Kept byte-identical in install-darling.ps1 and
# upgrade-darling.ps1.
function Get-DarlingConfiguredPathsUnder([string]$folder) {
    $config = Join-Path $folder 'darling.json'
    if (-not (Test-Path -LiteralPath $config -PathType Leaf)) { return [pscustomobject]@{ Lines = @(); Incomplete = $false } }
    try { $text = Get-Content -LiteralPath $config -Raw -ErrorAction Stop }
    catch { return [pscustomobject]@{ Lines = @(); Incomplete = $true } }
    $stripped = ($text -split "`r?`n" | ForEach-Object { $_ -replace '(?<!:)//.*$', '' }) -join "`n"
    $named = @()
    foreach ($match in [regex]::Matches($stripped, '"(dataDirectory|pfxPath|certPath|keyPath)"\s*:\s*"((?:[^"\\]|\\.)*)"', 'IgnoreCase')) {
        $what = if ($match.Groups[1].Value -ieq 'dataDirectory') { 'postgres.dataDirectory' } else { $match.Groups[1].Value }
        $named += , @($what, $match.Groups[2].Value)
    }
    foreach ($match in [regex]::Matches($stripped, '"file:\s*((?:[^"\\]|\\.)*)"')) {
        $named += , @('a file: reference', $match.Groups[1].Value)
    }
    $found = @()
    $incomplete = $false
    foreach ($pair in $named) {
        try {
            $value = ($pair[1] -replace '\\\\', '\').Trim()
            if ($value.Length -gt 260 -or $value -match '[\p{C}\p{Zl}\p{Zp}<>|"]') { $incomplete = $true; continue }
            if ($value -match '^(\\\\|//)') { $incomplete = $true; continue }
            if ($value -and [IO.Path]::IsPathRooted($value) -and (Test-PathIsAtOrUnder $value $folder)) {
                $found += "`"$value`" ($($pair[0]))"
            }
        }
        catch { $incomplete = $true }
    }
    $found = @($found | Select-Object -Unique)
    if ($found.Count -gt 10) { $found = @($found | Select-Object -First 10); $incomplete = $true }
    return [pscustomobject]@{ Lines = $found; Incomplete = $incomplete }
}

# The command that runs the service exe's own TLS key file verb by hand (#5700), with the darling.json it is to check. Kept
# byte-identical in install-darling.ps1 and upgrade-darling.ps1.
function Get-DarlingTlsKeyFileVerbCommand([string]$exe, [string]$configPath) {
    return "& `"$exe`" --harden-files --tls-key-files-only --config `"$configPath`""
}

# Runs the service exe's own TLS key file verb (#5700): `<exe> --harden-files --tls-key-files-only --config <configPath>`. The exe is
# the one place that reads darling.json for the key files it names (a pfxPath or keyPath under the web and mcp tls blocks) and sets
# their permissions; this script reads no key path and sets no key permission itself. $configPath is the darling.json in the
# install folder, the one the service itself reads, so a DARLING_CONFIG in this session cannot point the verb at another file. The
# verb prints one line per file, which this echoes. Every line that starts with two spaces is a status line (SECURED, or one of
# REFUSED, SKIPPED, FAILED, MISSING, NOT CHANGED, STILL READABLE, LOCKED OUT, OTHER ACCESS, UNTRUSTED OWNER, and any status the verb gains
# later), and every one but SECURED is written as a warning, even when the exit code is 0. The `Config file:` line, the
# parenthesised DARLING_CONFIG note and the `No TLS key files` line stay plain output. The exit code is 0 when it ran, whatever
# it reported per file, and nonzero only when it could not run.
# Returns $null when it ran, otherwise the sentence to warn with, which carries the verb to run by hand. It never throws, so an
# install or upgrade is never failed over it. Kept byte-identical in install-darling.ps1 and upgrade-darling.ps1.
function Invoke-DarlingTlsKeyFileHardening([string]$exe, [string]$configPath) {
    $priorErrorAction = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $reason = $null
    try {
        & $exe --harden-files --tls-key-files-only --config $configPath 2>&1 | ForEach-Object {
            $line = "$_"
            if ($line -match '^  ' -and $line -notmatch '^  (SECURED  |\()') { Write-Warning $line }
            else { Write-Host $line }
        }
        if ($LASTEXITCODE -ne 0) { $reason = "exit code $LASTEXITCODE" }
    }
    catch { $reason = $_.Exception.Message }
    finally { $ErrorActionPreference = $priorErrorAction }
    if ($null -eq $reason) { return $null }
    return "The service could not check the TLS key files darling.json names ($reason), so their permissions were not changed. Run from an elevated prompt: $(Get-DarlingTlsKeyFileVerbCommand $exe $configPath)"
}

# Where the new zip goes (#5627). Under C:\Program Files already, the folder to use is a different one.
function Get-DarlingNewFolderPhrase([string]$oldFolder) {
    if (Test-PathIsAtOrUnder $oldFolder $env:ProgramFiles) { return 'a new, empty folder with a different name under C:\Program Files.' }
    return 'a new, empty folder named C:\Program Files\PerformanceMonitorDarling. If that name is taken, use another new name under C:\Program Files.'
}

# The numbered steps for replacing the folder of an EXISTING install whose pre-lock check refused (#5627), for a
# service that runs from that folder: the service, its darling.json and its store are kept and only the program folder
# is replaced. Everything the steps rely on was read from install-darling.ps1: the service is re-pointed in place (only
# its binary path changes, its logon account and credentials are left alone), darling.json is the one file the zip does
# not ship that an install needs (without it the sample config is copied in), pg-runtime comes back from the new
# zip's pg-runtime.zip, and the store lives under %ProgramData%\PerformanceMonitorDarling unless darling.json sets
# postgres.dataDirectory. Anything darling.json names inside the old folder is listed in the last step, so the
# folder is deleted only after it has been moved; when a value could not be listed, the step says to check darling.json
# for paths inside the folder. Kept byte-identical in install-darling.ps1 and upgrade-darling.ps1.
function Get-DarlingExistingInstallSteps([string]$oldFolder, [string]$serviceName) {
    $newFolder = Get-DarlingNewFolderPhrase $oldFolder
    $configured = Get-DarlingConfiguredPathsUnder $oldFolder
    $inside = @($configured.Lines)
    $last = if ($inside.Count -gt 0) {
        $list = ($inside | ForEach-Object { "       $_." }) -join "`n"
        $unlisted = if ($configured.Incomplete) { "`n     Not every value in darling.json was listed here. Check it for other paths inside $oldFolder." } else { '' }
        "  6. Do not delete $oldFolder yet. darling.json names these paths inside it:`n$list$unlisted`n     Move each one to a folder outside it, and update darling.json to the new place.`n     Then, once the service is collecting, delete $oldFolder."
    }
    elseif ($configured.Incomplete) {
        "  6. Do not delete $oldFolder yet. Not every value in darling.json was listed here. Check it for paths`n     inside $oldFolder. Move each one to a folder outside it, and update darling.json to the new place.`n     Then, once the service is collecting, delete $oldFolder."
    }
    else {
        "  6. Delete $oldFolder once the service is collecting."
    }
    return @"
This folder already holds an install, so the installed folder itself needs replacing. Your service, your
darling.json and your store are kept. To move to a new folder:

  1. Stop the '$serviceName' service and leave it stopped until step 5.
  2. In an elevated session, extract the new zip into
     $newFolder
  3. Open darling.json in $oldFolder
     and check that every setting in it is yours. Then copy it into the new folder before anything else,
     and go straight on to step 4. Without it, install-darling.ps1 copies the SAMPLE config in.
     If darling.json names a certificate or key file in this folder, make a new one. Keep it outside the
     install folder, and point darling.json at it. These are the tls pfxPath, certPath and keyPath settings.
     Do not copy a darling-keys folder: the service makes its own.
     A darling.json.bak-* backup is optional.
  4. In an elevated session, run install-darling.ps1 from the new folder. It points the existing service
     at the new folder. It changes only the service's program path. Its logon account, its credentials and
     its store are not touched. The store is in C:\ProgramData\PerformanceMonitorDarling. If darling.json
     sets postgres.dataDirectory, it is there instead.
  5. Start the service and check that it collects.
$last

Do NOT try to repair this folder's permissions in place.
"@
}

# The one-line PowerShell command that extracts the zip this folder came from straight into Program Files (#6469), for the
# refusal of a folder the service cannot use. The zip is the one beside the extract folder with the folder's name, which is
# how Explorer's "Extract All" names its folder; with none there, a placeholder says what to put.
function Get-DarlingExtractCommand([string]$extractedFolder) {
    $destination = Join-Path $env:ProgramFiles 'PerformanceMonitorDarling'
    $zipPath = '<path to the Darling zip you downloaded>'
    try {
        $trimmed = $extractedFolder.TrimEnd('\')
        $parent = Split-Path -Parent $trimmed
        $leaf = Split-Path -Leaf $trimmed
        if ($parent -and $leaf) {
            $candidate = Join-Path $parent ($leaf + '.zip')
            if (Test-Path -LiteralPath $candidate -PathType Leaf) { $zipPath = $candidate }
        }
    }
    catch { $null = $_ }
    return "Expand-Archive -LiteralPath '" + $zipPath.Replace("'", "''") + "' -DestinationPath '" + $destination.Replace("'", "''") + "'"
}

# The refusal's advice when the service is NOT registered, or runs from some other folder, so this folder is not
# an install to keep (#5627): a fresh folder. Kept byte-identical in install-darling.ps1 and upgrade-darling.ps1.
function Get-DarlingFreshFolderAdvice([string]$folder) {
    $where = if (Test-PathIsAtOrUnder $folder $env:ProgramFiles) { 'a new, empty folder with a different name under C:\Program Files' } else { 'a new folder under C:\Program Files, or another folder only an administrator can write to' }
    return @"
Do NOT try to repair this folder's permissions in place. Extract the zip into $where.
Then run install-darling.ps1 from there.
"@
}

# The file names, when EVERY finding of a refusal is on a darling.json.bak-* file directly in $root (#5649). $findings
# are the records Get-UntrustedWriteGrantees returns with -AsRecords, and the decision is made from each record's Kind
# and Path, never from its text: only a Grant or an Owner finding on a file (as the walk listed it) sitting directly in
# $root with a backup name counts. Anything else among the findings, a folder, a link, a file with more than one
# name, a file that is not a backup, a finding about a path that could not be read or listed, returns nothing, and
# the refusal keeps its standard advice alone. Kept byte-identical in install-darling.ps1 and upgrade-darling.ps1.
function Get-DarlingBackupFileFindingNames([string]$root, [array]$findings) {
    if (@($findings).Count -eq 0) { return @() }
    $names = @()
    foreach ($finding in @($findings)) {
        if ($finding.Kind -ne 'Grant' -and $finding.Kind -ne 'Owner') { return @() }
        if (-not $finding.IsFile) { return @() }
        $segments = Get-DarlingSegmentsBelowRoot $root "$($finding.Path)"
        if ($null -eq $segments -or $segments.Count -ne 1) { return @() }
        $leaf = $segments[0]
        if (-not (Test-DarlingServiceWrittenFileName $leaf) -or [string]::Equals($leaf, 'darling.json', [StringComparison]::OrdinalIgnoreCase)) { return @() }
        if ($names -notcontains $leaf) { $names += $leaf }
    }
    return @($names)
}

# The note a refusal that lists only darling.json.bak-* files carries ahead of its standard advice (#5649). With the
# service's own backups trusted, a file listed here belongs to, or is writable by, some other account. Kept
# byte-identical in install-darling.ps1 and upgrade-darling.ps1.
function Get-DarlingBackupFileAdvice([string[]]$names) {
    $shown = (@($names) | Select-Object -First 10) -join ', '
    $more = if (@($names).Count -gt 10) { " and $(@($names).Count - 10) more" } else { '' }
    return @"
Every entry above is a darling.json.bak-* backup file in this folder: $shown$more.
A backup can hold the secrets that were in darling.json. These files can be moved out of this folder, into a
folder only Administrators can read.
"@
}

# The refusal's advice (#5627): the numbered steps when the registered service runs from $folder itself, that is,
# when its ImagePath folder ($registeredFolder, from Get-DarlingInstallRootFromService) is $folder, so this folder IS
# the install being replaced; a fresh folder otherwise, including when no service is registered or its path cannot be
# read. When every finding is a backup file (Get-DarlingBackupFileFindingNames), the note on those files comes first
# and the advice above follows it unchanged (#5649). $findings are the records from Get-UntrustedWriteGrantees
# -AsRecords. Kept byte-identical in install-darling.ps1 and upgrade-darling.ps1.
function Get-DarlingRefusalAdvice([string]$folder, [string]$serviceName, [string]$registeredFolder, [array]$findings = @()) {
    $default = if ($registeredFolder -and (Test-DarlingSamePath $registeredFolder $folder)) { Get-DarlingExistingInstallSteps $folder $serviceName } else { Get-DarlingFreshFolderAdvice $folder }
    $backupNames = @(Get-DarlingBackupFileFindingNames $folder $findings)
    if ($backupNames.Count -gt 0) { return (Get-DarlingBackupFileAdvice $backupNames) + "`n`n" + $default }
    return $default
}

# True when two paths name the same directory: both go through Get-DarlingCanonicalPath (#5673), so a folder typed as a
# short path (C:\PROGRA~1\x), with a trailing backslash or in another case gives the same answer as its long spelling, and
# the two are then compared without regard to case. A path Get-DarlingCanonicalPath cannot work out is compared as
# GetFullPath spells it. The separator comes from the runtime rather than a hardcoded backslash, so the rule can be run off
# Windows. Kept byte-identical in install-darling.ps1 and upgrade-darling.ps1.
function Test-DarlingSamePath([string]$left, [string]$right) {
    if ([string]::IsNullOrWhiteSpace($left) -or [string]::IsNullOrWhiteSpace($right)) { return $false }

    $sep = [IO.Path]::DirectorySeparatorChar
    $alt = [IO.Path]::AltDirectorySeparatorChar

    try {
        # The canonical long spelling (#5673); GetFullPath's answer for a path it cannot work out.
        $l = Get-DarlingCanonicalPath $left
        if ($null -eq $l) { $l = [IO.Path]::GetFullPath($left) }
        $r = Get-DarlingCanonicalPath $right
        if ($null -eq $r) { $r = [IO.Path]::GetFullPath($right) }
        $l = $l.TrimEnd($sep, $alt)
        $r = $r.TrimEnd($sep, $alt)
    }
    catch {
        return $false
    }

    return $l.Equals($r, [StringComparison]::OrdinalIgnoreCase)
}

# Where the service is ACTUALLY installed, read from the registered ImagePath. Returns $null when the service is not
# installed or its path cannot be parsed. The ImagePath is quoted when it contains spaces, so both spellings are
# handled. Kept byte-identical in install-darling.ps1 and upgrade-darling.ps1.
function Get-DarlingInstallRootFromService([string]$name) {
    try {
        $service = Get-CimInstance -ClassName Win32_Service -Filter "Name='$name'" -ErrorAction Stop
    }
    catch {
        return $null
    }

    if (-not $service) { return $null }

    $imagePath = $service.PathName
    if ([string]::IsNullOrWhiteSpace($imagePath)) { return $null }

    $imagePath = $imagePath.Trim()
    if ($imagePath.StartsWith('"')) {
        $close = $imagePath.IndexOf('"', 1)
        if ($close -gt 1) { $imagePath = $imagePath.Substring(1, $close - 1) }
    }
    else {
        # An unquoted path with arguments after it: everything up to the .exe is the executable.
        $exe = $imagePath.IndexOf('.exe', [StringComparison]::OrdinalIgnoreCase)
        if ($exe -ge 0) { $imagePath = $imagePath.Substring(0, $exe + 4) }
    }

    try { return [IO.Path]::GetDirectoryName([IO.Path]::GetFullPath($imagePath)) }
    catch { return $null }
}

# The folder with every junction and link on the way to it followed (#5673): the final path of the deepest part of $path
# that exists, with the names below it that do not exist yet put back after it. A junction or link that leads into a
# profile folder is under that profile whatever it is called, so the profile test is run on this spelling as well as the
# typed one. Returns $null when no part of the path can be opened. Only in install-darling.ps1.
function Get-DarlingResolvedFolder([string]$path) {
    if ([string]::IsNullOrWhiteSpace($path)) { return $null }
    try {
        if (-not ('DarlingFinalPath' -as [type])) {
            Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
using System.Text;

public static class DarlingFinalPath
{
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern IntPtr CreateFileW(string name, uint access, uint share, IntPtr security, uint disposition, uint flags, IntPtr template);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern uint GetFinalPathNameByHandleW(IntPtr handle, StringBuilder path, uint length, uint flags);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool CloseHandle(IntPtr handle);

    // The final path (drive-letter form, links followed) of an existing folder, or null.
    public static string Of(string folder)
    {
        IntPtr handle = CreateFileW(folder, 0x80, 7, IntPtr.Zero, 3, 0x02000000, IntPtr.Zero);
        if (handle == new IntPtr(-1)) { return null; }
        try
        {
            StringBuilder buffer = new StringBuilder(1024);
            uint length = GetFinalPathNameByHandleW(handle, buffer, (uint)buffer.Capacity, 0);
            if (length >= buffer.Capacity)
            {
                buffer = new StringBuilder((int)length + 1);
                length = GetFinalPathNameByHandleW(handle, buffer, (uint)buffer.Capacity, 0);
            }
            if (length == 0 || length >= buffer.Capacity) { return null; }
            return buffer.ToString();
        }
        finally { CloseHandle(handle); }
    }
}
'@
        }
        $existing = [IO.Path]::GetFullPath($path)
        if ($existing.Length -gt 3) { $existing = $existing.TrimEnd('\') }
        $below = @()
        while ($true) {
            $real = [DarlingFinalPath]::Of($existing)
            if ($null -ne $real) { break }
            $parent = [IO.Path]::GetDirectoryName($existing)
            if ([string]::IsNullOrEmpty($parent)) { return $null }
            $below = @([IO.Path]::GetFileName($existing)) + $below
            $existing = $parent
        }
        return (@((Convert-ExtendedLengthPath $real).TrimEnd('\')) + $below) -join '\'
    }
    catch { return $null }
}

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
        # Normalizes separators, resolves . and .., spells 8.3 short names out and makes the comparison
        # independent of how the path was typed (Get-DarlingCanonicalPath, #5673). A path it cannot work out is
        # compared as GetFullPath spells it, and anything GetFullPath rejects is not a path we can reason about,
        # so it is not matched.
        $c = Get-DarlingCanonicalPath $candidate
        if ($null -eq $c) { $c = [IO.Path]::GetFullPath($candidate) }
        $p = Get-DarlingCanonicalPath $parent
        if ($null -eq $p) { $p = [IO.Path]::GetFullPath($parent) }
        $c = $c.TrimEnd('\')
        $p = $p.TrimEnd('\')
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

# The logon account the Darling service is registered to run as, exactly as Windows reports it (LocalSystem,
# .\name and NT SERVICE\... spellings included), or $null when neither Win32_Service nor sc.exe qc can say.
# sc.exe qc is the non-WMI second opinion, so a broken WMI repository alone cannot hide the account; its field
# labels are localized, so a non-match there returns $null for the caller to refuse on, never a guess. Kept
# byte-identical in install-darling.ps1 and upgrade-darling.ps1, which DarlingInstallLocationTests compares.
function Get-DarlingServiceLogonName([string]$name) {
    try {
        $startName = (Get-CimInstance -ClassName Win32_Service -Filter "Name='$name'" -ErrorAction Stop).StartName
        if ($startName) { return $startName }
    }
    catch {
        Write-Host "Get-CimInstance could not read the service's logon account ($($_.Exception.Message)) - falling back to sc.exe qc." -ForegroundColor Yellow
    }
    $qc = & sc.exe qc $name 2>$null
    if ($LASTEXITCODE -eq 0) {
        $match = $qc | Select-String -Pattern '^\s*SERVICE_START_NAME\s*:\s*(\S.*?)\s*$'
        if ($match) { return $match.Matches[0].Groups[1].Value }
    }
    return $null
}

# Lock the install tree against ordinary users (#4034). The tree holds the service exe, its DLLs and pg-runtime,
# all of which run as the service account, and the scripts an administrator runs elevated. A folder created
# directly under C:\ - the documented location - inherits "Authenticated Users: Modify" from the volume root,
# so any local user could replace a binary and run code as the service. This stops the root inheriting, removes
# every broad principal (Authenticated Users, Users, Everyone, INTERACTIVE), and grants back BUILTIN\Users read
# and execute, plus, when $serviceAccount is given, the service account Modify: it held that before through
# Authenticated Users and still needs it, because it extracts pg-runtime into the tree. With no account it only
# locks. install-darling.ps1 calls it that way before anything runs from the tree, and again once the service
# exists, to make the grant.
#
# Removing the broad groups is not enough on its own (#4038's review). Whoever OWNS an object can rewrite its
# DACL, and a child that grants some specific account write keeps that grant through a folder lock: a local
# user who created the folder in advance, or re-created a file while an older install was open, would still
# control it. So the whole tree is given to Administrators as owner (darling.json and its backups go back to
# the service account, which install-darling.ps1 step 4b makes their owner), every explicit write grant held
# by an account outside the trusted set (SYSTEM, Administrators, TrustedInstaller, CREATOR OWNER and the
# service) is closed where it stands, and the walk reports any owner or writer outside that set that remains.
#
# The account is normalized the way install-darling.ps1 reads it from the service (LocalSystem is NT
# AUTHORITY\SYSTEM, a leading .\ is this computer), since neither spelling translates to a SID as written.
# Returns what is still open, one string per path: a writer or owner outside the trusted set, a junction or
# link below the root (the walk does not descend it and a real install never holds one), or a root that is
# itself a junction (the lock would change the link, not the folder it points to). Kept byte-identical in
# install-darling.ps1 and upgrade-darling.ps1, which DarlingInstallLocationTests compares.
#
# NARROWED (#4052): the service only ever writes under pg-runtime\ and pg-runtime-prev\ (it extracts the
# bundled runtime there and rescues the previous one on an update - see DarlingManagedPostgres and
# DarlingStoreUpgrade, whose stamp/blocked files (pg-runtime.sha256/.stamp/.blocked) all live INSIDE
# pg-runtime\, never beside it). So the service account gets Read & Execute on $root - it still needs to
# run its own exe and load its own DLLs - and Modify is granted ONLY on pg-runtime\, pg-runtime-prev\, and
# whatever $extraServiceDirectories names (the bring-your-own-Postgres log-hash-key folder,
# DarlingLogHashKeyFile.BringYourOwnDirectoryName, when that mode is configured - #4052's resolution of
# #4050's open write-site row). Every one of those directories is created ahead of time if missing, so the
# grant has something to land on and the first extraction never has to create its own root-owned folder.

# What $extraServiceDirectories should hold for a given install root (#4052): the bring-your-own-Postgres
# log-hash-key folder, DarlingLogHashKeyFile.BringYourOwnDirectoryName ('darling-keys'), beside darling.json -
# but ONLY when that install is configured for it (postgres.managed = false). A managed install's log-hash key
# lives inside the credential directory pg-runtime already covers, so granting a second directory there would
# widen the service's Modify footprint for nothing it writes.
#
# darling.json is JSONC (comments, trailing commas) and the file this runs against may not exist yet (a fresh
# extraction, or an operator who has not copied darling.sample.json over) or may not parse (hand-edited,
# mid-write) - none of those is this function's problem to solve, and guessing wrong here would silently drop
# the service's Modify grant on its own key folder in BYO mode. So every failure to read or parse falls back to
# treating the install as managed (no extra directory): that is always safe, because a BYO install with the
# grant missing is caught and reported by the same untrusted-write-grantee walk that catches everything else -
# an operator sees a finding on darling-keys\ and re-runs after fixing darling.json, rather than the lock
# silently widening access on a config it could not trust. A regex, not ConvertFrom-Json: Windows PowerShell
# 5.1's ConvertFrom-Json rejects comments and trailing commas outright, and this only ever needs one boolean
# out of one well-known key - not a general JSONC parser. It matches the LAST 'managed' key at the top level's
# "postgres" object width (a value of false explicitly disables managed mode; true or absent are both managed).
function Get-DarlingExtraServiceWriteDirectories([string]$root, [string]$configPath) {
    if (-not (Test-Path -LiteralPath $configPath -PathType Leaf)) { return @() }
    try { $text = Get-Content -LiteralPath $configPath -Raw -ErrorAction Stop }
    catch { return @() }
    # Strip // line comments (never inside a string in this file's own shipped shape) before matching, so a
    # commented-out "managed": false example above the real key can never be mistaken for it.
    $stripped = ($text -split "`r?`n" | ForEach-Object { $_ -replace '(?<!:)//.*$', '' }) -join "`n"
    $isManaged = $true
    $match = [regex]::Matches($stripped, '"managed"\s*:\s*(true|false)')
    if ($match.Count -gt 0) { $isManaged = ($match[$match.Count - 1].Groups[1].Value -eq 'true') }
    if ($isManaged) { return @() }
    # A NAME under the install root, not a path: Lock-DarlingInstallTree Join-Paths it onto the root, and Join-Path does not
    # treat a rooted child as rooted ('C:\a' + 'C:\a\b' = 'C:\a\C:\a\b'). A darling.json outside the root puts the key
    # folder outside the tree the lock covers, so it needs no grant from here.
    $configDir = Get-DarlingCanonicalPath ([System.IO.Path]::GetDirectoryName($configPath))   # Split-Path -LiteralPath -Parent is ambiguous on PowerShell 5.1
    $rootCanonical = Get-DarlingCanonicalPath $root
    if ($null -eq $configDir -or $null -eq $rootCanonical -or $configDir.TrimEnd('\') -ine $rootCanonical.TrimEnd('\')) { return @() }
    return @((Get-DarlingServiceWrittenNames).KeyDirectories)
}

function Lock-DarlingInstallTree([string]$root, [string]$serviceAccount, [string[]]$extraServiceDirectories = @()) {
    # icacls reports a file it could not change on stderr (under /C it carries on), and Windows PowerShell 5.1
    # turns redirected native stderr into a TERMINATING error when the preference is Stop, as both scripts set
    # it. So this function judges icacls by its exit code and output, and reports an unreadable object rather
    # than throwing out of the whole lock. The preference is local to this function.
    $ErrorActionPreference = 'Continue'
    # Every path below is compared with the listing's own spelling, which is the long one, so the root is put in that
    # spelling first: handed in as a short path (C:\PROGRA~1\...), the root file the service owns would not be recognised as
    # one directly in it and the service's own grant on it would be stripped (#5649). One that cannot be worked out is not locked.
    $rootCanonical = Get-DarlingCanonicalPath $root
    if ($null -eq $rootCanonical) { return @("$root (the full path of the install folder could not be worked out, so it was not locked)") }
    $root = $rootCanonical
    if ((Get-Item -LiteralPath $root -Force -ErrorAction Stop).Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
        return @("$root (the install folder itself is a junction or link: run from the folder it points to)")
    }
    $wk = [System.Security.Principal.WellKnownSidType]
    $sidType = [System.Security.Principal.SecurityIdentifier]
    $broad = @(
        (New-Object System.Security.Principal.SecurityIdentifier($wk::AuthenticatedUserSid, $null)),
        (New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinUsersSid, $null)),
        (New-Object System.Security.Principal.SecurityIdentifier($wk::WorldSid, $null)),
        (New-Object System.Security.Principal.SecurityIdentifier($wk::InteractiveSid, $null)))
    $usersSid = New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinUsersSid, $null)
    $adminsSid = New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinAdministratorsSid, $null)
    $trusted = @(
        (New-Object System.Security.Principal.SecurityIdentifier($wk::LocalSystemSid, $null)),
        $adminsSid,
        (New-Object System.Security.Principal.SecurityIdentifier($wk::CreatorOwnerSid, $null)),
        (New-Object System.Security.Principal.NTAccount('NT SERVICE\TrustedInstaller')).Translate($sidType))
    $grants = @('/grant', "*$($usersSid.Value):(OI)(CI)RX")
    $serviceSid = $null
    if ($serviceAccount) {
        if ($serviceAccount -eq 'LocalSystem') { $serviceAccount = 'NT AUTHORITY\SYSTEM' }
        $serviceAccount = $serviceAccount -replace '^\.\\', "$env:COMPUTERNAME\"
        $serviceSid = (New-Object System.Security.Principal.NTAccount($serviceAccount)).Translate($sidType)
        # NOT added to $trusted here (#4052): the service is only trusted to write under $serviceWritePaths,
        # below, once that list is known - a write grant it holds anywhere else in the tree is a stranger's.
        # Read & Execute on the root only - the service still has to run its own exe and load its own DLLs -
        # not Modify: that goes only on the specific directories below, so a write anywhere else in the tree
        # is a stranger's, even from the service account itself.
        #
        # /grant:r, not /grant (#4052 round 2): plain /grant ADDS to whatever explicit ACE the SID already
        # carries on $root - it never removes one. An install locked before this fix left an explicit
        # (OI)(CI)M ACE for the service on $root (#4038's shape), and re-running the OLD /grant against that
        # tree would only have ADDED (OI)(CI)RX beside the surviving Modify, not replaced it - Modify would
        # have outlived every upgrade. /grant:r replaces the service SID's own explicit grant outright, so
        # this one call is what actually narrows a tree #4038 (or an even older build) widened.
        $grants += @('/grant:r', "*$($serviceSid.Value):(OI)(CI)RX")
    }
    $rights = [System.Security.AccessControl.FileSystemRights]
    $allow = [System.Security.AccessControl.AccessControlType]::Allow
    $open = @()
    # The directories the service actually writes to: pg-runtime\ and pg-runtime-prev\ always, plus whatever
    # $extraServiceDirectories names (the BYO-Postgres log-hash-key folder, when that mode is configured).
    # Each is created ahead of time if missing - best-effort, since a directory this lock cannot create is a
    # directory the first extraction would have had to create itself, root-owned, which is no better - and
    # then granted Modify with its own icacls call, so a failure on one directory does not lose the others.
    $serviceWriteDirectories = @((Get-DarlingServiceWrittenNames).Directories) + @($extraServiceDirectories | Where-Object { $_ })

    # icacls, not Set-Acl: Set-Acl on what Get-Acl read writes the SACL too, which needs SeSecurityPrivilege, and
    # icacls is what the warnings tell an operator to run by hand, so the fix and the remediation are the same
    # commands. /inheritance:d stops inheriting and keeps what the volume root gave as explicit ACEs, so SYSTEM
    # and Administrators keep full control. SIDs, not names, so an account name with a space needs no quoting.
    # /L acts on a link itself, never on what it points to.
    $steps = @(
        @('/inheritance:d'),
        (@('/remove:g') + @($broad | ForEach-Object { "*$($_.Value)" })),
        $grants)
    foreach ($step in $steps) {
        $output = & icacls.exe $root @step 2>&1
        if ($LASTEXITCODE -ne 0) { return @("$root (icacls $($step -join ' ') failed: $($output -join ' '))") }
    }
    # The owner step (#5677). The tree is walked here and the owner of each object is set one at a time, in this process, on the
    # handle its name count is read from (see Initialize-DarlingNativeOwner). The root is opened once, without sharing delete or
    # rename, and kept open for the whole walk; each object's real path is read from its own handle and an object that is not
    # the root or below it is left alone (result 5), so a folder swapped for a junction while the walk runs cannot send it
    # outside the tree. A file whose count is not exactly 1, or cannot be read, keeps its owner, because a file with more than one
    # name (a hard link) has one owner that every name shares, some of them outside this folder. A name ending in a dot or a
    # space is listed and not acted on (Windows would open a different name). Each such object is listed as open here and put in
    # $skipped so the walk below leaves it out (it is listed once), except the root and an object whose owner could not be set:
    # the walk below still fixes and checks those. $skipped compares by ordinal, so two names that differ only in case (a
    # case-sensitive folder) stay two. The walk does not follow a junction or link: the link itself is the object, and nothing
    # below it is listed. The two privileges are turned on for the step and put back as they were after it; without them (the
    # session does not hold them) the step is not run and the root is reported.
    $skipped = New-Object 'System.Collections.Generic.HashSet[string]' ([StringComparer]::Ordinal)
    $ownerBytes = New-Object byte[] $adminsSid.BinaryLength
    $adminsSid.GetBinaryForm($ownerBytes, 0)
    $ownerReady = $false
    try {
        Initialize-DarlingNativeOwner
        $ownerReady = [DarlingNativeOwner]::EnablePrivileges()
        if (-not $ownerReady) { $open += "$root (could not make Administrators the owner of everything below it: this elevated session does not hold SeTakeOwnershipPrivilege and SeBackupPrivilege (see whoami /priv); a policy may have removed them from Administrators)" }
        else {
            $rootError = 0
            if (-not [DarlingNativeOwner]::OpenRoot($root, [ref]$rootError)) { $open += "$root (could not make Administrators the owner of everything below it: the folder could not be pinned and its real path read, error $rootError)" }
            else {
                $pending = New-Object System.Collections.Stack
                $pending.Push((Get-Item -LiteralPath $root -Force))
                $atRoot = $true
                while ($pending.Count -gt 0) {
                    $item = $pending.Pop()
                    $ownPath = [string]$item.FullName
                    $isRoot = $atRoot
                    $atRoot = $false
                    $belowNote = if ($item.PSIsContainer) { '; nothing below it was given an owner or checked' } else { '' }
                    if ((-not $isRoot) -and ($ownPath.EndsWith('.') -or $ownPath.EndsWith(' '))) {
                        $null = $skipped.Add($ownPath)
                        $open += "$ownPath (its name ends in a dot or a space, which Windows folds into another name, so it was not given an owner or checked$belowNote)"
                        continue
                    }
                    $detail = 0
                    $attributes = [uint32]0
                    $result = [DarlingNativeOwner]::SetOwner($ownPath, $ownerBytes, [ref]$detail, [ref]$attributes)
                    if ($result -ne 0) {
                        $why = switch ($result) {
                            1 { "has $detail names (a hard link): its permissions are the same under every one, so its owner and permissions were not changed" }
                            2 { "could not be opened, error $detail$belowNote" }
                            3 { "its names could not be counted, error $detail$belowNote" }
                            4 { "its owner could not be set, error $detail" }
                            default { "its path resolved outside the install folder, so its owner was not changed$belowNote" }
                        }
                        if ($result -ne 4 -and -not $isRoot) { $null = $skipped.Add($ownPath) }
                        $open += "$ownPath ($why)"
                    }
                    if (($attributes -band 0x10) -ne 0 -and ($attributes -band 0x400) -eq 0) {
                        try { foreach ($kid in @(Get-ChildItem -LiteralPath $ownPath -Force -ErrorAction Stop)) { $pending.Push($kid) } }
                        catch { $open += "$ownPath (its contents could not be listed, so nothing below it was given an owner: $($_.Exception.Message))" }
                    }
                }
            }
        }
    }
    catch { $open += "$root (could not make Administrators the owner of everything below it: $($_.Exception.Message))" }
    finally {
        try { [DarlingNativeOwner]::CloseRoot() } catch { }
        try { $null = [DarlingNativeOwner]::RestorePrivileges() } catch { }
    }
    $serviceWritePaths = @()
    if ($serviceSid) {
        foreach ($secret in @(Get-DarlingServiceWrittenRootFiles $root)) {
            $null = & icacls.exe $secret.FullName /setowner "*$($serviceSid.Value)" /L /Q 2>&1
        }
        foreach ($dir in $serviceWriteDirectories) {
            $dirPath = Join-Path $root $dir
            if (-not (Test-Path -LiteralPath $dirPath)) {
                try { $null = New-Item -ItemType Directory -Path $dirPath -Force -ErrorAction Stop }
                catch { $open += "$dirPath (could not create this directory to grant the service Modify on it: $($_.Exception.Message))"; continue }
            }
            $grantOutput = & icacls.exe $dirPath /grant "*$($serviceSid.Value):(OI)(CI)M" 2>&1
            if ($LASTEXITCODE -ne 0) { $open += "$dirPath (icacls /grant Modify for the service failed: $($grantOutput -join ' '))"; continue }
            $serviceWritePath = Get-DarlingCanonicalPath (Get-Item -LiteralPath $dirPath -Force).FullName
            if ($null -eq $serviceWritePath) { $open += "$dirPath (its full path could not be worked out, so the service's grant on it cannot be trusted)"; continue }
            $serviceWritePaths += $serviceWritePath.TrimEnd('\')
        }
    }
    # A write grant to the service SID is only trusted under one of $serviceWritePaths (path-prefix match,
    # case-insensitive as Windows paths are); everywhere else in the tree it is treated like a stranger's and
    # closed, same as any other untrusted grant, by the walk below.
    function Test-DarlingServiceWritePath([string]$candidate) {
        foreach ($allowed in $serviceWritePaths) {
            if ($candidate -ieq $allowed -or $candidate.StartsWith("$allowed\", [StringComparison]::OrdinalIgnoreCase)) { return $true }
        }
        return $false
    }

    # VERIFY rather than assume (#1957), and close what a folder lock cannot: every directory and file is read
    # back, and one with an explicit write grant to an account outside the trusted set is fixed where it stands.
    # darling.json and its backups keep their protected DACL and lose only that grant; anything else is reset to
    # inherit the tree's ACEs, since no shipped file carries its own and emptying one would leave the service
    # unable to run it. What remains is reported. A write right includes the generic bits (GENERIC_ALL
    # 0x10000000, GENERIC_WRITE 0x40000000) an inherit-only ACE can carry.
    $write = [int64]($rights::WriteData -bor $rights::AppendData -bor $rights::WriteAttributes -bor $rights::WriteExtendedAttributes -bor $rights::Delete -bor $rights::DeleteSubdirectoriesAndFiles -bor $rights::ChangePermissions -bor $rights::TakeOwnership) -bor 0x10000000 -bor 0x40000000
    if (-not (Get-Acl -LiteralPath $root).AreAccessRulesProtected) { $open += "$root (still inherits from its parent)" }
    $targets = @(Get-Item -LiteralPath $root -Force) + @(Get-ChildItem -LiteralPath $root -Recurse -Force -ErrorAction SilentlyContinue)
    foreach ($target in $targets) {
        if ($skipped.Contains($target.FullName)) { continue }
        $path = Get-DarlingCanonicalPath $target.FullName
        if ($null -eq $path) { $open += "$($target.FullName) (its full path could not be worked out)"; continue }
        if ($path.TrimEnd('\') -ine $root.TrimEnd('\') -and ($target.Attributes -band [System.IO.FileAttributes]::ReparsePoint)) {
            $open += "$path (a junction or link)"
            continue
        }
        # A root file the service writes that has more than one name (a hard link, or one whose names cannot be counted) is
        # reported and left alone, the way a junction is: resetting it or stripping a grant would change the permissions of
        # its other names too, and a reset makes the other name readable by every local user (#5649).
        if ((-not $target.PSIsContainer) -and ([IO.Path]::GetDirectoryName($path).TrimEnd('\') -ieq $root.TrimEnd('\')) -and (Test-DarlingServiceWrittenFileName $target.Name) -and (Get-DarlingFileNameCount $path) -ne 1) {
            $open += "$path (a hard link: this file has more than one name, or its names could not be counted, and its permissions are the same under every one)"
            continue
        }
        # The service SID is trusted here only if $path is $root itself (its RX grant) or falls under one of
        # $serviceWritePaths (its Modify grant) - everywhere else its write grant is a stranger's, same as any
        # other untrusted account, even though the SID is the same one the tree hands its own real access to.
        $trustedHere = $trusted
        $isServiceWritePath = $serviceSid -and (Test-DarlingServiceWritePath $path)
        # darling.json and its backups: step 4b gives the service an explicit FullControl there (#1647) and the setowner
        # above makes it the owner, so the service is trusted on those files too. Without this the walk strips the
        # service's own grant from its config (the explicit-ACE branch below) and the service cannot start.
        $isSecretFile = (-not $target.PSIsContainer) -and ([IO.Path]::GetDirectoryName($path).TrimEnd('\') -ieq $root.TrimEnd('\')) -and (Test-DarlingServiceWrittenFileName $target.Name)
        if ($serviceSid -and ($path.TrimEnd('\') -ieq $root.TrimEnd('\') -or $isServiceWritePath -or $isSecretFile)) { $trustedHere += $serviceSid }
        try { $acl = Get-Acl -LiteralPath $path -ErrorAction Stop }
        catch { $open += "$path (its permissions could not be read)"; continue }
        $explicit = @($acl.GetAccessRules($true, $false, $sidType) | Where-Object {
            $_.AccessControlType -eq $allow -and (([int64]$_.FileSystemRights) -band $write) -ne 0 -and $trustedHere -notcontains $_.IdentityReference })
        if ($explicit.Count -gt 0) {
            # Any file that has more than one name (a hard link anywhere in the tree, not only a backup), or whose names cannot be
            # counted, is reported and left as it is: /remove:g and /reset write to the one set of permissions all its names share,
            # and a reset would put the tree's inherited entries on the other name (#5649). Directories have one name.
            if ((-not $target.PSIsContainer) -and (Get-DarlingFileNameCount $path) -ne 1) {
                $open += "$path (a hard link: this file has more than one name, or its names could not be counted, and its permissions are the same under every one; its permissions were not changed)"
                continue
            }
            if ($isSecretFile) {
                foreach ($rule in $explicit) { $null = & icacls.exe $path /remove:g "*$($rule.IdentityReference.Value)" /L /Q 2>&1 }
            }
            else {
                $null = & icacls.exe $path /reset /L /Q 2>&1
                # /reset drops the directory's OWN explicit Modify grant along with a planted ACE, since both are
                # explicit ACEs on the same object - so a service write directory gets its real grant put straight
                # back, rather than left open until the next lock run.
                if ($isServiceWritePath) { $null = & icacls.exe $path /grant "*$($serviceSid.Value):(OI)(CI)M" 2>&1 }
            }
        }
        try { $acl = Get-Acl -LiteralPath $path -ErrorAction Stop }
        catch { $open += "$path (its permissions could not be read)"; continue }
        $writers = @($acl.GetAccessRules($true, $true, $sidType) | Where-Object {
            $_.AccessControlType -eq $allow -and (([int64]$_.FileSystemRights) -band $write) -ne 0 -and $trustedHere -notcontains $_.IdentityReference })
        $owner = $acl.GetOwner($sidType)
        if ($writers.Count -gt 0) { $open += $path }
        elseif ($trustedHere -notcontains $owner) { $open += "$path (owned by $($owner.Value))" }
    }
    return $open
}

# Runs Lock-DarlingInstallTree for this script (steps 1b2 and 4b2) and says what it found, with the by-hand fix.
# -StopOnOpen (1b2) then stops the install: everything after it runs the service exe, elevated, from this
# folder, and stopping costs nothing yet, the same argument 1b makes. At 4b2 the service already exists, so a
# failure warns loudly instead of leaving a half-made install. With no account yet the grant line names the
# virtual account the service is about to get.
function Invoke-InstallTreeLock([string]$account, [switch]$StopOnOpen) {
    $open = @()
    try {
        $open = @(Lock-DarlingInstallTree $root $account (Get-DarlingExtraServiceWriteDirectories $root $configPath))
    }
    catch {
        $open = @("$root ($($_.Exception.Message))")
    }
    if ($open.Count -eq 0) {
        if ($account) {
            Write-Host "Locked the install folder: only SYSTEM and Administrators can change what runs from $root; $account can write only its runtime folders." -ForegroundColor Green
        }
        else {
            Write-Host "Locked the install folder against ordinary users before running anything from it: $root" -ForegroundColor Green
        }
        return
    }
    $grantTo = if ($account) { $account } else { "NT SERVICE\$serviceName" }
    Write-Host ''
    Write-Host 'SECURITY WARNING: ordinary users can still write to these paths in the install folder:' -ForegroundColor Red
    foreach ($p in ($open | Select-Object -First 20)) { Write-Host "  $p" -ForegroundColor Red }
    if ($open.Count -gt 20) { Write-Host "  ...and $($open.Count - 20) more." -ForegroundColor Red }
    Write-Host '  The service and its PostgreSQL run from here as the service account, so anyone who can replace a' -ForegroundColor Red
    Write-Host '  binary can run code as that account. Fix it from an elevated prompt, then re-run this script:' -ForegroundColor Red
    Write-Host "    icacls `"$root`" /inheritance:d" -ForegroundColor Yellow
    Write-Host "    icacls `"$root`" /remove:g *S-1-5-11 *S-1-5-32-545 *S-1-1-0 *S-1-5-4" -ForegroundColor Yellow
    Write-Host "    icacls `"$root`" /grant `"*S-1-5-32-545:(OI)(CI)RX`" /grant:r `"$grantTo`:(OI)(CI)RX`"" -ForegroundColor Yellow
    Write-Host '    (then run this script again: it makes Administrators the owner of each path itself, and lists a file with more than one name)' -ForegroundColor Yellow
    Write-Host '  A path marked as a hard link (a file with more than one name): delete this name if it is not part of the install, or extract the file again if it is. Do not run icacls on it.' -ForegroundColor Red
    Write-Host '  Any other path listed as owned by an account, or BELOW the folder, needs its own fix; these two commands are only for paths not marked as a hard link: icacls "<path>" /setowner' -ForegroundColor Red
    Write-Host '  *S-1-5-32-544 /L, then icacls "<path>" /reset /C. Remove any junction or link it names.' -ForegroundColor Red
    Write-Host ''
    if ($StopOnOpen) {
        Fail 'Stopped before running anything from a folder ordinary users can still change. Fix the paths above, or extract the zip into a fresh folder, then re-run this script.'
    }
}

# -- 1. Environment checks ------------------------------------------------------------------------
# Can this session answer a question? Not when PowerShell was started -NonInteractive (any abbreviation of it),
# when its input is redirected, or when the process is not interactive: a Read-Host there throws or hangs, so
# those runs keep the plain stop below (#6469). The three facts are parameters so a test can stand in for them.
function Test-DarlingInteractiveSession(
    [string[]]$commandLine = [Environment]::GetCommandLineArgs(),
    [bool]$userInteractive = [Environment]::UserInteractive,
    [bool]$inputRedirected = [Console]::IsInputRedirected) {
    if (-not $userInteractive -or $inputRedirected) { return $false }
    foreach ($argument in $commandLine) {
        if ($argument -match '^[-/]non') { return $false }
    }
    return $true
}

# What to do about elevation: carry on, offer to restart elevated, or stop with the message (#6469).
function Get-DarlingElevationAction([bool]$isAdministrator, [bool]$interactive) {
    if ($isAdministrator) { return 'Continue' }
    if ($interactive) { return 'Offer' }
    return 'Stop'
}

# The arguments for the elevated restart: this script, with the execution policy bypassed for that one process
# (never Set-ExecutionPolicy), and every switch and valued parameter the person passed. -NoExit keeps the new window open so its output
# can be read once the script ends.
function Get-DarlingElevatedArguments([string]$scriptPath, [hashtable]$boundParameters) {
    $list = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-NoExit', '-File', ('"' + $scriptPath + '"'))
    foreach ($name in @($boundParameters.Keys | Sort-Object)) {
        $value = $boundParameters[$name]
        if ($value -is [System.Management.Automation.SwitchParameter]) {
            if ($value.IsPresent) { $list += "-$name" }
        }
        elseif ($null -ne $value) {
            # A valued parameter (-AddOperator, #6478): without it the elevated run would never add that account.
            $list += "-$name"
            $list += ('"' + [string]$value + '"')
        }
    }
    return $list
}

$identity = [Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()
$elevationAction = Get-DarlingElevationAction ($identity.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) (Test-DarlingInteractiveSession)
if ($elevationAction -eq 'Stop') {
    Fail 'Run this script from an ELEVATED PowerShell (service creation and Event Log registration require it). To skip that step, double-click Install-Darling.cmd beside this script instead: it asks Windows for administrator rights and runs this script for you.'
}
if ($elevationAction -eq 'Offer') {
    Write-Host 'This installer needs administrator rights (service creation and Event Log registration), and this PowerShell is not elevated.' -ForegroundColor Yellow
    $answer = Read-Host 'Restart it elevated now? Windows will ask you to approve. [Y/n]'
    if ($answer -match '^[Nn]') {
        Fail 'Not restarted. Run this script from an ELEVATED PowerShell, or double-click Install-Darling.cmd beside it.'
    }
    try {
        $powerShellExe = [Diagnostics.Process]::GetCurrentProcess().MainModule.FileName
        Start-Process -FilePath $powerShellExe -Verb RunAs -ArgumentList (Get-DarlingElevatedArguments $PSCommandPath $PSBoundParameters)
    }
    catch {
        Fail "Could not restart elevated: $($_.Exception.Message) Run this script from an ELEVATED PowerShell, or double-click Install-Darling.cmd beside it."
    }
    Write-Host 'The installer is running in the elevated window that just opened. Carry on there; this window has nothing more to do.' -ForegroundColor Green
    exit 0
}

# The MSI package (#6480) writes HKLM\SOFTWARE\PerformanceMonitor Darling, value InstalledBy = msi, and removes it at uninstall. A
# machine that holds it belongs to Windows Installer: a script install over it would leave two owners for one service. Returns the
# refusal text, or $null when the marker is absent. The registry read is a parameter so a test can stand in for the machine; the
# default reads the 64-bit view, so a 32-bit PowerShell is not sent to WOW6432Node and does not miss the marker. Kept byte-identical
# in install-darling.ps1, upgrade-darling.ps1 and uninstall-darling.ps1.
function Get-DarlingMsiInstallRefusal([string]$ScriptName, [scriptblock]$ReadMarker = $null) {
    if (-not $ReadMarker) {
        $ReadMarker = {
            $base = [Microsoft.Win32.RegistryKey]::OpenBaseKey([Microsoft.Win32.RegistryHive]::LocalMachine, [Microsoft.Win32.RegistryView]::Registry64)
            try {
                $key = $base.OpenSubKey('SOFTWARE\PerformanceMonitor Darling')
                if ($key) { try { $key.GetValue('InstalledBy') } finally { $key.Dispose() } }
            }
            finally { $base.Dispose() }
        }
    }
    if ("$(& $ReadMarker)" -ne 'msi') { return $null }
    return @"
PerformanceMonitor Darling on this machine was installed by the MSI package, so $ScriptName will not change it: two installers
would then own one service.
  To update it: run the newer MSI. Windows Installer replaces the files in place and keeps darling.json and the store.
  To remove it: Settings > Apps > Installed apps > PerformanceMonitor Darling > Uninstall. That keeps the store unless you tick
  the box to remove it.
  To go back to script installs: uninstall the MSI as above, extract the zip into a fresh folder, and run install-darling.ps1 there.
Nothing was installed or changed.
"@
}

# Fresh install or upgrade. By hand it is whether the service exists. Under -MsiPhase the package created the service before this
# script ran, so $Existing is set on a fresh MSI install too and would read as an upgrade: the package says which it is with
# -MsiUpgrade (#6480). Takes the facts as arguments so a test can stand in for the machine.
function Test-DarlingIsUpgrade([string]$MsiPhase, [bool]$MsiUpgrade, $Existing) {
    if ($MsiPhase) { return $MsiUpgrade }
    return ($null -ne $Existing)
}

# The one place a question is asked of a person at steps 1b and 2. Under -MsiPhase nobody can answer (a custom action has no console), so
# the answer is "continue" and nothing is read (#6480). The reader is a parameter so a test can see that it is not called.
function Read-DarlingAnswer([string]$MsiPhase, [string]$Prompt, [scriptblock]$Ask = $null) {
    if ($MsiPhase) {
        Write-Host "Continuing: a -MsiPhase run asks no questions ($Prompt)" -ForegroundColor Yellow
        return 'y'
    }
    if (-not $Ask) { $Ask = { param($text) Read-Host $text } }
    return (& $Ask $Prompt)
}

if ($MsiUpgrade -and -not $MsiPhase) { Fail '-MsiUpgrade is only for the MSI package: it goes with -MsiPhase.' }
if ($MsiPhase -and $Network) { Fail '-Network opens an interactive wizard, which -MsiPhase never does. Run install-darling.ps1 -Network by hand afterwards, or edit darling.json.' }
if (-not $MsiPhase) {
    $msiRefusal = Get-DarlingMsiInstallRefusal 'install-darling.ps1'
    if ($msiRefusal) { Fail $msiRefusal }
}

if (-not (Test-Path -LiteralPath $serviceExe)) {
    Fail "PerformanceMonitor.Darling.Service.exe not found beside this script. Extract the full Darling zip and run install-darling.ps1 from the extracted folder."
}

# Resolved here, ahead of both 1a and 1b, rather than only down at 1b where the fresh/upgrade split first
# needed it: 1a's pre-lock check (below) must also know fresh-versus-upgrade, so a re-run over a tree #4038
# already locked does not read that lock's own grant back as a stranger's (round-1 review, #4043).
$existing = Get-Service -Name $serviceName -ErrorAction SilentlyContinue

# Fresh install or upgrade, for 1b, 1c2 and 5a (see Test-DarlingIsUpgrade for why $existing alone is not enough under -MsiPhase).
$isUpgrade = Test-DarlingIsUpgrade $MsiPhase ([bool]$MsiUpgrade) $existing

# PostStart has nothing to check or lock: PreStart did all of it before the service started. The body below is not re-indented,
# to keep the diff of this long block readable.
if ($MsiPhase -ne 'PostStart') {

# -- 1a. Refuse an install tree an ordinary user can ALREADY write to (#4043) -----------------------
# #4038's lock at 1b2 closes the tree against writes from here on. It cannot undo one made before it ran:
# a folder created directly under C:\ - the documented location - inherits "Authenticated Users: Modify"
# from the volume root the instant it is extracted, and anything from then until this script reaches 1b2
# is a window a local user could use to replace the service exe, a DLL, or a pg-runtime binary. A planted
# binary loads as the service account on first start, and the lock never checks CONTENTS, only future
# writes, so it would not catch this.
#
# A signature or hash check run BY this script, AFTER the lock, was considered and withdrawn (#4043 review):
# it only catches an attacker who leaves install-darling.ps1 itself alone, and someone who can replace the
# service exe can just as easily replace this script, which the admin then runs elevated. The trust root is
# where the admin extracted the zip, not anything a script reads back from inside that tree.
#
# So this checks the one thing that actually predicts the risk: is the tree ALREADY writable by someone
# outside SYSTEM, Administrators, TrustedInstaller and the admin running this script, right now, before
# anything has run. That fires on every risky extraction, attack or not, and does not depend on an attacker
# having left anything else untouched. The walk is RECURSIVE (round-1 review, #4043, M2): the root alone
# stops showing anything once it has been re-permissioned by hand or re-inherited after a move, while a file
# under it keeps whatever owner or explicit grant it already had - see Get-UntrustedWriteGrantees for why
# checking every file, not just the root and the exe, is not the redundant work it looks like.
if (-not $AcceptWritableExtraction) {
    # $existing (resolved above, before this step) makes a re-run over an already-locked tree trust exactly
    # what that lock granted, rather than refusing the service account's own Modify grant as a stranger's -
    # and refuses outright, with 1b2's own message, when that account cannot be read or resolved at all
    # (round-1 review, L3/L4), rather than silently mis-judging the walk below.
    $preLockTrusted = Get-DarlingPreLockTrustedSidsForRerun $serviceName $existing
    $writableRecords = @(Get-UntrustedWriteGrantees $root $preLockTrusted.Trusted -AsRecords -Recurse -ServiceSids $preLockTrusted.ServiceSids -ServiceDirectories @(Get-DarlingExtraServiceWriteDirectories $root $configPath))
    $writable = @($writableRecords | ForEach-Object { $_.Text })
    if ($writable.Count -gt 0) {
        $lines = ($writable | Select-Object -First 20 | ForEach-Object { "  $_" }) -join "`n"
        $more = if ($writable.Count -gt 20) { "`n  ...and $($writable.Count - 20) more." } else { '' }
        # The service running from THIS folder means it is an existing install (#5627): its folder is what needs
        # replacing, so the refusal spells out how. A service registered elsewhere, or none, leaves nothing here to
        # keep, and the advice is a fresh folder.
        $advice = Get-DarlingRefusalAdvice $root $serviceName (Get-DarlingInstallRootFromService $serviceName) $writableRecords
        Fail @"
Ordinary users can already write to this install folder, before this script locks anything down:

$lines$more

Files here can differ from what the zip shipped. The lock this script applies next (1b2) only stops further
writes. It does not check what is already on disk. A folder made directly under C:\ gets this access from
the volume root, which gives Authenticated Users Modify.

$advice

If this folder is deliberately writable (a dev loop) and you accept the risk, re-run with
-AcceptWritableExtraction.

Nothing was installed or changed.
"@
    }
}
else {
    Write-Host 'WARNING: -AcceptWritableExtraction - not checking whether this folder was already writable by other local users. Files here may already differ from what the zip shipped.' -ForegroundColor Yellow
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
# account is a member of, which is why C:\Program Files\PerformanceMonitorDarling works and this does not.
#
# Step 4b is not a substitute: its ACL work is scoped to darling.json and its .bak-* copies, so pg-runtime
# keeps whatever the profile gave it. Fixing the tree's ACLs instead of refusing was considered and
# rejected in #2187 - it means the product starts silently ACLing directories inside somebody's profile.
#
# The residual, seen and accepted rather than discovered later: C:\Users\Public sits under the profile root
# and is refused, but an install there would actually WORK - it grants NT AUTHORITY\SERVICE:(OI)(CI)(IO)(M,DC),
# which every service account holds. It is deliberately not carved out. Nobody installs a Windows service
# into the shared documents profile, a carve-out would amount to documenting it as a reasonable place to
# install, and the refusal is not a dead end - it names C:\Program Files\PerformanceMonitorDarling. One rule, no
# exceptions, and the one location it costs is one nobody wants.
#
# This runs BEFORE the pre-flight, the Event Log source, and service creation, so a doomed location costs
# nothing and leaves nothing behind.

# $isUpgrade (set right after $existing, above) is the same fact as a boolean, for the post-start firewall reconcile at 5a
# (#2436). Named rather than reusing $existing at the far end of the script because what 5a actually depends on is not "a
# service object was found" but "a STORE already exists to be asked" - which is what an existing service implies by hand, and
# what -MsiUpgrade says under -MsiPhase.
# Classify the NORMALIZED spelling, but keep installing to $root exactly as given (#2348). The \\?\ prefix
# instructs the path parser and is not part of where the install lives, so stripping it for the decision
# changes which rules see the path and nothing about where files land.
$classifyRoot = Convert-ExtendedLengthPath $root

$networkKind = Get-NetworkPathKind $classifyRoot
# $env:USERPROFILE as well as the machine's profile root: a profile redirected outside ProfilesDirectory
# is still a profile, and it is the profile whose owner is most likely to be running this script.
$underProfile = (Test-PathIsAtOrUnder $classifyRoot (Get-ProfilesDirectory)) -or (Test-PathIsAtOrUnder $classifyRoot $env:USERPROFILE)

# A junction or link into a profile folder is under that profile whatever it is called (#5673): the same test, on the
# folder's final path.
$resolvedRoot = Get-DarlingResolvedFolder $classifyRoot
if (-not $underProfile -and $resolvedRoot) {
    $underProfile = (Test-PathIsAtOrUnder $resolvedRoot (Get-ProfilesDirectory)) -or (Test-PathIsAtOrUnder $resolvedRoot $env:USERPROFILE)
}

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

    $extractCommand = Get-DarlingExtractCommand $root
    $fix = @"
Extract the zip again, from an elevated session, into a machine-scoped local folder that only
administrators can write, and run this script from there:

  C:\Program Files\PerformanceMonitorDarling

That is the documented location. A folder created there inherits read + execute for BUILTIN\Users,
which the service's virtual account is a member of, and ordinary users can't write to it (#4043).
Copy your darling.json across if you have one.

To extract the zip straight into place, paste this into an ELEVATED PowerShell:

  $extractCommand

Then run install-darling.ps1 (or double-click Install-Darling.cmd) from that folder.
"@

    if (-not $isUpgrade) {
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
    $answer = Read-DarlingAnswer $MsiPhase 'Point the service at this folder anyway? [y/N]'
    if ($answer -notmatch '^[Yy]') { exit 4 }
}

# -- 1c2. Refuse a first install on an unedited sample (#6475) ----------------------------------------
# darling.sample.json is a reference, and its two example servers would be registered on the first start if the
# file were copied over unchanged (the sample's own first line tells a person to copy it). Since #6468 a missing
# darling.json is not a problem, because the installer writes a minimal one with no servers; an UNEDITED COPY of
# the sample is, because it still names servers that do not exist. Compared as text, ignoring line endings and
# trailing blanks, so a copy saved by an editor that changed them is still caught. Only a first install is held:
# an existing service has a store that is authoritative for its servers, and the file only matters to it as a
# fallback, so an upgrade through this script is never blocked by it. Before the lock, so a refusal leaves the folder's permissions as they were.
function Test-DarlingConfigIsUnchangedSample([string]$configPath, [string]$samplePath) {
    if (-not (Test-Path -LiteralPath $configPath)) { return $false }
    if (-not (Test-Path -LiteralPath $samplePath)) { return $false }
    $normalize = { param([string]$text) ((($text -replace "`r`n", "`n") -split "`n" | ForEach-Object { $_.TrimEnd() }) -join "`n").Trim() }
    $config = & $normalize ([System.IO.File]::ReadAllText($configPath))
    $sample = & $normalize ([System.IO.File]::ReadAllText($samplePath))
    return ($config -ceq $sample)
}

if (-not $isUpgrade -and (Test-DarlingConfigIsUnchangedSample $configPath $samplePath)) {
    Fail @"
darling.json is an unedited copy of darling.sample.json, so it still names the sample's example servers, which do not exist.
The service would register them on its first start.

  Either delete darling.json and run this script again (it writes a minimal darling.json with no servers, and you
  add your first server in the Viewer afterwards), or edit darling.json so it names your own servers.

Nothing was installed or changed.
"@
}

# -- 1b2. Lock the install tree before anything runs from it (#4034) ---------------------------------------
# The rest of this script runs the service exe (the pre-flight, --configure-firewall) and registers it to run
# as the service account, so the tree is closed to ordinary users first: see Lock-DarlingInstallTree. On a
# fresh install there is no service account to grant yet; 4b2 adds it once sc.exe create has made one. What no
# lock can undo is a file swapped between extracting the zip and running this script, which is why the README
# says to extract into a fresh folder and run this straight away.
# An upgrade through this script already has a service, and the lock removes any write grant outside the
# trusted set, so the service's own account goes in now or its access would be stripped until 4b2 (and for
# good if the script stopped before it). Unreadable, it stops here, as step 4 does, rather than guess.
$lockAccount = ''
if (Get-Service -Name $serviceName -ErrorAction SilentlyContinue) {
    $lockAccount = Get-DarlingServiceLogonName $serviceName
    if (-not $lockAccount) {
        Fail "Could not read which account the existing '$serviceName' service logs on as, so the install folder cannot be locked without taking that account's access away. Nothing was changed. Check it with: sc.exe qc `"$serviceName`", then re-run this script."
    }
    if ($lockAccount -eq 'LocalSystem') { $lockAccount = 'NT AUTHORITY\SYSTEM' }
    else { $lockAccount = $lockAccount -replace '^\.\\', "$env:COMPUTERNAME\" }
}
Invoke-InstallTreeLock $lockAccount -StopOnOpen

# A first install needs no hand-edited darling.json (#6468). The service starts with no monitored servers, so a
# missing darling.json is answered with a minimal one (the bundled store only) and the install carries on. The
# first server is added afterwards in the Viewer. The full sample is left beside it as the reference.

# The web dashboard's access token for a new darling.json (#6480). The web host refuses to start without a token, so turning the
# dashboard on with no token gives a dashboard that never starts. This makes 32 random letters and digits and returns them
# DPAPI-protected at LocalMachine scope with the service's own entropy string, base64: the same blob DarlingSecrets.Protect makes
# for --encrypt-password, so web.network.encryptedToken reads it back on this machine. The plain token is never written or shown
# here: --print-web-token reprints it.
function New-DarlingWebEncryptedToken {
    Add-Type -AssemblyName System.Security
    $alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789'
    $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $chars = New-Object System.Text.StringBuilder
        $buffer = New-Object byte[] 1
        while ($chars.Length -lt 32) {
            $rng.GetBytes($buffer)
            # 248 = 4 * 62: a byte at or above it is skipped so every letter and digit is equally likely.
            if ($buffer[0] -lt 248) { [void]$chars.Append($alphabet[$buffer[0] % 62]) }
        }
        $entropy = [System.Text.Encoding]::UTF8.GetBytes('PerformanceMonitor.Darling.v1')
        $blob = [System.Security.Cryptography.ProtectedData]::Protect(
            [System.Text.Encoding]::UTF8.GetBytes($chars.ToString()), $entropy, [System.Security.Cryptography.DataProtectionScope]::LocalMachine)
        return [Convert]::ToBase64String($blob)
    }
    finally {
        $rng.Dispose()
    }
}

function New-DarlingMinimalConfigText([bool]$EnableWebDashboard = $false, [string]$WebEncryptedToken = '') {
    $text = @'
{
  // Written by install-darling.ps1. The service keeps everything else in its own PostgreSQL store.
  // darling.sample.json beside this file is the full, commented reference for every setting.
  //
  // No server is named here on purpose: the service starts with none. Add the first one in the Viewer
  // (Add Server). Once the service has started, the store is authoritative for the server list, so adding
  // servers to this file later changes nothing.
  "postgres": {
    "managed": true,
    "port": 5641
  },
  "servers": []
}
'@
    # The "web" block goes ahead of "servers". allowAdminWrites is written true in every NEW file: a darling.json without the key keeps the
    # web admin writes off (so an upgrade from 3.11 changes nothing), and a new install keeps them on, as before.
    # -WebDashboard (the MSI's WEBDASHBOARD=1, #6480): the dashboard also starts on its loopback address and asks for the token below.
    # Tokenless loopback stays off: it would let every local user read the collected data.
    $webBlock = '  "web": {' + "`r`n"
    if ($EnableWebDashboard) { $webBlock += '    "enabled": true,' + "`r`n" }
    $webBlock += '    "allowAdminWrites": true'
    if ($EnableWebDashboard -and $WebEncryptedToken) {
        $webBlock += ',' + "`r`n" + '    "network": {' + "`r`n" + '      "encryptedToken": "' + $WebEncryptedToken + '"' + "`r`n" + '    }'
    }
    $webBlock += "`r`n" + '  },' + "`r`n"
    $text = $text -replace '(?m)^  "servers": \[\]', ($webBlock.Replace('$', '$$') + '  "servers": []')
    return $text
}

# Writes the minimal file as UTF-8 without a byte-order mark, so every reader (the service's, this script's
# regexes, Windows PowerShell 5.1) sees the same bytes. The file is created with a protected DACL (SYSTEM and Administrators
# only) instead of inheriting the install tree's, because the -WebDashboard file holds the encrypted web token and step 4b only
# restricts it much later: a run that stopped before then would leave a file the Users group can read, and the next run reuses it.
# Step 4b rebuilds the DACL to add the service account and the operator group.
function Write-DarlingMinimalConfig([string]$path, [bool]$EnableWebDashboard = $false) {
    $token = ''
    if ($EnableWebDashboard) { $token = New-DarlingWebEncryptedToken }
    $bytes = (New-Object System.Text.UTF8Encoding($false)).GetBytes((New-DarlingMinimalConfigText $EnableWebDashboard $token))
    $wk = [System.Security.Principal.WellKnownSidType]
    $acl = New-Object System.Security.AccessControl.FileSecurity
    $acl.SetAccessRuleProtection($true, $false)
    foreach ($kind in @($wk::LocalSystemSid, $wk::BuiltinAdministratorsSid)) {
        $sid = New-Object System.Security.Principal.SecurityIdentifier($kind, $null)
        $acl.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($sid, [System.Security.AccessControl.FileSystemRights]::FullControl, [System.Security.AccessControl.AccessControlType]::Allow)))
    }
    if ($PSVersionTable.PSEdition -eq 'Core') {
        $stream = [System.IO.FileSystemAclExtensions]::Create((New-Object System.IO.FileInfo($path)), [System.IO.FileMode]::CreateNew, [System.Security.AccessControl.FileSystemRights]::Write, [System.IO.FileShare]::None, 4096, [System.IO.FileOptions]::None, $acl)
    }
    else {
        $stream = [System.IO.File]::Create($path, 4096, [System.IO.FileOptions]::None, $acl)
    }
    try { $stream.Write($bytes, 0, $bytes.Length) } finally { $stream.Dispose() }
}

# Only a missing darling.json is written. An existing one (an upgrade or a re-run) is left exactly as it is, web token included.
# Returns $true when it wrote the file.
function Initialize-DarlingMinimalConfig([string]$path, [bool]$EnableWebDashboard = $false) {
    if (Test-Path -LiteralPath $path) { return $false }
    Write-DarlingMinimalConfig $path $EnableWebDashboard
    return $true
}

if (Initialize-DarlingMinimalConfig $configPath ([bool]$WebDashboard)) {
    Write-Host ''
    Write-Host 'darling.json did not exist, so a minimal one was written: the bundled PostgreSQL store and no servers.' -ForegroundColor Yellow
    Write-Host 'Nothing needs editing before the install. Add your first server afterwards, in the Viewer (Add Server).' -ForegroundColor Yellow
    Write-Host 'darling.sample.json beside it is the full reference for every other setting.' -ForegroundColor Yellow
}

# -- 2. Pre-flight --------------------------------------------------------------------------------
# A Windows that has not had its updates cannot start a .NET 10 app on a CPU with CET (#5651). The runtime
# dies before any of our code runs, with exit code 0x80131506 and this text on stderr:
#   Fatal error. Your Windows doesn't fully support CET. Please install all available Windows updates.
# Without this check the failure below reads "the config is invalid or a server is unreachable", which points
# at the wrong file. Only the message counts: 0x80131506 is .NET's general fatal-error code
# (COR_E_EXECUTIONENGINE), not a CET-only code, so an exit code of 0x80131506 with no such text keeps the
# ordinary failure path below.
function Test-CetStartFailure([string[]]$Output) {
    foreach ($line in @($Output)) {
        if ("$line" -match "doesn't fully support CET") { return $true }
    }
    return $false
}

if (-not $SkipPreflight) {
    Write-Host 'Pre-flight: validating darling.json and probing every configured server (--test-connection)...'
    # Show each line as it arrives AND keep it, so the CET check can read what the exe said. 2>&1 on a native
    # exe turns each stderr line into an error record, which -ErrorAction Stop would throw on; the exit code
    # below is what decides, so the preference is relaxed for this one call only.
    $savedErrorAction = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    $preflightOutput = @(& $serviceExe --test-connection 2>&1 | ForEach-Object { Write-Host "$_"; "$_" })
    $preflightExit = $LASTEXITCODE
    $ErrorActionPreference = $savedErrorAction
    if (Test-CetStartFailure $preflightOutput) {
        Write-Host ''
        Write-Host 'ERROR: Windows on this machine is missing updates that .NET 10 needs, so the service cannot start.' -ForegroundColor Red
        Write-Host 'The service exe printed the CET start error (Your Windows doesn''t fully support CET).' -ForegroundColor Red
        Write-Host 'Install all available Windows updates, restart, and run this script again. The service was not installed. The install folder''s permissions were already locked, and stay that way.' -ForegroundColor Yellow
        Write-Host 'If the updates cannot go on yet, the README (Prerequisites) has a workaround. It turns off one' -ForegroundColor Yellow
        Write-Host 'protection (UserShadowStack) for the Darling exes, and shows the command that undoes it.' -ForegroundColor Yellow
        exit 5
    }
    if ($preflightExit -ne 0) {
        Write-Host ''
        Write-Host "Pre-flight FAILED (exit $preflightExit) - the config is invalid or a server is unreachable." -ForegroundColor Yellow
        $answer = Read-DarlingAnswer $MsiPhase 'Install anyway? A monitoring service may legitimately be installed while a target is down. [y/N]'
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

}  # end of the PreStart-and-by-hand block that starts at 1a

# The Windows product type: 1 workstation, 2 domain controller, 3 server (Win32_OperatingSystem.ProductType), or 0 when it
# cannot be read. Its own function so a test can stand in for the machine. Kept byte-identical in install-darling.ps1 and
# upgrade-darling.ps1.
function Get-DarlingProductType {
    try { return [int](Get-CimInstance -ClassName Win32_OperatingSystem -ErrorAction Stop).ProductType }
    catch { return 0 }
}

# True on a domain controller. A domain controller has no local account database: a group made there with net localgroup
# is a domain group, which the machine-qualified lookup cannot find and which would exist for the whole domain. Nothing
# here creates or grants a local group on one. Kept byte-identical in install-darling.ps1 and upgrade-darling.ps1.
function Test-DarlingDomainController {
    return ((Get-DarlingProductType) -eq 2)
}

# -- Adding the installing person to the operator group (#6478) ------------------------------------------
# At the end of the install the script offers to add the person who is signed in to the operator group, so the Viewer and the
# CLI work for them without a sign-out. Who gets the group's access is still the administrator's decision: the answer defaults
# to No, -NoOperatorPrompt never asks, and -AddOperator names the account for a scripted install. Each function below is small
# so a test can stand in for the machine.

# True for a PowerShell remoting session (WinRM, Enter-PSSession, Invoke-Command). Such a session can report
# [Environment]::UserInteractive as true yet has no console to answer on, so a Read-Host there would hang. Its host is
# named ServerRemoteHost and $PSSenderInfo is set. Takes both as arguments so a test can stand in for the session.
function Test-DarlingRemoteSession([string]$HostName, $SenderInfo) {
    return ($HostName -eq 'ServerRemoteHost' -or $null -ne $SenderInfo)
}

# True only when a person can answer a prompt: an interactive window station, input that is not redirected, a host not
# started with -NonInteractive, and not a remoting session. Anything else is treated as No by the caller, never waited on.
function Test-DarlingInteractiveHost {
    if (-not [Environment]::UserInteractive) { return $false }
    if (Test-DarlingRemoteSession $Host.Name $PSSenderInfo) { return $false }
    try { if ([Console]::IsInputRedirected) { return $false } }
    catch { return $false }
    foreach ($argument in [Environment]::GetCommandLineArgs()) {
        if ($argument -match '^[-/]NonI') { return $false }
    }
    return $true
}

# DOMAIN\user of the person signed in to THIS session: the owner of the session's explorer.exe. That is not always the
# account the script runs as (a person who elevates with another administrator account is signed in as themselves, and the
# script runs as the administrator). $null when no desktop shell runs in this session (a remote or service session).
function Get-DarlingInteractiveUser {
    try {
        $sessionId = [System.Diagnostics.Process]::GetCurrentProcess().SessionId
        foreach ($shell in @(Get-CimInstance -ClassName Win32_Process -Filter "Name = 'explorer.exe' AND SessionId = $sessionId" -ErrorAction Stop)) {
            $owner = Invoke-CimMethod -InputObject $shell -MethodName GetOwner -ErrorAction Stop
            if ($owner.ReturnValue -eq 0 -and $owner.User) { return "$($owner.Domain)\$($owner.User)" }
        }
    }
    catch { }
    return $null
}

# The SID of an account named by -AddOperator or offered at the prompt. Throws with the reason when the name does not resolve
# or is not a user or group account (Everyone, Authenticated Users and BUILTIN\Users are not: a name like those would put
# every user inside the boundary the group exists to draw).
function Resolve-DarlingOperatorAccount([string]$account) {
    try { $sid = (New-Object System.Security.Principal.NTAccount($account)).Translate([System.Security.Principal.SecurityIdentifier]) }
    catch { throw "'$account' is not an account this machine can resolve." }
    if (-not $sid.IsAccountSid()) { throw "'$account' is a built-in or well-known account, not a user or group account." }
    return $sid
}

# Adds the account to the local operator group with net.exe, for the reason Initialize-DarlingOperatorGroup gives. Error 1378
# (already a member) is success: a re-run leaves the group exactly as it is.
function Add-DarlingOperatorToGroup([string]$groupName, [string]$account) {
    $ErrorActionPreference = 'Continue'
    $output = & net.exe localgroup $groupName $account /add 2>&1
    if ($LASTEXITCODE -ne 0 -and -not ((Get-DarlingNativeText $output) -match '\b1378\b')) {
        throw "net localgroup `"$groupName`" $account /add failed with exit code $LASTEXITCODE`: $(Get-DarlingNativeText $output)"
    }
}

# What the group itself holds, as a path and an icacls rights string: read on darling.json, Traverse on the store folder (this
# folder only), and read on the store's admin and viewer credential files. It mirrors DarlingFileSecurity and the grants made
# at 4b and 4b3 above; a store that is not managed has no store folder and no credential files.
function Get-DarlingOperatorGrantTargets([string]$configPath, [string]$storeFolder) {
    $targets = @([pscustomobject]@{ Path = $configPath; Rights = '(R)'; IsFolder = $false })
    if ($storeFolder) {
        $targets += [pscustomobject]@{ Path = $storeFolder; Rights = '(S,X)'; IsFolder = $true }
        foreach ($name in @('pg-admin-credential.dpapi', 'pg-viewer-credential.dpapi')) {
            $targets += [pscustomobject]@{ Path = (Join-Path $storeFolder $name); Rights = '(R)'; IsFolder = $false }
        }
    }
    return $targets
}

# True when the target exists and is what it should be: a plain file with one name, or a plain folder, not a link. icacls follows
# a link, so a target that is one is never granted through.
function Test-DarlingGrantTargetPlain($target) {
    $item = Get-Item -LiteralPath $target.Path -Force -ErrorAction SilentlyContinue
    if ($null -eq $item) { return $false }
    if ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) { return $false }
    if (-not $target.IsFolder -and (Get-DarlingFileNameCount $target.Path) -ne 1) { return $false }
    return $true
}

# True when the target carries a direct (not inherited) Allow entry for the account.
function Test-DarlingDirectGrant([string]$path, $sid) {
    try {
        foreach ($rule in (Get-Acl -LiteralPath $path).Access) {
            if ($rule.IsInherited -or $rule.AccessControlType -ne 'Allow') { continue }
            try { $ruleSid = $rule.IdentityReference.Translate([System.Security.Principal.SecurityIdentifier]) } catch { continue }
            if ($ruleSid -eq $sid) { return $true }
        }
    }
    catch { }
    return $false
}

# Gives the account directly the access the group has, so the Viewer and the CLI work in the session that is already open: a
# group membership only reaches a person at their next sign-in. icacls /grant ADDS one entry and leaves the rest of the
# protected ACL as it is. The service rewrites these ACLs from scratch at every start, which drops the entry (by then the group
# covers the account); uninstall-darling.ps1 removes it for an install that is taken off. The credential files are written by the
# service's first start, so on a fresh install they are waited for (a bounded wait), and the grant is applied again after a
# settle pause, because the first start can rewrite an ACL after it was granted. Returns the paths that do NOT hold the grant.
function Grant-DarlingOperatorDirect($sid, [string]$configPath, [string]$storeFolder, [int]$waitSeconds = 180) {
    $ErrorActionPreference = 'Continue'
    $targets = @(Get-DarlingOperatorGrantTargets $configPath $storeFolder)
    $deadline = (Get-Date).AddSeconds($waitSeconds)
    $announced = $false
    while ((Get-Date) -lt $deadline -and @($targets | Where-Object { -not (Test-Path -LiteralPath $_.Path) }).Count -gt 0) {
        if (-not $announced) {
            Write-Host 'Waiting for the service to write the store credential files (its first start does this) so the grant can include them...'
            $announced = $true
        }
        Start-Sleep -Seconds 5
    }
    $apply = {
        foreach ($target in $targets) {
            if (-not (Test-DarlingGrantTargetPlain $target)) { continue }
            if (Test-DarlingDirectGrant $target.Path $sid) { continue }
            $null = & icacls.exe $target.Path /grant "*$($sid.Value):$($target.Rights)" 2>&1
        }
    }
    & $apply
    Start-Sleep -Seconds 10
    & $apply
    return @($targets | Where-Object { -not ((Test-DarlingGrantTargetPlain $_) -and (Test-DarlingDirectGrant $_.Path $sid)) } | ForEach-Object { $_.Path })
}

# The closing guidance. For a No it is who to add and that the person has to sign out and back in. After a Yes the person who was
# added does not need the sign-out sentence, so it is left out; the rest is for everyone else who runs the Viewer or the CLI.
function Write-DarlingOperatorGuidance([string]$groupName, [bool]$added) {
    Write-Host ''
    if ($added) {
        Write-Host "Add anyone else who runs the Viewer or the CLI without elevation to the local group '$groupName' (Computer Management > Local Users and Groups, or net localgroup `"$groupName`" DOMAIN\user /add); each has to sign out and back in before the membership takes effect."
    }
    else {
        Write-Host "Local group '$groupName' is in place. No one was added to it."
        Write-Host "Add each person who runs the Viewer or the CLI without elevation to the local group '$groupName' (Computer Management > Local Users and Groups, or net localgroup `"$groupName`" DOMAIN\user /add)."
        Write-Host "Members can read darling.json and the store's admin and viewer credentials, which decrypt every stored password; give it only to people you would give the passwords to."
        Write-Host 'A person added to the group has to sign out and back in before the membership takes effect.'
    }
    Write-Host ''
}

# Step 7. Asks once, at the end of the install, whether to add the signed-in person to the operator group; returns $true when
# the account was added. The answer defaults to No in every case. -AddOperator adds the named account without asking,
# -NoOperatorPrompt never asks, and a host nobody can answer on (redirected input, -NonInteractive) is treated as No.
# Anything that goes wrong is a warning and the closing guidance, never a failed install: the install itself is done.
function Invoke-DarlingOperatorStep([string]$groupName, [string]$addOperator, [bool]$noPrompt, [string]$configPath, [string]$storeFolder) {
    $account = $null
    if ($addOperator) {
        $account = $addOperator
    }
    elseif ($noPrompt) {
        Write-DarlingOperatorGuidance $groupName $false
        return $false
    }
    elseif (-not (Test-DarlingInteractiveHost)) {
        Write-Host ''
        Write-Host "Not asking whether to add anyone to '$groupName': this host cannot take an answer, so the answer is No."
        Write-DarlingOperatorGuidance $groupName $false
        return $false
    }
    else {
        $runAs = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
        $account = Get-DarlingInteractiveUser
        if (-not $account) { $account = $runAs }
        Write-Host ''
        try { $differs = (Resolve-DarlingOperatorAccount $account) -ne (Resolve-DarlingOperatorAccount $runAs) }
        catch { $differs = $false }
        if ($differs) {
            Write-Host "You are signed in as $account, but this script runs as $runAs (you elevated with another account), so adding $account is a separate decision from the one that elevated."
        }
        Write-Host "Members can read darling.json and the store's admin and viewer credentials, which decrypt every stored password, and the Viewer connects with the store's admin role; give it only to people you would give the passwords to."
        try { $answer = Read-Host "Add $account to '$groupName'? [y/N]" }
        catch { $answer = '' }
        if ($answer -notmatch '^[Yy]') {
            Write-DarlingOperatorGuidance $groupName $false
            return $false
        }
    }

    try {
        $sid = Resolve-DarlingOperatorAccount $account
        $name = $sid.Translate([System.Security.Principal.NTAccount]).Value
        Add-DarlingOperatorToGroup $groupName $name
    }
    catch {
        Write-Host "Could not add '$account' to '$groupName': $($_.Exception.Message)" -ForegroundColor Yellow
        Write-DarlingOperatorGuidance $groupName $false
        return $false
    }
    Write-Host "Added $name to the local group '$groupName'."
    try { $missing = @(Grant-DarlingOperatorDirect $sid $configPath $storeFolder) }
    catch { $missing = @($configPath) }
    if ($missing.Count -eq 0) {
        Write-Host "$name also has a direct grant of the same access, so the Viewer and the CLI work in the session that is already open. The service drops it the next time it starts; from then on the group covers $name only after $name signs out and back in (a reboot does that, a service restart or an upgrade does not). uninstall-darling.ps1 removes it; to remove it now run icacls <darling.json, the store folder, each credential file> /remove *$($sid.Value)"
    }
    else {
        Write-Host "$name is in the group, but the direct grant could not be confirmed on: $($missing -join ', '). $name has to sign out and back in before the Viewer and the CLI work without elevation." -ForegroundColor Yellow
    }
    Write-DarlingOperatorGuidance $groupName $true
    return $true
}

# Decided before step 4 stops an existing service, so a refusal here can never leave it stopped. On a domain controller the
# operator group is skipped (step 4b says so) and the install finishes; the Viewer and the CLI then run elevated.
$onDomainController = Test-DarlingDomainController
if ($onDomainController) {
    Write-Host "WARNING: this machine is a domain controller, which has no local groups, so the local group '$operatorGroupName' is NOT created and nothing is granted to it. The install continues. The Viewer and the CLI must be run elevated here: darling.json and the store's credentials are readable by SYSTEM, Administrators and the service account only." -ForegroundColor Yellow
}

# -AddOperator is checked here, before anything is stopped or changed: a name that does not resolve is an install that would
# otherwise run to its last step and only then fail to add anyone.
if ($AddOperator) {
    if ($onDomainController) {
        Write-Host "WARNING: -AddOperator is ignored: this machine is a domain controller, which has no local group to add '$AddOperator' to." -ForegroundColor Yellow
        $AddOperator = ''
    }
    else {
        try { $null = Resolve-DarlingOperatorAccount $AddOperator }
        catch { Fail "-AddOperator: $($_.Exception.Message) Nothing was installed." }
    }
}

# -- 4. Create or upgrade the service -------------------------------------------------------------
# $existing was resolved back at 1b, which needs to know fresh-versus-upgrade before it decides whether a
# bad install location is a refusal or a question. Nothing between there and here creates or removes the
# service, so it is the same answer.
# The registered path carries its quotes, as the MSI registers it (#6597). sc.exe strips a plain pair of quotes as it
# parses its command line, which is why "`"$serviceExe`"" registered the path bare, so the value has to go in as
# "\"path\"" (the outer pair is stripped, the \" pair is kept). How PowerShell turns a quoted native argument into a
# command line differs between Windows PowerShell 5.1 and PowerShell 7.x (and by $PSNativeCommandArgumentPassing), so
# the command line is handed over verbatim as ONE string through Start-Process, which behaves the same in all of them.
function Invoke-ScWithQuotedBinPath([string]$verb, [string]$restOfLine) {
    $argLine = "$verb `"$serviceName`" binPath= `"\`"$serviceExe\`"`" $restOfLine".TrimEnd()
    $p = Start-Process -FilePath "$env:SystemRoot\System32\sc.exe" -ArgumentList $argLine -Wait -PassThru -NoNewWindow
    return $p.ExitCode
}

if ($MsiPhase) {
    # The package created the service (its ServiceInstall row) and starts and stops it; this script never touches the service itself (#6480).
    Write-Host "Service '$serviceName' is managed by the MSI package - not created, stopped or started here."
}
elseif ($existing) {
    Write-Host "Service already exists - upgrading its binPath in place (config, store data, and credentials untouched)."
    if ($existing.Status -ne 'Stopped') {
        Stop-Service -Name $serviceName -Force
        (Get-Service -Name $serviceName).WaitForStatus('Stopped', [TimeSpan]::FromSeconds(60))
    }

    $scExit = Invoke-ScWithQuotedBinPath 'config' ''
    if ($scExit -ne 0) { Fail "sc config failed ($scExit)." }
}
else {
    # The space after binPath=/start=/obj= is sc.exe syntax. The virtual service account is
    # password-less, per-service, unprivileged - and mandatory-in-practice for the bundled
    # PostgreSQL, which refuses to run with administrative privileges (LocalSystem bricks it).
    $scExit = Invoke-ScWithQuotedBinPath 'create' "start= auto obj= `"NT SERVICE\$serviceName`""
    if ($scExit -ne 0) { Fail "sc create failed ($scExit)." }
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
$startName = Get-DarlingServiceLogonName $serviceName

if ($startName) {
    if ($startName -eq 'LocalSystem') { $serviceAccount = 'NT AUTHORITY\SYSTEM' }
    else { $serviceAccount = $startName -replace '^\.\\', "$env:COMPUTERNAME\" }
}
elseif ($existing) {
    Fail @"
Could not determine which account service '$serviceName' logs on as, and it already existed before this run.
Assuming the default virtual account would re-ACL darling.json around 'NT SERVICE\$serviceName' and strip the
grant of whatever domain account or gMSA the service was re-homed to (#1802, #1823), locking it out of its own
config on the next start - so this install stops here instead. darling.json was not re-ACLed (step 1b2 locked the
install folder around the account the service already had); the service is stopped with its binPath already
updated.

Check the account, then re-run this script:
  sc.exe qc $serviceName
"@
}
else {
    Write-Host "Could not determine the service's logon account; using the virtual account 'NT SERVICE\$serviceName' that sc create just set." -ForegroundColor Yellow
}

# What a native command wrote, as one line of text. In Windows PowerShell 5.1 the stderr lines of a redirected
# native command arrive as error records, and its blank lines stringify as a type name.
function Get-DarlingNativeText($output) {
    (@($output) | ForEach-Object { if ($_ -is [System.Management.Automation.ErrorRecord]) { $_.Exception.Message } else { "$_" } } | Where-Object { $_ -and $_.Trim() }) -join ' '
}

# The SID of the local operator group ($operatorGroupName), creating the group - empty - when it is missing.
# Throws with the reason when it cannot be created.
#
# net.exe, not New-LocalGroup: the LocalAccounts module does not load reliably in PowerShell 7, and net.exe
# behaves the same in both shells. The lookup comes first, so a re-run, an upgrade and a group an
# administrator already made all leave the group and its members exactly as they are. net.exe is judged by its
# exit code, with the error preference local to this function, for the reason Lock-DarlingInstallTree gives:
# under $ErrorActionPreference = 'Stop' Windows PowerShell 5.1 turns the redirected stderr of a native command
# into a terminating error before its exit code can be read.
function Initialize-DarlingOperatorGroup([string]$groupName) {
    $ErrorActionPreference = 'Continue'
    if (Test-DarlingDomainController) {
        throw "this machine is a domain controller, which has no local groups: a group made here with net localgroup is a domain group. No group was created or looked up, and the Viewer and the CLI must be run elevated."
    }
    $sidType = [System.Security.Principal.SecurityIdentifier]
    # Machine-qualified, so only this machine's own account database answers: an unqualified name is also looked
    # up in the domain and in trusted domains, and a group of that name made there must never be the one granted.
    $account = New-Object System.Security.Principal.NTAccount($env:COMPUTERNAME, $groupName)
    $sid = $null
    try { $sid = $account.Translate($sidType) }
    catch [System.Security.Principal.IdentityNotMappedException] { }
    if (-not $sid) {
        $comment = 'Members can run the PerformanceMonitor Darling Viewer and CLI without elevation (read access to darling.json and the store role credentials).'
        $output = & net.exe localgroup $groupName /add "/comment:$comment" 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "net localgroup `"$groupName`" /add failed with exit code $LASTEXITCODE`: $(Get-DarlingNativeText $output)"
        }
        $sid = $account.Translate($sidType)
    }
    $owner = $sid.Translate([System.Security.Principal.NTAccount]).Value
    if (-not ($owner.StartsWith("$env:COMPUTERNAME\", [System.StringComparison]::OrdinalIgnoreCase) -or $owner.StartsWith('BUILTIN\', [System.StringComparison]::OrdinalIgnoreCase))) {
        throw "the name '$groupName' resolves to $owner, which is not a group on this machine. Create the local group from an elevated prompt (net localgroup `"$groupName`" /add) and run this script again."
    }
    return $sid
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
# account get full control on both, and the operator group gets read on the LIVE CONFIG ONLY (the Viewer and
# the CLI verbs run as an unelevated operator and must still read darling.json; nothing reads a backup, #1769).
# NT AUTHORITY\INTERACTIVE gets nothing: it covered every logged-on user, not only the people who run the
# Viewer. Best-effort: a failure warns rather than aborting an otherwise good install, and the service
# re-asserts the live config's ACL at every startup. The operator group is the one thing that stops the
# install, once the files are locked and the service can read them: see the end of step 4b2.
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
#
# The operator group is created first because the grant below needs its SID. A failure to create it does not
# skip the hardening (that would leave darling.json readable by BUILTIN\Users): the files are locked to SYSTEM,
# Administrators and the service account without the group, which fails closed, and the install stops at the
# end of step 4b2 with the reason, not in this step (see there).
$operatorSid = $null
$operatorGroupProblem = $null
if (-not $onDomainController) {
    try { $operatorSid = Initialize-DarlingOperatorGroup $operatorGroupName }
    catch { $operatorGroupProblem = $_.Exception.Message }
}

# 4b to 4b2 and the TLS key verb ran in PreStart. The body is not re-indented, for the reason given at the start of the block at 1a.
if ($MsiPhase -ne 'PostStart') {

# Every path here is named with -LiteralPath (#4745). PowerShell reads [ and ] in a -Path value as wildcard
# characters, so under an install folder named with either, -Path finds no .bak files at all, and Get-Acl or
# Set-Acl -Path can resolve to a same-named file in a sibling folder (a[bc]d to abd) and harden that instead.
# darling.json is hardened by name below, so it is checked here the way the backups are: a darling.json with more
# than one name (a hard link) or one that is a link would take the service as its owner and a FullControl grant on
# the OTHER name or the file it points to as well. Refused, not skipped: the service reads this file, so there is no
# safe way to carry on with it (#5649). The names are counted through Get-DarlingFileNameCount, never from LinkType.
$configItem = Get-Item -LiteralPath $configPath -Force -ErrorAction SilentlyContinue
if ($null -ne $configItem -and (($configItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -or (Get-DarlingFileNameCount $configPath) -ne 1)) {
    Fail ("$configPath has more than one name (it is a hard link), could not be checked, or is a link to another file, so its permissions cannot be set without also changing the other name or the file it points to. " +
        "The service is installed but has not been started. Replace it with an ordinary copy: copy it to a new file, delete the old darling.json, rename the copy to darling.json, then run this script again.")
}

$hardened = @()
$failed = @()
foreach ($secretFile in @($configPath) + @(Get-DarlingServiceWrittenRootFiles $root -BackupsOnly | ForEach-Object { $_.FullName })) {
    try {
        $wk = [System.Security.Principal.WellKnownSidType]
        $systemSid      = New-Object System.Security.Principal.SecurityIdentifier($wk::LocalSystemSid, $null)
        $adminsSid      = New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinAdministratorsSid, $null)
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
        # The operator group's read goes to the LIVE config only. darling.json is read by the Viewer and the CLI
        # verbs as an unelevated operator; a .bak-* copy is read by nothing (#1769) - the only code that knows the
        # name is the code that creates it, and restoring one already needs elevation because writing darling.json
        # does. So a backup granting the group was a second copy of every encrypted password and access token,
        # readable by every member, buying nothing. Mirrors DarlingFileSecurity.HardenFile for backups. This DACL
        # is built from scratch, so an INTERACTIVE grant an earlier install left on the file is gone once Set-Acl
        # applies it, and the verification below fails the file if one is somehow still there.
        if ($secretFile -eq $configPath -and $operatorSid) {
            $acl.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($operatorSid, $read, $allow)))
        }
        Set-Acl -LiteralPath $secretFile -AclObject $acl

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
            $owner = Get-Acl -LiteralPath $secretFile
            $owner.SetOwner($serviceSid)
            Set-Acl -LiteralPath $secretFile -AclObject $owner
        }
        catch {
            # Not fatal: the explicit FullControl grant above already carries WRITE_DAC.
        }

        # VERIFY rather than assume. A permissions call that appears to succeed and leaves the file
        # readable is exactly what went unnoticed for months on a field box.
        $after = Get-Acl -LiteralPath $secretFile
        $exposed = $after.Access | Where-Object {
            $aceSid = $_.IdentityReference.Translate([System.Security.Principal.SecurityIdentifier])
            $_.AccessControlType -eq 'Allow' -and ($aceSid.IsWellKnown($wk::BuiltinUsersSid) -or $aceSid.IsWellKnown($wk::InteractiveSid))
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
    if ($operatorSid) {
        Write-Host "Restricted $($hardened.Count) credential file(s) to SYSTEM, Administrators, and the service account (they hold encrypted passwords and access tokens). darling.json additionally allows read for members of the local group '$operatorGroupName', which the Viewer and the CLI verbs need without elevation; its .bak-* copies do not."
    }
    else {
        Write-Host "Restricted $($hardened.Count) credential file(s) to SYSTEM, Administrators, and the service account (they hold encrypted passwords and access tokens). darling.json has no operator read grant yet, because the local group '$operatorGroupName' could not be created."
    }
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
    Write-Host "    icacls `"<file>`" /remove:g `"BUILTIN\Users`" *S-1-5-4" -ForegroundColor Yellow
    Write-Host "    icacls `"<file>`" /grant `"$serviceAccount`:(F)`"" -ForegroundColor Yellow
    Write-Host '  ...or move the install out of a world-readable folder such as one created directly under C:\.' -ForegroundColor Red
    Write-Host ''
}

# Who gets the operator group's access is the administrator's decision: the script adds a member only when the administrator
# says so, at the end of the install (step 7: the answer to the question, or -AddOperator). Who to add, and the sign-out, are
# printed there. A group that could not be created stops the install, but at the end of step 4b2, not here.
if ($onDomainController) {
    Write-Host "Domain controller: the local group '$operatorGroupName' was not created and nothing was granted to it (see the warning above). Run the Viewer and the CLI elevated." -ForegroundColor Yellow
}
elseif (-not $operatorSid) {
    Write-Host "The local group '$operatorGroupName' could not be created: $operatorGroupProblem" -ForegroundColor Yellow
}

# -- 4b2. Grant the service its access to the locked tree (#4034) ----------------------------------------
# 1b2 locked the tree before anything ran from it, with no service to grant yet; sc.exe create has made the
# account now, so this runs the same lock again with it, which adds the service's Modify grant (it extracts
# pg-runtime into the tree) and re-verifies. After 4b on purpose: its files carry their own protected DACLs,
# which the tree's ACEs do not touch. Before the first start on purpose: the extraction needs the grant.
# upgrade-darling.ps1 runs the same function, so an install made before these steps is locked at its next
# upgrade.
Invoke-InstallTreeLock $serviceAccount

# A fresh install that could not make the operator group stops here, not back in step 4b where the failure
# happened. 1b2 locked the tree with no grant for the service, and sc.exe create registered it to start
# automatically, so a stop anywhere between those and the call above would leave a service that cannot read its
# own files. Now the service can, the files are locked closed, and nothing has started, so the refusal costs the
# operator one re-run, before Start-Service. (upgrade-darling.ps1 warns and carries on for the same condition:
# there the service is already installed and its own start tightens the files again.)
if (-not $operatorSid -and -not $onDomainController) {
    Fail @"
Could not create the local group '$operatorGroupName': $operatorGroupProblem
darling.json is restricted to SYSTEM, Administrators and the service account, so nothing is open to other accounts, but the
Viewer and the CLI cannot read it without elevation until the group exists. The install stops here, before
the service starts. Create the group from an elevated prompt, then re-run this script:
  net localgroup "$operatorGroupName" /add
"@
}

# The TLS private-key files darling.json names (#5700): the service exe's own verb sets their permissions, after the last tree lock
# above so the lock cannot undo it and before the service starts. A key file in the install folder is limited to SYSTEM and
# Administrators with Read for the service account; a key file outside the install folder is reported and left as it is. A verb
# that could not run is a warning with the command to run by hand, and the install carries on.
$tlsKeyWarning = Invoke-DarlingTlsKeyFileHardening $serviceExe $configPath
if ($tlsKeyWarning) { Write-Host "WARNING: $tlsKeyWarning" -ForegroundColor Yellow }

}  # end of the PreStart-and-by-hand block that starts at 4b

# -- 4b3. Create the store folder before the service does -------------------------------------------------
# The store folder is the parent of postgres.dataDirectory: %ProgramData%\PerformanceMonitorDarling unless
# darling.json says otherwise. It holds the PostgreSQL data directory, the service log and every role
# credential, and the service runs initdb inside it on first start. An ordinary user can create a folder
# under C:\ProgramData and would then OWN it - owners can rewrite the folder's ACL and read everything the
# service writes there - so the folder has to exist, owned by Administrators and locked, before the service
# has a chance to create it. The folder is made, opened and changed through one handle, never by its name, and
# what is already in it under the names the service writes into is checked before and after the lock (see
# Initialize-DarlingStoreFolder). The ACL below is the one the service applies on every start: SYSTEM,
# Administrators and the service account in full, inherited by everything below, and the operator group
# Traverse on this folder only, so a member can open a credential file by its known path but cannot list the
# folder or reach the data directory. Nothing else is granted, and the DACL does not inherit.

# The data directory darling.json names, or $null when there is nothing to do here: a bring-your-own PostgreSQL
# install (postgres.managed = false) has no store folder, and a config that cannot be read, or a data
# directory that is not a plain drive-letter path, is left to the service to resolve, which does not change
# what it will do. Reads darling.json the way Get-DarlingExtraServiceWriteDirectories does, with a regex
# rather than ConvertFrom-Json, because Windows PowerShell 5.1 rejects the comments and trailing commas
# darling.json allows. Mirrors DarlingManagedPostgres.ResolveDataDirectory: a null, missing or blank
# dataDirectory is the default.
function Get-DarlingDataDirectory([string]$configPath) {
    $text = ''
    if (Test-Path -LiteralPath $configPath -PathType Leaf) {
        try { $text = Get-Content -LiteralPath $configPath -Raw -ErrorAction Stop }
        catch { return $null }
    }
    $stripped = ($text -split "`r?`n" | ForEach-Object { $_ -replace '(?<!:)//.*$', '' }) -join "`n"
    $managed = [regex]::Matches($stripped, '"managed"\s*:\s*(true|false)')
    if ($managed.Count -gt 0 -and $managed[$managed.Count - 1].Groups[1].Value -eq 'false') { return $null }
    $dataDirectory = Join-Path ([Environment]::GetFolderPath('CommonApplicationData')) 'PerformanceMonitorDarling\pg'
    $configured = [regex]::Matches($stripped, '"dataDirectory"\s*:\s*(?:null|"((?:[^"\\]|\\.)*)")')
    if ($configured.Count -gt 0 -and $configured[$configured.Count - 1].Groups[1].Success) {
        # JSON string escapes ("C:\\Data\\pg"); a value that is not valid JSON is the service's to reject.
        try { $value = [regex]::Unescape($configured[$configured.Count - 1].Groups[1].Value) }
        catch { return $null }
        if (-not [string]::IsNullOrWhiteSpace($value)) { $dataDirectory = $value }
    }
    if ($dataDirectory -notmatch '^[A-Za-z]:\\') { return $null }
    return [System.IO.Path]::GetFullPath($dataDirectory).TrimEnd('\')
}

# The store folder darling.json names: the data directory's parent, or $null when there is nothing to do here (see
# Get-DarlingDataDirectory).
function Get-DarlingStoreFolder([string]$configPath) {
    $dataDirectory = Get-DarlingDataDirectory $configPath
    if (-not $dataDirectory) { return $null }
    $parent = [System.IO.Path]::GetDirectoryName($dataDirectory)
    if ([string]::IsNullOrEmpty($parent)) { return $null }
    return $parent
}

# Opens the store folder itself, never what a junction or link at its name points to, and keeps it open without sharing
# delete or rename, so the folder cannot be moved or replaced while this script holds it. The folder's owner and
# DACL are read and written through that handle, never by name, so a link put at the name after the folder was checked has
# nothing to redirect: what was checked is what is changed. Create makes a folder with a given DACL in the same call that
# creates it (0 when it made the folder, 183 when one was already there, anything else is the Win32 error). Open takes DELETE
# and WRITE_DAC always and WRITE_OWNER on request; Error is the Win32 error when it could not open (IsOpen is false then). FinalPath is the real
# path the open handle resolves to, and ResolvedPath the real path of an existing folder with every link followed, so a
# link anywhere on the way to the folder shows as a difference between the two. ReadSecurity returns the owner, group and DACL
# as a self-relative descriptor. WriteDacl installs a protected DACL from its binary form, WriteOwner an owner from a SID's
# binary form; both return 0 or the Win32 error. IsReparsePoint and Attributes are read from the handle. Written for the C#
# of Windows PowerShell 5.1.
function Initialize-DarlingNativeStoreFolder {
    if ('DarlingNativeStoreFolder' -as [type]) { return }
    Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

public sealed class DarlingNativeStoreFolder : IDisposable
{
    [StructLayout(LayoutKind.Sequential)]
    private struct SecurityAttributes { public int Length; public IntPtr Descriptor; public int InheritHandle; }

    [StructLayout(LayoutKind.Sequential)]
    private struct FileTimeValue { public uint Low; public uint High; }

    [StructLayout(LayoutKind.Sequential)]
    private struct ByHandleFileInformation
    {
        public uint FileAttributes;
        public FileTimeValue CreationTime;
        public FileTimeValue LastAccessTime;
        public FileTimeValue LastWriteTime;
        public uint VolumeSerialNumber;
        public uint FileSizeHigh;
        public uint FileSizeLow;
        public uint NumberOfLinks;
        public uint FileIndexHigh;
        public uint FileIndexLow;
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern SafeFileHandle CreateFileW(string fileName, uint desiredAccess, uint shareMode, IntPtr securityAttributes, uint creationDisposition, uint flagsAndAttributes, IntPtr templateFile);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern bool CreateDirectoryW(string path, ref SecurityAttributes attributes);

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern uint GetFinalPathNameByHandleW(SafeFileHandle file, StringBuilder path, uint length, uint flags);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool GetFileInformationByHandle(SafeFileHandle file, out ByHandleFileInformation information);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern bool GetKernelObjectSecurity(SafeFileHandle handle, uint requested, byte[] descriptor, uint length, out uint needed);

    [DllImport("advapi32.dll", SetLastError = true)]
    private static extern uint SetSecurityInfo(SafeFileHandle handle, int objectType, uint securityInfo, byte[] owner, IntPtr group, byte[] dacl, IntPtr sacl);

    private SafeFileHandle handle;
    public int Error;

    public bool IsOpen { get { return handle != null; } }

    // The \\?\ spelling of an absolute path (\\?\UNC\ for a UNC path), which Windows does not trim or fold.
    private static string ExtendedPath(string path)
    {
        if (path.StartsWith(@"\\?\", StringComparison.Ordinal)) { return path; }
        if (path.StartsWith(@"\\", StringComparison.Ordinal)) { return @"\\?\UNC\" + path.Substring(2); }
        return @"\\?\" + path;
    }

    private static string FinalPathOf(SafeFileHandle file)
    {
        StringBuilder buffer = new StringBuilder(1024);
        uint length = GetFinalPathNameByHandleW(file, buffer, (uint)buffer.Capacity, 0);
        if (length >= buffer.Capacity)
        {
            buffer = new StringBuilder((int)length + 1);
            length = GetFinalPathNameByHandleW(file, buffer, (uint)buffer.Capacity, 0);
        }
        if (length == 0 || length >= buffer.Capacity) { return null; }
        string path = buffer.ToString();
        if (path.StartsWith(@"\\?\UNC\", StringComparison.Ordinal)) { return @"\\" + path.Substring(8); }
        if (path.StartsWith(@"\\?\", StringComparison.Ordinal)) { return path.Substring(4); }
        return path;
    }

    // Creates the folder with the DACL in a self-relative descriptor. 0 = created, 183 = already there, else the Win32 error.
    public static int Create(string path, byte[] descriptor)
    {
        IntPtr unmanaged = Marshal.AllocHGlobal(descriptor.Length);
        try
        {
            Marshal.Copy(descriptor, 0, unmanaged, descriptor.Length);
            SecurityAttributes attributes = new SecurityAttributes();
            attributes.Length = Marshal.SizeOf(typeof(SecurityAttributes));
            attributes.Descriptor = unmanaged;
            attributes.InheritHandle = 0;
            if (CreateDirectoryW(ExtendedPath(path), ref attributes)) { return 0; }
            return Marshal.GetLastWin32Error();
        }
        finally { Marshal.FreeHGlobal(unmanaged); }
    }

    // DELETE | READ_CONTROL | WRITE_DAC | FILE_READ_ATTRIBUTES (| WRITE_OWNER); share read and write but not delete; OPEN_EXISTING;
    // FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS. DELETE is asked for so that the sharing mode counts: Windows
    // does not check the sharing of a handle that asks for nothing but attributes and permissions, and the folder could then
    // still be renamed or replaced while it is held. The folder is never deleted through this handle.
    public static DarlingNativeStoreFolder Open(string path, bool writeOwner)
    {
        DarlingNativeStoreFolder folder = new DarlingNativeStoreFolder();
        uint access = 0x00010000u | 0x00020000u | 0x00040000u | 0x80u | (writeOwner ? 0x00080000u : 0u);
        SafeFileHandle opened = CreateFileW(ExtendedPath(path), access, 3, IntPtr.Zero, 3, 0x00200000 | 0x02000000, IntPtr.Zero);
        if (opened.IsInvalid)
        {
            folder.Error = Marshal.GetLastWin32Error();
            opened.Dispose();
            return folder;
        }
        folder.handle = opened;
        return folder;
    }

    // The real path of an existing folder with every link on the way followed, or null.
    public static string ResolvedPath(string path)
    {
        using (SafeFileHandle followed = CreateFileW(ExtendedPath(path), 0x80u, 7, IntPtr.Zero, 3, 0x02000000, IntPtr.Zero))
        {
            if (followed.IsInvalid) { return null; }
            return FinalPathOf(followed);
        }
    }

    public string FinalPath { get { return handle == null ? null : FinalPathOf(handle); } }

    // The attribute bits as the handle reports them now; 0xFFFFFFFF when they cannot be read.
    public uint Attributes
    {
        get
        {
            ByHandleFileInformation information;
            if (handle == null || !GetFileInformationByHandle(handle, out information)) { return 0xFFFFFFFFu; }
            return information.FileAttributes;
        }
    }

    public bool IsReparsePoint { get { uint bits = Attributes; return bits == 0xFFFFFFFFu || (bits & 0x400u) != 0; } }

    public bool IsDirectory { get { uint bits = Attributes; return bits != 0xFFFFFFFFu && (bits & 0x10u) != 0; } }

    // OWNER | GROUP | DACL as a self-relative descriptor, or null.
    public byte[] ReadSecurity()
    {
        uint needed;
        GetKernelObjectSecurity(handle, 7u, null, 0, out needed);
        if (needed == 0) { Error = Marshal.GetLastWin32Error(); return null; }
        byte[] descriptor = new byte[needed];
        if (!GetKernelObjectSecurity(handle, 7u, descriptor, needed, out needed)) { Error = Marshal.GetLastWin32Error(); return null; }
        return descriptor;
    }

    // OWNER | GROUP | DACL of the object at the path itself, a link not followed (READ_CONTROL only; FILE_FLAG_OPEN_REPARSE_POINT |
    // FILE_FLAG_BACKUP_SEMANTICS), or null.
    public static byte[] ReadSecurityAt(string path)
    {
        using (SafeFileHandle opened = CreateFileW(ExtendedPath(path), 0x00020000u, 7, IntPtr.Zero, 3, 0x00200000 | 0x02000000, IntPtr.Zero))
        {
            if (opened.IsInvalid) { return null; }
            uint needed;
            GetKernelObjectSecurity(opened, 7u, null, 0, out needed);
            if (needed == 0) { return null; }
            byte[] descriptor = new byte[needed];
            return GetKernelObjectSecurity(opened, 7u, descriptor, needed, out needed) ? descriptor : null;
        }
    }

    // DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION. SE_FILE_OBJECT is 1.
    public int WriteDacl(byte[] dacl)
    {
        return (int)SetSecurityInfo(handle, 1, 0x00000004u | 0x80000000u, null, IntPtr.Zero, dacl, IntPtr.Zero);
    }

    // OWNER_SECURITY_INFORMATION.
    public int WriteOwner(byte[] ownerSid)
    {
        return (int)SetSecurityInfo(handle, 1, 0x00000001u, ownerSid, IntPtr.Zero, null, IntPtr.Zero);
    }

    public void Dispose()
    {
        if (handle != null) { handle.Dispose(); handle = null; }
    }
}
'@
}

# The owner of a file or folder as a SID, read by name, or $null when it cannot be read. READ_CONTROL is all it asks for.
function Get-DarlingItemOwnerSid([string]$path) {
    try { return (Get-Acl -LiteralPath $path -ErrorAction Stop).GetOwner([System.Security.Principal.SecurityIdentifier]) }
    catch { return $null }
}

# The owner of a junction or link ITSELF, read without following it, so the folder it points to is never asked; $null when it
# cannot be read.
function Get-DarlingLinkOwnerSid([string]$path) {
    try {
        Initialize-DarlingNativeStoreFolder
        $bytes = [DarlingNativeStoreFolder]::ReadSecurityAt($path)
        if ($null -eq $bytes) { return $null }
        return (New-Object System.Security.AccessControl.RawSecurityDescriptor($bytes, 0)).Owner
    }
    catch { return $null }
}

# Why the store folder's contents cannot be accepted, or $null when they can. Nothing here is ever treated as empty because it
# could not be listed: a folder this script cannot list, or an item whose owner it cannot read, is a reason to stop, not a folder
# with nothing in it. Checked, directly under $folder: the PostgreSQL data directory ($dataDirectory), pg.log, server.key,
# server.crt and logs. Each one that exists must not be a link and must be owned by one of $trusted (SYSTEM, Administrators,
# TrustedInstaller or the service account: what the service leaves behind). The data directory's own entries must be
# listable and owned the same way, and a link among them (a relocated pg_wal, say) is accepted when the link's own owner is
# trusted and is never followed, as the service accepts it. Those names are the ones the service writes into, and initdb, pg_ctl and the key and certificate
# writers each open them without asking who made them, so something another user put there would be used as it is.
function Get-DarlingStoreContentsProblem([string]$folder, [string]$dataDirectory, $trusted) {
    $named = @('pg.log', 'server.key', 'server.crt', 'logs')
    $dataName = ''
    if ($dataDirectory) {
        $dataParent = [System.IO.Path]::GetDirectoryName($dataDirectory)
        if ($dataParent -and $dataParent.TrimEnd('\') -ieq $folder.TrimEnd('\')) {
            $dataName = [System.IO.Path]::GetFileName($dataDirectory)
            $named = @($dataName) + $named
        }
    }
    try { $entries = @(Get-ChildItem -LiteralPath $folder -Force -ErrorAction Stop) }
    catch { return "$folder cannot be listed by this script ($($_.Exception.Message)), so what is in it is not known. It is not treated as empty." }
    foreach ($name in $named) {
        $match = @($entries | Where-Object { $_.Name -ieq $name })
        if ($match.Count -eq 0) { continue }
        $item = $match[0]
        if ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) { return "$($item.FullName) is a junction or link." }
        $problem = Get-DarlingItemOwnerProblem $item.FullName $trusted
        if ($problem) { return $problem }
        if ($item.PSIsContainer -and $dataName -and $name -ieq $dataName) {
            try { $children = @(Get-ChildItem -LiteralPath $item.FullName -Force -ErrorAction Stop) }
            catch { return "$($item.FullName) cannot be listed by this script ($($_.Exception.Message)), so what is in it is not known. It is not treated as empty." }
            foreach ($child in $children) {
                # A link among the entries is the operator's own relocation (pg_wal, say), which the service accepts: it is
                # allowed when its own owner is trusted, and it is never followed.
                if ($child.Attributes -band [System.IO.FileAttributes]::ReparsePoint) {
                    $linkOwner = Get-DarlingLinkOwnerSid $child.FullName
                    if ($null -eq $linkOwner -or -not ($trusted -contains $linkOwner)) { return "$($child.FullName) is a junction or link whose owner is not SYSTEM, Administrators or the service account (or cannot be read), so another user may have made it." }
                    continue
                }
                $childProblem = Get-DarlingItemOwnerProblem $child.FullName $trusted
                if ($childProblem) { return $childProblem }
            }
        }
    }
    return $null
}

# Why one file or folder's owner is not acceptable, or $null when it is one of $trusted.
function Get-DarlingItemOwnerProblem([string]$path, $trusted) {
    $owner = Get-DarlingItemOwnerSid $path
    if ($null -eq $owner) { return "The owner of $path cannot be read by this script, so it is not trusted." }
    if ($trusted -contains $owner) { return $null }
    try { $ownerName = $owner.Translate([System.Security.Principal.NTAccount]).Value }
    catch { $ownerName = $owner.Value }
    return "$path is owned by $ownerName, which is not SYSTEM, Administrators or the service account, so another user may have made it."
}

# Creates $folder if it is missing, makes Administrators its owner, and gives it the DACL described above.
# Throws with the reason when it cannot leave the folder in that state, so the caller stops before the
# service starts. Nothing about the folder is decided by name: the folder is created with its DACL in the call that
# creates it, or opened once without following a link and kept open without sharing delete or rename, and the
# owner and the DACL are read and written through that one handle (DarlingNativeStoreFolder). A junction or link
# at the name is refused, the folder's real path must still be its own path, and both are checked again after the
# write. A folder that already exists is trusted only if its owner is SYSTEM, Administrators, TrustedInstaller, the
# service account or this service's own service SID (see $trusted below): that is what the service leaves behind, so an
# upgrade passes, including one after the Log On account was changed. A folder owned by anyone else is what a pre-created folder looks like, and it is refused whether
# or not it is empty: taking it over cannot make what its owner holds, or can still do through a handle opened
# earlier, trustworthy. What is in the folder is judged by Get-DarlingStoreContentsProblem before the ACL is
# written and again once it is in place.
function Initialize-DarlingStoreFolder([string]$folder, [string]$serviceAccount, $operatorSid, [string]$dataDirectory = '', [string]$serviceName = '') {
    $ErrorActionPreference = 'Continue'
    $wk = [System.Security.Principal.WellKnownSidType]
    $sidType = [System.Security.Principal.SecurityIdentifier]
    $systemSid = New-Object System.Security.Principal.SecurityIdentifier($wk::LocalSystemSid, $null)
    $adminsSid = New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinAdministratorsSid, $null)
    $serviceSid = (New-Object System.Security.Principal.NTAccount($serviceAccount)).Translate($sidType)
    # The owners a store left by an earlier run can have: SYSTEM, Administrators, TrustedInstaller, the account the service
    # logs on as now, and this service's own service SID (S-1-5-80-...), which owns everything the service made while it ran
    # as the default virtual account. The Log On account can be changed to a domain account or a gMSA afterwards, and the
    # items stay owned by that SID. Only this service's processes (or a holder of SeRestorePrivilege) can make something
    # owned by it, so trusting it gives another user nothing. The service trusts exactly this list.
    $trusted = @($systemSid, $adminsSid, $serviceSid, (New-Object System.Security.Principal.NTAccount('NT SERVICE\TrustedInstaller')).Translate($sidType)) + @(Get-DarlingServiceSids '' $serviceName)

    $full = [System.IO.Path]::GetFullPath($folder).TrimEnd('\')
    $parent = [System.IO.Path]::GetDirectoryName($full)
    if ([string]::IsNullOrEmpty($parent)) { throw "$folder is a drive root, not a folder this script will lock. Point postgres.dataDirectory in darling.json at a folder below it." }

    $rights = [System.Security.AccessControl.FileSystemRights]
    $inheritFlags = [System.Security.AccessControl.InheritanceFlags]
    $none = [System.Security.AccessControl.PropagationFlags]::None
    $allow = [System.Security.AccessControl.AccessControlType]::Allow
    $acl = New-Object System.Security.AccessControl.DirectorySecurity
    $acl.SetAccessRuleProtection($true, $false)
    foreach ($sid in @($systemSid, $adminsSid, $serviceSid)) {
        $acl.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($sid, $rights::FullControl, ($inheritFlags::ContainerInherit -bor $inheritFlags::ObjectInherit), $none, $allow)))
    }
    if ($operatorSid) {
        $acl.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($operatorSid, ($rights::Traverse -bor $rights::Synchronize), $inheritFlags::None, $none, $allow)))
    }
    $descriptor = $acl.GetSecurityDescriptorBinaryForm()
    $raw = New-Object System.Security.AccessControl.RawSecurityDescriptor($descriptor, 0)
    $daclBytes = New-Object byte[] $raw.DiscretionaryAcl.BinaryLength
    $raw.DiscretionaryAcl.GetBinaryForm($daclBytes, 0)
    $adminsBytes = New-Object byte[] $adminsSid.BinaryLength
    $adminsSid.GetBinaryForm($adminsBytes, 0)

    Initialize-DarlingNativeStoreFolder
    if (-not (Test-Path -LiteralPath $parent -PathType Container)) { $null = [System.IO.Directory]::CreateDirectory($parent) }
    # Create, never "create if missing": a folder that appeared between a check and this call is reported as already there
    # (183), and then judged as one that existed, rather than being adopted as the folder this script made.
    $created = [DarlingNativeStoreFolder]::Create($full, $descriptor)
    if ($created -ne 0 -and $created -ne 183) { throw "$folder could not be created (Windows error $created)." }
    $existed = ($created -eq 183)

    $lock = [DarlingNativeStoreFolder]::Open($full, $true)
    if (-not $lock.IsOpen) {
        throw "$folder could not be opened to change its permissions (Windows error $($lock.Error)). If another program has it open, close that and re-run this script. If you did not create it, delete it. If it is your existing store, give Administrators full control of it first."
    }
    try {
        # What was opened must be the folder at this name and nothing a link redirects: not a reparse point itself, and its
        # real path is the real path of its parent plus its own name.
        $realParent = [DarlingNativeStoreFolder]::ResolvedPath($parent)
        if (-not $realParent) { throw "$parent could not be resolved." }
        $expectedPath = (Join-Path $realParent ([System.IO.Path]::GetFileName($full)))
        $assertSame = {
            param([string]$when)
            if (-not $lock.IsDirectory) { throw "$folder exists and is a file, not a folder ($when)." }
            if ($lock.IsReparsePoint) { throw "$folder is a junction or link ($when). Remove it, or point postgres.dataDirectory in darling.json at a real folder, then re-run this script." }
            if (-not [string]::Equals($lock.FinalPath, $expectedPath, [System.StringComparison]::OrdinalIgnoreCase)) {
                throw "$folder resolves to $($lock.FinalPath) ($when), not to $expectedPath, so a junction or link is on its path. Nothing was changed through it. Remove the link, or point postgres.dataDirectory in darling.json at a real folder, then re-run this script."
            }
        }
        & $assertSame 'before the permissions were changed'

        $before = New-Object System.Security.AccessControl.DirectorySecurity
        $securityBytes = $lock.ReadSecurity()
        if ($null -eq $securityBytes) { throw "the permissions of $folder could not be read (Windows error $($lock.Error))." }
        $before.SetSecurityDescriptorBinaryForm($securityBytes)
        $owner = $before.GetOwner($sidType)
        if ($existed -and $trusted -notcontains $owner) {
            try { $ownerName = $owner.Translate([System.Security.Principal.NTAccount]).Value }
            catch { $ownerName = $owner.Value }
            throw "$folder already exists and is owned by $ownerName, which is not SYSTEM, Administrators or the service account, so another user may have made it. If you did not create it, delete it and re-run this script: this script does not take over a folder it did not make. If it is your existing store (for example you changed the service's Log On account), make Administrators the owner of everything in it first, from an elevated prompt, then re-run this script: icacls `"$folder`" /setowner *$($adminsSid.Value) /T /C /L"
        }
        if ($existed) {
            $contents = Get-DarlingStoreContentsProblem $full $dataDirectory $trusted
            if ($contents) { throw "$contents If you did not create $folder, delete it (take ownership first if Windows refuses) and re-run this script. If it is your existing store, give Administrators ownership of, and full control over, everything in it from an elevated prompt, then re-run this script: icacls `"$folder`" /setowner *$($adminsSid.Value) /T /C /L" }
        }

        # A folder this script made is owned by whoever created it: Administrators, or the installing user's own account
        # where policy gives the object's creator ownership. The owner is set before the DACL is, through the same handle.
        if ($owner -ne $adminsSid) {
            if ($trusted -notcontains $owner -and ($existed -or $owner -ne [System.Security.Principal.WindowsIdentity]::GetCurrent().User)) {
                throw "$folder is owned by $($owner.Value), which is not SYSTEM, Administrators or the service account."
            }
            $error1 = $lock.WriteOwner($adminsBytes)
            if ($error1 -ne 0) { throw "could not make Administrators the owner of ${folder} (Windows error $error1)." }
        }
        $error2 = $lock.WriteDacl($daclBytes)
        if ($error2 -ne 0) { throw "could not set the permissions of ${folder} (Windows error $error2)." }

        # VERIFY rather than assume, through the same handle: the folder must now be protected, owned by Administrators,
        # and grant exactly the principals above, and it must still be the folder that was checked.
        & $assertSame 'after the permissions were changed'
        $after = New-Object System.Security.AccessControl.DirectorySecurity
        $afterBytes = $lock.ReadSecurity()
        if ($null -eq $afterBytes) { throw "the permissions of $folder could not be read back (Windows error $($lock.Error))." }
        $after.SetSecurityDescriptorBinaryForm($afterBytes)
        $expected = @(@($systemSid, $adminsSid, $serviceSid) + @($operatorSid | Where-Object { $_ }) | ForEach-Object { $_.Value } | Sort-Object -Unique)
        $actual = @($after.GetAccessRules($true, $true, $sidType) | ForEach-Object { $_.IdentityReference.Value } | Sort-Object -Unique)
        if (-not $after.AreAccessRulesProtected -or $after.GetOwner($sidType) -ne $adminsSid -or @(Compare-Object $expected $actual).Count -gt 0) {
            throw "$folder was not left protected, owned by Administrators, and granting only SYSTEM, Administrators, the service account and the operator group."
        }
        # What a user could put in the folder while it was still open to them is looked at once more under the lock.
        $contents = Get-DarlingStoreContentsProblem $full $dataDirectory $trusted
        if ($contents) { throw "$contents This was found after $folder was locked. Delete $folder and re-run this script." }
    }
    finally { $lock.Dispose() }
}

$storeFolder = Get-DarlingStoreFolder $configPath
if ($MsiPhase -eq 'PostStart') {
    # PreStart secured it before the service started; step 7 only needs to know where it is.
}
elseif ($storeFolder) {
    try { Initialize-DarlingStoreFolder $storeFolder $serviceAccount $operatorSid (Get-DarlingDataDirectory $configPath) $serviceName }
    catch {
        Fail @"
The store folder '$storeFolder' could not be secured before the service's first start: $($_.Exception.Message)
The service was not started: it would run initdb inside a folder this script could not lock. Fix the folder, then re-run this script.
"@
    }
    Write-Host "Store folder '$storeFolder' is owned by Administrators and locked to SYSTEM, Administrators and the service account; the operator group can only traverse it."
}
else {
    Write-Host 'No store folder to secure here: postgres.managed is false, or darling.json could not be read or names a data directory this script does not handle. The service creates and locks its own store folder on first start.'
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
# On a FRESH install that is not merely safe, it is the only correct time: config_service does not exist yet
# and is seeded FROM darling.json, so the file is the control plane's future answer and cannot be wrong.
# On an UPGRADE it can be: the store already holds a port that may have been moved in the Viewer, and the
# managed store is a child of the service, so with the service stopped there is nothing here to ask. That is
# what 5a below exists for - see the reasoning there.
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

if ($MsiPhase -ne 'PostStart') { Invoke-FirewallReconcile }

# PreStart ends here: the package starts the service next, and PostStart is the run after that (#6480).
if ($MsiPhase -eq 'PreStart') {
    Write-Host 'PreStart done: permissions, operator group, store folder and firewall rules are in place. The package starts the service next.' -ForegroundColor Green
    exit 0
}

# -- 5. Start + wait until the service is ready (#6470) -------------------------------------------
# "Running" only means the process is up. The first start still unpacks the bundled Postgres, runs initdb (its own timeout is
# 180 seconds), migrates the store and provisions the roles before the collection loop starts, and a failure in any of those
# leaves the service Running with nothing collected. So the install waits for the service's own words: its log, which is the
# one place every service surface already writes. The service logs "Postgres store ready" when the store is created and
# migrated, and "collection loop started" when collection begins; a startup it cannot get past logs a CRITICAL line instead.
# The collection loop starting is the ready signal, not a row in collection_log: a fresh install may list no servers yet, and
# then no collection would ever be written. The log is read by timestamp, so an upgrade does not mistake the previous run's
# lines for this one's.
$readyTimeoutSeconds = 420
$serviceLogDirectory = Join-Path ([Environment]::GetFolderPath('CommonApplicationData')) 'PerformanceMonitorDarling\logs'

# The service's log entries written at or after $since, oldest first. A line is "yyyy-MM-dd HH:mm:ss.fff [LEVEL] [category]
# message"; a line that does not start that way continues the entry above it. The files are read with sharing, because the
# service has today's open for append. Nothing here throws: a log that cannot be read yet is no entries. Callers wrap the call
# in @() so zero or one entry is still an array.
function Get-DarlingServiceLogEntries([string]$logDirectory, [datetime]$since) {
    $entries = New-Object System.Collections.Generic.List[object]
    if (-not (Test-Path -LiteralPath $logDirectory -PathType Container)) { return @() }
    $files = @(Get-ChildItem -LiteralPath $logDirectory -Filter 'darling-service_*.log' -File -ErrorAction SilentlyContinue |
        Where-Object { $_.LastWriteTime -ge $since } | Sort-Object Name)
    foreach ($file in $files) {
        try {
            $stream = [System.IO.File]::Open($file.FullName, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
            try { $text = (New-Object System.IO.StreamReader($stream)).ReadToEnd() } finally { $stream.Dispose() }
        }
        catch { continue }
        $current = $null
        foreach ($line in ($text -split "`r?`n")) {
            if ($line -match '^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) \[([A-Z? ]{5})\] \[[^\]]*\] (.*)$') {
                $time = [datetime]::ParseExact($Matches[1], 'yyyy-MM-dd HH:mm:ss.fff', [System.Globalization.CultureInfo]::InvariantCulture)
                if ($time -ge $since) {
                    $current = [pscustomobject]@{ Time = $time; Level = $Matches[2].Trim(); Text = $Matches[3] }
                    $entries.Add($current)
                }
                else { $current = $null }
            }
            elseif ($current -and $line.Trim()) { $current.Text += "`n" + $line.TrimEnd() }
        }
    }
    return $entries.ToArray()
}

# What the entries say about the start: 'Ready' once the collection loop has started (whatever failed and retried before it),
# 'Failed' when the service logged a startup failure it will not get past without help, otherwise 'Waiting'. The failure lines
# are the service's CRITICAL startup lines only: a CRITICAL line about a file's permissions is a warning to read, not a start
# that failed, so it does not stop the wait. The sustained-retry criticals (#4508: "...within the Ns retry budget ...; this is
# still a recoverable failure, so it keeps retrying") are logged at ~120 seconds while the service keeps trying every minute, so
# they are not terminal either: a store that comes up at 180 seconds still reaches the collection loop inside the wait.
function Get-DarlingStartupVerdict($entries) {
    $failurePattern = '^(Cannot load configuration|Configuration problem|Managed Postgres bootstrap|Cannot reach or migrate the Postgres store|postgres\.managed = true requires)'
    foreach ($entry in $entries) {
        if ($entry.Text -match '^PerformanceMonitor Darling collection loop started') { return [pscustomobject]@{ State = 'Ready'; Lines = @() } }
    }
    $failures = @($entries | Where-Object { $_.Level -eq 'CRIT' -and $_.Text -match $failurePattern -and $_.Text -notmatch 'still a recoverable failure' })
    if ($failures.Count -gt 0) { return [pscustomobject]@{ State = 'Failed'; Lines = $failures } }
    return [pscustomobject]@{ State = 'Waiting'; Lines = @() }
}

# The log lines that explain a start that did not finish: the warnings, errors and critical lines since the start, newest last.
function Get-DarlingStartupProblemLines($entries, [int]$last = 12) {
    $problems = @($entries | Where-Object { $_.Level -in @('WARN', 'ERROR', 'CRIT') })
    if ($problems.Count -gt $last) { $problems = @($problems | Select-Object -Last $last) }
    return @($problems | ForEach-Object {
        $first = ($_.Text -split "`n")[0]
        if ($first.Length -gt 400) { $first = $first.Substring(0, 400) + '...' }
        '{0} [{1}] {2}' -f $_.Time.ToString('HH:mm:ss'), $_.Level, $first
    })
}

# The one sentence that says what to do about a start that did not finish, true for the state it ended in: a logged failure and a
# stopped service are not retrying anything, and a wait that merely ran out of time may be a store that is still being tried.
function Get-DarlingStartupRemedy([string]$state, [int]$problemLineCount, [string]$serviceName) {
    $restart = "'Restart-Service -Name `"$serviceName`"'"
    switch ($state) {
        'Failed' { return "The service logged a startup failure it will not get past without help. Fix what the lines above say, then restart it ($restart), or re-run this script." }
        'Stopped' { return "The service is not running. Read the log lines above if there are any, fix what they say, then start it ('Start-Service -Name `"$serviceName`"'), or re-run this script." }
    }
    if ($problemLineCount -gt 0) { return "The service may still be retrying a store it cannot reach. Fix what the lines above say, then restart it ($restart), or re-run this script." }
    return "The service may only be slow. Check the log again in a minute; if it stays quiet, restart it ($restart) or re-run this script."
}

# What the end of the run says about a start that did not finish, and the exit code it ends with. By hand the install failed, so the
# exit is 1. Under -MsiPhase a nonzero exit rolls the whole package back, and by PostStart the store may already have migrated: the
# old binaries would go back on a newer store. So the package keeps its install, the same problem and remedy are written as a warning
# into the MSI log, and the exit is 0 (#6480).
function Write-DarlingNotReadyVerdict([string]$startupProblem, [string]$startupRemedy, [string]$msiPhase) {
    if ($msiPhase) {
        Write-Host "WARNING: installed, but not ready: $startupProblem" -ForegroundColor Yellow
        Write-Host $startupRemedy -ForegroundColor Yellow
        return 0
    }
    Write-Host "Installed, but not ready: $startupProblem" -ForegroundColor Red
    Write-Host $startupRemedy -ForegroundColor Yellow
    return 1
}

# Waits until the service is ready, has logged a startup failure, has stopped, or $timeoutSeconds have passed, and says which:
# State is Ready, Failed, Stopped or TimedOut, with the entries read last. Progress goes to the console: each step the service
# logs once, and a line every 30 seconds with its latest log line, so a long initdb does not look like a hang. The log, the
# service state and the sleep are parameters so a test can stand in for the machine; the elapsed time is the sum of the sleeps.
function Wait-DarlingServiceReady([string]$serviceName, [string]$logDirectory, [datetime]$since, [int]$timeoutSeconds,
    [int]$pollSeconds = 3, [scriptblock]$getEntries = $null, [scriptblock]$getStatus = $null, [scriptblock]$sleep = $null) {
    if (-not $getEntries) { $getEntries = { param($dir, $from) Get-DarlingServiceLogEntries $dir $from } }
    if (-not $getStatus) { $getStatus = { param($name) (Get-Service -Name $name -ErrorAction SilentlyContinue).Status } }
    if (-not $sleep) { $sleep = { param($seconds) Start-Sleep -Seconds $seconds } }
    $elapsed = 0
    $reported = @{}
    $lastBeat = 0
    while ($true) {
        $entries = @(& $getEntries $logDirectory $since)
        $verdict = Get-DarlingStartupVerdict $entries
        foreach ($step in @(
                @{ Pattern = '^Loaded configuration from'; Text = 'darling.json read' },
                @{ Pattern = '^Postgres store ready'; Text = 'store created and migrated' })) {
            if (-not $reported.ContainsKey($step.Text) -and @($entries | Where-Object { $_.Text -match $step.Pattern }).Count -gt 0) {
                $reported[$step.Text] = $true
                Write-Host ("  [{0,3}s] {1}" -f $elapsed, $step.Text)
            }
        }
        if ($verdict.State -ne 'Waiting') {
            return [pscustomobject]@{ State = $verdict.State; Seconds = $elapsed; Entries = $entries }
        }
        $status = & $getStatus $serviceName
        if ($status -ne 'Running' -and $status -ne 'StartPending') {
            return [pscustomobject]@{ State = 'Stopped'; Seconds = $elapsed; Entries = $entries }
        }
        if ($elapsed -ge $timeoutSeconds) {
            return [pscustomobject]@{ State = 'TimedOut'; Seconds = $elapsed; Entries = $entries }
        }
        if ($elapsed - $lastBeat -ge 30) {
            $lastBeat = $elapsed
            $latest = if ($entries.Count -gt 0) { (($entries[$entries.Count - 1].Text -split "`n")[0]) } else { 'nothing logged yet' }
            if ($latest.Length -gt 100) { $latest = $latest.Substring(0, 100) + '...' }
            Write-Host ("  [{0,3}s] still starting - latest log line: {1}" -f $elapsed, $latest)
        }
        & $sleep $pollSeconds
        $elapsed += $pollSeconds
    }
}

# The web dashboard address the service logged for this start ("Starting web dashboard on http://localhost:5153 ..."), or $null.
# A LAN-exposed dashboard logs the address it LISTENS on, and a wildcard listen (0.0.0.0 or ::) is not an address a browser can
# open, so the machine's name stands in for it.
function Get-DarlingWebAddress($entries, [string]$machineName = $env:COMPUTERNAME) {
    $address = $null
    foreach ($entry in $entries) {
        if ($entry.Text -match '^Starting web dashboard on (https?://\S+)') { $address = $Matches[1] }
    }
    if ($address -and $machineName) {
        $address = $address -replace '^(https?://)(0\.0\.0\.0|\[::\]|::)(?=:\d+)', ('${1}' + $machineName)
    }
    return $address
}

# How many servers darling.json listed when the service loaded it, or $null when it did not log that yet.
function Get-DarlingConfiguredServerCount($entries) {
    $count = $null
    foreach ($entry in $entries) {
        if ($entry.Text -match '^Loaded configuration from .*: (\d+) server\(s\)') { $count = [int]$Matches[1] }
    }
    return $count
}

# Under -MsiPhase the package has already started the service (StartServices runs before PostStart), so this script does not start
# it, and "now" is not when it started: on a warm upgrade the store can be ready within seconds, before this script got here, and a
# wait that began later would never see the line. The start is the service process's own (#6480). If that cannot be read, two
# minutes back is close enough, and the verdict only reads this start's lines or the ones just before it.
function Get-DarlingServiceStartedAt([string]$serviceName) {
    try {
        $servicePid = [int](Get-CimInstance -ClassName Win32_Service -Filter "Name='$serviceName'" -ErrorAction Stop).ProcessId
        if ($servicePid -gt 0) { return (Get-Process -Id $servicePid -ErrorAction Stop).StartTime }
    }
    catch { $null = $_ }
    return (Get-Date).AddMinutes(-2)
}

if ($MsiPhase) {
    $startedAt = Get-DarlingServiceStartedAt $serviceName
}
else {
    $startedAt = Get-Date
    Start-Service -Name $serviceName
    (Get-Service -Name $serviceName).WaitForStatus('Running', [TimeSpan]::FromSeconds(60))
}
Write-Host "Service is Running. Its first start unpacks the bundled Postgres, runs initdb (up to 3 minutes), migrates the store and starts collection." -ForegroundColor Green
Write-Host "Waiting for it to be ready (up to $([int]($readyTimeoutSeconds / 60)) minutes). It has not hung; do not stop it..." -ForegroundColor Cyan
$ready = Wait-DarlingServiceReady $serviceName $serviceLogDirectory $startedAt $readyTimeoutSeconds
$startupProblem = $null
if ($ready.State -eq 'Ready') {
    Write-Host "Service is ready: the store is created and collection has started ($($ready.Seconds) seconds)." -ForegroundColor Green
}
else {
    $startupProblem = switch ($ready.State) {
        'Failed' { 'The service logged a startup failure and is not collecting.' }
        'Stopped' { 'The service stopped while starting.' }
        default { "The service was not ready after $($ready.Seconds) seconds." }
    }
    Write-Host ''
    Write-Host "WARNING: $startupProblem" -ForegroundColor Red
    $problemLines = @(Get-DarlingStartupProblemLines $ready.Entries)
    if ($problemLines.Count -gt 0) {
        Write-Host 'The log lines that explain it (newest last):' -ForegroundColor Yellow
        foreach ($problemLine in $problemLines) { Write-Host "  $problemLine" -ForegroundColor Yellow }
    }
    else {
        Write-Host 'The service has logged no warning or error since it started, so it may only be slow. Check again in a minute.' -ForegroundColor Yellow
    }
    $startupRemedy = Get-DarlingStartupRemedy $ready.State $problemLines.Count $serviceName
    Write-Host ''
}
Write-Host "Primary log: $serviceLogDirectory\darling-service_yyyyMMdd.log"

# -- 5a. Re-reconcile the firewall on an UPGRADE (#2436) -------------------------------------------
# The 4c call ran with the service stopped, which on a managed install means the store was down too - it is
# a child of the service. So on an upgrade the verb fell back to darling.json's mcp.port / web.port, and on
# a box whose port was later moved in the Viewer's Settings that is the WRONG port: the rule lands where
# nothing listens while the served port stays shut. The verb says so when it falls back, and the running
# service's own start-up check then WARNs the exact command - so this always did self-heal with one operator
# action. This removes the need for that action in the case where it was never necessary, because the store
# is up now and can simply be asked.
#
# Only on an upgrade, and that is the point rather than an optimisation. On a fresh install the first start
# is doing initdb and the first migration for ~2 minutes, so a second call here could not read the store
# either - it would re-print the same fallback disclosure more loudly about a port that, on a fresh box, is
# by definition correct. Skipping it by construction beats skipping it by hoping the timing works out.
#
# Not a guarantee: the store answers when it answers, and the verb bounds its read to ten seconds. If it
# still cannot, this run says exactly what the 4c run said and the operator has the same remedy they had
# before - so the window narrows rather than closing, which is the honest claim. The verb is idempotent, so
# a redundant run costs nothing but output.
if ($isUpgrade) {
    Write-Host ''
    Write-Host 'Re-applying the firewall rules now that the store can answer (upgrade path)...' -ForegroundColor Cyan
    Invoke-FirewallReconcile
}

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

# The 'Darling Viewer' shortcut paths under the named special folders (#6475). A folder that does not resolve (GetFolderPath
# returns '' when there is no such folder for the account) is left out. The folder lookup is a parameter so a test can stand in
# for the machine. Kept byte-identical in install-darling.ps1 and uninstall-darling.ps1.
function Get-DarlingShortcutPaths([string[]]$Folders, [scriptblock]$GetFolder = $null) {
    if (-not $GetFolder) { $GetFolder = { param($name) [Environment]::GetFolderPath($name) } }
    $paths = @()
    foreach ($folder in $Folders) {
        $base = & $GetFolder $folder
        if ($base) { $paths += (Join-Path $base 'Darling Viewer.lnk') }
    }
    return $paths
}

# -- 6. Viewer shortcuts --------------------------------------------------------------------------
if (-not $NoShortcuts -and -not $MsiPhase) {
    if (Test-Path -LiteralPath $viewerExe) {
        # GetFolderPath returns '' for Desktop/StartMenu whenever there is no loaded user profile - a
        # Windows service, a scheduled task, a CI runner, a remote/SSM session - and Join-Path then
        # throws. Guard it: keep only the targets whose folder resolves, and skip cleanly otherwise.
        # The all-users folders, not the running account's: this script runs elevated, often as an administrator
        # account that is not the person who will open the Viewer, and that account's own Desktop is not theirs
        # (#6475). The public Desktop and the all-users Start Menu show the shortcut to everyone on the machine.
        $targets = @(Get-DarlingShortcutPaths @('CommonDesktopDirectory', 'CommonPrograms'))
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

# -- 7. Offer to add the signed-in person to the operator group (#6478) -----------------------------------
# No group on a domain controller, so no question there. The group exists by now on any other machine: a group that could not
# be made stopped the install at 4b2.
if ($operatorSid) {
    # Under -MsiPhase nobody can answer (a custom action has no console), so the question is never asked: the MSI's OPERATORACCOUNT
    # arrives as -AddOperator, and without it the closing guidance says who to add (#6480).
    $null = Invoke-DarlingOperatorStep $operatorGroupName $AddOperator ([bool]($NoOperatorPrompt -or $MsiPhase)) $configPath $storeFolder
}

# True when darling.json's "web" block turns the dashboard on. The match stays inside that block (nested braces one level deep, such as
# network, are skipped over; the braces are hex escapes so a brace counter reading this function stays balanced), so a later block's "enabled": true (alerts, analysis, mcp) cannot answer for a web block that says false.
function Test-DarlingWebEnabledText([string]$configText) {
    return [bool]($configText -match '(?s)"web"\s*:\s*\x7B(?:[^\x7B\x7D]|\x7B[^\x7B\x7D]*\x7D)*?"enabled"\s*:\s*true')
}

# -- 8. Next steps (#6470) --------------------------------------------------------------------------
# What the web dashboard's address is comes from the service's own log line for this start. The web host starts a few seconds
# after the store is ready, so when darling.json turns the dashboard on, wait for that line briefly.
$entriesNow = @(Get-DarlingServiceLogEntries $serviceLogDirectory $startedAt)
$webAddress = Get-DarlingWebAddress $entriesNow
$webEnabledInConfig = $false
if (-not $startupProblem -and -not $webAddress -and (Test-Path -LiteralPath $configPath -PathType Leaf)) {
    $configText = Get-Content -LiteralPath $configPath -Raw -ErrorAction SilentlyContinue
    if (Test-DarlingWebEnabledText $configText) {
        $webEnabledInConfig = $true
        for ($waited = 0; $waited -lt 15 -and -not $webAddress; $waited += 3) {
            Start-Sleep -Seconds 3
            $webAddress = Get-DarlingWebAddress @(Get-DarlingServiceLogEntries $serviceLogDirectory $startedAt)
        }
    }
}
$serverCount = Get-DarlingConfiguredServerCount $entriesNow

Write-Host ''
if ($startupProblem) {
    exit (Write-DarlingNotReadyVerdict $startupProblem $startupRemedy $MsiPhase)
}
Write-Host 'Done. Next steps:' -ForegroundColor Green
Write-Host "  1. Open the Viewer: the 'Darling Viewer' shortcut on the Desktop or in the Start Menu, or $viewerExe"
if ($serverCount -gt 0) {
    Write-Host "  2. darling.json already lists $serverCount server(s), so collection for them has started and their first data appears within a few minutes. Add more with 'Add Server' in the Viewer."
}
else {
    Write-Host "  2. Add your first SQL Server: click 'Add Server' in the Viewer's sidebar, or run:  & `"$serviceExe`" --add-server --help"
}
if ($webAddress) {
    Write-Host "  3. The web dashboard is on: $webAddress   (sign in with the token from:  & `"$serviceExe`" --print-web-token )"
}
elseif ($webEnabledInConfig) {
    # Turned on in darling.json but no address was logged during the wait above: say so, and where to look, instead of leaving the line out.
    Write-Host "  3. The web dashboard is turned on in darling.json, but the service did not report its address within 15 seconds. It may still be starting, or it did not start: its log is in $serviceLogDirectory (look for 'web dashboard')."
}
