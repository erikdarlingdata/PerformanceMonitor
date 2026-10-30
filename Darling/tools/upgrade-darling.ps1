<#
.SYNOPSIS
Upgrades an installed PerformanceMonitor Darling in place from a newer build, keeping a BOUNDED number of
rollback backups - or prunes the ones an earlier deploy left behind (-PruneOnly).

.DESCRIPTION
This is the supported version of a procedure that had been living in people's heads and in ad-hoc SSM
scripts: stop the service, copy the install tree's root files aside as _rollback_manual_<stamp>, lay the new
build over the top, start the service, verify. Every step of it was already documented somewhere. What was
missing was the step nobody remembered to do by hand, which is deleting the backups from the LAST twenty
deploys - a dogfood box was found carrying 46 of them, 5.48 GB, the oldest three weeks old, and the service
warning about every single one on every single start (#2525).

Retention lives HERE, at deploy time, and not in the service. The service does not delete things it did not
create; this script created every one of these directories, so this script is the only thing entitled to
remove them. It keeps the newest -KeepRollbacks (3 by default) and removes the rest, after the new backup is
made - so a failed upgrade always has something to roll back to, including the copy it just took.

What it does, in order:

  1. Verifies elevation, resolves the install root from the REGISTERED service (or -InstallRoot), and
     refuses if the service is not installed - this upgrades an install, it does not create one. Use
     install-darling.ps1 for that.
  2. Refuses to copy a source that IS the install root. The upgrade would be overwriting this very script
     while PowerShell is reading it, and Expand-Archive dying half-way through leaves the service stopped
     with mixed binaries. Run the copy that came out of the NEW zip instead.
  3. Verifies the source zip's SHA256 (against -Sha256, or a SHA256SUMS.txt sitting beside it). Refuses an
     unverified zip unless you say -SkipHashCheck out loud.
  4. Names any process running out of the install tree and STOPS - it never kills one. The bundled
     PostgreSQL lives under pg-runtime and a blanket kill takes the store down with it; the last time this
     guard fired, what it caught was an operator's own psql.exe sitting in the install directory from a
     diagnostic query.
  5. Stops the service and waits for it to actually be Stopped.
  6. Copies the install root's FILES (not its subdirectories) to _rollback_manual_<stamp>. A re-run of an
     INTERRUPTED upgrade inside -BackupWindowMinutes reuses the existing backup instead of taking a second one,
     because a second one would be a copy of the half-extracted tree written over your only good copy. An
     upgrade that FINISHED (darling-install-manifest.txt is newer than the backup) always gets a new backup,
     so two complete upgrades inside the window do not leave the newest backup two builds back. A missing or
     unreadable manifest counts as not finished.
  7. Prunes the rollback backups past -KeepRollbacks, each in its own handler.
  8. Lays the new build over the install root, retrying once on the transient file lock that has bitten this
     step before.
  9. Confirms darling.json is byte-identical to what it was.
 10. Names the files an EARLIER build shipped and this one does not, and removes them with
     -RemoveStaleFiles. An in-place upgrade is an OVERLAY: it writes what the new build ships and deletes
     nothing else, so a dropped dependency or a stranded runtimes\<rid>\lib\<tfm>\ subtree stays in the
     tree forever - in a directory .NET probes for assemblies. The authority is a manifest this script
     writes into the install root after every copy (#2529).
 11. Creates the local group 'PerformanceMonitor Darling Operators' if it is missing, exactly as
     install-darling.ps1 does, and says who to add. The group is created empty and this script adds no one.
     The service grants that group read on darling.json and the store's credential files and drops the
     NT AUTHORITY\INTERACTIVE entries at its first start on this build, so an install made before the group
     existed would otherwise leave every non-elevated Viewer and CLI user locked out.
 12. Starts the service, waits for Running, and prints the post-install health check - which is a STAGE of
     the install, not a favour.

Every step is safe to re-run. That is not a nicety: steps 5 through 9 leave the service DOWN if anything
between them fails, so "run it again" has to be the correct advice, and the script says so at the point of
failure rather than leaving you to guess.

.PARAMETER Source
The new build: either the .zip as downloaded, or a folder you already extracted it into. Defaults to the
folder this script is in, which is what you get by extracting the new zip to a staging folder ONLY AN
ADMINISTRATOR CAN WRITE TO (e.g. under C:\Program Files\) and running ITS copy of this script - the folder
you run the script from is the trust root, and anyone who can write to it can replace the script itself.

.PARAMETER InstallRoot
The install directory to upgrade. Defaults to the directory of the registered service's executable, which is
the one place that cannot be wrong about where the service is actually installed.

.PARAMETER Sha256
Expected SHA256 of the source zip. Without it the script looks for SHA256SUMS.txt beside the zip.

.PARAMETER KeepRollbacks
How many rollback backups to keep, newest first, INCLUDING the one this run takes. Three is enough to roll
back a bad deploy; the fourth can only roll back to a version nobody wants.

.PARAMETER BackupWindowMinutes
A backup newer than this is reused rather than replaced, so a re-run after an interrupted upgrade does not
overwrite the good pre-upgrade copy with a copy of the half-upgraded tree. It is reused only when that upgrade
did not finish: if darling-install-manifest.txt was written after the backup, the last upgrade completed and
this one takes a new backup. A missing or unreadable manifest keeps the backup in use.

.PARAMETER PruneOnly
Prune the rollback backups and exit. Nothing is stopped, nothing is copied, and the service keeps running.
This is what an existing box with a backlog needs, and it is the command the service's own layout report
tells operators to run.

.PARAMETER ListRollbacks
Show which backups would be kept and which pruned, and exit. Changes nothing.

.PARAMETER RemoveStaleFiles
Delete the files an earlier build shipped and this one does not, rather than only naming them.

The check itself runs on every upgrade and reports either way; this switch is the difference between a
report and a delete. It is OFF for the first release on purpose. Everything this can name provably came out
of one of our own build payloads - the manifest is written from the payload, so darling.json, the DPAPI
credential blobs, the rollback backups and pg-runtime were never in it and cannot come out of it - but a
delete that runs inside a monitoring host's install directory should spend a few deploys showing operators
its answer before it starts acting on it. Flip the default once boxes have been reporting it and the lists
have been the ones people expected.

.PARAMETER SkipHashCheck
Proceed with a source zip whose SHA256 could not be verified.

.PARAMETER SkipStopGuard
Proceed even though processes are running out of the install tree. Almost always the wrong answer - the
copy will fail on a locked file and leave mixed binaries - but there is no way to be sure from here that
your case is not the exception.

.PARAMETER AcceptWritableExtraction
Skip three checks that all ask the same question of a different folder (#4043): is it ALREADY writable by
ordinary local users, right now? A zip -Source's own CONTENT is always covered regardless of who could
write to the folder it sits in - it is copied into a private, protected folder and hashed and extracted
from THAT copy - but a FOLDER -Source has no content check at all: this script only confirms the service
exe is present in it, so a folder that ever inherited a broad write grant (extracted directly under C:\, or
into a folder in your own profile such as Downloads, which is writable by anything already running as you,
elevated or not) may already hold a swapped binary between whenever it was extracted and now. The existing
-InstallRoot gets the same check, before this script's own lock runs: an install made before #4038 shipped
was writable by ordinary users for its entire life until the day it is first locked, and an in-place
upgrade's overlay does not verify anything already in the tree besides what it replaces. Passing this
switch accepts the risk for whichever of the three applies - verify the zip's SHA256 instead where that
option exists.
#>
[CmdletBinding()]
param(
    [string]$Source = $PSScriptRoot,
    [string]$InstallRoot,
    [string]$Sha256,
    [ValidateRange(1, 100)]
    [int]$KeepRollbacks = 3,
    [ValidateRange(0, 10080)]
    [int]$BackupWindowMinutes = 60,
    [switch]$PruneOnly,
    [switch]$ListRollbacks,
    [switch]$RemoveStaleFiles,
    [switch]$SkipHashCheck,
    [switch]$SkipStopGuard,
    [switch]$AcceptWritableExtraction
)

$ErrorActionPreference = 'Stop'

# Windows PowerShell 5.1 leaves $PSScriptRoot EMPTY inside a [CmdletBinding()] script's param() defaults when the
# script is started with -File, so the -Source default above can resolve to nothing and the run would stop at
# "No -Source given" (#5627: measured by the upgrade-shape check on windows-2022 and windows-2025). The script body
# does see $PSScriptRoot, so the default is applied here.
if ([string]::IsNullOrWhiteSpace($Source)) { $Source = $PSScriptRoot }

$serviceName = 'PerformanceMonitor Darling'
$serviceExeName = 'PerformanceMonitor.Darling.Service.exe'
$configName = 'darling.json'

# The local group whose members read darling.json and the store's role credentials without elevation (the
# Viewer and the CLI verbs). It replaces NT AUTHORITY\INTERACTIVE, which put every logged-on user of a shared
# host inside the boundary of a secret that anyone who can read the file can decrypt. The name is the service
# account's name plus a role suffix, like Windows' own "Performance Log Users". It must match
# DarlingFileSecurity.OperatorGroupName in the service project, and install-darling.ps1's copy of it.
$operatorGroupName = 'PerformanceMonitor Darling Operators'

# #4466: how long phase two waits for a process that is still exiting after the service reports Stopped,
# before it refuses. Get-Service reporting Stopped means the SCM's own state machine reached that state,
# not that every process the service spawned has finished unwinding - a postmaster or a child process can
# still be a few seconds from actually exiting. A bound this generous is still bounded: it never turns into
# an indefinite hang, and it is far short of the two minutes already spent waiting for the service itself.
$script:InstallTreeClearWaitSeconds = 120
$script:InstallTreeClearPollSeconds = 2
$manifestName = 'darling-install-manifest.txt'

function Fail([string]$message) { Write-Host "ERROR: $message" -ForegroundColor Red; exit 1 }
function Note([string]$message) { Write-Host $message }

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

# The upgrade's TLS key file step (#5700). The verb runs the installed exe elevated, so it runs only over a tree the upgrade has
# locked: not when a lock reported paths ordinary users can still change, not when the lock was skipped, and not under
# -AcceptWritableExtraction (the tree may already hold a swapped binary). Otherwise it returns the warning, which names the verb and
# the command to run once the tree is locked. Returns $null when the verb ran, like Invoke-DarlingTlsKeyFileHardening.
function Get-UpgradeTlsKeyFileWarning([string]$exe, [string]$configPath) {
    $why = $null
    if ($AcceptWritableExtraction) { $why = 'this upgrade was started with -AcceptWritableExtraction, so the install folder may already hold a changed binary' }
    elseif ($installTreeLockProblem) { $why = $installTreeLockProblem }
    if ($why) {
        return "The TLS key files darling.json names were not checked and their permissions were not changed, because the verb runs the installed exe elevated and $why. Once the install folder is locked, run from an elevated prompt: $(Get-DarlingTlsKeyFileVerbCommand $exe $configPath)"
    }
    return Invoke-DarlingTlsKeyFileHardening $exe $configPath
}

# Gives a rollback backup folder a DACL of its own (#5700): inheritance off, SYSTEM and Administrators with full control that the
# files in it inherit, and nobody else. On a folder made for a new backup it is applied BEFORE the files are copied in, so every
# copy is born with only that. On an older retained backup, -ResetChildren also clears every explicit entry on the files in it
# so they inherit the folder's. A backup folder that is a link is not followed. A retained backup is walked first, and one that
# holds a link anywhere in it, or a file with more than one name, is not changed at all and is named, because setting a folder's
# DACL also rewrites the inherited entries of a file's other name. Each folder and file is read back, and one that still grants
# anyone else is a problem. Returns $null when all of it held, otherwise one sentence to warn with. It never throws; the caller
# decides what a problem means (the upgrade stops on a new backup folder it could not restrict, and warns on a retained one).
function Protect-DarlingRollbackBackupFolder([string]$path, [switch]$ResetChildren) {
    $ErrorActionPreference = 'Continue'
    $fix = "icacls `"$path`" /inheritance:r /grant:r `"*S-1-5-18:(OI)(CI)F`" `"*S-1-5-32-544:(OI)(CI)F`" /T /L"
    try {
        $item = Get-Item -LiteralPath $path -Force -ErrorAction Stop
        if ((-not $item.PSIsContainer) -or ($item.Attributes -band [System.IO.FileAttributes]::ReparsePoint)) {
            return "The rollback backup '$path' is a link, not a folder, so its permissions were not changed. Remove the link, or the backup it points to is not covered."
        }
        $wk = [System.Security.Principal.WellKnownSidType]
        $systemSid = New-Object System.Security.Principal.SecurityIdentifier($wk::LocalSystemSid, $null)
        $adminsSid = New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinAdministratorsSid, $null)
        $notOnlyAdmins = {
            param($target)
            @($target.Access | Where-Object {
                $aceSid = $_.IdentityReference.Translate([System.Security.Principal.SecurityIdentifier])
                -not ($aceSid.Equals($systemSid) -or $aceSid.Equals($adminsSid))
            })
        }
        # A retained backup is walked before anything in it is changed (links are listed, never entered). Setting the folder's DACL
        # pushes the inherited entries onto every child, and onto the OTHER name of a file with more than one name, so a backup
        # holding either is left exactly as it is and the warning says what to run.
        if ($ResetChildren) {
            $blocked = @()
            $pending = New-Object System.Collections.Generic.Stack[string]
            $pending.Push($path)
            while ($pending.Count -gt 0) {
                foreach ($entry in @(Get-ChildItem -LiteralPath $pending.Pop() -Force -ErrorAction Stop)) {
                    if ($entry.Attributes -band [System.IO.FileAttributes]::ReparsePoint) { $blocked += [pscustomobject]@{ Path = $entry.FullName; Name = $entry.Name; Link = $true } }
                    elseif ($entry.PSIsContainer) { $pending.Push($entry.FullName) }
                    elseif ((Get-DarlingFileNameCount $entry.FullName) -ne 1) { $blocked += [pscustomobject]@{ Path = $entry.FullName; Name = $entry.Name; Link = $false } }
                }
            }
            if ($blocked.Count -gt 0) {
                $first = $blocked[0]
                $more = if ($blocked.Count -gt 1) { " (and $($blocked.Count - 1) more like it)" } else { '' }
                if ($first.Link) {
                    $why = "'$($first.Path)' is a link$more, and setting the folder's permissions could reach what it points to. To see where it points: fsutil reparsepoint query `"$($first.Path)`". Delete the link, then run this script again, or restrict the backup by hand: $fix"
                }
                else {
                    $why = "'$($first.Path)' has more than one name (or its names could not be counted)$more, and setting the folder's permissions would also change the other name's permissions. To list the other names: fsutil hardlink list `"$($first.Path)`". Delete this name if it is not part of the backup (that leaves the other name as it is), then run this script again, or restrict the backup by hand: $fix"
                }
                return "The rollback backup '$path' was not changed, so it may still be readable by accounts other than SYSTEM and Administrators: $why"
            }
        }
        # The files are reset to inherit BEFORE the folder's DACL is set, so the folder's protected DACL then reaches them as it is set.
        $reset = @()
        $left = @()
        if ($ResetChildren) {
            foreach ($child in @(Get-ChildItem -LiteralPath $path -File -Force -ErrorAction Stop)) {
                if (($child.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -or (Get-DarlingFileNameCount $child.FullName) -ne 1) {
                    $left += $child.Name
                    continue
                }
                # icacls /reset /L: the file's own entries are dropped and it inherits again, on the file itself and never on a link's target. Its exit code is judged, with the error
                # preference local to this function, because Windows PowerShell 5.1 turns redirected native stderr into a stop.
                & icacls.exe $child.FullName /reset /L 2>&1 | Out-Null
                if ($LASTEXITCODE -ne 0) { $left += $child.Name; continue }
                $reset += $child
            }
        }
        $inherit = [System.Security.AccessControl.InheritanceFlags]'ContainerInherit, ObjectInherit'
        $acl = New-Object System.Security.AccessControl.DirectorySecurity
        $acl.SetAccessRuleProtection($true, $false)
        foreach ($sid in @($systemSid, $adminsSid)) {
            $acl.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($sid, 'FullControl', $inherit, 'None', 'Allow')))
        }
        Set-Acl -LiteralPath $path -AclObject $acl -ErrorAction Stop
        # DirectoryInfo.GetAccessControl is .NET Framework only, so PowerShell 7 stopped here (#6920). The DirectorySecurity and FileSecurity constructors read the
        # same DACL by name in both hosts, and (unlike Get-Acl) without opening the file, which a caller the new DACL does not name could not do.
        $after = New-Object System.Security.AccessControl.DirectorySecurity($path, [System.Security.AccessControl.AccessControlSections]::Access)
        if ((-not $after.AreAccessRulesProtected) -or (& $notOnlyAdmins $after).Count -gt 0) {
            return "The rollback backup '$path' still grants access to accounts other than SYSTEM and Administrators after its permissions were set. Run from an elevated prompt: $fix"
        }
        foreach ($file in $reset) {
            if ((& $notOnlyAdmins (New-Object System.Security.AccessControl.FileSecurity($file.FullName, [System.Security.AccessControl.AccessControlSections]::Access))).Count -gt 0) { $left += $file.Name }
        }
        if ($left.Count -gt 0) {
            return "In the rollback backup '$path', $($left -join ', ') could not be limited to SYSTEM and Administrators (a link, a file with more than one name, or a permission that would not change). Restrict each by hand, or delete the backup."
        }
        return $null
    }
    catch {
        return "The rollback backup '$path' could not be limited to SYSTEM and Administrators ($($_.Exception.Message)). Run from an elevated prompt: $fix"
    }
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
function Good([string]$message) { Write-Host $message -ForegroundColor Green }
function Warn([string]$message) { Write-Host "WARNING: $message" -ForegroundColor Yellow }

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

# ---- the store folder (also in install-darling.ps1, step 4b3; these functions are byte-identical) ----
# The store folder is the parent of postgres.dataDirectory. The same functions lock it here as the installer does, so an
# upgrade from a release that could not lock it ends with it locked, and a store the install trusts is trusted here too.

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

# ============================ the rollback-backup convention ============================
#
# The C# twin is DarlingRollbackBackups and the two must stay identical. That is not a style preference:
# the service RECOGNISES these directories so it can report the whole set on one line instead of one
# warning each, and this script CREATES and PRUNES them. If the two spellings ever drift apart the service
# goes back to naming 46 directories individually and nobody notices, because each of those lines is
# perfectly true. DarlingDeployRollbackRetentionTests runs the function below against the C# predicate over
# a shared case table to make the drift impossible to ship.

# What a rollback backup is called. Prefix plus at least one more character, matched case-insensitively
# because Windows paths are - a matcher stricter than the filesystem is a matcher that misses a directory
# the operator can see with their own eyes.
#
# No stamp parsing. The backlog this has to recognise was made over months by a procedure that has spelled
# its stamp more than one way, and a rule that only accepted today's spelling would leave yesterday's
# backups unrecognised - which is the entire complaint in #2525. The prefix is a namespace: everything
# inside it belongs to this procedure.
function Test-DarlingRollbackBackupName([string]$name) {
    $prefix = '_rollback_manual_'
    if ([string]::IsNullOrEmpty($name)) { return $false }
    if ($name.Length -le $prefix.Length) { return $false }
    return $name.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)
}

# The name for a backup taken now. Seconds are in it because two deploys in one minute is a rehearsal, not
# a hypothetical, and a stamp that collides silently merges two builds into one directory.
function New-DarlingRollbackBackupName([datetime]$whenUtc) {
    return '_rollback_manual_' + $whenUtc.ToString('yyyyMMdd-HHmmss')
}

# Every rollback backup in the install root, NEWEST FIRST. That ordering is a contract - the prune below
# keeps a prefix of this list - so it is produced in exactly one place.
#
# Ordered by LastWriteTimeUtc rather than by the stamps in the names, because the filesystem knows when a
# directory was written and the name only carries whatever spelling the procedure used that month. The name
# is the tiebreak so the ordering is still total when two backups share a timestamp.
#
# Note there is deliberately NO -Filter '_rollback_manual_*' here. Get-ChildItem's filter is handed to the
# filesystem, which matches a directory's Windows 8.3 SHORT name as well as its real one - so a wildcard
# can hand a DELETE a directory whose real name looks nothing like the pattern. DarlingStoreUpgrade guards
# that by re-checking the real name after the wildcard; enumerating everything and testing the real name is
# the same guard with the trap removed, and on a directory holding tens of entries it costs nothing.
function Get-DarlingRollbackBackups([string]$installRoot) {
    # Every return is unary-comma wrapped (,@(...)) so an EMPTY result reaches the caller as an empty array
    # rather than $null. A bare `return @()` UNROLLS on the way out of the function, the assignment collects
    # nothing, and `$backups` becomes $null - then `@($null)` is a one-element array holding $null, which
    # slips past a `.Count -eq 0` guard and gets indexed/subtracted downstream (#2671). The comma keeps the
    # contract every caller here relies on: this function always hands back a list, empty or not.
    if ([string]::IsNullOrWhiteSpace($installRoot)) { return ,@() }
    if (-not (Test-Path -LiteralPath $installRoot -PathType Container)) { return ,@() }

    $all = @(Get-ChildItem -LiteralPath $installRoot -Directory -Force -ErrorAction SilentlyContinue)
    $mine = @($all | Where-Object { Test-DarlingRollbackBackupName $_.Name })
    return ,@($mine | Sort-Object -Property LastWriteTimeUtc, Name -Descending)
}

# Every rollback backup that is kept is limited to SYSTEM and Administrators (#5700): a backup taken by an earlier run copied the
# install root's files under the root's inherited DACL. Each backup folder gets the protected DACL on its own, and the files in it
# are reset to inherit it. A problem is a warning and the upgrade carries on.
#
# The list is assigned before the loop and NOT wrapped in @( ): Get-DarlingRollbackBackups returns a unary-comma array, and @( )
# around the call makes a one-element array holding the whole list, so with two or more backups the loop ran once and the folder
# step received every path joined by spaces (#5700 follow-up).
function Protect-DarlingKeptRollbackBackups([string]$installRoot) {
    $kept = Get-DarlingRollbackBackups $installRoot
    foreach ($retained in $kept) {
        $retainedProblem = Protect-DarlingRollbackBackupFolder $retained.FullName -ResetChildren
        if ($retainedProblem) { Warn $retainedProblem }
    }
}

# The backups past retention, given the newest-first list Get-DarlingRollbackBackups produces.
#
# $keep counts the backup this run just took, so -KeepRollbacks 3 means "this one and the two before it".
# The floor of 1 is not defensive clutter: this function's output is fed straight to a recursive delete in
# an install directory, and the one input that must never be possible is the one that selects everything.
function Select-DarlingRollbackBackupsToPrune($backups, [int]$keep) {
    # Same $null filter as the recency guard: this list feeds a recursive delete, so a phantom $null element
    # must never be counted as a backup or reach the index below (#2671).
    $ordered = @($backups | Where-Object { $null -ne $_ })
    if ($keep -lt 1) { $keep = 1 }
    if ($ordered.Count -le $keep) { return @() }
    return @($ordered[$keep..($ordered.Count - 1)])
}

# True when the newest backup is recent enough that this run is a RE-RUN of an interrupted upgrade rather
# than a new deploy.
#
# The failure this prevents is specific and expensive. Step 8 dies on a locked DLL, leaving the tree half
# extracted; the operator does the right thing and runs the script again; without this, step 6 backs up the
# HALF-EXTRACTED tree, and now the newest rollback copy - the one anybody would reach for - is a mixture of
# two builds. Reusing the existing backup keeps the copy that was taken while the tree was still coherent.
#
# $nowUtc is a parameter rather than a call to Get-Date so the rule can be tested at a known instant.
#
# The elapsed time has to be non-negative as well as small, and that is not pedantry - it was a live bug
# caught by running this function against a planted tree. A backup whose timestamp is in the FUTURE (a
# clock that stepped backwards, a directory restored from elsewhere with its metadata) produces a negative
# elapsed time, which is less than any window, which reads as "recent" - and the upgrade would then skip
# taking a backup at all. The two ways to be wrong here are not symmetric: an extra backup costs 120 MB
# that the prune reclaims on the next deploy, and a missing one costs the rollback this whole procedure
# exists to provide. So anything the clock cannot vouch for falls through to taking a new backup.
function Test-DarlingRollbackBackupIsRecent($backups, [int]$withinMinutes, [datetime]$nowUtc) {
    # Filter $null out before counting: a caller that passed $null (or an array carrying one) would otherwise
    # give @($backups).Count = 1, defeat the empty guard, and reach `$nowUtc - $null` - the op_Subtraction
    # crash in #2671. The producer now returns a real empty array, so this is defense in depth on the two
    # steps below that index [0] and subtract.
    $ordered = @($backups | Where-Object { $null -ne $_ })
    if ($ordered.Count -eq 0) { return $false }
    if ($withinMinutes -le 0) { return $false }

    $elapsed = ($nowUtc - $ordered[0].LastWriteTimeUtc).TotalMinutes
    return ($elapsed -ge 0) -and ($elapsed -lt $withinMinutes)
}

# When the install manifest was last written (UTC), or $null when it is missing or cannot be read. The manifest is written
# only after a successful copy (step 8), so its time is the time the last upgrade FINISHED. A failure to read it is never
# fatal: $null sends the caller down the reuse side, the safe one.
function Get-DarlingInstallManifestWriteUtc([string]$manifestPath) {
    try {
        if ([string]::IsNullOrWhiteSpace($manifestPath)) { return $null }
        $item = Get-Item -LiteralPath $manifestPath -Force -ErrorAction Stop
        if ($item.PSIsContainer) { return $null }
        return [datetime]$item.LastWriteTimeUtc
    }
    catch {
        return $null
    }
}

# True when the upgrade that took the newest backup FINISHED: the install manifest was written after that backup (#6821).
#
# Test-DarlingRollbackBackupIsRecent alone looks only at the backup's age, so two complete upgrades inside one window
# reused the first one's backup, and the newest backup then held root files from two builds back. The manifest is written
# only after the copy succeeds and every copy failure stops before it, so a manifest newer than the backup means the last
# upgrade is not an interrupted one and this run needs a backup of its own.
#
# A missing or unreadable manifest ($null) and one not later than the backup both answer $false, which keeps the reuse: an
# interrupted first upgrade has no manifest, and a spare backup costs 120 MB while a lost good copy costs the rollback.
function Test-DarlingLastUpgradeFinishedAfterBackup($backups, $manifestWriteUtc) {
    $ordered = @($backups | Where-Object { $null -ne $_ })
    if ($ordered.Count -eq 0) { return $false }
    if ($null -eq $manifestWriteUtc) { return $false }
    return ([datetime]$manifestWriteUtc) -gt $ordered[0].LastWriteTimeUtc
}

# What step 6 does about the backup, as one word so the decision runs under test and the caller only acts on it:
#   'reuse'    a recent backup and the upgrade that took it did not finish (a re-run of an interrupted upgrade)
#   'finished' a recent backup, but the install manifest is newer, so that upgrade completed: take a new backup
#   'new'      no backup, or none inside the window: take a new backup
function Get-DarlingRollbackBackupDecision($backups, [int]$withinMinutes, [datetime]$nowUtc, [string]$manifestPath) {
    if (-not (Test-DarlingRollbackBackupIsRecent $backups $withinMinutes $nowUtc)) { return 'new' }
    if (Test-DarlingLastUpgradeFinishedAfterBackup $backups (Get-DarlingInstallManifestWriteUtc $manifestPath)) { return 'finished' }
    return 'reuse'
}

# ============================ the install tree ============================

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

# Rewrites an extended-length path to its ordinary spelling, leaving anything else alone:
# \\?\UNC\server\share -> \\server\share, and \\?\C:\dir -> C:\dir. The same function is in install-darling.ps1 (#2348).
# 'UNC' is tested first because it is the longer prefix, and is matched without regard to case.
function Convert-ExtendedLengthPath([string]$path) {
    if ([string]::IsNullOrEmpty($path)) { return $path }
    if ($path.StartsWith('\\?\UNC\', [StringComparison]::OrdinalIgnoreCase)) { return '\\' + $path.Substring(8) }
    if ($path.StartsWith('\\?\', [StringComparison]::Ordinal)) { return $path.Substring(4) }
    return $path
}

# The ordinary spelling of a folder typed as \\?\C:\dir or \\.\C:\dir (#5673), so the checks that ask whether the
# running script's folder or the source folder is the install folder see the same folder. Anything else is unchanged.
function Get-DarlingOrdinaryPath([string]$path) {
    if ([string]::IsNullOrEmpty($path)) { return $path }
    return Convert-ExtendedLengthPath ($path -replace '^\\\\\.\\(?=[A-Za-z]:)', '')
}

# True when two existing folders are the same folder on disk, however each is spelled: a junction or link in the path, a
# subst or mapped drive letter and a short name all lead to the same volume serial number and file index (#5673). False
# when either folder cannot be opened, so the caller keeps the answer of the text comparison.
function Test-DarlingSameFolderIdentity([string]$left, [string]$right) {
    if ([string]::IsNullOrWhiteSpace($left) -or [string]::IsNullOrWhiteSpace($right)) { return $false }
    try {
        if (-not ('DarlingFileIdentity' -as [type])) {
            Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;

public static class DarlingFileIdentity
{
    [StructLayout(LayoutKind.Sequential)]
    private struct ByHandleFileInformation
    {
        public uint FileAttributes;
        public System.Runtime.InteropServices.ComTypes.FILETIME CreationTime;
        public System.Runtime.InteropServices.ComTypes.FILETIME LastAccessTime;
        public System.Runtime.InteropServices.ComTypes.FILETIME LastWriteTime;
        public uint VolumeSerialNumber;
        public uint FileSizeHigh;
        public uint FileSizeLow;
        public uint NumberOfLinks;
        public uint FileIndexHigh;
        public uint FileIndexLow;
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern IntPtr CreateFileW(string name, uint access, uint share, IntPtr security, uint disposition, uint flags, IntPtr template);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool GetFileInformationByHandle(IntPtr handle, out ByHandleFileInformation info);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool CloseHandle(IntPtr handle);

    // 'volume:index' of an existing folder (opened with backup semantics, links followed), or null.
    public static string Of(string folder)
    {
        IntPtr handle = CreateFileW(folder, 0, 7, IntPtr.Zero, 3, 0x02000000, IntPtr.Zero);
        if (handle == new IntPtr(-1)) { return null; }
        try
        {
            ByHandleFileInformation info;
            if (!GetFileInformationByHandle(handle, out info)) { return null; }
            return info.VolumeSerialNumber.ToString("x") + ":" + info.FileIndexHigh.ToString("x") + ":" + info.FileIndexLow.ToString("x");
        }
        finally { CloseHandle(handle); }
    }
}
'@
        }
        $leftId = [DarlingFileIdentity]::Of($left)
        $rightId = [DarlingFileIdentity]::Of($right)
        if ($null -eq $leftId -or $null -eq $rightId) { return $false }
        return $leftId -eq $rightId
    }
    catch { return $false }
}

# Where the service is ACTUALLY installed, read from the registered ImagePath rather than guessed from
# where this script happens to be sitting. Returns $null when the service is not installed.
#
# The ImagePath is quoted when it contains spaces (the documented 'C:\Program Files\PerformanceMonitorDarling' does; an older
# folder made directly under C:\ usually does not), so
# both spellings are handled; a path that cannot be parsed returns $null and the caller asks for
# -InstallRoot rather than upgrading a directory it guessed at.
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

# #6597: a service a released install-darling.ps1 registered carries its image path bare, which is how the MSI never
# registered it, and the upgrade never touched the registration. When the registered path is exactly this install's
# service exe and has no quotes, register it again in quotes. A quoted path, a path with arguments after it and a path
# to some other folder are left alone. Call it with the service stopped. Returns $true when it re-registered.
# sc.exe strips a plain pair of quotes as it parses its command line, so the value goes in as "\"path\"" (the outer
# pair is stripped, the \" pair is kept). PowerShell 5.1 and 7.x turn a quoted native argument into a command line
# differently, so the command line is handed over verbatim as ONE string through Start-Process (same as install-darling.ps1).
function Set-DarlingServiceQuotedPath([string]$name, [string]$exePath) {
    try {
        $service = Get-CimInstance -ClassName Win32_Service -Filter "Name='$name'" -ErrorAction Stop
    }
    catch {
        return $false
    }
    if (-not $service -or [string]::IsNullOrWhiteSpace($service.PathName)) { return $false }

    $registered = $service.PathName.Trim()
    if ($registered.StartsWith('"')) { return $false }

    try { $sameExe = [string]::Equals([IO.Path]::GetFullPath($registered), [IO.Path]::GetFullPath($exePath), [StringComparison]::OrdinalIgnoreCase) }
    catch { return $false }
    if (-not $sameExe) { return $false }

    $argLine = "config `"$name`" binPath= `"\`"$exePath\`"`""
    $p = Start-Process -FilePath "$env:SystemRoot\System32\sc.exe" -ArgumentList $argLine -Wait -PassThru -NoNewWindow
    if ($p.ExitCode -ne 0) { Fail "sc config failed ($($p.ExitCode)) while registering the service path in quotes. The new build IS in place and the service is STOPPED: fix the cause and re-run this script." }
    return $true
}

# Every process whose executable lives under $root, so the copy can refuse instead of failing half-way.
#
# It NAMES them. It never kills one, and no version of this script ever should: the bundled PostgreSQL runs
# out of pg-runtime under this very directory, and a sweep that force-kills "everything under the install
# dir" takes the monitoring store down as its first act. Stopping the service stops the store properly;
# anything still holding the tree after that is a person's session, and a person can close it.
function Get-DarlingProcessesUnderPath([string]$root) {
    $hits = @()
    if ([string]::IsNullOrWhiteSpace($root)) { return @($hits) }

    try { $prefix = [IO.Path]::GetFullPath($root).TrimEnd('\') + '\' }
    catch { return @($hits) }

    foreach ($process in @(Get-Process -ErrorAction SilentlyContinue)) {
        $path = $null
        # A process owned by another account throws on .Path rather than returning empty, and an
        # inaccessible process is not evidence of anything - skip it and keep looking.
        try { $path = $process.Path } catch { continue }
        if ([string]::IsNullOrEmpty($path)) { continue }
        if ($path.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) { $hits += $process }
    }

    return @($hits)
}

# #4466: Get-Service reporting Stopped is the SCM's state, not proof that every process the service spawned
# has actually exited yet - a postmaster (or another child) can still be a moment from unwinding when phase
# two takes its snapshot, and that moment alone was enough to send an operator into the refusal below with
# nothing wrong. This polls Get-DarlingProcessesUnderPath until it comes back empty or $waitSeconds runs
# out, printing what it is waiting on the first time it sees a hit, so phase two only ever has to look once
# it returns. Factored out so it can be driven with a short bound and a fake process in a test, without
# waiting on a real two-minute service stop to exercise it.
function Wait-DarlingInstallTreeClear([string]$root, [int]$waitSeconds, [int]$pollSeconds) {
    $deadline = (Get-Date).AddSeconds($waitSeconds)
    $announced = $false
    $holding = @(Get-DarlingProcessesUnderPath $root)

    while ($holding.Count -gt 0 -and (Get-Date) -lt $deadline) {
        if (-not $announced) {
            $names = @($holding | ForEach-Object { "$($_.ProcessName) (pid $($_.Id))" })
            Note "Still exiting: $($names -join ', '). Waiting up to $waitSeconds seconds for the install tree to clear..."
            $announced = $true
        }
        Start-Sleep -Seconds $pollSeconds
        $holding = @(Get-DarlingProcessesUnderPath $root)
    }

    return @($holding)
}

# True for a process that stopping the service will take with it: the service's own executable, and
# anything under pg-runtime (the bundled PostgreSQL, which the service starts and stops).
#
# This exists because the stop guard has to run TWICE, and the two runs are asking different questions.
# Before the service is stopped, every install is holding its own tree - the service exe is right there and
# the store's postmaster is under pg-runtime - so an unfiltered check refuses every real upgrade there has
# ever been. Filtering those two out leaves exactly the processes a service stop will NOT clear: an
# operator's own psql.exe, a shell whose working directory is the install folder, a Darling Viewer someone
# left open holding viewer\*.dll. Catching those BEFORE the stop costs nothing but a re-run; catching them
# after costs an outage.
#
# The Viewer is deliberately NOT on this list even though we ship it. It is a separate process that the
# service does not own and stopping the service does not close, so it holds viewer\ exactly as hard as any
# other application would.
# The separator comes from the runtime rather than being typed as '\'. This script only ever RUNS on
# Windows, but this particular predicate decides which processes are EXCUSED from a guard, and a rule that
# excuses things is the one worth being able to test on the machine it was written on. A hardcoded
# backslash makes it verifiable only on the box it already shipped to.
function Test-DarlingProcessStopsWithTheService([string]$processPath, [string]$installRoot) {
    if ([string]::IsNullOrEmpty($processPath)) { return $false }

    $sep = [IO.Path]::DirectorySeparatorChar
    try { $root = [IO.Path]::GetFullPath($installRoot).TrimEnd($sep, [IO.Path]::AltDirectorySeparatorChar) }
    catch { return $false }

    if ($processPath.Equals(($root + $sep + 'PerformanceMonitor.Darling.Service.exe'), [StringComparison]::OrdinalIgnoreCase)) {
        return $true
    }

    # The trailing separator is what keeps pg-runtime-prev - the rescued previous runtime, a real directory
    # in this layout - from matching a pg-runtime prefix test.
    return $processPath.StartsWith(($root + $sep + 'pg-runtime' + $sep), [StringComparison]::OrdinalIgnoreCase)
}

function Get-DarlingDirectoryBytes([string]$path) {
    try {
        $files = @(Get-ChildItem -LiteralPath $path -File -Recurse -Force -ErrorAction SilentlyContinue)
        if ($files.Count -eq 0) { return [long]0 }
        return [long](($files | Measure-Object -Property Length -Sum).Sum)
    }
    catch {
        return [long]0
    }
}

function Format-DarlingBytes([long]$bytes) {
    if ($bytes -ge 1GB) { return ('{0:N2} GB' -f ($bytes / 1GB)) }
    if ($bytes -ge 1MB) { return ('{0:N1} MB' -f ($bytes / 1MB)) }
    if ($bytes -ge 1KB) { return ('{0:N0} KB' -f ($bytes / 1KB)) }
    return "$bytes bytes"
}

# Deletes the selected backups, EACH IN ITS OWN HANDLER.
#
# That is #1775's lesson paid for once already: the store's retained-copy sweep had its failure handling
# outside the loop, so one directory an antivirus scan still held abandoned the sweep for every other
# directory too - and kept abandoning it for as long as the condition lasted, which is how nothing aged out
# at all. One directory that cannot be deleted must cost exactly that directory.
#
# The try wraps ONLY the measure and the delete, and the reporting happens after it on a success flag. That
# is not tidiness. With the write-up inside the try, anything that went wrong while composing a LINE OF TEXT
# landed in the catch and was recorded as a delete failure - so the log said "could not remove X" about a
# directory that was already gone, and the returned failure count disagreed with the disk. A run of this
# function against a planted tree produced exactly that: two directories removed and two failures reported,
# for the same two directories. What the caller does with the answer (exit codes, "re-running is safe") is
# built on those counts, so they have to mean what they say.
function Remove-DarlingRollbackBackups($prunable) {
    $removed = 0
    $reclaimed = [long]0
    $failures = @()

    foreach ($backup in @($prunable)) {
        $bytes = [long]0
        $gone = $false
        $reason = ''

        try {
            $bytes = Get-DarlingDirectoryBytes $backup.FullName
            Remove-Item -LiteralPath $backup.FullName -Recurse -Force -ErrorAction Stop
            $gone = $true
        }
        catch {
            $reason = $_.Exception.Message
        }

        if ($gone) {
            $removed++
            $reclaimed += $bytes
            Note ("  removed {0} ({1})" -f $backup.Name, (Format-DarlingBytes $bytes))
        }
        else {
            $failures += $backup.Name
            Warn ("could not remove {0}: {1}. The other backups were still swept; delete this one by hand when whatever is holding it lets go." -f $backup.Name, $reason)
        }
    }

    return [pscustomobject]@{
        Removed   = $removed
        Reclaimed = $reclaimed
        Failures  = @($failures)
    }
}

# ============================ the install manifest, and files a build stopped shipping ============================
#
# THE PROBLEM (#2529). An in-place upgrade is an OVERLAY, not a replacement. Expand-Archive -Force and
# Copy-Item -Recurse -Force write what the new build ships and delete nothing else, so a file the old
# version had and the new one dropped stays in the install tree forever: a dependency that went away, an
# assembly that changed name, a satellite-resource directory for a culture nothing localizes into any more,
# a whole runtimes\<rid>\lib\<tfm>\ subtree stranded by a target-framework move. The last one is the one
# that bites. Those are .NET PROBING directories, so a stale assembly sitting in one is not inert clutter -
# it is a candidate for loading.
#
# IT IS MEASURED, not feared. The file lists of consecutive release zips, diffed: the Lite package dropped
# 44 shipped files across twelve consecutive releases - 43 of them in a single step, where a
# target-framework move stranded every runtimes\*\lib\net8.0\ assembly including two copies of
# Microsoft.Data.SqlClient.dll, plus one lone assembly in a later step. The Darling package has dropped
# none in the four steps it has existed for, which is the whole of its history. So the shape of the thing
# is: rare, bursty, tied to a packaging or framework change rather than to ordinary development - and when
# it does happen it arrives forty files at a time, in a probing path.
#
# THE AUTHORITY, and why it cannot produce a false positive. After every successful copy this script writes
# a manifest of the files that copy laid down. The next upgrade diffs its own payload against that manifest,
# and the difference is exactly "files one of OUR builds put here that this build does not ship". Every path
# it can name provably came out of one of our own payloads, which is what disposes of the whole class of
# false positives the obvious approach has. It cannot warn about darling.json, the DPAPI credential blobs,
# the .bak- config copies, the _rollback_manual_* backups, pg-runtime, or whatever an operator legitimately
# put here, because none of those was ever in a payload and so none of them is ever in the manifest. That is
# a property of where the list comes from, not a list of exceptions somebody has to keep up to date - which
# matters, because a list of exceptions that goes one entry out of date is #2525 again with a new subject.
#
# FAIL SAFE, AND LOUDLY. Two things have to be KNOWN before anything is removed: what an earlier build
# shipped, and what this one ships. If either cannot be determined - no manifest yet (every install that
# predates this change), a manifest that will not parse, one that disagrees with its own count, a source
# this cannot read - the answer is to remove NOTHING and say so on its own line. Guessing is not on the
# table: the wrong guess here deletes the store.

# One spelling of a manifest path for the whole script. Manifest paths are relative to the install root and
# written with backslashes, because that is what the tree they describe uses; zip entries arrive with
# forward slashes and get converted here rather than at six call sites.
function ConvertTo-DarlingManifestPath([string]$path) {
    if ([string]::IsNullOrWhiteSpace($path)) { return '' }

    $normalized = $path.Trim().Replace('/', '\')
    while ($normalized.StartsWith('\')) { $normalized = $normalized.Substring(1) }
    return $normalized.TrimEnd('\')
}

# Resolves a manifest path against the install root, in the separator the RUNTIME uses.
#
# Not a hardcoded backslash, for the same reason Test-DarlingSamePath is not: this composes the absolute
# path that a delete is handed, and a rule that decides what gets deleted should be verifiable on a machine
# other than the one it already shipped to. Returns '' for anything that will not resolve, and the callers
# treat '' as "skip", never as "the install root".
function Join-DarlingInstallPath([string]$installRoot, [string]$relativePath) {
    $normalized = ConvertTo-DarlingManifestPath $relativePath
    if ([string]::IsNullOrWhiteSpace($normalized)) { return '' }
    if ([string]::IsNullOrWhiteSpace($installRoot)) { return '' }

    $separator = [string][IO.Path]::DirectorySeparatorChar
    try { return [IO.Path]::GetFullPath([IO.Path]::Combine($installRoot, $normalized.Replace('\', $separator))) }
    catch { return '' }
}

# True for RAW text that names an absolute location rather than something inside the install root.
#
# It exists as its own function because of where it has to be CALLED, which review had to point out twice.
# The first attempt put this test inside Test-DarlingManifestPathIsPreserved, ahead of normalization, which
# read correctly and could not fire: every caller of that predicate normalizes first, and normalization
# strips leading separators - so '\\fileserver\share\x.dll' had already become the ordinary-looking
# relative 'fileserver\share\x.dll' before the predicate ever saw it. A guard belongs where the raw value
# IS, not where it reads best, so this is called at the two points raw text enters the script: a line of a
# manifest, and an entry of a payload. Both refuse the WHOLE input rather than dropping the line, on the
# same reasoning as the file-count check - this script never writes an absolute path, so one coming back
# means the input was edited or is not ours, and a partly-believed record is worse than none.
#
# Test-DarlingManifestPathIsPreserved calls it too, on its own raw argument, for the caller that hands it
# one directly.
function Test-DarlingPathIsAbsolute([string]$rawPath) {
    if ([string]::IsNullOrWhiteSpace($rawPath)) { return $false }

    $raw = $rawPath.Trim()

    # A colon is a drive qualifier or an alternate data stream, neither of which is a name a Windows file
    # can otherwise carry. A leading separator is root-relative, and two of them are UNC.
    if ($raw.Contains(':')) { return $true }
    if ($raw.StartsWith('\') -or $raw.StartsWith('/')) { return $true }

    return [IO.Path]::IsPathRooted($raw)
}

# THE EXCLUSION RULE. True for a path this procedure must never remove and must never record.
#
# It is applied THREE times and that is deliberate, because the three are different questions. On the way
# INTO a manifest, so a preserved path cannot be recorded and therefore cannot be nominated later. In
# Select-DarlingStaleFiles, so a manifest written by some older version of this script cannot nominate one
# either. And one statement before the delete in Remove-DarlingStaleFiles, because that is the function a
# future caller will reach for, and a guard that lives only in the caller upstream is a guard the next
# caller does not get.
#
# Everything here is a string test on a relative path, with no filesystem access at all, so it is the same
# answer on any machine and can be run against a table of cases rather than reasoned about.
function Test-DarlingManifestPathIsPreserved([string]$relativePath) {
    if ([string]::IsNullOrWhiteSpace($relativePath)) { return $true }

    # A manifest path is RELATIVE and stays inside the install root. Asked of THIS function's raw argument,
    # before normalization strips the leading separators that would answer it. It only fires for a caller
    # that hands over un-normalized text - Remove-DarlingStaleFiles is the one that does - because the
    # readers refuse an absolute path at the point they read it, which is where it can still be seen.
    if (Test-DarlingPathIsAbsolute $relativePath) { return $true }

    $path = ConvertTo-DarlingManifestPath $relativePath
    if ([string]::IsNullOrWhiteSpace($path)) { return $true }

    $segments = @($path.Split('\') | Where-Object { $_ })
    if ($segments.Count -eq 0) { return $true }

    foreach ($segment in $segments) {
        if ($segment -eq '.' -or $segment -eq '..') { return $true }
    }

    # THE STORE, and the single most important line in this file. pg-runtime holds the bundled PostgreSQL
    # and pg-runtime-prev the rescued previous one; deleting either destroys the monitoring store this
    # product exists to keep, and no backup in this procedure holds it.
    #
    # Matched as a PREFIX on the top-level name rather than as the two names we ship today. An enumeration
    # is a list somebody has to maintain, and the cost of it being one entry out of date HERE is the store -
    # so the rule is "the pg-runtime namespace is off limits" and a future pg-runtime-<anything> is covered
    # before it exists. Nothing legitimate is given up: pg-runtime.zip is shipped by every build, so it is
    # in both sides of every diff and could never have been in a difference anyway.
    if ($segments[0].StartsWith('pg-runtime', [StringComparison]::OrdinalIgnoreCase)) { return $true }

    # This script's own rollback backups. The prune above owns those, and two different pieces of one
    # script deleting the same directory is how you get a delete nobody can account for.
    if (Test-DarlingRollbackBackupName $segments[0]) { return $true }

    $leaf = $segments[-1]

    # Operator config and its backups. The zip ships darling.sample.json and never darling.json, so on a
    # manifest this script wrote these cannot fire - which is exactly the point of having them. They are
    # the assertion that the manifest is what it claims to be, placed where being wrong costs a monitoring
    # host its configuration.
    if ($leaf.Equals('darling.json', [StringComparison]::OrdinalIgnoreCase)) { return $true }
    if ($leaf.StartsWith((Get-DarlingServiceWrittenNames).BackupPrefix, [StringComparison]::OrdinalIgnoreCase)) { return $true }

    # DPAPI credential blobs. LocalMachine-scoped with an entropy constant that ships in an open-source
    # repo, so READ access is the secret - but they are also not recoverable from anywhere else, and a
    # deleted one is a credential gone rather than a file gone.
    if ($leaf.EndsWith('.dpapi', [StringComparison]::OrdinalIgnoreCase)) { return $true }

    # The manifest itself. It is written after the copy and is therefore in no payload, so it can never
    # appear in a difference either - but the file that decides what gets deleted should not be deletable
    # by the thing it decides for. Kept as a literal rather than reaching for $manifestName so this
    # function answers the same way when it is lifted out of the script and run on its own; the two are
    # pinned together by DarlingDeployStaleFileTests.
    if ($leaf.Equals('darling-install-manifest.txt', [StringComparison]::OrdinalIgnoreCase)) { return $true }

    return $false
}

# The file list of the build being installed, read from the PAYLOAD rather than from anything we maintain.
#
# Deriving it is what makes the manifest trustworthy. A hand-kept list of "what we ship" would be one more
# thing to forget to update, and forgetting it here does not produce a stale entry in a document - it
# produces a file the next upgrade believes was dropped.
#
# Returns an Ok/Files/Reason object rather than a bare array on purpose. PowerShell unrolls an empty array
# returned from a function into $null, so "read nothing" and "failed to read" would arrive at the caller
# looking identical - and those two have to be told apart here more than anywhere else in this script.
function Get-DarlingPayloadFiles([string]$source, [bool]$sourceIsZip) {
    if ([string]::IsNullOrWhiteSpace($source)) {
        return [pscustomobject]@{ Ok = $false; Files = @(); Reason = 'no source path was given' }
    }

    $paths = @()

    try {
        if ($sourceIsZip) {
            Add-Type -AssemblyName 'System.IO.Compression.FileSystem' -ErrorAction SilentlyContinue
            $archive = [IO.Compression.ZipFile]::OpenRead($source)
            try {
                foreach ($entry in $archive.Entries) {
                    # A directory entry has an empty Name and a FullName ending in '/'. Only files.
                    if ([string]::IsNullOrEmpty($entry.Name)) { continue }

                    # Checked HERE, on the entry as the archive spells it, because this is the last moment
                    # an absolute path is still recognisable as one - ConvertTo-DarlingManifestPath on the
                    # next line strips the leading separators that say so. No build of ours produces such an
                    # entry, so one is evidence about the archive rather than about the file.
                    if (Test-DarlingPathIsAbsolute $entry.FullName) {
                        return [pscustomobject]@{ Ok = $false; Files = @(); Reason = "the zip holds an entry with an absolute path ('$($entry.FullName)'), which no build of ours produces" }
                    }

                    $paths += (ConvertTo-DarlingManifestPath $entry.FullName)
                }
            }
            finally {
                $archive.Dispose()
            }
        }
        else {
            $prefix = [IO.Path]::GetFullPath($source).TrimEnd('\', '/') + [IO.Path]::DirectorySeparatorChar
            foreach ($file in @(Get-ChildItem -LiteralPath $source -File -Recurse -Force -ErrorAction Stop)) {
                if (-not $file.FullName.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) { continue }

                # The prefix carries a trailing separator, so this substring cannot begin with one and the
                # test below cannot fire. It is here anyway, so the two payload branches answer the same
                # question in the same words: a reader who has to work out which of two branches checks what
                # is a reader who will eventually get it wrong.
                $relative = $file.FullName.Substring($prefix.Length)
                if (Test-DarlingPathIsAbsolute $relative) {
                    return [pscustomobject]@{ Ok = $false; Files = @(); Reason = "the source folder produced an absolute path ('$relative')" }
                }

                $paths += (ConvertTo-DarlingManifestPath $relative)
            }
        }
    }
    catch {
        return [pscustomobject]@{ Ok = $false; Files = @(); Reason = $_.Exception.Message }
    }

    $files = @($paths | Where-Object { $_ })

    # An EMPTY payload is not a build that ships nothing, it is a build this did not read. Everything
    # downstream treats "the new build does not ship X" as grounds for removing X, so an empty list is the
    # one answer that must never be handed on as a success: it makes every file an earlier build shipped
    # stale in one step, which is the largest delete this code can express.
    if ($files.Count -eq 0) {
        return [pscustomobject]@{ Ok = $false; Files = @(); Reason = 'the source listed no files at all, which is not a build' }
    }

    return [pscustomobject]@{ Ok = $true; Files = $files; Reason = '' }
}

# What the last build to be installed here laid down, or a REASON this cannot be answered.
#
# Every failure path returns Ok = $false with something an operator can read, and none of them returns a
# partial list. A manifest that half-parsed is not a smaller manifest, it is a manifest of unknown content,
# and the caller's whole contract is that it removes nothing when it does not know.
function Read-DarlingInstallManifest([string]$path) {
    if ([string]::IsNullOrWhiteSpace($path) -or -not (Test-Path -LiteralPath $path -PathType Leaf)) {
        return [pscustomobject]@{ Ok = $false; Files = @(); Reason = 'this install has no manifest yet, so there is no record of what the build before this one shipped' }
    }

    try { $lines = @(Get-Content -LiteralPath $path -ErrorAction Stop) }
    catch { return [pscustomobject]@{ Ok = $false; Files = @(); Reason = "the manifest could not be read ($($_.Exception.Message))" } }

    $version = ''
    $declared = -1
    $sawMarker = $false
    $files = @()

    foreach ($line in $lines) {
        if ($sawMarker) {
            # THE PLACE THIS CHECK HAS TO BE. A manifest line is raw text off a disk this script does not
            # control, and one line further on it has been normalized into something that looks relative
            # whatever it was. Write-DarlingInstallManifest never emits an absolute path, so a manifest
            # holding one has been hand-edited, corrupted, or written by something else - and the answer to
            # a record that is not ours is to believe none of it.
            if (Test-DarlingPathIsAbsolute $line) {
                return [pscustomobject]@{ Ok = $false; Files = @(); Reason = "the manifest holds an absolute path ('$($line.Trim())'), which this script never writes - it has been edited, corrupted, or written by something else" }
            }

            $normalized = ConvertTo-DarlingManifestPath $line
            if ($normalized) { $files += $normalized }
            continue
        }

        $trimmed = "$line".Trim()
        if ($trimmed.Length -eq 0 -or $trimmed.StartsWith('#')) { continue }
        if ($trimmed -eq '--- files ---') { $sawMarker = $true; continue }

        if ($trimmed.StartsWith('manifest-version ')) {
            $version = $trimmed.Substring('manifest-version '.Length).Trim()
            continue
        }

        if ($trimmed.StartsWith('file-count ')) {
            $parsed = 0
            if ([int]::TryParse($trimmed.Substring('file-count '.Length).Trim(), [ref]$parsed)) { $declared = $parsed }
            continue
        }
    }

    if (-not $sawMarker) {
        return [pscustomobject]@{ Ok = $false; Files = @(); Reason = 'the manifest carries no file list' }
    }

    if ($version -ne '1') {
        return [pscustomobject]@{ Ok = $false; Files = @(); Reason = "the manifest is format version '$version' and this script only understands 1" }
    }

    if ($declared -lt 0) {
        return [pscustomobject]@{ Ok = $false; Files = @(); Reason = 'the manifest does not say how many files it holds' }
    }

    # The count is the TRUNCATION check. Writing goes temp-file-then-move, so half a manifest should not be
    # observable at all; this is what catches the case where it was observed anyway - an interrupted
    # restore, a hand edit, a copy that died. The direction a truncated manifest fails in is the safe one
    # (fewer files nominated, never more), so this is not a safety guard - it is the rule that a record this
    # script writes and later acts on does not get trusted while it disagrees with itself.
    if ($declared -ne $files.Count) {
        return [pscustomobject]@{ Ok = $false; Files = @(); Reason = "the manifest says it holds $declared file(s) but carries $($files.Count), so it is truncated or edited" }
    }

    return [pscustomobject]@{ Ok = $true; Files = @($files); Reason = '' }
}

# Records what this install now holds that one of our builds put there.
#
# Written LAST, after any removal, so an interrupted run leaves the old manifest in place and a re-run
# re-decides the same question from the same evidence.
function Write-DarlingInstallManifest([string]$path, $relativePaths, [string]$sourceName, [datetime]$whenUtc) {
    $files = @()
    $seen = New-Object 'System.Collections.Generic.HashSet[string]' ([StringComparer]::OrdinalIgnoreCase)

    foreach ($relative in @($relativePaths)) {
        $normalized = ConvertTo-DarlingManifestPath $relative
        if ([string]::IsNullOrWhiteSpace($normalized)) { continue }

        # Preserved paths never enter a manifest, and this is not the same guard as the one at the diff.
        # The ONE route by which pg-runtime could ever reach a manifest is a -Source FOLDER that is a copy
        # of a live install tree - somebody's staging directory made with Copy-Item from a box that has
        # run - and that folder walk happens right here, in Get-DarlingPayloadFiles. Filtering on the way
        # in means a path that must never be deleted cannot be written down, so it cannot be read back and
        # acted on by a later run of a later version of this script.
        #
        # Handed $relative and NOT $normalized. Review caught this one: the predicate's absolute-path half
        # only works on raw text, because normalization has already stripped the leading separators that a
        # UNC or root-relative path is recognised by. Passing the normalized value left that half doing
        # nothing here - safe only because the readers upstream refuse an absolute path first, which is an
        # invariant living in the CALLERS rather than in this function. A guard that holds only because of
        # who happens to call it today is a guard with an expiry date nobody wrote down.
        if (Test-DarlingManifestPathIsPreserved $relative) { continue }
        if (-not $seen.Add($normalized)) { continue }
        $files += $normalized
    }

    # SORTED, so two manifests can be diffed by eye and a rebuild of the same payload produces the
    # same file rather than the same set in a different order. Neither a zip's entry order nor a
    # directory walk's is stable enough to hand an operator as a record.
    $files = @($files | Sort-Object)

    $header = @(
        '# PerformanceMonitor Darling install manifest.',
        '#',
        '# Written by upgrade-darling.ps1 after every successful copy. Every line below the marker is one',
        '# file a build of ours laid down in this directory, spelled relative to it. The next upgrade diffs',
        '# its own payload against this list to find the files an earlier build shipped and the new one does',
        '# not, which is the only way an in-place upgrade can ever remove one (#2529).',
        '#',
        '# Paths this procedure may never remove are left out of the list ENTIRELY, so a manifest cannot',
        '# nominate one: anything under pg-runtime, darling.json and its .bak- copies, *.dpapi credential',
        '# blobs, the _rollback_manual_* backups, and this file.',
        '#',
        '# Deleting this file is safe. The next upgrade will say it cannot tell what the previous build',
        '# shipped, remove nothing, and write a fresh one.',
        'manifest-version 1',
        "build-source $sourceName",
        ('written-utc ' + $whenUtc.ToString('yyyy-MM-ddTHH:mm:ssZ')),
        "file-count $($files.Count)",
        '--- files ---'
    )

    # Temp file then move, so an interrupted write never leaves a manifest that is half a manifest. The
    # file-count header is the backstop for the day it happens anyway.
    $temporary = $path + '.tmp'

    try {
        Set-Content -LiteralPath $temporary -Value (@($header) + @($files)) -Encoding UTF8 -Force -ErrorAction Stop
        Move-Item -LiteralPath $temporary -Destination $path -Force -ErrorAction Stop
        return [pscustomobject]@{ Ok = $true; Count = $files.Count; Reason = '' }
    }
    catch {
        $reason = $_.Exception.Message
        try {
            if (Test-Path -LiteralPath $temporary) { Remove-Item -LiteralPath $temporary -Force -ErrorAction SilentlyContinue }
        }
        catch {
            # A leftover .tmp is worth nobody's error. The next write overwrites it.
        }

        return [pscustomobject]@{ Ok = $false; Count = 0; Reason = $reason }
    }
}

# The files an earlier build shipped and this one does not. Pure: two lists in, one list out, no clock and
# no filesystem, so the rule that decides what a delete is handed can be run against a table of cases.
function Select-DarlingStaleFiles($previousPaths, $currentPaths) {
    $shipped = New-Object 'System.Collections.Generic.HashSet[string]' ([StringComparer]::OrdinalIgnoreCase)

    foreach ($path in @($currentPaths)) {
        $normalized = ConvertTo-DarlingManifestPath $path
        if ($normalized) { [void]$shipped.Add($normalized) }
    }

    # THE ONE INPUT THAT MUST NOT BE EXPRESSIBLE, the same shape as Select-DarlingRollbackBackupsToPrune's
    # floor of 1. An empty "what this build ships" makes EVERY file the previous build shipped stale at
    # once. It cannot come from a real payload - Get-DarlingPayloadFiles refuses to call an empty list a
    # success, and the script has already refused a source with no service exe in it - so arriving here
    # with an empty set means something upstream failed and reported success. Selecting nothing is the
    # answer that is right either way.
    if ($shipped.Count -eq 0) { return @() }

    $stale = @()
    $seen = New-Object 'System.Collections.Generic.HashSet[string]' ([StringComparer]::OrdinalIgnoreCase)

    foreach ($path in @($previousPaths)) {
        $normalized = ConvertTo-DarlingManifestPath $path
        if ([string]::IsNullOrWhiteSpace($normalized)) { continue }

        # A file the NEW build ships is never removable, whatever a manifest says. Case-insensitively,
        # because Windows filenames are: a build that respelled wwwroot\js\App.js as app.js ships the same
        # file under a name an ordinal comparison calls absent, and removing it would delete what the copy
        # had just written.
        if ($shipped.Contains($normalized)) { continue }

        # $path, not $normalized, for the reason spelled out in Write-DarlingInstallManifest: the
        # predicate's absolute-path half can only see a UNC or root-relative path in RAW text, and
        # normalization has already removed what makes it one. The membership test above is a different
        # question and correctly uses the normalized form, which is why the two are not the same argument.
        if (Test-DarlingManifestPathIsPreserved $path) { continue }
        if (-not $seen.Add($normalized)) { continue }
        $stale += $normalized
    }

    return @($stale | Sort-Object)
}

# Deletes the selected files, EACH IN ITS OWN HANDLER - #1775's lesson, the same one
# Remove-DarlingRollbackBackups is built around. One file an antivirus scan still holds must cost exactly
# that file, and the write-up happens outside the try on a success flag so that a failure while composing a
# LINE OF TEXT cannot be recorded as a delete failure for a file that is already gone.
#
# Returns four lists rather than a count, because they mean four different things to an operator: what went,
# what was refused by a guard, what was tried and would not go, and which directories emptied out.
#
# $shippedPaths is what the build being installed ships, and it is a PARAMETER rather than something the
# caller is trusted to have filtered for. Select-DarlingStaleFiles already excludes them, and that is
# exactly the argument for asking again here: this is the function that performs the delete, so the
# property "a file the new build ships is never deletable" has to hold at THIS call and not only at the one
# that happens to precede it today. An empty $shippedPaths therefore refuses everything, on the same
# reasoning as the empty-payload guard in Select-DarlingStaleFiles: a caller that cannot say what the build
# ships has not earned a delete.
function Remove-DarlingStaleFiles([string]$installRoot, $relativePaths, $shippedPaths) {
    $removed = @()
    $refused = @()
    $failures = @()
    $emptied = @()
    $parents = @()
    $reclaimed = [long]0

    try { $root = [IO.Path]::GetFullPath($installRoot).TrimEnd('\', '/') }
    catch {
        return [pscustomobject]@{ Removed = @(); Refused = @($relativePaths); Failures = @(); Emptied = @(); Reclaimed = [long]0 }
    }

    $shipped = New-Object 'System.Collections.Generic.HashSet[string]' ([StringComparer]::OrdinalIgnoreCase)
    foreach ($path in @($shippedPaths)) {
        $normalized = ConvertTo-DarlingManifestPath $path
        if ($normalized) { [void]$shipped.Add($normalized) }
    }

    if ($shipped.Count -eq 0) {
        return [pscustomobject]@{ Removed = @(); Refused = @($relativePaths); Failures = @(); Emptied = @(); Reclaimed = [long]0 }
    }

    $prefix = $root + [IO.Path]::DirectorySeparatorChar

    foreach ($relative in @($relativePaths)) {
        if (Test-DarlingManifestPathIsPreserved $relative) { $refused += $relative; continue }

        # A file the NEW build ships, whatever the caller thinks. Case-insensitively, because the copy that
        # just ran wrote it under whichever spelling this build uses and Windows would hand a delete the
        # same file under either.
        if ($shipped.Contains((ConvertTo-DarlingManifestPath $relative))) { $refused += $relative; continue }

        # CONTAINMENT, and it is a different question from the predicate above rather than a repeat of it.
        # That one reads a STRING; this resolves the path the filesystem would actually open, so it is what
        # catches a spelling the string rule did not anticipate. Anything resolving outside the install
        # root is refused and REPORTED - a delete that quietly did not happen is worse than one that says
        # so, and on a manifest this script wrote neither of these can fire at all.
        $full = Join-DarlingInstallPath $root $relative
        if ([string]::IsNullOrWhiteSpace($full) -or -not $full.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) {
            $refused += $relative
            continue
        }

        if (-not (Test-Path -LiteralPath $full -PathType Leaf)) { continue }

        $bytes = [long]0
        $gone = $false
        $reason = ''

        try {
            $bytes = [long]((Get-Item -LiteralPath $full -Force).Length)
            Remove-Item -LiteralPath $full -Force -ErrorAction Stop
            $gone = $true
        }
        catch {
            $reason = $_.Exception.Message
        }

        if ($gone) {
            $removed += $relative
            $reclaimed += $bytes
            $parents += [IO.Path]::GetDirectoryName($full)
            Note ("  removed {0} ({1})" -f $relative, (Format-DarlingBytes $bytes))
        }
        else {
            $failures += $relative
            Warn ("could not remove the stale file {0}: {1}. The others were still swept; re-running the upgrade retries this one." -f $relative, $reason)
        }
    }

    # The directories that went empty because everything in them was stale.
    #
    # This is not tidiness, it is the difference between fixing a problem and moving it. The service's
    # install-layout report classifies a satellite-resource directory STRUCTURALLY - one holding nothing but
    # *.resources.dll - and an EMPTY directory deliberately fails that test, so it is reported as "not part
    # of the product's layout" on every single service start. Removing the last file out of de\ and leaving
    # the shell behind would convert one stale file into a permanent startup warning: the same
    # guard-goes-too-loud outcome #2525 was about, this time caused by our own cleanup.
    #
    # NON-RECURSIVE deletes, deepest first, each walking up only while the parent is empty too. A directory
    # delete that cannot recurse cannot destroy anything - the worst it can do is fail - which is what makes
    # this safe to do at all. Deepest first because a shallow directory still holding a soon-to-go child
    # would stop the walk and never be revisited.
    $unique = @($parents | Where-Object { $_ } | Sort-Object -Unique)
    $deepestFirst = @($unique | Sort-Object -Property Length -Descending)

    foreach ($directory in $deepestFirst) {
        $current = $directory
        while ($current -and $current.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) {
            $next = [IO.Path]::GetDirectoryName($current)
            try {
                if (@(Get-ChildItem -LiteralPath $current -Force -ErrorAction Stop).Count -gt 0) { break }
                [IO.Directory]::Delete($current)
            }
            catch {
                break
            }

            $emptied += $current.Substring($prefix.Length)
            $current = $next
        }
    }

    return [pscustomobject]@{
        Removed   = @($removed)
        Refused   = @($refused)
        Failures  = @($failures)
        Emptied   = @($emptied)
        Reclaimed = $reclaimed
    }
}

# ============================ preamble ============================

if (-not ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    Fail "Run this from an ELEVATED PowerShell. Stopping the service, writing to the install directory, and starting it again all need it."
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

$msiRefusal = Get-DarlingMsiInstallRefusal 'upgrade-darling.ps1'
if ($msiRefusal) { Fail $msiRefusal }

if ([string]::IsNullOrWhiteSpace($InstallRoot)) {
    $InstallRoot = Get-DarlingInstallRootFromService $serviceName
    if ([string]::IsNullOrWhiteSpace($InstallRoot)) {
        Fail "The '$serviceName' service is not installed (or its ImagePath could not be read), so there is nothing here to upgrade. Install with install-darling.ps1, or pass -InstallRoot to point at the tree you mean."
    }
}

try { $InstallRoot = [IO.Path]::GetFullPath((Get-DarlingOrdinaryPath $InstallRoot)).TrimEnd('\') }
catch { Fail "'$InstallRoot' is not a path this script can resolve." }

if (-not (Test-Path -LiteralPath $InstallRoot -PathType Container)) {
    Fail "The install directory '$InstallRoot' does not exist."
}

if (-not (Test-Path -LiteralPath (Join-Path $InstallRoot $serviceExeName))) {
    Fail "'$InstallRoot' does not hold $serviceExeName, so it is not a Darling install. Refusing to copy a build over it."
}

Note "Install directory: $InstallRoot"

# ============================ -ListRollbacks / -PruneOnly ============================
#
# Both run against a LIVE service on purpose. Neither touches a binary, and the backups are inert copies
# that nothing has open - so making an operator stop their monitoring host to reclaim 5 GB would be asking
# them to take an outage to clean up after us. -PruneOnly is what the service's own layout report tells
# people to run, and it has to be something they can run at 3pm on a Tuesday.

$backups = Get-DarlingRollbackBackups $InstallRoot

if ($ListRollbacks -or $PruneOnly) {
    if ($backups.Count -eq 0) {
        Good "No rollback backups in $InstallRoot."
        exit 0
    }

    $prunable = Select-DarlingRollbackBackupsToPrune $backups $KeepRollbacks
    $total = [long]0
    foreach ($backup in $backups) { $total += (Get-DarlingDirectoryBytes $backup.FullName) }

    Note ("{0} rollback backup(s), {1} total. Keeping the newest {2}:" -f $backups.Count, (Format-DarlingBytes $total), $KeepRollbacks)

    $prunableNames = @($prunable | ForEach-Object { $_.Name })
    foreach ($backup in $backups) {
        $verdict = if ($prunableNames -contains $backup.Name) { 'prune' } else { 'KEEP ' }
        Note ("  [{0}] {1}  ({2}, last written {3:yyyy-MM-dd HH:mm} UTC)" -f $verdict, $backup.Name, (Format-DarlingBytes (Get-DarlingDirectoryBytes $backup.FullName)), $backup.LastWriteTimeUtc)
    }

    if ($ListRollbacks) {
        Note "Nothing was changed (-ListRollbacks). Re-run with -PruneOnly to remove the ones marked prune."
        exit 0
    }

    if ($prunable.Count -eq 0) {
        Good "Nothing to prune."
        exit 0
    }

    $result = Remove-DarlingRollbackBackups $prunable
    Good ("Removed {0} rollback backup(s), reclaiming {1}." -f $result.Removed, (Format-DarlingBytes $result.Reclaimed))
    if ($result.Failures.Count -gt 0) {
        Warn ("{0} could not be removed: {1}. Re-running is safe and will retry them." -f $result.Failures.Count, ($result.Failures -join ', '))
        exit 1
    }

    exit 0
}

# Applies the H1 staging ACL to $path: SYSTEM and Administrators only, full control, inheritance removed,
# and Administrators as owner (#4043 round-1 review). Used only for the private folder a zip -Source is
# copied into before it is hashed and extracted - the zip is hashed and expanded from THAT copy, never the
# original, so a folder anyone else can write to between the hash and the extract cannot swap what actually
# lands on disk. icacls, not Set-Acl, for the same reason Lock-DarlingInstallTree uses it: it is what the
# fix-it-by-hand advice would tell an operator to run, and it needs no SeSecurityPrivilege. $path must
# already exist; the caller owns its lifetime and deletes it when done.
#
# /reset comes first because /inheritance:r strips only INHERITED entries. A folder created under a parent
# that passes nothing on gets its creator's default DACL as EXPLICIT entries (the creating account with full
# control, among them), and those survived the strip; CI's elevated runner is one such parent. /reset
# replaces every explicit entry with what the parent passes on, which /inheritance:r then removes, so the
# two grants below are the whole DACL.
function Protect-DarlingStagingFolder([string]$path) {
    $wk = [System.Security.Principal.WellKnownSidType]
    $systemSid = (New-Object System.Security.Principal.SecurityIdentifier($wk::LocalSystemSid, $null)).Value
    $adminsSid = (New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinAdministratorsSid, $null)).Value

    $steps = @(
        @('/reset'),
        @('/inheritance:r'),
        @('/grant', "*${systemSid}:(OI)(CI)F", '/grant', "*${adminsSid}:(OI)(CI)F"),
        @('/setowner', "*$adminsSid"))
    foreach ($step in $steps) {
        $output = & icacls.exe $path @step 2>&1
        if ($LASTEXITCODE -ne 0) { throw "icacls $($step -join ' ') on '$path' failed: $($output -join ' ')" }
    }
}

# A fresh, empty, protected folder under the machine temp directory for H1's zip staging copy. Elevation is
# assumed (this script requires it), so setting Administrators as owner of this new folder needs no extra privilege.
# Lock-DarlingInstallTree's owner step, which changes objects this process did not create, turns on
# SeTakeOwnershipPrivilege and SeBackupPrivilege itself.
function New-DarlingProtectedStagingFolder {
    $path = Join-Path $env:TEMP ('darling-upgrade-staging-' + [guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $path -Force | Out-Null
    Protect-DarlingStagingFolder $path
    return $path
}

# ============================ the install root has to already be trustworthy (#4043, M3) ============================
#
# #4038's lock (below, before the backup and the copy) only stops FURTHER writes to $InstallRoot. An install
# made before #4038 shipped was writable by ordinary local users for its entire life until the day it is
# first locked - which, for an existing install, is THIS run - so a binary in it may already have been
# swapped, and an in-place upgrade's overlay would not notice: it replaces what the new build ships and
# verifies nothing else already in the tree. install-darling.ps1 already refuses to (re-)install over a tree
# like this at its own step 1a; this is the same check, on the same tree, before this script's own lock runs.
#
# Skipped for -PruneOnly and -ListRollbacks, which exit above this and touch nothing but the rollback
# backups this script itself created.
#
# $preLockTrusted is computed once, here. The -Source folder and SHA256SUMS checks below use $sourceTrusted instead:
# a source build is a folder the service never writes to, so the service's own account is trusted nowhere in it.
$existingService = Get-Service -Name $serviceName -ErrorAction SilentlyContinue
$preLockTrusted = Get-DarlingPreLockTrustedSidsForRerun $serviceName $existingService
$sourceTrusted = @(Get-DarlingPreLockTrustedSids)
if (-not $AcceptWritableExtraction) {
    $writableRecords = @(Get-UntrustedWriteGrantees $InstallRoot $preLockTrusted.Trusted -AsRecords -Recurse -ServiceSids $preLockTrusted.ServiceSids -ServiceDirectories @(Get-DarlingExtraServiceWriteDirectories $InstallRoot (Join-Path $InstallRoot $configName)))
    $writable = @($writableRecords | ForEach-Object { $_.Text })
    if ($writable.Count -gt 0) {
        $lines = ($writable | Select-Object -First 20 | ForEach-Object { "  $_" }) -join "`n"
        $more = if ($writable.Count -gt 20) { "`n  ...and $($writable.Count - 20) more." } else { '' }
        Fail @"
This install directory is already writable by ordinary users, before this upgrade locks anything down:

$lines$more

A tree installed before #4038 was writable by the accounts above for its whole life. An in-place upgrade
only copies over what this build ships. It does not check the rest, so other files in the tree can differ
from what an earlier build shipped.

$(Get-DarlingRefusalAdvice $InstallRoot $serviceName (Get-DarlingInstallRootFromService $serviceName) $writableRecords)

If you accept the risk of upgrading this tree in place anyway, re-run with -AcceptWritableExtraction.

This refusal happens once for a given tree. The lock this upgrade applies next closes the folder to further
writes, so a later upgrade of this install passes straight through.

Nothing was stopped or copied.
"@
    }
}
else {
    Warn '-AcceptWritableExtraction - not checking whether the existing install directory was already writable by other local users before this upgrade.'
}

# ============================ the source build ============================

if ([string]::IsNullOrWhiteSpace($Source)) {
    Fail "No -Source given and this script is not running from a file, so there is nothing to install from."
}

try { $Source = [IO.Path]::GetFullPath((Get-DarlingOrdinaryPath $Source)).TrimEnd('\') }
catch { Fail "'$Source' is not a path this script can resolve." }

if (-not (Test-Path -LiteralPath $Source)) {
    Fail "The source '$Source' does not exist."
}

$sourceIsZip = (Test-Path -LiteralPath $Source -PathType Leaf) -and $Source.EndsWith('.zip', [StringComparison]::OrdinalIgnoreCase)
$sourceRoot = if ($sourceIsZip) { [IO.Path]::GetDirectoryName($Source) } else { $Source }
$zipStagingFolder = $null

# Everything from here to the end of the script runs inside this try - deliberately NOT re-indented, to keep
# this change reviewable as a diff rather than touching every line that follows - so that $zipStagingFolder,
# once H1's staging copy below creates it, is ALWAYS removed on the way out, including every Fail() between
# here and the extraction that uses it: Fail() calls exit, and exit still runs an enclosing finally (verified
# empirically; PowerShell's `trap` does NOT catch it, which is why this is a finally and not a trap).
try {

# THE SELF-OVERWRITE REFUSAL.
#
# This script ships inside the zip and therefore also lives in the install root, so an upgrade run from the
# INSTALLED copy would write the new build over the .ps1 that PowerShell is reading line by line. Best case
# the copy fails on the lock and the tree is half written with the service stopped; worst case the script's
# own remaining lines change underneath it. Neither is worth a clever workaround like relaunching from a
# temp copy: a refusal that names the fix is understood in one read and cannot go subtly wrong.
#
# The condition is WHERE THIS SCRIPT IS, not where the source is. Keying it off the source was the first
# spelling and it had a hole big enough to drive the whole failure through: a zip sitting inside the install
# directory - which is exactly where someone downloads it - has a source root equal to the install root and
# would have been waved past by any rule about folders. The hazard is "the file being executed is about to
# be overwritten", so that is the thing to ask about.
#
# Scoped to the copy path. -PruneOnly and -ListRollbacks exit above this and write no binaries, so the
# installed copy is exactly the right thing to run for those - and it is the one the service's report names.
$runningFrom = $PSScriptRoot
if (Test-DarlingSamePath $runningFrom $InstallRoot) {
    Fail "This script is running from the install directory, so the upgrade would write the new build over the copy of itself that PowerShell is currently reading. Extract the new zip to a staging folder only an administrator can write to (e.g. C:\Program Files\PerformanceMonitorDarling-staging\<version>, never a folder made directly under C:\) and run ITS upgrade-darling.ps1 instead. To prune rollback backups from the installed copy, use -PruneOnly, which copies nothing."
}
if (Test-DarlingSameFolderIdentity $runningFrom $InstallRoot) {
    Fail "This script is running from the install directory (reached by a different path), so the upgrade would write the new build over the copy of itself that PowerShell is currently reading. Extract the new zip to a staging folder only an administrator can write to and run ITS upgrade-darling.ps1 instead."
}

# And the degenerate case the rule above does not cover: a source folder that IS the install directory,
# handed in from a script running somewhere else. Copying a tree over itself is not an upgrade.
if (-not $sourceIsZip -and (Test-DarlingSamePath $sourceRoot $InstallRoot)) {
    Fail "The source folder is the install directory itself, so there is nothing to upgrade from. Point -Source at the new build."
}
if (-not $sourceIsZip -and (Test-DarlingSameFolderIdentity $sourceRoot $InstallRoot)) {
    Fail "The source folder is the install directory itself (reached by a different path), so there is nothing to upgrade from. Point -Source at the new build."
}

if ($sourceIsZip) {
    # #4043 round-1 review, H1: the zip is copied into a staging folder ONLY SYSTEM and Administrators can
    # write to before it is hashed, and is hashed and extracted from THAT copy alone, never the original
    # path. Without this, a zip sitting in a folder ordinary users can write to is hashed once here and read
    # AGAIN by Expand-Archive far below, after the stop guard, the service stop and the backup - nothing
    # holds the file open in between, so a zip that others can write is never proven to be the file that
    # actually gets extracted, no matter how good the hash check looks on screen.
    $zipStagingFolder = New-DarlingProtectedStagingFolder
    $stagedZip = Join-Path $zipStagingFolder ([IO.Path]::GetFileName($Source))
    Copy-Item -LiteralPath $Source -Destination $stagedZip -Force

    $expected = $Sha256

    if ([string]::IsNullOrWhiteSpace($expected)) {
        # SHA256SUMS.txt is trusted only when the folder it sits in is (#4043 round-1 review, H1): without
        # -Sha256, that sidecar file is the ENTIRE proof, and anyone who could write a swapped zip into this
        # folder could write a line into SHA256SUMS.txt saying it is fine. -Sha256 skips this folder check
        # entirely: the hash of the PROTECTED COPY above is already proof enough, independent of where the
        # original sat.
        $sums = Join-Path $sourceRoot 'SHA256SUMS.txt'
        if (Test-Path -LiteralPath $sums) {
            $sumsFolderWritable = @(Get-UntrustedWriteGrantees $sourceRoot $sourceTrusted -Recurse)
            if ($sumsFolderWritable.Count -gt 0) {
                $lines = ($sumsFolderWritable | Select-Object -First 20 | ForEach-Object { "  $_" }) -join "`n"
                $more = if ($sumsFolderWritable.Count -gt 20) { "`n  ...and $($sumsFolderWritable.Count - 20) more." } else { '' }
                Fail @"
SHA256SUMS.txt sits in a folder ordinary users can already write to:

$lines$more

That file is the only proof this script has without -Sha256, and anyone who could write a swapped zip into
this folder could write a line into SHA256SUMS.txt saying it is fine.

Pass -Sha256 <hash> instead, using the hash published on the release's GitHub page. Nothing has been
stopped or copied.
"@
            }
            $leaf = [IO.Path]::GetFileName($Source)
            foreach ($line in (Get-Content -LiteralPath $sums)) {
                # '<hash>  <name>' and '<hash> *<name>' are both in the wild; splitting on whitespace and
                # comparing the leaf handles either without a regex nobody can read.
                $parts = @($line -split '\s+' | Where-Object { $_ })
                if ($parts.Count -ge 2 -and $parts[-1].TrimStart('*').Equals($leaf, [StringComparison]::OrdinalIgnoreCase)) {
                    $expected = $parts[0]
                    break
                }
            }
        }
    }

    if ([string]::IsNullOrWhiteSpace($expected)) {
        if (-not $SkipHashCheck) {
            Fail "No SHA256 for '$Source' - pass -Sha256 <hash>, using the hash published on the release's GitHub page, put a SHA256SUMS.txt in a folder only an administrator can write to, or say -SkipHashCheck. This overwrites the binaries of a running monitoring host; an unverified zip is not something to find out about afterwards."
        }
        Warn "Proceeding with an UNVERIFIED source zip (-SkipHashCheck)."
    }
    else {
        # Hashed from the STAGED COPY, not $Source - that is the entire point of H1's fix: proving the bytes
        # about to be extracted below are the ones just hashed, with nothing else able to touch them in
        # between.
        $actual = (Get-FileHash -LiteralPath $stagedZip -Algorithm SHA256).Hash
        if (-not $actual.Equals($expected.Trim(), [StringComparison]::OrdinalIgnoreCase)) {
            Fail "SHA256 mismatch on '$Source'. Expected $expected, got $actual. Nothing has been stopped or copied."
        }
        Good "Source zip SHA256 verified."
    }

    # From here on, the STAGED, hashed, admin-only copy is what gets extracted - never the original path,
    # which nothing has held open since the hash above ran.
    $Source = $stagedZip
}
else {
    if (-not (Test-Path -LiteralPath (Join-Path $Source $serviceExeName))) {
        Fail "'$Source' does not hold $serviceExeName, so it is not an extracted Darling build."
    }
    Warn "The source is a folder, so this script cannot verify it - verify the zip's SHA256 before you extract it."

    # A zip -Source is covered above regardless of who could write to the folder it sits in, because it is
    # copied into a protected folder and hashed there. A folder -Source has no content check at all - only
    # that the exe is present - so this is its own pre-lock writable-extraction window (#4043): if the folder
    # ever inherited a broad write grant (extracted directly under C:\, or into a folder in your own profile
    # such as Downloads, which is writable by anything already running as you, elevated or not) between
    # whenever it was made and now, a local user could have swapped a binary in it, and nothing above would
    # notice. Same recursive check install-darling.ps1 runs at 1a (round-1 review, M2), same trusted set,
    # same override.
    if (-not $AcceptWritableExtraction) {
        $writable = @(Get-UntrustedWriteGrantees $Source $sourceTrusted -Recurse)
        if ($writable.Count -gt 0) {
            $lines = ($writable | Select-Object -First 20 | ForEach-Object { "  $_" }) -join "`n"
            $more = if ($writable.Count -gt 20) { "`n  ...and $($writable.Count - 20) more." } else { '' }
            Fail @"
Ordinary users can already write to the source folder '$Source':

$lines$more

This script only confirms $serviceExeName is present in a folder -Source - it does not, and cannot, verify
its CONTENT the way a zip's SHA256 does. A binary here may already have been replaced. Do NOT try to repair
this folder's permissions in place: fixing the ACL cannot undo a file that was already replaced. Extract the
new build fresh under C:\Program Files\<something>, or another folder only an administrator can write to,
and point -Source there instead - or verify the zip's SHA256 and pass the zip itself as -Source.

If this folder is deliberately writable (a dev loop) and you accept the risk, re-run with
-AcceptWritableExtraction. Nothing has been stopped or copied.
"@
        }
    }
    else {
        Warn '-AcceptWritableExtraction - not checking whether the source folder was already writable by other local users.'
    }
}

# ============================ the service has to exist ============================
#
# The auto-resolve path cannot reach here without a registered service - it reads the install root out of
# the ImagePath - but an explicit -InstallRoot skips that check entirely. A tree holding the binaries of a
# service that was renamed, removed, or never registered then sails through the stop guard, the backup, the
# prune and the copy, and falls over at Start-Service with a raw terminating error instead of one of this
# script's own messages. Failing HERE costs nothing and says what to do; failing there costs a completed
# copy, a stopped-that-was-never-running service, and an error nobody can interpret.
#
# Deliberately not applied to -PruneOnly, which exits above: reclaiming disk from a tree whose service is
# gone is a perfectly reasonable thing to want, and it copies nothing.
if (-not (Get-Service -Name $serviceName -ErrorAction SilentlyContinue)) {
    Fail "The '$serviceName' service is not registered on this machine, so there is nothing for this copy to stop and start around it. NOTHING has been stopped or copied. If '$InstallRoot' is a staging tree rather than an install, you want install-darling.ps1; if you only meant to reclaim disk, re-run with -PruneOnly, which needs no service."
}

# ...and it has to be THIS install's service.
#
# "A service by that name exists" and "that service runs from the directory I am about to overwrite" are
# different claims, and the gap between them is a SILENT SUCCESS - the worst shape a failure can take here.
# Stop-Service and Start-Service act on the service BY NAME, never by path, so an -InstallRoot pointing at
# a stale copy of the tree (an old runbook, a paste from another box, a leftover directory that still holds
# the exe and therefore passes every check above) produces a run in which nothing is detectably wrong: the
# REAL service is stopped - a real outage on a monitoring host - the new build is laid down in a directory
# nothing reads, the real service is restarted on its old untouched binaries, darling.json is unchanged
# because it was never touched, and the script reports success end to end. The operator believes they
# upgraded. Nothing did, and the next person to look will be debugging a version that never shipped.
#
# Not gated on whether -InstallRoot was passed. When it was not, the two are equal by construction and this
# costs a registry read; gating it would make the check absent for precisely the one caller who needs it.
$registeredRoot = Get-DarlingInstallRootFromService $serviceName

if ([string]::IsNullOrWhiteSpace($registeredRoot)) {
    # Registered but unreadable ImagePath. Not a refusal - we know the service exists and the auto-resolve
    # path would have failed earlier - but the operator should know this was not confirmed rather than
    # assume it was.
    Warn "Could not read the '$serviceName' service's ImagePath, so this could not confirm that '$InstallRoot' is the directory the service actually runs from. Check that before trusting the result."
}
elseif (-not (Test-DarlingSamePath $registeredRoot $InstallRoot) -and -not (Test-DarlingSameFolderIdentity $registeredRoot $InstallRoot)) {
    Fail "The '$serviceName' service runs from '$registeredRoot', not from '$InstallRoot'. NOTHING has been stopped or copied. Stopping and starting act on the service by NAME, so upgrading '$InstallRoot' would have taken the real service down, written the new build somewhere it does not read, and brought it back up on its old binaries - reporting success the whole way. Drop -InstallRoot to upgrade the registered install, or pass -InstallRoot '$registeredRoot' if that is really the one you meant."
}

# ============================ the stop guard ============================

# PHASE ONE, before anything is stopped: the processes a service stop will NOT clear.
#
# The service's own exe and the bundled PostgreSQL under pg-runtime are filtered out here because they are
# about to be stopped on purpose. Without that filter this guard refuses EVERY upgrade of a running install
# - the service is always holding its own tree - which is a guard that fails closed on the happy path and
# trains people to pass -SkipStopGuard, i.e. a guard that has stopped guarding by being unusable.
$holders = @(Get-DarlingProcessesUnderPath $InstallRoot |
    Where-Object { -not (Test-DarlingProcessStopsWithTheService $_.Path $InstallRoot) })

if ($holders.Count -gt 0 -and -not $SkipStopGuard) {
    # Parenthesised before -join on purpose: `$x | ForEach-Object { ... } -join ', '` binds -join to
    # ForEach-Object as a parameter and throws, which is a fine way to lose a deploy to a formatting bug.
    $names = @($holders | ForEach-Object { "$($_.ProcessName) (pid $($_.Id))" })
    Note "Processes are running out of the install tree that stopping the service will not close:"
    foreach ($name in $names) { Note "  $name" }
    Fail "Close them and re-run. NOTHING has been stopped or copied - the service is still running and the install is untouched. Do NOT kill them blindly. If these are your own psql.exe or a shell sitting in the install directory, or a Darling Viewer you left open, just exit them. Use -SkipStopGuard only if you are certain the copy will not hit a locked file."
}

# ============================ stop, back up, prune, copy, start ============================
#
# From here on a failure leaves the service DOWN. Every step below is idempotent and the failure messages
# say so, because "run it again" is the correct advice and an operator staring at a stopped monitoring
# service should not have to work that out.

$configPath = Join-Path $InstallRoot $configName
$configHashBefore = if (Test-Path -LiteralPath $configPath) { (Get-FileHash -LiteralPath $configPath -Algorithm SHA256).Hash } else { $null }

# The previous build's manifest, read BEFORE the copy for the same reason the config hash above is taken
# before it - and it took review to see why that reason applies here too (#2529).
#
# A -Source FOLDER is copied wholesale: Get-ChildItem -LiteralPath $Source -Force | Copy-Item -Recurse
# -Force takes everything in it, unfiltered. A staging directory made from a live install therefore carries
# THAT install's manifest, and
# -Force lays it over this one. Reading afterwards would compute this run's stale-file list against some
# other box's history - the one scenario the manifest's own write-side filter already names as expected,
# arriving from the other direction. It self-heals on the next upgrade, because the manifest written at the
# end comes from the real payload, but one cycle of a wrong answer is one too many for a list that feeds a
# delete.
#
# The copy cannot change the answer to "what did the build before this one lay down here", so before it is
# not merely safe, it is the only correct time to ask.
$manifestPath = Join-Path $InstallRoot $manifestName
$previousManifest = Read-DarlingInstallManifest $manifestPath

# Status is re-read here rather than carried down from the existence check above: it is a snapshot, and
# between the two the service can legitimately have been stopped by someone else or crashed on its own.
if ((Get-Service -Name $serviceName).Status -ne 'Stopped') {
    Note "Stopping '$serviceName'..."
    Stop-Service -Name $serviceName -Force
    try { (Get-Service -Name $serviceName).WaitForStatus('Stopped', [TimeSpan]::FromMinutes(2)) }
    catch { Fail "'$serviceName' did not reach Stopped within two minutes. Nothing has been copied. Check what it is waiting on and re-run." }
}
Good "Service is stopped."

# PHASE TWO, now that it is down: anything STILL holding the tree, with no exclusions at all.
#
# Everything phase one filtered out should be gone by now, so a hit here is the interesting case rather
# than the normal one - most often a postmaster under pg-runtime that outlived the service stop, which is
# precisely the process nothing may kill. Phase one cannot see this and phase two cannot see phase one's
# cases without an outage, which is why there are two.
#
# #4466: Get-Service already reported Stopped above, but that is the SCM's state, not proof every process
# the service spawned has actually finished exiting - so this gives one a bounded window to finish before
# treating its presence as the interesting case. -SkipStopGuard skips the wait along with the refusal it
# exists to avoid, same as it always has.
if (-not $SkipStopGuard) {
    Wait-DarlingInstallTreeClear $InstallRoot $script:InstallTreeClearWaitSeconds $script:InstallTreeClearPollSeconds | Out-Null
}
$stillHolding = Get-DarlingProcessesUnderPath $InstallRoot
if ($stillHolding.Count -gt 0 -and -not $SkipStopGuard) {
    $names = @($stillHolding | ForEach-Object { "$($_.ProcessName) (pid $($_.Id))" })
    Note "The service is stopped, but processes are STILL running out of the install tree:"
    foreach ($name in $names) { Note "  $name" }
    Fail "Nothing has been copied, so the install is intact - but the service is now STOPPED. Either close these and re-run (safe, and it will reuse the backup it is about to take), or abandon the upgrade with: Start-Service '$serviceName'. Do NOT kill anything under $InstallRoot\pg-runtime - that is the bundled PostgreSQL and killing it takes the store down; give a postmaster that outlived the stop a few seconds and re-run."
}

# ============================ lock the install tree (#4034) ============================
#
# The same lock install-darling.ps1 applies, so an install made before it is closed at its next upgrade: see
# Lock-DarlingInstallTree above. As soon as the service is down, so the rollback backup and the new build both
# land in a tree no ordinary user can change between here and the start; run again after the copy to verify.
# The account is read from the service's own registration, the account it really runs as, and the lock
# normalizes its LocalSystem and .\ spellings. Unreadable, the lock is SKIPPED with a warning rather than run
# around a guess: it removes any write grant outside the trusted set, so a wrong account would strip the real
# one's access to pg-runtime. NOTHING here calls Fail: the service is stopped and starting it is what the
# operator came for, so a failure is a loud warning.
$logonAccount = Get-DarlingServiceLogonName $serviceName
if ($logonAccount -eq 'LocalSystem') { $logonAccount = 'NT AUTHORITY\SYSTEM' }
elseif ($logonAccount) { $logonAccount = $logonAccount -replace '^\.\\', "$env:COMPUTERNAME\" }

# Set by Invoke-UpgradeTreeLock the first time a lock is skipped or leaves paths open; the TLS key file step reads it.
$installTreeLockProblem = $null

function Invoke-UpgradeTreeLock([string]$when) {
    if (-not $logonAccount) {
        if (-not $script:installTreeLockProblem) { $script:installTreeLockProblem = "the install folder was not locked (the account '$serviceName' logs on as could not be read)" }
        Warn "Could not read which account '$serviceName' logs on as (sc.exe qc `"$serviceName`"), so the install folder was NOT locked $when rather than locked around a guess that could strip that account's access. Fix the lookup and re-run, or run install-darling.ps1."
        return
    }
    $open = @()
    try {
        $open = @(Lock-DarlingInstallTree $InstallRoot $logonAccount (Get-DarlingExtraServiceWriteDirectories $InstallRoot $configPath))
    }
    catch {
        $open = @("$InstallRoot ($($_.Exception.Message))")
    }
    if ($open.Count -eq 0) {
        Good "Install folder locked ${when}: only SYSTEM and Administrators can change what runs from $InstallRoot; $logonAccount can write only its runtime folders."
        return
    }
    if (-not $script:installTreeLockProblem) { $script:installTreeLockProblem = "ordinary users can still change $($open.Count) path(s) in the install folder" }
    Warn ("Ordinary users can still change {0} path(s) in the install folder, and the service runs from there as {1}, so anyone who can replace a binary can run code as that account. First: {2}. Fix from an elevated prompt with: icacls `"{3}`" /inheritance:d, then icacls `"{3}`" /remove:g *S-1-5-11 *S-1-5-32-545 *S-1-1-0 *S-1-5-4, then icacls `"{3}`" /grant `"*S-1-5-32-545:(OI)(CI)RX`" /grant:r `"{1}:(OI)(CI)RX`", then run this script again (it makes Administrators the owner of each path itself, and lists a file with more than one name). A path marked as a hard link (a file with more than one name): delete this name if it is not part of the install, or extract the file again if it is, and do not run icacls on it. Any other path listed as owned by an account, or below the folder, needs its own fix; these two commands are only for paths not marked as a hard link: icacls `"<path>`" /setowner *S-1-5-32-544 /L, then icacls `"<path>`" /reset /C. Remove any junction or link it names." -f $open.Count, $logonAccount, (($open | Select-Object -First 5) -join ', '), $InstallRoot)
}

Invoke-UpgradeTreeLock 'before the backup and the copy'

$backups = Get-DarlingRollbackBackups $InstallRoot
$nowUtc = [datetime]::UtcNow

# A recent backup is reused only when the upgrade that took it did not finish (#6821): a manifest written after it means
# that upgrade completed, and reusing its backup would leave the newest backup holding files from two builds back.
$backupDecision = Get-DarlingRollbackBackupDecision $backups $BackupWindowMinutes $nowUtc $manifestPath

if ($backupDecision -eq 'reuse') {
    Note ("Reusing the rollback backup {0}, taken {1:N0} minute(s) ago - this looks like a re-run of an interrupted upgrade (no upgrade has finished since it was taken), and a second backup now would copy a half-upgraded tree over the good one." -f $backups[0].Name, ($nowUtc - $backups[0].LastWriteTimeUtc).TotalMinutes)
}
else {
    if ($backupDecision -eq 'finished') {
        Note ("The rollback backup {0} is inside the {1}-minute window, but the upgrade that took it finished (the install manifest is newer), so this is a new upgrade and it takes its own backup." -f $backups[0].Name, $BackupWindowMinutes)
    }
    $backupPath = Join-Path $InstallRoot (New-DarlingRollbackBackupName $nowUtc)
    Note "Backing up the install root's files to $backupPath ..."
    try {
        New-Item -ItemType Directory -Path $backupPath -Force | Out-Null
        # The folder is restricted to SYSTEM and Administrators BEFORE anything is copied into it (#5700), so every copy is born
        # with only that DACL instead of the install root's inherited one, and no file in it is ever readable by another account.
        # A folder that could not be restricted is never copied into: the upgrade stops here, with nothing copied or overwritten.
        $backupProblem = Protect-DarlingRollbackBackupFolder $backupPath
        if ($backupProblem) {
            # The empty folder is removed too, so a re-run does not take it for a backup that was taken.
            try { [System.IO.Directory]::Delete($backupPath) } catch { $null = $_ }
            Fail "The new rollback backup folder could not be restricted to SYSTEM and Administrators, so no file was copied into it. The service is STOPPED and NOTHING has been copied or overwritten, so the install is intact: start it with 'Start-Service ''$serviceName''', or fix the cause and re-run from an elevated prompt. Cause: $backupProblem"
        }
        # FILES only, not subdirectories - the shape the documented procedure has always used, and the
        # reason a backup is ~120 MB rather than ~1 GB. pg-runtime is the bundled PostgreSQL (extracted on
        # first run, and hundreds of megabytes of it), and viewer\ / wwwroot\ / runtimes\ all come back
        # from the zip.
        #
        # SO A BACKUP IS NOT A WHOLE-TREE SNAPSHOT, and the difference matters in exactly one scenario:
        # a copy that dies PARTWAY can leave viewer\ / wwwroot\ / runtimes\ mixed old-and-new, and
        # restoring these files over the top does not unmix them. The complete revert is these files PLUS
        # the previous version's zip re-extracted. Backing those directories up instead was considered and
        # rejected: it multiplies what #2525 is about - retained disk - to cover a case whose real fix is
        # re-extracting a zip you still have, and it still would not cover pg-runtime. The failure paths
        # below say this rather than leaving an operator to discover it while recovering.
        Get-ChildItem -LiteralPath $InstallRoot -File -Force | Copy-Item -Destination $backupPath -Force

        # HARDEN THE SECRETS WE JUST COPIED (#2574). darling.json holds every monitored server's
        # encryptedPassword plus the MCP and web tokens, all DPAPI LocalMachine scope with an entropy
        # constant published in this open-source repo - so anything that can READ a copy can decrypt the
        # lot, which is #1647's finding and the reason the LIVE file is hardened.
        #
        # Copy-Item into a new directory takes the DESTINATION's inherited DACL, not the source file's, so
        # the copy lands with whatever the install root grants. Measured on a real install root: BUILTIN\Users
        # ReadAndExecute - the inherited-from-C:\ DACL #1647 called out. Every retained backup was therefore
        # a config readable by any local user, on a box where the live file is locked down.
        #
        # No INTERACTIVE grant here, deliberately, and unlike the live file: the Viewer and the CLI verbs
        # read darling.json, and nothing reads a backup copy except a human recovering, who is an
        # administrator by then. install-darling.ps1 draws the same line for the .bak-* copies.
        #
        # Best-effort and non-fatal: a backup that is taken but not hardened is strictly better than an
        # upgrade that refuses to proceed, and the operator is told which.
        $secretCopies = @(Get-ChildItem -LiteralPath $backupPath -File -Force -ErrorAction SilentlyContinue |
            Where-Object { $_.Name -ieq 'darling.json' -or $_.Extension -ieq '.dpapi' })

        # An enumeration that fails - a locked handle, an AV scan mid-copy - yields an EMPTY set, not an
        # error, so without this the loop below would simply not run and nothing would be said. That is the
        # silent-exposure shape this whole change is about, so the absence is checked against what the
        # install root actually holds rather than assumed benign.
        if ($secretCopies.Count -eq 0 -and (Test-Path -LiteralPath (Join-Path $InstallRoot 'darling.json'))) {
            Warn "Found no darling.json in the rollback backup to restrict, though the install root has one. The backup may hold an unprotected copy of your credentials - check $backupPath by hand, or delete it."
        }

        foreach ($copy in $secretCopies) {
            try {
                $wk = [System.Security.Principal.WellKnownSidType]
                $systemSid = New-Object System.Security.Principal.SecurityIdentifier($wk::LocalSystemSid, $null)
                $adminsSid = New-Object System.Security.Principal.SecurityIdentifier($wk::BuiltinAdministratorsSid, $null)
                $acl = New-Object System.Security.AccessControl.FileSecurity
                $acl.SetAccessRuleProtection($true, $false)
                $acl.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($systemSid, 'FullControl', 'Allow')))
                $acl.AddAccessRule((New-Object System.Security.AccessControl.FileSystemAccessRule($adminsSid, 'FullControl', 'Allow')))
                Set-Acl -LiteralPath $copy.FullName -AclObject $acl

                # VERIFY, do not assume (#1957). A Set-Acl that returns without throwing is not proof the
                # file is protected: install-darling.ps1 carries the same re-read for the same files, added
                # after a permissions call that appeared to succeed left them readable on three consecutive
                # field installs. Trusting the absence of an exception is precisely how that went unnoticed
                # for months, and the stake here is every monitored server's encrypted credentials.
                $after = Get-Acl -LiteralPath $copy.FullName
                $stillOpen = @($after.Access | Where-Object { $_.IdentityReference -match 'Users|Everyone|Authenticated' })
                if ((-not $after.AreAccessRulesProtected) -or $stillOpen.Count -gt 0) {
                    Warn "Restricting $($copy.Name) in the rollback backup reported success but the file is STILL readable by ordinary users (protected=$($after.AreAccessRulesProtected)). It holds encrypted credentials - restrict it by hand, or delete the backup."
                }
            }
            catch {
                Warn "Could not restrict $($copy.Name) in the rollback backup ($($_.Exception.Message)). It holds encrypted credentials and inherited the install root's permissions - restrict it by hand, or delete the backup."
            }
        }
    }
    catch {
        Fail "Could not take the rollback backup ($($_.Exception.Message)). The service is STOPPED and NOTHING has been overwritten, so the install is intact: start it with 'Start-Service ''$serviceName''', or free up disk and re-run."
    }

    $backups = Get-DarlingRollbackBackups $InstallRoot
    Good ("Rollback backup taken ({0})." -f (Format-DarlingBytes (Get-DarlingDirectoryBytes $backupPath)))
}

# Pruning comes AFTER the backup, so the copy this run just took is counted among the ones kept and the
# tree never passes through a moment with fewer rollback points than retention promises.
$prunable = Select-DarlingRollbackBackupsToPrune $backups $KeepRollbacks
if ($prunable.Count -gt 0) {
    Note ("Pruning {0} rollback backup(s) past the newest {1}:" -f $prunable.Count, $KeepRollbacks)
    $result = Remove-DarlingRollbackBackups $prunable
    Good ("Reclaimed {0}." -f (Format-DarlingBytes $result.Reclaimed))
}

# Every kept rollback backup is limited to SYSTEM and Administrators, one folder at a time (#5700).
Protect-DarlingKeptRollbackBackups $InstallRoot

# THIS COPY IS AN OVERLAY, not a replacement. Expand-Archive -Force (and the Copy-Item -Recurse -Force
# folder path) overwrite what the new build ships and delete nothing else, so a file the old version had
# and the new one dropped survives this step by construction. DarlingInstallDirectoryReport cannot see it
# either: it walks top-level DIRECTORIES, so a stale DLL in the root, or in viewer\ or runtimes\, is
# structurally invisible to it.
#
# That is dealt with AFTER the copy rather than here, against the manifest the previous upgrade wrote
# (#2529) - search this file for "the files this build no longer ships". After, because the difference
# being looked for is between what an earlier build put in this tree and what THIS payload ships, and the
# second half of that only exists once the copy has landed.

Note "Laying the new build over $InstallRoot ..."
$copied = $false
foreach ($attempt in 1, 2) {
    try {
        if ($sourceIsZip) {
            Expand-Archive -LiteralPath $Source -DestinationPath $InstallRoot -Force
        }
        else {
            # The folder is listed with -LiteralPath and the items are piped in, not copied with -Path and a '*':
            # PowerShell reads [ and ] in a -Path value as wildcard characters, so a staging folder named build[1]
            # matched nothing, threw nothing, and the old build stayed in place under a success message (#4745).
            # -LiteralPath on Copy-Item itself would make the * literal too. Each piped item binds its own literal path.
            Get-ChildItem -LiteralPath $Source -Force | Copy-Item -Destination $InstallRoot -Recurse -Force
        }
        $copied = $true
        break
    }
    catch {
        # The transient one is a DLL an antivirus scan or a not-yet-exited process still holds, and a retry
        # a moment later has worked more than once. Two attempts, then stop: a third would just be a longer
        # way to arrive at the same half-written tree.
        if ($attempt -eq 1) {
            Warn "The copy failed ($($_.Exception.Message)). Retrying in 10 seconds - this step has lost to a transiently locked DLL before."
            Start-Sleep -Seconds 10
        }
        else {
            Fail "The copy failed twice ($($_.Exception.Message)). The service is STOPPED and the install tree may be HALF WRITTEN - do not start it. Re-run this script with the same arguments: it will reuse the rollback backup it already took rather than replacing it, and finish the copy, which is the FIRST thing to try. To go back to the old version instead, note that a half-written tree needs BOTH halves: re-extract the PREVIOUS version's zip over $InstallRoot (that restores viewer\, wwwroot\ and runtimes\, which the backup does not hold), then copy the files from the newest _rollback_manual_* directory over the top. Restoring only the backup leaves old root binaries paired with partly-new subdirectories."
        }
    }
}

if (-not $copied) { Fail "The copy did not complete." }

# A copy that reports success has not proved it copied anything. Until #4745 a staging folder whose name held
# [ or ] laid down nothing and still got the message below. So for a FOLDER -Source, compare the service
# executable in the source with the one in the install root before saying so. A zip -Source is left out:
# it is expanded with -LiteralPath, and its $Source is a file, not a folder holding the executable.
if (-not $sourceIsZip) {
    $sourceExeHash = (Get-FileHash -LiteralPath (Join-Path $Source $serviceExeName) -Algorithm SHA256).Hash
    $installedExeHash = (Get-FileHash -LiteralPath (Join-Path $InstallRoot $serviceExeName) -Algorithm SHA256).Hash
    if ($sourceExeHash -ne $installedExeHash) {
        Fail "The copy reported success, but the installed $serviceExeName is not the new build's, and the service is still STOPPED - re-run this script with the same arguments."
    }
}

Good "New build in place."

if ($configHashBefore) {
    $configHashAfter = if (Test-Path -LiteralPath $configPath) { (Get-FileHash -LiteralPath $configPath -Algorithm SHA256).Hash } else { $null }
    if ($configHashAfter -ne $configHashBefore) {
        # The zip ships darling.sample.json and never darling.json, so this should be impossible - which is
        # exactly why it is checked rather than trusted. A config replaced by a deploy is a monitoring host
        # that comes back up watching nothing, and the newest rollback backup still holds the original.
        Warn "darling.json CHANGED during the copy. The zip ships only darling.sample.json, so this should not happen. The original is in the newest _rollback_manual_* directory - compare them before starting the service."
    }
    else {
        Good "darling.json is unchanged."
    }
}

# ============================ the files this build no longer ships (#2529) ============================
#
# AFTER the copy - but only the half that has to be. The tree is now everything the new build says it
# should be plus whatever an earlier one left in it, so the DIFFERENCE, the existence check against the
# tree, and the removal all belong here. The previous build's manifest does NOT: it is read further up,
# before the copy, because a folder source can overwrite it. Review caught the two being conflated, and
# they are worth keeping apart in the reader's head as well as in the script.
#
# NOTHING HERE CALLS Fail, and that is a decision rather than an oversight. A monitoring host that is up
# with one stale DLL in it is in far better shape than one this script refused to start over a cleanup, so
# every failure below is a warning and a re-run. The service is still stopped at this point and starting it
# is what the operator came here for.

$payload = Get-DarlingPayloadFiles $Source $sourceIsZip
$carryForward = @()

if (-not $payload.Ok) {
    Warn "Could not read the new build's own file list ($($payload.Reason)), so this run cannot tell which files an earlier build left behind. NOTHING was removed, and no manifest was written - the next upgrade will be in the same position until a run gets a source it can read."
}
elseif (-not $previousManifest.Ok) {
    Note "Stale-file check skipped: $($previousManifest.Reason). Nothing was removed. This run writes the manifest, so the NEXT upgrade can answer the question."
}
else {
    $candidates = Select-DarlingStaleFiles $previousManifest.Files $payload.Files

    # Only the ones actually on disk. A path an operator already deleted by hand is not news, and carrying
    # it forward would keep it in the manifest, and in this report, for the rest of the install's life.
    $stale = @($candidates | Where-Object { $file = Join-DarlingInstallPath $InstallRoot $_; $file -and (Test-Path -LiteralPath $file -PathType Leaf) })

    if ($stale.Count -eq 0) {
        Good "No files from the previous build were dropped by this one."
    }
    elseif (-not $RemoveStaleFiles) {
        # Every path, uncapped. The service's layout report caps and summarises because it repeats on every
        # start; this prints once, at the moment of the deploy, with the operator reading it - and the case
        # that matters most is the forty-file one, where a count tells you nothing and the list tells you a
        # whole framework directory was stranded.
        Warn ("{0} file(s) in the install tree were shipped by an earlier build and are NOT in this one. Nothing was removed - re-run with -RemoveStaleFiles to delete them, or delete them by hand:" -f $stale.Count)
        foreach ($relative in $stale) { Note "  $relative" }
        Note "Every path above came out of one of our own build payloads: this check reads the manifest THIS SCRIPT wrote on the previous upgrade, so it can only ever name files one of our zips laid down - never darling.json, never the credential blobs, never pg-runtime."
        $carryForward = @($stale)
    }
    else {
        Note ("Removing {0} file(s) shipped by an earlier build and not by this one:" -f $stale.Count)
        $staleResult = Remove-DarlingStaleFiles $InstallRoot $stale $payload.Files

        Good ("Removed {0} stale file(s), reclaiming {1}." -f $staleResult.Removed.Count, (Format-DarlingBytes $staleResult.Reclaimed))

        if ($staleResult.Emptied.Count -gt 0) {
            Note ("Also removed {0} director(ies) that held nothing but those files: {1}" -f $staleResult.Emptied.Count, ($staleResult.Emptied -join ', '))
        }

        if ($staleResult.Refused.Count -gt 0) {
            Warn ("{0} path(s) were REFUSED rather than removed, because they name something this procedure never deletes: {1}. Nothing needs doing about it - it is reported because a delete that quietly did not happen is worse than one that says so." -f $staleResult.Refused.Count, ($staleResult.Refused -join ', '))
        }

        if ($staleResult.Failures.Count -gt 0) {
            Warn ("{0} stale file(s) could not be removed: {1}. Re-running the upgrade retries them." -f $staleResult.Failures.Count, ($staleResult.Failures -join ', '))
        }

        if ($staleResult.Removed.Count -gt 0) {
            Note "A stale file in a SUBDIRECTORY was not in the rollback backup, which holds the install root's files only. The complete revert is unchanged: re-extract the previous version's zip over the install root, then copy the newest _rollback_manual_* directory's files over the top."
        }

        # Whatever is still on disk stays NOMINATED. Without this the manifest written below would record
        # only what this build ships, a file that was reported and not removed would drop out of the record,
        # and no later upgrade would ever mention it again - a check that forgets, which is a check nobody
        # can act on at their own pace.
        $carryForward = @($staleResult.Failures) + @($staleResult.Refused)
    }
}

if ($payload.Ok) {
    $manifestResult = Write-DarlingInstallManifest $manifestPath (@($payload.Files) + @($carryForward)) ([IO.Path]::GetFileName($Source)) ([datetime]::UtcNow)

    if ($manifestResult.Ok) {
        Good ("Install manifest written ({0} file(s))." -f $manifestResult.Count)
    }
    else {
        Warn "Could not write the install manifest ($($manifestResult.Reason)). The upgrade itself is fine and the service will start; the next upgrade will not be able to tell which files this build shipped, and will remove nothing."
    }
}

# ============================ re-verify the install tree lock (#4034) ============================
#
# The lock went on before the copy (see there). Run again now that the new build and the stale-file cleanup
# have landed, so what is reported is the tree the service is about to start from.
Invoke-UpgradeTreeLock 'after the copy'

# ============================ the operator group ============================
#
# The service grants the local group '$operatorGroupName' read on darling.json and on the store's admin and
# viewer credentials, and at its first start on this build it drops the NT AUTHORITY\INTERACTIVE entries an
# earlier build left on them. An install made before the group existed has none, and without it every
# non-elevated Viewer and CLI user loses access at that start. So the group is made here, before the service
# starts, the way install-darling.ps1 makes it: the lookup comes first, so a group that already exists (an
# administrator's, or one an earlier run made) and its members are left exactly as they are, and this script
# adds nobody. Who gets the group's access is the administrator's decision.
#
# A group that cannot be made is a warning, and the service still starts. It is stopped and the new build is
# in place, and a monitoring host that is up is in far better shape than one this script left down over a
# group: the files stay readable by SYSTEM, Administrators and the service account only, and the service
# logs the missing group at every start until it exists.
$operatorSid = $null
$operatorGroupProblem = $null
# A domain controller has no local groups, and net localgroup there would make a domain group: it is skipped, and said so.
$onDomainController = Test-DarlingDomainController
if (-not $onDomainController) {
    try { $operatorSid = Initialize-DarlingOperatorGroup $operatorGroupName }
    catch { $operatorGroupProblem = $_.Exception.Message }
}

if ($operatorSid) {
    Note ''
    Note "Local group '$operatorGroupName' is in place. This script never adds members to it."
    Note "Add each person who runs the Viewer or the CLI without elevation to the local group '$operatorGroupName' (Computer Management > Local Users and Groups, or net localgroup `"$operatorGroupName`" DOMAIN\user /add)."
    Note "Members can read darling.json and the store's admin and viewer credentials, which decrypt every stored password; give it only to people you would give the passwords to."
    Note 'A person added to the group has to sign out and back in before the membership takes effect.'
    Note ''
}
elseif ($onDomainController) {
    Warn "This machine is a domain controller, which has no local groups, so the local group '$operatorGroupName' was NOT created and nothing was granted to it. The service will start. The Viewer and the CLI must be run elevated here: darling.json and the store's credentials are readable by SYSTEM, Administrators and the service account only."
}
else {
    Warn "Could not create the local group '$operatorGroupName': $operatorGroupProblem The service will start, but darling.json and the store's admin and viewer credentials stay readable by SYSTEM, Administrators and the service account only, so the Viewer and the CLI cannot read them without elevation. Create the group from an elevated prompt (net localgroup `"$operatorGroupName`" /add), add the people who run the Viewer or the CLI, then restart the service so it grants the group."
}

# ============================ the TLS key files (#5700) ============================
#
# The TLS private-key files darling.json names (a pfxPath or keyPath): the service exe's own verb sets their permissions, after
# the last tree lock above so the lock cannot undo it and before the service starts. A key file in the install folder is limited
# to SYSTEM and Administrators with Read for the service account; a key file outside the install folder is reported and left as
# it is. A verb that could not run is a warning with the command to run by hand, and the service still starts.
$tlsKeyWarning = Get-UpgradeTlsKeyFileWarning (Join-Path $InstallRoot $serviceExeName) $configPath
if ($tlsKeyWarning) { Warn $tlsKeyWarning }

# ============================ the store folder ============================
#
# The same lock install-darling.ps1 step 4b3 applies, through the same functions: the store folder is made or kept owned by
# Administrators and locked to SYSTEM, Administrators and the service account (the operator group can only traverse it),
# after its owner, its contents and any link on its path have been checked. A store made by a release that could not lock
# it ends locked here, and the service finds it locked at its first start. The service is stopped by now and starts below.
#
# A store this cannot lock is a WARNING, not a stop: the service checks the same things itself at its start and says what
# it refuses, and a host this script left down over a folder is worse off than one that is up. NOTHING here calls Fail.
$storeFolder = Get-DarlingStoreFolder $configPath
if (-not $storeFolder) {
    Note 'No store folder to secure here: postgres.managed is false, or darling.json could not be read or names a data directory this script does not handle.'
}
elseif (-not $logonAccount) {
    Warn "Could not read which account '$serviceName' logs on as, so the store folder '$storeFolder' was NOT locked rather than locked around a guess. Fix the lookup and re-run this script, or run install-darling.ps1."
}
else {
    try {
        Initialize-DarlingStoreFolder $storeFolder $logonAccount $operatorSid (Get-DarlingDataDirectory $configPath) $serviceName
        Good "Store folder '$storeFolder' is owned by Administrators and locked to SYSTEM, Administrators and the service account; the operator group can only traverse it."
    }
    catch {
        Warn "The store folder '$storeFolder' could not be secured: $($_.Exception.Message) The service will start, and refuses a store it cannot trust. Fix the folder, then re-run this script."
    }
}

# #6597: the registered path keeps its quotes, as the MSI registers it. The service is stopped here and the new build is in place.
if (Set-DarlingServiceQuotedPath $serviceName (Join-Path $InstallRoot $serviceExeName)) {
    Good "Service path re-registered in quotes."
}

Note "Starting '$serviceName'..."
Start-Service -Name $serviceName
try { (Get-Service -Name $serviceName).WaitForStatus('Running', [TimeSpan]::FromMinutes(2)) }
catch { Fail "'$serviceName' did not reach Running within two minutes. The new build IS in place - read %ProgramData%\PerformanceMonitorDarling\logs before rolling back." }

Good "Service is Running."
Note ""
Note "The install is NOT verified yet. 'Running' means the process started, not that it collects."
Note "About 10-15 minutes after the start, check these:"
Note "  1. Open the Viewer. Every monitored server shows Online, and the status bar shows 'Collection: Active'."
Note "  2. On each server card, Collectors shows OK and 'Failing: 0'. The status bar shows 'Collectors: N OK'. The web page has a Collection Health tab for each server."
Note "  3. The service log (%ProgramData%\PerformanceMonitorDarling\logs) has a line 'Postgres store ready (schema v<N>, ...)' from this start. N is the store version this build expects. If the line is missing, the store did not finish its update: read the log lines near the start."
Note "  If you connect to the store, you can also run SELECT MAX(version) FROM darling_schema_version. It returns the same N."

}
finally {
    # H1's staging copy (#4043 round-1 review): best-effort, and expected to be a no-op on the folder
    # -Source path, where $zipStagingFolder is never set. Administrators owns it and holds full control (see
    # Protect-DarlingStagingFolder), and this script is already running elevated, so nothing here needs the
    # icacls reset the Lock-DarlingInstallTree test cleanup uses for a non-admin test principal.
    if ($zipStagingFolder -and (Test-Path -LiteralPath $zipStagingFolder)) {
        Remove-Item -LiteralPath $zipStagingFolder -Recurse -Force -ErrorAction SilentlyContinue
    }
}
