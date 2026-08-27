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
  6. Copies the install root's FILES (not its subdirectories) to _rollback_manual_<stamp>. A re-run inside
     -BackupWindowMinutes reuses the existing backup instead of taking a second one, because a second one
     would be a copy of the half-extracted tree written over your only good copy.
  7. Prunes the rollback backups past -KeepRollbacks, each in its own handler.
  8. Lays the new build over the install root, retrying once on the transient file lock that has bitten this
     step before.
  9. Confirms darling.json is byte-identical to what it was.
 10. Names the files an EARLIER build shipped and this one does not, and removes them with
     -RemoveStaleFiles. An in-place upgrade is an OVERLAY: it writes what the new build ships and deletes
     nothing else, so a dropped dependency or a stranded runtimes\<rid>\lib\<tfm>\ subtree stays in the
     tree forever - in a directory .NET probes for assemblies. The authority is a manifest this script
     writes into the install root after every copy (#2529).
 11. Starts the service, waits for Running, and prints the post-install health check - which is a STAGE of
     the install, not a favour.

Every step is safe to re-run. That is not a nicety: steps 5 through 9 leave the service DOWN if anything
between them fails, so "run it again" has to be the correct advice, and the script says so at the point of
failure rather than leaving you to guess.

.PARAMETER Source
The new build: either the .zip as downloaded, or a folder you already extracted it into. Defaults to the
folder this script is in, which is what you get by extracting the new zip to a staging directory and running
ITS copy of this script.

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
overwrite the good pre-upgrade copy with a copy of the half-upgraded tree.

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
    [switch]$SkipStopGuard
)

$ErrorActionPreference = 'Stop'
$serviceName = 'PerformanceMonitor Darling'
$serviceExeName = 'PerformanceMonitor.Darling.Service.exe'
$configName = 'darling.json'
$manifestName = 'darling-install-manifest.txt'

function Fail([string]$message) { Write-Host "ERROR: $message" -ForegroundColor Red; exit 1 }
function Note([string]$message) { Write-Host $message }
function Good([string]$message) { Write-Host $message -ForegroundColor Green }
function Warn([string]$message) { Write-Host "WARNING: $message" -ForegroundColor Yellow }

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

# ============================ the install tree ============================

# True when two paths name the same directory. One spelling of path equality for the whole script: the
# comparison decides whether an upgrade is allowed to touch a tree, and two hand-rolled copies of it is
# the same "two spellings of one rule" that #2525 is a case study in.
#
# Separator from the runtime rather than a hardcoded backslash, for the same reason as the process filter
# below - a rule that gates a destructive operation should be verifiable off the platform it ships to.
function Test-DarlingSamePath([string]$left, [string]$right) {
    if ([string]::IsNullOrWhiteSpace($left) -or [string]::IsNullOrWhiteSpace($right)) { return $false }

    $sep = [IO.Path]::DirectorySeparatorChar
    $alt = [IO.Path]::AltDirectorySeparatorChar

    try {
        $l = [IO.Path]::GetFullPath($left).TrimEnd($sep, $alt)
        $r = [IO.Path]::GetFullPath($right).TrimEnd($sep, $alt)
    }
    catch {
        return $false
    }

    return $l.Equals($r, [StringComparison]::OrdinalIgnoreCase)
}

# Where the service is ACTUALLY installed, read from the registered ImagePath rather than guessed from
# where this script happens to be sitting. Returns $null when the service is not installed.
#
# The ImagePath is quoted when it contains spaces and 'C:\PerformanceMonitorDarling' usually does not, so
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
    if ($leaf.StartsWith('darling.json.bak-', [StringComparison]::OrdinalIgnoreCase)) { return $true }

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

if ([string]::IsNullOrWhiteSpace($InstallRoot)) {
    $InstallRoot = Get-DarlingInstallRootFromService $serviceName
    if ([string]::IsNullOrWhiteSpace($InstallRoot)) {
        Fail "The '$serviceName' service is not installed (or its ImagePath could not be read), so there is nothing here to upgrade. Install with install-darling.ps1, or pass -InstallRoot to point at the tree you mean."
    }
}

try { $InstallRoot = [IO.Path]::GetFullPath($InstallRoot).TrimEnd('\') }
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

# ============================ the source build ============================

if ([string]::IsNullOrWhiteSpace($Source)) {
    Fail "No -Source given and this script is not running from a file, so there is nothing to install from."
}

try { $Source = [IO.Path]::GetFullPath($Source).TrimEnd('\') }
catch { Fail "'$Source' is not a path this script can resolve." }

if (-not (Test-Path -LiteralPath $Source)) {
    Fail "The source '$Source' does not exist."
}

$sourceIsZip = (Test-Path -LiteralPath $Source -PathType Leaf) -and $Source.EndsWith('.zip', [StringComparison]::OrdinalIgnoreCase)
$sourceRoot = if ($sourceIsZip) { [IO.Path]::GetDirectoryName($Source) } else { $Source }

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
    Fail "This script is running from the install directory, so the upgrade would write the new build over the copy of itself that PowerShell is currently reading. Extract the new zip to a staging folder (e.g. C:\staging\<version>) and run ITS upgrade-darling.ps1 instead. To prune rollback backups from the installed copy, use -PruneOnly, which copies nothing."
}

# And the degenerate case the rule above does not cover: a source folder that IS the install directory,
# handed in from a script running somewhere else. Copying a tree over itself is not an upgrade.
if (-not $sourceIsZip -and (Test-DarlingSamePath $sourceRoot $InstallRoot)) {
    Fail "The source folder is the install directory itself, so there is nothing to upgrade from. Point -Source at the new build."
}

if ($sourceIsZip) {
    $expected = $Sha256

    if ([string]::IsNullOrWhiteSpace($expected)) {
        $sums = Join-Path ([IO.Path]::GetDirectoryName($Source)) 'SHA256SUMS.txt'
        if (Test-Path -LiteralPath $sums) {
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
            Fail "No SHA256 for '$Source' - pass -Sha256 <hash>, put SHA256SUMS.txt beside the zip, or say -SkipHashCheck. This overwrites the binaries of a running monitoring host; an unverified zip is not something to find out about afterwards."
        }
        Warn "Proceeding with an UNVERIFIED source zip (-SkipHashCheck)."
    }
    else {
        $actual = (Get-FileHash -LiteralPath $Source -Algorithm SHA256).Hash
        if (-not $actual.Equals($expected.Trim(), [StringComparison]::OrdinalIgnoreCase)) {
            Fail "SHA256 mismatch on '$Source'. Expected $expected, got $actual. Nothing has been stopped or copied."
        }
        Good "Source zip SHA256 verified."
    }
}
else {
    if (-not (Test-Path -LiteralPath (Join-Path $Source $serviceExeName))) {
        Fail "'$Source' does not hold $serviceExeName, so it is not an extracted Darling build."
    }
    Warn "The source is a folder, so this script cannot verify it - verify the zip's SHA256 before you extract it."
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
elseif (-not (Test-DarlingSamePath $registeredRoot $InstallRoot)) {
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
# A -Source FOLDER is copied wholesale: Copy-Item -Path "$Source\*" -Recurse -Force takes everything in it,
# unfiltered. A staging directory made from a live install therefore carries THAT install's manifest, and
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
$stillHolding = Get-DarlingProcessesUnderPath $InstallRoot
if ($stillHolding.Count -gt 0 -and -not $SkipStopGuard) {
    $names = @($stillHolding | ForEach-Object { "$($_.ProcessName) (pid $($_.Id))" })
    Note "The service is stopped, but processes are STILL running out of the install tree:"
    foreach ($name in $names) { Note "  $name" }
    Fail "Nothing has been copied, so the install is intact - but the service is now STOPPED. Either close these and re-run (safe, and it will reuse the backup it is about to take), or abandon the upgrade with: Start-Service '$serviceName'. Do NOT kill anything under $InstallRoot\pg-runtime - that is the bundled PostgreSQL and killing it takes the store down; give a postmaster that outlived the stop a few seconds and re-run."
}

$backups = Get-DarlingRollbackBackups $InstallRoot
$nowUtc = [datetime]::UtcNow

if (Test-DarlingRollbackBackupIsRecent $backups $BackupWindowMinutes $nowUtc) {
    Note ("Reusing the rollback backup {0}, taken {1:N0} minute(s) ago - this looks like a re-run of an interrupted upgrade, and a second backup now would copy a half-upgraded tree over the good one." -f $backups[0].Name, ($nowUtc - $backups[0].LastWriteTimeUtc).TotalMinutes)
}
else {
    $backupPath = Join-Path $InstallRoot (New-DarlingRollbackBackupName $nowUtc)
    Note "Backing up the install root's files to $backupPath ..."
    try {
        New-Item -ItemType Directory -Path $backupPath -Force | Out-Null
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
            Copy-Item -Path (Join-Path $Source '*') -Destination $InstallRoot -Recurse -Force
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

Note "Starting '$serviceName'..."
Start-Service -Name $serviceName
try { (Get-Service -Name $serviceName).WaitForStatus('Running', [TimeSpan]::FromMinutes(2)) }
catch { Fail "'$serviceName' did not reach Running within two minutes. The new build IS in place - read %ProgramData%\PerformanceMonitorDarling\logs before rolling back." }

Good "Service is Running."
Note ""
Note "The install is NOT verified yet. 'Running' means the process started, not that it collects."
Note "In 10-15 minutes, against the store, confirm all of:"
Note "  1. MAX(version) FROM darling_schema_version equals this build's expected schema rung."
Note "  2. COUNT(DISTINCT server_id) FROM collect.collection_log over the last 15 minutes equals the fleet size."
Note "  3. COUNT(DISTINCT collector_name) over the same window is in the mid-30s, not single digits."
Note "  4. Any non-SUCCESS rows since the restart are READ, not just counted - YIELDED is the lock-timeout guard working; anything else is a finding."
Note "  5. If this build added a collector or a migration, one targeted probe that ITS table moved."
