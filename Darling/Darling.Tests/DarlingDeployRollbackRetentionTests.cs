/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the deploy-time half of #2525: <c>upgrade-darling.ps1</c> takes the rollback backup, keeps a bounded
/// number of them, and prunes the rest — and its idea of what one is called stays identical to the
/// service's.
///
/// <para><b>What went wrong, and why it needs pinning on BOTH sides.</b> The install tree's backups were
/// being created by a procedure that lived in people's heads. Nothing pruned them, so a dogfood box reached
/// 46 directories and 5.48 GB, and the layout report — correctly, by its own rules — named every one of them
/// on every start. Retention alone does not close that: the boxes carrying the backlog got it before any
/// script pruned anything, and an upgrade does not delete directories the product did not create. So the
/// service learns the convention too, and the moment the script's spelling and the service's matcher drift
/// apart, the service goes quietly back to 46 warnings a start. Each of those warnings is TRUE, which is
/// exactly why nobody would notice.</para>
///
/// <para><b>These tests run the shipped script, not a description of it.</b> The functions are lifted out of
/// the <c>.ps1</c> by brace matching and executed under Windows PowerShell against planted directories —
/// the idiom <c>DarlingInstallLocationTests</c> uses to keep the installer's rules and the service's rules
/// answering alike. An <c>Assert.Contains</c> on the script's text could never see that a sort key changed,
/// and a sort key is precisely what decides which directories a recursive delete is handed.</para>
/// </summary>
public sealed class DarlingDeployRollbackRetentionTests
{
    private static string DeployScript => ReadRepoFile(Path.Combine("Darling", "tools", "upgrade-darling.ps1"));

    /// <summary>
    /// The shared case table. Both the script's predicate and the service's run over it, and both are also
    /// checked against the expectation — so a mutual mistake cannot pass itself off as agreement.
    ///
    /// <para>The interesting rows are the near misses. <c>rollback_manual_…</c> without the leading
    /// underscore and <c>_rollback_manual</c> without the trailing one are the two ways a hand-typed
    /// directory lands next to the convention without being in it; the bare prefix is a directory with no
    /// stamp, which no procedure produces, and it stays OUTSIDE the convention so it keeps being reported
    /// individually — a thing nobody can explain belongs in the report for things nobody can explain.</para>
    /// </summary>
    public static readonly IReadOnlyList<(string Name, bool Expected, string Because)> NameCases =
    [
        ("_rollback_manual_20260720-151203", true, "what the deploy script writes today"),
        ("_rollback_manual_20260720", true, "the date-only spelling the field box carries"),
        ("_ROLLBACK_MANUAL_20260720", true, "Windows paths are case-insensitive and so is the convention"),
        ("_rollback_manual_x", true, "prefix plus anything at all is inside the namespace"),
        ("_rollback_manual_", false, "the bare prefix is not a backup any procedure makes"),
        ("_rollback_manua", false, "a truncated prefix is not the prefix"),
        ("_rollback_manual", false, "no trailing underscore, so not the namespace"),
        ("rollback_manual_20260720", false, "no leading underscore, so not the namespace"),
        ("pg-runtime", false, "the product's own layout"),
        ("viewer", false, "the product's own layout"),
        ("logs", false, "an ordinary directory"),
        ("", false, "nothing at all"),
    ];

    /// <summary>
    /// THE drift pin. The script creates and DELETES these directories; the service recognises them so it
    /// can report the set on one line. Two independent spellings of one convention is how #2525 happened in
    /// the first place, and a disagreement is silent in both directions: the service starts warning 46 times
    /// again, or the script stops pruning a shape it made.
    /// </summary>
    [Fact]
    public void TheDeployScriptAndTheService_AgreeOnWhatARollbackBackupIsCalled()
    {
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(DeployScript, "Test-DarlingRollbackBackupName"));

        foreach (var (name, _, _) in NameCases)
        {
            /* Single-quoted so nothing in a directory name is expanded; the table holds no quote characters
               and the assertion below would fail loudly if one were ever added. */
            probe.AppendLine($"Test-DarlingRollbackBackupName '{name}'");
        }

        var answers = RunWindowsPowerShell(probe.ToString());
        Assert.Equal(NameCases.Count, answers.Count);

        var disagreements = new List<string>();
        for (var i = 0; i < NameCases.Count; i++)
        {
            var (name, expected, because) = NameCases[i];
            var script = string.Equals(answers[i], "True", StringComparison.OrdinalIgnoreCase);
            var service = DarlingRollbackBackups.IsRollbackBackup(name);

            if (script != expected || service != expected)
            {
                disagreements.Add(
                    $"'{name}': upgrade-darling.ps1 said {script}, the service said {service}, the table says {expected} ({because})");
            }
        }

        Assert.True(disagreements.Count == 0,
            "the deploy script and the service disagree about what a rollback backup is called (#2525):\n  "
            + string.Join("\n  ", disagreements));
    }

    /// <summary>
    /// Retention, run as shipped against a planted install tree: five backups, keep three, and the two
    /// OLDEST are the ones selected — chosen by last-write time, not by the stamps in the names.
    ///
    /// <para>The fixture makes the two orderings disagree on purpose. The names ascend while the timestamps
    /// descend, so a prune that sorted on the name would select the two directories this test requires it to
    /// KEEP. A fixture where both orderings agree cannot tell the two rules apart, and the rule that matters
    /// is the one the filesystem can vouch for: #2525's box carries more than one stamp spelling, and a
    /// deploy from a box whose clock or naming changed must still prune the genuinely old copies.</para>
    /// </summary>
    [Fact]
    public void TheDeployScript_KeepsTheNewestN_AndSelectsOnlyTheOldestRestToPrune()
    {
        var root = Directory.CreateTempSubdirectory("darling-prune-order-");
        try
        {
            /* Names ascend, write times DESCEND: '…-a' is the newest directory on disk. */
            var stamps = new[] { "20260819-a", "20260815-b", "20260810-c", "20260801-d", "20260720-e" };
            for (var i = 0; i < stamps.Length; i++)
            {
                PlantBackup(root.FullName, DarlingRollbackBackups.Prefix + stamps[i], new DateTime(2026, 8, 19, 0, 0, 0, DateTimeKind.Utc).AddDays(-i));
            }

            /* Neighbours that must never be eligible: the product's own layout, an unrelated directory, and
               the bare prefix. A delete loose enough to take one of these is the failure that costs an
               install, not a report. */
            Directory.CreateDirectory(Path.Combine(root.FullName, "pg-runtime"));
            Directory.CreateDirectory(Path.Combine(root.FullName, "viewer"));
            Directory.CreateDirectory(Path.Combine(root.FullName, "operator-notes"));
            Directory.CreateDirectory(Path.Combine(root.FullName, DarlingRollbackBackups.Prefix));

            var selected = RunPrune(root.FullName, DarlingRollbackBackups.DefaultRetained);

            Assert.Equal(
                [DarlingRollbackBackups.Prefix + "20260801-d", DarlingRollbackBackups.Prefix + "20260720-e"],
                selected);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// The floor. This function's output goes straight into a recursive delete inside an install directory,
    /// so the one input that must not be expressible is the one that selects everything — a zero from a
    /// typo, a variable that never got set, a future caller doing arithmetic. Asking for zero keeps one.
    /// </summary>
    [Fact]
    public void TheDeployScript_NeverSelectsEveryBackup_EvenWhenAskedToKeepNone()
    {
        var root = Directory.CreateTempSubdirectory("darling-prune-floor-");
        try
        {
            for (var i = 0; i < 3; i++)
            {
                PlantBackup(root.FullName, DarlingRollbackBackups.Prefix + $"2026080{i}-000000", new DateTime(2026, 8, 19, 0, 0, 0, DateTimeKind.Utc).AddDays(-i));
            }

            var selected = RunPrune(root.FullName, 0);

            Assert.Equal(2, selected.Count);
            Assert.DoesNotContain(DarlingRollbackBackups.Prefix + "20260800-000000", selected);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// The re-run rule, and the defect a live run of it found.
    ///
    /// <para>Step 8 of an upgrade dies on a locked DLL; the operator re-runs, which is the right thing to
    /// do; without this rule the re-run backs up the HALF-EXTRACTED tree, and the newest rollback copy — the
    /// one anyone would reach for — becomes a mixture of two builds. So a backup inside the window is reused
    /// rather than replaced.</para>
    ///
    /// <para>The third case is why this is a test and not a comment. A backup whose timestamp is in the
    /// FUTURE — a clock that stepped back, a directory restored with its metadata — gives a NEGATIVE elapsed
    /// time, which is less than any window, which read as "recent": the upgrade would have skipped taking a
    /// backup at all. The two errors are not symmetric. A spare backup costs 120 MB the next prune reclaims;
    /// a missing one costs the rollback the whole procedure exists to provide.</para>
    /// </summary>
    [Fact]
    public void TheDeployScript_ReusesARecentBackup_ButNeverOneDatedInTheFuture()
    {
        var root = Directory.CreateTempSubdirectory("darling-prune-window-");
        try
        {
            PlantBackup(root.FullName, DarlingRollbackBackups.Prefix + "20260819-000000", new DateTime(2026, 8, 19, 0, 0, 0, DateTimeKind.Utc));

            var probe = new StringBuilder();
            probe.AppendLine(ExtractFunction(DeployScript, "Test-DarlingRollbackBackupName"));
            probe.AppendLine(ExtractFunction(DeployScript, "Get-DarlingRollbackBackups"));
            probe.AppendLine(ExtractFunction(DeployScript, "Test-DarlingRollbackBackupIsRecent"));
            probe.AppendLine($"$backups = Get-DarlingRollbackBackups '{root.FullName}'");

            /* Thirty minutes after the backup: a re-run of an interrupted upgrade. */
            probe.AppendLine("Test-DarlingRollbackBackupIsRecent $backups 60 ([datetime]::new(2026,8,19,0,30,0,[DateTimeKind]::Utc))");
            /* A day later: a new deploy, which must take its own backup. */
            probe.AppendLine("Test-DarlingRollbackBackupIsRecent $backups 60 ([datetime]::new(2026,8,20,0,30,0,[DateTimeKind]::Utc))");
            /* Before the backup exists: the clock-skew case. */
            probe.AppendLine("Test-DarlingRollbackBackupIsRecent $backups 60 ([datetime]::new(2026,8,18,0,30,0,[DateTimeKind]::Utc))");

            var answers = RunWindowsPowerShell(probe.ToString());

            Assert.Equal(["True", "False", "False"], answers);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// The script must never kill a process under the install tree, and this is the pin that says so out
    /// loud rather than trusting that nobody adds one.
    ///
    /// <para>It is an easy and terrible fix. The copy fails on a locked file, so a future edit sweeps
    /// "everything running under the install directory" — and the bundled PostgreSQL runs out of
    /// <c>pg-runtime</c> under that very directory, so the first thing such a sweep does is take the
    /// monitoring store down. Stopping the service stops the store properly; anything still holding the tree
    /// after that is a person's own session, and the guard's job is to name it and stop.</para>
    /// </summary>
    [Fact]
    public void TheDeployScript_NeverKillsAProcessUnderTheInstallTree()
    {
        var script = DeployScript;
        string[] killers = ["Stop-Process", "taskkill", ".Kill(", "CloseMainWindow"];

        var found = new List<string>();
        foreach (var killer in killers)
        {
            if (script.Contains(killer, StringComparison.OrdinalIgnoreCase))
            {
                found.Add(killer);
            }
        }

        Assert.True(found.Count == 0,
            "upgrade-darling.ps1 kills processes (" + string.Join(", ", found)
            + "). It must not: the bundled PostgreSQL runs from <install>\\pg-runtime and a sweep of the install tree takes the store down with it (#2525).");
    }

    /// <summary>
    /// The self-overwrite refusal. This script ships inside the zip, so it also lives in the install root —
    /// and an upgrade run from the INSTALLED copy would write the new build over the .ps1 PowerShell is
    /// reading. Best case the copy dies on the lock and leaves a half-written tree with the service
    /// stopped, which is the exact disaster the whole procedure is arranged to avoid.
    ///
    /// <para><b>The condition is where the SCRIPT is, and that is the whole test.</b> The first spelling
    /// asked whether the SOURCE folder was the install directory, which a zip downloaded INTO the install
    /// directory — the obvious place to put it — walks straight past: its source root equals the install
    /// root and the rule exempted zips outright. Keying on <c>$PSScriptRoot</c> asks the question the
    /// hazard actually poses, which is whether the file being executed is about to be overwritten. So this
    /// pin requires the <c>$PSScriptRoot</c> form specifically; a rule about the source would satisfy a
    /// looser assertion while leaving the hole open.</para>
    ///
    /// <para>The refusal is scoped to the copy. <c>-PruneOnly</c> writes no binaries, so the installed copy
    /// is the right thing to run for it — which matters because the installed copy is the one the service's
    /// report names.</para>
    /// </summary>
    [Fact]
    public void TheDeployScript_RefusesToRunTheCopyThatLivesInTheInstallDirectory()
    {
        var script = DeployScript;

        Assert.Contains("$runningFrom = $PSScriptRoot", script, StringComparison.Ordinal);
        Assert.Contains("if (Test-DarlingSamePath $runningFrom $InstallRoot) {", script, StringComparison.Ordinal);

        /* The refusal has to happen before anything is stopped or copied, or it is not a refusal. */
        var refusal = script.IndexOf("would write the new build over the copy of itself", StringComparison.Ordinal);
        var stop = script.IndexOf("Stop-Service -Name $serviceName", StringComparison.Ordinal);
        Assert.True(refusal >= 0, "upgrade-darling.ps1 no longer refuses to run from the install directory (#2525)");
        Assert.True(stop >= 0, "upgrade-darling.ps1 no longer stops the service");
        Assert.True(refusal < stop, "the self-overwrite refusal must come BEFORE the service is stopped (#2525)");
    }

    /// <summary>
    /// Each backup is deleted inside its OWN handler, which is #1775's lesson paid for once already: the
    /// store's retained-copy sweep had its try/catch outside the loop, so one directory an antivirus scan
    /// still held abandoned the sweep for every other directory too — and kept abandoning it for as long as
    /// the condition lasted, which is how nothing aged out at all.
    ///
    /// <para>Structural rather than textual: the <c>try</c> has to sit INSIDE the <c>foreach</c>, and a
    /// version that wrapped the loop would still contain both keywords.</para>
    ///
    /// <para><b>And the reporting has to sit OUTSIDE the try, which is the second half of this test and a
    /// defect a live run actually produced.</b> With the write-up inside the guarded block, anything that
    /// failed while composing a line of TEXT was caught by the delete's handler and counted as a delete
    /// failure — the run reported two directories removed and two failures, for the same two directories,
    /// while the disk agreed with neither. The returned counts drive the exit code and the "re-running is
    /// safe" advice, so a log line must not be able to make a successful delete look failed.</para>
    /// </summary>
    [Fact]
    public void TheDeployScript_PrunesEachBackupInItsOwnHandler_AndReportsOutsideIt()
    {
        var body = ExtractFunction(DeployScript, "Remove-DarlingRollbackBackups");

        var loop = body.IndexOf("foreach ($backup in", StringComparison.Ordinal);
        var attempt = body.IndexOf("try {", StringComparison.Ordinal);
        var handler = body.IndexOf("catch {", StringComparison.Ordinal);

        Assert.True(loop >= 0, "Remove-DarlingRollbackBackups no longer loops over the backups");
        Assert.True(attempt > loop && handler > attempt,
            "Remove-DarlingRollbackBackups must handle failures INSIDE its loop — one directory that cannot be deleted must cost that directory and nothing else (#1775).");

        /* The guarded block is what sits between 'try {' and 'catch {': the delete belongs in it, the
           bookkeeping and the operator-facing lines do not. */
        var guarded = body.Substring(attempt, handler - attempt);

        Assert.Contains("Remove-Item", guarded, StringComparison.Ordinal);
        Assert.DoesNotContain("Note (", guarded, StringComparison.Ordinal);
        Assert.DoesNotContain("Warn (", guarded, StringComparison.Ordinal);
        Assert.DoesNotContain("$removed++", guarded, StringComparison.Ordinal);
        Assert.DoesNotContain("$failures +=", guarded, StringComparison.Ordinal);
    }

    /// <summary>
    /// The service quotes a retention number in its report and the script implements one. Advertising a
    /// retention the script does not keep is worse than saying nothing: the operator reads "keeps the newest
    /// 3", runs it, and gets a different answer with no way to know which of the two was lying.
    /// </summary>
    [Fact]
    public void TheServiceAndTheDeployScript_AgreeOnHowManyBackupsAreKept()
    {
        var match = Regex.Match(DeployScript, @"\$KeepRollbacks\s*=\s*(\d+)");
        Assert.True(match.Success, "upgrade-darling.ps1 no longer declares a -KeepRollbacks default (#2525)");

        Assert.Equal(DarlingRollbackBackups.DefaultRetained, int.Parse(match.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// The report tells an operator to run a command; the command has to be one the script accepts, and the
    /// script has to be one that is ON THE BOX.
    ///
    /// <para>The second half is the one that would rot silently. <c>upgrade-darling.ps1</c> reaches an
    /// install only because the service csproj copies it into the publish output beside the exe, the same
    /// way <c>install-darling.ps1</c> gets there — no packaging workflow mentions either by name. Drop that
    /// one line and the service starts advising operators to run a file that does not exist, which is a
    /// worse outcome than the 46 warnings, because it wastes the credibility the single line just bought.</para>
    /// </summary>
    [Fact]
    public void TheServicesPruneAdvice_NamesASwitchTheDeployScriptAccepts_OnAScriptThatShips()
    {
        Assert.Contains("upgrade-darling.ps1", DarlingRollbackBackups.PruneCommand, StringComparison.Ordinal);
        Assert.Contains("-PruneOnly", DarlingRollbackBackups.PruneCommand, StringComparison.Ordinal);

        Assert.Contains("[switch]$PruneOnly", DeployScript, StringComparison.Ordinal);

        var csproj = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "PerformanceMonitor.Darling.Service.csproj"));
        Assert.Contains("upgrade-darling.ps1", csproj, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>-PruneOnly</c> must not stop the service. It is the command an operator with a 5 GB backlog is
    /// told to run, the backups are inert copies nothing has open, and requiring an outage to clean up after
    /// our own deploy procedure is how the advice goes unfollowed.
    /// </summary>
    [Fact]
    public void TheDeployScript_PrunesWithoutStoppingTheService()
    {
        var script = DeployScript;

        var pruneExit = script.IndexOf("if ($ListRollbacks -or $PruneOnly)", StringComparison.Ordinal);
        var stop = script.IndexOf("Stop-Service -Name $serviceName", StringComparison.Ordinal);

        Assert.True(pruneExit >= 0, "upgrade-darling.ps1 no longer has a prune-only path (#2525)");
        Assert.True(stop > pruneExit,
            "-PruneOnly must return before the service is stopped — pruning backups is not a reason to take a monitoring host down (#2525).");
    }

    /// <summary>
    /// The stop guard runs TWICE, and each run has to be on the correct side of <c>Stop-Service</c>.
    ///
    /// <para><b>This is the ordering bug review caught, and it made the script refuse every real
    /// upgrade.</b> There was one unfiltered check and it ran BEFORE the service was stopped — so it found
    /// the service's own <c>PerformanceMonitor.Darling.Service.exe</c>, which lives in the install root and
    /// is running by definition on any install worth upgrading, and failed. A guard that fails closed on the
    /// happy path is not a strict guard, it is a guard that teaches people to pass
    /// <c>-SkipStopGuard</c>, and then it guards nothing at all. The same disease as #2525's 46 warnings,
    /// caught one layer earlier.</para>
    ///
    /// <para>So: phase one runs before the stop and filters out what the stop will clear — the service exe
    /// and the bundled PostgreSQL under <c>pg-runtime</c> — which leaves an operator's own <c>psql.exe</c>
    /// or a left-open Viewer, caught at the cost of a re-run and no outage. Phase two runs after the stop
    /// with no exclusions, where anything remaining is genuinely interesting. Neither phase can see the
    /// other's cases, which is why there are two, and the ordering IS the correctness — hence a test about
    /// ordering rather than about text.</para>
    /// </summary>
    [Fact]
    public void TheDeployScript_ChecksForHoldersBothBeforeAndAfterStoppingTheService()
    {
        var script = DeployScript;

        var phaseOne = script.IndexOf("$holders = @(Get-DarlingProcessesUnderPath $InstallRoot |", StringComparison.Ordinal);
        var stop = script.IndexOf("Stop-Service -Name $serviceName", StringComparison.Ordinal);
        var phaseTwo = script.IndexOf("$stillHolding = Get-DarlingProcessesUnderPath $InstallRoot", StringComparison.Ordinal);

        Assert.True(phaseOne >= 0, "upgrade-darling.ps1 no longer runs a pre-stop holder check (#2525)");
        Assert.True(stop >= 0, "upgrade-darling.ps1 no longer stops the service");
        Assert.True(phaseTwo >= 0, "upgrade-darling.ps1 no longer re-checks for holders after the stop (#2525)");

        Assert.True(phaseOne < stop, "the pre-stop holder check must run BEFORE Stop-Service, or it costs an outage it did not need to");
        Assert.True(phaseTwo > stop, "the unfiltered holder check must run AFTER Stop-Service — before it, the service's own exe is always holding the tree and the guard refuses every upgrade");

        /* Phase one MUST filter, phase two MUST NOT. Getting either backwards reproduces the bug. */
        var phaseOneCall = script.Substring(phaseOne, script.IndexOf("if ($holders.Count", StringComparison.Ordinal) - phaseOne);
        Assert.Contains("Test-DarlingProcessStopsWithTheService", phaseOneCall, StringComparison.Ordinal);

        var phaseTwoCall = script.Substring(phaseTwo, script.IndexOf("if ($stillHolding.Count", StringComparison.Ordinal) - phaseTwo);
        Assert.DoesNotContain("Test-DarlingProcessStopsWithTheService", phaseTwoCall, StringComparison.Ordinal);
    }

    /// <summary>
    /// What the pre-stop filter does and does not excuse, run as shipped.
    ///
    /// <para>The service exe and the bundled PostgreSQL are excused because the stop clears them. The
    /// <b>Viewer is not</b>, and that is the case worth pinning: we ship it, it lives under the install
    /// root, and it is the intuitive thing to add to an exclusion list — but it is a separate process the
    /// service does not own, stopping the service does not close it, and it holds <c>viewer\*.dll</c>
    /// exactly as hard as any other application would. Excusing it would let the copy walk into the locked
    /// file this guard exists to prevent.</para>
    /// </summary>
    [Fact]
    public void TheDeployScript_ExcusesOnlyWhatTheServiceStopActuallyCloses()
    {
        const string root = @"C:\PerformanceMonitorDarling";

        (string Path, bool Excused, string Because)[] cases =
        [
            (root + @"\PerformanceMonitor.Darling.Service.exe", true, "the service being upgraded — the stop clears it"),
            (root + @"\pg-runtime\pgsql\bin\postgres.exe", true, "the bundled store — the service stops it"),
            (root + @"\pg-runtime\pgsql\bin\psql.exe", true, "under pg-runtime, so the same"),
            (root + @"\viewer\PerformanceMonitor.Darling.Viewer.exe", false, "ours, but the stop does not close it and it holds viewer\\"),
            (@"C:\tools\psql.exe", false, "an operator's own, not under the install root at all"),
            (root + @"\PerformanceMonitor.Darling.Service.Extra.exe", false, "not the service exe, however similar the name"),
            ("", false, "nothing at all"),
        ];

        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(DeployScript, "Test-DarlingProcessStopsWithTheService"));
        foreach (var (path, _, _) in cases)
        {
            probe.AppendLine($"Test-DarlingProcessStopsWithTheService '{path}' '{root}'");
        }

        var answers = RunWindowsPowerShell(probe.ToString());
        Assert.Equal(cases.Length, answers.Count);

        var wrong = new List<string>();
        for (var i = 0; i < cases.Length; i++)
        {
            var (path, excused, because) = cases[i];
            if (!string.Equals(answers[i], excused ? "True" : "False", StringComparison.OrdinalIgnoreCase))
            {
                wrong.Add($"'{path}': said {answers[i]}, expected {excused} ({because})");
            }
        }

        Assert.True(wrong.Count == 0, "the pre-stop filter excuses the wrong processes (#2525):\n  " + string.Join("\n  ", wrong));
    }

    /// <summary>
    /// The copy path requires the service to be REGISTERED, and refuses before anything is stopped or
    /// copied when it is not.
    ///
    /// <para>The auto-resolve path cannot get here without one — it reads the install root out of the
    /// registered ImagePath — but an explicit <c>-InstallRoot</c> skips that entirely. A tree holding the
    /// binaries of a service that was renamed, removed, or never registered would otherwise pass the stop
    /// guard, take a backup, prune, complete the copy, and only fall over at <c>Start-Service</c> with a raw
    /// terminating error rather than one of this script's own messages — having already changed the tree.
    /// The refusal has to sit after the <c>-PruneOnly</c> exit, though: reclaiming disk from a tree whose
    /// service is gone is reasonable and copies nothing.</para>
    /// </summary>
    [Fact]
    public void TheDeployScript_RefusesAnUnregisteredService_BeforeItStopsOrCopiesAnything()
    {
        var script = DeployScript;

        var check = script.IndexOf("if (-not (Get-Service -Name $serviceName -ErrorAction SilentlyContinue)) {", StringComparison.Ordinal);
        Assert.True(check >= 0, "upgrade-darling.ps1 no longer refuses an unregistered service on the copy path (#2525)");

        var pruneExit = script.IndexOf("if ($ListRollbacks -or $PruneOnly)", StringComparison.Ordinal);
        var stopGuard = script.IndexOf("$holders = @(Get-DarlingProcessesUnderPath $InstallRoot |", StringComparison.Ordinal);
        var stop = script.IndexOf("Stop-Service -Name $serviceName", StringComparison.Ordinal);
        var copy = script.IndexOf("Laying the new build over", StringComparison.Ordinal);

        Assert.True(check > pruneExit, "-PruneOnly must not require a registered service — it copies nothing");
        Assert.True(check < stopGuard && check < stop && check < copy,
            "the unregistered-service refusal must come before the stop guard, the stop, and the copy — after any of them it has already cost something (#2525)");
    }

    /// <summary>
    /// The service must not only exist — it must be the one that RUNS FROM the directory about to be
    /// overwritten.
    ///
    /// <para><b>The failure this prevents is a clean, successful-looking run that upgraded nothing.</b>
    /// <c>Stop-Service</c> and <c>Start-Service</c> act on the service by NAME, never by path. So an
    /// <c>-InstallRoot</c> aimed at a stale copy of the tree — an old runbook, a paste from another box, a
    /// leftover directory that still holds the exe and therefore satisfies every other precondition — stops
    /// the REAL service (a real outage on a monitoring host), lays the new build down in a directory nothing
    /// reads, restarts the real service on its old untouched binaries, finds <c>darling.json</c> unchanged
    /// because it was never touched, and reports success from end to end. Every check passes and the
    /// operator believes they upgraded.</para>
    ///
    /// <para>Checked unconditionally rather than only when <c>-InstallRoot</c> was passed: when it was not,
    /// the two are equal by construction and this costs a registry read, and a check gated on the caller's
    /// argument is absent for exactly the caller who needs it.</para>
    /// </summary>
    [Fact]
    public void TheDeployScript_RefusesAnInstallRootTheServiceDoesNotRunFrom()
    {
        var script = DeployScript;

        var crossCheck = script.IndexOf("$registeredRoot = Get-DarlingInstallRootFromService $serviceName", StringComparison.Ordinal);
        Assert.True(crossCheck >= 0, "upgrade-darling.ps1 no longer cross-checks -InstallRoot against the registered ImagePath (#2525)");
        Assert.Contains("elseif (-not (Test-DarlingSamePath $registeredRoot $InstallRoot)) {", script, StringComparison.Ordinal);

        /* Unconditional. A `if ($PSBoundParameters.ContainsKey('InstallRoot'))` around this would make it
           absent for the caller it exists to protect. */
        Assert.DoesNotContain("PSBoundParameters", script, StringComparison.Ordinal);

        var stopGuard = script.IndexOf("$holders = @(Get-DarlingProcessesUnderPath $InstallRoot |", StringComparison.Ordinal);
        var stop = script.IndexOf("Stop-Service -Name $serviceName", StringComparison.Ordinal);
        var copy = script.IndexOf("Laying the new build over", StringComparison.Ordinal);

        Assert.True(crossCheck < stopGuard && crossCheck < stop && crossCheck < copy,
            "the wrong-install-root refusal must come before the stop guard, the stop and the copy — after the stop it has already cost an outage (#2525)");
    }

    /// <summary>
    /// Path equality has ONE spelling in this script, and it is the one that gates every destructive step:
    /// the self-overwrite refusal, the source-is-the-install-directory refusal, and the wrong-install-root
    /// refusal all ask <c>Test-DarlingSamePath</c>. Two hand-rolled copies of a comparison that decides
    /// whether an upgrade may touch a tree is the same failure this whole PR is a case study in.
    ///
    /// <para>Run as shipped over a table, with the platform's own separator so the same cases execute here
    /// and on Windows. The trailing-separator and case rows are the ones that matter: a registered
    /// <c>ImagePath</c> and a hand-typed <c>-InstallRoot</c> routinely differ by exactly those, and a
    /// comparison that called them different would refuse every correct upgrade.</para>
    /// </summary>
    [Fact]
    public void TheDeployScript_ComparesPathsOneWay_AndToleratesTheWaysPeopleTypeThem()
    {
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(DeployScript, "Test-DarlingSamePath"));
        probe.AppendLine("$sep = [IO.Path]::DirectorySeparatorChar");
        probe.AppendLine("$root = [IO.Path]::GetFullPath((Join-Path ([IO.Path]::GetTempPath()) 'PerformanceMonitorDarling')).TrimEnd($sep)");

        /* same, same + trailing separator, same in another case, a sibling, a child, and the empty case */
        probe.AppendLine("Test-DarlingSamePath $root $root");
        probe.AppendLine("Test-DarlingSamePath $root ($root + $sep)");
        probe.AppendLine("Test-DarlingSamePath $root $root.ToUpperInvariant()");
        probe.AppendLine("Test-DarlingSamePath $root ($root + 'Old')");
        probe.AppendLine("Test-DarlingSamePath $root ($root + $sep + 'viewer')");
        probe.AppendLine("Test-DarlingSamePath $root ''");

        var answers = RunWindowsPowerShell(probe.ToString());

        Assert.Equal(["True", "True", "True", "False", "False", "False"], answers);
    }

    /// <summary>
    /// A rollback backup holds the install root's FILES and not its subdirectories, so the guidance after a
    /// failed copy has to name both halves of a revert: re-extract the previous zip (for <c>viewer\</c>,
    /// <c>wwwroot\</c>, <c>runtimes\</c>) and then restore the backup's files over it.
    ///
    /// <para>Backing those directories up instead was rejected — it multiplies exactly the retained disk
    /// #2525 is about, to cover a case whose real fix is a zip the operator still has, and it would still
    /// not cover <c>pg-runtime</c>. That makes the accuracy of this message the whole mitigation, which is
    /// why it is pinned rather than trusted.</para>
    /// </summary>
    [Fact]
    public void TheDeployScript_TellsTheTruthAboutWhatARollbackBackupDoesNotHold()
    {
        var script = DeployScript;

        var failure = script.IndexOf("The copy failed twice", StringComparison.Ordinal);
        Assert.True(failure >= 0, "upgrade-darling.ps1 no longer reports a double copy failure (#2525)");

        var message = script.Substring(failure, Math.Min(1200, script.Length - failure));

        Assert.Contains("re-extract the PREVIOUS version's zip", message, StringComparison.Ordinal);
        Assert.Contains("which the backup does not hold", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The prune enumerates the install root and tests the REAL directory names rather than handing a
    /// wildcard to the filesystem.
    ///
    /// <para><c>Get-ChildItem -Filter</c> matches a directory's Windows 8.3 SHORT name as well as its real
    /// one, so a pattern can select a directory whose real name looks nothing like it — and this selection
    /// feeds a recursive delete. <c>DarlingStoreUpgrade</c> handles that by re-checking the real name after
    /// the wildcard; not using a wildcard at all is the same guard with the trap removed.</para>
    /// </summary>
    [Fact]
    public void TheDeployScript_NeverHandsAWildcardToTheFilesystemWhenChoosingWhatToDelete()
    {
        var body = ExtractFunction(DeployScript, "Get-DarlingRollbackBackups");

        Assert.DoesNotContain("-Filter", body, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Test-DarlingRollbackBackupName $_.Name", body, StringComparison.Ordinal);
    }

    /// <summary>Plants a rollback backup with a controlled last-write time, so ordering is the fixture's
    /// decision rather than the speed of the loop that created it.</summary>
    private static void PlantBackup(string root, string name, DateTime lastWriteUtc)
    {
        var directory = Path.Combine(root, name);
        Directory.CreateDirectory(directory);
        File.WriteAllText(Path.Combine(directory, "PerformanceMonitor.Darling.Service.exe"), new string('x', 64));
        Directory.SetLastWriteTimeUtc(directory, lastWriteUtc);
    }

    /// <summary>
    /// Runs the shipped <c>Get-</c> and <c>Select-</c> pair over <paramref name="root"/> and returns the
    /// names selected for pruning.
    ///
    /// <para>The COMPOSITION is what runs, not either function alone: the selection keeps a prefix of the
    /// list the enumeration produced, so the ordering and the cut are one rule and testing them apart would
    /// leave the join — which is where a delete's scope actually comes from — unexercised.</para>
    /// </summary>
    private static List<string> RunPrune(string root, int keep)
    {
        var probe = new StringBuilder();
        probe.AppendLine(ExtractFunction(DeployScript, "Test-DarlingRollbackBackupName"));
        probe.AppendLine(ExtractFunction(DeployScript, "Get-DarlingRollbackBackups"));
        probe.AppendLine(ExtractFunction(DeployScript, "Select-DarlingRollbackBackupsToPrune"));
        probe.AppendLine($"$backups = Get-DarlingRollbackBackups '{root}'");
        probe.AppendLine($"$prunable = Select-DarlingRollbackBackupsToPrune $backups {keep}");
        /* Parenthesised before anything downstream: `$x | ForEach-Object { } -join ','` binds -join to
           ForEach-Object and throws, which is the formatting bug this repo has already lost a deploy to. */
        probe.AppendLine("foreach ($p in @($prunable)) { $p.Name }");

        return RunWindowsPowerShell(probe.ToString());
    }

    /// <summary>Runs <paramref name="script"/> under Windows PowerShell 5.1 and returns its non-empty
    /// output lines. Written to a temp file rather than passed with -Command: the script under test is a set
    /// of whole function bodies, and quoting those through a command line fails for reasons that have
    /// nothing to do with what is being tested. Same idiom as <c>DarlingInstallLocationTests</c>.</summary>
    private static List<string> RunWindowsPowerShell(string script)
    {
        var path = Path.Combine(Path.GetTempPath(), $"darling-2525-{Guid.NewGuid():N}.ps1");
        File.WriteAllText(path, script);
        try
        {
            using var process = Process.Start(new ProcessStartInfo("powershell.exe", $"-NoProfile -ExecutionPolicy Bypass -File \"{path}\"")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true,
            });
            Assert.NotNull(process);

            /* BOTH streams drained CONCURRENTLY. Reading stdout to the end and then stderr is the classic
               redirect deadlock: a probe that writes enough to stderr to fill the OS pipe buffer blocks the
               child, which never closes stdout, which blocks this thread forever — and WaitForExit's
               timeout sits behind the block, so it never fires. The result is a CI job that HANGS rather
               than fails. Raised in review on #2529 against the sibling file, which carries the same
               idiom; fixed in both, because the same defect twice is a category. */
            var stdoutTask = process!.StandardOutput.ReadToEndAsync();
            var stderrTask = process.StandardError.ReadToEndAsync();

            var exited = process.WaitForExit(60_000);
            if (!exited)
            {
                try { process.Kill(entireProcessTree: true); }
                catch (Exception ex) when (ex is InvalidOperationException or NotSupportedException or System.ComponentModel.Win32Exception)
                {
                    /* Already gone between the timeout and the kill. Nothing to do. */
                }
            }

            var stdout = stdoutTask.GetAwaiter().GetResult();
            var stderr = stderrTask.GetAwaiter().GetResult();

            Assert.True(exited, $"powershell.exe did not exit within 60 seconds running the extracted functions. Output so far:\n{stdout}\n{stderr}");
            Assert.True(string.IsNullOrWhiteSpace(stderr), $"powershell.exe reported an error running the extracted functions:\n{stderr}");

            var lines = new List<string>();
            foreach (var line in stdout.Split('\n'))
            {
                var trimmed = line.Trim();
                if (trimmed.Length > 0) { lines.Add(trimmed); }
            }

            return lines;
        }
        finally
        {
            try { File.Delete(path); } catch (IOException) { /* best-effort */ }
        }
    }

    /// <summary>Returns the full <c>function NAME(...) { ... }</c> definition text from the script —
    /// signature included, so the extracted copy takes its parameters the way the shipped one does.</summary>
    /// <summary>
    /// #2574: the rollback backup must RESTRICT the secrets it copies. `darling.json` holds every monitored
    /// server's `encryptedPassword` plus the MCP and web tokens, DPAPI LocalMachine with an entropy constant
    /// published in this open-source repo — so read access to a copy is decrypt access to all of it, which is
    /// #1647's whole finding and why the live file is hardened.
    ///
    /// <para><c>Copy-Item</c> into a new directory takes the DESTINATION's inherited DACL, not the source
    /// file's. Measured on a real install root: <c>BUILTIN\Users : ReadAndExecute</c>, the inherited-from-C:\
    /// DACL #1647 called out. So every retained backup was a config readable by any local user on a box whose
    /// live config is locked down — the hardening and the copying disagreed, and only the copying ran.</para>
    ///
    /// <para>Asserted against the SOURCE rather than by running it, like the rest of this class: the script is
    /// Windows-only and this suite has to say something useful on any host.</para>
    /// </summary>
    [Fact]
    public void TheRollbackBackup_RestrictsTheConfigCopyItTakes()
    {
        var script = DeployScript;

        var copy = script.IndexOf("Copy-Item -Destination $backupPath", StringComparison.Ordinal);
        Assert.True(copy > 0, "the rollback backup no longer copies the install root's files — this pin needs rewriting");

        var harden = script.IndexOf("Set-Acl -LiteralPath $copy.FullName", StringComparison.Ordinal);
        Assert.True(harden > copy, "the rollback backup does not restrict the darling.json copy it just took (#2574)");

        /* It must select the secrets, not everything: a backup is ~120 MB of binaries and re-ACLing all of
           them would be slow and would lock an operator out of files that are not secret. */
        Assert.Contains("$_.Name -ieq 'darling.json'", script, StringComparison.Ordinal);
        Assert.Contains("$_.Extension -ieq '.dpapi'", script, StringComparison.Ordinal);

        /* Protected from inheritance, or the install root's Users ACE flows straight back in and the
           hardening achieves nothing. */
        Assert.Contains("SetAccessRuleProtection($true, $false)", script, StringComparison.Ordinal);

        /* SYSTEM and Administrators only. Deliberately NO Interactive grant, unlike the live darling.json:
           the Viewer and the CLI verbs read the live file, and nothing reads a backup except a human
           recovering, who is an administrator by then. install-darling.ps1 draws the same line for .bak-*. */
        Assert.DoesNotContain("InteractiveSid", script[harden..Math.Min(harden + 1200, script.Length)], StringComparison.Ordinal);
    }

    private static string ExtractFunction(string script, string name)
    {
        var start = script.IndexOf("function " + name, StringComparison.Ordinal);
        Assert.True(start >= 0, $"upgrade-darling.ps1 no longer defines {name} (#2525)");

        var open = script.IndexOf('{', start);
        Assert.True(open >= 0, $"expected an opening brace after {name}");

        var depth = 0;
        for (var i = open; i < script.Length; i++)
        {
            if (script[i] == '{') { depth++; }
            else if (script[i] == '}')
            {
                depth--;
                if (depth == 0)
                {
                    return script.Substring(start, i - start + 1);
                }
            }
        }

        Assert.Fail($"unbalanced braces while extracting {name} from upgrade-darling.ps1");
        return string.Empty;
    }

    private static void TryDeleteTree(string path)
    {
        try
        {
            Directory.Delete(path, recursive: true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* A leftover temp tree is not a test failure. */
        }
    }

    private static string ReadRepoFile(string relativePath)
    {
        var root = FindRepoRoot();
        Assert.NotNull(root);
        var path = Path.Combine(root!, relativePath);
        Assert.True(File.Exists(path), $"expected {path} to exist");
        return File.ReadAllText(path);
    }

    /// <summary>Walks up from the test output directory to the repo root (the directory holding
    /// <c>PerformanceMonitor.sln</c>) — the same idiom <c>DarlingFileSecurityTests</c> uses.</summary>
    private static string? FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        return null;
    }
}
