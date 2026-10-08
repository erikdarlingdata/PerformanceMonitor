/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A Darling.Tests class that starts a clock runs in the <c>timing</c> collection or is on the allow list with its
/// reason (#5602). xunit runs test classes in parallel, so a class that times its work and fails past a limit is
/// judging the whole runner's load: #5561 (a 27 MB plan) and #5600 (the cloud probe's step limit, 1083 ms against a
/// 1000 ms bar in the whole-suite guards job) were that. <see cref="TimingCollection"/> is declared with
/// <c>DisableParallelization</c>, so xunit runs it alone, after the parallel classes.
///
/// <para>"Starts a clock" is <see cref="TimingTestScanner"/>'s rule: <c>Stopwatch.StartNew</c>, <c>GetTimestamp</c>,
/// <c>GetElapsedTime</c>, <c>new Stopwatch</c>, a <c>DateTime.UtcNow</c> subtraction or <c>Environment.TickCount</c> in
/// code (comments and string literals do not count). That is wider than "asserts on the elapsed time", so a class that
/// only reports a time is named too, and one more assertion cannot slip in unseen. The allow list is per file; every
/// entry carries its reason, and an entry whose file no longer starts a clock outside the collection fails, so the
/// list only shrinks.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class TimingTestCensusTests
{
    private const string HangGuard = "hang guard: the bound is a generous ceiling that only stops a stuck test, so load cannot trip it and a regression is not what it measures";

    private const string PollLoop = "poll loop: the clock only bounds a wait for a state that the test then asserts on; no assertion is made on the time";

    private const string Diagnostic = "diagnostic: the time is written to the test output and nothing asserts on it";

    private const string LiveStore = "live-postgres member: it needs that collection's shared migrated store, and a class is in one collection only; the bound is a server-side timeout with a wide margin";

    /// <summary>Anti-vacuity: the timing collection holds well over this many classes, so a scan that stopped seeing it fails.</summary>
    private const int TimingMemberFloor = 15;

    private static readonly Dictionary<string, string> s_allowed = new(StringComparer.Ordinal)
    {
        ["CaggGroupIndexDropUpgradeLiveTests.cs"] = "lower bound only: it asserts the migration waited out a ~8 s lock hold (elapsed >= 7 s); load makes the wait longer, never shorter",
        ["CollectionLogDrainForensicsStoreTests.cs"] = "the clock is handed to a counting reader under test; the assertions are on the readings it records (rows, bytes, a last-read stamp >= 0), with no upper bound on any time",
        ["CollectionLogSegmentByLiveTests.cs"] = LiveStore + "; the 30 s ceiling is a hang guard on a lock wait",
        ["ComposeTimeoutPhaseLiveTests.cs"] = HangGuard + " (60 s against a 5 s compose timeout)",
        ["DarlingManagedPostgresTests.cs"] = PollLoop + " (a retry of a file operation the OS may still hold)",
        ["MigrationLockWaitContentionTests.cs"] = LiveStore + "; the 750 ms ceiling separates a first-attempt acquire (one round trip) from the one-second poll sleep, 3 orders of magnitude of headroom",
        ["MoveDirectoryOnceReleasedTests.cs"] = "lower bound only: it asserts the retry waited its patience before giving up; load makes the wait longer, never shorter",
        ["PerDatabaseFaultPathSplitTests.cs"] = "the clock is a running slice the code under test reads; the test spins until it passes a minimum and asserts it keeps running and freezes when stopped, with no upper bound",
        ["PgWaitSamplerLiveTests.cs"] = LiveStore + "; the bound is the sampling window plus 30 s, with the window as the lower bound",
        ["PlanRegressionDailyTriggerCostRig.cs"] = Diagnostic + " (a cost rig, never asserted)",
        ["ProvisioningSerializationTests.cs"] = "lower bounds on a deliberate wait plus ceilings of 20 s and 90 s over it, which are hang guards; the same file also holds a live-postgres member",
        ["QueryStoreBackfillCutChunkLiveTests.cs"] = Diagnostic,
        ["QueryStoreIntervalConvergenceLiveTests.cs"] = Diagnostic + " (each step's duration is recorded for the failure message)",
        ["QueryStoreIntervalPurgeRowCappedTests.cs"] = HangGuard + " (120 s for a purge drain)",
        ["RdsLogStateConcurrencyTests.cs"] = "the clock sets how long the test hammers the code (rounds for one second); no assertion is made on a time",
        ["ReadScopeStatementCaptureLiveTests.cs"] = "live-postgres member; lower bound only (the recorded duration must reach 550 ms after a 600 ms delay)",
        ["RollupBackfillLiveTests.cs"] = PollLoop + " (a held-chunk wait with a limit)",
        ["SensitiveStatementJsonTests.cs"] = Diagnostic + " (median of 20 identity walks)",
        ["SensitiveStatementOutputFilterTests.cs"] = "every time bound goes through TimingClaim.AtMost, which asserts only when DARLING_TIMING_TESTS=1 and otherwise records the reading; the best of three is what it judges",
        ["SensitiveStatementSessionTests.cs"] = Diagnostic + " (throughput records)",
        ["SensitiveStatementXmlTests.cs"] = Diagnostic + " (best-of-seven records; the only assertion is that the pass took time)",
        ["SensitiveStatementsTests.cs"] = "time bounds go through TimingClaim.AtMost (asserts only when DARLING_TIMING_TESTS=1, best of several runs); the real assertions are lower bounds (a budgeted judge ran at least 60% of its limit) and fake-clock arithmetic",
        ["StatementFilterBlockingReadsLiveTests.cs"] = LiveStore + "; the 10 s bound is a hang guard on a filter that judges 1 MB in tens of milliseconds",
        ["StatementFilterPlanReadsLiveTests.cs"] = LiveStore + "; the 15 s bound is a hang guard on a filter that judges a plan in tens of milliseconds",
        ["StatementScrubBlockingTests.cs"] = Diagnostic + " (scrub throughput records)",
        ["StatementScrubSnapshotTests.cs"] = HangGuard + " (2 minutes for a worst-case snapshot read)",
        ["StoreCopyPhaseLivePostgresTests.cs"] = LiveStore + "; the window is the copy-start deadline (its own clock, not Npgsql's) minus 1 s up to the command timeout",
        ["TrendStaleStatsLiveTests.cs"] = Diagnostic + " (the assertions are on the answer status)",
        ["TuningDropLockRecoveryLiveTests.cs"] = HangGuard + " (60 s against a single blocked drop)",
        ["WebExceptionTextCensusTests.cs"] = "the stopwatch's reading is passed as the elapsed-milliseconds argument of a fake tool's timeout result; no assertion is made on a time",
        ["TuningReaderBudgetLiveTests.cs"] = LiveStore + "; the bound is half of each reader's 15 s or 30 s command deadline",
    };

    [Fact]
    public void TheDetector_SeesAClockAndTheCollection_AndIgnoresProseAndStrings()
    {
        const string text = "namespace N;\n\n"
          + "[Collection(\"timing\")]\npublic sealed class Timed\n{\n    void A() { var w = Stopwatch.StartNew(); }\n}\n\n"
          + "public sealed class Bare\n{\n    void B() { long t = Stopwatch.GetTimestamp(); }\n}\n\n"
          + "[Trait(\"Stage\", \"Guard\")]\n[Collection(\"timing\")]\npublic sealed class TwoAttributes\n{\n    void C() { var d = DateTime.UtcNow - start; }\n}\n\n"
          + "public sealed class Prose\n{\n    // var w = Stopwatch.StartNew();\n    /* new Stopwatch() */\n"
          + "    void D() { Assert.Contains(\"Stopwatch.GetTimestamp()\", source); var s = @\"a \"\"Stopwatch.StartNew()\"\" b\"; }\n"
          + "    /// <summary>Environment.TickCount</summary>\n    void E() { }\n}\n\n"
          + "public sealed class Interpolated\n{\n    void F() => Log($\"x {Stopwatch.StartNew()}\");\n}\n";

        var rows = TimingTestScanner.ScanText("t.cs", text).ToDictionary(r => r.Name);

        Assert.True(rows["Timed"].StartsClock);
        Assert.True(rows["Timed"].InTimingCollection);
        Assert.True(rows["Bare"].StartsClock);
        Assert.False(rows["Bare"].InTimingCollection);
        Assert.True(rows["TwoAttributes"].StartsClock);
        Assert.True(rows["TwoAttributes"].InTimingCollection);
        Assert.False(rows["Prose"].StartsClock);
        Assert.False(rows["Interpolated"].StartsClock);
    }

    [Fact]
    public void TheDetector_ReportsAClockOutsideTheCollection_AndAStaleAllowEntry()
    {
        var rows = TimingTestScanner.ScanText(
            "Bare.cs",
            "public sealed class Bare\n{\n    void B() { var w = new Stopwatch(); }\n}\n");

        var problems = TimingTestScanner.Problems(rows, new Dictionary<string, string>());
        Assert.Single(problems);
        Assert.Contains("Bare.cs", problems[0], StringComparison.Ordinal);

        Assert.Empty(TimingTestScanner.Problems(rows, new Dictionary<string, string> { ["Bare.cs"] = "reason" }));
        Assert.Equal(new[] { "Gone.cs" }, TimingTestScanner.Stale(rows, new Dictionary<string, string> { ["Bare.cs"] = "reason", ["Gone.cs"] = "reason" }));
    }

    [Fact]
    public void EveryClassThatStartsAClock_RunsInTheTimingCollection_OrIsAllowedWithAReason()
    {
        var rows = TimingTestScanner.Scan(TestsRoot());

        var problems = TimingTestScanner.Problems(rows, s_allowed);
        Assert.True(
            problems.Count == 0,
            "A test class starts a clock outside the `timing` collection (#5602). xunit runs classes in parallel, so a time limit "
          + "judged there is judged against the runner's load. Add [Collection(\"timing\")] above the class (TimingCollection runs "
          + "alone, after the parallel classes), assert on the work instead of the clock, or, for a hang guard, a poll loop or a "
          + "diagnostic, name the file in TimingTestCensusTests.s_allowed with the reason:\n  " + string.Join("\n  ", problems));

        var stale = TimingTestScanner.Stale(rows, s_allowed);
        Assert.True(
            stale.Count == 0,
            "These allow-list entries no longer name a file that starts a clock outside the timing collection; delete them:\n  "
          + string.Join("\n  ", stale));

        var members = rows.Count(r => r.InTimingCollection);
        Assert.True(
            members >= TimingMemberFloor,
            $"only {members} Darling.Tests classes are in the timing collection; the scan has stopped seeing them");
    }

    [Fact]
    public void EveryAllowListEntry_NamesARealFile_AndGivesAReason()
    {
        foreach (var (file, reason) in s_allowed)
        {
            Assert.True(File.Exists(Path.Combine(TestsRoot(), file)), $"allow-list entry {file} is not a file under Darling.Tests");
            Assert.False(string.IsNullOrWhiteSpace(reason), $"allow-list entry {file} has no reason");
        }
    }

    [Fact]
    public void TheTimingCollection_IsNotParallel_AndIsTheOnlyOneOfItsName()
    {
        var definitions = typeof(TimingTestCensusTests).Assembly.GetTypes()
            .Select(t => (Type: t, Attribute: t.GetCustomAttribute<CollectionDefinitionAttribute>()))
            .Where(d => d.Attribute is not null && string.Equals(d.Attribute.Name, TimingTestScanner.CollectionName, StringComparison.Ordinal))
            .ToList();

        var definition = Assert.Single(definitions);
        Assert.True(definition.Attribute!.DisableParallelization, "the timing collection must run alone, or it protects nothing");
    }

    private static string TestsRoot([CallerFilePath] string thisFile = "") => Path.GetDirectoryName(thisFile)!;
}
