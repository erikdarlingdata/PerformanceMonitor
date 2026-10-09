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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A live Postgres test that counts blocks from a plan (<c>Shared Hit Blocks</c> / <c>Shared Read Blocks</c>) seeds its
/// window through <see cref="LiveClock"/> or is on the allow list with its reason (#5608). Block counts depend on where
/// a Timescale chunk boundary falls in the seeded rows, and rows seeded "N hours before the clock" move that boundary
/// with the hour the test runs: the newest-snapshot test measured perfmon old=264 new=120 at 00:50 UTC against
/// new=57 at 01:13, and failed the pull request that happened to run then. <see cref="LiveClock.AnchoredEnd"/> keeps
/// a window inside one chunk; <see cref="LiveClock.Now"/> carries the test-only clock override, so a run at midday can
/// be proved as if it were 00:30.
///
/// <para>The rule is per file, as <see cref="TimingTestCensusTests"/> does it. "Reads a wall clock" is
/// <c>DateTime.UtcNow</c>, <c>DateTime.Now</c>, <c>DateTimeOffset.UtcNow</c> or <c>DateTimeOffset.Now</c> in code
/// (comments and string literals do not count); "counts blocks" is one of the two plan property names above. A file
/// that does both and does not call <c>LiveClock.</c> fails unless it is named in the allow list with a reason, and an
/// allow entry that no longer matches fails, so the list only shrinks.</para>
///
/// <para>The second rule covers plan shape and the chunk catalog. A live file that reads <c>EXPLAIN</c>, <c>show_chunks</c>,
/// <c>timescaledb_information.chunks</c>, <c>compress_chunk</c> or <c>ChunkAppend</c> and reads the wall clock directly asserts
/// on chunk layout, or may, and the layout moves with the hour. Such a file seeds and reads through <see cref="LiveClock"/>, or
/// is named in <c>s_allowedPlanShape</c> with the reason its assertions cannot depend on where a boundary falls (it reads no
/// time, or the product reads the real clock itself and the assertion holds for any layout). A stale entry fails here too.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class LiveClockCensusTests
{
    private static readonly Dictionary<string, string> s_allowed = new(StringComparer.Ordinal)
    {
        // Empty on purpose: every file that counts blocks anchors through LiveClock. Add an entry only with a reason that
        // says why the count cannot depend on where a chunk boundary falls.
    };

    private const string E2Audits = "e2 audits this file (#5608): lane e2 replaces this entry with its verdict or removes it when it converts the file.";

    /// <summary>
    /// Live files that touch a plan or the chunk catalog and read the clock directly, with the reason each is safe as it is.
    /// Every other such file goes through <see cref="LiveClock"/>.
    /// </summary>
    private static readonly Dictionary<string, string> s_allowedPlanShape = new(StringComparer.Ordinal)
    {
        ["AnomalyObjectStatsLatestSnapshotsLiveTests.cs"] = "The read has no time predicate and the plan assertion names an index, not a chunk. The two snapshots are two days apart, so they sit in different chunks at any hour, and nothing passes the seeded time to the product.",
        ["CaggGroupIndexDropUpgradeLiveTests.cs"] = "The only clock reads time a stopwatch around an upgrade. No row is seeded from the wall clock.",
        ["CollectionGapAtStartTests.cs"] = "Seeds 35 days of hourly rows ending now, so 'at least four chunks' holds at any hour. The read takes no time, and the plan assertion is the access path (an index scan, no sequential scan), which a boundary does not change.",
        ["CollectionHealthAggregateTests.cs"] = "Seeds with SQL now() and the policy refresh runs on the server's own clock, so the window cannot be anchored from outside. Its assertions (at least N daily chunks over eight days, at least one covered chunk below the watermark and outside the hole's chunk) hold for any layout.",
        ["CollectionLogSegmentByLiveTests.cs"] = "Seeds from midnight six days back to now minus 15 minutes and compresses chunks older than three days with SQL now(), so the product reads the real clock. The assertions are the segment-by setting of every chunk, the same for each chunk, so no layout changes the answer.",
        ["DarlingDeltaSeederTests.cs"] = "SeedFromStoreAsync reads a window back from the product's own real clock, so a seed placed by an override falls outside it (the two live tests failed under 00:30Z). The rows sit minutes before now, and the assertions are delta values, not plan shape or chunk membership.",
        ["DarlingModuleMapGroupedSourceLiveTests.cs"] = "The daily refresh statement reads now() - 2 days with no bound parameter, so the product reads the real clock and a seed placed by an override falls outside its window (the refresh test failed under 12:00Z). The rows sit three hours before now, far inside the window, and the assertions are the newest name per handle, not chunk layout.",
        ["DarlingStoreUpgradeTests.cs"] = "The only clock read is a 30 second polling deadline for a marker file. No row is seeded from it.",
        ["PerfmonRegroupLiveTests.cs"] = "The only direct clock reads are a poll deadline. The chunk-window question the class documents (the day-3 chunk dropping out of the count at midnight UTC) is audited by lane e2 together with PerfmonRegroupLiveSupport.cs.",
        ["ParameterSensitiveDrillDownTextLiveTests.cs"] = E2Audits,
        ["PayloadDimensionLiveTests.cs"] = E2Audits,
        ["PerfmonRegroupLiveSupport.cs"] = E2Audits,
        ["PgDeadlockRemaskTests.cs"] = E2Audits,
        ["PgPlanCaptureCsvLiveTests.cs"] = E2Audits,
        ["PgPlanCaptureLiveTests.cs"] = E2Audits,
        ["PgServerConfigToolBoundTests.cs"] = E2Audits,
        ["PgTargetConfigSnapshotBoundTests.cs"] = E2Audits,
        ["PgTargetSeqScanTests.cs"] = E2Audits,
        ["PlanRegressionDailyBuilderLiveTests.cs"] = E2Audits,
        ["PlanRegressionDrillDownReuseLiveTests.cs"] = E2Audits,
        ["QueryStoreBackfillCutChunkLiveTests.cs"] = E2Audits,
        ["QueryStoreBackgroundIndexesLiveTests.cs"] = E2Audits,
        ["QueryStoreIntervalPurgeRowCappedTests.cs"] = E2Audits,
        ["QueryStoreIntervalWideBelowFloorLiveTests.cs"] = E2Audits,
        ["QueryStoreIntervalWideGridLiveTests.cs"] = E2Audits,
        ["QueryStoreTopLiteralEndStraddleLiveTests.cs"] = E2Audits,
        ["QueryStoreTopMcpLiveTests.cs"] = E2Audits,
        ["RawChunkIntervalReconcilerLiveTests.cs"] = E2Audits,
        ["RawPurgeTriggerGateErrorTests.cs"] = E2Audits,
        ["RawPurgeTriggerLiveTests.cs"] = E2Audits,
        ["RawTablesLeaveCatalogSweepLiveTests.cs"] = E2Audits,
        ["RollupBackfillLiveTests.cs"] = E2Audits,
        ["RollupFloorCacheLiveTests.cs"] = E2Audits,
        ["ServerListAndSummaryPlanShapeTests.cs"] = E2Audits,
        ["StoreMetricsLatestSkipScanLiveTests.cs"] = E2Audits,
        ["StoreSelfMetricsTests.cs"] = E2Audits,
        ["TimescaleSupportTests.cs"] = E2Audits,
        ["TopCpuQueriesTextLiveTests.cs"] = E2Audits,
        ["TopTextLookupPlanLiveTests.cs"] = E2Audits,
    };

    private static readonly Regex s_clock = new(@"\bDateTime(?:Offset)?\s*\.\s*(?:UtcNow|Now)\b", RegexOptions.Compiled);

    private static readonly Regex s_blocks = new(@"Shared (?:Hit|Read) Blocks", RegexOptions.Compiled);

    /// <summary>A plan or the chunk catalog: the second family of assertions that depend on where a chunk boundary falls.</summary>
    private static readonly Regex s_plan = new(@"EXPLAIN|show_chunks|timescaledb_information\.chunks|compress_chunk|ChunkAppend", RegexOptions.Compiled);

    private static readonly Regex s_live = new(@"DARLING_TEST_PG|ScratchPostgres", RegexOptions.Compiled);

    private static readonly Regex s_anchor = new(@"\bLiveClock\s*\.", RegexOptions.Compiled);

    /// <summary>Anti-vacuity: at least this many files count blocks, so a scan that stopped matching fails.</summary>
    private const int BlockCountingFloor = 2;

    /// <summary>Anti-vacuity for the plan and chunk-catalog rule: dozens of live files read a plan today.</summary>
    private const int PlanReadingFloor = 20;

    internal sealed record Row(string File, bool Live, bool ReadsClock, bool CountsBlocks, bool UsesAnchor, bool ReadsPlanOrChunks = false);

    internal static Row ScanText(string file, string text)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(text);
        return new Row(file, s_live.IsMatch(text), s_clock.IsMatch(code), s_blocks.IsMatch(text), s_anchor.IsMatch(code), s_plan.IsMatch(text));
    }

    internal static List<string> Problems(IEnumerable<Row> rows, IReadOnlyDictionary<string, string> allowed) =>
        Problems(rows, allowed, r => r.CountsBlocks);

    internal static List<string> Stale(IEnumerable<Row> rows, IReadOnlyDictionary<string, string> allowed) =>
        Stale(rows, allowed, r => r.CountsBlocks);

    /// <summary>A live file of the given kind that reads the wall clock directly, not through <see cref="LiveClock"/>, and is not allowed.</summary>
    internal static List<string> Problems(IEnumerable<Row> rows, IReadOnlyDictionary<string, string> allowed, Func<Row, bool> kind) =>
        rows.Where(r => r.Live && kind(r) && r.ReadsClock && !r.UsesAnchor && !allowed.ContainsKey(r.File))
            .Select(r => r.File).ToList();

    internal static List<string> Stale(IEnumerable<Row> rows, IReadOnlyDictionary<string, string> allowed, Func<Row, bool> kind)
    {
        var byFile = rows.ToDictionary(r => r.File, StringComparer.Ordinal);
        return allowed.Keys
            .Where(f => !byFile.TryGetValue(f, out var r) || !(r.Live && kind(r) && r.ReadsClock && !r.UsesAnchor))
            .ToList();
    }

    private static List<Row> Scan() =>
        Directory.EnumerateFiles(TestsRoot(), "*.cs", SearchOption.TopDirectoryOnly)
            .Select(p => ScanText(Path.GetFileName(p), File.ReadAllText(p)))
            .ToList();

    [Fact]
    public void TheDetector_FlagsAClockedBlockCounter_AndIgnoresAnchoredProseAndStringFiles()
    {
        var bare = ScanText("Bare.cs", "// DARLING_TEST_PG\nclass A { void B() { var e = DateTime.UtcNow; var s = \"Shared Hit Blocks\"; } }");
        var anchored = ScanText("Anchored.cs", "// DARLING_TEST_PG\nclass A { void B() { var e = LiveClock.AnchoredEnd(); var s = \"Shared Hit Blocks\"; var n = DateTime.UtcNow; } }");
        var prose = ScanText("Prose.cs", "// DARLING_TEST_PG DateTime.UtcNow\nclass A { void B() { var s = \"DateTime.UtcNow Shared Hit Blocks\"; } }");
        var noBlocks = ScanText("NoBlocks.cs", "// DARLING_TEST_PG\nclass A { void B() { var e = DateTime.UtcNow; } }");

        Assert.Equal(new[] { "Bare.cs" }, Problems(new[] { bare, anchored, prose, noBlocks }, new Dictionary<string, string>()));
        Assert.Empty(Problems(new[] { bare }, new Dictionary<string, string> { ["Bare.cs"] = "reason" }));
        Assert.Equal(new[] { "Gone.cs" }, Stale(new[] { bare }, new Dictionary<string, string> { ["Bare.cs"] = "reason", ["Gone.cs"] = "reason" }));
    }

    [Fact]
    public void EveryLiveTestThatCountsBlocks_SeedsThroughTheLiveClock_OrIsAllowedWithAReason()
    {
        var rows = Scan();

        var problems = Problems(rows, s_allowed);
        Assert.True(
            problems.Count == 0,
            "A live Postgres test counts plan blocks and reads the wall clock without LiveClock (#5608). The count depends on where a "
          + "chunk boundary falls in the seeded rows, so it changes with the hour the test runs. Seed the end of the window with "
          + "LiveClock.AnchoredEnd() (or LiveClock.Now() for a window that may straddle a boundary the same way every run), or name the "
          + "file in LiveClockCensusTests.s_allowed with the reason the count cannot depend on a boundary:\n  " + string.Join("\n  ", problems));

        var stale = Stale(rows, s_allowed);
        Assert.True(
            stale.Count == 0,
            "These allow-list entries no longer name a live block-counting file that reads the wall clock without LiveClock; delete them:\n  "
          + string.Join("\n  ", stale));

        Assert.True(
            rows.Count(r => r.Live && r.CountsBlocks) >= BlockCountingFloor,
            "the scan found fewer live block-counting files than expected, so it stopped matching and 'no problems' would be vacuous");
    }

    [Fact]
    public void TheDetector_FlagsAClockedPlanReader_AndIgnoresAnchoredAndUnclockedFiles()
    {
        var bare = ScanText("Bare.cs", "// DARLING_TEST_PG\nclass A { void B() { var e = DateTime.UtcNow; var s = \"EXPLAIN (COSTS OFF) SELECT 1\"; } }");
        var catalog = ScanText("Catalog.cs", "// ScratchPostgres\nclass A { void B() { var e = DateTimeOffset.UtcNow; var s = \"SELECT * FROM timescaledb_information.chunks\"; } }");
        var anchored = ScanText("Anchored.cs", "// DARLING_TEST_PG\nclass A { void B() { var e = LiveClock.AnchoredEnd(); var s = \"EXPLAIN SELECT 1\"; var n = DateTime.UtcNow; } }");
        var noClock = ScanText("NoClock.cs", "// DARLING_TEST_PG\nclass A { void B() { var s = \"EXPLAIN SELECT 1\"; } }");
        var notLive = ScanText("NotLive.cs", "class A { void B() { var e = DateTime.UtcNow; var s = \"EXPLAIN SELECT 1\"; } }");
        var noPlan = ScanText("NoPlan.cs", "// DARLING_TEST_PG\nclass A { void B() { var e = DateTime.UtcNow; } }");
        var rows = new[] { bare, catalog, anchored, noClock, notLive, noPlan };

        Assert.Equal(new[] { "Bare.cs", "Catalog.cs" }, Problems(rows, new Dictionary<string, string>(), r => r.ReadsPlanOrChunks));
        Assert.Equal(new[] { "Catalog.cs" }, Problems(rows, new Dictionary<string, string> { ["Bare.cs"] = "reason" }, r => r.ReadsPlanOrChunks));
        Assert.Equal(new[] { "Gone.cs" }, Stale(rows, new Dictionary<string, string> { ["Bare.cs"] = "reason", ["Gone.cs"] = "reason" }, r => r.ReadsPlanOrChunks));
        Assert.Equal(new[] { "Anchored.cs" }, Stale(rows, new Dictionary<string, string> { ["Anchored.cs"] = "reason" }, r => r.ReadsPlanOrChunks));
    }

    [Fact]
    public void EveryLiveTestThatReadsAPlanOrTheChunkCatalog_SeedsThroughTheLiveClock_OrIsAllowedWithAReason()
    {
        var rows = Scan();

        var problems = Problems(rows, s_allowedPlanShape, r => r.ReadsPlanOrChunks);
        Assert.True(
            problems.Count == 0,
            "A live Postgres test reads a plan or the chunk catalog and reads the wall clock without LiveClock (#5608). Its seeded window "
          + "can straddle a chunk boundary, and the plan, the chunks scanned or the chunk membership then change with the hour the test "
          + "runs. Seed and read through LiveClock.AnchoredEnd() (or LiveClock.Now() where the answer holds in any layout), or name the file "
          + "in LiveClockCensusTests.s_allowedPlanShape with the reason no assertion in it depends on where a boundary falls:\n  " + string.Join("\n  ", problems));

        var stale = Stale(rows, s_allowedPlanShape, r => r.ReadsPlanOrChunks);
        Assert.True(
            stale.Count == 0,
            "These s_allowedPlanShape entries no longer name a live plan or chunk-catalog file that reads the wall clock without LiveClock; delete them:\n  "
          + string.Join("\n  ", stale));

        Assert.All(s_allowedPlanShape, entry => Assert.False(string.IsNullOrWhiteSpace(entry.Value), entry.Key + " needs a reason"));
        Assert.True(
            rows.Count(r => r.Live && r.ReadsPlanOrChunks) >= PlanReadingFloor,
            "the scan found fewer live plan-reading files than expected, so it stopped matching and 'no problems' would be vacuous");
    }

    private static string TestsRoot([CallerFilePath] string thisFile = "") => Path.GetDirectoryName(thisFile)!;
}
