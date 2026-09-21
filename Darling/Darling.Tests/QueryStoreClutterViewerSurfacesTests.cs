/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The two VIEWER surfaces of the Query Store clutter view (#3797) — the web server tab's section and the
/// WPF Viewer's Query Store Clutter sub-tab — pinned as source, because neither can be executed here: the
/// repository carries no JavaScript runner, and the Viewer is <c>net10.0-windows</c>.
///
/// <para><b>What these pins are actually defending.</b> The issue asked for the SAME rows on three surfaces.
/// The way that claim rots is not a broken render — it is a second copy: a viewer that grows its own SQL, its
/// own threshold, or its own wording for a remedy, and then disagrees with the tool while both look right on
/// their own page. So the assertions below are mostly NEGATIVE — the viewer reader contains no SQL, the two
/// surfaces state no verdict prose of their own — because the positive half (it renders) is the half a
/// screenshot would catch and the negative half is the half nothing would.</para>
/// </summary>
public sealed class QueryStoreClutterViewerSurfacesTests
{
    private const string ToolName = "get_query_store_clutter";

    private static readonly string[] ServerTabsJsPath =
        ["Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js"];

    private static readonly string[] ViewerXamlPath =
        ["Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.xaml"];

    private static readonly string[] ViewerReaderPath =
        ["Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.QueryStoreClutter.cs"];

    private static readonly string[] ViewerTabPath =
        ["Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.QueryStoreClutter.cs"];

    private static readonly string[] ViewerQueriesPath =
        ["Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.Queries.cs"];

    /* ─────────────────────────── the web section ─────────────────────────── */

    /// <summary>
    /// The web section reads the tool ONCE, on the Queries tab, and renders the three things the payload
    /// keeps apart: the per-database rows, the per-server wait block and the per-server clerk.
    ///
    /// <para>One fetch is the load-bearing half. The read composes four arms over two raw hypertables; three
    /// descriptors would each own their own fetch and pay for that three times on one tab, which is exactly
    /// the cost <c>fanout</c> exists to avoid (<c>NoTab_FetchesTheSameReadTwice</c> holds the general rule;
    /// this holds the shape that keeps this read inside it).</para>
    /// </summary>
    [Fact]
    public void TheWebSection_ReadsTheToolOnce_AndKeepsTheDatabaseRowsApartFromTheServerBlock()
    {
        var js = ReadRepoFileLf(ServerTabsJsPath);

        Assert.True(
            Regex.Matches(js, Regex.Escape("\"" + ToolName + "\"")).Count == 1,
            "the server page names get_query_store_clutter more than once — a second descriptor is a second fetch of a four-arm read on one tab.");
        Assert.Contains("...fanout(\"" + ToolName + "\", { server, hours: ctx.hours, limit: 50 }, [", js, StringComparison.Ordinal);

        /* the three panels, by the payload path each one reads */
        Assert.Contains("rowsKey: \"databases\"", js, StringComparison.Ordinal);
        Assert.Contains("rowsKey: \"qs_overhead.wait_stats.included\"", js, StringComparison.Ordinal);
        Assert.Contains("stats: QS_CLERK_STATS", js, StringComparison.Ordinal);

        /* the per-server block says so in its own title, so a reader never takes it for a database's cost */
        Assert.Contains("\"Query Store Overhead (per server)\"", js, StringComparison.Ordinal);
    }

    /// <summary>
    /// The web row shows the verdict, the reasons that raised it, the replica exclusion, the capture mode
    /// and the server's own recommendation prose.
    ///
    /// <para>The verdict is COLOURED off <c>sevKey</c> — a pre-computed band — and the page never recomputes a
    /// threshold (R1). The reasons column is what keeps the colour from being the only evidence.</para>
    /// </summary>
    [Fact]
    public void TheWebRow_CarriesTheVerdict_TheReasons_TheExclusion_TheCaptureMode_AndTheProse()
    {
        var js = ReadRepoFileLf(ServerTabsJsPath);
        var block = Between(js, "const QS_CLUTTER_COLUMNS = [", "];");

        Assert.Contains("key: \"verdict\", label: \"Verdict\", sevKey: \"verdict\"", block, StringComparison.Ordinal);
        Assert.Contains("r.verdict_reasons", block, StringComparison.Ordinal);
        Assert.Contains("key: \"excluded_reason\"", block, StringComparison.Ordinal);
        Assert.Contains("key: \"config.query_capture_mode\"", block, StringComparison.Ordinal);
        Assert.Contains("r.recommendations", block, StringComparison.Ordinal);

        /* The page renders the server's sentences; it does not write its own. A literal remedy here would be
           a second wording of a remedy the tool already words. */
        foreach (var forbidden in new[] { "switch to AUTO", "Tighten MAX_PLANS_PER_QUERY", "readable secondary" })
        {
            Assert.DoesNotContain(forbidden, block, StringComparison.Ordinal);
        }
    }

    /* ─────────────────────────── the WPF sub-tab ─────────────────────────── */

    /// <summary>
    /// The Viewer sub-tab exists, sits where the dispatch says it does, and is reached by its NAMED index
    /// constant rather than a literal — the property that lets a future tab be inserted without silently
    /// pointing an existing case at the wrong grid.
    /// </summary>
    [Fact]
    public void TheViewerSubTab_IsWiredAtItsNamedIndex_InTheOrderTheXamlDeclares()
    {
        var xaml = ReadRepoFileLf(ViewerXamlPath);
        var queries = ReadRepoFileLf(ViewerQueriesPath);

        /* The Queries group's sub-tabs, in XAML order: the constants below must be this list's indices. */
        var headers = Regex.Matches(
                Between(xaml, "<TabItem Header=\"Queries\"", "<TabItem Header=\"Plan Viewer\""),
                "<TabItem Header=\"([^\"]+)\"")
            .Select(m => m.Groups[1].Value)
            .ToArray();

        var clutterAt = Array.IndexOf(headers, "Query Store Clutter");
        Assert.True(clutterAt >= 0, "ViewerServerTab.xaml has no Query Store Clutter sub-tab");

        var declared = int.Parse(
            Regex.Match(queries, @"private const int QueryStoreClutterSubTabIndex = (\d+);").Groups[1].Value,
            System.Globalization.CultureInfo.InvariantCulture);
        Assert.Equal(clutterAt, declared);

        /* Every sibling constant still agrees with the XAML too — the failure this guards is an INSERT that
           moved a neighbour's tab without moving its constant, which compiles and loads the wrong grid. */
        foreach (var (header, name) in new[]
                 {
                     ("Performance Trends", "PerformanceTrendsSubTabIndex"),
                     ("Active Queries", "ActiveQueriesSubTabIndex"),
                     ("Current Active Queries", "CurrentActiveQueriesSubTabIndex"),
                     ("Top Queries by Duration", "TopQueriesSubTabIndex"),
                     ("Top Procedures by Duration", "TopProceduresSubTabIndex"),
                     ("Query Store by Duration", "QueryStoreSubTabIndex"),
                     ("Query Store Regressions", "QueryStoreRegressionsSubTabIndex"),
                     ("Query Store Clutter", "QueryStoreClutterSubTabIndex"),
                     ("Plan Corrections", "PlanCorrectionsSubTabIndex"),
                     ("Query Heatmap", "QueryHeatmapSubTabIndex"),
                 })
        {
            var value = int.Parse(
                Regex.Match(queries, @"private const int " + name + @" = (\d+);").Groups[1].Value,
                System.Globalization.CultureInfo.InvariantCulture);
            Assert.Equal(Array.IndexOf(headers, header), value);
        }

        Assert.Equal(10, headers.Length);
        Assert.Contains("case QueryStoreClutterSubTabIndex:\n                await LoadQueryStoreClutterAsync(startUtc, endUtc);", queries, StringComparison.Ordinal);
    }

    /// <summary>
    /// The Viewer's reader runs NO SQL of its own: it calls the shared statements and the shared composition,
    /// which is the whole reason they were moved to <c>.Storage</c> beside the <c>DarlingPg*Reader</c> family.
    ///
    /// <para>A second copy of a four-arm read is not a tidiness point. The arms carry a window floor, a
    /// discrete-median definition that has to agree with <c>percentile_disc</c>, and a replica gate read off
    /// one engine bit; a copy that drifts on any of those calls a database Critical on one surface and
    /// Healthy on the other, and the copy that drifted is never the one being read.</para>
    /// </summary>
    [Fact]
    public void TheViewerReader_RunsTheSharedStatements_AndWritesNoneOfItsOwn()
    {
        var reader = ReadRepoFileLf(ViewerReaderPath);

        foreach (var sql in new[] { "SELECT", "FROM ", "CreateCommand", "NpgsqlCommand" })
        {
            Assert.DoesNotContain(sql, reader, StringComparison.Ordinal);
        }

        foreach (var arm in new[]
                 {
                     "DarlingQueryStoreClutterReader.GetReadCostAsync",
                     "DarlingQueryStoreClutterReader.GetPlanChurnAsync",
                     "DarlingQueryStoreClutterReader.GetConfigAsync",
                     "DarlingQueryStoreClutterReader.GetQdsWaitsAsync",
                     "DarlingQueryStoreClutterReader.GetQueryStoreClerkAsync",
                     "QueryStoreClutter.Compose(",
                     "QueryStoreClutter.Recommendations(",
                 })
        {
            Assert.Contains(arm, reader, StringComparison.Ordinal);
        }

        /* And it derives nothing itself: every figure on the row comes from the shared arithmetic. */
        foreach (var derivation in new[]
                 {
                     "QueryStoreClutter.RunsSlowestPct(", "QueryStoreClutter.DominanceRatio(",
                     "QueryStoreClutter.NewPlansPerDay(", "QueryStoreClutter.NeverSeenTwiceFraction(",
                     "QueryStoreClutter.PctOfCap(", "QueryStoreClutter.WaitMsPerHour(",
                 })
        {
            Assert.Contains(derivation, reader, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// A replica is a ROW with the reason on it, on both viewer surfaces — never a filtered-out database.
    ///
    /// <para>The topology ruling (2026-09-20) excludes replicas by architecture, and the honest way to do that
    /// is to say so on the row: an operator who knows the database exists and cannot see it cannot tell "not
    /// cluttered" from "not shown", and files the second as the first.</para>
    /// </summary>
    [Fact]
    public void BothSurfaces_ShowAnExcludedReplicaAsARow_WithTheReasonOnIt()
    {
        var js = ReadRepoFileLf(ServerTabsJsPath);
        var xaml = ReadRepoFileLf(ViewerXamlPath);
        var reader = ReadRepoFileLf(ViewerReaderPath);

        Assert.Contains("label: \"Excluded Because\"", Between(js, "const QS_CLUTTER_COLUMNS = [", "];"), StringComparison.Ordinal);
        Assert.Contains("Text=\"Excluded Because\"", xaml, StringComparison.Ordinal);
        Assert.Contains("ExcludedReason = row.ExcludedReason ?? Absent", reader, StringComparison.Ordinal);

        /* The database pipeline's ONLY filter is the tab's database filter. Asserted as the exact expression
           rather than as a count of Where() calls: the overhead projection has a Where of its own (the sleep
           waits), so a count would be satisfied by moving the wrong filter into the right total. */
        Assert.Contains(
            "            .Where(r => wanted is null || wanted.Contains(r.DatabaseName))",
            reader,
            StringComparison.Ordinal);

        /* Two filters in the file — the database one above and the sleep-wait one in the overhead
           projection — and NEITHER may read the row's own Excluded flag. Spelled `.Excluded` so the wait
           filter's IsExcludedWaitType, which is a different word about a different thing, is not mistaken
           for one. */
        var filters = Regex.Matches(reader, @"\.Where\((?<lambda>[^\n]*)").Select(m => m.Groups["lambda"].Value).ToArray();
        Assert.Equal(2, filters.Length);
        Assert.All(filters, f => Assert.DoesNotContain(".Excluded", f, StringComparison.Ordinal));
    }

    /// <summary>
    /// Neither viewer surface words a remedy of its own: both render
    /// <c>QueryStoreClutter.Recommendations</c>'s sentences, so the three surfaces say one thing.
    /// </summary>
    [Fact]
    public void TheViewerTab_RendersTheCompositionsProse_AndWritesNoRemedyOfItsOwn()
    {
        var tab = ReadRepoFileLf(ViewerTabPath);
        var reader = ReadRepoFileLf(ViewerReaderPath);

        Assert.Contains("Recommendation = string.Join(\" \", QueryStoreClutter.Recommendations(row))", reader, StringComparison.Ordinal);
        Assert.Contains("row.Recommendation", tab, StringComparison.Ordinal);

        foreach (var forbidden in new[] { "switch to AUTO", "MAX_PLANS_PER_QUERY", "STALE_QUERY_THRESHOLD_DAYS" })
        {
            Assert.DoesNotContain(forbidden, tab, StringComparison.Ordinal);
            Assert.DoesNotContain(forbidden, reader, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The per-server overhead block is labelled per-server on both surfaces, and both name the sleep waits
    /// the collection filter drops — so neither is read as the whole of Query Store's cost.
    /// </summary>
    [Fact]
    public void BothSurfaces_LabelTheOverheadPerServer_AndNameTheExcludedSleepWaits()
    {
        var js = ReadRepoFileLf(ServerTabsJsPath);
        var xaml = ReadRepoFileLf(ViewerXamlPath);
        var tab = ReadRepoFileLf(ViewerTabPath);
        var reader = ReadRepoFileLf(ViewerReaderPath);

        Assert.Contains("(per server)", js, StringComparison.Ordinal);
        Assert.Contains("Header=\"Query Store overhead (per server)\"", xaml, StringComparison.Ordinal);

        /* The web reads the payload's own excluded_note; the Viewer reads the shared list. Neither restates
           the four wait names, which live in IgnoredWaitDefaults and are derived from it. */
        Assert.Contains("noteKey: \"qs_overhead.wait_stats.excluded_note\"", js, StringComparison.Ordinal);
        Assert.Contains("QueryStoreClutter.ExcludedQdsWaitTypes", reader, StringComparison.Ordinal);
        Assert.Contains("Sleep waits excluded at collection: ", tab, StringComparison.Ordinal);

        foreach (var name in new[] { "QDS_ASYNC_QUEUE", "QDS_SHUTDOWN_QUEUE" })
        {
            Assert.DoesNotContain(name, reader, StringComparison.Ordinal);
            Assert.DoesNotContain(name, tab, StringComparison.Ordinal);
        }
    }

    /// <summary>The text strictly BETWEEN the two anchors — the opening anchor is excluded, because the
    /// Queries group's own <c>&lt;TabItem Header="Queries"&gt;</c> would otherwise be counted as the first of
    /// its own sub-tabs and shift every index by one.</summary>
    /// <summary>
    /// Every measured column on the clutter grid binds a NUMBER, not a pre-formatted string.
    ///
    /// <para>This grid exists to rank. A <c>DataGrid</c> sorts on the binding's path, so a column bound to
    /// text sorts lexicographically — 9 above 1,234, 0.9 above 0.10 — while looking exactly like a working
    /// sort, which is the same defect class as a band sorted alphabetically. The em-dash for an unmeasured
    /// arm comes from <c>TargetNullValue</c> instead, so the absence still reads like every other absent
    /// cell in the Viewer without the value becoming text or becoming a zero.</para>
    /// </summary>
    [Fact]
    public void EveryMeasuredColumn_BindsANumber_WithTheAbsenceOnTheBinding()
    {
        var reader = ReadRepoFileLf(ViewerReaderPath);
        var xaml = ReadRepoFileLf(ViewerXamlPath);
        var grid = Between(xaml, "x:Name=\"QueryStoreClutterGrid\"", "</DataGrid>");

        /* The row's measured members are nullable numerics, never strings. */
        foreach (var member in new[]
                 {
                     "double? RunsSlowestPct", "double? SlowestSharePct", "int? SlowestItemMsP50",
                     "double? DominanceRatio", "int? PlansPerQueryP95", "int? PlansPerQueryMax",
                     "double? NewPlansPerDay", "double? OneShotFraction", "int? DistinctPlans",
                     "long? MaxPlansPerQuery", "long? StaleQueryThresholdDays", "double? PctOfCap",
                     "DateTime? OptionsCaptured",
                 })
        {
            Assert.Contains("public " + member + " { get; init; }", reader, StringComparison.Ordinal);
        }

        /* And every one of those columns carries its own absence on the binding. */
        foreach (var column in new[]
                 {
                     "RunsSlowestPct", "SlowestSharePct", "SlowestItemMsP50", "DominanceRatio",
                     "PlansPerQueryP95", "PlansPerQueryMax", "NewPlansPerDay", "OneShotFraction",
                     "DistinctPlans", "MaxPlansPerQuery", "StaleQueryThresholdDays", "PctOfCap",
                     "OptionsCaptured",
                 })
        {
            var binding = Regex.Match(grid, @"\{Binding " + column + @"(?<rest>[^}]*)\}");
            Assert.True(binding.Success, column + " has no binding on the clutter grid");
            Assert.Contains("TargetNullValue=", binding.Groups["rest"].Value, StringComparison.Ordinal);
            Assert.Contains("StringFormat=", binding.Groups["rest"].Value, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The clutter grid takes NO default SortDescription, and that is a decision rather than an omission.
    ///
    /// <para>The composition orders rows worst-first by band, then the read-cost share, then plans per query.
    /// A <c>DataGrid</c>'s default sort is one column, and the nearest single-column stand-in — the Verdict
    /// text, descending — sorts alphabetically: Warning, Unknown, Healthy, Critical. That is the ranking
    /// reversed at the top, on a grid whose whole job is to put the worst database first, and it would look
    /// like a working sort. The arrival order is the ranking.</para>
    /// </summary>
    [Fact]
    public void TheClutterGrid_TakesNoDefaultSort_BecauseTheArrivalOrderIsTheRanking()
    {
        var tab = ReadRepoFileLf(ViewerTabPath);
        Assert.DoesNotContain("SetDefaultSortIfNone", tab, StringComparison.Ordinal);
        Assert.Contains("Warning above Critical", tab, StringComparison.Ordinal);
    }

    private static string Between(string text, string open, string close)
    {
        var start = text.IndexOf(open, StringComparison.Ordinal);
        Assert.True(start >= 0, "anchor not found: " + open);
        var from = start + open.Length;
        var end = text.IndexOf(close, from, StringComparison.Ordinal);
        Assert.True(end > from, "closing anchor not found after: " + open);
        return text[from..end];
    }
}
