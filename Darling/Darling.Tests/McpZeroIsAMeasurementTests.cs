/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #3541 A12 — contract rule 5, "zero is a measurement": a zero the product publishes must have been measured,
/// and an absence must be null / unknown WITH its reason. Six sites published a 0, an <c>empty</c>, or a
/// nominal-window label where nothing had been measured:
///
/// <list type="number">
/// <item>eight of the nine <c>get_health_parser_*</c> tools answered a dead <c>system_health</c> session (or a
/// collector that never ran) with the same <c>empty</c> a healthy quiet hour earns;</item>
/// <item><c>get_query_store_regressions</c> coerced a NULL percent (baseline side 0 → no denominator) to 0,
/// publishing the largest possible I/O regression as "no change";</item>
/// <item>the LAG-differenced duration trends rated the window's first collection 0.0 — a fabricated quiet
/// instant — on both SKUs and on the Query Store rollup route (#3540 A8's "first point of every differenced
/// series");</item>
/// <item><c>get_pg_xmin_horizon</c> divided a holder's wins by its OWN rows, so 2 wins in 2 holder-bearing
/// collections out of 288 captures read as 100% chronic;</item>
/// <item><c>get_pvs_stats</c> published a measured 0 MB and an unmeasured NULL identically (both
/// <c>pct_of_database: null</c>);</item>
/// <item><c>get_table_index_sizes</c> folded a missing 30-day baseline onto the 7-day one (and that onto the
/// oldest, and that onto <c>current</c> → growth 0) and labelled the result with the window asked for.</item>
/// </list>
///
/// <para>This file is the census: the discriminators are witnessed against the defect shapes as literals (a
/// matcher that quietly stopped matching reports a clean bill), the fixed shapes are pinned on BOTH SKUs from
/// source (Lite's assembly is not referenced here, the <see cref="McpPageContractTests"/> arrangement), the
/// SQL consts are pinned for the new columns, and the pure derivations (growth, PVS reasons) are executed.
/// <see cref="McpZeroIsAMeasurementLivePostgresTests"/> runs the tools against live Postgres.</para>
/// </summary>
public sealed class McpZeroIsAMeasurementTests
{
    private const string DarlingMcp = "Darling/PerformanceMonitor.Darling.Service/Mcp";
    private const string LiteMcp = "Lite/Mcp";

    private static readonly string[] HealthParserTools =
    [
        "get_health_parser_cpu_tasks",
        "get_health_parser_io_issues",
        "get_health_parser_memory_broker",
        "get_health_parser_memory_conditions",
        "get_health_parser_memory_node_oom",
        "get_health_parser_scheduler_issues",
        "get_health_parser_severe_errors",
        "get_health_parser_significant_waits",
        "get_health_parser_system_health",
    ];

    /* ───────────────────────── 1. the health-parser source witness ───────────────────────── */

    /// <summary>
    /// Every one of the nine, on both SKUs, publishes the witness on its data envelope and routes its
    /// zero-row case through the shared four-rung ladder — a tool that kept a private
    /// <c>Status("empty", …)</c> would be the defect returning under one name.
    /// </summary>
    [Theory]
    [InlineData(DarlingMcp + "/DarlingMcpHealthParserTools.cs")]
    [InlineData(LiteMcp + "/McpHealthParserTools.cs")]
    public void EveryHealthParserTool_PublishesTheSourceWitness_AndClimbsTheSharedLadder(string file)
    {
        var source = ReadRepoFile(file.Split('/'));

        foreach (var tool in HealthParserTools)
        {
            var body = Strip(ToolBody(source, tool));
            Assert.Contains("source_observed = true", body, StringComparison.Ordinal);
            Assert.Contains("last_captured_at = Stamp(", body, StringComparison.Ordinal);
            Assert.Contains("EmptyAsync(", body, StringComparison.Ordinal);
            Assert.DoesNotContain("McpHelpers.Status(\"empty\"", body, StringComparison.Ordinal);

            var description = DescriptionOf(source, tool);
            Assert.Contains("source_observed", description, StringComparison.Ordinal);
            Assert.Contains("last_captured_at", description, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The ladder itself: four rungs, three of them <c>empty</c> and exactly one <c>unavailable</c> — the
    /// nothing-of-any-type-ever rung, with <c>source_observed: false</c> and the sentence the
    /// EngineCapabilityMissTests pin (<c>system_health session is started</c>). Rung 3 — this type never, the
    /// session alive — must be <c>empty</c>: a memory-node OOM that never happened is the healthy measurement.
    /// </summary>
    [Theory]
    [InlineData(DarlingMcp + "/DarlingMcpHealthParserTools.cs")]
    [InlineData(LiteMcp + "/McpHealthParserTools.cs")]
    public void TheEmptyLadder_HasFourRungs_AndOnlyTheDeadSessionIsUnavailable(string file)
    {
        var source = ReadRepoFile(file.Split('/'));
        var start = source.IndexOf("private static async Task<string> EmptyAsync", StringComparison.Ordinal);
        Assert.True(start > 0, $"{file} has no shared EmptyAsync ladder");
        var end = source.IndexOf("private static string WitnessStatus", start, StringComparison.Ordinal);
        var ladder = Strip(source[start..end]);

        Assert.Equal(3, Regex.Matches(ladder, "WitnessStatus\\(\\s*\"empty\"").Count);
        Assert.Single(Regex.Matches(ladder, "WitnessStatus\\(\\s*\"unavailable\""));
        Assert.Single(Regex.Matches(ladder, "sourceObserved: false"));
        Assert.Contains("NOT an all-clear", ladder, StringComparison.Ordinal);
        Assert.Contains("system_health session is started", ladder, StringComparison.Ordinal);
        /* The engine-capability probe goes FIRST on the dead rung — the stronger claim. */
        Assert.True(
            ladder.IndexOf("NotCollectedStatusAsync", StringComparison.Ordinal) < ladder.IndexOf("\"unavailable\"", StringComparison.Ordinal));
        /* The healthy rungs never reach for the dead rung's word, so a caller keying on it cannot be misled. */
        var rung3 = ladder[..ladder.IndexOf("NotCollectedStatusAsync", StringComparison.Ordinal)];
        Assert.DoesNotContain("EVER", rung3, StringComparison.Ordinal);
    }

    /// <summary>
    /// The witness reads the SAME view the tools read and is not the collection log — the log records a
    /// SUCCESS for a run that read a dead session and stored nothing, which is the shape being fixed.
    /// </summary>
    [Fact]
    public void TheWitness_IsTheEventsView_NotTheCollectionLog_OnBothSkus()
    {
        Assert.Contains("FROM v_system_health_events", DarlingSystemHealthReader.LastCaptureSql, StringComparison.Ordinal);
        Assert.DoesNotContain("collection_log", DarlingSystemHealthReader.LastCaptureSql, StringComparison.Ordinal);

        var lite = ReadRepoFile("Lite", "Services", "LocalDataService.SystemEvents.cs");
        var probe = lite[lite.IndexOf("GetLastSystemHealthCaptureAsync", StringComparison.Ordinal)..];
        probe = probe[..probe.IndexOf("GetLastSystemHealthCaptureOfTypeAsync", StringComparison.Ordinal)];
        Assert.Contains("SELECT MAX(collection_time)", probe, StringComparison.Ordinal);
        Assert.Contains("FROM v_system_health_events", probe, StringComparison.Ordinal);
        Assert.DoesNotContain("collection_log", probe, StringComparison.Ordinal);
    }

    /* ───────────────────────── 2. NULL percents stay NULL ───────────────────────── */

    private static readonly Regex NullPercentCoerced = new(@"RegressionPercent = reader\.IsDBNull\(\d+\) \? 0 ", RegexOptions.Compiled);

    [Fact]
    public void TheRegressionReaders_NeverCoerceANullPercentToZero_OnEitherSku()
    {
        /* Witness: the defect as it shipped on Lite. */
        Assert.Matches(NullPercentCoerced, "DurationRegressionPercent = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),");

        var lite = ReadRepoFile("Lite", "Services", "LocalDataService.QueryStoreRegressions.cs");
        Assert.DoesNotMatch(NullPercentCoerced, lite);
        Assert.Equal(3, Regex.Matches(lite, @"RegressionPercent = reader\.IsDBNull\(\d+\) \? null ").Count);
        Assert.Equal(3, Regex.Matches(lite, @"public double\? \w+RegressionPercent").Count);

        /* Darling's reader is positional; the three percent columns are 4, 7 and 10. */
        var darling = ReadRepoFile(DarlingMcp.Split('/').Append("DarlingQueryStoreRegressionReader.cs").ToArray());
        foreach (var column in new[] { 4, 7, 10 })
        {
            Assert.Contains($"reader.IsDBNull({column}) ? null : Convert.ToDouble(reader.GetValue({column}))", darling, StringComparison.Ordinal);
        }

        var percents = typeof(DarlingQueryStoreRegressionReader.RegressionRow).GetProperties()
            .Where(p => p.Name.EndsWith("RegressionPercent", StringComparison.Ordinal))
            .ToArray();
        Assert.Equal(3, percents.Length);
        Assert.All(percents, p => Assert.Equal(typeof(double?), p.PropertyType));
    }

    /// <summary>Both tools publish the reason beside the null, and a null duration ratio nulls the
    /// severity banded from it rather than letting the TVF's <c>ELSE 'LOW'</c> stand.</summary>
    [Theory]
    [InlineData(DarlingMcp + "/DarlingMcpQueryStoreRegressionTools.cs")]
    [InlineData(LiteMcp + "/McpQueryTools.cs")]
    public void TheRegressionTool_SaysWhyAPercentIsNull_AndDoesNotBandAMissingRatio(string file)
    {
        var body = Strip(ToolBody(ReadRepoFile(file.Split('/')), "get_query_store_regressions"));
        Assert.Contains("undefined_percents = UndefinedPercentNotes(r)", body, StringComparison.Ordinal);
        Assert.Contains("severity = r.DurationRegressionPercent is null ? null : r.Severity", body, StringComparison.Ordinal);

        var description = DescriptionOf(ReadRepoFile(file.Split('/')), "get_query_store_regressions");
        Assert.Contains("undefined_percents", description, StringComparison.Ordinal);
        Assert.Contains("null percent never sorts as 0", description, StringComparison.Ordinal);
    }

    /* ───────────────────────── 3. differenced trends: the first point is unrated ───────────────────────── */

    private static readonly Regex FabricatedFirstPoint = new(@"ELSE 0 END AS \w+_per_second", RegexOptions.Compiled);

    private static IEnumerable<(string Name, string Sql)> DifferencedTrendSql()
    {
        yield return (nameof(DarlingTrendReader.QueryDurationTrendSql), DarlingTrendReader.QueryDurationTrendSql);
        yield return (nameof(DarlingTrendReader.ProcedureDurationTrendSql), DarlingTrendReader.ProcedureDurationTrendSql);
        yield return (nameof(DarlingTrendReader.QueryStoreDurationTrendSql), DarlingTrendReader.QueryStoreDurationTrendSql);
        yield return ("BuildRollupTrendSql(false)", QueryStoreTrendRouting.BuildRollupTrendSql(withDatabaseFilter: false));
        yield return ("BuildRollupTrendSql(true)", QueryStoreTrendRouting.BuildRollupTrendSql(withDatabaseFilter: true));
        /* #3653 A11: the raw query-stats trend the viewer runs (and the MCP raw const's alias-in-waiting) — its
           LAG is the pre-V128 fallback arm of the three-state interval, and the rule below still holds. */
        yield return ("QueryDurationTrendRawSql(false)", DurationTrendRouting.QueryDurationTrendRawSql(withDatabaseFilter: false));
        yield return ("QueryDurationTrendRawSql(true)", DurationTrendRouting.QueryDurationTrendRawSql(withDatabaseFilter: true));
    }

    [Fact]
    public void EveryDifferencedTrend_LeavesTheFirstPointUnrated_NeverZero()
    {
        /* Witness: the shipped shape. */
        Assert.Matches(FabricatedFirstPoint, "CASE WHEN interval_seconds > 0 THEN total_elapsed_ms / interval_seconds ELSE 0 END AS elapsed_ms_per_second,");

        foreach (var (name, sql) in DifferencedTrendSql())
        {
            Assert.Contains("LAG(", sql, StringComparison.Ordinal);
            Assert.DoesNotMatch(FabricatedFirstPoint, sql);
            /* Every rate column is a CASE with no ELSE — NULL where the denominator does not exist. */
            Assert.True(
                Regex.Matches(sql, @"CASE WHEN interval_seconds > 0 THEN [^\n]*? END AS \w+_per_second").Count >= 2,
                $"{name} no longer rates through a no-ELSE CASE");
            /* And the row is KEPT, not filtered: a lone collection is "no rate yet", not an empty window. */
            Assert.DoesNotContain("WHERE interval_seconds > 0", sql, StringComparison.Ordinal);
        }

        /* Lite's four differenced trends, read from source: every `AS interval_seconds` statement — the
           LAG-only Query Store shape (`))) AS interval_seconds`) and the three-state delta-family shape whose
           LAG is the pre-v61 fallback arm (`END AS interval_seconds`, #3540 / #3653 A11) — rates through a
           no-ELSE CASE and none carries the fabricated 0. */
        foreach (var file in new[] { "LocalDataService.QueryStats.cs", "LocalDataService.QueryStore.cs" })
        {
            var lite = ReadRepoFile("Lite", "Services", file);
            Assert.DoesNotMatch(FabricatedFirstPoint, lite);
            var lagged = Regex.Matches(lite, @"(\)\)\)|END) AS interval_seconds").Count;
            var rated = Regex.Matches(lite, @"CASE WHEN interval_seconds > 0 THEN [^\n]*? END AS \w+_per_second").Count;
            Assert.True(lagged >= 1, $"{file}: the differenced-interval idiom is gone, so this pin is looking at nothing");
            Assert.True(rated >= lagged, $"{file}: {lagged} differenced statement(s) but only {rated} no-ELSE rate column(s)");
        }

        /* #3653 A11: Lite's three delta-family trends read the interval the store HAS — the three-state
           MAX(sample_interval_seconds) shape, 0 → NULL, LAG only for a pre-v61 collection — and only the
           Query Store trend (no interval column on its source) keeps the LAG-only shape. */
        var liteQueryStats = ReadRepoFile("Lite", "Services", "LocalDataService.QueryStats.cs");
        Assert.Equal(3, Regex.Matches(liteQueryStats, @"CASE WHEN MAX\(sample_interval_seconds\) IS NULL").Count);
        Assert.Equal(3, Regex.Matches(liteQueryStats, @"ELSE NULLIF\(MAX\(sample_interval_seconds\), 0\)").Count);
        Assert.DoesNotMatch(new Regex(@"\)\)\) AS interval_seconds"), liteQueryStats);
        /* The wrong spelling, in its SQL shape (a COALESCE whose fallback is the LAG derivation): it would fall
           back to a fabricated interval on exactly the restart row the 0 marker flags. */
        Assert.DoesNotMatch(new Regex(@"COALESCE\(NULLIF\(sample_interval_seconds, 0\),\s*(CAST\()?extract"), liteQueryStats);
        var liteQueryStore = ReadRepoFile("Lite", "Services", "LocalDataService.QueryStore.cs");
        Assert.Single(Regex.Matches(liteQueryStore, @"\)\)\) AS interval_seconds"));
        Assert.DoesNotMatch(new Regex(@"MAX\(sample_interval_seconds\)"), liteQueryStore);

        /* The readers carry the null through instead of re-fabricating it. */
        var point = typeof(DarlingTrendReader.QueryDurationTrendPoint);
        Assert.Equal(typeof(double?), point.GetProperty("Value")!.PropertyType);
        Assert.Equal(typeof(long?), point.GetProperty("ExecutionCount")!.PropertyType);
        Assert.Equal(typeof(double?), point.GetProperty("ExecutionsPerSecond")!.PropertyType);
        var litePoint = ReadRepoFile("Lite", "Services", "LocalDataService.QueryStats.cs");
        Assert.Contains("public double? Value { get; set; }", litePoint, StringComparison.Ordinal);
        Assert.Contains("public long? ExecutionCount { get; set; }", litePoint, StringComparison.Ordinal);
        Assert.Contains("public double? ExecutionsPerSecond { get; set; }", litePoint, StringComparison.Ordinal);
    }

    /// <summary>The three trend tools on both SKUs count and explain their unrated points with one sentence.</summary>
    [Theory]
    [InlineData(DarlingMcp + "/DarlingMcpTrendTools.cs")]
    [InlineData(LiteMcp + "/McpQueryTools.cs")]
    public void TheTrendTools_CountAndExplainUnratedPoints(string file)
    {
        var source = Strip(ReadRepoFile(file.Split('/')));
        Assert.Contains("envelope[\"unrated_points\"] = unrated;", source, StringComparison.Ordinal);
        Assert.Contains("Unknowable is not 0", source, StringComparison.Ordinal);
        foreach (var tool in new[] { "get_query_duration_trend", "get_procedure_duration_trend", "get_query_store_duration_trend" })
        {
            Assert.Contains("unrated_points", DescriptionOf(ReadRepoFile(file.Split('/')), tool), StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The Darling viewer shares the rollup builder, so its chart reader must tolerate the NULL first bucket —
    /// by skipping it, since a chart has nowhere to draw "unknown" and coercing it to 0 would be the defect
    /// plotted. (The viewer's OWN raw-route SQL copies still carry <c>ELSE 0</c>; that is the viewer lane's
    /// A2 residual, out of this change's boundary, and not what this pin asserts.)
    /// </summary>
    [Fact]
    public void TheViewerRollupReader_SkipsTheUnratedBucket_RatherThanPlottingZero()
    {
        var viewer = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.QueryTrends.cs");
        var start = viewer.IndexOf("QueryStoreDurationTrendRollupSql);", StringComparison.Ordinal);
        var rollupReader = viewer[start..viewer.IndexOf("return items;", start, StringComparison.Ordinal)];
        Assert.Contains("if (reader.IsDBNull(1))", rollupReader, StringComparison.Ordinal);
        Assert.Contains("continue;", rollupReader, StringComparison.Ordinal);
        Assert.DoesNotContain("IsDBNull(1) ? 0", rollupReader, StringComparison.Ordinal);

        /* Lite's charts do the same with the nullable point. */
        var charts = ReadRepoFile("Lite", "Controls", "ServerTab.Charts.cs");
        Assert.Equal(4, Regex.Matches(charts, @"var rated = data\.Where\(d => d\.HasRate\)\.ToList\(\);").Count);
    }

    /* ───────────────────────── 4. xmin: the window's denominator ───────────────────────── */

    /// <summary>
    /// The MCP share and the alert evaluator's horizon arm fraction over the SAME denominator: the
    /// collector's own SUCCESS rows in <c>collection_log</c>, every time it looked, held or not.
    /// </summary>
    [Fact]
    public void TheXminShare_DividesByEveryCapture_AndAgreesWithTheAlertEvaluator()
    {
        foreach (var sql in new[] { DarlingPgXminReader.XminCapturesInWindowSql, DarlingPostgresAlertReadAdapter.XminSql })
        {
            Assert.Contains("FROM collection_log", sql, StringComparison.Ordinal);
            Assert.Contains("collector_name = 'pg_xmin_horizon'", sql, StringComparison.Ordinal);
            Assert.Contains("status = 'SUCCESS'", sql, StringComparison.Ordinal);
        }

        /* And it is not the holder table: that table has no rows for an unheld capture. */
        var captures = DarlingPgXminReader.XminCapturesInWindowSql;
        Assert.DoesNotContain("pg_xmin_horizon\n", captures, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS captures_in_window", captures, StringComparison.Ordinal);

        var body = Strip(ToolBody(ReadRepoFile(DarlingMcp.Split('/').Append("DarlingMcpPgXminTools.cs").ToArray()), "get_pg_xmin_horizon"));
        Assert.Contains("/ capturesInWindow * 100", body, StringComparison.Ordinal);
        Assert.DoesNotContain("/ r.Samples", body, StringComparison.Ordinal);
        Assert.Contains("captures_in_window = capturesInWindow", body, StringComparison.Ordinal);
        /* No captures and no holders is not an all-clear. */
        Assert.Contains("if (capturesInWindow == 0)", body, StringComparison.Ordinal);
        Assert.Contains("status = \"unavailable\"", body, StringComparison.Ordinal);
        /* Null, not 0, when there is nothing to divide by. */
        Assert.Contains(": (double?)null", body, StringComparison.Ordinal);
    }

    /* ───────────────────────── 5. PVS: a measured zero is a measurement ───────────────────────── */

    [Theory]
    [InlineData(DarlingMcp + "/DarlingMcpPvsTools.cs")]
    [InlineData(LiteMcp + "/McpPvsTools.cs")]
    public void ThePvsShare_DividesAnyMeasuredSize_AndSaysWhyWhenItCannot(string file)
    {
        var body = Strip(ToolBody(ReadRepoFile(file.Split('/')), "get_pvs_stats"));
        /* The defect: only a POSITIVE size divided, so 0 MB fell through to the same null as unmeasured. */
        Assert.DoesNotContain("PvsSizeMb is > 0 &&", body, StringComparison.Ordinal);
        Assert.Contains("r.PvsSizeMb is { } pvsMb && r.DatabaseDataSizeMb is > 0", body, StringComparison.Ordinal);
        Assert.Contains("pvs_measured = r.PvsSizeMb.HasValue", body, StringComparison.Ordinal);
        Assert.Contains("pct_of_database_reason = PctReason(", body, StringComparison.Ordinal);
        /* #3653: the TREND arm carries the same flag per point, on both SKUs. Lite's landed in #3666; the
           Darling twin read the column as a bare double with `IsDBNull ? 0`, so an unmeasured pass was
           published as pvs_size_mb: 0 in the series — a cliff drawn into a series that had none. */
        Assert.Contains("pvs_measured = p.PvsSizeMb.HasValue,", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// The Darling service reader behind the trend arm (#3653): <c>PvsTrendPoint.PvsSizeMb</c> is nullable and
    /// the trend read keeps a NULL as null. Pinned as the record signature and the read expression, the same
    /// two shapes <c>ViewerTrendRoutingPortTests.PvsTrend_BothSkus_NeverPlotAnUnmeasuredPointAsZero</c> pins on
    /// Lite's shared reader, so the two SKUs' MCP trend payloads are built from the same nullable.
    /// </summary>
    [Fact]
    public void TheDarlingPvsTrendReader_CarriesAnUnmeasuredPointAsNull_NeverZero()
    {
        var source = ReadRepoFile(DarlingMcp.Split('/').Append("DarlingPvsReader.cs").ToArray());
        Assert.Contains("double? PvsSizeMb,", source[source.IndexOf("public sealed record PvsTrendPoint(", StringComparison.Ordinal)..], StringComparison.Ordinal);

        var trend = source[source.IndexOf("GetPvsTrendAsync(", StringComparison.Ordinal)..];
        trend = trend[..trend.IndexOf("return rows;", StringComparison.Ordinal)];
        Assert.Contains("reader.IsDBNull(2) ? null : Convert.ToDouble(reader.GetValue(2)),", trend, StringComparison.Ordinal);
        Assert.DoesNotContain("IsDBNull(2) ? 0", trend, StringComparison.Ordinal);
    }

    /// <summary>
    /// One tool, one contract: the two SKUs' <c>get_pvs_stats</c> descriptions are byte-identical and both
    /// teach <c>pvs_measured</c>, so a client learns the same thing about the flag — on the snapshot rows and
    /// on the trend points — whichever SKU answered.
    /// </summary>
    [Fact]
    public void BothSkusPvsDescriptions_StayByteIdentical_AndTeachPvsMeasured()
    {
        var darling = ToolDescription(ToolBody(ReadRepoFile(DarlingMcp.Split('/').Append("DarlingMcpPvsTools.cs").ToArray()), "get_pvs_stats"));
        var lite = ToolDescription(ToolBody(ReadRepoFile(LiteMcp.Split('/').Append("McpPvsTools.cs").ToArray()), "get_pvs_stats"));
        Assert.False(string.IsNullOrEmpty(darling), "could not locate Darling's get_pvs_stats description");
        Assert.Equal(darling, lite);
        Assert.Contains("pvs_measured says whether the DMV reported a size", darling, StringComparison.Ordinal);
    }

    /// <summary>The string literal of a tool body's <c>Description("…")</c> attribute; the PVS tools spell it
    /// on its own line after the attribute's opening, so the match is anchored on the closing <c>")]</c>.</summary>
    private static string ToolDescription(string body) =>
        Regex.Match(body, @"Description\(\s*""((?:[^""\\]|\\.)*)""\)\]", RegexOptions.Singleline).Groups[1].Value;

    [Fact]
    public void ThePvsReason_IsNullOnlyWhenTheShareIsDefined_IncludingADefinedZero()
    {
        Assert.Null(DarlingMcpPvsTools.PctReason(pvsMeasured: true, databaseDataSizeMb: 1280));
        Assert.Contains("unknown — not zero", DarlingMcpPvsTools.PctReason(pvsMeasured: false, databaseDataSizeMb: 1280), StringComparison.Ordinal);
        Assert.Contains("no denominator", DarlingMcpPvsTools.PctReason(pvsMeasured: true, databaseDataSizeMb: null), StringComparison.Ordinal);
        Assert.Contains("no denominator", DarlingMcpPvsTools.PctReason(pvsMeasured: true, databaseDataSizeMb: 0), StringComparison.Ordinal);
    }

    /* ───────────────────────── 6. growth: only over history the store holds ───────────────────────── */

    private static readonly Regex FoldedBaseline = new(@"COALESCE\(p30\.reserved_mb, p7\.reserved_mb|COALESCE\(p7\.reserved_mb, o\.reserved_mb", RegexOptions.Compiled);

    [Fact]
    public void TheGrowthRead_NeverFoldsAMissingBaselineOntoANearerOne_OnEitherSku()
    {
        /* Witness: the shipped chain. */
        Assert.Matches(FoldedBaseline, "l.current_reserved_mb - COALESCE(p30.reserved_mb, p7.reserved_mb, o.reserved_mb, l.current_reserved_mb) AS growth_30d_mb,");

        var darling = DarlingObjectStatsReader.ObjectSizeGrowthSql;
        Assert.DoesNotMatch(FoldedBaseline, darling);
        Assert.Contains("MAX(collection_time) FILTER (WHERE collection_time <= $2) AS snapshot_7d_time", darling, StringComparison.Ordinal);
        Assert.Contains("MAX(collection_time) FILTER (WHERE collection_time <= $3) AS snapshot_30d_time", darling, StringComparison.Ordinal);
        foreach (var column in new[] { "reserved_mb_7d_ago", "reserved_mb_30d_ago", "reserved_mb_oldest", "b.snapshot_7d_time", "b.snapshot_30d_time", "b.earliest_time", "b.latest_time", "b.days_of_data" })
        {
            Assert.Contains(column, darling, StringComparison.Ordinal);
        }
        Assert.DoesNotContain("growth_7d_mb", darling, StringComparison.Ordinal);

        var lite = ReadRepoFile("Lite", "Services", "LocalDataService.FinOps.IndexObjects.cs");
        var read = lite[lite.IndexOf("GetObjectSizeGrowthAsync(int serverId", StringComparison.Ordinal)..];
        read = read[..read.IndexOf("return items;", StringComparison.Ordinal)];
        Assert.DoesNotMatch(FoldedBaseline, read);
        Assert.Contains("MAX(collection_time) FILTER (WHERE collection_time <= $2) AS snapshot_7d_time", read, StringComparison.Ordinal);
        Assert.Contains("ReservedMb30dAgo = reader.IsDBNull(8) ? null", read, StringComparison.Ordinal);

        foreach (var file in new[] { DarlingMcp + "/DarlingMcpObjectStatsTools.cs", LiteMcp + "/McpObjectStatsTools.cs" })
        {
            var body = Strip(ToolBody(ReadRepoFile(file.Split('/')), "get_table_index_sizes"));
            foreach (var key in new[] { "history_days_available", "covers_7d", "covers_30d", "growth_over_available_history_mb", "growth_over_available_history_pct", "growth_window_days", "growth_note = GrowthNote(r)", "tables_returned = page.Count", "truncated," })
            {
                Assert.Contains(key, body, StringComparison.Ordinal);
            }
            /* Truncation observed by over-fetch, never inferred from a full page (A3's rule). */
            Assert.Contains("TableSizesTop + 1", body, StringComparison.Ordinal);
            Assert.Contains("var truncated = rows.Count > TableSizesTop;", body, StringComparison.Ordinal);
        }
    }

    /// <summary>The derivations, executed: each figure comes from exactly the baseline it names or is null.</summary>
    [Fact]
    public void TheGrowthRow_RefusesEveryFigureItsBaselineCannotSupport()
    {
        var earliest = new DateTime(2026, 3, 1, 3, 0, 0);
        var latest = new DateTime(2026, 3, 4, 3, 0, 0);

        /* Three days of history: no 7-day or 30-day snapshot; the oldest holds the table at 100 MB. */
        var threeDays = new DarlingObjectStatsReader.ObjectSizeGrowthRow(
            "db", "dbo", "Posts", CurrentReservedMb: 160, CurrentUsedMb: 150, TotalRows: 10, IndexCount: 2,
            ReservedMb7dAgo: null, ReservedMb30dAgo: null, ReservedMbOldest: 100,
            Snapshot7dTime: null, Snapshot30dTime: null, EarliestSnapshotTime: earliest, LatestSnapshotTime: latest, DaysOfData: 3);
        Assert.Null(threeDays.Growth7dMb);
        Assert.Null(threeDays.Growth30dMb);
        Assert.Null(threeDays.GrowthPct30d);
        Assert.Equal(60, threeDays.GrowthOverAvailableHistoryMb);
        Assert.Equal(60.0, threeDays.GrowthOverAvailableHistoryPct!.Value, 9);
        Assert.Equal(20.0, threeDays.DailyGrowthRateMb!.Value, 9);
        var note = DarlingMcpObjectStatsTools.GrowthNote(threeDays)!;
        Assert.Contains("no snapshot 7+ days old exists", note, StringComparison.Ordinal);
        Assert.Contains("no snapshot 30+ days old exists", note, StringComparison.Ordinal);

        /* The shipped chain would have said growth_30d = 60 (labelled 30d, measured over 3). Now the 30-day
           figure is null and the 3-day figure carries its own name and span. */

        /* A table created since the earliest snapshot: nothing but growth, and the shipped chain said 0. */
        var newTable = threeDays with { ReservedMbOldest = null };
        Assert.Null(newTable.GrowthOverAvailableHistoryMb);
        Assert.Null(newTable.DailyGrowthRateMb);
        Assert.Contains("not in the earliest snapshot", DarlingMcpObjectStatsTools.GrowthNote(newTable)!, StringComparison.Ordinal);

        /* A single day of snapshots: no span, so no growth is knowable — every figure null. */
        var oneDay = threeDays with { DaysOfData = 0, EarliestSnapshotTime = latest };
        Assert.Null(oneDay.GrowthOverAvailableHistoryMb);
        Assert.Null(oneDay.DailyGrowthRateMb);
        Assert.Contains("no growth is knowable yet", DarlingMcpObjectStatsTools.GrowthNote(oneDay)!, StringComparison.Ordinal);

        /* Full history, every baseline present: every figure defined and NO note. */
        var full = threeDays with
        {
            ReservedMb7dAgo = 140, ReservedMb30dAgo = 80, ReservedMbOldest = 50,
            Snapshot7dTime = latest.AddDays(-7), Snapshot30dTime = latest.AddDays(-30), EarliestSnapshotTime = latest.AddDays(-40), DaysOfData = 40,
        };
        Assert.Equal(20, full.Growth7dMb);
        Assert.Equal(80, full.Growth30dMb);
        Assert.Equal(100.0, full.GrowthPct30d!.Value, 9);
        Assert.Equal(110, full.GrowthOverAvailableHistoryMb);
        Assert.Null(DarlingMcpObjectStatsTools.GrowthNote(full));

        /* A 0 baseline has no ratio — the absolute stands, the percent is null with its reason. */
        var wasEmpty = full with { ReservedMb30dAgo = 0 };
        Assert.Equal(160, wasEmpty.Growth30dMb);
        Assert.Null(wasEmpty.GrowthPct30d);
        Assert.Contains("no denominator", DarlingMcpObjectStatsTools.GrowthNote(wasEmpty)!, StringComparison.Ordinal);
    }

    /* ───────────────────────── helpers ───────────────────────── */

    private static string ToolBody(string source, string toolName)
    {
        var marker = $"[McpServerTool(Name = \"{toolName}\")";
        var start = source.IndexOf(marker, StringComparison.Ordinal);
        Assert.True(start >= 0, $"no tool named {toolName} in the source");
        var next = source.IndexOf("[McpServerTool(", start + marker.Length, StringComparison.Ordinal);
        return next < 0 ? source[start..] : source[start..next];
    }

    /// <summary>The Description literal of one tool, whichever side of a line break it sits on (Lite's
    /// get_pvs_stats opens its string on the next line).</summary>
    private static string DescriptionOf(string source, string toolName)
    {
        var body = ToolBody(source, toolName);
        var match = Regex.Match(body, @"Description\(\s*""((?:[^""\\]|\\.)*)""", RegexOptions.Singleline);
        Assert.True(match.Success, $"{toolName} has no Description literal");
        return match.Groups[1].Value;
    }

    private static string Strip(string source) =>
        Regex.Replace(Regex.Replace(source, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline), @"//[^\n]*", string.Empty);
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for #3541 A12, through the real tool methods: the Query Store
/// regression whose baseline read nothing, the xmin holder that won 2 of 6 captures, the PVS row measured at
/// 0 MB beside one not measured at all, and the growth read over three days of history.
/// </summary>
[Collection("live-postgres")]
public sealed class McpZeroIsAMeasurementLivePostgresTests
{
    private const string ServerName = "zero-is-a-measurement-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "ZeroDb";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ARegressionWithNoBaselineReads_StaysNull_AndSaysWhy()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #3541 A12 regression test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* Query 1: baseline read NOTHING (0 logical reads), recent reads 50,000 — the largest possible
               I/O regression, and the row the shipped reader published as io_regression_percent 0. CPU
               regressed 100% so it passes the gate. Query 2: every side defined. */
            await SeedQueryStoreAsync(connection, ct, HoursAgo(30), queryId: 1, intervalId: 1, executions: 10, avgDurationUs: 1_000, avgCpuUs: 500, avgReads: 0);
            await SeedQueryStoreAsync(connection, ct, MinutesAgo(30), queryId: 1, intervalId: 2, executions: 10, avgDurationUs: 2_000, avgCpuUs: 1_000, avgReads: 50_000);
            await SeedQueryStoreAsync(connection, ct, HoursAgo(30), queryId: 2, intervalId: 3, executions: 10, avgDurationUs: 1_000, avgCpuUs: 500, avgReads: 100);
            await SeedQueryStoreAsync(connection, ct, MinutesAgo(30), queryId: 2, intervalId: 4, executions: 10, avgDurationUs: 3_000, avgCpuUs: 1_000, avgReads: 200);

            var root = JsonDocument.Parse(await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, ServerName, hours_back: 24)).RootElement;
            Assert.Equal(2, root.GetProperty("regression_count").GetInt32());

            var rows = root.GetProperty("regressions").EnumerateArray().ToDictionary(r => r.GetProperty("query_id").GetInt64());

            var noBaselineReads = rows[1];
            Assert.Equal(JsonValueKind.Null, noBaselineReads.GetProperty("io_regression_percent").ValueKind);
            Assert.Equal(0, noBaselineReads.GetProperty("baseline_reads").GetDouble());
            Assert.Equal(50_000, noBaselineReads.GetProperty("recent_reads").GetDouble());
            var notes = noBaselineReads.GetProperty("undefined_percents").EnumerateArray().Select(n => n.GetString()!).ToArray();
            Assert.Single(notes);
            Assert.Contains("io_regression_percent is null: no_baseline", notes[0], StringComparison.Ordinal);
            Assert.Contains("NOT 0% change", notes[0], StringComparison.Ordinal);
            /* The other two ratios exist and are unaffected; severity is banded from the duration ratio. */
            Assert.Equal(100.0, noBaselineReads.GetProperty("duration_regression_percent").GetDouble(), 6);
            Assert.Equal(100.0, noBaselineReads.GetProperty("cpu_regression_percent").GetDouble(), 6);
            Assert.Equal("HIGH", noBaselineReads.GetProperty("severity").GetString());   /* 100% duration: > 50, not > 100 */

            var defined = rows[2];
            Assert.Equal(100.0, defined.GetProperty("io_regression_percent").GetDouble(), 6);
            Assert.Equal(JsonValueKind.Null, defined.GetProperty("undefined_percents").ValueKind);

            /* The ranking key is the absolute delta, defined for both — query 2's 20 ms × 10 outranks
               query 1's 10 ms × 10, whatever their ratios. */
            var order = root.GetProperty("regressions").EnumerateArray().Select(r => r.GetProperty("query_id").GetInt64()).ToArray();
            Assert.Equal(new long[] { 2, 1 }, order);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task TheXminShare_IsOverEveryCapture_AndAgreesWithTheAlertAdapter()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #3541 A12 xmin test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE servers SET engine_kind = $2 WHERE server_id = $1", ServerId, MonitoredEngineKind.AuroraPostgres);

            /* No holders and no captures: the collector did not look, so this is NOT "nothing holds the
               horizon". */
            var blind = JsonDocument.Parse(await DarlingMcpPgXminTools.GetPgXminHorizon(postgres, ServerName, hours_back: 4)).RootElement;
            Assert.Equal("unavailable", blind.GetProperty("status").GetString());
            Assert.Equal(0, blind.GetProperty("captures_in_window").GetInt32());

            /* Six successful captures, one failed one (does not count), two of which recorded pid 104 as the
               winner. The shipped payload divided 2 by the source's OWN 2 rows: 100%, "chronic". */
            for (var i = 1; i <= 6; i++)
                await SeedLogAsync(connection, ct, MinutesAgo(i * 10), "SUCCESS");
            await SeedLogAsync(connection, ct, MinutesAgo(70), "ERROR");

            /* Captures only, no holder: the healthy answer, and it names the denominator it rests on. */
            var clear = JsonDocument.Parse(await DarlingMcpPgXminTools.GetPgXminHorizon(postgres, ServerName, hours_back: 4)).RootElement;
            Assert.Equal("no_holder", clear.GetProperty("status").GetString());
            Assert.Equal(6, clear.GetProperty("captures_in_window").GetInt32());
            Assert.Contains("captured 6 time(s)", clear.GetProperty("finding").GetString()!, StringComparison.Ordinal);

            await SeedHolderAsync(connection, ct, MinutesAgo(20), "session", 80_000_000, "104", "state=idle in transaction", isWinner: true);
            await SeedHolderAsync(connection, ct, MinutesAgo(10), "session", 81_000_000, "104", "state=idle in transaction", isWinner: true);

            var held = JsonDocument.Parse(await DarlingMcpPgXminTools.GetPgXminHorizon(postgres, ServerName, hours_back: 4)).RootElement;
            Assert.Equal("holder_present", held.GetProperty("status").GetString());
            Assert.Equal(6, held.GetProperty("captures_in_window").GetInt32());
            var session = held.GetProperty("holders").EnumerateArray().Single(h => h.GetProperty("source").GetString() == "session");
            Assert.Equal(2, session.GetProperty("samples_as_winner").GetInt32());
            Assert.Equal(2, session.GetProperty("captures_recording_this_source").GetInt32());
            Assert.Equal(33.3, session.GetProperty("pct_of_window_winning").GetDouble(), 1);

            /* The alert evaluator's horizon arm counts the same six captures — one denominator, two surfaces. */
            var adapter = new DarlingPostgresAlertReadAdapter(postgres);
            var info = await adapter.GetXminHorizonAsync(ServerId, ct);
            Assert.NotNull(info);
            Assert.Equal(6, info!.CapturesInWindow);
            Assert.Equal(6L, await DarlingPgXminReader.GetXminCapturesInWindowAsync(postgres, ServerId, HoursAgo(4), DateTime.UtcNow, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task AMeasuredZeroPvs_IsAMeasurement_AndAnUnmeasuredOneSaysSo()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #3541 A12 PVS test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var t = MinutesAgo(5);
            /* #3653: an EARLIER pass on which BusyDb's size was not read. The old trend shape published it as
               pvs_size_mb: 0 beside the newest 912.82 — a cliff in a series that had none. */
            var earlier = MinutesAgo(65);
            await SeedPvsAsync(connection, ct, earlier, "CleanDb", pvsSizeMb: 0m, dataSizeMb: 1280m);
            await SeedPvsAsync(connection, ct, earlier, "UnreadDb", pvsSizeMb: null, dataSizeMb: 1280m);
            await SeedPvsAsync(connection, ct, earlier, "BusyDb", pvsSizeMb: null, dataSizeMb: 1280m);
            await SeedPvsAsync(connection, ct, t, "CleanDb", pvsSizeMb: 0m, dataSizeMb: 1280m);
            await SeedPvsAsync(connection, ct, t, "UnreadDb", pvsSizeMb: null, dataSizeMb: 1280m);
            await SeedPvsAsync(connection, ct, t, "BusyDb", pvsSizeMb: 912.82m, dataSizeMb: 1280m);

            var root = JsonDocument.Parse(await DarlingMcpPvsTools.GetPvsStats(postgres, ServerName)).RootElement;
            var byDb = root.GetProperty("databases").EnumerateArray().ToDictionary(d => d.GetProperty("database_name").GetString()!);

            /* 0 MB of 1,280 MB is 0.00% — measured, and said so. Before: pct_of_database null, same as unread. */
            var clean = byDb["CleanDb"];
            Assert.True(clean.GetProperty("pvs_measured").GetBoolean());
            Assert.Equal(0, clean.GetProperty("pvs_size_mb").GetDouble());
            Assert.Equal(0.0, clean.GetProperty("pct_of_database").GetDouble());
            Assert.Equal(JsonValueKind.Null, clean.GetProperty("pct_of_database_reason").ValueKind);

            var unread = byDb["UnreadDb"];
            Assert.False(unread.GetProperty("pvs_measured").GetBoolean());
            Assert.Equal(JsonValueKind.Null, unread.GetProperty("pvs_size_mb").ValueKind);
            Assert.Equal(JsonValueKind.Null, unread.GetProperty("pct_of_database").ValueKind);
            Assert.Contains("not zero", unread.GetProperty("pct_of_database_reason").GetString()!, StringComparison.Ordinal);

            var busy = byDb["BusyDb"];
            Assert.True(busy.GetProperty("pvs_measured").GetBoolean());
            Assert.Equal(71.31, busy.GetProperty("pct_of_database").GetDouble(), 2);

            /* The trend (#3653): the same three verdicts per POINT. BusyDb's unmeasured earlier pass is null
               and flagged, not 0; CleanDb's measured zeros are 0 and flagged measured; UnreadDb is null on
               both. The payload's keys are Lite's (#3666) byte for byte. */
            var withTrend = JsonDocument.Parse(await DarlingMcpPvsTools.GetPvsStats(postgres, ServerName, trend_hours_back: 24)).RootElement;
            Assert.Equal(24, withTrend.GetProperty("trend_hours_back").GetInt32());
            var series = withTrend.GetProperty("trend").EnumerateArray().ToDictionary(s => s.GetProperty("database_name").GetString()!);

            var busyPoints = series["BusyDb"].GetProperty("points").EnumerateArray().ToArray();
            Assert.Equal(2, busyPoints.Length);
            Assert.Equal(JsonValueKind.Null, busyPoints[0].GetProperty("pvs_size_mb").ValueKind);
            Assert.False(busyPoints[0].GetProperty("pvs_measured").GetBoolean());
            Assert.Equal(JsonValueKind.Null, busyPoints[0].GetProperty("pct_of_database").ValueKind);
            Assert.Equal(912.82, busyPoints[1].GetProperty("pvs_size_mb").GetDouble(), 2);
            Assert.True(busyPoints[1].GetProperty("pvs_measured").GetBoolean());
            Assert.Equal(71.31, busyPoints[1].GetProperty("pct_of_database").GetDouble(), 2);

            var cleanPoints = series["CleanDb"].GetProperty("points").EnumerateArray().ToArray();
            Assert.Equal(2, cleanPoints.Length);
            Assert.All(cleanPoints, p =>
            {
                Assert.Equal(0, p.GetProperty("pvs_size_mb").GetDouble());
                Assert.True(p.GetProperty("pvs_measured").GetBoolean());
                Assert.Equal(0.0, p.GetProperty("pct_of_database").GetDouble());
            });

            var unreadPoints = series["UnreadDb"].GetProperty("points").EnumerateArray().ToArray();
            Assert.Equal(2, unreadPoints.Length);
            Assert.All(unreadPoints, p =>
            {
                Assert.Equal(JsonValueKind.Null, p.GetProperty("pvs_size_mb").ValueKind);
                Assert.False(p.GetProperty("pvs_measured").GetBoolean());
            });

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task GrowthOverThreeDaysOfHistory_IsLabelledAsThreeDays_NotThirty()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live #3541 A12 growth test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = MinutesAgo(5);
            var threeDaysAgo = now.AddDays(-3);

            /* Posts: 100 MB three days ago, 160 MB now. Comments: created since — only in the latest snapshot. */
            await SeedIndexAsync(connection, ct, threeDaysAgo, "Posts", reservedMb: 100m);
            await SeedIndexAsync(connection, ct, now, "Posts", reservedMb: 160m);
            await SeedIndexAsync(connection, ct, now, "Comments", reservedMb: 40m);

            var root = JsonDocument.Parse(await DarlingMcpObjectStatsTools.GetTableIndexSizes(postgres, ServerName)).RootElement;

            var history = root.GetProperty("history");
            Assert.Equal(3, history.GetProperty("history_days_available").GetInt32());
            Assert.False(history.GetProperty("covers_7d").GetBoolean());
            Assert.False(history.GetProperty("covers_30d").GetBoolean());
            Assert.Contains("3 day(s) of index snapshots", history.GetProperty("note").GetString()!, StringComparison.Ordinal);
            Assert.Equal(2, root.GetProperty("tables_returned").GetInt32());
            Assert.False(root.GetProperty("truncated").GetBoolean());

            var byTable = root.GetProperty("tables").EnumerateArray().ToDictionary(t => t.GetProperty("table_name").GetString()!);

            /* The shipped read said growth_30d_mb = 60 for Posts — measured over three days, labelled thirty. */
            var posts = byTable["Posts"];
            Assert.Equal(JsonValueKind.Null, posts.GetProperty("growth_7d_mb").ValueKind);
            Assert.Equal(JsonValueKind.Null, posts.GetProperty("growth_30d_mb").ValueKind);
            Assert.Equal(JsonValueKind.Null, posts.GetProperty("growth_pct_30d").ValueKind);
            Assert.Equal(60, posts.GetProperty("growth_over_available_history_mb").GetDouble());
            Assert.Equal(60.0, posts.GetProperty("growth_over_available_history_pct").GetDouble(), 6);
            Assert.Equal(3, posts.GetProperty("growth_window_days").GetInt32());
            Assert.Equal(20.0, posts.GetProperty("daily_growth_rate_mb").GetDouble(), 6);
            Assert.Contains("no snapshot 30+ days old exists", posts.GetProperty("growth_note").GetString()!, StringComparison.Ordinal);

            /* And the shipped read said Comments grew 0 MB — for a table that is nothing but growth. */
            var comments = byTable["Comments"];
            Assert.Equal(JsonValueKind.Null, comments.GetProperty("growth_over_available_history_mb").ValueKind);
            Assert.Equal(JsonValueKind.Null, comments.GetProperty("daily_growth_rate_mb").ValueKind);
            Assert.Contains("not in the earliest snapshot", comments.GetProperty("growth_note").GetString()!, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* ───────────────────────── seeds ───────────────────────── */

    private static DateTime MinutesAgo(int minutes) => DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));
    private static DateTime HoursAgo(int hours) => DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-hours));

    private static Task SeedQueryStoreAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTime, long queryId, long intervalId,
        long executions, long avgDurationUs, long avgCpuUs, long avgReads) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads,
     runtime_stats_interval_id, query_text, last_execution_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
            CollectionIdGenerator.Next(), collectionTime, ServerId, ServerName, Db, queryId, 9L, "Regular",
            executions, avgDurationUs, avgCpuUs, avgReads, intervalId, $"SELECT {queryId}", collectionTime);

    private static Task SeedHolderAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTime, string source, long xminAge, string holder, string? detail, bool isWinner) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO pg_xmin_horizon
    (collection_id, collection_time, server_id, server_name, source, xmin_age, holder, detail, is_winner)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            CollectionIdGenerator.Next(), collectionTime, ServerId, ServerName, source, xminAge, holder, detail, isWinner);

    private static Task SeedLogAsync(NpgsqlConnection connection, CancellationToken ct, DateTime collectionTime, string status) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, 'pg_xmin_horizon', $4, 50, $5, NULL, 0, 40, 10)",
            CollectionIdGenerator.Next(), ServerId, ServerName, collectionTime, status);

    private static Task SeedPvsAsync(NpgsqlConnection connection, CancellationToken ct, DateTime collectionTime, string database, decimal? pvsSizeMb, decimal dataSizeMb) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.pvs_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id, is_accelerated_database_recovery_on,
     persistent_version_store_size_mb, online_index_version_store_size_mb, database_data_size_mb, current_aborted_transaction_count)
VALUES ($1, $2, $3, $4, $5, $6, TRUE, $7, 0, $8, 0)",
            CollectionIdGenerator.Next(), collectionTime, ServerId, ServerName, database, 7, pvsSizeMb, dataSizeMb);

    private static Task SeedIndexAsync(NpgsqlConnection connection, CancellationToken ct, DateTime collectionTime, string table, decimal reservedMb) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO index_object_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows)
VALUES ($1, $2, $3, $4, $5, 'dbo', $6, $7, 1, $8, 'CLUSTERED', $9, $9, 1000)",
            CollectionIdGenerator.Next(), collectionTime, ServerId, ServerName, Db, table.GetHashCode(StringComparison.Ordinal), table, "PK_" + table, reservedMb);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM query_store_stats WHERE server_id = {ServerId}; DELETE FROM pg_xmin_horizon WHERE server_id = {ServerId}; "
            + $"DELETE FROM collection_log WHERE server_id = {ServerId}; DELETE FROM collect.pvs_stats WHERE server_id = {ServerId}; "
            + $"DELETE FROM index_object_stats WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
