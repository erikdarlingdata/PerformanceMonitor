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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins get_query_heatmap (#2484) — the last of the ten viewer surfaces that had no <c>/api/read</c> endpoint.
///
/// <para>The BUCKETING pins carry the weight here. The whole reason this read exists in the shape it does is
/// that a browser, an agent and the desktop viewer must not draw different pictures of the same server over
/// the same window, and every one of those pictures is decided by two constants: the 5-minute bin the viewer
/// hardcodes, and the seven log-magnitude bands. Both are pinned, on both SKUs, along with the epoch ORIGIN —
/// which is invisible at the default and load-bearing the moment the bin width became a parameter.</para>
/// </summary>
public sealed class DarlingQueryHeatmapSurfaceAndSqlTests
{
    private static readonly string[] ToolSurface =
    {
        "get_query_heatmap",
    };

    private static readonly HeatmapMetric[] AllMetrics =
    {
        HeatmapMetric.Duration, HeatmapMetric.Cpu, HeatmapMetric.LogicalReads,
        HeatmapMetric.LogicalWrites, HeatmapMetric.ExecutionCount,
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpQueryHeatmapTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_IsExactlyTheHeatmapRead()
    {
        var names = ToolMethods()
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(ToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpQueryHeatmapTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(ToolMethods(), m => Assert.True(m.IsStatic, $"{m.Name} must be static"));
        Assert.All(ToolMethods(), m => Assert.True(m.ReturnType == typeof(Task<string>), $"{m.Name} must return Task<string>"));
    }

    [Fact]
    public void ParamContract_AllOptional_MatchesLite()
    {
        var method = ToolMethods().Single();
        var p = method.GetParameters()
            .Where(x => x.GetCustomAttribute<DescriptionAttribute>() is not null)
            .Select(x => (x.Name!, x.HasDefaultValue))
            .ToArray();

        Assert.Equal(
            new[] { "server_name", "hours_back", "metric", "database_name", "bucket_minutes", "limit", "as_of" },
            p.Select(x => x.Item1).ToArray());
        Assert.All(p, x => Assert.True(x.Item2, $"{x.Item1} must be optional"));

        /* #2495's anchor is LAST and shares the one description constant. Pinned by identity rather than by
           name so the parameter cannot drift into a second wording of the same idea on one SKU. */
        var anchor = method.GetParameters().Single(x => x.Name == "as_of");
        Assert.Equal(
            McpHelpers.AsOfDescription,
            anchor.GetCustomAttribute<DescriptionAttribute>()!.Description);
    }

    /// <summary>
    /// The bin width defaults to the desktop viewer's 5 minutes, and that default is the point of the read
    /// rather than a convenience. A browser and a desktop pointed at the same server over the same window
    /// drawing different grids would be worse than the browser drawing nothing.
    /// </summary>
    [Fact]
    public void TheDefaultBucket_IsTheViewers_FiveMinutes()
    {
        Assert.Equal(5, DarlingQueryHeatmapReader.ViewerBucketMinutes);

        var bucketParam = ToolMethods().Single().GetParameters().Single(p => p.Name == "bucket_minutes");
        Assert.Equal(DarlingQueryHeatmapReader.ViewerBucketMinutes, bucketParam.DefaultValue);
    }

    /// <summary>
    /// The bin width is a bound parameter and the ORIGIN is the Unix epoch, spelled out.
    /// <para>The origin is not decoration. Postgres <c>date_bin</c> requires one; DuckDB's <c>time_bucket</c>
    /// defaults to a DIFFERENT one (2000-01-03), and the two are 15,780,960 minutes apart. 5 divides that
    /// exactly, so the default agrees by luck — 7 does not, and the two SKUs would bin the same row a minute
    /// apart the first time anyone passed an odd bucket_minutes. Lite's twin passes the epoch explicitly for
    /// this reason.</para>
    /// </summary>
    [Fact]
    public void HeatmapSql_BinsWithABoundWidth_FromTheUnixEpoch_ForEveryMetric()
    {
        foreach (var metric in AllMetrics)
        {
            var sql = DarlingQueryHeatmapReader.BuildQueryHeatmapSql(metric);
            Assert.Contains(
                "date_bin(($5::integer * INTERVAL '1 minute'), collection_time, TIMESTAMP '1970-01-01 00:00:00') AS time_bin",
                sql, StringComparison.Ordinal);

            /* Not the literal the viewer carries: the width has to move for a caller to widen it. */
            Assert.DoesNotContain("INTERVAL '5 minutes'", sql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The viewer's magnitude CASE, its filters and its 120-character preview, kept verbatim. These decide
    /// which cell a query lands in, so they are the other half of "the two surfaces agree".
    /// </summary>
    [Fact]
    public void HeatmapSql_KeepsTheViewersMagnitudeBands_Filters_AndPreview()
    {
        var sql = DarlingQueryHeatmapReader.BuildQueryHeatmapSql(HeatmapMetric.Duration);

        foreach (var band in new[]
                 {
                     "WHEN metric_value < 1 THEN 0",
                     "WHEN metric_value < 10 THEN 1",
                     "WHEN metric_value < 100 THEN 2",
                     "WHEN metric_value < 1000 THEN 3",
                     "WHEN metric_value < 10000 THEN 4",
                     "WHEN metric_value < 100000 THEN 5",
                     "ELSE 6",
                 })
        {
            Assert.Contains(band, sql, StringComparison.Ordinal);
        }

        Assert.Contains("delta_execution_count > 0", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT(query_text, 120) AS query_preview", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_query_stats", sql, StringComparison.Ordinal);

        /* DuckDB's ARG_MAX has no Postgres equivalent; the viewer's replacement is a top-1 window over the
           cell, with the cell's count carried alongside so one pass yields both. */
        Assert.Contains("ROW_NUMBER() OVER (PARTITION BY time_bin, bucket_index ORDER BY delta_execution_count DESC)", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) OVER (PARTITION BY time_bin, bucket_index)", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE rn = 1", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The cap orders NEWEST bin first. An MCP read has a row cap the desktop chart does not, and cutting the
    /// recent end of an incident window is the one thing a capped heatmap must not do.
    /// </summary>
    [Fact]
    public void HeatmapSql_CapsFromTheRecentEnd()
    {
        var sql = DarlingQueryHeatmapReader.BuildQueryHeatmapSql(HeatmapMetric.Cpu);
        Assert.Contains("ORDER BY time_bin DESC, bucket_index", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $6", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(HeatmapMetric.Duration, "(delta_elapsed_time / 1000.0) / NULLIF(delta_execution_count, 0)")]
    [InlineData(HeatmapMetric.Cpu, "(delta_worker_time / 1000.0) / NULLIF(delta_execution_count, 0)")]
    [InlineData(HeatmapMetric.LogicalReads, "CAST(delta_logical_reads AS DOUBLE PRECISION) / NULLIF(delta_execution_count, 0)")]
    [InlineData(HeatmapMetric.LogicalWrites, "CAST(delta_logical_writes AS DOUBLE PRECISION) / NULLIF(delta_execution_count, 0)")]
    [InlineData(HeatmapMetric.ExecutionCount, "CAST(delta_execution_count AS DOUBLE PRECISION)")]
    public void MetricExpressions_AreTheViewersVerbatim(HeatmapMetric metric, string expr)
    {
        Assert.Equal(expr, DarlingQueryHeatmapReader.MetricExpression(metric));
        Assert.Contains(expr, DarlingQueryHeatmapReader.BuildQueryHeatmapSql(metric), StringComparison.Ordinal);
    }

    /// <summary>Seven bands for every metric, labelled in the metric's own unit — milliseconds for duration
    /// and CPU, plain counts for the other three. A caller cannot read a bucket_index without them.</summary>
    [Fact]
    public void BucketLabels_AreSeven_PerMetric_AndSplitByUnit()
    {
        foreach (var metric in AllMetrics)
        {
            Assert.Equal(DarlingQueryHeatmapReader.BucketCount, DarlingQueryHeatmapReader.BucketLabels[metric].Length);
        }

        Assert.Equal(
            new[] { "0-1ms", "1-10ms", "10-100ms", "100ms-1s", "1-10s", "10-100s", ">100s" },
            DarlingQueryHeatmapReader.BucketLabels[HeatmapMetric.Duration]);
        Assert.Equal(
            DarlingQueryHeatmapReader.BucketLabels[HeatmapMetric.Duration],
            DarlingQueryHeatmapReader.BucketLabels[HeatmapMetric.Cpu]);
        Assert.Equal(
            new[] { "0-1", "1-10", "10-100", "100-1K", "1K-10K", "10K-100K", ">100K" },
            DarlingQueryHeatmapReader.BucketLabels[HeatmapMetric.LogicalReads]);
    }

    /// <summary>
    /// A metric name we do not know is REFUSED. Falling back to duration would hand a caller who asked about
    /// CPU a grid of elapsed time with nothing anywhere saying so.
    /// </summary>
    [Theory]
    [InlineData("duration", HeatmapMetric.Duration)]
    [InlineData("CPU", HeatmapMetric.Cpu)]
    [InlineData(" logical_reads ", HeatmapMetric.LogicalReads)]
    [InlineData("logical_writes", HeatmapMetric.LogicalWrites)]
    [InlineData("execution_count", HeatmapMetric.ExecutionCount)]
    [InlineData(null, HeatmapMetric.Duration)]
    [InlineData("", HeatmapMetric.Duration)]
    public void KnownMetricNames_Parse(string? name, HeatmapMetric expected)
    {
        Assert.True(DarlingQueryHeatmapReader.TryParseMetric(name, out var parsed));
        Assert.Equal(expected, parsed);

        /* And the name round-trips, so the echoed `metric` in a result is the one a caller can pass back. */
        Assert.True(DarlingQueryHeatmapReader.TryParseMetric(DarlingQueryHeatmapReader.MetricName(parsed), out var again));
        Assert.Equal(parsed, again);
    }

    [Theory]
    [InlineData("reads")]
    [InlineData("logicalreads")]
    [InlineData("elapsed")]
    public void UnknownMetricNames_AreRefused(string name)
    {
        Assert.False(DarlingQueryHeatmapReader.TryParseMetric(name, out _));
    }

    /// <summary>
    /// The coverage probe reads the SAME relation the heatmap reads, and answers BOTH questions in one round
    /// trip. It probes the DATA rather than SUCCESS runs in collection_log, which is a judgement about the
    /// table: query_stats is PERIODIC, so no rows really does mean nobody looked.
    /// </summary>
    [Fact]
    public void CoverageSql_ProbesTheSameView_ForEverAndForTheWindow()
    {
        var sql = DarlingQueryHeatmapReader.HeatmapCoverageSql;
        Assert.Equal(2, CountOf(sql, "FROM v_query_stats"));
        Assert.Equal(2, CountOf(sql, "EXISTS ("));
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);

        /* The read itself is over the view, so the probe must be too - a probe on the base table could say
           "collected" about rows the read cannot see. */
        Assert.Contains("FROM v_query_stats", DarlingQueryHeatmapReader.BuildQueryHeatmapSql(HeatmapMetric.Duration), StringComparison.Ordinal);

        /* The "ever" arm is deliberately UNBOUNDED - it is the question "did anyone ever collect this
           server", and bounding it would make a quiet window look like a missing collector. */
        var everArm = sql[..sql.IndexOf(") AS has_any", StringComparison.Ordinal)];
        Assert.DoesNotContain("collection_time", everArm, StringComparison.Ordinal);
        Assert.Contains(") AS has_in_window", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Reads_ArePostgresDialect_NoTsqlIsms()
    {
        foreach (var sql in AllMetrics.Select(DarlingQueryHeatmapReader.BuildQueryHeatmapSql)
                     .Append(DarlingQueryHeatmapReader.HeatmapCoverageSql))
        {
            var lower = sql.ToLowerInvariant();
            Assert.DoesNotContain("getdate", lower);
            Assert.DoesNotContain("top (", lower);
            Assert.DoesNotContain("isnull(", lower);
            Assert.DoesNotContain("time_bucket", lower);
            Assert.DoesNotContain("arg_max", lower);
            Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
            Assert.Contains("$1", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ReadColumns_ExistInTheGeneratedCollectorTable()
    {
        var store = PgSchemaGenerator.CreateTable(QueryStatsCollector.Instance);
        Assert.Equal("query_stats", QueryStatsCollector.Instance.TargetTable);
        foreach (var col in new[]
                 {
                     "collection_time", "database_name", "query_hash", "query_text",
                     "delta_execution_count", "delta_worker_time", "delta_elapsed_time",
                     "delta_logical_reads", "delta_logical_writes",
                 })
        {
            Assert.Contains(col, store, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_NoRequiredParams()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpQueryHeatmapTools>();
        using var provider = services.BuildServiceProvider();
        var tools = provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();

        Assert.Equal(ToolSurface.Length, tools.Count);
        var violations = tools.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));
        Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(tools[0].InputSchema));
    }

    private static int CountOf(string haystack, string needle)
    {
        var count = 0;
        var i = haystack.IndexOf(needle, StringComparison.Ordinal);
        while (i >= 0)
        {
            count++;
            i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal);
        }
        return count;
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trip for get_query_heatmap.
///
/// <para>Three kinds of nothing, a real grid, and the two things a capped grid must get right: the cap keeps
/// the RECENT end of the window, and it never hands back a column with holes in it. A partial column reads as
/// "nothing fast ran then" rather than "we stopped looking", which is the kind of quiet wrong answer a grid
/// makes very easy to believe.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingQueryHeatmapLiveTests
{
    private const int ServerId = -949557;
    private const string ServerName = "query-heatmap";
    private const string Db = "AppDb";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ThreeKindsOfNothing_AndARealGrid_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live heatmap test.");

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
            var baseNow = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

            /* ── 1. nothing ever collected ── */
            var never = Root(await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(postgres, ServerName));
            Assert.Equal("unavailable", never.GetProperty("status").GetString());
            var neverText = never.GetProperty("message").GetString()!;
            Assert.Contains("EVER", neverText, StringComparison.Ordinal);
            Assert.Contains("PERIODIC table rather than an edge table", neverText, StringComparison.Ordinal);

            /*
                ── 2. rows exist, but not in the window ──
                Seeded FIRST, and the ordering is load-bearing: once a row exists 30 minutes ago no window a
                caller can legally ask for excludes it, so this branch is unreachable afterwards. The states
                are walked by moving hours_back over one growing set of rows.
            */
            await SeedAsync(connection, ct, baseNow.AddHours(-40), "0xOLD", deltaExec: 4, deltaElapsed: 200_000);

            var noWindow = Root(await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(postgres, ServerName, 2));
            Assert.Equal("empty", noWindow.GetProperty("status").GetString());
            var noWindowText = noWindow.GetProperty("message").GetString()!;
            Assert.Contains("Widen hours_back", noWindowText, StringComparison.Ordinal);
            Assert.DoesNotContain("EVER", noWindowText, StringComparison.Ordinal);

            /*
                ── 3. collected in the window, and every capture recorded ZERO executions ──
                The branch only this read has. Collection is healthy and the server is idle; telling the
                caller to widen the window here would point them at the wrong problem.
            */
            await SeedAsync(connection, ct, baseNow.AddMinutes(-30), "0xIDLE", deltaExec: 0, deltaElapsed: 999_000);

            var idle = Root(await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(postgres, ServerName, 24));
            Assert.Equal("empty", idle.GetProperty("status").GetString());
            var idleText = idle.GetProperty("message").GetString()!;
            Assert.Contains("zero execution delta", idleText, StringComparison.Ordinal);
            Assert.DoesNotContain("Widen hours_back", idleText, StringComparison.Ordinal);
            Assert.DoesNotContain("EVER", idleText, StringComparison.Ordinal);

            /* Three sentences, not one sentence three times. */
            var messages = new[] { neverText, noWindowText, idleText };
            Assert.Equal(3, messages.Distinct(StringComparer.Ordinal).Count());

            /*
                ── a real grid ──
                Two queries at ~50 ms/exec share one 5-minute bin's bucket 2; a third at 0.5 ms/exec lands in
                bucket 0 of the SAME bin. A fourth sits 35 minutes later, in its own bin. The zero-execution
                row from state 3 must contribute to no cell at all.
            */
            /*
                Floored to the hour, and that is not tidiness. date_bin aligns bins to the epoch, so an
                unfloored "three hours ago" lands at an arbitrary minute and the three rows below straddle a
                5-minute boundary roughly three times in five - and the 60-minute assertion further down
                straddles an hour boundary whenever t1 sits past :25. Both would fail on a clock, not on a
                defect.
            */
            var t1 = FloorToHour(baseNow.AddHours(-3));
            await SeedAsync(connection, ct, t1, "0xHOT", deltaExec: 5, deltaElapsed: 250_000);
            await SeedAsync(connection, ct, t1.AddMinutes(1), "0xCOLD", deltaExec: 1, deltaElapsed: 50_000);
            await SeedAsync(connection, ct, t1.AddMinutes(2), "0xLOW", deltaExec: 2, deltaElapsed: 1_000);
            await SeedAsync(connection, ct, t1.AddMinutes(35), "0xNEXT", deltaExec: 3, deltaElapsed: 90_000);

            var grid = Root(await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(postgres, ServerName, 24));
            Assert.Equal(ServerName, grid.GetProperty("server").GetString());
            Assert.Equal("duration", grid.GetProperty("metric").GetString());
            Assert.Equal(5, grid.GetProperty("bucket_minutes").GetInt32());
            Assert.True(grid.GetProperty("bucket_minutes_matches_desktop_viewer").GetBoolean());
            Assert.Equal(2, grid.GetProperty("time_bin_count").GetInt32());
            Assert.Equal(3, grid.GetProperty("cell_count").GetInt32());
            Assert.False(grid.GetProperty("truncated").GetBoolean());
            Assert.Equal(7, grid.GetProperty("magnitude_buckets").GetArrayLength());

            var cells = grid.GetProperty("cells").EnumerateArray().ToArray();

            /* Chronological, whatever order the cap fetched them in. */
            var times = cells.Select(c => DateTime.Parse(c.GetProperty("time_bucket").GetString()!,
                System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.RoundtripKind)).ToArray();
            Assert.Equal(times.OrderBy(t => t).ToArray(), times);

            /*
                The reported window has to be the window the read USED. Here it is by construction — the
                tool computes start/end once and passes them into the query — and the pin keeps it that
                way: computed after the read instead, it would drift by the read's own duration, on the one
                read whose entire output is a time axis.
            */
            var windowStart = ParseUtc(grid.GetProperty("window_start").GetString()!);
            var windowEnd = ParseUtc(grid.GetProperty("window_end").GetString()!);
            Assert.Equal(24.0, (windowEnd - windowStart).TotalHours, 3);
            Assert.True(windowStart <= times[0], "window_start is after the first bin the read returned");
            Assert.True(windowEnd >= times[^1], "window_end is before the last bin the read returned");

            /* The first bin: bucket 0 holds the 0.5 ms query, bucket 2 holds the two ~50 ms ones, and the
               cell's top query is the most-EXECUTED of them rather than the slowest. */
            var bucket0 = cells.Single(c => c.GetProperty("bucket_index").GetInt32() == 0);
            Assert.Equal(1, bucket0.GetProperty("query_count").GetInt64());
            Assert.Equal("0xLOW", bucket0.GetProperty("top_query_hash").GetString());
            Assert.Equal("0-1ms", bucket0.GetProperty("bucket_label").GetString());

            var bucket2First = cells.First(c => c.GetProperty("bucket_index").GetInt32() == 2);
            Assert.Equal(2, bucket2First.GetProperty("query_count").GetInt64());
            Assert.Equal("0xHOT", bucket2First.GetProperty("top_query_hash").GetString());
            Assert.Equal("10-100ms", bucket2First.GetProperty("bucket_label").GetString());

            /* ── a coarser bin collapses the two columns into one ── */
            var coarse = Root(await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(postgres, ServerName, 24, null, null, 60));
            Assert.Equal(60, coarse.GetProperty("bucket_minutes").GetInt32());
            Assert.False(coarse.GetProperty("bucket_minutes_matches_desktop_viewer").GetBoolean());
            Assert.Equal(1, coarse.GetProperty("time_bin_count").GetInt32());
            Assert.Equal(2, coarse.GetProperty("cell_count").GetInt32());

            /* ── another metric is a different grid, not the same one relabelled ── */
            var execCount = Root(await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(postgres, ServerName, 24, "execution_count"));
            Assert.Equal("execution_count", execCount.GetProperty("metric").GetString());
            Assert.Equal("0-1", execCount.GetProperty("magnitude_buckets")[0].GetProperty("label").GetString());
            Assert.Contains("a total, not a per-execution average", execCount.GetProperty("metric_unit").GetString()!, StringComparison.Ordinal);

            /*
                ── the anchor moves the window, and the resolved instant reaches the QUERY ──
                #2495's own failure mode is a tool that takes as_of, validates it, refuses a bad one
                correctly, and then queries NOW — the validation succeeding is what makes the caller believe
                the window moved. So this proves it by CONTENT: one hour ending half an hour after the
                seeded rows returns them, and the same one-hour window at the default anchor cannot.
            */
            var anchor = DateTime.SpecifyKind(t1.AddMinutes(30), DateTimeKind.Utc).ToString("o");
            var anchored = Root(await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(
                postgres, ServerName, 1, null, null, 5, 500, anchor));

            Assert.Equal(1, anchored.GetProperty("time_bin_count").GetInt32());
            Assert.Equal(2, anchored.GetProperty("cell_count").GetInt32());
            Assert.Equal(anchor, anchored.GetProperty("window_end").GetString());

            /* The same LENGTH of window at the default anchor sees nothing but the idle row — so it is the
               anchor doing the work, not hours_back. Widening hours_back instead would be a different
               question anyway: on a heatmap the extra hours arrive as extra COLUMNS. */
            var unanchored = Root(await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(postgres, ServerName, 1));
            Assert.Equal("empty", unanchored.GetProperty("status").GetString());

            /* An anchor we cannot use is refused, never silently treated as now. */
            var badAnchor = await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(
                postgres, ServerName, 1, null, null, 5, 500, "last tuesday");
            Assert.Contains("Invalid as_of", badAnchor, StringComparison.Ordinal);

            /* ── the caps and the bounds REFUSE out of range rather than quietly rewriting them ── */
            var refusedRows = await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(postgres, ServerName, 24, null, null, 5, 5000);
            Assert.Contains("exceeds maximum of 1000", refusedRows, StringComparison.Ordinal);

            var refusedBucket = await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(postgres, ServerName, 24, null, null, 0);
            Assert.Contains("Must be between 1 and 1440", refusedBucket, StringComparison.Ordinal);

            var refusedMetric = await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(postgres, ServerName, 24, "reads");
            Assert.Contains("Invalid metric 'reads'", refusedMetric, StringComparison.Ordinal);

            /*
                ── truncation keeps the RECENT end, and hands back no partial column ──
                Cap of 2 against a 3-cell grid whose oldest bin holds 2 of those cells: the newest bin's one
                cell fits, the oldest bin does not fit whole, so it is dropped rather than shown with a hole.
            */
            var capped = Root(await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(postgres, ServerName, 24, null, null, 5, 2));
            Assert.True(capped.GetProperty("truncated").GetBoolean());
            Assert.Equal(1, capped.GetProperty("time_bin_count").GetInt32());
            Assert.Equal(1, capped.GetProperty("cell_count").GetInt32());
            Assert.Equal(
                "0xNEXT",
                capped.GetProperty("cells")[0].GetProperty("top_query_hash").GetString());

            /* first/last say which slice came back, so the missing part of the window is visible. */
            Assert.Equal(
                capped.GetProperty("first_time_bin").GetString(),
                capped.GetProperty("last_time_bin").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;

    private static DateTime ParseUtc(string value) => DateTime.Parse(
        value, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind);

    /// <summary>Floors to the top of the hour, which is also a 5-minute boundary on date_bin's epoch grid
    /// (the minutes from year 1 to 1970 divide by both 5 and 60), so the seeded rows land where the test
    /// says they do whatever time CI runs at.</summary>
    private static DateTime FloorToHour(DateTime value) =>
        new(value.Ticks - (value.Ticks % TimeSpan.TicksPerHour), value.Kind);

    private static async Task SeedAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime collectionTime, string queryHash,
        long deltaExec, long deltaElapsed) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash,
     sample_interval_seconds, delta_execution_count, delta_worker_time, delta_elapsed_time,
     delta_logical_reads, delta_logical_writes, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTime), ServerId, ServerName,
            Db, queryHash, 60, deltaExec, 0L, deltaElapsed, 0L, 0L, "SELECT * FROM dbo.Widgets");

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM query_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
