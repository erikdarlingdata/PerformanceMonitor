/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The Collection Health tab's Duration Trends chart draws its own read over the whole toolbar range (#4966), not the Collection Log
/// grid's page. The grid keeps the newest <see cref="ViewerDataService.CollectionLogRowCap"/> runs, so on a server logging about 20
/// runs a minute the chart drew the newest ~25 minutes of "Last 24 hours" and left the rest of its axis empty. These are the pins that
/// need no store (the read's shape, the bucket width held to the shared helper, the chart's feed and the lines it draws); the
/// store-backed cases are <see cref="ViewerCollectorDurationChartLiveTests"/>. The web server page draws no chart from the Collection
/// Log, which one pin holds.
/// </summary>
public sealed class ViewerCollectorDurationChartTests
{
    private static readonly DateTime Origin = new(2026, 9, 1, 12, 0, 0, DateTimeKind.Unspecified);

    private static string ViewerFile(string file) => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);

    private static string TabFile() => ViewerFile("ViewerServerTab.CollectionHealth.cs");

    private static string DataServiceFile() => ViewerFile("ViewerDataService.CollectionHealth.cs");

    private static int Matches(string source, string pattern) => Regex.Matches(source, pattern).Count;

    /* From the signature line to the first closing brace at that line's own indent. */
    private static string MethodBody(string source, string signaturePattern)
    {
        var signature = Regex.Match(source, @"(?m)^(?<indent>[ ]*)[^\r\n]*" + signaturePattern);
        Assert.True(signature.Success, $"{signaturePattern} was not found");

        var indent = signature.Groups["indent"].Value;
        var rest = source[signature.Index..];
        var close = Regex.Match(rest, @"\r?\n" + indent + @"\}\r?\n");
        Assert.True(close.Success, $"the end of {signaturePattern} was not found");
        return rest[..(close.Index + close.Length)];
    }

    private static CollectorDurationBucket Bucket(string collector, int minute, int max, double avg = 0, long runs = 1) =>
        new(collector, Origin.AddMinutes(minute), max, avg, runs);

    // ── The chart's feed: its own read, joined with the tab's others, never the grid's page ──

    [Fact]
    public void TheChart_IsFedItsOwnRead_NotTheGridsPage()
    {
        var load = MethodBody(TabFile(), @"private async Task LoadHealthAsync\(");

        /* The chart's read starts beside the grid's, from the same window, and the chart draws what it returned. */
        Assert.Equal(1, Matches(load, @"var durationTask = _dataService\.GetCollectorDurationTrendAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(1, Matches(load, @"RenderCollectorDurationChart\(durationTask\.Result\);"));
        Assert.DoesNotContain("RenderCollectorDurationChart(logTask", load, StringComparison.Ordinal);

        /* The grid's page goes to the grid's filter manager and to its banner, and nowhere else. */
        Assert.Equal(2, Matches(load, @"logTask\.Result"));

        /* The chart cannot be handed the page: its parameter is the buckets, and its body never names a log row. */
        var render = typeof(ViewerServerTab).GetMethod("RenderCollectorDurationChart", BindingFlags.Instance | BindingFlags.NonPublic);
        Assert.NotNull(render);
        Assert.Equal(typeof(List<CollectorDurationBucket>), render!.GetParameters().Single().ParameterType);
        Assert.DoesNotContain("CollectionLogRow", MethodBody(TabFile(), @"private void RenderCollectorDurationChart\("), StringComparison.Ordinal);
    }

    [Fact]
    public void TheGridKeepsItsPage_AndItsCap()
    {
        /* Nothing about the grid's read changed: one call, the default cap, the Collection Log banner naming the same cap. */
        var load = MethodBody(TabFile(), @"private async Task LoadHealthAsync\(");
        Assert.Equal(1, Matches(load, @"var logTask = _dataService\.GetRecentCollectionLogAsync\(_server\.ServerId,\s*startUtc,\s*endUtc\);"));
        Assert.Equal(500, ViewerDataService.CollectionLogRowCap);
    }

    // ── The read: no row cap, one bucket per collector and width, from the shared origin, clamped to the window's start ──

    [Fact]
    public void TheSql_IsUncapped_BucketedFromTheSharedOrigin_AndClampedToTheWindowStart()
    {
        var sql = ViewerDataService.CollectorDurationTrendSql;

        Assert.Contains("FROM v_collection_log", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);

        /* The runs the chart drew before: successful, with a duration. */
        Assert.Contains("status = 'SUCCESS'", sql, StringComparison.Ordinal);
        Assert.Contains("duration_ms IS NOT NULL", sql, StringComparison.Ordinal);

        /* What a bucket carries: the longest and the average duration and the run count, per collector. */
        Assert.Contains("MAX(duration_ms) AS max_duration_ms", sql, StringComparison.Ordinal);
        Assert.Contains("AVG(duration_ms) AS avg_duration_ms", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS run_count", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY collector_name, 2", sql, StringComparison.Ordinal);

        /* The bucket start is the shared origin's, clamped to the window's start as every trend read clamps it. */
        Assert.Contains(
            $"GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {TrendBucketSql.OriginSql}), $2) AS bucket_start",
            sql, StringComparison.Ordinal);

        /* No page: the grid's read is capped by its LIMIT, and this one must not be. */
        Assert.DoesNotContain("LIMIT", sql, StringComparison.OrdinalIgnoreCase);

        /* The file's dialect rules: positional parameters, no bare now(), no N literals. */
        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheRead_BindsItsWindowAsNaiveUtc_AndTheSharedWidth_UnderTheInteractiveDeadline()
    {
        var body = MethodBody(DataServiceFile(), @"public async Task<List<CollectorDurationBucket>> GetCollectorDurationTrendAsync\(");

        Assert.Contains("CreateCommand(CollectorDurationTrendSql)", body, StringComparison.Ordinal);
        Assert.Contains("command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;", body, StringComparison.Ordinal);
        Assert.Matches(@"TypedValue = DateTime\.SpecifyKind\(startUtc,\s*DateTimeKind\.Unspecified\),", body);
        Assert.Matches(@"TypedValue = DateTime\.SpecifyKind\(endUtc,\s*DateTimeKind\.Unspecified\),", body);
        Assert.Equal(1, Matches(body, @"new NpgsqlParameter<int> \{ TypedValue = CollectorDurationBucketMinutes\(startUtc, endUtc\) \}"));

        /* No row cap parameter, and the clock is not read: the window is the caller's. */
        Assert.DoesNotContain("maxRows", body, StringComparison.Ordinal);
        Assert.DoesNotContain("DateTime.UtcNow", body, StringComparison.Ordinal);
    }

    // ── The bucket width: the shared helper's, as the other viewer trend reads size theirs ──

    [Fact]
    public void TheBucketWidth_ForA24HourAndA7DayRange_IsTheSharedHelpers()
    {
        var end = new DateTime(2026, 9, 1, 12, 0, 0, DateTimeKind.Utc);

        foreach (var hours in new[] { 24, 7 * 24 })
        {
            var start = end.AddHours(-hours);
            Assert.Equal(
                TrendBuckets.AutoMinutes(hours * 60, 1, TrendBudget.Chart.AutoPoints),
                ViewerDataService.CollectorDurationBucketMinutes(start, end));
        }

        /* The wider the range, the wider the bucket: a week does not read at a day's width. */
        Assert.True(
            ViewerDataService.CollectorDurationBucketMinutes(end.AddDays(-7), end) > ViewerDataService.CollectorDurationBucketMinutes(end.AddDays(-1), end));
    }

    [Fact]
    public void TheBucketWidth_RoundsAPartialMinuteUp_AndNeverGoesBelowOne()
    {
        var end = new DateTime(2026, 9, 1, 12, 0, 0, DateTimeKind.Utc);

        /* A range is sized in whole minutes, rounding up, as the CPU and blocking trend reads size theirs. */
        Assert.Equal(
            TrendBuckets.AutoMinutes(1441, 1, TrendBudget.Chart.AutoPoints),
            ViewerDataService.CollectorDurationBucketMinutes(end.AddHours(-24).AddSeconds(-30), end));

        /* A range of nothing (a pinned custom range with equal ends) still has a width. */
        Assert.Equal(TrendBuckets.AutoMinutes(1, 1, TrendBudget.Chart.AutoPoints), ViewerDataService.CollectorDurationBucketMinutes(end, end));
    }

    [Fact]
    public void TheWidthFunction_SizesLikeTheOtherViewerTrendReads()
    {
        /* The same call the CPU and blocking trends make: one series, the chart budget. */
        var body = MethodBody(DataServiceFile(), @"public static int CollectorDurationBucketMinutes\(");
        Assert.Matches(@"var windowMinutes = Math\.Max\(1, \(int\)Math\.Ceiling\(\(endUtc - startUtc\)\.TotalMinutes\)\);", body);
        Assert.Matches(@"return TrendBuckets\.AutoMinutes\(windowMinutes,\s*1,\s*TrendBudget\.Chart\.AutoPoints\);", body);

        foreach (var other in new[] { "ViewerDataService.Cpu.cs", "ViewerDataService.BlockingTrends.cs" })
        {
            Assert.Matches(@"TrendBuckets\.AutoMinutes\(windowMinutes,\s*1,\s*TrendBudget\.Chart\.AutoPoints\)", ViewerFile(other));
        }
    }

    // ── The lines the chart draws: each collector's bucket maxima ──

    [Fact]
    public void TheLines_AreEachCollectorsBucketMaxima_InCollectorThenTimeOrder_WithTheAverageAndCountAlongside()
    {
        /* Out of order on purpose: two collectors, buckets shuffled. The average is below the maximum in every bucket. */
        var lines = CollectorDurationSeries.Build(
        [
            Bucket("wait_stats", 20, max: 900, avg: 300.5, runs: 4),
            Bucket("cpu_utilization", 10, max: 50, avg: 40, runs: 10),
            Bucket("wait_stats", 0, max: 120, avg: 60, runs: 10),
            Bucket("wait_stats", 10, max: 5000, avg: 590, runs: 10),
            Bucket("cpu_utilization", 0, max: 70, avg: 45, runs: 9),
        ]);

        Assert.Equal(new[] { "cpu_utilization", "wait_stats" }, lines.Select(l => l.Collector).ToArray());

        var cpu = lines[0];
        Assert.Equal(new[] { Origin.ToOADate(), Origin.AddMinutes(10).ToOADate() }, cpu.Times);
        Assert.Equal(new[] { 70d, 50d }, cpu.MaxMs);
        Assert.Equal(new[] { 45d, 40d }, cpu.AvgMs);
        Assert.Equal(new[] { 9L, 10L }, cpu.Runs);

        /* The slow bucket plots its maximum, not its average: a slow run shows however wide the bucket is. */
        var wait = lines[1];
        Assert.Equal(new[] { Origin.ToOADate(), Origin.AddMinutes(10).ToOADate(), Origin.AddMinutes(20).ToOADate() }, wait.Times);
        Assert.Equal(new[] { 120d, 5000d, 900d }, wait.MaxMs);
        Assert.Equal(new[] { 60d, 590d, 300.5d }, wait.AvgMs);
        Assert.Equal(new[] { 10L, 10L, 4L }, wait.Runs);
    }

    [Fact]
    public void ACollectorWithOneBucket_DrawsNoLine_AsOneRunDidNot()
    {
        var lines = CollectorDurationSeries.Build(
        [
            Bucket("latch_stats", 5, max: 7),
            Bucket("wait_stats", 0, max: 100),
            Bucket("wait_stats", 10, max: 110),
        ]);

        Assert.Equal(new[] { "wait_stats" }, lines.Select(l => l.Collector).ToArray());
        Assert.Empty(CollectorDurationSeries.Build([]));
    }

    [Fact]
    public void TheEmptyRange_ClearsTheHover_BeforeItDrawsNothing()
    {
        /* The old lines leave the plot on every render; the hover must forget them on the empty-range path too, or it tooltips ghosts. */
        var body = MethodBody(TabFile(), @"private void RenderCollectorDurationChart\(");
        var clear = body.IndexOf("_collectorDurationHover?.Clear();", StringComparison.Ordinal);
        var empty = body.IndexOf("if (data.Count == 0)", StringComparison.Ordinal);

        Assert.True(clear >= 0 && empty > clear, "the hover is cleared before the empty-range return");
        Assert.Equal(1, Matches(body, @"_collectorDurationHover\?\.Clear\(\);"));
    }

    // ── The web: its Collection Log is a table, so there is no chart to feed ──

    [Fact]
    public void TheWebServerPage_DrawsNoChartFromTheCollectionLog()
    {
        /* Both server-page tab registries (SQL Server and PostgreSQL) read get_collection_log as a table panel only. A chart drawn from
           this page's capped rows would draw the newest 200 runs against a range's whole axis, as the viewer's did (#4966): give it its
           own bucketed read, as the viewer's chart has. */
        var tabs = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");

        var reads = Matches(tabs, "\"get_collection_log\"");
        Assert.Equal(2, reads);
        Assert.Equal(reads, Matches(tabs, @"table\(\s*""Collection Log"",\s*""get_collection_log"","));
    }
}
