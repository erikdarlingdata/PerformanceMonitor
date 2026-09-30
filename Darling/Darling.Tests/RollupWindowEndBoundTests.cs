/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A rollup bucket is stamped at its START. A read that bounds the window end with <c>bucket &lt;= end</c>
/// therefore takes the whole bucket that begins AT the end whenever the end falls exactly on a bucket start: one
/// extra hour in an hourly sum, one extra day on a daily-tier Custom View. Nine reads over the hourly (and, for
/// Custom Views, daily) rollups now bound the end with <c>bucket &lt; end</c>. These are the text-level pins: each
/// read's end parameter is bounded by <c>&lt;</c> and no <c>bucket &lt;=</c> is left in the text. The behavior
/// itself (the hour that starts at the end is not summed) is pinned against real rollups in
/// <c>RollupWindowEndBoundLiveTests</c>.
/// </summary>
public sealed class RollupWindowEndBoundTests
{
    private const string ViewName = "collect.query_stats_hourly";

    /// <summary>The text bounds <c>bucket</c> from above with <c>&lt; $endParam</c> (a bare <c>$N</c>, not a
    /// longer number) and holds no <c>bucket &lt;=</c> anywhere.</summary>
    private static void AssertEndIsExclusive(string sql, int endParam, string because)
    {
        Assert.True(
            Regex.IsMatch(sql, @"\bbucket\s*<\s*\$" + endParam + @"(?!\d)"),
            because + ": the window end is no longer bounded by `bucket < $" + endParam + "`.");
        Assert.False(
            Regex.IsMatch(sql, @"\bbucket\s*<="),
            because + ": a `bucket <=` bound is back, and takes the bucket that starts at the window end.");
    }

    /* Path 1: the Queries tab's hourly arm (a viewer read, so its text is a builder). */
    [Fact]
    public void ViewerTopQueriesHourlySql_StopsBeforeTheWindowEnd()
    {
        var sql = ViewerDataService.BuildTopQueriesHourlySql(ViewName);

        AssertEndIsExclusive(sql, 3, "Queries tab hourly arm");
        Assert.Contains("bucket >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("FROM " + ViewName, sql, StringComparison.Ordinal);
    }

    /* Path 2: the Procedures tab's hourly arm. */
    [Fact]
    public void ViewerTopProceduresHourlySql_StopsBeforeTheWindowEnd()
    {
        var sql = ViewerDataService.BuildTopProceduresHourlySql(ViewName);

        AssertEndIsExclusive(sql, 3, "Procedures tab hourly arm");
        Assert.Contains("bucket >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("FROM " + ViewName, sql, StringComparison.Ordinal);
    }

    /* Paths 3 and 4: the MCP top-queries / top-procedures hourly reads. The materialization-ceiling placeholder
       stays right after the bound, so a known ceiling still narrows the read. */
    [Fact]
    public void McpTopQueriesHourlySql_StopsBeforeTheWindowEnd_AndKeepsTheCeilingSlot()
    {
        var sql = DarlingDataReader.TopQueriesHourlySql;

        AssertEndIsExclusive(sql, 3, "get_top_queries_by_cpu hourly read");
        Assert.Contains("bucket < $3$CEIL$", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void McpTopProceduresHourlySql_StopsBeforeTheWindowEnd_AndKeepsTheCeilingSlot()
    {
        var sql = DarlingDataReader.TopProceduresHourlySql;

        AssertEndIsExclusive(sql, 3, "get_top_procedures_by_cpu hourly read");
        Assert.Contains("bucket < $3$CEIL$", sql, StringComparison.Ordinal);
    }

    /* Path 6: the unbucketed hourly duration trend, both databases-filtered and not, over both rollups. */
    [Theory]
    [InlineData(TimescaleSupport.QueryStatsHourlyView, false)]
    [InlineData(TimescaleSupport.QueryStatsHourlyView, true)]
    [InlineData(TimescaleSupport.ProcedureStatsHourlyView, false)]
    [InlineData(TimescaleSupport.ProcedureStatsHourlyView, true)]
    public void HourlyDurationTrendSql_StopsBeforeTheWindowEnd(string view, bool withDatabaseFilter)
    {
        var sql = DurationTrendRouting.BuildHourlyTrendSql(view, withDatabaseFilter);

        AssertEndIsExclusive(sql, 3, "hourly duration trend");
        Assert.Contains("FROM " + view, sql, StringComparison.Ordinal);
    }

    /* Path 7: the same trend gathered into $4-minute buckets. */
    [Theory]
    [InlineData(TimescaleSupport.QueryStatsHourlyView)]
    [InlineData(TimescaleSupport.ProcedureStatsHourlyView)]
    public void BucketedHourlyDurationTrendSql_StopsBeforeTheWindowEnd(string view)
    {
        var sql = DurationTrendRouting.BuildBucketedHourlyTrendSql(view);

        AssertEndIsExclusive(sql, 3, "bucketed hourly duration trend");
        Assert.Contains("FROM " + view, sql, StringComparison.Ordinal);
    }

    /* Path 8: the Query Store rollup-routed trend. Its rollup arm keeps the partition seam, `bucket < $4`, next to
       the window end, so a bucket is read only when it starts before both. */
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void QueryStoreRollupTrendSql_StopsBeforeTheWindowEnd_AndKeepsTheSeam(bool withDatabaseFilter)
    {
        var sql = QueryStoreTrendRouting.BuildRollupTrendSql(withDatabaseFilter);

        AssertEndIsExclusive(sql, 3, "Query Store rollup trend");
        Assert.Matches(@"\bbucket\s*<\s*\$4(?!\d)", sql);
    }

    /* Path 9: one query's hourly history, over the legacy rollup and its interval-honest successor. */
    [Theory]
    [InlineData(TimescaleSupport.QueryStatsHourlyView)]
    [InlineData("query_stats_interval_hourly")]
    public void QueryHistoryHourlySql_StopsBeforeTheWindowEnd(string relation)
    {
        var sql = DarlingTrendReader.QueryHistoryHourlySqlFor(relation);

        AssertEndIsExclusive(sql, 5, "query history hourly read");
        Assert.Contains("bucket >= $4", sql, StringComparison.Ordinal);
        AssertEndIsExclusive(DarlingTrendReader.QueryHistoryHourlySql, 5, "query history hourly constant");
    }

    /* Path 5: a Custom View panel. The compiler picks the rollup by the window's age; a rollup route bounds its
       `bucket` column with `<`, and a raw route keeps `<=` on the raw time column, where a sample stamped at the
       end still belongs to the window. */
    private static readonly DateTime WindowEnd = new(2026, 7, 18, 6, 0, 0, DateTimeKind.Utc);

    private const string QueryStatsPanel =
        "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}";

    private static ComposeCompiled CompileOverWindowAged(int daysOld)
    {
        var (plan, planError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(QueryStatsPanel)!, Array.Empty<string>());
        Assert.True(planError is null, planError);
        var start = WindowEnd.AddDays(-daysOld);

        /* EndUtc doubles as "now" for the tier decision, exactly as the compiler pins elsewhere do. */
        var (compiled, error) = ComposeCompiler.Compile(
            plan!, new ComposeRunContext(null, start, WindowEnd, ComposeRunContext.NoVariables, RollupAvailability.All, WindowEnd, RollupCoverage.Unknown));
        Assert.True(error is null, error);
        Assert.NotNull(compiled);
        return compiled!;
    }

    [Fact]
    public void ComposeHourlyRoute_StopsBeforeTheWindowEnd()
    {
        var compiled = CompileOverWindowAged(10);

        Assert.Equal(ComposeSourceTier.Hourly, compiled.Route.Tier);
        Assert.Contains("FROM collect.query_stats_hourly AS f", compiled.Sql, StringComparison.Ordinal);
        Assert.Contains("f.bucket >= $1", compiled.Sql, StringComparison.Ordinal);
        Assert.Contains("f.bucket < $2", compiled.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("f.bucket <=", compiled.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ComposeDailyRoute_StopsBeforeTheWindowEnd()
    {
        var compiled = CompileOverWindowAged(120);

        Assert.Equal(ComposeSourceTier.Daily, compiled.Route.Tier);
        Assert.Contains("FROM collect.query_stats_daily AS f", compiled.Sql, StringComparison.Ordinal);
        Assert.Contains("f.bucket >= $1", compiled.Sql, StringComparison.Ordinal);
        Assert.Contains("f.bucket < $2", compiled.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("f.bucket <=", compiled.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ComposeRawRoute_KeepsTheInclusiveEndOnItsRawTimeColumn()
    {
        var compiled = CompileOverWindowAged(1);

        Assert.Equal(ComposeSourceTier.Raw, compiled.Route.Tier);
        Assert.Contains("FROM collect.query_stats AS f", compiled.Sql, StringComparison.Ordinal);
        Assert.Contains("f.collection_time >= $1", compiled.Sql, StringComparison.Ordinal);
        Assert.Contains("f.collection_time <= $2", compiled.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("f.collection_time < $2", compiled.Sql, StringComparison.Ordinal);
    }

    /* The three first-bucket probes are NOT part of the change: they only locate the first bucket the window
       holds (where the served span starts), and a bucket at the very end can be that first bucket only when the
       window holds no other, in which case the read returns no rows either way. */
    [Fact]
    public void FirstBucketProbes_AreUnchanged()
    {
        Assert.Contains("f.bucket <= $3$CEIL$", DarlingDataReader.HourlyFirstBucketSql, StringComparison.Ordinal);
        Assert.Contains("f.bucket <= $3$CEIL$", DarlingDataReader.HourlyFirstBucketSingleRelationSql, StringComparison.Ordinal);
    }
}
