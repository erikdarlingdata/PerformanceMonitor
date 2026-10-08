/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5582: compile-level pins for the Query Store RankedTimeSeries single scan. The gate (which panels read the wide table once),
/// the shape of the base CTE, and a golden copy of the two-scan text, which is the oracle of
/// <see cref="ComposeQueryStoreRankedSingleScanLiveTests"/>: a change to it must be deliberate, so it shows here.
/// </summary>
public sealed class ComposeQueryStoreRankedSingleScanTests
{
    private static string Json(string groupBy, string bucket, string measureKind = "ratio", string key = "qs_total_cpu_us", string extra = "") =>
        "{\"source\":\"query_store_stats\",\"" + measureKind + "\":\"" + key + "\",\"timeBucket\":\"" + bucket + "\",\"topN\":10,\"groupBy\":[\"" + groupBy + "\"],\"viz\":\"line\"" + extra + "}";

    private static string Sql(string json, DateTime start, DateTime end, bool singleScan, bool wide = true)
    {
        var plan = QueryStoreRankedHarness.Parse(json);
        var context = new ComposeRunContext(null, start, end, ComposeRunContext.NoVariables, RollupAvailability.None, end, RollupCoverage.Unknown, QueryStoreWideEligible: wide);
        var (compiled, error) = ComposeCompiler.CompileCore(plan, context, singleScan);
        Assert.True(error is null, error);
        return compiled!.Sql;
    }

    private static readonly DateTime Day = new(2026, 10, 7, 0, 0, 0, DateTimeKind.Unspecified);

    private static int Occurrences(string text, string needle) =>
        (text.Length - text.Replace(needle, string.Empty, StringComparison.Ordinal).Length) / needle.Length;

    [Theory]
    [InlineData("module_name", "hour", 24, true)]
    [InlineData("database_name", "hour", 24, true)]
    [InlineData("server", "hour", 24, true)]
    [InlineData("module_name", "hour", 100, true)]      /* the bound: 100 buckets is still bounded */
    [InlineData("module_name", "hour", 101, false)]     /* one bucket past it */
    [InlineData("module_name", "minute", 24, false)]    /* 1,440 buckets */
    [InlineData("query_hash", "hour", 24, false)]       /* a group as large as the fact rows */
    [InlineData("query_hash", "minute", 1, false)]
    public void TheWideRoute_ReadsTheFactRowsOnce_OnlyWhereTheBaseCteIsBounded(string group, string bucket, int hours, bool single)
    {
        var json = Json(group, bucket);
        var sql = Sql(json, Day, Day.AddHours(hours), singleScan: true);
        Assert.Equal(single, sql.Contains("rank_base AS (", StringComparison.Ordinal));
        Assert.Equal(single ? 1 : 2, Occurrences(sql, "collect.query_store_interval_wide"));
        Assert.Equal(!single, ComposeCompiler.RankedTimeSeriesScansFactRowsTwice(QueryStoreRankedHarness.Parse(json), Day, Day.AddHours(hours)));

        /* The oracle switch is the old text whatever the gate says. */
        var old = Sql(json, Day, Day.AddHours(hours), singleScan: false);
        Assert.DoesNotContain("rank_base", old, StringComparison.Ordinal);
        Assert.Equal(2, Occurrences(old, "collect.query_store_interval_wide"));
    }

    [Fact]
    public void OtherRoutes_AndOtherSources_KeepTheTwoScanText()
    {
        /* The raw dedupe route (wide not eligible). */
        var raw = Sql(Json("module_name", "hour"), Day, Day.AddHours(24), singleScan: true, wide: false);
        Assert.DoesNotContain("rank_base", raw, StringComparison.Ordinal);

        /* A source that is not Query Store. */
        var waits = Sql("{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"topN\":5,\"groupBy\":[\"wait_type\"],\"viz\":\"line\"}",
            Day, Day.AddHours(24), singleScan: true);
        Assert.DoesNotContain("rank_base", waits, StringComparison.Ordinal);

        /* A panel that is not a RankedTimeSeries reads the rows once and never asks for a second scan. */
        var ranked = QueryStoreRankedHarness.Parse("{\"source\":\"query_store_stats\",\"ratio\":\"qs_total_cpu_us\",\"topN\":10,\"groupBy\":[\"query_hash\"],\"viz\":\"table\"}");
        Assert.False(ComposeCompiler.RankedTimeSeriesScansFactRowsTwice(ranked, Day, Day.AddHours(24)));
    }

    [Fact]
    public void TheBaseCte_HoldsThePartialsOnce_AndTheRankAndTheSeriesReadIt()
    {
        var sql = Sql(Json("module_name", "hour", "measure", "qs_executions", ",\"aggregate\":\"avg\""), Day, Day.AddHours(24), singleScan: true);
        Assert.StartsWith("WITH rank_base AS (", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(f.execution_count) AS p0, COUNT(f.execution_count) AS p1", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(b.p0) / NULLIF(SUM(b.p1), 0) AS double precision)", sql, StringComparison.Ordinal);
        Assert.Equal(2, Occurrences(sql, "FROM rank_base AS b"));
        Assert.Equal(1, Occurrences(sql, "collect.query_store_interval_wide"));
    }

    /// <summary>The oracle, byte for byte, for the "top 10 queries by CPU over time" panel of one fleet-day at minute grain.</summary>
    [Fact]
    public void TheOracleText_ForTheFleetDayTopQueriesPanel_IsPinned()
    {
        var sql = Sql(Json("query_hash", "minute"), Day, Day.AddDays(1), singleScan: false).Replace("\r\n", "\n", StringComparison.Ordinal);
        const string Expected = """
            WITH topn AS (
                SELECT f.query_hash AS query_hash, (CAST(SUM(f.avg_cpu_time_us * f.execution_count) AS double precision)) * 1.0 / 1000000.0 AS value
                FROM (SELECT w.*, s.server_name FROM collect.query_store_interval_wide AS w JOIN collect.servers AS s ON s.server_id = w.server_id WHERE w.collection_time >= $1 AND w.collection_time <= $2) AS f
                WHERE f.collection_time >= $1
                  AND f.collection_time <= $2
                GROUP BY f.query_hash
                ORDER BY value DESC NULLS LAST
                LIMIT $3
            )
            SELECT * FROM (
            SELECT date_trunc('minute', f.collection_time) AS bucket, f.query_hash AS query_hash, (CAST(SUM(f.avg_cpu_time_us * f.execution_count) AS double precision)) * 1.0 / 1000000.0 AS value
            FROM (SELECT w.*, s.server_name FROM collect.query_store_interval_wide AS w JOIN collect.servers AS s ON s.server_id = w.server_id WHERE w.collection_time >= $1 AND w.collection_time <= $2) AS f
            WHERE f.collection_time >= $1
              AND f.collection_time <= $2
              AND EXISTS (SELECT 1 FROM topn AS t WHERE t.query_hash IS NOT DISTINCT FROM f.query_hash)
            GROUP BY date_trunc('minute', f.collection_time), f.query_hash
            ORDER BY bucket DESC
            LIMIT 10000
            ) AS capped
            ORDER BY bucket
            """;
        Assert.Equal(Expected.Replace("\r\n", "\n", StringComparison.Ordinal), sql);

        /* The product compiles the same text for it (a query_hash group is not bounded). */
        Assert.Equal(sql, Sql(Json("query_hash", "minute"), Day, Day.AddDays(1), singleScan: true).Replace("\r\n", "\n", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(1.0, 1.0, 0d, true)]
    [InlineData(1.0, 1.0000001, 0d, false)]
    [InlineData(1.0, 1.0000001, 1e-6, true)]
    [InlineData(1.0, 1.01, 1e-6, false)]
    [InlineData(null, null, 0d, true)]
    [InlineData(null, 0.0, 1.0, false)]
    public void TheHarnessComparer_IsExactAtZeroTolerance_AndRelativeAboveIt(double? a, double? b, double tolerance, bool equal) =>
        Assert.Equal(equal, QueryStoreRankedHarness.ValuesEqual(a, b, tolerance));
}
