/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4231 stage 3 pins for <see cref="DarlingDataReader.TopQueriesHourlySql"/> and the routed
/// <c>GetTopQueriesByCpuRoutedAsync</c>'s hourly arm. Purely source-level (no live store): the SQL-shape
/// pin RED before this lane (the const did not exist); the source pin RED if a future edit ever names
/// <c>query_stats_interval_hourly</c> / <c>query_stats_hourly</c> directly in the hourly path instead of
/// going through <see cref="PerformanceMonitor.Darling.Storage.RollupCoverage.StitchedRelationSql"/>.
/// </summary>
public sealed class TopQueriesHourlyRoutingTests
{
    /// <summary>
    /// SQL-shape pin: <see cref="DarlingDataReader.TopQueriesHourlySql"/> groups by
    /// (database_name, query_hash) only — no host_object_name, the rollup has none — and ranks by
    /// SUM(worker_time_sum) DESC, mirroring TopQueriesSql's CPU-ranking promise. RED before this lane: the
    /// const did not exist, so this test would not compile.
    /// </summary>
    [Fact]
    public void TopQueriesHourlySql_GroupsByDatabaseAndQueryHash_RanksByWorkerTimeSum()
    {
        var sql = DarlingDataReader.TopQueriesHourlySql;

        Assert.Contains("GROUP BY database_name, query_hash", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("host_object_name", sql, StringComparison.Ordinal);
        /* The ranking is the const's anchor; the CPU read is the const expanded to the worker-time sum (#5226). */
        Assert.Contains("ORDER BY rank_metric DESC NULLS LAST", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(worker_time_sum) AS rank_metric", TopRankings.Apply(sql, TopRanking.Cpu, hourly: true), StringComparison.Ordinal);
        Assert.Contains(DarlingDataReader.TopQueriesHourlyFromPlaceholder, sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Source pin (the standing gate rule): <see cref="DarlingDataReader.TopQueriesHourlySql"/> reads its
    /// hourly-tier FROM clause ONLY through the <c>$FROM$</c> placeholder — the constant text itself never
    /// names <c>query_stats_interval_hourly</c> or <c>query_stats_hourly</c> literally, and the reader source
    /// file's <c>GetTopQueriesByCpuHourlyAsync</c> method builds that placeholder's substitution ONLY via
    /// <c>RollupCoverage.StitchedRelationSql</c>. RED if a future edit inlines either relation name instead.
    /// </summary>
    [Fact]
    public void TopQueriesHourlySql_NeverNamesRollupRelationDirectly()
    {
        var sql = DarlingDataReader.TopQueriesHourlySql;
        Assert.DoesNotContain("query_stats_interval_hourly", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("query_stats_hourly", sql, StringComparison.Ordinal);

        var readerPath = FindReaderSourcePath();
        var source = File.ReadAllText(readerPath);
        var methodStart = source.IndexOf("private static async Task<(List<TopQueryRow> Rows, DateTime? FirstBucket)> GetTopQueriesByCpuHourlyAsync", StringComparison.Ordinal);
        Assert.True(methodStart >= 0, "GetTopQueriesByCpuHourlyAsync not found in DarlingDataReader.cs — the brief's method name may have changed.");

        // Bound the scan to roughly this one method's body (next top-level "private static" or "public static" after it).
        var nextMemberStart = source.IndexOf("\n    private static async Task<List<ViewerQueryStatsRow", methodStart + 1, StringComparison.Ordinal);
        var searchEnd = source.IndexOf("\n    /* ─────────────────────────── top procedures", methodStart + 1, StringComparison.Ordinal);
        if (searchEnd < 0 || (nextMemberStart >= 0 && nextMemberStart < searchEnd))
        {
            searchEnd = nextMemberStart >= 0 ? nextMemberStart : source.Length;
        }

        var methodBody = source.Substring(methodStart, (searchEnd > methodStart ? searchEnd : source.Length) - methodStart);
        Assert.Contains("coverage.StitchedRelationSql(", methodBody, StringComparison.Ordinal);
        Assert.DoesNotContain("\"query_stats_interval_hourly\"", methodBody, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM query_stats_interval_hourly", methodBody, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM query_stats_hourly", methodBody, StringComparison.Ordinal);
    }

    /// <summary>The ranked read looks the text up in the same statement, over-fetches for WAITFOR shells, carries
    /// sql_handle and selects none of the rollup's per-collection extremes; a raw-only filter forces raw.</summary>
    [Fact]
    public void TopQueriesHourlySql_LooksUpTextInStatement_AndSelectsNoDeltaExtremes()
    {
        var sql = DarlingDataReader.TopQueriesHourlySql;
        Assert.DoesNotContain("LATERAL", sql, StringComparison.Ordinal);   /* #5309: one lookup, not one per row */
        Assert.Contains("latest_in_window AS (", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(sql_handle)", sql, StringComparison.Ordinal);
        Assert.Contains("NOT LIKE 'WAITFOR%'", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $6", sql, StringComparison.Ordinal);   /* #5313: the candidate limit; the ceiling moved to $7 */
        Assert.DoesNotContain("+ 5", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("worker_time_min", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$7", sql, StringComparison.Ordinal);   /* the ceiling is a $CEIL$ placeholder, bound only when known */
        Assert.Null(typeof(DarlingDataReader).GetField("TopQueriesHourlyTextLookupSql",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static));

        var source = File.ReadAllText(FindReaderSourcePath());
        var start = source.IndexOf("public static async Task<TopQueriesReadResult> GetTopQueriesByCpuRoutedAsync", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var body = source.Substring(start, source.IndexOf("GetTopQueriesByCpuHourlyAsync(", start, StringComparison.Ordinal) - start);
        Assert.Contains("tier == RetentionTier.Hourly && (minMaxDop > 0 || rollUpByHostObject)", body, StringComparison.Ordinal);
        Assert.Contains("tier = RetentionTier.Raw;", body, StringComparison.Ordinal);
    }

    /// <summary>The stitched coverage probe is two ordered first-row probes split at the stitch floor, with no
    /// UNION; the single-relation probe reads the spliced relation.</summary>
    [Fact]
    public void HourlyFirstBucketSql_IsLeastOfTwoOrderedFirstRowProbes_WithNoUnion()
    {
        var sql = DarlingDataReader.HourlyFirstBucketSql;
        Assert.Contains("least(", sql, StringComparison.Ordinal);
        Assert.Contains("$LEGACY$", sql, StringComparison.Ordinal);
        Assert.Contains("$SUCCESSOR$", sql, StringComparison.Ordinal);
        Assert.Contains("$4", sql, StringComparison.Ordinal);
        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(sql, "ORDER BY f.bucket LIMIT 1").Count);
        Assert.DoesNotContain("UNION", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("query_stats_interval_hourly", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void HourlyFirstBucketSingleRelationSql_ReadsThePlaceholderRelation_OrderedWithLimitOne()
    {
        var sql = DarlingDataReader.HourlyFirstBucketSingleRelationSql;
        Assert.Contains(DarlingDataReader.TopQueriesHourlyFromPlaceholder, sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY f.bucket LIMIT 1", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("UNION", sql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>The seam splits at the same floor the stitch uses, and both hourly readers go through it.</summary>
    [Fact]
    public void GetHourlyFirstBucketAsync_SplitsAtStitchFloor_AndBothReadersUseIt()
    {
        var source = File.ReadAllText(FindReaderSourcePath());
        var start = source.IndexOf("private static async Task<DateTime?> GetHourlyFirstBucketAsync(", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var end = source.IndexOf("return value is DateTime bucket", start, StringComparison.Ordinal);
        var body = source[start..end];
        Assert.Contains(".StitchFloor(", body, StringComparison.Ordinal);
        Assert.Contains("SuccessorOf(", body, StringComparison.Ordinal);
        Assert.Contains("HourlyFirstBucketSingleRelationSql", body, StringComparison.Ordinal);
        Assert.Contains("\"UNION\"", body, StringComparison.Ordinal);
        Assert.Contains("GetHourlyFirstBucketAsync(postgres, coverage, TimescaleSupport.QueryStatsHourlyView,", source, StringComparison.Ordinal);
        Assert.Contains("GetHourlyFirstBucketAsync(postgres, coverage, TimescaleSupport.ProcedureStatsHourlyView,", source, StringComparison.Ordinal);
    }

    private static string FindReaderSourcePath()
        => Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingDataReader.cs");

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
}
