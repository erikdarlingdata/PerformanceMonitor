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
        Assert.Contains("ORDER BY SUM(worker_time_sum) DESC", sql, StringComparison.Ordinal);
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
        var methodStart = source.IndexOf("private static async Task<List<TopQueryRow>> GetTopQueriesByCpuHourlyAsync", StringComparison.Ordinal);
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

    private static string FindReaderSourcePath()
        => Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingDataReader.cs");

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
}
