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
/// #4231 stage 3b pins for <see cref="DarlingDataReader.TopProceduresHourlySql"/> and the routed
/// <c>GetTopProceduresByCpuRoutedAsync</c>'s hourly arm. Purely source-level (no live store): the
/// SQL-shape pin RED before this lane (the const did not exist); the source pin RED if a future edit ever
/// names <c>procedure_stats_interval_hourly</c> / <c>procedure_stats_hourly</c> directly in the hourly path
/// instead of going through <see cref="PerformanceMonitor.Darling.Storage.RollupCoverage.StitchedRelationSql"/>.
/// </summary>
public sealed class TopProceduresHourlyRoutingTests
{
    /// <summary>
    /// SQL-shape pin: <see cref="DarlingDataReader.TopProceduresHourlySql"/> groups by
    /// (database_name, schema_name, object_name) only — no object_type, the rollup has none — and ranks by
    /// SUM(worker_time_sum) DESC, mirroring TopProceduresSql's CPU-ranking promise. RED before this lane: the
    /// const did not exist, so this test would not compile.
    /// </summary>
    [Fact]
    public void TopProceduresHourlySql_GroupsByDatabaseSchemaObject_RanksByWorkerTimeSum()
    {
        var sql = DarlingDataReader.TopProceduresHourlySql;

        Assert.Contains("GROUP BY database_name, schema_name, object_name", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("object_type", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY SUM(worker_time_sum) DESC", sql, StringComparison.Ordinal);
        Assert.Contains(DarlingDataReader.TopProceduresHourlyFromPlaceholder, sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Source pin (the standing gate rule): <see cref="DarlingDataReader.TopProceduresHourlySql"/> reads its
    /// hourly-tier FROM clause ONLY through the <c>$FROM$</c> placeholder — the constant text itself never
    /// names <c>procedure_stats_interval_hourly</c> or <c>procedure_stats_hourly</c> literally, and the
    /// reader source file's <c>GetTopProceduresByCpuHourlyAsync</c> method builds that placeholder's
    /// substitution ONLY via <c>RollupCoverage.StitchedRelationSql</c>. RED if a future edit inlines either
    /// relation name instead.
    /// </summary>
    [Fact]
    public void TopProceduresHourlySql_NeverNamesRollupRelationDirectly()
    {
        var sql = DarlingDataReader.TopProceduresHourlySql;
        Assert.DoesNotContain("procedure_stats_interval_hourly", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("procedure_stats_hourly", sql, StringComparison.Ordinal);

        var readerPath = FindReaderSourcePath();
        var source = File.ReadAllText(readerPath);
        var methodStart = source.IndexOf("private static async Task<List<TopProcedureRow>> GetTopProceduresByCpuHourlyAsync", StringComparison.Ordinal);
        Assert.True(methodStart >= 0, "GetTopProceduresByCpuHourlyAsync not found in DarlingDataReader.cs — the brief's method name may have changed.");

        // Bound the scan to roughly this one method's body (the next top-level member, or end of file).
        var nextMemberStart = source.IndexOf("\n    /* ─────────────────────────── ", methodStart + 1, StringComparison.Ordinal);
        var searchEnd = nextMemberStart >= 0 ? nextMemberStart : source.Length;

        var methodBody = source.Substring(methodStart, (searchEnd > methodStart ? searchEnd : source.Length) - methodStart);
        Assert.Contains("coverage.StitchedRelationSql(", methodBody, StringComparison.Ordinal);
        Assert.DoesNotContain("\"procedure_stats_interval_hourly\"", methodBody, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM procedure_stats_interval_hourly", methodBody, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM procedure_stats_hourly", methodBody, StringComparison.Ordinal);
    }

    private static string FindReaderSourcePath()
        => Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingDataReader.cs");

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
}
