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
/// #2364: <c>get_query_store_top</c> reports the window it actually served.
///
/// <para><b>The defect.</b> It reads raw <c>query_store_stats</c>, and on a store with the rollups armed that
/// table is dropped at four days — measured: a 4d 13h span over 17,162,516 rows under a
/// <c>policy_retention {"drop_after": "4 days"}</c>. A caller asking for 30 days therefore got at most four,
/// with <c>hours_back: 720</c> echoed back unchanged and nothing marking the difference.</para>
///
/// <para><b>Why this could not be fixed the way #2353 was.</b> That one routed to the hourly rollup. Here there
/// is no route: <c>QueryStoreTopSql</c> groups by <c>database_name, query_id, plan_id, query_hash,
/// replica_role</c> and the corrected CAGG groups by <c>database_name, module_name, query_hash</c> — no
/// <c>query_id</c>, no <c>plan_id</c>. Plan identity is the entire purpose of this tool, and a rollup grained to
/// it would approach the size of the raw data. So the fix is honesty, not routing.</para>
/// </summary>
public class QueryStoreTopWindowTests
{
    private static string ReaderSource => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingDataReader.cs"));

    private static string ToolSource => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpDataTools.cs"));

    /// <summary>
    /// The floor probe is bounded on BOTH sides of the window. Bounding the partitioning column is what lets
    /// TimescaleDB exclude chunks; an unbounded <c>MIN</c> would read the retention window to answer a question
    /// about it.
    /// </summary>
    [Fact]
    public void TheWindowFloorProbe_IsBoundedOnBothSides()
    {
        var sql = DarlingDataReader.QueryStoreWindowFloorSql;

        Assert.Contains("MIN(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("FROM query_store_stats", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >=", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <=", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// <b>The floor must come from a probe, never from the returned rows.</b> The result set is the top N by
    /// COST, so its timestamps say nothing about how far back the read reached — the most expensive query in a
    /// month may have run this morning. Deriving the window from the rows would produce a confident, wrong
    /// answer, which is worse than the silence it replaces.
    /// </summary>
    [Fact]
    public void TheEffectiveWindow_ComesFromTheProbe_NotTheRows()
    {
        Assert.Contains("GetQueryStoreWindowFloorAsync", ToolSource, StringComparison.Ordinal);

        var at = ToolSource.IndexOf("var effectiveStart =", StringComparison.Ordinal);
        Assert.True(at >= 0, "the tool no longer computes an effective window (#2364)");

        var line = ToolSource[at..ToolSource.IndexOf(';', at)];
        Assert.Contains("floor", line, StringComparison.Ordinal);

        /* Not from the projection: rows are ordered by cost, and LastExecutionTime is a per-query fact. */
        Assert.DoesNotContain("rows.Min(", ToolSource, StringComparison.Ordinal);
        Assert.DoesNotContain("rows.Max(", ToolSource, StringComparison.Ordinal);
    }

    /// <summary>The payload describes the data, not just the request.</summary>
    [Fact]
    public void ThePayload_CarriesTheServedWindow()
    {
        Assert.Contains("effective_start", ToolSource, StringComparison.Ordinal);
        Assert.Contains("effective_hours_back", ToolSource, StringComparison.Ordinal);
        Assert.Contains("truncated", ToolSource, StringComparison.Ordinal);

        /* hours_back is still echoed — the caller needs to see what it asked for beside what it got. */
        Assert.Contains("hours_back,", ToolSource, StringComparison.Ordinal);
    }

    /// <summary>
    /// The empty path must not assert absence over a span it never read. The old message said Query Store "may
    /// not be enabled", which for a window reaching past raw retention is a confident wrong diagnosis — an agent
    /// acts on it by going to look at a Query Store configuration that is fine.
    /// </summary>
    [Fact]
    public void TheEmptyPath_NamesTheWindowSearched_AndDoesNotOnlyBlameConfiguration()
    {
        var at = ToolSource.IndexOf("No Query Store rows for this server", StringComparison.Ordinal);
        Assert.True(at >= 0, "the empty message no longer names the window searched (#2364)");

        var message = ToolSource[at..Math.Min(ToolSource.Length, at + 700)];

        Assert.Contains("hours_back", message, StringComparison.Ordinal);
        Assert.Contains("4 days", message, StringComparison.Ordinal);
        Assert.Contains("shorter window", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The tool still reads RAW, and that is deliberate rather than an oversight — pinned so a well-meaning
    /// "route it to the rollup like #2353" edit has to confront why it cannot.
    /// </summary>
    [Fact]
    public void TheTool_StillReadsRaw_BecauseTheRollupCannotCarryPlanIdentity()
    {
        Assert.Contains("FROM query_store_stats", DarlingDataReader.QueryStoreTopSql, StringComparison.Ordinal);
        Assert.DoesNotContain("query_store_stats_corrected", DarlingDataReader.QueryStoreTopSql, StringComparison.Ordinal);
        Assert.DoesNotContain("query_store_stats_hourly", DarlingDataReader.QueryStoreTopSql, StringComparison.Ordinal);

        /* plan_id is the reason: it is a grouping key here and absent from every rollup. */
        Assert.Contains("plan_id", DarlingDataReader.QueryStoreTopSql, StringComparison.Ordinal);
    }

    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate);
            }
        }

        throw new FileNotFoundException($"Could not locate {relative} walking up from {thisFile}");
    }
}
