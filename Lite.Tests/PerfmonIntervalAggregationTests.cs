/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2234: Lite's half of the "a zero delta must be readable" contract, and the aggregate that makes it
/// so.
///
/// <para><c>cntr_value</c> and <c>delta_cntr_value</c> are additive across a counter's instance rows —
/// summing Transactions/sec over every database is a meaningful total. <c>sample_interval_seconds</c> is
/// NOT: it is one measured sweep gap repeated once per instance, so <c>SUM</c> multiplies the denominator
/// by the instance count. Measured on the production fleet, Transactions/sec, Log Flushes/sec, Log Bytes
/// Flushed/sec and Log Flush Write Time carry a median of 12 and up to 17 rows per <c>collection_time</c>
/// (the Lock* counters 15), so a summed denominator yields rates 12-17x too LOW.</para>
///
/// <para>Both Darling surfaces carry a dedicated pin for this (<c>DarlingTrendReader</c> and the Viewer's
/// <c>PerfmonTrendsSql</c>). Lite's side is a live multi-instance DuckDB case: the Transactions/sec case of
/// <c>LitePerfmonTotalInstanceLiveTests</c> seeds three instance rows per collection and asserts the interval
/// reads 60 on each read (#7843), and its mixed-object case (two objects under one counter name) pins the per-collection
/// layer the same way (#7771), so a summed interval fails there at either layer. The tests below keep the rest of the
/// interval's contract.</para>
/// </summary>
public sealed class PerfmonIntervalAggregationTests
{
    private const string ReadPath = "Lite/Services/LocalDataService.Perfmon.cs";

    /// <summary>Guards the fix from being over-applied: the two columns that ARE additive are never taken as
    /// MAX, in either read. A well-meant "make it consistent" edit that turned these into MAX would silently
    /// report one instance's value as the whole counter. Which instance rows each read sums is pinned against
    /// an embedded DuckDB in <c>LitePerfmonTotalInstanceLiveTests</c>, not against the statement text.</summary>
    [Fact]
    public void TheAdditiveColumnsStaySummed()
    {
        var source = ParitySource.ReadFile(ReadPath);

        Assert.DoesNotContain("MAX(cntr_value)", source, StringComparison.Ordinal);
        Assert.DoesNotContain("MAX(delta_cntr_value)", source, StringComparison.Ordinal);
    }

    /// <summary>The interval has to reach the caller, or the distinction dies at the API boundary and a
    /// Lite caller is back to an ambiguous zero — the half of #2234 that only landed for Darling first.
    /// </summary>
    [Fact]
    public void LitesPerfmonTrendTool_ProjectsTheInterval()
    {
        /* #3960: the per-collection inline projection this pinned moved into the shared TrendPayloads.PerfmonTrend
           builder (PerformanceMonitor.Common), which both SKUs now call — so the interval reaching the caller is
           proven at the ONE site that projects it, and by McpPerfmonTools.cs calling that builder rather than
           building its own envelope. */
        var tool = ParitySource.ReadFile("Lite/Mcp/McpPerfmonTools.cs");
        Assert.Contains("TrendPayloads.PerfmonTrend(", tool, StringComparison.Ordinal);

        var shared = ParitySource.ReadFile("PerformanceMonitor.Common/Mcp/TrendPayloads.cs");
        Assert.Contains("[\"sample_interval_seconds\"] = seconds", shared, StringComparison.Ordinal);
    }
}
