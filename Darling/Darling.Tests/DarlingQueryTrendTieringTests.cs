/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2353: <c>get_query_trend</c> reads the tier that can actually serve the window, and says which one it read.
///
/// <para><b>The defect.</b> The read went to raw <c>query_stats</c> only. The raw tier of a ROLLED table is
/// dropped at <see cref="TimescaleSupport.RawRetentionSpan"/> — four days — independently of the collector's
/// much longer advertised retention, so a request for 168 hours returned whatever had not aged out while
/// echoing <c>hours_back: 168</c> back unchanged. Worse than the short array: the empty path asserted "No
/// history found ... within the last 168 hours", which for a query whose history had simply aged out is a
/// false statement rather than an incomplete one, and an agent acts on it by concluding the query never ran.</para>
/// </summary>
public class DarlingQueryTrendTieringTests
{
    private static readonly DateTime Now = new(2026, 8, 19, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>A window inside the raw horizon keeps per-collection resolution — the common case must not regress.</summary>
    [Theory]
    [InlineData(1)]
    [InlineData(24)]
    [InlineData(48)]
    [InlineData(70)]
    public void AWindowInsideTheRawHorizon_UsesRaw(int hoursBack)
    {
        Assert.True(DarlingTrendReader.ShouldUseRawTier(Now.AddHours(-hoursBack), Now));
    }

    /// <summary>
    /// The reported case: 168 hours cannot come from a tier that keeps four days, so it must not be asked to.
    /// </summary>
    [Theory]
    [InlineData(96)]
    [InlineData(168)]
    [InlineData(720)]
    public void AWindowReachingPastTheRawHorizon_UsesTheAggregate(int hoursBack)
    {
        Assert.False(DarlingTrendReader.ShouldUseRawTier(Now.AddHours(-hoursBack), Now));
    }

    /// <summary>
    /// The boundary itself, pinned rather than restated: the switch happens one margin INSIDE four days, so a
    /// window landing exactly on the purge line takes the aggregate instead of depending on when the purge ran.
    /// </summary>
    [Fact]
    public void TheBoundary_SitsOneMarginInsideTheRawHorizon()
    {
        var switchPoint = Now - TimescaleSupport.RawRetentionSpan + DarlingTrendReader.RawTierMargin;

        Assert.True(DarlingTrendReader.ShouldUseRawTier(switchPoint, Now));
        Assert.False(DarlingTrendReader.ShouldUseRawTier(switchPoint.AddSeconds(-1), Now));

        /* And the margin really is inside the horizon, not outside it - a margin on the wrong side would send
           windows to raw that raw has already dropped. */
        Assert.True(DarlingTrendReader.RawTierMargin > TimeSpan.Zero);
        Assert.True(switchPoint > Now - TimescaleSupport.RawRetentionSpan);
    }

    /// <summary>
    /// Routing by the oldest point, never by width: a NARROW window sitting entirely in last week is exactly as
    /// unservable from raw as a wide one. Routing by width would send it to a tier holding no rows for it, which
    /// is the same silent-empty failure in a new place.
    /// </summary>
    [Fact]
    public void ANarrowWindowInThePast_StillTakesTheAggregate()
    {
        var start = Now.AddDays(-10);

        /* Two hours wide and ten days old. Raw dropped these rows six days ago, so width must not rescue it. */
        Assert.False(DarlingTrendReader.ShouldUseRawTier(start, Now));

        /* The regression this pins: measuring the start against the window's END instead of wall clock made
           this window look "recent" - it IS recent relative to its own end - and routed it to a tier holding
           nothing for it, which is the same silent-empty failure in a new place. Passing the window end here
           would return true under that mistake; it must be false, because now is what retention answers to. */
        var windowEnd = start.AddHours(2);
        Assert.True(windowEnd < Now - TimescaleSupport.RawRetentionSpan);
        Assert.False(DarlingTrendReader.ShouldUseRawTier(start, Now));
    }

    /// <summary>
    /// One mapper serves both queries, so the two projections must agree on ordinals. A column added to one and
    /// not the other would mis-map silently — the reader would keep reading, just off by one.
    /// </summary>
    [Fact]
    public void BothProjections_HaveTheSameShape()
    {
        static string[] Columns(string sql) =>
            sql[(sql.IndexOf("SELECT", StringComparison.Ordinal) + 6)..sql.IndexOf("FROM", StringComparison.Ordinal)]
                .Split(',')
                .Select(c => c.Trim())
                .Select(c =>
                {
                    var at = c.LastIndexOf(" AS ", StringComparison.OrdinalIgnoreCase);
                    return (at >= 0 ? c[(at + 4)..] : c).Trim();
                })
                .ToArray();

        var raw = Columns(DarlingTrendReader.QueryHistorySql);
        var hourly = Columns(DarlingTrendReader.QueryHistoryHourlySql);

        Assert.Equal(12, raw.Length);
        Assert.Equal(raw.Length, hourly.Length);
        Assert.Equal(raw, hourly);
    }

    /// <summary>
    /// The aggregate reads the rollup and buckets by it — asserted on the shipped SQL so a later edit that
    /// quietly repoints it at the raw table (which would reintroduce the bug while every test still passed)
    /// has to argue with this.
    /// </summary>
    [Fact]
    public void TheAggregateQuery_ReadsTheRollup_AndBucketsByIt()
    {
        var sql = DarlingTrendReader.QueryHistoryHourlySql;

        Assert.Contains("FROM query_stats_hourly", sql, StringComparison.Ordinal);
        Assert.Contains("bucket >=", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY bucket", sql, StringComparison.Ordinal);

        /* The columns the rollup does not carry are typed NULLs, never zeros: on an aggregate row a zero reads
           as "none observed", which is a measurement nobody made. */
        Assert.Contains("CAST(NULL AS bigint) AS delta_logical_reads", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(NULL AS text) AS query_plan_hash", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The raw query still reads the raw table, so the common path keeps per-collection resolution and every
    /// column. Pinned alongside the above so the pair cannot drift into both reading the same place.
    /// </summary>
    [Fact]
    public void TheRawQuery_StillReadsPerCollectionRows()
    {
        var sql = DarlingTrendReader.QueryHistorySql;

        Assert.Contains("FROM query_stats", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("query_stats_hourly", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time", sql, StringComparison.Ordinal);
    }
}
