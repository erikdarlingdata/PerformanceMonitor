/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4605: <c>collect.query_store_interval_wide</c> has two indexes, the unique key (it leads with
/// <c>server_id</c>) and <c>idx_query_store_interval_wide_first_exec</c>. The four reads of the table filter
/// <c>collection_time</c> (the Trends interval arm <c>interval_start_time_utc</c>), which neither serves, so a
/// fleet-wide day read walked the whole table. Each read now also carries
/// <c>first_execution_time &gt;= &lt;window start&gt; - </c><see cref="QueryStoreIntervalWide.PurgeEdgeMarginSql"/>.
/// The compose read's text is pinned in <c>DarlingComposeTests.Compile_QueryStoreWideEligible_*</c>; this class
/// pins the constant's bound and the other three texts. The floor is built from the margin constant, never a
/// literal, so a text that restates the number would slip past a change of the margin.
/// </summary>
public sealed class QueryStoreIntervalWideFirstExecFloorTests
{
    /// <summary>The floor as it must read in a table SQL, over the given window-start placeholder.</summary>
    private static string Floor(string startPlaceholder) =>
        "first_execution_time >= " + startPlaceholder + " - " + QueryStoreIntervalWide.PurgeEdgeMarginSql;

    [Fact]
    public void Margin_IsAtLeastTheBoundTheCollectorGuarantees_AndItsSqlFormIsNeverShorter()
    {
        /* The proof, term by term. The collector keeps only intervals with end_time > cutoff, and the cutoff is
           never more than WatermarkPolicy.MaxCatchup before the row's collection_time (ClampCatchup). A backfill
           slice spans at most MaxSliceSpan and stamps its ceiling as collection_time, so a slice is bounded by
           the same cap only while MaxSliceSpan does not exceed MaxCatchup. An interval spans at most a day
           (IntervalSpanMargin) and first_execution_time lies inside it. */
        Assert.True(QueryStoreBackfillState.MaxSliceSpan <= WatermarkPolicy.MaxCatchup,
            "a backfill slice must not span more than the catch-up cap, or the floor's bound is wrong");

        var bound = QueryStoreIntervalWide.IntervalSpanMargin + WatermarkPolicy.MaxCatchup;
        Assert.True(QueryStoreIntervalWide.PurgeEdgeMargin >= bound,
            $"PurgeEdgeMargin {QueryStoreIntervalWide.PurgeEdgeMargin} must cover IntervalSpanMargin + MaxCatchup {bound}");

        var match = Regex.Match(QueryStoreIntervalWide.PurgeEdgeMarginSql, @"^interval '(?<minutes>\d+) minutes'$");
        Assert.True(match.Success, "the margin's SQL form is an interval literal in whole minutes: " + QueryStoreIntervalWide.PurgeEdgeMarginSql);
        var sqlMargin = TimeSpan.FromMinutes(long.Parse(match.Groups["minutes"].Value, System.Globalization.CultureInfo.InvariantCulture));
        Assert.True(sqlMargin >= QueryStoreIntervalWide.PurgeEdgeMargin,
            $"{QueryStoreIntervalWide.PurgeEdgeMarginSql} must not be shorter than PurgeEdgeMargin {QueryStoreIntervalWide.PurgeEdgeMargin}");
        Assert.True(sqlMargin >= bound, $"{QueryStoreIntervalWide.PurgeEdgeMarginSql} must cover the collector's bound {bound}");
    }

    [Fact]
    public void McpQueryStoreTopTableSql_FloorsFirstExecutionTimeOnItsReadStart_AndTheRawReadIsUntouched()
    {
        var sql = DarlingDataReader.QueryStoreTopTableSql;
        Assert.Contains(Floor("$2"), sql, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(sql, Regex.Escape(QueryStoreIntervalWide.PurgeEdgeMarginSql)));

        /* The floor is inside the table CTE, beside the collection_time bound it accompanies, and $1 stayed
           literal through the raw string's interpolation. */
        var floor = sql.IndexOf(Floor("$2"), StringComparison.Ordinal);
        var collectionBound = sql.IndexOf("collection_time >= $2", StringComparison.Ordinal);
        Assert.True(collectionBound >= 0 && collectionBound < floor, "the floor follows the collection_time bound it accompanies");
        Assert.True(floor < sql.IndexOf("ranked AS", StringComparison.Ordinal));
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);

        Assert.DoesNotContain(QueryStoreIntervalWide.PurgeEdgeMarginSql, DarlingDataReader.QueryStoreTopSql, StringComparison.Ordinal);
        Assert.StartsWith("WITH deduped AS (", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ViewerQueryStoreTopTableSql_FloorsFirstExecutionTimeOnItsReadStart_AndTheRawReadIsUntouched()
    {
        var sql = ViewerDataService.QueryStoreTopTableSql;
        Assert.Contains(Floor("$2"), sql, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(sql, Regex.Escape(QueryStoreIntervalWide.PurgeEdgeMarginSql)));

        var floor = sql.IndexOf(Floor("$2"), StringComparison.Ordinal);
        var collectionBound = sql.IndexOf("collection_time >= $2", StringComparison.Ordinal);
        Assert.True(collectionBound >= 0 && collectionBound < floor, "the floor follows the collection_time bound it accompanies");
        Assert.True(floor < sql.IndexOf("ranked AS", StringComparison.Ordinal));
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);

        Assert.DoesNotContain(QueryStoreIntervalWide.PurgeEdgeMarginSql, ViewerDataService.QueryStoreTopSql, StringComparison.Ordinal);
        Assert.StartsWith("WITH deduped AS (", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ViewerQueryStoreDurationTrendTableSql_FloorsFirstExecutionTimeOnBothArms_AndTheRawTrendIsUntouched()
    {
        var sql = ViewerDataService.QueryStoreDurationTrendTableSql;
        var floor = Floor("$2");

        /* Both arms, each after its own time filter: arm 1 (interval_start_time_utc) and arm 2 (the legacy
           rows, collection_time). The margin appears exactly twice, once per arm. */
        var floors = Regex.Matches(sql, Regex.Escape(floor));
        Assert.Equal(2, floors.Count);
        Assert.Equal(2, Regex.Matches(sql, Regex.Escape(QueryStoreIntervalWide.PurgeEdgeMarginSql)).Count);

        var arm1End = sql.IndexOf("interval_start_time_utc <= $3", StringComparison.Ordinal);
        var arm2End = sql.IndexOf("collection_time <= $4", StringComparison.Ordinal);
        Assert.True(arm1End >= 0 && arm1End < floors[0].Index, "arm 1's floor follows its interval_start_time_utc bounds");
        Assert.True(arm2End >= 0 && arm2End < floors[1].Index && floors[0].Index < arm2End, "arm 2's floor follows its collection_time bounds");
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);

        Assert.DoesNotContain(QueryStoreIntervalWide.PurgeEdgeMarginSql, ViewerDataService.QueryStoreDurationTrendSql, StringComparison.Ordinal);
    }
}
