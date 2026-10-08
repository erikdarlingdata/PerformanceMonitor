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
/// #4605: among the btrees of <c>collect.query_store_interval_wide</c> are the unique key (it leads with
/// <c>server_id</c> and holds <c>first_execution_time</c> as a key column),
/// <c>idx_query_store_interval_wide_first_exec</c>, and, where the service has built it, the wide btree on
/// <c>(server_id, first_execution_time)</c> (#4952), which the service builds in the background rather than a
/// migration. The three per-server reads of the table (the MCP Query Store top,
/// the Queries grid and the Trends chart, all <c>WHERE server_id = $1</c>) filter <c>collection_time</c> (the Trends
/// interval arm <c>interval_start_time_utc</c>), which none of them serves, so each walked all of the server's rows.
/// Each now also carries
/// <c>first_execution_time &gt;= &lt;window start&gt; - </c><see cref="QueryStoreIntervalWide.PurgeEdgeMarginSql"/>.
/// The Custom Views route, for all servers or some, carries no floor; its compose text is pinned in
/// <c>DarlingComposeTests.Compile_QueryStoreWideEligible_CarriesNoFirstExecutionTimeFloor</c>. This class pins the
/// constant's bound and the three per-server texts. The floor is built from the margin constant, never a literal,
/// so a text that restates the number would slip past a change of the margin.
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
    public void ViewerQueryStoreDurationTrendTableSql_BoundsFirstExecutionTimeOnBothArms_AndTheRawTrendIsUntouched()
    {
        var sql = ViewerDataService.QueryStoreDurationTrendTableSql;
        var arms = sql.Split("UNION ALL", StringSplitOptions.None);
        Assert.Equal(2, arms.Length);

        /* Arm 1 (#5523) is placed by interval_start_time_utc, so its range comes from the window: the first execution
           lies inside the interval, which starts in [$2, $3] and spans at most a day. The collector-derived floor
           (PurgeEdgeMarginSql) is arm 2's alone, which is placed by collection_time, and that arm takes the upper twin. */
        Assert.Contains("first_execution_time >= $2 - " + QueryStoreIntervalWide.IntervalStartSlackSql, arms[0], StringComparison.Ordinal);
        Assert.Contains("first_execution_time <= $3 + " + QueryStoreIntervalWide.IntervalStartFirstExecMarginSql, arms[0], StringComparison.Ordinal);
        Assert.DoesNotContain(QueryStoreIntervalWide.PurgeEdgeMarginSql, arms[0], StringComparison.Ordinal);
        Assert.Contains(Floor("$2"), arms[1], StringComparison.Ordinal);
        Assert.Contains("first_execution_time <= $4 + " + QueryStoreIntervalWide.FirstExecUpperSlackSql, arms[1], StringComparison.Ordinal);
        Assert.Single(Regex.Matches(sql, Regex.Escape(QueryStoreIntervalWide.PurgeEdgeMarginSql)));

        /* Each bound follows its own arm's time filter. */
        var arm1End = arms[0].IndexOf("interval_start_time_utc <= $3", StringComparison.Ordinal);
        Assert.True(arm1End >= 0 && arm1End < arms[0].IndexOf("first_execution_time >=", StringComparison.Ordinal), "arm 1's bounds follow its interval_start_time_utc bounds");
        var arm2End = arms[1].IndexOf("collection_time <= $4", StringComparison.Ordinal);
        Assert.True(arm2End >= 0 && arm2End < arms[1].IndexOf("first_execution_time >=", StringComparison.Ordinal), "arm 2's bounds follow its collection_time bounds");
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);

        Assert.DoesNotContain(QueryStoreIntervalWide.PurgeEdgeMarginSql, ViewerDataService.QueryStoreDurationTrendSql, StringComparison.Ordinal);
        Assert.DoesNotContain(QueryStoreIntervalWide.FirstExecUpperSlackSql, ViewerDataService.QueryStoreDurationTrendSql, StringComparison.Ordinal);
        Assert.DoesNotContain(QueryStoreIntervalWide.IntervalStartSlackSql, ViewerDataService.QueryStoreDurationTrendSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #5523: the floor alone made the <c>first_execution_time</c> range run from the window start to now, so a window that
    /// ended days ago, or the edge of a long one, scanned every newer row of the server. Each per-server read now also
    /// carries the upper twin, placed after its floor. The raw reads carry neither.
    /// </summary>
    [Fact]
    public void EveryPerServerTableRead_CarriesTheUpperBoundBesideItsFloor()
    {
        var slack = QueryStoreIntervalWide.FirstExecUpperSlackSql;

        var mcp = DarlingDataReader.QueryStoreTopTableSql;
        Assert.Single(Regex.Matches(mcp, Regex.Escape("first_execution_time <= $3 + " + slack)));
        Assert.True(mcp.IndexOf(Floor("$2"), StringComparison.Ordinal) < mcp.IndexOf("first_execution_time <= $3 + ", StringComparison.Ordinal));
        Assert.DoesNotContain(slack, DarlingDataReader.QueryStoreTopSql, StringComparison.Ordinal);

        /* The long-window read's two edge arms, each ending at its own window end: [$2, $8) and [$9, $3]. */
        var daily = DarlingDataReader.QueryStoreTopDailyTableSql;
        Assert.Single(Regex.Matches(daily, Regex.Escape("first_execution_time < $8::date + " + slack)));
        Assert.Single(Regex.Matches(daily, Regex.Escape("first_execution_time <= $3 + " + slack)));
        Assert.Equal(2, Regex.Matches(daily, Regex.Escape("first_execution_time >= ")).Count);

        var grid = ViewerDataService.QueryStoreTopTableSql;
        Assert.Single(Regex.Matches(grid, Regex.Escape("($3::timestamp IS NULL OR first_execution_time <= $3 + " + slack + ")")));
        Assert.DoesNotContain(slack, ViewerDataService.QueryStoreTopSql, StringComparison.Ordinal);

        var history = DarlingMcpQueryStoreHistoryTools.HistoryTableSql;
        Assert.Single(Regex.Matches(history, Regex.Escape("first_execution_time <= $5 + " + slack)));
        Assert.True(history.IndexOf("first_execution_time >= $4 - ", StringComparison.Ordinal) < history.IndexOf("first_execution_time <= $5 + ", StringComparison.Ordinal));
        Assert.DoesNotContain(slack, DarlingMcpQueryStoreHistoryTools.HistorySql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheUpperSlack_IsNeverShorterThanTheDailyBuilders_AndItsSqlFormsAreNeverShorter()
    {
        /* The reads were exact before #5523 and the daily summary is documented as approximate, so the reads' slack (twelve
           hours: clock set to local time, a long collection cycle) may be longer than the builder's hour but never shorter. */
        Assert.True(QueryStoreIntervalWide.FirstExecUpperSlack >= QueryStoreTopDaily.SkewSlack);
        Assert.Equal(TimeSpan.FromHours(12), QueryStoreIntervalWide.FirstExecUpperSlack);

        var slack = Regex.Match(QueryStoreIntervalWide.FirstExecUpperSlackSql, @"^interval '(?<m>\d+) minutes'$");
        Assert.True(slack.Success, QueryStoreIntervalWide.FirstExecUpperSlackSql);
        Assert.True(TimeSpan.FromMinutes(long.Parse(slack.Groups["m"].Value, System.Globalization.CultureInfo.InvariantCulture)) >= QueryStoreIntervalWide.FirstExecUpperSlack);

        /* An interval placed by its start can hold a first execution a whole interval later, plus its own hour of slack
           (both columns are the monitored clock, so no cross-clock skew applies to it). */
        Assert.Equal(TimeSpan.FromHours(1), QueryStoreIntervalWide.IntervalStartSlack);
        var floor = Regex.Match(QueryStoreIntervalWide.IntervalStartSlackSql, @"^interval '(?<m>\d+) minutes'$");
        Assert.True(floor.Success, QueryStoreIntervalWide.IntervalStartSlackSql);
        Assert.True(TimeSpan.FromMinutes(long.Parse(floor.Groups["m"].Value, System.Globalization.CultureInfo.InvariantCulture)) >= QueryStoreIntervalWide.IntervalStartSlack);
        Assert.Equal(QueryStoreIntervalWide.IntervalSpanMargin + QueryStoreIntervalWide.IntervalStartSlack, QueryStoreIntervalWide.IntervalStartFirstExecMargin);
        var start = Regex.Match(QueryStoreIntervalWide.IntervalStartFirstExecMarginSql, @"^interval '(?<m>\d+) minutes'$");
        Assert.True(start.Success, QueryStoreIntervalWide.IntervalStartFirstExecMarginSql);
        Assert.True(TimeSpan.FromMinutes(long.Parse(start.Groups["m"].Value, System.Globalization.CultureInfo.InvariantCulture)) >= QueryStoreIntervalWide.IntervalStartFirstExecMargin);
    }
}
