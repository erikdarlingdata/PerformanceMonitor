/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5809: the table form of the Query Store regressions read, without a database. The first form shipped with its two
/// table arms declared below the field that concatenated them, and static field initializers run in textual order, so
/// both copies (service and viewer) began at <c>baseline_performance</c>: every call failed with 42601 and quietly fell
/// back to raw. These pins hold the composed text, and the fault log that would have shown it.
/// </summary>
public sealed class QueryStoreRegressionsTableReadTests
{
    [Fact]
    public void ServiceTableSql_StartsWithItsTwoTableArms()
    {
        AssertTableShape(DarlingQueryStoreRegressionReader.QueryStoreRegressionsTableSql);
    }

    [Fact]
    public void ViewerTableSql_StartsWithItsTwoTableArms()
    {
        AssertTableShape(ViewerDataService.QueryStoreRegressionsTableSql);
    }

    private static void AssertTableShape(string sql)
    {
        Assert.StartsWith("WITH deduped_baseline AS (", sql.TrimStart(), StringComparison.Ordinal);
        Assert.Contains("deduped_recent AS (", sql, StringComparison.Ordinal);
        Assert.Contains("FROM query_store_interval_wide", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("query_store_stats", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ATableReadFault_WarnsOncePerProcessWithItsMessage_ThenLogsAtDebug()
    {
        var logger = new CapturingTestLogger();
        DarlingQueryStoreRegressionReader.ResetTableFaultWarningForTests();
        try
        {
            DarlingQueryStoreRegressionReader.LogTableFault(logger, new InvalidOperationException("first fault text"));
            DarlingQueryStoreRegressionReader.LogTableFault(logger, new InvalidOperationException("second fault text"));
            DarlingQueryStoreRegressionReader.LogTableFault(logger, new InvalidOperationException("third fault text"));

            Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
            Assert.Equal(2, logger.CountAtLevel(LogLevel.Debug));
            Assert.Contains(logger.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal) && l.Contains("first fault text", StringComparison.Ordinal));
            Assert.DoesNotContain(logger.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal) && l.Contains("second fault text", StringComparison.Ordinal));
        }
        finally
        {
            DarlingQueryStoreRegressionReader.ResetTableFaultWarningForTests();
        }
    }

    [Fact]
    public void ATableReadFault_WithNoLogger_LeavesTheOnceFlagForTheFirstLoggedFault()
    {
        var logger = new CapturingTestLogger();
        DarlingQueryStoreRegressionReader.ResetTableFaultWarningForTests();
        try
        {
            DarlingQueryStoreRegressionReader.LogTableFault(null, new InvalidOperationException("unscoped fault"));
            DarlingQueryStoreRegressionReader.LogTableFault(logger, new InvalidOperationException("scoped fault"));

            Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
            Assert.Contains("scoped fault", logger.Joined, StringComparison.Ordinal);
        }
        finally
        {
            DarlingQueryStoreRegressionReader.ResetTableFaultWarningForTests();
        }
    }

    /// <summary>#5506: the Regular arms may read <c>query_store_interval_latest</c> only where its coverage starts at or before
    /// the baseline start and no batch is pending; anything else keeps the wide table. #6750: and only where latest's floor is at
    /// least the purge-edge margin (26 hours) before the baseline start, since the per-arm start can now reach below raw's floor.</summary>
    [Fact]
    public void TheLatestGate_AllowsOnlyCoverageFromTheBaselineStartWithNoPendingBatch_AndAFloorAMarginBeforeIt()
    {
        var baselineStart = new DateTime(2026, 10, 21, 12, 0, 0);
        var oldFloor = baselineStart.AddDays(-14);
        Assert.True(QueryStoreRegressionBaseline.LatestCoversRegularArms(baselineStart.AddDays(-20), hasPending: false, oldFloor, baselineStart));
        Assert.True(QueryStoreRegressionBaseline.LatestCoversRegularArms(baselineStart, hasPending: false, oldFloor, baselineStart));
        Assert.False(QueryStoreRegressionBaseline.LatestCoversRegularArms(baselineStart.AddTicks(1), hasPending: false, oldFloor, baselineStart));
        Assert.False(QueryStoreRegressionBaseline.LatestCoversRegularArms(baselineStart.AddDays(-20), hasPending: true, oldFloor, baselineStart));
        Assert.False(QueryStoreRegressionBaseline.LatestCoversRegularArms(null, hasPending: false, oldFloor, baselineStart));

        var edge = baselineStart - QueryStoreIntervalWide.PurgeEdgeMargin;
        Assert.True(QueryStoreRegressionBaseline.LatestCoversRegularArms(baselineStart.AddDays(-20), hasPending: false, edge, baselineStart));
        Assert.False(QueryStoreRegressionBaseline.LatestCoversRegularArms(baselineStart.AddDays(-20), hasPending: false, edge.AddTicks(1), baselineStart));
        Assert.True(QueryStoreRegressionBaseline.LatestCoversRegularArms(baselineStart.AddDays(-20), hasPending: false, null, baselineStart));
    }

    /// <summary>#5506: each reader runs the latest form where the plan allows it, and the wide form otherwise, with and
    /// without run days. The wide form is today's statement.</summary>
    [Fact]
    public void EachReader_PicksTheLatestForm_OnlyWhenThePlanAllowsIt()
    {
        var windowStart = new DateTime(2026, 10, 24, 18, 0, 0);
        var live = QueryStoreRegressionBaseline.AllLive(windowStart);
        var days = new QueryStoreRegressionBaseline.Plan([new DateOnly(2026, 10, 22)], new DateTime(2026, 10, 22), new DateTime(2026, 10, 22));
        foreach (var plan in new[] { live, days })
        {
            var daily = plan.BuiltDayCount > 0;
            Assert.Equal(DarlingQueryStoreRegressionReader.BuildSql(wide: true, useDailyTotals: daily), DarlingQueryStoreRegressionReader.TableSqlFor(plan));
            Assert.Equal(DarlingQueryStoreRegressionReader.BuildSql(wide: true, useDailyTotals: daily, regularFromLatest: true),
                DarlingQueryStoreRegressionReader.TableSqlFor(plan with { RegularFromLatest = true }));
            Assert.Equal(ViewerDataService.BuildQueryStoreRegressionsSql(wide: true, useDailyTotals: daily), ViewerDataService.QueryStoreRegressionsTableSqlFor(plan));
            Assert.Equal(ViewerDataService.BuildQueryStoreRegressionsSql(wide: true, useDailyTotals: daily, regularFromLatest: true),
                ViewerDataService.QueryStoreRegressionsTableSqlFor(plan with { RegularFromLatest = true }));
            Assert.NotEqual(DarlingQueryStoreRegressionReader.TableSqlFor(plan), DarlingQueryStoreRegressionReader.TableSqlFor(plan with { RegularFromLatest = true }));
        }

        Assert.False(live.RegularFromLatest);

        /* #5506: with recent days on the latest form each reader runs the recent-days text; on the wide form the recent days are
           not read, and the text is the wide one. */
        foreach (var plan in new[] { live, days })
        {
            var daily = plan.BuiltDayCount > 0;
            var recent = QueryStoreRegressionBaseline.WithRecentDays(plan with { RegularFromLatest = true }, [new DateOnly(2026, 10, 25)]);
            Assert.Equal(DarlingQueryStoreRegressionReader.BuildSql(wide: true, useDailyTotals: daily, regularFromLatest: true, recentFromDaily: true),
                DarlingQueryStoreRegressionReader.TableSqlFor(recent));
            Assert.Equal(ViewerDataService.BuildQueryStoreRegressionsSql(wide: true, useDailyTotals: daily, regularFromLatest: true, recentFromDaily: true),
                ViewerDataService.QueryStoreRegressionsTableSqlFor(recent));
            Assert.NotEqual(DarlingQueryStoreRegressionReader.TableSqlFor(plan with { RegularFromLatest = true }), DarlingQueryStoreRegressionReader.TableSqlFor(recent));
            Assert.Equal(DarlingQueryStoreRegressionReader.TableSqlFor(plan), DarlingQueryStoreRegressionReader.TableSqlFor(recent with { RegularFromLatest = false }));
        }
    }

    private static PlanRegressionDaily.BuiltDay Whole(int day) => new(new DateOnly(2026, 10, day), null);

    /// <summary>#5506: the recent window's days from the daily totals, at each of its edges. The window starts at 24 Oct
    /// 06:00 and ends at 28 Oct 06:00, the baseline at 17 Oct 06:00: the first midnight at or after the start is 25 Oct, and
    /// the last day whose next midnight is at or before the end is 27 Oct.</summary>
    [Fact]
    public void ChooseRecent_TakesTheWholeDaysInsideTheWindow_AndOneRunOfThem()
    {
        var baselineStart = new DateTime(2026, 10, 17, 6, 0, 0);
        var start = new DateTime(2026, 10, 24, 6, 0, 0);
        var end = new DateTime(2026, 10, 28, 6, 0, 0);
        DateOnly[] Days(params int[] days) => days.Select(d => new DateOnly(2026, 10, d)).ToArray();

        /* The day the window starts in is partial; the day it ends in too. */
        Assert.Equal(Days(25, 26, 27), QueryStoreRegressionBaseline.ChooseRecent([Whole(23), Whole(24), Whole(25), Whole(26), Whole(27), Whole(28)], baselineStart, start, end));

        /* A window that starts exactly at a midnight takes that day. */
        Assert.Equal(Days(24, 25), QueryStoreRegressionBaseline.ChooseRecent([Whole(23), Whole(24), Whole(25)], baselineStart, new DateTime(2026, 10, 24), end));
        Assert.Equal(Days(25), QueryStoreRegressionBaseline.ChooseRecent([Whole(25)], baselineStart, new DateTime(2026, 10, 24).AddTicks(1), end));

        /* D + 1 equal to the end is taken; one tick less is not. */
        Assert.Equal(Days(25, 26, 27), QueryStoreRegressionBaseline.ChooseRecent([Whole(25), Whole(26), Whole(27)], baselineStart, start, new DateTime(2026, 10, 28)));
        Assert.Equal(Days(25, 26), QueryStoreRegressionBaseline.ChooseRecent([Whole(25), Whole(26), Whole(27)], baselineStart, start, new DateTime(2026, 10, 28).AddTicks(-1)));

        /* D - 1 day equal to the baseline start is taken; a baseline start one tick later drops the day (the strict rule). */
        Assert.Equal(Days(25, 26), QueryStoreRegressionBaseline.ChooseRecent([Whole(25), Whole(26)], new DateTime(2026, 10, 24), new DateTime(2026, 10, 24, 6, 0, 0), end));
        Assert.Equal(Days(26), QueryStoreRegressionBaseline.ChooseRecent([Whole(25), Whole(26)], new DateTime(2026, 10, 24).AddTicks(1), new DateTime(2026, 10, 24, 6, 0, 0), end));

        /* A gap: the longest run is taken, the later one on a tie. */
        Assert.Equal(Days(25, 26), QueryStoreRegressionBaseline.ChooseRecent([Whole(25), Whole(26), Whole(28)], baselineStart, start, new DateTime(2026, 10, 30)));
        Assert.Equal(Days(27), QueryStoreRegressionBaseline.ChooseRecent([Whole(25), Whole(27)], baselineStart, start, end));

        /* A partial build is never taken, and neither is anything from an empty list. */
        Assert.Equal(Days(26), QueryStoreRegressionBaseline.ChooseRecent([new PlanRegressionDaily.BuiltDay(new DateOnly(2026, 10, 25), new DateTime(2026, 10, 25, 20, 0, 0)), Whole(26)], baselineStart, start, end));
        Assert.Empty(QueryStoreRegressionBaseline.ChooseRecent([], baselineStart, start, end));
        Assert.Empty(QueryStoreRegressionBaseline.ChooseRecent([Whole(24)], baselineStart, start, end));
    }

    /// <summary>#5506: the recent high range is raised by the measured margin on the latest form, like the baseline's, and the
    /// recent low end is the first recent day's midnight.</summary>
    [Fact]
    public void TheRecentHighStart_IsRaisedByTheMeasuredMargin_AndTheLowEndIsTheFirstDay()
    {
        var plan = QueryStoreRegressionBaseline.WithRecentDays(
            QueryStoreRegressionBaseline.AllLive(new DateTime(2026, 10, 24, 6, 0, 0)) with { RegularFromLatest = true },
            [new DateOnly(2026, 10, 25), new DateOnly(2026, 10, 26)]);
        Assert.Equal(new DateTime(2026, 10, 25), plan.RecentLowEnd);
        Assert.Equal(new DateTime(2026, 10, 26), plan.RecentHighStart);
        Assert.Equal(new DateTime(2026, 10, 26, 21, 0, 0), QueryStoreRegressionBaseline.WithMeasuredHighStart(plan, TimeSpan.FromHours(3)).RecentHighStart);
        Assert.Equal(new DateTime(2026, 10, 26), QueryStoreRegressionBaseline.WithMeasuredHighStart(plan, QueryStoreIntervalWide.PurgeEdgeMargin).RecentHighStart);
        Assert.Empty(QueryStoreRegressionBaseline.WithRecentDays(plan, []).RecentDays);
    }

    /// <summary>#5506: the latest form's high range starts at the next midnight minus the measured margin, never below the
    /// last run day's midnight, and the wide form and the all-live plan keep their bounds.</summary>
    [Fact]
    public void TheHighStart_IsRaisedByTheMeasuredMargin_OnTheLatestFormWithRunDaysOnly()
    {
        var lastRunDay = new DateTime(2026, 10, 23);
        var days = new QueryStoreRegressionBaseline.Plan([new DateOnly(2026, 10, 22), new DateOnly(2026, 10, 23)], new DateTime(2026, 10, 22), lastRunDay);
        var latest = days with { RegularFromLatest = true };

        Assert.Equal(new DateTime(2026, 10, 23, 21, 0, 0), QueryStoreRegressionBaseline.WithMeasuredHighStart(latest, TimeSpan.FromHours(3)).HighStart);
        Assert.Equal(lastRunDay, QueryStoreRegressionBaseline.WithMeasuredHighStart(latest, QueryStoreIntervalWide.PurgeEdgeMargin).HighStart);
        Assert.Equal(lastRunDay, QueryStoreRegressionBaseline.WithMeasuredHighStart(days, TimeSpan.FromHours(3)).HighStart);
        var live = QueryStoreRegressionBaseline.AllLive(new DateTime(2026, 10, 24, 18, 0, 0)) with { RegularFromLatest = true };
        Assert.Equal(live, QueryStoreRegressionBaseline.WithMeasuredHighStart(live, TimeSpan.FromHours(3)));
    }
}
