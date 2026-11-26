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

    private static PlanRegressionDaily.BuiltDay Partial(int day, int throughDay, int throughHour) =>
        new(new DateOnly(2026, 10, day), new DateTime(2026, 10, throughDay, throughHour, 0, 0));

    /// <summary>#9606: the partial day the recent arm reads from the daily totals, at each of its edges. The window starts at
    /// 24 Oct 06:00 and ends at 28 Oct 06:00, the baseline at 17 Oct 06:00, and 28 Oct is built through 03:00.</summary>
    [Fact]
    public void ChooseRecentPartial_TakesTheOpenDayAfterTheWholeDays_OnlyWithItsWatermarkInsideTheDayAndTheWindow()
    {
        var baselineStart = new DateTime(2026, 10, 17, 6, 0, 0);
        var start = new DateTime(2026, 10, 24, 6, 0, 0);
        var end = new DateTime(2026, 10, 28, 6, 0, 0);
        DateOnly[] Days(params int[] days) => days.Select(d => new DateOnly(2026, 10, d)).ToArray();
        PlanRegressionDaily.BuiltDay? Chosen(PlanRegressionDaily.BuiltDay[] built, DateOnly[] recentDays, DateTime? windowStart = null, DateTime? windowEnd = null, DateTime? baseline = null) =>
            QueryStoreRegressionBaseline.ChooseRecentPartial(built, baseline ?? baselineStart, windowStart ?? start, windowEnd ?? end, recentDays);

        /* The day after the last whole day, with its watermark. */
        var today = Partial(28, 28, 3);
        Assert.Equal(today, Chosen([Whole(25), Whole(26), Whole(27), today], Days(25, 26, 27)));

        /* A watermark equal to the window end is taken; a window that ends one tick before it reads the day live. */
        Assert.Equal(today, Chosen([Whole(27), today], Days(27), windowEnd: new DateTime(2026, 10, 28, 3, 0, 0)));
        Assert.Null(Chosen([Whole(27), today], Days(27), windowEnd: new DateTime(2026, 10, 28, 3, 0, 0).AddTicks(-1)));

        /* A watermark at or below the day's midnight has no row of the day, and one at the next midnight is a whole build. */
        Assert.Null(Chosen([Whole(27), Partial(28, 28, 0)], Days(27)));
        Assert.Null(Chosen([Whole(27), Partial(28, 27, 21)], Days(27)));
        Assert.Null(Chosen([Whole(27), Partial(28, 29, 0)], Days(27), windowEnd: new DateTime(2026, 10, 30)));
        Assert.Null(Chosen([Whole(27), Whole(28)], Days(27), windowEnd: new DateTime(2026, 10, 30)));

        /* Not the day after the last whole day (a gap, here a stale 27 Oct): the day is read live. */
        Assert.Null(Chosen([Whole(25), Whole(26), today], Days(25, 26)));

        /* No whole day: the latest open day is taken, yesterday while today has no row, then today. */
        var yesterday = Partial(27, 27, 21);
        Assert.Equal(yesterday, Chosen([yesterday, Partial(28, 27, 21)], []));
        Assert.Equal(today, Chosen([yesterday, today], []));

        /* The whole days' lower bounds: the day the window starts in is not taken, a day that starts at the window start is,
           and a baseline start above D - 1 day drops the day (the strict rule). */
        Assert.Null(Chosen([Partial(24, 24, 20)], []));
        Assert.Equal(Partial(24, 24, 20), Chosen([Partial(24, 24, 20)], [], windowStart: new DateTime(2026, 10, 24)));
        Assert.Equal(today, Chosen([today], [], baseline: new DateTime(2026, 10, 27)));
        Assert.Null(Chosen([today], [], baseline: new DateTime(2026, 10, 27).AddTicks(1)));
        Assert.Null(Chosen([], Days(27)));
    }

    /// <summary>#9606: the partial day is the last recent day, and the live high range starts at its watermark or at the next
    /// midnight minus the measured margin, whichever is lower, never below the day's midnight. Without the partial day the
    /// plan carries no watermark.</summary>
    [Fact]
    public void ThePartialDay_IsTheLastRecentDay_AndTheHighRangeNeverStartsAboveItsWatermark()
    {
        var live = QueryStoreRegressionBaseline.AllLive(new DateTime(2026, 10, 24, 6, 0, 0)) with { RegularFromLatest = true };
        var plan = QueryStoreRegressionBaseline.WithRecentDays(live, [new DateOnly(2026, 10, 26), new DateOnly(2026, 10, 27)], Partial(28, 28, 3));
        Assert.Equal([new DateOnly(2026, 10, 26), new DateOnly(2026, 10, 27), new DateOnly(2026, 10, 28)], plan.RecentDays);
        Assert.Equal(new DateTime(2026, 10, 28, 3, 0, 0), plan.RecentPartialThrough);
        Assert.Equal(new DateTime(2026, 10, 26), plan.RecentLowEnd);
        Assert.Equal(new DateTime(2026, 10, 28), plan.RecentHighStart);
        Assert.True(QueryStoreRegressionBaseline.ReadsRecentDays(plan));

        /* The next midnight minus three hours is 21:00, above the watermark: the range starts at the watermark. */
        Assert.Equal(new DateTime(2026, 10, 28, 3, 0, 0), QueryStoreRegressionBaseline.WithMeasuredHighStart(plan, TimeSpan.FromHours(3)).RecentHighStart);
        /* A margin that reaches below the watermark wins: a row of the next day may start that early. */
        Assert.Equal(new DateTime(2026, 10, 28, 1, 0, 0), QueryStoreRegressionBaseline.WithMeasuredHighStart(plan, TimeSpan.FromHours(23)).RecentHighStart);
        Assert.Equal(new DateTime(2026, 10, 28), QueryStoreRegressionBaseline.WithMeasuredHighStart(plan, QueryStoreIntervalWide.PurgeEdgeMargin).RecentHighStart);

        /* Yesterday built through 21:00, read just after midnight: both bounds are 21:00. */
        var yesterday = QueryStoreRegressionBaseline.WithRecentDays(live, [new DateOnly(2026, 10, 26)], Partial(27, 27, 21));
        Assert.Equal(new DateTime(2026, 10, 27, 21, 0, 0), QueryStoreRegressionBaseline.WithMeasuredHighStart(yesterday, TimeSpan.FromHours(3)).RecentHighStart);

        /* The partial day alone is a recent day; no day at all carries no watermark, and neither do whole days. */
        var alone = QueryStoreRegressionBaseline.WithRecentDays(live, [], Partial(28, 28, 3));
        Assert.Equal([new DateOnly(2026, 10, 28)], alone.RecentDays);
        Assert.Equal(new DateTime(2026, 10, 28), alone.RecentLowEnd);
        Assert.Null(QueryStoreRegressionBaseline.WithRecentDays(plan, []).RecentPartialThrough);
        Assert.Empty(QueryStoreRegressionBaseline.WithRecentDays(plan, []).RecentDays);
        Assert.Null(QueryStoreRegressionBaseline.WithRecentDays(plan, [new DateOnly(2026, 10, 26)]).RecentPartialThrough);
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
