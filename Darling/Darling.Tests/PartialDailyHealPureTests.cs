/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4716: the pure halves of the daily-after-hole-repair fix and of the one-time heal, with no store. The first
/// three groups pin what the fix that stops new partial days is built from (<c>CompleteDaysTouched</c> and
/// <c>DependentDailiesOf</c>, which the fix's live test proves only end to end); the rest pin the heal's order,
/// its decision, and both edges of the day window it walks.
/// </summary>
public sealed class PartialDailyHealPureTests
{
    private static readonly DateTime Day = new(2026, 3, 10, 0, 0, 0, DateTimeKind.Unspecified);

    /* ─────────────── CompleteDaysTouched ─────────────── */

    [Fact]
    public void CompleteDaysTouched_AnEmptyOrInvertedRange_TouchesNoDay()
    {
        var now = Day.AddDays(10);

        Assert.Empty(TimescaleSupport.CompleteDaysTouched(Day.AddHours(5), Day.AddHours(5), now));
        Assert.Empty(TimescaleSupport.CompleteDaysTouched(Day.AddHours(9), Day.AddHours(5), now));
        Assert.Empty(TimescaleSupport.CompleteDaysTouched(Day.AddDays(2), Day, now));
    }

    [Fact]
    public void CompleteDaysTouched_ARangeInsideOneCompleteDay_IsThatDay()
    {
        var days = TimescaleSupport.CompleteDaysTouched(Day.AddHours(9), Day.AddHours(11), Day.AddDays(10));

        Assert.Equal(new[] { Day }, days);
    }

    [Fact]
    public void CompleteDaysTouched_ARangeAcrossMidnight_IsBothDays_OldestFirst()
    {
        var days = TimescaleSupport.CompleteDaysTouched(Day.AddHours(22), Day.AddDays(1).AddHours(2), Day.AddDays(10));

        Assert.Equal(new[] { Day, Day.AddDays(1) }, days);
    }

    [Fact]
    public void CompleteDaysTouched_ARangeReachingTodaysFillingDay_LeavesThatDayOut()
    {
        /* Now is mid-afternoon on Day+3: Day+2 is the newest complete day, Day+3 is still filling. */
        var now = Day.AddDays(3).AddHours(15);

        var days = TimescaleSupport.CompleteDaysTouched(Day.AddDays(2).AddHours(22), Day.AddDays(3).AddHours(2), now);

        Assert.Equal(new[] { Day.AddDays(2) }, days);

        /* A range wholly inside the filling day touches no complete day. */
        Assert.Empty(TimescaleSupport.CompleteDaysTouched(Day.AddDays(3).AddHours(1), Day.AddDays(3).AddHours(3), now));

        /* The day that ended exactly at the start of today is complete. */
        Assert.Equal(new[] { Day.AddDays(2) },
            TimescaleSupport.CompleteDaysTouched(Day.AddDays(2), Day.AddDays(3), Day.AddDays(3)));
    }

    [Fact]
    public void CompleteDaysTouched_ARangeEndingExactlyAtMidnight_DoesNotTouchTheDayAfter()
    {
        var days = TimescaleSupport.CompleteDaysTouched(Day.AddHours(20), Day.AddDays(1), Day.AddDays(10));

        Assert.Equal(new[] { Day }, days);

        /* One tick past midnight does touch it. */
        Assert.Equal(new[] { Day, Day.AddDays(1) },
            TimescaleSupport.CompleteDaysTouched(Day.AddHours(20), Day.AddDays(1).AddTicks(1), Day.AddDays(10)));
    }

    /* ─────────────── DependentDailiesOf ─────────────── */

    [Fact]
    public void DependentDailiesOf_TheIntervalQueryStoreHourly_GivesBothOfItsDailies_AndNotTheCorrectedHourly()
    {
        var dependents = TimescaleSupport.DependentDailiesOf(TimescaleSupport.QueryStoreStatsIntervalHourlyView);

        Assert.Equal(
            new[] { TimescaleSupport.QueryStoreStatsCorrectedDailyView, TimescaleSupport.QueryStoreStatsIntervalDailyView }.OrderBy(v => v, StringComparer.Ordinal),
            dependents.OrderBy(v => v, StringComparer.Ordinal));
        Assert.DoesNotContain(TimescaleSupport.QueryStoreStatsCorrectedHourlyView, dependents);
    }

    [Fact]
    public void DependentDailiesOf_TheIntervalQueryStoreDaily_GivesTheDayGrainDaily()
    {
        var dependents = TimescaleSupport.DependentDailiesOf(TimescaleSupport.QueryStoreStatsIntervalDailyView);

        Assert.Equal(new[] { TimescaleSupport.QueryStoreStatsDayGrainDailyView }, dependents);
    }

    [Fact]
    public void DependentDailiesOf_AViewNoDailyReads_GivesNone()
    {
        Assert.Empty(TimescaleSupport.DependentDailiesOf(TimescaleSupport.QueryStoreStatsDayGrainDailyView));
        Assert.Empty(TimescaleSupport.DependentDailiesOf(TimescaleSupport.QueryStoreStatsCorrectedHourlyView));
        Assert.Empty(TimescaleSupport.DependentDailiesOf("query_stats"));
        Assert.Empty(TimescaleSupport.DependentDailiesOf("no_such_view"));
    }

    [Fact]
    public void DependentDailiesOf_NeverNamesAFrozenLegacyDaily()
    {
        var frozen = TimescaleSupport.FrozenRollupAggregates.Select(a => a.View).ToHashSet(StringComparer.Ordinal);
        Assert.Contains(TimescaleSupport.QueryStatsDailyView, frozen);

        foreach (var view in TimescaleSupport.RollupViews.Select(r => r.View).Concat(TimescaleSupport.RollupViews.Select(r => r.Source)).Distinct())
        {
            Assert.DoesNotContain(TimescaleSupport.DependentDailiesOf(view), dependent => frozen.Contains(dependent));
        }

        /* The legacy hourly's only daily is frozen, so it has none. */
        Assert.Empty(TimescaleSupport.DependentDailiesOf(TimescaleSupport.QueryStatsHourlyView));
    }

    /* ─────────────── the heal's order ─────────────── */

    [Fact]
    public void PartialDailyHealOrder_IsEveryNonFrozenDailyWithItsSource_AndADailyOnADailyComesAfterIt()
    {
        var order = TimescaleSupport.PartialDailyHealOrder();

        Assert.Equal(
            TimescaleSupport.DailyAggregates.Select(a => a.View).OrderBy(v => v, StringComparer.Ordinal),
            order.Select(p => p.Daily).OrderBy(v => v, StringComparer.Ordinal));

        foreach (var (daily, source) in order)
        {
            Assert.Equal(TimescaleSupport.RollupViews.Single(r => r.View == daily).Source, source);
            Assert.DoesNotContain(TimescaleSupport.FrozenRollupAggregates, f => f.View == daily);
        }

        var names = order.Select(p => p.Daily).ToList();
        Assert.Contains((TimescaleSupport.QueryStoreStatsDayGrainDailyView, TimescaleSupport.QueryStoreStatsIntervalDailyView), order);
        Assert.True(
            names.IndexOf(TimescaleSupport.QueryStoreStatsIntervalDailyView) < names.IndexOf(TimescaleSupport.QueryStoreStatsDayGrainDailyView),
            "the interval daily must be healed before the day-grain daily built on it");

        /* Structurally: every daily whose source is itself a daily sits after every daily whose source is not. */
        var dailyNames = order.Select(p => p.Daily).ToHashSet(StringComparer.Ordinal);
        var lastOnHourly = order.Select((p, i) => (p, i)).Where(x => !dailyNames.Contains(x.p.Source)).Max(x => x.i);
        var firstOnDaily = order.Select((p, i) => (p, i)).Where(x => dailyNames.Contains(x.p.Source)).Min(x => x.i);
        Assert.True(lastOnHourly < firstOnDaily);
    }

    /* ─────────────── the decision ─────────────── */

    [Theory]
    [InlineData(50L, 48L, true)]     // both hold rows, the daily is short: rebuild it
    [InlineData(50L, null, false)]   // source rows, no daily row: the hole scan owns it
    [InlineData(null, 48L, false)]   // daily row, no source rows: the daily may be the only copy left
    [InlineData(50L, 50L, false)]    // equal: nothing to do
    [InlineData(48L, 50L, false)]    // the daily holds MORE than its source: never shrunk
    [InlineData(null, null, false)]
    public void PartialDayNeedsRefresh_RefreshesOnlyADailyThatHoldsFewerSamplesThanItsSource(long? source, long? daily, bool expected)
    {
        Assert.Equal(expected, TimescaleSupport.PartialDayNeedsRefresh(source, daily));
    }

    /* ─────────────── the day window ─────────────── */

    [Fact]
    public void PartialDailyHealDays_StartsTheDayAfterTheSourcesOldestBucket_WhereverInThatDayItSits()
    {
        var now = Day.AddDays(20);
        var expectedFirst = Day.AddDays(1);

        /* Mid-day, and exactly at midnight: both start the day after. */
        Assert.Equal(expectedFirst, TimescaleSupport.PartialDailyHealDays(Day.AddHours(13), now)[0]);
        Assert.Equal(expectedFirst, TimescaleSupport.PartialDailyHealDays(Day, now)[0]);
        Assert.Equal(expectedFirst, TimescaleSupport.PartialDailyHealDays(Day.AddHours(23).AddMinutes(59), now)[0]);

        /* The next hour past the boundary is the next day's, so the window moves with it. */
        Assert.Equal(Day.AddDays(2), TimescaleSupport.PartialDailyHealDays(Day.AddDays(1).AddHours(1), now)[0]);
    }

    [Fact]
    public void PartialDailyHealDays_StopsBeforeTheDailyPolicysOwnWindow_ExclusiveOfItsFirstDay()
    {
        var oldest = Day;
        var now = Day.AddDays(12).AddHours(15);

        /* Today is Day+12; its policy window starts at Day+9; the last day the heal may compare is Day+8. */
        var days = TimescaleSupport.PartialDailyHealDays(oldest, now);
        Assert.Equal(Day.AddDays(8), days[^1]);
        Assert.Equal(Enumerable.Range(1, 8).Select(i => Day.AddDays(i)), days);
        Assert.DoesNotContain(Day.AddDays(9), days);

        /* One tick before midnight it is still the previous day's window: one fewer day. */
        var justBeforeMidnight = TimescaleSupport.PartialDailyHealDays(oldest, Day.AddDays(12).AddTicks(-1));
        Assert.Equal(Day.AddDays(7), justBeforeMidnight[^1]);

        /* At midnight exactly it is the new day's window: one more. */
        var atMidnight = TimescaleSupport.PartialDailyHealDays(oldest, Day.AddDays(13));
        Assert.Equal(Day.AddDays(9), atMidnight[^1]);
    }

    [Fact]
    public void PartialDailyHealDays_HasNothingToWalk_WhenTheSourceIsEmptyOrTheOldestBucketIsInsideTheWindow()
    {
        var now = Day.AddDays(12).AddHours(15);

        Assert.Empty(TimescaleSupport.PartialDailyHealDays(null, now));
        Assert.Empty(TimescaleSupport.PartialDailyHealDays(now.AddHours(-1), now));

        /* Oldest bucket on Day+8: the day after it, Day+9, is the first day of the policy window, so nothing is left. */
        Assert.Empty(TimescaleSupport.PartialDailyHealDays(Day.AddDays(8).AddHours(6), now));

        /* One day earlier leaves exactly one day to walk: Day+8. */
        Assert.Equal(new[] { Day.AddDays(8) }, TimescaleSupport.PartialDailyHealDays(Day.AddDays(7).AddHours(6), now));
    }

    /* ─────────────── the comparison statement ─────────────── */

    [Fact]
    public void PartialDailyDaySamplesSql_ReadsTheMaterializations_BehindAnOffsetZeroFence_AndSumsSampleCount()
    {
        var sql = TimescaleSupport.PartialDailyDaySamplesSql(("_timescaledb_internal", "_materialized_hypertable_11"), ("_timescaledb_internal", "_materialized_hypertable_22"));

        Assert.Contains("\"_timescaledb_internal\".\"_materialized_hypertable_11\"", sql, StringComparison.Ordinal);
        Assert.Contains("\"_timescaledb_internal\".\"_materialized_hypertable_22\"", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("collect.", sql, StringComparison.Ordinal);
        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(sql, "OFFSET 0").Count);
        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(sql, @"sum\(\w\.sample_count\)").Count);

        /* The daily side is read first and gates the source side, so a day with no daily row costs one probe. */
        Assert.True(sql.IndexOf("_materialized_hypertable_22", StringComparison.Ordinal) < sql.IndexOf("_materialized_hypertable_11", StringComparison.Ordinal));
        Assert.Contains("CASE WHEN d.samples IS NULL THEN NULL", sql, StringComparison.Ordinal);
    }
}
