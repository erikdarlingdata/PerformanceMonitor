/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6750 part 3: the cost baseline's first day from the daily totals (<see cref="QueryStoreRegressionBaseline.ExtendToBuiltFirstDay"/>).
/// Latest drops whole days at 15 days, so its floor plus the 26-hour purge-edge margin is 02:00 of the baseline's first day, and
/// <see cref="QueryStoreRegressionBaseline.Choose"/> could never take that day whole. Each case is a read at 00:30 and at 02:30 UTC:
/// before the change the start was 02:00 at both (a 168-hour read 1.5 hours short at 00:30), and the first day was read live.
/// </summary>
public sealed class QueryStoreRegressionsBuiltFirstDayTests
{
    private static readonly DateTime Today = new(2026, 10, 26, 0, 0, 0, DateTimeKind.Unspecified);

    public static TheoryData<int, int> Clocks => new() { { 0, 30 }, { 2, 30 } };

    /// <summary>The read at <paramref name="hour"/>:<paramref name="minute"/>: the baseline start, the window start, and the cost
    /// start today's rule gives (<see cref="QueryStoreIntervalWide.ExactBelowFloorStart"/> over latest's floor, the start of the day
    /// 15 days back).</summary>
    private static (DateTime BaselineStart, DateTime WindowStart, DateTime RegularStart) Read(int hour, int minute)
    {
        var now = Today.AddHours(hour).AddMinutes(minute);
        var windowStart = now.AddHours(-168);
        var baselineStart = windowStart.AddDays(-7);
        var latestFloor = now.AddDays(-15).Date;
        var below = QueryStoreIntervalWide.ExactBelowFloorStart(now.AddDays(-4), baselineStart.Date, now.AddDays(-20), latestFloor);
        Assert.NotNull(below);
        return (baselineStart, windowStart, below!.Value.Start);
    }

    /// <summary>Every whole day from <paramref name="from"/> to the day before <paramref name="windowStart"/>.</summary>
    private static List<PlanRegressionDaily.BuiltDay> WholeBuilds(DateTime from, DateTime windowStart)
    {
        var days = new List<PlanRegressionDaily.BuiltDay>();
        for (var d = from.Date; d.AddDays(1) <= windowStart; d = d.AddDays(1))
        {
            days.Add(new PlanRegressionDaily.BuiltDay(DateOnly.FromDateTime(d), null));
        }

        return days;
    }

    [Theory]
    [MemberData(nameof(Clocks))]
    public void AWholeValidFirstDay_StartsTheCostBaselineAtItsMidnight_AndComesFromTheDailyTotals(int hour, int minute)
    {
        var (baselineStart, windowStart, regularStart) = Read(hour, minute);
        var day = baselineStart.Date;
        Assert.Equal(day.AddHours(2), regularStart);

        var built = WholeBuilds(day, windowStart);
        var start = QueryStoreRegressionBaseline.ExtendToBuiltFirstDay(built, day, regularStart, windowStart);
        Assert.Equal(day, start);

        var plan = QueryStoreRegressionBaseline.Choose(built, start, windowStart);
        Assert.Equal(DateOnly.FromDateTime(day), plan.RunDays[0]);
        /* The live low arm reads nothing below the first day: its range ends where the cost baseline starts. */
        Assert.Equal(day, plan.LowEnd);
        /* Before the change the first day was never a run day. */
        Assert.NotEqual(DateOnly.FromDateTime(day), QueryStoreRegressionBaseline.Choose(built, regularStart, windowStart).RunDays[0]);
    }

    [Theory]
    [MemberData(nameof(Clocks))]
    public void AStaleOrPartialFirstDay_KeepsTodaysStart(int hour, int minute)
    {
        var (baselineStart, windowStart, regularStart) = Read(hour, minute);
        var day = baselineStart.Date;

        /* Stale: BuiltDaysSql does not return a day whose late_seq moved past its built_seq. */
        var stale = WholeBuilds(day, windowStart).Skip(1).ToList();
        Assert.Equal(regularStart, QueryStoreRegressionBaseline.ExtendToBuiltFirstDay(stale, day, regularStart, windowStart));

        /* Partial: a watermark leaves part of the day live. */
        var partial = WholeBuilds(day, windowStart);
        partial[0] = new PlanRegressionDaily.BuiltDay(partial[0].Day, day.AddHours(20));
        Assert.Equal(regularStart, QueryStoreRegressionBaseline.ExtendToBuiltFirstDay(partial, day, regularStart, windowStart));
    }

    [Theory]
    [MemberData(nameof(Clocks))]
    public void AGapAfterTheFirstDay_KeepsTodaysStart(int hour, int minute)
    {
        var (baselineStart, windowStart, regularStart) = Read(hour, minute);
        var day = baselineStart.Date;
        var gap = WholeBuilds(day, windowStart);
        gap.RemoveAt(1);
        Assert.Equal(regularStart, QueryStoreRegressionBaseline.ExtendToBuiltFirstDay(gap, day, regularStart, windowStart));
    }

    [Fact]
    public void AStartOutsideTheFirstDay_IsUnchanged()
    {
        var (baselineStart, windowStart, _) = Read(2, 30);
        var day = baselineStart.Date;
        var built = WholeBuilds(day, windowStart);

        /* Latest's floor later than the day before the baseline (a young server): the start is past the first day. */
        var later = day.AddDays(1).AddHours(3);
        Assert.Equal(later, QueryStoreRegressionBaseline.ExtendToBuiltFirstDay(built, day, later, windowStart));
        /* Already at the midnight. */
        Assert.Equal(day, QueryStoreRegressionBaseline.ExtendToBuiltFirstDay(built, day, day, windowStart));
    }
}
