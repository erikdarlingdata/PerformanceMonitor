/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The day arithmetic of the PLAN_REGRESSION daily read (#5448), with no store: M, the day-aligned window start, and the
/// live floor that bounds the interval-table half of the read.
/// </summary>
public sealed class PlanRegressionDailyBoundaryTests
{
    private static DateOnly D(int day) => new(2026, 9, day);

    private static DateTime At(int day, int hour = 0, int minute = 0) => new(2026, 9, day, hour, minute, 0, DateTimeKind.Unspecified);

    [Fact]
    public void TheWindowFloor_IsTheStartOfTheDayTheExactEdgeFallsIn()
    {
        Assert.Equal(At(10), PgFactCollector.PlanRegressionWindowFloor(At(10)));
        Assert.Equal(At(10), PgFactCollector.PlanRegressionWindowFloor(At(10, 17, 45)));
        /* One tick before midnight is still the same day; midnight is the next. */
        Assert.Equal(At(10), PgFactCollector.PlanRegressionWindowFloor(At(11).AddTicks(-1)));
        Assert.Equal(At(11), PgFactCollector.PlanRegressionWindowFloor(At(11)));
        Assert.Equal(DateTimeKind.Unspecified, PgFactCollector.PlanRegressionWindowFloor(DateTime.SpecifyKind(At(10, 5), DateTimeKind.Utc)).Kind);
    }

    [Fact]
    public void TheLiveFloor_IsADayBelowTheFirstUnbuiltDay()
    {
        var floor = At(1);

        /* Nothing built: the first day itself is unbuilt, so the floor is the day before the window. */
        Assert.Equal(At(1).AddDays(-1), PgFactCollector.PlanRegressionLiveFloor(floor, []));

        /* A contiguous run from the start: the floor follows its end (day 5 is the first unbuilt, so day 4's start). */
        Assert.Equal(At(4), PgFactCollector.PlanRegressionLiveFloor(floor, [D(1), D(2), D(3), D(4)]));
    }

    [Fact]
    public void TheLiveFloor_StopsAtTheFirstGap_EvenWithLaterDaysBuilt()
    {
        /* Days 1-2 and 4-9 built, 3 missing: every unbuilt row from day 3 on must be reachable, and a row of day 3 may
           have begun a day earlier, so the floor is day 2's start. */
        var built = new[] { D(1), D(2), D(4), D(5), D(6), D(7), D(8), D(9) };
        Assert.Equal(At(2), PgFactCollector.PlanRegressionLiveFloor(At(1), built));

        /* A gap at the very start, though later days are built. */
        Assert.Equal(At(1).AddDays(-1), PgFactCollector.PlanRegressionLiveFloor(At(1), [D(2), D(3)]));
    }

    [Fact]
    public void TheLiveFloor_IgnoresBuiltDaysBelowTheWindow_AndDuplicates()
    {
        /* Window from day 5; days 5 and 6 built, 7 is the first unbuilt, so the floor is day 6's start. */
        Assert.Equal(At(6), PgFactCollector.PlanRegressionLiveFloor(At(5), [D(1), D(2), D(5), D(5), D(6)]));
    }

    [Fact]
    public void AStraddlingInterval_IsAdmittedByTheLiveFloor()
    {
        /* An interval that opens 23:40 on day 3 and closes 00:35 on day 4 belongs to day 4 (its last execution). With days
           1-3 built and day 4 live, the live floor is day 3's start, so first_execution_time >= floor admits it. */
        var floor = PgFactCollector.PlanRegressionLiveFloor(At(1), [D(1), D(2), D(3)]);
        Assert.Equal(At(3), floor);
        Assert.True(At(3, 23, 40) >= floor);

        /* An interval ending exactly at midnight belongs to the new day; one ending a tick before to the old one. */
        Assert.Equal(D(4), DateOnly.FromDateTime(At(4)));
        Assert.Equal(D(3), DateOnly.FromDateTime(At(4).AddTicks(-1)));
    }

    [Fact]
    public void TheDailyRead_IsInTheCensus_AndSharesTheSuffixWithTheTableRead()
    {
        Assert.Contains(PgFactCollector.PlanRegressionDailySql, PgFactCollector.AllSql);
        var marker = "plan_dedup AS";
        var tail = PgFactCollector.PlanRegressionTableSql[PgFactCollector.PlanRegressionTableSql.IndexOf(marker, StringComparison.Ordinal)..];
        Assert.EndsWith(tail, PgFactCollector.PlanRegressionDailySql, StringComparison.Ordinal);
    }

    /// <summary>The fact runs the built-days read and the daily read in one REPEATABLE READ READ ONLY transaction (#5448),
    /// and the daily read's live floor is a bound parameter the planner can see, not a sub-select of the statement.</summary>
    [Fact]
    public void TheFact_RunsBothReadsInOneRepeatableReadReadOnlyTransaction_WithTheLiveFloorBound()
    {
        var dir = AppContext.BaseDirectory;
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir, "Darling", "PerformanceMonitor.Darling.Analysis", "PgFactCollector.QueryPerf.cs")))
        {
            dir = System.IO.Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        var source = System.IO.File.ReadAllText(System.IO.Path.Combine(dir!, "Darling", "PerformanceMonitor.Darling.Analysis", "PgFactCollector.QueryPerf.cs"));
        var start = source.IndexOf("private async Task CollectPlanRegressionFactsAsync(", StringComparison.Ordinal);
        var fact = source[start..source.IndexOf("public const string ProcedureStatsSql", start, StringComparison.Ordinal)];

        Assert.Contains("BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead", fact, StringComparison.Ordinal);
        Assert.Contains("SET TRANSACTION READ ONLY", fact, StringComparison.Ordinal);
        /* #5812 bound 2: the work memory is raised for this transaction only (SET LOCAL), inside the snapshot. */
        Assert.Contains("new NpgsqlCommand(PlanRegressionWorkMemSql, connection, snapshot)", fact, StringComparison.Ordinal);
        Assert.StartsWith("SET LOCAL work_mem = ", PgFactCollector.PlanRegressionWorkMemSql, StringComparison.Ordinal);
        Assert.Contains("connection, snapshot, context.ServerId, windowFloor", fact, StringComparison.Ordinal);
        Assert.Contains("readsDays ? PlanRegressionDailySql : readsTable ? PlanRegressionTableSql : PlanRegressionSql, connection, snapshot)", fact, StringComparison.Ordinal);

        var sql = PgFactCollector.PlanRegressionDailySql;
        Assert.Contains("l.first_execution_time >= $4::timestamp", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("live_floor", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("built_days", sql, StringComparison.Ordinal);
    }

    private static PlanRegressionDaily.BuiltDay Whole(int day) => new(D(day), null);

    private static PlanRegressionDaily.BuiltDay Through(int day, DateTime watermark) => new(D(day), watermark);

    private static DateTime Floor(int windowDay, params PlanRegressionDaily.BuiltDay[] built) =>
        PgFactCollector.PlanRegressionLiveFloorWithWatermarks(At(windowDay), built);

    [Fact]
    public void TheWatermarkFloor_WithNothingBuilt_IsADayBelowTheWindow()
    {
        /* #5812: the first unbuilt day is the window's own first day, and a row of it may have begun the day before (R1's
           one-day lookback), so the floor is one day below the window floor, as before V185. */
        Assert.Equal(At(1).AddDays(-1), Floor(1));
    }

    [Fact]
    public void TheWatermarkFloor_OverContiguousWholeDays_IsADayBelowTheFirstUnbuiltDay()
    {
        /* Days 1-4 whole (NULL through, a pre-V185 build, reads as the next midnight): day 5 is the first unbuilt day, so
           the floor is day 4's start, not day 5's (that would drop day 5's rows that began on day 4) nor day 3's. */
        Assert.Equal(At(4), Floor(1, Whole(1), Whole(2), Whole(3), Whole(4)));

        /* A watermark exactly at the next midnight is a whole day, not a partial one: it does not pull the floor up to the
           day's end or below the lookback. */
        Assert.Equal(At(4), Floor(1, Whole(1), Through(2, At(3)), Through(3, At(4)), Whole(4)));
    }

    [Fact]
    public void TheWatermarkFloor_WithPartialDays_IsTheLowestPartialWatermark()
    {
        /* A current store: days 1-12 whole, today (13) and tomorrow (14) built through two hours before the current hour.
           The first unbuilt day is 15, whose lookback (day 14's start) is above today's watermark, so the floor is that
           watermark. No lookback is taken below a partial day's watermark: a row of a built day is live only when its first
           execution is at or above the day's watermark, so the watermark itself bounds every live row of that day. */
        var today = At(13, 10);
        var built = Enumerable.Range(1, 12).Select(Whole).Append(Through(13, today)).Append(Through(14, today)).ToArray();
        Assert.Equal(today, Floor(1, built));

        /* Two partial days: the lower watermark wins, wherever it sits in the run. */
        Assert.Equal(At(11, 22), Floor(10, Whole(10), Through(11, At(11, 22)), Whole(12), Through(13, today)));

        /* A partial watermark above the first unbuilt day's lookback does not raise the floor past it. */
        Assert.Equal(At(11), Floor(10, Whole(10), Through(11, At(11, 23))));
    }

    [Fact]
    public void TheWatermarkFloor_StopsAtTheFirstGap_AndIgnoresPartialDaysAfterIt()
    {
        /* Days 1-3 whole, 4 missing: the floor is day 3's start. Day 5's partial watermark lies beyond the gap, where every
           row is read live anyway, so it does not take part. */
        Assert.Equal(At(3), Floor(1, Whole(1), Whole(2), Whole(3), Through(5, At(5, 6))));

        /* A partial day before the gap still counts: its watermark is below the gap's lookback. */
        Assert.Equal(At(2, 5), Floor(1, Whole(1), Through(2, At(2, 5)), Whole(3)));
    }

    [Fact]
    public void TheWatermarkFloor_NeverFallsMoreThanTheLookbackBelowTheWindow()
    {
        /* Built days below the window take no part, even a partial one with an early watermark: the walk starts at the
           window floor. Window from day 5, days 5-6 whole, so the floor is day 6's start. */
        Assert.Equal(At(6), Floor(5, Through(1, At(1, 3)), Whole(2), Whole(5), Whole(6)));

        /* The lowest the floor goes for any built set whose watermarks lie in their own days is the window less one day:
           the unbuilt lookback when the window's first day is not built, and the window's own midnight when its first
           day is built through that midnight. */
        Assert.Equal(At(4), Floor(5, Whole(6), Whole(7)));
        Assert.Equal(At(5), Floor(5, Through(5, At(5)), Whole(6)));
        foreach (var watermarkHour in new[] { 0, 1, 12, 23 })
        {
            Assert.True(Floor(5, Through(5, At(5, watermarkHour))) >= At(5).AddDays(-1));
        }
    }
}
