/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Storage.FinOps;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The days the Viewer's FinOps day tables bucket by (<see cref="ViewerDataService.DisplayDays"/>): one per wall-clock day of the
/// display zone across the read's window, bounded by the UTC instants its midnights fall on. Adjacent days share a bound, so every
/// sample lands in exactly one day, on the clock-change days too, where the autumn day runs 25 hours and the spring day 23. The
/// zone is built here with the US rule, so the facts do not depend on the machine's zone data.
/// </summary>
public sealed class ViewerFinOpsDisplayDaysTests
{
    /// <summary>The US west coast by rule: UTC-8, an hour forward from 02:00 on March's second Sunday to 02:00 on November's first Sunday.</summary>
    private static readonly TimeZoneInfo WestCoast = TimeZoneInfo.CreateCustomTimeZone(
        "day-table-west-coast", TimeSpan.FromHours(-8), "West Coast", "West Coast Standard", "West Coast Daylight",
        [
            TimeZoneInfo.AdjustmentRule.CreateAdjustmentRule(
                DateTime.MinValue.Date, DateTime.MaxValue.Date, TimeSpan.FromHours(1),
                TimeZoneInfo.TransitionTime.CreateFloatingDateRule(new DateTime(1, 1, 1, 2, 0, 0), 3, 2, DayOfWeek.Sunday),
                TimeZoneInfo.TransitionTime.CreateFloatingDateRule(new DateTime(1, 1, 1, 2, 0, 0), 11, 1, DayOfWeek.Sunday)),
        ]);

    private static DateTime Utc(int year, int month, int day, int hour = 0) => new(year, month, day, hour, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public void TheAutumnChangeDay_Holds25Hours_AndTheDaysTileWithoutAGapOrAnOverlap()
    {
        /* 2026-11-01 is the first Sunday of November: the west coast falls back at 09:00 UTC. */
        var days = ViewerDataService.DisplayDays(Utc(2026, 10, 31, 12), Utc(2026, 11, 3, 12), WestCoast);

        Assert.Equal([Utc(2026, 10, 31), Utc(2026, 11, 1), Utc(2026, 11, 2), Utc(2026, 11, 3)], days.Select(d => d.Day));
        var changeDay = days[1];
        Assert.Equal(Utc(2026, 11, 1, 7), changeDay.StartUtc);
        Assert.Equal(Utc(2026, 11, 2, 8), changeDay.EndUtc);
        Assert.Equal(TimeSpan.FromHours(25), changeDay.EndUtc - changeDay.StartUtc);
        AssertTiles(days, Utc(2026, 10, 31, 12), Utc(2026, 11, 3, 12));
    }

    [Fact]
    public void TheSpringChangeDay_Holds23Hours()
    {
        /* 2026-03-08 is the second Sunday of March: the west coast springs forward at 10:00 UTC. */
        var days = ViewerDataService.DisplayDays(Utc(2026, 3, 7, 12), Utc(2026, 3, 9, 12), WestCoast);

        var changeDay = Assert.Single(days, d => d.Day == Utc(2026, 3, 8));
        Assert.Equal(Utc(2026, 3, 8, 8), changeDay.StartUtc);
        Assert.Equal(Utc(2026, 3, 9, 7), changeDay.EndUtc);
        Assert.Equal(TimeSpan.FromHours(23), changeDay.EndUtc - changeDay.StartUtc);
        AssertTiles(days, Utc(2026, 3, 7, 12), Utc(2026, 3, 9, 12));
    }

    [Fact]
    public void ASampleAtAUtcSmallHour_LandsInTheDisplaysPreviousDay()
    {
        var sample = Utc(2026, 11, 9, 3);
        var days = ViewerDataService.DisplayDays(sample.AddDays(-7), sample.AddHours(5), WestCoast);

        var holder = Assert.Single(days, d => d.StartUtc <= sample && sample < d.EndUtc);
        Assert.Equal(Utc(2026, 11, 8), holder.Day);
    }

    [Fact]
    public void Utc_GivesTheUtcDays_OnTheirMidnights_TheSameAsTheReadersOwn()
    {
        var from = Utc(2026, 11, 8, 22);
        var to = Utc(2026, 11, 9, 3);

        var days = ViewerDataService.DisplayDays(from, to, TimeZoneInfo.Utc);

        Assert.Equal(
            [new DayBucket(Utc(2026, 11, 8), Utc(2026, 11, 8), Utc(2026, 11, 9)), new DayBucket(Utc(2026, 11, 9), Utc(2026, 11, 9), Utc(2026, 11, 10))],
            days);
        Assert.Equal(DarlingFinOpsUtilizationReader.UtcDays(from, to), days);
    }

    /// <summary>The first day starts at or before the window, the last ends after it, and each day ends where the next begins.</summary>
    private static void AssertTiles(System.Collections.Generic.IReadOnlyList<DayBucket> days, DateTime fromUtc, DateTime toUtc)
    {
        Assert.True(days[0].StartUtc <= fromUtc, $"the first day starts at {days[0].StartUtc:O}, after the window's {fromUtc:O}");
        Assert.True(days[^1].EndUtc > toUtc, $"the last day ends at {days[^1].EndUtc:O}, before the window's {toUtc:O}");
        for (var i = 1; i < days.Count; i++)
        {
            Assert.Equal(days[i - 1].EndUtc, days[i].StartUtc);
        }
    }
}
