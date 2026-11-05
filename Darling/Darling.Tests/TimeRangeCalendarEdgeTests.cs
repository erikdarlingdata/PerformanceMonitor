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
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7062: the calendar periods of the shared time range picker, resolved through the clock seam
/// (<see cref="TimeRangeSpec.TryResolve"/> takes "now" and the display zone) at the instants a calendar bends:
/// the first minutes of a month and a year, the last second of both, 29 February and the day after it, and both
/// daylight saving changes in zones north and south of the equator, plus a zone whose change skips local midnight.
/// No test here reads the real clock, so the answer is the same on every day the suite runs.
/// </summary>
public sealed class TimeRangeCalendarEdgeTests
{
    private static readonly string[] ZoneIds =
    [
        "UTC",
        "America/New_York",
        "Australia/Sydney",
        "America/Havana" /* spring forward at 00:00, so local midnight does not exist on that day */
    ];

    private static readonly DateTime[] EdgeInstants =
    [
        new(2026, 10, 31, 23, 59, 59), /* last second of a month */
        new(2026, 11, 1, 0, 0, 0),     /* first second of a month */
        new(2026, 11, 1, 0, 49, 0),    /* the window #7055 hit */
        new(2026, 12, 31, 23, 59, 59), /* last second of a year */
        new(2027, 1, 1, 0, 0, 0),      /* first second of a year */
        new(2028, 2, 29, 0, 0, 0),     /* leap day */
        new(2028, 2, 29, 23, 59, 59),
        new(2028, 3, 1, 0, 0, 0),      /* the day after the leap day */
        new(2029, 1, 1, 0, 10, 0),     /* a Monday: the week and the year start together */
        new(2026, 3, 8, 5, 30, 0),     /* New York spring forward is 07:00 UTC, Havana's 05:00 UTC */
        new(2026, 3, 8, 12, 0, 0),
        new(2026, 11, 1, 5, 30, 0),    /* New York fall back is 06:00 UTC */
        new(2026, 11, 1, 6, 30, 0),
        new(2026, 10, 3, 15, 30, 0),   /* Sydney spring forward is Oct 3 16:00 UTC */
        new(2026, 10, 3, 16, 30, 0),
        new(2027, 4, 3, 15, 30, 0),    /* Sydney fall back is Apr 3 16:00 UTC */
        new(2027, 4, 3, 16, 30, 0)
    ];

    public static IEnumerable<object[]> InstantsAndZones()
    {
        foreach (var instant in EdgeInstants)
        {
            foreach (var zoneId in ZoneIds)
            {
                yield return [instant, zoneId];
            }
        }
    }

    private static ResolvedTimeRange Resolve(CalendarPeriod period, DateTime nowUtc, TimeZoneInfo zone)
    {
        Assert.True(TimeRangeSpec.ForPeriod(period).TryResolve(nowUtc, zone, out var range, out var error),
            $"{period} at {nowUtc:O} in {zone.Id} refused: {error?.Message}");
        return range!;
    }

    /// <summary>
    /// A to-date period at its own first instant is zero length and refused as "too_short" by design (the picker
    /// asks for another range). Any other refusal is a failure, so this returns null only for that case.
    /// </summary>
    private static ResolvedTimeRange? ResolveToDate(CalendarPeriod period, DateTime nowUtc, TimeZoneInfo zone, DateTime localStartDate)
    {
        if (TimeRangeSpec.ForPeriod(period).TryResolve(nowUtc, zone, out var range, out var error))
        {
            return range;
        }

        Assert.Equal("too_short", error!.Code);
        Assert.True(DisplayZone.ToDisplay(nowUtc, zone) - localStartDate < TimeSpan.FromHours(2),
            $"{period} at {nowUtc:O} in {zone.Id} was refused as just started, but {localStartDate:yyyy-MM-dd} began long before");
        return null;
    }

    /// <summary>
    /// Each "previous" period ends exactly where its to-date partner starts, and a to-date period starts on the first
    /// local day of its week, month or year and ends at now. Local midnight that a daylight saving change skipped
    /// is read as the first valid local time after it, so the start still falls on the right local date.
    /// </summary>
    [Theory]
    [MemberData(nameof(InstantsAndZones))]
    public void ThePeriods_TileAndStartOnTheRightLocalDay_AtEveryEdge(DateTime nowUtc, string zoneId)
    {
        var zone = TimeZoneInfo.FindSystemTimeZoneById(zoneId);
        var localToday = DisplayZone.ToDisplay(nowUtc, zone).Date;
        var localMonday = localToday.AddDays(-(((int)localToday.DayOfWeek + 6) % 7));

        var firstOfMonth = new DateTime(localToday.Year, localToday.Month, 1);
        var firstOfYear = new DateTime(localToday.Year, 1, 1);
        var today = ResolveToDate(CalendarPeriod.Today, nowUtc, zone, localToday);
        var yesterday = Resolve(CalendarPeriod.Yesterday, nowUtc, zone);
        var week = ResolveToDate(CalendarPeriod.WeekToDate, nowUtc, zone, localMonday);
        var lastWeek = Resolve(CalendarPeriod.PreviousWeek, nowUtc, zone);
        var month = ResolveToDate(CalendarPeriod.MonthToDate, nowUtc, zone, firstOfMonth);
        var lastMonth = Resolve(CalendarPeriod.PreviousMonth, nowUtc, zone);
        var year = ResolveToDate(CalendarPeriod.YearToDate, nowUtc, zone, firstOfYear);
        var lastYear = Resolve(CalendarPeriod.PreviousYear, nowUtc, zone);

        if (today is not null) Assert.Equal(yesterday.EndUtc, today.StartUtc);
        if (week is not null) Assert.Equal(lastWeek.EndUtc, week.StartUtc);
        if (month is not null) Assert.Equal(lastMonth.EndUtc, month.StartUtc);
        if (year is not null) Assert.Equal(lastYear.EndUtc, year.StartUtc);

        foreach (var toDate in new[] { today, week, month, year }.OfType<ResolvedTimeRange>())
        {
            Assert.True(toDate.IsLive);
            Assert.Equal(nowUtc, toDate.EndUtc);
            Assert.True(toDate.StartUtc <= nowUtc, $"{toDate.Spec.Id} starts after now at {nowUtc:O} in {zoneId}");
        }

        foreach (var finished in new[] { yesterday, lastWeek, lastMonth, lastYear })
        {
            Assert.False(finished.IsLive);
            Assert.True(finished.StartUtc < finished.EndUtc, $"{finished.Spec.Id} is empty at {nowUtc:O} in {zoneId}");
        }

        if (today is not null) AssertStartsOn(today.StartUtc, localToday, zone);
        AssertStartsOn(yesterday.StartUtc, localToday.AddDays(-1), zone);
        if (week is not null) AssertStartsOn(week.StartUtc, localMonday, zone);
        AssertStartsOn(lastWeek.StartUtc, localMonday.AddDays(-7), zone);
        if (month is not null) AssertStartsOn(month.StartUtc, firstOfMonth, zone);
        AssertStartsOn(lastMonth.StartUtc, new DateTime(localToday.Year, localToday.Month, 1).AddMonths(-1), zone);
        if (year is not null) AssertStartsOn(year.StartUtc, firstOfYear, zone);
        AssertStartsOn(lastYear.StartUtc, new DateTime(localToday.Year - 1, 1, 1), zone);
    }

    private static void AssertStartsOn(DateTime startUtc, DateTime localDate, TimeZoneInfo zone)
    {
        var local = DisplayZone.ToDisplay(startUtc, zone);
        Assert.Equal(localDate, local.Date);
        /* Midnight itself, or the first local time after a midnight a spring-forward change skipped. */
        Assert.True(local.TimeOfDay < TimeSpan.FromHours(2), $"{startUtc:O} reads {local:O} in {zone.Id}, not the start of {localDate:yyyy-MM-dd}");
    }

    /// <summary>
    /// The lengths a calendar gives, in UTC where a day is always 24 hours: the month after a leap February,
    /// the leap year itself, and a week that spans the new year.
    /// </summary>
    [Theory]
    [InlineData(2028, 3, 1, 0, 0, 0, CalendarPeriod.PreviousMonth, 29)]   /* February 2028 */
    [InlineData(2028, 2, 29, 12, 0, 0, CalendarPeriod.PreviousMonth, 31)] /* January 2028 */
    [InlineData(2028, 2, 29, 12, 0, 0, CalendarPeriod.Yesterday, 1)]
    [InlineData(2028, 3, 1, 0, 0, 0, CalendarPeriod.Yesterday, 1)]
    [InlineData(2029, 1, 1, 0, 10, 0, CalendarPeriod.PreviousYear, 366)]  /* 2028 */
    [InlineData(2028, 1, 1, 0, 0, 0, CalendarPeriod.PreviousYear, 365)]   /* 2027 */
    [InlineData(2027, 1, 1, 0, 0, 0, CalendarPeriod.PreviousMonth, 31)]   /* December 2026 */
    [InlineData(2029, 1, 1, 0, 10, 0, CalendarPeriod.PreviousWeek, 7)]
    [InlineData(2026, 11, 1, 0, 49, 0, CalendarPeriod.PreviousMonth, 31)] /* October 2026 */
    [InlineData(2026, 12, 1, 0, 0, 0, CalendarPeriod.PreviousMonth, 30)]  /* November 2026 */
    public void TheFinishedPeriods_HaveTheLengthTheCalendarGives_InUtc(int y, int mo, int d, int h, int mi, int s, CalendarPeriod period, int days)
    {
        var range = Resolve(period, new DateTime(y, mo, d, h, mi, s), TimeZoneInfo.Utc);
        Assert.Equal(TimeSpan.FromDays(days), range.EndUtc - range.StartUtc);
    }

    /// <summary>The exact instants at the two edges that matter most: a year turning, and a leap day closing.</summary>
    [Fact]
    public void ANewYearsFirstSecond_RefusesAnEmptyYearToDate_AndReadsTheWholeYearBefore()
    {
        var now = new DateTime(2027, 1, 1, 0, 0, 0);
        Assert.False(TimeRangeSpec.ForPeriod(CalendarPeriod.YearToDate).TryResolve(now, TimeZoneInfo.Utc, out _, out var refusal));
        Assert.Equal("too_short", refusal!.Code);
        Assert.True(TimeRangeSpec.ForPeriod(CalendarPeriod.YearToDate).TryResolve(now.AddMinutes(1), TimeZoneInfo.Utc, out var year, out _));
        Assert.Equal(now, year!.StartUtc);

        var lastYear = Resolve(CalendarPeriod.PreviousYear, now, TimeZoneInfo.Utc);
        Assert.Equal(new DateTime(2026, 1, 1), lastYear.StartUtc);
        Assert.Equal(now, lastYear.EndUtc);

        var lastMonth = Resolve(CalendarPeriod.PreviousMonth, now, TimeZoneInfo.Utc);
        Assert.Equal(new DateTime(2026, 12, 1), lastMonth.StartUtc);
    }

    [Fact]
    public void TheLeapDay_IsTheWholeOfYesterday_OnTheFirstOfMarch()
    {
        var yesterday = Resolve(CalendarPeriod.Yesterday, new DateTime(2028, 3, 1, 0, 30, 0), TimeZoneInfo.Utc);
        Assert.Equal(new DateTime(2028, 2, 29), yesterday.StartUtc);
        Assert.Equal(new DateTime(2028, 3, 1), yesterday.EndUtc);

        var month = Resolve(CalendarPeriod.MonthToDate, new DateTime(2028, 2, 29, 23, 59, 59), TimeZoneInfo.Utc);
        Assert.Equal(new DateTime(2028, 2, 1), month.StartUtc);
    }
}
