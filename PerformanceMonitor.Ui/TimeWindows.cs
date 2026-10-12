/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The windows a chart, a drill and a comparison line are built from, all in the naive-UTC frame (#4766): a
/// window is a pair of UTC instants from the moment it is made to the read that fetches it, and no step in
/// between converts a bound through a server wall clock. Where a window is DEFINED by the wall clock (a
/// comparison "the same hours yesterday"), the wall clock is applied once, here, through a
/// <see cref="TimeZoneInfo"/> the caller passes, because <c>PerformanceMonitor.Ui</c> does not reference the
/// server-clock type. Pure functions of their arguments: nothing reads the machine's zone or its clock.
/// </summary>
public static class TimeWindows
{
    /// <summary>
    /// The window a chart's X axis spans: the custom range as held when both bounds are set, else a preset's last
    /// <paramref name="hoursBack"/> real hours ending at <paramref name="utcNow"/>. Real hours, not wall-clock
    /// hours: across a clock change the preset holds exactly <paramref name="hoursBack"/> hours of data.
    /// </summary>
    public static (DateTime FromUtc, DateTime ToUtc) ChartAxis(
        int hoursBack, DateTime? fromUtc, DateTime? toUtc, DateTime utcNow)
    {
        if (fromUtc.HasValue && toUtc.HasValue)
        {
            return (Naive(fromUtc.Value), Naive(toUtc.Value));
        }

        return (Naive(utcNow.AddHours(-hoursBack)), Naive(utcNow));
    }

    /// <summary>
    /// The window a drill from <paramref name="centerUtc"/> opens: <paramref name="minutesBefore"/> real minutes
    /// before it to <paramref name="minutesAfter"/> after it. Arithmetic on the instant, so a drill in the repeated
    /// hour opens the sixty real minutes around the point clicked and never collapses to one instant.
    /// </summary>
    public static (DateTime FromUtc, DateTime ToUtc) Drill(DateTime centerUtc, int minutesBefore, int minutesAfter)
    {
        var center = Naive(centerUtc);
        return (center.AddMinutes(-minutesBefore), center.AddMinutes(minutesAfter));
    }

    /// <summary>
    /// The comparison window of the Overview lanes: the same wall-clock hours <paramref name="days"/> days earlier
    /// in <paramref name="zone"/> (the monitored server's). Each current bound is read as its wall clock, moved back
    /// whole days on that clock, and turned into an instant by the range rule (<see cref="DisplayZone.ToUtcBound"/>):
    /// From at the earliest instant the moved time can mean and To at the latest. So 09:00 to 13:00 today reads as
    /// 09:00 to 13:00 yesterday even when a clock change puts yesterday's 09:00 more or less than 24 real hours
    /// before today's.
    /// </summary>
    public static (DateTime FromUtc, DateTime ToUtc) OverviewReference(
        DateTime fromUtc, DateTime toUtc, int days, TimeZoneInfo zone)
    {
        var from = DisplayZone.ToDisplay(fromUtc, zone).AddDays(-days);
        var to = DisplayZone.ToDisplay(toUtc, zone).AddDays(-days);
        return (DisplayZone.ToUtcBound(from, zone, BoundSide.From), DisplayZone.ToUtcBound(to, zone, BoundSide.To));
    }

    /// <summary>
    /// Where a comparison row lands on the current axis: the instant of the row's wall clock in
    /// <paramref name="zone"/> plus <paramref name="days"/> whole days. The inverse alignment of
    /// <see cref="OverviewReference"/>, so a row at 09:00 yesterday sits at 09:00 today. A time that is repeated
    /// today takes its first occurrence and one that never happens moves forward by the gap
    /// (<see cref="DisplayZone.ToUtcFirstOccurrence"/>), the rule the head applies to a stored server-local stamp.
    /// </summary>
    public static DateTime GhostX(DateTime rowUtc, int days, TimeZoneInfo zone)
        => DisplayZone.ToUtcFirstOccurrence(DisplayZone.ToDisplay(rowUtc, zone).AddDays(days), zone);

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);
}
