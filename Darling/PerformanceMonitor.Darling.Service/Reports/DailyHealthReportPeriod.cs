/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;

namespace PerformanceMonitor.Darling.Service.Reports;

/// <summary>
/// Where a Daily Health Report period ends (#7041): at the report's <c>send_at</c> on a local date in the report's time zone.
/// <c>get_daily_health_report</c> uses it to turn a date into the instant its period ends.
/// </summary>
internal static class DailyHealthReportPeriod
{
    /// <summary>The UTC instant that <paramref name="sendAt"/> falls on <paramref name="date"/> in <paramref name="zone"/>. A time the
    /// zone skips (the spring-forward hour) moves to the end of the gap (the first valid moment after it), and a repeated time takes the first occurrence.</summary>
    public static DateTime EndUtcFor(DateOnly date, TimeOnly sendAt, TimeZoneInfo zone)
    {
        ArgumentNullException.ThrowIfNull(zone);
        var local = DateTime.SpecifyKind(date.ToDateTime(sendAt), DateTimeKind.Unspecified);
        if (zone.IsInvalidTime(local))
        {
            /* The end of the gap, not an hour past the send time: 02:30 on a spring-forward day ends at 03:00, the instant the
               clock jumps, so a report built the moment it is due does not claim a period that ends in the future (#7201). Step
               by whole minutes from the send time's minute: a transition lands on one, and the gap is at most a few hours. */
            local = new DateTime(local.Ticks - (local.Ticks % TimeSpan.TicksPerMinute), DateTimeKind.Unspecified);
            while (zone.IsInvalidTime(local)) local = local.AddMinutes(1);
        }

        return LocalToUtc(local, zone);
    }

    /// <summary>The UTC instant of a local wall-clock time. A time that repeats when the clock falls back is the FIRST occurrence (the
    /// daylight one, the larger offset), where <see cref="TimeZoneInfo.ConvertTimeToUtc(DateTime, TimeZoneInfo)"/> alone picks the
    /// second and would put a period end an hour in the future.</summary>
    internal static DateTime LocalToUtc(DateTime local, TimeZoneInfo zone)
    {
        var unspecified = DateTime.SpecifyKind(local, DateTimeKind.Unspecified);
        if (zone.IsAmbiguousTime(unspecified))
        {
            var daylightOffset = zone.GetAmbiguousTimeOffsets(unspecified).Max();
            return DateTime.SpecifyKind(unspecified - daylightOffset, DateTimeKind.Utc);
        }

        return DateTime.SpecifyKind(TimeZoneInfo.ConvertTimeToUtc(unspecified, zone), DateTimeKind.Utc);
    }

    /// <summary>The local date whose period was the latest to end at or before <paramref name="nowUtc"/>: today's when
    /// <paramref name="sendAt"/> has passed in the zone, otherwise yesterday's.</summary>
    public static DateOnly LatestEndedDate(DateTime nowUtc, TimeOnly sendAt, TimeZoneInfo zone)
    {
        ArgumentNullException.ThrowIfNull(zone);
        var nowUtcKind = DateTime.SpecifyKind(nowUtc, DateTimeKind.Utc);
        var today = DateOnly.FromDateTime(TimeZoneInfo.ConvertTimeFromUtc(nowUtcKind, zone));
        return EndUtcFor(today, sendAt, zone) <= nowUtcKind ? today : today.AddDays(-1);
    }
}
