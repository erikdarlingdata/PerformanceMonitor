/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Service.Reports;

/// <summary>
/// When the Daily Health Report (#7041) is due, as pure arithmetic over a UTC instant, so the due rule is testable without a clock or a
/// store. The report is due at the first check at or after <see cref="SendAt"/> on the local day in <see cref="Zone"/>; the
/// delivered-today gate (a stamp keyed on that local date) is what stops a second one the same day. The comparison is made in local
/// time, so a send time inside a spring-forward gap becomes due at the first local minute after it, and a send time inside a fall-back
/// repeat is due at its first occurrence.
/// </summary>
public readonly record struct DailyHealthReportSchedule(bool Enabled, TimeOnly SendAt, TimeZoneInfo Zone, string Title)
{
    /// <summary>The local wall-clock time in the report's zone for a UTC instant.</summary>
    public DateTime LocalNow(DateTime nowUtc) =>
        TimeZoneInfo.ConvertTimeFromUtc(DateTime.SpecifyKind(nowUtc, DateTimeKind.Utc), Zone);

    /// <summary>True when the report is enabled and the local time of day has reached <see cref="SendAt"/>.</summary>
    public bool IsDue(DateTime nowUtc) => Enabled && TimeOnly.FromDateTime(LocalNow(nowUtc)) >= SendAt;

    /// <summary>The local calendar date an instant belongs to, the key the delivered-today gate compares.</summary>
    public DateOnly LocalDate(DateTime utc) => DateOnly.FromDateTime(LocalNow(utc));

    /// <summary>
    /// The UTC instant of the most recent <see cref="SendAt"/> at or before <paramref name="nowUtc"/>: today's when it has passed in the
    /// zone, otherwise yesterday's. The 24 hours that end here are the period the report covers. A send time that does not exist on its
    /// day (a spring-forward gap) is moved an hour later, the first valid local time after the gap.
    /// </summary>
    public DateTime MostRecentSendAtUtc(DateTime nowUtc)
    {
        var local = LocalNow(nowUtc);
        var day = local.Date;
        if (TimeOnly.FromDateTime(local) < SendAt)
        {
            day = day.AddDays(-1);
        }

        var slot = DateTime.SpecifyKind(day + SendAt.ToTimeSpan(), DateTimeKind.Unspecified);
        if (Zone.IsInvalidTime(slot))
        {
            slot = slot.AddHours(1);
        }

        return TimeZoneInfo.ConvertTimeToUtc(slot, Zone);
    }
}
