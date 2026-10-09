/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Ui;

/// <summary>
/// A <see cref="TimeRangeSpec"/> pinned to a now and a display zone (#5562): the naive-UTC start and end a tab
/// reads, whether the end slides with now, and the one-line label the picker shows. Immutable; a live range is
/// re-resolved (<see cref="TimeRangeSpec.TryResolve"/>) on each refresh.
/// </summary>
public sealed class ResolvedTimeRange
{
    internal ResolvedTimeRange(TimeRangeSpec spec, DateTime startUtc, DateTime endUtc, bool isLive, TimeZoneInfo zone, DateTime nowUtc)
    {
        Spec = spec;
        StartUtc = startUtc;
        EndUtc = endUtc;
        IsLive = isLive;
        Zone = zone;
        NowUtc = nowUtc;
    }

    /// <summary>The range as named.</summary>
    public TimeRangeSpec Spec { get; }

    /// <summary>The first instant, naive UTC.</summary>
    public DateTime StartUtc { get; }

    /// <summary>The last instant, naive UTC. For a live range this is the now it was resolved at.</summary>
    public DateTime EndUtc { get; }

    /// <summary>True when the end slides with now: a relative range, Today and the to-date periods, and "since".</summary>
    public bool IsLive { get; }

    /// <summary>The display zone the range was resolved in.</summary>
    public TimeZoneInfo Zone { get; }

    /// <summary>The now the range was resolved at, naive UTC.</summary>
    public DateTime NowUtc { get; }

    /// <summary>The length of the range.</summary>
    public TimeSpan Span => EndUtc - StartUtc;

    /// <summary>The length as the picker words it ("3d", "7h 1m", "45m").</summary>
    public string Length => TimeRangeSpec.FormatLength(Span);

    /// <summary>The start as text in the display zone: "Oct 5, 12:00 am" (the year too when a bound is not in now's year).</summary>
    public string StartText => FormatBound(StartUtc, includeSeconds: Spec.Kind == TimeRangeKind.Fixed);

    /// <summary>The end as text in the display zone, like <see cref="StartText"/>.</summary>
    public string EndText => FormatBound(EndUtc, includeSeconds: Spec.Kind == TimeRangeKind.Fixed);

    /// <summary>
    /// The zone as the label words it: "UTC-04:00", or "UTC-05:00 to UTC-04:00" when a daylight saving change
    /// falls between the two ends.
    /// </summary>
    public string ZoneText
    {
        get
        {
            var a = "UTC" + DisplayZone.UtcOffsetText(StartUtc, Zone);
            var b = "UTC" + DisplayZone.UtcOffsetText(EndUtc, Zone);
            return a == b ? a : a + " to " + b;
        }
    }

    /// <summary>The one line the picker shows and the parser echoes: "3d  Oct 5, 12:00 am - Oct 8, 7:01 am (UTC-04:00)".</summary>
    public string Label => Length + "  " + StartText + " - " + EndText + " (" + ZoneText + ")";

    /// <summary>
    /// A moment as the picker prints it: month, day, 12-hour clock with lower-case am/pm, the year when this
    /// range touches a year other than now's, and the UTC offset after a wall time that happens twice.
    /// </summary>
    internal string FormatBound(DateTime utc, bool includeSeconds)
    {
        var wall = DisplayZone.ToDisplay(utc, Zone);
        var wallNow = DisplayZone.ToDisplay(NowUtc, Zone);
        var showYear = DisplayZone.ToDisplay(StartUtc, Zone).Year != wallNow.Year || DisplayZone.ToDisplay(EndUtc, Zone).Year != wallNow.Year;
        var text = TimeRangeFormat.Wall(wall, showYear, includeSeconds);
        var suffix = DisplayZone.AmbiguousOffsetSuffix(utc, Zone);
        return suffix is null ? text : text + " " + suffix;
    }
}

/// <summary>The text forms the picker shares with the web module, so the shared fixture can pin them.</summary>
internal static class TimeRangeFormat
{
    /// <summary>"Oct 5, 12:00 am", "Oct 5, 2025, 12:00 am", with ":ss" when asked and the seconds are not zero.</summary>
    internal static string Wall(DateTime wall, bool showYear, bool includeSeconds)
    {
        var hour12 = wall.Hour % 12 == 0 ? 12 : wall.Hour % 12;
        var clock = hour12.ToString(CultureInfo.InvariantCulture) + ":" + wall.Minute.ToString("00", CultureInfo.InvariantCulture);
        if (includeSeconds && wall.Second != 0)
        {
            clock += ":" + wall.Second.ToString("00", CultureInfo.InvariantCulture);
        }

        var day = wall.ToString(showYear ? "MMM d, yyyy" : "MMM d", CultureInfo.InvariantCulture);
        return day + ", " + clock + (wall.Hour < 12 ? " am" : " pm");
    }
}
