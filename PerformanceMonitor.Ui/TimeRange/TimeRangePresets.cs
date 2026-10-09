/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The ranges the picker lists (#5562): nine short presets and eight calendar periods. All are
/// <see cref="TimeRangeSpec"/>s, so a preset, a typed range and a saved setting resolve through one path.
/// </summary>
public static class TimeRangePresets
{
    /// <summary>The short presets, shortest first: 5m 15m 30m 1h 4h 1d 2d 1w 1mo (1mo is 30 real days).</summary>
    public static IReadOnlyList<TimeRangeSpec> Rolling { get; } = new[]
    {
        TimeRangeSpec.Relative(TimeSpan.FromMinutes(5)),
        TimeRangeSpec.Relative(TimeSpan.FromMinutes(15)),
        TimeRangeSpec.Relative(TimeSpan.FromMinutes(30)),
        TimeRangeSpec.Relative(TimeSpan.FromHours(1)),
        TimeRangeSpec.Relative(TimeSpan.FromHours(4)),
        TimeRangeSpec.Relative(TimeSpan.FromDays(1)),
        TimeRangeSpec.Relative(TimeSpan.FromDays(2)),
        TimeRangeSpec.Relative(TimeSpan.FromDays(7)),
        TimeRangeSpec.Relative(TimeSpan.FromDays(30))
    };

    /// <summary>The calendar periods: Today, Yesterday, Week to Date, Previous Week, Month to Date, Previous Month, Year to Date, Previous Year.</summary>
    public static IReadOnlyList<TimeRangeSpec> CalendarPeriods { get; } = new[]
    {
        TimeRangeSpec.ForPeriod(CalendarPeriod.Today),
        TimeRangeSpec.ForPeriod(CalendarPeriod.Yesterday),
        TimeRangeSpec.ForPeriod(CalendarPeriod.WeekToDate),
        TimeRangeSpec.ForPeriod(CalendarPeriod.PreviousWeek),
        TimeRangeSpec.ForPeriod(CalendarPeriod.MonthToDate),
        TimeRangeSpec.ForPeriod(CalendarPeriod.PreviousMonth),
        TimeRangeSpec.ForPeriod(CalendarPeriod.YearToDate),
        TimeRangeSpec.ForPeriod(CalendarPeriod.PreviousYear)
    };

    /// <summary>The range the old hour lists meant: 1, 4, 12, 24 and 168 hours, and any other whole number of hours. <c>null</c> for zero or less.</summary>
    /// <remarks>24 hours is "1d" and 168 is "1w": the same instants, the id the new lists use. Also <c>null</c> past the 100-year relative limit (#5562 review: a corrupt settings value threw OverflowException).</remarks>
    public static TimeRangeSpec? FromLegacyHours(int hours)
        => hours <= 0 || hours > TimeRangeSpec.MaxRelative.TotalHours ? null : TimeRangeSpec.Relative(TimeSpan.FromHours(hours));

    /// <summary>What "Apply to All" sends for a held range (#5562, M2; Lite and the Viewer share this). A relative range and a fixed or
    /// since range go as they are (every tab then windows on the same period, as #4766 settled). A calendar period is resolved ONCE
    /// here, in the source tab's zone, and sent as the instants it names - or as a since range while it still runs (Today) - because
    /// each tab draws its own server's clock and "Today" would otherwise mean a different day on each. A period that cannot be
    /// resolved right now goes as it is.</summary>
    public static TimeRangeSpec ForBroadcast(TimeRangeSpec spec, DateTime nowUtc, TimeZoneInfo zone)
    {
        ArgumentNullException.ThrowIfNull(spec);
        ArgumentNullException.ThrowIfNull(zone);
        if (spec.Kind != TimeRangeKind.Calendar || !spec.TryResolve(nowUtc, zone, out var range, out _) || range is null)
        {
            return spec;
        }

        return range.IsLive ? TimeRangeSpec.SinceInstant(range.StartUtc) : TimeRangeSpec.FixedRange(range.StartUtc, range.EndUtc);
    }

    /// <summary>The length of a calendar period at <paramref name="nowUtc"/> in <paramref name="zone"/> ("3d", "7h 1m"), or <c>null</c> when the period cannot be used right now: only at zero length (exactly midnight for Today), because a calendar period is exempt from the 5-minute floor (#5562).</summary>
    public static string? CurrentLength(TimeRangeSpec spec, DateTime nowUtc, TimeZoneInfo zone)
        => spec.TryResolve(nowUtc, zone, out var range, out _) ? range!.Length : null;

    /// <summary>Finds a preset (short or calendar) by <see cref="TimeRangeSpec.Id"/>, or <c>null</c>.</summary>
    public static TimeRangeSpec? Find(string? id)
    {
        if (string.IsNullOrWhiteSpace(id))
        {
            return null;
        }

        var wanted = id.Trim().ToLowerInvariant();
        foreach (var spec in Rolling)
        {
            if (string.Equals(spec.Id, wanted, StringComparison.Ordinal))
            {
                return spec;
            }
        }

        foreach (var spec in CalendarPeriods)
        {
            if (string.Equals(spec.Id, wanted, StringComparison.Ordinal))
            {
                return spec;
            }
        }

        return null;
    }

    /// <summary>The short label on a preset button: "5m", "4h", "1d", "1w", "1mo".</summary>
    public static string ShortLabel(TimeRangeSpec spec) => spec.Id;
}

/// <summary>
/// The one-line notes beside the picker (#5562): where the stored data starts, and how often a tab's collector
/// samples when the range holds too few samples to draw. Pure, so the picker, the tabs and the tests word them alike.
/// </summary>
public static class TimeRangeNotes
{
    /// <summary>How far before the data's start a range may begin before the note appears (collection stamps and the first sample's lag).</summary>
    public static readonly TimeSpan DataStartSlack = TimeSpan.FromMinutes(90);

    /// <summary>The fewest samples a range should hold before the collector-interval note appears.</summary>
    public const int MinSamples = 3;

    /// <summary>
    /// "Data starts Oct 3, 2:00 pm" when the range starts more than <see cref="DataStartSlack"/> before
    /// <paramref name="dataStartUtc"/>, else <c>null</c>. The time is in the range's display zone.
    /// </summary>
    public static string? DataStartNote(ResolvedTimeRange range, DateTime? dataStartUtc)
    {
        if (dataStartUtc is null)
        {
            return null;
        }

        var dataStart = TimeRangeSpec.Naive(dataStartUtc.Value);
        if (range.StartUtc >= dataStart - DataStartSlack)
        {
            return null;
        }

        return "Data starts " + range.FormatBound(dataStart, includeSeconds: false);
    }

    /// <summary>
    /// "Data here is collected every 5 minutes." when the range holds fewer than <see cref="MinSamples"/> samples at
    /// <paramref name="interval"/>, else <c>null</c>. A tab names its main collector's actual interval; the note
    /// explains an empty or one-point chart and never widens the range.
    /// </summary>
    public static string? SampleIntervalNote(TimeSpan span, TimeSpan? interval)
    {
        if (interval is null || interval.Value <= TimeSpan.Zero || span >= TimeSpan.FromTicks(interval.Value.Ticks * MinSamples))
        {
            return null;
        }

        return "Data here is collected every " + IntervalText(interval.Value) + ".";
    }

    /// <summary>"minute", "5 minutes", "2 hours", "30 seconds".</summary>
    public static string IntervalText(TimeSpan interval)
    {
        long count;
        string unit;
        if (interval.Ticks % TimeSpan.TicksPerHour == 0)
        {
            count = (long)interval.TotalHours;
            unit = "hour";
        }
        else if (interval.Ticks % TimeSpan.TicksPerMinute == 0)
        {
            count = (long)interval.TotalMinutes;
            unit = "minute";
        }
        else
        {
            count = Math.Max(1, (long)Math.Round(interval.TotalSeconds));
            unit = "second";
        }

        return count == 1 ? unit : count.ToString(CultureInfo.InvariantCulture) + " " + unit + "s";
    }
}
