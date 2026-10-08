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

/// <summary>How a <see cref="TimeRangeSpec"/> names its range (#5562).</summary>
public enum TimeRangeKind
{
    /// <summary>A length counted back from now: 45m, 4h, 10d. Real elapsed time, so a day is 24 hours across a daylight saving change.</summary>
    Relative,

    /// <summary>A calendar period in the display zone: Today, Previous Week, Year to Date. Wall-clock boundaries.</summary>
    Calendar,

    /// <summary>A start and an end instant, both fixed.</summary>
    Fixed,

    /// <summary>A fixed start and an end that slides with now ("since 10/1", "Oct 2 12pm to now").</summary>
    Since
}

/// <summary>The calendar periods the picker offers (#5562). Weeks start on Monday.</summary>
public enum CalendarPeriod
{
    /// <summary>From the last local midnight to now.</summary>
    Today,

    /// <summary>The whole local day before today.</summary>
    Yesterday,

    /// <summary>From Monday 00:00 to now.</summary>
    WeekToDate,

    /// <summary>The whole Monday-to-Sunday week before this one.</summary>
    PreviousWeek,

    /// <summary>From the first of this month to now.</summary>
    MonthToDate,

    /// <summary>The whole calendar month before this one.</summary>
    PreviousMonth,

    /// <summary>From January 1 to now.</summary>
    YearToDate,

    /// <summary>The whole calendar year before this one.</summary>
    PreviousYear
}

/// <summary>Why a range was refused: a stable <see cref="Code"/> (the shared fixture compares it) and a plain <see cref="Message"/>.</summary>
public sealed record TimeRangeError(string Code, string Message);

/// <summary>
/// A time range the way a person names it, not yet tied to a clock (#5562): "past 4 hours", "Previous Week",
/// "Oct 1 to Oct 2". <see cref="TryResolve"/> turns it into the instants for a given now and display zone, so a
/// live range slides and a calendar period follows the zone, while a fixed range keeps its instants. Pure: nothing
/// here reads the machine's clock or zone. Every DateTime is naive UTC (Kind Unspecified), the frame the stores keep.
///
/// <para>Rules (settled, #5562): relative lengths are real elapsed time; calendar periods use the display zone's
/// wall-clock boundaries, a boundary being the earliest instant labelled at or after the wall midnight, so
/// consecutive periods tile with no gap or overlap; no typed or relative range is shorter than <see cref="MinimumSpan"/> (a calendar period is exempt: Today at 00:02 reads 00:00 to now); there is no
/// upper cap here (a tab reads what its data holds).</para>
/// </summary>
public sealed class TimeRangeSpec : IEquatable<TimeRangeSpec>
{
    /// <summary>The shortest range there is, everywhere. A shorter one is refused, never widened.</summary>
    public static readonly TimeSpan MinimumSpan = TimeSpan.FromMinutes(5);

    /// <summary>The longest relative length the model resolves; past it a count is refused as a typing slip.</summary>
    internal static readonly TimeSpan MaxRelative = TimeSpan.FromDays(36500);

    private TimeRangeSpec(TimeRangeKind kind)
    {
        Kind = kind;
    }

    /// <summary>Which kind of range this is.</summary>
    public TimeRangeKind Kind { get; }

    /// <summary>The length, for <see cref="TimeRangeKind.Relative"/>.</summary>
    public TimeSpan Span { get; private init; }

    /// <summary>The period, for <see cref="TimeRangeKind.Calendar"/>.</summary>
    public CalendarPeriod Period { get; private init; }

    /// <summary>The start instant, for <see cref="TimeRangeKind.Fixed"/> and <see cref="TimeRangeKind.Since"/>.</summary>
    public DateTime StartUtc { get; private init; }

    /// <summary>The end instant, for <see cref="TimeRangeKind.Fixed"/>.</summary>
    public DateTime EndUtc { get; private init; }

    /// <summary>A length counted back from now.</summary>
    public static TimeRangeSpec Relative(TimeSpan span)
        => new(TimeRangeKind.Relative) { Span = span };

    /// <summary>A calendar period in the display zone.</summary>
    public static TimeRangeSpec ForPeriod(CalendarPeriod period)
        => new(TimeRangeKind.Calendar) { Period = period };

    /// <summary>A start and an end instant (naive UTC).</summary>
    public static TimeRangeSpec FixedRange(DateTime startUtc, DateTime endUtc)
        => new(TimeRangeKind.Fixed) { StartUtc = Naive(startUtc), EndUtc = Naive(endUtc) };

    /// <summary>A fixed start with an end that is always now.</summary>
    public static TimeRangeSpec SinceInstant(DateTime startUtc)
        => new(TimeRangeKind.Since) { StartUtc = Naive(startUtc) };

    /// <summary>
    /// A stable text for settings and for telling two specs apart: "5m", "4h", "1d", "1w", "1mo", "today",
    /// "previous-week", "fixed:2026-10-01T04:00:00Z/2026-10-02T04:00:00Z", "since:2026-10-01T04:00:00Z".
    /// A relative length of exactly 7 days is "1w" and of exactly 30 days "1mo", whichever way it was typed.
    /// </summary>
    public string Id => Kind switch
    {
        TimeRangeKind.Relative => RelativeId(Span),
        TimeRangeKind.Calendar => PeriodId(Period),
        TimeRangeKind.Fixed => "fixed:" + IsoZ(StartUtc) + "/" + IsoZ(EndUtc),
        _ => "since:" + IsoZ(StartUtc)
    };

    /// <summary>The whole hours of a relative range, or <c>null</c> when it is not a whole number of hours (or not relative). For readers that still take "hours back".</summary>
    public int? WholeHours
        => Kind == TimeRangeKind.Relative && Span.Ticks % TimeSpan.TicksPerHour == 0 && Span.TotalHours <= int.MaxValue
            ? (int)Span.TotalHours
            : null;

    /// <summary>The name the picker lists: "Past 4 hours", "Previous Week"; a fixed or since range reads as a generic name.</summary>
    public string Name => Kind switch
    {
        TimeRangeKind.Relative => "Past " + PastLength(Span),
        TimeRangeKind.Calendar => PeriodName(Period),
        TimeRangeKind.Fixed => "Custom range",
        _ => "Since a start"
    };

    /// <summary>
    /// The instants this range means at <paramref name="nowUtc"/> in <paramref name="zone"/>, or the reason it
    /// cannot be used (shorter than <see cref="MinimumSpan"/> unless it is a calendar period, ends before it starts, reaches back too far).
    /// </summary>
    public bool TryResolve(DateTime nowUtc, TimeZoneInfo zone, out ResolvedTimeRange? range, out TimeRangeError? error)
    {
        var now = Naive(nowUtc);
        range = null;
        error = null;
        DateTime start;
        DateTime end;
        bool live;

        switch (Kind)
        {
            case TimeRangeKind.Relative:
                if (Span <= TimeSpan.Zero)
                {
                    error = new TimeRangeError("too_short", ShortMessage(TimeSpan.Zero));
                    return false;
                }

                if (Span > MaxRelative)
                {
                    error = new TimeRangeError("too_far_back", "That reaches back more than 100 years.");
                    return false;
                }

                start = now - Span;
                end = now;
                live = true;
                break;

            case TimeRangeKind.Calendar:
                (start, end, live) = ResolvePeriod(Period, now, zone);
                break;

            case TimeRangeKind.Since:
                start = StartUtc;
                end = now;
                live = true;
                break;

            default:
                start = StartUtc;
                end = EndUtc;
                live = false;
                break;
        }

        if (end < start)
        {
            error = Kind == TimeRangeKind.Since
                ? new TimeRangeError("start_in_future", "That start has not happened yet.")
                : new TimeRangeError("end_before_start", "The end is before the start.");
            return false;
        }

        /* A calendar period is exempt from the floor (#5562, seat ruling): "Today" at 00:02 reads 00:00 to now and the "collected
           every N min" note explains a sparse chart; no other range is ever substituted. The floor applies to typed spans and
           custom start/end only. A period that has not started (zero length) is still refused. */
        if (Kind == TimeRangeKind.Calendar ? end <= start : end - start < MinimumSpan)
        {
            error = new TimeRangeError("too_short", ShortMessage(end - start));
            return false;
        }

        range = new ResolvedTimeRange(this, start, end, live, zone, now);
        return true;
    }

    /// <summary>Reads back an <see cref="Id"/>: a preset id, a count and unit ("90m", "3d", "2w", "2mo"), "fixed:..." or "since:...". <c>false</c> for anything else.</summary>
    public static bool TryFromId(string? id, out TimeRangeSpec? spec)
    {
        spec = null;
        if (string.IsNullOrWhiteSpace(id))
        {
            return false;
        }

        var text = id.Trim().ToLowerInvariant();
        foreach (CalendarPeriod period in Enum.GetValues<CalendarPeriod>())
        {
            if (PeriodId(period) == text)
            {
                spec = ForPeriod(period);
                return true;
            }
        }

        if (text.StartsWith("since:", StringComparison.Ordinal))
        {
            if (TryIso(text.Substring(6), out var since))
            {
                spec = SinceInstant(since);
                return true;
            }

            return false;
        }

        if (text.StartsWith("fixed:", StringComparison.Ordinal))
        {
            var parts = text.Substring(6).Split('/');
            if (parts.Length == 2 && TryIso(parts[0], out var a) && TryIso(parts[1], out var b))
            {
                spec = FixedRange(a, b);
                return true;
            }

            return false;
        }

        var unitAt = 0;
        while (unitAt < text.Length && char.IsAsciiDigit(text[unitAt]))
        {
            unitAt++;
        }

        if (unitAt == 0 || unitAt == text.Length || !long.TryParse(text.AsSpan(0, unitAt), NumberStyles.None, CultureInfo.InvariantCulture, out var count) || count <= 0 || count > 1_000_000)
        {
            return false;
        }

        var unit = text.Substring(unitAt);
        double minutesPerUnit;
        switch (unit)
        {
            case "m": minutesPerUnit = 1d; break;
            case "h": minutesPerUnit = 60d; break;
            case "d": minutesPerUnit = 1440d; break;
            case "w": minutesPerUnit = 7d * 1440d; break;
            case "mo": minutesPerUnit = 30d * 1440d; break;
            default: return false;
        }

        // #5562 review: a hand-edited settings value like "400000mo" overflowed TimeSpan and threw at startup. A span past the parser's
        // own limit is not a saved id, so the caller falls back to its default instead of throwing.
        if (count * minutesPerUnit > MaxRelative.TotalMinutes)
        {
            return false;
        }

        var span = TimeSpan.FromMinutes(count * minutesPerUnit);
        spec = Relative(span);
        return true;
    }

    /// <summary>The length of a span as the picker words it: "3d" for three days and some hours, "7h 1m" under a day, "45m". Floors; a day or more drops the hours.</summary>
    public static string FormatLength(TimeSpan span)
    {
        if (span < TimeSpan.Zero)
        {
            span = TimeSpan.Zero;
        }

        var totalMinutes = (long)Math.Floor(span.TotalMinutes);
        if (totalMinutes >= 1440)
        {
            return (totalMinutes / 1440).ToString(CultureInfo.InvariantCulture) + "d";
        }

        var hours = totalMinutes / 60;
        var minutes = totalMinutes % 60;
        if (hours == 0)
        {
            return minutes.ToString(CultureInfo.InvariantCulture) + "m";
        }

        return minutes == 0
            ? hours.ToString(CultureInfo.InvariantCulture) + "h"
            : hours.ToString(CultureInfo.InvariantCulture) + "h " + minutes.ToString(CultureInfo.InvariantCulture) + "m";
    }

    /// <summary>The refusal text for a range shorter than the minimum.</summary>
    internal static string ShortMessage(TimeSpan actual)
        => actual <= TimeSpan.Zero
            ? "The shortest range is 5 minutes."
            : "That range is only " + FormatLength(actual) + " long. The shortest range is 5 minutes.";

    internal static DateTime Naive(DateTime value)
        => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    internal static string IsoZ(DateTime naiveUtc)
        => naiveUtc.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);

    private static bool TryIso(string text, out DateTime naiveUtc)
    {
        if (DateTime.TryParseExact(text.ToUpperInvariant(), "yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed))
        {
            naiveUtc = Naive(parsed);
            return true;
        }

        naiveUtc = default;
        return false;
    }

    private static string RelativeId(TimeSpan span)
    {
        if (span == TimeSpan.FromDays(7))
        {
            return "1w";
        }

        if (span == TimeSpan.FromDays(30))
        {
            return "1mo";
        }

        if (span.Ticks % TimeSpan.TicksPerDay == 0)
        {
            return ((long)span.TotalDays).ToString(CultureInfo.InvariantCulture) + "d";
        }

        if (span.Ticks % TimeSpan.TicksPerHour == 0)
        {
            return ((long)span.TotalHours).ToString(CultureInfo.InvariantCulture) + "h";
        }

        return ((long)Math.Round(span.TotalMinutes)).ToString(CultureInfo.InvariantCulture) + "m";
    }

    private static string PastLength(TimeSpan span)
    {
        if (span == TimeSpan.FromDays(7))
        {
            return "week";
        }

        if (span == TimeSpan.FromDays(30))
        {
            return "30 days";
        }

        long count;
        string unit;
        if (span.Ticks % TimeSpan.TicksPerDay == 0)
        {
            count = (long)span.TotalDays;
            unit = "day";
        }
        else if (span.Ticks % TimeSpan.TicksPerHour == 0)
        {
            count = (long)span.TotalHours;
            unit = "hour";
        }
        else
        {
            count = (long)Math.Round(span.TotalMinutes);
            unit = "minute";
        }

        return count == 1 ? unit : count.ToString(CultureInfo.InvariantCulture) + " " + unit + "s";
    }

    internal static string PeriodId(CalendarPeriod period) => period switch
    {
        CalendarPeriod.Today => "today",
        CalendarPeriod.Yesterday => "yesterday",
        CalendarPeriod.WeekToDate => "week-to-date",
        CalendarPeriod.PreviousWeek => "previous-week",
        CalendarPeriod.MonthToDate => "month-to-date",
        CalendarPeriod.PreviousMonth => "previous-month",
        CalendarPeriod.YearToDate => "year-to-date",
        _ => "previous-year"
    };

    internal static string PeriodName(CalendarPeriod period) => period switch
    {
        CalendarPeriod.Today => "Today",
        CalendarPeriod.Yesterday => "Yesterday",
        CalendarPeriod.WeekToDate => "Week to Date",
        CalendarPeriod.PreviousWeek => "Previous Week",
        CalendarPeriod.MonthToDate => "Month to Date",
        CalendarPeriod.PreviousMonth => "Previous Month",
        CalendarPeriod.YearToDate => "Year to Date",
        _ => "Previous Year"
    };

    /// <summary>
    /// The instants of a calendar period. A boundary is the earliest instant labelled at or after the wall midnight
    /// (<see cref="BoundSide.From"/>), so the end of one period is exactly the start of the next. The to-date
    /// periods and Today end at now and are live; the previous periods end at the next boundary and are not.
    /// </summary>
    private static (DateTime Start, DateTime End, bool Live) ResolvePeriod(CalendarPeriod period, DateTime now, TimeZoneInfo zone)
    {
        var today = DisplayZone.ToDisplay(now, zone).Date;
        var monday = today.AddDays(-(((int)today.DayOfWeek + 6) % 7));
        var firstOfMonth = new DateTime(today.Year, today.Month, 1, 0, 0, 0, DateTimeKind.Unspecified);
        var firstOfYear = new DateTime(today.Year, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

        switch (period)
        {
            case CalendarPeriod.Today:
                return (Boundary(today, zone), now, true);
            case CalendarPeriod.Yesterday:
                return (Boundary(today.AddDays(-1), zone), Boundary(today, zone), false);
            case CalendarPeriod.WeekToDate:
                return (Boundary(monday, zone), now, true);
            case CalendarPeriod.PreviousWeek:
                return (Boundary(monday.AddDays(-7), zone), Boundary(monday, zone), false);
            case CalendarPeriod.MonthToDate:
                return (Boundary(firstOfMonth, zone), now, true);
            case CalendarPeriod.PreviousMonth:
                return (Boundary(firstOfMonth.AddMonths(-1), zone), Boundary(firstOfMonth, zone), false);
            case CalendarPeriod.YearToDate:
                return (Boundary(firstOfYear, zone), now, true);
            default:
                return (Boundary(firstOfYear.AddYears(-1), zone), Boundary(firstOfYear, zone), false);
        }
    }

    private static DateTime Boundary(DateTime wallMidnight, TimeZoneInfo zone)
        => DisplayZone.ToUtcBound(wallMidnight, zone, BoundSide.From);

    /// <inheritdoc/>
    public bool Equals(TimeRangeSpec? other)
        => other is not null && string.Equals(Id, other.Id, StringComparison.Ordinal);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => Equals(obj as TimeRangeSpec);

    /// <inheritdoc/>
    public override int GetHashCode() => StringComparer.Ordinal.GetHashCode(Id);

    /// <inheritdoc/>
    public override string ToString() => Id;
}
