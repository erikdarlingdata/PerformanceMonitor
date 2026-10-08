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
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Ui;

/// <summary>What <see cref="TimeRangeParser.Parse"/> made of a line of text (#5562).</summary>
public sealed class TimeRangeParseResult
{
    internal TimeRangeParseResult(ResolvedTimeRange range)
    {
        Range = range;
        Echo = range.Label;
    }

    internal TimeRangeParseResult(string code, string message)
    {
        ErrorCode = code;
        Error = message;
        Echo = string.Empty;
    }

    /// <summary>The range, resolved at the now and zone the parse was given; <c>null</c> when the text was refused.</summary>
    public ResolvedTimeRange? Range { get; }

    /// <summary>What the picker shows before Apply: the range's one-line label ("3d  Oct 5, 12:00 am - Oct 8, 7:01 am (UTC-04:00)"), empty on an error.</summary>
    public string Echo { get; }

    /// <summary>The plain reason the text was refused, or <c>null</c>.</summary>
    public string? Error { get; }

    /// <summary>A stable code for the refusal: empty, unrecognized, bad_date, bad_time, bad_range, needs_end, too_short, end_before_start, start_in_future, range_not_started, too_far_back. <c>null</c> on success.</summary>
    public string? ErrorCode { get; }

    /// <summary>The named range, or <c>null</c> on an error.</summary>
    public TimeRangeSpec? Spec => Range?.Spec;

    /// <summary>True when the text became a range.</summary>
    public bool Ok => Range is not null;
}

/// <summary>
/// One line of text to a time range (#5562). The picker's text box, the web module and the shared fixture
/// (<c>time-range-cases.json</c>) all follow this grammar; the text is case-insensitive and commas, extra spaces and
/// en or em dashes do not matter.
///
/// <list type="bullet">
///   <item><b>Relative</b>: <c>45m</c>, <c>12 hours</c>, <c>10d</c>, <c>2 weeks</c>, <c>last 3 days</c>, <c>past 90 min</c>. Units m/min/minute(s), h/hr/hour(s),
///   d/day(s), w/wk/week(s), mo/month(s) (a month is 30 real days). Counts may have a decimal part. Real elapsed time.</item>
///   <item><b>Calendar</b>: <c>today</c>, <c>yesterday</c>, <c>this week</c> (<c>week to date</c>, <c>wtd</c>), <c>last week</c>, <c>this month</c>
///   (<c>mtd</c>), <c>last month</c>, <c>this year</c> (<c>ytd</c>), <c>last year</c>; "previous" works for "last". Wall-clock days of the display zone, weeks start Monday.</item>
///   <item><b>Fixed</b>: one point or two joined by <c>-</c>, <c>to</c> or <c>until</c>. A point is a date, a time, or both. Dates: <c>Oct 1</c>, <c>October 1 2025</c>,
///   <c>1 Oct</c>, <c>10/1</c>, <c>10/1/2026</c> (month first), <c>2026-10-01</c>, <c>today</c>, <c>yesterday</c>. Times: <c>1:00 am</c>, <c>12pm</c>, <c>13:00</c>,
///   <c>13:00:30</c>, <c>noon</c>, <c>midnight</c>. One date alone is the whole day; in a range a date alone as the end includes that whole day. A time alone takes
///   the other end's date, or the latest day that is not in the future. A date without a year is the latest such date not in the future.</item>
///   <item><b>Growing</b>: <c>since 10/1</c>, <c>Oct 2 12pm to now</c>. The end slides with now.</item>
///   <item><b>Unix</b>: <c>1790852543 - 1791457343</c>; ten digits are seconds, thirteen are milliseconds.</item>
/// </list>
///
/// <para>Typed wall times resolve through <see cref="DisplayZone.ToUtcBound"/> (the start takes the earliest instant,
/// the end the latest; a repeated hour widens, a skipped time maps to the change instant). Nothing shorter than
/// <see cref="TimeRangeSpec.MinimumSpan"/> is accepted, and nothing is widened. Pure: the caller gives the now and
/// the zone.</para>
/// </summary>
public static class TimeRangeParser
{
    private const string Examples = "Try 45m, 3 days, last month, Oct 1 - Oct 2 or 1:00 am - 7:00 am.";

    private static readonly Regex RelativeRx = new(@"^(?:(?:last|past|previous)\s+)?(\d+(?:\.\d+)?)\s*([a-z]+)$", RegexOptions.CultureInvariant);
    private static readonly Regex SeparatorRx = new(@"\s+(?:-|to|until)\s+", RegexOptions.CultureInvariant);
    private static readonly Regex UnixRx = new(@"^(?:\d{10}|\d{13})$", RegexOptions.CultureInvariant);
    private static readonly Regex IsoTRx = new(@"(\d{4}-\d{1,2}-\d{1,2})t(\d)", RegexOptions.CultureInvariant);
    private static readonly Regex Time12Rx = new(@"(?:^|\s)(\d{1,2})(?::(\d{2}))?(?::(\d{2}))?\s*(am|pm)$", RegexOptions.CultureInvariant);
    private static readonly Regex Time24Rx = new(@"(?:^|\s)(\d{1,2}):(\d{2})(?::(\d{2}))?$", RegexOptions.CultureInvariant);
    private static readonly Regex TimeWordRx = new(@"(?:^|\s)(noon|midnight)$", RegexOptions.CultureInvariant);
    private static readonly Regex MonthDayRx = new(@"^([a-z]+)\s+(\d{1,2})(?:st|nd|rd|th)?(?:\s+(\d{4}))?$", RegexOptions.CultureInvariant);
    private static readonly Regex DayMonthRx = new(@"^(\d{1,2})(?:st|nd|rd|th)?\s+([a-z]+)(?:\s+(\d{4}))?$", RegexOptions.CultureInvariant);
    private static readonly Regex SlashRx = new(@"^(\d{1,2})/(\d{1,2})(?:/(\d{4}|\d{2}))?$", RegexOptions.CultureInvariant);
    private static readonly Regex IsoRx = new(@"^(\d{4})-(\d{1,2})-(\d{1,2})$", RegexOptions.CultureInvariant);
    private static readonly Regex SpacesRx = new(@"\s+", RegexOptions.CultureInvariant);

    private static readonly Dictionary<string, CalendarPeriod> Periods = new(StringComparer.Ordinal)
    {
        ["today"] = CalendarPeriod.Today,
        ["yesterday"] = CalendarPeriod.Yesterday,
        ["this week"] = CalendarPeriod.WeekToDate,
        ["week to date"] = CalendarPeriod.WeekToDate,
        ["wtd"] = CalendarPeriod.WeekToDate,
        ["last week"] = CalendarPeriod.PreviousWeek,
        ["previous week"] = CalendarPeriod.PreviousWeek,
        ["this month"] = CalendarPeriod.MonthToDate,
        ["month to date"] = CalendarPeriod.MonthToDate,
        ["mtd"] = CalendarPeriod.MonthToDate,
        ["last month"] = CalendarPeriod.PreviousMonth,
        ["previous month"] = CalendarPeriod.PreviousMonth,
        ["this year"] = CalendarPeriod.YearToDate,
        ["year to date"] = CalendarPeriod.YearToDate,
        ["ytd"] = CalendarPeriod.YearToDate,
        ["last year"] = CalendarPeriod.PreviousYear,
        ["previous year"] = CalendarPeriod.PreviousYear
    };

    private static readonly Dictionary<string, int> Months = new(StringComparer.Ordinal)
    {
        ["jan"] = 1, ["january"] = 1, ["feb"] = 2, ["february"] = 2, ["mar"] = 3, ["march"] = 3,
        ["apr"] = 4, ["april"] = 4, ["may"] = 5, ["jun"] = 6, ["june"] = 6, ["jul"] = 7, ["july"] = 7,
        ["aug"] = 8, ["august"] = 8, ["sep"] = 9, ["sept"] = 9, ["september"] = 9,
        ["oct"] = 10, ["october"] = 10, ["nov"] = 11, ["november"] = 11, ["dec"] = 12, ["december"] = 12
    };

    private static readonly Dictionary<string, double> Units = new(StringComparer.Ordinal)
    {
        ["m"] = 60, ["min"] = 60, ["mins"] = 60, ["minute"] = 60, ["minutes"] = 60,
        ["h"] = 3600, ["hr"] = 3600, ["hrs"] = 3600, ["hour"] = 3600, ["hours"] = 3600,
        ["d"] = 86400, ["day"] = 86400, ["days"] = 86400,
        ["w"] = 604800, ["wk"] = 604800, ["wks"] = 604800, ["week"] = 604800, ["weeks"] = 604800,
        ["mo"] = 2592000, ["month"] = 2592000, ["months"] = 2592000
    };

    /// <summary>One side of a typed range, before it has a year, a day or a zone.</summary>
    private sealed class Point
    {
        public bool IsNow;
        public DateTime? InstantUtc;
        public bool HasDate;
        public int? Year;
        public int Month;
        public int Day;
        public TimeSpan? Time;
    }

    private sealed class ParseFailure : Exception
    {
        public ParseFailure(string code, string message)
            : base(message)
        {
            Code = code;
        }

        public string Code { get; }
    }

    /// <summary>Reads <paramref name="text"/> as a range at <paramref name="nowUtc"/> (naive UTC) in <paramref name="zone"/>.</summary>
    public static TimeRangeParseResult Parse(string? text, DateTime nowUtc, TimeZoneInfo zone)
    {
        var now = TimeRangeSpec.Naive(nowUtc);
        try
        {
            var spec = ParseSpec(text, now, zone);
            return spec.TryResolve(now, zone, out var range, out var error)
                ? new TimeRangeParseResult(range!)
                : new TimeRangeParseResult(error!.Code, error.Message);
        }
        catch (ParseFailure ex)
        {
            return new TimeRangeParseResult(ex.Code, ex.Message);
        }
        catch (ArgumentOutOfRangeException)
        {
            return new TimeRangeParseResult("bad_date", "That date is out of range.");
        }
    }

    private static TimeRangeSpec ParseSpec(string? text, DateTime now, TimeZoneInfo zone)
    {
        var s = Normalize(text);
        if (s.Length == 0)
        {
            throw new ParseFailure("empty", "Type a range. " + Examples);
        }

        if (Periods.TryGetValue(s, out var period))
        {
            return TimeRangeSpec.ForPeriod(period);
        }

        var relative = RelativeRx.Match(s);
        if (relative.Success && Units.TryGetValue(relative.Groups[2].Value, out var unitSeconds))
        {
            var count = double.Parse(relative.Groups[1].Value, CultureInfo.InvariantCulture);
            var seconds = count * unitSeconds;
            if (seconds > 36500d * 86400d)
            {
                throw new ParseFailure("too_far_back", "That reaches back more than 100 years.");
            }

            return TimeRangeSpec.Relative(TimeSpan.FromSeconds(Math.Round(seconds)));
        }

        var wallNow = DisplayZone.ToDisplay(now, zone);
        if (s.StartsWith("since ", StringComparison.Ordinal))
        {
            return SinceSpec(ParsePoint(s.Substring(6), wallNow), wallNow, zone);
        }

        if (s.StartsWith("from ", StringComparison.Ordinal))
        {
            s = s.Substring(5);
        }

        var sides = SplitSides(s);
        if (sides.Count == 1)
        {
            var only = ParsePoint(sides[0], wallNow);
            if (only.IsNow || only.InstantUtc is not null || !only.HasDate || only.Time is not null)
            {
                throw new ParseFailure("needs_end", "Give an end as well, like \"Oct 1 12pm - 3pm\", or say \"since Oct 1 12pm\".");
            }

            /* One date alone is the whole day. */
            var day = DateFor(only, wallNow, null);
            return FixedSpec(day, day.AddDays(1), zone);
        }

        var a = ParsePoint(sides[0], wallNow);
        var b = ParsePoint(sides[1], wallNow);
        if (a.IsNow)
        {
            throw new ParseFailure("bad_range", "The start cannot be now. Use \"since\" for a range that grows.");
        }

        if (b.IsNow)
        {
            return SinceSpec(a, wallNow, zone);
        }

        if (a.InstantUtc is not null || b.InstantUtc is not null)
        {
            if (a.InstantUtc is null || b.InstantUtc is null)
            {
                throw new ParseFailure("bad_range", "Use two Unix times, or two dates and times, not one of each.");
            }

            return TimeRangeSpec.FixedRange(a.InstantUtc.Value, b.InstantUtc.Value);
        }

        return RangeSpec(a, b, wallNow, zone);
    }

    private static TimeRangeSpec FixedSpec(DateTime startWall, DateTime endWall, TimeZoneInfo zone)
        => TimeRangeSpec.FixedRange(
            DisplayZone.ToUtcBound(startWall, zone, BoundSide.From),
            DisplayZone.ToUtcBound(endWall, zone, BoundSide.To));

    private static TimeRangeSpec SinceSpec(Point start, DateTime wallNow, TimeZoneInfo zone)
    {
        if (start.IsNow)
        {
            throw new ParseFailure("bad_range", "The start cannot be now.");
        }

        if (start.InstantUtc is not null)
        {
            return TimeRangeSpec.SinceInstant(start.InstantUtc.Value);
        }

        DateTime wall;
        if (start.HasDate)
        {
            wall = DateFor(start, wallNow, start.Time) + (start.Time ?? TimeSpan.Zero);
        }
        else
        {
            /* A time alone is the latest such time that is not in the future. */
            wall = wallNow.Date + start.Time!.Value;
            if (wall > wallNow)
            {
                wall = wall.AddDays(-1);
            }
        }

        return TimeRangeSpec.SinceInstant(DisplayZone.ToUtcBound(wall, zone, BoundSide.From));
    }

    private static TimeRangeSpec RangeSpec(Point a, Point b, DateTime wallNow, TimeZoneInfo zone)
    {
        DateTime aDate;
        DateTime bDate;

        if (a.HasDate && b.HasDate)
        {
            if (a.Year is null && b.Year is null)
            {
                aDate = DateFor(a, wallNow, a.Time);
                bDate = WithYear(b, aDate.Year);
                bDate = RollEndIntoNextYear(b, aDate, bDate, wallNow);
            }
            else if (a.Year is null)
            {
                bDate = WithYear(b, b.Year!.Value);
                aDate = WithYear(a, bDate.Year);
                if (aDate > bDate)
                {
                    aDate = WithYear(a, bDate.Year - 1);
                }
            }
            else
            {
                aDate = WithYear(a, a.Year.Value);
                bDate = WithYear(b, b.Year ?? aDate.Year);
                if (b.Year is null)
                {
                    bDate = RollEndIntoNextYear(b, aDate, bDate, wallNow);
                }
            }

            if (bDate < aDate)
            {
                throw new ParseFailure("end_before_start", "The end is before the start.");
            }
        }
        else if (a.HasDate)
        {
            aDate = DateFor(a, wallNow, a.Time);
            bDate = aDate;
            if (b.Time!.Value < (a.Time ?? TimeSpan.Zero))
            {
                bDate = aDate.AddDays(1);
            }
        }
        else if (b.HasDate)
        {
            bDate = DateFor(b, wallNow, b.Time);
            aDate = bDate;
            if (b.Time is not null && a.Time!.Value > b.Time.Value)
            {
                aDate = bDate.AddDays(-1);
            }
        }
        else
        {
            /* Two times: the latest day whose range has ended by now; an end earlier on the clock than the start is the next day. */
            var rollsOver = b.Time!.Value < a.Time!.Value ? 1 : 0;
            aDate = wallNow.Date;
            for (var back = 0; back < 3; back++)
            {
                aDate = wallNow.Date.AddDays(-back);
                if (aDate.AddDays(rollsOver) + b.Time.Value <= wallNow)
                {
                    break;
                }
            }

            bDate = aDate.AddDays(rollsOver);
        }

        var startWall = aDate + (a.Time ?? TimeSpan.Zero);
        var endWall = b.Time is null ? bDate.AddDays(1) : bDate + b.Time.Value;
        return FixedSpec(startWall, endWall, zone);
    }

    /// <summary>
    /// An end typed without a year that falls before its start ("Dec 30 - Jan 2") means the next year, but only when
    /// that end has already happened: "Oct 2 - Oct 1" is a mistake, not a range into next year.
    /// </summary>
    private static DateTime RollEndIntoNextYear(Point end, DateTime startDate, DateTime endDate, DateTime wallNow)
    {
        if (endDate >= startDate || startDate.Year >= 9999)
        {
            return endDate;
        }

        try
        {
            var next = WithYear(end, startDate.Year + 1);
            return next <= wallNow ? next : endDate;
        }
        catch (ParseFailure)
        {
            return endDate;
        }
    }

    /// <summary>The calendar day of a point that has a date. Without a year it is the latest such day-and-time not after now.</summary>
    private static DateTime DateFor(Point p, DateTime wallNow, TimeSpan? time)
    {
        if (p.Year is not null)
        {
            return WithYear(p, p.Year.Value);
        }

        for (var year = wallNow.Year; year >= wallNow.Year - 8; year--)
        {
            if (p.Day <= DateTime.DaysInMonth(year, p.Month))
            {
                var candidate = new DateTime(year, p.Month, p.Day, 0, 0, 0, DateTimeKind.Unspecified);
                if (candidate + (time ?? TimeSpan.Zero) <= wallNow)
                {
                    return candidate;
                }
            }
        }

        throw new ParseFailure("bad_date", "That date does not exist.");
    }

    private static DateTime WithYear(Point p, int year)
    {
        if (year < 1 || year > 9999 || p.Day > DateTime.DaysInMonth(year, p.Month))
        {
            throw new ParseFailure("bad_date", "That date does not exist.");
        }

        return new DateTime(year, p.Month, p.Day, 0, 0, 0, DateTimeKind.Unspecified);
    }

    private static string Normalize(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return string.Empty;
        }

        var s = text.Trim().ToLowerInvariant();
        s = s.Replace('–', '-').Replace('—', '-').Replace('−', '-').Replace(',', ' ');
        return SpacesRx.Replace(s, " ").Trim();
    }

    /// <summary>Splits on one separator: " - ", " to ", " until ", or, with none of those, a single dash. Two or more dashes and no spaced separator is one point (an ISO date).</summary>
    private static List<string> SplitSides(string s)
    {
        var parts = SeparatorRx.Split(s);
        if (parts.Length == 1)
        {
            var dashes = s.Split('-');
            if (dashes.Length == 2)
            {
                parts = dashes;
            }
        }

        if (parts.Length > 2)
        {
            throw new ParseFailure("bad_range", "Use one start and one end. " + Examples);
        }

        var sides = new List<string>();
        foreach (var part in parts)
        {
            var side = part.Trim();
            if (side.Length == 0)
            {
                throw new ParseFailure("unrecognized", "A range needs something on both sides of the dash. " + Examples);
            }

            sides.Add(side);
        }

        return sides;
    }

    private static Point ParsePoint(string text, DateTime wallNow)
    {
        var p = text.Trim();
        if (p == "now")
        {
            return new Point { IsNow = true };
        }

        if (UnixRx.IsMatch(p))
        {
            var number = long.Parse(p, CultureInfo.InvariantCulture);
            var seconds = p.Length == 13 ? number / 1000d : number;
            var instant = DateTime.UnixEpoch.AddSeconds(seconds);
            return new Point { InstantUtc = DateTime.SpecifyKind(instant, DateTimeKind.Unspecified) };
        }

        p = IsoTRx.Replace(p, "$1 $2");
        if (p.StartsWith("at ", StringComparison.Ordinal))
        {
            p = p.Substring(3);
        }
        else if (p.StartsWith("on ", StringComparison.Ordinal))
        {
            p = p.Substring(3);
        }

        p = p.Replace(" at ", " ");

        var point = new Point();
        var datePart = p;
        var m12 = Time12Rx.Match(p);
        var m24 = Time24Rx.Match(p);
        var mWord = TimeWordRx.Match(p);
        if (m12.Success)
        {
            var hour = int.Parse(m12.Groups[1].Value, CultureInfo.InvariantCulture);
            var minute = m12.Groups[2].Success ? int.Parse(m12.Groups[2].Value, CultureInfo.InvariantCulture) : 0;
            var second = m12.Groups[3].Success ? int.Parse(m12.Groups[3].Value, CultureInfo.InvariantCulture) : 0;
            if (hour < 1 || hour > 12 || minute > 59 || second > 59)
            {
                throw new ParseFailure("bad_time", "\"" + m12.Value.Trim() + "\" is not a time on a 12-hour clock.");
            }

            var pm = m12.Groups[4].Value == "pm";
            point.Time = new TimeSpan((hour % 12) + (pm ? 12 : 0), minute, second);
            datePart = p.Substring(0, m12.Index);
        }
        else if (m24.Success)
        {
            var hour = int.Parse(m24.Groups[1].Value, CultureInfo.InvariantCulture);
            var minute = int.Parse(m24.Groups[2].Value, CultureInfo.InvariantCulture);
            var second = m24.Groups[3].Success ? int.Parse(m24.Groups[3].Value, CultureInfo.InvariantCulture) : 0;
            if (hour > 23 || minute > 59 || second > 59)
            {
                throw new ParseFailure("bad_time", "\"" + m24.Value.Trim() + "\" is not a time on a 24-hour clock.");
            }

            point.Time = new TimeSpan(hour, minute, second);
            datePart = p.Substring(0, m24.Index);
        }
        else if (mWord.Success)
        {
            point.Time = mWord.Groups[1].Value == "noon" ? TimeSpan.FromHours(12) : TimeSpan.Zero;
            datePart = p.Substring(0, mWord.Index);
        }

        datePart = datePart.Trim();
        if (datePart.Length == 0)
        {
            if (point.Time is null)
            {
                throw new ParseFailure("unrecognized", "\"" + text.Trim() + "\" is not a date or a time. " + Examples);
            }

            return point;
        }

        ParseDate(datePart, point, wallNow, text);
        return point;
    }

    private static void ParseDate(string d, Point point, DateTime wallNow, string original)
    {
        point.HasDate = true;
        if (d == "today" || d == "yesterday")
        {
            var day = d == "today" ? wallNow.Date : wallNow.Date.AddDays(-1);
            point.Year = day.Year;
            point.Month = day.Month;
            point.Day = day.Day;
            return;
        }

        int year = 0;
        int month;
        int dayOfMonth;
        var yearText = string.Empty;

        var md = MonthDayRx.Match(d);
        var dm = DayMonthRx.Match(d);
        var slash = SlashRx.Match(d);
        var iso = IsoRx.Match(d);
        if (md.Success && Months.TryGetValue(md.Groups[1].Value, out month))
        {
            dayOfMonth = int.Parse(md.Groups[2].Value, CultureInfo.InvariantCulture);
            yearText = md.Groups[3].Value;
        }
        else if (dm.Success && Months.TryGetValue(dm.Groups[2].Value, out month))
        {
            dayOfMonth = int.Parse(dm.Groups[1].Value, CultureInfo.InvariantCulture);
            yearText = dm.Groups[3].Value;
        }
        else if (slash.Success)
        {
            month = int.Parse(slash.Groups[1].Value, CultureInfo.InvariantCulture);
            dayOfMonth = int.Parse(slash.Groups[2].Value, CultureInfo.InvariantCulture);
            yearText = slash.Groups[3].Value;
            if (yearText.Length == 2)
            {
                yearText = "20" + yearText;
            }
        }
        else if (iso.Success)
        {
            yearText = iso.Groups[1].Value;
            month = int.Parse(iso.Groups[2].Value, CultureInfo.InvariantCulture);
            dayOfMonth = int.Parse(iso.Groups[3].Value, CultureInfo.InvariantCulture);
        }
        else
        {
            throw new ParseFailure("unrecognized", "\"" + original.Trim() + "\" is not a date or a time. " + Examples);
        }

        if (month < 1 || month > 12 || dayOfMonth < 1 || dayOfMonth > 31)
        {
            throw new ParseFailure("bad_date", "\"" + original.Trim() + "\" is not a date that exists.");
        }

        if (yearText.Length > 0)
        {
            year = int.Parse(yearText, CultureInfo.InvariantCulture);
            if (year < 1900 || year > 9999 || dayOfMonth > DateTime.DaysInMonth(year, month))
            {
                throw new ParseFailure("bad_date", "\"" + original.Trim() + "\" is not a date that exists.");
            }

            point.Year = year;
        }
        else if (dayOfMonth > DateTime.DaysInMonth(2024, month))
        {
            throw new ParseFailure("bad_date", "\"" + original.Trim() + "\" is not a date that exists.");
        }

        point.Month = month;
        point.Day = dayOfMonth;
    }
}
