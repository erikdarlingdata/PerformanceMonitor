/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #4938: the shared rules for a run time on a daily collector, so a heavy once-a-day collector runs in a quiet
/// hour instead of whenever the service happened to start. They sit beside <see cref="CollectorCadence"/> and are
/// pure: no clock, no zone, no store. Each app passes the pieces in, and nothing here changes behavior until an
/// app calls it.
///
/// <para><b>The run time is the monitored server's wall clock.</b> This type never knows a time zone. It takes a
/// local-to-UTC conversion as a delegate (an app passes <c>ServerClock.ToUtc</c>), and calls it once per slot with
/// the server-local time as a <see cref="DateTimeKind.Unspecified"/> value. While a server's clock is not known
/// yet, pass <see cref="LocalIsUtc"/>, which reads the run time as UTC. Every slot is computed from its own
/// date's local time, never as the last slot plus 1440 minutes, so the slot stays at the same local time across a
/// daylight-saving change (a day is 23 or 25 hours long twice a year).</para>
///
/// <para><b>The slot.</b> A server's slot on a local date is that date at the run time plus a fixed
/// per-server spread (<see cref="Spread"/>, under 60 minutes, a pure function of the server id), so a fleet-wide
/// run time lands on the fleet across one hour instead of one minute. A run may start from the slot until the
/// slot plus <see cref="Grace"/> (60 minutes). Neither number is a setting.</para>
///
/// <para><b>When it runs.</b> <see cref="NextDue"/> holds the rules. A collector that has not run, or has missed
/// its day, waits for its next slot; inside a slot's grace it runs now (the caller adds its seed jitter).
/// Outside the grace it waits for the next slot, never now plus jitter. After a run the next due time is the slot
/// <c>interval / 1440</c> days on. A missed day is skipped, never replayed.</para>
///
/// <para><b>Only whole days.</b> A run time on an hourly collector would only pick the minute past the hour, which
/// does not move a busy hour, so <see cref="AllowsRunAt"/> accepts only a positive multiple of 1440 minutes.</para>
/// </summary>
public static class CollectorRunTime
{
    /// <summary>The period the per-server spread is taken over, in seconds: one hour.</summary>
    public const int SpreadSeconds = 3600;

    /// <summary>How long after its slot a run may still start, in minutes.</summary>
    public const int GraceMinutes = 60;

    /// <summary>How long after its slot a run may still start. A run time is a quiet hour, so a run that cannot
    /// start inside it waits for the next slot. A start at exactly the slot, or at exactly the slot plus this, is
    /// inside.</summary>
    public static readonly TimeSpan Grace = TimeSpan.FromMinutes(GraceMinutes);

    /// <summary>The text every editor shows for a run time that is not a 24-hour <c>HH:MM</c> time, so the Darling
    /// viewer, the CLI and Lite show one wording.</summary>
    public const string InvalidRunAtMessage = "Run at must be a 24-hour time from 00:00 to 23:59, such as 02:00.";

    private const int MinutesPerDay = 1440;

    /* The room a run-time stamp gets past its interval before the clock-step check reads it as a step: an hour for
       the spread and an hour for an autumn day that is 25 hours long. The stamps NextDue returns fit in the
       interval plus the autumn hour; the spread hour is slack, so a stamp on either edge is never misread. */
    private const int StampSpreadMinutes = SpreadSeconds / 60;
    private const int StampAutumnHourMinutes = 60;

    /* A last run at the bottom of the calendar is a "never" sentinel, not a date; the date arithmetic below would
       throw on it. */
    private static readonly DateTime s_firstRealRun = DateTime.MinValue.AddDays(7);

    /// <summary>The text an editor shows when a run time is set on a collector whose interval is not a whole number
    /// of days. <paramref name="name"/> is the collector's name and <paramref name="intervalMinutes"/> its interval.</summary>
    public static string IntervalRefusalMessage(string name, int intervalMinutes) =>
        string.Create(CultureInfo.InvariantCulture,
            $"'{name}' runs every {intervalMinutes} minutes. A run time works only for a collector that runs once a day or less often (1440 minutes, or a multiple of 1440). Change the frequency or clear the run time.");

    /// <summary>Whether a collector that runs every <paramref name="intervalMinutes"/> minutes may have a run time:
    /// only a positive multiple of 1440 (once a day, or less often). A collector that runs on load and then daily
    /// is judged by its daily interval.</summary>
    public static bool AllowsRunAt(int intervalMinutes) => intervalMinutes > 0 && intervalMinutes % MinutesPerDay == 0;

    /// <summary>Reads a run time written as 24-hour <c>HH:MM</c> (two digits, a colon, two digits, from 00:00 to
    /// 23:59) into minutes after midnight. Surrounding spaces are ignored. Anything else, such as <c>24:00</c>,
    /// <c>2:5</c> or <c>abc</c>, is refused with <paramref name="minuteOfDay"/> set to 0.</summary>
    public static bool TryParse(string? text, out int minuteOfDay)
    {
        minuteOfDay = 0;
        var span = text.AsSpan().Trim();
        if (span.Length != 5 || span[2] != ':'
            || !char.IsAsciiDigit(span[0]) || !char.IsAsciiDigit(span[1])
            || !char.IsAsciiDigit(span[3]) || !char.IsAsciiDigit(span[4]))
        {
            return false;
        }

        var hour = (span[0] - '0') * 10 + (span[1] - '0');
        var minute = (span[3] - '0') * 10 + (span[4] - '0');
        if (hour > 23 || minute > 59)
        {
            return false;
        }

        minuteOfDay = hour * 60 + minute;
        return true;
    }

    /// <summary>Writes minutes after midnight (0 to 1439) as 24-hour <c>HH:MM</c>, the form <see cref="TryParse"/>
    /// reads back.</summary>
    public static string Format(int minuteOfDay)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(minuteOfDay);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(minuteOfDay, MinutesPerDay);

        return string.Create(CultureInfo.InvariantCulture, $"{minuteOfDay / 60:00}:{minuteOfDay % 60:00}");
    }

    /// <summary>The fixed spread for one server: <see cref="CollectorCadence.CadencePhaseOffset"/> over
    /// <see cref="SpreadSeconds"/>, so from 0 to just under 60 minutes, the same on every restart because it is a
    /// pure function of the id.</summary>
    public static TimeSpan Spread(int serverId) => CollectorCadence.CadencePhaseOffset(serverId, SpreadSeconds);

    /// <summary>The local time is already UTC. The conversion to pass while a server's clock is not known yet.</summary>
    public static DateTime LocalIsUtc(DateTime local) => local;

    /// <summary>The UTC instant of the slot for <paramref name="localDate"/> on the server's clock: that date at
    /// <paramref name="runAtMinute"/> (minutes after local midnight, 0 to 1439) plus the server's
    /// <see cref="Spread"/>, converted by <paramref name="localToUtc"/>. The spread is added to the local time, so
    /// it moves with the run time across a daylight-saving change. The result is <see cref="DateTimeKind.Utc"/>.</summary>
    public static DateTime SlotUtc(DateOnly localDate, int runAtMinute, int serverId, Func<DateTime, DateTime> localToUtc)
    {
        ArgumentNullException.ThrowIfNull(localToUtc);
        ArgumentOutOfRangeException.ThrowIfNegative(runAtMinute);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(runAtMinute, MinutesPerDay);

        var local = localDate.ToDateTime(TimeOnly.MinValue).AddMinutes(runAtMinute) + Spread(serverId);
        return DateTime.SpecifyKind(localToUtc(local), DateTimeKind.Utc);
    }

    /// <summary>
    /// The next time a collector with a run time is due, in UTC.
    ///
    /// <para>A day is identified by its slot: the date a run belongs to is the latest date whose slot is at or
    /// before the run. That keeps a slot that spills past local midnight (a late run time plus the spread) with the
    /// date it was computed for, and it needs only the local-to-UTC conversion, because the candidate dates are the
    /// few around the UTC date (an offset is never more than 14 hours). A run before the day's slot, such as an
    /// on-load run in the small hours, counts for the day before, so that day's slot is still owed.</para>
    ///
    /// <para>With no <paramref name="lastRunUtc"/> (never run), or one that is long past, the slots are every
    /// day. With one, they are every <c>intervalMinutes / 1440</c> days from the date of that run, so a two-day
    /// collector keeps its two-day grid. Of those, the latest slot at or before <paramref name="nowUtc"/> is due
    /// when <paramref name="nowUtc"/> is still inside its grace: the answer is <paramref name="nowUtc"/> itself, and
    /// the caller adds its seed jitter. Past the grace that slot is lost, a missed day is skipped, and the answer is
    /// the next slot. A slot still ahead is the answer as it stands.</para>
    ///
    /// <para>After a run, pass the instant of the run (or of the slot it ran for) as
    /// <paramref name="lastRunUtc"/>: the answer is the slot <c>intervalMinutes / 1440</c> dates on.</para>
    ///
    /// <para>Throws <see cref="ArgumentOutOfRangeException"/> for an interval <see cref="AllowsRunAt"/> refuses
    /// or a <paramref name="runAtMinute"/> outside 0 to 1439.</para>
    /// </summary>
    /// <param name="nowUtc">The current instant, UTC.</param>
    /// <param name="lastRunUtc">When the collector last ran, UTC, or null when it has not.</param>
    /// <param name="runAtMinute">The run time, in minutes after midnight on the server's clock.</param>
    /// <param name="intervalMinutes">The collector's interval: a positive multiple of 1440.</param>
    /// <param name="serverId">The server id the spread is taken from.</param>
    /// <param name="localToUtc">Server-local time to UTC, such as <c>ServerClock.ToUtc</c>, or <see cref="LocalIsUtc"/>.</param>
    public static DateTime NextDue(DateTime nowUtc, DateTime? lastRunUtc, int runAtMinute, int intervalMinutes, int serverId,
        Func<DateTime, DateTime> localToUtc)
    {
        ArgumentNullException.ThrowIfNull(localToUtc);
        if (!AllowsRunAt(intervalMinutes))
        {
            throw new ArgumentOutOfRangeException(nameof(intervalMinutes), intervalMinutes,
                "A run time works only for an interval that is a positive multiple of 1440 minutes.");
        }

        ArgumentOutOfRangeException.ThrowIfNegative(runAtMinute);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(runAtMinute, MinutesPerDay);

        var days = intervalMinutes / MinutesPerDay;
        var nowDate = LatestSlotDate(nowUtc, runAtMinute, serverId, localToUtc);

        if (lastRunUtc is not { } lastRun || lastRun <= s_firstRealRun)
        {
            var today = SlotUtc(nowDate, runAtMinute, serverId, localToUtc);
            return nowUtc <= today + Grace ? nowUtc : SlotUtc(nowDate.AddDays(1), runAtMinute, serverId, localToUtc);
        }

        var lastDate = LatestSlotDate(lastRun, runAtMinute, serverId, localToUtc);
        var dueDate = lastDate.AddDays(days);
        if (dueDate > nowDate)
        {
            return SlotUtc(dueDate, runAtMinute, serverId, localToUtc);
        }

        /* The latest slot on the grid at or before now: whole steps of `days` from the last run's date. */
        var latestDate = lastDate.AddDays((nowDate.DayNumber - lastDate.DayNumber) / days * days);
        var latest = SlotUtc(latestDate, runAtMinute, serverId, localToUtc);
        return nowUtc <= latest + Grace ? nowUtc : SlotUtc(latestDate.AddDays(days), runAtMinute, serverId, localToUtc);
    }

    /// <summary>How far ahead of now a due time from <see cref="NextDue"/> can sit, for the clock-step check
    /// (<see cref="CollectorCadence.ClampDue"/>): the interval, plus an hour for the spread, plus an hour for a
    /// 25-hour autumn day. Pass it as the clamp's interval for a collector that has a run time; the plain interval
    /// would read an ordinary wait as a clock that stepped back and run the collector at once.</summary>
    public static TimeSpan MaxStampAhead(int intervalMinutes) =>
        TimeSpan.FromMinutes(intervalMinutes + StampSpreadMinutes + StampAutumnHourMinutes);

    /* The latest local date whose slot is at or before the instant. Slots rise with the date, and an offset from
       UTC is within 14 hours, so the answer is within the UTC date and the date after it; start two dates back,
       where the slot is always behind the instant, and step forward while the next date's slot has also passed. */
    private static DateOnly LatestSlotDate(DateTime utc, int runAtMinute, int serverId, Func<DateTime, DateTime> localToUtc)
    {
        var date = DateOnly.FromDateTime(utc).AddDays(-2);
        for (var step = 0; step < 4; step++)
        {
            var following = date.AddDays(1);
            if (SlotUtc(following, runAtMinute, serverId, localToUtc) > utc)
            {
                break;
            }

            date = following;
        }

        return date;
    }
}
