using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Common;

/// <summary>
/// #6261: the streak of a recurring alert, "the Nth day in a row at this time". The streak is the count of consecutive days,
/// ending the day before the anchor alert, on which the same metric fired inside <see cref="InvestigationAlertRecurrence.WindowMinutes"/>
/// minutes either side of the anchor's clock time. A muted fire counts: it fired, it only reached no one. The streak never exceeds
/// <see cref="InvestigationAlertRecurrence.Days"/>, the whole look-back; a streak that equals it is a floor, not an exact count.
/// <para>A "day" is the occurrence of the anchor's clock time N x 24 hours before the anchor, not the fire's calendar date: for an
/// anchor at 23:50 a fire at 00:10 belongs to the occurrence that began the evening before, and counting by date would place it on
/// the anchor's own date and lose the day. For an anchor more than the window away from midnight the two are the same.</para>
/// <para>One rule shared by every reader: the brief's SQL (both stores) computes the same whole-day number in the query
/// (<c>round((anchor - fire) / 1 day)</c>) over the same fires <see cref="DaysAgo"/> accepts, and <c>get_alert_history</c> calls
/// <see cref="DaysAgo"/> on each fire it reads.</para>
/// </summary>
public static class InvestigationAlertStreak
{
    /// <summary>
    /// The day number (1 = 24 hours back) a fire at <paramref name="fireUtc"/> counts toward for an alert at
    /// <paramref name="anchorUtc"/>, or null when it is not history for this alert: within the window minutes before the anchor
    /// (this incident's own rise), after it, older than <see cref="InvestigationAlertRecurrence.Days"/> days, or farther than the
    /// window from the anchor's clock time (circular, across midnight).
    /// </summary>
    public static int? DaysAgo(DateTime anchorUtc, DateTime fireUtc) => DaysAgo(anchorUtc, fireUtc, null);

    /// <summary>
    /// <see cref="DaysAgo(DateTime, DateTime)"/> with the server's <paramref name="zone"/>. A fire also counts when it sits within the
    /// window of the anchor's clock time moved by the difference between the zone's UTC offsets at the two instants, that is when a
    /// DST change fell between them: a schedule kept in the server's local clock (an Agent job at 02:00) moves an hour in UTC at
    /// such a change, and without this its streak reads 0 or 1 for the week after. A job the change skipped runs the moment the
    /// clock lands, at the instant of the change itself, so the offset just before the fire's window is read as well. A null zone,
    /// or one with no daylight rules, takes the plain rule only, so a UTC recurrence an hour off is still not a streak.
    /// </summary>
    public static int? DaysAgo(DateTime anchorUtc, DateTime fireUtc, TimeZoneInfo? zone)
    {
        var span = anchorUtc - fireUtc;
        var window = TimeSpan.FromMinutes(InvestigationAlertRecurrence.WindowMinutes);
        if (span <= window || span > TimeSpan.FromDays(InvestigationAlertRecurrence.Days))
            return null;

        if (OnTheClock(span.Ticks, window.Ticks))
            return WholeDays(span.Ticks);

        if (zone is null || !zone.SupportsDaylightSavingTime)
            return null;

        var anchorOffset = zone.GetUtcOffset(AsUtc(anchorUtc));
        var shift = zone.GetUtcOffset(AsUtc(fireUtc)) - anchorOffset;
        if (shift == TimeSpan.Zero)
            shift = zone.GetUtcOffset(AsUtc(fireUtc - window)) - anchorOffset;
        if (shift == TimeSpan.Zero)
            return null;

        /* The same local clock time on the two sides of the change is span = k days + (fire offset - anchor offset). */
        var moved = span.Ticks - shift.Ticks;
        if (moved <= window.Ticks || !OnTheClock(moved, window.Ticks))
            return null;

        return WholeDays(moved);
    }

    private static bool OnTheClock(long spanTicks, long windowTicks)
    {
        var day = TimeSpan.TicksPerDay;
        var offset = ((spanTicks % day) + day) % day;
        return Math.Min(offset, day - offset) <= windowTicks;
    }

    private static int WholeDays(long spanTicks) => (int)((spanTicks + TimeSpan.TicksPerDay / 2) / TimeSpan.TicksPerDay);

    private static DateTimeOffset AsUtc(DateTime utc) => new(DateTime.SpecifyKind(utc, DateTimeKind.Utc));

    /// <summary>
    /// The streak over the day numbers a metric fired on (each from <see cref="DaysAgo"/>): the run 1, 2, 3, ... that starts at
    /// day 1 and stops at the first day without a fire, capped at <see cref="InvestigationAlertRecurrence.Days"/>. Order,
    /// repeats and numbers outside 1..Days do not matter.
    /// </summary>
    public static int Length(IEnumerable<int> daysAgo)
    {
        var fired = new HashSet<int>(daysAgo);
        var streak = 0;
        while (streak < InvestigationAlertRecurrence.Days && fired.Contains(streak + 1))
            streak++;
        return streak;
    }

    /// <summary>
    /// The streak for one alert at <paramref name="anchorUtc"/> over the times the same metric on the same server fired
    /// (<paramref name="firesUtc"/>, muted or not; the alert's own row and fires outside the window do not count). What
    /// <c>get_alert_history</c> reads per row.
    /// </summary>
    public static int ForAlert(DateTime anchorUtc, IEnumerable<DateTime> firesUtc) => ForAlert(anchorUtc, firesUtc, null);

    /// <summary><see cref="ForAlert(DateTime, IEnumerable{DateTime})"/> in the server's <paramref name="zone"/>, so a fire an hour off across
    /// a DST change counts (<see cref="DaysAgo(DateTime, DateTime, TimeZoneInfo?)"/>).</summary>
    public static int ForAlert(DateTime anchorUtc, IEnumerable<DateTime> firesUtc, TimeZoneInfo? zone) =>
        Length(firesUtc.Select(f => DaysAgo(anchorUtc, f, zone)).Where(d => d.HasValue).Select(d => d!.Value));
}
