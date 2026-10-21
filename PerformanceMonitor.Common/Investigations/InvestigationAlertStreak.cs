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
    public static int? DaysAgo(DateTime anchorUtc, DateTime fireUtc)
    {
        var span = anchorUtc - fireUtc;
        var window = TimeSpan.FromMinutes(InvestigationAlertRecurrence.WindowMinutes);
        if (span <= window || span > TimeSpan.FromDays(InvestigationAlertRecurrence.Days))
            return null;

        var day = TimeSpan.TicksPerDay;
        var offset = span.Ticks % day;
        if (Math.Min(offset, day - offset) > window.Ticks)
            return null;

        return (int)((span.Ticks + day / 2) / day);
    }

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
    public static int ForAlert(DateTime anchorUtc, IEnumerable<DateTime> firesUtc) =>
        Length(firesUtc.Select(f => DaysAgo(anchorUtc, f)).Where(d => d.HasValue).Select(d => d!.Value));
}
