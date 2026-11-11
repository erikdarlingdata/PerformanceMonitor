/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Common;

/// <summary>
/// The staleness note for a latest-snapshot Query Store read (<c>get_query_store_health</c>, the <c>automatic_tuning</c> block of
/// <c>get_plan_corrections</c>). Those reads answer the newest capture whatever its age, and on a server whose Query Store collectors
/// stopped (a role change to a readable secondary, a disabled schedule) the newest capture can be weeks old while the payload reads
/// as current. The note is attached when the capture is older than <see cref="Threshold"/>, three hours: three cycles of a
/// collector on the hourly schedule <c>query_store_health</c> ships with, so a cycle that ran late is not called stale. The
/// note names the cadence the collector is scheduled at on the server, which the caller resolves.
/// </summary>
public static class QueryStoreSnapshotStaleness
{
    /// <summary>How old a latest capture may be before the answer says so: three hours.</summary>
    public static readonly TimeSpan Threshold = TimeSpan.FromHours(3);

    /// <summary>Two optional notes as one, or null when both are null.</summary>
    public static string? Join(string? first, string? second) =>
        first is null ? second : second is null ? first : first + " " + second;

    /// <summary>The note for a capture at <paramref name="capturedUtc"/> read at <paramref name="nowUtc"/>, or null while it is fresh.
    /// <paramref name="collectorName"/> names the collector that writes it. <paramref name="collectorEnabled"/> is whether that
    /// collector's schedule runs it on this server: a disabled collector takes no newer capture, and the note says that instead of
    /// naming a cadence and sending the reader to look for a failed run. <paramref name="intervalMinutes"/> is the interval that
    /// schedule runs it at on this server, which the note of an enabled collector names (<see cref="Cadence"/>).</summary>
    public static string? Note(DateTime capturedUtc, DateTime nowUtc, string collectorName, bool collectorEnabled, int intervalMinutes)
    {
        var age = nowUtc - capturedUtc;
        if (age < Threshold) return null;
        var ago = Ago(age);
        if (!collectorEnabled)
        {
            return $"This is the newest capture, and it is {ago} old: the {collectorName} collector is disabled for this server, so no "
                + "newer capture is taken and the state shown is the server's as of captured_at, not now. get_collector_schedules "
                + "shows the schedule.";
        }

        return $"This is the newest capture, and it is {ago} old: the {collectorName} collector runs {Cadence(intervalMinutes)}, so the state shown is "
            + "the server's as of captured_at, not now. Check get_collection_health for why no newer capture exists.";
    }

    /// <summary>
    /// #8541: the sentence a desktop Query Store panel adds over an empty window when the server's newest
    /// <c>query_store_health</c> capture is older than <see cref="Threshold"/>, or null while it is fresh. It gives the reason
    /// <see cref="Note"/> gives, the capture's age and whether the collector's schedule still takes one (and at what cadence), in
    /// the words of a panel: the panel shows no capture and names no tool.
    /// </summary>
    public static string? PanelNote(DateTime capturedUtc, DateTime nowUtc, string collectorName, bool collectorEnabled, int intervalMinutes)
    {
        var age = nowUtc - capturedUtc;
        if (age < Threshold) return null;
        var ago = Ago(age);
        return collectorEnabled
            ? $"The newest Query Store health capture is {ago} old: the {collectorName} collector runs {Cadence(intervalMinutes)}, so collection health shows why no newer capture exists."
            : $"The newest Query Store health capture is {ago} old: the {collectorName} collector is disabled for this server, so no newer capture is taken.";
    }

    /// <summary>
    /// The <c>unavailable</c> answer of <c>get_query_store_health</c> for a server the store holds no capture for, in both
    /// products. It names the cadence <paramref name="collectorName"/> is scheduled at on the server
    /// (<paramref name="intervalMinutes"/>, through <see cref="Cadence"/>).
    /// </summary>
    public static string NoCaptureMessage(string collectorName, int intervalMinutes) =>
        $"No Query Store health data available. The {collectorName} collector runs {Cadence(intervalMinutes)} (SQL Server 2016+); "
        + "a server with no rows either predates Query Store or has not completed a cycle yet.";

    /// <summary>
    /// How a note words an interval in minutes after "runs": "hourly" for 60, "every minute" for 1, "every H hours" for other
    /// whole hours, and "every N minutes" otherwise. An interval of zero or less is a collector with no recurring interval,
    /// worded "on its own schedule".
    /// </summary>
    public static string Cadence(int intervalMinutes)
    {
        if (intervalMinutes <= 0) return "on its own schedule";
        if (intervalMinutes == 60) return "hourly";
        if (intervalMinutes == 1) return "every minute";
        return intervalMinutes % 60 == 0
            ? string.Format(CultureInfo.InvariantCulture, "every {0} hours", intervalMinutes / 60)
            : string.Format(CultureInfo.InvariantCulture, "every {0} minutes", intervalMinutes);
    }

    /// <summary>An age as whole days and hours ("19 days 4 hours"), or hours alone under a day.</summary>
    private static string Ago(TimeSpan age)
    {
        var days = (int)age.TotalDays;
        var hours = age.Hours;
        return days > 0
            ? string.Format(CultureInfo.InvariantCulture, "{0} day{1} {2} hour{3}", days, days == 1 ? "" : "s", hours, hours == 1 ? "" : "s")
            : string.Format(CultureInfo.InvariantCulture, "{0} hour{1}", hours, hours == 1 ? "" : "s");
    }
}
