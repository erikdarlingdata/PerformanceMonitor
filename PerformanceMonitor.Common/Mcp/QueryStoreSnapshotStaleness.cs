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
/// as current. The note is attached when the capture is older than <see cref="Threshold"/>: three of the hourly cycles that
/// capture it, so a cycle that ran late is not called stale.
/// </summary>
public static class QueryStoreSnapshotStaleness
{
    /// <summary>How old a latest capture may be before the answer says so: three hourly cycles.</summary>
    public static readonly TimeSpan Threshold = TimeSpan.FromHours(3);

    /// <summary>Two optional notes as one, or null when both are null.</summary>
    public static string? Join(string? first, string? second) =>
        first is null ? second : second is null ? first : first + " " + second;

    /// <summary>The note for a capture at <paramref name="capturedUtc"/> read at <paramref name="nowUtc"/>, or null while it is fresh.
    /// <paramref name="collectorName"/> names the hourly collector that writes it. <paramref name="collectorEnabled"/> is whether that
    /// collector's schedule runs it on this server: a disabled collector takes no newer capture, and the note says that instead of
    /// naming a cadence and sending the reader to look for a failed run.</summary>
    public static string? Note(DateTime capturedUtc, DateTime nowUtc, string collectorName, bool collectorEnabled)
    {
        var age = nowUtc - capturedUtc;
        if (age < Threshold) return null;
        var days = (int)age.TotalDays;
        var hours = age.Hours;
        var ago = days > 0
            ? string.Format(CultureInfo.InvariantCulture, "{0} day{1} {2} hour{3}", days, days == 1 ? "" : "s", hours, hours == 1 ? "" : "s")
            : string.Format(CultureInfo.InvariantCulture, "{0} hour{1}", hours, hours == 1 ? "" : "s");
        if (!collectorEnabled)
        {
            return $"This is the newest capture, and it is {ago} old: the {collectorName} collector is disabled for this server, so no "
                + "newer capture is taken and the state shown is the server's as of captured_at, not now. get_collector_schedules "
                + "shows the schedule.";
        }

        return $"This is the newest capture, and it is {ago} old: the {collectorName} collector runs hourly, so the state shown is "
            + "the server's as of captured_at, not now. Check get_collection_health for why no newer capture exists.";
    }
}
