/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The hour-bucket edges of a read served from an hourly rollup. A bucket is keyed by its start hour and the
/// read takes <c>bucket &gt;= start AND bucket &lt;= end</c>, so an unaligned window start drops the partial
/// hour it falls inside, and the bucket holding the window end is counted whole, up to the top of the next hour.
/// </summary>
public static class HourlyWindowEdges
{
    /// <summary>One sentence per edge that moved, joined; null only when neither edge moved (never true for the
    /// end, because <c>&lt;=</c> on an aligned end still includes the following hour). All times are UTC.</summary>
    public static string? Note(DateTime requestedStart, DateTime? firstBucket, DateTime requestedEnd)
    {
        var parts = new System.Collections.Generic.List<string>(2);
        var startHour = FloorHour(requestedStart);
        if (startHour != requestedStart)
        {
            var served = firstBucket ?? startHour.AddHours(1);
            parts.Add(string.Create(CultureInfo.InvariantCulture,
                $"hourly buckets start on the hour: the partial hour from {requestedStart:o} to {served:o} is not included"));
        }

        var endHour = FloorHour(requestedEnd);
        parts.Add(string.Create(CultureInfo.InvariantCulture,
            $"the bucket at {endHour:o} is included whole, so up to {endHour.AddHours(1):o} is counted past as_of"));
        return parts.Count == 0 ? null : string.Join("; ", parts);
    }

    private static DateTime FloorHour(DateTime value) =>
        new(value.Year, value.Month, value.Day, value.Hour, 0, 0, value.Kind);
}
