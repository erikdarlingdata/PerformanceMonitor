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
/// read takes <c>bucket &gt;= start AND bucket &lt; end</c>, so an unaligned window start drops the partial
/// hour it falls inside, and an unaligned window end counts the bucket holding it whole, up to the top of the
/// next hour. A window end exactly on the hour stops there: the hour that begins at the end is not read.
/// </summary>
public static class HourlyWindowEdges
{
    /// <summary>The span an hourly read actually served: from the first bucket it held (the next hour edge when
    /// the window start is unaligned and no bucket was found) to the top of the end hour, cut at the rollup's
    /// materialization ceiling. The end is null when the rollup holds no materialized bucket at all.</summary>
    public static (DateTime Start, DateTime? End) ServedSpan(
        DateTime requestedStart, DateTime? firstBucket, DateTime requestedEnd, DateTime? ceiling)
    {
        var startHour = FloorHour(requestedStart);
        var start = firstBucket ?? (startHour != requestedStart ? startHour.AddHours(1) : requestedStart);
        var endHour = FloorHour(requestedEnd);
        var wholeEnd = endHour != requestedEnd ? endHour.AddHours(1) : endHour;
        DateTime? end = ceiling is null ? null : (ceiling.Value < wholeEnd ? ceiling.Value : wholeEnd);
        return (start, end);
    }

    /// <summary>One sentence per edge that moved, joined. <paramref name="ceiling"/> is the materialization ceiling
    /// of the relation serving the window end (null = nothing materialized). When the ceiling is at or before
    /// the window end, the end sentence says nothing after it was read, and, if the start also moved, names the
    /// whole served span. All times are UTC.</summary>
    public static string? Note(DateTime requestedStart, DateTime? firstBucket, DateTime requestedEnd, DateTime? ceiling)
    {
        var parts = new System.Collections.Generic.List<string>(3);
        var startHour = FloorHour(requestedStart);
        var (served, servedEnd) = ServedSpan(requestedStart, firstBucket, requestedEnd, ceiling);
        var startMoved = served != requestedStart;
        if (startHour != requestedStart)
        {
            parts.Add(string.Create(CultureInfo.InvariantCulture,
                $"hourly buckets start on the hour: no bucket before {served:o}: the data from {requestedStart:o} to {served:o} is not included"));
        }

        var endHour = FloorHour(requestedEnd);
        if (ceiling is null)
        {
            parts.Add("materialization ceiling unknown; the end edge is not verified");
        }
        else if (ceiling.Value <= requestedEnd)
        {
            var cut = string.Create(CultureInfo.InvariantCulture,
                $"the hourly rollup is materialized only to {ceiling.Value:o}; nothing after it was read");
            parts.Add(startMoved
                ? string.Create(CultureInfo.InvariantCulture, $"served from {served:o} to {ceiling.Value:o}; {cut}")
                : cut);
        }
        else if (endHour != requestedEnd)
        {
            parts.Add(string.Create(CultureInfo.InvariantCulture,
                $"the bucket at {endHour:o} is included whole, so up to {servedEnd!.Value:o} is counted past as_of"));
        }

        return string.Join("; ", parts);
    }

    private static DateTime FloorHour(DateTime value) =>
        new(value.Year, value.Month, value.Day, value.Hour, 0, 0, value.Kind);
}
