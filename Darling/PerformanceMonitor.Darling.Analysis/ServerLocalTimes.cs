/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// The row conversions behind the analysis and alert reads of a SQL Server's LOCAL wall-clock columns (#4821):
/// a Default Trace event time, a plan's creation time, a running job's start time. Every other timestamp in the
/// store is naive UTC, so these columns are converted to UTC to be compared with a window or shown beside one.
///
/// <para>The reads used to do it in SQL with ONE offset, the newest <c>server_properties.utc_offset_minutes</c>.
/// That is right for a value from after the zone's last daylight saving change and an hour off for a value from
/// before it. Each read now returns the raw local column together with the zone id and offset of the newest
/// snapshot (one row, so the two describe one snapshot), builds the <see cref="ServerClock"/> with
/// <see cref="ClockFrom"/> and converts each row here. Where the server reports its time zone (SQL Server 2022
/// and later) the clock follows the zone; an older server keeps its fixed offset; a server with neither is UTC.
/// The methods are pure so the tests reach them without a database.</para>
///
/// <para>A read that filters on the converted time keeps the newest offset in SQL only as a rough first filter
/// widened by an hour on the side that matters (the offset in force at a row can differ from the newest by one
/// daylight saving hour), and the exact test happens here, after the conversion.</para>
/// </summary>
internal static class ServerLocalTimes
{
    /// <summary>The clock for a newest snapshot's zone id and offset: the zone where it resolves, else the offset, else UTC.</summary>
    public static ServerClock ClockFrom(string? timeZoneId, int? utcOffsetMinutes) =>
        ServerClock.Resolve(timeZoneId, utcOffsetMinutes);

    /// <summary>
    /// Whether a plan whose <c>creation_time</c> is <paramref name="creationTimeLocal"/> (the server's own wall
    /// clock) was compiled at or before <paramref name="windowStartUtc"/> (naive UTC) — the "compiled before
    /// the window" test the parameter-sensitivity detectors make. A plan with no creation time is not.
    /// </summary>
    public static bool CreatedByWindowStart(ServerClock clock, DateTime? creationTimeLocal, DateTime windowStartUtc)
    {
        if (!creationTimeLocal.HasValue)
        {
            return false;
        }

        return clock.ToUtc(creationTimeLocal.Value) <= windowStartUtc;
    }

    /// <summary>
    /// The default trace's sp_configure lines, with each event time converted to naive UTC through
    /// <paramref name="clock"/>, kept when it falls in (<paramref name="windowStartExclusiveUtc"/>,
    /// <paramref name="windowEndInclusiveUtc"/>] — the window the SQL pre-filter only approximates. A row with
    /// no event time cannot be inside a window. Ordered by the converted time; a stable sort, so lines at one
    /// instant keep the read's order.
    /// </summary>
    public static List<ConfigChangeAttribution.TraceLine> TraceLinesInWindow(
        IEnumerable<(DateTime? EventTimeLocal, string? TextData)> rows,
        ServerClock clock,
        DateTime windowStartExclusiveUtc,
        DateTime windowEndInclusiveUtc)
    {
        var lines = new List<ConfigChangeAttribution.TraceLine>();
        foreach (var (eventTimeLocal, textData) in rows)
        {
            if (eventTimeLocal is not { } local)
            {
                continue;
            }

            var utc = clock.ToUtc(local);
            if (utc > windowStartExclusiveUtc && utc <= windowEndInclusiveUtc)
            {
                lines.Add(new ConfigChangeAttribution.TraceLine(utc, textData));
            }
        }

        return lines.OrderBy(static l => l.EventTimeUtc).ToList();
    }

    /// <summary>
    /// The offset in force at a running job's <paramref name="startTime"/> (the server's own wall clock), the
    /// value <c>AnomalousJobInfo.UtcOffsetMinutes</c> carries so the shared <c>StartTimeUtc</c> and the alert's
    /// "Started" label come out right for a job that started on the other side of a daylight saving change from
    /// the newest snapshot. A server with no offset collected yet stays null (the alert renders that as an
    /// unconverted server-clock instant), and an unknown start (<see cref="DateTime.MinValue"/>, what a NULL
    /// <c>start_time</c> reads as) keeps the snapshot's offset because there is no instant to look one up at.
    /// </summary>
    public static int? JobStartOffsetMinutes(ServerClock clock, DateTime startTime, int? snapshotOffsetMinutes)
    {
        if (snapshotOffsetMinutes is null || startTime == DateTime.MinValue)
        {
            return snapshotOffsetMinutes;
        }

        return (int)Math.Round((startTime - clock.ToUtc(startTime)).TotalMinutes);
    }
}
