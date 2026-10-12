/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data.Common;
using PerformanceMonitor.Analysis.Baselines;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// The compiled-before-the-window test the PARAMETER_SENSITIVITY reads share (#4821). <c>query_stats.creation_time</c>
/// is the monitored server's LOCAL wall clock while the window bound is naive UTC. SQL keeps a rough first
/// filter (<c>creation_time_utc</c>, the newest collected offset applied to every row) whose bound is opened by
/// <see cref="RoughFilterMarginMinutes"/>, and returns the raw <c>creation_time</c> beside the newest properties
/// row's offset and <c>time_zone_id</c>; the exact test happens here, per row, with the offset that was in force
/// when the plan was created. One offset for every row put a plan compiled before the last daylight-saving
/// change an hour off, on the wrong side of the window bound when it was compiled near it.
/// </summary>
internal static class PlanCreationClock
{
    /// <summary>
    /// How much wider than the exact bound the SQL first filter is. A daylight-saving change moves the offset by
    /// at most an hour, so a plan the exact test keeps is never outside a first filter opened by this much.
    /// </summary>
    internal const int RoughFilterMarginMinutes = 60;

    /// <summary>The bound the SQL first filter (<c>creation_time_utc &lt;= $n</c>) takes for a window that opens at <paramref name="windowStartUtc"/>.</summary>
    internal static DateTime RoughBound(DateTime windowStartUtc) => windowStartUtc.AddMinutes(RoughFilterMarginMinutes);

    /// <summary>
    /// The server's clock from the offset and zone columns of the newest properties row: the zone where SQL Server
    /// reports one, else the fixed offset, else UTC (both NULL).
    /// </summary>
    internal static ServerClock ClockFrom(DbDataReader reader, int offsetOrdinal, int zoneOrdinal)
    {
        int? offset = reader.IsDBNull(offsetOrdinal) ? null : Convert.ToInt32(reader.GetValue(offsetOrdinal));
        var zone = reader.IsDBNull(zoneOrdinal) ? null : reader.GetString(zoneOrdinal);
        return ServerClock.Resolve(zone, offset);
    }

    /// <summary>
    /// True when the plan was created at or before the window opened: its server-local creation time converted
    /// to UTC with the offset in force then, against the window's naive-UTC start.
    /// </summary>
    internal static bool CompiledBeforeWindow(ServerClock clock, DateTime creationTimeServerLocal, DateTime windowStartUtc)
        => clock.ToUtc(creationTimeServerLocal) <= windowStartUtc;
}
