/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #6794: when the tag roll-up's plan-cache and procedures sections read the hourly io rollup for a window that is still inside raw
/// retention. The one definition: the service's <c>get_resource_use_by_tag</c> routing and the Viewer's "Resource use by tag" view both
/// call <see cref="ReadsHourlyInsideRaw"/>, so the two cannot drift apart (#3684).
/// </summary>
public static class QueryTagFleetRoute
{
    /// <summary>
    /// The least share of the asked window the hourly rollup must serve before it may replace raw inside raw retention. The rollup's
    /// edges cost up to about three hours: the refresh policy materializes only to <c>now - 1 hour</c> and runs hourly, so the ceiling
    /// trails the window end by one to two hours, and an unaligned start drops up to one more hour. At 24 hours that is at most about
    /// 12 % of the window, so the hourly route stays (measured on one production store with 42 servers: the served span was about 22 of
    /// 24 hours, and the hours past the ceiling held 14 to 18 % of the executions). Under 75 % a window loses most of its newest
    /// rows or all of them (a 1 or 2 hour window serves no bucket at all), and raw is cheap there because the window is short.
    /// </summary>
    public const double MinimumServedShare = 0.75;

    /// <summary>
    /// True when a read for <paramref name="serverCount"/> servers over [<paramref name="windowStart"/>, <paramref name="windowEnd"/>) takes
    /// the hourly io rollup although raw still holds the window. It needs more than one server (raw over every server's rows for a whole
    /// day is what made the fleet read slow, and one server's raw read is cheap and exact to the minute, also on a store with only one
    /// registered server), a measured materialization ceiling (<paramref name="ceiling"/>, null: nothing materialized), and a served span,
    /// as <see cref="HourlyWindowEdges.ServedSpan"/> computes it, of at least <see cref="MinimumServedShare"/> of the window, and the rollup's
    /// floor (<paramref name="ioFloor"/>, the oldest bucket the io rollup holds, null: it holds nothing) at or before the window start, so
    /// the rollup holds the window's first hour. Both callers pass the floor here, so they make one decision: a floor inside the window,
    /// where the service's route table would still answer <c>Io</c> because the rollup reaches further back than raw does, keeps raw.
    /// Otherwise the read stays on raw.
    /// </summary>
    public static bool ReadsHourlyInsideRaw(
        int serverCount, DateTime windowStart, DateTime windowEnd, DateTime? ceiling, DateTime? ioFloor)
    {
        if (serverCount < 2 || ceiling is null || ioFloor is not DateTime floor || floor > windowStart || windowEnd <= windowStart)
        {
            return false;
        }

        var (servedStart, servedEnd) = HourlyWindowEdges.ServedSpan(windowStart, null, windowEnd, ceiling);
        var end = servedEnd is DateTime e && e < windowEnd ? e : windowEnd;
        var served = end - servedStart;
        return served > TimeSpan.Zero && served.Ticks >= MinimumServedShare * (windowEnd - windowStart).Ticks;
    }
}
