/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// #6005 (D13): the open gate for an AGENT request about a deadlock. A request opens for a new signature, or when the last
/// 24 hours hold more than <see cref="SpikeFactor"/> times the daily average of the 7 days before today. The rest go to the
/// <c>deadlock_digest</c> event. A pure function of counts read from the daily rollup; the human page never asks it.
/// </summary>
public static class DeadlockAgentGate
{
    /// <summary>The multiple of the 7-day daily average the 24-hour count must exceed.</summary>
    public const int SpikeFactor = 3;

    /// <summary>The days of history the daily average spans, today excluded.</summary>
    public const int AverageDays = 7;

    /// <summary>Whether an agent request opens for one signature.</summary>
    /// <param name="isNew">The signature has never been counted for this server.</param>
    /// <param name="count24Hours">The last 24 hours' count, from <see cref="Count24Hours"/>.</param>
    /// <param name="dailyAverage">The sum of the 7 days before today, divided by 7. Zero means no history, which is not a spike: a signature with none is new, and new is the other arm.</param>
    public static bool Opens(bool isNew, double count24Hours, double dailyAverage) =>
        isNew || (dailyAverage > 0 && count24Hours > SpikeFactor * dailyAverage);

    /// <summary>
    /// The rolling 24-hour count from two day rows: today's count plus the share of yesterday's that still falls inside 24
    /// hours (the day rows are the only counts kept, so yesterday is prorated by the time left in today).
    /// </summary>
    public static double Count24Hours(int today, int yesterday, DateTime nowUtc)
    {
        var elapsed = nowUtc.TimeOfDay.TotalHours / 24.0;
        return today + yesterday * (1.0 - elapsed);
    }
}
