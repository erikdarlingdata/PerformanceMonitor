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
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Where the Overview's blocking chart says its data starts (#4966). The chart draws two event-count series, blocking reports and
/// deadlocks, so an empty stretch before a series starts reads as "nothing happened". Each series starts at the earlier of its
/// coverage and its earliest bar drawn (<see cref="ViewerEventDataStart.Of"/>); the chart has one note, and it names the LATER of the
/// two starts so it is true of both series. A series that has no start (its probe found nothing and it drew no bar, or its probe
/// threw) is left out; when neither has one the chart shows no note.
/// </summary>
internal static class ViewerBlockingLaneDataStart
{
    /// <summary>
    /// The instant the chart's note names, or null when it names nothing. A probe that throws costs its series' start and
    /// nothing else: it is logged and the other series answers alone.
    /// </summary>
    internal static async Task<DateTime?> ChooseAsync(
        Task<DateTime?> blockingProbe, Task<DateTime?> deadlockProbe, IEnumerable<DateTime> blockingBars, IEnumerable<DateTime> deadlockBars)
    {
        ArgumentNullException.ThrowIfNull(blockingProbe);
        ArgumentNullException.ThrowIfNull(deadlockProbe);
        ArgumentNullException.ThrowIfNull(blockingBars);
        ArgumentNullException.ThrowIfNull(deadlockBars);

        var (blockingFailed, blockingCoverage) = await AnswerAsync(blockingProbe, "blocking");
        var (deadlockFailed, deadlockCoverage) = await AnswerAsync(deadlockProbe, "deadlocks");

        return Later(
            ViewerEventDataStart.Of(blockingCoverage, ViewerEventDataStart.EarliestOf(blockingBars.Select(t => (DateTime?)t)), probeFailed: blockingFailed),
            ViewerEventDataStart.Of(deadlockCoverage, ViewerEventDataStart.EarliestOf(deadlockBars.Select(t => (DateTime?)t)), probeFailed: deadlockFailed));
    }

    /// <summary>The later of two series' starts; the one that answered when only one did; null when neither did.</summary>
    internal static DateTime? Later(DateTime? blocking, DateTime? deadlock) =>
        blocking.HasValue && deadlock.HasValue ? (blocking.Value >= deadlock.Value ? blocking : deadlock) : blocking ?? deadlock;

    private static async Task<(bool Failed, DateTime? Start)> AnswerAsync(Task<DateTime?> probe, string series)
    {
        try
        {
            return (false, await probe);
        }
        catch (Exception ex)
        {
            ViewerLogger.Warn(
                "CorrelatedTimelineLanesControl",
                $"Overview blocking chart ({series}): the data-start probe failed, so that series names no start | {ex.GetType().Name}: {ex.Message}");
            return (true, null);
        }
    }
}
