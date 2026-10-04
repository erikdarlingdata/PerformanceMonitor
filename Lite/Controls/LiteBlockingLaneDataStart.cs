/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Controls;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// Where the Overview's blocking chart says its data starts (#4966). The chart draws two event-count series, blocking and
/// deadlocks, so an empty stretch before a series starts reads as "nothing happened". Each series starts at the EARLIER of its
/// coverage floor and its earliest bar drawn (a zero bucket is not a bar); the chart has one note, and it names the LATER of
/// the two starts so it is true of both series. A series with no start (no floor, no bar, or a probe that failed with no bar)
/// is left out; when neither has one the chart shows no note. The Darling viewer's twin is <c>ViewerBlockingLaneDataStart</c>.
/// </summary>
internal static class LiteBlockingLaneDataStart
{
    /// <summary>
    /// The instant the chart's note names, or null when it names nothing. A probe that throws costs its series' start and
    /// nothing else: it is logged and the other series answers alone.
    /// </summary>
    internal static async Task<DateTime?> ChooseAsync(
        Task<DateTime?> blockingProbe, Task<DateTime?> deadlockProbe,
        IEnumerable<TrendPoint> blockingBars, IEnumerable<TrendPoint> deadlockBars)
    {
        ArgumentNullException.ThrowIfNull(blockingProbe);
        ArgumentNullException.ThrowIfNull(deadlockProbe);
        ArgumentNullException.ThrowIfNull(blockingBars);
        ArgumentNullException.ThrowIfNull(deadlockBars);

        var blockingFloor = await AnswerAsync(blockingProbe, "blocking");
        var deadlockFloor = await AnswerAsync(deadlockProbe, "deadlocks");

        return Later(SeriesStart(blockingFloor, blockingBars), SeriesStart(deadlockFloor, deadlockBars));
    }

    /// <summary>
    /// The note step: chooses the start the note names, then words the banner in <paramref name="zone"/>
    /// (<see cref="ServerTab.ApplyWindowFloorToBanner"/>). The banner is touched on the caller's thread only, after the start is chosen.
    /// </summary>
    internal static async Task ShowAsync(
        TextBlock banner, Func<QueryWindowRelation, Task<DateTime?>> floorOf, DateTime startUtc, DateTime endUtc,
        IEnumerable<TrendPoint> blockingBars, IEnumerable<TrendPoint> deadlockBars, TimeZoneInfo zone)
    {
        var start = await StartAsync(floorOf, startUtc, endUtc, blockingBars, deadlockBars);
        ServerTab.ApplyWindowFloorToBanner(banner, start, startUtc, zone);
    }

    /// <summary>
    /// Probes BOTH relations over the window the chart's reads took (a window of 90 minutes or less starts no probe, and a probe
    /// that throws costs only its series' start) and chooses the start. <paramref name="floorOf"/> is
    /// <c>LocalDataService.GetQueryWindowFloorAsync</c> for the relation. Touches no WPF object, so the live-store tests drive it.
    /// </summary>
    internal static async Task<DateTime?> StartAsync(
        Func<QueryWindowRelation, Task<DateTime?>> floorOf, DateTime startUtc, DateTime endUtc,
        IEnumerable<TrendPoint> blockingBars, IEnumerable<TrendPoint> deadlockBars)
    {
        var blockingProbe = ServerTab.ProbeWindowFloorOrNullAsync(
            () => floorOf(QueryWindowRelation.BlockedProcessReports), "Overview blocking chart (blocking)", startUtc, endUtc);
        var deadlockProbe = ServerTab.ProbeWindowFloorOrNullAsync(
            () => floorOf(QueryWindowRelation.Deadlocks), "Overview blocking chart (deadlocks)", startUtc, endUtc);

        return await ChooseAsync(blockingProbe, deadlockProbe, blockingBars, deadlockBars);
    }

    /// <summary>One series' start: the earlier of its floor and its earliest bar with a count above zero; whichever exists; null when neither.</summary>
    internal static DateTime? SeriesStart(DateTime? floor, IEnumerable<TrendPoint> bars)
    {
        DateTime? firstBar = null;
        foreach (var bar in bars)
        {
            if (bar.Count > 0 && (firstBar is null || bar.Time < firstBar.Value))
            {
                firstBar = bar.Time;
            }
        }

        return floor is DateTime f && firstBar is DateTime b ? (b < f ? b : f) : floor ?? firstBar;
    }

    /// <summary>The later of two series' starts; the one that answered when only one did; null when neither did.</summary>
    internal static DateTime? Later(DateTime? blocking, DateTime? deadlock) =>
        blocking.HasValue && deadlock.HasValue ? (blocking.Value >= deadlock.Value ? blocking : deadlock) : blocking ?? deadlock;

    private static async Task<DateTime?> AnswerAsync(Task<DateTime?> probe, string series)
    {
        try
        {
            return await probe;
        }
        catch (Exception ex)
        {
            AppLogger.Warn("CorrelatedLanes", $"Overview blocking chart ({series}): the data-start probe failed, so that series names no start: {ex.Message}");
            return null;
        }
    }
}
