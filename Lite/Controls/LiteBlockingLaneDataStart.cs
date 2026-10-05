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
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// Where the Overview's blocking chart says its data starts (#4966). The chart draws two event-count series, blocking and
/// deadlocks, so an empty stretch before a series starts reads as "nothing happened". Each series starts at the EARLIER of its
/// coverage floor and its earliest bar drawn (a zero bucket is not a bar); the chart has one note, and it names the LATER of
/// the two starts so it is true of both series. A series with no start (no floor, no bar, or a probe that failed with no bar)
/// is left out; when neither has one the chart shows no note. The Darling viewer's twin is <c>ViewerBlockingLaneDataStart</c>.
/// <para>Two rules: a probe that FAILED gives its series no start, whatever its bars (a failure is not evidence of a late start, so
/// the other series answers alone); a probe that ANSWERED null (no collector run in the window) with bars drawn names the first
/// bar, as the viewer does (<c>ViewerEventDataStart.Of</c>). Lite's own Blocking tab differs: it keeps the null there
/// (<c>EarlierOfFloorAndRowShown</c>).</para>
/// </summary>
internal static class LiteBlockingLaneDataStart
{
    /// <summary>#5098: the blocked process threshold's history over the window (the viewer's tuple, named so a seam can carry it).</summary>
    internal readonly record struct BlockedProcessThreshold(bool OnAtWindowStart, DateTime? FirstOnInWindow, bool SawZeroSnapshot)
    {
        internal (bool OnAtWindowStart, DateTime? FirstOnInWindow, bool SawZeroSnapshot) AsTuple() => (OnAtWindowStart, FirstOnInWindow, SawZeroSnapshot);
    }

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

        var blocking = await AnswerAsync(blockingProbe, "blocking");
        var deadlock = await AnswerAsync(deadlockProbe, "deadlocks");

        return Later(
            blocking.Failed ? null : SeriesStart(blocking.Floor, blockingBars),
            deadlock.Failed ? null : SeriesStart(deadlock.Floor, deadlockBars));
    }

    /// <summary>
    /// The note step: chooses the start the note names, then words the banner in <paramref name="zone"/>
    /// (<see cref="ServerTab.ApplyWindowFloorToBanner"/>). The banner is touched on the caller's thread only, after the start is chosen.
    /// </summary>
    internal static async Task ShowAsync(
        TextBlock banner, Func<QueryWindowRelation, Task<DateTime?>> floorOf, DateTime startUtc, DateTime endUtc,
        IEnumerable<TrendPoint> blockingBars, IEnumerable<TrendPoint> deadlockBars, TimeZoneInfo zone,
        Func<Task<bool>>? blockingReadTookXe = null, Func<Task<DateTime?>>? xeOnlyBlockingFloorOf = null,
        Func<Task<DateTime?>>? earliestReportOf = null, Func<Task<BlockedProcessThreshold>>? thresholdOf = null)
    {
        var start = await StartAsync(floorOf, startUtc, endUtc, blockingBars, deadlockBars, blockingReadTookXe, xeOnlyBlockingFloorOf, earliestReportOf, thresholdOf);
        ServerTab.ApplyWindowFloorToBanner(banner, start, startUtc, zone);
    }

    /// <summary>
    /// Probes BOTH relations over the window the chart's reads took (a window of 90 minutes or less starts no probe, and a probe
    /// that throws costs only its series' start) and chooses the start. <paramref name="floorOf"/> is
    /// <c>LocalDataService.GetQueryWindowFloorAsync</c> for the relation. Touches no WPF object, so the live-store tests drive it.
    /// </summary>
    internal static async Task<DateTime?> StartAsync(
        Func<QueryWindowRelation, Task<DateTime?>> floorOf, DateTime startUtc, DateTime endUtc,
        IEnumerable<TrendPoint> blockingBars, IEnumerable<TrendPoint> deadlockBars,
        Func<Task<bool>>? blockingReadTookXe = null, Func<Task<DateTime?>>? xeOnlyBlockingFloorOf = null,
        Func<Task<DateTime?>>? earliestReportOf = null, Func<Task<BlockedProcessThreshold>>? thresholdOf = null)
    {
        if (!McpQueryTools.CanWindowBeTruncated(startUtc, endUtc))
        {
            return null;
        }

        /* The shared floor-or-null wrapper is not used here: it turns a throw into null, and null here would read as "no collector run". */
        var blockingProbe = ProbeBlockingAsync(floorOf, blockingReadTookXe, xeOnlyBlockingFloorOf, earliestReportOf, thresholdOf);
        var deadlockProbe = Probe(floorOf, QueryWindowRelation.Deadlocks);

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

    /// <summary>
    /// #5098: when the blocking read drew the XE reports (<paramref name="blockingReadTookXe"/> true), the start is the XE collector's
    /// alone (<paramref name="xeOnlyBlockingFloorOf"/>): a DMV that covers the window must not hide where the XE data starts. A check that
    /// throws falls back to the two-source probe (today's); a throw from the threshold read falls back to #5159's answer; a throw from the
    /// probe itself fails the series as before.
    /// </summary>
    private static async Task<DateTime?> ProbeBlockingAsync(
        Func<QueryWindowRelation, Task<DateTime?>> floorOf, Func<Task<bool>>? blockingReadTookXe, Func<Task<DateTime?>>? xeOnlyBlockingFloorOf,
        Func<Task<DateTime?>>? earliestReportOf, Func<Task<BlockedProcessThreshold>>? thresholdOf)
    {
        var fromXe = false;
        if (blockingReadTookXe is not null && xeOnlyBlockingFloorOf is not null)
        {
            try
            {
                fromXe = await blockingReadTookXe();
            }
            catch (Exception ex)
            {
                AppLogger.Warn("CorrelatedLanes", $"Overview blocking chart: the blocked-process-report source check failed, so the note probes both sources: {ex.Message}");
            }
        }

        if (fromXe && earliestReportOf is not null && thresholdOf is not null)
        {
            /* #5098: the XE start is the collector's floor, the earliest report and the threshold's history, combined by the shared rule. */
            return await LocalDataService.CombineBlockingXeStartAsync(
                xeOnlyBlockingFloorOf!, earliestReportOf, () => ThresholdOrNoneAsync(thresholdOf));
        }

        return await (fromXe ? xeOnlyBlockingFloorOf!() : floorOf(QueryWindowRelation.BlockedProcessReports));
    }

    /// <summary>
    /// The threshold's history, or "no snapshots" when the read throws: the shared rule then returns the XE-only floor combined with the
    /// earliest report (#5159's answer), so a failed threshold read costs only the threshold's refinement, not the whole blocking series.
    /// </summary>
    internal static async Task<(bool OnAtWindowStart, DateTime? FirstOnInWindow, bool SawZeroSnapshot)> ThresholdOrNoneAsync(
        Func<Task<BlockedProcessThreshold>> thresholdOf)
    {
        try
        {
            return (await thresholdOf()).AsTuple();
        }
        catch (Exception ex)
        {
            AppLogger.Warn("CorrelatedLanes", $"Overview blocking chart: the blocked process threshold read failed, so the note keeps the collector's start: {ex.Message}");
            return default;
        }
    }

    private static Task<DateTime?> Probe(Func<QueryWindowRelation, Task<DateTime?>> floorOf, QueryWindowRelation relation)
    {
        try
        {
            return floorOf(relation);
        }
        catch (Exception ex)
        {
            return Task.FromException<DateTime?>(ex);
        }
    }

    private static async Task<(bool Failed, DateTime? Floor)> AnswerAsync(Task<DateTime?> probe, string series)
    {
        try
        {
            return (false, await probe);
        }
        catch (Exception ex)
        {
            AppLogger.Warn("CorrelatedLanes", $"Overview blocking chart ({series}): the data-start probe failed, so that series names no start: {ex.Message}");
            return (true, null);
        }
    }
}
