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
/// #4966: the "Showing since" note on the Blocking tab's charts that draw a zero baseline or zero ends: the Trends
/// sub-tab's three charts and the Blocking Stats sub-tab's two chart pairs. Each chart names its own source's start (the
/// Lock Wait Trend reads wait_stats, the Blocking Trend and the blocking pair the blocked process reports with the DMV
/// blocking snapshots, the Deadlock Trend and the deadlock pair deadlocks), through the shared step
/// (<c>RefreshWindowTruncatedBannerAsync</c>), so the probe, the 90-minute rule, the picker zone and the failed-probe rule
/// are the ones every other note uses. The Blocking and Deadlock series name the EARLIER of the coverage start and the
/// earliest point drawn (<see cref="ServerTab.EarlierOfFloorAndRowShown"/>); the Lock Wait Trend is a rate series, so it
/// names its coverage alone. The same Darling viewer wording and placement as #5030.
/// </summary>
public partial class ServerTab
{
    /// <summary>The time of the earliest bucket a count chart draws as an event: only buckets with a non-zero count (a zero bucket is the chart's baseline). Null when it draws none.</summary>
    internal static DateTime? EarliestBlockingTrendPointDrawn(IEnumerable<TrendPoint> data) =>
        data.Where(p => p.Count > 0).Select(p => (DateTime?)p.Time).Min();

    /// <summary>The earliest per-minute blocking-severity bucket that holds an event. Null when none does.</summary>
    internal static DateTime? EarliestBlockingStatsPointDrawn(IEnumerable<BlockingDurationStatsPoint> data) =>
        data.Where(p => p.EventCount > 0).Select(p => (DateTime?)p.Time).Min();

    /// <summary>The earliest deadlock-severity bucket. Null when there is none.</summary>
    internal static DateTime? EarliestDeadlockStatsPointDrawn(IEnumerable<PerformanceMonitor.Common.DeadlockSeverityStatsPoint> data) =>
        data.Select(p => (DateTime?)p.Time).Min();

    /// <summary>
    /// #5098: whether the Blocking Trend / Blocking Stats read drew the XE blocked process reports (it falls back to the DMV snapshots
    /// only when the window holds none). The note then probes the XE collector alone: a DMV that covers the whole window must not hide
    /// where the XE data starts. A window that can never get a note starts no check, and a check that throws keeps today's probe.
    /// </summary>
    private async System.Threading.Tasks.Task<bool> BlockingReadTookXeAsync(DateTime start, DateTime end, IReadOnlyList<string>? databaseNames)
    {
        if (!McpQueryTools.CanWindowBeTruncated(start, end))
        {
            return false;
        }

        try
        {
            return await Task.Run(() => _dataService.HasBlockedProcessReportsInWindowAsync(_serverId, start, end, databaseNames));
        }
        catch (Exception ex)
        {
            AppLogger.Warn("BlockingCharts", $"[{_server.DisplayName}] the blocked-process-report source check failed, so the note probes both sources: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// #5098: the blocking chart's note. The DMV branch is the shared step, unchanged. The XE branch names the combined start
    /// (<see cref="LocalDataService.GetBlockingXeDataStartAsync"/>: the collector's floor, the earliest report and the blocked process
    /// threshold's history) through the same guarded probe, so a failed probe costs only the note.
    /// </summary>
    private async System.Threading.Tasks.Task RefreshBlockingBannerAsync(
        bool blockingFromXe, TextBlock banner, DateTime start, DateTime end, DateTime? earliestDrawn, IReadOnlyList<string>? databaseNames)
    {
        if (!blockingFromXe)
        {
            await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.BlockedProcessReports, banner, start, end, earliestDrawn);
            return;
        }

        var floor = await ProbeWindowFloorOrNullAsync(
            () => Task.Run(() => _dataService.GetBlockingXeDataStartAsync(_serverId, start, end, databaseNames)),
            $"[{_server.DisplayName}] {QueryWindowRelation.BlockedProcessReports}", start, end);
        ApplyWindowFloorToBanner(banner, EarlierOfFloorAndRowShown(floor, earliestDrawn), start, GetPickerZone());
    }

    /// <summary>The Trends sub-tab's three notes, over the SAME UTC window the three reads took.</summary>
    private async System.Threading.Tasks.Task RefreshBlockingTrendsBannersAsync(
        List<LockWaitTrendPoint> lockWait, List<TrendPoint> blocking, List<TrendPoint> deadlocks,
        int hoursBack, DateTime? fromDate, DateTime? toDate, IReadOnlyList<string>? databaseNames = null)
    {
        var (start, end) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
        var blockingFromXe = await BlockingReadTookXeAsync(start, end, databaseNames);
        await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.WaitStats, LockWaitTrendTruncationBanner, start, end);
        await RefreshBlockingBannerAsync(blockingFromXe, BlockingTrendTruncationBanner, start, end, EarliestBlockingTrendPointDrawn(blocking), databaseNames);
        await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.Deadlocks, DeadlockTrendTruncationBanner, start, end, EarliestBlockingTrendPointDrawn(deadlocks));
    }

    /// <summary>The Blocking Stats sub-tab's two notes (one per chart pair).</summary>
    private async System.Threading.Tasks.Task RefreshBlockingStatsBannersAsync(
        List<BlockingDurationStatsPoint> durationStats, List<PerformanceMonitor.Common.DeadlockSeverityStatsPoint> deadlockSeverity,
        int hoursBack, DateTime? fromDate, DateTime? toDate, IReadOnlyList<string>? databaseNames = null)
    {
        var (start, end) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
        var blockingFromXe = await BlockingReadTookXeAsync(start, end, databaseNames);
        await RefreshBlockingBannerAsync(blockingFromXe, BlockingStatsBlockingTruncationBanner, start, end, EarliestBlockingStatsPointDrawn(durationStats), databaseNames);
        await RefreshWindowTruncatedBannerAsync(QueryWindowRelation.Deadlocks, BlockingStatsDeadlockTruncationBanner, start, end, EarliestDeadlockStatsPointDrawn(deadlockSeverity));
    }
}
