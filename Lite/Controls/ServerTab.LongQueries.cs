/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Threading.Tasks;
using System.Windows;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab
{
    /// <summary>
    /// The disabled-trace banner for an Azure SQL Database target or any other: where the switch is, then where the
    /// session lives (<see cref="LongQueryCompletionsCollector.SessionScopeSentence"/>). On Azure SQL Database the
    /// session is per monitored database, so the banner must not say it is on the server.
    /// </summary>
    internal static string LongQueriesDisabledText(bool isAzureSqlDatabase) =>
        "The long-query completion trace is OFF for this server. It is opt-in because a completion trace adds overhead "
        + "on busy servers. Turn it on in Settings → Collector Schedules → Edit (Default or per-server), then tick "
        + "the 'long_query_completions' Enabled box. "
        + LongQueryCompletionsCollector.SessionScopeSentence(isAzureSqlDatabase);

    /// <summary>
    /// The instant the Long Queries grid's "Showing since" notice compares (#4989): the completion's own time,
    /// <c>event_time</c>, the XE <c>@timestamp</c>, which is UTC (the grid shows it through the server's clock, and the
    /// read orders and caps on it), and not the time a run stored the row. A completion without an event time falls back to
    /// the time its run was collected, which is UTC too. The Default Trace is the one grid whose time is the server's wall
    /// clock (<see cref="LocalDataService.QueryWindowRelationTimeIsServerLocal"/>); this one needs no conversion.
    /// </summary>
    internal static DateTime LongQueryRowTimeUtc(LongQueryCompletionRow row) => row.EventTime ?? row.CollectionTime;

    /// <summary>
    /// Tab 20 — Long Queries (#1496): completed long-running queries (rpc/batch over the duration
    /// threshold) plus attentions (cancels/timeouts) from the opt-in XE session. Because the collector
    /// is default OFF, an explicit empty-state banner is shown while it is disabled, pointing the
    /// operator at the exact switch (Settings → Collector Schedules → Edit → the Enabled box). The
    /// enabled state is read live each refresh so toggling it updates the banner without reopening.
    ///
    /// <para>The grid reads only the newest <see cref="LocalDataService.LongQueryGridCap"/> completions, so its "Showing
    /// since" notice goes through the cap-aware step (#4989): a read that hit the cap is worded from the oldest completion
    /// it returned, even where the store covers the range, and with no slack (the notice shows whenever that completion is
    /// later than the range's start, on a range of an hour too). A read under the cap keeps the coverage notice.</para>
    /// </summary>
    private async Task RefreshLongQueriesAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        LongQueriesDisabledWarning.Text = LongQueriesDisabledText(_isAzureSqlDatabase);
        LongQueriesDisabledWarning.Visibility = _isLongQueryTraceEnabled()
            ? Visibility.Collapsed
            : Visibility.Visible;

        try
        {
            var task = Task.Run(() => SafeQueryAsync(() =>
                _dataService.GetRecentLongQueryCompletionsAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter)));
            await task;
            _longQueryFilterMgr!.UpdateData(task.Result);

            /* View-only DESCENDING-by-duration sort — never flips the reader's chronological ORDER BY. */
            SetDefaultSortIfNone(LongQueryCompletionsGrid, "DurationMicroseconds", ListSortDirection.Descending);
            var (windowStart, windowEnd) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
            await RefreshCappedGridBannerAsync(QueryWindowRelation.LongQueryCompletions, LongQueriesWindowTruncatedBanner, windowStart, windowEnd, task.Result, LocalDataService.LongQueryGridCap, LongQueryRowTimeUtc);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshLongQueriesAsync failed: {ex.Message}");
        }
    }
}
