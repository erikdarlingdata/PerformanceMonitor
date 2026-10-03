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
    /// Tab 20 — Long Queries (#1496): completed long-running queries (rpc/batch over the duration
    /// threshold) plus attentions (cancels/timeouts) from the opt-in XE session. Because the collector
    /// is default OFF, an explicit empty-state banner is shown while it is disabled, pointing the
    /// operator at the exact switch (Settings → Collector Schedules → Edit → the Enabled box). The
    /// enabled state is read live each refresh so toggling it updates the banner without reopening.
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
            await RefreshStoredWindowBannerAsync(QueryWindowRelation.LongQueryCompletions, LongQueriesWindowTruncatedBanner, hoursBack, fromDate, toDate);
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshLongQueriesAsync failed: {ex.Message}");
        }
    }
}
