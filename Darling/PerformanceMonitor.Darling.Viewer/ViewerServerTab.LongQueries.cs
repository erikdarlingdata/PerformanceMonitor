/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Threading.Tasks;
using System.Windows;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Viewer;

public partial class ViewerServerTab
{
    /// <summary>
    /// The disabled-trace banner for a target of <paramref name="engineEdition"/>: where the switch is, then where
    /// the session lives (<see cref="LongQueryCompletionsCollector.SessionScopeSentence"/>). On Azure SQL Database the
    /// session is per monitored database, so the banner must not say it is on the server.
    /// </summary>
    internal static string LongQueriesDisabledText(int engineEdition) =>
        "The long-query completion trace is OFF for this server. It is opt-in because a completion trace adds overhead "
        + "on busy servers. Turn it on in Settings → Collection Schedule → Edit Collector Schedules…, then tick "
        + "the 'long_query_completions' Enabled box. "
        + LongQueryCompletionsCollector.SessionScopeSentence(engineEdition == CollectorEngineCapability.AzureSqlDatabaseEngineEdition);

    /// <summary>
    /// Long Queries inner tab (#1496): completed long-running queries (rpc/batch over the duration
    /// threshold) plus attentions (cancels/timeouts) from the opt-in XE session, windowed on the toolbar's
    /// range and bound through the filter manager. Because the collector is default OFF, an explicit
    /// empty-state banner is shown while it is disabled, pointing the operator at the exact switch
    /// (Settings → Collection Schedule → Edit Collector Schedules… → the Enabled box). The grid applies a
    /// view-only DESCENDING-by-duration sort, never touching the reader's chronological ORDER BY.
    /// </summary>
    private async Task LoadLongQueriesAsync()
    {
        LongQueriesDisabledWarning.Text = LongQueriesDisabledText(_server.EngineEdition);
        LongQueriesDisabledWarning.Visibility =await _dataService.GetLongQueryTraceEnabledAsync(_server.ServerId)
            ? Visibility.Collapsed
            : Visibility.Visible;

        var (startUtc, endUtc) = GetWindowUtc();
        var rows = await _dataService.GetRecentLongQueryCompletionsAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
        _longQueryFilterMgr!.UpdateData(rows);

        SetDefaultSortIfNone(LongQueryCompletionsGrid, "DurationMicroseconds", ListSortDirection.Descending);
    }
}
