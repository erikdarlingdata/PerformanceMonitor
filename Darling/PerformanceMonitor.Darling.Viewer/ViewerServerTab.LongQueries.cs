/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Linq;
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

        /* #5555: the trace check does not depend on the grid's reads, so it starts beside them and is awaited once the grid is in.
           It used to be awaited first, which put one whole store round trip in front of the grid's two reads on every load. */
        var traceTask = Timed("trace check", _dataService.GetLongQueryTraceEnabledAsync(_server.ServerId));
        var (startUtc, endUtc) = GetWindowUtc();
        var dataStartTask = Timed("data start", _dataService.GetLongQueriesDataStartAsync(_server.ServerId, startUtc, endUtc));
        var dataReadTask = Timed("grid read", _dataService.GetRecentLongQueryCompletionsAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter));
        try
        {
            await AwaitReadWatchingProbeAsync(dataReadTask, dataStartTask, "Long Queries");
        }
        catch
        {
            /* The grid's own error is the one the user sees; the trace check is observed, never waited for. */
            _ = ViewerDataService.ObserveAsync(traceTask);
            throw;
        }

        LongQueriesDisabledWarning.Visibility = await traceTask
            ? Visibility.Collapsed
            : Visibility.Visible;
        var rows = dataReadTask.Result;
        _longQueryFilterMgr!.UpdateData(rows);
        /* #4966: the read windows on collection_time but the grid shows event_time, and a first collection stores the events
           the session still held, so the notice names the earlier of the coverage start and the earliest event shown; a full
           page (the read keeps the newest 200) names its oldest event. */
        await ShowEventDataStartAsync(LongQueriesTruncationBanner, dataStartTask, "Long Queries", startUtc, rows.Select(r => r.EventTime), ViewerDataService.LongQueriesRowCap);

        SetDefaultSortIfNone(LongQueryCompletionsGrid, "DurationMicroseconds", ListSortDirection.Descending);
    }
}
