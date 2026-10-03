/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// The System Events tab (system_health parity) — the Lite port of the Darling viewer's System Events tab,
/// itself the faithful reproduction of the full Dashboard's System Events. Eleven inner sub-tabs cover all
/// the Dashboard categories. The two lead sub-tabs (Corruption Events + Contention Events) are the
/// SYSTEM-component counter CHARTS rendered from a single sp_server_diagnostics SYSTEM feed with no
/// significance filter (every snapshot plotted over time), wired in <c>ServerTab.SystemHealthCharts.cs</c>.
/// The remaining nine sub-tabs are parse-on-read GRIDS, one per warning category: the reader
/// (<see cref="LocalDataService"/>) fetches the raw event_xml for the category over the toolbar's window
/// from v_system_health_events, the Common <c>SystemHealthParser</c> shreds it, and the shared
/// <c>SystemHealthSignificance</c> keeps the sp_HealthParser-significant rows. Loads the ACTIVE sub-tab only
/// (Lite's visible-only rule) via the RefreshVisibleTabAsync switch (case 18); a sub-tab switch reloads
/// through MainTabControl_SelectionChanged (SystemEventsSubTabControl is in its e.Source allow-list). Reads
/// run off the UI thread (Task.Run), mirroring the deadlock-graph parse.
/// </summary>
public partial class ServerTab : UserControl
{
    /* System Events sub-tab order (matches the XAML TabItem order — Corruption + Contention lead, the two
       SYSTEM-component counter-chart tabs sharing one feed, then the grid categories in Darling's order). */
    private const int SystemEventsCorruptionSubTabIndex = 0;
    private const int SystemEventsContentionSubTabIndex = 1;
    private const int SystemEventsSchedulerIssuesSubTabIndex = 2;
    private const int SystemEventsSevereErrorsSubTabIndex = 3;
    private const int SystemEventsMemoryConditionsSubTabIndex = 4;
    private const int SystemEventsMemoryBrokerSubTabIndex = 5;
    private const int SystemEventsMemoryNodeOomSubTabIndex = 6;
    private const int SystemEventsSignificantWaitsSubTabIndex = 7;
    private const int SystemEventsCpuTasksSubTabIndex = 8;
    private const int SystemEventsIoIssuesSubTabIndex = 9;
    private const int SystemEventsDefaultTraceSubTabIndex = 10;

    /// <summary>
    /// Tab 18 — System Events. Loads the ACTIVE sub-tab only. The chart sub-tabs (Corruption / Contention)
    /// share the one SYSTEM feed, so both render from a single load — mirroring the Dashboard's
    /// one-refresh-feeds-both.
    /// </summary>
    private async System.Threading.Tasks.Task RefreshSystemEventsAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            switch (SystemEventsSubTabControl.SelectedIndex)
            {
                case SystemEventsSchedulerIssuesSubTabIndex:
                    await LoadSchedulerIssuesAsync(hoursBack, fromDate, toDate);
                    break;
                case SystemEventsSevereErrorsSubTabIndex:
                    await LoadSevereErrorsAsync(hoursBack, fromDate, toDate);
                    break;
                case SystemEventsMemoryConditionsSubTabIndex:
                    await LoadMemoryConditionsAsync(hoursBack, fromDate, toDate);
                    break;
                case SystemEventsMemoryBrokerSubTabIndex:
                    await LoadMemoryBrokerAsync(hoursBack, fromDate, toDate);
                    break;
                case SystemEventsMemoryNodeOomSubTabIndex:
                    await LoadMemoryNodeOomAsync(hoursBack, fromDate, toDate);
                    break;
                case SystemEventsSignificantWaitsSubTabIndex:
                    await LoadSignificantWaitsAsync(hoursBack, fromDate, toDate);
                    break;
                case SystemEventsCpuTasksSubTabIndex:
                    await LoadCpuTasksAsync(hoursBack, fromDate, toDate);
                    break;
                case SystemEventsIoIssuesSubTabIndex:
                    await LoadIoIssuesAsync(hoursBack, fromDate, toDate);
                    break;
                case SystemEventsDefaultTraceSubTabIndex:
                    await LoadDefaultTraceEventsAsync(hoursBack, fromDate, toDate);
                    break;
                // Corruption Events (0) and Contention Events (1) share the one SYSTEM feed, so both render
                // from a single load — this is also the default, so the first-shown sub-tab loads on activation.
                case SystemEventsCorruptionSubTabIndex:
                case SystemEventsContentionSubTabIndex:
                default:
                    await RefreshSystemHealthChartsAsync(hoursBack, fromDate, toDate);
                    break;
            }
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshSystemEventsAsync failed: {ex.Message}");
        }
    }

    private async System.Threading.Tasks.Task LoadSchedulerIssuesAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => _dataService.GetSchedulerIssuesAsync(_serverId, hoursBack, fromDate, toDate));
        _seSchedulerFilterMgr!.UpdateData(data);
        ShowSystemHealthEmptyState(SchedulerIssuesNoDataMessage, data.Count);
        SchedulerIssuesCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
        await RefreshStoredWindowBannerAsync(QueryWindowRelation.SystemHealthEvents, SchedulerIssuesWindowTruncatedBanner, hoursBack, fromDate, toDate);
    }

    private async System.Threading.Tasks.Task LoadSevereErrorsAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => _dataService.GetSevereErrorsAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter));
        _seSevereErrorFilterMgr!.UpdateData(data);
        ShowSystemHealthEmptyState(SevereErrorsNoDataMessage, data.Count);
        SevereErrorsCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
        await RefreshStoredWindowBannerAsync(QueryWindowRelation.SystemHealthEvents, SevereErrorsWindowTruncatedBanner, hoursBack, fromDate, toDate);
    }

    private async System.Threading.Tasks.Task LoadMemoryConditionsAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => _dataService.GetMemoryConditionsAsync(_serverId, hoursBack, fromDate, toDate));
        _seMemoryConditionsFilterMgr!.UpdateData(data);
        ShowSystemHealthEmptyState(MemoryConditionsNoDataMessage, data.Count);
        MemoryConditionsCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
        await RefreshStoredWindowBannerAsync(QueryWindowRelation.SystemHealthEvents, MemoryConditionsWindowTruncatedBanner, hoursBack, fromDate, toDate);
    }

    private async System.Threading.Tasks.Task LoadMemoryBrokerAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => _dataService.GetMemoryBrokerAsync(_serverId, hoursBack, fromDate, toDate));
        _seMemoryBrokerFilterMgr!.UpdateData(data);
        ShowSystemHealthEmptyState(MemoryBrokerNoDataMessage, data.Count);
        MemoryBrokerCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
        await RefreshStoredWindowBannerAsync(QueryWindowRelation.SystemHealthEvents, MemoryBrokerWindowTruncatedBanner, hoursBack, fromDate, toDate);
    }

    private async System.Threading.Tasks.Task LoadMemoryNodeOomAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => _dataService.GetMemoryNodeOomAsync(_serverId, hoursBack, fromDate, toDate));
        _seMemoryNodeOomFilterMgr!.UpdateData(data);
        ShowSystemHealthEmptyState(MemoryNodeOomNoDataMessage, data.Count);
        MemoryNodeOomCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
        await RefreshStoredWindowBannerAsync(QueryWindowRelation.SystemHealthEvents, MemoryNodeOomWindowTruncatedBanner, hoursBack, fromDate, toDate);
    }

    private async System.Threading.Tasks.Task LoadSignificantWaitsAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => _dataService.GetSignificantWaitsAsync(_serverId, hoursBack, fromDate, toDate));
        _seSignificantWaitsFilterMgr!.UpdateData(data);
        ShowSystemHealthEmptyState(SignificantWaitsNoDataMessage, data.Count);
        SignificantWaitsCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
        await RefreshStoredWindowBannerAsync(QueryWindowRelation.SystemHealthEvents, SignificantWaitsWindowTruncatedBanner, hoursBack, fromDate, toDate);
    }

    private async System.Threading.Tasks.Task LoadCpuTasksAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => _dataService.GetCpuTasksAsync(_serverId, hoursBack, fromDate, toDate));
        _seCpuTasksFilterMgr!.UpdateData(data);
        ShowSystemHealthEmptyState(CpuTasksNoDataMessage, data.Count);
        CpuTasksCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
        await RefreshStoredWindowBannerAsync(QueryWindowRelation.SystemHealthEvents, CpuTasksWindowTruncatedBanner, hoursBack, fromDate, toDate);
    }

    private async System.Threading.Tasks.Task LoadIoIssuesAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => _dataService.GetIoIssuesAsync(_serverId, hoursBack, fromDate, toDate));
        _seIoIssuesFilterMgr!.UpdateData(data);
        ShowSystemHealthEmptyState(IoIssuesNoDataMessage, data.Count);
        IoIssuesCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
        await RefreshStoredWindowBannerAsync(QueryWindowRelation.SystemHealthEvents, IoIssuesWindowTruncatedBanner, hoursBack, fromDate, toDate);
    }

    private async System.Threading.Tasks.Task LoadDefaultTraceEventsAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => _dataService.GetDefaultTraceEventsAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter));
        _seDefaultTraceFilterMgr!.UpdateData(data);
        if (DefaultTraceGapNote(_server.DisplayName, _isAzureSqlDatabase) is { } gap)
            DefaultTraceNoDataMessage.Text = gap;
        DefaultTraceNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        DefaultTraceCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
        await RefreshStoredWindowBannerAsync(QueryWindowRelation.DefaultTraceEvents, DefaultTraceWindowTruncatedBanner, hoursBack, fromDate, toDate);
    }

    /// <summary>
    /// The note a grid shows in place of its empty-window text when the collector behind it cannot run on this
    /// server's engine: the sentence the MCP tools return as <c>not_collected</c>
    /// (<see cref="CollectorEngineCapability.NotCollectedMessage"/>), which the Darling viewer's panels show too.
    /// Null where the collector does run. The tab only knows whether the server is an Azure SQL Database, so any
    /// other server goes in as an unknown edition, which makes no claim.
    /// </summary>
    internal static string? EngineGapNote(string serverName, bool isAzureSqlDatabase, string collectorName) =>
        CollectorEngineCapability.NotCollectedMessage(
            serverName,
            isAzureSqlDatabase ? CollectorEngineCapability.AzureSqlDatabaseEngineEdition : CollectorEngineCapability.UnknownEngineEdition,
            engineKind: null,
            collectorName);

    /// <summary>The Default Trace grid's note on an Azure SQL Database, which has no default trace; null anywhere else,
    /// where the grid keeps its "no events in this window" text.</summary>
    internal static string? DefaultTraceGapNote(string serverName, bool isAzureSqlDatabase) =>
        EngineGapNote(serverName, isAzureSqlDatabase, "default_trace_events");

    /// <summary>The note every sub-tab the system_health session feeds shows on an Azure SQL Database, where the
    /// system_health_events collector does not run: the eight grids and the two chart sub-tabs. Null anywhere else,
    /// where each grid keeps its "no events in this window" text and the charts show.</summary>
    internal static string? SystemHealthGapNote(string serverName, bool isAzureSqlDatabase) =>
        EngineGapNote(serverName, isAzureSqlDatabase, "system_health_events");

    /// <summary>
    /// #4966: the "Showing since" banner of one grid that reads stored rows over the toolbar's window (the Collection Log,
    /// the System Events and Default Trace grids, the Config Changes grids and Long Queries). It probes the UTC window the
    /// grid's read took, the same <see cref="LocalDataService.GetTimeRange"/> pair
    /// (<see cref="LocalDataService.GetQueriesTabWindowUtc"/>), through <see cref="RefreshWindowTruncatedBannerAsync"/>, so a
    /// probe that throws hides the banner and the refresh goes on. Each surface calls it after its rows are bound, from the one
    /// method its read runs through, so every read path of the surface (the sub-tab switch, a range change and the Refresh
    /// button) refreshes it.
    /// </summary>
    private System.Threading.Tasks.Task RefreshStoredWindowBannerAsync(QueryWindowRelation relation, TextBlock banner, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var (startUtc, endUtc) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate);
        return RefreshWindowTruncatedBannerAsync(relation, banner, startUtc, endUtc);
    }

    /// <summary>A system_health grid's empty state: shown when the window has no rows. On an Azure SQL Database it says
    /// that the collector does not run there, in place of the grid's "no events in this window" text.</summary>
    private void ShowSystemHealthEmptyState(TextBlock message, int rowCount)
    {
        if (SystemHealthGapNote(_server.DisplayName, _isAzureSqlDatabase) is { } gap)
            message.Text = gap;
        message.Visibility = rowCount == 0 ? Visibility.Visible : Visibility.Collapsed;
    }

    /// <summary>The per-sub-tab Refresh button reloads the active System Events sub-tab over the toolbar's
    /// current window (mirrors the other tabs' toolbar-driven refresh). It skips RefreshVisibleTabAsync, so it learns the
    /// engine edition itself before the loader words its empty state.</summary>
    private async void SystemEventsRefresh_Click(object sender, RoutedEventArgs e)
    {
        var (hoursBack, fromDate, toDate) = GetCurrentWindowUtc();

        await RefreshEngineEditionAsync();
        await RefreshSystemEventsAsync(hoursBack, fromDate, toDate);
    }
}
