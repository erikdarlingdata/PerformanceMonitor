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
        SchedulerIssuesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        SchedulerIssuesCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
    }

    private async System.Threading.Tasks.Task LoadSevereErrorsAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => _dataService.GetSevereErrorsAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter));
        _seSevereErrorFilterMgr!.UpdateData(data);
        SevereErrorsNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        SevereErrorsCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
    }

    private async System.Threading.Tasks.Task LoadMemoryConditionsAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => _dataService.GetMemoryConditionsAsync(_serverId, hoursBack, fromDate, toDate));
        _seMemoryConditionsFilterMgr!.UpdateData(data);
        MemoryConditionsNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        MemoryConditionsCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
    }

    private async System.Threading.Tasks.Task LoadMemoryBrokerAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => _dataService.GetMemoryBrokerAsync(_serverId, hoursBack, fromDate, toDate));
        _seMemoryBrokerFilterMgr!.UpdateData(data);
        MemoryBrokerNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        MemoryBrokerCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
    }

    private async System.Threading.Tasks.Task LoadMemoryNodeOomAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => _dataService.GetMemoryNodeOomAsync(_serverId, hoursBack, fromDate, toDate));
        _seMemoryNodeOomFilterMgr!.UpdateData(data);
        MemoryNodeOomNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        MemoryNodeOomCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
    }

    private async System.Threading.Tasks.Task LoadSignificantWaitsAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => _dataService.GetSignificantWaitsAsync(_serverId, hoursBack, fromDate, toDate));
        _seSignificantWaitsFilterMgr!.UpdateData(data);
        SignificantWaitsNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        SignificantWaitsCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
    }

    private async System.Threading.Tasks.Task LoadCpuTasksAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => _dataService.GetCpuTasksAsync(_serverId, hoursBack, fromDate, toDate));
        _seCpuTasksFilterMgr!.UpdateData(data);
        CpuTasksNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        CpuTasksCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
    }

    private async System.Threading.Tasks.Task LoadIoIssuesAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => _dataService.GetIoIssuesAsync(_serverId, hoursBack, fromDate, toDate));
        _seIoIssuesFilterMgr!.UpdateData(data);
        IoIssuesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        IoIssuesCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
    }

    private async System.Threading.Tasks.Task LoadDefaultTraceEventsAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => _dataService.GetDefaultTraceEventsAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter));
        _seDefaultTraceFilterMgr!.UpdateData(data);
        DefaultTraceNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        DefaultTraceCountIndicator.Text = data.Count > 0 ? $"{data.Count} event(s)" : "";
    }

    /// <summary>The per-sub-tab Refresh button reloads the active System Events sub-tab over the toolbar's
    /// current window (mirrors the other tabs' toolbar-driven refresh).</summary>
    private async void SystemEventsRefresh_Click(object sender, RoutedEventArgs e)
    {
        var (hoursBack, fromDate, toDate) = GetCurrentWindowUtc();

        await RefreshSystemEventsAsync(hoursBack, fromDate, toDate);
    }
}
