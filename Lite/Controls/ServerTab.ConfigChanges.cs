/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// The Configuration Changes tab (Tier-1 parity) — the Lite port of the Darling viewer's Configuration Changes
/// tab. Three sub-tab GRIDS show server-config / database-config / trace-flag DRIFT over the toolbar window,
/// COMPUTED by diffing the store's append-only config snapshots via the shared
/// <c>PerformanceMonitor.Common.ConfigChangeDiff</c> (never re-implemented here — the reader
/// <see cref="LocalDataService.GetServerConfigChangesAsync"/> etc. fetch the DuckDB snapshots and call it).
/// Loads the ACTIVE sub-tab only (Lite's visible-only rule) via the RefreshVisibleTabAsync switch (case 19); a
/// sub-tab switch reloads through MainTabControl_SelectionChanged (ConfigChangesSubTabControl is in its
/// e.Source allow-list). Config is captured on connect/restart (not a timer), so a category needs &gt;= 2
/// captures bracketing a change to appear — each grid shows an honest no-data hint otherwise.
/// </summary>
public partial class ServerTab : UserControl
{
    /* Configuration Changes sub-tab order (matches the XAML TabItem order). */
    private const int ConfigChangesServerSubTabIndex = 0;
    private const int ConfigChangesDatabaseSubTabIndex = 1;
    private const int ConfigChangesTraceFlagSubTabIndex = 2;

    /// <summary>
    /// Tab 19 — Configuration Changes. Loads the ACTIVE sub-tab only (Server Config / Database Config / Trace
    /// Flag changes), mirroring the System Events tab's visible-only load.
    /// </summary>
    private async System.Threading.Tasks.Task RefreshConfigChangesAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        try
        {
            switch (ConfigChangesSubTabControl.SelectedIndex)
            {
                case ConfigChangesDatabaseSubTabIndex:
                    await LoadDatabaseConfigChangesAsync(hoursBack, fromDate, toDate);
                    break;
                case ConfigChangesTraceFlagSubTabIndex:
                    await LoadTraceFlagChangesAsync(hoursBack, fromDate, toDate);
                    break;
                case ConfigChangesServerSubTabIndex:
                default:
                    await LoadServerConfigChangesAsync(hoursBack, fromDate, toDate);
                    break;
            }
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] RefreshConfigChangesAsync failed: {ex.Message}");
        }
    }

    private async System.Threading.Tasks.Task LoadServerConfigChangesAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => _dataService.GetServerConfigChangesAsync(_serverId, hoursBack, fromDate, toDate));
        _serverConfigChangesFilterMgr!.UpdateData(data);
        if (EngineGapNote(_server.DisplayName, _isAzureSqlDatabase, "server_config") is { } gap)
            ServerConfigChangesNoDataMessage.Text = gap;
        ServerConfigChangesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        ServerConfigChangesCountIndicator.Text = data.Count > 0 ? $"{data.Count} change(s)" : "";
    }

    private async System.Threading.Tasks.Task LoadDatabaseConfigChangesAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => _dataService.GetDatabaseConfigChangesAsync(_serverId, hoursBack, fromDate, toDate, SelectedDatabaseFilter));
        _databaseConfigChangesFilterMgr!.UpdateData(data);
        DatabaseConfigChangesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        DatabaseConfigChangesCountIndicator.Text = data.Count > 0 ? $"{data.Count} change(s)" : "";
    }

    private async System.Threading.Tasks.Task LoadTraceFlagChangesAsync(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var data = await Task.Run(() => _dataService.GetTraceFlagChangesAsync(_serverId, hoursBack, fromDate, toDate));
        _traceFlagChangesFilterMgr!.UpdateData(data);
        if (EngineGapNote(_server.DisplayName, _isAzureSqlDatabase, "trace_flags") is { } gap)
            TraceFlagChangesNoDataMessage.Text = gap;
        TraceFlagChangesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        TraceFlagChangesCountIndicator.Text = data.Count > 0 ? $"{data.Count} change(s)" : "";
    }

    /// <summary>The per-sub-tab Refresh button reloads the active Configuration Changes sub-tab over the
    /// toolbar's current window (mirrors the other tabs' toolbar-driven refresh). It skips RefreshVisibleTabAsync, so it
    /// learns the engine edition itself before the loader words its empty state.</summary>
    private async void ConfigChangesRefresh_Click(object sender, RoutedEventArgs e)
    {
        var (hoursBack, fromDate, toDate) = GetCurrentWindowUtc();

        await RefreshEngineEditionAsync();
        await RefreshConfigChangesAsync(hoursBack, fromDate, toDate);
    }
}
