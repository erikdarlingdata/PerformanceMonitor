/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Configuration Changes inner tab — the Dashboard's dedicated <c>ConfigChangesContent</c> ported as its
/// own per-server tab (three grids: server-config, database-config, trace-flag DRIFT over the window). The
/// read + C# snapshot-diff lives in <see cref="ViewerDataService"/> (ViewerDataService.ConfigChanges.cs,
/// mirroring the service's DarlingConfigHistoryReader / #1421); this loader just windows on the per-server
/// toolbar's settable range (so "Apply to All" flows through unchanged) and hands each result to its column
/// filter manager (registered in ViewerServerTab.Filters.cs) so active filters survive a refresh. Each grid's
/// EmptyHint overlay shows the honest "needs &gt;= 2 captures" state when the diff yields nothing (config is
/// captured on connect/restart, not on a timer, so change history is inherently sparse). LoadInnerTabAsync
/// owns the try/catch that surfaces failures on the status bar.
/// </summary>
public partial class ViewerServerTab : UserControl
{
    private async Task LoadConfigChangesAsync()
    {
        var (startUtc, endUtc) = GetWindowUtc();

        using var readFanOut = ViewerReadFanOut.Of(3);

        var serverTask = _dataService.GetServerConfigChangesAsync(_server.ServerId, startUtc, endUtc);
        var databaseTask = _dataService.GetDatabaseConfigChangesAsync(_server.ServerId, startUtc, endUtc, databaseNames: SelectedDatabaseFilter);
        var traceFlagTask = _dataService.GetTraceFlagChangesAsync(_server.ServerId, startUtc, endUtc);

        await Task.WhenAll(serverTask, databaseTask, traceFlagTask);

        var serverChanges = serverTask.Result;
        var databaseChanges = databaseTask.Result;
        var traceFlagChanges = traceFlagTask.Result;

        _serverConfigChangesFilterMgr!.UpdateData(serverChanges);
        _databaseConfigChangesFilterMgr!.UpdateData(databaseChanges);
        _traceFlagChangesFilterMgr!.UpdateData(traceFlagChanges);

        /* Empty state keyed on the unfiltered change count (a real "no drift in this window" signal), matching
           the Dashboard's data.Count == 0 check — independent of any active column filter. */
        ServerConfigChangesNoDataMessage.Visibility = serverChanges.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        DatabaseConfigChangesNoDataMessage.Visibility = databaseChanges.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        TraceFlagChangesNoDataMessage.Visibility = traceFlagChanges.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
    }
}
