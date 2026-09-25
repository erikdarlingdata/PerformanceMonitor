/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Microsoft.Win32;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using static PerformanceMonitor.Ui.DataGridHelpers;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    /* DataGrid copy helpers */
    private void CopyCell_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyCell(sender);

    private void CopyRow_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyRow(sender);

    private void CopyAllRows_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyAllRows(sender);

    private async void CopyReproScript_Click(object sender, RoutedEventArgs e)
    {
        if (sender is not MenuItem menuItem) return;
        var grid = FindParentDataGrid(menuItem);
        if (grid?.CurrentItem == null) return;

        string? queryText = null;
        string? databaseName = null;
        string? planXml = null;
        string? isolationLevel = null;
        string source = "Query";

        switch (grid.CurrentItem)
        {
            case QuerySnapshotRow snapshot:
                queryText = snapshot.QueryText;
                databaseName = snapshot.DatabaseName;
                isolationLevel = snapshot.TransactionIsolationLevel;
                source = "Active Queries";
                /* #4239: the grid no longer carries the payload in-row — fetch it by capture key,
                   same enrichment-on-demand shape as the QueryStatsRow/QueryStoreRow arms above. */
                if (snapshot.HasQueryPlan)
                {
                    try
                    {
                        planXml = await Task.Run(() => _dataService.ResolveSnapshotEstimatedPlanAsync(_serverId, snapshot));
                    }
                    catch { /* Plan fetch failed — continue without plan */ }
                }
                break;

            case QueryStatsRow stats:
                queryText = stats.QueryText;
                databaseName = stats.DatabaseName;
                source = "Top Queries (dm_exec_query_stats)";
                /* Fetch plan on-demand from SQL Server */
                if (!string.IsNullOrEmpty(stats.QueryHash))
                {
                    try
                    {
                        var connStr = _credentialResolver.GetConnectionString(_server);
                        planXml = await LocalDataService.FetchQueryPlanOnDemandAsync(connStr, stats.QueryHash);
                    }
                    catch { /* Plan fetch failed — continue without plan */ }
                }
                break;

            case QueryStoreRow qs:
                queryText = qs.QueryText;
                databaseName = qs.DatabaseName;
                source = "Query Store";
                /* Fetch plan on-demand from Query Store */
                if (qs.PlanId > 0 && !string.IsNullOrEmpty(qs.DatabaseName))
                {
                    try
                    {
                        var connStr = _credentialResolver.GetConnectionString(_server);
                        planXml = await LocalDataService.FetchQueryStorePlanAsync(connStr, qs.DatabaseName, qs.PlanId);
                    }
                    catch { /* Plan fetch failed — continue without plan */ }
                }
                break;

            default:
                /* Not a supported grid for repro scripts — copy query text if available */
                var textProp = grid.CurrentItem.GetType().GetProperty("QueryText");
                queryText = textProp?.GetValue(grid.CurrentItem)?.ToString();
                if (string.IsNullOrEmpty(queryText))
                {
                    return;
                }
                var dbProp = grid.CurrentItem.GetType().GetProperty("DatabaseName");
                databaseName = dbProp?.GetValue(grid.CurrentItem)?.ToString();
                break;
        }

        if (string.IsNullOrEmpty(queryText))
        {
            return;
        }

        var script = ReproScriptBuilder.BuildReproScript(queryText, databaseName, planXml, isolationLevel, source, productName: "SQL Server Performance Monitor Lite");

        /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() operation.
           See: https://github.com/dotnet/wpf/issues/9901 */
        Clipboard.SetDataObject(script, false);
    }

    private void ExportToCsv_Click(object sender, RoutedEventArgs e) =>
        DataGridExport.ExportToCsv(sender, _server.DisplayName, App.CsvSeparator);
}
