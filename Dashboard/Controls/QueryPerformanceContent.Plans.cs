/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Windows;
using System.Windows.Controls;
using Microsoft.Win32;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Controls
{
    public partial class QueryPerformanceContent : UserControl
    {
        private async void DownloadActiveQueryPlan_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Button button && button.DataContext is QuerySnapshotItem item && _databaseService != null)
            {
                try
                {
                    SetStatus("Fetching query plan...");

                    // Fetch the plan on-demand (not loaded with grid data for performance)
                    var queryPlan = await _databaseService.GetQuerySnapshotPlanAsync(item.CollectionTime, item.SessionId);

                    if (string.IsNullOrWhiteSpace(queryPlan))
                    {
                        MessageBox.Show("No query plan available.", "No Plan", MessageBoxButton.OK, MessageBoxImage.Information);
                        SetStatus("Ready");
                        return;
                    }

                    var rowNumber = ActiveQueriesDataGrid.Items.IndexOf(item) + 1;
                    var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
                    var defaultFileName = $"active_query_plan_{item.SessionId}_{rowNumber}_{timestamp}.sqlplan";

                    var saveFileDialog = new SaveFileDialog
                    {
                        FileName = defaultFileName,
                        DefaultExt = ".sqlplan",
                        Filter = "SQL Plan (*.sqlplan)|*.sqlplan|XML Files (*.xml)|*.xml|All Files (*.*)|*.*",
                        Title = "Save Query Plan"
                    };

                    if (saveFileDialog.ShowDialog() == true)
                    {
                        File.WriteAllText(saveFileDialog.FileName, queryPlan);
                        MessageBox.Show($"Query plan saved to:\n{saveFileDialog.FileName}", "Success", MessageBoxButton.OK, MessageBoxImage.Information);
                    }

                    SetStatus("Ready");
                }
                catch (Exception ex)
                {
                    MessageBox.Show($"Error fetching/saving query plan:\n{ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                    SetStatus("Error fetching query plan");
                }
            }
        }

        private void DownloadCurrentActiveEstPlan_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.DataContext is not LiveQueryItem item) return;

            if (string.IsNullOrEmpty(item.QueryPlan))
            {
                MessageBox.Show("No estimated plan is available for this query.", "No Plan Available",
                    MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
            var defaultFileName = $"estimated_plan_{item.SessionId}_{timestamp}.sqlplan";

            var saveFileDialog = new SaveFileDialog
            {
                FileName = defaultFileName,
                DefaultExt = ".sqlplan",
                Filter = "SQL Plan (*.sqlplan)|*.sqlplan|XML Files (*.xml)|*.xml|All Files (*.*)|*.*",
                Title = "Save Query Plan"
            };

            if (saveFileDialog.ShowDialog() == true)
            {
                File.WriteAllText(saveFileDialog.FileName, item.QueryPlan);
            }
        }

        private void DownloadCurrentActiveLivePlan_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.DataContext is not LiveQueryItem item) return;

            if (string.IsNullOrEmpty(item.LiveQueryPlan))
            {
                MessageBox.Show(
                    "No live query plan is available for this session. The query may have completed before the plan could be captured.",
                    "No Plan Available",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
                return;
            }

            var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
            var defaultFileName = $"live_plan_{item.SessionId}_{timestamp}.sqlplan";

            var saveFileDialog = new SaveFileDialog
            {
                FileName = defaultFileName,
                DefaultExt = ".sqlplan",
                Filter = "SQL Plan (*.sqlplan)|*.sqlplan|XML Files (*.xml)|*.xml|All Files (*.*)|*.*",
                Title = "Save Live Query Plan"
            };

            if (saveFileDialog.ShowDialog() == true)
            {
                File.WriteAllText(saveFileDialog.FileName, item.LiveQueryPlan);
            }
        }

        private async void ViewEstimatedPlan_Click(object sender, RoutedEventArgs e)
        {
            var item = GetContextMenuDataItem(sender);
            if (item == null) return;

            string? planXml = null;
            string? queryText = null;
            string label = "Estimated Plan";

            switch (item)
            {
                case QuerySnapshotItem snap when !string.IsNullOrEmpty(snap.QueryPlan):
                    planXml = snap.QueryPlan;
                    queryText = snap.QueryText;
                    label = $"Est Plan - SPID {snap.SessionId}";
                    break;
                case LiveQueryItem live when !string.IsNullOrEmpty(live.LiveQueryPlan):
                    planXml = live.LiveQueryPlan;
                    queryText = live.QueryText;
                    label = $"Plan - SPID {live.SessionId}";
                    break;
                case LiveQueryItem live when !string.IsNullOrEmpty(live.QueryPlan):
                    planXml = live.QueryPlan;
                    queryText = live.QueryText;
                    label = $"Est Plan - SPID {live.SessionId}";
                    break;
                case QueryStatsItem stats when !string.IsNullOrEmpty(stats.QueryPlanXml):
                    planXml = stats.QueryPlanXml;
                    queryText = stats.QueryText;
                    label = $"Est Plan - {stats.QueryHash}";
                    break;
                case ProcedureStatsItem proc when !string.IsNullOrEmpty(proc.QueryPlanXml):
                    planXml = proc.QueryPlanXml;
                    queryText = proc.ObjectName;
                    label = $"Est Plan - {proc.ProcedureName}";
                    break;
                case QueryStoreItem qs:
                    if (string.IsNullOrEmpty(qs.QueryPlanXml) && _databaseService != null)
                    {
                        qs.QueryPlanXml = await _databaseService.GetQueryStorePlanXmlAsync(qs.DatabaseName, qs.QueryId);
                    }
                    planXml = qs.QueryPlanXml;
                    queryText = qs.QueryText;
                    label = $"Est Plan - QS {qs.QueryId}";
                    break;
                case QueryStoreRegressionItem reg:
                    if (string.IsNullOrEmpty(reg.QueryPlanXml) && _databaseService != null)
                    {
                        reg.QueryPlanXml = await _databaseService.GetQueryStorePlanXmlAsync(reg.DatabaseName, reg.QueryId);
                    }
                    planXml = reg.QueryPlanXml;
                    queryText = reg.QueryTextSample;
                    label = $"Est Plan - QS {reg.QueryId}";
                    break;
            }

            if (planXml == null && item is LongRunningQueryPatternItem)
            {
                MessageBox.Show(
                    "Query trace patterns are aggregated data with no cached plan. Use 'Get Actual Plan' to generate one.",
                    "No Cached Plan",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
                return;
            }

            if (planXml != null)
            {
                ViewPlanRequested?.Invoke(planXml, label, queryText);
            }
            else
            {
                MessageBox.Show(
                    "No query plan is available for this row. The plan may have been evicted from the plan cache since it was last collected.",
                    "No Plan Available",
                    MessageBoxButton.OK,
                    MessageBoxImage.Information);
            }
        }

        private async void GetActualPlan_Click(object sender, RoutedEventArgs e)
        {
            if (_databaseService == null) return;

            var item = GetContextMenuDataItem(sender);
            if (item == null) return;

            string? queryText = null;
            string? databaseName = null;
            string? planXml = null;
            string? isolationLevel = null;
            string label = "Actual Plan";

            switch (item)
            {
                case QuerySnapshotItem snap:
                    queryText = snap.QueryText;
                    databaseName = snap.DatabaseName;
                    planXml = snap.QueryPlan;
                    label = $"Actual Plan - SPID {snap.SessionId}";
                    break;
                case LiveQueryItem live:
                    queryText = live.QueryText;
                    databaseName = live.DatabaseName;
                    planXml = live.LiveQueryPlan ?? live.QueryPlan;
                    label = $"Actual Plan - SPID {live.SessionId}";
                    break;
                case QueryStatsItem stats:
                    queryText = stats.QueryText;
                    databaseName = stats.DatabaseName;
                    planXml = stats.QueryPlanXml;
                    label = $"Actual Plan - {stats.QueryHash}";
                    break;
                case QueryStoreItem qs:
                    queryText = qs.QueryText;
                    databaseName = qs.DatabaseName;
                    if (string.IsNullOrEmpty(qs.QueryPlanXml))
                        qs.QueryPlanXml = await _databaseService.GetQueryStorePlanXmlAsync(qs.DatabaseName, qs.QueryId);
                    planXml = qs.QueryPlanXml;
                    label = $"Actual Plan - QS {qs.QueryId}";
                    break;
                case QueryStoreRegressionItem reg:
                    queryText = reg.QueryTextSample;
                    databaseName = reg.DatabaseName;
                    if (string.IsNullOrEmpty(reg.QueryPlanXml))
                        reg.QueryPlanXml = await _databaseService.GetQueryStorePlanXmlAsync(reg.DatabaseName, reg.QueryId);
                    planXml = reg.QueryPlanXml;
                    label = $"Actual Plan - QS {reg.QueryId}";
                    break;
                case LongRunningQueryPatternItem lrq:
                    queryText = lrq.SampleQueryText;
                    databaseName = lrq.DatabaseName;
                    label = $"Actual Plan - Pattern";
                    break;
            }

            if (string.IsNullOrWhiteSpace(queryText))
            {
                MessageBox.Show("No query text available for this row.", "No Query Text",
                    MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            var result = MessageBox.Show(
                $"You are about to execute this query against the monitored server in database [{databaseName ?? "default"}].\n\n" +
                "Make sure you understand what the query does before proceeding.\n" +
                "The query will execute with SET STATISTICS XML ON to capture the actual plan.\n" +
                "All data results will be discarded.",
                "Get Actual Plan",
                MessageBoxButton.OKCancel,
                MessageBoxImage.Warning);

            if (result != MessageBoxResult.OK) return;

            ActualPlanStarted?.Invoke(label);

            _actualPlanCts?.Dispose();
            _actualPlanCts = new CancellationTokenSource();

            try
            {
                _statusCallback?.Invoke("Executing query for actual plan...");

                var actualPlanXml = await ActualPlanExecutor.ExecuteForActualPlanAsync(
                    _databaseService.ConnectionString,
                    databaseName ?? "",
                    queryText,
                    planXml,
                    isolationLevel,
                    isAzureSqlDb: false,
                    timeoutSeconds: 0,
                    _actualPlanCts.Token);

                if (!string.IsNullOrEmpty(actualPlanXml))
                {
                    ViewPlanRequested?.Invoke(actualPlanXml, label, queryText);
                    _statusCallback?.Invoke("Actual plan captured successfully.");
                }
                else
                {
                    MessageBox.Show("Query executed but no execution plan was captured.",
                        "No Plan", MessageBoxButton.OK, MessageBoxImage.Information);
                    _statusCallback?.Invoke("No actual plan captured.");
                }
            }
            catch (OperationCanceledException)
            {
                MessageBox.Show("The query was cancelled or timed out.",
                    "Cancelled", MessageBoxButton.OK, MessageBoxImage.Information);
                _statusCallback?.Invoke("Actual plan capture cancelled.");
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to get actual plan:\n\n{ex.Message}",
                    "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                _statusCallback?.Invoke("Actual plan capture failed.");
            }
            finally
            {
                ActualPlanFinished?.Invoke();
            }
        }

        private static object? GetContextMenuDataItem(object sender)
        {
            if (sender is not MenuItem menuItem) return null;
            var contextMenu = menuItem.Parent as ContextMenu;

            // Context menu is on a DataGridRow — get its DataContext
            if (contextMenu?.PlacementTarget is DataGridRow row)
                return row.DataContext;

            return null;
        }
    }
}
