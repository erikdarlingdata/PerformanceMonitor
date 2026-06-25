/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard
{
    public partial class ServerTab : UserControl
    {
        /// <summary>
        /// Opens the deadlock graph viewer for the right-clicked deadlock row. The grid query defers the
        /// graph XML (CONVERT(xml) over ~100 graphs is expensive), so fetch it on demand; the fetch + parse
        /// run off the UI thread, only the render touches the UI.
        /// </summary>
        private async void ViewDeadlockGraph_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not MenuItem menuItem) return;
            if (menuItem.Parent is not ContextMenu cm) return;
            var grid = FindDataGridFromContextMenu(cm);
            if (grid?.SelectedItem is not DeadlockItem row) return;
            await OpenDeadlockGraphAsync(row);
        }

        /// <summary>Double-clicking a deadlock row opens the same graph viewer (no toolbar button — see plan).</summary>
        private async void DeadlocksDataGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
        {
            if (!Helpers.TabHelpers.IsDoubleClickOnRow((DependencyObject)e.OriginalSource)) return;
            if (DeadlocksDataGrid.SelectedItem is not DeadlockItem row) return;
            await OpenDeadlockGraphAsync(row);
        }

        private async Task OpenDeadlockGraphAsync(DeadlockItem row)
        {
            if (_databaseService == null) return;

            try
            {
                // Fetch the deferred graph (if not already on the row) + parse, all off the UI thread.
                var model = await Task.Run(async () =>
                {
                    var xml = row.DeadlockGraph;
                    if (string.IsNullOrWhiteSpace(xml))
                        xml = await _databaseService.GetDeadlockGraphAsync(row.CollectionTime, row.DeadlockId) ?? string.Empty;
                    return DeadlockGraphParser.Parse(xml);
                });

                var control = new DeadlockGraphControl();
                control.LoadModel(model);
                GraphViewerWindow.ShowGraph(
                    Window.GetWindow(this),
                    control,
                    $"Deadlock Graph — {_serverConnection.DisplayName}");
            }
            catch (Exception ex)
            {
                MessageBox.Show(
                    Window.GetWindow(this)!,
                    $"Failed to build the deadlock graph view:\n\n{ex.Message}",
                    "Deadlock Graph Error",
                    MessageBoxButton.OK,
                    MessageBoxImage.Warning);
            }
        }
    }
}
