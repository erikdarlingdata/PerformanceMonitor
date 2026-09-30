/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Windows
{
    public partial class CollectionLogWindow : Window
    {
        private readonly string _collectorName;
        private readonly LocalDataService _dataService;
        private readonly int _serverId;
        private readonly ServerClock? _serverClock;

        /// <param name="serverClock">The clock of the server whose history this is (the tab it was opened from), so its
        /// run times read that server's wall time in Server mode (#4766). Null falls back to the active tab's.</param>
        public CollectionLogWindow(LocalDataService dataService, int serverId, string collectorName, ServerClock? serverClock = null)
        {
            InitializeComponent();

            _dataService = dataService;
            _serverId = serverId;
            _collectorName = collectorName;
            _serverClock = serverClock;

            CollectorNameText.Text = $"Collection History: {collectorName}";

            Loaded += CollectionLogWindow_Loaded;
        }

        private async void CollectionLogWindow_Loaded(object sender, RoutedEventArgs e)
        {
            await LoadCollectionLogAsync();
        }

        private async Task LoadCollectionLogAsync()
        {
            try
            {
                var logs = await _dataService.GetCollectionLogByCollectorAsync(_serverId, _collectorName);
                foreach (var log in logs) log.Clock = _serverClock;
                LogDataGrid.ItemsSource = logs;

                if (logs.Count > 0)
                {
                    var successCount = logs.Count(l => l.Status == "SUCCESS");
                    var errorCount = logs.Count(l => l.Status == "ERROR");
                    var avgDuration = logs.Where(l => l.Status == "SUCCESS" && l.DurationMs.HasValue)
                                           .Select(l => (double)l.DurationMs!.Value)
                                           .DefaultIfEmpty(0)
                                           .Average();

                    SummaryText.Text = $"Total Runs: {logs.Count} | Success: {successCount} | Errors: {errorCount} | Avg Duration: {avgDuration:F0} ms";
                }
                else
                {
                    SummaryText.Text = "No collection history found for this collector.";
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(
                    $"Failed to load collection history:\n\n{ex.Message}",
                    "Error",
                    MessageBoxButton.OK,
                    MessageBoxImage.Error
                );
            }
        }

        private void CopyCell_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyCell(sender);
        private void CopyRow_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyRow(sender);
        private void CopyAllRows_Click(object sender, RoutedEventArgs e) => DataGridExport.CopyAllRows(sender);
        private void ExportToCsv_Click(object sender, RoutedEventArgs e) => DataGridExport.ExportToCsv(sender, "collection_log", App.CsvSeparator);

        private void Close_Click(object sender, RoutedEventArgs e)
        {
            Close();
        }
    }
}
