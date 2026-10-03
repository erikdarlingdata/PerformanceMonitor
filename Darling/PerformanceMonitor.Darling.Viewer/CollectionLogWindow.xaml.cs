/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer
{
    /// <summary>
    /// The per-collector collection-history drill window (W1i), copied from Lite's
    /// <c>Windows/CollectionLogWindow.xaml.cs</c>. The Collection Health "Health Summary" grid opens it
    /// on double-click. Purely data-driven; the one change from Lite is the data call — repointed to
    /// <see cref="ViewerDataService.GetCollectionLogByCollectorAsync(int, string, DateTime, DateTime, CancellationToken)"/> (Postgres). Copy/Export use the
    /// shared PerformanceMonitor.Ui <see cref="DataGridExport"/> with the viewer's comma separator.
    /// </summary>
    public partial class CollectionLogWindow : Window
    {
        private readonly string _collectorName;
        private readonly ViewerDataService _dataService;
        private readonly int _serverId;

        public CollectionLogWindow(ViewerDataService dataService, int serverId, string collectorName)
        {
            InitializeComponent();

            _dataService = dataService;
            _serverId = serverId;
            _collectorName = collectorName;

            CollectorNameText.Text = $"Collection History: {collectorName}";

            Loaded += CollectionLogWindow_Loaded;
        }

        private async void CollectionLogWindow_Loaded(object sender, RoutedEventArgs e)
        {
            await LoadCollectionLogAsync();
        }

        /// <summary>
        /// The drill's read and its "Showing since" note in one call (#4966): one collector's runs over the trailing week
        /// (<see cref="ViewerDataService.CollectionLogDrillHours"/>), and the note on <paramref name="banner"/> saying where this
        /// server's collection log starts when that is later than the week's start. The read has no row cap, so the note follows the
        /// coverage rule: the later of the server's first collection and the log's retention edge, never the collector's first run,
        /// so a collector that ran late in a covered week says nothing. The probe starts beside the read and asks about the same
        /// window: one start and one end, worked out once, go to both, so a pinned end bounds the read as it bounds the probe, and a
        /// run after that end is in neither the rows nor the note. The two run at once, so the call declares its width of two to the
        /// store's deadline (<see cref="ViewerReadFanOut"/>) as the tabs do, and ends it when the read is in, before the note awaits
        /// its probe. The note names its times in the viewer's display zone, as the tabs' notes do.
        /// </summary>
        /// <param name="asOfUtc">The window's end; null for now. A test names it to pin the window.</param>
        internal static async Task<List<CollectionLogRow>> ReadDrillAsync(
            ViewerDataService dataService, int serverId, string collectorName, TextBlock banner, DateTime? asOfUtc = null)
        {
            var endUtc = asOfUtc ?? DateTime.UtcNow;
            var startUtc = endUtc.AddHours(-ViewerDataService.CollectionLogDrillHours);
            using var readFanOut = ViewerReadFanOut.Of(2);
            var dataStartTask = dataService.GetCollectionLogDataStartAsync(serverId, startUtc, endUtc);
            var logs = await dataService.GetCollectionLogByCollectorAsync(serverId, collectorName, startUtc, endUtc);

            /* The read is in: end the declared width here, before the note awaits its probe, so that await is not priced against
               contention that has already finished. */
            readFanOut.Release();

            await ViewerServerTab.ShowEventDataStartAsync(
                banner, dataStartTask, "Collection Log Drill", startUtc, logs.Select(l => (DateTime?)l.CollectionTime));
            return logs;
        }

        private async Task LoadCollectionLogAsync()
        {
            try
            {
                var logs = await ReadDrillAsync(_dataService, _serverId, _collectorName, CollectionLogTruncationBanner);
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
        private void ExportToCsv_Click(object sender, RoutedEventArgs e) => DataGridExport.ExportToCsv(sender, "collection_log", ViewerExportSettings.CsvSeparator);

        private void Close_Click(object sender, RoutedEventArgs e)
        {
            Close();
        }
    }
}
