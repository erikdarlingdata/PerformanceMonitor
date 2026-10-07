/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Controls;
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

        /// <summary>The span the drill reads: the trailing week, in hours.</summary>
        internal const int DrillHours = 168;

        /// <summary>
        /// One load of the drill (#4966): the collector's runs, the start of the window they were read over, and the answer of
        /// the window-floor probe over that same window (null when the server's log has nothing to report on, or when the
        /// probe failed). Kept apart from the banner so the load runs off the UI thread and the note is applied on it.
        /// </summary>
        internal sealed record DrillRead(List<CollectionLogRow> Rows, DateTime StartUtc, DateTime? Floor);

        /// <summary>
        /// The drill's read and the probe behind its "Showing since" note, over the trailing week (#4966): one collector's runs
        /// (<see cref="LocalDataService.GetCollectionLogByCollectorAsync"/>), and the probe the Collection Log grid on the server
        /// tab asks (<c>QueryWindowRelation.CollectionLog</c>, <see cref="LocalDataService.GetQueryWindowFloorAsync"/>). The probe
        /// reads the SERVER's log, never the collector's own first run, so a collector that started late in a week the server's
        /// log covers says nothing. The read has no row cap, so only that coverage rule applies, not the capped grids' oldest-row
        /// rule. Passed <paramref name="asOfUtc"/> as the window's end for a test to pin; null is now.
        /// </summary>
        internal static Task<DrillRead> ReadDrillAsync(LocalDataService dataService, int serverId, string collectorName, DateTime? asOfUtc = null) =>
            ReadDrillAsync(
                (startUtc, endUtc) => dataService.GetCollectionLogByCollectorAsync(serverId, collectorName, startUtc, endUtc),
                (startUtc, endUtc) => Task.Run(() => dataService.GetQueryWindowFloorAsync(QueryWindowRelation.CollectionLog, serverId, startUtc, endUtc)),
                $"[server {serverId}] Collection Log drill",
                asOfUtc);

        /// <summary>
        /// The drill's load over passed-in steps, so a test drives a probe that throws and records the window each step is given.
        /// The window is worked out HERE, once: the end from <paramref name="asOfUtc"/> or one reading of the clock, the start a
        /// week before it, and that pair goes to both steps, so the rows and the note cannot disagree about it. The probe goes
        /// through the guard the server tab's banners use (<see cref="ServerTab.ProbeWindowFloorOrNullAsync"/>): a probe that
        /// throws is logged and answers null, which shows no note and costs the grid none of its rows.
        /// </summary>
        internal static async Task<DrillRead> ReadDrillAsync(
            Func<DateTime, DateTime, Task<List<CollectionLogRow>>> readRuns,
            Func<DateTime, DateTime, Task<DateTime?>> probeFloor, string what, DateTime? asOfUtc = null)
        {
            var endUtc = asOfUtc ?? DateTime.UtcNow;
            var startUtc = endUtc.AddHours(-DrillHours);
            var rows = await readRuns(startUtc, endUtc);
            var floor = await ServerTab.ProbeWindowFloorOrNullAsync(() => probeFloor(startUtc, endUtc), what, startUtc, endUtc);
            return new DrillRead(rows, startUtc, floor);
        }

        /// <summary>
        /// Words the note on <paramref name="banner"/> from a load: the tab's banner rule
        /// (<see cref="ServerTab.ApplyWindowFloorToBanner"/>, with its 90-minute slack for a first collection that lands just
        /// after the window starts), in the zone this window's grid prints its times in (<c>CollectionHealthTime.Zone</c>, the
        /// window's server clock, else the active tab's). A floor at or before the start, a quiet start, a probe that failed and a
        /// week with no run all hide it. Runs on the UI thread, after the rows are bound.
        /// </summary>
        internal static void ShowDrillDataStart(TextBlock banner, DrillRead read, ServerClock? clock) =>
            ServerTab.ApplyWindowFloorToBanner(banner, read.Floor, read.StartUtc, CollectionHealthTime.Zone(clock));

        private async Task LoadCollectionLogAsync()
        {
            try
            {
                var load = await ReadDrillAsync(_dataService, _serverId, _collectorName);
                var logs = load.Rows;
                foreach (var log in logs) log.Clock = _serverClock;
                LogDataGrid.ItemsSource = logs;
                ShowDrillDataStart(CollectionLogDrillTruncatedBanner, load, _serverClock);

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
