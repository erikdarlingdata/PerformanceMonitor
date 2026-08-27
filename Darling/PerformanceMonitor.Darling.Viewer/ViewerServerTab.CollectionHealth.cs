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
using System.Text.Json;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Collection Health inner tab — a COPY of Lite's 3-sub-tab Collection Health surface
/// (ServerTab.xaml + the <c>RefreshCollectionHealthAsync</c> load, the
/// <c>CollectionHealthGrid_MouseDoubleClick</c> drill, and <c>UpdateCollectorDurationChart</c>), reads
/// rewired to Postgres. It REPLACES the shell's single latest-run-per-collector grid. Health Summary =
/// the 7-day per-collector aggregate (double-click opens the per-collector CollectionLogWindow drill);
/// Collection Log = the recent run log; Duration Trends = the per-collector success-duration scatter.
/// The chart's only render-body change from Lite is the time axis: where Lite shifts the raw stored
/// time by its per-server <c>UtcOffsetMinutes</c>, the viewer runs every point through
/// <see cref="ViewerTimeHelper.ForDisplay"/> (the naive-UTC-to-viewer-local convention every Darling
/// chart uses), and line polish flows through the shared <see cref="ChartStyle"/> like the other viewer
/// charts. Lite's per-chart context menu / "Open Log File" button are intentionally not ported.
/// </summary>
public partial class ViewerServerTab
{
    private ChartHoverHelper? _collectorDurationHover;

    /// <summary>
    /// Applies the shared chrome to the Duration Trends chart and wires its hover tooltip. Called from
    /// the constructor after <c>InitializeComponent</c> so it doesn't flash white before its first load,
    /// matching Lite's ServerTab (and the viewer's other chart inits).
    /// </summary>
    private void InitializeCollectionHealthChart()
    {
        ApplyTheme(CollectorDurationChart);
        CollectorDurationChart.Refresh();
        _collectorDurationHover = new ChartHoverHelper(CollectorDurationChart, "ms");
    }

    /// <summary>
    /// Collection Health tab load: the 7-day per-collector health aggregate and the recent collection
    /// log read concurrently (NpgsqlDataSource pools a connection for each), then each grid goes through
    /// its filter manager's UpdateData so active column filters survive the refresh, and the log also
    /// feeds the Duration Trends chart. Mirrors Lite's <c>RefreshCollectionHealthAsync</c> — but the
    /// reads are genuinely async, so there is no Task.Run wrap. LoadInnerTabAsync owns the try/catch that
    /// surfaces failures on the status bar.
    /// </summary>
    private async Task LoadHealthAsync()
    {
        /* Health Summary stays Lite's fixed 7-day per-collector rollup (its staleness banding needs a
           stable horizon regardless of the toolbar window). The Collection Log + Duration Trends honor the
           settable window EXACTLY — a preset or a custom From/To — via GetWindowUtc(), matching the Wait
           Stats / Blocking tabs (the old GetWindowHoursBack() rounded a custom range to a now-relative span). */
        var (startUtc, endUtc) = GetWindowUtc();
        var healthTask = _dataService.GetCollectionHealthAsync(_server.ServerId);
        var logTask = _dataService.GetRecentCollectionLogAsync(_server.ServerId, startUtc, endUtc);
        await Task.WhenAll(healthTask, logTask);

        _collectionHealthFilterMgr!.UpdateData(healthTask.Result);
        _collectionLogFilterMgr!.UpdateData(logTask.Result);
        RenderCollectorDurationChart(logTask.Result);
    }

    /// <summary>
    /// "Purge Now" (Collection Health): runs the daily retention purge on demand via the fleet-wide
    /// <c>purge_now</c> control command, after a confirm — it permanently deletes collected data older than the
    /// configured retention horizons across ALL monitored servers (the purge is fleet-wide over the shared
    /// store). A read-only viewer seat can't enqueue commands, so it shows an explanation instead (same rule as
    /// Pause / live-plan fetch). On success it shows the purged summary and reloads the tab so the grids/chart
    /// reflect the purge.
    /// </summary>
    private async void PurgeNow_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService.IsReadOnly)
        {
            MessageBox.Show(
                "Purging asks the service to run the retention purge, which it does by running a command — a " +
                "read-only viewer seat can't enqueue commands. The command is queued in the MONITORING STORE, " +
                "and the purge only ever deletes from the store — never from a monitored server. Reconnect " +
                "with a read-write store profile to purge.",
                "Read-Only Viewer", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        var confirm = MessageBox.Show(
            "Run the retention purge now?\n\n" +
            "This permanently deletes collected data older than the configured retention horizons across ALL " +
            "monitored servers (the purge is fleet-wide over the shared store). It is exactly what the service " +
            "does automatically once a day — running it now just does it immediately. This cannot be undone.",
            "Purge Now", MessageBoxButton.YesNo, MessageBoxImage.Warning);
        if (confirm != MessageBoxResult.Yes)
        {
            return;
        }

        PurgeNowButton.IsEnabled = false;
        PurgeNowIndicator.Text = "Purging...";
        try
        {
            var result = await _dataService.RequestPurgeNowAsync();
            if (result is null)
            {
                PurgeNowIndicator.Text = "Purge still running — re-open Collection Health to see the result";
            }
            else if (result.Status != ViewerDataService.StatusSucceeded)
            {
                PurgeNowIndicator.Text = $"Purge failed: {result.ResultStatus ?? "unknown error"}";
            }
            else
            {
                PurgeNowIndicator.Text = FormatPurgeSummary(result.ResultJson);
                /* Reflect the purge in the grids + duration chart. */
                await LoadHealthAsync();
            }
        }
        catch (ViewerReadOnlyException)
        {
            /* Grants changed under us (the enqueue already threw). */
            PurgeNowIndicator.Text = "Read-only viewer — cannot purge";
        }
        catch (Exception ex)
        {
            PurgeNowIndicator.Text = "";
            StatusChanged?.Invoke($"purge failed: {ex.Message}");
        }
        finally
        {
            PurgeNowButton.IsEnabled = true;
        }
    }

    /// <summary>
    /// Formats the <c>purge_now</c> result_json (<c>{ tablesPurged, rowsPurged, ... }</c>) into the one-line
    /// summary the indicator shows. Degrades to a plain "Purge complete" if the JSON is missing/unparseable.
    /// </summary>
    private static string FormatPurgeSummary(string? resultJson)
    {
        if (string.IsNullOrWhiteSpace(resultJson))
        {
            return "Purge complete";
        }

        try
        {
            using var doc = JsonDocument.Parse(resultJson);
            var root = doc.RootElement;
            var tables = root.TryGetProperty("tablesPurged", out var t) && t.TryGetInt32(out var ti) ? ti : 0;
            var rows = root.TryGetProperty("rowsPurged", out var r) && r.TryGetInt32(out var ri) ? ri : 0;
            return $"Purged {rows:N0} row(s)/chunk(s) across {tables:N0} table(s)";
        }
        catch (JsonException)
        {
            return "Purge complete";
        }
    }

    /// <summary>
    /// Double-click a Health Summary row to open that collector's full collection history. Copied from
    /// Lite's <c>CollectionHealthGrid_MouseDoubleClick</c>, repointed to the viewer's CollectionLogWindow.
    /// </summary>
    private void CollectionHealthGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
    {
        if (CollectionHealthGrid.SelectedItem is not CollectorHealthRow item) return;

        var window = new CollectionLogWindow(_dataService, _server.ServerId, item.CollectorName)
        {
            Owner = Window.GetWindow(this)
        };
        window.ShowDialog();
    }

    /// <summary>
    /// Per-collector success-duration scatter over the window. Copied from Lite's
    /// <c>UpdateCollectorDurationChart</c>: one line per collector (SUCCESS runs with a duration, needing
    /// at least two points), cycling the shared palette. The one change is the time axis — every point
    /// runs through <see cref="ViewerTimeHelper.ForDisplay"/> (Lite shifts by its per-server
    /// UtcOffsetMinutes) — and line polish uses the shared <see cref="ChartStyle.StyleScatter"/>.
    /// </summary>
    private void RenderCollectorDurationChart(List<CollectionLogRow> data)
    {
        ClearChart(CollectorDurationChart);
        ApplyTheme(CollectorDurationChart);

        /* Pin the X axis to the toolbar's settable window (the same idiom as the wait / tempdb-size charts)
           rather than AutoScale()'ing to the data — an AutoScale fits X to the data plus ScottPlot's ~10%
           side margins, which reads as symmetric dead space. This is the one chart the #1483/#1484/#1487
           window-pin campaign missed. The store is naive-UTC; display converts through ViewerTimeHelper.ForDisplay. */
        var (startUtc, endUtc) = GetWindowUtc();
        var rangeStart = ViewerTimeHelper.ForDisplay(startUtc).ToOADate();
        var rangeEnd = ViewerTimeHelper.ForDisplay(endUtc).ToOADate();

        if (data.Count == 0)
        {
            CollectorDurationChart.Plot.Axes.DateTimeTicksBottomDateChange();
            CollectorDurationChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
            ReapplyAxisColors(CollectorDurationChart);
            CollectorDurationChart.Refresh();
            return;
        }

        /* Group by collector, plot each as a separate series */
        var groups = data
            .Where(d => d.DurationMs.HasValue && d.Status == "SUCCESS")
            .GroupBy(d => d.CollectorName)
            .OrderBy(g => g.Key)
            .ToList();

        _collectorDurationHover?.Clear();
        int colorIdx = 0;
        foreach (var group in groups)
        {
            var points = group.OrderBy(d => d.CollectionTime).ToList();
            if (points.Count < 2) continue;

            var times = points.Select(d => ViewerTimeHelper.ForDisplay(d.CollectionTime).ToOADate()).ToArray();
            var durations = points.Select(d => (double)d.DurationMs!.Value).ToArray();

            var scatter = CollectorDurationChart.Plot.Add.TimeSeries(times, durations);
            scatter.LegendText = group.Key;
            scatter.Color = ScottPlot.Color.FromHex(SeriesColors[colorIdx % SeriesColors.Length]);
            ChartStyle.StyleScatter(scatter);
            _collectorDurationHover?.Add(scatter, group.Key);
            colorIdx++;
        }

        CollectorDurationChart.Plot.Axes.DateTimeTicksBottomDateChange();
        ReapplyAxisColors(CollectorDurationChart);
        CollectorDurationChart.Plot.YLabel("Duration (ms)");
        CollectorDurationChart.Plot.Axes.AutoScaleY();
        CollectorDurationChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        ShowChartLegend(CollectorDurationChart);
        CollectorDurationChart.Refresh();
    }

    /// <summary>Tears down the Duration Trends hover helper. Forwarded to from the tab's single Dispose().</summary>
    private void DisposeCollectionHealthHelpers()
    {
        _collectorDurationHover?.Dispose();
    }
}
