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
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Collection Health inner tab — a COPY of Lite's 3-sub-tab Collection Health surface
/// (ServerTab.xaml + the <c>RefreshCollectionHealthAsync</c> load, the
/// <c>CollectionHealthGrid_MouseDoubleClick</c> drill, and <c>UpdateCollectorDurationChart</c>), reads
/// rewired to Postgres. It REPLACES the shell's single latest-run-per-collector grid. Health Summary =
/// the 7-day per-collector aggregate (double-click opens the per-collector CollectionLogWindow drill);
/// Collection Log = the recent run log (the newest <see cref="ViewerDataService.CollectionLogRowCap"/> runs of the range);
/// Duration Trends = the per-collector success-duration lines, drawn from their own bucketed read over the whole range (#4966), not
/// from the grid's page: a page of the newest 500 runs ends wherever the 500th falls, and the chart's axis spans the range.
/// The chart's only render-body change from Lite is the time axis: where Lite shifts the raw stored
/// time by its per-server <c>UtcOffsetMinutes</c>, the viewer plots every point at its naive-UTC instant
/// and draws the labels in <see cref="ViewerTimeHelper.CurrentDisplayZone"/> (the convention every Darling
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
        _collectorDurationHover = new ChartHoverHelper(CollectorDurationChart, "ms", displayZone: ViewerTimeHelper.CurrentDisplayZone);
    }

    /// <summary>
    /// Collection Health tab load: the 7-day per-collector health aggregate, the recent collection
    /// log and the Duration Trends chart's own bucketed read (#4966) concurrently (NpgsqlDataSource pools a
    /// connection for each), then each grid goes through
    /// its filter manager's UpdateData so active column filters survive the refresh, and the chart draws its read's
    /// buckets over the whole range rather than the log's page. Mirrors Lite's <c>RefreshCollectionHealthAsync</c> — but the
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
        using var readFanOut = ViewerReadFanOut.Of(5);
        var healthTask = _dataService.GetCollectionHealthAsync(_server.ServerId);
        var dataStartTask = _dataService.GetCollectionLogDataStartAsync(_server.ServerId, startUtc, endUtc);
        var logTask = _dataService.GetRecentCollectionLogAsync(_server.ServerId, startUtc, endUtc);
        /* The chart's own read (#4966): the grid's page ends at the 500th newest run, and this chart's axis spans the range. */
        var durationTask = _dataService.GetCollectorDurationTrendAsync(_server.ServerId, startUtc, endUtc);
        var caveatsTask = _dataService.GetCollectionCaveatsAsync(_server.ServerId);
        await Task.WhenAll(healthTask, logTask, durationTask, caveatsTask);

        /* The four reads are done: end the declared width here, before the data-start note below awaits its probe (#4966), so that
           await is not priced against contention that has already finished. */
        readFanOut.Release();

        _collectionHealthFilterMgr!.UpdateData(healthTask.Result);
        _collectionLogFilterMgr!.UpdateData(logTask.Result);
        await ShowCollectionLogDataStartAsync(CollectionLogTruncationBanner, dataStartTask, startUtc, logTask.Result);
        RenderCollectorDurationChart(durationTask.Result);

        /* #3691 part a2: collapse the section entirely when there is nothing to report — the common case
           (a healthy analysis pass, or a store below V141) — rather than showing an empty grid. */
        var caveats = caveatsTask.Result;
        CollectionCaveatsGrid.ItemsSource = caveats;
        CollectionCaveatsExpander.Visibility = caveats.Count > 0 ? Visibility.Visible : Visibility.Collapsed;
    }

    /// <summary>
    /// The Collection Log grid's "Showing since" note (#4966): the grid says where the log starts when the range reaches before it.
    /// The read keeps the newest <see cref="ViewerDataService.CollectionLogRowCap"/> runs, so a full page names its oldest run,
    /// whatever the store covers, with no slack; a page under the cap names the earlier of the probe's answer (the later of the
    /// server's first collection and the log's retention edge, never a collector's first run) and its earliest run. A step of its
    /// own so a test drives the tab's banner call on a real store.
    /// </summary>
    /// <param name="banner">The grid's banner.</param>
    /// <param name="probe">The data-start probe the tab started beside its read.</param>
    /// <param name="startUtc">The start of the range the grid just drew.</param>
    /// <param name="shown">The runs the read returned.</param>
    internal static Task ShowCollectionLogDataStartAsync(TextBlock banner, Task<DateTime?> probe, DateTime startUtc, IEnumerable<CollectionLogRow> shown) =>
        ShowEventDataStartAsync(banner, probe, "Collection Log", startUtc, shown.Select(r => (DateTime?)r.CollectionTime), ViewerDataService.CollectionLogRowCap);

    /// <summary>
    /// "Purge Now" (Collection Health): runs the daily retention purge on demand via the fleet-wide
    /// <c>purge_now</c> control command, after a confirm — it permanently deletes collected data older than the
    /// configured retention horizons across ALL monitored servers (the purge is fleet-wide over the shared
    /// store). A read-only viewer seat can't enqueue commands, so it shows an explanation instead (same rule as
    /// Pause / live-plan fetch). #4825: a current service starts the purge in the background, paced, and answers
    /// at once, with the time it started; this then watches the collection log for the purge's totals
    /// (<see cref="StartPurgeWatch"/>) and reloads the tab when they turn up. "Already running" shows as it is and
    /// starts no watch. An older service still runs the purge inline and answers with its totals, which are shown,
    /// and the tab is reloaded so the grids/chart reflect the purge.
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
                PurgeNowIndicator.Text = "The service has not answered yet — the purge may not have started. Try again in a moment";
            }
            else if (result.Status != ViewerDataService.StatusSucceeded)
            {
                PurgeNowIndicator.Text = $"Purge failed: {result.ResultStatus ?? "unknown error"}";
            }
            else
            {
                PurgeNowIndicator.Text = FormatPurgeSummary(result.ResultJson, out var purgeFinished);
                if (purgeFinished)
                {
                    /* An older service ran the whole purge before answering: reflect it in the grids + chart. */
                    await LoadHealthAsync();
                }
                else if (PurgeNowWatch.TryReadStartedAtUtc(result.ResultJson, out var startedAtUtc))
                {
                    /* #4825: the purge runs in the background; its totals reach the collection log when it ends. */
                    StartPurgeWatch(startedAtUtc);
                }
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

    /* #4825: the watch on a purge the service started in the background (PurgeNowWatch). At most one runs at a
       time; the tab closing or unloading ends it. */
    private CancellationTokenSource? _purgeWatchCts;

    private const string PurgeStartedText =
        "Purge started. It runs in the background, paced; when it finishes, its totals are written to the collection log under (fleet)";

    /// <summary>
    /// Starts watching the collection log for the totals of the purge the service started at
    /// <paramref name="startedAtUtc"/>. Never two at once: a purge started while an earlier one is still being
    /// watched (the earlier one's wait for its raw-table record can outlive the purge itself) takes the indicator
    /// over, so the earlier watch is cancelled first and, having been cancelled, writes nothing more. The
    /// watch ends when the tab is closed (<see cref="DisposeCollectionHealthHelpers"/>) or unloaded
    /// (<see cref="OnPurgeWatchTabUnloaded"/>).
    /// </summary>
    private void StartPurgeWatch(DateTime startedAtUtc)
    {
        _purgeWatchCts?.Cancel();
        var cts = new CancellationTokenSource();
        _purgeWatchCts = cts;

        Unloaded -= OnPurgeWatchTabUnloaded;
        Unloaded += OnPurgeWatchTabUnloaded;

        _ = RunPurgeWatchAsync(startedAtUtc, cts);
    }

    /// <summary>
    /// Runs <see cref="PurgeNowWatch.WatchAsync"/> against the real read, delay and clock, and owns its
    /// <paramref name="cts"/>. The viewer's clock is only used for how long the watch has been going; the read's
    /// lower bound is the service's own <paramref name="startedAtUtc"/>. Nothing awaits this task, so nothing is
    /// allowed to escape it.
    /// </summary>
    private async Task RunPurgeWatchAsync(DateTime startedAtUtc, CancellationTokenSource cts)
    {
        try
        {
            await PurgeNowWatch.WatchAsync(
                startedAtUtc,
                (since, token) => _dataService.GetManualPurgeRunRecordsAsync(since, token),
                (span, token) => Task.Delay(span, token),
                () => DateTime.UtcNow,
                text => PurgeNowIndicator.Text = text,
                async () =>
                {
                    try
                    {
                        await LoadHealthAsync();
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        StatusChanged?.Invoke($"reloading Collection Health after the purge failed: {ex.Message}");
                    }
                },
                cts.Token);
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"watching the purge failed: {ex.Message}");
        }
        finally
        {
            if (ReferenceEquals(_purgeWatchCts, cts))
            {
                _purgeWatchCts = null;
            }

            cts.Dispose();
        }
    }

    /// <summary>
    /// The tab left the visual tree (another server's tab was selected, or it was closed): stop watching. The
    /// indicator goes back to the plain "started" line, which stays true wherever the run has got to.
    /// </summary>
    private void OnPurgeWatchTabUnloaded(object sender, RoutedEventArgs e)
    {
        if (_purgeWatchCts is null)
        {
            return;
        }

        _purgeWatchCts.Cancel();
        PurgeNowIndicator.Text = PurgeStartedText;
    }

    /// <summary>
    /// Formats the <c>purge_now</c> result_json into the one-line summary the indicator shows. #4825: a current
    /// service answers <c>{ started: true }</c> (the purge is running in the background, so
    /// <paramref name="purgeFinished"/> is false) or <c>{ started: false, alreadyRunning: true }</c>; an older
    /// service answers the totals, <c>{ tablesPurged, rowsPurged, ... }</c>, once the purge is done. Degrades to a
    /// plain "Purge complete" if the JSON is missing/unparseable.
    /// </summary>
    internal static string FormatPurgeSummary(string? resultJson, out bool purgeFinished)
    {
        purgeFinished = true;
        if (string.IsNullOrWhiteSpace(resultJson))
        {
            return "Purge complete";
        }

        try
        {
            using var doc = JsonDocument.Parse(resultJson);
            var root = doc.RootElement;
            if (root.TryGetProperty("alreadyRunning", out var running) && running.ValueKind == JsonValueKind.True)
            {
                purgeFinished = false;
                return "A purge is already running in the background; nothing new was started";
            }

            if (root.TryGetProperty("started", out var started) && started.ValueKind == JsonValueKind.True)
            {
                purgeFinished = false;
                return PurgeStartedText;
            }

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
    /// Per-collector success-duration lines over the window. Copied from Lite's
    /// <c>UpdateCollectorDurationChart</c>: one line per collector (SUCCESS runs with a duration, needing
    /// at least two points), cycling the shared palette. Two changes. The time axis: every point's X
    /// is the naive-UTC instant itself, drawn in <see cref="ViewerTimeHelper.CurrentDisplayZone"/> (Lite
    /// shifts by its per-server UtcOffsetMinutes) — and line polish uses the shared <see cref="ChartStyle.StyleScatter"/>.
    /// And the feed (#4966): the buckets of <see cref="ViewerDataService.GetCollectorDurationTrendAsync"/> over the whole range,
    /// not the Collection Log grid's page of the newest runs, so the lines reach across the axis the range pins. A point is a
    /// bucket and draws its MAXIMUM, so a slow run still shows however wide the bucket is (a minute for 24 hours, ten for 7 days);
    /// the shared hover popup prints a label, a value and a time, so a point's hover names the collector, that maximum and the
    /// bucket's start, and the bucket's average and run count (<see cref="CollectorDurationSeries"/> carries them) are not drawn.
    /// </summary>
    private void RenderCollectorDurationChart(List<CollectorDurationBucket> data)
    {
        ClearChart(CollectorDurationChart);
        ApplyTheme(CollectorDurationChart);

        /* The old lines are off the plot: drop them from the hover as well, or an empty range would still tooltip their points. */
        _collectorDurationHover?.Clear();

        /* Pin the X axis to the toolbar's settable window (the same idiom as the wait / tempdb-size charts)
           rather than AutoScale()'ing to the data — an AutoScale fits X to the data plus ScottPlot's ~10%
           side margins, which reads as symmetric dead space. This is the one chart the #1483/#1484/#1487
           window-pin campaign missed. The store is naive-UTC and the chart plots it as is; the labels are drawn in ViewerTimeHelper.CurrentDisplayZone. */
        var (startUtc, endUtc) = GetWindowUtc();
        var rangeStart = startUtc.ToOADate();
        var rangeEnd = endUtc.ToOADate();

        if (data.Count == 0)
        {
            CollectorDurationChart.Plot.Axes.DateTimeTicksBottomUtc(ViewerTimeHelper.CurrentDisplayZone);
            CollectorDurationChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
            ReapplyAxisColors(CollectorDurationChart);
            CollectorDurationChart.Refresh();
            return;
        }

        /* Group by collector, plot each as a separate series: its bucket maxima, in collector then time order. */
        int colorIdx = 0;
        foreach (var series in CollectorDurationSeries.Build(data))
        {
            var scatter = CollectorDurationChart.Plot.Add.TimeSeries(series.Times, series.MaxMs);
            scatter.LegendText = series.Collector;
            scatter.Color = ScottPlot.Color.FromHex(SeriesColors[colorIdx % SeriesColors.Length]);
            ChartStyle.StyleScatter(scatter);
            _collectorDurationHover?.Add(scatter, series.Collector);
            colorIdx++;
        }

        CollectorDurationChart.Plot.Axes.DateTimeTicksBottomUtc(ViewerTimeHelper.CurrentDisplayZone);
        ReapplyAxisColors(CollectorDurationChart);
        CollectorDurationChart.Plot.YLabel("Slowest run per bucket (ms)");
        CollectorDurationChart.Plot.Axes.AutoScaleY();
        CollectorDurationChart.Plot.Axes.SetLimitsX(rangeStart, rangeEnd);
        ShowChartLegend(CollectorDurationChart);
        CollectorDurationChart.Refresh();
    }

    /// <summary>Tears down the Duration Trends hover helper and ends any purge watch. Forwarded to from the tab's single Dispose().</summary>
    private void DisposeCollectionHealthHelpers()
    {
        _collectorDurationHover?.Dispose();

        /* #4825: a closed tab must not keep polling. RunPurgeWatchAsync disposes the source when the watch ends. */
        Unloaded -= OnPurgeWatchTabUnloaded;
        _purgeWatchCts?.Cancel();
    }
}
