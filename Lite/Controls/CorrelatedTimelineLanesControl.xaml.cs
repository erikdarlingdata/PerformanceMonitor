/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 *
 * SYNC WARNING: Dashboard has a matching copy at Dashboard/Controls/CorrelatedTimelineLanesControl.xaml.cs.
 * Changes here must be mirrored there.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Controls;

public partial class CorrelatedTimelineLanesControl : UserControl
{
    private LocalDataService? _dataService;
    private int _serverId;
    private CorrelatedCrosshairManager? _crosshairManager;
    private bool _isRefreshing;

    /// <summary>
    /// The zone the tick labels and the crosshair word an instant in (#4766). A provider rather than a zone, read on
    /// every render, so a display-mode switch relabels the lanes and moves no point. <see cref="Initialize"/> sets it.
    /// </summary>
    private Func<TimeZoneInfo> _displayZone = () => TimeZoneInfo.Utc;

    public CorrelatedTimelineLanesControl()
    {
        InitializeComponent();
        /* #8104: the lanes grid's Height follows the scroll viewer's viewport, and WPF clamps an explicit Height only by the
           element's own MinHeight, whatever its rows ask for. The floor is therefore the rows' total, the rule the shared
           ChartLayout guard applies to the chart tabs' grids, so the scroll viewer's extent holds every lane and scrolls when
           the viewport is shorter, instead of clipping the last lane under the window with no scrollbar. */
        LanesGrid.MinHeight = LanesGrid.RowDefinitions.Sum(r => r.MinHeight);
        BackCaptions();
        /* No Unloaded → Dispose() handler: WPF fires Unloaded for transient
           reasons (tab virtualization, layout rebuilds) and Dispose() clears
           the crosshair manager's lane list, permanently breaking the crosshair
           until the ServerTab is rebuilt. The manager holds only managed state
           (a Popup + lane references) — letting GC clean it up with the control
           is fine. */
    }

    /// <summary>
    /// Initializes the control with the data service, the server ID and the zone the lanes' text is worded in (#4766):
    /// every lane plots the naive-UTC instant, and <paramref name="displayZone"/> is applied only to the tick labels
    /// and the crosshair time. Must be called before RefreshAsync.
    /// </summary>
    public void Initialize(LocalDataService dataService, int serverId, Func<TimeZoneInfo> displayZone)
    {
        _dataService = dataService;
        _serverId = serverId;
        _displayZone = displayZone;

        var charts = new[] { CpuChart, WaitStatsChart, BlockingChart, MemoryChart, FileIoChart };
        var laneNames = new[] { "SQL CPU", "Wait Stats", "Blocking", "Buffer Pool", "I/O Latency" };
        var emptyLook = new ChartRenderHelper();
        for (var lane = 0; lane < charts.Length; lane++)
        {
            var chart = charts[lane];
            ApplyTheme(chart);
            /* #8086: until the first read comes back a lane shows the empty chart's sentence, not ScottPlot's default -10 to 10
               axes. The first update resets the chart, which drops this. */
            emptyLook.RefreshEmptyChart(chart, laneNames[lane], "");
            // Disable zoom/pan/drag but keep mouse events for crosshair
            chart.UserInputProcessor.UserActionResponses.Clear();
            SetupLaneDrillDown(chart);
        }

        /* The zone is set before the first lane is added: the manager words its tooltip time from it on every mouse
           move, and without one it would read X as the app's server-time value. */
        _crosshairManager = new CorrelatedCrosshairManager { DisplayZoneProvider = displayZone };
        _crosshairManager.AddLane(CpuChart, "SQL CPU", "%");
        _crosshairManager.AddLane(WaitStatsChart, "Wait Stats", "ms/sec");
        _crosshairManager.AddLane(BlockingChart, "Blocking", PerformanceMonitor.Ui.BlockingTrendLabels.ReportsUnit);
        _crosshairManager.AddLane(MemoryChart, "Buffer Pool", "MB");
        _crosshairManager.AddLane(FileIoChart, "I/O Latency", "ms");
    }

    /// <summary>
    /// Raised when the user picks "Show Active Queries at This Time" on a lane. The argument is the clicked instant,
    /// naive UTC (a lane's X is the instant, #4766); the host navigates to Active Queries.
    /// </summary>
    public event Action<DateTime>? ShowActiveQueriesRequested;

    /// <summary>
    /// #4766: the instant a click on a lane means. A lane's X is the naive-UTC instant, so the clicked X is that
    /// instant as it stands: no conversion, in any display mode. An empty lane sets X to [-1, 1], which is 1899.
    /// </summary>
    internal static DateTime DrillInstant(double chartX) => DateTime.FromOADate(chartX);

    /// <summary>
    /// Adds a minimal right-click menu (just the Active Queries drill-down) to a lane. The lanes are a
    /// stripped-chrome view with pan/zoom disabled, so the clicked time is read straight from the X axis.
    /// </summary>
    private void SetupLaneDrillDown(ScottPlot.WPF.WpfPlot chart)
    {
        var menu = new ContextMenu();
        var item = new MenuItem { Header = "Show _Active Queries at This Time" };
        menu.Items.Add(item);

        menu.Opened += (s, _) =>
        {
            try
            {
                var pos = System.Windows.Input.Mouse.GetPosition(chart);
                var dpi = System.Windows.Media.VisualTreeHelper.GetDpi(chart);
                var pixel = new ScottPlot.Pixel((float)(pos.X * dpi.DpiScaleX), (float)(pos.Y * dpi.DpiScaleY));
                var t = DrillInstant(chart.Plot.GetCoordinates(pixel).X);
                // Empty-state lanes set the X axis to [-1, 1] (~year 1899); only offer the drill-down
                // when the click resolves to a real timestamp.
                bool valid = t.Year >= 2000;
                item.Tag = valid ? t : (DateTime?)null;
                item.IsEnabled = valid;
            }
            catch
            {
                item.Tag = null;
                item.IsEnabled = false;
            }
        };

        item.Click += (s, _) =>
        {
            if (item.Tag is DateTime t)
                ShowActiveQueriesRequested?.Invoke(t);
        };

        chart.PreviewMouseRightButtonDown += (s, e) =>
        {
            e.Handled = true;
            menu.PlacementTarget = chart;
            menu.Placement = System.Windows.Controls.Primitives.PlacementMode.MousePoint;
            menu.IsOpen = true;
        };
    }

    /// <summary>
    /// #4296, #4766: RefreshOverviewAsync's comparison window for the Overview tab's ghost-line overlay, as UTC
    /// instants, and the number of whole days the ghost lines are moved by (1 for Yesterday, 7 for Last week and
    /// Same day last week; null for "no comparison"). NOT ServerTab.Comparison.cs's GetComparisonRange, which serves
    /// the Queries tab's own comparison reads. The current window is what the axis spans
    /// (<see cref="TimeWindows.ChartAxis"/>), and the comparison is "the same wall-clock hours N days earlier" on the
    /// MONITORED server's clock, not in the display zone (<see cref="TimeWindows.OverviewReference"/>): 09:00 to
    /// 13:00 today reads as 09:00 to 13:00 yesterday even when a clock change makes that 23 or 25 real hours
    /// earlier. The reads take the bounds as they are, and a ghost row is put back on the current axis by the
    /// same rule (<see cref="TimeWindows.GhostX"/>), so the day count is all the caller needs to carry.
    /// utcNow/serverClock are explicit parameters so a test can drive this on either side of a change.
    /// </summary>
    internal static (DateTime FromUtc, DateTime ToUtc, int Days)? GetOverviewComparisonRange(
        int selectedIndex, int hoursBack, DateTime? fromDate, DateTime? toDate, DateTime utcNow, ServerClock serverClock)
    {
        var days = selectedIndex switch
        {
            1 => 1,   // Yesterday
            2 => 7,   // Last week
            3 => 7,   // Same day last week
            _ => 0
        };

        if (days == 0)
        {
            return null;
        }

        var (currentFromUtc, currentToUtc) = TimeWindows.ChartAxis(hoursBack, fromDate, toDate, utcNow);
        var (refFromUtc, refToUtc) = TimeWindows.OverviewReference(currentFromUtc, currentToUtc, days, serverClock.AsTimeZone());
        return (refFromUtc, refToUtc, days);
    }

    /// <summary>
    /// Refreshes all lane data for the given time range. <paramref name="serverClock"/> is the clock of the server this
    /// tab monitors (the tab's own, whichever tab is selected): it defines "the same hours" the ghost lines are
    /// moved by (<see cref="TimeWindows.GhostX"/>).
    ///
    /// <para>A read that fails costs its lane, which is drawn as failed (<see cref="ShowFailed"/>), and the other lanes draw as
    /// usual. The return value names the reads that failed (<see cref="OverviewLaneReadFailures.Summary"/>), so the host does not
    /// call the tab refreshed; null when all ten completed.</para>
    /// </summary>
    public async Task<string?> RefreshAsync(int hoursBack, DateTime? fromDate, DateTime? toDate, ServerClock serverClock,
        (DateTime FromUtc, DateTime ToUtc, int Days)? comparisonRange = null)
    {
        if (_dataService == null || _isRefreshing) return null;
        _isRefreshing = true;

        try
        {
            _crosshairManager?.PrepareForRefresh();
            ComparisonEmptyBanner.Visibility = Visibility.Collapsed;

            /* #4966: the UTC window the blocking and deadlock reads below take (their own GetTimeRange call), for the data-start note. */
            var (windowStartUtc, windowEndUtc) = LocalDataService.GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null);

            var cpuTask = Task.Run(() => _dataService.GetCpuUtilizationAsync(_serverId, hoursBack, fromDate, toDate, frame: CpuTimeFrame.Utc));
            var waitTask = Task.Run(() => _dataService.GetTotalWaitTrendAsync(_serverId, hoursBack, fromDate, toDate));
            var blockingTask = Task.Run(() => _dataService.GetBlockingTrendAsync(_serverId, hoursBack, fromDate, toDate));
            var deadlockTask = Task.Run(() => _dataService.GetDeadlockTrendAsync(_serverId, hoursBack, fromDate, toDate));
            var memoryTask = Task.Run(() => _dataService.GetMemoryTrendAsync(_serverId, hoursBack, fromDate, toDate));
            var fileIoTask = Task.Run(() => _dataService.GetFileIoOverviewLatencyTrendAsync(_serverId, hoursBack, fromDate, toDate));

            // Fetch baselines for band rendering — chart-unit-matched metrics, computed at now whatever window is shown (#7644),
            // as the Viewer's and the web page's are.
            /* #7644: a band per local hour of the axis window, each hour from its own hour-of-week bucket. The I/O lane reads the chart-unit arm
               (reads-weighted per collection), the unit of the line it sits behind, not the per-file arm the anomaly detector reads. */
            var (axisFromUtc, axisToUtc) = GetXAxisWindow(hoursBack, fromDate, toDate, DateTime.UtcNow);
            Task<IReadOnlyList<PerformanceMonitor.Ui.BaselineBand.HourStep>> BandsFor(string metric) => Task.Run(() =>
                _dataService.GetBaselineStepsForLaneAsync(_serverId, metric, axisFromUtc, axisToUtc));
            var cpuBaselineTask = BandsFor(MetricNames.Cpu);
            var waitBaselineTask = BandsFor(MetricNames.WaitMsPerSec);
            var ioBaselineTask = BandsFor(MetricNames.IoLatencyChart);
            var blockingBaselineTask = BandsFor(MetricNames.BlockingPerMinute);

            try
            {
                await Task.WhenAll(cpuTask, waitTask, blockingTask, deadlockTask, memoryTask, fileIoTask,
                    cpuBaselineTask, waitBaselineTask, ioBaselineTask, blockingBaselineTask);
            }
            catch (Exception ex)
            {
                AppLogger.Info("CorrelatedLanes", $"Data fetch failed: {ex.Message}");
            }

            var cpuBaseline = cpuBaselineTask.IsCompletedSuccessfully ? cpuBaselineTask.Result : null;
            var waitBaseline = waitBaselineTask.IsCompletedSuccessfully ? waitBaselineTask.Result : null;
            var ioBaseline = ioBaselineTask.IsCompletedSuccessfully ? ioBaselineTask.Result : null;
            var blockingBaseline = blockingBaselineTask.IsCompletedSuccessfully ? blockingBaselineTask.Result : null;

            /* #4766: every lane X below is the sample's own instant, naive UTC, with no conversion. The ticks and the
               crosshair word it in the display zone (DateTimeTicksBottomUtc, DisplayZoneProvider), so a switch of
               display mode relabels the lanes and moves no point, and a sample from before a daylight saving change
               sits where it happened. The CPU read asks for the UTC frame so each point carries its own instant
               (SampleTimeUtc): the two readings of a repeated hour are two points, not one. */

            // minAnomalyValue: absolute floor below which dots/arrows are suppressed even if outside band.
            // Prevents "1% CPU above 0.5% baseline" false alarms on idle servers.
            if (cpuTask.IsCompletedSuccessfully)
            {
                var sqlSeries = cpuTask.Result.Select(d => (d.SampleTimeUtc.ToOADate(), (double)d.SqlServerCpu)).ToList();
                /* #7682: a bucket without other-process CPU (SQL Server on Linux before 2025 CU1) has no total. Plot the
                   buckets that have one; with none, the SQL line is all that is known. */
                var measuredTotals = cpuTask.Result.Where(d => d.TotalCpu.HasValue).ToList();
                var totalSeries = measuredTotals.Count == 0
                    ? sqlSeries
                    : measuredTotals.Select(d => (d.SampleTimeUtc.ToOADate(), (double)d.TotalCpu!.Value)).ToList();
                UpdateCpuLane(sqlSeries, totalSeries, cpuBaseline);
            }
            else
                ShowFailed(CpuChart, "CPU %");

            if (waitTask.IsCompletedSuccessfully)
                UpdateLane(WaitStatsChart, "Wait ms/sec",
                    waitTask.Result.Select(d => (d.CollectionTime.ToOADate(), d.WaitTimeMsPerSecond)).ToList(),
                    "#FFB74D", bands: waitBaseline, minAnomalyValue: 100);
            else
                ShowFailed(WaitStatsChart, "Wait ms/sec");

            {
                var blockingData = blockingTask.IsCompletedSuccessfully
                    ? blockingTask.Result.Select(d => (d.Time.ToOADate(), (double)d.Count)).ToList()
                    : new List<(double, double)>();
                var deadlockData = deadlockTask.IsCompletedSuccessfully
                    ? deadlockTask.Result.Select(d => (d.Time.ToOADate(), (double)d.Count)).ToList()
                    : new List<(double, double)>();
                var blockingUnit = blockingTask.IsCompletedSuccessfully
                    ? PerformanceMonitor.Ui.BlockingTrendLabels.UnitFor(blockingTask.Result.Select(d => d.Source))
                    : PerformanceMonitor.Ui.BlockingTrendLabels.ReportsUnit;
                UpdateBlockingLane(blockingData, deadlockData, blockingBaseline, (windowEndUtc - windowStartUtc).TotalDays, blockingUnit);
                /* The lane draws the series that read; a series that did not is not "no events", so the lane says so. */
                if (!blockingTask.IsCompletedSuccessfully || !deadlockTask.IsCompletedSuccessfully)
                    MarkFailed(BlockingChart);
            }

            if (memoryTask.IsCompletedSuccessfully)
                UpdateLane(MemoryChart, "Buffer Pool MB",
                    memoryTask.Result.Select(d => (d.CollectionTime.ToOADate(), d.BufferPoolMb)).ToList(),
                    "#CE93D8");
            else
                ShowFailed(MemoryChart, "Memory MB");

            if (fileIoTask.IsCompletedSuccessfully)
            {
                /* #6970: the read already returns one weighted point per time, not one per file. */
                var ioGrouped = fileIoTask.Result
                    .Select(d => (d.CollectionTime.ToOADate(), d.ReadLatencyMs))
                    .ToList();
                /* #7624: the lane marks a point only when its reads reach the web strip's bar for the span they were counted over. */
                var ioPoints = fileIoTask.Result;
                var (ioReads, ioMinReads) = OverviewIoReadFloor.LaneInputs(
                    ioPoints.Select(d => (d.CollectionTime, d.Reads, d.BucketMinutes)).ToList(),
                    PgTargetScorer.IoBaselineBucketMinimumReads);
                UpdateLane(FileIoChart, "I/O ms", ioGrouped, "#81C784", bands: ioBaseline, minAnomalyValue: 2,
                    pointReads: ioReads, minPointReads: ioMinReads);
            }
            else
                ShowFailed(FileIoChart, "I/O ms");

            // Comparison overlay — fetch reference period data and render as ghost lines
            if (comparisonRange.HasValue)
            {
                /* The comparison window arrives as UTC instants (#4766): "the same wall-clock hours N days earlier" on
                   the server's clock, worked out once by GetOverviewComparisonRange, so the reads take it as it is.
                   Each ghost row goes back onto the current axis by the same rule in reverse (TimeWindows.GhostX),
                   row by row on the server's clock, so a row from before a clock change lands on the wall hour it was
                   collected at and not a fixed number of real hours away. */
                var refFromUtc = comparisonRange.Value.FromUtc;
                var refToUtc = comparisonRange.Value.ToUtc;
                var days = comparisonRange.Value.Days;
                var zone = serverClock.AsTimeZone();

                var refCpuTask = Task.Run(() => _dataService.GetCpuUtilizationAsync(_serverId, 0, refFromUtc, refToUtc, frame: CpuTimeFrame.Utc));
                var refWaitTask = Task.Run(() => _dataService.GetTotalWaitTrendAsync(_serverId, 0, refFromUtc, refToUtc));
                var refBlockingTask = Task.Run(() => _dataService.GetBlockingTrendAsync(_serverId, 0, refFromUtc, refToUtc));
                var refMemoryTask = Task.Run(() => _dataService.GetMemoryTrendAsync(_serverId, 0, refFromUtc, refToUtc));
                var refIoTask = Task.Run(() => _dataService.GetFileIoOverviewLatencyTrendAsync(_serverId, 0, refFromUtc, refToUtc));

                try { await Task.WhenAll(refCpuTask, refWaitTask, refBlockingTask, refMemoryTask, refIoTask); }
                catch (Exception ex) { AppLogger.Info("CorrelatedLanes", $"Comparison fetch failed: {ex.Message}"); }

                /* A read that failed has no Result (reading it rethrows), and the failure is already logged above. */
                AppLogger.Info("CorrelatedLanes",
                    $"Comparison: refFrom={refFromUtc:o}, refTo={refToUtc:o}, days={days}, " +
                    $"cpuRows={(refCpuTask.IsCompletedSuccessfully ? refCpuTask.Result?.Count ?? 0 : 0)}, " +
                    $"waitRows={(refWaitTask.IsCompletedSuccessfully ? refWaitTask.Result?.Count ?? 0 : 0)}");

                if (refCpuTask.IsCompletedSuccessfully && refCpuTask.Result != null)
                    AddGhostLine(CpuChart, refCpuTask.Result.Where(d => d.TotalCpu.HasValue)
                        .Select(d => (TimeWindows.GhostX(d.SampleTimeUtc, days, zone).ToOADate(), (double)d.TotalCpu!.Value)).ToList(), "#FF7043");

                if (refWaitTask.IsCompletedSuccessfully && refWaitTask.Result != null)
                    AddGhostLine(WaitStatsChart, refWaitTask.Result
                        .Select(d => (TimeWindows.GhostX(d.CollectionTime, days, zone).ToOADate(), d.WaitTimeMsPerSecond)).ToList(), "#FFB74D");

                if (refBlockingTask.IsCompletedSuccessfully && refBlockingTask.Result != null)
                {
                    var refBlocking = refBlockingTask.Result
                        .Select(d => (TimeWindows.GhostX(d.Time, days, zone).ToOADate(), (double)d.Count)).ToList();
                    if (refBlocking.Count > 0)
                        AddGhostLine(BlockingChart, refBlocking, "#E57373");
                }

                if (refMemoryTask.IsCompletedSuccessfully && refMemoryTask.Result != null)
                    AddGhostLine(MemoryChart, refMemoryTask.Result
                        .Select(d => (TimeWindows.GhostX(d.CollectionTime, days, zone).ToOADate(), d.BufferPoolMb)).ToList(), "#CE93D8");

                if (refIoTask.IsCompletedSuccessfully && refIoTask.Result != null)
                {
                    var refIo = refIoTask.Result
                        .Select(d => (TimeWindows.GhostX(d.CollectionTime, days, zone).ToOADate(), d.ReadLatencyMs))
                        .ToList();
                    AddGhostLine(FileIoChart, refIo, "#81C784");
                }

                /* Release walk V12d: a comparison window the store holds nothing for (a store newer than the offset) drew no
                   line and said nothing. When every read came back, and every one was empty, the chart says so. A read that
                   failed is logged above and is not "no data". */
                if (refCpuTask.IsCompletedSuccessfully && refWaitTask.IsCompletedSuccessfully && refBlockingTask.IsCompletedSuccessfully
                    && refMemoryTask.IsCompletedSuccessfully && refIoTask.IsCompletedSuccessfully
                    && ComparisonHasNoData(refCpuTask.Result?.Count ?? 0, refWaitTask.Result?.Count ?? 0, refBlockingTask.Result?.Count ?? 0,
                        refMemoryTask.Result?.Count ?? 0, refIoTask.Result?.Count ?? 0))
                {
                    ComparisonEmptyBanner.Text = ComparisonEmptyText(days);
                    ComparisonEmptyBanner.Visibility = Visibility.Visible;
                }

                // Register reference data with crosshair manager for tooltip
                _crosshairManager?.SetComparisonLabel(ComparisonLabel(days));
            }

            /* VLines must be re-attached before SyncXAxes so they're part of
               the render set when the chart refreshes. */
            _crosshairManager?.ReattachVLines();
            SyncXAxes(hoursBack, fromDate, toDate);

            /* #4966: the blocking chart draws event counts, so an empty stretch reads as "nothing happened" and the chart says
               where its data starts. The note comes last, after every lane and the ghost lines are drawn and synced, and on its
               own: the probes are not part of the reads' WhenAll above, and a probe that fails costs the note, never the bars. */
            await ShowBlockingLaneDataStartAsync(
                windowStartUtc, windowEndUtc,
                blockingTask.IsCompletedSuccessfully ? blockingTask.Result : [],
                deadlockTask.IsCompletedSuccessfully ? deadlockTask.Result : []);

            /* The reads that failed, named in lane order for the host's status line; null when every lane read. A failed baseline
               read alone is logged and is not a failed refresh: its lane drew, and only the band is missing. */
            (string Read, Task Task)[] laneReads =
            [
                ("CPU %", cpuTask), ("Wait ms/sec", waitTask), ("Blocking", blockingTask), ("Deadlocks", deadlockTask),
                ("Buffer Pool MB", memoryTask), ("I/O ms", fileIoTask),
            ];
            (string Read, Task Task)[] baselineReads =
            [
                ("CPU baseline", cpuBaselineTask), ("Wait baseline", waitBaselineTask), ("I/O baseline", ioBaselineTask),
                ("Blocking baseline", blockingBaselineTask),
            ];
            if (OverviewLaneReadFailures.Summary(baselineReads) is { } failedBaselines)
                AppLogger.Info("CorrelatedLanes", failedBaselines);
            return OverviewLaneReadFailures.Status(laneReads, baselineReads);
        }
        finally
        {
            /* Safety net: if something threw between PrepareForRefresh() and the
               ReattachVLines() call above, VLines are still null. EnsureVLinesAttached
               creates them only for lanes where VLine is null, so it's idempotent. */
            _crosshairManager?.EnsureVLinesAttached();
            _isRefreshing = false;
        }
    }

    /// <summary>Raised with the data start the blocking lane's note chose (#5562 R7), so the tab's picker can name it. Null when the note found none.</summary>
    internal event Action<DateTime?>? DataStartFound;

    /// <summary>
    /// Raises or hides the blocking chart's "Showing since" note (#4966). The chart's two series, blocking and deadlocks, are
    /// event counts: an empty stretch before a series starts reads as "nothing happened". A window of 90 minutes or less
    /// starts no probe (<see cref="ServerTab.ProbeWindowFloorOrNullAsync"/>). The text goes through the shared banner step in
    /// the lanes' own clock (<c>_displayZone</c>, which is the tab's <c>GetPickerZone</c>), to the second.
    /// </summary>
    private async Task ShowBlockingLaneDataStartAsync(
        DateTime startUtc, DateTime endUtc, IReadOnlyList<TrendPoint> blockingBars, IReadOnlyList<TrendPoint> deadlockBars)
    {
        await LiteBlockingLaneDataStart.ShowAsync(
            BlockingLaneDataStartBanner,
            relation => Task.Run(() => _dataService!.GetQueryWindowFloorAsync(relation, _serverId, startUtc, endUtc)),
            startUtc, endUtc, blockingBars, deadlockBars, _displayZone(),
            blockingReadTookXe: () => Task.Run(() => _dataService!.HasBlockedProcessReportsInWindowAsync(_serverId, startUtc, endUtc)),
            xeOnlyBlockingFloorOf: () => Task.Run(() => _dataService!.GetQueryWindowFloorAsync(QueryWindowRelation.BlockedProcessReports, _serverId, startUtc, endUtc, includeAlsoCovered: false)),
            earliestReportOf: () => Task.Run(() => _dataService!.GetEarliestBlockedProcessReportInWindowAsync(_serverId, startUtc, endUtc)),
            thresholdOf: () => Task.Run(async () =>
            {
                var (on, first, zero) = await _dataService!.GetBlockedProcessThresholdOnAsync(_serverId, startUtc, endUtc);
                return new LiteBlockingLaneDataStart.BlockedProcessThreshold(on, first, zero);
            }),
            onStartChosen: start => DataStartFound?.Invoke(start));
    }

    internal void UpdateBlockingLane(List<(double Time, double Value)> blockingData,
        List<(double Time, double Value)> deadlockData, IReadOnlyList<PerformanceMonitor.Ui.BaselineBand.HourStep>? bands = null, double windowSpanDays = 0,
        string blockingUnit = PerformanceMonitor.Ui.BlockingTrendLabels.ReportsUnit)
    {
        ClearChart(BlockingChart);
        ApplyTheme(BlockingChart);

        // Register blocking and deadlock as separate named series for the tooltip
        var blockTimes = blockingData.Select(d => d.Time).ToArray();
        var blockValues = blockingData.Select(d => d.Value).ToArray();
        var deadTimes = deadlockData.Select(d => d.Time).ToArray();
        var deadValues = deadlockData.Select(d => d.Value).ToArray();

        // First series clears any previous data
        /* #7219: the blocking lane counts blocked-process reports, or DMV snapshot rows when the window holds no reports. */
        _crosshairManager?.SetLaneUnit(BlockingChart, blockingUnit);
        _crosshairManager?.SetLaneData(BlockingChart, blockTimes, blockValues, isEventBased: true,
            hoverToleranceDays: EventLaneGeometry.HoverToleranceDays(windowSpanDays));
        // Rename the auto-created series and add the second
        _crosshairManager?.AddLaneSeries(BlockingChart, "Deadlocks", "events",
            deadTimes, deadValues, isEventBased: true,
            hoverToleranceDays: EventLaneGeometry.HoverToleranceDays(windowSpanDays));

        if (blockingData.Count == 0 && deadlockData.Count == 0)
        {
            ShowEmpty(BlockingChart, "Block/Dead");
            return;
        }

        /* #6284: a bar follows the window it is drawn in, and the hover reaches as far as the bar does. A fixed 30-second
           bar is a sub-pixel sliver in a 24-hour window, and a hover that reached one minute read 0 over it. */
        double barWidth = EventLaneGeometry.BarWidthDays(windowSpanDays);
        double maxCount = 0;

        // Baseline band for blocking: added before the bars so it paints behind them (#7433)
        if (bands is { Count: > 0 })
        {
            /* #7644: one band per local hour, each hour judged on its own. */
            _crosshairManager?.SetLaneBaselineSteps(BlockingChart, bands, isEventBased: true);
            PerformanceMonitor.Ui.BaselineBand.AddBand(BlockingChart.Plot, bands, ChartPalette.AccentColor("BaselineBlocking"));
        }

        // Blocking bars — red
        if (blockingData.Count > 0)
        {
            var bars = blockingData.Select(d => new ScottPlot.Bar
            {
                Position = d.Time,
                Value = d.Value,
                Size = barWidth,
                FillColor = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Blocking")),
                LineWidth = 0
            }).ToArray();
            BlockingChart.Plot.Add.Bars(bars);
            maxCount = Math.Max(maxCount, blockingData.Max(d => d.Value));
        }

        // Deadlock bars — slightly narrower so both are visible over the blocking bars
        if (deadlockData.Count > 0)
        {
            var bars = deadlockData.Select(d => new ScottPlot.Bar
            {
                Position = d.Time,
                Value = d.Value,
                Size = barWidth * 0.6,
                FillColor = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("Deadlocks")),
                LineWidth = 0
            }).ToArray();
            BlockingChart.Plot.Add.Bars(bars);
            maxCount = Math.Max(maxCount, deadlockData.Max(d => d.Value));
        }

        BlockingChart.Plot.Axes.DateTimeTicksBottomUtc(_displayZone);
        BlockingChart.Plot.Axes.Bottom.TickLabelStyle.IsVisible = false;
        ReapplyAxisColors(BlockingChart);

        BlockingChart.Plot.Title("");
        BlockingChart.Plot.YLabel("");
        BlockingChart.Plot.Legend.IsVisible = false;
        BlockingChart.Plot.Axes.Margins(bottom: 0);
        ChartStyle.UseCountAxis(BlockingChart); // #8224: events are counted, so the lane labels whole numbers only
        BlockingChart.Plot.Axes.SetLimitsY(0, Math.Max(maxCount * 1.3, 2));

        BlockingChart.Refresh();
    }

    /* Two-series CPU lane: SQL CPU (blue) + Total non-idle CPU (orange).
       Total is what the alert evaluates by default (see CpuAlertMode), so
       plotting it directly avoids the "alert says 95% but chart shows 60%"
       confusion (PM #1004). Baseline band + anomaly markers stay on SQL
       because that's the metric the CPU baseline was computed from. */
    internal void UpdateCpuLane(
        List<(double Time, double Value)> sqlData,
        List<(double Time, double Value)> totalData,
        IReadOnlyList<PerformanceMonitor.Ui.BaselineBand.HourStep>? bands = null)
    {
        ClearChart(CpuChart);
        ApplyTheme(CpuChart);

        if (sqlData.Count == 0 && totalData.Count == 0)
        {
            ShowEmpty(CpuChart, "CPU %");
            return;
        }

        var sqlTimes = sqlData.Select(d => d.Time).ToArray();
        var sqlValues = sqlData.Select(d => d.Value).ToArray();
        var totalTimes = totalData.Select(d => d.Time).ToArray();
        var totalValues = totalData.Select(d => d.Value).ToArray();

        // Register SQL as the first (lane-default-named) series, Total as a named second series
        _crosshairManager?.SetLaneData(CpuChart, sqlTimes, sqlValues);
        _crosshairManager?.AddLaneSeries(CpuChart, "Total", "%", totalTimes, totalValues);

        // Baseline band — applies to SQL CPU (what the baseline was computed on)
        List<int>? cpuAnomalyIndices = null;
        if (bands is { Count: > 0 })
        {
            _crosshairManager?.SetLaneBaselineSteps(CpuChart, bands, 10);

            PerformanceMonitor.Ui.BaselineBand.AddBand(CpuChart.Plot, bands, ChartPalette.AccentColor("BaselineCpu"));

            // Anomaly markers on SQL series only, each point judged against its own hour's band (#7644); an hour with no band marks nothing
            var anomalyIndices = new List<int>();
            for (int i = 0; i < sqlValues.Length; i++)
            {
                if (PerformanceMonitor.Ui.BaselineBand.HourStep.At(bands, sqlTimes[i]) is { } step
                    && ((sqlValues[i] > step.Upper && sqlValues[i] >= 10) || sqlValues[i] < step.Lower))
                    anomalyIndices.Add(i);
            }

            cpuAnomalyIndices = anomalyIndices;
        }

        if (totalValues.Length > 0)
        {
            var totalScatter = CpuChart.Plot.Add.TimeSeries(totalTimes, totalValues);
            totalScatter.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("TotalCpu"));
            totalScatter.MarkerSize = 0;
            totalScatter.LineWidth = 1.5f;
            totalScatter.LegendText = "Total";
            totalScatter.ConnectStyle = ScottPlot.ConnectStyle.Straight;
        }

        if (sqlValues.Length > 0)
        {
            var sqlScatter = CpuChart.Plot.Add.TimeSeries(sqlTimes, sqlValues);
            sqlScatter.Color = ScottPlot.Color.FromHex(ChartPalette.SeriesColor("SqlCpu"));
            sqlScatter.MarkerSize = 0;
            sqlScatter.LineWidth = 1.5f;
            sqlScatter.LegendText = "SQL";
            sqlScatter.ConnectStyle = ScottPlot.ConnectStyle.Straight;
        }

        // #7678: the marks go AFTER the series lines so a run reads as the line's stretch redrawn thicker in the anomaly colour.
        if (cpuAnomalyIndices is not null)
            PerformanceMonitor.Ui.AnomalyMarks.Add(CpuChart.Plot, sqlTimes, sqlValues, cpuAnomalyIndices);

        CpuChart.Plot.Axes.DateTimeTicksBottomUtc(_displayZone);
        if (CpuChart != FileIoChart)
            CpuChart.Plot.Axes.Bottom.TickLabelStyle.IsVisible = false;

        ReapplyAxisColors(CpuChart);

        CpuChart.Plot.Title("");
        CpuChart.Plot.YLabel("");
        CpuChart.Plot.Legend.IsVisible = false;
        CpuChart.Plot.Axes.Margins(bottom: 0);
        CpuChart.Plot.Axes.SetLimitsY(0, 105);

        CpuChart.Refresh();
    }

    internal void UpdateLane(ScottPlot.WPF.WpfPlot chart, string title,
        List<(double Time, double Value)> data, string colorHex,
        double? yMin = null, double? yMax = null, IReadOnlyList<PerformanceMonitor.Ui.BaselineBand.HourStep>? bands = null,
        double minAnomalyValue = 0, IReadOnlyList<long>? pointReads = null, long minPointReads = 0)
    {
        ClearChart(chart);
        ApplyTheme(chart);

        if (data.Count == 0)
        {
            ShowEmpty(chart, title);
            return;
        }

        var times = data.Select(d => d.Time).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        // Render baseline band FIRST (behind the data line)
        List<int>? laneAnomalyIndices = null;
        if (bands is { Count: > 0 })
        {
            _crosshairManager?.SetLaneBaselineSteps(chart, bands, minAnomalyValue);

            /* #8224: the Wait lane's amber at the stock opacity is a brown block on Dark and beige on Cool Breeze, so it takes the lighter tint. */
            PerformanceMonitor.Ui.BaselineBand.AddBand(chart.Plot, bands, colorHex,
                chart == WaitStatsChart ? PerformanceMonitor.Ui.BaselineBand.LightFillAlpha : PerformanceMonitor.Ui.BaselineBand.FillAlpha);

            // Highlight anomalous points (outside ± 2σ band AND above absolute minimum), each judged against its own hour's band (#7644)
            var anomalyIndices = new List<int>();
            for (int i = 0; i < values.Length; i++)
            {
                if (PerformanceMonitor.Ui.BaselineBand.HourStep.At(bands, times[i]) is not { } step) continue;
                /* #7624: a lane with a read floor (I/O) marks only a point whose reads reach it, as the web strip does. */
                var reads = pointReads != null && i < pointReads.Count ? pointReads[i] : 0;
                if (OverviewIoReadFloor.IsAnomaly(values[i], step.Upper, step.Lower, minAnomalyValue, reads, pointReads == null ? 0 : minPointReads))
                    anomalyIndices.Add(i);
            }

            laneAnomalyIndices = anomalyIndices;
        }

        var scatter = chart.Plot.Add.TimeSeries(times, values);
        scatter.Color = ScottPlot.Color.FromHex(colorHex);
        scatter.MarkerSize = 0;
        scatter.LineWidth = 1.5f;
        scatter.LegendText = title;
        scatter.ConnectStyle = ScottPlot.ConnectStyle.Straight;

        // #7678: the marks go AFTER the series line so a run reads as the line's stretch redrawn thicker in the anomaly colour.
        if (laneAnomalyIndices is not null)
            PerformanceMonitor.Ui.AnomalyMarks.Add(chart.Plot, times, values, laneAnomalyIndices);

        _crosshairManager?.SetLaneData(chart, times, values,
            reads: pointReads?.ToArray(), minReads: pointReads == null ? 0 : minPointReads);

        chart.Plot.Axes.DateTimeTicksBottomUtc(_displayZone);
        // Hide bottom tick labels on all lanes except the last (File I/O)
        if (chart != FileIoChart)
            chart.Plot.Axes.Bottom.TickLabelStyle.IsVisible = false;

        ReapplyAxisColors(chart);

        // Compact layout: hide Y label, minimize title, no legend
        chart.Plot.Title("");
        chart.Plot.YLabel("");
        chart.Plot.Legend.IsVisible = false;
        chart.Plot.Axes.Margins(bottom: 0);

        if (yMin.HasValue && yMax.HasValue)
            chart.Plot.Axes.SetLimitsY(yMin.Value, yMax.Value);
        else
        {
            var maxVal = data.Max(d => d.Value);
            var minVal = data.Min(d => d.Value);
            var padding = Math.Max((maxVal - minVal) * 0.1, 1);
            chart.Plot.Axes.SetLimitsY(Math.Max(0, minVal - padding), maxVal + padding);
        }

        chart.Refresh();
    }

    /// <summary>
    /// #4766: the X-axis window every lane is pinned to. The lanes plot the UTC instant, so the window is UTC as it is
    /// held: a custom range's two bounds, or a preset's last hoursBack REAL hours ending at utcNow
    /// (<see cref="TimeWindows.ChartAxis"/>). No wall-clock arithmetic is involved, so a clock change inside the window
    /// cannot move its start; with only one bound supplied the axis falls back to the preset, as it always did. utcNow is
    /// an explicit parameter so a test can pin the clock.
    /// </summary>
    internal static (DateTime FromUtc, DateTime ToUtc) GetXAxisWindow(
        int hoursBack, DateTime? fromDate, DateTime? toDate, DateTime utcNow) =>
        TimeWindows.ChartAxis(hoursBack, fromDate, toDate, utcNow);

    /// <summary>When the server was first collected (#7386), set by the server tab from its own read. The lanes' shared X axis does
    /// not start before it. Null leaves the axis as picked.</summary>
    internal DateTime? ServerDataStartUtc { get; set; }

    /// <summary>
    /// Sets identical X-axis limits across all lanes.
    /// </summary>
    internal void SyncXAxes(int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        var (xStart, xEnd) = GetXAxisWindow(hoursBack, fromDate, toDate, DateTime.UtcNow);

        var charts = new[] { CpuChart, WaitStatsChart, BlockingChart, MemoryChart, FileIoChart };

        /* #7386: all five lanes get the same start, so they keep one ruler. It is never later than the earliest point any lane
           drew, whichever relation fed that lane. */
        double xMin = ChartAxisStart.XStart(xStart, xEnd, ServerDataStartUtc, charts.Select(c => c.Plot).ToArray()).ToOADate();
        double xMax = xEnd.ToOADate();

        foreach (var chart in charts)
            chart.Plot.Axes.SetLimitsX(xMin, xMax);

        /* #2535: same defect as #2533 on the Darling viewer, same fix. Every lane's X and Y limits are
           final by now, so this is the one place that can see all five gutters at once and hand them the
           widest. It has to run on EVERY refresh, not once at Initialize: ClearChart() calls
           WpfPlot.Reset(), which swaps in a fresh Plot and takes any axis floor with it. */
        var gutter = LaneAxisAligner.AlignLeftGutters(charts);
        PlaceCaptions(gutter);

        foreach (var chart in charts)
            chart.Refresh();
    }

    /// <summary>
    /// Renders a semi-transparent dashed ghost line for comparison overlay.
    /// </summary>
    private static void AddGhostLine(ScottPlot.WPF.WpfPlot chart,
        List<(double Time, double Value)> data, string colorHex)
    {
        if (data.Count == 0) return;

        var times = data.Select(d => d.Time).ToArray();
        var values = data.Select(d => d.Value).ToArray();

        var scatter = chart.Plot.Add.TimeSeries(times, values);
        // White-ish ghost line — distinct from the primary colored line
        scatter.Color = ScottPlot.Color.FromHex(ChartPalette.AccentColor("GhostLine")).WithAlpha(140);
        scatter.MarkerSize = 0;
        scatter.LineWidth = 1.5f;
        scatter.LinePattern = ScottPlot.LinePattern.Dashed;

        chart.Refresh();
    }

    /// <summary>True when none of the comparison window's reads returned a row, so no ghost line can be drawn on any lane.</summary>
    internal static bool ComparisonHasNoData(int cpuRows, int waitRows, int blockingRows, int memoryRows, int ioRows)
        => cpuRows == 0 && waitRows == 0 && blockingRows == 0 && memoryRows == 0 && ioRows == 0;

    /// <summary>What the chart says when the comparison window holds no data, in the same words as <see cref="ComparisonLabel"/>.</summary>
    internal static string ComparisonEmptyText(int days)
        => $"No comparison line: no data was collected for the same hours {(days == 1 ? "yesterday" : days == 7 ? "last week" : $"{days:N0} days earlier")}";

    /// <summary>The words the crosshair tooltip puts beside a ghost line: how many whole days it is moved by.</summary>
    internal static string ComparisonLabel(int days)
    {
        if (days == 1) return "yesterday";
        if (days == 7) return "last week";
        return $"{days:N0}d ago";
    }

    private static void ClearChart(ScottPlot.WPF.WpfPlot chart)
    {
        chart.Reset();
        PerformanceMonitor.Ui.ChartDisplayScale.Apply(chart); // #6678: Reset swaps in a plot at scale 1
        chart.Plot.Clear();
    }

    /* A lane whose read failed is drawn as the empty lane is, then says so over it, so it cannot pass for a quiet healthy lane. */
    private void ShowFailed(ScottPlot.WPF.WpfPlot chart, string title)
    {
        ShowEmpty(chart, title);
        MarkFailed(chart);
    }

    /* The note is anchored to the lane's pixels, not its axes, so SyncXAxes leaves it where it is. */
    private static void MarkFailed(ScottPlot.WPF.WpfPlot chart)
    {
        var note = chart.Plot.Add.Annotation("read failed", ScottPlot.Alignment.MiddleCenter);
        note.LabelFontColor = ChartStyle.GetThemeColors().Text;
        note.LabelFontSize = 11;
        note.LabelBackgroundColor = ScottPlot.Colors.Transparent;
        note.LabelBorderColor = ScottPlot.Colors.Transparent;
        note.LabelShadowColor = ScottPlot.Colors.Transparent;
        chart.Refresh();
    }

    /* Render an empty lane as a live, gridded chart instead of a dead black box. This is most often
       the Blocking/Deadlocking lane on a healthy server (no events = good news), so make it match the
       populated lanes: keep the grid and show a 0-1 Y axis. X limits and the vertical gridlines are
       set afterward by SyncXAxes so the time axis aligns with the other lanes; the left axis keeps its
       default numeric ticks, so 0/1 labels and horizontal gridlines render. #8307: the lane also says it
       is empty, in the one muted sentence the other charts use, anchored to the plot's center in pixels so
       the X limits SyncXAxes sets afterwards do not carry it off the screen. The title arg is retained
       for call-site readability. */
    private void ShowEmpty(ScottPlot.WPF.WpfPlot chart, string title)
    {
        chart.Plot.Axes.DateTimeTicksBottomUtc(_displayZone);
        // Only the bottom (File I/O) lane shows time labels; the upper lanes hide them (matches UpdateLane).
        if (chart != FileIoChart)
            chart.Plot.Axes.Bottom.TickLabelStyle.IsVisible = false;

        ReapplyAxisColors(chart);

        chart.Plot.Title("");
        chart.Plot.YLabel("");
        chart.Plot.Legend.IsVisible = false;
        chart.Plot.Axes.Margins(bottom: 0);
        if (chart == BlockingChart) ChartStyle.UseCountAxis(chart); // #8224: an empty count lane still labels whole numbers
        chart.Plot.Axes.SetLimitsY(0, 1);

        var note = chart.Plot.Add.Annotation($"No {LaneName(chart)} data in this window", ScottPlot.Alignment.MiddleCenter);
        note.LabelFontSize = 14;
        note.LabelFontColor = EmptyTextColor();
        note.LabelBackgroundColor = ScottPlot.Colors.Transparent;
        note.LabelBorderColor = ScottPlot.Colors.Transparent;
        note.LabelBorderWidth = 0;
        note.LabelShadowColor = ScottPlot.Colors.Transparent;

        chart.Refresh();
    }

    /// <summary>The lane's name in its empty sentence, the one Initialize uses before the first read.</summary>
    private string LaneName(ScottPlot.WPF.WpfPlot chart) =>
        chart == CpuChart ? "SQL CPU"
        : chart == WaitStatsChart ? "Wait Stats"
        : chart == BlockingChart ? "Blocking"
        : chart == MemoryChart ? "Buffer Pool"
        : "I/O Latency";

    /// <summary>The theme's muted text color (the one the XAML empty hints use), or the palette's placeholder grey when no theme is loaded.</summary>
    private static ScottPlot.Color EmptyTextColor() =>
        System.Windows.Application.Current?.TryFindResource("ForegroundMutedBrush") is System.Windows.Media.SolidColorBrush brush
            ? new ScottPlot.Color(brush.Color.R, brush.Color.G, brush.Color.B, brush.Color.A)
            : ScottPlot.Color.FromHex(ChartPalette.AccentColor("Placeholder"));

    /// <summary>
    /// Reapplies theme to all lane charts (call on theme change).
    /// </summary>
    public void ReapplyTheme()
    {
        var charts = new[] { CpuChart, WaitStatsChart, BlockingChart, MemoryChart, FileIoChart };
        foreach (var chart in charts)
        {
            ApplyTheme(chart);
            chart.Refresh();
        }
        BackCaptions();
    }

    /// <summary>
    /// #8162: each lane's label is a caption at the top left of its lane, over the plot and clear of the Y tick labels, so the
    /// lanes need no gutter of their own. The tick labels' width is the lanes' shared left gutter (<paramref name="gutter"/>, in
    /// the same units the layout uses: at a display scale above 100% it is not a device pixel count, and dividing it by the scale put "CPU %" over the top Y label), and the caption starts just past it.
    /// </summary>
    private void PlaceCaptions(double gutter)
    {
        var margin = new Thickness(gutter + 6, 3, 0, 0);
        foreach (var caption in new[] { CpuCaption, WaitStatsCaption, BlockingCaption, MemoryCaption, FileIoCaption })
            caption.Margin = margin;
    }

    /// <summary>#8224: a caption sits over the plot's top edge, where the top Y tick's gridline ran through its letters; it is drawn on the plot's own background, so the line stops at the text.</summary>
    private void BackCaptions()
    {
        var c = ChartStyle.GetThemeColors().DataBackground;
        var brush = new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(c.R, c.G, c.B));
        brush.Freeze();
        foreach (var caption in new[] { CpuCaption, WaitStatsCaption, BlockingCaption, MemoryCaption, FileIoCaption })
        {
            caption.Background = brush;
            caption.Padding = new Thickness(4, 0, 4, 0);
        }
    }

    /// <summary>#8162: the height a lane that shows no X labels keeps under its plot: its tick marks.</summary>
    private const float LaneTickRoom = 6f;

    private static void ApplyTheme(ScottPlot.WPF.WpfPlot chart)
    {
        // Lanes are a deliberately stripped chrome variant (legend hidden, no font-size / first-render
        // hook). Colors come from the shared ChartStyle so the per-theme hexes can't drift.
        var c = ChartStyle.GetThemeColors();
        chart.Plot.FigureBackground.Color = c.FigureBackground;
        chart.Plot.DataBackground.Color = c.DataBackground;
        chart.Plot.Axes.Color(c.Text);
        chart.Plot.Grid.MajorLineColor = c.Grid;
        chart.Plot.Legend.IsVisible = false;
        chart.Plot.Axes.Margins(bottom: 0);
        chart.Plot.Axes.Bottom.TickLabelStyle.ForeColor = c.Text;
        chart.Plot.Axes.Left.TickLabelStyle.ForeColor = c.Text;
        chart.Plot.Axes.Bottom.Label.ForeColor = c.Text;
        chart.Plot.Axes.Left.Label.ForeColor = c.Text;

        chart.Background = new System.Windows.Media.SolidColorBrush(
            System.Windows.Media.Color.FromRgb(c.FigureBackground.R, c.FigureBackground.G, c.FigureBackground.B));
    }

    /* #8162: a lane's plot fills its lane. This runs after DateTimeTicksBottomUtc, which swaps in a new bottom axis and takes any
       size limit with it. The lane label is a caption over the lane, so no top axis sits above the plot, and a lane that shows no
       X labels keeps no room for them (ScottPlot reserves the label height even when the labels are hidden, which left a band
       under every lane). The last lane keeps the room its labels need. */
    private void ReapplyAxisColors(ScottPlot.WPF.WpfPlot chart)
    {
        chart.Plot.Axes.Top.IsVisible = false;
        chart.Plot.Axes.Bottom.MaximumSize = chart == FileIoChart ? float.PositiveInfinity : LaneTickRoom;
        var text = ChartStyle.GetThemeColors().Text;
        chart.Plot.Axes.Bottom.TickLabelStyle.ForeColor = text;
        chart.Plot.Axes.Left.TickLabelStyle.ForeColor = text;
        chart.Plot.Axes.Bottom.Label.ForeColor = text;
        chart.Plot.Axes.Left.Label.ForeColor = text;
    }
}
