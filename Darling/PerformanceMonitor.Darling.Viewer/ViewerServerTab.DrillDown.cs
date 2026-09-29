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
using System.Windows.Controls.Primitives;
using System.Windows.Input;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The per-chart right-click drill-downs — a faithful port of Lite's <c>ServerTab.DrillDown.cs</c> +
/// <c>AddChartDrillDownMenuItem</c> / <c>AddWaitDrillDownMenuItem</c> (ServerTab.xaml.cs:319-365), adapted to
/// the viewer's already-shipped drill-down surfaces (the query heatmap already navigates through
/// <see cref="NavigateToActiveQueriesForWindowAsync"/>). Every resource / trend chart gets a
/// "Show Active Queries at This Time"; the Blocking / Deadlocks trend charts get
/// "Show Blocking / Deadlocks at This Time"; the Wait Stats chart gets a wait-specific
/// "Show Queries With &lt;wait&gt;" that opens the <see cref="WaitDrillDownWindow"/>.
///
/// <para>Two notes, both inherent to the viewer: (1) Like Lite, each drill-down item is Inserted at the top of
/// the chart's copy/save/export <see cref="ContextMenu"/> (now a Darling-local port of Lite's
/// <c>ContextMenuHelper</c>, in ViewerServerTab.ChartContextMenu.cs) — <see cref="BuildChartContextMenu"/>
/// builds that menu and takes right-click over from ScottPlot (<see cref="RemoveScottPlotContextMenuResponses"/>),
/// then <see cref="AddChartDrillDownMenuItem"/> adds the drill-down item to the SAME menu, so both coexist.
/// (2) The chart X axis is the naive-UTC instant (the display zone only relabels it, #4766), so the
/// nearest-series time the <see cref="ChartHoverHelper"/> returns is already the store's naive UTC, and the
/// +/-30-minute window is read around it as is.</para>
/// </summary>
public partial class ViewerServerTab
{
    /// <summary>The drill-down half-window: a click resolves to a +/-30-minute read around the point
    /// (Lite's OnActiveQueriesDrillDown / OnBlockingDrillDown / OnDeadlockDrillDown all use 30).</summary>
    internal const int DrillDownHalfWindowMinutes = 30;

    /// <summary>
    /// Wires the right-click drill-down menus onto every chart that has one (Items 2-4). Called from the
    /// constructor after the Initialize*Charts methods, so each chart's <see cref="ChartHoverHelper"/> exists.
    /// </summary>
    private void WireChartDrillDowns()
    {
        /* Each drill-down chart's menu is ONE ContextMenu = [drill-down item] + [separator] + [copy/save/
           export items], exactly like Lite (ServerTab.xaml.cs:318-365 pairs SetupChartContextMenu with
           AddChartDrillDownMenuItem on the SAME menu). BuildChartContextMenu builds the copy/export items and
           takes right-click over from ScottPlot; AddChart/WaitDrillDownMenuItem then Insert their item at the
           top of that returned menu — one right-click owner, no competing handler.

           The hover accessors are lambdas, not captured values: WaitStats + Perfmon create their
           ChartHoverHelper LAZILY on the tab's first load (not in an Initialize*Charts call), so their fields
           are still null here in the constructor. Reading the field at menu-Opened time (by which point the
           tab has loaded and the hover exists) is correct for both the eager and the lazy charts. */

        /* Wait Stats — the specialized "Show Queries With <wait>" drill-down (opens WaitDrillDownWindow). */
        AddWaitDrillDownMenuItem(WaitStatsChart, BuildChartContextMenu(WaitStatsChart, "Wait_Stats"), () => _waitStatsHover);

        /* CPU / Memory / tempdb / Perfmon — "Show Active Queries at This Time" (Item 2, 10 charts). */
        AddChartDrillDownMenuItem(CpuChart, BuildChartContextMenu(CpuChart, "CPU_Usage"), () => _cpuHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(MemoryChart, BuildChartContextMenu(MemoryChart, "Memory_Usage"), () => _memoryHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(MemoryClerksChart, BuildChartContextMenu(MemoryClerksChart, "Memory_Clerks"), () => _memoryClerksHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(MemoryGrantSizingChart, BuildChartContextMenu(MemoryGrantSizingChart, "Memory_Grant_Sizing"), () => _memoryGrantSizingHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(MemoryGrantActivityChart, BuildChartContextMenu(MemoryGrantActivityChart, "Memory_Grant_Activity"), () => _memoryGrantActivityHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(MemoryPressureEventsChart, BuildChartContextMenu(MemoryPressureEventsChart, "Memory_Pressure_Events"), () => _memoryPressureEventsHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(TempDbChart, BuildChartContextMenu(TempDbChart, "TempDB_Stats"), () => _tempDbHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(TempDbSizeChart, BuildChartContextMenu(TempDbSizeChart, "TempDB_Allocated_Size"), () => _tempDbSizeHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(TempDbFileIoChart, BuildChartContextMenu(TempDbFileIoChart, "TempDB_File_IO"), () => _tempDbFileIoHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(PerfmonChart, BuildChartContextMenu(PerfmonChart, "Perfmon_Counters"), () => _perfmonHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);

        /* Performance Trends — "Show Active Queries at This Time" (Item 3, 4 charts). */
        AddChartDrillDownMenuItem(QueryDurationTrendChart, BuildChartContextMenu(QueryDurationTrendChart, "Query_Duration_Trends"), () => _queryDurationTrendHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(ProcDurationTrendChart, BuildChartContextMenu(ProcDurationTrendChart, "Procedure_Duration_Trends"), () => _procDurationTrendHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(QueryStoreDurationTrendChart, BuildChartContextMenu(QueryStoreDurationTrendChart, "QueryStore_Duration_Trends"), () => _queryStoreDurationTrendHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(ExecutionCountTrendChart, BuildChartContextMenu(ExecutionCountTrendChart, "Execution_Count_Trends"), () => _executionCountTrendHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);

        /* Blocking Trends — Lock Wait + Blocking -> "Show Blocking at This Time"; Deadlock ->
           "Show Deadlocks at This Time" (Item 3, 3 charts). */
        AddChartDrillDownMenuItem(LockWaitTrendChart, BuildChartContextMenu(LockWaitTrendChart, "Lock_Wait_Trends"), () => _lockWaitTrendHover, "Show _Blocking at This Time", OnBlockingDrillDown);
        AddChartDrillDownMenuItem(BlockingTrendChart, BuildChartContextMenu(BlockingTrendChart, "Blocking_Trends"), () => _blockingTrendHover, "Show _Blocking at This Time", OnBlockingDrillDown);
        AddChartDrillDownMenuItem(DeadlockTrendChart, BuildChartContextMenu(DeadlockTrendChart, "Deadlock_Trends"), () => _deadlockTrendHover, "Show Deadloc_ks at This Time", OnDeadlockDrillDown);

        /* Blocking Stats (this feature) — the severity duration charts drill to the same blocked-process
           reports as their count-trend siblings, so right-clicking a duration spike opens the blocks at that
           time ("Show Blocking at This Time" -> OnBlockingDrillDown). The deadlock-severity charts drill to
           the Deadlocks grid instead ("Show Deadlocks at This Time" -> OnDeadlockDrillDown), matching the
           Trends tab's deadlock-count chart. */
        AddChartDrillDownMenuItem(BlockingDurationChart, BuildChartContextMenu(BlockingDurationChart, "Blocking_Duration"), () => _blockingDurationHover, "Show _Blocking at This Time", OnBlockingDrillDown);
        AddChartDrillDownMenuItem(BlockingTotalDurationChart, BuildChartContextMenu(BlockingTotalDurationChart, "Blocking_Total_Duration"), () => _blockingTotalDurationHover, "Show _Blocking at This Time", OnBlockingDrillDown);
        AddChartDrillDownMenuItem(DeadlockWaitChart, BuildChartContextMenu(DeadlockWaitChart, "Deadlock_Wait"), () => _deadlockWaitHover, "Show Deadloc_ks at This Time", OnDeadlockDrillDown);
        AddChartDrillDownMenuItem(DeadlockTotalWaitChart, BuildChartContextMenu(DeadlockTotalWaitChart, "Deadlock_Total_Wait"), () => _deadlockTotalWaitHover, "Show Deadloc_ks at This Time", OnDeadlockDrillDown);

        /* File I/O (4) + Blocking > Current Waits (2) — the rest of Lite's "Show Active Queries at This Time"
           set (ServerTab.xaml.cs:340-363, in the same referenced block). Charts in this same viewer surface,
           same drill target; wired for faithful parity so no sibling chart is left without the drill-down. */
        AddChartDrillDownMenuItem(FileIoReadChart, BuildChartContextMenu(FileIoReadChart, "File_IO_Read_Latency"), () => _fileIoReadHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(FileIoWriteChart, BuildChartContextMenu(FileIoWriteChart, "File_IO_Write_Latency"), () => _fileIoWriteHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(FileIoReadThroughputChart, BuildChartContextMenu(FileIoReadThroughputChart, "File_IO_Read_Throughput"), () => _fileIoReadThroughputHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(FileIoWriteThroughputChart, BuildChartContextMenu(FileIoWriteThroughputChart, "File_IO_Write_Throughput"), () => _fileIoWriteThroughputHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(CurrentWaitsDurationChart, BuildChartContextMenu(CurrentWaitsDurationChart, "Current_Waits_Duration"), () => _currentWaitsDurationHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(CurrentWaitsBlockedChart, BuildChartContextMenu(CurrentWaitsBlockedChart, "Current_Waits_Blocked"), () => _currentWaitsBlockedHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);

        /* Darling-only time-series charts Lite lacks — CPU Scheduler, Latch/Spinlock contention, Plan Cache,
           Session Counts, and the eight System Events (system_health XE) counter charts. All plot the UTC
           instant on X, so "Show Active Queries at This Time" navigates to the correlated +/-30-minute active-queries
           window exactly like their sibling resource/trend charts. Peer-consistency (these previously had only
           the copy/export menu); no per-latch/per-spinlock-type drill (no correlation data captured). Only
           CollectorDuration stays menu-only, matching Lite's drill-less collector-duration chart. */
        AddChartDrillDownMenuItem(CpuSchedulerChart, BuildChartContextMenu(CpuSchedulerChart, "CPU_Scheduler"), () => _cpuSchedulerHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(LatchStatsChart, BuildChartContextMenu(LatchStatsChart, "Latch_Stats"), () => _latchStatsHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(SpinlockStatsChart, BuildChartContextMenu(SpinlockStatsChart, "Spinlock_Stats"), () => _spinlockStatsHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(SessionStatsChart, BuildChartContextMenu(SessionStatsChart, "Session_Stats"), () => _sessionStatsHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(PlanCacheChart, BuildChartContextMenu(PlanCacheChart, "Plan_Cache"), () => _planCacheHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);

        AddChartDrillDownMenuItem(BadPagesChart, BuildChartContextMenu(BadPagesChart, "Bad_Pages"), () => _badPagesHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(DumpRequestsChart, BuildChartContextMenu(DumpRequestsChart, "Dump_Requests"), () => _dumpRequestsHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(AccessViolationsChart, BuildChartContextMenu(AccessViolationsChart, "Access_Violations"), () => _accessViolationsHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(WriteAccessViolationsChart, BuildChartContextMenu(WriteAccessViolationsChart, "Write_Access_Violations"), () => _writeAccessViolationsHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(NonYieldingTasksChart, BuildChartContextMenu(NonYieldingTasksChart, "Non_Yielding_Tasks"), () => _nonYieldingTasksHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(LatchWarningsChart, BuildChartContextMenu(LatchWarningsChart, "Latch_Warnings"), () => _latchWarningsHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(SickSpinlocksChart, BuildChartContextMenu(SickSpinlocksChart, "Sick_Spinlocks"), () => _sickSpinlocksHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
        AddChartDrillDownMenuItem(CpuComparisonChart, BuildChartContextMenu(CpuComparisonChart, "CPU_Comparison"), () => _cpuComparisonHover, "Show _Active Queries at This Time", OnActiveQueriesDrillDown);
    }

    /// <summary>
    /// Inserts a right-click drill-down item at the TOP of a chart's existing copy/export
    /// <paramref name="menu"/> (Lite's <c>AddChartDrillDownMenuItem</c>: <c>Insert(0, item)</c> +
    /// <c>Insert(0, separator)</c>, so the menu reads [drill-down] [separator] [copy/save/export]). The menu's
    /// right-click was already taken over by <see cref="BuildChartContextMenu"/>; this only adds the item +
    /// its Opened/Click behavior, so the two coexist in one menu. The Opened handler resolves the
    /// nearest-series time under the cursor via the chart's hover helper (disabling the item off any
    /// series); the Click routes that time (a UTC instant) to <paramref name="handler"/>, which reads the
    /// window around it. <paramref name="hoverAccessor"/> is read at Opened time (not captured) so
    /// lazily-created hovers resolve.
    /// </summary>
    private void AddChartDrillDownMenuItem(
        ScottPlot.WPF.WpfPlot chart, ContextMenu menu, Func<ChartHoverHelper?> hoverAccessor, string label, Action<DateTime> handler)
    {
        menu.Items.Insert(0, new Separator());
        var item = new MenuItem { Header = label };
        menu.Items.Insert(0, item);

        menu.Opened += (_, _) =>
        {
            var nearest = hoverAccessor()?.GetNearestSeries(Mouse.GetPosition(chart));
            if (nearest.HasValue)
            {
                item.Tag = nearest.Value.Time;   /* the chart X is the naive-UTC instant */
                item.IsEnabled = true;
            }
            else
            {
                item.Tag = null;
                item.IsEnabled = false;
            }
        };

        item.Click += (_, _) =>
        {
            if (item.Tag is DateTime centreUtc)
                handler(centreUtc);
        };
    }

    /// <summary>
    /// The Wait Stats chart's specialized right-click "Show Queries With &lt;wait&gt;" (Lite's
    /// <c>AddWaitDrillDownMenuItem</c>), inserted at the top of the chart's copy/export <paramref name="menu"/>
    /// (same coexistence as <see cref="AddChartDrillDownMenuItem"/>). The hover's nearest series is a wait TYPE
    /// (the series label), so the item header names it and the Click opens the <see cref="WaitDrillDownWindow"/>
    /// for that wait over the drill window. Underscores in the wait type are doubled so WPF doesn't treat them
    /// as an access key.
    /// </summary>
    private void AddWaitDrillDownMenuItem(ScottPlot.WPF.WpfPlot chart, ContextMenu menu, Func<ChartHoverHelper?> hoverAccessor)
    {
        menu.Items.Insert(0, new Separator());
        var item = new MenuItem { Header = "Show _Queries With This Wait" };
        menu.Items.Insert(0, item);

        menu.Opened += (_, _) =>
        {
            var nearest = hoverAccessor()?.GetNearestSeries(Mouse.GetPosition(chart));
            if (nearest.HasValue)
            {
                item.Tag = (nearest.Value.Label, nearest.Value.Time);
                item.Header = $"Show _Queries With {nearest.Value.Label.Replace("_", "__")}";
                item.IsEnabled = true;
            }
            else
            {
                item.Tag = null;
                item.Header = "Show _Queries With This Wait";
                item.IsEnabled = false;
            }
        };

        item.Click += (_, _) =>
        {
            if (item.Tag is ValueTuple<string, DateTime> tag)
                ShowQueriesForWaitType(tag.Item1, tag.Item2);
        };
    }

    /// <summary>
    /// Takes right-click over from ScottPlot and shows <paramref name="menu"/> at the cursor — the ported
    /// heatmap drill-down's exact approach (<see cref="RemoveScottPlotContextMenuResponses"/> +
    /// PreviewMouseRightButtonDown), factored so every drill-down chart and the heatmap share one path.
    /// </summary>
    private static void AttachChartContextMenu(ScottPlot.WPF.WpfPlot chart, ContextMenu menu)
    {
        RemoveScottPlotContextMenuResponses(chart);
        chart.PreviewMouseRightButtonDown += (_, e) =>
        {
            e.Handled = true;
            menu.PlacementTarget = chart;
            menu.Placement = PlacementMode.MousePoint;
            menu.IsOpen = true;
        };
    }

    /// <summary>
    /// Removes ScottPlot's built-in right-click responses (its default context menu / pan) so a WPF
    /// <see cref="ContextMenu"/> can own right-click. Shared by every drill-down chart and the query heatmap.
    /// </summary>
    internal static void RemoveScottPlotContextMenuResponses(ScottPlot.WPF.WpfPlot chart)
    {
        chart.UserInputProcessor.UserActionResponses.RemoveAll(r =>
            r.GetType().Name.Contains("Context", StringComparison.Ordinal) ||
            r.GetType().Name.Contains("RightClick", StringComparison.Ordinal) ||
            r.GetType().Name.Contains("Menu", StringComparison.Ordinal));
    }

    /// <summary>The store's naive-UTC +/-30-minute window around a clicked chart time. The chart X is the UTC
    /// instant, so the time is the centre as is (pure, unit-tested).</summary>
    internal static (DateTime FromUtc, DateTime ToUtc) DrillWindowUtc(DateTime centreUtc)
    {
        return (centreUtc.AddMinutes(-DrillDownHalfWindowMinutes), centreUtc.AddMinutes(DrillDownHalfWindowMinutes));
    }

    /// <summary>
    /// "Show Active Queries at This Time" — navigates to Queries -> Active Queries filtered to the +/-30-minute
    /// window around the clicked point (Lite's OnActiveQueriesDrillDown). Also the target of the Overview
    /// lanes' <c>ShowActiveQueriesRequested</c> event.
    /// </summary>
    private async void OnActiveQueriesDrillDown(DateTime centreUtc)
    {
        try
        {
            var (fromUtc, toUtc) = DrillWindowUtc(centreUtc);
            var indicator = $"Drill-down: {ViewerTimeHelper.ForDisplay(fromUtc):HH:mm} → {ViewerTimeHelper.ForDisplay(toUtc):HH:mm}";
            await NavigateToActiveQueriesForWindowAsync(fromUtc, toUtc, indicator);
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"active-queries drill-down failed: {ex.Message}");
        }
    }

    /// <summary>
    /// "Show Blocking at This Time" — navigates to Blocking -> Blocked Process Reports filtered to the
    /// +/-30-minute window (Lite's OnBlockingDrillDown, targeting Darling's existing
    /// <see cref="LoadBlockedProcessReportsAsync"/> loader).
    /// </summary>
    private async void OnBlockingDrillDown(DateTime centreUtc)
    {
        var (fromUtc, toUtc) = DrillWindowUtc(centreUtc);
        _suppressDrillDownAutoRefresh = true;
        try
        {
            InnerTabs.SelectedIndex = BlockingInnerTabIndex;
            BlockingSubTabs.SelectedIndex = BlockedProcessReportsSubTabIndex;
        }
        finally
        {
            _suppressDrillDownAutoRefresh = false;
        }

        try
        {
            await LoadBlockedProcessReportsAsync(fromUtc, toUtc);
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"blocking drill-down failed: {ex.Message}");
        }
    }

    /// <summary>
    /// "Show Deadlocks at This Time" — navigates to Blocking -> Deadlocks filtered to the +/-30-minute window
    /// (Lite's OnDeadlockDrillDown, targeting Darling's existing <see cref="LoadDeadlocksAsync"/> loader).
    /// </summary>
    private async void OnDeadlockDrillDown(DateTime centreUtc)
    {
        var (fromUtc, toUtc) = DrillWindowUtc(centreUtc);
        _suppressDrillDownAutoRefresh = true;
        try
        {
            InnerTabs.SelectedIndex = BlockingInnerTabIndex;
            BlockingSubTabs.SelectedIndex = DeadlocksSubTabIndex;
        }
        finally
        {
            _suppressDrillDownAutoRefresh = false;
        }

        try
        {
            await LoadDeadlocksAsync(fromUtc, toUtc);
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"deadlock drill-down failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Opens the <see cref="WaitDrillDownWindow"/> for a wait type over the +/-30-minute drill window (Lite's
    /// ShowQueriesForWaitType_Click). Owned by this tab's window so the shared plan window it spawns floats
    /// above it; the drill window reads correlated snapshots from Postgres (no live SQL).
    /// </summary>
    private void ShowQueriesForWaitType(string waitType, DateTime centreUtc)
    {
        var (fromUtc, toUtc) = DrillWindowUtc(centreUtc);
        var window = new WaitDrillDownWindow(_dataService, _server.ServerId, waitType, fromUtc, toUtc)
        {
            Owner = Window.GetWindow(this),
        };
        window.ShowDialog();
    }
}
