/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Daily Summary inner tab — a COPY of Lite's Performance Calendar wiring, reads rewired to Postgres.
/// A month heatmap of composite daily health (the shared <see cref="PerformanceCalendar"/>); clicking a day
/// opens the shared day-detail panel (band header, "why this day is &lt;band&gt;" reasons, key metrics, and
/// drill buttons). The tab-refresh loop (auto-refresh timer / tab switch, via <see cref="LoadDailySummaryAsync"/>)
/// reloads the currently displayed month; the panel re-shows itself for the selected day when it stays in view.
/// Clicking a drill button (<see cref="DailyCalendar_DayDrillRequested"/>) scopes the toolbar to that day and
/// jumps to the target grid, reusing the same mechanism as the per-chart "Show Active Queries at This Time" drill.
/// </summary>
public partial class ViewerServerTab
{
    /// <summary>
    /// Entry point from LoadInnerTabAsync (index 14) and the auto-refresh loop: (re)load the calendar's
    /// currently displayed month. LoadInnerTabAsync owns this path's try/catch.
    /// </summary>
    private async Task LoadDailySummaryAsync()
    {
        await LoadCalendarMonthAsync(DailyCalendar.DisplayMonth);
    }

    /// <summary>
    /// Loads a month's daily summaries into the Performance Calendar. One banded cell per collected day;
    /// days with no collection are absent (the calendar renders them No-Data grey). Each day carries its
    /// signals + key metrics so the shared day-detail panel can explain the day and offer the right drills.
    /// The calendar re-shows the day-detail panel for the selected day itself when it stays in view.
    /// </summary>
    private async Task LoadCalendarMonthAsync(DateTime month)
    {
        var start = new DateTime(month.Year, month.Month, 1);
        var end = start.AddMonths(1);

        var rows = await _dataService.GetDailySummaryRangeAsync(_server.ServerId, start, end);

        DailyCalendar.Days = rows.Select(r => new PerformanceCalendarDay
        {
            Date = r.SummaryDate.Date,
            Band = r.HealthBand,
            Tooltip = $"{r.SummaryDate:MMM d, yyyy} - {r.OverallHealth}{Environment.NewLine}{r.SignalsTooltip}",
            Signals = r.ToSignals(),
            TopWaitType = r.TopWaitType,
            TotalWaitSeconds = r.TotalWaitTimeSec,
            UniqueQueries = r.UniqueQueries,
            PeakBlockMs = r.MaxBlockDurationMs,
        }).ToList();
    }

    private async void DailyCalendar_MonthChanged(object? sender, PerformanceCalendarMonthEventArgs e)
    {
        try
        {
            await LoadCalendarMonthAsync(e.Month);
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"Daily Summary load failed: {ex.Message}");
        }
    }

    private async void DailySummaryRefresh_Click(object sender, RoutedEventArgs e)
    {
        try
        {
            await LoadCalendarMonthAsync(DailyCalendar.DisplayMonth);
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"Daily Summary refresh failed: {ex.Message}");
        }
    }

    /// <summary>
    /// A day-detail drill button (View Deadlocks / Blocking / Top Queries): scope the toolbar to the
    /// clicked day's [00:00, next-day 00:00) UTC window and jump to the target inner tab, then load that grid
    /// over the day. This reuses the exact mechanism the per-chart drills use — set the toolbar's custom
    /// window, switch the inner tab under <see cref="_suppressDrillDownAutoRefresh"/> (so the tab-switch
    /// auto-refresh doesn't race the targeted read), then call the same loader the tab uses.
    /// </summary>
    private async void DailyCalendar_DayDrillRequested(object? sender, PerformanceCalendarDrillEventArgs e)
    {
        try
        {
            var (startUtc, endUtc) = DailyHealthBandCalculator.DayWindowUtc(e.Date);

            // Server-mode picker conversion needs this server's offset applied (cached; a no-op once loaded).
            await RefreshServerClockAsync();
            ApplyServerClockToHelper();

            // Scope the toolbar to the whole day so the grid the user lands on — and anywhere they navigate
            // next — stays on that day. Suppressed so it drives no reload of its own; we load the target below.
            SetToolbarWindowUtc(startUtc, endUtc);

            _suppressDrillDownAutoRefresh = true;
            try
            {
                switch (e.Target)
                {
                    case DayDrillTarget.Deadlocks:
                        InnerTabs.SelectedIndex = BlockingInnerTabIndex;
                        BlockingSubTabs.SelectedIndex = DeadlocksSubTabIndex;
                        break;
                    case DayDrillTarget.Blocking:
                        InnerTabs.SelectedIndex = BlockingInnerTabIndex;
                        BlockingSubTabs.SelectedIndex = BlockedProcessReportsSubTabIndex;
                        break;
                    case DayDrillTarget.TopQueries:
                        InnerTabs.SelectedIndex = QueriesInnerTabIndex;
                        QueriesSubTabControl.SelectedIndex = TopQueriesSubTabIndex;
                        break;
                }
            }
            finally
            {
                _suppressDrillDownAutoRefresh = false;
            }

            switch (e.Target)
            {
                case DayDrillTarget.Deadlocks:
                    await LoadDeadlocksAsync(startUtc, endUtc);
                    break;
                case DayDrillTarget.Blocking:
                    await LoadBlockedProcessReportsAsync(startUtc, endUtc);
                    break;
                case DayDrillTarget.TopQueries:
                    await LoadTopQueriesAsync(startUtc, endUtc);
                    break;
            }
        }
        catch (Exception ex)
        {
            StatusChanged?.Invoke($"day drill failed: {ex.Message}");
        }
    }
}
