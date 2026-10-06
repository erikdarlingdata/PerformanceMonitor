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
using System.Windows.Controls;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    /// <summary>
    /// Loads a month's daily summaries into the Performance Calendar. One banded cell per collected day;
    /// days with no collection are simply absent (the calendar renders them No-Data grey). Each day carries
    /// its signals + key metrics so the shared day-detail panel can explain the day and offer the right
    /// drills. The heavy DuckDB read runs off the UI thread; the calendar re-shows its day-detail panel for
    /// the selected day itself when it stays in view.
    /// </summary>
    private async Task LoadCalendarMonthAsync(DateTime month)
    {
        try
        {
            var start = new DateTime(month.Year, month.Month, 1);
            var end = start.AddMonths(1);

            var rows = await Task.Run(() => _dataService.GetDailySummaryRangeAsync(_serverId, start, end));

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
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] LoadCalendarMonthAsync failed: {ex.Message}");
        }
    }

    private async void DailyCalendar_MonthChanged(object? sender, PerformanceCalendarMonthEventArgs e)
        => await LoadCalendarMonthAsync(e.Month);

    private async void DailySummaryRefresh_Click(object sender, RoutedEventArgs e)
        => await LoadCalendarMonthAsync(DailyCalendar.DisplayMonth);

    /// <summary>
    /// A day-detail drill button (View Deadlocks / Blocking / Top Queries): scope the toolbar to the
    /// clicked day's [00:00, next-day 00:00) window and jump to the target tab, then load it over the day.
    /// This reuses the exact mechanism the per-chart drills use — <see cref="SetDrillDownTimeRange"/> sets the
    /// toolbar's custom window, the tab switches run under <see cref="_suppressActiveQueriesAutoRefresh"/> (so
    /// the tab-switch auto-refresh doesn't race the targeted read), then the standard per-sub-tab refresh
    /// loads that grid over the window. The calendar buckets days in UTC and the reads take UTC (#4766), so the
    /// UTC day goes through as it is.
    /// </summary>
    private async void DailyCalendar_DayDrillRequested(object? sender, PerformanceCalendarDrillEventArgs e)
    {
        try
        {
            var (startUtc, endUtc) = DailyHealthBandCalculator.DayWindowUtc(e.Date);

            // Scope the toolbar to the whole day so the grid the user lands on — and anywhere they navigate
            // next — stays on that day (SetDrillDownTimeRange switches to Custom without triggering a reload).
            SetDrillDownTimeRange(startUtc, endUtc);

            _suppressActiveQueriesAutoRefresh = true;
            try
            {
                switch (e.Target)
                {
                    case DayDrillTarget.Deadlocks:
                        MainTabControl.SelectedIndex = 8;        // Blocking
                        BlockingSubTabControl.SelectedIndex = 3; // Deadlocks
                        break;
                    case DayDrillTarget.Blocking:
                        MainTabControl.SelectedIndex = 8;        // Blocking
                        BlockingSubTabControl.SelectedIndex = 2; // Blocked Process Reports
                        break;
                    case DayDrillTarget.TopQueries:
                        MainTabControl.SelectedIndex = 2;        // Queries
                        QueriesSubTabControl.SelectedIndex = 2;  // Top Queries by Duration
                        break;
                }
            }
            finally
            {
                _suppressActiveQueriesAutoRefresh = false;
            }

            // One targeted load of the tab we just switched to, over the day window (grid + slicer + comparison).
            /* #5371: through the refresh coordinator like every other load, so a timer tick cannot start a second read
               beside it. SetDrillDownTimeRange just held this day as the toolbar's custom range, so the pass's own
               GetCurrentWindowUtc is exactly (startUtc, endUtc). */
            await RefreshVisibleTabOnlyAsync();
        }
        catch (Exception ex)
        {
            AppLogger.Info("ServerTab", $"[{_server.DisplayName}] DailyCalendar_DayDrillRequested failed: {ex.Message}");
        }
    }
}
