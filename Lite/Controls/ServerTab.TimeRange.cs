/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Controls.Primitives;
using System.ComponentModel;
using System.Windows.Data;
using System.Windows.Threading;
using Microsoft.Data.SqlClient;
using Microsoft.Win32;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using ScottPlot;
using PerformanceMonitor.Ui;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    private void InitializeTimeComboBoxes()
    {
        // Populate hour ComboBoxes (12-hour format with AM/PM)
        var hours = new List<string>();
        for (int h = 0; h < 24; h++)
        {
            var dt = DateTime.Today.AddHours(h);
            hours.Add(dt.ToString("HH:00")); // "00:00", "01:00", ..., "23:00"
        }

        FromHourCombo.ItemsSource = hours;
        ToHourCombo.ItemsSource = hours;
        FromHourCombo.SelectedIndex = 0;  // Default to 12 AM
        ToHourCombo.SelectedIndex = 23;   // Default to 11 PM

        // Populate minute ComboBoxes (15-minute intervals)
        var minutes = new List<string> { ":00", ":15", ":30", ":45" };
        FromMinuteCombo.ItemsSource = minutes;
        ToMinuteCombo.ItemsSource = minutes;
        FromMinuteCombo.SelectedIndex = 0; // Default to :00
        ToMinuteCombo.SelectedIndex = 3;   // Default to :45 (so 11:45 PM is end)
    }

    private DateTime? GetDateTimeFromPickers(DatePicker datePicker, ComboBox hourCombo, ComboBox minuteCombo)
    {
        if (!datePicker.SelectedDate.HasValue) return null;

        var date = datePicker.SelectedDate.Value.Date;
        int hour = hourCombo.SelectedIndex >= 0 ? hourCombo.SelectedIndex : 0;
        int minute = minuteCombo.SelectedIndex >= 0 ? minuteCombo.SelectedIndex * 15 : 0;

        return date.AddHours(hour).AddMinutes(minute);
    }

    /// <summary>
    /// Gets the selected time range in hours.
    /// </summary>
    private int GetHoursBack()
    {
        return TimeRangeCombo.SelectedIndex switch
        {
            0 => 1,
            1 => 4,
            2 => 12,
            3 => 24,
            4 => 168,
            _ => 4
        };
    }

    /// <summary>
    /// Gets the UTC time range for slicer display, matching GetTimeRange in LocalDataService.
    /// </summary>
    private static (DateTime start, DateTime end) GetSlicerTimeRange(
        int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        if (fromDate.HasValue && toDate.HasValue)
        {
            var startUtc = fromDate.Value.AddMinutes(-ServerTimeHelper.UtcOffsetMinutes);
            var endUtc = toDate.Value.AddMinutes(-ServerTimeHelper.UtcOffsetMinutes);
            return (startUtc, endUtc);
        }

        return (DateTime.UtcNow.AddHours(-hoursBack), DateTime.UtcNow);
    }

    /// <summary>
    /// Sets the time range dropdown from outside (used by Apply to All).
    /// </summary>
    public void SetTimeRangeIndex(int index)
    {
        if (index >= 0 && index < TimeRangeCombo.Items.Count)
        {
            TimeRangeCombo.SelectedIndex = index;
        }
    }

    private void ApplyTimeRangeToAll_Click(object sender, RoutedEventArgs e)
    {
        ApplyTimeRangeRequested?.Invoke(TimeRangeCombo.SelectedIndex);
    }

    private void AutoRefreshCheckBox_Changed(object sender, RoutedEventArgs e)
    {
        if (_refreshTimer == null) return;

        if (AutoRefreshCheckBox.IsChecked == true)
        {
            UpdateAutoRefreshInterval();
            _refreshTimer.Start();
        }
        else
        {
            _refreshTimer.Stop();
        }
    }

    private void AutoRefreshInterval_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (_refreshTimer == null) return;
        UpdateAutoRefreshInterval();
    }

    private void UpdateAutoRefreshInterval()
    {
        if (AutoRefreshIntervalCombo == null) return;

        _refreshTimer.Interval = AutoRefreshIntervalCombo.SelectedIndex switch
        {
            0 => TimeSpan.FromSeconds(30),
            1 => TimeSpan.FromMinutes(1),
            2 => TimeSpan.FromMinutes(5),
            _ => TimeSpan.FromMinutes(1)
        };
    }

    private async void RefreshDataButton_Click(object sender, RoutedEventArgs e)
    {
        RefreshDataButton.IsEnabled = false;
        try
        {
            if (ManualRefreshRequested != null)
            {
                await ManualRefreshRequested.Invoke();
            }
            /* Manual refresh loads all sub-tabs of the visible tab, not all 13 tabs */
            await RefreshAllDataAsync();
        }
        finally
        {
            RefreshDataButton.IsEnabled = true;
        }
    }

    private async void TimeDisplayMode_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded) return;
        if (TimeDisplayModeBox.SelectedItem is not ComboBoxItem item) return;
        var tag = item.Tag?.ToString();
        var mode = tag switch
        {
            "LocalTime" => TimeDisplayMode.LocalTime,
            "UTC" => TimeDisplayMode.UTC,
            _ => TimeDisplayMode.ServerTime
        };
        if (mode == ServerTimeHelper.CurrentDisplayMode) return;

        // Re-convert custom range pickers from old display mode to new.
        // Suppress refreshes while updating pickers to avoid cascading queries.
        var oldMode = ServerTimeHelper.CurrentDisplayMode;
        _isRefreshing = true;
        try
        {
            if (IsCustomRange)
            {
                var fromPicker = GetDateTimeFromPickers(FromDatePicker!, FromHourCombo, FromMinuteCombo);
                var toPicker = GetDateTimeFromPickers(ToDatePicker!, ToHourCombo, ToMinuteCombo);
                if (fromPicker.HasValue && toPicker.HasValue)
                {
                    var fromServer = ServerTimeHelper.DisplayTimeToServerTime(fromPicker.Value, oldMode);
                    var toServer = ServerTimeHelper.DisplayTimeToServerTime(toPicker.Value, oldMode);
                    ServerTimeHelper.CurrentDisplayMode = mode;
                    var fromNew = ServerTimeHelper.ConvertForDisplay(fromServer, mode);
                    var toNew = ServerTimeHelper.ConvertForDisplay(toServer, mode);
                    FromDatePicker.SelectedDate = fromNew.Date;
                    FromHourCombo.SelectedIndex = fromNew.Hour;
                    FromMinuteCombo.SelectedIndex = fromNew.Minute / 15;
                    ToDatePicker.SelectedDate = toNew.Date;
                    ToHourCombo.SelectedIndex = toNew.Hour;
                    ToMinuteCombo.SelectedIndex = toNew.Minute / 15;
                }
                else
                {
                    ServerTimeHelper.CurrentDisplayMode = mode;
                }
            }
            else
            {
                ServerTimeHelper.CurrentDisplayMode = mode;
            }
        }
        finally
        {
            _isRefreshing = false;
        }

        // Refresh all DataGrid bindings so ServerTimeConverter re-evaluates
        QuerySnapshotsGrid.Items.Refresh();
        QueryStatsGrid.Items.Refresh();
        ProcedureStatsGrid.Items.Refresh();
        QueryStoreGrid.Items.Refresh();
        BlockedProcessReportGrid.Items.Refresh();
        DeadlockGrid.Items.Refresh();
        RunningJobsGrid.Items.Refresh();
        CollectionHealthGrid.Items.Refresh();
        CollectionLogGrid.Items.Refresh();

        // Refresh slicer labels
        ActiveQueriesSlicer.Redraw();
        QueryStatsSlicer.Redraw();
        ProcStatsSlicer.Redraw();
        QueryStoreSlicer.Redraw();
        BlockingSlicer.Redraw();
        DeadlockSlicer.Redraw();

        /* #1831: the chart axes convert at render time through the shared formatter, but nothing
           re-rendered them on a toggle flip — the new mode only showed after the next data cycle,
           which read as "refresh doesn't help" in the field (refresh re-plotted server time under
           the OLD un-converting formatter; now it re-plots and converts). Re-plot everything, the
           same full refresh a range change does — the Viewer's toggle ends with
           RefreshActiveInnerTabAsync for the same reason. */
        await RefreshAllDataAsync();
    }

    private async void TimeRangeCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded || _isRefreshing) return;

        /* Show/hide custom date pickers and time ComboBoxes */
        var isCustom = TimeRangeCombo.SelectedIndex == 5;
        var visibility = isCustom ? Visibility.Visible : Visibility.Collapsed;

        if (FromDatePicker != null)
        {
            FromDatePicker.Visibility = visibility;
            FromHourCombo.Visibility = visibility;
            FromMinuteCombo.Visibility = visibility;
            ToLabel.Visibility = visibility;
            ToDatePicker.Visibility = visibility;
            ToHourCombo.Visibility = visibility;
            ToMinuteCombo.Visibility = visibility;

            if (isCustom && FromDatePicker.SelectedDate == null)
            {
                FromDatePicker.SelectedDate = DateTime.Today.AddDays(-1);
                ToDatePicker.SelectedDate = DateTime.Today;
            }

            if (!isCustom)
            {
                /* #2154: a DatePicker's calendar dropdown is a POPUP, which lives outside the visual
                   tree's visibility — collapsing the picker does not close an already-open dropdown,
                   so backing out of Custom Range without picking a date left an orphaned floating
                   calendar on screen. Close them explicitly alongside the collapse. */
                FromDatePicker.IsDropDownOpen = false;
                ToDatePicker.IsDropDownOpen = false;
            }
        }

        if (!isCustom)
        {
            /* #2640: remember the choice. Before this the picker was write-only — the settings key
               default_time_range_hours existed and was read at startup, but the only thing that wrote it
               was the Settings window, so choosing "Last 7 days" here and restarting came back at four
               hours with nothing to explain why. A control that offers a choice and discards it reads as
               broken, and the reporter read it that way.

               Custom Range is deliberately NOT persisted: it has no hours value to store, and restoring a
               window that ended two days ago would be worse than restoring nothing — the app would open
               showing an empty chart of a range the operator has moved on from. */
            PersistSelectedTimeRange();

            await RefreshAllDataAsync();
        }
    }

    /// <summary>
    /// Writes the picked range to <c>default_time_range_hours</c>, the same key the Settings window writes
    /// and startup reads, so the two cannot disagree about what the range means. Failure is logged by
    /// <see cref="App.WriteSetting"/> and never interrupts the refresh — a settings file that cannot be
    /// written must not stop the user looking at data.
    /// </summary>
    private void PersistSelectedTimeRange()
    {
        var hours = TimeRangeCombo.SelectedIndex switch
        {
            0 => 1,
            1 => 4,
            2 => 12,
            3 => 24,
            4 => 168,
            _ => 0,
        };

        if (hours == 0)
        {
            return;
        }

        /* The in-memory value too, not only the file: a second server tab opened in this same session
           reads App.DefaultTimeRangeHours in its constructor, and a tab that opens on a different range
           from the one just chosen is the same complaint in a smaller window. */
        App.DefaultTimeRangeHours = hours;

        App.WriteSetting("time range", root => root["default_time_range_hours"] = hours);
    }

    private async void CustomDateRange_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded || _isRefreshing) return;
        if (FromDatePicker?.SelectedDate != null && ToDatePicker?.SelectedDate != null)
        {
            await RefreshAllDataAsync();
        }
    }

    private async void CustomTimeCombo_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded || _isRefreshing) return;
        /* Only refresh if we have valid dates selected */
        if (FromDatePicker?.SelectedDate != null && ToDatePicker?.SelectedDate != null)
        {
            await RefreshAllDataAsync();
        }
    }

    private void DatePicker_CalendarOpened(object sender, RoutedEventArgs e)
    {
        if (sender is DatePicker datePicker)
        {
            /* Use Dispatcher to ensure visual tree is ready */
            Dispatcher.BeginInvoke(new Action(() =>
            {
                var popup = datePicker.Template.FindName("PART_Popup", datePicker) as System.Windows.Controls.Primitives.Popup;
                if (popup?.Child is System.Windows.Controls.Calendar calendar)
                {
                    ApplyThemeToCalendar(calendar);
                }
            }));
        }
    }

    private void ApplyThemeToCalendar(System.Windows.Controls.Calendar calendar)
    {
        SolidColorBrush primaryBg, fg, borderBrush;

        if (ThemeManager.CurrentTheme == "CoolBreeze")
        {
            primaryBg   = new SolidColorBrush((System.Windows.Media.Color)ColorConverter.ConvertFromString("#EEF4FA")!);
            fg          = new SolidColorBrush((System.Windows.Media.Color)ColorConverter.ConvertFromString("#1A2A3A")!);
            borderBrush = new SolidColorBrush((System.Windows.Media.Color)ColorConverter.ConvertFromString("#A8BDD0")!);
        }
        else if (ThemeManager.HasLightBackground)
        {
            primaryBg   = new SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xFF, 0xFF));
            fg          = new SolidColorBrush(System.Windows.Media.Color.FromRgb(0x1A, 0x1D, 0x23));
            borderBrush = new SolidColorBrush(System.Windows.Media.Color.FromRgb(0xDE, 0xE2, 0xE6));
        }
        else
        {
            primaryBg   = new SolidColorBrush((System.Windows.Media.Color)ColorConverter.ConvertFromString("#111217")!);
            fg          = new SolidColorBrush((System.Windows.Media.Color)ColorConverter.ConvertFromString("#E4E6EB")!);
            borderBrush = new SolidColorBrush((System.Windows.Media.Color)ColorConverter.ConvertFromString("#2a2d35")!);
        }

        calendar.Background = primaryBg;
        calendar.Foreground = fg;
        calendar.BorderBrush = borderBrush;

        ApplyThemeRecursively(calendar, primaryBg, fg);
    }

    private void ApplyThemeRecursively(DependencyObject parent, Brush primaryBg, Brush fg)
    {
        bool HasLightBackground = ThemeManager.HasLightBackground;
        for (int i = 0; i < VisualTreeHelper.GetChildrenCount(parent); i++)
        {
            var child = VisualTreeHelper.GetChild(parent, i);

            if (child is System.Windows.Controls.Primitives.CalendarItem calendarItem)
            {
                calendarItem.Background = primaryBg;
                calendarItem.Foreground = fg;
            }
            else if (child is System.Windows.Controls.Primitives.CalendarDayButton dayButton)
            {
                dayButton.Background = Brushes.Transparent;
                dayButton.Foreground = fg;
            }
            else if (child is System.Windows.Controls.Primitives.CalendarButton calButton)
            {
                calButton.Background = Brushes.Transparent;
                calButton.Foreground = fg;
            }
            else if (child is Button button)
            {
                button.Background = Brushes.Transparent;
                button.Foreground = fg;
            }
            else if (child is TextBlock textBlock)
            {
                textBlock.Foreground = fg;
            }
            else if (!HasLightBackground)
            {
                if (child is Border border && border.Background is SolidColorBrush bg && bg.Color.R > 200 && bg.Color.G > 200 && bg.Color.B > 200)
                    border.Background = primaryBg;
                else if (child is Grid grid && grid.Background is SolidColorBrush gridBg && gridBg.Color.R > 200 && gridBg.Color.G > 200 && gridBg.Color.B > 200)
                    grid.Background = primaryBg;
            }

            ApplyThemeRecursively(child, primaryBg, fg);
        }
    }
}
