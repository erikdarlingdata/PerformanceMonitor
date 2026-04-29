/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitorDashboard.Helpers;

namespace PerformanceMonitorDashboard
{
    public partial class ServerTab : UserControl
    {
        // ====================================================================
        // Global Time Range Controls
        // ====================================================================

        private int _globalHoursBack = 24;
        private DateTime? _globalFromDate = null;
        private DateTime? _globalToDate = null;

        // Original range tracking for zoom/reset functionality
        private int? _originalHoursBack = null;
        private DateTime? _originalFromDate = null;
        private DateTime? _originalToDate = null;
        private bool _isZoomed = false;

        private void InitializeDefaultTimeRanges()
        {
            var prefs = _preferencesService.GetPreferences();
            int defaultHours = prefs.DefaultHoursBack;

            // Initialize query logging settings
            Helpers.QueryLogger.SetEnabled(prefs.LogSlowQueries);
            Helpers.QueryLogger.SetThreshold(prefs.SlowQueryThresholdSeconds);

            // Initialize global time range to user's preferred default
            _globalHoursBack = defaultHours;

            // Initialize time picker ComboBoxes
            InitializeTimeComboBoxes();

            // Initialize all hours-back fields to the user's preferred default
            _collectionHealthHoursBack = defaultHours;
            _blockingHoursBack = defaultHours;
            _deadlocksHoursBack = defaultHours;
            _blockingStatsHoursBack = defaultHours;
            // Performance tab state variables now managed by QueryPerformanceContent UserControl
            // Memory state variables now managed by MemoryContent UserControl
            ConfigChangesTab.SetTimeRange(defaultHours, null, null);
            // _sessionStatsHoursBack and _queryPerfTrendsHoursBack now managed by QueryPerformanceContent UserControl
            // _criticalIssuesHoursBack now managed by CriticalIssuesTab UserControl
            // System Health/HealthParser state now managed by SystemEventsContent UserControl
            SystemEventsContent.SetTimeRange(defaultHours);
            // Resource Metrics state now managed by ResourceMetricsContent UserControl
            ResourceMetricsContent.SetTimeRange(defaultHours);
        }

        private void InitializeTimeComboBoxes()
        {
            // Populate hour ComboBoxes (12-hour format with AM/PM)
            var hours = new List<string>();
            for (int h = 0; h < 24; h++)
            {
                var dt = DateTime.Today.AddHours(h);
                hours.Add(dt.ToString("HH:00")); // "00:00", "01:00", ..., "23:00"
            }

            GlobalFromHour.ItemsSource = hours;
            GlobalToHour.ItemsSource = hours;
            GlobalFromHour.SelectedIndex = 0;  // Default to 12 AM
            GlobalToHour.SelectedIndex = 23;   // Default to 11 PM

            // Populate minute ComboBoxes (15-minute intervals)
            var minutes = new List<string> { ":00", ":15", ":30", ":45" };
            GlobalFromMinute.ItemsSource = minutes;
            GlobalToMinute.ItemsSource = minutes;
            GlobalFromMinute.SelectedIndex = 0; // Default to :00
            GlobalToMinute.SelectedIndex = 3;   // Default to :45 (so 11:45 PM is end)
        }

        private DateTime? GetDateTimeFromPickers(DatePicker datePicker, ComboBox hourCombo, ComboBox minuteCombo)
        {
            if (!datePicker.SelectedDate.HasValue) return null;

            var date = datePicker.SelectedDate.Value.Date;
            int hour = hourCombo.SelectedIndex >= 0 ? hourCombo.SelectedIndex : 0;
            int minute = minuteCombo.SelectedIndex >= 0 ? minuteCombo.SelectedIndex * 15 : 0;

            return date.AddHours(hour).AddMinutes(minute);
        }

        private void SetPickersFromDateTime(DateTime serverTime, DatePicker datePicker, ComboBox hourCombo, ComboBox minuteCombo)
        {
            // Display in the current time mode (server time, local time, or UTC)
            var displayTime = Helpers.ServerTimeHelper.ConvertForDisplay(serverTime, Helpers.ServerTimeHelper.CurrentDisplayMode);
            datePicker.SelectedDate = displayTime.Date;
            hourCombo.SelectedIndex = displayTime.Hour;
            minuteCombo.SelectedIndex = displayTime.Minute / 15;
        }

        private async void GlobalTimeRange_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (sender is Button button && button.Tag is string hoursStr)
                {
                    _globalHoursBack = int.Parse(hoursStr, CultureInfo.InvariantCulture);
                    _globalFromDate = null;
                    _globalToDate = null;

                    // Clear any zoom state when user clicks a time button
                    ClearZoomStateWithoutRefresh();

                    // Update button visual states
                    HighlightTimeButton(_globalHoursBack);

                    // Clear custom date/time pickers
                    GlobalFromDate.SelectedDate = null;
                    GlobalToDate.SelectedDate = null;

                    // Update status indicator
                    GlobalDateRangeIndicator.Text = GetGlobalDateRangeText();

                    // Apply to current tab and refresh it
                    await ApplyAndRefreshCurrentTabAsync();
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Error changing time range: {ex.Message}", ex);
                StatusText.Text = "Error changing time range";
            }
        }

        private async void GlobalCustomDateTime_Changed(object sender, SelectionChangedEventArgs e)
        {
            if (_suppressPickerUpdates) return;
            await UpdateGlobalDateTimeRange();
        }

        private async void GlobalTimeCombo_Changed(object sender, SelectionChangedEventArgs e)
        {
            if (_suppressPickerUpdates) return;
            // Only update if both dates are selected (time change alone isn't meaningful without dates)
            if (GlobalFromDate.SelectedDate.HasValue && GlobalToDate.SelectedDate.HasValue)
            {
                await UpdateGlobalDateTimeRange();
            }
        }

        private async Task UpdateGlobalDateTimeRange()
        {
            try
            {
                var fromDateTime = GetDateTimeFromPickers(GlobalFromDate, GlobalFromHour, GlobalFromMinute);
                var toDateTime = GetDateTimeFromPickers(GlobalToDate, GlobalToHour, GlobalToMinute);

                if (fromDateTime.HasValue && toDateTime.HasValue)
                {
                    /* Convert display-mode time back to server time — pickers show time
                       in the current display mode (server, local, or UTC) */
                    _globalFromDate = Helpers.ServerTimeHelper.DisplayTimeToServerTime(fromDateTime.Value, Helpers.ServerTimeHelper.CurrentDisplayMode);
                    _globalToDate = Helpers.ServerTimeHelper.DisplayTimeToServerTime(toDateTime.Value, Helpers.ServerTimeHelper.CurrentDisplayMode);

                    if (_globalFromDate > _globalToDate)
                    {
                        MessageBox.Show("Start date/time cannot be after end date/time.", "Invalid Date Range", MessageBoxButton.OK, MessageBoxImage.Warning);
                        GlobalFromDate.SelectedDate = null;
                        GlobalToDate.SelectedDate = null;
                        return;
                    }

                    // Clear any zoom state when user manually changes date pickers
                    ClearZoomStateWithoutRefresh();

                    // Clear button selection
                    ClearTimeButtonHighlights();

                    _globalHoursBack = 0;
                    GlobalDateRangeIndicator.Text = GetGlobalDateRangeText();

                    // Apply to current tab and refresh it
                    await ApplyAndRefreshCurrentTabAsync();
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Error applying custom date range: {ex.Message}", ex);
                StatusText.Text = "Error applying date range";
            }
        }

        private void DatePicker_CalendarOpened(object sender, RoutedEventArgs e)
        {
            if (sender is DatePicker datePicker)
            {
                // Use BeginInvoke to ensure visual tree is ready
                Dispatcher.BeginInvoke(new Action(() =>
                {
                    // Get the Popup and Calendar from the DatePicker template
                    var popup = datePicker.Template.FindName("PART_Popup", datePicker) as System.Windows.Controls.Primitives.Popup;
                    if (popup?.Child is System.Windows.Controls.Calendar calendar)
                    {
                        TabHelpers.ApplyThemeToCalendar(calendar);
                    }
                }));
            }
        }

        private async void ApplyToAllTabs_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                // Apply the global time range to all tab-specific fields (ServerTab's own fields)
                ApplyGlobalRangeToAllTabs();

                // Apply the global time range to all extracted UserControls
                PerformanceTab.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                MemoryTab.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                ResourceMetricsContent.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                SystemEventsContent.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                CriticalIssuesTab.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                DefaultTraceTab.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);

                // Refresh all data
                StatusText.Text = GetLoadingMessage();
                await LoadDataAsync();
                StatusText.Text = "Time range applied to all tabs";
            }
            catch (Exception ex)
            {
                Logger.Error($"Error applying time range to all tabs: {ex.Message}", ex);
                StatusText.Text = "Error applying time range";
            }
        }

        private string GetGlobalDateRangeText()
        {
            DateTime from, to;

            if (_globalFromDate.HasValue && _globalToDate.HasValue)
            {
                from = _globalFromDate.Value;
                to = _globalToDate.Value;
            }
            else
            {
                // Calculate actual range from hours back using server time
                to = Helpers.ServerTimeHelper.ServerNow;
                from = to.AddHours(-_globalHoursBack);
            }

            return FormatDateRange("Showing", from, to);
        }

        private string GetOriginalRangeText()
        {
            DateTime from, to;

            if (_originalFromDate.HasValue && _originalToDate.HasValue)
            {
                from = _originalFromDate.Value;
                to = _originalToDate.Value;
            }
            else if (_originalHoursBack.HasValue)
            {
                to = Helpers.ServerTimeHelper.ServerNow;
                from = to.AddHours(-_originalHoursBack.Value);
            }
            else
            {
                return "";
            }

            return FormatDateRange("Original", from, to);
        }

        private static string FormatDateRange(string prefix, DateTime from, DateTime to)
        {
            var tz = Helpers.ServerTimeHelper.GetTimezoneLabel(Helpers.ServerTimeHelper.CurrentDisplayMode);
            var displayFrom = Helpers.ServerTimeHelper.ConvertForDisplay(from, Helpers.ServerTimeHelper.CurrentDisplayMode);
            var displayTo = Helpers.ServerTimeHelper.ConvertForDisplay(to, Helpers.ServerTimeHelper.CurrentDisplayMode);

            // Same day: "Feb 7, 2:15 PM – 3:15 PM (PST)"
            if (displayFrom.Date == displayTo.Date)
            {
                return $"{prefix}: {displayFrom:MMM d, h:mm tt} – {displayTo:h:mm tt} ({tz})";
            }

            // Same year, different days: "Feb 6, 3:15 PM – Feb 7, 3:15 PM (PST)"
            if (displayFrom.Year == displayTo.Year)
            {
                return $"{prefix}: {displayFrom:MMM d, h:mm tt} – {displayTo:MMM d, h:mm tt} ({tz})";
            }

            // Different years: "Dec 31, 2025, 11:00 PM – Jan 1, 2026, 11:00 PM (PST)"
            return $"{prefix}: {displayFrom:MMM d, yyyy, h:mm tt} – {displayTo:MMM d, yyyy, h:mm tt} ({tz})";
        }

        private void StoreOriginalRangeIfNeeded()
        {
            if (!_isZoomed)
            {
                // Store current range as original before zooming
                _originalHoursBack = _globalHoursBack;
                _originalFromDate = _globalFromDate;
                _originalToDate = _globalToDate;
            }
        }

        private async Task ZoomToTimeRange(DateTime from, DateTime to)
        {
            // Store original if this is the first zoom
            StoreOriginalRangeIfNeeded();

            // Update global range to the zoomed range
            _globalFromDate = from;
            _globalToDate = to;
            _isZoomed = true;

            // Update date/time pickers with full datetime
            SetPickersFromDateTime(from, GlobalFromDate, GlobalFromHour, GlobalFromMinute);
            SetPickersFromDateTime(to, GlobalToDate, GlobalToHour, GlobalToMinute);

            // Clear button highlighting since we're using custom range
            ClearTimeButtonHighlights();

            // Update indicators
            GlobalDateRangeIndicator.Text = GetGlobalDateRangeText();
            var originalText = GetOriginalRangeText();
            if (!string.IsNullOrEmpty(originalText))
            {
                OriginalRangeIndicator.Text = "Original: " + originalText;
                OriginalRangeIndicator.Visibility = Visibility.Visible;
                RevertHintText.Visibility = Visibility.Visible;
            }
            else
            {
                OriginalRangeIndicator.Visibility = Visibility.Collapsed;
                RevertHintText.Visibility = Visibility.Collapsed;
            }

            // Refresh current tab
            await ApplyAndRefreshCurrentTabAsync();
        }

        private async Task ResetToOriginalRange()
        {
            if (!_isZoomed) return;

            // Restore original range
            if (_originalFromDate.HasValue && _originalToDate.HasValue)
            {
                _globalFromDate = _originalFromDate;
                _globalToDate = _originalToDate;
                SetPickersFromDateTime(_originalFromDate.Value, GlobalFromDate, GlobalFromHour, GlobalFromMinute);
                SetPickersFromDateTime(_originalToDate.Value, GlobalToDate, GlobalToHour, GlobalToMinute);
                ClearTimeButtonHighlights();
            }
            else if (_originalHoursBack.HasValue)
            {
                _globalHoursBack = _originalHoursBack.Value;
                _globalFromDate = null;
                _globalToDate = null;
                GlobalFromDate.SelectedDate = null;
                GlobalToDate.SelectedDate = null;
                HighlightTimeButton(_originalHoursBack.Value);
            }

            // Clear zoom state
            _isZoomed = false;
            _originalHoursBack = null;
            _originalFromDate = null;
            _originalToDate = null;

            // Update indicators
            GlobalDateRangeIndicator.Text = GetGlobalDateRangeText();
            OriginalRangeIndicator.Text = "";
            OriginalRangeIndicator.Visibility = Visibility.Collapsed;
            RevertHintText.Visibility = Visibility.Collapsed;

            // Refresh current tab
            await ApplyAndRefreshCurrentTabAsync();
        }

        private void ClearZoomStateWithoutRefresh()
        {
            _isZoomed = false;
            _originalHoursBack = null;
            _originalFromDate = null;
            _originalToDate = null;
            OriginalRangeIndicator.Text = "";
            OriginalRangeIndicator.Visibility = Visibility.Collapsed;
            RevertHintText.Visibility = Visibility.Collapsed;
        }

        private void ClearTimeButtonHighlights()
        {
            GlobalLast1HourButton.FontWeight = FontWeights.Normal;
            GlobalLast1HourButton.ClearValue(Control.BackgroundProperty);
            GlobalLast4HoursButton.FontWeight = FontWeights.Normal;
            GlobalLast4HoursButton.ClearValue(Control.BackgroundProperty);
            GlobalLast8HoursButton.FontWeight = FontWeights.Normal;
            GlobalLast8HoursButton.ClearValue(Control.BackgroundProperty);
            GlobalLast12HoursButton.FontWeight = FontWeights.Normal;
            GlobalLast12HoursButton.ClearValue(Control.BackgroundProperty);
            GlobalLast24HoursButton.FontWeight = FontWeights.Normal;
            GlobalLast24HoursButton.ClearValue(Control.BackgroundProperty);
            GlobalLast7DaysButton.FontWeight = FontWeights.Normal;
            GlobalLast7DaysButton.ClearValue(Control.BackgroundProperty);
            GlobalLast30DaysButton.FontWeight = FontWeights.Normal;
            GlobalLast30DaysButton.ClearValue(Control.BackgroundProperty);
        }

        private void HighlightTimeButton(int hours)
        {
            ClearTimeButtonHighlights();
            // Use accent color (#2eaef1) for selected button
            var highlightBrush = new SolidColorBrush(Color.FromRgb(0x2E, 0xAE, 0xF1));
            switch (hours)
            {
                case 1:
                    GlobalLast1HourButton.FontWeight = FontWeights.Bold;
                    GlobalLast1HourButton.Background = highlightBrush;
                    break;
                case 4:
                    GlobalLast4HoursButton.FontWeight = FontWeights.Bold;
                    GlobalLast4HoursButton.Background = highlightBrush;
                    break;
                case 8:
                    GlobalLast8HoursButton.FontWeight = FontWeights.Bold;
                    GlobalLast8HoursButton.Background = highlightBrush;
                    break;
                case 12:
                    GlobalLast12HoursButton.FontWeight = FontWeights.Bold;
                    GlobalLast12HoursButton.Background = highlightBrush;
                    break;
                case 24:
                    GlobalLast24HoursButton.FontWeight = FontWeights.Bold;
                    GlobalLast24HoursButton.Background = highlightBrush;
                    break;
                case 168:
                    GlobalLast7DaysButton.FontWeight = FontWeights.Bold;
                    GlobalLast7DaysButton.Background = highlightBrush;
                    break;
                case 720:
                    GlobalLast30DaysButton.FontWeight = FontWeights.Bold;
                    GlobalLast30DaysButton.Background = highlightBrush;
                    break;
            }
        }

        private void ApplyGlobalRangeToAllTabs()
        {
            // Apply global settings to all per-tab time range fields
            // Collection Health
            _collectionHealthHoursBack = _globalHoursBack;
            _collectionHealthFromDate = _globalFromDate;
            _collectionHealthToDate = _globalToDate;

            // Resource Overview (on Overview tab)
            _resourceOverviewHoursBack = _globalHoursBack;
            _resourceOverviewFromDate = _globalFromDate;
            _resourceOverviewToDate = _globalToDate;

            // Blocking
            _blockingHoursBack = _globalHoursBack;
            _blockingFromDate = _globalFromDate;
            _blockingToDate = _globalToDate;

            // Deadlocks
            _deadlocksHoursBack = _globalHoursBack;
            _deadlocksFromDate = _globalFromDate;
            _deadlocksToDate = _globalToDate;

            // Blocking Stats
            _blockingStatsHoursBack = _globalHoursBack;
            _blockingStatsFromDate = _globalFromDate;
            _blockingStatsToDate = _globalToDate;

        }

        /// <summary>
        /// Extracts the text from a TabItem's header, handling both simple string headers
        /// and complex headers (like StackPanel with TextBlock for tabs with badges).
        /// </summary>
        private static string GetTabHeaderText(TabItem tabItem)
        {
            if (tabItem.Header is string headerString)
                return headerString;

            if (tabItem.Header is StackPanel stackPanel)
            {
                var textBlock = stackPanel.Children.OfType<TextBlock>().FirstOrDefault();
                if (textBlock != null)
                    return textBlock.Text;
            }

            return tabItem.Header?.ToString() ?? "";
        }

        private async Task ApplyAndRefreshCurrentTabAsync()
        {
            if (_databaseService == null) return;

            // Get the current tab
            var selectedTab = DataTabControl.SelectedItem as TabItem;
            if (selectedTab == null) return;

            var tabHeader = GetTabHeaderText(selectedTab);
            StatusText.Text = GetLoadingMessage();

            try
            {
                switch (tabHeader)
                {
                    case "Overview":
                        // Overview tab has Collection Health, Daily Summary, Critical Issues, Resource Overview sub-tabs
                        _collectionHealthHoursBack = _globalHoursBack;
                        _collectionHealthFromDate = _globalFromDate;
                        _collectionHealthToDate = _globalToDate;
                        _resourceOverviewHoursBack = _globalHoursBack;
                        _resourceOverviewFromDate = _globalFromDate;
                        _resourceOverviewToDate = _globalToDate;
                        CriticalIssuesTab.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                        DefaultTraceTab.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                        ConfigChangesTab.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                        CollectionHealth_Refresh_Click(null, new RoutedEventArgs());
                        await CriticalIssuesTab.RefreshDataAsync();
                        await DefaultTraceTab.RefreshAllDataAsync();
                        await CurrentConfigTab.RefreshAllDataAsync();
                        await ConfigChangesTab.RefreshAllDataAsync();
                        await RefreshResourceOverviewAsync();
                        break;

                    case "Locking":
                        // Locking tab has sub-tabs, refresh all of them
                        _blockingHoursBack = _globalHoursBack;
                        _blockingFromDate = _globalFromDate;
                        _blockingToDate = _globalToDate;
                        _deadlocksHoursBack = _globalHoursBack;
                        _deadlocksFromDate = _globalFromDate;
                        _deadlocksToDate = _globalToDate;
                        _blockingStatsHoursBack = _globalHoursBack;
                        _blockingStatsFromDate = _globalFromDate;
                        _blockingStatsToDate = _globalToDate;
                        Blocking_Refresh_Click(null, new RoutedEventArgs());
                        Deadlocks_Refresh_Click(null, new RoutedEventArgs());
                        BlockingStats_Refresh_Click(null, new RoutedEventArgs());
                        break;

                    case "Queries":
                        // Queries tab content is in QueryPerformanceContent UserControl
                        PerformanceTab.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                        PerformanceTab.IsRefreshing = true;
                        try { await PerformanceTab.RefreshAllDataAsync(); }
                        finally { PerformanceTab.IsRefreshing = false; }
                        break;

                    case "Memory":
                        // Memory tab content is now in MemoryContent UserControl
                        MemoryTab.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                        await MemoryTab.RefreshAllDataAsync();
                        break;

                    case "Resource Metrics":
                        // Resource Metrics tab content is now in ResourceMetricsContent UserControl
                        ResourceMetricsContent.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                        await ResourceMetricsContent.RefreshAllDataAsync();
                        break;

                    case "System Events":
                        // System Events tab - HealthParser data is handled by SystemEventsContent UserControl
                        SystemEventsContent.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                        await SystemEventsContent.RefreshAllDataAsync();
                        break;

                    default:
                        // For tabs without time range filters, just note we can't filter
                        StatusText.Text = $"{tabHeader} doesn't use time range filters";
                        return;
                }

                StatusText.Text = $"{tabHeader} refreshed with new time range";
            }
            catch (Exception ex)
            {
                StatusText.Text = $"Error refreshing {tabHeader}: {ex.Message}";
                Logger.Error($"Error refreshing {tabHeader}", ex);
            }
        }

        private void TimeDisplayMode_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
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
            // Suppress picker change handlers to avoid validation errors and cascading refreshes.
            var oldMode = ServerTimeHelper.CurrentDisplayMode;
            var fromPicker = GetDateTimeFromPickers(GlobalFromDate, GlobalFromHour, GlobalFromMinute);
            var toPicker = GetDateTimeFromPickers(GlobalToDate, GlobalToHour, GlobalToMinute);
            _suppressPickerUpdates = true;
            try
            {
                ServerTimeHelper.CurrentDisplayMode = mode;
                if (fromPicker.HasValue && toPicker.HasValue)
                {
                    var fromServer = Helpers.ServerTimeHelper.DisplayTimeToServerTime(fromPicker.Value, oldMode);
                    var toServer = Helpers.ServerTimeHelper.DisplayTimeToServerTime(toPicker.Value, oldMode);
                    SetPickersFromDateTime(fromServer, GlobalFromDate, GlobalFromHour, GlobalFromMinute);
                    SetPickersFromDateTime(toServer, GlobalToDate, GlobalToHour, GlobalToMinute);
                }
            }
            finally
            {
                _suppressPickerUpdates = false;
            }

            // Persist preference
            var prefs = _preferencesService.GetPreferences();
            prefs.TimeDisplayMode = mode.ToString();
            _preferencesService.SavePreferences(prefs);

            // Refresh all DataGrid bindings so ServerTimeConverter re-evaluates
            RefreshTimestampBindings();
        }

        private void RefreshTimestampBindings()
        {
            // Force WPF to re-evaluate converter bindings on all query performance grids
            PerformanceTab.RefreshGridBindings();

            // Refresh blocking/deadlock grids and slicers in Locking tab
            BlockingEventsDataGrid.Items.Refresh();
            DeadlocksDataGrid.Items.Refresh();
            BlockingSlicer.Redraw();
            DeadlockSlicer.Redraw();

            // Refresh Default Trace slicer
            DefaultTraceTab.RefreshTimeDisplay();
        }

        private void LoadUserPreferences()
        {
            var prefs = _preferencesService.GetPreferences();

            // Blocking - uses global time range now
            _blockingHoursBack = prefs.BlockingHoursBack;
            if (prefs.BlockingUseCustomDates && !string.IsNullOrEmpty(prefs.BlockingFromDate) && !string.IsNullOrEmpty(prefs.BlockingToDate))
            {
                _blockingFromDate = DateTime.Parse(prefs.BlockingFromDate, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                _blockingToDate = DateTime.Parse(prefs.BlockingToDate, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
            }
        }

        // Date range filtering state

        private string GetDateRangeDisplayText(int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            if (fromDate.HasValue && toDate.HasValue)
            {
                return $"Showing: Custom Range ({fromDate.Value:yyyy-MM-dd} to {toDate.Value:yyyy-MM-dd})";
            }

            return hoursBack switch
            {
                1 => "Showing: Last Hour",
                6 => "Showing: Last 6 Hours",
                24 => "Showing: Last 24 Hours",
                168 => "Showing: Last 7 Days",
                _ => $"Showing: Last {hoursBack} Hours"
            };
        }
    }
}
