/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using PerformanceMonitorDashboard.Helpers;

namespace PerformanceMonitorDashboard
{
    public partial class ServerTab : UserControl
    {
        // ── Critical Issues → Investigate Navigation (#684) ──

        private async void OnInvestigateCriticalIssue(string problemArea, DateTime logDate, string? affectedDatabase, string? investigateQuery)
        {
            // Set a 2-hour window centered on the incident
            var from = logDate.AddHours(-1);
            var to = logDate.AddHours(1);

            // Populate global custom date pickers so user can Apply to All
            SetPickersFromDateTime(from, GlobalFromDate, GlobalFromHour, GlobalFromMinute);
            SetPickersFromDateTime(to, GlobalToDate, GlobalToHour, GlobalToMinute);

            // Set global range so Apply to All works
            _globalHoursBack = 0;
            _globalFromDate = from;
            _globalToDate = to;
            HighlightTimeButton(0); // deselect preset buttons
            GlobalDateRangeIndicator.Text = GetGlobalDateRangeText();

            switch (problemArea)
            {
                case "Blocking":
                    LockingTabItem.IsSelected = true;
                    LockingSubTabControl.SelectedIndex = 2; // Blocked Process Reports
                    _blockingHoursBack = 0;
                    _blockingFromDate = from;
                    _blockingToDate = to;
                    await RefreshLockingTabAsync();
                    break;

                case "Deadlocking":
                    LockingTabItem.IsSelected = true;
                    LockingSubTabControl.SelectedIndex = 3; // Deadlocks
                    _deadlocksHoursBack = 0;
                    _deadlocksFromDate = from;
                    _deadlocksToDate = to;
                    await RefreshLockingTabAsync();
                    break;

                case "Blocking and Deadlocking":
                    LockingTabItem.IsSelected = true;
                    LockingSubTabControl.SelectedIndex = 0; // Blocking/Deadlock Trends
                    _blockingStatsHoursBack = 0;
                    _blockingStatsFromDate = from;
                    _blockingStatsToDate = to;
                    await RefreshLockingTabAsync();
                    break;

                case "CPU Scheduling Pressure":
                    QueriesTabItem.IsSelected = true;
                    PerformanceTab.SetTimeRange(0, from, to);
                    await RefreshQueriesTabAsync();
                    break;

                case "Memory Pressure":
                case "Memory Grant Pressure":
                case "Memory Clerk Growth":
                    MemoryTabItem.IsSelected = true;
                    MemoryTab.SetTimeRange(0, from, to);
                    await RefreshMemoryTabAsync();
                    break;

                default:
                    ShowInvestigateQuery(problemArea, logDate, affectedDatabase, investigateQuery);
                    break;
            }
        }

        private void ShowInvestigateQuery(string problemArea, DateTime logDate, string? affectedDatabase, string? investigateQuery)
        {
            var query = investigateQuery;
            if (string.IsNullOrWhiteSpace(query))
            {
                query = $"-- No investigate query available for: {problemArea}";
            }

            var header = $"{problemArea}";
            if (!string.IsNullOrWhiteSpace(affectedDatabase))
                header += $" — {affectedDatabase}";
            header += $"\n{Helpers.ServerTimeHelper.ConvertForDisplay(logDate, Helpers.ServerTimeHelper.CurrentDisplayMode):yyyy-MM-dd HH:mm:ss}";

            var win = new Window
            {
                Title = "Investigate: " + problemArea,
                Width = 700,
                Height = 400,
                WindowStartupLocation = WindowStartupLocation.CenterOwner,
                Owner = Window.GetWindow(this),
                Background = (System.Windows.Media.Brush)FindResource("BackgroundBrush")
            };

            var grid = new Grid { Margin = new Thickness(12) };
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
            grid.RowDefinitions.Add(new RowDefinition { Height = new GridLength(1, GridUnitType.Star) });
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });

            var headerTb = new TextBlock
            {
                Text = header,
                FontSize = 13,
                FontWeight = FontWeights.SemiBold,
                Foreground = (System.Windows.Media.Brush)FindResource("ForegroundBrush"),
                Margin = new Thickness(0, 0, 0, 8)
            };
            Grid.SetRow(headerTb, 0);
            grid.Children.Add(headerTb);

            var queryBox = new TextBox
            {
                Text = query,
                IsReadOnly = true,
                TextWrapping = TextWrapping.Wrap,
                VerticalScrollBarVisibility = ScrollBarVisibility.Auto,
                FontFamily = new System.Windows.Media.FontFamily("Consolas"),
                FontSize = 12,
                Background = (System.Windows.Media.Brush)FindResource("BackgroundLightBrush"),
                Foreground = (System.Windows.Media.Brush)FindResource("ForegroundBrush"),
                Padding = new Thickness(8)
            };
            Grid.SetRow(queryBox, 1);
            grid.Children.Add(queryBox);

            var buttonPanel = new StackPanel { Orientation = Orientation.Horizontal, HorizontalAlignment = HorizontalAlignment.Right, Margin = new Thickness(0, 8, 0, 0) };
            var copyBtn = new Button { Content = "Copy to Clipboard", Padding = new Thickness(12, 4, 12, 4), Margin = new Thickness(0, 0, 8, 0) };
            copyBtn.Click += (_, _) => { Clipboard.SetText(query ?? ""); };
            var closeBtn = new Button { Content = "Close", Padding = new Thickness(12, 4, 12, 4) };
            closeBtn.Click += (_, _) => win.Close();
            buttonPanel.Children.Add(copyBtn);
            buttonPanel.Children.Add(closeBtn);
            Grid.SetRow(buttonPanel, 2);
            grid.Children.Add(buttonPanel);

            win.Content = grid;
            win.ShowDialog();
        }

        // ── Chart Drill-Down (#682) ──

        private void AddChartDrillDownMenuItem(
            ScottPlot.WPF.WpfPlot chart, ContextMenu contextMenu,
            Helpers.ChartHoverHelper? hover, string label, Action<DateTime> handler)
        {
            contextMenu.Items.Insert(0, new Separator());
            var item = new MenuItem { Header = label };
            contextMenu.Items.Insert(0, item);

            contextMenu.Opened += (s, _) =>
            {
                var pos = System.Windows.Input.Mouse.GetPosition(chart);
                var nearest = hover?.GetNearestSeries(pos);
                if (nearest.HasValue)
                {
                    item.Tag = nearest.Value.Time;
                    item.IsEnabled = true;
                }
                else
                {
                    item.Tag = null;
                    item.IsEnabled = false;
                }
            };

            item.Click += (s, _) =>
            {
                if (item.Tag is DateTime time)
                    handler(time);
            };
        }

        private void SetDrillDownGlobalRange(DateTime from, DateTime to)
        {
            SetPickersFromDateTime(from, GlobalFromDate, GlobalFromHour, GlobalFromMinute);
            SetPickersFromDateTime(to, GlobalToDate, GlobalToHour, GlobalToMinute);
            _globalHoursBack = 0;
            _globalFromDate = from;
            _globalToDate = to;
            HighlightTimeButton(0);
            GlobalDateRangeIndicator.Text = GetGlobalDateRangeText();
        }

        private async void OnBlockingChartDrillDown(DateTime time)
        {
            var from = time.AddMinutes(-30);
            var to = time.AddMinutes(30);
            SetDrillDownGlobalRange(from, to);

            LockingSubTabControl.SelectedIndex = 2; // Blocked Process Reports
            _blockingHoursBack = 0;
            _blockingFromDate = from;
            _blockingToDate = to;
            await RefreshLockingTabAsync();
        }

        private async void OnDeadlockChartDrillDown(DateTime time)
        {
            var from = time.AddMinutes(-30);
            var to = time.AddMinutes(30);
            SetDrillDownGlobalRange(from, to);

            LockingSubTabControl.SelectedIndex = 3; // Deadlocks
            _deadlocksHoursBack = 0;
            _deadlocksFromDate = from;
            _deadlocksToDate = to;
            await RefreshLockingTabAsync();
        }

        private async void OnQueryDrillDown(DateTime time)
        {
            var from = time.AddMinutes(-30);
            var to = time.AddMinutes(30);
            SetDrillDownGlobalRange(from, to);

            QueriesTabItem.IsSelected = true;
            PerformanceTab.SelectSubTab(1); // Active Queries
            PerformanceTab.SetTimeRange(0, from, to);
            PerformanceTab.IsRefreshing = true;
            try { await RefreshQueriesTabAsync(); }
            finally { PerformanceTab.IsRefreshing = false; }
        }

        private async void OnChildChartDrillDown(string chartType, DateTime time)
        {
            var from = time.AddMinutes(-30);
            var to = time.AddMinutes(30);
            SetDrillDownGlobalRange(from, to);

            QueriesTabItem.IsSelected = true;
            PerformanceTab.SelectSubTab(1); // Active Queries
            PerformanceTab.SetTimeRange(0, from, to);
            PerformanceTab.IsRefreshing = true;
            try { await RefreshQueriesTabAsync(); }
            finally { PerformanceTab.IsRefreshing = false; }
        }
    }
}
