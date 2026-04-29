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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using Microsoft.Win32;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;


namespace PerformanceMonitorDashboard.Controls
{
    public partial class SystemEventsContent : UserControl
    {
        #region Scheduler Issues Tab

        private async System.Threading.Tasks.Task RefreshSchedulerIssuesAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetHealthParserSchedulerIssuesAsync(_schedulerIssuesHoursBack, _schedulerIssuesFromDate, _schedulerIssuesToDate);
                // Grid removed per todo.md #13 - chart + summary only
                LoadSchedulerIssuesChart(data, _schedulerIssuesHoursBack, _schedulerIssuesFromDate, _schedulerIssuesToDate);
                UpdateSchedulerIssuesSummaryPanel(data);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading scheduler issues: {ex.Message}", ex);
            }
        }

        private void LoadSchedulerIssuesChart(IEnumerable<HealthParserSchedulerIssueItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(SchedulerIssuesChart, out var existingSchedulerIssuesPanel) && existingSchedulerIssuesPanel != null)
            {
                SchedulerIssuesChart.Plot.Axes.Remove(existingSchedulerIssuesPanel);
                _legendPanels[SchedulerIssuesChart] = null;
            }
            SchedulerIssuesChart.Plot.Clear();
            _schedulerIssuesHover?.Clear();
            TabHelpers.ApplyThemeToChart(SchedulerIssuesChart);

            var dataList = data?.Where(d => d.EventTime.HasValue).ToList() ?? new List<HealthParserSchedulerIssueItem>();
            bool hasData = false;
            if (dataList.Count > 0)
            {
                // Helper to parse NonYieldingTimeMs (it's a string)
                long ParseNonYield(string? value)
                {
                    if (string.IsNullOrEmpty(value)) return 0;
                    var numericPart = new string(value.Where(c => char.IsDigit(c) || c == '-').ToArray());
                    return long.TryParse(numericPart, out var result) ? result : 0;
                }

                // Group by hour and sum non-yielding time
                var grouped = dataList
                    .GroupBy(d => new DateTime(d.EventTime!.Value.Year, d.EventTime!.Value.Month, d.EventTime!.Value.Day, d.EventTime!.Value.Hour, 0, 0))
                    .OrderBy(g => g.Key)
                    .ToList();

                if (grouped.Count > 0)
                {
                    hasData = true;
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                        grouped.Select(g => g.Key),
                        grouped.Select(g => (double)g.Sum(i => ParseNonYield(i.NonYieldingTimeMs))));

                    var scatter = SchedulerIssuesChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = TabHelpers.ChartColors[2];
                    scatter.LegendText = "Total Non-Yield Time";
                    _schedulerIssuesHover?.Add(scatter, "Total Non-Yield Time");

                    _legendPanels[SchedulerIssuesChart] = SchedulerIssuesChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    SchedulerIssuesChart.Plot.Legend.FontSize = 12;
                }
            }

            if (!hasData)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = SchedulerIssuesChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            SchedulerIssuesChart.Plot.Axes.DateTimeTicksBottomDateChange();
            SchedulerIssuesChart.Plot.Axes.SetLimitsX(xMin, xMax);
            SchedulerIssuesChart.Plot.YLabel("Total Non-Yield Time (ms)");
            TabHelpers.LockChartVerticalAxis(SchedulerIssuesChart);
            SchedulerIssuesChart.Refresh();
        }

        private void UpdateSchedulerIssuesSummaryPanel(List<HealthParserSchedulerIssueItem> dataList)
        {
            if (dataList == null || dataList.Count == 0)
            {
                SchedulerIssuesTotalText.Text = "0";
                SchedulerIssuesTotalNonYieldText.Text = "0 ms";
                SchedulerIssuesMaxNonYieldText.Text = "0 ms";
                SchedulerIssuesSchedulersText.Text = "0";
                SchedulerIssuesOfflineText.Text = "0";
                SchedulerIssuesOfflineText.Foreground = new System.Windows.Media.SolidColorBrush(
                    (System.Windows.Media.Color)System.Windows.Media.ColorConverter.ConvertFromString("#CCCCCC"));
                return;
            }

            // Total issues count
            SchedulerIssuesTotalText.Text = dataList.Count.ToString("N0", CultureInfo.CurrentCulture);

            // Total and Max non-yield time (NonYieldingTimeMs is a string, need to parse)
            long ParseNonYieldTime(string? value)
            {
                if (string.IsNullOrEmpty(value)) return 0;
                // Remove any non-numeric characters and parse
                var numericPart = new string(value.Where(c => char.IsDigit(c) || c == '-').ToArray());
                return long.TryParse(numericPart, CultureInfo.InvariantCulture, out var result) ? result : 0;
            }
            var totalNonYield = dataList.Sum(d => ParseNonYieldTime(d.NonYieldingTimeMs));
            var maxNonYield = dataList.Max(d => ParseNonYieldTime(d.NonYieldingTimeMs));
            SchedulerIssuesTotalNonYieldText.Text = string.Format(CultureInfo.CurrentCulture, "{0:N0} ms", totalNonYield);
            SchedulerIssuesMaxNonYieldText.Text = string.Format(CultureInfo.CurrentCulture, "{0:N0} ms", maxNonYield);

            // Distinct schedulers affected
            var schedulersAffected = dataList.Select(d => d.SchedulerId).Distinct().Count();
            SchedulerIssuesSchedulersText.Text = schedulersAffected.ToString("N0", CultureInfo.CurrentCulture);

            // Offline events (IsOnline = false)
            var offlineCount = dataList.Count(d => d.IsOnline == false);
            SchedulerIssuesOfflineText.Text = offlineCount.ToString("N0", CultureInfo.CurrentCulture);

            // Color offline count red if > 0
            if (offlineCount > 0)
            {
                SchedulerIssuesOfflineText.Foreground = new System.Windows.Media.SolidColorBrush(
                    System.Windows.Media.Colors.Red);
            }
            else
            {
                SchedulerIssuesOfflineText.Foreground = new System.Windows.Media.SolidColorBrush(
                    (System.Windows.Media.Color)System.Windows.Media.ColorConverter.ConvertFromString("#CCCCCC"));
            }
        }

        // SchedulerIssuesFilter_Click removed - grid removed per todo.md #13

        // ApplySchedulerIssuesFilters removed - grid removed per todo.md #13

        // UpdateSchedulerIssuesFilterButtonStyles removed - grid removed per todo.md #13

        // SchedulerIssuesFilterTextBox_TextChanged removed - grid removed per todo.md #13

        // SchedulerIssuesNumericFilterTextBox_TextChanged removed - grid removed per todo.md #13

        // SchedulerIssuesBoolFilter_Changed removed - grid removed per todo.md #13

        // ApplySchedulerIssuesFilter removed - grid removed per todo.md #13

        #endregion
    }
}
