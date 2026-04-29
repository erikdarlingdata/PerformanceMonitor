/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Windows.Data;
using System.Text;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Media;
using Microsoft.Win32;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitorDashboard.Helpers;
using ScottPlot.WPF;


namespace PerformanceMonitorDashboard.Controls
{
    public partial class ResourceMetricsContent : UserControl
    {
        #region Session Stats Tab

        private async Task RefreshSessionStatsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetSessionStatsAsync(_sessionStatsHoursBack, _sessionStatsFromDate, _sessionStatsToDate);
                LoadSessionStatsChart(data, _sessionStatsHoursBack, _sessionStatsFromDate, _sessionStatsToDate);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading session stats: {ex.Message}", ex);
            }
        }

        private void LoadSessionStatsChart(IEnumerable<SessionStatsItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(SessionStatsChart, out var existingSessionPanel) && existingSessionPanel != null)
            {
                SessionStatsChart.Plot.Axes.Remove(existingSessionPanel);
                _legendPanels[SessionStatsChart] = null;
            }
            SessionStatsChart.Plot.Clear();
            TabHelpers.ApplyThemeToChart(SessionStatsChart);
            _sessionStatsHover?.Clear();

            var dataList = data?.OrderBy(d => d.CollectionTime).ToList() ?? new List<SessionStatsItem>();
            if (dataList.Count > 0)
            {
                var timePoints = dataList.Select(d => d.CollectionTime);
                double[] totalCounts = dataList.Select(d => (double)d.TotalSessions).ToArray();
                double[] runningCounts = dataList.Select(d => (double)d.RunningSessions).ToArray();
                double[] sleepingCounts = dataList.Select(d => (double)d.SleepingSessions).ToArray();

                if (totalCounts.Any(c => c > 0))
                {
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, totalCounts.Select(c => c));
                    var totalScatter = SessionStatsChart.Plot.Add.Scatter(xs, ys);
                    totalScatter.LineWidth = 2;
                    totalScatter.MarkerSize = 5;
                    totalScatter.Color = TabHelpers.ChartColors[0];
                    totalScatter.LegendText = "Total";
                    _sessionStatsHover?.Add(totalScatter, "Total");
                }

                if (runningCounts.Any(c => c > 0))
                {
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, runningCounts.Select(c => c));
                    var runningScatter = SessionStatsChart.Plot.Add.Scatter(xs, ys);
                    runningScatter.LineWidth = 2;
                    runningScatter.MarkerSize = 5;
                    runningScatter.Color = TabHelpers.ChartColors[1];
                    runningScatter.LegendText = "Running";
                    _sessionStatsHover?.Add(runningScatter, "Running");
                }

                if (sleepingCounts.Any(c => c > 0))
                {
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, sleepingCounts.Select(c => c));
                    var sleepingScatter = SessionStatsChart.Plot.Add.Scatter(xs, ys);
                    sleepingScatter.LineWidth = 2;
                    sleepingScatter.MarkerSize = 5;
                    sleepingScatter.Color = TabHelpers.ChartColors[2];
                    sleepingScatter.LegendText = "Sleeping";
                    _sessionStatsHover?.Add(sleepingScatter, "Sleeping");
                }

                double[] backgroundCounts = dataList.Select(d => (double)d.BackgroundSessions).ToArray();
                if (backgroundCounts.Any(c => c > 0))
                {
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, backgroundCounts.Select(c => c));
                    var backgroundScatter = SessionStatsChart.Plot.Add.Scatter(xs, ys);
                    backgroundScatter.LineWidth = 2;
                    backgroundScatter.MarkerSize = 5;
                    backgroundScatter.Color = TabHelpers.ChartColors[4];
                    backgroundScatter.LegendText = "Background";
                    _sessionStatsHover?.Add(backgroundScatter, "Background");
                }

                double[] dormantCounts = dataList.Select(d => (double)d.DormantSessions).ToArray();
                if (dormantCounts.Any(c => c > 0))
                {
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, dormantCounts.Select(c => c));
                    var dormantScatter = SessionStatsChart.Plot.Add.Scatter(xs, ys);
                    dormantScatter.LineWidth = 2;
                    dormantScatter.MarkerSize = 5;
                    dormantScatter.Color = TabHelpers.ChartColors[5];
                    dormantScatter.LegendText = "Dormant";
                    _sessionStatsHover?.Add(dormantScatter, "Dormant");
                }

                double[] idleOver30MinCounts = dataList.Select(d => (double)d.IdleSessionsOver30Min).ToArray();
                if (idleOver30MinCounts.Any(c => c > 0))
                {
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, idleOver30MinCounts.Select(c => c));
                    var idleScatter = SessionStatsChart.Plot.Add.Scatter(xs, ys);
                    idleScatter.LineWidth = 2;
                    idleScatter.MarkerSize = 5;
                    idleScatter.Color = TabHelpers.ChartColors[9];
                    idleScatter.LegendText = "Idle >30m";
                    _sessionStatsHover?.Add(idleScatter, "Idle >30m");
                }

                double[] waitingForMemoryCounts = dataList.Select(d => (double)d.SessionsWaitingForMemory).ToArray();
                if (waitingForMemoryCounts.Any(c => c > 0))
                {
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, waitingForMemoryCounts.Select(c => c));
                    var waitingScatter = SessionStatsChart.Plot.Add.Scatter(xs, ys);
                    waitingScatter.LineWidth = 2;
                    waitingScatter.MarkerSize = 5;
                    waitingScatter.Color = TabHelpers.ChartColors[3];
                    waitingScatter.LegendText = "Waiting for Memory";
                    _sessionStatsHover?.Add(waitingScatter, "Waiting for Memory");
                }

                // Update summary panel with latest data point
                var latestData = dataList.LastOrDefault();
                UpdateSessionStatsSummary(latestData);

                _legendPanels[SessionStatsChart] = SessionStatsChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                SessionStatsChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = SessionStatsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                UpdateSessionStatsSummary(null);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            SessionStatsChart.Plot.Axes.DateTimeTicksBottomDateChange();
            SessionStatsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            TabHelpers.SetChartYLimitsWithLegendPadding(SessionStatsChart);
            SessionStatsChart.Plot.YLabel("Session Count");
            TabHelpers.LockChartVerticalAxis(SessionStatsChart);
            SessionStatsChart.Refresh();
        }

        private void UpdateSessionStatsSummary(SessionStatsItem? data)
        {
            if (data != null)
            {
                SessionStatsTopAppText.Text = !string.IsNullOrEmpty(data.TopApplicationName) 
                    ? $"{data.TopApplicationName} ({data.TopApplicationConnections ?? 0})" 
                    : "N/A";
                SessionStatsTopHostText.Text = !string.IsNullOrEmpty(data.TopHostName) 
                    ? $"{data.TopHostName} ({data.TopHostConnections ?? 0})" 
                    : "N/A";
                SessionStatsDatabasesText.Text = data.DatabasesWithConnections.ToString(CultureInfo.CurrentCulture);
            }
            else
            {
                SessionStatsTopAppText.Text = "N/A";
                SessionStatsTopHostText.Text = "N/A";
                SessionStatsDatabasesText.Text = "N/A";
            }
        }

        #endregion
    }
}
