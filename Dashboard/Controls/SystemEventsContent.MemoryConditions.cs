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
        #region Memory Conditions Tab

        private async System.Threading.Tasks.Task RefreshMemoryConditionsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetHealthParserMemoryConditionsAsync(_memoryConditionsHoursBack, _memoryConditionsFromDate, _memoryConditionsToDate);
                // Grid removed per todo.md #14 - chart only
                LoadMemoryConditionsChart(data, _memoryConditionsHoursBack, _memoryConditionsFromDate, _memoryConditionsToDate);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading memory conditions: {ex.Message}", ex);
            }
        }

        private void LoadMemoryConditionsChart(IEnumerable<HealthParserMemoryConditionItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(MemoryConditionsChart, out var existingPanel) && existingPanel != null)
            {
                MemoryConditionsChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[MemoryConditionsChart] = null;
            }
            MemoryConditionsChart.Plot.Clear();
            _memoryConditionsHover?.Clear();
            TabHelpers.ApplyThemeToChart(MemoryConditionsChart);

            var dataList = data?.Where(d => d.EventTime.HasValue).ToList() ?? new List<HealthParserMemoryConditionItem>();
            bool hasData = false;
            if (dataList.Count > 0)
            {
                // Group by hour
                var grouped = dataList
                    .GroupBy(d => new DateTime(d.EventTime!.Value.Year, d.EventTime!.Value.Month, d.EventTime!.Value.Day, d.EventTime!.Value.Hour, 0, 0))
                    .OrderBy(g => g.Key)
                    .ToList();

                if (grouped.Count > 0)
                {
                    hasData = true;
                    var timePoints = grouped.Select(g => g.Key);
                    double[] oomCounts = grouped.Select(g => (double)g.Sum(i => i.OutOfMemoryExceptions ?? 0)).ToArray();

                    if (oomCounts.Any(c => c > 0))
                    {
                        var (xs, ys) = TabHelpers.FillTimeSeriesGaps(timePoints, oomCounts.Select(c => c));
                        var scatter = MemoryConditionsChart.Plot.Add.Scatter(xs, ys);
                        scatter.LineWidth = 2;
                        scatter.MarkerSize = 5;
                        scatter.Color = TabHelpers.ChartColors[3];
                        scatter.LegendText = "OOM Exceptions";
                        _memoryConditionsHover?.Add(scatter, "OOM Exceptions");
                        hasData = true;

                        _legendPanels[MemoryConditionsChart] = MemoryConditionsChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                        MemoryConditionsChart.Plot.Legend.FontSize = 12;
                    }
                }
            }

            if (!hasData)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = MemoryConditionsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            MemoryConditionsChart.Plot.Axes.DateTimeTicksBottomDateChange();
            MemoryConditionsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            MemoryConditionsChart.Plot.YLabel("Count");
            TabHelpers.LockChartVerticalAxis(MemoryConditionsChart);
            MemoryConditionsChart.Refresh();
        }

        // MemoryConditionsFilter_Click removed - grid removed per todo.md #14

        // ApplyMemoryConditionsFilters removed - grid removed per todo.md #14

        // UpdateMemoryConditionsFilterButtonStyles removed - grid removed per todo.md #14

        // MemoryConditionsFilterTextBox_TextChanged removed - grid removed per todo.md #14







        #endregion
    }
}
