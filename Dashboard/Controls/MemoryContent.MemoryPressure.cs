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
    public partial class MemoryContent : UserControl
    {
        #region Memory Pressure Events

        private async System.Threading.Tasks.Task RefreshMemoryPressureEventsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetMemoryPressureEventsAsync(_memoryPressureEventsHoursBack, _memoryPressureEventsFromDate, _memoryPressureEventsToDate);
                LoadMemoryPressureEventsChart(data.ToList(), _memoryPressureEventsHoursBack, _memoryPressureEventsFromDate, _memoryPressureEventsToDate);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading memory pressure events: {ex.Message}");
            }
        }

        private void LoadMemoryPressureEventsChart(IEnumerable<MemoryPressureEventItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(MemoryPressureEventsChart, out var existingPanel) && existingPanel != null)
            {
                MemoryPressureEventsChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[MemoryPressureEventsChart] = null;
            }
            MemoryPressureEventsChart.Plot.Clear();
            _memoryPressureEventsHover?.Clear();
            TabHelpers.ApplyThemeToChart(MemoryPressureEventsChart);

            // Count rows where SQL Server reported actual pressure (indicator >= 2 matches sp_pressuredetector).
            var dataList = data?
                .Where(d => d.MemoryIndicatorsProcess >= 2 || d.MemoryIndicatorsSystem >= 2)
                .OrderBy(d => d.SampleTime)
                .ToList() ?? new List<MemoryPressureEventItem>();

            bool hasData = false;
            int maxBarCount = 0;

            if (dataList.Count > 0)
            {
                var grouped = dataList
                    .GroupBy(d => new DateTime(d.SampleTime.Year, d.SampleTime.Month, d.SampleTime.Day, d.SampleTime.Hour, 0, 0))
                    .OrderBy(g => g.Key)
                    .ToList();

                double hourWidth = 1.0 / 24.0;
                double barSize = hourWidth * 0.4;
                double barOffset = hourWidth * 0.22;

                // Four series: SQL Server medium, SQL Server severe (stacked on top of medium),
                // OS medium, OS severe. Stacking uses ValueBase so severe bars sit on top of medium.
                var sqlMediumBars = new List<ScottPlot.Bar>();
                var sqlSevereBars = new List<ScottPlot.Bar>();
                var osMediumBars = new List<ScottPlot.Bar>();
                var osSevereBars = new List<ScottPlot.Bar>();

                var sqlMediumColor = ScottPlot.Color.FromHex("#FFB74D");  // orange 300
                var sqlSevereColor = ScottPlot.Color.FromHex("#E65100");  // orange 900
                var osMediumColor = ScottPlot.Color.FromHex("#E57373");   // red 300
                var osSevereColor = ScottPlot.Color.FromHex("#B71C1C");   // red 900

                foreach (var g in grouped)
                {
                    int sqlMedium = g.Count(d => d.MemoryIndicatorsProcess == 2);
                    int sqlSevere = g.Count(d => d.MemoryIndicatorsProcess >= 3);
                    int osMedium = g.Count(d => d.MemoryIndicatorsSystem == 2);
                    int osSevere = g.Count(d => d.MemoryIndicatorsSystem >= 3);
                    double x = g.Key.ToOADate();

                    if (sqlMedium > 0)
                    {
                        sqlMediumBars.Add(new ScottPlot.Bar
                        {
                            Position = x - barOffset,
                            ValueBase = 0,
                            Value = sqlMedium,
                            Size = barSize,
                            FillColor = sqlMediumColor,
                            LineWidth = 0
                        });
                    }
                    if (sqlSevere > 0)
                    {
                        sqlSevereBars.Add(new ScottPlot.Bar
                        {
                            Position = x - barOffset,
                            ValueBase = sqlMedium,
                            Value = sqlMedium + sqlSevere,
                            Size = barSize,
                            FillColor = sqlSevereColor,
                            LineWidth = 0
                        });
                    }
                    if (osMedium > 0)
                    {
                        osMediumBars.Add(new ScottPlot.Bar
                        {
                            Position = x + barOffset,
                            ValueBase = 0,
                            Value = osMedium,
                            Size = barSize,
                            FillColor = osMediumColor,
                            LineWidth = 0
                        });
                    }
                    if (osSevere > 0)
                    {
                        osSevereBars.Add(new ScottPlot.Bar
                        {
                            Position = x + barOffset,
                            ValueBase = osMedium,
                            Value = osMedium + osSevere,
                            Size = barSize,
                            FillColor = osSevereColor,
                            LineWidth = 0
                        });
                    }

                    int sqlTotal = sqlMedium + sqlSevere;
                    int osTotal = osMedium + osSevere;
                    if (sqlTotal > maxBarCount) maxBarCount = sqlTotal;
                    if (osTotal > maxBarCount) maxBarCount = osTotal;
                }

                bool anyBars = sqlMediumBars.Count > 0 || sqlSevereBars.Count > 0
                    || osMediumBars.Count > 0 || osSevereBars.Count > 0;

                if (anyBars)
                {
                    hasData = true;

                    if (sqlMediumBars.Count > 0)
                    {
                        var bp = MemoryPressureEventsChart.Plot.Add.Bars(sqlMediumBars);
                        bp.LegendText = "SQL Server (medium)";
                        _memoryPressureEventsHover?.Add(bp, "SQL Server (medium)");
                    }
                    if (sqlSevereBars.Count > 0)
                    {
                        var bp = MemoryPressureEventsChart.Plot.Add.Bars(sqlSevereBars);
                        bp.LegendText = "SQL Server (severe)";
                        _memoryPressureEventsHover?.Add(bp, "SQL Server (severe)");
                    }
                    if (osMediumBars.Count > 0)
                    {
                        var bp = MemoryPressureEventsChart.Plot.Add.Bars(osMediumBars);
                        bp.LegendText = "Operating System (medium)";
                        _memoryPressureEventsHover?.Add(bp, "Operating System (medium)");
                    }
                    if (osSevereBars.Count > 0)
                    {
                        var bp = MemoryPressureEventsChart.Plot.Add.Bars(osSevereBars);
                        bp.LegendText = "Operating System (severe)";
                        _memoryPressureEventsHover?.Add(bp, "Operating System (severe)");
                    }

                    _legendPanels[MemoryPressureEventsChart] = MemoryPressureEventsChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    MemoryPressureEventsChart.Plot.Legend.FontSize = 12;
                }
            }

            if (!hasData)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = MemoryPressureEventsChart.Plot.Add.Text("No memory pressure events in selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            MemoryPressureEventsChart.Plot.Axes.DateTimeTicksBottomDateChange();
            MemoryPressureEventsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            MemoryPressureEventsChart.Plot.YLabel("Pressure Events per Hour");
            MemoryPressureEventsChart.Plot.Axes.SetLimitsY(0, Math.Max(maxBarCount * 1.1, 5.0));

            TabHelpers.LockChartVerticalAxis(MemoryPressureEventsChart);
            MemoryPressureEventsChart.Refresh();
        }

        #endregion
    }
}
