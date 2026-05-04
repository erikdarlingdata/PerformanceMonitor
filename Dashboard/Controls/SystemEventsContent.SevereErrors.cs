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
        #region Severe Errors Tab

        private async System.Threading.Tasks.Task RefreshSevereErrorsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var data = await _databaseService.GetHealthParserSevereErrorsAsync(_severeErrorsHoursBack, _severeErrorsFromDate, _severeErrorsToDate);
                _severeErrorsUnfilteredData = data;
                SevereErrorsDataGrid.ItemsSource = data;
                SevereErrorsNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                LoadSevereErrorsChart(data, _severeErrorsHoursBack, _severeErrorsFromDate, _severeErrorsToDate);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading severe errors: {ex.Message}", ex);
            }
        }

        private void LoadSevereErrorsChart(IEnumerable<HealthParserSevereErrorItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(SevereErrorsChart, out var existingSevereErrorsPanel) && existingSevereErrorsPanel != null)
            {
                SevereErrorsChart.Plot.Axes.Remove(existingSevereErrorsPanel);
                _legendPanels[SevereErrorsChart] = null;
            }
            SevereErrorsChart.Plot.Clear();
            _severeErrorsHover?.Clear();
            TabHelpers.ApplyThemeToChart(SevereErrorsChart);

            var dataList = data?.ToList() ?? new List<HealthParserSevereErrorItem>();
            bool hasData = false;
            if (dataList.Count > 0)
            {
                // Group by hour and count events
                var grouped = dataList
                    .Where(d => d.EventTime.HasValue)
                    .GroupBy(d => new DateTime(d.EventTime!.Value.Year, d.EventTime!.Value.Month, d.EventTime!.Value.Day, d.EventTime!.Value.Hour, 0, 0))
                    .OrderBy(g => g.Key)
                    .ToList();

                if (grouped.Count > 0)
                {
                    hasData = true;
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                        grouped.Select(g => g.Key),
                        grouped.Select(g => (double)g.Count()));

                    var scatter = SevereErrorsChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = TabHelpers.ChartColors[3];
                    scatter.LegendText = "Error Count";
                    _severeErrorsHover?.Add(scatter, "Error Count");

                    _legendPanels[SevereErrorsChart] = SevereErrorsChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                    SevereErrorsChart.Plot.Legend.FontSize = 12;
                }
            }

            if (!hasData)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = SevereErrorsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            SevereErrorsChart.Plot.Axes.DateTimeTicksBottomDateChange();
            SevereErrorsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            SevereErrorsChart.Plot.YLabel("Event Count");
            TabHelpers.LockChartVerticalAxis(SevereErrorsChart);
            SevereErrorsChart.Refresh();
        }

        private void SevereErrorsFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;

            ShowFilterPopup(button, columnName, "SevereErrors", _severeErrorsFilters,
                args => { },
                () => { });
        }

        private void ApplySevereErrorsFilters()
        {
            if (_severeErrorsUnfilteredData == null)
            {
                _severeErrorsUnfilteredData = SevereErrorsDataGrid.ItemsSource as List<HealthParserSevereErrorItem>;
                if (_severeErrorsUnfilteredData == null && SevereErrorsDataGrid.ItemsSource != null)
                {
                    _severeErrorsUnfilteredData = (SevereErrorsDataGrid.ItemsSource as IEnumerable<HealthParserSevereErrorItem>)?.ToList();
                }
            }

            if (_severeErrorsUnfilteredData == null) return;

            if (_severeErrorsFilters.Count == 0)
            {
                SevereErrorsDataGrid.ItemsSource = _severeErrorsUnfilteredData;
                return;
            }

            var filteredData = _severeErrorsUnfilteredData.Where(item =>
            {
                foreach (var filter in _severeErrorsFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            SevereErrorsDataGrid.ItemsSource = filteredData;
        }

        private void UpdateSevereErrorsFilterButtonStyles()
        {
            foreach (var columnName in new[] { "CollectionTime", "EventTime", "ErrorNumber", "Severity", "State", "DatabaseName", "Message" })
            {
                UpdateFilterButtonStyle(SevereErrorsDataGrid, columnName, _severeErrorsFilters);
            }
        }

        private void SevereErrorsFilterTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DataGridFilterService.ApplyFilter(SevereErrorsDataGrid, sender as TextBox);
        }

        private void SevereErrorsNumericFilterTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            DataGridFilterService.ApplyFilter(SevereErrorsDataGrid, sender as TextBox);
        }

        #endregion
    }
}
