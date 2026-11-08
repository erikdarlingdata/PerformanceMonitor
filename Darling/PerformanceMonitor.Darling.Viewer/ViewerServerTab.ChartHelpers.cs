/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Per-tab chart-helper forwarders. The STATEFUL shapes (legend-panel tracking + the empty-state chart) live
/// in the shared <see cref="ChartRenderHelper"/>; these thin instance forwarders keep the many Render* call
/// sites unchanged. The stateless theme / axis-color / y-limit shapes delegate to the shared
/// <see cref="ChartStyle"/>. (Formerly held a verbatim copy of Lite's ServerTab.Charts.cs helpers.)
/// </summary>
public partial class ViewerServerTab
{
    private readonly ChartRenderHelper _chartHelper = new();

    private void ClearChart(ScottPlot.WPF.WpfPlot chart)
    {
        _chartHelper.ClearChart(chart);
        ViewerChartCards.AttachChart(chart); // #8151: the clear swapped in a new plot
    }

    private void ShowChartLegend(ScottPlot.WPF.WpfPlot chart, bool oneRow = false) => _chartHelper.ShowChartLegend(chart, oneRow);

    private void RefreshEmptyChart(ScottPlot.WPF.WpfPlot chart, string legendText, string yAxisLabel)
        => _chartHelper.RefreshEmptyChart(chart, legendText, yAxisLabel);

    private static void ApplyTheme(ScottPlot.WPF.WpfPlot chart) => ChartStyle.ApplyThemeToChart(chart);

    private static void ReapplyAxisColors(ScottPlot.WPF.WpfPlot chart) => ChartStyle.ReapplyAxisColors(chart);

    private static void SetChartYLimitsWithLegendPadding(ScottPlot.WPF.WpfPlot chart, double dataYMin = 0, double dataYMax = 0)
        => ChartStyle.SetChartYLimitsWithLegendPadding(chart, dataYMin, dataYMax);
}
