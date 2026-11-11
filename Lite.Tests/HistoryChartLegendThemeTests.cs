/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Ui;
using ScottPlot.WPF;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8540: a history window's chart colours its legend from the theme. ScottPlot draws a legend inside the plot for any named series, and
/// the history windows theme their chart with <see cref="ChartStyle.ApplyMinimalChartTheme"/>; the legend it leaves is read from the plot
/// and held to the colours the full chart theme gives one.
/// </summary>
public sealed class HistoryChartLegendThemeTests
{
    [Fact]
    public void TheMinimalChartTheme_ColoursTheLegendAsTheFullThemeDoes()
    {
        var (full, minimal) = StaTestThread.Run(() =>
        {
            var themed = new WpfPlot();
            ChartStyle.ApplyThemeToChart(themed);
            var history = new WpfPlot();
            history.Plot.Add.Scatter(new double[] { 1, 2, 3 }, new double[] { 4, 5, 6 }).LegendText = "Avg CPU (ms)";
            ChartStyle.ApplyMinimalChartTheme(history);
            var a = themed.Plot.Legend;
            var b = history.Plot.Legend;
            return ((a.BackgroundColor, a.FontColor, a.OutlineColor), (b.BackgroundColor, b.FontColor, b.OutlineColor));
        });

        Assert.Equal(full.BackgroundColor, minimal.BackgroundColor);
        Assert.Equal(full.FontColor, minimal.FontColor);
        Assert.Equal(full.OutlineColor, minimal.OutlineColor);
    }
}
