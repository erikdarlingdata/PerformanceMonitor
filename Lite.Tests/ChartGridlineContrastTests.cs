/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using Darling.Tests;
using PerformanceMonitor.Ui;
using ScottPlot.WPF;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8266: a chart's gridlines read against the plot in every Lite theme. A themed chart is rendered and its pixels sampled: the
/// gridline's contrast against the plot background has a floor Dark's grid clears, and the grid stays quieter than the axis text.
/// </summary>
public sealed class ChartGridlineContrastTests
{
    /* A 1 px gridline rendered over the plot; Dark's (white at alpha 40 over #111217) measures about 1.57. */
    private const double Floor = 1.45;

    public static TheoryData<string> Themes => new() { "Dark", "Light", "CoolBreeze" };

    [Theory]
    [MemberData(nameof(Themes))]
    public void TheGridlines_ReadAgainstThePlot_AndStayQuieterThanTheAxis(string theme)
    {
        var (grid, text, background, line) = StaTestThread.Run(() => WithTheme(theme, () =>
        {
            var chart = new WpfPlot();
            ChartStyle.ApplyThemeToChart(chart);
            return GridlineContrast.Measure(chart.Plot, ChartStyle.GetThemeColors().Text);
        }));

        Assert.True(grid >= Floor, $"{theme}: the gridline ({line}) against the plot ({background}) has a contrast of {grid:F2}; the floor is {Floor:F2}");
        Assert.True(grid < text, $"{theme}: the gridline's contrast {grid:F2} is not under the axis text's {text:F2}");
    }

    private static T WithTheme<T>(string theme, Func<T> body)
    {
        _ = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var previous = ThemeManager.CurrentTheme;
        ThemeManager.ThemeUriProvider = name => new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{name}Theme.xaml");
        try
        {
            ThemeManager.Apply(theme);
            return body();
        }
        finally
        {
            ThemeManager.Apply(previous);
            ThemeManager.ThemeUriProvider = ThemeManager.ThemeUri;
        }
    }
}
