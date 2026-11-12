/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Ui;
using ScottPlot.WPF;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7478: the probe the two server-tab tests share must see the defects it exists for. Each case draws a real chart that has the
/// defect, and a sound one beside it.
/// </summary>
public sealed class ChartLayoutProbeTests
{
    private static readonly DateTime Now = new(2026, 11, 3, 12, 0, 0, DateTimeKind.Utc);

    private static (Window Window, WpfPlot Chart) Draw(double width, double height, double axisStartHoursBack, bool hiddenLineAtZero = false)
    {
        var chart = new WpfPlot();
        var window = new Window { Content = new Grid { Children = { chart } }, Width = width, Height = height, Left = 0, Top = 0 };
        StaTestThread.ShowOffScreen(window);
        ChartDisplayScale.Apply(chart, 1.0);
        ChartStyle.ApplyThemeToChart(chart);
        var xs = Enumerable.Range(0, 40).Select(i => Now.AddHours(-6).AddMinutes(i * 9).ToOADate()).ToArray();
        chart.Plot.Add.Scatter(xs, xs.Select((_, i) => 10 + (i % 7)).ToArray());
        if (hiddenLineAtZero)
        {
            chart.Plot.Add.VerticalLine(0).IsVisible = false;
        }

        chart.Plot.Axes.DateTimeTicksBottom();
        chart.Plot.Axes.SetLimitsX(Now.AddHours(-axisStartHoursBack).ToOADate(), Now.ToOADate());
        ChartLayoutProbe.Layout(window);
        ChartLayoutProbe.Settle(window, new[] { chart });
        return (window, chart);
    }

    [Fact]
    public void AnAxisThatStartsLongBeforeTheData_IsReported_AndOneThatStartsAtTheDataIsNot() =>
        StaTestThread.Run(() =>
        {
            var (lateWindow, late) = Draw(900, 500, axisStartHoursBack: 24);
            var (fitWindow, fit) = Draw(900, 500, axisStartHoursBack: 6.1);
            try
            {
                var dataStart = Now.AddHours(-6).ToOADate();
                Assert.Contains(ChartLayoutProbe.Measure(late, dataStart), p => p.Contains("empty before the first plotted point"));
                Assert.DoesNotContain(ChartLayoutProbe.Measure(fit, dataStart), p => p.Contains("empty before the first plotted point"));
                Assert.Equal(dataStart, ChartLayoutProbe.FirstPlottedX(late.Plot)!.Value, precision: 6);
            }
            finally
            {
                lateWindow.Close();
                fitWindow.Close();
            }
        });

    [Fact]
    public void AHiddenLineAtZero_IsNotTakenForTheFirstPlottedPoint() =>
        StaTestThread.Run(() =>
        {
            var (window, chart) = Draw(900, 500, axisStartHoursBack: 24, hiddenLineAtZero: true);
            try
            {
                Assert.Equal(Now.AddHours(-6).ToOADate(), ChartLayoutProbe.FirstPlottedX(chart.Plot)!.Value, precision: 6);
            }
            finally
            {
                window.Close();
            }
        });

    [Fact]
    public void APlotSqueezedUnderTheFloor_IsReported_AndATallChartIsNot() =>
        StaTestThread.Run(() =>
        {
            var (shortWindow, squeezed) = Draw(900, 190, axisStartHoursBack: 6.1);
            var (tallWindow, tall) = Draw(900, 700, axisStartHoursBack: 6.1);
            try
            {
                Assert.Contains(ChartLayoutProbe.Measure(squeezed), p => p.Contains("floor") || p.Contains("under 55%"));
                Assert.Empty(ChartLayoutProbe.Measure(tall));
            }
            finally
            {
                shortWindow.Close();
                tallWindow.Close();
            }
        });
}
