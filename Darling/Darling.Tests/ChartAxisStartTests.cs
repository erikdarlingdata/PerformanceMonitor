/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Windows;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7386: the X axis of a time chart starts at the later of the picked start and the server's data start. The pure rule, then the
/// real charts of the Viewer (the Overview lanes and the CPU chart) drawn under "Past day" for a server with three hours of history.
/// </summary>
public sealed class ChartAxisStartTests
{
    private static readonly DateTime End = new(2026, 11, 3, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime DayAgo = End.AddHours(-24);

    [Fact]
    public void ADataStartInsideTheRange_MovesTheStartUp_WithASmallRunIn()
    {
        var start = ChartAxisStart.XStart(DayAgo, End, End.AddHours(-3));

        Assert.True(start > DayAgo);
        Assert.True(start < End.AddHours(-3), "the first collection is not drawn on the plot's edge");
        Assert.True(start > End.AddHours(-3.2), "the run-in is a sliver, not a second window");
    }

    [Fact]
    public void ANullDataStart_LeavesTheAxisAsPicked() =>
        Assert.Equal(DayAgo, ChartAxisStart.XStart(DayAgo, End, null));

    [Theory]
    [InlineData(-48)]
    [InlineData(-24)]
    public void ADataStartAtOrBeforeThePickedStart_LeavesTheAxisAsPicked(double hoursFromEnd) =>
        Assert.Equal(DayAgo, ChartAxisStart.XStart(DayAgo, End, End.AddHours(hoursFromEnd)));

    [Theory]
    [InlineData(0)]
    [InlineData(5)]
    public void ADataStartAtOrAfterThePickedEnd_LeavesTheAxisAsPicked(double hoursAfterEnd) =>
        Assert.Equal(DayAgo, ChartAxisStart.XStart(DayAgo, End, End.AddHours(hoursAfterEnd)));

    [Fact]
    public void DataThatBeganAMinuteAgo_DoesNotGiveAOneMinuteChart() =>
        Assert.Equal(DayAgo, ChartAxisStart.XStart(DayAgo, End, End.AddMinutes(-1)));

    [Fact]
    public void ARangeShorterThanTheShortestSpan_IsLeftAlone() =>
        Assert.Equal(End.AddMinutes(-15), ChartAxisStart.XStart(End.AddMinutes(-15), End, End.AddMinutes(-10)));

    [Fact]
    public void TheDataStartJustPastTheShortestSpan_IsHonored()
    {
        var start = ChartAxisStart.XStart(DayAgo, End, End.AddMinutes(-40));

        Assert.True(start > DayAgo);
        Assert.True(End - start >= ChartAxisStart.MinimumSpan);
    }

    [Fact]
    public void TheOleAutomationOverload_AgreesWithTheInstantOne_AndKeepsAnUnmovedStartExactly()
    {
        var first = End.AddHours(-3);
        Assert.Equal(
            ChartAxisStart.XStart(DayAgo, End, first).ToOADate(),
            ChartAxisStart.XStart(DayAgo.ToOADate(), End.ToOADate(), first),
            precision: 6);
        Assert.Equal(DayAgo.ToOADate(), ChartAxisStart.XStart(DayAgo.ToOADate(), End.ToOADate(), End.AddHours(-30)));
    }

    private static List<CpuUtilizationSample> ThreeHoursOfCpu()
    {
        var samples = new List<CpuUtilizationSample>();
        for (var minute = 0; minute <= 180; minute += 5)
        {
            samples.Add(new CpuUtilizationSample(End.AddHours(-3).AddMinutes(minute), 20 + (minute % 40), 5));
        }

        return samples;
    }

    /// <summary>Draws the Viewer's CPU chart for the past day on the real tab and returns its X limits, optionally saving the render.</summary>
    private static (double Left, double Right) CpuAxis(DateTime? dataStart, string? pngPath = null, List<CpuUtilizationSample>? samples = null) =>
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            ResourceDictionary? added = null;
            if (!app.Resources.Contains("NumericCell"))
            {
                added = new ResourceDictionary
                {
                    Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml"),
                };
                app.Resources.MergedDictionaries.Add(added);
            }

            try
            {
                var tab = new ViewerServerTab(
                    new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                    new DarlingServer(1, "axis-start-test", "axis-start-test", true, null));
                var window = new Window { Content = tab, Width = 1800, Height = 1000 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    tab.ServerDataStartUtc = dataStart;
                    tab.RenderCpuChart(samples ?? ThreeHoursOfCpu(), DayAgo, End);
                    var chart = (ScottPlot.WPF.WpfPlot)tab.FindName("CpuChart");
                    if (pngPath is not null)
                    {
                        chart.Plot.GetImage(1000, 360).SavePng(pngPath);
                    }

                    var limits = chart.Plot.Axes.GetLimits();
                    return (limits.Left, limits.Right);
                }
                finally
                {
                    window.Close();
                }
            }
            finally
            {
                if (added is not null)
                {
                    app.Resources.MergedDictionaries.Remove(added);
                }
            }
        });

    [Fact]
    public void TheCpuChartOnAServerWithThreeHours_StartsWhereTheServersDataStarts()
    {
        var png = Environment.GetEnvironmentVariable("CHART_AXIS_START_PNG");
        var (left, right) = CpuAxis(End.AddHours(-3), png);

        Assert.Equal(End.ToOADate(), right, precision: 6);
        Assert.InRange(left, End.AddHours(-3.2).ToOADate(), End.AddHours(-3).ToOADate());
        if (png is not null)
        {
            Assert.True(File.Exists(png));
        }
    }

    [Fact]
    public void TheCpuChartOnAServerCollectedAllDay_OrWithNoKnownStart_KeepsThePickedDay()
    {
        Assert.Equal(DayAgo.ToOADate(), CpuAxis(null).Left, precision: 6);
        Assert.Equal(DayAgo.ToOADate(), CpuAxis(End.AddDays(-9)).Left, precision: 6);
    }

    [Fact]
    public void TheOverviewLanes_ShareOneStart_WhichTheDataStartMoves()
    {
        var (shared, withoutStart) = StaTestThread.Run(() =>
        {
            var lanes = new CorrelatedTimelineLanesControl();
            lanes.ServerDataStartUtc = DateTime.UtcNow.AddHours(-3);
            lanes.SyncXAxes(24, null, null);
            var charts = new[] { lanes.CpuChart, lanes.WaitStatsChart, lanes.BlockingChart, lanes.MemoryChart, lanes.FileIoChart };
            var lefts = new HashSet<double>();
            foreach (var chart in charts)
            {
                lefts.Add(chart.Plot.Axes.GetLimits().Left);
            }

            lanes.ServerDataStartUtc = null;
            lanes.SyncXAxes(24, null, null);
            return (lefts, lanes.CpuChart.Plot.Axes.GetLimits().Left);
        });

        Assert.Single(shared);
        Assert.True(withoutStart < DateTime.UtcNow.AddHours(-23).ToOADate());
        Assert.True(shared.Single() > DateTime.UtcNow.AddHours(-4).ToOADate());
    }

    /// <summary>A chart never hides a point it plotted (#7386, part 1). The server's data start is the oldest wait_stats row, but a
    /// chart may read a relation that reaches back further: an hourly tier, or a collector whose own retention is longer. The
    /// Viewer's CPU chart is given a server whose data starts three hours ago and ten hours of samples: the axis must reach the
    /// oldest sample, not stop at the data start.</summary>
    [Fact]
    public void ACpuChartWithAPointOlderThanTheServersDataStart_KeepsThatPointInsideTheAxis()
    {
        var samples = Enumerable.Range(0, 11)
            .Select(h => new CpuUtilizationSample(End.AddHours(-10).AddHours(h), 30, 5))
            .ToList();

        var (left, _) = CpuAxis(End.AddHours(-3), samples: samples);

        Assert.True(left <= End.AddHours(-10).ToOADate(), $"axis starts at {DateTime.FromOADate(left):u}, after the oldest plotted sample");
        Assert.True(left >= DayAgo.ToOADate());
    }

    [Fact]
    public void TheStartIsNeverLaterThanTheEarliestPlottedPoint_ButNeverBeforeThePickedStart()
    {
        var dataStart = End.AddHours(-3);

        double Start(params DateTime[] points)
        {
            var plot = new ScottPlot.Plot();
            if (points.Length > 0)
            {
                plot.Add.Scatter(points.Select(x => x.ToOADate()).ToArray(), points.Select(_ => 1.0).ToArray());
            }

            return ChartAxisStart.XStart(DayAgo.ToOADate(), End.ToOADate(), dataStart, plot);
        }

        Assert.True(Start(End.AddHours(-10), End.AddHours(-1)) <= End.AddHours(-10).ToOADate());
        Assert.True(Start(End.AddHours(-2), End.AddHours(-1)) > End.AddHours(-3.2).ToOADate());
        Assert.Equal(DayAgo.ToOADate(), Start(End.AddHours(-30), End.AddHours(-1)), precision: 6);
        Assert.Equal(ChartAxisStart.XStart(DayAgo.ToOADate(), End.ToOADate(), dataStart), Start(), precision: 6);
        Assert.Equal(DayAgo.ToOADate(), ChartAxisStart.XStart(DayAgo.ToOADate(), End.ToOADate(), null, new ScottPlot.Plot()), precision: 6);
    }

    [Fact]
    public void WideningAChartDrawnByASharedRenderer_MovesItsLeftEdgeBackToTheOldestPoint()
    {
        var plot = new ScottPlot.Plot();
        var oldest = End.AddHours(-8).ToOADate();
        plot.Add.Scatter(new[] { oldest, End.AddHours(-1).ToOADate() }, new[] { 1.0, 2.0 });
        plot.Axes.SetLimitsX(End.AddHours(-3.06).ToOADate(), End.ToOADate());

        Assert.True(ChartAxisStart.WidenToPlotted(plot, DayAgo.ToOADate(), End.ToOADate(), End.AddHours(-3)));
        Assert.True(plot.Axes.GetLimits().Left <= oldest);
        Assert.Equal(End.ToOADate(), plot.Axes.GetLimits().Right, precision: 6);
        Assert.False(ChartAxisStart.WidenToPlotted(plot, DayAgo.ToOADate(), End.ToOADate(), End.AddHours(-3)));
    }
}
