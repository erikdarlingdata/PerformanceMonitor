/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitor.Darling.Viewer;
using ScottPlot.WPF;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8162: the Overview timeline lanes (Viewer), built as the app builds them, shown in a window at real sizes, fed made-up data and
/// read back from what ScottPlot drew and WPF laid out. Each lane's plot fills its lane (no blank band under it), only the last lane
/// carries X labels and gets room for them, each lane's caption sits in the lane's top left (no left gutter), and the lanes' plot
/// rectangles still line up.
/// </summary>
public sealed class ViewerOverviewLanesFillTests
{
    private readonly ITestOutputHelper _output;

    public ViewerOverviewLanesFillTests(ITestOutputHelper output) => _output = output;

    public static TheoryData<int, int> Windows => new() { { 1536, 912 }, { 1280, 800 } };

    /// <summary>The most a lane that shows no X labels may hold under its plot: its tick marks and a hair of margin.</summary>
    private const double MaxBlankBelowUpperLane = 8;

    /// <summary>The most the last lane may hold under its plot: its X labels (a line of text plus tick marks) and a hair of margin.</summary>
    private const double MaxBelowLastLane = 40;

    private sealed record Lane(int Index, Rect Chart, ScottPlot.PixelRect Data, float BottomPanel, bool TickLabelsOn, int CaptionsInTopLeft, double CaptionClearance);

    private static List<(double Time, double Value)> Series(DateTime from, DateTime to, double lo, double hi) =>
        Enumerable.Range(0, 96).Select(i => (from.AddTicks((to - from).Ticks * i / 95).ToOADate(), lo + (hi - lo) * (i % 12) / 11.0)).ToList();

    private static IReadOnlyList<WpfPlot> Lanes(CorrelatedTimelineLanesControl c) =>
        [c.CpuChart, c.WaitStatsChart, c.BlockingChart, c.MemoryChart, c.FileIoChart];

    private static IEnumerable<T> Descendants<T>(DependencyObject root) where T : DependencyObject
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(root); i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            if (child is T match)
            {
                yield return match;
            }

            foreach (var deeper in Descendants<T>(child))
            {
                yield return deeper;
            }
        }
    }

    private static List<Lane> Measure(int width, int height)
    {
        List<Lane> result = [];
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var added = new List<ResourceDictionary>
            {
                new() { Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml") },
                new() { Source = new Uri("pack://application:,,,/PerformanceMonitor.Ui;component/Themes/SharedStyles.xaml") },
            };
            foreach (var dictionary in added)
            {
                app.Resources.MergedDictionaries.Add(dictionary);
            }

            try
            {
                var control = new CorrelatedTimelineLanesControl();
                control.Initialize(null!, 1);
                var window = new Window { Content = control, Width = width, Height = height, Left = 0, Top = 0 };
                window.Show();
                try
                {
                    var to = DateTime.UtcNow;
                    var from = to.AddHours(-24);
                    control.UpdateCpuLane(Series(from, to, 5, 90), Series(from, to, 8, 95));
                    control.UpdateLane(control.WaitStatsChart, "Wait", Series(from, to, 150, 900), "#FFB74D");
                    control.UpdateBlockingLane(Series(from, to, 1, 6), Series(from, to, 0, 2).Take(5).ToList());
                    control.UpdateLane(control.MemoryChart, "Buffer Pool", Series(from, to, 2000, 9000), "#4FC3F7");
                    control.UpdateLane(control.FileIoChart, "I/O", Series(from, to, 1, 40), "#81C784");
                    control.SyncXAxes(24, null, null);
                    ChartLayoutProbe.Settle(window, Lanes(control));

                    var captions = Descendants<TextBlock>(control)
                        .Where(t => t.IsVisible && t.ActualWidth > 0 && !string.IsNullOrWhiteSpace(t.Text)
                            && t.Name is not "ComparisonEmptyBanner" and not "BlockingLaneDataStartBanner")
                        .Select(t => t.TransformToAncestor(control).TransformBounds(new Rect(t.RenderSize)))
                        .ToList();
                    result = Lanes(control).Select((chart, i) =>
                    {
                        var bounds = chart.TransformToAncestor(control).TransformBounds(new Rect(chart.RenderSize));
                        var last = chart.Plot.RenderManager.LastRender;
                        // A caption belongs to the lane when it sits inside the lane's top left corner, over the plot.
                        var inCorner = captions.Count(c => c.Left >= bounds.Left - 1 && c.Left <= bounds.Left + 90
                            && c.Top >= bounds.Top - 1 && c.Top <= bounds.Top + 24);
                        // How far past the plot's left edge (the end of the Y labels) the lane's caption starts. The plot rectangle is in
                        // the same units as the layout (ScottPlot reports layout units, not device pixels).
                        var clearance = captions.Where(c => c.Left >= bounds.Left - 1 && c.Left <= bounds.Left + 290
                                && c.Top >= bounds.Top - 1 && c.Top <= bounds.Top + 24)
                            .Select(c => c.Left - (bounds.Left + last.DataRect.Left)).DefaultIfEmpty(double.NaN).Min();
                        return new Lane(i, bounds, last.DataRect, last.Layout.PanelSizes[chart.Plot.Axes.Bottom],
                            chart.Plot.Axes.Bottom.TickLabelStyle.IsVisible, inCorner, clearance);
                    }).ToList();
                }
                finally
                {
                    window.Close();
                }
            }
            finally
            {
                foreach (var dictionary in added)
                {
                    app.Resources.MergedDictionaries.Remove(dictionary);
                }
            }
        });
        return result;
    }

    [Theory]
    [MemberData(nameof(Windows))]
    public void EachLanesPlot_FillsItsLane_WithNoBlankBandUnderIt(int width, int height)
    {
        var lanes = Measure(width, height);
        foreach (var l in lanes)
        {
            _output.WriteLine($"lane {l.Index}: chart {l.Chart.Height:0.#} px, plot {l.Data.Height:0.#} px, top {l.Data.Top:0.#}, bottom panel {l.BottomPanel:0.#}, under the plot {l.Chart.Height - l.Data.Bottom:0.#}");
        }

        foreach (var l in lanes)
        {
            Assert.True(l.Data.Top <= 2, $"lane {l.Index} holds {l.Data.Top:0.#} px above its plot");
            var under = l.Chart.Height - l.Data.Bottom;
            var limit = l.Index == lanes.Count - 1 ? MaxBelowLastLane : MaxBlankBelowUpperLane;
            Assert.True(under <= limit, $"lane {l.Index} holds {under:0.#} px under its plot (limit {limit}) in a {l.Chart.Height:0.#} px chart");
        }
    }

    [Theory]
    [MemberData(nameof(Windows))]
    public void OnlyTheLastLane_ShowsXLabels_AndItsPlotIsNotSqueezedByThem(int width, int height)
    {
        var lanes = Measure(width, height);
        Assert.All(lanes.Take(4), l => Assert.False(l.TickLabelsOn, $"lane {l.Index} shows X labels"));
        Assert.True(lanes[4].TickLabelsOn, "the last lane shows no X labels");
        // The labels take their own room: a line of text, where the lanes above keep only their tick marks.
        Assert.True(lanes[4].BottomPanel >= 20, $"the last lane keeps {lanes[4].BottomPanel:0.#} px for its X labels");
        Assert.All(lanes.Take(4), l => Assert.True(l.BottomPanel <= MaxBlankBelowUpperLane, $"lane {l.Index} keeps {l.BottomPanel:0.#} px under its plot"));
        // Not squeezed: the last lane's plot is within a tenth of the other lanes' plots.
        var others = lanes.Take(4).Average(l => (double)l.Data.Height);
        Assert.True(lanes[4].Data.Height >= others * 0.9, $"the last lane's plot is {lanes[4].Data.Height:0.#} px against {others:0.#} px for the others");
    }

    [Theory]
    [MemberData(nameof(Windows))]
    public void EachLaneCaption_SitsInTheLanesTopLeft_AndThePlotsStartNearTheLeftEdge(int width, int height)
    {
        var lanes = Measure(width, height);
        // No gutter: the plots start where the control starts (its own 8 px margin), not 100 px in.
        Assert.All(lanes, l => Assert.True(l.Chart.Left <= 12, $"lane {l.Index} starts {l.Chart.Left:0.#} px from the left edge"));
        Assert.All(lanes, l => Assert.True(l.CaptionsInTopLeft == 1, $"lane {l.Index} has {l.CaptionsInTopLeft} captions in its top left corner"));
    }

    /// <summary>
    /// A caption sits past the end of its lane's Y labels, never over them. The labels' width is measured in the shared gutter pass, but
    /// at a display scale above 100% the lane renders wider than that pass measured, so a caption placed from the measurement alone lands on
    /// the top Y label ("CPU %" over "100"). The caption follows the plot's rendered left edge.
    /// </summary>
    [Theory]
    [MemberData(nameof(Windows))]
    public void EachLaneCaption_StartsPastItsLanesYLabels(int width, int height)
    {
        var lanes = Measure(width, height);
        Assert.All(lanes, l => Assert.True(l.CaptionClearance >= -0.5, $"lane {l.Index} caption starts {l.CaptionClearance:0.#} px from the plot's left edge"));
    }

    [Theory]
    [MemberData(nameof(Windows))]
    public void TheLanesPlotRectangles_StillLineUpLeftAndRight(int width, int height)
    {
        var lanes = Measure(width, height);
        Assert.All(lanes, l =>
        {
            Assert.True(Math.Abs(l.Data.Left - lanes[0].Data.Left) <= 1.5, $"lane {l.Index} plot starts at {l.Data.Left:0.#}, lane 0 at {lanes[0].Data.Left:0.#}");
            Assert.True(Math.Abs(l.Data.Right - lanes[0].Data.Right) <= 1.5, $"lane {l.Index} plot ends at {l.Data.Right:0.#}, lane 0 at {lanes[0].Data.Right:0.#}");
        });
    }
}
