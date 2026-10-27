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
using System.Windows.Threading;
using PerformanceMonitor.Ui;
using ScottPlot;
using ScottPlot.WPF;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6652: at 1366x768 the Viewer's stacked charts get short plots. A long left-axis title was cut at both ends (the unit
/// lost) and the y axis could show only "0". The title fit and the two-tick floor are the pure rules behind the fix.
/// </summary>
public sealed class ViewerShortPlotTests
{
    // The plot height a chart row at its minimum leaves for the rotated title.
    private static double MinRowTitleSpace => ChartLayout.MinPlotHeight - ChartAxisTitle.ReservedHeight;

    public static TheoryData<string> LongTitles => new()
    {
        "Lock Wait Time (ms/sec)",
        "Total Block Duration (ms)",
        "Total Deadlock Wait (ms)",
        "Total Wait Duration (ms)",
        "Avg Wait Time (ms/wait)",
        "Read Throughput (MB/s)",
        "Write Throughput (MB/s)",
        "Pressure Events per Hour",
        "Slowest run per bucket (ms)",
        "Plan Cache Size (MB)",
    };

    [Theory]
    [MemberData(nameof(LongTitles))]
    public void LongTitle_FitsTheMinimumRowAndKeepsItsUnit(string title)
    {
        var (text, size) = ChartAxisTitle.Fit(title, MinRowTitleSpace);

        Assert.InRange(size, ChartAxisTitle.MinFontSize, ChartAxisTitle.BaseFontSize);
        Assert.True(text.Length * 0.6 * size <= MinRowTitleSpace + 0.5, $"'{text}' at {size}pt is longer than {MinRowTitleSpace}px");
        if (title.EndsWith(')'))
        {
            Assert.EndsWith(title[title.LastIndexOf('(')..], text);
        }
    }

    [Fact]
    public void ShortTitle_IsLeftAlone()
    {
        Assert.Equal(("CPU %", ChartAxisTitle.BaseFontSize), ChartAxisTitle.Fit("CPU %", MinRowTitleSpace));
    }

    [Fact]
    public void LongSourceNote_IsTrimmedNotTreatedAsAUnit()
    {
        var (text, _) = ChartAxisTitle.Fit("Blocking events (blocked-process-report)", MinRowTitleSpace);
        Assert.StartsWith("Blocking", text);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    [InlineData(double.NaN)]
    public void UnknownSpace_ReturnsTheTitleUnchanged(double available)
    {
        Assert.Equal(("Lock wait ms/s", ChartAxisTitle.BaseFontSize), ChartAxisTitle.Fit("Lock wait ms/s", available));
    }

    [Fact]
    public void AllZeroAxis_GetsAtLeastTwoLabelledTicks()
    {
        var ticks = MinimumTickGenerator.EnsureMinimum(System.Array.Empty<Tick>(), 0, 1);

        Assert.True(ticks.Count(t => t.IsMajor) >= 2);
        Assert.Contains(ticks, t => t.Label == "0");
        Assert.Contains(ticks, t => t.Label == "1");
    }

    [Fact]
    public void OnlyZeroTick_GetsTheTopOfTheAxisAdded()
    {
        var ticks = MinimumTickGenerator.EnsureMinimum(new[] { Tick.Major(0, "0") }, 0, 1.15);

        Assert.Equal(new[] { 0.0, 1.15 }, ticks.Select(t => t.Position).ToArray());
        Assert.Equal("1.15", ticks[1].Label);
    }

    [Fact]
    public void EnoughTicks_AreReturnedUntouched()
    {
        var given = new[] { Tick.Major(0, "0"), Tick.Major(50, "50"), Tick.Major(100, "100") };
        Assert.Same(given, MinimumTickGenerator.EnsureMinimum(given, 0, 105));
    }

    [Fact]
    public void ZeroWidthRange_StillGetsTwoTicks()
    {
        var ticks = MinimumTickGenerator.EnsureMinimum(System.Array.Empty<Tick>(), 0, 0);
        Assert.Equal(new[] { 0.0, 1.0 }, ticks.Select(t => t.Position).ToArray());
    }

    [Fact]
    public void MinimumPlotHeight_IsTallEnoughForTicks()
    {
        Assert.InRange(ChartLayout.MinPlotHeight, 140, 200);
    }

    // ---- #6832: the plot area, not the chart row, keeps its minimum height under a large legend ----

    private const int ViewerWidth = 1300; // 1366 wide, less the tab's margins and the scroll bar

    /// <summary>
    /// The Blocking Trends layout of the Viewer at 1366x768: a scroll viewer's viewport about 450 tall, star rows with the shared
    /// minimum (the banners in the Auto rows collapsed), each chart in a Border. Charts are built the way the tabs build them
    /// (theme, series, discontinuity marker, bottom legend, y-limits) and rendered at the size the layout gives them, with the
    /// dispatcher pumped between renders so the row fit that follows a render has run.
    /// </summary>
    private static (Grid Grid, WpfPlot[] Charts) HostCharts(int legendEntries, bool restartMarker, params string[] titles)
    {
        var grid = new Grid { Height = 450, MinHeight = 540 };
        var charts = new WpfPlot[titles.Length];
        for (var i = 0; i < titles.Length; i++)
        {
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
            grid.RowDefinitions.Add(new RowDefinition { Height = new GridLength(1, GridUnitType.Star), MinHeight = ChartLayout.MinPlotHeight });
            var chart = charts[i] = new WpfPlot();
            var border = new Border { Margin = new Thickness(0, 4, 0, 4), Child = chart };
            Grid.SetRow(border, i * 2 + 1);
            grid.Children.Add(border);
            ChartStyle.ApplyThemeToChart(chart);
            var t0 = new DateTime(2026, 10, 26).ToOADate();
            var xs = Enumerable.Range(0, 50).Select(k => t0 + k / 48.0).ToArray();
            for (var e = 0; e < legendEntries; e++)
            {
                var scatter = chart.Plot.Add.Scatter(xs, xs.Select(x => (x - t0) * 100 + e).ToArray());
                scatter.LegendText = $"Database{e}_data_file_{e}.mdf Read Latency";
            }
            if (restartMarker) ChartStyle.AddDiscontinuityMarker(chart, xs[20], "baseline discontinuity at 2026-10-26 10:03 (restart)");
            chart.Plot.Axes.DateTimeTicksBottom();
            ChartStyle.ReapplyAxisColors(chart);
            chart.Plot.YLabel(titles[i]);
            ChartStyle.SetChartYLimitsWithLegendPadding(chart, 0, 398);
            new ChartRenderHelper().ShowChartLegend(chart);
        }
        Settle(grid, charts);
        return (grid, charts);
    }

    private static void Settle(Grid grid, WpfPlot[] charts)
    {
        for (var pass = 0; pass < 4; pass++)
        {
            grid.InvalidateMeasure();
            grid.Measure(new Size(ViewerWidth, 450));
            grid.Arrange(new Rect(0, 0, ViewerWidth, Math.Max(450, grid.DesiredSize.Height)));
            grid.UpdateLayout();
            foreach (var chart in charts)
            {
                using var image = chart.Plot.GetImage((int)chart.ActualWidth, (int)chart.ActualHeight);
            }
            // The row fit runs at background priority after a render, as it does in the app.
            Dispatcher.CurrentDispatcher.Invoke(() => { }, DispatcherPriority.ContextIdle);
        }
    }

    private static double PlotHeight(WpfPlot chart) => chart.Plot.RenderManager.LastRender.DataRect.Height;

    [Theory]
    [InlineData(10, false)]
    [InlineData(10, true)]
    [InlineData(8, false)]
    public void LargeLegend_StillLeavesTheMinimumPlotArea(int legendEntries, bool restartMarker) =>
        StaTestThread.Run(() =>
        {
            var (_, charts) = HostCharts(legendEntries, restartMarker, "Lock wait ms/s", "Blocking events", "Deadlocks");

            foreach (var chart in charts)
            {
                Assert.True(PlotHeight(chart) >= ChartLayout.MinPlotAreaHeight - 0.5,
                    $"plot area {PlotHeight(chart):0.#} px in a {chart.ActualHeight:0.#} px chart is under {ChartLayout.MinPlotAreaHeight}");
            }
        });

    [Fact]
    public void RowGrowth_RaisesTheGridMinimumSoTheTabScrollsInsteadOfClipping() =>
        StaTestThread.Run(() =>
        {
            var (grid, _) = HostCharts(10, false, "Lock wait ms/s", "Blocking events", "Deadlocks");

            var rows = grid.RowDefinitions.Where(r => r.Height.IsStar).Sum(r => r.MinHeight);
            Assert.True(rows > 3 * ChartLayout.MinPlotHeight);
            Assert.True(grid.MinHeight >= rows, $"grid minimum {grid.MinHeight} is under the rows' {rows}");
            Assert.True(grid.ActualHeight >= rows - 0.5);
        });

    [Fact]
    public void RowMinimum_FollowsWhatTheLegendAndTicksTake()
    {
        Assert.Equal(ChartLayout.MinPlotHeight, ChartLayout.RowMinHeightFor(40));
        Assert.Equal(171 + ChartLayout.MinPlotAreaHeight, ChartLayout.RowMinHeightFor(171));
    }

    // ---- #6832: the fitted title stays inside the plot, clear of the top and bottom tick labels, and keeps its unit ----

    public static TheoryData<string> IssueTitles => new()
    {
        "Read Latency (ms)",
        "Write Latency (ms)",
        "Lock wait ms/s",
        "Duration ms/s",
        "Executions/sec",
        "Lock Wait Time (ms/sec)",
    };

    [Theory]
    [MemberData(nameof(IssueTitles))]
    public void AxisTitle_StaysClearOfTheTickLabelsAndKeepsItsUnit(string title) =>
        StaTestThread.Run(() =>
        {
            var (_, charts) = HostCharts(10, true, title, "Blocking events", "Deadlocks"); // three rows, as the tab has: one chart alone would take the whole viewport
            var plot = charts[0].Plot;
            var data = plot.RenderManager.LastRender.DataRect;

            var label = plot.Axes.Left.Label;
            using var paint = Paint.NewDisposablePaint();
            var titleHalf = label.Measure(paint).Width / 2;
            var centre = (data.Top + data.Bottom) / 2;

            // The rotated title is centred on the plot. Its extent must lie inside the plot, so it is not cut off, and clear of
            // the labels of the highest and lowest ticks, which sit at the two ends of the plot.
            Assert.True(centre - titleHalf >= data.Top - 0.5 && centre + titleHalf <= data.Bottom + 0.5,
                $"'{label.Text}' is {titleHalf * 2:0.#} px long in a {data.Height:0.#} px plot");
            // Side by side in the left panel the title takes its height at the outer edge and the tick labels hug the plot, so they
            // are clear of each other when the panel is as wide as the title's height plus the widest label drawn plus a tick mark.
            var layout = plot.RenderManager.LastRender.Layout;
            var panel = layout.PanelSizes[plot.Axes.Left];
            var widest = plot.Axes.Left.TickGenerator.Ticks.Where(t => t.IsMajor)
                .Max(t => new LabelStyle { Text = t.Label, FontSize = plot.Axes.Left.TickLabelStyle.FontSize }.Measure(paint).Width);
            var titleHeight = label.Measure(paint).Height;
            const float TickMark = 5;
            Assert.True(titleHeight + widest + TickMark <= panel,
                $"the {panel:0.#} px panel cannot hold the {titleHeight:0.#} px title and a {widest:0.#} px tick label");

            // Whatever was trimmed, the unit (the last word, "(ms)" or "ms/s") is still there.
            Assert.EndsWith(title.Split(' ')[^1], label.Text);
        });
}
