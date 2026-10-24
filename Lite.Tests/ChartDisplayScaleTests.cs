/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Ui;
using Darling.Tests;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6678: chart text drew at 40% of its size at 250% Windows scaling because nothing set the plot's scale from the window's DPI.
/// <see cref="ChartDisplayScale"/> is that rule. Each fact drives a real chart: the plot's scale follows the DPI scale it is given
/// (also after a DPI change, and after <c>Reset()</c> swaps in a fresh plot), the plot then lays out in scaled units so a 13 point
/// label is 13 units of a figure 1/2.5 the pixel size, and a mouse pixel handed to the hover still finds the point under it.
/// </summary>
public sealed class ChartDisplayScaleTests
{
    [Fact]
    public void The_plot_draws_at_the_DPI_scale_and_follows_a_DPI_change() =>
        StaTestThread.Run(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();

            Assert.True(ChartDisplayScale.Apply(chart, 2.5));
            Assert.Equal(2.5, chart.Plot.ScaleFactor, 3);
            Assert.Equal(2.5f, chart.DisplayScale);

            Assert.False(ChartDisplayScale.Apply(chart, 2.5)); // nothing changed, nothing to refresh

            Assert.True(ChartDisplayScale.Apply(chart, 1.0)); // moved to a 100% monitor
            Assert.Equal(1.0, chart.Plot.ScaleFactor, 3);
            Assert.Equal(1.0f, chart.DisplayScale);
        });

    [Theory]
    [InlineData(0.0)]
    [InlineData(-1.0)]
    [InlineData(double.NaN)]
    public void An_unknown_DPI_draws_at_100_percent(double dpiScale) =>
        Assert.Equal(1f, ChartDisplayScale.ScaleFor(dpiScale));

    [Fact]
    public void A_reset_plot_is_scaled_again_by_Apply() =>
        StaTestThread.Run(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            ChartDisplayScale.Apply(chart, 2.5);

            chart.Reset(); // ClearChart does this on every refresh: a new plot is at scale 1
            Assert.Equal(1.0, chart.Plot.ScaleFactor, 3);

            Assert.True(ChartDisplayScale.Apply(chart, 2.5));
            Assert.Equal(2.5, chart.Plot.ScaleFactor, 3);
        });

    [Fact]
    public void At_250_percent_the_plot_lays_out_in_units_2_point_5_times_smaller_than_its_pixels() =>
        StaTestThread.Run(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            chart.Plot.Add.Scatter(new double[] { 0, 1, 2 }, new double[] { 0, 1, 2 });

            var rect = default(ScottPlot.PixelRect);
            chart.Plot.RenderManager.RenderStarting += (_, rp) => rect = rp.ScaledFigureRect;

            using (chart.Plot.GetImage(1000, 500)) { }
            Assert.Equal(1000f, rect.Width, 1);

            ChartDisplayScale.Apply(chart, 2.5);
            using (chart.Plot.GetImage(1000, 500)) { }
            Assert.Equal(400f, rect.Width, 1);
            Assert.Equal(200f, rect.Height, 1); // ChartAxisTitle fits the title to this height, so it fits at every scaling
        });

    [Theory]
    [InlineData(1.0)]
    [InlineData(2.5)]
    public void The_hover_finds_the_point_under_a_device_pixel_at_every_scaling(double dpiScale) =>
        StaTestThread.Run(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            var hover = new ChartHoverHelper(chart, "ms");
            var xs = Enumerable.Range(0, 10).Select(i => new DateTime(2026, 10, 1).AddMinutes(i * 10).ToOADate()).ToArray();
            var ys = xs.Select((_, i) => 1000.0 + i * 111).ToArray();
            hover.Add(chart.Plot.Add.Scatter(xs, ys), "Series");
            ChartDisplayScale.Apply(chart, dpiScale);
            chart.Plot.Axes.AutoScale();
            using (chart.Plot.GetImage(1000, 500)) { }

            for (var i = 0; i < xs.Length; i++)
            {
                var text = hover.HoverTextAt(chart.Plot.GetPixel(new ScottPlot.Coordinates(xs[i], ys[i])));
                Assert.NotNull(text);
                Assert.Contains((1000 + i * 111).ToString("N0", CultureInfo.CurrentCulture), text);
            }
        });
}
