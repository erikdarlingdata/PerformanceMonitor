/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Darling.Tests;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4989: what the Duration Trends hover says over a line with a collection gap in it, and what the chart does with its
/// hover and its Y axis label when it is redrawn. The line breaks at every gap by inserting a point of its own (a NaN
/// between the two buckets it separates), so a hover line counted by the point's position in the scatter falls one bucket
/// behind after each break: it said the NEXT bucket's runs for every bucket after the first break and nothing for the
/// last. The hover lines are keyed by the X each bucket is drawn at, so a bucket finds its own and the inserted point finds
/// none. Each test draws the series on a real chart, renders it so the chart knows its pixels, and asks the hover what it
/// says with the mouse over a bucket's point.
/// </summary>
public class CollectorDurationHoverTests
{
    private static readonly DateTime Start = new(2026, 10, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>
    /// One collector's twenty buckets, ten minutes apart, with a seventy-minute hole after the fifth: the line breaks there
    /// once. Bucket k holds k + 1 runs that averaged 50 + k ms, and its slowest took 100 + 3k ms.
    /// </summary>
    private static List<CollectorDurationBucket> TwentyBucketsWithOneBreak() =>
        Enumerable.Range(0, 20).Select(k => new CollectorDurationBucket
        {
            CollectorName = "wait_stats",
            BucketStart = Start.AddMinutes(10 * k + (k >= 5 ? 60 : 0)),
            MaxDurationMs = 100 + 3 * k,
            AverageDurationMs = 50 + k,
            RunCount = k + 1,
        }).ToList();

    /// <summary>The hover text over each of the line's twenty points in turn (the mouse exactly on the point), in bucket order.</summary>
    private static List<string?> HoverOverEachBucket(List<CollectorDurationBucket> buckets) =>
        OnStaThread(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            var hover = new ChartHoverHelper(chart, "ms", displayZone: () => TimeZoneInfo.Utc);
            var line = Assert.Single(ServerTab.BuildCollectorDurationSeries(buckets));
            ServerTab.PlotCollectorDurationSeries(chart, hover, new[] { line });
            chart.Plot.Axes.AutoScale();
            using var render = chart.Plot.GetImage(1000, 400);

            return line.Xs
                .Select((x, i) => hover.HoverTextAt(chart.Plot.GetPixel(new ScottPlot.Coordinates(x, line.MaxMs[i]))))
                .ToList();
        });

    /// <summary>The popup's fourth line, or <c>null</c> when it is the three lines it is without one.</summary>
    private static string? DetailOf(string? hover)
    {
        Assert.NotNull(hover);
        var lines = hover!.Split('\n');
        return lines.Length == 4 ? lines[3] : null;
    }

    /// <summary>
    /// The observed case: twenty buckets and one break. The hover on bucket 10 said "Slowest of 12 runs; average 61 ms" (bucket
    /// 11's), because the break pushed it one point along the scatter, and the last bucket said nothing, because its place
    /// ran off the end of the lines. Each bucket says its own.
    /// </summary>
    [Fact]
    public void ABucketAfterABreak_ShowsItsOwnRunsAndAverage_NotTheNextBucketsAndTheLastOneShowsItsOwn()
    {
        var text = HoverOverEachBucket(TwentyBucketsWithOneBreak());

        Assert.Equal(20, text.Count);
        Assert.Equal("Slowest of 11 runs; average 60 ms", DetailOf(text[10]));
        Assert.Equal("Slowest of 20 runs; average 69 ms", DetailOf(text[19]));
        for (var k = 0; k < text.Count; k++)
        {
            Assert.Equal($"Slowest of {k + 1} {(k == 0 ? "run" : "runs")}; average {50 + k} ms", DetailOf(text[k]));
        }
    }

    /// <summary>
    /// The hover lines are keyed by the X each bucket is drawn at, and the point the line inserts at the break is none of
    /// them: every real point of the drawn scatter finds its own bucket's line, and the NaN point between the two buckets the
    /// break separates finds none.
    /// </summary>
    [Fact]
    public void TheLinesAreKeyedByX_SoEveryDrawnBucketFindsItsOwn_AndThePointTheBreakInsertsFindsNone()
    {
        var (line, drawn) = OnStaThread(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            var series = Assert.Single(ServerTab.BuildCollectorDurationSeries(TwentyBucketsWithOneBreak()));
            ServerTab.PlotCollectorDurationSeries(chart, null, new[] { series });
            var scatter = Assert.Single(chart.Plot.PlottableList.OfType<ScottPlot.Plottables.Scatter>());

            return (series, scatter.Data.GetScatterPoints().ToList());
        });

        var byX = line.DetailsByX();

        Assert.Equal(20, byX.Count);
        Assert.Equal(21, drawn.Count);
        var inserted = Assert.Single(drawn, p => double.IsNaN(p.Y));
        Assert.False(byX.ContainsKey(inserted.X), "the point the break inserted has a hover line");
        var real = drawn.Where(p => !double.IsNaN(p.Y)).ToList();
        Assert.Equal(20, real.Count);
        for (var k = 0; k < real.Count; k++)
        {
            Assert.Equal(line.Details[k], byX[real[k].X]);
        }
    }

    /// <summary>
    /// A line with no gap, and a series registered without hover lines, keep what they always said: the label, the value and
    /// the time, three lines, and the buckets before the first break are unchanged either way.
    /// </summary>
    [Fact]
    public void ASeriesRegisteredWithoutDetails_KeepsTheThreeLineText()
    {
        var text = OnStaThread(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            var hover = new ChartHoverHelper(chart, "ms", displayZone: () => TimeZoneInfo.Utc);
            var xs = new[] { Start.ToOADate(), Start.AddMinutes(10).ToOADate(), Start.AddMinutes(20).ToOADate() };
            var scatter = chart.Plot.Add.Scatter(xs, new double[] { 5, 9, 7 });
            hover.Add(scatter, "wait_stats");
            chart.Plot.Axes.AutoScale();
            using var render = chart.Plot.GetImage(1000, 400);

            return hover.HoverTextAt(chart.Plot.GetPixel(new ScottPlot.Coordinates(xs[1], 9)));
        });

        Assert.NotNull(text);
        var lines = text!.Split('\n');
        Assert.Equal(3, lines.Length);
        Assert.Equal("wait_stats", lines[0]);
        Assert.Equal("9 ms", lines[1]);
    }

    /// <summary>
    /// Clearing the hover is what an empty range does to what the last range registered (the render clears it before it
    /// draws): with the mouse over a point that had a line, the hover said it, and after the clear it says nothing.
    /// </summary>
    [Fact]
    public void ClearingTheHover_LeavesItHoldingNothing()
    {
        var (before, after) = OnStaThread(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            var hover = new ChartHoverHelper(chart, "ms", displayZone: () => TimeZoneInfo.Utc);
            var line = Assert.Single(ServerTab.BuildCollectorDurationSeries(TwentyBucketsWithOneBreak()));
            ServerTab.PlotCollectorDurationSeries(chart, hover, new[] { line });
            chart.Plot.Axes.AutoScale();
            using var render = chart.Plot.GetImage(1000, 400);
            var pixel = chart.Plot.GetPixel(new ScottPlot.Coordinates(line.Xs[10], line.MaxMs[10]));

            var held = hover.HoverTextAt(pixel);
            hover.Clear();
            return (held, hover.HoverTextAt(pixel));
        });

        Assert.NotNull(before);
        Assert.Null(after);
    }

    /// <summary>
    /// The source of <c>UpdateCollectorDurationChart</c>, comments blanked where the pins below look for code and kept where
    /// they look for the label's text: the method's braces, found in the code-only text, cut the same span out of the file.
    /// </summary>
    private static (string Source, string Code) UpdateCollectorDurationChartBody()
    {
        var file = Lite.Tests.ParitySource.ReadFile("Lite/Controls/ServerTab.Charts.cs");
        var fileCode = CSharpSourceWalker.StripCommentsAndStrings(file);
        var start = fileCode.IndexOf("private void UpdateCollectorDurationChart(", StringComparison.Ordinal);
        Assert.True(start >= 0, "ServerTab.Charts.cs has no UpdateCollectorDurationChart");
        var open = fileCode.IndexOf('{', start);
        var length = CSharpSourceWalker.BraceBalanced(fileCode, open).Length;

        return (file.Substring(open, length), fileCode.Substring(open, length));
    }

    /// <summary>
    /// The chart draws each bucket's slowest run, so its Y axis says that, in the words the Darling viewer's chart uses.
    /// It used to say "Duration (ms)", which reads as the average a run takes.
    /// </summary>
    [Fact]
    public void TheYAxisLabel_NamesTheSlowestRunPerBucket()
    {
        var (source, code) = UpdateCollectorDurationChartBody();
        var literals = CSharpSourceWalker.StringLiteralBodies(source).Select(l => l.Text).ToList();

        Assert.Contains("Plot.YLabel(", code, StringComparison.Ordinal);
        Assert.Contains("Slowest run per bucket (ms)", literals);
        Assert.DoesNotContain("Duration (ms)", literals);
    }

    /// <summary>
    /// A range with no points returns early, so the hover must be cleared before that return: it was cleared only on the way
    /// to drawing, and a tooltip registered by the range before was left on an empty chart.
    /// </summary>
    [Fact]
    public void AnEmptyRange_ClearsTheHover_BeforeItReturns()
    {
        var (_, code) = UpdateCollectorDurationChartBody();

        var clear = code.IndexOf("_collectorDurationHover?.Clear();", StringComparison.Ordinal);
        var emptyReturn = code.IndexOf("if (data.Count == 0)", StringComparison.Ordinal);

        Assert.True(clear >= 0, "UpdateCollectorDurationChart no longer clears the hover");
        Assert.True(emptyReturn >= 0, "UpdateCollectorDurationChart no longer has its empty-range path");
        Assert.True(clear < emptyReturn, "UpdateCollectorDurationChart clears the hover only after the empty range has returned");
        Assert.Equal(code.IndexOf("_collectorDurationHover?.Clear();", StringComparison.Ordinal), code.LastIndexOf("_collectorDurationHover?.Clear();", StringComparison.Ordinal));
    }

    private static T OnStaThread<T>(Func<T> body)
    {
        T result = default!;
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { result = body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }

        return result;
    }
}
