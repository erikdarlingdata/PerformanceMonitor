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
using System.Runtime.ExceptionServices;
using System.Text.RegularExpressions;
using System.Threading;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// What the Collection Health tab's Duration Trends hover says over a point (#4966), the viewer's half of Lite's #4989: each point draws
/// its bucket's slowest run, so the hover's fourth line says how many runs that is the slowest of and what the bucket's average took,
/// in the words both apps share (<see cref="CollectorDurationHoverText"/>). The line breaks at every collection gap by inserting a
/// point of its own, so the lines are keyed by the X each bucket is drawn at: a line counted by position would say the NEXT bucket's
/// runs after the first break and nothing for the last. Each fact draws the product's own lines (<c>PlotCollectorDurationSeries</c>) on
/// a real chart, renders it so the chart knows its pixels, and asks the hover what it says with the mouse over a bucket's point.
/// </summary>
public sealed class ViewerCollectorDurationHoverTests
{
    private static readonly DateTime Start = new(2026, 10, 1, 0, 0, 0, DateTimeKind.Unspecified);

    private static readonly CultureInfo English = new("en-US");

    /// <summary>
    /// One collector's twenty buckets, ten minutes apart, with a seventy-minute hole after the fifth: the line breaks there once.
    /// Bucket k holds k + 1 runs that averaged 50 + k ms, and its slowest took 100 + 3k ms.
    /// </summary>
    private static List<CollectorDurationBucket> TwentyBucketsWithOneBreak() =>
        Enumerable.Range(0, 20).Select(k => new CollectorDurationBucket(
            "wait_stats", Start.AddMinutes(10 * k + (k >= 5 ? 60 : 0)), 100 + 3 * k, 50 + k, k + 1)).ToList();

    /// <summary>Runs <paramref name="body"/> on an STA thread in US English (the chart is a WPF control), and rethrows what it threw.</summary>
    private static T OnStaThread<T>(Func<T> body)
    {
        T result = default!;
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try
            {
                CultureInfo.CurrentCulture = English;
                result = body();
            }
            catch (Exception ex)
            {
                error = ex;
            }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            ExceptionDispatchInfo.Capture(error).Throw();
        }

        return result;
    }

    /// <summary>The hover text over each of the line's twenty points in turn (the mouse exactly on the point), in bucket order.</summary>
    private static List<string?> HoverOverEachBucket(List<CollectorDurationBucket> buckets) =>
        OnStaThread(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            var hover = new ChartHoverHelper(chart, "ms", displayZone: () => TimeZoneInfo.Utc);
            var line = Assert.Single(CollectorDurationSeries.Build(buckets));
            ViewerServerTab.PlotCollectorDurationSeries(chart, hover, [line]);
            chart.Plot.Axes.AutoScale();
            using var render = chart.Plot.GetImage(1000, 400);

            return line.Times
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

    // ── The shared text: one wording for both apps ──

    /* The exact strings, for one run (singular), many runs, a thousands separator, a whole average and a fractional one. Lite pins the
       same strings through its own method (CollectorDurationDetailTextTests), so the two apps cannot drift apart. */
    [Theory]
    [InlineData(1L, 50.0, "Slowest of 1 run; average 50 ms")]
    [InlineData(12L, 61.0, "Slowest of 12 runs; average 61 ms")]
    [InlineData(1234L, 2000.0, "Slowest of 1,234 runs; average 2,000 ms")]
    [InlineData(10L, 910.8, "Slowest of 10 runs; average 910.8 ms")]
    [InlineData(12L, 0.5, "Slowest of 12 runs; average 0.5 ms")]
    public void TheSharedText_SaysTheRunCountAndTheAverage_InTheseExactWords(long runs, double average, string expected)
    {
        var text = OnStaThread(() => CollectorDurationHoverText.Detail(runs, average));

        Assert.Equal(expected, text);
    }

    /* The words follow the current culture, as Lite's always did, so the two apps read the same on one machine. */
    [Fact]
    public void TheSharedText_FollowsTheCurrentCulture()
    {
        string text = "";
        var thread = new Thread(() =>
        {
            CultureInfo.CurrentCulture = new CultureInfo("de-DE");
            text = CollectorDurationHoverText.Detail(1234, 910.8);
        });
        thread.Start();
        thread.Join();

        Assert.Equal("Slowest of 1.234 runs; average 910,8 ms", text);
    }

    /* The viewer words nothing itself: no source file in the viewer holds the text, so a second wording cannot grow back. */
    [Fact]
    public void TheViewer_HoldsNoCopyOfTheWords()
    {
        var offenders = Directory.EnumerateFiles(PathTo("Darling", "PerformanceMonitor.Darling.Viewer"), "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}") && !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}"))
            .Where(f => File.ReadAllText(f).Contains("Slowest of", StringComparison.Ordinal))
            .Select(Path.GetFileName)
            .ToList();

        Assert.Empty(offenders);
        Assert.Contains("CollectorDurationHoverText.Detail(Runs[i], AvgMs[i])", ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "CollectorDurationSeries.cs"), StringComparison.Ordinal);
    }

    // ── The viewer's hover ──

    /* The series carries each bucket's run count and average beside its slowest run, index for index with the X it is drawn at, and its
       lines are the shared words over those. */
    [Fact]
    public void TheSeriesLines_AreTheSharedWords_OverEachBucketsOwnRunsAndAverage_KeyedByItsX()
    {
        var line = Assert.Single(CollectorDurationSeries.Build(TwentyBucketsWithOneBreak()));

        var byX = OnStaThread(() => line.DetailsByX());

        Assert.Equal(20, byX.Count);
        for (var k = 0; k < 20; k++)
        {
            Assert.Equal($"Slowest of {k + 1} {(k == 0 ? "run" : "runs")}; average {50 + k} ms", byX[line.Times[k]]);
        }
    }

    /// <summary>
    /// The observed case: twenty buckets and one break. A line counted by position would say bucket 11's runs over bucket 10 (the break
    /// pushes it one point along the scatter) and nothing over the last bucket, whose place runs off the end of the lines. Each bucket
    /// says its own.
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
    /// The lines are keyed by the X each bucket is drawn at, and the point the line inserts at the break is none of them: every real
    /// point of the drawn scatter finds its own bucket's line, and the NaN point between the two buckets the break separates finds none.
    /// </summary>
    [Fact]
    public void TheLinesAreKeyedByX_SoEveryDrawnBucketFindsItsOwn_AndThePointTheBreakInsertsFindsNone()
    {
        var (line, drawn) = OnStaThread(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            var series = Assert.Single(CollectorDurationSeries.Build(TwentyBucketsWithOneBreak()));
            ViewerServerTab.PlotCollectorDurationSeries(chart, null, [series]);
            var scatter = Assert.Single(chart.Plot.PlottableList.OfType<ScottPlot.Plottables.Scatter>());

            return (series, scatter.Data.GetScatterPoints().ToList());
        });

        var byX = OnStaThread(() => line.DetailsByX());

        Assert.Equal(20, byX.Count);
        Assert.Equal(21, drawn.Count);
        var inserted = Assert.Single(drawn, p => double.IsNaN(p.Y));
        Assert.False(byX.ContainsKey(inserted.X), "the point the break inserted has a hover line");
        var real = drawn.Where(p => !double.IsNaN(p.Y)).ToList();
        Assert.Equal(20, real.Count);
        for (var k = 0; k < real.Count; k++)
        {
            Assert.Equal(CollectorDurationHoverText.Detail(line.Runs[k], line.AvgMs[k]), byX[real[k].X]);
        }
    }

    /* The render hands its lines to the plot step with the tab's hover, which is what puts the lines on the popup. */
    [Fact]
    public void TheRender_DrawsItsLinesThroughThePlotStep_WithTheTabsHover()
    {
        var source = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.CollectionHealth.cs");

        Assert.Single(Regex.Matches(source, @"PlotCollectorDurationSeries\(CollectorDurationChart,\s*_collectorDurationHover,\s*CollectorDurationSeries\.Build\(data\)\);"));
        Assert.Single(Regex.Matches(source, @"hover\?\.Add\(scatter,\s*line\.Collector,\s*line\.DetailsByX\(\)\);"));
    }
}
