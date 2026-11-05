/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7661: a web trend chart asks for about one point per 2 px of its plot, so a day does not come back as 1-minute buckets that a 1,114 px plot
/// cannot show. The page half runs the shipped <c>trend-points.js</c> under Node (<c>trend-points-harness.mjs</c>, skipped when Node is not
/// installed); the read half is the budget the <c>points</c> value becomes and the bucket width the ladder picks from it.
/// </summary>
public sealed class TrendPointsPageTests
{
    private static readonly Lazy<JsonElement> Result = new(Run);

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "trend-points-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the trend points harness did not finish in 60 s");
            }

            Assert.True(proc.ExitCode == 0, "the trend points harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void ThePage_AsksForFewerPointsAt1280ThanAt2560()
    {
        var run = Result.Value;
        var narrow = run.GetProperty("p1280").GetInt32();
        var wide = run.GetProperty("p2560").GetInt32();
        Assert.True(narrow < wide, $"1280 px asked for {narrow} points and 2560 px asked for {wide}");
        // One point per 2 px of the plot: the plot is the window less the sidebar and the card, so well under half the window's pixels.
        Assert.InRange(narrow, 200, 640);
        Assert.InRange(wide, 600, 1280);
        Assert.True(run.GetProperty("halfWidth2560").GetInt32() < wide, "a half-width panel has a narrower plot");
    }

    [Fact]
    public void ALevelRead_KeepsTheOldBudget_SoAShortSpikeKeepsItsHeight()
    {
        // The CPU chart draws only each bucket's mean: at a plot-sized ask a one-minute 100% spike over 0% would draw as about 20%.
        var run = Result.Value;
        Assert.Equal(JsonValueKind.Null, run.GetProperty("cpu1280").ValueKind);
        Assert.Equal(JsonValueKind.Null, run.GetProperty("pgCpu1280").ValueKind);
        Assert.Equal(JsonValueKind.Null, run.GetProperty("serverLevel").ValueKind);
        Assert.Equal(JsonValueKind.Null, run.GetProperty("perfmonGauge").ValueKind);
        Assert.Equal(JsonValueKind.Null, run.GetProperty("readToolCpuLinePlot").ValueKind);
        // A rate keeps the ask: the bucket's mean is its honest value.
        Assert.Equal(run.GetProperty("p1280").GetInt32(), run.GetProperty("serverRate").GetInt32());
        Assert.Equal(run.GetProperty("p1280").GetInt32(), run.GetProperty("perfmonRate").GetInt32());
    }

    [Fact]
    public void ATableOrStatPanel_AndACallerThatNamesNoPlot_SendNoPoints_ButALinePanelStillDoes()
    {
        var run = Result.Value;
        // A saved-view panel measures a plot only when it draws a line.
        Assert.Equal(1114, run.GetProperty("panelLine").GetInt32());
        Assert.Equal(0, run.GetProperty("panelTable").GetInt32());
        Assert.Equal(0, run.GetProperty("panelStat").GetInt32());
        Assert.Equal(0, run.GetProperty("panelBandlist").GetInt32());
        // The real readTool on a 1280 px page: no plot named, no points (it used to estimate one from the window).
        Assert.Equal(JsonValueKind.Null, run.GetProperty("readToolNoPlot").ValueKind);
        Assert.Equal(JsonValueKind.Null, run.GetProperty("readToolNoPlotCaller").ValueKind);
        Assert.Equal(557, run.GetProperty("readToolLinePlot").GetInt32());
    }

    /// <summary>
    /// The page's two sets of reads (the rate reads that take a plot-sized ask, and the level reads that do not) are exactly the routes of
    /// <c>/api/read</c> that take the chart budget, so a route added to WebReads.cs and not classified in trend-points.js fails here. There is no
    /// behavioural way to list the routes (the table holds delegates over a database), so the route names are read from the table's text.
    /// </summary>
    [Fact]
    public void TheClassifiedReads_AreExactlyTheRoutesThatTakeTheChartBudget()
    {
        var web = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.WebReads.cs");
        var entries = System.Text.RegularExpressions.Regex.Split(web, @"(?=\[""get_[a-z_]+""\] = )");
        var routes = entries
            .Where(e => e.Contains("ChartBudget(c)", StringComparison.Ordinal))
            .Select(e => System.Text.RegularExpressions.Regex.Match(e, @"^\[""(get_[a-z_]+)""\]").Groups[1].Value)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();
        var run = Result.Value;
        var kept = run.GetProperty("keptReads").EnumerateArray().Select(e => e.GetString()!).ToArray();
        var meanOnly = run.GetProperty("meanOnlyReads").EnumerateArray().Select(e => e.GetString()!).ToArray();
        Assert.Empty(kept.Intersect(meanOnly));
        Assert.Equal(routes, kept.Concat(meanOnly).OrderBy(n => n, StringComparer.Ordinal).ToArray());
        Assert.Equal(16, routes.Length);
    }

    [Fact]
    public void AMeasuredPanel_AsksOnePointPerTwoPixelsOfItsPlot()
    {
        // The plot of the 1,114 px chart in the issue: 557 points.
        Assert.Equal(557, Result.Value.GetProperty("measured1114").GetInt32());
    }

    [Fact]
    public void AReadThatIsNotATrend_AWidthTheCallerNamed_AndAnUnmeasurablePanel_AreLeftAlone()
    {
        var run = Result.Value;
        Assert.Equal(JsonValueKind.Null, run.GetProperty("notATrend").ValueKind);
        Assert.True(run.GetProperty("untouched").GetBoolean());
        Assert.Equal(JsonValueKind.Null, run.GetProperty("namedWidth").ValueKind);
        Assert.Equal(99, run.GetProperty("namedPoints").GetInt32());
        Assert.Equal(JsonValueKind.Null, run.GetProperty("unmeasurable").ValueKind);
    }

    [Theory]
    [InlineData(24, 557, 5)]
    [InlineData(24, 1150, 2)]
    [InlineData(24, 280, 10)]
    [InlineData(168, 557, 30)]
    [InlineData(168, 1150, 10)]
    public void TheRead_PicksTheWidestLadderWidthThatFitsThePlot_ForADayAndAWeek(int hours, int asked, int expectedMinutes)
    {
        Assert.Null(TrendBuckets.Resolve(hours, null, 1, TrendBudget.ChartFor(asked), out var minutes));
        Assert.Equal(expectedMinutes, minutes);
        Assert.True(TrendBuckets.PointsFor(hours * 60, minutes, 1) <= asked);
    }

    [Fact]
    public void TheAsk_IsClampedAtBothEnds_AndNeverFinerThanTheChartBudgetWas()
    {
        // Floor: a sliver of a panel still gets a line, 30-minute points for a day (49 of 60).
        Assert.Null(TrendBuckets.Resolve(24, null, 1, TrendBudget.ChartFor(3), out var floor));
        Assert.Equal(30, floor);
        Assert.Null(TrendBuckets.Resolve(24, null, 1, TrendBudget.ChartFor(-40), out var negative));
        Assert.Equal(30, negative);
        // Ceiling: a huge ask is today's budget, 1-minute points for a day.
        Assert.Null(TrendBuckets.Resolve(24, null, 1, TrendBudget.ChartFor(100000), out var ceiling));
        Assert.Equal(1, ceiling);
        Assert.Null(TrendBuckets.Resolve(24, null, 1, TrendBudget.Chart, out var today));
        Assert.Equal(today, ceiling);
        // Several series: the ask is per series but the total stays under today's cap, so an eight-line chart is never finer than before.
        Assert.Null(TrendBuckets.Resolve(24, null, 8, TrendBudget.ChartFor(1150), out var many));
        Assert.Null(TrendBuckets.Resolve(24, null, 8, TrendBudget.Chart, out var manyToday));
        Assert.Equal(manyToday, many);
        // An explicit width is still held to the chart budget's cap, whatever the page asked.
        Assert.Null(TrendBuckets.Resolve(24, 1, 1, TrendBudget.ChartFor(60), out var explicitWidth));
        Assert.Equal(1, explicitWidth);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("many")]
    public void AnAbsentOrUnreadablePointsValue_IsTheFullChartBudget(string? raw) =>
        Assert.Equal(TrendBudget.Chart, DarlingWebEndpoints.ChartBudgetFrom(raw));

    [Fact]
    public void APointsValue_BecomesAPlotSizedBudget() =>
        Assert.Equal(TrendBudget.ChartFor(557), DarlingWebEndpoints.ChartBudgetFrom("557"));
}
