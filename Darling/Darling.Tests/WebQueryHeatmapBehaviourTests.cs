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
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The web Query Heatmap colour grid (#6293), run under Node (<c>web-query-heatmap-harness.mjs</c>) with the shipped page script on a
/// fake DOM: how the read's cells are laid out by time and magnitude bucket, the shade each count gets, what hover and a click show,
/// and that "Show Active Queries at This Time" opens the Active Queries tab at the cell's time. Node is skipped when it is not installed.
/// </summary>
public sealed class WebQueryHeatmapBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-query-heatmap-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);
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
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the query heatmap harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the query heatmap harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    /// <summary>
    /// Three cells (14:00, 14:05 and 14:20) become 5 time columns by 7 magnitude rows: the two quiet bins between are empty columns, not
    /// squeezed out, and each count's shade rises with the count while an empty cell stays 0, so "no query" is never the lightest shade.
    /// </summary>
    [Fact]
    public void TheCellsLayOutOnATimeByMagnitudeGrid_WithQuietBinsKeptAsEmptyColumns_AndShadesThatRiseWithTheCount()
    {
        var r = Run("model");

        Assert.Equal(5, r.GetProperty("columns").GetInt32());
        Assert.Equal(7, r.GetProperty("rows").GetInt32());
        Assert.Equal(3, r.GetProperty("filled").GetInt32());
        Assert.True(r.GetProperty("gapColumnsEmpty").GetBoolean());
        Assert.Equal(40, r.GetProperty("max").GetInt32());

        var low = r.GetProperty("shadeLow").GetInt32();
        var mid = r.GetProperty("shadeMid").GetInt32();
        var high = r.GetProperty("shadeHigh").GetInt32();
        Assert.True(low >= 1 && low < mid && mid < high && high == 8, $"low {low}, mid {mid}, high {high}");
        Assert.Equal(0, r.GetProperty("shadeOfZero").GetInt32());
        Assert.Equal(0, r.GetProperty("shadeOfNull").GetInt32());
        Assert.Equal(8, r.GetProperty("shadeOfOneOfOne").GetInt32());
        Assert.True(r.GetProperty("monotone").GetBoolean());
        Assert.True(r.GetProperty("empty").GetBoolean());
    }

    /// <summary>
    /// The bin is the Viewer's 5 minutes up to 12 hours, then widens so a window stays near 144 columns (the read keeps 1000 cells, newest
    /// first). The metric picker is the Viewer's, minus logical reads.
    /// </summary>
    [Fact]
    public void TheBinWidthWidensWithTheWindow_AndTheMetricsAreTheViewersWithoutLogicalReads()
    {
        var r = Run("model");

        Assert.Equal(new[] { 5, 5, 10, 70, 1440 }, r.GetProperty("bins").EnumerateArray().Select(e => e.GetInt32()).ToArray());
        Assert.Equal(new[] { "duration", "cpu", "logical_writes", "execution_count" }, r.GetProperty("metrics").EnumerateArray().Select(e => e.GetString()).ToArray());
    }

    /// <summary>
    /// 32 of the 35 cells are empty (outlined, no shade class) and 3 are shaded; hover and click show the cell's values; the action button
    /// appears only for the clicked cell and opens Active Queries at the middle of its bin; "Show as table" swaps in the table of the same
    /// cells and back.
    /// </summary>
    [Fact]
    public void HoverShowsTheCellsValues_AClickOffersShowActiveQueriesAtThatTime_AndTheTableIsOneClickAway()
    {
        var r = Run("render");

        Assert.Equal(35, r.GetProperty("cells").GetInt32());
        Assert.Equal(32, r.GetProperty("emptyCells").GetInt32());
        Assert.Equal(3, r.GetProperty("shaded").GetInt32());

        var hover = r.GetProperty("hoverText").GetString()!;
        Assert.Contains("100ms-1s", hover, StringComparison.Ordinal);
        Assert.Contains("40 queries", hover, StringComparison.Ordinal);
        Assert.Contains("SELECT 1", hover, StringComparison.Ordinal);
        Assert.Equal(hover, r.GetProperty("tooltip").GetString());
        Assert.True(r.GetProperty("openHiddenAtStart").GetBoolean());
        Assert.True(r.GetProperty("openHiddenOnHover").GetBoolean());
        Assert.True(r.GetProperty("afterLeave").GetBoolean());
        Assert.False(r.GetProperty("openHiddenAfterClick").GetBoolean());

        // 2026-10-21 14:05:00 UTC plus half the 5-minute bin.
        var calls = r.GetProperty("calls").EnumerateArray().ToArray();
        var call = Assert.Single(calls);
        Assert.Equal("SRV1", call.GetProperty("server").GetString());
        Assert.Equal("queries", call.GetProperty("item").GetString());
        Assert.Equal(new DateTimeOffset(2026, 10, 21, 14, 5, 0, TimeSpan.Zero).ToUnixTimeMilliseconds() + 150_000, call.GetProperty("tMs").GetInt64());

        Assert.True(r.GetProperty("gridShownAtStart").GetBoolean());
        Assert.True(r.GetProperty("tableHiddenAtStart").GetBoolean());
        Assert.True(r.GetProperty("tableShownAfterToggle").GetBoolean());
        Assert.True(r.GetProperty("gridHiddenAfterToggle").GetBoolean());
        Assert.Equal("true", r.GetProperty("pressed").GetString());
        Assert.Equal("3", r.GetProperty("tableRows").GetString());
        Assert.True(r.GetProperty("gridBack").GetBoolean());
    }

    /// <summary>An answer with no cells draws no grid and no empty cells: the table's own empty text shows, never a grid of blanks.</summary>
    [Fact]
    public void TheCaptionSaysWhatIsAtTheTop_ForTheMetricPicked()
    {
        // #6353: the caption said "the slowest queries are at the top" for every metric; the rows follow the metric picked.
        var captions = Run("caption").GetProperty("captions");

        Assert.Contains("the slowest queries are at the top", captions.GetProperty("duration").GetString());
        Assert.Contains("the most-executed queries are at the top", captions.GetProperty("execution_count").GetString());
        Assert.DoesNotContain("slowest", captions.GetProperty("execution_count").GetString());
        Assert.Contains("the queries using the most CPU are at the top", captions.GetProperty("cpu").GetString());
        Assert.Contains("the queries doing the most logical writes are at the top", captions.GetProperty("logical_writes").GetString());
    }

    [Fact]
    public void AnAnswerWithNoCells_DrawsNoGrid()
    {
        var r = Run("renderEmpty");

        Assert.Equal(0, r.GetProperty("tds").GetInt32());
        Assert.Equal(0, r.GetProperty("toggle").GetInt32());
        Assert.True(r.GetProperty("tableShown").GetBoolean());
    }
}
