/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #6296, #6297, #6298: the desktop viewer draws file I/O throughput, the tempdb allocated size and file I/O latency, and the collectors'
/// durations; the web page's File I/O and Collection Health tabs now draw them from <c>get_server_trend</c>. The shipped page runs under Node
/// (<c>web-trend-series-harness.mjs</c>) against a scripted read, and these tests read what each panel drew: the lines in its legend, the top of
/// its value axis, and the sentence it shows when the read has nothing. Node is skipped when it is not installed.
/// </summary>
public sealed class WebTrendSeriesBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-trend-series-harness.mjs"));
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
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the trend series harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the trend series harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static JsonElement Card(JsonElement run, string name)
    {
        var card = run.GetProperty(name);
        Assert.True(card.GetProperty("found").GetBoolean(), name + " must be drawn");
        return card;
    }

    private static string[] Labels(JsonElement chart) => chart.GetProperty("labels").EnumerateArray().Select(e => e.GetString()!).ToArray();

    private static string Top(JsonElement chart) => chart.GetProperty("ticks").EnumerateArray().Select(e => e.GetString()!).Last();

    /// <summary>File I/O throughput is two charts, read then write, one line per file; a point with no figure is left out, never drawn as 0.</summary>
    [Fact]
    public void Throughput_DrawsAReadChartAndAWriteChart_OneLinePerFile()
    {
        var card = Card(Run("data"), "throughput");
        Assert.Equal(["Read throughput (MB/s)", "Write throughput (MB/s)"], card.GetProperty("headings").EnumerateArray().Select(e => e.GetString()!));
        var charts = card.GetProperty("charts").EnumerateArray().ToList();
        Assert.Equal(2, charts.Count);
        Assert.Equal(["SalesDb.sales_data", "SalesDb.sales_log"], Labels(charts[0]));
        Assert.Equal(["SalesDb.sales_data", "SalesDb.sales_log"], Labels(charts[1]));
        Assert.Equal("15 MB/s", Top(charts[0]));
        Assert.Equal("10 MB/s", Top(charts[1]));
        // #6359: the width and the summed-read fact, never the read's MCP-facing aggregate_note.
        Assert.Contains("Each point covers 15 minutes. A bucket the window cuts short holds smaller counts; rates are true rates over the time the bucket covers.", card.GetProperty("notes").EnumerateArray().Select(e => e.GetString()));
        Assert.DoesNotContain(card.GetProperty("notes").EnumerateArray().Select(e => e.GetString()), n => n!.Contains("bucket_minutes", System.StringComparison.Ordinal));
    }

    /// <summary>The tempdb file chart plots each file's read latency plus its write latency, as the desktop does, and a file with no figure at all has no line.</summary>
    [Fact]
    public void TempDbFileIo_PlotsReadPlusWriteLatency_AndSkipsAFileWithNoFigure()
    {
        var chart = Card(Run("data"), "tempdbFileIo").GetProperty("charts").EnumerateArray().Single();
        Assert.Equal(["tempdev"], Labels(chart));
        Assert.Equal("10 ms", Top(chart));
    }

    /// <summary>The allocated size is one line, labelled as the desktop labels it.</summary>
    [Fact]
    public void TempDbSize_DrawsOneAllocatedLine()
    {
        var chart = Card(Run("data"), "tempdbSize").GetProperty("charts").EnumerateArray().Single();
        Assert.Equal(["Allocated MB"], Labels(chart));
    }

    /// <summary>A collector needs two buckets to draw a line, as in the desktop; one with a single bucket in the window draws none.</summary>
    [Fact]
    public void CollectorDuration_DrawsACollectorOnlyWithTwoBuckets()
    {
        var chart = Card(Run("data"), "collectors").GetProperty("charts").EnumerateArray().Single();
        Assert.Equal(["wait_stats"], Labels(chart));
        Assert.Equal("1,000 ms", Top(chart));
    }

    /// <summary>Every chart says why it is empty in the read's own sentence, and draws no chart.</summary>
    [Theory]
    [InlineData("throughput", "THROUGHPUT EMPTY SENTENCE")]
    [InlineData("tempdbSize", "TEMPDB SIZE EMPTY SENTENCE")]
    [InlineData("tempdbFileIo", "TEMPDB FILE IO EMPTY SENTENCE")]
    [InlineData("collectors", "COLLECTOR EMPTY SENTENCE")]
    public void AnEmptyRead_ShowsItsSentence_AndNoChart(string card, string sentence)
    {
        var drawn = Card(Run("empty"), card);
        Assert.Equal([sentence], drawn.GetProperty("strips").EnumerateArray().Select(e => e.GetString()!));
        Assert.Empty(drawn.GetProperty("charts").EnumerateArray());
    }

    /// <summary>
    /// A quiet window answers the service's own sentence, which says "in the last 48 hours" whatever window the page asked for (it never sees
    /// the custom range's end). The page shows the panel's sentence about "this window" instead, as the panels that set emptyWins do, so a custom
    /// past window is never described as the last 48 hours.
    /// </summary>
    [Theory]
    [InlineData("throughput")]
    [InlineData("tempdbSize")]
    [InlineData("tempdbFileIo")]
    [InlineData("collectors")]
    public void AQuietCustomPastWindow_NeverSaysItIsTheLast48Hours(string card)
    {
        var drawn = Card(Run("quiet"), card);
        var strips = drawn.GetProperty("strips").EnumerateArray().Select(e => e.GetString()!).ToList();
        var sentence = Assert.Single(strips);
        Assert.Contains("in this window", sentence);
        Assert.DoesNotContain("48 hours", sentence);
        Assert.Empty(drawn.GetProperty("charts").EnumerateArray());
    }
}
