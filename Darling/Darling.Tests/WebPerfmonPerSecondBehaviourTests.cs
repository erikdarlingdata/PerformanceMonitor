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
/// What the web Perfmon Counters panel draws, from the shipped <c>server-tabs.js</c> and <c>charts.js</c> run under
/// Node (<c>web-perfmon-harness.mjs</c>) against the rows <c>get_perfmon_stats</c> and <c>get_perfmon_trend</c> send.
/// A rate counter's stored value is its running total (Batch Requests/sec 11,641 with a delta of 66 read as 11,641 a
/// second), so the grid shows the row's <c>per_second</c> in a Per second column and the running total under a
/// Total since counter start header, and the trend chart draws the rate's <c>per_second</c> line, its axis labels
/// and its tooltip printed so that a real rate never reads as 0. A gauge keeps its reading in the grid and its chart.
/// Node is skipped when it is not installed, the way <see cref="WebRenderSettleTests"/> does.
/// </summary>
public sealed class WebPerfmonPerSecondBehaviourTests
{
    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-perfmon-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the perfmon harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the perfmon harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            Assert.Empty(result.GetProperty("errors").EnumerateArray());
            Assert.Empty(result.GetProperty("rejections").EnumerateArray());
            return true;
        }
    }

    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.GetString()!).ToArray();

    private static string[] Row(JsonElement result, string counter) =>
        result.GetProperty("rows").EnumerateArray().Select(Strings).Single(cells => cells[0] == counter);

    /// <summary>The rows a chart's tooltip showed with the pointer at its left or right edge, as [label, value] pairs.</summary>
    private static string[][] Tooltip(JsonElement chart, string edge) =>
        chart.GetProperty("tooltips").GetProperty(edge).EnumerateArray().Select(Strings).ToArray();

    /// <summary>The rate under Per second, and the running total under a header that says it is a total: Value is a
    /// reading, and a total under it would read as a rate beside the delta.</summary>
    [Fact]
    public void TheGrid_ShowsARateCounterPerSecond_AndItsRunningTotalUnderATotalHeader()
    {
        if (!TryRun("mixed", out var r)) return;

        Assert.Equal(new[] { "Counter", "Instance", "Per second", "Total since counter start", "Value", "Delta" }, Strings(r.GetProperty("headers")));
        Assert.Equal(new[] { "Batch Requests/sec", "—", "0.22", "11,641", "—", "66.00" }, Row(r, "Batch Requests/sec"));

        /* The four number columns line up on the right, as every number column in the grids does. */
        Assert.Equal(
            new[] { false, false, true, true, true, true },
            r.GetProperty("numericHeaders").EnumerateArray().Select(h => h.GetBoolean()).ToArray());
    }

    /// <summary>A rate with no knowable delta (a first collection, a counter reset, a restart) has no rate to show,
    /// and the 0 stored beside its interval of 0 is a stand-in, not a count: those two cells are blank. Its running
    /// total was measured all the same, so it stays.</summary>
    [Fact]
    public void ARateRowWithNoKnowableDelta_KeepsItsTotal_AndShowsNoRateAndNoDelta()
    {
        if (!TryRun("mixed", out var r)) return;

        Assert.Equal(new[] { "SQL Compilations/sec", "—", "—", "4,000", "—", "—" }, Row(r, "SQL Compilations/sec"));
    }

    /// <summary>A counter that seldom fires: one deadlock in 300 s is 0.0033 a second, and the total of 37 is the
    /// count a reader of the grid has. Neither reads as 0.</summary>
    [Fact]
    public void ARateThatSeldomFires_ShowsItsRealRate_BesideItsTotal()
    {
        if (!TryRun("mixed", out var r)) return;

        Assert.Equal(new[] { "Number of Deadlocks/sec", "—", "0.0033", "37", "—", "1.00" }, Row(r, "Number of Deadlocks/sec"));
    }

    /// <summary>A gauge's value is its reading, and an average's numerator keeps its value and delta: only a rate's
    /// row changes.</summary>
    [Fact]
    public void AGaugeRow_AndAnAverageRow_KeepTheirNumbers()
    {
        if (!TryRun("mixed", out var r)) return;

        Assert.Equal(new[] { "Total Server Memory (KB)", "—", "—", "—", "8,000,000.00", "—" }, Row(r, "Total Server Memory (KB)"));
        Assert.Equal(new[] { "Lock waits", "Average wait time (ms)", "—", "—", "5,000.00", "40.00" }, Row(r, "Lock waits"));
    }

    [Fact]
    public void TheTrendChart_DrawsARateCounterPerSecond()
    {
        if (!TryRun("mixed", out var r)) return;

        var chart = Assert.Single(r.GetProperty("charts").EnumerateArray());
        var line = Assert.Single(chart.GetProperty("series").EnumerateArray());
        Assert.Equal("per_second", line.GetProperty("key").GetString());
        Assert.Equal("Per second", line.GetProperty("label").GetString());
        Assert.Equal("/s", chart.GetProperty("unit").GetString());

        /* The first point's rate was not knowable, so it reads as no number; the second is 66 over 300 s. */
        Assert.Equal(new[] { new[] { "Per second", "—" } }, Tooltip(chart, "left"));
        Assert.Equal(new[] { new[] { "Per second", "0.22" } }, Tooltip(chart, "right"));
    }

    /// <summary>One deadlock in a 300 s collection is 0.0033 a second. Printed to two decimals it was 0, on every
    /// label of the value axis (0 0 0 0 0) and in the tooltip. Only an idle interval, a true 0, reads as 0.</summary>
    [Fact]
    public void TheTrendChart_ForARateThatSeldomFires_NeverReadsItsRateAsZero()
    {
        if (!TryRun("lowrate", out var r)) return;

        var chart = Assert.Single(r.GetProperty("charts").EnumerateArray());
        Assert.Equal(new[] { "0", "0.001", "0.002", "0.003", "0.004" }, Strings(chart.GetProperty("axis")));
        Assert.Equal(new[] { new[] { "Per second", "0.0033" } }, Tooltip(chart, "right"));
        Assert.Equal(new[] { new[] { "Per second", "0" } }, Tooltip(chart, "left"));
    }

    /// <summary>A bucket a day wide holds 86,400 s, so one deadlock in it is 0.000012 a second: smaller than any
    /// rate a short bucket can show, and still not 0.</summary>
    [Fact]
    public void TheTrendChart_ForALongRangeBucket_NeverReadsItsRateAsZero()
    {
        if (!TryRun("longbucket", out var r)) return;

        var chart = Assert.Single(r.GetProperty("charts").EnumerateArray());
        Assert.Equal(new[] { "0", "0.000005", "0.00001", "0.000015" }, Strings(chart.GetProperty("axis")));
        Assert.Equal(new[] { new[] { "Per second", "0.000012" } }, Tooltip(chart, "right"));
    }

    /// <summary>A gauge's chart is what it was: its reading, and the delta line its points leave empty.</summary>
    [Fact]
    public void TheTrendChart_ForAGauge_IsUnchanged()
    {
        if (!TryRun("gauge", out var r)) return;

        var chart = Assert.Single(r.GetProperty("charts").EnumerateArray());
        Assert.Equal(new[] { "value", "delta_value" }, chart.GetProperty("series").EnumerateArray().Select(s => s.GetProperty("key").GetString()!).ToArray());
        Assert.Equal(new[] { "Value", "Delta" }, chart.GetProperty("series").EnumerateArray().Select(s => s.GetProperty("label").GetString()!).ToArray());
        Assert.Equal(JsonValueKind.Null, chart.GetProperty("unit").ValueKind);

        /* ...and it prints as it did: grouped whole numbers, with no digits added for it. */
        Assert.Equal(new[] { "0", "2,000,000", "4,000,000", "6,000,000", "8,000,000" }, Strings(chart.GetProperty("axis")));
        Assert.Equal(new[] { new[] { "Value", "8,000,000" }, new[] { "Delta", "—" } }, Tooltip(chart, "right"));
    }
}
