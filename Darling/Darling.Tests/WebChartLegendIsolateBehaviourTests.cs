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
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// A web line chart's legend hides, shows and isolates a series (#5247). These run the shipped <c>charts.js</c> under
/// Node (<c>web-chart-legend-harness.mjs</c>). Skipped when Node is not installed.
/// </summary>
public sealed class WebChartLegendIsolateBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-chart-legend-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped chart script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the chart legend harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the chart legend harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    private static JsonElement S(JsonElement r, string name) => r.GetProperty(name);
    private static string[] Off(JsonElement s) => s.GetProperty("off").EnumerateArray().Select(e => e.GetString()!).ToArray();
    private static int Lines(JsonElement s) => s.GetProperty("lines").GetInt32();
    private static double Top(JsonElement s) => s.GetProperty("top").GetDouble();

    [Fact]
    public void ClickingAnEntry_HidesTheSeries_AndClickingAgainShowsIt()
    {
        var r = Run();
        Assert.Equal(3, Lines(S(r, "initial")));
        Assert.Equal(2, Lines(S(r, "afterHide")));
        Assert.Equal(3, Lines(S(r, "afterShow")));
        Assert.Empty(Off(S(r, "afterShow")));
    }

    [Fact]
    public void AHiddenSeries_StaysInTheLegend_MarkedHidden()
    {
        var hidden = S(Run(), "afterHide");
        Assert.Equal(new[] { "BIG" }, Off(hidden));
        Assert.Equal("false", hidden.GetProperty("pressed").GetProperty("BIG").GetString());
        Assert.Equal("true", hidden.GetProperty("pressed").GetProperty("SMALL").GetString());
    }

    [Fact]
    public void TheAxis_RescalesToTheVisibleSeries()
    {
        var r = Run();
        Assert.True(Top(S(r, "initial")) >= 1900, "the dominant series sets the full axis");
        Assert.True(Top(S(r, "rescaled")) <= 10, "with it hidden, the axis fits the small series");
        Assert.True(Top(S(r, "isolated")) <= 5);
        Assert.Equal(Top(S(r, "initial")), Top(S(r, "afterShow")));
    }

    [Fact]
    public void AStackedChart_RestacksOverTheVisibleSeries()
    {
        var r = Run();
        Assert.True(Top(S(r, "stackedBefore")) >= 2900);
        Assert.True(Top(S(r, "stackedAfter")) <= 10);
    }

    [Fact]
    public void Isolate_ShowsExactlyOneSeries_AndASecondIsolateRestoresAll()
    {
        var r = Run();
        Assert.Equal(1, Lines(S(r, "isolated")));
        Assert.Equal(new[] { "BIG", "MID" }, Off(S(r, "isolated")));
        Assert.Equal(3, Lines(S(r, "isolateUndone")));
    }

    [Fact]
    public void ShowAll_AppearsOnlyWhileSomethingIsHidden_AndRestoresEverything()
    {
        var r = Run();
        Assert.False(S(r, "initial").GetProperty("showAll").GetBoolean());
        Assert.True(S(r, "afterHide").GetProperty("showAll").GetBoolean());
        Assert.Equal("Show all (2 hidden)", S(r, "beforeShowAll").GetProperty("showAllText").GetString());
        var after = S(r, "afterShowAll");
        Assert.False(after.GetProperty("showAll").GetBoolean());
        Assert.Equal(3, Lines(after));
    }

    [Fact]
    public void ToggleNeverHidesTheLastVisibleSeries()
    {
        var kept = S(Run(), "lastKept");
        Assert.Equal(1, Lines(kept));
        Assert.Equal(new[] { "BIG", "SMALL" }, Off(kept));
    }

    [Fact]
    public void TheHiddenState_SurvivesARebuildOfTheSameChart()
    {
        var r = Run();
        Assert.Equal(new[] { "BIG" }, Off(S(r, "rebuilt")));
        Assert.Equal(2, Lines(S(r, "rebuilt")));
        Assert.Equal("big", S(r, "held")[0].GetString());
    }

    [Fact]
    public void TheHiddenState_DoesNotLeakToAnotherServerRangeOrChart()
    {
        var r = Run();
        foreach (var other in new[] { "otherServer", "otherRange", "otherChart" })
        {
            Assert.Empty(Off(S(r, other)));
            Assert.Equal(3, Lines(S(r, other)));
        }
    }

    [Fact]
    public void ExportToCsv_CarriesExactlyTheShownSeries()
    {
        var r = Run();
        Assert.Equal(new[] { "BIG", "SMALL", "MID" }, S(r, "csvAll").EnumerateArray().Select(e => e.GetString()!).ToArray());
        Assert.Equal(new[] { "SMALL", "MID" }, S(r, "csvHidden").EnumerateArray().Select(e => e.GetString()!).ToArray());
    }
}
