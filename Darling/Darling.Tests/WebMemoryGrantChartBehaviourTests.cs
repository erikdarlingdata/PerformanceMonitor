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
/// What the web Memory tab's two memory-grant charts draw (#6299), from the shipped <c>memory-grant-charts.js</c> run under Node
/// (<c>web-memory-grant-chart-harness.mjs</c>) against the series <c>get_memory_grants</c> sends with <c>chart: true</c>: one
/// line per pool per metric over the whole window, a snapshot with no grants drawn as a point of zeros, and a window with no
/// snapshot drawn as the empty sentence and no chart. Node is skipped when it is not installed.
/// </summary>
public sealed class WebMemoryGrantChartBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-memory-grant-chart-harness.mjs"));
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
                Assert.Fail("the memory grant chart harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the memory grant chart harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            var result = doc.RootElement.Clone();
            Assert.Empty(result.GetProperty("rejections").EnumerateArray());
            return result;
        }
    }

    [Fact]
    public void TheWindowIsDrawnAsOneLinePerPoolPerMetric_OnePointPerCollectionTime()
    {
        var r = Run("series");
        Assert.Equal(new[] { "Memory Grant Sizing", "Memory Grant Activity" }, r.GetProperty("panels").EnumerateArray().Select(p => p.GetString()).ToArray());
        var charts = r.GetProperty("charts").EnumerateArray().ToArray();
        Assert.Equal(2, charts.Length);

        // Sizing: the desktop's five metrics (the two ceilings included) for each of two pools. Activity: four metrics each.
        var sizing = charts[0];
        Assert.Equal(10, sizing.GetProperty("series").GetArrayLength());
        Assert.Contains("Pool 2: Max Target MB (ceiling)", sizing.GetProperty("series").EnumerateArray().Select(s => s.GetProperty("label").GetString()));
        var activity = charts[1];
        Assert.Equal(8, activity.GetProperty("series").GetArrayLength());
        Assert.Contains("Pool 1: Forced Grants", activity.GetProperty("series").EnumerateArray().Select(s => s.GetProperty("label").GetString()));

        // Three collection times, each its own x position: the window is a series, not one instant.
        var times = sizing.GetProperty("points").EnumerateArray().Select(p => p.GetProperty("collection_time").GetString()).ToArray();
        Assert.Equal(3, times.Distinct().Count());
        Assert.Equal(times.OrderBy(t => t, StringComparer.Ordinal).ToArray(), times);

        // One read serves both charts, for the whole window as a chart read.
        var fetch = Assert.Single(r.GetProperty("fetches").EnumerateArray()).GetString()!;
        Assert.Contains("chart=true", fetch, StringComparison.Ordinal);
        Assert.Contains("get_memory_grants", fetch, StringComparison.Ordinal);
    }

    [Fact]
    public void ASnapshotWithNoGrants_IsAPointOfZeros_NotAGap()
    {
        var sizing = Run("series").GetProperty("charts")[0];
        var middle = sizing.GetProperty("points")[1];
        Assert.Equal("2026-01-02T10:05:00.0000000", middle.GetProperty("collection_time").GetString());
        // Pool 1 at 10:05 collected with nothing granted or used: 0 on the line, with the level it did report beside it.
        Assert.Equal(0, middle.GetProperty("p1_granted_memory_mb").GetDouble());
        Assert.Equal(0, middle.GetProperty("p1_used_memory_mb").GetDouble());
        Assert.Equal(1000, middle.GetProperty("p1_available_memory_mb").GetDouble());

        var activityMiddle = Run("series").GetProperty("charts")[1].GetProperty("points")[1];
        Assert.Equal(0, activityMiddle.GetProperty("p1_waiter_count").GetDouble());
        Assert.Equal(0, activityMiddle.GetProperty("p2_grantee_count").GetDouble());
    }

    [Theory]
    [InlineData("empty")]
    [InlineData("noentries")]
    public void AWindowWithNoSnapshot_ShowsTheEmptyText_AndNoChartAtAll(string scenario)
    {
        var r = Run(scenario);
        Assert.Empty(r.GetProperty("charts").EnumerateArray());
        Assert.Equal(2, r.GetProperty("empties").GetArrayLength());
        Assert.Empty(r.GetProperty("errors").EnumerateArray());
    }

    [Fact]
    public void AFailedRead_IsAnError_NotAnEmptyChart()
    {
        var r = Run("failed");
        Assert.Empty(r.GetProperty("charts").EnumerateArray());
        Assert.Equal(2, r.GetProperty("errors").GetArrayLength());
    }
}
