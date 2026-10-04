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
/// What the web Memory tab's pressure events panels draw, from the shipped <c>server-tabs.js</c> run under Node
/// (<c>web-memory-pressure-harness.mjs</c>) against the rows <c>get_memory_pressure_events</c> sends: the samples are
/// counted per hour the way the desktop chart counts them (indicator 2 medium, 3 and above severe, per source), hours
/// with no sample are drawn as 0 across the window, and the notice says what a 0 means. Node is skipped when it is not
/// installed.
/// </summary>
public sealed class WebMemoryPressureChartBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-memory-pressure-harness.mjs"));
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
                Assert.Fail("the memory pressure harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the memory pressure harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            var result = doc.RootElement.Clone();
            Assert.Empty(result.GetProperty("errors").EnumerateArray());
            Assert.Empty(result.GetProperty("rejections").EnumerateArray());
            return result;
        }
    }

    [Fact]
    public void Samples_AreCountedPerHour_ByPressureSourceAndSeverity()
    {
        var chart = Run("counts").GetProperty("charts")[0];
        var rows = chart.GetProperty("points").EnumerateArray()
            .Select(p => p.EnumerateArray().Select(v => v.ValueKind == JsonValueKind.Number ? v.GetInt32() : -1).Skip(1).ToArray())
            .ToArray();

        // Hours oldest first: [sql medium, sql severe, os medium, os severe]. One sample 3 hours back; an hour 1 back holds
        // a SQL medium, a SQL severe with OS medium, and an OS severe; the OS-only normal sample is not drawn.
        Assert.Contains(new[] { 1, 0, 0, 0 }, rows);
        Assert.Contains(new[] { 1, 1, 1, 1 }, rows);
        Assert.Equal(2, rows.Count(r => r.Sum() > 0));
        Assert.Equal(4, chart.GetProperty("series").GetArrayLength());
        Assert.Equal("events", chart.GetProperty("unit").GetString());
    }

    [Fact]
    public void HoursWithNoSample_AreDrawnAsZero_AcrossTheWindow_AndTheNoticeSaysWhatAZeroMeans()
    {
        var r = Run("counts");
        var points = r.GetProperty("charts")[0].GetProperty("points");

        // A 6 hour window is 7 hourly buckets whether or not an event fell in them.
        Assert.InRange(points.GetArrayLength(), 6, 8);
        var zero = points.EnumerateArray().Count(p => p.EnumerateArray().Skip(1).All(v => v.GetInt32() == 0));
        Assert.True(zero >= 4, "empty hours must be present as 0");
        var notice = Assert.Single(r.GetProperty("notices").EnumerateArray()).GetString()!;
        Assert.Contains("an hour with none shows 0", notice, StringComparison.Ordinal);
        Assert.Contains("before collection began", notice, StringComparison.Ordinal);
    }

    [Fact]
    public void ASampleThatNeverReachedMedium_IsNotDrawn_ButTheTableStillListsIt()
    {
        var r = Run("quiet");
        Assert.Empty(r.GetProperty("charts").EnumerateArray());
        Assert.Equal(2, r.GetProperty("tableRows").GetInt32());
        Assert.Contains(r.GetProperty("empties").EnumerateArray(), e => e.GetString()!.Contains("reached medium pressure", StringComparison.Ordinal));
    }

    [Fact]
    public void AnEmptyAnswer_DrawsNoChart_AndOneRead_FeedsBothPanels()
    {
        var r = Run("empty");
        Assert.Empty(r.GetProperty("charts").EnumerateArray());
        Assert.Single(r.GetProperty("fetches").EnumerateArray());
    }
}
