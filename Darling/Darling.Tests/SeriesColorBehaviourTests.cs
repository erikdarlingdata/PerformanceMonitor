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
/// #7535, #7563: one name keeps one color. The File I/O Latency panel draws a Read chart and a Write chart that each keep their own
/// top series, and a file must be one color in both. A Custom View chart colors its series by a rank that the rows' arrival order
/// cannot change. The shipped page script runs under Node (<c>series-colors-harness.mjs</c>); Node is skipped when it is not installed.
/// </summary>
public sealed class SeriesColorBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "series-colors-harness.mjs"));
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
                Assert.Fail("the series colors harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the series colors harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    /// <summary>A legend as (name, color) pairs, in the order the chart lists them.</summary>
    private static (string Name, string Color)[] Pairs(JsonElement legend) =>
        legend.EnumerateArray().Select(p => (p[0].GetString() ?? "", p[1].GetString() ?? "")).ToArray();

    [Fact]
    public void FileIoLatency_ReadAndWriteChartsWithDifferentTopLists_GiveOneFileOneColor()
    {
        var charts = Run("fileio").GetProperty("charts").EnumerateArray().Select(Pairs).ToArray();

        Assert.Equal(2, charts.Length);
        var read = charts[0];
        var write = charts[1];
        // Each chart keeps its own order: Read leads with Sales, Write with tempdb.
        Assert.Equal(new[] { "Sales", "Orders", "tempdb", "Archive", "(other)" }, read.Select(p => p.Name).ToArray());
        Assert.Equal(new[] { "tempdb", "Orders", "Archive", "(other)", "Sales" }, write.Select(p => p.Name).ToArray());

        var readColors = read.ToDictionary(p => p.Name, p => p.Color);
        foreach (var (name, color) in write)
        {
            Assert.True(readColors[name] == color, name + " is " + readColors[name] + " in the Read chart and " + color + " in the Write chart");
        }

        // One color per name, and no two names share one.
        Assert.Equal(readColors.Count, readColors.Values.Distinct().Count());
    }

    [Fact]
    public void CustomView_SameRowsInAnyOrder_GiveTheSameSeriesOrderAndColors()
    {
        var run = Run("compose");
        var base_ = Pairs(run.GetProperty("base").GetProperty("series"));

        // Largest total first (CXPACKET 50, SOS_SCHEDULER_YIELD 40, LCK_M_S 30, WRITELOG 10 and LCK_M_X 5), then (other) last
        // although it is the largest of all.
        Assert.Equal(new[] { "CXPACKET", "SOS_SCHEDULER_YIELD", "LCK_M_S", "WRITELOG", "LCK_M_X", "(other)" }, base_.Select(p => p.Name).ToArray());
        Assert.Equal(base_.Length, base_.Select(p => p.Color).Distinct().Count());
        Assert.Equal(base_, Pairs(run.GetProperty("reversed").GetProperty("series")));
        Assert.Equal(base_, Pairs(run.GetProperty("swapped").GetProperty("series")));
    }

    [Fact]
    public void CustomView_TiesAtTheCap_FallByNameWhateverTheRowOrder()
    {
        var run = Run("compose");
        var forward = run.GetProperty("tiesForward");
        var reversed = run.GetProperty("tiesReversed");

        // Z leads on total, then the ten-point names in name order until the cap of eight real series; the other four are hidden.
        // The residual is the largest of all, takes no slot and comes last.
        Assert.Equal(new[] { "Z", "A", "B", "C", "D", "E", "F", "G", "(other)" }, Pairs(forward.GetProperty("series")).Select(p => p.Name).ToArray());
        Assert.Equal(4, forward.GetProperty("hidden").GetInt32());
        Assert.Equal(Pairs(forward.GetProperty("series")), Pairs(reversed.GetProperty("series")));
        Assert.Equal(4, reversed.GetProperty("hidden").GetInt32());
    }
}
