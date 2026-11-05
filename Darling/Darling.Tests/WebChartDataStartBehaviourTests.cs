/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #7386: a web viewer line chart over a range longer than the server's history starts its time axis where the server's data
/// starts, so a server with three hours of history under "Past day" does not spend seven eighths of the plot on empty time.
/// The picked start stays the floor, a chart never hides a point it plotted, and a zoomed window is exact. These run the
/// shipped <c>charts.js</c> and <c>util.js</c> under Node (<c>web-chart-data-start-harness.mjs</c>); a point's place is its
/// fraction of the plot width, 0 at the left edge and 1 at the right. Node is skipped when it is not installed.
/// </summary>
public sealed class WebChartDataStartBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-chart-data-start-harness.mjs"));
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
                Assert.Fail("the chart data start harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the chart data start harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    private static double First(JsonElement r, string name) => r.GetProperty(name).GetProperty("first").GetDouble();

    /// <summary>The report's case: three hours of history under a 24 hour window. Without the data start the line sits in the right
    /// eighth (first point at 0.875 of the width); with it the line fills the plot, at 1,000 px and at 500 px.</summary>
    [Fact]
    public void AChartWhoseDataStartsLate_HasNoDeadSpaceOnTheLeft()
    {
        var r = Run();
        Assert.InRange(First(r, "noClaim"), 0.87, 0.88);
        Assert.InRange(First(r, "late"), 0.0, 0.03);
        Assert.InRange(First(r, "lateNarrow"), 0.0, 0.03);
        Assert.Equal(1.0, r.GetProperty("late").GetProperty("last").GetDouble(), 3);
        // Every point is still drawn: the axis changed, the data did not.
        Assert.Equal(r.GetProperty("noClaim").GetProperty("count").GetInt32(), r.GetProperty("late").GetProperty("count").GetInt32());
    }

    /// <summary>A server with data for the whole range draws as it does without a claim, and a page that dropped the claim
    /// (any other page) keeps the picked window.</summary>
    [Fact]
    public void AServerWithDataForTheWholeRange_DrawsAsBefore_AndNoClaimKeepsThePickedWindow()
    {
        var r = Run();
        Assert.True(r.GetProperty("fullSame").GetBoolean());
        Assert.Equal(0.0, r.GetProperty("fullFirst").GetDouble(), 6);
        Assert.InRange(First(r, "dropped"), 0.87, 0.88);
    }

    /// <summary>A point older than the server's data start (another relation with its own retention) is plotted inside the
    /// plot, not cut off the left edge: the axis moves back to it, and the picked start stays the floor.</summary>
    [Fact]
    public void AChartWithAnOlderPlottedPoint_IsNotTrimmed()
    {
        var r = Run();
        Assert.InRange(First(r, "olderPoint"), 0.0, 0.03);
        Assert.Equal(41, r.GetProperty("olderPoint").GetProperty("count").GetInt32());
    }

    /// <summary>A zoomed chart is exact: the window is marked exact, and a point three hours back in a window of four hours
    /// back to one hour back sits a third of the way in, whatever the server's data start is.</summary>
    [Fact]
    public void AZoomedChart_IsNeverTrimmed()
    {
        var r = Run();
        Assert.InRange(First(r, "zoomed"), 0.33, 0.34);
        Assert.Equal(First(r, "zoomed"), First(r, "zoomedNoClaim"), 6);
        Assert.True(r.GetProperty("zoomApplied").GetProperty("exact").GetBoolean());
    }

    /// <summary>The twin of ChartAxisStart.XStart: the later of the picked start and the data start less a run-in, never before the
    /// picked start, and no change for a missing start, one at the end, or one that leaves under 30 minutes.</summary>
    [Fact]
    public void XStartMs_IsTheTwinOfTheDesktopRule()
    {
        var pure = Run().GetProperty("pure");
        // 24 hours, data from 3 hours back: 21 hours in, less a fiftieth of the 3 hours shown (3.6 minutes).
        Assert.InRange(pure.GetProperty("late").GetDouble(), 1256.3, 1256.5);
        Assert.True(pure.GetProperty("older").GetBoolean());
        Assert.True(pure.GetProperty("none").GetBoolean());
        Assert.True(pure.GetProperty("atEnd").GetBoolean());
        Assert.True(pure.GetProperty("aMinuteAgo").GetBoolean());
    }
}
