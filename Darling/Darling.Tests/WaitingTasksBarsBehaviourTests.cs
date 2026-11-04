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
/// #7571: the Blocking tab's Waiting Tasks and Blocked Sessions charts read one row per (collection time, wait type) and per
/// (collection time, database). They are drawn as one bar per collection time with a segment per group, not as one line through every
/// row, and the Wait Stats tab's Waiting Tasks grid carries <c>thread_count</c>. The shipped page runs under Node
/// (<c>waiting-tasks-bars-harness.mjs</c>) against a scripted <c>/api/read</c>; Node is skipped when it is not installed.
/// </summary>
public sealed class WaitingTasksBarsBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "waiting-tasks-bars-harness.mjs"));
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
                Assert.Fail("the waiting tasks bars harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the waiting tasks bars harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static int[] Ints(JsonElement chart, string key) => chart.GetProperty(key).EnumerateArray().Select(e => e.GetInt32()).ToArray();

    private static string[] Text(JsonElement chart) => chart.GetProperty("text").EnumerateArray().Select(e => e.GetString() ?? "").ToArray();

    [Fact]
    public void WaitingTasks_TwoWaitTypesAtOneTime_AreOneBarWithTwoSegments_NotALine()
    {
        var waiting = Run("default").GetProperty("waiting");

        // Three collection times, so three bars, and the time that had two wait types stacks two segments.
        Assert.Equal(0, waiting.GetProperty("lines").GetInt32());
        Assert.Equal(3, waiting.GetProperty("barColumns").GetInt32());
        Assert.Equal(new[] { 2, 1, 1 }, Ints(waiting, "segmentsPerColumn"));
        // The wait type names the segment in the legend.
        Assert.Contains("CXCONSUMER", Text(waiting));
        Assert.Contains("LATCH_EX", Text(waiting));
        Assert.Contains("PREEMPTIVE_OS_CREATEFILE", Text(waiting));
    }

    [Fact]
    public void BlockedSessions_TwoDatabasesAtOneTime_AreOneBarWithTwoSegments_NotALine()
    {
        var blocked = Run("default").GetProperty("blocked");

        Assert.Equal(0, blocked.GetProperty("lines").GetInt32());
        Assert.Equal(2, blocked.GetProperty("barColumns").GetInt32());
        Assert.Equal(new[] { 2, 1 }, Ints(blocked, "segmentsPerColumn"));
        Assert.Contains("Sales", Text(blocked));
        Assert.Contains("Orders", Text(blocked));
    }

    [Fact]
    public void ABarIsWideEnoughToSee_AndNeverASlab()
    {
        var run = Run("default");

        foreach (var chart in new[] { run.GetProperty("waiting"), run.GetProperty("blocked") })
        {
            var widths = Ints(chart, "widths");
            Assert.NotEmpty(widths);
            Assert.All(widths, w => Assert.InRange(w, 8, 64));
        }
    }

    [Fact]
    public void CollectionsAMinuteApartInADayWindow_StillDrawBarsYouCanSee()
    {
        var waiting = Run("close").GetProperty("waiting");

        Assert.Equal(3, waiting.GetProperty("barColumns").GetInt32());
        // The tightest gap (one minute in 24 hours) is well under a pixel; the floor keeps each bar at 8 px.
        Assert.All(Ints(waiting, "widths"), w => Assert.True(w >= 8, "a bar one minute from its neighbour was " + w + " px wide, under the 8 px floor"));
    }

    [Fact]
    public void ALoneCollectionInADayWindow_IsOneBarNoWiderThan64Px_NotASlabAcrossThePlot()
    {
        var waiting = Run("lone").GetProperty("waiting");

        Assert.Equal(1, waiting.GetProperty("barColumns").GetInt32());
        Assert.All(Ints(waiting, "widths"), w => Assert.True(w <= 64, "a lone bar was " + w + " px wide, over the 64 px cap"));
    }

    [Fact]
    public void MoreThanEightWaitTypes_FoldTheSmallestIntoOther_SoTheBarKeepsItsWholeHeight()
    {
        var waiting = Run("many").GetProperty("waiting");

        Assert.Equal(1, waiting.GetProperty("barColumns").GetInt32());
        Assert.Equal(new[] { 8 }, Ints(waiting, "segmentsPerColumn"));
        Assert.Contains("(other)", Text(waiting));
    }

    [Fact]
    public void WaitingTasksGrid_ShowsThreadCount_BesideTheDuration()
    {
        var run = Run("grid");
        var headers = run.GetProperty("headers").EnumerateArray().Select(e => e.GetString()).ToList();

        var duration = headers.IndexOf("Duration");
        Assert.True(duration >= 0, "the grid lost its Duration column");
        Assert.Equal("Threads", headers[duration + 1]);
        Assert.Contains("2", run.GetProperty("cells").EnumerateArray().Select(e => e.GetString()));
    }
}
