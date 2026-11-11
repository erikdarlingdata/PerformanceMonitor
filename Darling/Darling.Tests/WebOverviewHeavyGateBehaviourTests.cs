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
/// #8554: a server's Overview under its first visit, run in Node against a recording fetch whose answers land only when the
/// harness says so. The tab asks for the daily summary range, the baseline read and the three 24 hour trends together; on a server
/// nobody had opened they took 4 to 10 s each and ended within 300 ms of one another, and held four of the browser's six
/// connections to the origin while the page's own reads waited behind them. At most two of those five are in flight at once, the
/// daily summary range is asked for first (the slowest read, #8554 part 2), then the strips' trends, and the baseline last, the small reads (CPU, blocking, deadlocks) are
/// sent at once, and every read is still sent. No clock is read on either side. Behavior only; the pages' source is not pinned.
/// </summary>
public sealed class WebOverviewHeavyGateBehaviourTests
{
    private static readonly string[] Trends = ["get_server_trend", "get_memory_trend", "get_file_io_trend"];
    private static readonly string[] Small = ["get_cpu_utilization", "get_blocking_trend", "get_deadlock_trend"];
    private static readonly string[] Heavy = [.. Trends, "get_overview_baselines", "get_daily_summary_range"];

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-overview-heavy-gate-harness.mjs"));
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
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the Overview heavy read harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the Overview heavy read harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    [Fact]
    public void TheFirstLoad_SendsTheSmallReadsAtOnce_AndAtMostTwoOfTheFiveHeavyReads()
    {
        var load = Run().GetProperty("load");
        var sent = Strings(load.GetProperty("sent"));
        Assert.All(Small, tool => Assert.Contains(tool, sent));
        Assert.Equal(2, Strings(load.GetProperty("heavyInFlight")).Length);
    }

    [Fact]
    public void NoMoreThanTwoHeavyReadsAreEverInFlightTogether_WhileTheAnswersLandOneByOne()
    {
        Assert.Equal(2, Run().GetProperty("maxHeavyInFlight").GetInt32());
    }

    [Fact]
    public void TheDailySummaryIsAskedForFirst_ThenTheStripsTrends_AndTheBaselineLast()
    {
        /* #8554: the daily summary range is the slowest read on a first visit, so the gate that lets two heavy reads go at a time starts it first. */
        var order = Strings(Run().GetProperty("heavySentOrder"));
        Assert.Equal(["get_daily_summary_range", .. Trends, "get_overview_baselines"], order);
    }

    [Fact]
    public void TheDailySummaryIsOneOfTheTwoHeavyReadsInFlightOnTheFirstLoad()
    {
        var heavy = Strings(Run().GetProperty("load").GetProperty("heavyInFlight"));
        Assert.Contains("get_daily_summary_range", heavy);
    }

    [Fact]
    public void EveryReadIsStillSent_SoNoPanelLosesItsData()
    {
        /* The tab's whole build runs, so its other panels' reads are in the list too; each heavy and small read is sent exactly once. */
        var all = Strings(Run().GetProperty("sentAll"));
        Assert.All(Heavy.Concat(Small), tool => Assert.Single(all, tool));
    }
}
