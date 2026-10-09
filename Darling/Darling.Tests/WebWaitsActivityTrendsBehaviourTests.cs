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
/// What the web Wait Stats tab's latch and spinlock trends and the Activity tab's session stats trend draw, from the shipped
/// <c>server-tabs.js</c> run under Node against a scripted <c>/api/read</c>: series, unit, picker defaults, state across a
/// rebuild, <c>missing_names</c> at the top level and under hints, discontinuity notices and empty answers. Node is skipped when it is not installed.
/// </summary>
public sealed class WebWaitsActivityTrendsBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-waits-activity-trends-harness.mjs"));
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
                Assert.Fail("the waits-activity-trends harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the waits-activity-trends harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            var result = doc.RootElement.Clone();
            Assert.Empty(result.GetProperty("rejections").EnumerateArray());
            return result;
        }
    }

    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.GetString()!).ToArray();

    private static readonly string[] FirstFive = { "LATCH_A", "LATCH_B", "LATCH_C", "LATCH_D", "LATCH_E" };

    [Fact]
    public void TheLatchTrend_ChecksTheHeaviestFive_AndNamesThemInOneRead_WithTheDiscontinuityNotice()
    {
        var r = Run("latch");
        Assert.Equal(FirstFive, Strings(r.GetProperty("checked")));
        Assert.Equal(7, r.GetProperty("listed").GetInt32());
        var read = Assert.Single(r.GetProperty("reads").EnumerateArray());
        Assert.Equal("latch", read.GetProperty("metric").GetString());
        Assert.Equal(string.Join(",", FirstFive), read.GetProperty("names").GetString());
        Assert.Equal(FirstFive, Strings(r.GetProperty("chart").GetProperty("labels")));
        Assert.Equal("ms/s", r.GetProperty("chart").GetProperty("unit").GetString());
        Assert.Contains("baseline discontinuity at", Strings(r.GetProperty("notices")).Single());
    }

    [Fact]
    public void TheSpinlockTrend_DrawsCollisionsPerSecond()
    {
        var r = Run("spinlock");
        Assert.Equal(new[] { "SPIN_A", "SPIN_B", "SPIN_C" }, Strings(r.GetProperty("chart").GetProperty("labels")));
        Assert.Equal("collisions/s", r.GetProperty("chart").GetProperty("unit").GetString());
        Assert.Equal("spinlock", Assert.Single(r.GetProperty("reads").EnumerateArray()).GetProperty("metric").GetString());
    }

    [Fact]
    public void TheSessionStatsTrend_DrawsTheCounts_AndNamesTheTopApplicationAndHost()
    {
        var r = Run("session");
        var labels = Strings(r.GetProperty("chart").GetProperty("labels"));
        Assert.Contains("Total", labels);
        Assert.Contains("Running", labels);
        Assert.Equal("sessions", r.GetProperty("chart").GetProperty("unit").GetString());
        Assert.Equal("session_stats", Assert.Single(r.GetProperty("reads").EnumerateArray()).GetProperty("metric").GetString());
        Assert.Contains("baseline discontinuity at", Strings(r.GetProperty("notices")).Single());
        Assert.Contains(Strings(r.GetProperty("notes")), n => n.Contains("application AppOne (4)") && n.Contains("host HostOne (6)"));
    }

    [Fact]
    public void TheChoices_SurviveARebuildForTheSameServer_AndStartFreshForAnotherServerAndKind()
    {
        var r = Run("survives");
        var rebuilt = Strings(r.GetProperty("rebuilt"));
        Assert.DoesNotContain("LATCH_B", rebuilt);
        Assert.Contains("LATCH_G", rebuilt);
        Assert.Equal(FirstFive, Strings(r.GetProperty("other")));
        Assert.Equal(new[] { "SPIN_A", "SPIN_B", "SPIN_C" }, Strings(r.GetProperty("spin")));
        Assert.Equal(string.Join(",", rebuilt), r.GetProperty("lastRead").GetProperty("names").GetString());
    }

    [Fact]
    public void NamesTheReadCouldNotFind_AreShown_AndTheRestStillChart()
    {
        var r = Run("missing");
        Assert.Contains("No samples in this window for: LATCH_C.", Strings(r.GetProperty("notices")));
        Assert.DoesNotContain("LATCH_C", Strings(r.GetProperty("chart").GetProperty("labels")));
        Assert.Equal(4, r.GetProperty("chart").GetProperty("labels").GetArrayLength());
    }

    [Fact]
    public void WhenNoNamedSeriesHasSamples_TheHintsListIsShown_NotAChart()
    {
        var r = Run("allMissing");
        Assert.Contains(Strings(r.GetProperty("notices")), n => n.StartsWith("No samples in this window for: LATCH_A"));
        Assert.Equal(JsonValueKind.Null, r.GetProperty("chart").ValueKind);
    }

    [Fact]
    public void AnEmptyAnswer_ShowsTheReadsMessage_NotAChart()
    {
        foreach (var scenario in new[] { "emptyLatch", "emptySession" })
        {
            var r = Run(scenario);
            Assert.Equal(JsonValueKind.Null, r.GetProperty("chart").ValueKind);
            Assert.Contains("recorded in the last 24 hour(s)", Strings(r.GetProperty("empties")).Single());
        }
    }

    [Fact]
    public void WithNoLatchRows_NoTrendReadIsSent()
    {
        var r = Run("noOptions");
        Assert.Empty(r.GetProperty("reads").EnumerateArray());
        Assert.Single(Strings(r.GetProperty("empties")));
    }
}
