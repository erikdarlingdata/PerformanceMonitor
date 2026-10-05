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
/// What the web CPU and Memory tabs' get_server_trend panels draw, from the shipped <c>server-tabs.js</c> run under Node
/// (<c>web-server-trends-harness.mjs</c>) against a scripted <c>/api/read</c>: the series and unit of each chart, the
/// clerk selector's defaults, its state across a rebuild, the clerk types the read could not find, and an empty answer.
/// Node is skipped when it is not installed.
/// </summary>
public sealed class WebServerTrendsBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-server-trends-harness.mjs"));
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
                Assert.Fail("the server-trends harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the server-trends harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            var result = doc.RootElement.Clone();
            Assert.Empty(result.GetProperty("rejections").EnumerateArray());
            return result;
        }
    }

    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.GetString()!).ToArray();

    private static readonly string[] FirstFive =
    {
        "MEMORYCLERK_SQLBUFFERPOOL", "CACHESTORE_SQLCP", "CACHESTORE_OBJCP", "OBJECTSTORE_LOCK_MANAGER", "MEMORYCLERK_SQLQERESERVATIONS",
    };

    [Fact]
    public void TheCpuSchedulerTrend_DrawsRunnableBlockedAndQueued_FromOneAbortableRead_WithTheDiscontinuityNotice()
    {
        var r = Run("cpu");
        Assert.Equal(new[] { "Runnable Tasks", "Blocked Tasks", "Queued Requests" }, Strings(r.GetProperty("chart").GetProperty("labels")));
        Assert.Equal("tasks", r.GetProperty("chart").GetProperty("unit").GetString());
        var read = Assert.Single(r.GetProperty("reads").EnumerateArray());
        Assert.Equal("cpu_scheduler", read.GetProperty("metric").GetString());
        Assert.Equal("24", read.GetProperty("hours").GetString());
        Assert.True(r.GetProperty("signals")[0].GetBoolean());
        Assert.Contains("baseline discontinuity at", Strings(r.GetProperty("notices")).Single());
    }

    [Fact]
    public void ThePlanCacheTrend_DrawsSingleUseAndMultiUseInMb()
    {
        var r = Run("plan");
        Assert.Equal(new[] { "Single-Use", "Multi-Use" }, Strings(r.GetProperty("chart").GetProperty("labels")));
        Assert.Equal("MB", r.GetProperty("chart").GetProperty("unit").GetString());
        Assert.Equal("plan_cache", Assert.Single(r.GetProperty("reads").EnumerateArray()).GetProperty("metric").GetString());
        Assert.Empty(r.GetProperty("notices").EnumerateArray());
    }

    [Fact]
    public void TheClerkTrend_ChecksTheHeaviestFiveFirst_AndNamesThemInOneRead()
    {
        var r = Run("clerks");
        Assert.Equal(FirstFive, Strings(r.GetProperty("checked")));
        Assert.Equal(7, r.GetProperty("listed").GetInt32());
        var read = Assert.Single(r.GetProperty("reads").EnumerateArray());
        Assert.Equal("memory_clerks", read.GetProperty("metric").GetString());
        Assert.Equal(string.Join(",", FirstFive), read.GetProperty("clerk_types").GetString());
        Assert.Equal(FirstFive, Strings(r.GetProperty("chart").GetProperty("labels")));
        Assert.Equal("MB", r.GetProperty("chart").GetProperty("unit").GetString());
    }

    [Fact]
    public void TheClerkChoices_SurviveARebuildForTheSameServer_AndStartFreshForAnother()
    {
        var r = Run("survives");
        var rebuilt = Strings(r.GetProperty("rebuilt"));
        Assert.DoesNotContain("CACHESTORE_SQLCP", rebuilt);
        Assert.Contains("MEMORYCLERK_XE", rebuilt);
        Assert.Equal(FirstFive, Strings(r.GetProperty("other")));
        Assert.Equal(string.Join(",", rebuilt), r.GetProperty("lastRead").GetProperty("clerk_types").GetString());
    }

    [Fact]
    public void ClerkTypesTheReadCouldNotFind_AreShown_AndTheRestStillChart()
    {
        var r = Run("missing");
        Assert.Contains("No samples in this window for: CACHESTORE_OBJCP.", Strings(r.GetProperty("notices")));
        Assert.DoesNotContain("CACHESTORE_OBJCP", Strings(r.GetProperty("chart").GetProperty("labels")));
        Assert.Equal(4, r.GetProperty("chart").GetProperty("labels").GetArrayLength());
    }

    [Fact]
    public void AnEmptyAnswer_ShowsTheReadsMessage_NotAChart()
    {
        foreach (var scenario in new[] { "emptyCpu", "emptyClerks" })
        {
            var r = Run(scenario);
            Assert.Equal(JsonValueKind.Null, r.GetProperty("chart").ValueKind);
            Assert.Contains("recorded in the last 24 hour(s)", Strings(r.GetProperty("empties")).Single());
        }
    }

    [Fact]
    public void WithNoClerkSnapshot_NoTrendReadIsSent()
    {
        var r = Run("noClerks");
        Assert.Empty(r.GetProperty("reads").EnumerateArray());
        Assert.Single(Strings(r.GetProperty("empties")));
    }
}
