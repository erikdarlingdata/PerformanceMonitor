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
/// #7699: the Latch Stats Severity and Avg ms/req columns, the Memory Grant Efficiency figures, the Memory Broker whole numbers and the Query Store
/// Duration Trend caption. The shipped page scripts run under Node (<c>web-details-7699-harness.mjs</c>); Node is skipped when it is not installed.
/// </summary>
public sealed class WebDetails7699HarnessTests
{
    private static readonly Lazy<JsonElement> Result = new(Run);

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-details-7699-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page scripts cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the web details harness did not finish in 60 s");
            }

            Assert.True(proc.ExitCode == 0, "the web details harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void LatchSeverity_PrintsAWordWithAColor_AndTheHeaderNamesTheWindowItIsJudgedOn()
    {
        var run = Result.Value;
        Assert.Equal("Severity, latest interval", run.GetProperty("latchSeverityHeader").GetString());
        var cells = run.GetProperty("latchSeverity").EnumerateArray().Select(c => (c[0].GetString()!, c[1].GetString()!)).ToArray();
        Assert.Equal(new[] { ("Low", "Healthy"), ("Medium", "Warning"), ("High", "Critical"), ("—", "Unknown") }, cells);
    }

    [Fact]
    public void LatchAverage_NamesItsUnit_BesideAWaitColumnInMinutes()
    {
        Assert.Equal("Avg ms/req", Result.Value.GetProperty("latchAvgHeader").GetString());
    }

    [Fact]
    public void MemoryGrantFigures_KeepSmallValues_SoThePercentageHasSomethingUnderIt()
    {
        var grant = Result.Value.GetProperty("grant");
        Assert.Equal("0.03 MB", grant.GetProperty("avg_granted_mb").GetString());
        Assert.Equal("0.0026 MB", grant.GetProperty("avg_used_mb").GetString());
        Assert.Equal("0.03 MB", grant.GetProperty("wasted_mb").GetString());
        Assert.Equal("1.5 GB", grant.GetProperty("peak_granted_mb").GetString());
        Assert.Equal("9.1", grant.GetProperty("efficiency_pct").GetString());
        // The figures carry their own unit, so the headers no longer say (MB).
        Assert.DoesNotContain(Result.Value.GetProperty("grantHeaders").EnumerateArray(), h => h.GetString()!.Contains("(MB)") && h.GetString() != "Efficiency (%)");
    }

    [Fact]
    public void MemoryBroker_PrintsWholeNumbersWithoutDecimals()
    {
        var broker = Result.Value.GetProperty("broker");
        Assert.Equal("99", broker.GetProperty("memory_ratio").GetString());
        Assert.Equal("-13,012", broker.GetProperty("rate").GetString());
    }

    [Fact]
    public void QueryStoreDurationTrend_SaysWhereTheLineChangesGrain_AndNothingWhenThereIsNoSuchTime()
    {
        var run = Result.Value;
        var time = run.GetProperty("captionToday7").GetString();
        Assert.Equal($"Hourly averages until {time}, then one point per collection.", run.GetProperty("captionToday").GetString());
        // A seam on another day names the day.
        var earlier = run.GetProperty("captionEarlier").GetString()!;
        Assert.StartsWith("Hourly averages until ", earlier, StringComparison.Ordinal);
        Assert.EndsWith($", {time}, then one point per collection.", earlier, StringComparison.Ordinal);
        Assert.Equal(JsonValueKind.Null, run.GetProperty("captionNone").ValueKind);
    }
}
