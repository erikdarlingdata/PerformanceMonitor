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
/// What the web File I/O latency panel draws, from the shipped <c>server-tabs.js</c> run under Node
/// (<c>web-server-trends-harness.mjs</c>, file I/O scenarios) against a scripted <c>/api/read</c>: a read-latency chart
/// and a write-latency chart from one read, the discontinuity notice, a window with no writes, and an empty answer.
/// </summary>
public sealed class WebFileIoLatencyBehaviourTests
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
                Assert.Fail("the file I/O harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the file I/O harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            var result = doc.RootElement.Clone();
            Assert.Empty(result.GetProperty("rejections").EnumerateArray());
            return result;
        }
    }

    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void OneRead_DrawsAReadLatencyChartAndAWriteLatencyChart_WithTheSameLines()
    {
        var r = Run("fileio");
        Assert.Single(r.GetProperty("reads").EnumerateArray());
        var charts = r.GetProperty("charts").EnumerateArray().ToArray();
        Assert.Equal(new[] { "file-io-latency", "file-io-write-latency" }, charts.Select(c => c.GetProperty("id").GetString()).ToArray());
        foreach (var c in charts)
        {
            Assert.Equal("ms", c.GetProperty("unit").GetString());
            Assert.Equal(new[] { "db_a ROWS", "db_b ROWS" }, Strings(c.GetProperty("labels")).OrderBy(x => x).ToArray());
        }

        Assert.Equal(charts[0].GetProperty("scope").GetString(), charts[1].GetProperty("scope").GetString());
    }

    [Fact]
    public void ABaselineDiscontinuity_IsShownAboveTheCharts()
    {
        var r = Run("fileioGap");
        Assert.Contains("baseline discontinuity at", Strings(r.GetProperty("notices")).Single(n => n.Contains("baseline")));
        Assert.Equal(2, r.GetProperty("charts").GetArrayLength());
    }

    [Fact]
    public void AWindowWithNoWrites_StillDrawsReads_AndSaysSoForWrites()
    {
        var r = Run("fileioNoWrites");
        var charts = r.GetProperty("charts").EnumerateArray().ToArray();
        Assert.Equal("file-io-latency", Assert.Single(charts).GetProperty("id").GetString());
        Assert.Contains("No writes in this window.", Strings(r.GetProperty("empties")));
    }

    [Fact]
    public void AnEmptyAnswer_DrawsNoChart_AndShowsTheReadsOwnMessage()
    {
        var r = Run("fileioEmpty");
        Assert.Equal(0, r.GetProperty("charts").GetArrayLength());
        Assert.Contains("No file I/O recorded.", Strings(r.GetProperty("empties")));
    }
}
