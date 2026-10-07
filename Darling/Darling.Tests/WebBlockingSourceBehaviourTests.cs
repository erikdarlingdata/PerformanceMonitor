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
/// #5244: the web viewer's blocking panels say which collector answered (<c>web-blocking-source-harness.mjs</c>, the shipped
/// <c>panels.js</c>, <c>util.js</c> and <c>pages/server-tabs.js</c> run under Node with a stub fetch). get_blocking_trend and
/// get_blocking_stats already carry a top-level <c>source</c> ("blocked-process-report" or "DMV snapshot", null with no rows); the
/// panels that draw them show it as a line above the chart: Blocking Events on the Overview and Blocking tabs, and Blocking Severity.
/// No other panel gets one, and no line is drawn when the answer has no rows or carries no recognized source. Node is skipped when
/// it is not installed, as <see cref="WebDatabaseFilterChipsBehaviourTests"/> does.
/// </summary>
public sealed class WebBlockingSourceBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-blocking-source-harness.mjs"));
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
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the blocking source harness did not finish in 60 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the blocking source harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            var result = doc.RootElement.Clone();
            Assert.Empty(result.GetProperty("rejections").EnumerateArray());
            return result;
        }
    }

    /// <summary>The source lines every panel with that title drew, one entry per panel.</summary>
    private static string[][] Lines(JsonElement r, string title) =>
        r.GetProperty("panels").EnumerateArray()
            .Where(p => p.GetProperty("title").GetString() == title)
            .Select(p => p.GetProperty("strips").EnumerateArray().Select(s => s.GetString()!).ToArray())
            .ToArray();

    [Theory]
    [InlineData("xe", "Source: blocked-process-report")]
    [InlineData("dmv", "Source: DMV snapshot")]
    public void BlockingEventsAndSeverity_NameTheCollectorThatAnswered(string scenario, string expected)
    {
        var r = Run(scenario);

        /* Blocking Events is on the Overview tab and the Blocking tab; Blocking Severity only on the Blocking tab. */
        var events = Lines(r, "Blocking Events");
        Assert.Equal(2, events.Length);
        Assert.All(events, lines => Assert.Equal(new[] { expected }, lines));
        var severity = Lines(r, "Blocking Severity");
        Assert.Single(severity);
        Assert.Equal(new[] { expected }, severity[0]);
    }

    [Theory]
    [InlineData("zeroRows")]
    [InlineData("envelope")]
    [InlineData("noSource")]
    [InlineData("other")]
    public void NoRows_OrNoRecognizedSource_DrawsNoSourceLine(string scenario)
    {
        var r = Run(scenario);

        Assert.All(r.GetProperty("panels").EnumerateArray(), p => Assert.Empty(p.GetProperty("strips").EnumerateArray()));
    }

    [Fact]
    public void OnlyTheBlockingPanelsGetOne()
    {
        var r = Run("dmv");

        var titles = r.GetProperty("panels").EnumerateArray()
            .Where(p => p.GetProperty("strips").GetArrayLength() > 0)
            .Select(p => p.GetProperty("title").GetString())
            .ToArray();
        Assert.Equal(new[] { "Blocking Events", "Blocking Events", "Blocking Severity" }, titles);
    }
}
