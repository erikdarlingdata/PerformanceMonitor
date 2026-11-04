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
/// #7402: the web pages' wording and ordering, run as the shipped scripts under Node (<c>web-layout-wording-harness.mjs</c>), skipped
/// when Node is not installed. The Query Tag Usage sections follow the display name, the Optimization cost line starts with a capital,
/// a FinOps tab's plain count is a quiet caption while a caveat stays yellow, and a line chart whose points hold no number says what
/// happened instead of "No numeric values to chart.".
/// </summary>
public sealed class WebLayoutWordingBehaviourTests
{
    private static JsonElement? _run;

    private static JsonElement Run()
    {
        if (_run is { } cached)
        {
            return cached;
        }

        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-layout-wording-harness.mjs"));
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
                Assert.Fail("the layout wording harness did not finish in 60 s");
            }

            Assert.True(proc.ExitCode == 0, "the layout wording harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            _run = doc.RootElement.Clone();
            return _run.Value;
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    private static (string Cls, string Text)[] Strips(JsonElement e) =>
        e.EnumerateArray().Select(x => (x.GetProperty("cls").GetString()!, x.GetProperty("text").GetString()!)).ToArray();

    [Fact]
    public void QueryTagUsage_SectionsFollowTheDisplayName_NotTheConnectionKey()
    {
        // The read lists the servers k3, k1, k2; the reader sees Alpha, Charlie, Bravo.
        Assert.Equal(new[] { "Alpha", "Bravo", "Charlie" }, Strings(Run().GetProperty("serverHeadings")));
    }

    [Fact]
    public void TheOptimizationCostLine_StartsWithACapital_ForEveryCostReason()
    {
        var lines = Strings(Run().GetProperty("costLines"));
        // No reason, the tool's own words, and a reason the tool might one day write in lower case.
        Assert.Equal("Monthly cost not set, so Est. cost share is blank.", lines[0]);
        Assert.Equal("Monthly cost not set, so Est. cost share is blank.", lines[1]);
        Assert.Equal("Monthly cost not set, so Est. cost share is blank.", lines[2]);
        Assert.Equal("No monthly cost is set for this server, so Est. cost share is blank.", lines[3]);
        Assert.All(lines, l => Assert.True(char.IsUpper(l[0]), l));
    }

    [Fact]
    public void AFinOpsCount_IsAQuietCaption_AndACaveatStaysYellow()
    {
        var run = Run();
        var rec = Strips(run.GetProperty("recommendationStrips"));
        Assert.Equal("strip source", rec.Single(s => s.Text == "1 recommendation, High severity first.").Cls);
        Assert.Equal("strip notice", rec.Single(s => s.Text.StartsWith("Monthly cost not set", System.StringComparison.Ordinal)).Cls);
        Assert.Equal("strip notice", rec.Single(s => s.Text.StartsWith("Some checks could not run", System.StringComparison.Ordinal)).Cls);

        // The same count is yellow once the list was cut, because then it carries a caveat.
        Assert.Equal("strip source", Strips(run.GetProperty("resourcesWhole")).Single().Cls);
        Assert.Equal("strip notice", Strips(run.GetProperty("resourcesCut")).Single().Cls);
    }

    [Fact]
    public void ALineWhosePointsHoldNoNumber_SaysWhatHappened_InFullWords()
    {
        var run = Run();
        // The one point after a restart has no rate: a full sentence, not the tool's unrated_note and not the chart's old words.
        var noRate = run.GetProperty("noRate").GetString()!;
        Assert.Equal("No rate can be worked out yet for this period, so there is nothing to draw.", noRate);
        Assert.DoesNotContain("knowable", noRate, System.StringComparison.Ordinal);
        // A message the read returned is used as it is.
        Assert.Equal("The server restarted a moment ago, so no rate is known yet.", run.GetProperty("noRateWithMessage").GetString());
        // Without either, the sentence is still whole, and no chart can show the old words.
        var none = run.GetProperty("noValues").GetString()!;
        Assert.EndsWith("so there is nothing to draw.", none, System.StringComparison.Ordinal);
        Assert.DoesNotContain("No numeric values to chart", none, System.StringComparison.Ordinal);
        // A point with a number still draws the chart.
        Assert.Equal("chart", run.GetProperty("withValues").GetString());
    }
}
