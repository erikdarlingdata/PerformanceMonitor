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
/// #7729: small details on the web pages, run through the shipped scripts under Node (skipped when Node is not installed):
/// a whole number prints without a decimal, the Query Store Regressions grid lists the worst severity first, the Query Store Overhead
/// grid leaves out a wait with no delta, the Default Trace note counts every event, a server listed only for an advisory tile does not read Healthy,
/// the stat tiles take a balanced column count, and a grid that overflows pins its name column.
/// </summary>
public sealed class WebDetails7729BehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-details-7729-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped panel script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the #7729 harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the #7729 harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString() ?? "").ToArray();

    [Fact]
    public void AWholeNumber_PrintsWithoutADecimal_AndAFractionKeepsItsPlace()
    {
        var r = Run();
        Assert.Equal(new[] { "1", "309", "2,485", "1.5", "309", "0", "—", "12.3" }, Strings(r.GetProperty("num1")));
    }

    [Fact]
    public void TheRegressionsGrid_ListsSeverityFirst_ThenTheDurationItAdded()
    {
        var r = Run();
        /* CRITICAL (6), HIGH by added duration (4 then 2), MEDIUM (7), LOW by added duration (3 then 1), and a row with no severity last (5). */
        Assert.Equal(new[] { 6, 4, 2, 7, 3, 1, 5 }, r.GetProperty("regressionOrder").EnumerateArray().Select(x => x.GetInt32()).ToArray());
    }

    [Fact]
    public void NoDeadlockGrid_PrintsTheGraphsProcessId()
    {
        var g = Run().GetProperty("deadlockGrids");
        var page = Strings(g.GetProperty("page"));
        var template = Strings(g.GetProperty("template"));
        Assert.Equal(new[] { "deadlock_time", "victim_sql_text", "process_summary", "has_deadlock_xml" }, page);
        Assert.Equal(new[] { "deadlock_time", "victim_sql_text", "process_summary" }, template);
    }

    [Fact]
    public void TheOverheadWaits_LeaveOutAWaitWithNoDelta_SoTheEmptySentencePrints()
    {
        var r = Run();
        Assert.Equal(new[] { "QDS_PERSIST_TASK_MAIN_LOOP_SLEEP" }, Strings(r.GetProperty("waitsKept")));
        Assert.Equal(new[] { "QDS_PERSIST_TASK_MAIN_LOOP_SLEEP" }, Strings(r.GetProperty("waitGridRows")));
        Assert.Contains("No non-sleep QDS_* wait deltas in this window.", r.GetProperty("waitGridEmpty").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheDefaultTraceNote_CountsEveryEvent_AndNamesNothingWhenThereIsNone()
    {
        var r = Run();
        Assert.Equal("Events in this window: Server Memory Change 143, Object:Altered 3.", r.GetProperty("eventNote").GetString());
        Assert.Equal(JsonValueKind.Null, r.GetProperty("eventNoteNone").ValueKind);
        Assert.EndsWith("and 3 more kinds.", r.GetProperty("eventNoteMany").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void ANeedsAttentionRow_ListedOnlyForAnAdvisoryTile_ReadsAttentionNotHealthy()
    {
        var r = Run();
        Assert.Equal(new[] { "Attention:badge band-Warning", "Warning:badge band-Warning", "Healthy:badge band-Healthy" }, Strings(r.GetProperty("bandPills")));
        Assert.Equal(new[] { "dot band-Warning", "dot band-Warning", "dot band-Healthy" }, Strings(r.GetProperty("bandDots")));
        /* A list that does not name advisoryKey keeps the server's band word. */
        Assert.Equal(new[] { "Healthy", "Warning", "Healthy" }, Strings(r.GetProperty("bandPillsWithoutKey")));
    }

    [Fact]
    public void TheStatTiles_TakeTheFewestColumnsThatKeepTheRows()
    {
        var t = Run().GetProperty("tiles");
        /* 15 tiles with 7 across (1536 px) are 3 rows, so 5 across, not 7, 7 and 1. */
        Assert.Equal("repeat(5, minmax(0, 1fr))", t.GetProperty("fifteenAt1536").GetString());
        Assert.Equal("repeat(5, minmax(0, 1fr))", t.GetProperty("fifteenAt1280").GetString());
        Assert.Equal("repeat(8, minmax(0, 1fr))", t.GetProperty("fifteenAt2560").GetString());
        Assert.Equal("repeat(5, minmax(0, 1fr))", t.GetProperty("tenAt1189").GetString());
        Assert.Equal("repeat(5, minmax(0, 1fr))", t.GetProperty("nineAt1189").GetString());
        Assert.Equal("repeat(2, minmax(0, 1fr))", t.GetProperty("fourAt500").GetString());
        /* Two tiles are left to the CSS fill. */
        Assert.Equal(JsonValueKind.Null, t.GetProperty("twoTiles").ValueKind);
    }

    [Fact]
    public void AGridThatOverflows_PinsItsNameColumn_AndOneThatFitsDoesNot()
    {
        var r = Run();
        Assert.Contains("pin-first", r.GetProperty("pinOverflow").GetProperty("wrapClass").GetString(), StringComparison.Ordinal);
        Assert.Equal(new[] { "th:Database", "td:db1" }, Strings(r.GetProperty("pinOverflow").GetProperty("pinned")));
        Assert.DoesNotContain("pin-first", r.GetProperty("pinFits").GetProperty("wrapClass").GetString(), StringComparison.Ordinal);
    }
    /// <summary>The Viewer's Needs Attention list reads like the web page's: a Healthy server listed only for an advisory tile says Attention in the Warning colour.</summary>
    [Fact]
    public void TheViewersNeedsAttentionRow_ForAnAdvisoryTileOnAHealthyServer_ReadsAttention()
    {
        var advisory = new PerformanceMonitor.Darling.Viewer.FleetRankedServer { Band = PerformanceMonitor.Common.FleetHealthBand.Healthy, HasAdvisoryTile = true };
        var warning = new PerformanceMonitor.Darling.Viewer.FleetRankedServer { Band = PerformanceMonitor.Common.FleetHealthBand.Warning, HasAdvisoryTile = true };
        var healthy = new PerformanceMonitor.Darling.Viewer.FleetRankedServer { Band = PerformanceMonitor.Common.FleetHealthBand.Healthy };
        Assert.Equal("Attention", advisory.BandLabel);
        Assert.Equal(warning.BandBrush.Color, advisory.BandBrush.Color);
        Assert.Equal("Warning", warning.BandLabel);
        Assert.Equal("Healthy", healthy.BandLabel);
        Assert.NotEqual(healthy.BandBrush.Color, advisory.BandBrush.Color);
    }
}
