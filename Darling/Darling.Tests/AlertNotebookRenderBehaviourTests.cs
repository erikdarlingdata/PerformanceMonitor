/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// What the alert notebook page draws, from the shipped <c>views.js</c> run under Node
/// (<c>alert-notebook-harness.mjs</c>): a read the catalog lists renders instead of failing as "Unknown read",
/// markdown and chart cells render, a chart is scoped to the registry name the endpoint sent (never a display
/// name) with its own range, and a cell the page cannot draw says so. Node is skipped when it is not installed, the way
/// <see cref="WebRenderSettleTests"/> does; <see cref="AlertNotebookRenderClientTests"/> pins the same shape
/// in the source text.
/// </summary>
public sealed class AlertNotebookRenderBehaviourTests
{
    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "alert-notebook-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "views.js"));
        psi.ArgumentList.Add(scenario);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the alert notebook harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the alert notebook harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    private static string[] Strings(JsonElement node, string name) =>
        node.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void AReadTheCatalogLists_Renders_AndMarkdownAndChartCellsRenderWithThem()
    {
        if (!TryRun("good", out var r)) return;

        Assert.Empty(Strings(r, "errors"));
        Assert.Empty(Strings(r, "notices"));
        Assert.Equal(0, r.GetProperty("loading").GetInt32());

        Assert.Equal("get_blocking", Assert.Single(r.GetProperty("reads").EnumerateArray()).GetProperty("read").GetString());
        Assert.Equal("**Category:** blocking", Assert.Single(Strings(r, "markdown")));
        Assert.Equal(
            new[] { "Alert", "Blocking chains", "Blocked-process reports over time", "Lock modes" },
            Strings(r, "titles"));
    }

    [Fact]
    public void AChartCell_IsScopedToTheAlertsServer_AndKeepsItsOwnRange()
    {
        if (!TryRun("good", out var r)) return;

        var charts = r.GetProperty("composed").EnumerateArray().ToArray();
        Assert.Equal(2, charts.Length);
        foreach (var chart in charts)
        {
            Assert.Equal("SRV1", chart.GetProperty("server").GetString());
            Assert.Equal("2026-01-01T00:00:00Z", chart.GetProperty("range").GetProperty("windowStart").GetString());
            Assert.Equal("2026-01-01T12:00:00Z", chart.GetProperty("range").GetProperty("windowEnd").GetString());
        }
    }

    [Fact]
    public void AChartCell_IsScopedByTheEndpointsRegistryName_NotByTheAlertRowsOrTheLinksDisplayName()
    {
        if (!TryRun("displayName", out var r)) return;

        var chart = Assert.Single(r.GetProperty("composed").EnumerateArray());
        Assert.Equal("registry-key-a", chart.GetProperty("server").GetString());
        Assert.Empty(Strings(r, "notices"));
    }

    [Fact]
    public void WithDisplayNamesButNoScopeServer_TheChartIsNotShown_RatherThanScopedByAName()
    {
        if (!TryRun("displayNameOnly", out var r)) return;

        Assert.Empty(r.GetProperty("composed").EnumerateArray());
        Assert.Contains("names no server", Assert.Single(Strings(r, "notices")), StringComparison.Ordinal);
    }

    [Fact]
    public void WithNoMatchedAlert_TheLinksServerScopesTheChart_AndWithNoServerTheCellSaysSo()
    {
        if (!TryRun("linkServer", out var link)) return;
        Assert.Equal("SRV9", Assert.Single(link.GetProperty("composed").EnumerateArray()).GetProperty("server").GetString());

        TryRun("noServer", out var none);
        Assert.Empty(none.GetProperty("composed").EnumerateArray());
        Assert.Contains("names no server", Assert.Single(Strings(none, "notices")), StringComparison.Ordinal);
    }

    [Fact]
    public void WithoutACatalog_TheCellsFailTheirOwnCheck_AndACellKindNothingDrawsSaysItIsNotShown()
    {
        if (!TryRun("noCatalog", out var none)) return;
        Assert.Contains(Strings(none, "errors"), e => e.StartsWith("Unknown read 'get_blocking'", StringComparison.Ordinal));
        Assert.Empty(none.GetProperty("reads").EnumerateArray());

        TryRun("unknownKind", out var kind);
        Assert.Contains("is not shown here", Assert.Single(Strings(kind, "notices")), StringComparison.Ordinal);
        Assert.Contains("Mystery cell", Strings(kind, "titles"));
    }

    [Fact]
    public void PanelCellsThatCannotLoad_NeverHoldALimiterSlot_SoALaterReadStillRenders()
    {
        if (!TryRun("badPanelsThenRead", out var r)) return;

        Assert.Equal(3, Strings(r, "errors").Length);
        Assert.Equal("get_blocking", Assert.Single(r.GetProperty("reads").EnumerateArray()).GetProperty("read").GetString());
        Assert.Equal(0, r.GetProperty("loading").GetInt32());
    }

    /// <summary>#7918: a dismissed firing's card says it is dismissed, so a reader who followed the link knows the alert
    /// was acknowledged. A live firing's card carries no such notice (the "good" scenario pins zero notices).</summary>
    [Fact]
    public void ADismissedAlert_SaysSoOnItsCard()
    {
        if (!TryRun("dismissed", out var r)) return;

        Assert.Empty(Strings(r, "errors"));
        Assert.Contains(Strings(r, "notices"), n => n.Contains("dismissed", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>#8555: a report row declares no notebook. The page draws only the caller's notes: no "Alert notebook" header, no
    /// Open live button and no "no cells yet" line, which had nothing to act on and read as a broken notebook.</summary>
    [Fact]
    public void AReportRow_DrawsNoNotebookChrome()
    {
        if (!TryRun("noNotebook", out var r)) return;

        Assert.Empty(Strings(r, "heads"));
        Assert.Empty(Strings(r, "buttons"));
        Assert.Empty(Strings(r, "empties"));
        Assert.Empty(Strings(r, "errors"));
    }

    /// <summary>The counterpart of the report row: an ordinary alert whose notebook has no cells keeps its header, its button and its line.</summary>
    [Fact]
    public void AnOrdinaryAlertWithNoCells_KeepsTheNotebookHeaderAndTheEmptyLine()
    {
        if (!TryRun("emptyNotebook", out var r)) return;

        Assert.Equal(new[] { "Alert notebook" }, Strings(r, "heads"));
        Assert.Contains("Open live", Strings(r, "buttons"));
        Assert.Equal(new[] { "This notebook has no cells yet." }, Strings(r, "empties"));
    }

    /// <summary>The header card's labelled fields, label to value.</summary>
    private static Dictionary<string, string?> Fields(JsonElement node) =>
        node.GetProperty("fields").EnumerateArray().ToDictionary(f => f[0].GetString()!, f => f[1].GetString());

    /// <summary>A resolution row has no measured value or threshold: the row's state_only says its stored zeros are not readings,
    /// and Alert History shows a dash for each. The header leaves the value-vs-threshold line out rather than print 0 against 0.</summary>
    [Fact]
    public void AResolutionRow_HasNoValueVsThresholdLine()
    {
        if (!TryRun("resolution", out var r)) return;

        Assert.Empty(Strings(r, "errors"));
        var fields = Fields(r);
        Assert.Equal("Client Timeouts Cleared", fields["Metric"]);
        Assert.DoesNotContain("Value vs threshold", fields.Keys);
    }

    /// <summary>A state-only row with a real number on one side keeps that number and shows the dash for the stored zero, the per-side
    /// rule Alert History applies.</summary>
    [Fact]
    public void AStateOnlyRow_WithARealReading_ShowsIt_AndADashForTheStoredZero()
    {
        if (!TryRun("stateMeasured", out var r)) return;

        Assert.Empty(Strings(r, "errors"));
        Assert.Equal("current 3 vs threshold —", Fields(r)["Value vs threshold"]);
    }

    /// <summary>A measured row keeps both numbers, and a measured reading that really is 0 is still a number: only a state-only row's
    /// stored zero is a dash.</summary>
    [Fact]
    public void AMeasuredRow_ShowsBothNumbers_EvenWhenTheReadingIsZero()
    {
        if (!TryRun("measured", out var measured)) return;
        Assert.Equal("current 90 vs threshold 80", Fields(measured)["Value vs threshold"]);

        TryRun("measuredZero", out var zero);
        Assert.Equal("current 0 vs threshold 0", Fields(zero)["Value vs threshold"]);
    }

    /// <summary>The notebook's own Alert History table cell shows a resolution row's stored zeros as the dash too: the read's table
    /// (resolved from the shipped read catalog) carries the state-zero dash rule on its Value and Threshold columns and on no other.</summary>
    [Fact]
    public void TheAlertHistoryTableCell_CarriesTheStateZeroDashRule_OnItsValueColumns()
    {
        if (!TryRun("historyTable", out var r)) return;

        Assert.Empty(Strings(r, "errors"));
        var read = r.GetProperty("reads").EnumerateArray().Single(x => x.GetProperty("read").GetString() == "get_alert_history");
        Assert.Equal(8, read.GetProperty("columns").GetInt32());
        Assert.Equal(
            new[] { "current_value", "threshold_value" },
            read.GetProperty("stateZeroDash").EnumerateArray().Select(e => e.GetString()).ToArray());
    }
}
