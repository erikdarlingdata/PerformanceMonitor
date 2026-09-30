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
/// What the alert notebook page draws, from the shipped <c>views.js</c> run under Node
/// (<c>alert-notebook-harness.mjs</c>): a read the catalog lists renders instead of failing as "Unknown read",
/// markdown and chart cells render, a chart is scoped to the alert's server with its own range, and a cell
/// the page cannot draw says so. Node is skipped when it is not installed, the way
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
}
