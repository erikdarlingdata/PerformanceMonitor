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
/// Column-header sort on the web dashboard's shared table renderer (#4843), from the shipped <c>panels.js</c> run
/// under Node (<c>web-grid-sort-harness.mjs</c>). Node is skipped when it is not installed.
/// </summary>
public sealed class GridSortBehaviourTests
{
    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-grid-sort-harness.mjs"));
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
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the grid sort harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the grid sort harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            result = doc.RootElement.Clone();
            return true;
        }
    }

    private static string[] S(JsonElement r, string name) => r.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void AHeaderClick_CyclesAscending_Descending_ThenTheServersOrder_WithIndicatorAndAriaSort()
    {
        if (!TryRun("cycle", out var r)) return;
        Assert.Equal(new[] { "10", "9", "—", "100", "2" }, S(r, "initial"));
        Assert.Equal(new[] { "2", "9", "10", "100", "—" }, S(r, "asc"));
        Assert.Equal("ascending", r.GetProperty("ascSort").GetString());
        Assert.Equal(" ▲", r.GetProperty("ascInd").GetString());
        Assert.Equal(new[] { "100", "10", "9", "2", "—" }, S(r, "desc"));
        Assert.Equal("descending", r.GetProperty("descSort").GetString());
        Assert.Equal(" ▼", r.GetProperty("descInd").GetString());
        Assert.Equal(S(r, "initial"), S(r, "back"));
        Assert.Equal("none", r.GetProperty("backSort").GetString());
        Assert.Equal("0", r.GetProperty("tabindex").GetString());
    }

    [Fact]
    public void Text_SortsWithoutCase_TiesKeepTheServersOrder_AndEmptyIsLastBothWays()
    {
        if (!TryRun("text", out var r)) return;
        // Rows 2 ("Alpha") and 4 ("alpha") tie; rows 3 (null) and 5 ("") are empty.
        Assert.Equal(new[] { "2", "4", "1", "3", "5" }, S(r, "asc"));
        Assert.Equal(new[] { "1", "2", "4", "3", "5" }, S(r, "desc"));
    }

    [Fact]
    public void Time_SortsByTheStoredInstant_NotTheDisplayedText_AndEmptyIsLastBothWays()
    {
        if (!TryRun("time", out var r)) return;
        Assert.Equal(new[] { "5", "1", "4", "2", "3" }, S(r, "asc"));
        Assert.Equal(new[] { "2", "4", "1", "5", "3" }, S(r, "desc"));
    }

    [Fact]
    public void EnterAndSpace_ActivateAHeader_OtherKeysDoNot()
    {
        if (!TryRun("keyboard", out var r)) return;
        Assert.Equal(new[] { "2", "9", "10", "100", "—" }, S(r, "enter"));
        Assert.Equal(new[] { "100", "10", "9", "2", "—" }, S(r, "space"));
        Assert.Equal(S(r, "space"), S(r, "other"));
    }

    [Fact]
    public void ARebuildUnderTheSameKey_ReappliesTheSort_ButADifferentTableOrRouteStartsInTheServersOrder()
    {
        if (!TryRun("rebuild", out var r)) return;
        var desc = new[] { "100", "10", "9", "2", "—" };
        var server = new[] { "10", "9", "—", "100", "2" };
        Assert.Equal(desc, S(r, "sameKey"));
        Assert.Equal("descending", r.GetProperty("sameKeySort").GetString());
        Assert.Equal(server, S(r, "otherKey"));
        Assert.Equal(server, S(r, "otherRoute"));
        Assert.Equal(desc, S(r, "sameRouteWithQuery"));
    }

    [Fact]
    public void AnOptedOutColumnOrTable_GetsNoSortAffordance()
    {
        if (!TryRun("optOut", out var r)) return;
        Assert.Equal("", r.GetProperty("fixedCls").GetString());
        Assert.Equal(JsonValueKind.Null, r.GetProperty("fixedTab").ValueKind);
        Assert.All(S(r, "offCls"), c => Assert.DoesNotContain("sortable", c));
    }

    [Fact]
    public void TheSharedRendererCarriesTheSort_AndTheOptOutIsHonouredAndSet()
    {
        var panels = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "panels.js");
        Assert.Contains("desc.sortable === false", panels);
        Assert.Contains("gridSortState", panels);
        Assert.Contains("aria-sort", panels);
        Assert.Contains("export function reapplyGridSort", panels);
        var util = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "finops", "utilization.js");
        Assert.Contains("columns: TREND_COLUMNS, sortable: false", util);
        var alerts = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "alerts.js");
        Assert.Contains("reapplyGridSort(state.tbody)", alerts);
    }
}
