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
    public void AReconciledGrid_ReappliesTheSortToTheNewRow_AndAClearedSortGoesBackToTheServersOrder()
    {
        if (!TryRun("reapply", out var r)) return;
        // Sorted ascending by N, every tr still maps to the row it was built from.
        Assert.Equal(new[] { "", "Alpha", "beta", "alpha", null }, r.GetProperty("rowsOf").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal(new[] { "50", "2", "9", "10", "100", "—" }, S(r, "serverOrder"));
        Assert.Equal(new[] { "2", "9", "10", "50", "100", "—" }, S(r, "sorted"));
        Assert.Equal("gamma", r.GetProperty("freshRow").GetString());
        // After desc then clear, the grid is back to the server order, and a reapply keeps it.
        Assert.Equal(new[] { "50", "2", "9", "10", "100", "—" }, S(r, "cleared"));
        Assert.Equal(S(r, "cleared"), S(r, "clearedAfterReapply"));
    }

    [Fact]
    public void ASortValueDecidesTheOrder_ListColumnsGetNoSort_AndAColumnFilledLaterBecomesSortable()
    {
        if (!TryRun("columns", out var r)) return;
        Assert.Contains("sortable", S(r, "cls")[0]);
        Assert.DoesNotContain("sortable", S(r, "cls")[1]);
        Assert.Equal(new[] { "99.000", "1,234.500", "2,000.000" }, S(r, "sizeAsc"));
        // Max size descending: the unlimited row (raw -1) is the largest, so it comes first.
        Assert.Equal("99.000", S(r, "maxDesc")[0]);
        Assert.DoesNotContain("sortable", r.GetProperty("laterBefore").GetString());
        Assert.Contains("sortable", r.GetProperty("laterAfter").GetString());
        Assert.Equal(new[] { "alpha", "zeta" }, S(r, "laterSorted"));
    }

    [Fact]
    public void ANumericColumnOverPreformattedText_MustCarryASortValueOnTheRawNumber()
    {
        var js = PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js");
        var offenders = new System.Collections.Generic.List<string>();
        foreach (var file in System.IO.Directory.EnumerateFiles(js, "*.js", System.IO.SearchOption.AllDirectories))
        {
            foreach (var line in System.IO.File.ReadAllLines(file))
            {
                if (!System.Text.RegularExpressions.Regex.IsMatch(line, "key: *\"[a-z_0-9.]*_text\"")) continue;
                var numeric = line.Contains("align: \"right\"") || System.Text.RegularExpressions.Regex.IsMatch(line, "format: *\"(int|num1|num2|rate|ms|mb|pct)\"");
                if (numeric && !line.Contains("sortValue:")) offenders.Add(System.IO.Path.GetFileName(file) + ": " + line.Trim());
            }
        }
        Assert.Empty(offenders);
    }

    [Fact]
    public void AnOptedOutColumnOrTable_GetsNoSortAffordance()
    {
        if (!TryRun("optOut", out var r)) return;
        Assert.DoesNotContain("sortable", r.GetProperty("fixedCls").GetString());
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
