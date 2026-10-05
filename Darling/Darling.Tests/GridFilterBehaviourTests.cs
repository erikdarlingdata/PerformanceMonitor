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
/// Per-column filters on the web dashboard's shared table renderer (#4843), from the shipped <c>panels.js</c> run under Node (<c>web-grid-filter-harness.mjs</c>). Node is skipped when it is not installed.
/// </summary>
public sealed class GridFilterBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-grid-filter-harness.mjs"));
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
                Assert.Fail("the grid filter harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the grid filter harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static string Str(JsonElement r, string name) => r.GetProperty(name).GetString()!;

    private static bool Bool(JsonElement r, string name) => r.GetProperty(name).GetBoolean();

    private static string[] Arr(JsonElement r, string name) => r.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void AFilter_NarrowsTheRows_CaseInsensitively_AndSaysSo()
    {
        var r = Run("narrows");
        Assert.Equal(new[] { "Alpha", "beta", "ALPHA two" }, Arr(r, "before"));
        Assert.Equal(new[] { true, true, true, true }, r.GetProperty("hasButtons").EnumerateArray().Select(e => e.GetBoolean()).ToArray());
        Assert.Equal(new[] { "Alpha", "ALPHA two" }, Arr(r, "after"));
        Assert.Equal("Showing 2 of 3 rows", Str(r, "count"));
        Assert.Equal(new[] { "Name: ALPHA×" }, Arr(r, "chips"));
        Assert.Equal("true", Str(r, "pressed"));
    }

    [Fact]
    public void TheMatch_ReadsTheRenderedText_AListCellAndAFormattedNumberIncluded()
    {
        var r = Run("listAndFormatted");
        Assert.Equal(new[] { "Alpha", "ALPHA two" }, Arr(r, "list"));
        Assert.Equal(new[] { "ALPHA two" }, Arr(r, "listExact"));
        Assert.Equal(new[] { "n" }, Arr(r, "formatted"));
    }

    [Fact]
    public void SeveralFilters_CombineWithAnd()
    {
        var r = Run("ands");
        Assert.Equal(new[] { "Alpha" }, Arr(r, "both"));
        Assert.Equal(new[] { "Name: alpha×", "Tags: red×" }, Arr(r, "chips"));
        Assert.Equal("Showing 1 of 3 rows", Str(r, "count"));
    }

    [Fact]
    public void TheFilter_AndTheOpenBox_SurviveARebuild_AndAnotherServerIsUnaffected()
    {
        var r = Run("repaint");
        Assert.Equal(new[] { "beta" }, Arr(r, "rebuilt"));
        Assert.Equal("Showing 1 of 3 rows", Str(r, "count"));
        Assert.Equal("beta", Str(r, "boxValue"));
        Assert.Equal("Filter Name", Str(r, "boxLabel"));
        Assert.Equal(new[] { "Alpha", "beta", "ALPHA two" }, Arr(r, "otherServer"));
        Assert.False(Bool(r, "otherBox"));
        Assert.Equal("", Str(r, "otherCount"));
        Assert.Equal(new[] { "beta" }, Arr(r, "backToA"));
    }

    [Fact]
    public void Escape_ClosesTheBox_AndReturnsFocusToTheHeaderButton()
    {
        var r = Run("escape");
        Assert.True(Bool(r, "openFocus"));
        Assert.True(Bool(r, "closed"));
        Assert.True(Bool(r, "refocus"));
        Assert.True(Bool(r, "reopened"));
        Assert.True(Bool(r, "toggledClosed"));
        Assert.Equal("none", Str(r, "sortStateUntouched"));
    }

    [Fact]
    public void CopyAndCsv_CarryTheFilteredRows_InSortOrder_WithEveryColumn()
    {
        var r = Run("exportFiltered");
        Assert.Equal(new[] { "Alpha", "ALPHA two" }, Arr(r, "shown"));
        Assert.Equal("Name\tA\tB\tTags\nAlpha\t3\t30\tred; hot\nALPHA two\t2\t20\tblue; hot", Str(r, "copy"));
        Assert.Equal("Name,A,B,Tags\r\nAlpha,3,30,red; hot\r\nALPHA two,2,20,blue; hot", Str(r, "csv"));
        Assert.Equal("Exported 2 rows.", Str(r, "status"));
    }

    [Fact]
    public void ClearingOneFilter_AndClearingAll_RestoreTheRows()
    {
        var r = Run("clearing");
        Assert.Equal(new[] { "Alpha" }, Arr(r, "afterOne"));
        Assert.Equal(new[] { "Tags: red×" }, Arr(r, "chipsAfterOne"));
        Assert.Equal("red", Str(r, "boxAfterOne"));
        Assert.Equal(3, Arr(r, "afterAll").Length);
        Assert.Equal("", Str(r, "countAfterAll"));
        Assert.Empty(Arr(r, "chipsAfterAll"));
        Assert.Equal(3, Arr(r, "rebuiltAfterAll").Length);
        Assert.True(Bool(r, "openSurvivedClearAll"));
        Assert.Equal(new[] { "beta" }, Arr(r, "typedAfterRebuild"));
        Assert.Equal(3, Arr(r, "afterBoxClear").Length);
    }

    [Fact]
    public void FilterFalse_AndControlColumns_DrawNoFilterButton()
    {
        var r = Run("plain");
        Assert.True(Bool(r, "noButtons"));
        Assert.Equal(new[] { true, false, true, true }, r.GetProperty("perColumn").EnumerateArray().Select(e => e.GetBoolean()).ToArray());
        Assert.False(Bool(r, "controlColumn"));
        Assert.Equal(3, Arr(r, "heads").Length);
    }
}
