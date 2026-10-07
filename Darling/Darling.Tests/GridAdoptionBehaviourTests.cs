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
/// Per-column filters on the web dashboard's shared table renderer (#4843), from the shipped <c>panels.js</c> run under Node (<c>web-grid-adoption-harness.mjs</c>). Node is skipped when it is not installed.
/// </summary>
public sealed class GridAdoptionBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-grid-adoption-harness.mjs"));
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
                Assert.Fail("the grid adoption harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the grid adoption harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static string Str(JsonElement r, string name) => r.GetProperty(name).GetString()!;

    private static bool Bool(JsonElement r, string name) => r.GetProperty(name).GetBoolean();

    private static string[] Arr(JsonElement r, string name) => r.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void ComposedTable_Sorts_Filters_CopiesAndExports_RawValues_AndKeepsStateAcrossARebuild()
    {
        var r = Run("composed");
        Assert.Equal(new[] { "Time", "Db", "Value" }, Arr(r, "heads"));
        Assert.Equal(new[] { "20", "300", "1,500" }, Arr(r, "sortedAsc"));
        Assert.Equal(new[] { "1,500", "300", "20" }, Arr(r, "sortedDesc"));
        Assert.Equal(new[] { "alpha", "Alpha two" }, Arr(r, "filtered"));
        var copy = Str(r, "copy").Split('\n');
        Assert.Equal(3, copy.Length);
        Assert.Equal("Time\tDb\tValue", copy[0]);
        Assert.EndsWith("\talpha\t1,500", copy[1]);
        Assert.EndsWith("\tAlpha two\t300", copy[2]);
        Assert.Equal("Time,Db,Value\r\n2026-01-02T10:00:00,alpha,1500\r\n2026-01-02T12:00:00,Alpha two,300", Str(r, "csv"));
        Assert.Equal(new[] { "alpha", "Alpha two" }, Arr(r, "rebuilt"));
        Assert.Equal("descending", Str(r, "rebuiltSortInd"));
        Assert.True(Bool(r, "rebuiltBox"));
        Assert.Equal(new[] { "alpha", "beta", "Alpha two" }, Arr(r, "otherServer"));
        Assert.Equal("none", Str(r, "otherSort"));
    }

    [Fact]
    public void AlertRuleTestResult_GetsTheTools_WithVerdictTextAndRawValues_PerRule()
    {
        var r = Run("alert");
        Assert.Equal(new[] { "srv-a", "srv-d", "srv-b", "srv-c" }, Arr(r, "byValueDesc"));
        Assert.Equal(new[] { "srv-a", "srv-d" }, Arr(r, "fired"));
        Assert.Equal("Server\tCurrent value\tWould fire now\nsrv-a\t90.0%\tYes — Critical\nsrv-d\t40.0%\tYes — Warning", Str(r, "copy"));
        Assert.Equal("Server,Current value,Would fire now\r\nsrv-a,90,Yes — Critical\r\nsrv-d,40,Yes — Warning", Str(r, "csv"));
        Assert.Equal(new[] { "srv-a", "srv-d" }, Arr(r, "rebuilt"));
        Assert.Equal("descending", Str(r, "rebuiltSort"));
        Assert.Equal(4, Arr(r, "otherRule").Length);
        Assert.Equal("none", Str(r, "otherRuleSort"));
        Assert.Equal("ascending", Str(r, "draftSort"));
        Assert.Equal("none", Str(r, "nextDraftSort"));
    }

    [Fact]
    public void SweepTables_GetTheTools_AndKeepThemAcrossARedraw()
    {
        var r = Run("sweeps");
        Assert.True(Bool(r, "found"));
        Assert.Equal(new[] { "Server", "From", "To", "Reason" }, Arr(r, "heads"));
        Assert.Equal(new[] { "api-one", "web-one", "web-two" }, Arr(r, "sorted"));
        Assert.Equal(new[] { "api-one", "web-two" }, Arr(r, "filtered"));
        Assert.Equal("Server\tFrom\tTo\tReason\napi-one\tHealthy\tWarning\tcpu\nweb-two\tWarning\tHealthy\trecovered", Str(r, "copy"));
        Assert.Equal(new[] { "api-one", "web-two" }, Arr(r, "rebuilt"));
        Assert.Equal("ascending", Str(r, "rebuiltSort"));
        Assert.Equal(4, Arr(r, "watchRows").Length);
        // The position column orders by its count, not its sentence ("12 consecutive hits" sorts after "3 ...").
        Assert.Equal(new[] { "fleet", "disk", "orphan", "cpu" }, Arr(r, "posAsc"));
        Assert.Equal(new[] { "cpu", "orphan", "disk", "fleet" }, Arr(r, "posDesc"));
        // The fallback labels the page shows are what the column sorts on and the CSV carries.
        Assert.Equal(new[] { "api-one", "Fleet", "server 9", "web-one" }, Arr(r, "serverAsc"));
        Assert.Contains("\r\nFleet,fleet,", Str(r, "serverCsv"));
        Assert.Contains("\r\nserver 9,orphan,", Str(r, "serverCsv"));
        Assert.StartsWith("Server,Item,State,Position,First seen,Last seen,Condition / evidence\r\nweb-one,disk,open,3 consecutive hits,2026-01-02T08:00:00", Str(r, "watchCsv"));
    }

    [Fact]
    public void VizTable_IsUnchanged_GroupsCopyFalseFiltersCopyAndCsv()
    {
        var r = Run("viz");
        Assert.Equal(new[] { "Name", "A", "B", "" }, Arr(r, "heads"));
        Assert.False(Bool(r, "controlFilter"));
        Assert.Equal("none", Str(r, "hiddenB"));
        Assert.Equal(new[] { "Alpha", "ALPHA two" }, Arr(r, "shown"));
        Assert.Equal("Name\tA\tB\nAlpha\t3\t30\nALPHA two\t2\t20", Str(r, "copy"));
        Assert.Equal("Name,A,B,\r\nAlpha,3,30,\r\nALPHA two,2,20,", Str(r, "csv"));
        Assert.Equal(new[] { "Alpha", "ALPHA two" }, Arr(r, "rebuilt"));
        Assert.True(Bool(r, "sameHeads"));
        Assert.True(Bool(r, "sameRows"));
        Assert.Equal("No rows in this window.", Str(r, "emptyViz"));
        Assert.StartsWith("No fields configured", Str(r, "noFields"));
    }

    [Fact]
    public void TwoUntitledComposedTablePanels_OnOneView_DoNotShareSortOrFilters()
    {
        var r = Run("twoPanels");
        Assert.Equal("descending", Str(r, "aSort"));
        Assert.Equal(new[] { "alpha", "Alpha two" }, Arr(r, "aRows"));
        Assert.Equal("none", Str(r, "bSort"));
        Assert.Equal(3, Arr(r, "bRows").Length);
        Assert.Equal("descending", Str(r, "a2Sort"));
        Assert.Equal(2, Arr(r, "a2Rows").Length);
    }

    [Fact]
    public void RenderComposedInto_ThreadsThePanelSlotToTheTable_AgainstTheRealGrid()
    {
        var r = Run("composedInto");
        Assert.True(Bool(r, "found"));
        Assert.Equal(3, Arr(r, "rows").Length);
        Assert.Equal("ascending", Str(r, "sameSlotSort"));
        Assert.Equal("none", Str(r, "otherSlotSort"));
    }

    [Fact]
    public void VizTable_EdgeShapes_SingleColumn_AllHidden_DotRowsKey_ToolsAndFilterOff_EmptyCellClass()
    {
        var r = Run("vizEdges");
        Assert.Equal(new[] { "Name" }, Arr(r, "oneHeads"));
        Assert.Equal(new[] { "Alpha", "beta" }, Arr(r, "oneSorted"));
        Assert.Equal(new[] { "none", "none" }, Arr(r, "hiddenShown"));
        Assert.Equal("Name\tN\nAlpha\t3\nbeta\t1", Str(r, "hiddenCopy"));
        Assert.Equal(new[] { "solo" }, Arr(r, "dotRows"));
        Assert.Equal(new[] { "7" }, Arr(r, "dotN"));
        Assert.False(Bool(r, "noToolsCopy"));
        Assert.True(Bool(r, "noToolsFilter"));
        Assert.True(Bool(r, "noFilterTools"));
        Assert.False(Bool(r, "noFilterBtn"));
        Assert.True(Bool(r, "noFilterSort"));
        Assert.DoesNotContain("undefined", Str(r, "cls"));
    }
}
