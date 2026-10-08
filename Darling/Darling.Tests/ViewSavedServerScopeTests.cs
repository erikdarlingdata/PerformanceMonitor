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
/// W4 of the morning walk: a custom view saved with a server (its server-dimension variable's default) opened on
/// "All servers (fleet)" and read the whole fleet, because the scope control always seeded "All" and the run body's
/// explicit server "All" beat the variable on the service side. The shipped <c>views.js</c> seeding is run under Node
/// (<c>view-scope-harness.mjs</c>); Node is skipped when it is not installed.
/// </summary>
public sealed class ViewSavedServerScopeTests
{
    private static bool TryRun(out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "view-scope-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "views.js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped view-scope code cannot be run.");
            return false;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the view-scope harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the view-scope harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    private static string[] Names(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    [Fact]
    public void AViewSavedWithAServer_OpensOnThatServer()
    {
        if (!TryRun(out var r)) return;
        Assert.Equal(new[] { "sql2025" }, Names(r.GetProperty("byName")));
        Assert.Equal(new[] { "sql2022" }, Names(r.GetProperty("byDisplayName")));
        Assert.Equal(new[] { "sql2025", "sql2022" }, Names(r.GetProperty("list")));
    }

    [Theory]
    [InlineData("none")]
    [InlineData("all")]
    [InlineData("blank")]
    [InlineData("reference")]
    [InlineData("unknown")]
    [InlineData("twoServerVariables")]
    [InlineData("otherDimension")]
    public void AViewWithNoUsableSavedServer_StaysOnTheWholeFleet(string scenario)
    {
        if (!TryRun(out var r)) return;
        Assert.Equal("All", r.GetProperty(scenario).GetString());
    }

    // ---- W4b: a view with no server variable whose every panel filters to one server (custom view 3's shape) ----

    [Fact]
    public void AViewWhoseEveryPanelFiltersToOneServer_OpensOnThatServer()
    {
        if (!TryRun(out var r)) return;
        Assert.Equal("sql2025", r.GetProperty("panelServerOfWholeView").GetString());
        Assert.Equal(new[] { "sql2025" }, Names(r.GetProperty("panelSeed")));
        // an explicit server variable still decides first; no panel server leaves the fleet
        Assert.Equal(new[] { "sql2022" }, Names(r.GetProperty("panelSeedVariableWins")));
        Assert.Equal("All", r.GetProperty("panelSeedNone").GetString());
        Assert.Contains("every panel is set to SQL2025", r.GetProperty("noteOnServer").GetString());
    }

    [Theory]
    [InlineData("panelServerMixed")]
    [InlineData("panelServerOnePanelUnfiltered")]
    [InlineData("panelServerTwoServerFilters")]
    [InlineData("panelServerInOp")]
    [InlineData("panelServerNotInFleet")]
    [InlineData("panelServerNoComposed")]
    public void APanelSetThatIsNotOneServerEverywhere_HasNoPanelServer(string scenario)
    {
        if (!TryRun(out var r)) return;
        Assert.Equal(JsonValueKind.Null, r.GetProperty(scenario).ValueKind);
    }

    [Fact]
    public void ChoosingAnotherServer_ReplacesThePanelsServerFilter_InsteadOfEmptyingThem()
    {
        if (!TryRun(out var r)) return;
        // still on the panels' server: the panel runs as saved (both filters kept)
        Assert.Equal(2, r.GetProperty("underSame").GetInt32());
        // another server, or All servers: only the server filter goes, the other filters stay
        Assert.Equal(new[] { "database" }, Names(r.GetProperty("underOther")));
        Assert.Equal(0, r.GetProperty("underAll").GetInt32());
        // a panel that is not composed passes through, and read panels do not stop a view from having a panel server
        Assert.Equal("get_x", r.GetProperty("underReadPanel").GetProperty("read").GetString());
        Assert.Equal("sql2025", r.GetProperty("panelServerReadPanelsIgnored").GetString());
        Assert.Contains("Showing your pick instead of SQL2025", r.GetProperty("noteOnOther").GetString());
    }

    // ---- review round (L6, L7): an explicit server variable decides first; "your pick" only when the reader picked; one eq only ----

    [Theory]
    [InlineData("panelSeedVariableAll")]
    [InlineData("panelSeedVariableReference")]
    [InlineData("panelSeedVariableUnknown")]
    public void AnExplicitServerVariable_AllIncluded_DecidesBeforeThePanelServerSeeds(string scenario)
    {
        if (!TryRun(out var r)) return;
        Assert.Equal("All", r.GetProperty(scenario).GetString());
        // a variable of another dimension is not a server variable: the panels' server still seeds
        Assert.Equal(new[] { "sql2025" }, Names(r.GetProperty("panelSeedOtherDimensionVariable")));
    }

    [Fact]
    public void TheNoteSaysYourPick_OnlyWhenTheReaderPicked_AndNamesTheViewsOwnSettingOtherwise()
    {
        if (!TryRun(out var r)) return;
        Assert.Contains("Showing your pick instead of SQL2025", r.GetProperty("noteOnOther").GetString());
        var variableNote = r.GetProperty("noteOnOtherNotPicked").GetString()!;
        Assert.DoesNotContain("your pick", variableNote, System.StringComparison.Ordinal);
        Assert.Contains("this view's own setting", variableNote, System.StringComparison.Ordinal);
        Assert.Contains("SQL2025", variableNote, System.StringComparison.Ordinal);

        var flags = r.GetProperty("pickedFlags");
        Assert.False(flags.GetProperty("fresh").GetBoolean());
        Assert.False(flags.GetProperty("variable").GetBoolean());
        Assert.True(flags.GetProperty("remembered").GetBoolean());
        Assert.False(flags.GetProperty("rememberedHoursOnly").GetBoolean());
    }

    [Theory]
    [InlineData("panelServerEqPlusIn")]
    [InlineData("panelServerEqPlusNeq")]
    public void APanelWithAnyServerFilterBesidesTheOneEq_HasNoPanelServer(string scenario)
    {
        if (!TryRun(out var r)) return;
        Assert.Equal(JsonValueKind.Null, r.GetProperty(scenario).ValueKind);
        // a filter on another dimension beside the eq is fine
        Assert.Equal("sql2025", r.GetProperty("panelServerEqPlusOtherDimension").GetString());
    }
}
