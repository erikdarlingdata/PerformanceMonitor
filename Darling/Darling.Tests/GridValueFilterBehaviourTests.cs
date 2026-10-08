/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The value-list column filter on the web dashboard's shared table renderer (#5565), from the shipped <c>panels.js</c> and
/// <c>grid-value-filter.js</c> run under Node (<c>web-grid-value-filter-harness.mjs</c>). The shared cases in
/// <c>Fixtures/column-value-filter-cases.json</c> are driven through the real boxes (tick, untick, Select All, the search
/// box, the text match), and the same cases run on the desktop. Node is skipped when it is not installed.
/// </summary>
public sealed class GridValueFilterBehaviourTests
{
    private static readonly string FixturePath = PathTo("Darling", "Darling.Tests", "Fixtures", "column-value-filter-cases.json");

    private static JsonElement Run(string scenario, params string[] extra)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-grid-value-filter-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);
        foreach (var x in extra)
        {
            psi.ArgumentList.Add(x);
        }

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            // CI (GITHUB_ACTIONS is set there) must have Node: a missing Node fails there instead of quietly skipping the page tests.
            Assert.False(System.Environment.GetEnvironmentVariable("GITHUB_ACTIONS") == "true", "Node is not installed on this CI runner, so the value filter page tests cannot run.");
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the value filter harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the value filter harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static string?[] Strs(JsonElement r) => r.EnumerateArray().Select(e => e.ValueKind == JsonValueKind.Null ? null : e.GetString()).ToArray();

    private static int[] Ints(JsonElement r) => r.EnumerateArray().Select(e => e.GetInt32()).ToArray();

    public static IEnumerable<object[]> FixtureCases()
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(FixturePath));
        foreach (var c in doc.RootElement.GetProperty("cases").EnumerateArray())
        {
            yield return [c.GetProperty("id").GetInt32()];
        }
    }

    [Theory]
    [MemberData(nameof(FixtureCases))]
    public void TheSharedCases_GiveTheSameListStateAndRows_AsTheDesktop(int id)
    {
        using var fixture = JsonDocument.Parse(File.ReadAllText(FixturePath));
        var kase = fixture.RootElement.GetProperty("cases").EnumerateArray().First(c => c.GetProperty("id").GetInt32() == id);
        var actual = Run("fixtureCase", FixturePath, id.ToString()).GetProperty("steps").EnumerateArray().ToArray();
        var steps = kase.GetProperty("steps").EnumerateArray().ToArray();
        Assert.Equal(steps.Length, actual.Length);
        for (var i = 0; i < steps.Length; i++)
        {
            var at = "case " + id + " step " + i + " (" + steps[i].GetProperty("action").GetString() + "): ";
            if (!steps[i].TryGetProperty("expect", out var expect))
            {
                continue;
            }

            var got = actual[i];
            foreach (var p in expect.EnumerateObject())
            {
                switch (p.Name)
                {
                    case "listBlank":
                        Assert.True(p.Value.GetBoolean() == got.GetProperty("listBlank").GetBoolean(), at + "listBlank");
                        break;
                    case "listValues":
                        Assert.Equal(Strs(p.Value), Strs(got.GetProperty("listValues")));
                        break;
                    case "listCount":
                        Assert.Equal(p.Value.GetInt32(), got.GetProperty("listCount").GetInt32());
                        break;
                    case "shownCount":
                        Assert.Equal(p.Value.GetInt32(), got.GetProperty("shownCount").GetInt32());
                        break;
                    case "note":
                        Assert.Equal(p.Value.ValueKind == JsonValueKind.Null ? null : p.Value.GetString(), got.GetProperty("note").ValueKind == JsonValueKind.Null ? null : got.GetProperty("note").GetString());
                        break;
                    case "shown":
                        Assert.Equal(Strs(p.Value), Strs(got.GetProperty("shown")));
                        break;
                    case "state":
                        Assert.Equal(p.Value.GetProperty("mode").GetString(), got.GetProperty("state").GetProperty("mode").GetString());
                        Assert.Equal(p.Value.GetProperty("blank").GetBoolean(), got.GetProperty("state").GetProperty("blank").GetBoolean());
                        Assert.Equal(
                            Strs(p.Value.GetProperty("values")).Select(v => v!.ToUpperInvariant()).Order().ToArray(),
                            Strs(got.GetProperty("state").GetProperty("values")).Select(v => v!.ToUpperInvariant()).Order().ToArray());
                        break;
                    default:
                        Assert.Fail(at + "the fixture has an expectation this test does not know: " + p.Name);
                        break;
                }
            }
        }
    }

    [Fact]
    public void SelectAll_UnderASearch_ChangesOnlyTheValuesTheSearchMatches()
    {
        var r = Run("selectAllUnderSearch");
        Assert.Equal(new[] { "app", "app2" }, Strs(r.GetProperty("listed")));
        Assert.Equal("Hide", r.GetProperty("state").GetProperty("mode").GetString());
        Assert.Equal(new[] { "app", "app2" }, Strs(r.GetProperty("state").GetProperty("values")));
        Assert.Equal(new[] { 0, 4, 5 }, Ints(r.GetProperty("shown")));
        Assert.False(r.GetProperty("allChecked").GetBoolean());
        Assert.True(r.GetProperty("allIndeterminateFull").GetBoolean());
        Assert.True(r.GetProperty("blankTicked").GetBoolean());
        Assert.Equal(new[] { "No value matches the search." }, Strs(r.GetProperty("noMatch")));
    }

    [Fact]
    public void AValueIsBuiltFromText_NeverFromMarkup()
    {
        var r = Run("textOnly");
        Assert.Equal(new[] { "<b>x</b>", "<img src=x onerror=alert(1)>", "plain" }, Strs(r.GetProperty("items")));
        Assert.Equal(0, r.GetProperty("elements").GetInt32());
        Assert.Equal(Strs(r.GetProperty("items")), Strs(r.GetProperty("labels")));
        Assert.Equal(new[] { "Login: hides 1 value×" }, Strs(r.GetProperty("chip")));
        Assert.Equal(new[] { 1, 2 }, Ints(r.GetProperty("shown")));
    }

    [Fact]
    public void TheList_IsOfferedOnShortTextColumnsOnly()
    {
        // Login (text), Ix (number), Seen (time), Query (valueList: false), Long (a 257-character value), Edge (256 characters).
        Assert.Equal(new[] { true, false, false, false, false, true }, Run("offered").GetProperty("offered").EnumerateArray().Select(e => e.GetBoolean()).ToArray());
    }

    [Fact]
    public void TheNoListNameRule_GivesTheDesktopsAnswer_ForEveryPairInTheSharedFixture()
    {
        var r = Run("namePairs", FixturePath);
        Assert.NotEmpty(r.GetProperty("excluded").EnumerateArray());
        Assert.NotEmpty(r.GetProperty("listed").EnumerateArray());
        Assert.All(r.GetProperty("excluded").EnumerateArray(), p => Assert.True(p[1].GetBoolean(), p[0].GetString() + " must get no list"));
        Assert.All(r.GetProperty("listed").EnumerateArray(), p => Assert.False(p[1].GetBoolean(), p[0].GetString() + " must get a list"));
    }

    [Fact]
    public void AColumnNamedLikeProse_OrAStatement_GetsNoList_EvenWithShortCells()
    {
        var r = Run("offeredByName");
        var names = Strs(r.GetProperty("names"));
        var offered = r.GetProperty("offered").EnumerateArray().Select(e => e.GetBoolean()).ToArray();
        // The Alerts "Detail" column (detail_text), job step and severe-error messages, recommendation details, descriptions,
        // scripts, errors and info cells, and a column marked valueList: false (the stored-caveat "reason") get no list;
        // an ordinary name column does.
        for (var i = 0; i < names.Length; i++)
        {
            Assert.True(offered[i] == (names[i] == "login_name" || names[i] == "status"), names[i] + " offered=" + offered[i]);
        }
    }

    [Fact]
    public void TheList_HoldsTheRawValue_NotTheTextTheCellDraws_AndABlankIsABlankRawValue()
    {
        var r = Run("rawValues");
        Assert.Equal(new[] { "(Blanks)", "App", "job", "sa" }, Strs(r.GetProperty("items")));
        Assert.True(r.GetProperty("hasBlank").GetBoolean());
        Assert.Equal(new[] { "<<App>>", "<<app>>", "<<none>>", "<<  >>", "<<job>>" }, Strs(r.GetProperty("shown")));
        Assert.Equal(new[] { "<<App>>", "<<app>>", "<<job>>" }, Strs(r.GetProperty("shownNoBlank")));
    }

    [Fact]
    public void TheValueSearch_FoldsCaseLikeDotNetOrdinalIgnoreCase()
    {
        var r = Run("searchFold");
        Assert.Equal(new[] { "stra\u00DFe" }, Strs(r.GetProperty("sharpS")));
        Assert.Equal(new[] { "STRASSE" }, Strs(r.GetProperty("ss")));
        Assert.Equal(new[] { "App" }, Strs(r.GetProperty("app")));
        Assert.Equal(new[] { "App" }, Strs(r.GetProperty("appLower")));
    }

    [Fact]
    public void TheKeptCopy_IsWrittenOnceAfterABurst_AndAtOnceOnPagehide()
    {
        var r = Run("debounced");
        Assert.Equal(0, r.GetProperty("writesAtOnce").GetInt32());
        Assert.False(r.GetProperty("keyAtOnce").GetBoolean());
        Assert.Equal(0, r.GetProperty("writesAt150").GetInt32());
        Assert.Equal(1, r.GetProperty("writesAfterQuiet").GetInt32());
        var column = r.GetProperty("setAfterQuiet").EnumerateObject().Single().Value.GetProperty("values");
        Assert.Equal("showOnly", column.GetProperty("mode").GetString());
        Assert.Equal(new[] { "job_svc" }, Strs(column.GetProperty("set")));
        Assert.Equal(1, r.GetProperty("writesBeforePagehide").GetInt32());
        Assert.Equal(2, r.GetProperty("writesAfterPagehide").GetInt32());
        Assert.Empty(r.GetProperty("setAfterPagehide").GetProperty("set").EnumerateArray());
    }

    [Fact]
    public void ATextMatch_OnAColumnWithNoList_LastsTheSessionOnly_AndOneOnAListColumnIsKept()
    {
        var r = Run("sessionText");
        // all three matches filter the page now
        Assert.Equal(new[] { "sa" }, Strs(r.GetProperty("inPage")));
        // only the list column's match was written
        var written = JsonDocument.Parse(r.GetProperty("storedText").GetString()!).RootElement.GetProperty("grids")[0][1];
        var only = written.EnumerateObject().Single();
        Assert.StartsWith("u\u0001", only.Name);
        Assert.Equal("a", only.Value.GetProperty("text").GetString());
        // and after a restart only that one comes back
        Assert.Equal(new[] { "sa", "app" }, Strs(r.GetProperty("afterRestart")));
        Assert.Equal(new[] { "Login: a\u00D7" }, Strs(r.GetProperty("chipsAfterRestart")));
    }

    [Fact]
    public void AValuePartKeptOnAColumnThatHasNoList_ShowsReadOnlyWithAClearButton_AndItsTextMatchIsDropped()
    {
        var r = Run("keptValuePart");
        Assert.Equal(new[] { 0 }, Ints(r.GetProperty("before")));
        Assert.Equal(new[] { 0, 2 }, Ints(r.GetProperty("afterShown")));
        Assert.False(r.GetProperty("hasList").GetBoolean());
        Assert.Equal(new[] { "A kept value filter hides 1 value. This column has no value list." }, Strs(r.GetProperty("keptNote")));
        Assert.Equal("", r.GetProperty("textBox").GetString());
        Assert.True(r.GetProperty("hasClear").GetBoolean());
        Assert.Equal(new[] { 0, 1, 2 }, Ints(r.GetProperty("afterClear")));
        Assert.True(r.GetProperty("keptGone").GetBoolean());
        Assert.Equal("null", r.GetProperty("storedAfterClear").GetString());
    }

    [Fact]
    public void OnABareFinOpsAddress_EachServerGetsItsOwnFilters()
    {
        var r = Run("finopsKey");
        Assert.Equal("#/finops/srv-a/utilization", r.GetProperty("hashA").GetString());
        Assert.Equal(new[] { 0, 2 }, Ints(r.GetProperty("shownA")));
        Assert.Equal("#/finops/srv-b/utilization", r.GetProperty("hashB").GetString());
        Assert.Equal(new[] { 0, 1, 2 }, Ints(r.GetProperty("shownB")));
        Assert.Equal(new[] { 0, 2 }, Ints(r.GetProperty("shownBackOnA")));
        Assert.Equal(0, r.GetProperty("callsOnSameHash").GetInt32());
    }

    [Fact]
    public void TheFinOpsPaint_PinsTheServerBeforeItBuildsTheTab()
    {
        // The behaviour above runs pinFinopsHash itself; this pins that paint() calls it ahead of the tab's build, which
        // is what makes the tab's tables carry the server in their filter key (the page cannot be run without a server).
        var src = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "finops.js");
        var pin = src.IndexOf("pinFinopsHash(chosen, active.id);", System.StringComparison.Ordinal);
        var build = src.IndexOf("active.build(chosen,", System.StringComparison.Ordinal);
        Assert.True(pin > 0 && build > pin, "paint() must call pinFinopsHash before active.build");
    }

    [Fact]
    public void TheList_CarriesAriaLabels_AndEscapeClosesIt()
    {
        var r = Run("keyboard");
        Assert.Equal(new[] { "Search the values of Login", "Select All", "Values of Login" }, Strs(r.GetProperty("labels")));
        Assert.True(r.GetProperty("closed").GetBoolean());
        Assert.True(r.GetProperty("refocus").GetBoolean());
    }

    [Fact]
    public void AChip_SaysHowManyValuesItHidesOrShows_BesideTheTextMatch()
    {
        var r = Run("chips");
        Assert.Equal(new[] { "Login: hides 1 value×" }, Strs(r.GetProperty("one")));
        Assert.Equal(new[] { "Login: hides 2 values×" }, Strs(r.GetProperty("two")));
        Assert.Equal("true", r.GetProperty("pressed").GetString());
        Assert.Equal(new[] { "Login: shows 2 values×" }, Strs(r.GetProperty("shows")));
        Assert.Equal(new[] { "Login: shows 2 values, x×" }, Strs(r.GetProperty("both")));
        Assert.Equal(new[] { 4 }, Ints(r.GetProperty("shownBoth")));
        Assert.Equal("Showing 1 of 5 rows", r.GetProperty("countText").GetString());
        Assert.Equal(new[] { 0, 1, 2, 3, 4 }, Ints(r.GetProperty("afterX")));
        Assert.Equal(JsonValueKind.Null, r.GetProperty("afterXStored").ValueKind);
    }

    [Fact]
    public void AnOpenList_IsRelistedWhenTheRowsAreReconciledInPlace()
    {
        var r = Run("reconciled");
        Assert.Equal(new[] { "app", "sa" }, Strs(r.GetProperty("before")));
        Assert.Equal(new[] { "app", "zed" }, Strs(r.GetProperty("after")));
    }

    [Fact]
    public void TheFilter_SurvivesARefreshATabSwitchAndARestart_AndClearAllForgetsIt()
    {
        var r = Run("kept");
        var column = r.GetProperty("stored").GetProperty("grids")[0][1].EnumerateObject().Single().Value;
        Assert.Equal("hide", column.GetProperty("values").GetProperty("mode").GetString());
        Assert.Equal(new[] { "job_svc" }, Strs(column.GetProperty("values").GetProperty("set")));
        Assert.Equal(new[] { 0, 1, 3, 4 }, Ints(r.GetProperty("afterRefresh")));
        Assert.Equal(new[] { 0, 1, 2, 3 }, Ints(r.GetProperty("otherTab")));
        Assert.Equal(new[] { 0, 1, 3 }, Ints(r.GetProperty("backOnTab")));
        Assert.Equal(new[] { 0, 1, 3 }, Ints(r.GetProperty("afterRestart")));
        Assert.Equal(new[] { "Login: hides 1 value×" }, Strs(r.GetProperty("restartChip")));
        Assert.Equal("true", r.GetProperty("restartButtonOn").GetString());
        Assert.Equal(
            new[] { ("(Blanks)", true), ("app", true), ("job_svc", false), ("sa", true) },
            r.GetProperty("restartTicks").EnumerateArray().Select(t => (t[0].GetString()!, t[1].GetBoolean())).ToArray());
        Assert.Equal(new[] { 0, 1, 2, 3 }, Ints(r.GetProperty("afterClearAll")));
        Assert.Equal(JsonValueKind.Null, r.GetProperty("storedAfterClearAll").ValueKind);
        Assert.False(r.GetProperty("keyAfterClearAll").GetBoolean());
        Assert.Equal(new[] { 0, 1, 2, 3 }, Ints(r.GetProperty("restartAfterClearAll")));
    }

    [Fact]
    public void AStoreThatIsBadOrRefused_NeverStopsAGridDrawing()
    {
        var r = Run("keptBad");
        Assert.All(r.GetProperty("drawn").EnumerateArray(), n => Assert.Equal(2, n.GetInt32()));
        Assert.Equal(7, r.GetProperty("drawn").GetArrayLength());
        Assert.Equal(new[] { 0 }, Ints(r.GetProperty("refusedShown")));
        Assert.Equal(1, r.GetProperty("warnings").GetInt32());
        Assert.Equal(new[] { 0, 1 }, Ints(r.GetProperty("noStorageShown")));
    }

    [Fact]
    public void TheKeptCopy_IsBounded()
    {
        var r = Run("keptBounds");
        Assert.Equal(200, r.GetProperty("gridCount").GetInt32());
        Assert.Equal("#/server/a/g51|VF|k", r.GetProperty("firstKept").GetString());
        Assert.True(r.GetProperty("lastKept").GetBoolean());
        // 1,111 ticked values still filter the page, but are not kept (a cut set would show the wrong rows).
        Assert.Equal(1111, r.GetProperty("bigShown").GetInt32());
        Assert.Equal(0, r.GetProperty("bigStored").GetProperty("grids").GetArrayLength());
        Assert.Equal(new[] { "Login: shows 1111 values×" }, Strs(r.GetProperty("bigChip")));
        // A stored set over 1,000 values is refused on the way in.
        Assert.Equal(2, r.GetProperty("refusedLong").GetInt32());
    }
}
