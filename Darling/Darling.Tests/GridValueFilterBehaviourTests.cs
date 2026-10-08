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
