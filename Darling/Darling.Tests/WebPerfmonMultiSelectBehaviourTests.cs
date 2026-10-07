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
/// What the web Perfmon panel's multi-select counter picker does, from the shipped <c>server-tabs.js</c>,
/// <c>multi-picker.js</c> and <c>charts.js</c> run under Node (<c>web-perfmon-multi-harness.mjs</c>) against a scripted
/// <c>/api/read</c>: the desktop's General Throughput default set, check and uncheck, search, the cap of ten, the state
/// surviving the 60 s rebuild per server, a failed series and the one read per checked counter. Node is skipped when it
/// is not installed.
/// </summary>
public sealed class WebPerfmonMultiSelectBehaviourTests
{
    private static readonly string[] Defaults =
        { "Batch Requests/sec", "Network IO waits", "Query optimizations/sec", "SQL Compilations/sec", "SQL Re-Compilations/sec" };

    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-perfmon-multi-harness.mjs"));
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
                Assert.Fail("the perfmon multi harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the perfmon multi harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            Assert.Empty(result.GetProperty("rejections").EnumerateArray());
            return true;
        }
    }

    private static string[] Strings(JsonElement array) => array.EnumerateArray().Select(e => e.GetString()!).ToArray();

    private static JsonElement Run(string scenario)
    {
        Assert.True(TryRun(scenario, out var result));
        return result;
    }

    [Fact]
    public void TheDefaultSet_IsTheDesktopsGeneralThroughputPack_OneReadEach_NoMetricSwitch()
    {
        var r = Run("defaults");
        Assert.Equal(Defaults, Strings(r.GetProperty("checked")));
        Assert.Equal(Defaults, Strings(r.GetProperty("reads")));
        Assert.Equal(Defaults, Strings(r.GetProperty("chart").GetProperty("labels")));
        Assert.Equal("5 / 10 selected", r.GetProperty("count").GetString());
        Assert.Equal(0, r.GetProperty("selects").GetInt32());
    }

    [Fact]
    public void WhenNoPackCounterIsCollected_TheFirstCounterIsChecked()
    {
        var r = Run("noPackCounters");
        Assert.Equal(new[] { "Alpha" }, Strings(r.GetProperty("checked")));
        Assert.Equal(new[] { "Alpha" }, Strings(r.GetProperty("reads")));
    }

    [Fact]
    public void CheckingAndUnchecking_RedrawsWithOneReadPerCheckedCounter()
    {
        var r = Run("checkUncheck");
        Assert.DoesNotContain("Batch Requests/sec", Strings(r.GetProperty("afterUncheck")));
        Assert.Equal(Strings(r.GetProperty("afterUncheck")), Strings(r.GetProperty("readsAfterUncheck")));
        Assert.Contains("Page Splits/sec", Strings(r.GetProperty("afterCheck")));
        Assert.Equal(Strings(r.GetProperty("afterCheck")), Strings(r.GetProperty("readsAfterCheck")));
        Assert.Equal(5, r.GetProperty("chart").GetProperty("labels").GetArrayLength());
    }

    [Fact]
    public void Search_FiltersTheList_CaseInsensitively_AndSaysWhenNothingMatches()
    {
        var r = Run("search");
        Assert.Equal(new[] { "Lock Waits/sec" }, Strings(r.GetProperty("found")));
        Assert.Empty(r.GetProperty("none").EnumerateArray());
        Assert.Equal(new[] { "No counter matches the search." }, Strings(r.GetProperty("noneText")));
        Assert.Equal(15, r.GetProperty("all").GetInt32());
    }

    [Fact]
    public void TheCapIsTen_SelectAllStopsThere_ClearAllEmptiesTheChart_AndTheDefaultButtonRestoresThePack()
    {
        var r = Run("cap");
        Assert.Empty(r.GetProperty("afterClear").EnumerateArray());
        Assert.Equal(10, r.GetProperty("checked").GetArrayLength());
        Assert.Equal(5, r.GetProperty("disabled").GetInt32());
        Assert.Equal(new[] { "Limit reached: uncheck a counter to pick another." }, Strings(r.GetProperty("hint")));
        Assert.Equal(1, r.GetProperty("emptyChart").GetInt32());
        Assert.Equal(Defaults, Strings(r.GetProperty("top")));
    }

    [Fact]
    public void TheChosenSet_AndSearch_SurviveTheRebuild_PerServer()
    {
        var r = Run("survives");
        Assert.Equal(new[] { "Lock Waits/sec" }, Strings(r.GetProperty("rebuilt")));
        Assert.Equal("Lock", r.GetProperty("search").GetString());
        Assert.Equal(Defaults, Strings(r.GetProperty("other")));
    }

    [Fact]
    public void ACheckedCounter_OutsideThisPollsList_StaysCheckedListedAndRead_AndIsStillThereWhenItReturns()
    {
        var r = Run("droppedOut");
        Assert.Equal(new[] { "C13" }, Strings(r.GetProperty("checked")));
        Assert.Equal(new[] { "C13" }, Strings(r.GetProperty("listedLast")));
        Assert.Equal(new[] { "C13" }, Strings(r.GetProperty("reads")));
        Assert.Equal(new[] { "C13" }, Strings(r.GetProperty("checkedBack")));
    }

    [Fact]
    public void OneFailedRead_GetsANote_AndTheOtherLinesStillDraw()
    {
        var r = Run("oneFailed");
        Assert.Equal(4, r.GetProperty("chart").GetProperty("labels").GetArrayLength());
        Assert.Contains(Strings(r.GetProperty("notes")), n => n.StartsWith("SQL Compilations/sec:"));
        Assert.Empty(r.GetProperty("errors").EnumerateArray());
    }

    [Fact]
    public void EveryReadFailed_ShowsAnError_AndEveryReadEmpty_ShowsTheEmptyState_NotAnError()
    {
        var failed = Run("allFailed");
        Assert.Single(failed.GetProperty("errors").EnumerateArray());
        Assert.Equal(5, failed.GetProperty("notes").GetArrayLength());
        var empty = Run("allEmpty");
        Assert.Empty(empty.GetProperty("errors").EnumerateArray());
        Assert.Contains(Strings(empty.GetProperty("empties")), e => e.StartsWith("None of the 5 checked counters has trend data"));
    }

    [Fact]
    public void ACounterWithNoTrend_ListsTheCollectedCountersAsText_NotAsAJsonPath()
    {
        var r = Run("hintedEmpty");
        var note = Assert.Single(Strings(r.GetProperty("notes")), n => n.StartsWith("SQL Compilations/sec:"));
        Assert.Contains("Collected here: Alpha/sec, Beta pages.", note);
        Assert.DoesNotContain("hints.collected_counters", note);
        Assert.Empty(r.GetProperty("errors").EnumerateArray());
    }

    [Fact]
    public void FastClicks_AbortTheSupersededReads_AndLeaveOneDrawsReadsInFlight()
    {
        var r = Run("fastClicks");
        Assert.Equal(30, r.GetProperty("sent").GetInt32());
        Assert.Equal(25, r.GetProperty("aborted").GetInt32());
        Assert.Equal(5, r.GetProperty("stillInFlight").GetInt32());
    }

    [Fact]
    public void TheSearchBox_KeepsItsFocusAndCaret_AcrossTheRebuild_AndStealsNoFocus()
    {
        var r = Run("focusKept");
        Assert.False(r.GetProperty("sameNode").GetBoolean());
        Assert.True(r.GetProperty("focused").GetBoolean());
        Assert.Equal(1, r.GetProperty("start").GetInt32());
        Assert.Equal(2, r.GetProperty("end").GetInt32());
        Assert.Equal("Batch", r.GetProperty("value").GetString());
        Assert.False(Run("noFocusStolen").GetProperty("focused").GetBoolean());
    }

    [Fact]
    public void TheChart_ReadsAtMostTenCounters_WhateverItIsHanded()
    {
        Assert.Equal(10, Run("capInDraw").GetProperty("reads").GetInt32());
    }

    [Fact]
    public void RatesShareAPerSecondAxis_ButAChartThatMixesInAGauge_DropsTheUnit()
    {
        Assert.Equal("/s", Run("defaults").GetProperty("chart").GetProperty("unit").GetString());
        Assert.Equal(JsonValueKind.Null, Run("mixedKinds").GetProperty("chart").GetProperty("unit").ValueKind);
    }

    [Fact]
    public void ThePerfmonPanel_ReadsGetPerfmonTrend_OncePerCounter_AndKeepsTheDiscontinuityNotice()
    {
        var tabs = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");
        Assert.Contains("readToolWithinKeptHistory(\"get_perfmon_trend\", { server, counter_name: counter, hours: ctx.hours }, mine.signal)", tabs);
        Assert.Contains("const MAX_COUNTERS_CHARTED = 10;", tabs);
        Assert.Contains("export async function drawPerfmonTrends(", tabs);
        Assert.Contains("discontinuityNotes(trend.data)", tabs);
    }
}
