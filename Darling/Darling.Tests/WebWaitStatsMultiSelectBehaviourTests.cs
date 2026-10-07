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
/// What the web Wait Stats panel's multi-select picker does, from the shipped <c>server-tabs.js</c>,
/// <c>multi-picker.js</c> and <c>charts.js</c> run under Node (<c>web-waits-harness.mjs</c>) against a scripted
/// <c>/api/read</c>: the default set, check and uncheck, search, the cap of ten, the state surviving the 60 s rebuild for
/// one server and starting fresh for another, a failed series and the one read per checked wait. Node is skipped when it
/// is not installed.
/// </summary>
public sealed class WebWaitStatsMultiSelectBehaviourTests
{
    private static readonly string[] FirstTen =
        { "CXPACKET", "PAGEIOLATCH_SH", "LCK_M_X", "WRITELOG", "ASYNC_NETWORK_IO", "SOS_SCHEDULER_YIELD", "THREADPOOL", "W08", "W09", "W10" };

    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-waits-harness.mjs"));
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
                Assert.Fail("the waits harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the waits harness failed for scenario " + scenario + ": " + error.Result);
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
    public void TheDefaultSet_IsTheDesktopsTopWaits_BoundedByTheCap_AndEachDrawsOneRead()
    {
        var r = Run("defaults");
        Assert.Equal(FirstTen, Strings(r.GetProperty("checked")).ToArray());
        Assert.Equal(FirstTen, Strings(r.GetProperty("reads")));
        Assert.Equal(FirstTen, Strings(r.GetProperty("chart").GetProperty("labels")));
        Assert.Equal("10 / 10 selected", r.GetProperty("count").GetString());
        Assert.Equal("wait-trend|SRV1|wait_time_ms_per_second", r.GetProperty("chart").GetProperty("id").GetString());
    }

    [Fact]
    public void CheckingAndUnchecking_RedrawsTheChartFromOneReadPerCheckedWait()
    {
        var r = Run("checkUncheck");
        Assert.DoesNotContain("CXPACKET", Strings(r.GetProperty("afterUncheck")));
        Assert.Equal(9, Strings(r.GetProperty("readsAfterUncheck")).Length);
        Assert.DoesNotContain("CXPACKET", Strings(r.GetProperty("readsAfterUncheck")));
        Assert.Contains("W11", Strings(r.GetProperty("afterCheck")));
        Assert.Equal(Strings(r.GetProperty("afterCheck")), Strings(r.GetProperty("readsAfterCheck")));
        Assert.Equal(Strings(r.GetProperty("afterCheck")), Strings(r.GetProperty("chart").GetProperty("labels")));
    }

    [Fact]
    public void Search_NarrowsTheList_AndKeepsTheChecks()
    {
        var r = Run("search");
        Assert.Equal(new[] { "LCK_M_X" }, Strings(r.GetProperty("found")));
        Assert.Empty(Strings(r.GetProperty("none")));
        Assert.Single(Strings(r.GetProperty("noneText")));
        Assert.Equal(14, r.GetProperty("all").GetInt32());
    }

    [Fact]
    public void TheCap_DisablesTheEleventhCheckbox_WithAHint_AndClearAllEmptiesTheChart()
    {
        var r = Run("cap");
        Assert.Empty(Strings(r.GetProperty("afterClear")));
        Assert.Equal(10, r.GetProperty("checked").GetArrayLength());
        Assert.Equal(4, r.GetProperty("disabled").GetInt32());
        Assert.Contains("Limit reached", Strings(r.GetProperty("hint")).Single());
        Assert.True(r.GetProperty("emptyChart").GetInt32() >= 1);
        Assert.Equal(FirstTen, Strings(r.GetProperty("top")));
    }

    [Fact]
    public void TheChoices_SurviveARebuildForTheSameServer_AndStartFreshForAnother()
    {
        var r = Run("survives");
        Assert.Equal(new[] { "WRITELOG" }, Strings(r.GetProperty("rebuilt")));
        Assert.Equal("WRITE", r.GetProperty("search").GetString());
        Assert.Equal("signal_wait_time_ms_per_second", r.GetProperty("metric").GetString());
        Assert.Equal(FirstTen, Strings(r.GetProperty("other")));
        Assert.Equal("wait_time_ms_per_second", r.GetProperty("otherMetric").GetString());
    }

    [Fact]
    public void OneFailedSeries_GetsANote_AndTheRestStillChart()
    {
        var r = Run("oneFailed");
        var labels = Strings(r.GetProperty("chart").GetProperty("labels"));
        Assert.Equal(9, labels.Length);
        Assert.DoesNotContain("WRITELOG", labels);
        Assert.Contains(Strings(r.GetProperty("notes")), n => n.StartsWith("WRITELOG:"));
        Assert.Empty(r.GetProperty("errors").EnumerateArray());
    }

    [Fact]
    public void WhenEverySeriesFails_TheReaderSeesAnErrorAndEachNote_NotABrokenChart()
    {
        var r = Run("allFailed");
        Assert.Equal(JsonValueKind.Null, r.GetProperty("chart").ValueKind);
        Assert.Equal(10, Strings(r.GetProperty("notes")).Length);
        Assert.Single(Strings(r.GetProperty("errors")));
    }

    [Fact]
    public void TheMultiSeriesChart_IsZoomable()
    {
        var r = Run("zoom");
        Assert.Equal(10, r.GetProperty("series").GetInt32());
        Assert.True(r.GetProperty("zoomed").GetBoolean());
        Assert.Equal(1, r.GetProperty("wrapped").GetInt32());
        Assert.Equal("wait-trend|SRV1|wait_time_ms_per_second", r.GetProperty("id").GetString());
    }

    [Fact]
    public void WhenEveryReadIsEmpty_AndNoneFailed_TheChartShowsTheEmptyState_NotAnError()
    {
        var r = Run("allEmpty");
        Assert.Equal(JsonValueKind.Null, r.GetProperty("chart").ValueKind);
        Assert.Empty(r.GetProperty("errors").EnumerateArray());
        Assert.Single(Strings(r.GetProperty("empties")), e => e.Contains("trend data"));
    }

    [Fact]
    public void FastClicks_AbortTheSupersededReads_SoAtMostOneDrawsReadsAreInFlight()
    {
        var r = Run("fastClicks");
        Assert.Equal(40, r.GetProperty("sent").GetInt32());
        Assert.True(r.GetProperty("maxInFlight").GetInt32() <= 10, "more than ten reads were in flight at once");
        Assert.Equal(10, r.GetProperty("stillInFlight").GetInt32());
        Assert.Equal(30, r.GetProperty("aborted").GetInt32());
        Assert.Equal(10, r.GetProperty("chart").GetProperty("labels").GetArrayLength());
    }

    [Fact]
    public void ARebuild_PutsTheFocusAndCaretBackInTheSearchBox_OnlyWhenItHadThem()
    {
        var r = Run("focusKept");
        Assert.False(r.GetProperty("sameNode").GetBoolean());
        Assert.True(r.GetProperty("focused").GetBoolean());
        Assert.Equal(1, r.GetProperty("start").GetInt32());
        Assert.Equal(2, r.GetProperty("end").GetInt32());
        Assert.Equal("WRI", r.GetProperty("value").GetString());
        Assert.False(Run("noFocusStolen").GetProperty("focused").GetBoolean());
    }

    [Fact]
    public void TheChart_NeverDrawsMoreThanTheCap_EvenWhenHandedMore()
    {
        Assert.Equal(10, Run("capInDraw").GetProperty("reads").GetInt32());
    }

    [Fact]
    public void ACheckedWait_OutsideThisPollsList_StaysCheckedListedAndRead_AndIsStillThereWhenItReturns()
    {
        var r = Run("droppedOut");
        Assert.Equal(new[] { "W13" }, Strings(r.GetProperty("checked")));
        Assert.Equal(new[] { "W13" }, Strings(r.GetProperty("listedLast")));
        Assert.Equal(new[] { "W13" }, Strings(r.GetProperty("reads")));
        Assert.Equal(new[] { "W13" }, Strings(r.GetProperty("checkedBack")));
    }

    [Fact]
    public void TheWaitsPanel_ReadsGetWaitTrend_OncePerWait_WithTheWaitTypeAndHours_AndImportsThePickerOnlyHere()
    {
        var tabs = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");
        Assert.Contains("readToolWithinKeptHistory(\"get_wait_trend\", { server, wait_type: waitType, hours: ctx.hours }, mine.signal)", tabs);
        Assert.Contains("waitTypes.map(", tabs);
        Assert.Contains("const MAX_WAITS_CHARTED = 10;", tabs);
        Assert.Contains("from \"../multi-picker.js\"", tabs);
        var panels = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "panels.js");
        Assert.DoesNotContain("multi-picker", panels);
    }
}
