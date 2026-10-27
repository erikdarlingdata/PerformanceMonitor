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
/// #6793: leaving a web page cancels that page's reads, and a read left behind never holds the next page's poll, Refresh
/// button, tab-return refresh or render-time back-off. These run the shipped <c>app.js</c>, <c>util.js</c> and the Job History,
/// Query Tag Usage, Server and FinOps pages and the view and notebook pages under Node (<c>web-route-change-abort-harness.mjs</c>) on a virtual clock and a fetch that
/// records every request with its AbortSignal. The slow page's read stays out for 140 s of virtual time, the length measured
/// on a production store. Node is skipped when it is not installed.
/// </summary>
[Trait("Cost", "Slow")]
public sealed class WebRouteChangeAbortBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-route-change-abort-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page scripts cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the route change harness did not finish in 60 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the route change harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void LeavingAPage_AbortsItsRead_AndTheNextPagesRefreshButtonWorks()
    {
        var r = Run("leaveSlowPage");
        Assert.False(r.GetProperty("before").GetProperty("aborted").GetBoolean());
        Assert.True(r.GetProperty("afterLeave").GetProperty("aborted").GetBoolean());
        Assert.False(r.GetProperty("busyAfterSettle").GetBoolean());
        Assert.Equal(1, r.GetProperty("refreshClickReads").GetInt32());
    }

    [Fact]
    public void ThePollOfThePageMovedTo_RunsWhileTheOldReadWouldStillBeOut()
    {
        var r = Run("pollAfterLeaving");
        Assert.Equal(1, r.GetProperty("pollReads").GetInt32());
    }

    [Fact]
    public void ReturningToTheTab_RefreshesThePageMovedTo()
    {
        var r = Run("tabReturnAfterLeaving");
        Assert.Equal(1, r.GetProperty("returnReads").GetInt32());
    }

    [Fact]
    public void TheBackOffAfterLeaving_UsesTheNewPagesRenderTime_NotTheOldRead()
    {
        var r = Run("backOffAfterLeaving");
        Assert.Equal("Auto-refresh: 1 min", r.GetProperty("hint").GetString());
    }

    [Fact]
    public void QueryTagUsage_IsNotReRenderedByThePoll()
    {
        var r = Run("tagUsagePoll");
        Assert.Equal(1, r.GetProperty("first").GetInt32());
        Assert.Equal(1, r.GetProperty("afterPoll").GetInt32());
        Assert.Equal("Auto-refresh off", r.GetProperty("hint").GetString());
    }

    [Fact]
    public void QueryTagUsage_ServerPicker_StillReReads_AndAbortsTheOlderRead()
    {
        var r = Run("tagUsagePickerStillReads");
        Assert.Equal(2, r.GetProperty("reads").GetInt32());
        Assert.True(r.GetProperty("firstAborted").GetBoolean());
    }

    [Fact]
    public void ASamePageReload_StillAbortsItsOlderLoad()
    {
        var r = Run("sameRouteLoadAbortsOlderLoad");
        Assert.True(r.GetProperty("olderAborted").GetBoolean());
        Assert.Equal(2, r.GetProperty("reads").GetInt32());
    }

    [Fact]
    public void LeavingTheServerPage_ForASavedView_SendsTheViewsPanelReads_NotAborted()
    {
        var r = Run("leaveServerForView");
        Assert.Equal(1, r.GetProperty("reads").GetInt32());
        Assert.False(r.GetProperty("abortedAtStart").GetBoolean());
        Assert.False(r.GetProperty("aborted").GetBoolean());
    }

    [Fact]
    public void LeavingTheServerPage_ForANotebook_SendsTheCellReads_NotAborted()
    {
        var r = Run("leaveServerForNotebook");
        Assert.Equal(1, r.GetProperty("reads").GetInt32());
        Assert.False(r.GetProperty("abortedAtStart").GetBoolean());
        Assert.False(r.GetProperty("aborted").GetBoolean());
    }

    [Fact]
    public void LeavingFinOps_WhileItsServerListIsOut_KeepsItsCache_AndPaintsFromItOnReturn()
    {
        var r = Run("finopsLeaveAndReturn");
        Assert.True(r.GetProperty("pollOut").GetBoolean());
        Assert.True(r.GetProperty("pollAborted").GetBoolean());
        Assert.Equal(0, r.GetProperty("readsOnReturn").GetInt32());
        Assert.False(r.GetProperty("loadingStrip").GetBoolean());
    }

    [Fact]
    public void AFleetReadThatLandsAfterLeavingTheServerPage_StartsNoPanelReads_AndBlocksNothing()
    {
        var r = Run("fleetReadLandsAfterLeaving");
        Assert.Equal(0, r.GetProperty("panelReadsStartedAfterLeave").GetInt32());
        Assert.False(r.GetProperty("refreshDisabled").GetBoolean());
        Assert.Equal(1, r.GetProperty("pollReads").GetInt32());
    }

    [Fact]
    public void LeavingTheServerPage_AbortsTheTrendReadsOfItsHandBuiltChartPanels()
    {
        var r = Run("leaveServerWithTrendReadOut");
        var before = r.GetProperty("before");
        Assert.True(before.GetProperty("reads").GetInt32() > 0);
        Assert.False(before.GetProperty("abortedAtStart").GetBoolean());
        Assert.False(before.GetProperty("aborted").GetBoolean());
        Assert.True(r.GetProperty("allAborted").GetBoolean());
    }

    [Fact]
    public void LeavingTheServerPage_AbortsTheFanoutReads_AndDrawsNoErrorStrip()
    {
        var r = Run("leaveServerWithFanoutReadOut");
        var before = r.GetProperty("before");
        Assert.True(before.GetProperty("reads").GetInt32() > 0);
        Assert.False(before.GetProperty("aborted").GetBoolean());
        Assert.True(r.GetProperty("allAborted").GetBoolean());
        Assert.Equal(0, r.GetProperty("errorStripsDrawn").GetInt32());
    }

    [Fact]
    public void LeavingTheHealthTab_AbortsTheCalendarMonthRead_AndReturningReadsTheMonthAgain()
    {
        var r = Run("leaveHealthWithCalendarReadOut");
        var before = r.GetProperty("before");
        Assert.Equal(2, before.GetProperty("reads").GetInt32());
        Assert.False(before.GetProperty("aborted").GetBoolean());
        Assert.True(r.GetProperty("afterLeave").GetProperty("monthReadAborted").GetBoolean());
        // The tab's own range read and the calendar's month read both start again: the aborted month was not cached as empty.
        Assert.Equal(2, r.GetProperty("monthReadsAfterReturn").GetInt32());
    }

    [Fact]
    public void LeavingFinops_AbortsTheDatabaseBoxNameRead()
    {
        var r = Run("finopsDatabaseBoxReadOut");
        var before = r.GetProperty("before");
        Assert.True(before.GetProperty("reads").GetInt32() > 0);
        Assert.False(before.GetProperty("aborted").GetBoolean());
        Assert.True(r.GetProperty("allAborted").GetBoolean());
    }

    [Fact]
    public void QueryTagUsage_FooterTime_MovesWhenItsReadLands_NotAtRenderStart()
    {
        var r = Run("tagUsageFooterStamp");
        Assert.False(r.GetProperty("stampedAtStart").GetBoolean());
        Assert.True(r.GetProperty("stampedWhenReadLanded").GetBoolean());
    }
}
