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
/// The server and FinOps pages carry an auto-refresh interval selector (30 sec / 1 min / 5 min / Off) and a Refresh
/// button, matching the desktop Viewer's choices. These run the shipped <c>app.js</c> scheduler under Node
/// (<c>web-page-refresh-harness.mjs</c>) with a controllable clock and a counting fetch. Node is skipped when it is
/// not installed.
/// </summary>
public sealed class WebPageRefreshIntervalBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-page-refresh-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped scheduler cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the page refresh harness did not finish in 60 s");
            }

            Assert.True(proc.ExitCode == 0, "the page refresh harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void TheSelector_OffersThirtySecondsOneMinuteFiveMinutesAndOff_WithOneMinuteSelectedByDefault()
    {
        var r = Run();
        Assert.Equal(new[] { "30s=30 sec", "1m=1 min", "5m=5 min", "off=Off" },
            r.GetProperty("options").EnumerateArray().Select(e => e.GetString()!).ToArray());
        Assert.Equal("1m", r.GetProperty("selectedByDefault").GetString());
    }

    [Fact]
    public void EachInterval_ReRendersThePage_AfterItsOwnSpan()
    {
        var r = Run();
        var s30 = r.GetProperty("next30").GetInt32();
        var s1m = r.GetProperty("next1m").GetInt32();
        var s5m = r.GetProperty("next5m").GetInt32();
        Assert.InRange(s30, 30, 35);
        Assert.InRange(s1m, 60, 65);
        Assert.InRange(s5m, 300, 305);
    }

    [Fact]
    public void Off_FiresNoPageReads_ForFifteenMinutes_ButTheShellReadsContinue()
    {
        var r = Run();
        Assert.Equal(0, r.GetProperty("offPageReads").GetInt32());
        // 900 s at the 60 s shell roll-up: sidebar, view list and AG nav keep running, so a lapsed session still shows.
        Assert.True(r.GetProperty("offShellReads").GetInt32() >= 15, "shell reads stopped under Off");
    }

    [Fact]
    public void TheRefreshButton_IgnoresClicksWhileReadsRun_AndShowsItIsBusy()
    {
        var r = Run();
        Assert.Equal(1, r.GetProperty("doubleClickViews").GetInt32());
        Assert.Equal(1, r.GetProperty("doubleClickRenders").GetInt32());
        Assert.True(r.GetProperty("busyWhileReading").GetBoolean());
        Assert.True(r.GetProperty("idleAfterSettle").GetBoolean());
    }

    [Fact]
    public void AnIntervalSavedInAnotherTab_AppliesHere()
    {
        var r = Run();
        Assert.Equal("off", r.GetProperty("storageSelectValue").GetString());
        Assert.Equal(0, r.GetProperty("storageOffRenders").GetInt32());
        Assert.Equal("Auto-refresh off", r.GetProperty("storageHint").GetString());
    }

    [Fact]
    public void TheRefreshButton_ReRendersThePageOnce_EvenWhenAutoRefreshIsOff()
    {
        var r = Run();
        Assert.Equal(1, r.GetProperty("refreshClickReads").GetInt32());
        Assert.Equal(0, r.GetProperty("refreshAfterOffReads").GetInt32());
    }

    [Fact]
    public void OnAFixedCustomRange_ThePollReadsNothing_AndTheRefreshButtonReReadsThePage()
    {
        var r = Run();
        Assert.True(r.GetProperty("fixedRangeApplyError").ValueKind == JsonValueKind.Null, "the fixed range was not taken");
        Assert.Equal(0, r.GetProperty("fixedRangePollReads").GetInt32());
        Assert.True(r.GetProperty("fixedRangeRefreshReads").GetInt32() > 0, "Refresh did not re-read the fixed range");
    }

    [Fact]
    public void ThirtySeconds_RespectsTheSlowPageBackoff_AndNeverStacksRenders()
    {
        var r = Run();
        var starts = r.GetProperty("backoffStarts").EnumerateArray().Select(e => e.GetInt32()).ToArray();
        Assert.True(starts.Length >= 2, "expected at least two renders in the window");
        // Reads that take 20 s are more than half of 30 s, so the next render waits 4x the render time, not 30 s.
        var gaps = r.GetProperty("backoffGaps").EnumerateArray().Select(e => e.GetInt32()).ToArray();
        Assert.True(gaps[0] >= 80, "the slow page re-rendered after " + gaps[0] + " s");
        // Every later gap is held to the same backoff: the fleet read counts as in flight, so the render is timed for
        // the whole 20 s its reads ran.
        Assert.All(gaps, g => Assert.True(g >= 80, "a render started " + g + " s after the previous one"));
        Assert.True(r.GetProperty("backoffHintSaysSlow").GetBoolean());
        // Reads that never finish inside the window: the one render only, no stacked second one.
        Assert.Equal(1, r.GetProperty("stackedRenders").GetInt32());
    }

    [Fact]
    public void TheChoice_IsPersistedInLocalStorage_AndRestoredByTheNextLoad()
    {
        var r = Run();
        Assert.Equal("30s", r.GetProperty("saved").GetString());
        Assert.Equal("30s", r.GetProperty("restoredValue").GetString());
    }

    [Fact]
    public void TheStatusBarHint_NamesTheInterval_AndRefreshShowsOnEveryPageWithTheIntervalOnServerAndFinOpsPagesOnly()
    {
        var r = Run();
        Assert.Equal("Auto-refresh: 30 sec", r.GetProperty("hint30").GetString());
        Assert.Equal("Auto-refresh off", r.GetProperty("hintOff").GetString());
        // #7690: the row stays on every page so the sidebar list does not move and Refresh is always there; the interval is blanked, not removed.
        Assert.False(r.GetProperty("controlHiddenOnFleet").GetBoolean());
        Assert.False(r.GetProperty("controlHiddenOnFinops").GetBoolean());
        Assert.Equal("hidden", r.GetProperty("intervalVisibilityOnFleet").GetString());
        Assert.Equal("", r.GetProperty("intervalVisibilityOnFinops").GetString());
    }

    [Fact]
    public void ResumeAndTabReturn_ReRenderThePageInTheSamePassAsTheShell_SoOneFleetReadServesBoth()
    {
        // #7935: whether the page may render is decided before the shell's reads start, since those count as page reads too. The page then
        // renders at once and its /api/fleet joins the sidebar's (#3895), instead of waiting a tick and sending a second fleet roll-up.
        var r = Run();
        // Resume: the page renders in the same pass (its panel reads start as soon as the pass's fleet read answers, before any tick) and
        // the one fleet read serves the sidebar and the page.
        Assert.Equal(1, r.GetProperty("resumeFleetReads").GetInt32());
        Assert.Equal(1, r.GetProperty("resumeRendersBeforeTick").GetInt32());
        Assert.Equal(1, r.GetProperty("resumeRendersAfter").GetInt32());
        Assert.Equal(1, r.GetProperty("resumeFleetReadsAfter").GetInt32());
        // Tab return with the page due: the same, where a page decided after the shell's reads would render a tick later with its own fleet read.
        Assert.Equal(1, r.GetProperty("returnFleetReads").GetInt32());
        Assert.Equal(1, r.GetProperty("returnRendersBeforeTick").GetInt32());
        Assert.Equal(1, r.GetProperty("returnRendersAfter").GetInt32());
        Assert.Equal(1, r.GetProperty("returnFleetReadsAfter").GetInt32());
    }

    [Fact]
    public void ATickDueForThePageAndTheShell_RendersThePageOnceInTheSamePass_WithOneFleetRead()
    {
        // #7935: the scheduler tick decides whether the page may render before the shell's reads start. A tick that finds both due
        // renders the page in the same pass, so one /api/fleet serves the sidebar and the page, instead of the page waiting out the
        // shell's reads and sending its own fleet read a tick later.
        var r = Run();
        Assert.Equal(1, r.GetProperty("tickShellReads").GetInt32());
        Assert.Equal(1, r.GetProperty("tickFleetReads").GetInt32());
        Assert.Equal(1, r.GetProperty("tickRendersBeforeTick").GetInt32());
        Assert.Equal(1, r.GetProperty("tickFleetReadsAfter").GetInt32());
        // #4191: the ticks that land while that render's reads are still in flight do not start it again, so no read is sent twice.
        Assert.Equal(1, r.GetProperty("tickRendersAfter").GetInt32());
        Assert.Empty(r.GetProperty("tickRepeatedReads").EnumerateArray());
    }
}
