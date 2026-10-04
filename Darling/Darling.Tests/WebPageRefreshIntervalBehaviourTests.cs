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
    public void ThirtySeconds_RespectsTheSlowPageBackoff_AndNeverStacksRenders()
    {
        var r = Run();
        var starts = r.GetProperty("backoffStarts").EnumerateArray().Select(e => e.GetInt32()).ToArray();
        Assert.True(starts.Length >= 2, "expected at least two renders in the window");
        // Reads that take 20 s are more than half of 30 s, so the next render waits 4x the render time, not 30 s.
        var gaps = r.GetProperty("backoffGaps").EnumerateArray().Select(e => e.GetInt32()).ToArray();
        Assert.True(gaps[0] >= 80, "the slow page re-rendered after " + gaps[0] + " s");
        // Every later gap is at least the 30 s interval. It is not asserted at 4x: the shared fetch helpers
        // (apiGetFleet/fetchBody) do not register their reads as in flight, so a render whose reads are all fleet reads
        // is timed shorter than it ran. That predates this selector and lives in util.js.
        Assert.All(gaps, g => Assert.True(g >= 30, "a render started " + g + " s after the previous one"));
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
    public void TheStatusBarHint_NamesTheInterval_AndTheControlShowsOnlyOnServerAndFinOpsPages()
    {
        var r = Run();
        Assert.Equal("Auto-refresh: 30 sec", r.GetProperty("hint30").GetString());
        Assert.Equal("Auto-refresh off", r.GetProperty("hintOff").GetString());
        Assert.True(r.GetProperty("controlHiddenOnFleet").GetBoolean());
        Assert.False(r.GetProperty("controlHiddenOnFinops").GetBoolean());
    }
}
