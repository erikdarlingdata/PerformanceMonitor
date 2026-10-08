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
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Release click-through findings on stale state and one slow read (#5489). The web half runs the shipped page
/// script under Node (<c>web-clickthrough-stale-harness.mjs</c>): the fleet card chips of a server that stopped
/// collecting, and the Wait Stats tab's read count. The Viewer half drives the WPF fleet card's own properties,
/// which are the twin of the web chips. Node is skipped when it is not installed.
/// </summary>
public sealed class WebClickthroughStaleStateTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-clickthrough-stale-harness.mjs"));
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
                Assert.Fail("the click-through harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the click-through harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            var result = doc.RootElement.Clone();
            Assert.Empty(result.GetProperty("rejections").EnumerateArray());
            return result;
        }
    }

    private static string[] Texts(JsonElement chips) =>
        chips.EnumerateArray().Select(c => c.GetProperty("text").GetString()!).ToArray();

    [Fact]
    public void AServerPastTheOfflineMark_ReadsNotAvailableWithItsAge_NotLiveLookingValues()
    {
        var r = Run("cards");
        var offline = r.GetProperty("offline");
        var texts = Texts(offline);

        /* CPU, Threads, Memory, Blocking and Deadlocks: n/a with the age, never 0% / 77 free / ok. */
        for (var i = 0; i < 5; i++)
        {
            Assert.Contains("n/a", texts[i], StringComparison.Ordinal);
            Assert.Contains("last collected 12d ago", texts[i], StringComparison.Ordinal);
            Assert.Contains("sev-Unknown", offline[i].GetProperty("cls").GetString(), StringComparison.Ordinal);
        }

        Assert.DoesNotContain(texts, t => t.Contains("0%", StringComparison.Ordinal) || t.Contains("free", StringComparison.Ordinal) || t.Contains("ok", StringComparison.Ordinal));
        Assert.StartsWith("CollectorsStale", texts[5], StringComparison.Ordinal);
    }

    [Fact]
    public void AnOnlineServer_KeepsItsLiveValues()
    {
        var online = Texts(Run("cards").GetProperty("online"));
        Assert.Equal("CPU0%SQL 0%", online[0]);
        Assert.StartsWith("Threads77 free", online[1], StringComparison.Ordinal);
        Assert.StartsWith("Memoryok", online[2], StringComparison.Ordinal);
    }

    [Fact]
    public void AStaleAvailabilityGroupCard_SaysNoCurrentData_AndAFreshOneDoesNot()
    {
        var r = Run("agStale");
        var stale = Texts(r.GetProperty("stale")).Single();
        Assert.StartsWith("Stale: no current data. ", stale, StringComparison.Ordinal);
        Assert.Contains("last collected", stale, StringComparison.Ordinal);
        var fresh = Texts(r.GetProperty("fresh")).Single();
        Assert.DoesNotContain("Stale", fresh, StringComparison.Ordinal);
        Assert.Contains("collected just now", fresh, StringComparison.Ordinal);
    }

    [Fact]
    public void TheWaitStatsTab_AsksForTheSpinlockAndLatchListsOnce_NotOncePerPanel()
    {
        /* The Spinlock Stats grid and the Spinlock Trend picker (and the latch pair) read the same tool with the
           same arguments; at 7 days on a large store the spinlock read took 3.0 s and 5.7 s, paid twice. */
        var r = Run("waits");
        Assert.Equal(1, r.GetProperty("spinlock").GetInt32());
        Assert.Equal(1, r.GetProperty("latch").GetInt32());
    }

    [Fact]
    public void ASharedRead_CancelsOnlyTheCallerThatAborted_AndALaterCallSendsAFreshRequest()
    {
        var r = Run("joinAbort");
        Assert.Equal("aborted", r.GetProperty("first").GetString());
        Assert.Equal("data", r.GetProperty("second").GetString());
        Assert.Equal(1, r.GetProperty("sentTogether").GetInt32());
        Assert.Equal("data", r.GetProperty("later").GetString());
        Assert.Equal(2, r.GetProperty("sent").GetInt32());
    }

    [Fact]
    public void TheViewerFleetCard_OfflineReadsDashesAndAge_NotLiveValues()
    {
        var card = new ServerSummaryItem
        {
            IsOnline = false,
            CpuPercent = 0,
            MemoryMb = 908,
            TotalThreads = 100,
            CurrentWorkers = 23,
            LastCollectionTime = DateTime.UtcNow.AddDays(-12),
        };

        Assert.Equal("--", card.CpuDisplay);
        Assert.Equal("--", card.MemoryDisplay);
        Assert.Equal("--", card.ThreadsDisplay);
        Assert.Equal("--", card.BlockingDisplay);
        Assert.Equal("--", card.DeadlockDisplay);
        Assert.StartsWith("last collected 1w ago", card.CpuDetail, StringComparison.Ordinal);
        Assert.Equal("", card.ThreadsDetail);

        var live = new ServerSummaryItem { IsOnline = true, CpuPercent = 0, MemoryMb = 908, TotalThreads = 100, CurrentWorkers = 23 };
        Assert.Equal("0%", live.CpuDisplay);
        Assert.Equal("0.9 GB", live.MemoryDisplay);
        Assert.Equal("OK", live.ThreadsDisplay);
    }
}
