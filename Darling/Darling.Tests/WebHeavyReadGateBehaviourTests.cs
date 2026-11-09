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
/// #8452: the web read layer under a Queries tab load, run in Node against a recording fetch whose answers land only when the
/// harness says so. The tab's four Query Store reads take seconds each, and sent together they held four of the browser's
/// connections to the origin while the page's own reads (store size, alert badge) and the tab's small reads queued behind them.
/// At most two of the heavy reads are in flight per page; every other read is sent at once; a finished heavy read hands its slot
/// to the next one waiting, in the order asked; a heavy read whose caller left while it waited is never sent; and two callers of
/// one heavy read share one request and one slot. No clock is read on either side. Behavior only; the layer's source is not
/// pinned.
/// </summary>
public sealed class WebHeavyReadGateBehaviourTests
{
    private static readonly string[] Heavy = ["get_query_store_clutter", "get_query_store_regressions", "get_query_heatmap", "get_plan_corrections"];
    private static readonly string[] Others = ["get_store_host", "get_alert_history", "get_query_store_top", "get_top_queries_by_cpu", "get_active_queries", "get_long_query_completions"];

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-heavy-read-gate-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
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
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the heavy read gate harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the heavy read gate harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    [Fact]
    public void TheTabsLoad_SendsEveryPageAndSmallReadAtOnce_AndAtMostTwoHeavyReads()
    {
        var load = Run().GetProperty("load");
        Assert.Equal(Others, Strings(load.GetProperty("otherSent")));
        Assert.Equal(new[] { Heavy[0], Heavy[1] }, Strings(load.GetProperty("heavySent")));
        Assert.All(Strings(load.GetProperty("kinds")), kind => Assert.Equal("data", kind));
    }

    [Fact]
    public void AFinishedHeavyRead_HandsItsSlotToTheNextOneWaiting_InTheOrderAsked()
    {
        var root = Run();
        var afterFirst = Strings(root.GetProperty("afterFirstLands").GetProperty("sent"));
        Assert.Contains(Heavy[2], afterFirst);
        Assert.DoesNotContain(Heavy[3], afterFirst);
        var afterSecond = Strings(root.GetProperty("afterSecondLands").GetProperty("sent"));
        Assert.Contains(Heavy[3], afterSecond);
    }

    [Fact]
    public void AHeavyReadWhoseCallerLeftWhileItWaited_IsNeverSent_AndTheNextOneTakesTheSlot()
    {
        var leaver = Run().GetProperty("leaver");
        Assert.Equal(new[] { Heavy[0], Heavy[1] }, Strings(leaver.GetProperty("sentBeforeAnyLands")));
        Assert.Equal("aborted", leaver.GetProperty("leftKind").GetString());
        var afterFirst = Strings(leaver.GetProperty("sentAfterFirstLands"));
        Assert.DoesNotContain(Heavy[2], afterFirst);
        Assert.Contains(Heavy[3], afterFirst);
    }

    [Fact]
    public void TwoCallersOfOneHeavyRead_ShareOneRequestAndOneSlot()
    {
        var joined = Run().GetProperty("joined");
        Assert.Equal(new[] { Heavy[0], Heavy[1] }, Strings(joined.GetProperty("sent")));
    }
}
