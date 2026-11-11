/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Diagnostics;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #8632: a server's first Queries tab visit, run in Node against a stand-in browser with six connections to the origin
/// (HTTP/1.1), a fixed answer time per read and a virtual clock. Five of the tab's reads (the three duration trends and the two
/// top lists at detail=full) took 3 to 6 s each in the field and were sent at once, so they held five of the six connections and
/// the page's own ping, store host and alert badge waited 3.4 to 4.5 s for one although the service answered each in under 300 ms.
/// The harness gives the same visit the field's answer times; on the old code the ping waits 4.4 s. The five now share the heavy
/// read gate under one cap below six, so the page's reads wait well under half a second, and the tab still settles in about
/// 11.6 s where sending all nine reads through the old cap of two would take 19 s.
/// #8681: the tab's active-query snapshots and Query Store top list (3.9 and 3.3 s in the field) were not gated and held the two
/// connections the gate leaves free, so ping, store host and the badge still queued 1.1 to 3.3 s. The harness now makes all seven
/// reads slow: the page's reads must get a connection within a fraction of the shortest of them, and the seven share the cap of four.
/// Behavior only; the layer's source is not pinned.
/// </summary>
public sealed class WebQueriesPageReadsBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-queries-page-reads-harness.mjs"));
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
                Assert.Fail("the Queries page reads harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the Queries page reads harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    [Theory]
    [InlineData("pageReadsAfterTheTab")]
    [InlineData("pageReadsFirst")]
    public void ThePagesOwnReads_WaitUnderHalfASecondForAConnection_WhileTheSevenSlowReadsAreInFlight(string order)
    {
        var visit = Run().GetProperty(order);
        Assert.True(visit.GetProperty("pingQueuedMs").GetInt32() < 500, "ping queued " + visit.GetProperty("pingQueuedMs").GetInt32() + " ms");
        Assert.True(visit.GetProperty("storeHostQueuedMs").GetInt32() < 500, "store host queued " + visit.GetProperty("storeHostQueuedMs").GetInt32() + " ms");
        Assert.True(visit.GetProperty("badgeQueuedMs").GetInt32() < 500, "alert badge queued " + visit.GetProperty("badgeQueuedMs").GetInt32() + " ms");
    }

    [Fact]
    public void TheSlowReadsHoldFewerConnectionsThanTheBrowserHas_AndTheHeavyOnesStayAtTwo()
    {
        var visit = Run().GetProperty("pageReadsAfterTheTab");
        Assert.True(visit.GetProperty("maxGatedInFlight").GetInt32() <= 4);
        Assert.True(visit.GetProperty("maxHeavyInFlight").GetInt32() <= 2);
    }

    [Fact]
    public void TheTab_StillSettlesSoon_SoTheSevenAreNotJustPutBehindTheOldCapOfTwo()
    {
        var visit = Run().GetProperty("pageReadsAfterTheTab");
        var settles = visit.GetProperty("tabSettlesMs").GetInt32();
        Assert.True(settles < 16000, "the tab settled in " + settles + " ms");
    }
}
