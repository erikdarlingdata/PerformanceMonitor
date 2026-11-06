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
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The web's session probe (#7928): the shipped <c>views-api.js</c> <c>getSession()</c>, with the real <c>apiGet</c>, run under
/// Node (<c>session-retry-harness.mjs</c>) against a stand-in <c>/api/session</c>. A failed read must not stay the page's answer
/// for the life of the tab: the next call after a short wait asks again, a good answer is kept, and a burst of callers shares one
/// read. Skipped when Node is not installed.
/// </summary>
public sealed class SessionRetryWebTests
{
    private static bool TryRun(string scenario, out JsonElement result)
    {
        result = default;
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "session-retry-harness.mjs"));
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
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the session retry harness did not finish in 30 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the session retry harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            result = doc.RootElement.Clone();
            return true;
        }
    }

    [Fact]
    public void AFailedSessionRead_IsAskedAgainAfterTheWait_AndTheGoodAnswerWins()
    {
        if (!TryRun("recovers", out var r)) return;

        Assert.True(r.GetProperty("first").GetProperty("probe_failed").GetBoolean());
        Assert.False(r.GetProperty("first").GetProperty("can_edit").GetBoolean());
        Assert.True(r.GetProperty("second").GetProperty("can_edit").GetBoolean());
        Assert.False(r.GetProperty("second").TryGetProperty("probe_failed", out _));
        Assert.True(r.GetProperty("peekAfterSuccess").GetProperty("can_edit").GetBoolean());
        Assert.Equal(2, r.GetProperty("calls").GetInt32());
    }

    [Fact]
    public void AnHttpFailureOnTheSessionRead_IsAskedAgainToo_AndAReadOnlySeatIsNotMarkedFailed()
    {
        if (!TryRun("failed-status-recovers", out var r)) return;

        Assert.True(r.GetProperty("first").GetProperty("probe_failed").GetBoolean());
        Assert.False(r.GetProperty("second").GetProperty("can_edit").GetBoolean());
        Assert.False(r.GetProperty("second").TryGetProperty("probe_failed", out _));
        Assert.Equal(2, r.GetProperty("calls").GetInt32());
    }

    [Fact]
    public void AGoodSessionAnswer_IsReadOnce_AndKeptForThePage()
    {
        if (!TryRun("good-answer-kept", out var r)) return;

        Assert.True(r.GetProperty("second").GetProperty("can_edit").GetBoolean());
        Assert.Equal(1, r.GetProperty("calls").GetInt32());
    }

    [Fact]
    public void ASessionServiceThatStaysDown_IsAskedOncePerBurstAndOncePerWait_NotOnEveryCall()
    {
        if (!TryRun("no-storm", out var r)) return;

        Assert.Equal(1, r.GetProperty("callsAfterBurst").GetInt32());
        Assert.Equal(1, r.GetProperty("callsInsideWait").GetInt32());
        Assert.Equal(2, r.GetProperty("callsAfterWait").GetInt32());
    }

    [Fact]
    public void WhenTheFirstSessionReadFailedBehindAFrontDoor_TheFrontDoorStartRunsOnceOnTheFirstGoodAnswer()
    {
        if (!TryRun("front-door-late", out var r)) return;

        // Until a good answer arrives nothing is started and edit controls stay hidden (a failed answer says can_edit: false).
        var start = r.GetProperty("afterStart");
        Assert.False(start.GetProperty("frontDoor").GetBoolean());
        Assert.Equal(0, start.GetProperty("storeListReads").GetInt32());
        Assert.Equal(0, start.GetProperty("pickerParts").GetInt32());
        Assert.Equal(0, start.GetProperty("started").GetArrayLength());
        Assert.False(r.GetProperty("peekAtStart").GetProperty("can_edit").GetBoolean());

        // The retried answer is the front door's: the store list is read, the store in use is set, and the start is reported once.
        var after = r.GetProperty("afterRecovery");
        Assert.True(r.GetProperty("retried").GetProperty("front_door").GetBoolean());
        Assert.True(after.GetProperty("frontDoor").GetBoolean());
        Assert.Equal("alpha", after.GetProperty("store").GetString());
        Assert.Equal(1, after.GetProperty("storeListReads").GetInt32());
        Assert.True(after.GetProperty("pickerParts").GetInt32() > 0);
        Assert.Equal(1, after.GetProperty("started").GetArrayLength());
        Assert.True(after.GetProperty("started")[0].GetBoolean());

        // The store's own flags replace the front door's fixed false ones on the session the pages read.
        var session = r.GetProperty("sessionAfter");
        Assert.True(session.GetProperty("admin_writes_off").GetBoolean());
        Assert.True(session.GetProperty("managed_store").GetBoolean());
    }

    [Fact]
    public void WhenTheRetriedSessionAnswerIsNotAFrontDoor_NothingIsStarted()
    {
        if (!TryRun("front-door-late-not-a-front-door", out var r)) return;

        var after = r.GetProperty("afterRecovery");
        Assert.True(r.GetProperty("retried").GetProperty("can_edit").GetBoolean());
        Assert.False(after.GetProperty("frontDoor").GetBoolean());
        Assert.Equal(0, after.GetProperty("storeListReads").GetInt32());
        Assert.Equal(0, after.GetProperty("pickerParts").GetInt32());
        Assert.Equal(0, after.GetProperty("started").GetArrayLength());
    }

    [Fact]
    public void WhenTheFirstSessionReadIsGoodBehindAFrontDoor_TheFrontDoorStartRunsAtOnce()
    {
        if (!TryRun("front-door-first-good", out var r)) return;

        var after = r.GetProperty("afterRecovery");
        Assert.True(after.GetProperty("frontDoor").GetBoolean());
        Assert.Equal("alpha", after.GetProperty("store").GetString());
        Assert.Equal(1, after.GetProperty("storeListReads").GetInt32());
        Assert.True(after.GetProperty("pickerParts").GetInt32() > 0);
        Assert.Equal(1, after.GetProperty("started").GetArrayLength());
        Assert.False(after.GetProperty("started")[0].GetBoolean());
        Assert.True(r.GetProperty("sessionAfter").GetProperty("admin_writes_off").GetBoolean());
    }
}
