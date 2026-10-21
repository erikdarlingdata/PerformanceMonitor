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
/// The Admin "Server actions" tab's switch buttons (#6310) and the Agent changes tab's Kind words, run under Node
/// (<c>web-server-actions-harness.mjs</c>) on a fake DOM and a scripted fetch. Node is skipped when it is not installed.
/// </summary>
public sealed class WebServerActionsBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-server-actions-harness.mjs"));
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
                Assert.Fail("the server actions harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the server actions harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    /// <summary>
    /// A click on a switch while another change was in flight hit <c>if (busy) return;</c> and vanished with no message (4 of 5 back-to-back
    /// cycles in a live check). The switches now lock and a Saving line shows until the answer is in, so no click is taken and dropped.
    /// </summary>
    [Fact]
    public void WhileASwitchChangeIsInFlight_TheSwitchesAreLockedAndSavingShows_AndTheyFreeUpAfterTheAnswer()
    {
        var r = Run("clickWhileSaving");

        Assert.Equal(1, r.GetProperty("putsAfterFirst").GetInt32());
        Assert.True(r.GetProperty("disabledWhileSaving").GetBoolean());
        Assert.True(r.GetProperty("savingLine").GetBoolean());
        // A click on a locked switch never reaches the page, so nothing is silently dropped.
        Assert.False(r.GetProperty("secondClickDelivered").GetBoolean());
        Assert.Equal(1, r.GetProperty("putsAfterSecond").GetInt32());
        Assert.True(r.GetProperty("enabledAfter").GetBoolean());
        Assert.False(r.GetProperty("savingLineAfter").GetBoolean());
        Assert.True(r.GetProperty("answerShown").GetBoolean());
        Assert.Equal(2, r.GetProperty("putsAfterRetry").GetInt32());
    }

    [Fact]
    public void TheTurnOnConfirmPanel_IsLockedToo_WhileAnotherChangeIsInFlight()
    {
        var r = Run("confirmWhileSaving");

        Assert.True(r.GetProperty("confirmShown").GetBoolean());
        Assert.True(r.GetProperty("turnItOnPresent").GetBoolean());
        Assert.Equal(1, r.GetProperty("putsAfterOff").GetInt32());
        Assert.False(r.GetProperty("turnItOnDelivered").GetBoolean());
        Assert.Equal(1, r.GetProperty("putsAfterConfirm").GetInt32());
    }

    /// <summary>#6310: the kill row read the raw <c>session_kill</c>; the Viewer says "kill session".</summary>
    [Fact]
    public void TheAgentChangesKindColumn_WordsASessionKillAsTheViewerDoes()
    {
        var kinds = Run("agentKinds").GetProperty("kinds").EnumerateArray().Select(e => e.GetString()).ToArray();

        Assert.Equal(new[] { "force plan", "unforce plan", "evict plan", "kill session", "something_new" }, kinds);
    }
}
