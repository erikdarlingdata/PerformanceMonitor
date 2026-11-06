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
/// The web Alert Rules list under its 60 s poll (#7927), run in Node against a recording fetch: a poll over an unchanged list
/// costs the list and the templates reads only, an open "New from template" menu stays open through it, a rule's definition is
/// read again only when its version changed, and a failed poll keeps the list with a notice. Behavior only; the spelling of the
/// page's source is not pinned.
/// </summary>
public sealed class AlertRulesPollBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "alert-rules-poll-harness.mjs"));
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
                Assert.Fail("the alert rules poll harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the alert rules poll harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    [Fact]
    public void TheFirstPaint_DrawsTheCards_AndReadsEveryRuleOnce()
    {
        var first = Run().GetProperty("first");
        Assert.Equal(new[] { "CPU hot", "PLE low", "Blocked sessions" }, Strings(first.GetProperty("names")));
        Assert.Equal(
            new[] { "max cpu_utilization | ≥ 80 / crit 95 | All servers", "page_life_expectancy | < 300 | 3 servers", "max blocked_sessions | outside 10–100 | 1 server" },
            Strings(first.GetProperty("chips")));
        Assert.Equal(
            new[] { "/api/alert-templates", "/api/alerts", "/api/alerts/1", "/api/alerts/2", "/api/alerts/3", "/api/session" },
            Strings(first.GetProperty("requests")));
        Assert.Equal(new[] { "High CPUCPU over 80% for five minutes" }, Strings(first.GetProperty("templateItems")));
    }

    [Fact]
    public void ThePoll_OverAnUnchangedList_MakesTwoRequests_AndKeepsTheOpenTemplateMenu()
    {
        var poll = Run().GetProperty("unchangedPoll");
        Assert.Equal(new[] { "/api/alert-templates", "/api/alerts" }, Strings(poll.GetProperty("requests")));
        Assert.False(poll.GetProperty("loadingDuringPoll").GetBoolean());
        Assert.True(poll.GetProperty("menuKept").GetBoolean());
        Assert.Equal(new[] { "CPU hot", "PLE low", "Blocked sessions" }, Strings(poll.GetProperty("names")));
        Assert.Equal(
            new[] { "max cpu_utilization | ≥ 80 / crit 95 | All servers", "page_life_expectancy | < 300 | 3 servers", "max blocked_sessions | outside 10–100 | 1 server" },
            Strings(poll.GetProperty("chips")));
        Assert.Equal(0, poll.GetProperty("refreshError").GetInt32());
    }

    [Fact]
    public void ThePoll_ReadsARuleAgain_OnlyWhenItsVersionChanged()
    {
        var poll = Run().GetProperty("changedPoll");
        Assert.Equal(new[] { "/api/alert-templates", "/api/alerts", "/api/alerts/3" }, Strings(poll.GetProperty("requests")));
        Assert.Equal(new[] { "CPU hot", "PLE low, renamed", "Blocked sessions" }, Strings(poll.GetProperty("names")));
        Assert.Equal(
            new[] { "max cpu_utilization | ≥ 80 / crit 95 | All servers", "page_life_expectancy | < 300 | 3 servers", "max blocked_sessions | > 5 | All servers" },
            Strings(poll.GetProperty("chips")));
        Assert.True(poll.GetProperty("menuKept").GetBoolean());
    }

    [Fact]
    public void AFailedPoll_KeepsTheListAndTheMenu_AndSaysSo_UntilTheNextPollSucceeds()
    {
        var run = Run();
        var failed = run.GetProperty("failedPoll");
        Assert.Equal(new[] { "CPU hot", "PLE low, renamed", "Blocked sessions" }, Strings(failed.GetProperty("names")));
        Assert.True(failed.GetProperty("menuKept").GetBoolean());
        var notice = Assert.Single(Strings(failed.GetProperty("refreshError")));
        Assert.Contains("Could not refresh", notice);
        Assert.Contains("the store is down", notice);
        Assert.Equal(new[] { "/api/alert-templates", "/api/alerts" }, Strings(failed.GetProperty("requests")));
        var recovered = run.GetProperty("recoveredPoll");
        Assert.Equal(0, recovered.GetProperty("refreshError").GetInt32());
        Assert.Equal(new[] { "CPU hot", "PLE low, renamed", "Blocked sessions" }, Strings(recovered.GetProperty("names")));
    }
}
