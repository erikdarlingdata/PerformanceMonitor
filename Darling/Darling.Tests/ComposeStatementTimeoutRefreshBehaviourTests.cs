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
/// #9357: a Custom View cell the store stopped at its statement timeout is not sent again by the page's own auto-refresh. The re-send
/// came from the tick itself: <c>renderView</c> with <c>{ poll: true }</c> builds a new card for every cell, and each new card sends its
/// run. The cell is sent again after a click (its Retry button, the Refresh button, opening the page again) or a change of window. A
/// cell that failed another way and a cell that worked are refreshed as before. Run as the shipped page scripts under Node against a
/// scripted <c>/api/compose/run</c> (<c>compose-statement-timeout-harness.mjs</c>), for a saved dashboard and a saved notebook. Node is
/// skipped when it is not installed.
/// </summary>
public sealed class ComposeStatementTimeoutRefreshBehaviourTests
{
    private static readonly Lazy<JsonElement> Result = new(Run);

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "compose-statement-timeout-harness.mjs"));
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
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the statement timeout refresh harness did not finish in 60 s");
            }

            Assert.True(proc.ExitCode == 0, "the statement timeout refresh harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    /// <summary>The cells a render sent, by source, in a stable order (the page starts its reads two at a time).</summary>
    private static string Sent(JsonElement run, string name) =>
        string.Join(",", run.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).OrderBy(s => s, StringComparer.Ordinal));

    [Theory]
    [InlineData("dashboard")]
    [InlineData("notebook")]
    public void TheFirstOpen_SendsEveryCell_AndTheStoppedAndTheFailedCellEachOfferRetry(string kind)
    {
        var run = Result.Value.GetProperty(kind);
        Assert.Equal("s1,s2,s3", Sent(run, "open"));
        Assert.Equal(1, run.GetProperty("openTimeoutStrips").GetInt32());
        Assert.Equal(2, run.GetProperty("openRetryButtons").GetInt32());
    }

    [Theory]
    [InlineData("dashboard")]
    [InlineData("notebook")]
    public void TheAutoRefreshTick_DoesNotSendAStoppedCellAgain_ButRefreshesTheFailedAndTheGoodCell(string kind)
    {
        var run = Result.Value.GetProperty(kind);
        // s1 was stopped at the statement timeout, s2 failed another way, s3 worked.
        Assert.Equal("s2,s3", Sent(run, "tick1"));
        Assert.Equal("s2,s3", Sent(run, "tick2"));
        // The stopped cell still says so, with its Retry button, while the tick refreshes the others.
        Assert.Equal(1, run.GetProperty("tick1TimeoutStrips").GetInt32());
        Assert.Equal(2, run.GetProperty("tick1RetryButtons").GetInt32());
    }

    [Theory]
    [InlineData("dashboard")]
    [InlineData("notebook")]
    public void TheCellsRetryButton_SendsOnlyThatCell_AndTheNextTickLeavesItAloneWhenItIsStoppedAgain(string kind)
    {
        var run = Result.Value.GetProperty(kind);
        Assert.Equal("s1", Sent(run, "retryClick"));
        Assert.Equal("s2,s3", Sent(run, "afterRetryTick"));
    }

    [Theory]
    [InlineData("dashboard")]
    [InlineData("notebook")]
    public void TheRefreshButton_AndOpeningThePageAgain_SendTheStoppedCell(string kind)
    {
        var run = Result.Value.GetProperty(kind);
        Assert.Equal("s1,s2,s3", Sent(run, "manual"));
        Assert.Equal("s2,s3", Sent(run, "tick3"));
        Assert.Equal("s1,s2,s3", Sent(run, "reopen"));
    }

    [Theory]
    [InlineData("dashboard")]
    [InlineData("notebook")]
    public void ACellThatWorksAfterAClick_IsRefreshedByTheTickAgain(string kind)
    {
        var run = Result.Value.GetProperty(kind);
        // The store would answer it now, but the tick still leaves the stopped cell alone until the click.
        Assert.Equal("s2,s3", Sent(run, "tickWhileHeld"));
        Assert.Equal("s1", Sent(run, "retryWhenWorking"));
        Assert.Equal("s1,s2,s3", Sent(run, "tickAfterRecovered"));
        Assert.Equal(0, run.GetProperty("tickAfterRecoveredTimeoutStrips").GetInt32());
    }

    [Fact]
    public void ADifferentWindow_IsADifferentCell_ThatRunsWhileTheStoppedWindowStaysHeld()
    {
        var window = Result.Value.GetProperty("window");
        Assert.Equal(1, window.GetProperty("first").GetInt32());
        Assert.Equal(0, window.GetProperty("sameWindow").GetInt32());
        Assert.Equal(1, window.GetProperty("otherWindow").GetInt32());
        Assert.Equal(0, window.GetProperty("backToFirst").GetInt32());
    }
}
