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
/// #5311 (PR2 lane H2): a click on a row of the Locking &amp; Contention table opens an index detail pane that asks ONCE for that
/// index's four counters (the <c>detail_*</c> selector), says "Loading..." while it waits, draws the counters as text, lets the
/// newest click win when two overlap, shows a plain sentence for a failed or empty answer, and closes with Escape or its Close
/// button. Run from the shipped page script under Node (<c>finops-locking-detail-harness.mjs</c>), skipped when Node is not installed.
/// </summary>
public sealed class FinOpsLockingDetailBehaviourTests
{
    private static readonly Lazy<JsonElement> Result = new(Run);

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "finops-locking-detail-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
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
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the locking detail harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the locking detail harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string Text(string name) => Result.Value.GetProperty(name).GetString()!;

    private static string[] List(string name) => Result.Value.GetProperty(name).EnumerateArray().Select(e => e.GetString()!).ToArray();

    [Fact]
    public void AClick_AsksOnceWithTheSelector_AndShowsLoadingWhileItWaits()
    {
        Assert.Equal("", Text("paneBefore"));
        Assert.EndsWith("Loading...", Text("loading"));
        Assert.Equal(1, Result.Value.GetProperty("oneCalls").GetInt32());
        Assert.Equal(
            "/api/read/get_object_locking?server=srv-a&detail_database=Db&detail_schema=dbo&detail_table=one&detail_index=IX_one",
            Text("oneUrl"));
        /* the list was read once, at build; opening a detail never re-reads it */
        Assert.Equal(1, Result.Value.GetProperty("listCalls").GetInt32());
    }

    [Fact]
    public void TheFourCounters_AreDrawnAsText_WithTheirLabels()
    {
        Assert.Equal(["Row lock count", "Page lock count", "Page latch wait count", "Page I/O latch wait count"], List("labels"));
        Assert.Equal(["10", "11", "12", "13"], List("values"));
    }

    [Fact]
    public void TheNewestClickWins_AnOlderSlowerAnswerIsDropped()
    {
        Assert.Contains("Db.dbo.fast.IX_fast", Text("newest"));
        Assert.Equal(["2,000", "2,001", "2,002", "2,003"], List("newestValues"));
        Assert.DoesNotContain("1,000", Text("newest"));
    }

    [Fact]
    public void AFailedOrEmptyAnswer_IsAPlainSentence_NeverTheServersErrorText()
    {
        Assert.EndsWith("The index detail could not be loaded.", Text("failure"));
        Assert.DoesNotContain("boom", Text("failure"));
        Assert.EndsWith("No such index in the latest snapshot of x.", Text("empty"));
    }

    [Fact]
    public void EveryValueIsText_AnIndexNameThatLooksLikeMarkupMakesNoElement()
    {
        Assert.Contains("<img src=x onerror=alert(1)>", Text("html"));
        Assert.Equal(0, Result.Value.GetProperty("htmlElements").GetInt32());
    }

    [Fact]
    public void EscapeClosesThePane_AndSoDoesTheCloseButton_AndOtherKeysDoNot()
    {
        Assert.Equal("function", Text("escapeListener"));
        Assert.Equal("open", Text("afterOtherKey"));
        Assert.Equal("", Text("afterEscape"));
        Assert.Equal("undefined", Text("listenerAfterEscape"));
        Assert.Equal("Close", Text("closeLabel"));
        Assert.Equal("", Text("afterClose"));
    }

    /* #5372 review M1: renderFinops rebuilds the active tab on every page poll. */
    [Fact]
    public void APagePoll_ReopensTheOpenRow_AndLeavesExactlyOneEscapeListener()
    {
        Assert.Contains("Db.dbo.one.IX_one", Text("pollBefore"));
        Assert.Contains("Db.dbo.one.IX_one", Text("pollAfter"));
        Assert.Equal(["10", "11", "12", "13"], List("pollValues"));
        Assert.Equal(1, Result.Value.GetProperty("pollListeners").GetInt32());
        Assert.Equal(1, Result.Value.GetProperty("pollListenersAfterSecond").GetInt32());
        Assert.Contains("Db.dbo.one.IX_one", Text("pollAfterSecond"));
    }

    [Fact]
    public void APagePoll_AbortsTheDetailReadStillRunning_AndRemovesTheOldListener()
    {
        Assert.True(Result.Value.GetProperty("slowReadAbortedByRebuild").GetBoolean());
        Assert.Equal(0, Result.Value.GetProperty("pollListenersAfterAbort").GetInt32());
    }

    [Fact]
    public void ARowTheReaderClosed_IsNotReopenedByTheNextPoll() => Assert.Equal("", Text("afterChosenClose"));
}
