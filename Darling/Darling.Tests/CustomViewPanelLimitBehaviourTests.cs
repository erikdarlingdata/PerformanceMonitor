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
/// #7257: a saved custom view starts its panel reads two at a time, sends a read the service refused busy once more, and puts a Retry button on a
/// panel whose read failed. Run as the shipped page scripts under Node against a scripted <c>/api/compose/run</c>
/// (<c>custom-view-panel-limit-harness.mjs</c>). Node is skipped when it is not installed.
/// </summary>
public sealed class CustomViewPanelLimitBehaviourTests
{
    private static readonly Lazy<JsonElement> Result = new(Run);

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "custom-view-panel-limit-harness.mjs"));
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
                Assert.Fail("the custom view panel limit harness did not finish in 60 s");
            }

            Assert.True(proc.ExitCode == 0, "the custom view panel limit harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    [Fact]
    public void SevenPanels_StartTwoReads_AndEachNextReadStartsWhenAnEarlierOneEnds()
    {
        var run = Result.Value;
        var started = run.GetProperty("sevenStarted").EnumerateArray().Select(Strings).ToArray();
        Assert.Equal(new[] { "s1", "s2" }, started[0]);
        // Each answer frees one slot, and the next panel in order takes it.
        for (var i = 1; i <= 5; i++) Assert.Equal(2 + i, started[i].Length);
        Assert.Equal(new[] { "s1", "s2", "s3", "s4", "s5", "s6", "s7" }, started[^1]);
        Assert.Equal(5, run.GetProperty("sevenWaitingAtStart").GetInt32());
        Assert.Equal(2, run.GetProperty("sevenMaxOpen").GetInt32());
        Assert.Equal(0, run.GetProperty("sevenLoadingAtEnd").GetInt32());
        Assert.Equal(0, run.GetProperty("sevenErrorsAtEnd").GetInt32());
    }

    [Fact]
    public void ABusyRefusalThenAnAnswer_ShowsTheRetryStrip_ThenDraws_WithTwoReadsSent_AndTheSlotKept()
    {
        var run = Result.Value;
        Assert.Contains(Strings(run.GetProperty("busyOnceStrips")), s => s.StartsWith("The service is running as many long reads as it allows. Retrying in 1 second"));
        // While the first panel waits to retry it keeps its slot, so the third panel has not started.
        Assert.Equal(new[] { "s1", "s2" }, Strings(run.GetProperty("busyOnceCallsWhileWaiting")));
        Assert.Equal(new[] { "s1", "s2", "s1" }, Strings(run.GetProperty("busyOnceCallsAfterWait")));
        Assert.Equal(new[] { "s1", "s2", "s1", "s3" }, Strings(run.GetProperty("busyOnceCallsAtEnd")));
        Assert.Equal(0, run.GetProperty("busyOnceLoadingAtEnd").GetInt32());
        Assert.Equal(0, run.GetProperty("busyOnceErrorsAtEnd").GetInt32());
    }

    [Fact]
    public void TwoBusyRefusals_ShowTheErrorAndARetryButton_AndRetryRunsThatPanelAgainThroughTheLimiter()
    {
        var run = Result.Value;
        Assert.Equal(new[] { "s1", "s2", "s1" }, Strings(run.GetProperty("busyTwiceCalls")));
        Assert.Contains("Try again in about 48 seconds.", Assert.Single(Strings(run.GetProperty("busyTwiceErrors"))));
        Assert.Equal(1, run.GetProperty("busyTwiceRetryButtons").GetInt32());
        // Retry sends one more read, for that panel only.
        Assert.Equal(new[] { "s1", "s2", "s1", "s1" }, Strings(run.GetProperty("retryCalls")));
        Assert.Equal(0, run.GetProperty("retryLoadingAtEnd").GetInt32());
        Assert.Equal(0, run.GetProperty("retryButtonsAtEnd").GetInt32());
        // A server error has the button too, and a Retry made while two other reads hold both slots waits for one to end.
        Assert.Equal(1, run.GetProperty("serverErrorRetryButtons").GetInt32());
        Assert.Equal(new[] { "s1", "s2", "s3" }, Strings(run.GetProperty("serverErrorRetryCallsWhileFull")));
        Assert.Contains("Waiting for the panels above to finish…", Strings(run.GetProperty("serverErrorRetryStrips")));
        Assert.Equal(new[] { "s1", "s2", "s3", "s1" }, Strings(run.GetProperty("serverErrorRetryCallsAfterOneEnds")));
    }

    [Fact]
    public void AScopeChange_OrACancelledRefresh_NeverStartsThePanelsOfTheOldRenderThatStillWait()
    {
        var run = Result.Value;
        // Four panels: two started under the old scope, and the change drew new panels that started two reads of their own.
        Assert.Equal(new[] { "old:s1", "old:s2", "new:s1", "new:s2" }, Strings(run.GetProperty("scopeCallsAfterChange")));
        // The old scope's slots ending let the old waiting panels ask for a slot, and they start nothing.
        Assert.Equal(new[] { "old:s1", "old:s2", "new:s1", "new:s2", "new:s3", "new:s4" }, Strings(run.GetProperty("scopeCallsAtEnd")));
        Assert.Equal(0, run.GetProperty("scopeLoadingAtEnd").GetInt32());
        // An in-place refresh that a newer poll cancels: the first poll started two reads, and only the second poll's four reads follow.
        Assert.Equal(new[] { "s1", "s2" }, Strings(run.GetProperty("pollFirstStarted")));
        Assert.Equal(new[] { "s1", "s2", "s1", "s2", "s3", "s4" }, Strings(run.GetProperty("pollCallsAtEnd")));
    }

    [Fact]
    public void AMarkdownCell_AndAPanelWithAnUnknownSource_TakeNoSlot_AndGetNoRetryButton()
    {
        var run = Result.Value;
        // Two unknown-source panels and a markdown cell come first, and the first two real panels still start at once.
        Assert.Equal(new[] { "s1", "s2" }, Strings(run.GetProperty("noSlotStarted")));
        var text = run.GetProperty("noSlotText").GetString()!;
        Assert.Contains("Unknown source 'nope'", text);
        Assert.Contains("Unknown source 'nope2'", text);
        Assert.Contains("Notes", text);
        Assert.Equal(1, run.GetProperty("noSlotWaiting").GetInt32());
        Assert.Equal(0, run.GetProperty("noSlotRetryButtons").GetInt32());
        Assert.Equal(new[] { "s1", "s2", "s3" }, Strings(run.GetProperty("noSlotStartedAfterOne")));
    }
}
