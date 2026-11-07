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
/// #7937: a reply that lands after the author has moved on is dropped. In the dashboard composer's read-panel editor a sample
/// that returns after the author picked a chart type keeps the pick, and the older of two samples does not fill the fields.
/// In the alert-rule editor the validate and test replies of an earlier check do not land over the "Checking…" strip of a later
/// one, nor over the hint an incomplete rule shows. A chart picked while a newly chosen read's sample is in flight takes its fields
/// from that sample. Run as the shipped <c>editor.js</c> and <c>alert-editor.js</c> under Node against a scripted service whose replies are
/// held until released (<c>web-editor-late-reply-harness.mjs</c>). Node is skipped when it is not installed.
/// </summary>
public sealed class WebEditorLateReplyBehaviourTests
{
    private static readonly Lazy<JsonElement> Result = new(Run);

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-editor-late-reply-harness.mjs"));
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
                Assert.Fail("the editor late reply harness did not finish in 60 s");
            }

            Assert.True(proc.ExitCode == 0, "the editor late reply harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n').First(l => l.StartsWith('{')));
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();
    private static int[] Ints(JsonElement e) => e.EnumerateArray().Select(x => x.GetInt32()).ToArray();

    [Fact]
    public void AChartTypePickedWhileTheSampleIsInFlight_IsKeptWhenTheSampleLands()
    {
        var run = Result.Value;
        Assert.Equal(new[] { "r1" }, Strings(run.GetProperty("pickInFlight")));
        Assert.Equal("line", run.GetProperty("pickVizBeforeReply").GetString());
        Assert.Equal("line", run.GetProperty("pickVizAfterReply").GetString());
        // The sample itself is still taken: the fields come from it.
        Assert.Equal(new[] { "a1_rows" }, Strings(run.GetProperty("pickRowsKeys")));
    }

    [Fact]
    public void TheOlderOfTwoSamples_LandingLast_DoesNotFillTheFields()
    {
        var run = Result.Value;
        Assert.Equal(new[] { "r1", "r2" }, Strings(run.GetProperty("twiceInFlight")));
        Assert.Equal(new[] { "b2_rows" }, Strings(run.GetProperty("twiceRowsKeysAfterNewer")));
        Assert.Equal("r2", run.GetProperty("twiceReadAtEnd").GetString());
        Assert.Equal(new[] { "b2_rows" }, Strings(run.GetProperty("twiceRowsKeysAfterOlder")));
    }

    [Fact]
    public void AnEarlierChecksReplies_DoNotLandOverTheLaterChecksStrip()
    {
        var run = Result.Value;
        Assert.Equal(new[] { "validate:10" }, Strings(run.GetProperty("alertFirstChecks")));
        Assert.Equal(new[] { "validate:10", "validate:20" }, Strings(run.GetProperty("alertChecksAfterSecondEdit")));
        Assert.Equal("Checking…", run.GetProperty("alertPreviewWhileBothWait").GetString());
        // The older validate's answer starts no test and draws nothing.
        Assert.Empty(Ints(run.GetProperty("alertTestsAfterOlderValidate")));
        Assert.Equal("Checking…", run.GetProperty("alertPreviewAfterOlderReplies").GetString());
        // The newer check's own replies draw its result.
        Assert.StartsWith("Tested with warning 20", run.GetProperty("alertPreviewAtEnd").GetString());
        Assert.Equal(new[] { "validate:10", "validate:20", "test:20" }, Strings(run.GetProperty("alertAllChecks")));
    }

    [Fact]
    public void ClearingTheThreshold_WhileValidateWaits_SendsNoTestAndKeepsTheHint()
    {
        var run = Result.Value;
        Assert.Equal(new[] { "validate:10" }, Strings(run.GetProperty("clearChecksBefore")));
        Assert.StartsWith("Choose a metric and a warning threshold", run.GetProperty("clearPreviewAfterClear").GetString());
        // The earlier check's validate answer starts no test of the definition the author has left, and the hint stays.
        Assert.Empty(Ints(run.GetProperty("clearTestsAfterOlderValidate")));
        Assert.StartsWith("Choose a metric and a warning threshold", run.GetProperty("clearPreviewAtEnd").GetString());
    }

    [Fact]
    public void AChartPickedWhileTheNewReadsSampleIsInFlight_TakesItsFieldsFromTheNewSample()
    {
        var run = Result.Value;
        Assert.Equal("line", run.GetProperty("switchVizAtEnd").GetString());
        var fields = run.GetProperty("switchFieldsAtEnd");
        Assert.Equal("b2_rows", fields.GetProperty("rowsKey").GetString());
        Assert.Equal("y", fields.GetProperty("xKey").GetString());
    }
}
