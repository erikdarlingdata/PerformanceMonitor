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
/// Dismiss Selected and Dismiss All on the web Alert History page, from the shipped <c>alerts.js</c> run under
/// Node (<c>alert-history-dismiss-harness.mjs</c>) against a recording fetch: the controls exist only for a seat
/// that can edit, the right keys are posted to <c>/api/alert-history/dismiss</c>, the checked set survives a
/// rebuild, a refusal is shown and never reported as success, and the reply's counts are summarised.
/// </summary>
public sealed class AlertHistoryDismissBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "alert-history-dismiss-harness.mjs"));
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
                Assert.Fail("the Alert History dismiss harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the Alert History dismiss harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Metrics(JsonElement post) =>
        post.GetProperty("req").GetProperty("alerts").EnumerateArray().Select(a => a.GetProperty("metric_name").GetString()!).ToArray();

    [Fact]
    public void ASeatThatCannotEdit_SeesNoCheckboxesAndNoDismissButtons()
    {
        var r = Run("readOnly");
        Assert.Equal(0, r.GetProperty("checkboxes").GetInt32());
        Assert.False(r.GetProperty("hasSelected").GetBoolean());
        Assert.False(r.GetProperty("hasAll").GetBoolean());
    }

    [Fact]
    public void DismissSelected_PostsTheCheckedRowsKeys_AndRefreshesTheList()
    {
        var r = Run("selected");
        Assert.Equal(3, r.GetProperty("boxes").GetInt32());
        Assert.True(r.GetProperty("selectedDisabledBefore").GetBoolean());
        var post = Assert.Single(r.GetProperty("posts").EnumerateArray());
        Assert.Equal("/api/alert-history/dismiss", post.GetProperty("url").GetString());
        Assert.Equal("application/json", post.GetProperty("contentType").GetString());
        Assert.Equal(new[] { "High CPU 1", "High CPU 3" }, Metrics(post));
        var first = post.GetProperty("req").GetProperty("alerts")[0];
        Assert.Equal(1, first.GetProperty("server_id").GetInt32());
        Assert.Equal("2026-01-01T11:59:00.000Z", first.GetProperty("alert_time").GetString());
        Assert.Equal("Dismissed 2 of 2", r.GetProperty("status").GetString());
        Assert.Equal(1, r.GetProperty("rowsAfter").GetInt32());
        Assert.Equal(2, r.GetProperty("reads").GetInt32());
    }

    [Fact]
    public void WithShowDismissedOn_ADismissedRowStaysAndSaysDismissed()
    {
        var r = Run("showDismissed");
        Assert.Equal(2, r.GetProperty("rows").GetInt32());
        Assert.True(r.GetProperty("dismissedWord").GetBoolean());
        Assert.Equal(0, r.GetProperty("firstHasBox").GetInt32());
    }

    [Fact]
    public void DismissAll_PostsEveryLiveRowTheFiltersShow()
    {
        var r = Run("all");
        var filtered = Assert.Single(r.GetProperty("filtered").EnumerateArray());
        Assert.Equal(new[] { 2 }, filtered.EnumerateArray().Select(e => e.GetInt32()).ToArray());
        var post = Assert.Single(r.GetProperty("all").EnumerateArray());
        Assert.Equal("/api/alert-history/dismiss", post.GetProperty("url").GetString());
        Assert.Equal(3, post.GetProperty("keys").GetArrayLength());
    }

    [Fact]
    public void ALongList_GoesInRequestsOfAtMostTheRoutesCap_AndTheCountsAreAddedUp()
    {
        var r = Run("chunks");
        Assert.Equal(new[] { 1000, 500 }, r.GetProperty("sizes").EnumerateArray().Select(e => e.GetInt32()).ToArray());
        Assert.Equal("Dismissed 1500 of 1500", r.GetProperty("status").GetString());
    }

    [Fact]
    public void TheCheckedSet_SurvivesARebuild_AndClearsWhenAFilterChanges()
    {
        var r = Run("survives");
        Assert.Equal(new[] { false, false, true }, r.GetProperty("afterPoll").EnumerateArray().Select(e => e.GetBoolean()).ToArray());
        Assert.Equal(new[] { false, false, true }, r.GetProperty("afterReopen").EnumerateArray().Select(e => e.GetBoolean()).ToArray());
        Assert.Equal("Dismiss Selected (1)", r.GetProperty("label").GetString());
        Assert.DoesNotContain(true, r.GetProperty("afterFilter").EnumerateArray().Select(e => e.GetBoolean()));
    }

    [Fact]
    public void ARefusal_ShowsItsText_NeverClaimsSuccess_AndKeepsTheSelection()
    {
        var r = Run("refused");
        Assert.Equal("This seat is read-only.", r.GetProperty("status").GetString());
        Assert.Equal(2, r.GetProperty("rows").GetInt32());
        Assert.True(r.GetProperty("stillChecked").GetBoolean());
        var expired = r.GetProperty("status401").GetString()!;
        Assert.DoesNotContain("Dismissed", expired);
        Assert.Contains("Session expired", expired);
    }

    [Fact]
    public void DismissAllPrompt_SaysStore_AndWhenThePageIsTruncatedSaysTheRestStayLive()
    {
        var r = Run("truncatedPrompt");
        var full = r.GetProperty("fullAsk").GetString()!;
        Assert.Contains("remain in the store", full);
        Assert.DoesNotContain("database", full);
        Assert.DoesNotContain("More alerts exist", full);
        var truncated = r.GetProperty("truncatedAsk").GetString()!;
        Assert.Contains("remain in the store", truncated);
        Assert.Contains("More alerts exist than the 2 listed", truncated);
        Assert.Contains("the rest stay live", truncated);
    }

    [Fact]
    public void CopyRowAndCopyTable_LeaveOutTheSelectColumn_ButKeepMute()
    {
        var r = Run("copy");
        var all = r.GetProperty("copyAll").GetString()!.Split('\n');
        var header = all[0].Split('\t');
        Assert.DoesNotContain("Select", header);
        Assert.Contains("Mute", header);
        var row = r.GetProperty("copyRow").GetString()!.Split('\t');
        Assert.Equal(header.Length, row.Length);
        Assert.Equal(header.Length, all[1].Split('\t').Length);
        Assert.Equal(header.Length + 1, r.GetProperty("cells").GetInt32());
    }

    [Fact]
    public void AFailedPollRead_KeepsTheCheckedSet()
    {
        var r = Run("failedPoll");
        Assert.Equal("Dismiss Selected (1)", r.GetProperty("labelAfterFailedRead").GetString());
        Assert.Equal(new[] { false, true }, r.GetProperty("afterGoodRead").EnumerateArray().Select(e => e.GetBoolean()).ToArray());
        Assert.Equal("Dismiss Selected (1)", r.GetProperty("labelAfterGoodRead").GetString());
    }

    [Fact]
    public void ABoxTickedWhileADismissIsInFlight_IsNotClearedAndNotSent()
    {
        var r = Run("tickedInFlight");
        var sent = Assert.Single(r.GetProperty("sent").EnumerateArray());
        Assert.Equal(new[] { "High CPU 1" }, sent.EnumerateArray().Select(e => e.GetString()!).ToArray());
        Assert.Equal("Dismiss Selected (1)", r.GetProperty("label").GetString());
        Assert.Equal(new[] { false, true }, r.GetProperty("checked").EnumerateArray().Select(e => e.GetBoolean()).ToArray());
    }

    [Fact]
    public void TheStatusLine_SaysHowManyWereDismissedAndHowManyWereAlreadyGone()
    {
        var r = Run("counts");
        Assert.Equal("Dismissed 1 of 4; 3 were already gone", r.GetProperty("status").GetString());
    }

    [Fact]
    public void ThePageSendsOnlyTheRoutesKeysThroughTheSharedWriteHelper()
    {
        var page = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "alerts.js").ReplaceLineEndings("\n");
        Assert.Contains("apiSend(\"POST\", \"/api/alert-history/dismiss\"", page);
        Assert.Contains("const DISMISS_CHUNK = 1000;", page);
        Assert.Contains("\nconst selected = new Map();", page);
    }
}
