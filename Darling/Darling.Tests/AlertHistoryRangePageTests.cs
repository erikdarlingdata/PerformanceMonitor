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
/// Alert History's range, row-limit, server and Show dismissed choices (#4843): source pins for what the page asks
/// get_alert_history for, and a run of the shipped page under Node (<c>alert-history-range-harness.mjs</c>) for what
/// it does with each choice. Node is skipped when it is not installed.
/// </summary>
public sealed class AlertHistoryRangePageTests
{
    private static string Page() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "alerts.js").ReplaceLineEndings("\n");

    [Fact]
    public void ThePageReadsGetAlertHistoryWithTheFourChoicesAndNeverAFixedWindow()
    {
        var page = Page();
        Assert.DoesNotContain("hours_back: 24", page);
        Assert.DoesNotContain("limit: 200 }", page);
        Assert.Contains("readTool(\"get_alert_history\", readParams())", page);
        // #5562: the range is the shared picker's spec, read as of each read; a finished range also sends its end.
        Assert.Contains("const w = windowOfSpec(choices.spec);", page);
        Assert.Contains("hours_back: w ? w.hours : DEFAULT_HOURS", page);
        Assert.Contains("as_of: w ? w.asOf : null", page);
        Assert.Contains("limit: choices.limit", page);
        Assert.Contains("server_name: choices.server || null", page);
        Assert.Contains("include_dismissed: choices.dismissed ? \"true\" : null", page);
        Assert.Contains("readTool(\"list_servers\", {})", page);
        Assert.Contains("res.data.truncated === true", page);
    }

    [Fact]
    public void NoWindowOrLimitIsOfferedThatTheToolOrTheDispatchLayerRefuses()
    {
        var page = Page();
        // #5562 R8: no hand-listed windows and no "All". The picker takes its reach from the catalog's max_hours for the read, which is
        // the 168 hours the read's validator accepts, so a longer range is greyed out with the reason and never clamped. The alert table
        // keeps 90 days (AlertHistoryRetentionDays, DarlingRetention.cs), but the web read stops at the common reach (R2).
        Assert.DoesNotContain("WINDOW_CHOICES", page);
        Assert.Contains("read: \"get_alert_history\"", page);
        Assert.Contains("pageRangePicker({", page);
        Assert.Equal(PerformanceMonitor.Common.McpHelpers.MaxHoursBack, PerformanceMonitor.Darling.Service.WebReadReach.All["get_alert_history"].MaxHours);
        Assert.Equal(90, PerformanceMonitor.Darling.Storage.DarlingRetentionHorizons.AlertHistoryRetentionDays);

        var limits = System.Text.RegularExpressions.Regex.Match(page, "const LIMIT_CHOICES = \\[([0-9, ]+)\\]").Groups[1].Value
            .Split(',').Select(s => int.Parse(s.Trim())).ToList();
        Assert.Equal(new[] { 200, 500, 1000 }, limits);
        Assert.True(limits.Max() <= 1000, "the dispatch layer clamps a row count to 1000");
    }

    [Fact]
    public void TheChoicesLiveAtModuleScopeAndTheDismissRouteIsNotTheDesktopOne()
    {
        var page = Page();
        Assert.Contains("\nconst choices = {", page);
        Assert.DoesNotContain("/api/alerts/dismiss", page);
    }

    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "alert-history-range-harness.mjs"));
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
                Assert.Fail("the Alert History harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the Alert History harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static string Get(JsonElement read, string name) =>
        read.TryGetProperty(name, out var v) ? v.GetString()! : "(absent)";

    [Fact]
    public void EachChoiceReachesTheRead()
    {
        var r = Run("reads");
        var first = r.GetProperty("first");
        Assert.Equal("24", Get(first, "hours_back"));
        Assert.Equal("200", Get(first, "limit"));
        Assert.Equal("(absent)", Get(first, "server_name"));
        Assert.Equal("(absent)", Get(first, "include_dismissed"));

        Assert.Equal("168", Get(r.GetProperty("window"), "hours_back"));
        Assert.Equal("1000", Get(r.GetProperty("limit"), "limit"));
        Assert.Equal("srv-b", Get(r.GetProperty("server"), "server_name"));
        var dismissed = r.GetProperty("dismissed");
        Assert.Equal("true", Get(dismissed, "include_dismissed"));
        Assert.Equal("168", Get(dismissed, "hours_back"));
        Assert.Equal("1000", Get(dismissed, "limit"));
        Assert.Equal("srv-b", Get(dismissed, "server_name"));

        // The popup offers the short presets and the calendar periods within 7 days; the longer ones are greyed out, never clamped.
        Assert.Equal("Past 5 minutes,Past 15 minutes,Past 30 minutes,Past hour,Past 4 hours,Past day,Past 2 days,Past week,Today,Yesterday,Week to Date,Previous Week",
            string.Join(",", r.GetProperty("windowOptions").EnumerateArray().Select(e => e.GetString())));
        Assert.Equal("Past 30 days,Month to Date,Previous Month,Year to Date,Previous Year",
            string.Join(",", r.GetProperty("windowGreyed").EnumerateArray().Select(e => e.GetString())));
        Assert.Equal("This page's reads reach at most 7 days back.", r.GetProperty("refused").GetString());
        // A finished range sends its end as as_of; a shorter-than-an-hour range reads the whole hour.
        Assert.EndsWith("Z", Get(r.GetProperty("finished"), "as_of"));
        Assert.Equal("1", Get(r.GetProperty("thirtyMinutes"), "hours_back"));
        Assert.Equal("200,500,1000", string.Join(",", r.GetProperty("limitOptions").EnumerateArray().Select(e => e.GetString())));
        Assert.Equal(",srv-a,srv-b", string.Join(",", r.GetProperty("serverOptions").EnumerateArray().Select(e => e.GetString())));
    }

    [Fact]
    public void TheChoicesSurviveThePollAndAVisitElsewhere()
    {
        var r = Run("survive");
        foreach (var step in new[] { "poll", "back" })
        {
            var reads = r.GetProperty(step).EnumerateArray().ToList();
            Assert.Single(reads);
            Assert.Equal("4", Get(reads[0], "hours_back"));
            Assert.Equal("500", Get(reads[0], "limit"));
            Assert.Equal("srv-a", Get(reads[0], "server_name"));
            Assert.Equal("true", Get(reads[0], "include_dismissed"));
        }

        var shown = r.GetProperty("shown");
        Assert.Equal("Past 4 hours", shown.GetProperty("window").GetString());
        Assert.Equal("500", shown.GetProperty("limit").GetString());
        Assert.Equal("srv-a", shown.GetProperty("server").GetString());
        Assert.True(shown.GetProperty("dismissed").GetBoolean());
    }

    [Fact]
    public void ATruncatedReplySaysMoreAlertsExistAndAClearOneDoesNot()
    {
        var r = Run("truncated");
        Assert.True(r.GetProperty("truncated").GetBoolean());
        Assert.False(r.GetProperty("notTruncated").GetBoolean());
        Assert.True(r.GetProperty("pollTruncated").GetBoolean());
        Assert.False(r.GetProperty("pollCleared").GetBoolean());
    }

    [Fact]
    public void DismissedRowsAreMarkedOnFirstDrawAndOnLaterReconciles()
    {
        var r = Run("dismissed");
        Assert.Equal(new[] { "", "alert-dismissed" }, r.GetProperty("classes").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal(new[] { "alert-dismissed", "", "alert-dismissed" }, r.GetProperty("afterPoll").EnumerateArray().Select(e => e.GetString()).ToArray());
    }

    [Fact]
    public void AKeptRowTakesItsDismissedMarkingFromALaterPoll()
    {
        var r = Run("dismissedLater");
        string[] Strings(string n) => r.GetProperty(n).EnumerateArray().Select(e => e.GetString() ?? "").ToArray();
        Assert.Equal(new[] { "", "" }, Strings("before"));
        Assert.Equal(new[] { "", "alert-dismissed" }, Strings("after"));
        Assert.Equal(new[] { "", "Dismissed" }, Strings("afterTitle"));
        Assert.Equal(new[] { false, true }, r.GetProperty("afterStatus").EnumerateArray().Select(e => e.GetBoolean()).ToArray());
        Assert.Equal("High CPU 1,High CPU 2", string.Join(",", Strings("order")));
        Assert.Equal(new[] { "", "" }, Strings("restored"));
    }

    [Fact]
    public void TwoServersThatShareADisplayNameKeepBothRows()
    {
        Assert.Equal(2, Run("sharedName").GetProperty("rows").GetInt32());
    }

    [Fact]
    public void TheRangePickerSaysTheWebReadsAtMostSevenDays()
    {
        // #5562: the picker greys out what the read cannot reach and says why (the catalog's max_hours), so the page needs no hand-written
        // "at most 7 days" tooltip; the reach is the read's own 168 hours.
        Assert.DoesNotContain("The web reads at most 7 days of alert history, so there is no All choice.", Page());
        Assert.Contains("the web read stops at the common 7 day reach", Page());
    }

    [Fact]
    public void ANewWindowReconcilesInPlace_RowsLeaveAndNewOnesLandInOrder()
    {
        var r = Run("reconcile");
        Assert.Equal("High CPU 1,High CPU 3,High CPU 5", string.Join(",", r.GetProperty("before").EnumerateArray().Select(e => e.GetString())));
        Assert.Equal("High CPU 0,High CPU 1,High CPU 2,High CPU 3", string.Join(",", r.GetProperty("after").EnumerateArray().Select(e => e.GetString())));
        Assert.True(r.GetProperty("keptNode").GetBoolean(), "an unchanged row keeps its DOM node across a window change");
        Assert.Equal("High CPU 3", string.Join(",", r.GetProperty("shrunk").EnumerateArray().Select(e => e.GetString())));
    }

    [Fact]
    public void TheMuteLinksStillRenderOnEveryRow()
    {
        var links = Run("mute").GetProperty("links").EnumerateArray().Select(e => e.GetString()!).ToList();
        Assert.Contains(links, l => l.StartsWith("Mute this alert #/mute-rules?server_id=1&server_name=srv-a&metric_name=", StringComparison.Ordinal));
        Assert.Contains(links, l => l.StartsWith("Mute similar #/mute-rules?metric_name=", StringComparison.Ordinal));
    }
}
