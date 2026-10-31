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
/// #5244: the web viewer's blocking panels say which collector answered (<c>web-blocking-source-harness.mjs</c>, the shipped
/// <c>panels.js</c>, <c>util.js</c> and <c>pages/server-tabs.js</c> run under Node with a stub fetch). get_blocking_trend and
/// get_blocking_stats already carry a top-level <c>source</c> ("blocked-process-report" or "DMV snapshot", null with no rows); the
/// panels that draw them show it as a line above the chart: Blocking Events on the Overview and Blocking tabs, and Blocking Severity.
/// No other panel gets one, and no line is drawn when the answer has no rows or carries no recognized source. Node is skipped when
/// it is not installed, as <see cref="WebDatabaseFilterChipsBehaviourTests"/> does.
/// </summary>
public sealed class WebBlockingSourceBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-blocking-source-harness.mjs"));
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
            if (!proc.WaitForExit(60000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the blocking source harness did not finish in 60 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the blocking source harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            var result = doc.RootElement.Clone();
            Assert.Empty(result.GetProperty("rejections").EnumerateArray());
            return result;
        }
    }

    /// <summary>The source lines every panel with that title drew, one entry per panel.</summary>
    private static string[][] Lines(JsonElement r, string title) =>
        r.GetProperty("panels").EnumerateArray()
            .Where(p => p.GetProperty("title").GetString() == title)
            .Select(p => p.GetProperty("strips").EnumerateArray().Select(s => s.GetString()!).ToArray())
            .ToArray();

    [Theory]
    [InlineData("xe", "Source: blocked-process-report", "Blocked process report trend")]
    [InlineData("dmv", "Source: DMV snapshot", "Blocking snapshot trend")]
    public void BlockingEventsAndSeverity_NameTheCollectorThatAnswered(string scenario, string expected, string trendTitle)
    {
        var r = Run(scenario);

        /* Blocking Events and Blocking Severity are on the Blocking tab (the Overview draws blocking as one of its five strips, #6291). */
        var events = Lines(r, trendTitle);
        Assert.Single(events);
        Assert.All(events, lines => Assert.Equal(new[] { expected }, lines));
        var severity = Lines(r, "Blocking Severity");
        Assert.Single(severity);
        Assert.Equal(new[] { expected }, severity[0]);
    }

    [Theory]
    [InlineData("zeroRows")]
    [InlineData("envelope")]
    [InlineData("noSource")]
    [InlineData("other")]
    public void NoRows_OrNoRecognizedSource_DrawsNoSourceLine(string scenario)
    {
        var r = Run(scenario);

        Assert.All(r.GetProperty("panels").EnumerateArray(), p => Assert.Empty(p.GetProperty("strips").EnumerateArray()));
    }

    /// <summary>The empty sentences every panel of one tab drew, for the panels with that title.</summary>
    private static string[][] Empties(JsonElement r, string tab, string title) =>
        r.GetProperty("panels").EnumerateArray()
            .Where(p => p.GetProperty("tab").GetString() == tab && p.GetProperty("title").GetString() == title)
            .Select(p => p.GetProperty("empties").EnumerateArray().Select(s => s.GetString()!).ToArray())
            .ToArray();

    /// <summary>#5644 W16: each deadlock read answers its own empty message (the trend's carries the collector-run count, the table's says
    /// "specified time range"), and only the Deadlock Severity panel showed the page's sentence. Every deadlock empty state on the
    /// Blocking tab now says that one sentence, whatever the read said.</summary>
    [Fact]
    public void EveryDeadlockEmptyState_OnTheBlockingTab_SaysTheOneSentence()
    {
        var r = Run("deadlockEmpty");
        const string One = "No deadlocks in this window.";

        /* The Blocking tab: the chart (titled Deadlock trend), the Deadlocks grid and the Deadlock Severity panel. */
        Assert.Equal(new[] { One }, Assert.Single(Empties(r, "blocking", "Deadlock trend")));
        Assert.Equal(new[] { One }, Assert.Single(Empties(r, "blocking", "Deadlocks")));
        Assert.Equal(new[] { One }, Assert.Single(Empties(r, "blocking", "Deadlock Severity")));
    }

    /// <summary>#5644 W16: the panels that are NOT deadlock states keep the read's own message (the sentence is a per-panel opt-in).</summary>
    [Fact]
    public void ABlockingEmptyState_StillShowsTheReadsOwnMessage()
    {
        var r = Run("deadlockEmpty");

        /* get_blocking_stats answered the envelope, so Blocking Severity (same read as Deadlock Severity) shows the read's message. */
        Assert.Equal(new[] { "No blocking or deadlock data in the specified time range." }, Assert.Single(Empties(r, "blocking", "Blocking Severity")));
    }

    /// <summary>#5644 W16: the chart counts one point per blocked-process REPORT while the tile beside it counts one event per wait, so
    /// the chart says reports (title, legend and empty sentence), on the Overview and Blocking tabs and in the starter dashboard.</summary>
    [Fact]
    public void TheBlockingTrendChart_SaysItCountsBlockedProcessReports()
    {
        var tabs = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");
        var templates = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "view-templates.js");

        Assert.DoesNotContain("\"Blocking Events\"", tabs);
        Assert.DoesNotContain("\"Blocking Events\"", templates);
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(tabs, "linePanel\\(\"Blocked process report trend\", \"get_blocking_trend\""));
        Assert.Contains("const BLOCKING_REPORT_SERIES = [{ key: \"count\", label: \"Reports\" }];", tabs);
        Assert.DoesNotContain("No blocking events in this window — an empty trend", tabs);
        /* The starter dashboard panel has no collector tag to follow, so it words itself for both sources. */
        Assert.Contains("title: \"Blocking trend (reports or snapshot rows)\"", templates);
        Assert.Contains("label: \"Reports or snapshot rows\"", templates);
        Assert.DoesNotContain("title: \"Blocked process report trend\"", templates);
        /* The axis caption says it too: the Blocking tab chart and the starter dashboard panel carry unit "reports". */
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(tabs, "unit: \"reports\","));
        Assert.Contains("unit: \"reports or snapshot rows\",", templates);
    }

    /// <summary>#5644 W16 review: only a true negative (status "empty") takes the page's one sentence. A not_collected or unavailable
    /// deadlock read keeps its own message; "No deadlocks in this window." over an Azure SQL Database target would be a false all-clear.</summary>
    [Fact]
    public void ANotCollectedOrUnavailableDeadlockRead_KeepsItsOwnMessage_NotTheAllClear()
    {
        var r = Run("deadlockNotCollected");
        const string One = "No deadlocks in this window.";
        var trend = Assert.Single(Empties(r, "blocking", "Deadlock trend"));
        var table = Assert.Single(Empties(r, "blocking", "Deadlocks"));
        var severity = Assert.Single(Empties(r, "blocking", "Deadlock Severity"));
        Assert.All(new[] { trend, table, severity }, e => { Assert.Single(e); Assert.NotEqual(One, e[0]); Assert.False(string.IsNullOrWhiteSpace(e[0])); });
        Assert.Equal("Deadlock data is unavailable right now: the collector has not run.", table[0]);
    }

    /// <summary>#5644 W16 review: when get_blocking_trend falls back to the DMV snapshot its points are snapshot rows, not blocked process
    /// reports, so the title, legend and axis caption follow the answer's collector tag.</summary>
    [Theory]
    [InlineData("xe", "Blocked process report trend", "Reports", "reports")]
    [InlineData("dmv", "Blocking snapshot trend", "Snapshot rows", "snapshot rows")]
    public void TheBlockingTrendChart_WordsItselfByTheCollectorThatAnswered(string scenario, string title, string label, string unit)
    {
        var r = Run(scenario);
        var charts = r.GetProperty("charts").EnumerateArray()
            .Where(c => c.GetProperty("labels").EnumerateArray().Any(l => l.GetString() == label))
            .ToList();
        Assert.Single(charts);
        Assert.All(charts, c =>
        {
            Assert.Equal(title, c.GetProperty("title").GetString());
            Assert.Equal(unit, c.GetProperty("unit").GetString());
        });
    }

    [Fact]
    public void OnlyTheBlockingPanelsGetOne()
    {
        var r = Run("dmv");

        var titles = r.GetProperty("panels").EnumerateArray()
            .Where(p => p.GetProperty("strips").GetArrayLength() > 0)
            .Select(p => p.GetProperty("title").GetString())
            .ToArray();
        Assert.Equal(new[] { "Blocking snapshot trend", "Blocking Severity" }, titles);
    }
}
