/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4766: the "Ask AI" prompt on a viewer Recommendations card names the finding's window in the monitored server's
/// local time. The card was handed <c>LocalUtcOffsetMinutes()</c>, the offset in force now of the VIEWER MACHINE, and
/// added it to both ends of the window: so a viewer in one zone showed another zone's server on its own clock, and a
/// finding from before a daylight saving change was an hour off even where the two zones agreed today. The card now
/// takes the selected server's <see cref="ServerClock"/> and converts each end of the window at its own instant.
///
/// <para>US Eastern is the test zone: the 2026 spring change is 8 March (02:00 EST becomes 03:00 EDT at 07:00 UTC)
/// and the fall change is 1 November (02:00 EDT becomes 01:00 EST at 06:00 UTC). These are the same cases as
/// Lite's <c>LiteRecommendationServerClockTests</c>.</para>
/// </summary>
/* Serialized with the other classes that read or set the process-wide ViewerTimeHelper statics: the status-line
   tests below set ActiveServerClock and CurrentDisplayMode to prove the line ignores them. */
[Collection("viewer-time-statics")]
public sealed class ViewerRecommendationServerClockTests
{
    /* The dash between the two ends of the window in the prompt text. */
    private const string Dash = "–";

    private static readonly ServerClock Eastern = ServerClock.Resolve("Eastern Standard Time", -240);

    private static DateTime Utc(int year, int month, int day, int hour, int minute)
        => new(year, month, day, hour, minute, 0, DateTimeKind.Utc);

    private static RecommendationItem Finding(DateTime startUtc, DateTime endUtc)
        => new()
        {
            Severity = RecommendationSeverity.Warning,
            Title = "High CPU",
            ServerName = "PRODSQL",
            WindowStartUtc = startUtc,
            WindowEndUtc = endUtc,
        };

    private static string Prompt(DateTime startUtc, DateTime endUtc, ServerClock clock)
        => new RecommendationCardViewModel(Finding(startUtc, endUtc), clock).AskAiPrompt;

    // ── the window, each end on its own date's offset ────────────────────────────

    [Fact]
    public void AskAiPrompt_AWinterFinding_ShowsStandardTime()
    {
        var prompt = Prompt(Utc(2026, 1, 15, 14, 0), Utc(2026, 1, 15, 16, 0), Eastern);

        Assert.Contains($"2026-01-15 09:00{Dash}11:00", prompt, StringComparison.Ordinal);
    }

    [Fact]
    public void AskAiPrompt_ASummerFinding_ShowsDaylightTime()
    {
        var prompt = Prompt(Utc(2026, 7, 15, 14, 0), Utc(2026, 7, 15, 16, 0), Eastern);

        Assert.Contains($"2026-07-15 10:00{Dash}12:00", prompt, StringComparison.Ordinal);
    }

    [Fact]
    public void AskAiPrompt_AWindowAcrossTheSpringChange_ShowsEachEndWithItsOwnOffset()
    {
        /* 05:30 UTC is 00:30 EST; 08:30 UTC, an hour after the change, is 04:30 EDT. One offset for both ends
           reads 00:30-03:30 (standard) or 01:30-04:30 (daylight). */
        var prompt = Prompt(Utc(2026, 3, 8, 5, 30), Utc(2026, 3, 8, 8, 30), Eastern);

        Assert.Contains($"2026-03-08 00:30{Dash}04:30", prompt, StringComparison.Ordinal);
    }

    [Fact]
    public void AskAiPrompt_AWindowAcrossTheFallChange_ShowsEachEndWithItsOwnOffset()
    {
        /* 04:30 UTC is 00:30 EDT; 07:30 UTC, an hour after the change, is 02:30 EST. */
        var prompt = Prompt(Utc(2026, 11, 1, 4, 30), Utc(2026, 11, 1, 7, 30), Eastern);

        Assert.Contains($"2026-11-01 00:30{Dash}02:30", prompt, StringComparison.Ordinal);
    }

    [Fact]
    public void AskAiPrompt_WithNoClock_ShowsTheWindowInUtc()
    {
        var card = new RecommendationCardViewModel(Finding(Utc(2026, 6, 1, 10, 0), Utc(2026, 6, 1, 12, 0)));

        Assert.Contains($"2026-06-01 10:00{Dash}12:00", card.AskAiPrompt, StringComparison.Ordinal);
    }

    // ── the window a finding without one gets ────────────────────────────────────

    [Fact]
    public void FallbackWindow_AcrossTheSpringChange_IsTwoRealHours()
    {
        /* Now is 08:30 UTC, 04:30 EDT. Two real hours back is 06:30 UTC, which is 01:30 EST: the wall clock reads
           three hours apart because 02:00 to 03:00 did not happen. Subtracting two hours from the wall clock lands
           on 02:30, a time that never existed that day. */
        var now = Utc(2026, 3, 8, 8, 30);

        var (from, to) = RecommendationCardViewModel.FallbackWindow(Eastern, now);

        Assert.Equal(new DateTime(2026, 3, 8, 1, 30, 0), from);
        Assert.Equal(new DateTime(2026, 3, 8, 4, 30, 0), to);
        Assert.Equal(TimeSpan.FromHours(2), Eastern.ToUtc(to) - Eastern.ToUtc(from));
    }

    [Fact]
    public void FallbackWindow_OnAnOrdinaryDay_IsTheLastTwoHoursOnTheServersClock()
    {
        var (from, to) = RecommendationCardViewModel.FallbackWindow(Eastern, Utc(2026, 7, 15, 16, 0));

        Assert.Equal(new DateTime(2026, 7, 15, 10, 0, 0), from);
        Assert.Equal(new DateTime(2026, 7, 15, 12, 0, 0), to);
    }

    // ── the clock reaches every card ─────────────────────────────────────────────

    [Fact]
    public void FromFindings_CarriesTheServersClockOntoEveryCard_OfAnIncident()
    {
        var rows = new[]
        {
            Row(Utc(2026, 1, 15, 14, 0), Utc(2026, 1, 15, 16, 0), "winter"),
            Row(Utc(2026, 7, 15, 14, 0), Utc(2026, 7, 15, 16, 0), "summer"),
        };

        var vm = RecommendationsViewModel.FromFindings(rows, "PRODSQL", Eastern);
        var prompts = vm.Sections.SelectMany(s => s.Cards).Select(c => c.AskAiPrompt).ToList();

        Assert.Equal(2, prompts.Count);
        Assert.Contains(prompts, p => p.Contains($"2026-01-15 09:00{Dash}11:00", StringComparison.Ordinal));
        Assert.Contains(prompts, p => p.Contains($"2026-07-15 10:00{Dash}12:00", StringComparison.Ordinal));
    }

    private static ViewerFindingRow Row(DateTime startUtc, DateTime endUtc, string title)
    {
        var finding = new AnalysisFinding
        {
            FindingId = 1,
            AnalysisTime = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc),
            ServerId = 1,
            ServerName = "PRODSQL",
            TimeRangeStart = startUtc,
            TimeRangeEnd = endUtc,
            Severity = 1.0,
            Confidence = 0.9,
            Category = "cpu",
            StoryPath = "a -> b",
            StoryPathHash = "hash-" + title,
            StoryText = "",
            RootFactKey = "FACT",
            IncidentId = "inc-1",
            FactCount = 1,
        };
        return new ViewerFindingRow
        {
            SeverityBand = ViewerDataService.SeverityBand(1.0),
            RawSeverity = 1.0,
            Title = title,
            Category = "cpu",
            DatabaseName = "",
            Finding = finding,
        };
    }

    // ── a server with no collected clock yet ─────────────────────────────────────

    /// <summary>
    /// A server whose <c>server_properties</c> has no offset yet has no entry in the per-server clock read. The
    /// viewer's own Server mode shows the viewer machine's offset for it, so the cards and the "Last analyzed" line
    /// take that offset too and do not drop to UTC. The machine is at -240 here and the finding is at 15:00 UTC:
    /// both read 11:00, where UTC would read 15:00 (UTC+0:00). Another server's clock is in the read, and is not
    /// this server's.
    /// </summary>
    [Fact]
    public void ClockForServerOrMachine_WithNoCollectedClock_UsesTheViewerMachinesOffset()
    {
        var machine = TimeZoneInfo.CreateCustomTimeZone("machine-minus-4", TimeSpan.FromHours(-4), "machine -4", "machine -4");
        var clocks = new Dictionary<int, ServerClock> { [2] = ServerClock.FixedOffset(330) };
        var analyzed = Utc(2026, 6, 1, 15, 0);

        var clock = ViewerTimeHelper.ClockForServerOrMachine(clocks, 1, machine, analyzed);

        Assert.Contains(
            $"2026-06-01 11:00{Dash}13:00",
            Prompt(analyzed, Utc(2026, 6, 1, 17, 0), clock),
            StringComparison.Ordinal);
        Assert.Equal(
            "Last analyzed 2026-06-01 11:00:00 (UTC-4:00)",
            RecommendationsViewModel.FormatLastAnalyzed(analyzed, TimeDisplayMode.ServerTime, clock));
    }

    [Fact]
    public void ClockForServerOrMachine_WithACollectedClock_UsesItAndNotTheMachines()
    {
        var machine = TimeZoneInfo.CreateCustomTimeZone("machine-plus-9", TimeSpan.FromHours(9), "machine +9", "machine +9");
        var clocks = new Dictionary<int, ServerClock> { [1] = Eastern };

        var clock = ViewerTimeHelper.ClockForServerOrMachine(clocks, 1, machine, Utc(2026, 1, 15, 14, 0));

        Assert.Contains(
            $"2026-01-15 09:00{Dash}11:00",
            Prompt(Utc(2026, 1, 15, 14, 0), Utc(2026, 1, 15, 16, 0), clock),
            StringComparison.Ordinal);
    }

    // ── the tab asks for the clock of the server it is showing ───────────────────

    /// <summary>
    /// The viewer's Recommendations tab reads findings for the server ITS selector names, so the card's clock has to
    /// be that server's, read through the per-server clock source the viewer's other reads use
    /// (<c>GetServerClocksAsync</c>: the server's zone where known, else its offset), and not the viewer machine's
    /// offset. A server with no collected clock gets the viewer machine's offset (<c>ClockForServerOrMachine</c>),
    /// not the UTC that <c>ViewerDataService.ClockFor</c> gives the stored-times reads. The tab is a WPF window this
    /// suite does not instantiate, so this is a source pin on the loader.
    /// </summary>
    [Fact]
    public void TheRecommendationsLoader_TakesTheSelectedServersClock_NotTheViewerMachinesOffset()
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml.cs"));

        var signature = source.IndexOf("Task LoadRecommendationsAsync()", StringComparison.Ordinal);
        Assert.True(signature >= 0, "LoadRecommendationsAsync is gone, so this pin would read nothing.");
        var body = CSharpSourceWalker.BraceBalanced(source, source.IndexOf('{', signature));

        Assert.DoesNotContain("LocalUtcOffsetMinutes", source, StringComparison.Ordinal);
        Assert.DoesNotContain("ViewerDataService.ClockFor(", body, StringComparison.Ordinal);
        Assert.Matches(
            new Regex(@"ViewerTimeHelper\s*\.\s*ClockForServerOrMachine\(\s*await\s+_dataService\s*\.\s*GetServerClocksAsync\(\s*server\s*\.\s*ServerId\s*,[^;]*,\s*server\s*\.\s*ServerId\s*,\s*TimeZoneInfo\s*\.\s*Local\s*,\s*DateTime\s*\.\s*UtcNow\s*\)"),
            body);
        Assert.Matches(new Regex(@"FromFindings\(\s*rows\s*,\s*server\s*\.\s*DisplayName\s*,\s*serverClock\s*,"), body);
    }

    // ── the status line names the display mode its time is in ────────────────────

    /// <summary>
    /// The line under the Recommendations tab, "Last analyzed ...", showed a time that follows the display mode
    /// (Server, Local or UTC) and always ended in "(local)". It now ends in the zone the time is in, taken from
    /// the same mode and clock that produced the time.
    /// </summary>
    [Theory]
    [InlineData(TimeDisplayMode.ServerTime)]
    [InlineData(TimeDisplayMode.UTC)]
    [InlineData(TimeDisplayMode.LocalTime)]
    public void LastAnalyzed_NamesTheDisplayModeItsTimeIsIn(TimeDisplayMode mode)
    {
        var analyzed = Utc(2026, 7, 15, 14, 0);
        var machine = TimeZoneInfo.Local;

        var (time, zone) = mode switch
        {
            /* 14:00 UTC is 10:00 on a US Eastern server in July, which is on daylight time. */
            TimeDisplayMode.ServerTime => ("2026-07-15 10:00:00", "UTC-4:00"),
            TimeDisplayMode.UTC => ("2026-07-15 14:00:00", "UTC"),
            _ => (
                TimeZoneInfo.ConvertTimeFromUtc(analyzed, machine).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
                machine.IsDaylightSavingTime(analyzed) ? machine.DaylightName : machine.StandardName),
        };

        var text = WithAnotherServersClockActive(() => RecommendationsViewModel.FormatLastAnalyzed(analyzed, mode, Eastern));

        Assert.Equal($"Last analyzed {time} ({zone})", text);
    }

    [Fact]
    public void LastAnalyzed_InServerMode_ShowsTheSelectedServersOwnTimeAndOffset_WhateverClockTheLastServerTabSet()
    {
        var (winter, summer) = WithAnotherServersClockActive(() => (
            RecommendationsViewModel.FormatLastAnalyzed(Utc(2026, 1, 15, 14, 0), TimeDisplayMode.ServerTime, Eastern),
            RecommendationsViewModel.FormatLastAnalyzed(Utc(2026, 7, 15, 14, 0), TimeDisplayMode.ServerTime, Eastern)));

        Assert.Equal("Last analyzed 2026-01-15 09:00:00 (UTC-5:00)", winter);
        Assert.Equal("Last analyzed 2026-07-15 10:00:00 (UTC-4:00)", summer);
    }

    /// <summary>
    /// Runs <paramref name="read"/> while the process-wide clock is another server's (India, +05:30, no daylight
    /// saving) and the process-wide mode is UTC: what a viewer holds after a different server's tab rendered last.
    /// The status line must not read either, so a formatter that went back through
    /// <c>ViewerTimeHelper.ForDisplay</c> would show India's time here. Both statics are restored.
    /// </summary>
    private static T WithAnotherServersClockActive<T>(Func<T> read)
    {
        var savedClock = ViewerTimeHelper.ActiveServerClock;
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        try
        {
            ViewerTimeHelper.ActiveServerClock = ServerClock.FixedOffset(330);
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            return read();
        }
        finally
        {
            ViewerTimeHelper.ActiveServerClock = savedClock;
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
        }
    }

    /// <summary>
    /// The tab is a WPF window this suite does not instantiate, so this is a source pin on the loader: the status
    /// line goes through <c>FormatLastAnalyzed</c> with the display mode in force and the selected server's clock,
    /// and the loader holds no "Last analyzed" text of its own that could end in a fixed zone name.
    /// </summary>
    [Fact]
    public void TheRecommendationsLoader_BuildsTheStatusLineFromTheDisplayModeAndTheSelectedServersClock()
    {
        var raw = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml.cs");
        var source = CSharpSourceWalker.StripCommentsAndStrings(raw);

        var signature = source.IndexOf("Task LoadRecommendationsAsync()", StringComparison.Ordinal);
        Assert.True(signature >= 0, "LoadRecommendationsAsync is gone, so this pin would read nothing.");
        var open = source.IndexOf('{', signature);
        var body = CSharpSourceWalker.BraceBalanced(source, open);

        Assert.Matches(
            new Regex(@"RecommendationsStatusText\s*\.\s*Text\s*=\s*rows\s*\.\s*Count\s*>\s*0\s*\?\s*RecommendationsViewModel\s*\.\s*FormatLastAnalyzed\(\s*rows\[0\]\s*\.\s*Finding\s*\.\s*AnalysisTime\s*,\s*ViewerTimeHelper\s*\.\s*CurrentDisplayMode\s*,\s*serverClock\s*\)"),
            body);

        var literals = CSharpSourceWalker.StringLiteralBodies(raw.Substring(open, body.Length)).Select(l => l.Text);
        Assert.DoesNotContain(literals, l => l.Contains("Last analyzed", StringComparison.Ordinal));
    }
}
