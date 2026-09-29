/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
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

    // ── the tab asks for the clock of the server it is showing ───────────────────

    /// <summary>
    /// The viewer's Recommendations tab reads findings for the server ITS selector names, so the card's clock has to
    /// be that server's, read through the per-server clock source the viewer's other reads use
    /// (<c>GetServerClocksAsync</c> with <c>ClockFor</c>: the server's zone where known, else its offset, else UTC),
    /// and not the viewer machine's offset. The tab is a WPF window this suite does not instantiate, so this is a
    /// source pin on the loader.
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
        Assert.Matches(
            new Regex(@"ViewerDataService\s*\.\s*ClockFor\(\s*await\s+_dataService\s*\.\s*GetServerClocksAsync\(\s*server\s*\.\s*ServerId\s*,[^;]*,\s*server\s*\.\s*ServerId\s*\)"),
            body);
        Assert.Matches(new Regex(@"FromFindings\(\s*rows\s*,\s*server\s*\.\s*DisplayName\s*,\s*serverClock\s*,"), body);
    }
}
