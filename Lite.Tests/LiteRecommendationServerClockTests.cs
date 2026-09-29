/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Analysis.Recommendations;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: the "Ask AI" prompt on a Recommendations card names the finding's window in the monitored server's local
/// time. It used to add ONE offset to both ends of the window, and the offset was whatever
/// <c>ServerTimeHelper.UtcOffsetMinutes</c> held when the card was built: the offset in force today, of the server
/// whose TAB the main window last selected. The Recommendations tab has its own server selector, so that could be
/// another server, and a finding from before a daylight saving change was an hour off even on the right one.
///
/// <para>The card now takes the server's <see cref="ServerClock"/> and converts each end of the window at its own
/// instant. US Eastern is the test zone: the 2026 spring change is 8 March (02:00 EST becomes 03:00 EDT at 07:00
/// UTC) and the fall change is 1 November (02:00 EDT becomes 01:00 EST at 06:00 UTC).</para>
/// </summary>
/* Names the shared server-time settings inside strings the source pin searches for, and the census in
   ServerTimeHelperCollectionTests counts a test file that names one as reading it. The class reads none of them. */
[Collection("server-time-helper")]
public sealed class LiteRecommendationServerClockTests
{
    /* The dash between the two ends of the window in the prompt text. */
    private const string Dash = "–";

    private static readonly ServerClock Eastern = ServerClock.Resolve("Eastern Standard Time", -240);

    private static DateTime Utc(int year, int month, int day, int hour, int minute)
        => new(year, month, day, hour, minute, 0, DateTimeKind.Utc);

    private static LiteRecommendationItem Finding(DateTime startUtc, DateTime endUtc, string incidentId = "")
        => new()
        {
            Severity = LiteRecommendationSeverity.Warning,
            RawSeverity = 1.0,
            Title = "High CPU",
            ServerName = "PRODSQL",
            IncidentId = incidentId,
            WindowStartUtc = startUtc,
            WindowEndUtc = endUtc,
        };

    private static string Prompt(DateTime startUtc, DateTime endUtc, ServerClock clock)
        => new LiteRecommendationCardViewModel(Finding(startUtc, endUtc), clock).AskAiPrompt;

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
        var card = new LiteRecommendationCardViewModel(Finding(Utc(2026, 6, 1, 10, 0), Utc(2026, 6, 1, 12, 0)));

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

        var (from, to) = LiteRecommendationCardViewModel.FallbackWindow(Eastern, now);

        Assert.Equal(new DateTime(2026, 3, 8, 1, 30, 0), from);
        Assert.Equal(new DateTime(2026, 3, 8, 4, 30, 0), to);
        Assert.Equal(TimeSpan.FromHours(2), Eastern.ToUtc(to) - Eastern.ToUtc(from));
    }

    [Fact]
    public void FallbackWindow_OnAnOrdinaryDay_IsTheLastTwoHoursOnTheServersClock()
    {
        var (from, to) = LiteRecommendationCardViewModel.FallbackWindow(Eastern, Utc(2026, 7, 15, 16, 0));

        Assert.Equal(new DateTime(2026, 7, 15, 10, 0, 0), from);
        Assert.Equal(new DateTime(2026, 7, 15, 12, 0, 0), to);
    }

    // ── the clock reaches every card ─────────────────────────────────────────────

    [Fact]
    public void FromItems_CarriesTheServersClockOntoEveryCard_OfAnIncident()
    {
        var winter = Finding(Utc(2026, 1, 15, 14, 0), Utc(2026, 1, 15, 16, 0), incidentId: "inc-1");
        var summer = Finding(Utc(2026, 7, 15, 14, 0), Utc(2026, 7, 15, 16, 0), incidentId: "inc-1");

        var vm = LiteRecommendationsViewModel.FromItems(new[] { winter, summer }, Eastern);
        var prompts = vm.Sections.SelectMany(s => s.Cards).Select(c => c.AskAiPrompt).ToList();

        Assert.Equal(2, prompts.Count);
        Assert.Contains(prompts, p => p.Contains($"2026-01-15 09:00{Dash}11:00", StringComparison.Ordinal));
        Assert.Contains(prompts, p => p.Contains($"2026-07-15 10:00{Dash}12:00", StringComparison.Ordinal));
    }

    // ── a server with no collected clock yet ─────────────────────────────────────

    /// <summary>
    /// A server whose first <c>server_properties</c> row has not arrived has no collected clock. Its own server tab
    /// keeps the fixed offset the connect probe read until that row lands, so its cards keep that offset too and do
    /// not drop to UTC. The probe read -240 here, so a finding at 15:00 UTC is 11:00 on the tab; read in UTC the same
    /// prompt says 15:00.
    /// </summary>
    [Fact]
    public void CardClock_WithNoCollectedClock_KeepsTheOffsetTheServerTabShows()
    {
        var tabsClock = ServerClock.FixedOffset(-240);

        var clock = LiteRecommendationsViewModel.CardClock(collected: null, openTabClock: tabsClock);
        var prompt = Prompt(Utc(2026, 6, 1, 15, 0), Utc(2026, 6, 1, 17, 0), clock);

        Assert.Contains($"2026-06-01 11:00{Dash}13:00", prompt, StringComparison.Ordinal);
    }

    /// <summary>
    /// The tab the main window has SELECTED is not necessarily the server the Recommendations tab is showing (it has
    /// its own selector). With no collected clock, the cards take the clock of the server's OWN open tab, and the
    /// active clock, here another server's at +5:30, is not read at all.
    /// </summary>
    [Fact]
    public void CardClock_WithNoCollectedClockAndAnOpenTab_TakesThatTabsClock_NotTheActiveOne()
    {
        var savedActive = ServerTimeHelper.ActiveServerClock;
        try
        {
            ServerTimeHelper.ActiveServerClock = ServerClock.FixedOffset(330);
            var ownTab = ServerClock.FixedOffset(-240);

            var clock = LiteRecommendationsViewModel.CardClock(collected: null, openTabClock: ownTab);

            Assert.Same(ownTab, clock);
            Assert.Contains(
                $"2026-06-01 11:00{Dash}13:00",
                Prompt(Utc(2026, 6, 1, 15, 0), Utc(2026, 6, 1, 17, 0), clock),
                StringComparison.Ordinal);
        }
        finally
        {
            ServerTimeHelper.ActiveServerClock = savedActive;
        }
    }

    /// <summary>A server with no collected clock and no open tab is shown on the machine's offset, not in UTC.</summary>
    [Fact]
    public void CardClock_WithNoCollectedClockAndNoOpenTab_IsTheMachinesClock_NotUtc()
    {
        var clock = LiteRecommendationsViewModel.CardClock(collected: null, openTabClock: null);

        var expected = (int)TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow).TotalMinutes;
        Assert.Equal(expected, clock.OffsetMinutesAt(DateTime.UtcNow));
    }

    [Fact]
    public void CardClock_WithACollectedClock_UsesItAndNotTheTabs()
    {
        var indiaOnTheTab = ServerClock.FixedOffset(330);

        var clock = LiteRecommendationsViewModel.CardClock(collected: Eastern, openTabClock: indiaOnTheTab);
        var prompt = Prompt(Utc(2026, 1, 15, 14, 0), Utc(2026, 1, 15, 16, 0), clock);

        Assert.Contains($"2026-01-15 09:00{Dash}11:00", prompt, StringComparison.Ordinal);
    }

    // ── the tab asks for the clock of the server it is showing ───────────────────

    /// <summary>
    /// The Recommendations tab reads findings for the server ITS selector names, so it has to take that server's
    /// clock, by the same server id, at both places that build the cards (the refresh read and Generate now). It
    /// used to hand every card <c>ServerTimeHelper.UtcOffsetMinutes</c>, which is the offset in force now of
    /// whichever server tab the main window last selected. The fallback, for a server with no collected clock yet,
    /// is that SAME server's open tab (the lookup <c>MainWindow</c> passes to <c>Initialize</c>, asked by the same
    /// server id), never the active tab's, and it is not the MCP tools' one, which reads such a server in UTC. The
    /// tab is a WPF control this suite does not instantiate, so this is a source pin.
    /// </summary>
    [Fact]
    public void RecommendationsTab_TakesTheClockOfItsOwnSelectedServer_AndFallsBackToThatServersOwnOpenTab()
    {
        var code = CodeOnly(ReadLite("Controls", "RecommendationsTab.xaml.cs"));

        Assert.DoesNotContain("ServerTimeHelper.UtcOffsetMinutes", code, StringComparison.Ordinal);
        Assert.DoesNotContain("McpServerLocalWindow", code, StringComparison.Ordinal);

        /* Both builds take the id from the tab's own selector, and both resolve the clock by that same id. */
        Assert.Equal(2, Regex.Matches(code, @"var\s+serverId\s*=\s*GetSelectedServerId\(\)").Count);
        Assert.Equal(
            2,
            Regex.Matches(code, @"await\s+ReadCardClockAsync\(\s*_dataService\s*,\s*serverId\s*,\s*_openTabClock\s*\)").Count);
        Assert.Equal(2, Regex.Matches(code, @"FromItems\(\s*items\s*,\s*serverClock\s*\)").Count);

        /* One read, of that server's clock; the fallback is that same server's open tab, asked by the same id, and
           the active server clock is not named at all. */
        Assert.Single(Regex.Matches(code, @"dataService\s*\.\s*GetServerClockAsync\(\s*serverId\s*\)"));
        Assert.Single(Regex.Matches(code, @"openTabClock\s*\?\s*\.\s*Invoke\(\s*serverId\s*\)"));
        Assert.Single(Regex.Matches(
            code,
            @"LiteRecommendationsViewModel\s*\.\s*CardClock\(\s*collected\s*,\s*openTab\s*\)"));
        Assert.DoesNotContain("ActiveServerClock", code, StringComparison.Ordinal);
    }

    /* Line and block comments removed, and line endings normalised, so a pin reads code only. */
    private static string CodeOnly(string source)
    {
        var lf = source.Replace("\r\n", "\n");
        lf = Regex.Replace(lf, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        return Regex.Replace(lf, @"//[^\n]*", string.Empty);
    }

    private static string ReadLite(string folder, string file, [CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", folder, file)));
}
