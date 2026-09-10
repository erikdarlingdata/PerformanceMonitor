/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows.Media;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2452: Lite's Overview card banded five metric rows and a status word, and none of them was collection
/// FRESHNESS. A server could go quiet and stay green — the connection check still passing, no collector
/// reporting an error, and the store taking no new rows for hours — while the card showed "Online" in green,
/// a neutral border, and a "Last Collect" timestamp rendered in the plain foreground brush, where a reading
/// from four hours ago looked exactly like one from four seconds ago.
///
/// <para><b>Freshness is a THIRD axis and these pins exist mostly to keep it one.</b> The card already
/// answers two questions: did the last connection check succeed (<c>IsOnline</c>) and are any collectors
/// erroring (<c>HasCollectorErrors</c>). "Has anything landed lately" is neither of those, and the way this
/// goes wrong is well documented next door: #2429 found the Darling viewer says the same word, "Warning", for
/// a stale collection AND for a metric breach, with nothing on the card telling them apart — the card @ehaar
/// wrote in about in #2422. So <see cref="TheStatusWordIsNotToldAboutFreshness"/> and
/// <see cref="EachAxisIsNamedOnTheCardWithoutBorrowingTheOthersWords"/> are the load-bearing tests here; the
/// band arithmetic below is the shared classifier's and is pinned in <c>PerformanceMonitor.Common</c>.</para>
///
/// <para><b>The thresholds are read, never restated.</b> Every boundary case is built from
/// <see cref="ServerHealthThresholds"/> rather than from the literals 2 and 15, so a test cannot pass while
/// the card has quietly grown numbers of its own — which is the drift the shared class was created to end
/// (#1562).</para>
/// </summary>
public class ServerCollectionFreshnessTests
{
    private static readonly DateTime Now = new(2026, 8, 21, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>A card in the state #2452 describes: reachable, nothing erroring, every metric calm. The
    /// only variable is how long ago the newest collection_log row landed.</summary>
    private static ServerSummaryItem QuietServer(TimeSpan? sinceLastCollection)
    {
        var card = new ServerSummaryItem
        {
            DisplayName = "sql-01",
            ServerId = 1,
            IsOnline = true,
            HasCollectorErrors = false,
            CpuPercent = 4,
            OtherProcessCpuPercent = 1,
            MemoryMb = 8192,
            BlockingCount = 0,
            DeadlockCount = 0,
            LastCollectionTime = sinceLastCollection.HasValue ? Now - sinceLastCollection.Value : null,
        };

        card.ApplyCollectionFreshness(Now);
        return card;
    }

    private static string Hex(SolidColorBrush brush) => brush.Color.ToString(System.Globalization.CultureInfo.InvariantCulture);

    private const string Green = "#FF81C784";
    private const string RowAmber = "#FFFFB74D";
    private const string Red = "#FFE57373";
    private const string StatusAmber = "#FFFFD54F";
    private const string NeutralBorder = "#FF2A2D35";
    private const string UnknownGrey = "#FF888888";

    // ── The band itself, off the shared thresholds ────────────────────────────────────────────────────

    [Fact]
    public void ACollectionInsideTheCadenceIsFresh()
    {
        var card = QuietServer(ServerHealthThresholds.CollectorCadence);

        Assert.Equal(ServerFreshness.Fresh, card.CollectionFreshness);
        Assert.False(card.CollectionIsNotFresh);
        Assert.Equal(Green, Hex(card.LastCollectionBrush));
    }

    [Fact]
    public void PastTwiceTheCadenceTheRowGoesAmberAndSaysStale()
    {
        var card = QuietServer(ServerHealthThresholds.StaleThreshold + TimeSpan.FromSeconds(1));

        Assert.Equal(ServerFreshness.Stale, card.CollectionFreshness);
        Assert.Equal(RowAmber, Hex(card.LastCollectionBrush));
        Assert.EndsWith(" (stale)", card.LastCollectionDisplay, StringComparison.Ordinal);
    }

    [Fact]
    public void PastTheOfflineThresholdTheRowGoesRedAndSaysStopped()
    {
        var card = QuietServer(ServerHealthThresholds.OfflineThreshold + TimeSpan.FromSeconds(1));

        Assert.Equal(ServerFreshness.Offline, card.CollectionFreshness);
        Assert.Equal(Red, Hex(card.LastCollectionBrush));
        Assert.EndsWith(" (stopped)", card.LastCollectionDisplay, StringComparison.Ordinal);
    }

    /// <summary>Exactly ON a threshold is still the lower band — the shared classifier compares with a strict
    /// <c>&gt;</c>. Pinned because a card that re-derived the comparison is exactly how the two SKUs would
    /// come to disagree by one tick, and nothing else would ever show it.</summary>
    [Fact]
    public void TheBoundariesAreTheSharedClassifiersAndAreInclusiveOfTheLowerBand()
    {
        Assert.Equal(ServerFreshness.Fresh, QuietServer(ServerHealthThresholds.StaleThreshold).CollectionFreshness);
        Assert.Equal(ServerFreshness.Stale, QuietServer(ServerHealthThresholds.OfflineThreshold).CollectionFreshness);
    }

    /// <summary>A server that has never been collected is queued, not dead. Amber, never the red the
    /// stopped band gets — the viewer learned this from a 24-server field report that went chasing a
    /// phantom scheduler bug (<see cref="ServerFreshness.NeverCollected"/>).</summary>
    [Fact]
    public void NeverCollectedIsAmberRatherThanRed()
    {
        var card = QuietServer(null);

        Assert.Equal(ServerFreshness.NeverCollected, card.CollectionFreshness);
        Assert.Equal(RowAmber, Hex(card.LastCollectionBrush));
        Assert.Equal("Never", card.LastCollectionDisplay);
    }

    /// <summary>An item nobody classified renders EXACTLY what dev's row rendered: the bare stamp, the
    /// card's unknown grey, and no tooltip. Unreachable in the shipped app — every ServerSummaryItem is
    /// built by GetServerSummaryAsync, which stamps the band — but a fixture or a new data path can produce
    /// it, and a named "not classified" beats falling through onto a band that would be a guess.</summary>
    [Fact]
    public void AnUnclassifiedCardClaimsNothing()
    {
        var card = new ServerSummaryItem { LastCollectionTime = Now.AddHours(-4) };

        Assert.Null(card.CollectionFreshness);
        Assert.False(card.CollectionIsNotFresh);
        Assert.Equal(UnknownGrey, Hex(card.LastCollectionBrush));
        Assert.Null(card.CollectionFreshnessTooltip);
        Assert.DoesNotContain("(", card.LastCollectionDisplay, StringComparison.Ordinal);
    }

    // ── The defect, and the conflation the fix must not create ────────────────────────────────────────

    /// <summary>
    /// The #2452 card itself. Everything the card used to answer still says the server is fine, because
    /// those answers are still TRUE — the connection check really did succeed and no collector really is
    /// erroring. What changes is that the card no longer looks calm: the border escalates and the row that
    /// holds the evidence is red and says so.
    /// </summary>
    [Fact]
    public void AServerThatWentQuietNoLongerLooksCalm()
    {
        var quiet = QuietServer(TimeSpan.FromHours(4));

        /* Unchanged, and deliberately so: these two answer different questions and both are still yes. */
        Assert.Equal("Online", quiet.StatusDisplay);
        Assert.Equal(Green, Hex(quiet.StatusBrush));

        /* Changed: the card now shows the third axis. */
        Assert.Equal(StatusAmber, Hex(quiet.CardBorderBrush));
        Assert.Equal(Red, Hex(quiet.LastCollectionBrush));
        Assert.Contains("stopped", quiet.LastCollectionDisplay, StringComparison.Ordinal);
        Assert.Contains("Collection has stopped", quiet.CollectionFreshnessTooltip!, StringComparison.Ordinal);

        /* And #2451's card tooltip picks it up through the gate pattern, which is what option 1 in the
           issue meant by "let the tooltip name it, the way it names CPU and Blocking today". */
        Assert.Contains("Needs attention: Last collect ", quiet.StatusTooltip, StringComparison.Ordinal);
    }

    /// <summary>
    /// The invariant #2451 established for the metric rows, extended to this one: the tooltip names the Last
    /// Collect row exactly when that row is not green. Asserted against the SHIPPED brush rather than against
    /// a copy of the thresholds, so the clause and the colour cannot drift apart — a tooltip that named a
    /// green row, or stayed silent about a red one, is the single thing this property exists to prevent.
    /// </summary>
    [Fact]
    public void TheTooltipNamesTheCollectionRowExactlyWhenItIsNotGreen()
    {
        foreach (var age in new TimeSpan?[]
                 {
                     TimeSpan.FromSeconds(20),
                     ServerHealthThresholds.StaleThreshold + TimeSpan.FromSeconds(1),
                     TimeSpan.FromHours(4),
                     null,
                 })
        {
            var card = QuietServer(age);

            Assert.Equal(
                Hex(card.LastCollectionBrush) != Green,
                card.StatusTooltip.Contains("Last collect ", StringComparison.Ordinal));
        }
    }

    /// <summary>
    /// Collection goes FIRST in the reason, ahead of the metrics. It is the row that says whether the other
    /// four can be believed: a card whose collection stopped four hours ago is showing four-hour-old CPU,
    /// and meeting "CPU 4%" before learning that is the wrong order to be told the two facts in.
    /// </summary>
    [Fact]
    public void TheReasonLeadsWithCollectionWhenBothAxesAreInTrouble()
    {
        var card = QuietServer(TimeSpan.FromHours(4));
        card.CpuPercent = 96;
        card.OtherProcessCpuPercent = 0;
        card.DeadlockCount = 2;

        Assert.StartsWith("Last collect ", card.StatusReason, StringComparison.Ordinal);
        Assert.Contains("CPU ", card.StatusReason, StringComparison.Ordinal);
        Assert.Contains("Deadlocks ", card.StatusReason, StringComparison.Ordinal);
    }

    /// <summary>
    /// The same card while collection is current is the control: neutral border, green row. Without this the
    /// test above would pass on a card that had simply been painted amber for everyone.
    /// </summary>
    [Fact]
    public void ACalmCardWithCurrentCollectionStaysNeutral()
    {
        var healthy = QuietServer(TimeSpan.FromSeconds(20));

        Assert.Equal(NeutralBorder, Hex(healthy.CardBorderBrush));
        Assert.Equal(Green, Hex(healthy.LastCollectionBrush));
    }

    /// <summary>
    /// The anti-conflation pin, and the reason option 2 in #2452 was turned down. Lite's status word is a
    /// CONNECTION word — <c>IsOnline</c> comes from a live check that succeeds whether or not anything is
    /// being collected, and "Warning" there already means collectors are erroring. If freshness were folded
    /// into it, one amber word would mean two unrelated failures, which is precisely the viewer defect
    /// #2429 found and #2422 reported. So a stale card's word and colour are byte-identical to a fresh
    /// card's, and every difference is on the row that owns the axis.
    /// </summary>
    [Fact]
    public void TheStatusWordIsNotToldAboutFreshness()
    {
        var fresh = QuietServer(TimeSpan.FromSeconds(20));
        var stale = QuietServer(ServerHealthThresholds.StaleThreshold + TimeSpan.FromSeconds(1));
        var stopped = QuietServer(TimeSpan.FromHours(4));

        Assert.Equal(fresh.StatusDisplay, stale.StatusDisplay);
        Assert.Equal(fresh.StatusDisplay, stopped.StatusDisplay);
        Assert.Equal(Hex(fresh.StatusBrush), Hex(stale.StatusBrush));
        Assert.Equal(Hex(fresh.StatusBrush), Hex(stopped.StatusBrush));
    }

    /// <summary>
    /// Both directions of the split. A stale collection must not be describable as a metric problem, and a
    /// metric breach must not be describable as a collection problem — the card carries both at once often
    /// enough that only an adversarial pair proves the two vocabularies stayed apart.
    /// </summary>
    [Fact]
    public void EachAxisIsNamedOnTheCardWithoutBorrowingTheOthersWords()
    {
        var stale = QuietServer(ServerHealthThresholds.StaleThreshold + TimeSpan.FromSeconds(1));
        var tooltip = stale.CollectionFreshnessTooltip!;

        Assert.Contains("Collection is stale", tooltip, StringComparison.Ordinal);
        Assert.Contains("not about the server's metrics", tooltip, StringComparison.Ordinal);
        /* The one word that means "collectors are erroring" on this card. Freshness may never claim it. */
        Assert.DoesNotContain("Warning", tooltip, StringComparison.Ordinal);

        /* A busy server whose collection is perfectly current: the border escalates for the METRIC, and the
           collection row keeps saying collection is fine. */
        var busy = QuietServer(TimeSpan.FromSeconds(20));
        busy.CpuPercent = 96;
        busy.OtherProcessCpuPercent = 0;

        Assert.Equal(RowAmber, Hex(busy.CardBorderBrush));
        Assert.Equal(Green, Hex(busy.LastCollectionBrush));
        Assert.Contains("Collection is current", busy.CollectionFreshnessTooltip!, StringComparison.Ordinal);
        Assert.DoesNotContain("Last collect ", busy.StatusTooltip, StringComparison.Ordinal);
    }

    /// <summary>The tooltip quotes the shared thresholds instead of restating them, so the sentence cannot
    /// come to disagree with the band it is explaining.</summary>
    [Fact]
    public void TheTooltipQuotesTheSharedThresholds()
    {
        var stale = QuietServer(ServerHealthThresholds.StaleThreshold + TimeSpan.FromSeconds(1));
        var stopped = QuietServer(ServerHealthThresholds.OfflineThreshold + TimeSpan.FromSeconds(1));

        Assert.Contains(
            $"{ServerHealthThresholds.StaleThreshold.TotalMinutes:0.#} minute",
            stale.CollectionFreshnessTooltip!,
            StringComparison.Ordinal);
        Assert.Contains(
            $"{ServerHealthThresholds.OfflineThreshold.TotalMinutes:0.#} minute",
            stopped.CollectionFreshnessTooltip!,
            StringComparison.Ordinal);
    }

    /// <summary>The band is a pure function of (last collection, now): the same card classified against two
    /// clocks lands in two bands. That is what makes the stamp testable without a store, and what keeps the
    /// clock out of the card's property getters.</summary>
    [Fact]
    public void TheBandIsPureOverTheClockItIsGiven()
    {
        var card = new ServerSummaryItem { LastCollectionTime = Now };

        card.ApplyCollectionFreshness(Now);
        Assert.Equal(ServerFreshness.Fresh, card.CollectionFreshness);

        card.ApplyCollectionFreshness(Now + ServerHealthThresholds.OfflineThreshold + TimeSpan.FromMinutes(1));
        Assert.Equal(ServerFreshness.Offline, card.CollectionFreshness);
    }

    // ── Wiring: the half that lives in XAML, where no assertion about a C# object can reach it ─────────

    /// <summary>
    /// The Last Collect row has to actually BE bound to the band, and removing either binding compiles
    /// perfectly clean — the properties would simply go unread and the row would render exactly as it did
    /// on dev. <c>Background="Transparent"</c> is checked with them because a <c>TextBlock</c> with a null
    /// Background hit-tests on its rendered glyphs alone, so without it the tooltip appears over the letters
    /// and nowhere in the space around them (#2429 found this on the viewer's status line).
    /// </summary>
    [Fact]
    public void TheCardsLastCollectRowIsBoundToTheBand()
    {
        var xaml = ParitySource.ReadFile("Lite/MainWindow.xaml");
        var row = RowFour(xaml);

        Assert.Contains("{Binding LastCollectionBrush}", row, StringComparison.Ordinal);
        Assert.Contains("{Binding CollectionFreshnessTooltip}", row, StringComparison.Ordinal);
        Assert.DoesNotContain("Foreground=\"{DynamicResource ForegroundBrush}\"", row, StringComparison.Ordinal);
        Assert.Equal(2, CountOccurrences(row, "Background=\"Transparent\""));
        Assert.Equal(2, CountOccurrences(row, "ToolTip=\"{Binding CollectionFreshnessTooltip}\""));
    }

    /// <summary>
    /// The band is stamped at the ONE place a ServerSummaryItem is built, which is what stops the Overview
    /// and the two MCP reads from getting different answers about the same server. It is pinned in the
    /// source because deleting that single line compiles perfectly clean and silently returns every card to
    /// dev's behaviour — no band, so a grey row and a neutral border. A property that nothing sets is the
    /// failure mode this whole issue is about, one level up.
    /// </summary>
    [Fact]
    public void EveryCardIsStampedWithItsBandWhereItIsBuilt()
    {
        var source = ParitySource.ReadFile("Lite/Services/LocalDataService.Overview.cs");

        Assert.Equal(1, CountOccurrences(source, "new ServerSummaryItem"));
        Assert.Equal(1, CountOccurrences(source, "ApplyCollectionFreshness(DateTime.UtcNow)"));

        /* And the stamp is after the object is built: banding an object that has already been handed back
           would band nothing, and the ordering is the half a count cannot see. */
        Assert.True(
            source.IndexOf("new ServerSummaryItem", StringComparison.Ordinal)
                < source.IndexOf("ApplyCollectionFreshness(DateTime.UtcNow)", StringComparison.Ordinal),
            "The freshness stamp no longer follows the summary it is meant to band.");
    }

    /// <summary>The two Grid.Row="4" TextBlocks of the Overview card template — the "Last Collect:" label
    /// and its value. Sliced by the marker rather than by line numbers so an edit above the row does not
    /// silently move the window this test reads.</summary>
    private static string RowFour(string xaml)
    {
        const string marker = "Text=\"Last Collect:\"";
        var start = xaml.IndexOf(marker, StringComparison.Ordinal);
        Assert.True(start >= 0, "The Overview card no longer has a Last Collect row.");
        Assert.Equal(start, xaml.LastIndexOf(marker, StringComparison.Ordinal));

        var lineStart = xaml.LastIndexOf('<', start);
        var end = xaml.IndexOf("</Grid>", start, StringComparison.Ordinal);
        Assert.True(end > lineStart, "The Last Collect row is no longer inside the card's metric Grid.");
        return xaml[lineStart..end];
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0;
             i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}
