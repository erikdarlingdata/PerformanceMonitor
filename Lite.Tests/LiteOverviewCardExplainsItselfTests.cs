/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Windows.Media;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #2437 / #2422: Lite's Overview card stops keeping the answer to itself.
///
/// <para><b>What was reported.</b> Against the Darling viewer, but the defect is verbatim here: a card said a
/// word in a colour and would not say what about. @ehaar's question was "what is it that this text warns me
/// about?", and the reader's only recourse was to scan the metric rows guessing which one the card meant —
/// once per card, on every card. #2429 answered it on the viewer; <c>Lite/MainWindow.xaml:442</c> was the same
/// bare <c>Text="{Binding StatusDisplay}"</c> with no ToolTip.</para>
///
/// <para><b>What is pinned here.</b> The property that makes the viewer's version work, and the one thing a
/// port could quietly lose: the sentence is assembled from the card's OWN metric displays, gated on the SAME
/// predicates the row brushes are painted from, so it can never name a metric whose row is green or stay
/// silent about one that is not. <see cref="TheTooltip_NamesExactlyTheRowsTheCardHasColoured"/> asserts that
/// against the shipped brushes rather than against a copy of the thresholds — a re-derivation of severity
/// would pass every text assertion below and still be worse than no tooltip.</para>
///
/// <para><b>What Lite does NOT have.</b> There is no <c>AwaitingFirstCollection</c> flag on Lite's card, so the
/// status/tooltip desync family #2429 spent four review rounds on cannot arise. What Lite has instead is the
/// inverse conflation, pinned by <see cref="TheTooltip_SaysWhichAxisTheStatusWordIsAbout"/>: its status word is
/// a CONNECTION word, so a card in real metric trouble reads a green "Online" while its border is red, and its
/// amber "Warning" is about failing collectors and says nothing about the metrics at all.</para>
/// </summary>
public sealed class LiteOverviewCardExplainsItselfTests
{
    /* Every fixture leaves OtherProcessCpuPercent null on purpose. CpuPercentForAlert reads the process-wide
       App.AlertCpuMode, which another suite legitimately flips; with no other-process figure both modes read
       the same number, so nothing here depends on which one happens to be set. */

    private static ServerSummaryItem Healthy() =>
        new() { DisplayName = "calm", ServerId = 1, IsOnline = true, CpuPercent = 5 };

    private static ServerSummaryItem Busy() =>
        new() { DisplayName = "busy", ServerId = 2, IsOnline = true, CpuPercent = 96, DeadlockCount = 2 };

    private static ServerSummaryItem CollectorsFailing() =>
        new() { DisplayName = "erroring", ServerId = 3, IsOnline = true, CpuPercent = 5, HasCollectorErrors = true };

    private static ServerSummaryItem Offline() =>
        new() { DisplayName = "dark", ServerId = 4, IsOnline = false, CpuPercent = 96, DeadlockCount = 2 };

    private static ServerSummaryItem NotYetChecked() =>
        new() { DisplayName = "queued", ServerId = 5, IsOnline = null, CpuPercent = 62 };

    private static IEnumerable<ServerSummaryItem> EveryState() =>
        new[] { Healthy(), Busy(), CollectorsFailing(), Offline(), NotYetChecked() };

    // ── the card explains itself ───────────────────────────────────────────────────────────────────

    /// <summary>
    /// The invariant the whole change rests on, asserted against the SHIPPED brushes rather than a copy of the
    /// thresholds: the tooltip names a metric exactly when that metric's row is not green. A Lite tooltip that
    /// recomputed severity independently would satisfy every wording assertion below and still drift from the
    /// rows sitting directly underneath it, which is the one failure mode a tooltip must not have.
    /// </summary>
    [Fact]
    public void TheTooltip_NamesExactlyTheRowsTheCardHasColoured()
    {
        var green = Healthy().CpuBrush.Color;

        foreach (var card in EveryState())
        {
            /* Offline is the documented exception and is checked on its own below: the card draws a dimming
               overlay over those rows because the numbers under it are pre-blackout. */
            if (card.IsOffline)
            {
                continue;
            }

            var tooltip = card.StatusTooltip;

            Assert.Equal(card.CpuBrush.Color != green, tooltip.Contains("CPU ", StringComparison.Ordinal));
            Assert.Equal(card.BlockingBrush.Color != green, tooltip.Contains("Blocking ", StringComparison.Ordinal));
            Assert.Equal(card.DeadlockBrush.Color != green, tooltip.Contains("Deadlocks ", StringComparison.Ordinal));
        }
    }

    /// <summary>The numbers in the sentence are the card's own display strings, character for character —
    /// not a second formatting of the same values, which is how a tooltip ends up rounding differently from
    /// the row it is explaining.</summary>
    [Fact]
    public void TheTooltip_QuotesTheCardsOwnDisplays()
    {
        var card = Busy();

        Assert.Equal("96%", card.CpuDisplay);
        Assert.Equal("2", card.DeadlockDisplay);
        Assert.Equal("CPU 96%, Deadlocks 2", card.StatusReason);
        Assert.Contains("Needs attention: CPU 96%, Deadlocks 2", card.StatusTooltip, StringComparison.Ordinal);
    }

    /// <summary>
    /// Lite's conflation, which is not the viewer's. There the amber "Warning" means the collection has gone
    /// stale; here it means collectors are erroring, and the metric rows have no say in the status word at all —
    /// so a card in real trouble reads a green "Online" over a red border. Both halves have to be named or the
    /// tooltip just repeats the word.
    /// </summary>
    [Fact]
    public void TheTooltip_SaysWhichAxisTheStatusWordIsAbout()
    {
        var erroring = CollectorsFailing();

        Assert.Equal("Warning", erroring.StatusDisplay);
        Assert.Contains("collectors are failing", erroring.StatusTooltip, StringComparison.Ordinal);
        /* ...and says so without claiming a metric problem the rows do not show. */
        Assert.Contains("Every metric on this card is inside its threshold", erroring.StatusTooltip, StringComparison.Ordinal);

        var busy = Busy();

        Assert.Equal("Online", busy.StatusDisplay);
        Assert.Contains("Needs attention: CPU 96%, Deadlocks 2", busy.StatusTooltip, StringComparison.Ordinal);
        /* The border is already red for this card. The word never was, which is the whole point. */
        Assert.NotEqual(Healthy().CardBorderBrush.Color, busy.CardBorderBrush.Color);
    }

    /// <summary>A calm card gets an all-clear, not a demand. The viewer had to guard this because its ranking's
    /// "Needs attention" fallback would otherwise reach every healthy card in the fleet; the same sentence is
    /// available to get wrong here, and the card grid shows EVERY server.</summary>
    [Fact]
    public void TheTooltip_OnACalmCard_DoesNotClaimItNeedsAttention()
    {
        var tooltip = Healthy().StatusTooltip;

        Assert.Contains("Online — the last connection check succeeded", tooltip, StringComparison.Ordinal);
        Assert.Contains("Every metric on this card is inside its threshold", tooltip, StringComparison.Ordinal);
        Assert.DoesNotContain("Needs attention", tooltip, StringComparison.Ordinal);
    }

    /// <summary>
    /// An offline card is not told to act on the numbers behind its own blackout overlay. Those are the last
    /// values collected before the server went dark; the card dims them for that reason, and a tooltip
    /// demanding attention for them would contradict the card while the reader is looking at it.
    /// </summary>
    [Fact]
    public void TheTooltip_OnAnOfflineCard_DoesNotDemandActionOnPreBlackoutNumbers()
    {
        var card = Offline();
        var tooltip = card.StatusTooltip;

        Assert.True(card.IsOffline);
        Assert.Contains("Offline — the last connection check failed", tooltip, StringComparison.Ordinal);
        Assert.DoesNotContain("Needs attention", tooltip, StringComparison.Ordinal);
        Assert.DoesNotContain("CPU", tooltip, StringComparison.Ordinal);

        /* The reason itself is still computed — the omission is a tooltip decision, made in one place, not a
           hole in what the card knows. */
        Assert.Equal("CPU 96%, Deadlocks 2", card.StatusReason);
    }

    /// <summary>"Unknown" is the one word with nothing behind it: no connection check has run. Not knowing
    /// whether a server is reachable is no reason to withhold the CPU number that WAS collected.</summary>
    [Fact]
    public void TheTooltip_OnAnUncheckedCard_StillReportsWhatWasCollected()
    {
        var tooltip = NotYetChecked().StatusTooltip;

        Assert.Contains("Unknown — this server has not been connection-checked yet", tooltip, StringComparison.Ordinal);
        Assert.Contains("Needs attention: CPU 62%", tooltip, StringComparison.Ordinal);
    }

    /// <summary>Every card gets a tooltip, every tooltip ends on the gesture that acts on it, and no tooltip is
    /// ever the bare word the reader already read.</summary>
    [Fact]
    public void EveryCard_GetsATooltipThatSaysSomethingTheWordDidNot()
    {
        foreach (var card in EveryState())
        {
            var tooltip = card.StatusTooltip;

            Assert.NotEqual(card.StatusDisplay, tooltip);
            Assert.Contains(card.StatusDisplay + " — ", tooltip, StringComparison.Ordinal);
            Assert.EndsWith("Double-click the card to open this server's tab", tooltip, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The word, the colour and the tooltip's first line are three renderings of ONE discriminant, which is what
    /// makes them incapable of disagreeing rather than merely observed not to. The viewer arrived at the same
    /// collapse over two review rounds on #2429, each of which found a different flag pair where two independent
    /// readings contradicted each other.
    ///
    /// <para>Since #2458 the ladder is <c>ServerCardStatusRules.Classify</c> rather than a member of this class,
    /// and the count below spans the sidebar's file too. The reason is the shape of what this pin missed: it
    /// counted one literal in ONE file, which was true and stayed true while a fourth copy of the same ladder sat
    /// in <c>Lite/Models/ServerConnection.cs</c> driving the sidebar dot. A pin scoped to one file cannot see the
    /// duplicate it exists to forbid.</para>
    /// </summary>
    [Fact]
    public void TheCardStatus_IsTheOnlyPlaceTheStatusFlagsAreRead()
    {
        Assert.Equal(ServerCardStatus.Online, Healthy().CardStatus);
        Assert.Equal(ServerCardStatus.CollectorErrors, CollectorsFailing().CardStatus);
        Assert.Equal(ServerCardStatus.Offline, Offline().CardStatus);
        Assert.Equal(ServerCardStatus.Unknown, NotYetChecked().CardStatus);

        /* An offline card's collector-error marker must not turn it amber — the flags are read once, in order. */
        var offlineAndErroring = Offline();
        offlineAndErroring.HasCollectorErrors = true;
        Assert.Equal(ServerCardStatus.Offline, offlineAndErroring.CardStatus);
        Assert.Equal("Offline", offlineAndErroring.StatusDisplay);
        Assert.Contains("Offline", offlineAndErroring.StatusTooltip, StringComparison.Ordinal);

        var source = ReadRepoFile(Path.Combine("Lite", "Services", "LocalDataService.Overview.cs"));
        Assert.Contains(
            "public static ServerCardStatus Classify(bool? isOnline, bool hasCollectorErrors) => isOnline switch",
            source, StringComparison.Ordinal);
        Assert.Contains(
            "public ServerCardStatus CardStatus => ServerCardStatusRules.Classify(IsOnline, HasCollectorErrors);",
            source, StringComparison.Ordinal);
        Assert.Contains("public string StatusDisplay => CardStatus.Word();", source, StringComparison.Ordinal);
        Assert.Contains("public SolidColorBrush StatusBrush => MakeBrush(CardStatus switch", source, StringComparison.Ordinal);
        Assert.Contains("private string StatusHeadline => CardStatus.Headline();", source, StringComparison.Ordinal);

        /* And exactly one switch on the flag itself, counted across BOTH files that render it. This is the
           assertion behind the claim in ServerCardStatus's doc comment; without it the claim is prose that
           review has to re-check by hand, which is how it came to overstate what was true in the first place
           (raised on #2451). The literal moved with the ladder — it is now the classifier's own parameter
           list — and the card may not switch on the property again. The sidebar's file is scanned for either
           casing, because the copy #2458 removed was the property-cased one. */
        var sidebar = ReadRepoFile(Path.Combine("Lite", "Models", "ServerConnection.cs"));
        Assert.Equal(1, CountOccurrences(source, "isOnline switch"));
        Assert.Equal(0, CountOccurrences(source, "IsOnline switch"));
        Assert.Equal(0, CountOccurrences(sidebar, "sOnline switch"));
    }

    /// <summary>
    /// The card's BORDER renders the same discriminant its word does. Review on #2451 found the last place it
    /// did not: <c>CardBorderBrush</c> read <c>IsOffline</c> and <c>HasCollectorErrors</c> raw, so it agreed
    /// with the status word by coincidence rather than by construction — and on one pair it already did not
    /// agree. An unchecked card carrying a collector-error marker drew the amber "collectors failing" border
    /// while its word read "Unknown". The loader only sets that marker when the connection check succeeded, so
    /// the pair is unreached in practice, which is the argument for making it unrepresentable rather than
    /// leaving a caller to keep avoiding it.
    /// </summary>
    [Fact]
    public void TheCardBorder_RendersTheSameDiscriminantTheWordDoes()
    {
        var neutral = Healthy().CardBorderBrush.Color;

        var notChecked = NotYetChecked();
        notChecked.HasCollectorErrors = true;

        Assert.Equal(ServerCardStatus.Unknown, notChecked.CardStatus);
        Assert.Equal("Unknown", notChecked.StatusDisplay);
        Assert.DoesNotContain("collectors", notChecked.StatusTooltip, StringComparison.Ordinal);
        Assert.Equal(neutral, notChecked.CardBorderBrush.Color);

        /* A card that IS erroring still gets its amber border — and it is now literally the same amber the
           status word is painted, because both render the same state. */
        var erroring = CollectorsFailing();
        Assert.NotEqual(neutral, erroring.CardBorderBrush.Color);
        Assert.Equal(erroring.StatusBrush.Color, erroring.CardBorderBrush.Color);

        /* Precedence is unchanged: a dark server outranks its own metrics. */
        Assert.Equal(Offline().CardBorderBrush.Color, Busy().CardBorderBrush.Color);

        var source = ReadRepoFile(Path.Combine("Lite", "Services", "LocalDataService.Overview.cs"));
        Assert.Contains("CardStatus == ServerCardStatus.Offline ? \"#E57373\"", source, StringComparison.Ordinal);
        Assert.Contains("CardStatus == ServerCardStatus.CollectorErrors ? \"#FFD54F\"", source, StringComparison.Ordinal);
        Assert.Contains("public bool IsOffline => CardStatus == ServerCardStatus.Offline;", source, StringComparison.Ordinal);
        Assert.DoesNotContain("IsOffline ? \"#E57373\"", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The row brushes and the reason read the SAME gates. This is the source-level half of
    /// <see cref="TheTooltip_NamesExactlyTheRowsTheCardHasColoured"/>: that test proves they agree today, this
    /// one forbids the second copy of a threshold that would let them stop agreeing later. Memory is absent from
    /// both because the Memory row carries no severity brush — there is no band on this card to report.
    /// </summary>
    [Fact]
    public void TheRowBrushes_AndTheReason_ReadOneGateEach()
    {
        var source = ReadRepoFile(Path.Combine("Lite", "Services", "LocalDataService.Overview.cs"));

        Assert.Contains("private bool CpuIsElevated => CpuPercentForAlert >= 50;", source, StringComparison.Ordinal);
        Assert.Contains("private bool CpuIsCritical => CpuPercentForAlert >= 80;", source, StringComparison.Ordinal);
        Assert.Contains("private bool BlockingIsElevated => BlockingCount > 0;", source, StringComparison.Ordinal);
        Assert.Contains("private bool DeadlocksAreElevated => DeadlockCount > 0;", source, StringComparison.Ordinal);

        /* Both readers of each gate, named: the row's brush, and the reason. */
        Assert.Contains("MakeBrush(CpuIsCritical ? \"#E57373\" : CpuIsElevated ? \"#FFB74D\"", source, StringComparison.Ordinal);
        Assert.Contains("MakeBrush(BlockingIsElevated ? \"#FFB74D\"", source, StringComparison.Ordinal);
        Assert.Contains("MakeBrush(DeadlocksAreElevated ? \"#E57373\"", source, StringComparison.Ordinal);
        Assert.Contains("if (CpuIsElevated)", source, StringComparison.Ordinal);
        Assert.Contains("if (BlockingIsElevated)", source, StringComparison.Ordinal);
        Assert.Contains("if (DeadlocksAreElevated)", source, StringComparison.Ordinal);

        /* And no second copy of any threshold. These are the literals the brushes carried before the gates
           existed; a tooltip written against its own copy of them is the drift this whole class is about. */
        Assert.DoesNotContain("CpuPercentForAlert >= 80 ? \"#FFB74D\"", source, StringComparison.Ordinal);
        Assert.DoesNotContain("BlockingCount > 0 ? \"#FFB74D\"", source, StringComparison.Ordinal);
        Assert.DoesNotContain("DeadlockCount > 0 ? \"#E57373\"", source, StringComparison.Ordinal);
    }

    // ── the wiring, which only source can show ─────────────────────────────────────────────────────

    /// <summary>
    /// The card's status line actually carries the tooltip. <c>Background="Transparent"</c> is load-bearing
    /// rather than decorative: a TextBlock with a null Background hit-tests on its rendered glyphs alone, so
    /// the tooltip would appear over the letters of "Warning" and nowhere in the space around them. Removing
    /// either attribute compiles perfectly clean, which is why this reads XAML.
    /// </summary>
    [Fact]
    public void TheCardStatusLine_IsBoundToTheTooltip_AndIsHoverable()
    {
        var xaml = ReadRepoFile(Path.Combine("Lite", "MainWindow.xaml"));

        var at = xaml.IndexOf("Text=\"{Binding StatusDisplay}\"", StringComparison.Ordinal);
        Assert.True(at > 0, "the Overview card's status TextBlock is gone — find where it moved before editing this test");

        var element = xaml[at..(xaml.IndexOf("/>", at, StringComparison.Ordinal) + 2)];

        Assert.Contains("ToolTip=\"{Binding StatusTooltip}\"", element, StringComparison.Ordinal);
        Assert.Contains("Background=\"Transparent\"", element, StringComparison.Ordinal);

        /* The status dot carries it too — it is the same signal, and it is the thing a reader points at first. */
        var dot = xaml.IndexOf("Fill=\"{Binding StatusBrush}\"", StringComparison.Ordinal);
        Assert.True(dot > 0, "the Overview card's status dot is gone");
        Assert.Contains("ToolTip=\"{Binding StatusTooltip}\"", xaml[dot..(xaml.IndexOf("/>", dot, StringComparison.Ordinal) + 2)], StringComparison.Ordinal);
    }

    /// <summary>
    /// The closing line is the viewer's, verbatim. The reason all three surfaces are being fixed together is
    /// that a reader moving between Lite, the viewer and the web dashboard should meet one vocabulary rather
    /// than three levels of helpfulness — so this is a pin, not a coincidence.
    /// </summary>
    [Fact]
    public void TheTooltipsClosingLine_MatchesTheDarlingViewers()
    {
        const string action = "Double-click the card to open this server's tab";

        Assert.EndsWith(action, Healthy().StatusTooltip, StringComparison.Ordinal);

        var viewer = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.Fleet.cs"));
        Assert.Contains("\"" + action + "\"", viewer, StringComparison.Ordinal);
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────────────

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }
        return count;
    }

    /// <summary>Locates a repo file by walking up from this file's compile-time path — the
    /// <c>AlertFiringLogTests</c> / <c>ThemeCompletenessTests</c> idiom, no build-output copying.</summary>
    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate).Replace("\r\n", "\n", StringComparison.Ordinal);
            }
        }

        throw new FileNotFoundException($"Could not locate {relative} walking up from {thisFile}");
    }
}
