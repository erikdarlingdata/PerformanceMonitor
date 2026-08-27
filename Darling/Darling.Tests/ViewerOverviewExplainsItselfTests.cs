/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2424 / #2422: the Overview stops keeping the answer to itself.
///
/// <para><b>What was reported.</b> A card said "Warning" in amber and would not say what about. The reporter's
/// two questions were "what is it that this text warns me about?" and, of "+52 more need attention", "where do I
/// find these warnings?". Both are questions the tab could already answer for itself — it computes a per-server
/// reason and the full problem set, then renders a colour and a count.</para>
///
/// <para><b>What is pinned here.</b> The reason on the card comes from <see cref="FleetRollup.BuildReason"/>
/// rather than from a second derivation — that method's whole value is being built from the card's OWN metric
/// displays, so a tooltip built any other way could contradict the six rows the reader is looking at. And the
/// overflow line reaches the servers it counts, through the same banding that counted them, with the filter's
/// active state visible and clearable.</para>
///
/// <para>The wiring half text-scans SOURCE (located by walking up from this file's compile-time path, the
/// <c>ViewerServerInventoryEnabledTests</c> pattern) because a <c>ToolTip</c> attribute and a click handler live
/// in XAML, where no assertion about a C# object can reach them. Removing either compiles perfectly.</para>
/// </summary>
public sealed class ViewerOverviewExplainsItselfTests
{
    private static ServerSummaryItem Healthy(string name = "h1", int id = 1) =>
        new() { DisplayName = name, ServerId = id, IsOnline = true };

    private static ServerSummaryItem Busy(string name = "b1", int id = 2) =>
        new()
        {
            DisplayName = name,
            ServerId = id,
            IsOnline = true,
            CpuPercent = 96,
            BlockingCount = 6,
            MaxBlockingWaitMs = 70000,
        };

    private static ServerSummaryItem Stale(string name = "s1", int id = 3) =>
        new() { DisplayName = name, ServerId = id, IsOnline = true, HasCollectorErrors = true };

    private static ServerSummaryItem Offline(string name = "o1", int id = 4) =>
        new() { DisplayName = name, ServerId = id, IsOnline = false };

    private static ServerSummaryItem Awaiting(string name = "a1", int id = 5) =>
        new() { DisplayName = name, ServerId = id, IsOnline = null, AwaitingFirstCollection = true };

    /// <summary>The pair StatusDisplay renders as "Unknown": freshness was never classified. ApplyFreshness
    /// cannot produce it, but a fixture or a future data path can, which is the whole point of pinning it.</summary>
    private static ServerSummaryItem UnknownStatus(string name = "u1", int id = 6) =>
        new() { DisplayName = name, ServerId = id, IsOnline = null, AwaitingFirstCollection = false };

    // ── The card explains itself ───────────────────────────────────────────────────────────────────

    /// <summary>
    /// The tooltip is BuildReason's own output, not a paraphrase of it. This is the assertion that matters: it
    /// is what stops a later edit "improving" the card's wording into something that disagrees with the Needs
    /// Attention row for the same server, which is the exact drift BuildReason was written to prevent.
    /// </summary>
    [Fact]
    public void TheCardsTooltip_IsTheSameSentenceTheRankingShows()
    {
        foreach (var card in new[] { Busy(), Stale(), Offline(), Awaiting() })
        {
            Assert.Contains(FleetRollup.BuildReason(card), FleetRollup.BuildStatusTooltip(card), StringComparison.Ordinal);
        }
    }

    /// <summary>The reporter's first question, answered: the amber word now names the metrics behind it.</summary>
    [Fact]
    public void TheCardsTooltip_NamesTheBandAndEveryBadMetric()
    {
        var tooltip = FleetRollup.BuildStatusTooltip(Busy());

        Assert.Contains("Critical", tooltip, StringComparison.Ordinal);
        Assert.Contains("CPU 96%", tooltip, StringComparison.Ordinal);
        Assert.Contains("Blocking 6", tooltip, StringComparison.Ordinal);
    }

    /// <summary>
    /// The reporter's card. <c>StatusDisplay</c> renders "Warning" for exactly one condition — an online server
    /// whose collection has gone stale — and nothing on the card said so. The tooltip has to answer THAT card,
    /// not just the metric-driven ones.
    /// </summary>
    [Fact]
    public void TheCardsTooltip_ExplainsTheAmberWarningThatMeansStaleCollection()
    {
        var stale = Stale();

        Assert.Equal("Warning", stale.StatusDisplay);
        Assert.Contains("Warning", stale.StatusTooltip, StringComparison.Ordinal);
        Assert.Contains("collection stale", stale.StatusTooltip, StringComparison.Ordinal);
    }

    /// <summary>
    /// A green card must not be told it needs attention. BuildReason's fallback is written for a ranking that only
    /// ever holds problem servers; the card grid shows EVERY server, so reusing the fallback unguarded would put
    /// "Needs attention" on every healthy card in the fleet — a worse defect than the silence it replaced.
    /// </summary>
    [Fact]
    public void TheCardsTooltip_OnAHealthyCard_DoesNotClaimItNeedsAttention()
    {
        var tooltip = FleetRollup.BuildStatusTooltip(Healthy());

        Assert.DoesNotContain("Needs attention", tooltip, StringComparison.Ordinal);
        Assert.Contains("Healthy", tooltip, StringComparison.Ordinal);
    }

    /// <summary>Offline and awaiting-first-collection already come back as whole sentences naming themselves, so
    /// the band label is not stamped in front of them a second time.</summary>
    [Fact]
    public void TheCardsTooltip_DoesNotSayOfflineTwice()
    {
        var tooltip = FleetRollup.BuildStatusTooltip(Offline());

        Assert.StartsWith("Offline — no recent collection", tooltip, StringComparison.Ordinal);
        Assert.Equal(1, CountOccurrences(tooltip, "Offline"));
        Assert.StartsWith("Awaiting first collection", FleetRollup.BuildStatusTooltip(Awaiting()), StringComparison.Ordinal);
    }

    /// <summary>
    /// Every arm of <c>StatusDisplay</c> has a matching arm here. The one that does not follow from the band is
    /// "Unknown" — <c>IsOnline</c> null with no first-collection marker — where <c>ClassifyBand</c> falls through
    /// to the metrics and, on an otherwise clean card, lands on Healthy. A tooltip hanging off the word "Unknown"
    /// and reading "Healthy" is the same defect as the silence it replaced, so the two are pinned in lockstep.
    /// Raised in review on #2429; unreachable through <c>ApplyFreshness</c> today, which is why it needs a pin
    /// rather than a bug report.
    /// </summary>
    [Fact]
    public void TheCardsTooltip_NeverContradictsAnUnknownStatus()
    {
        var unknown = UnknownStatus();

        Assert.Equal("Unknown", unknown.StatusDisplay);
        Assert.StartsWith("Unknown — no collection status for this server", unknown.StatusTooltip, StringComparison.Ordinal);
        Assert.DoesNotContain("Healthy", unknown.StatusTooltip, StringComparison.Ordinal);

        /* An unknown card with a genuinely bad metric still gets the metric named — the status word being
           unknown is not a reason to withhold the one thing that IS known. */
        var unknownAndBusy = UnknownStatus();
        unknownAndBusy.CpuPercent = 96;

        Assert.Contains("CPU 96%", unknownAndBusy.StatusTooltip, StringComparison.Ordinal);
        Assert.DoesNotContain("Needs attention", unknownAndBusy.StatusTooltip, StringComparison.Ordinal);
    }

    /// <summary>
    /// The sidebar alert badge's tooltip is the shape being followed — the breakdown, then how to act on it — so
    /// every card tooltip ends on the gesture that gets the reader to the detail. "How can I resolve this warning"
    /// was the other half of the report.
    /// </summary>
    [Fact]
    public void TheCardsTooltip_EndsWithHowToActOnIt()
    {
        foreach (var card in new[] { Healthy(), Busy(), Stale(), Offline(), Awaiting(), UnknownStatus() })
        {
            var tooltip = FleetRollup.BuildStatusTooltip(card);

            Assert.EndsWith("\nDouble-click the card to open this server's tab", tooltip, StringComparison.Ordinal);
            Assert.False(tooltip.StartsWith('\n'), "the reason must come first, on its own line");
        }
    }

    /// <summary>
    /// THE INVARIANT, rather than the two instances of it that review found one at a time. The card's status
    /// word and its tooltip must agree for EVERY combination of the two freshness flags, including the ones
    /// <c>ApplyFreshness</c> cannot reach — both flags are plain settable properties, so a fixture or a new
    /// data path can construct any of them.
    ///
    /// <para>Two rounds on #2429 each surfaced a different contradicting pair (<c>IsOnline</c> null with no
    /// awaiting marker, then <c>IsOnline</c> true WITH one). Guarding those two would have left the third to
    /// be found the same way, so the renderings now share one discriminant
    /// (<see cref="ServerCollectionStatus"/>) and this walks the whole product to prove no pair is left.</para>
    /// </summary>
    [Fact]
    public void TheCardsTooltip_AgreesWithTheStatusWord_ForEveryCombinationOfTheFreshnessFlags()
    {
        bool?[] online = { true, false, null };
        bool[] awaiting = { true, false };
        bool[] stale = { true, false };
        double?[] cpu = { null, 96 };

        /* A long max-wait with a zero event count bands Blocking as Warning while BuildReason's own gate
           (count > 0) skips it — a card outside Healthy with nothing to name, which is the other way the
           ranking-only fallback reaches a tooltip. It is in the sweep because widening the sweep is what
           found it. */
        long[] maxBlockingMs = { 0, 20000 };
        int[] failedCollectors = { 0, 1 };

        /* Status word -> the words no tooltip under it may use, because each names a different state. */
        var forbidden = new Dictionary<string, string[]>(StringComparer.Ordinal)
        {
            ["Online"] = new[] { "Offline", "Awaiting first collection", "Unknown" },
            ["Warning"] = new[] { "Offline", "Awaiting first collection", "Unknown" },
            ["Offline"] = new[] { "Awaiting first collection", "Unknown" },
            ["Awaiting first collection"] = new[] { "Offline", "Unknown" },
            ["Unknown"] = new[] { "Offline", "Awaiting first collection" },
        };

        var seen = new HashSet<string>(StringComparer.Ordinal);
        var cases = 0;

        foreach (var o in online)
        foreach (var a in awaiting)
        foreach (var st in stale)
        foreach (var c in cpu)
        foreach (var blockMs in maxBlockingMs)
        foreach (var failed in failedCollectors)
        {
            var card = new ServerSummaryItem
            {
                DisplayName = "s",
                ServerId = 1,
                IsOnline = o,
                AwaitingFirstCollection = a,
                HasCollectorErrors = st,
                CpuPercent = c,
                MaxBlockingWaitMs = blockMs,
                FailedCollectorCount = failed,
            };

            var word = card.StatusDisplay;
            var tooltip = card.StatusTooltip;
            seen.Add(word);
            cases++;

            var where = $"IsOnline={o?.ToString() ?? "null"} Awaiting={a} Stale={st} Cpu={c?.ToString() ?? "null"} " +
                        $"MaxBlockingMs={blockMs} FailedCollectors={failed} " +
                        $"-> word '{word}' tooltip '{tooltip.Replace("\n", " | ", StringComparison.Ordinal)}'";

            foreach (var banned in forbidden[word])
            {
                Assert.False(tooltip.Contains(banned, StringComparison.Ordinal),
                    $"the tooltip claims '{banned}' under a status word of '{word}'. {where}");
            }

            /* And the ranking-only fallback never reaches a card, whatever the flags say. */
            Assert.False(tooltip.Contains("Needs attention", StringComparison.Ordinal), where);
        }

        Assert.Equal(96, cases);

        /* The sweep really did exercise all five status words, so a future rewrite that collapses one of them
           cannot leave this test passing over a smaller surface than it claims to cover. */
        Assert.Equal(5, seen.Count);
    }

    /// <summary>The word, the colour, the reason and the tooltip are four renderings of ONE discriminant. That is
    /// what makes the sweep above hold by construction instead of by luck.</summary>
    [Fact]
    public void TheCardStatus_IsTheOnlyPlaceTheFreshnessFlagsAreRead()
    {
        Assert.Equal(ServerCollectionStatus.Online, Healthy().CardStatus);
        Assert.Equal(ServerCollectionStatus.Stale, Stale().CardStatus);
        Assert.Equal(ServerCollectionStatus.Offline, Offline().CardStatus);
        Assert.Equal(ServerCollectionStatus.AwaitingFirstCollection, Awaiting().CardStatus);
        Assert.Equal(ServerCollectionStatus.Unknown, UnknownStatus().CardStatus);

        /* The pair the second review round found: awaiting set alongside an online card. The status word has
           always ignored the marker there, and now so does everything downstream of it. */
        var onlineAndAwaiting = Healthy();
        onlineAndAwaiting.AwaitingFirstCollection = true;

        Assert.Equal(ServerCollectionStatus.Online, onlineAndAwaiting.CardStatus);
        Assert.Equal("Online", onlineAndAwaiting.StatusDisplay);
        Assert.DoesNotContain("Awaiting", onlineAndAwaiting.StatusTooltip, StringComparison.Ordinal);
        Assert.DoesNotContain("Awaiting", FleetRollup.BuildReason(onlineAndAwaiting), StringComparison.Ordinal);

        /* The source pin: nothing but CardStatus may branch on the flag triple, and since #2473 the ladder
           itself is not written here either — the card RENDERS PerformanceMonitor.Common's one copy, which
           the sidebar row and the service's two status surfaces also render. The syntax-agnostic half of that
           claim (nobody re-spells the words anywhere) is ViewerSidebarDotRendersTheCardStatusTests'. */
        var overview = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.Overview.cs"));
        Assert.Contains("public ServerCollectionStatus CardStatus =>", overview, StringComparison.Ordinal);
        Assert.Contains(
            "ServerCollectionStatusRules.Classify(IsOnline, HasCollectorErrors, AwaitingFirstCollection);",
            overview, StringComparison.Ordinal);
        Assert.Contains("public string StatusDisplay => CardStatus.Word();", overview, StringComparison.Ordinal);
        Assert.Contains("public SolidColorBrush StatusBrush => MakeBrush(CardStatus switch", overview, StringComparison.Ordinal);
    }

    /// <summary>The card view-model exposes it, because a static nothing binds to fixes nothing.</summary>
    [Fact]
    public void TheCardViewModel_ExposesTheTooltip_WithoutReimplementingIt()
    {
        var card = Busy();

        Assert.Equal(FleetRollup.BuildStatusTooltip(card), card.StatusTooltip);
    }

    // ── "+N more need attention" reaches the servers it counts ─────────────────────────────────────

    /// <summary>
    /// The filter and the count are the SAME banding, so "+52 more need attention" lands on a grid holding
    /// exactly those servers plus the five already listed. Two predicates here would mean a link whose
    /// destination disagrees with its own label, which is the defect wearing a new hat.
    /// </summary>
    [Fact]
    public void TheFilter_KeepsExactlyTheServersTheRollupCounted()
    {
        var fleet = BuildFleet(healthy: 45, critical: 4, warning: 6, offline: 2);
        var rollup = FleetRollup.Build(fleet, new FleetTotals());

        var filtered = FleetRollup.NeedsAttention(fleet);

        Assert.Equal(12, filtered.Count);
        Assert.Equal(rollup.WorstServers.Count + rollup.AdditionalProblemCount, filtered.Count);
        Assert.DoesNotContain(filtered, s => FleetRollup.ClassifyBand(s) == FleetHealthBand.Healthy);

        /* Every server the capped ranking DID list is still in the grid the link lands on — the reader should not
           have to hold five names in their head while looking at the other seven. */
        foreach (var ranked in rollup.WorstServers)
        {
            Assert.Contains(filtered, s => s.ServerId == ranked.ServerId);
        }
    }

    /// <summary>The grid's chosen sort survives the filter — filtering is not a re-order.</summary>
    [Fact]
    public void TheFilter_PreservesTheCallersOrder()
    {
        var fleet = new List<ServerSummaryItem>
        {
            Offline("z-offline", 1),
            Healthy("m-healthy", 2),
            Busy("a-busy", 3),
        };

        Assert.Equal(new[] { "z-offline", "a-busy" }, FleetRollup.NeedsAttention(fleet).Select(s => s.DisplayName));
    }

    /// <summary>
    /// The active state carries its own arithmetic. A filtered grid that looks like an unfiltered one is a worse
    /// bug than the dead-end count it replaced, and the all-clear case — an empty grid with the filter still on —
    /// is the one that would otherwise read as a broken tab.
    /// </summary>
    [Fact]
    public void TheFilter_SaysWhatItDid_IncludingWhenItLeavesNothing()
    {
        Assert.Equal("showing 12 of 57", FleetRollup.AttentionFilterCountText(12, 57));
        Assert.Equal("all 57 servers are healthy", FleetRollup.AttentionFilterCountText(0, 57));
        Assert.Equal("the 1 server monitored is healthy", FleetRollup.AttentionFilterCountText(0, 1));

        /* And an Overview with no cards at all does not report that zero servers are healthy. */
        Assert.Equal("no servers to filter", FleetRollup.AttentionFilterCountText(0, 0));
    }

    /// <summary>
    /// The ranking cap stays at five, deliberately, on a fleet where that hides 52. The roll-up panel is docked to
    /// the top of the Overview and does not scroll — only the card grid beneath it does — so a list that grew with
    /// the fleet would push the cards it points at off the screen and stop being a shortlist. The overflow is
    /// answered by giving it a destination, not by making the shortlist long.
    /// </summary>
    [Fact]
    public void TheRankingCap_StaysShort_BecauseTheOverflowNowHasSomewhereToGo()
    {
        Assert.Equal(5, FleetRollup.DefaultWorstCount);

        var rollup = FleetRollup.Build(BuildFleet(healthy: 5, critical: 40, warning: 12, offline: 0), new FleetTotals());

        Assert.Equal(5, rollup.WorstServers.Count);
        Assert.Equal(47, rollup.AdditionalProblemCount);
        Assert.Equal("+47 more need attention", rollup.AdditionalProblemText);
    }

    // ── The wiring, which only source can show ─────────────────────────────────────────────────────

    private static string Xaml => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml"));

    private static string CodeBehind => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml.cs"));

    /// <summary>
    /// The card's status actually carries the tooltip. <c>Background="Transparent"</c> is load-bearing rather than
    /// decorative: a TextBlock with a null Background hit-tests on its rendered glyphs alone, so the tooltip would
    /// appear over the letters of "Warning" and nowhere in the space around them.
    /// </summary>
    [Fact]
    public void TheCardStatus_IsBoundToTheTooltip_AndIsHoverable()
    {
        var at = Xaml.IndexOf("Text=\"{Binding StatusDisplay}\"", StringComparison.Ordinal);
        Assert.True(at > 0, "the Overview card's status TextBlock is gone — find where it moved before editing this test");

        var element = Xaml[at..(Xaml.IndexOf("/>", at, StringComparison.Ordinal) + 2)];

        Assert.Contains("ToolTip=\"{Binding StatusTooltip}\"", element, StringComparison.Ordinal);
        Assert.Contains("Background=\"Transparent\"", element, StringComparison.Ordinal);
    }

    /// <summary>The overflow line navigates, and looks like it does. A dead count that merely reports a number was
    /// the whole complaint.</summary>
    [Fact]
    public void TheOverflowLine_IsAClickableLinkIntoTheFilter()
    {
        var at = Xaml.IndexOf("x:Name=\"FleetAdditionalProblems\"", StringComparison.Ordinal);
        Assert.True(at > 0, "the '+N more need attention' affordance is gone");

        var element = Xaml[at..(Xaml.IndexOf("</Border>", at, StringComparison.Ordinal) + 9)];

        Assert.Contains("MouseLeftButtonUp=\"FleetAdditionalProblems_Click\"", element, StringComparison.Ordinal);
        Assert.Contains("Cursor=\"Hand\"", element, StringComparison.Ordinal);
        Assert.Contains("{Binding AdditionalProblemText}", element, StringComparison.Ordinal);
        Assert.Contains("FleetAdditionalProblems_Click(object sender", CodeBehind, StringComparison.Ordinal);
    }

    /// <summary>
    /// The filter is clearable and its state is visible: a toggle that reads both ways, wired on BOTH transitions
    /// so unchecking restores the fleet, sitting in the docked roll-up header that never scrolls away from the grid
    /// it shrank, with a count beside it.
    /// </summary>
    [Fact]
    public void TheFilter_IsClearable_AndItsActiveStateIsVisible()
    {
        Assert.Contains("x:Name=\"OverviewAttentionOnlyCheck\"", Xaml, StringComparison.Ordinal);
        Assert.Contains("Checked=\"OverviewAttentionOnlyCheck_Changed\"", Xaml, StringComparison.Ordinal);
        Assert.Contains("Unchecked=\"OverviewAttentionOnlyCheck_Changed\"", Xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"OverviewAttentionCountText\"", Xaml, StringComparison.Ordinal);

        Assert.Contains("OverviewAttentionOnlyCheck_Changed(object sender", CodeBehind, StringComparison.Ordinal);
        Assert.Contains("FleetRollup.AttentionFilterCountText(", CodeBehind, StringComparison.Ordinal);

        /* The colour follows the sentence: this line says either "N servers need attention" or an all-clear, and
           painting the all-clear amber would be a colour contradicting its own text — the same defect in
           miniature. Raised in review on #2429. */
        Assert.Contains("shown > 0 ? \"WarningBrush\" : \"SuccessBrush\"", CodeBehind, StringComparison.Ordinal);
    }

    /// <summary>
    /// One projection seam. The grid is set from the full card set through the filter and from nowhere else, so a
    /// refresh, a re-sort and a tag re-stamp cannot each have their own opinion about whether the filter is on —
    /// and nothing reads the bound list BACK to answer "what cards exist", which is the mistake
    /// <see cref="FleetView"/> exists to prevent on the sidebar and would be just as silent here.
    /// </summary>
    [Fact]
    public void TheGrid_IsProjectedThroughTheFilter_AndNothingReadsTheBoundListBack()
    {
        Assert.Contains("FleetRollup.NeedsAttention(_overviewCards)", CodeBehind, StringComparison.Ordinal);
        Assert.Contains("_overviewCards = cards;", CodeBehind, StringComparison.Ordinal);

        Assert.DoesNotContain("OverviewItemsControl.ItemsSource is IEnumerable<ServerSummaryItem>", CodeBehind, StringComparison.Ordinal);
        Assert.DoesNotContain("OverviewItemsControl.ItemsSource = cards;", CodeBehind, StringComparison.Ordinal);

        /* Exactly two writers: the empty-fleet clear, and the filter projection. */
        Assert.Equal(2, CountOccurrences(CodeBehind, "OverviewItemsControl.ItemsSource ="));
    }

    // ── helpers ────────────────────────────────────────────────────────────────────────────────────

    private static List<ServerSummaryItem> BuildFleet(int healthy, int critical, int warning, int offline)
    {
        var fleet = new List<ServerSummaryItem>();
        var id = 0;

        for (var i = 0; i < healthy; i++)
        {
            fleet.Add(new ServerSummaryItem { DisplayName = $"h{i:00}", ServerId = ++id, IsOnline = true });
        }
        for (var i = 0; i < critical; i++)
        {
            fleet.Add(new ServerSummaryItem { DisplayName = $"c{i:00}", ServerId = ++id, IsOnline = true, DeadlockCount = 1 });
        }
        for (var i = 0; i < warning; i++)
        {
            fleet.Add(new ServerSummaryItem { DisplayName = $"w{i:00}", ServerId = ++id, IsOnline = true, FailedCollectorCount = 1 });
        }
        for (var i = 0; i < offline; i++)
        {
            fleet.Add(new ServerSummaryItem { DisplayName = $"o{i:00}", ServerId = ++id, IsOnline = false });
        }

        return fleet;
    }

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

    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate);
            }
        }

        throw new FileNotFoundException($"Could not locate {relative} walking up from {thisFile}");
    }
}
