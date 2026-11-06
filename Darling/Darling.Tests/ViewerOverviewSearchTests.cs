/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5352: the viewer's Overview gets Lite's server-name / tag search. The composition (search AND the
/// needs-attention toggle) lives in the pure <see cref="OverviewCardView"/> so it is testable without WPF; the
/// source pins show the XAML box is wired to the one projection point.
/// </summary>
public sealed class ViewerOverviewSearchTests
{
    private static ServerSummaryItem Card(string name, int id, bool online = true, params string[] tags) =>
        new()
        {
            DisplayName = name,
            ServerName = name + ".corp",
            ServerId = id,
            IsOnline = online,
            TagPills = tags.Select(t => new ServerTagPill(t, null)).ToList(),
        };

    private static List<ServerSummaryItem> Fleet() => new()
    {
        Card("alpha", 1, true, "Production"),
        Card("beta", 2, false, "Production"),   // Offline -> needs attention
        Card("gamma", 3, true, "Dev"),
        Card("delta-PROD", 4, false),           // Offline, name-only match
        Card("epsilon", 5, false, "Dev"),       // Offline, not a prod match: the intersection must drop it
    };

    private static List<string> Names(IEnumerable<ServerSummaryItem> cards) => cards.Select(c => c.DisplayName).ToList();

    [Fact]
    public void Search_MatchesATagName()
    {
        Assert.Equal(new[] { "alpha", "beta", "delta-PROD" }, Names(OverviewCardView.Project(Fleet(), false, "prod")));
    }

    [Fact]
    public void Search_MatchesTheServerOrInstanceName_CaseInsensitively()
    {
        Assert.Equal(new[] { "gamma" }, Names(OverviewCardView.Project(Fleet(), false, "GAMMA")));
        Assert.Equal(new[] { "alpha" }, Names(OverviewCardView.Project(Fleet(), false, "ALPHA.CORP")));
    }

    [Fact]
    public void EmptyOrBlankSearch_ShowsEveryCard_InTheCallersOrder()
    {
        var fleet = Fleet();
        Assert.Equal(Names(fleet), Names(OverviewCardView.Project(fleet, false, "")));
        Assert.Equal(Names(fleet), Names(OverviewCardView.Project(fleet, false, "   ")));
        Assert.Equal(Names(fleet), Names(OverviewCardView.Project(fleet, false, null)));
    }

    [Fact]
    public void SearchAndAttentionOnly_AreTheIntersection()
    {
        // "prod" matches alpha, beta, delta-PROD; attention-only keeps the offline ones.
        Assert.Equal(new[] { "beta", "delta-PROD" }, Names(OverviewCardView.Project(Fleet(), true, "prod")));
        Assert.Equal(new[] { "beta", "delta-PROD", "epsilon" }, Names(OverviewCardView.Project(Fleet(), true, null)));
    }

    [Fact]
    public void NoMatch_ReturnsNone_AndTheLineIsTheNoMatchSentence_NotTheAllClear()
    {
        var fleet = Fleet();
        var shown = OverviewCardView.Project(fleet, false, "zzz");
        Assert.Empty(shown);
        Assert.Equal(ServerOverviewFilter.NoMatchText, OverviewCardView.CountText(fleet, shown.Count, false, "zzz"));

        // With the toggle on as well, the all-clear ("all N servers are healthy") must still not appear.
        shown = OverviewCardView.Project(fleet, true, "zzz");
        Assert.Empty(shown);
        var text = OverviewCardView.CountText(fleet, shown.Count, true, "zzz");
        Assert.Equal(ServerOverviewFilter.NoMatchText, text);
        Assert.DoesNotContain("healthy", text, StringComparison.Ordinal);
    }

    [Fact]
    public void AttentionOnly_WhereTheSearchMatchedOnlyHealthyCards_DoesNotClaimNoMatch()
    {
        var fleet = Fleet();
        var shown = OverviewCardView.Project(fleet, true, "gamma");
        Assert.Empty(shown);
        var text = OverviewCardView.CountText(fleet, shown.Count, true, "gamma");
        Assert.Equal("No server matching the search needs attention.", text);
        Assert.NotEqual(ServerOverviewFilter.NoMatchText, text);
    }

    [Fact]
    public void CountText_IsUnchangedWithoutASearch_AndAbsentWhenNothingNarrowsTheGrid()
    {
        var fleet = Fleet();
        Assert.Null(OverviewCardView.CountText(fleet, fleet.Count, false, null));
        Assert.Equal("showing 3 of 5", OverviewCardView.CountText(fleet, 3, true, null));
        Assert.Equal("showing 3 of 5", OverviewCardView.CountText(fleet, 3, false, "prod"));
        Assert.Equal("no servers to filter", OverviewCardView.CountText(new List<ServerSummaryItem>(), 0, true, null));
        Assert.Null(OverviewCardView.CountText(new List<ServerSummaryItem>(), 0, false, "x"));
    }

    // ── wiring, which only source can show ──────────────────────────────────────────────────────────

    private static string Xaml => ReadRepoFile(System.IO.Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml"));

    private static string CodeBehind => ReadRepoFile(System.IO.Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml.cs"));

    [Fact]
    public void ViewerXaml_HasTheSearchBox_WiredToTheOneProjectionPoint()
    {
        Assert.Contains("x:Name=\"OverviewSearchBox\"", Xaml, StringComparison.Ordinal);
        Assert.Contains("TextChanged=\"OverviewSearchBox_TextChanged\"", Xaml, StringComparison.Ordinal);
        Assert.Contains("Filter the Overview by server name or tag", Xaml, StringComparison.Ordinal);
        Assert.Contains("server name / tag", Xaml, StringComparison.Ordinal);

        var at = CodeBehind.IndexOf("void OverviewSearchBox_TextChanged(", StringComparison.Ordinal);
        Assert.True(at >= 0, "the handler must exist");
        var body = CodeBehind.Substring(at, 160);
        Assert.Contains("ApplyOverviewCardFilter()", body, StringComparison.Ordinal);

        /* The projection reads the box, so every refresh path through ApplyOverviewCardFilter keeps the search. */
        Assert.Contains("OverviewCardView.Project(_overviewCards, _overviewAttentionOnly, OverviewSearchBox?.Text)",
            CodeBehind, StringComparison.Ordinal);
    }

    [Fact]
    public void Lite_ShowsTheSameNoMatchSentence()
    {
        var lite = ReadRepoFile(System.IO.Path.Combine("Lite", "MainWindow.xaml.cs"));
        Assert.Contains("ServerOverviewFilter.NoMatchText", lite, StringComparison.Ordinal);
        Assert.Contains("OverviewCardView.CountText(", CodeBehind, StringComparison.Ordinal);
    }

    /// <summary>A search narrows every number on the roll-up panel from the same cards: the blocking and deadlock
    /// totals come from the servers the search left, not from the fleet-wide read, so the totals and the band counts
    /// under them describe the same servers. Each card adds the unit the fleet-wide read adds for it: a SQL Server
    /// card its blocked waits, a PostgreSQL card its captures that saw blocking, and a PostgreSQL card with no capture
    /// in the window nothing. Without a search the fleet-wide read still stands.</summary>
    [Fact]
    public void BuildRollup_WithASearch_TotalsOnlyTheMatchingCards()
    {
        var alpha = Card("alpha", 1, true, "Production");
        alpha.BlockingCount = 5;
        alpha.DeadlockCount = 2;
        var beta = Card("beta", 2, true, "Dev");
        beta.BlockingCount = 7;
        beta.DeadlockCount = 3;
        /* A PostgreSQL card's blocking is its captures that saw blocking; BlockingCount stays 0 on it. */
        var gamma = Card("gamma", 3, true, "Production");
        gamma.IsPostgres = true;
        gamma.PgBlockingCapturesTotal = 60;
        gamma.PgBlockingCapturesWithBlocking = 4;
        gamma.DeadlockCount = 1;
        /* A PostgreSQL card with no capture in the window has no blocking reading, and adds nothing. */
        var delta = Card("delta", 4, true, "Dev");
        delta.IsPostgres = true;
        var cards = new List<ServerSummaryItem> { alpha, beta, gamma, delta };
        var fleetWide = new FleetTotals { TotalBlockingEvents = 16, TotalDeadlocks = 6 };

        var whole = OverviewCardView.BuildRollup(cards, fleetWide, registeredCount: 4, search: null);
        Assert.Equal(16, whole.TotalBlockingEvents);
        Assert.Equal(6, whole.TotalDeadlocks);

        var narrowed = OverviewCardView.BuildRollup(cards, fleetWide, registeredCount: 4, search: "alpha");
        Assert.Equal(1, narrowed.TotalServers);
        Assert.Equal(5, narrowed.TotalBlockingEvents);
        Assert.Equal(2, narrowed.TotalDeadlocks);

        /* The Production tag spans a SQL Server card and a PostgreSQL card: 5 waits plus 4 captures. */
        var production = OverviewCardView.BuildRollup(cards, fleetWide, registeredCount: 4, search: "production");
        Assert.Equal(2, production.TotalServers);
        Assert.Equal(9, production.TotalBlockingEvents);
        Assert.Equal(3, production.TotalDeadlocks);

        var dev = OverviewCardView.BuildRollup(cards, fleetWide, registeredCount: 4, search: "dev");
        Assert.Equal(2, dev.TotalServers);
        Assert.Equal(7, dev.TotalBlockingEvents);
        Assert.Equal(3, dev.TotalDeadlocks);

        var none = OverviewCardView.BuildRollup(cards, fleetWide, registeredCount: 4, search: "nothing-matches");
        Assert.Equal(0, none.TotalBlockingEvents);
        Assert.Equal(0, none.TotalDeadlocks);
    }

    // ── #7984: a search matches the tags above a card's tags, the way an alert rule on a tag covers its subtree ──

    private static DarlingTag Tag(int id, string name, int? parentId = null) => new(id, name, parentId, 0);

    /* PROD > BULK > HOSTS and PROD > BULK > LISTENERS, plus DR as a second root. */
    private static List<DarlingTag> Forest() => new()
    {
        Tag(1, "PROD"), Tag(2, "BULK", 1), Tag(3, "HOSTS", 2), Tag(4, "LISTENERS", 2), Tag(5, "DR"),
    };

    /// <summary>The Viewer's pill builder, reached by name so this file compiles against a Viewer that stamps pills
    /// in its window code and fails here, at an assert, rather than at the compiler.</summary>
    private static IReadOnlyList<ServerTagPill> Pills(IReadOnlyList<DarlingTag> tags, params int[] assigned)
    {
        var builder = typeof(OverviewCardView).GetMethod("TagPills", BindingFlags.Public | BindingFlags.Static);
        Assert.True(builder is not null,
            "OverviewCardView.TagPills does not exist: a pill carries only its own tag's name, so a search for the name of a tag above it matches no server.");
        return (IReadOnlyList<ServerTagPill>)builder!.Invoke(null, new object[] { tags, assigned })!;
    }

    private static ServerSummaryItem TaggedCard(string name, int id, IReadOnlyList<DarlingTag> tags, params int[] assigned) =>
        new() { DisplayName = name, ServerName = name + ".corp", ServerId = id, IsOnline = true, TagPills = Pills(tags, assigned) };

    [Fact]
    public void Search_MatchesTheNameOfEveryTagAboveAnAssignedTag()
    {
        var cards = new List<ServerSummaryItem>
        {
            TaggedCard("bulk-host-1", 1, Forest(), 3),
            TaggedCard("ag-listener", 2, Forest(), 4),
            TaggedCard("standby", 3, Forest(), 5),
        };

        Assert.Equal(new[] { "bulk-host-1", "ag-listener" }, Names(OverviewCardView.Project(cards, false, "BULK")));
        Assert.Equal(new[] { "bulk-host-1", "ag-listener" }, Names(OverviewCardView.Project(cards, false, "prod")));
        Assert.Equal(new[] { "bulk-host-1" }, Names(OverviewCardView.Project(cards, false, "HOSTS")));
        Assert.Equal(new[] { "ag-listener" }, Names(OverviewCardView.Project(cards, false, "listeners")));
        Assert.Equal(new[] { "standby" }, Names(OverviewCardView.Project(cards, false, "DR")));
        Assert.Equal(ServerOverviewFilter.NoMatchText, OverviewCardView.CountText(cards, 0, false, "zzz"));
    }

    [Fact]
    public void ThePill_StillShowsOnlyTheAssignedTag_OrderedByName()
    {
        var pills = Pills(Forest(), 4, 3);
        Assert.Equal(new[] { "HOSTS", "LISTENERS" }, pills.Select(p => p.Name));

        /* An assignment to a tag that no longer exists stamps nothing. */
        Assert.Empty(Pills(Forest(), 42));
    }

    [Fact]
    public void BuildRollup_WithAParentTagSearch_CountsTheServersUnderIt()
    {
        var cards = new List<ServerSummaryItem>
        {
            TaggedCard("bulk-host-1", 1, Forest(), 3),
            TaggedCard("ag-listener", 2, Forest(), 4),
            TaggedCard("standby", 3, Forest(), 5),
        };

        var bulk = OverviewCardView.BuildRollup(cards, new FleetTotals(), registeredCount: 3, search: "BULK");
        Assert.Equal(2, bulk.TotalServers);
        Assert.Equal(3, bulk.FilteredOfTotal);
    }

    [Fact]
    public void AParentCycle_ADanglingParent_AndADeepChain_AllEndTheWalk()
    {
        /* A under B under A cannot be written through the tag rules, but a store can hold it; the walk must end. */
        var cycle = new List<DarlingTag> { Tag(1, "A", 2), Tag(2, "B", 1) };
        var looped = TaggedCard("looped", 1, cycle, 1);
        Assert.Equal(new[] { "looped" }, Names(OverviewCardView.Project(new[] { looped }, false, "B")));
        Assert.Equal(new[] { "A", "B" }, OverviewCardView.SearchFields(looped).Skip(2));

        /* A parent id with no row contributes no name and is not an error. */
        var dangling = new List<DarlingTag> { Tag(1, "ORPHAN", 99) };
        var orphan = TaggedCard("orphaned", 2, dangling, 1);
        Assert.Equal(new[] { "orphaned" }, Names(OverviewCardView.Project(new[] { orphan }, false, "orphan")));
        Assert.Equal(new[] { "ORPHAN" }, OverviewCardView.SearchFields(orphan).Skip(2));

        /* A chain far deeper than the tag rules allow still stamps, and the nearest parents are searchable. */
        var chain = Enumerable.Range(1, 200).Select(i => Tag(i, "level-" + i, i == 1 ? null : i - 1)).ToList();
        var deep = TaggedCard("deep", 3, chain, 200);
        Assert.Equal(new[] { "deep" }, Names(OverviewCardView.Project(new[] { deep }, false, "level-199")));
        Assert.Equal(new[] { "deep" }, Names(OverviewCardView.Project(new[] { deep }, false, "level-150")));
    }
}
