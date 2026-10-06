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
}
