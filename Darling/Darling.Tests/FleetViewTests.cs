/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins <see cref="FleetView"/>, the seam that separates "the whole fleet" from "what the sidebar is
/// currently showing" — now a projected tree of group headers and servers, not a flat list.
///
/// Before it, <c>ServerList.ItemsSource</c> WAS the fleet, and call sites read it back to answer "what
/// servers exist?" — the alert-badge painter, the Overview builder, the deep-link resolver, the card
/// click handlers, the schedule editor. That is correct only while the bound list is complete. Once the
/// sidebar gains tag rows, every one of those consumers would silently answer the wrong question if it
/// read the bound list — badges stop painting for collapsed servers, a card click resolves to a header.
/// The rules live here, WPF-free and asserted, rather than in a window.
/// </summary>
public sealed class FleetViewTests
{
    private static DarlingServer Server(int id, string name, bool favorite = false)
    {
        var server = new DarlingServer(id, name, name, isEnabled: true, sqlMajorVersion: 16)
        {
            IsFavorite = favorite
        };
        return server;
    }

    private static DarlingTag Tag(int id, string name, int? parentId = null, int sortOrder = 0) =>
        new(id, name, parentId, sortOrder);

    private static DarlingTagAssignment Assign(int serverId, int tagId) => new(serverId, tagId);

    private static FleetView FleetOf(params DarlingServer[] servers)
    {
        var fleet = new FleetView();
        fleet.SetAll(servers);
        return fleet;
    }

    private static IEnumerable<FleetHeaderRow> Headers(FleetView fleet) => fleet.Visible.OfType<FleetHeaderRow>();

    private static IEnumerable<FleetServerRow> ServerRows(FleetView fleet) => fleet.Visible.OfType<FleetServerRow>();

    // ── The whole-fleet invariants that predate tags ─────────────────────────────────────────────────

    [Fact]
    public void SetAll_PublishesTheWholeFleet_AndProjectsAFlatListUntilTagsExist()
    {
        var fleet = FleetOf(Server(1, "SQL01"), Server(2, "SQL02"));

        Assert.Equal(new[] { 1, 2 }, fleet.All.Select(s => s.ServerId));
        Assert.Equal(2, fleet.TotalCount);

        /* Tags are opt-in: with none defined the projection is a flat server list with no headers, exactly
           as the sidebar looked before tags existed. */
        Assert.Empty(Headers(fleet));
        Assert.Equal(new[] { 1, 2 }, ServerRows(fleet).Select(r => r.Server.ServerId));
        Assert.All(fleet.Visible, r => Assert.Equal(0, r.Depth));
    }

    [Fact]
    public void SetAll_RaisesProjectionChanged_SoTheWindowRebindsOnce()
    {
        var fleet = new FleetView();
        var raised = 0;
        fleet.ProjectionChanged += (_, _) => raised++;

        fleet.SetAll(new[] { Server(1, "SQL01") });

        Assert.Equal(1, raised);
    }

    [Fact]
    public void Find_ResolvesAgainstTheWholeFleet_NotTheProjection()
    {
        var fleet = FleetOf(Server(1, "SQL01"), Server(2, "SQL02"));

        /* The load-bearing rule: a deep link, an Overview card click, or an acknowledge must reach a
           server even when the sidebar is not showing it (collapsed under a tag). */
        Assert.Equal("SQL02", fleet.Find(2)!.ServerName);
        Assert.Null(fleet.Find(999));
    }

    [Fact]
    public void SetAll_ReplacesRatherThanAccumulates()
    {
        var fleet = FleetOf(Server(1, "SQL01"), Server(2, "SQL02"));

        fleet.SetAll(new[] { Server(3, "SQL03") });

        Assert.Equal(new[] { 3 }, fleet.All.Select(s => s.ServerId));
        Assert.Null(fleet.Find(1));
    }

    // ── Selection through mixed rows ─────────────────────────────────────────────────────────────────

    [Fact]
    public void ResolveSelection_KeepsThePreviousServer_WhenItSurvives()
    {
        var fleet = FleetOf(Server(1, "SQL01"), Server(2, "SQL02"), Server(3, "SQL03"));

        Assert.Equal(3, fleet.ResolveSelection(previousServerId: 3)!.Server.ServerId);
    }

    [Fact]
    public void ResolveSelection_FallsBackToTheFirstVisibleServer()
    {
        var fleet = FleetOf(Server(7, "SQL07"), Server(8, "SQL08"));

        Assert.Equal(7, fleet.ResolveSelection(previousServerId: 999)!.Server.ServerId);
        Assert.Equal(7, fleet.ResolveSelection(previousServerId: null)!.Server.ServerId);
    }

    [Fact]
    public void ResolveSelection_IsNullWhenThereIsNothingToSelect()
    {
        var fleet = FleetOf();

        Assert.Null(fleet.ResolveSelection(previousServerId: null));
        Assert.Null(fleet.ResolveSelection(previousServerId: 1));
    }

    [Fact]
    public void ResolveSelection_ReturnsAServerRow_NeverAHeader()
    {
        var fleet = new FleetView();
        fleet.SetAll(new[] { Server(1, "SQL01") });
        fleet.SetTags(new[] { Tag(10, "Prod") }, new[] { Assign(1, 10) });

        /* Row 0 is now the Prod header; ResolveSelection must skip it and land on the server. */
        var selected = fleet.ResolveSelection(previousServerId: null);
        Assert.NotNull(selected);
        Assert.Equal(1, selected!.Server.ServerId);
    }

    [Fact]
    public void ResolveSelection_WhenThePreviousServerIsCollapsedAway_FallsBackToFirstVisible()
    {
        var fleet = new FleetView();
        fleet.SetAll(new[] { Server(1, "SQL01"), Server(2, "SQL02") });
        fleet.SetTags(
            new[] { Tag(10, "Prod"), Tag(20, "Staging") },
            new[] { Assign(1, 10), Assign(2, 20) });

        fleet.ToggleExpanded(Headers(fleet).Single(h => h.Title == "Prod"));

        /* Server 1 is now hidden under a collapsed Prod; the resolver skips to the first VISIBLE server. */
        Assert.Equal(2, fleet.ResolveSelection(previousServerId: 1)!.Server.ServerId);
    }

    // ── The tag tree projection ──────────────────────────────────────────────────────────────────────

    [Fact]
    public void SetTags_ProjectsFavorites_ThenTags_ThenUntagged()
    {
        var fleet = new FleetView();
        fleet.SetAll(new[] { Server(1, "SQL01", favorite: true), Server(2, "SQL02"), Server(3, "SQL03") });
        fleet.SetTags(new[] { Tag(10, "Prod") }, new[] { Assign(1, 10), Assign(2, 10) });

        var headers = Headers(fleet).Select(h => (h.Kind, h.Title, h.ServerCount)).ToList();
        Assert.Equal(
            new[]
            {
                (FleetGroupKind.Favorites, "Favorites", 1),
                (FleetGroupKind.Tag, "Prod", 2),
                (FleetGroupKind.Untagged, "Untagged", 1),
            },
            headers);
    }

    [Fact]
    public void SetTags_FavoritesGroupAppearsOnlyWhenAFavoriteExists_AndAlsoUnderItsTag()
    {
        var withFavorite = new FleetView();
        withFavorite.SetAll(new[] { Server(1, "SQL01", favorite: true) });
        withFavorite.SetTags(new[] { Tag(10, "Prod") }, new[] { Assign(1, 10) });

        Assert.Contains(Headers(withFavorite), h => h.Kind == FleetGroupKind.Favorites);
        /* The favourite appears in Favorites AND under Prod — the same duplication as any multi-tag. */
        Assert.Equal(2, ServerRows(withFavorite).Count(r => r.Server.ServerId == 1));

        var noFavorite = new FleetView();
        noFavorite.SetAll(new[] { Server(1, "SQL01") });
        noFavorite.SetTags(new[] { Tag(10, "Prod") }, new[] { Assign(1, 10) });

        Assert.DoesNotContain(Headers(noFavorite), h => h.Kind == FleetGroupKind.Favorites);
    }

    [Fact]
    public void SetTags_ServerWithMultipleTags_AppearsUnderEachTag()
    {
        var fleet = new FleetView();
        fleet.SetAll(new[] { Server(1, "SQL01") });
        fleet.SetTags(
            new[] { Tag(10, "Prod"), Tag(20, "Staging") },
            new[] { Assign(1, 10), Assign(1, 20) });

        Assert.Equal(2, ServerRows(fleet).Count(r => r.Server.ServerId == 1));
        /* A server carrying any tag is never "untagged". */
        Assert.DoesNotContain(Headers(fleet), h => h.Kind == FleetGroupKind.Untagged);
    }

    [Fact]
    public void SetTags_NestedTags_IndentByDepth_ChildTagsBeforeServers()
    {
        var fleet = new FleetView();
        fleet.SetAll(new[] { Server(1, "SQL01") });
        fleet.SetTags(
            new[] { Tag(10, "Prod"), Tag(11, "US", parentId: 10) },
            new[] { Assign(1, 11) });

        var rows = fleet.Visible.ToList();
        var prod = rows.OfType<FleetHeaderRow>().Single(h => h.Title == "Prod");
        var us = rows.OfType<FleetHeaderRow>().Single(h => h.Title == "US");
        var server = rows.OfType<FleetServerRow>().Single(r => r.Server.ServerId == 1);

        Assert.Equal(0, prod.Depth);
        Assert.Equal(1, us.Depth);
        Assert.Equal(2, server.Depth);
        /* Child tags emit before the parent's own servers, so a nested header never appears below the
           rows it contains. */
        Assert.True(rows.IndexOf(us) < rows.IndexOf(server));
    }

    [Fact]
    public void SetTags_UntaggedGroup_HoldsOnlyServersWithNoAssignment()
    {
        var fleet = new FleetView();
        fleet.SetAll(new[] { Server(1, "SQL01"), Server(2, "SQL02") });
        fleet.SetTags(new[] { Tag(10, "Prod") }, new[] { Assign(1, 10) });

        var untagged = Headers(fleet).Single(h => h.Kind == FleetGroupKind.Untagged);
        Assert.Equal(1, untagged.ServerCount);

        /* Server 2 is the only untagged one; it appears under Untagged, not under Prod. */
        var prodIndex = fleet.Visible.ToList().FindIndex(r => r is FleetHeaderRow { Title: "Prod" });
        var untaggedIndex = fleet.Visible.ToList().FindIndex(r => r is FleetHeaderRow { Kind: FleetGroupKind.Untagged });
        Assert.True(prodIndex < untaggedIndex);
    }

    [Fact]
    public void EmptyTags_CollapsesBackToTheFlatList()
    {
        var fleet = new FleetView();
        fleet.SetAll(new[] { Server(1, "SQL01") });
        fleet.SetTags(new[] { Tag(10, "Prod") }, new[] { Assign(1, 10) });
        Assert.NotEmpty(Headers(fleet));

        /* Deleting the last tag returns the sidebar to the plain server list. */
        fleet.SetTags(System.Array.Empty<DarlingTag>(), System.Array.Empty<DarlingTagAssignment>());

        Assert.Empty(Headers(fleet));
        Assert.Equal(new[] { 1 }, ServerRows(fleet).Select(r => r.Server.ServerId));
    }

    // ── Expand / collapse ────────────────────────────────────────────────────────────────────────────

    [Fact]
    public void ToggleExpanded_Collapse_HidesDescendants_ButKeepsTheChevron()
    {
        var fleet = new FleetView();
        fleet.SetAll(new[] { Server(1, "SQL01"), Server(2, "SQL02") });
        fleet.SetTags(new[] { Tag(10, "Prod") }, new[] { Assign(1, 10), Assign(2, 10) });

        Assert.Equal(2, ServerRows(fleet).Count());

        fleet.ToggleExpanded(Headers(fleet).Single(h => h.Title == "Prod"));

        var prod = Headers(fleet).Single(h => h.Title == "Prod");
        Assert.False(prod.IsExpanded);
        Assert.True(prod.HasChildren);
        Assert.Empty(ServerRows(fleet));
    }

    [Fact]
    public void CollapseState_SurvivesADataReload_KeyedByGroupIdentity()
    {
        var fleet = new FleetView();
        fleet.SetAll(new[] { Server(1, "SQL01") });
        fleet.SetTags(new[] { Tag(10, "Prod") }, new[] { Assign(1, 10) });
        fleet.ToggleExpanded(Headers(fleet).Single(h => h.Title == "Prod"));

        /* A status refresh reloads tags/assignments; the collapse must persist (keyed by tag id, so a
           rename would keep it too). */
        fleet.SetTags(new[] { Tag(10, "Prod") }, new[] { Assign(1, 10) });

        Assert.False(Headers(fleet).Single(h => h.Title == "Prod").IsExpanded);
    }

    [Fact]
    public void SetCollapsedKeys_RestoresPersistedExpandState()
    {
        var fleet = new FleetView();
        fleet.SetAll(new[] { Server(1, "SQL01") });
        fleet.SetTags(new[] { Tag(10, "Prod") }, new[] { Assign(1, 10) });

        fleet.SetCollapsedKeys(new[] { FleetGroupKey.ForTag(10) });

        Assert.False(Headers(fleet).Single(h => h.Title == "Prod").IsExpanded);
        Assert.Contains(FleetGroupKey.ForTag(10), fleet.CollapsedKeys);
    }

    // ── Defensive: a hand-edited store must never lose servers or hang ────────────────────────────────

    [Fact]
    public void Rebuild_DanglingParent_SurfacesTheTagAsARoot_WithoutLosingItsServers()
    {
        var fleet = new FleetView();
        fleet.SetAll(new[] { Server(1, "SQL01") });
        fleet.SetTags(new[] { Tag(11, "Orphan", parentId: 99) }, new[] { Assign(1, 11) });

        var orphan = Headers(fleet).Single(h => h.Title == "Orphan");
        Assert.Equal(0, orphan.Depth);
        Assert.Contains(ServerRows(fleet), r => r.Server.ServerId == 1);
    }

    [Fact]
    public void Rebuild_CyclicTags_Terminate_AndRenderEachTagOnce()
    {
        var fleet = new FleetView();
        fleet.SetAll(new[] { Server(1, "SQL01") });

        /* A pure 10↔11 cycle: neither tag is a root, so without the unvisited-tag fallback nothing would
           render and the server would vanish. */
        fleet.SetTags(
            new[] { Tag(10, "A", parentId: 11), Tag(11, "B", parentId: 10) },
            new[] { Assign(1, 10) });

        Assert.Equal(2, Headers(fleet).Count(h => h.Kind == FleetGroupKind.Tag));
        Assert.Contains(ServerRows(fleet), r => r.Server.ServerId == 1);
    }

    // ── FleetGroupKey persistence round-trip (expand/collapse survives a restart) ─────────────────────

    [Theory]
    [InlineData("Favorites")]
    [InlineData("Untagged")]
    public void FleetGroupKey_RoundTripsThePseudoGroups(string stored)
    {
        Assert.True(FleetGroupKey.TryParse(stored, out var key));
        Assert.Equal(stored, key.ToStorageString());
    }

    [Fact]
    public void FleetGroupKey_RoundTripsATag()
    {
        var key = FleetGroupKey.ForTag(42);

        Assert.Equal("Tag:42", key.ToStorageString());
        Assert.True(FleetGroupKey.TryParse("Tag:42", out var parsed));
        Assert.Equal(key, parsed);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("Bogus")]
    [InlineData("Tag:")]
    [InlineData("Tag:notanumber")]
    public void FleetGroupKey_TryParse_RejectsUnrecognizedOrStaleForms(string? stored)
    {
        Assert.False(FleetGroupKey.TryParse(stored, out _));
    }

    // ── Server search: the reserved Rebuild seam (name + tag filter) ──────────────────────────────────

    [Fact]
    public void Search_FlatList_FiltersByNameCaseInsensitiveSubstring()
    {
        var fleet = FleetOf(Server(1, "sql-prod-01"), Server(2, "sql-dev-02"), Server(3, "app-prod-03"));

        fleet.SetSearch("PROD");

        Assert.Equal(new[] { "sql-prod-01", "app-prod-03" }, ServerRows(fleet).Select(r => r.Server.ServerName));
        Assert.True(fleet.IsSearching);
        Assert.Equal(2, fleet.VisibleServerCount);
        Assert.Equal(3, fleet.TotalCount);   // a filter narrows what shows, never the fleet itself
    }

    [Fact]
    public void Search_EmptyOrWhitespace_RestoresTheWholeList()
    {
        var fleet = FleetOf(Server(1, "alpha"), Server(2, "beta"));

        fleet.SetSearch("alpha");
        Assert.Single(ServerRows(fleet));

        fleet.SetSearch("   ");
        Assert.False(fleet.IsSearching);
        Assert.Equal(2, ServerRows(fleet).Count());
    }

    [Fact]
    public void Search_MatchesByTagName_EvenWhenTheServerNameDoesNot()
    {
        var fleet = FleetOf(Server(1, "sql-01"), Server(2, "sql-02"));
        fleet.SetTags(new[] { Tag(10, "Production") }, new[] { Assign(1, 10) });

        fleet.SetSearch("production");

        /* sql-01 matches via its "Production" tag; sql-02 (untagged, no name match) drops out entirely. */
        Assert.Equal(new[] { "sql-01" }, ServerRows(fleet).Select(r => r.Server.ServerName).Distinct());
        Assert.Contains(Headers(fleet), h => h.Title == "Production");
        Assert.DoesNotContain(Headers(fleet), h => h.Title == "Untagged");
    }

    [Fact]
    public void Search_DropsTagGroupsWithNoMatchAnywhereInTheirSubtree()
    {
        var fleet = FleetOf(Server(1, "web-01"), Server(2, "db-01"));
        fleet.SetTags(
            new[] { Tag(10, "Web"), Tag(20, "Database") },
            new[] { Assign(1, 10), Assign(2, 20) });

        fleet.SetSearch("web");

        Assert.Contains(Headers(fleet), h => h.Title == "Web");
        Assert.DoesNotContain(Headers(fleet), h => h.Title == "Database");
        Assert.Equal(new[] { "web-01" }, ServerRows(fleet).Select(r => r.Server.ServerName));
    }

    [Fact]
    public void Search_NestedMatch_KeepsTheAncestorGroupThatHasNoDirectMatch()
    {
        /* A parent tag with no directly-assigned matching server must still show when a descendant matches,
           or the match would be unreachable. */
        var fleet = FleetOf(Server(1, "sql-01"));
        fleet.SetTags(
            new[] { Tag(10, "Environments"), Tag(20, "Production", parentId: 10) },
            new[] { Assign(1, 20) });

        fleet.SetSearch("sql-01");

        Assert.Contains(Headers(fleet), h => h.Title == "Environments");
        Assert.Contains(Headers(fleet), h => h.Title == "Production");
        Assert.Equal(new[] { "sql-01" }, ServerRows(fleet).Select(r => r.Server.ServerName));
    }

    [Fact]
    public void Search_RevealsAMatchInACollapsedGroup_ThenRestoresCollapseOnClear()
    {
        var fleet = FleetOf(Server(1, "sql-prod"), Server(2, "sql-dev"));
        fleet.SetTags(new[] { Tag(10, "Prod"), Tag(20, "Dev") },
            new[] { Assign(1, 10), Assign(2, 20) });

        /* Collapse the Prod group — its server is hidden. */
        var prodHeader = Headers(fleet).First(h => h.Title == "Prod");
        fleet.ToggleExpanded(prodHeader);
        Assert.DoesNotContain(ServerRows(fleet), r => r.Server.ServerName == "sql-prod");

        /* A search matching the collapsed server reveals it — every group renders expanded while searching. */
        fleet.SetSearch("sql-prod");
        Assert.Contains(ServerRows(fleet), r => r.Server.ServerName == "sql-prod");

        /* Clearing the search restores the persisted collapse: Prod is collapsed again, hiding its server,
           but the header itself is back. */
        fleet.SetSearch(null);
        Assert.DoesNotContain(ServerRows(fleet), r => r.Server.ServerName == "sql-prod");
        Assert.Contains(Headers(fleet), h => h.Title == "Prod");
    }

    /// <summary>#7984: a search for a tag's name finds the servers tagged under it, the same fields as the Overview
    /// search and the same coverage a custom alert rule on the tag has: the server tagged HOSTS under PROD &gt; BULK
    /// is shown for "BULK" and for "PROD", and its branch stays in the tree.</summary>
    [Fact]
    public void Search_MatchesTheTagsAboveAServersTag()
    {
        var fleet = FleetOf(Server(1, "bulk-host-1"), Server(2, "ag-listener"), Server(3, "standby"));
        fleet.SetTags(
            new[] { Tag(1, "PROD"), Tag(2, "BULK", 1), Tag(3, "HOSTS", 2), Tag(4, "LISTENERS", 2), Tag(5, "DR") },
            new[] { Assign(1, 3), Assign(2, 4), Assign(3, 5) });

        fleet.SetSearch("BULK");
        Assert.Equal(new[] { 1, 2 }, ServerRows(fleet).Select(r => r.Server.ServerId).Distinct().OrderBy(i => i));
        Assert.Contains(Headers(fleet), h => h.Title == "BULK");
        Assert.Contains(Headers(fleet), h => h.Title == "HOSTS");

        fleet.SetSearch("prod");
        Assert.Equal(new[] { 1, 2 }, ServerRows(fleet).Select(r => r.Server.ServerId).Distinct().OrderBy(i => i));

        fleet.SetSearch("HOSTS");
        Assert.Equal(new[] { 1 }, ServerRows(fleet).Select(r => r.Server.ServerId).Distinct());

        fleet.SetSearch("DR");
        Assert.Equal(new[] { 3 }, ServerRows(fleet).Select(r => r.Server.ServerId).Distinct());
    }

    /// <summary>#7984: a parent cycle a store can hold, and a parent id with no row, end the walk instead of
    /// hanging or throwing, and the search still reaches the names that exist.</summary>
    [Fact]
    public void Search_EndsOnAParentCycle_AndOnADanglingParent()
    {
        var fleet = FleetOf(Server(1, "looped"), Server(2, "orphaned"));
        fleet.SetTags(
            new[] { Tag(1, "A", 2), Tag(2, "B", 1), Tag(3, "ORPHAN", 99) },
            new[] { Assign(1, 1), Assign(2, 3) });

        /* A cycle has no root, so the tree draws neither tag; the point here is that the search returns. */
        fleet.SetSearch("B");
        Assert.NotNull(fleet.Visible);

        fleet.SetSearch("orphan");
        Assert.Equal(new[] { 2 }, ServerRows(fleet).Select(r => r.Server.ServerId).Distinct());
    }
}
