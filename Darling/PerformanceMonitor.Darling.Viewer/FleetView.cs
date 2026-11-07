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
using PerformanceMonitor.Common.Tags;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Owns the fleet: the AUTHORITATIVE full server set (<see cref="All"/>) and the projected rows the
/// sidebar renders (<see cref="Visible"/>) — now a mix of group headers and servers, not a flat server
/// list.
///
/// <para><b>Why this exists.</b> Before this class, <c>ServerList.ItemsSource</c> WAS the fleet: call
/// sites read it back with <c>as IEnumerable&lt;DarlingServer&gt;</c> to answer "what servers exist?" —
/// the alert-badge painter, the Overview builder, the deep-link resolver, the card click handlers, the
/// schedule editor. That is fine only while the bound list is the complete list. The moment the sidebar
/// shows a SUBSET or a MIXED row type (tag headers above servers), every one of those consumers silently
/// answers the wrong question. So the two ideas are separated permanently: ask <see cref="All"/> "does
/// this server exist / give me the whole fleet", and bind <see cref="Visible"/> to the list.</para>
///
/// <para><b>Tags are opt-in.</b> With no tags defined, <see cref="Visible"/> is exactly the flat, ordered
/// server list it always was — a viewer that never touches tags sees no change. The grouped tree
/// (Favorites → tags → Untagged) appears only once the user creates their first tag.</para>
///
/// <para>Free of any WPF dependency — <see cref="FleetRow"/> is a plain POCO — so the projection and
/// lookup rules are unit-testable without a window. Filtering (search, tag filter) will slot into
/// <see cref="Rebuild"/> alone, the one seam, so no consumer changes again when it lands.</para>
/// </summary>
internal sealed class FleetView
{
    private List<DarlingServer> _all = new();
    private List<DarlingTag> _tags = new();

    /// <summary>serverId → the set of tag ids assigned to it. Empty/absent = untagged.</summary>
    private Dictionary<int, HashSet<int>> _serverTags = new();

    /// <summary>The groups the user has collapsed. Absence = expanded (the friendly default), so a fresh
    /// fleet shows everything. Keyed by <see cref="FleetGroupKey"/> so state survives reprojection and a
    /// tag rename.</summary>
    private readonly HashSet<FleetGroupKey> _collapsed = new();

    private List<FleetRow> _visible = new();

    /// <summary>The active server-search term, or null for "no filter" (the whole fleet shows). Normalised
    /// (trimmed, empty → null) by <see cref="SetSearch"/> so <see cref="Rebuild"/> can null-check it cheaply.</summary>
    private string? _search;

    /// <summary>Every known server, regardless of what the sidebar is currently showing. This is what a
    /// consumer asking "what is in the fleet?" must read — never the bound list.</summary>
    public IReadOnlyList<DarlingServer> All => _all;

    /// <summary>The rows the sidebar shows: group headers and server rows after projection. Bound to the
    /// list control; a fresh list each rebuild.</summary>
    public IReadOnlyList<FleetRow> Visible => _visible;

    /// <summary>Total fleet size — what the "Servers: N" count reports.</summary>
    public int TotalCount => _all.Count;

    /// <summary>The keys of the currently-collapsed groups, so the window can persist expand/collapse
    /// state across sessions.</summary>
    public IReadOnlyCollection<FleetGroupKey> CollapsedKeys => _collapsed;

    /// <summary>Raised after <see cref="Visible"/> is recomputed, so the window can rebind once.</summary>
    public event EventHandler? ProjectionChanged;

    /// <summary>Replaces the fleet (a load, or a refresh that changed membership) and reprojects. The
    /// caller hands servers already favourites-first sorted; that order is preserved for the flat
    /// (no-tags) projection and for the pseudo-groups.</summary>
    public void SetAll(IEnumerable<DarlingServer> servers)
    {
        ArgumentNullException.ThrowIfNull(servers);

        _all = servers.ToList();
        Rebuild();
    }

    /// <summary>
    /// Replaces the tag forest and the server→tag assignments, then reprojects. Passing an empty tag list
    /// collapses the view back to the flat server list (the opt-in default). Assignments referencing a
    /// server not in <see cref="All"/> are simply never rendered; the reverse index is rebuilt each call.
    /// </summary>
    public void SetTags(IEnumerable<DarlingTag> tags, IEnumerable<DarlingTagAssignment> assignments)
    {
        ArgumentNullException.ThrowIfNull(tags);
        ArgumentNullException.ThrowIfNull(assignments);

        _tags = tags.ToList();

        _serverTags = new Dictionary<int, HashSet<int>>();
        foreach (var a in assignments)
        {
            if (!_serverTags.TryGetValue(a.ServerId, out var set))
            {
                set = new HashSet<int>();
                _serverTags[a.ServerId] = set;
            }

            set.Add(a.TagId);
        }

        Rebuild();
    }

    /// <summary>
    /// Sets the sidebar search term and reprojects — the one seam the class summary reserves for filtering.
    /// A server matches when the term is a case-insensitive substring of its display name, its instance
    /// name, OR any tag name assigned to it (so "prod" finds both <c>sql-prod-01</c> and every server tagged
    /// "Production"). Empty/whitespace clears the filter. While a search is active every group renders
    /// expanded so matches are never hidden behind a collapsed header, and tag groups with no match anywhere
    /// in their subtree drop out entirely; clearing the term restores the persisted collapse state untouched.
    /// </summary>
    public void SetSearch(string? term)
    {
        _search = ServerOverviewFilter.Normalize(term);
        Rebuild();
    }

    /// <summary>True while a server-search term is filtering the projection.</summary>
    public bool IsSearching => _search is not null;

    /// <summary>The number of SERVER rows currently projected (matches under a search, else the whole
    /// fleet) — what the sidebar shows as "Servers: N of <see cref="TotalCount"/>" while filtering.</summary>
    public int VisibleServerCount => _visible.OfType<FleetServerRow>().Select(r => r.Server.ServerId).Distinct().Count();

    /// <summary>Flips a group's expand/collapse state and reprojects. A snapshot-safe toggle: header rows
    /// are immutable snapshots, so this keys on the group identity, not the row instance.</summary>
    public void ToggleExpanded(FleetHeaderRow header)
    {
        ArgumentNullException.ThrowIfNull(header);

        var key = header.Key;
        if (!_collapsed.Remove(key))
        {
            _collapsed.Add(key);
        }

        Rebuild();
    }

    /// <summary>Restores persisted expand/collapse state (the collapsed groups) and reprojects.</summary>
    public void SetCollapsedKeys(IEnumerable<FleetGroupKey> keys)
    {
        ArgumentNullException.ThrowIfNull(keys);

        _collapsed.Clear();
        foreach (var k in keys)
        {
            _collapsed.Add(k);
        }

        Rebuild();
    }

    /// <summary>A group renders expanded when the user has not collapsed it — OR whenever a search is
    /// active, so a match can never hide behind a group the user happened to leave collapsed. The persisted
    /// <see cref="_collapsed"/> set is not touched, so clearing the search restores the exact prior state.</summary>
    private bool IsExpanded(FleetGroupKey key) => _search is not null || !_collapsed.Contains(key);

    /// <summary>
    /// Recomputes <see cref="Visible"/> from the fleet, the tag forest, and the collapse state — the
    /// SINGLE place projection happens. Projection order: the Favorites pseudo-group (when any favourite
    /// exists), the root tags depth-first (child tags before that tag's own servers), then the Untagged
    /// pseudo-group (when any untagged server exists). A collapsed header emits itself and nothing
    /// beneath it. A server carrying multiple tags appears under each — the intended label model.
    /// </summary>
    public void Rebuild()
    {
        var rows = new List<FleetRow>();

        /* No tags anywhere → behaviour-neutral flat list, exactly as before tags existed (a null search
           adds every server; a term filters on name only, there being no tags to match). */
        if (_tags.Count == 0)
        {
            foreach (var s in _all)
            {
                if (_search is null || ServerOverviewFilter.Matches(_search, s.DisplayName, s.ServerName))
                {
                    rows.Add(new FleetServerRow(s, depth: 0));
                }
            }

            _visible = rows;
            ProjectionChanged?.Invoke(this, EventArgs.Empty);
            return;
        }

        /* Search predicate for the tagged projection: a server matches on its name OR on any tag name
           assigned to it OR the name of any tag above one of its tags (#7984), the same fields as the Overview
           search. Built once (tag id → the names that tag answers to), then applied wherever servers enter the
           tree — the favourites group, each tag's server list, and the untagged group — so a filtered server
           vanishes from every place it would otherwise appear. A null search matches everything, keeping the
           no-search projection byte-identical to before. */
        var nodes = _tags.Select(t => new TagTreeNode(t.Id, t.Name, t.ParentId)).ToList();
        var searchNamesByTag = _tags.ToDictionary(
            t => t.Id,
            t => TagTreeRules.Ancestors(nodes, t.Id).Select(a => a.Name).Prepend(t.Name).ToList());

        bool ServerMatches(DarlingServer s)
        {
            if (_search is null)
            {
                return true;
            }

            if (ServerOverviewFilter.Matches(_search, s.DisplayName, s.ServerName))
            {
                return true;
            }

            if (_serverTags.TryGetValue(s.ServerId, out var tagIds))
            {
                foreach (var tid in tagIds)
                {
                    if (searchNamesByTag.TryGetValue(tid, out var names) && ServerOverviewFilter.Matches(_search, names.ToArray()))
                    {
                        return true;
                    }
                }
            }

            return false;
        }

        /* Favorites float to the top so collapsing a tag can never hide a starred server. _all arrives
           favourites-first + name sorted, so this preserves that order. Favourites still also appear
           under their own tags below. */
        var favorites = _all.Where(s => s.IsFavorite && ServerMatches(s)).ToList();
        if (favorites.Count > 0)
        {
            var expanded = IsExpanded(FleetGroupKey.Favorites);
            rows.Add(new FleetHeaderRow(
                FleetGroupKind.Favorites, "Favorites", depth: 0,
                serverCount: favorites.Count, hasChildren: true, isExpanded: expanded));

            if (expanded)
            {
                foreach (var s in favorites)
                {
                    rows.Add(new FleetServerRow(s, depth: 1));
                }
            }
        }

        /* Tag forest: parent id (null = root) → ordered children, and tag id → its directly-assigned
           servers (favourites-first, then name). Assignment is to a specific tag, never inherited to
           ancestors — a server tagged only on a child shows under that child, not its parent. */
        var childrenByParent = _tags
            .Where(t => t.ParentId is not null)
            .GroupBy(t => t.ParentId!.Value)
            .ToDictionary(
                g => g.Key,
                g => g.OrderBy(t => t.SortOrder).ThenBy(t => t.Name, StringComparer.OrdinalIgnoreCase).ToList());

        var knownTagIds = _tags.Select(t => t.Id).ToHashSet();

        var serversByTag = new Dictionary<int, List<DarlingServer>>();
        foreach (var s in _all)
        {
            if (!ServerMatches(s))
            {
                continue;
            }

            if (!_serverTags.TryGetValue(s.ServerId, out var tagIds))
            {
                continue;
            }

            foreach (var tid in tagIds)
            {
                if (!serversByTag.TryGetValue(tid, out var list))
                {
                    list = new List<DarlingServer>();
                    serversByTag[tid] = list;
                }

                list.Add(s);
            }
        }

        foreach (var list in serversByTag.Values)
        {
            list.Sort(CompareFavoriteThenName);
        }

        /* Under a search, a tag group is worth showing only if a match lives somewhere in its subtree —
           directly assigned, or under a descendant tag. serversByTag already holds matches only, so this is
           just "any direct match, or any child subtree matches", memoised and cycle-guarded like the DFS
           below. Never consulted when _search is null, so it costs nothing off the search path. */
        var subtreeMemo = new Dictionary<int, bool>();
        var subtreeInProgress = new HashSet<int>();

        bool SubtreeHasMatch(int tagId)
        {
            if (subtreeMemo.TryGetValue(tagId, out var cached))
            {
                return cached;
            }

            if (!subtreeInProgress.Add(tagId))
            {
                return false;   // a cycle: this path contributes no new match
            }

            var has = serversByTag.TryGetValue(tagId, out var direct) && direct.Count > 0;
            if (childrenByParent.TryGetValue(tagId, out var kids))
            {
                foreach (var child in kids)
                {
                    if (SubtreeHasMatch(child.Id))
                    {
                        has = true;
                    }
                }
            }

            subtreeInProgress.Remove(tagId);
            subtreeMemo[tagId] = has;
            return has;
        }

        /* A visited guard makes the DFS cycle-proof: CRUD forbids cycles and enforces the 4-level cap,
           but a hand-edited store must not be able to hang the viewer. */
        var visited = new HashSet<int>();

        void Emit(DarlingTag tag, int depth)
        {
            if (!visited.Add(tag.Id))
            {
                return;
            }

            /* Searching: drop a whole branch that contains no match, header and all. */
            if (_search is not null && !SubtreeHasMatch(tag.Id))
            {
                return;
            }

            var expanded = IsExpanded(FleetGroupKey.ForTag(tag.Id));
            var kids = childrenByParent.TryGetValue(tag.Id, out var k) ? k : EmptyTags;
            var servers = serversByTag.TryGetValue(tag.Id, out var sv) ? sv : EmptyServers;
            var hasChildren = _search is null
                ? kids.Count > 0 || servers.Count > 0
                : servers.Count > 0 || kids.Any(c => SubtreeHasMatch(c.Id));

            rows.Add(new FleetHeaderRow(
                FleetGroupKind.Tag, tag.Name, depth,
                serverCount: servers.Count, hasChildren: hasChildren, isExpanded: expanded, tag: tag));

            if (!expanded)
            {
                return;
            }

            foreach (var child in kids)
            {
                Emit(child, depth + 1);
            }

            foreach (var s in servers)
            {
                rows.Add(new FleetServerRow(s, depth + 1));
            }
        }

        /* Roots: parent is null OR a dangling parent id (an orphaned subtree still surfaces rather than
           vanishing with its servers). */
        var roots = _tags
            .Where(t => t.ParentId is null || !knownTagIds.Contains(t.ParentId.Value))
            .OrderBy(t => t.SortOrder)
            .ThenBy(t => t.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();

        foreach (var root in roots)
        {
            Emit(root, 0);
        }

        /* Any tag not reached from a root is part of a cycle or a fully disconnected component — only
           possible from a hand-edited store, since CRUD forbids cycles. Surface each as a root so no tag,
           and no server assigned only to it, silently vanishes; the visited guard keeps it terminating. */
        foreach (var tag in _tags)
        {
            if (!visited.Contains(tag.Id))
            {
                Emit(tag, 0);
            }
        }

        /* Untagged: servers with no assignment at all (that also match the search, when one is active).
           Shown last so the tree always accounts for the whole fleet. */
        var untagged = _all
            .Where(s => (!_serverTags.TryGetValue(s.ServerId, out var t) || t.Count == 0) && ServerMatches(s))
            .ToList();

        if (untagged.Count > 0)
        {
            var expanded = IsExpanded(FleetGroupKey.Untagged);
            rows.Add(new FleetHeaderRow(
                FleetGroupKind.Untagged, "Untagged", depth: 0,
                serverCount: untagged.Count, hasChildren: true, isExpanded: expanded));

            if (expanded)
            {
                foreach (var s in untagged)
                {
                    rows.Add(new FleetServerRow(s, depth: 1));
                }
            }
        }

        _visible = rows;
        ProjectionChanged?.Invoke(this, EventArgs.Empty);
    }

    /// <summary>
    /// The server with this id, or null. Resolves against <see cref="All"/> ON PURPOSE: a deep link, an
    /// Overview card click, or a selection restore must still find a server the current view happens to be
    /// hiding (collapsed, or filtered) — otherwise the action silently does nothing, which is exactly the
    /// class of bug this type exists to prevent.
    /// </summary>
    public DarlingServer? Find(int serverId) => _all.Find(s => s.ServerId == serverId);

    /// <summary>
    /// The row to select after a rebuild: the server row for the previously-selected server if it is still
    /// visible (by id, so a rename or re-sort still resolves), else the first visible SERVER row, else
    /// null when nothing is showing. Returns a server row, never a header — callers must not fall back to
    /// index 0 of the bound list, since row 0 is now often a group header.
    /// </summary>
    public FleetServerRow? ResolveSelection(int? previousServerId)
    {
        var serverRows = _visible.OfType<FleetServerRow>().ToList();

        if (previousServerId is int previous)
        {
            var kept = serverRows.FirstOrDefault(r => r.Server.ServerId == previous);
            if (kept is not null)
            {
                return kept;
            }
        }

        return serverRows.Count > 0 ? serverRows[0] : null;
    }

    private static int CompareFavoriteThenName(DarlingServer a, DarlingServer b)
    {
        var byFavorite = b.IsFavorite.CompareTo(a.IsFavorite);
        return byFavorite != 0
            ? byFavorite
            : string.Compare(a.DisplayName, b.DisplayName, StringComparison.OrdinalIgnoreCase);
    }

    private static readonly List<DarlingTag> EmptyTags = new();
    private static readonly List<DarlingServer> EmptyServers = new();
}
