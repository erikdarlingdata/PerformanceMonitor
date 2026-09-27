/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite;

// CI proof: comment-only touch to this file (not in the store-upgrade filter).

/// <summary>
/// Owns the fleet projection: the projected rows the sidebar renders (<see cref="Visible"/>) — a mix of
/// group headers and servers, not a flat server list. Ported from the Darling viewer's <c>FleetView</c>.
///
/// <para><b>Why this exists.</b> The Lite sidebar's bound list used to BE the fleet — a flat
/// <c>List&lt;ServerConnection&gt;</c>. The moment the sidebar shows a MIXED row type (tag headers above
/// servers), the double-click handler and the row context menu can no longer assume the selected item is a
/// server. The projection separates the two ideas: the authoritative fleet stays
/// <c>ServerManager.GetAllServers()</c> (every "what servers exist?" consumer already reads it directly),
/// and only the sidebar binds <see cref="Visible"/>.</para>
///
/// <para><b>Tags are opt-in.</b> With no tags defined, <see cref="Visible"/> is exactly the flat, ordered
/// server list it always was — a user who never touches tags sees no change. The grouped tree
/// (Favorites → tags → Untagged) appears only once the first tag exists.</para>
///
/// <para>Free of any WPF dependency — it projects over the plain <see cref="FleetServer"/> POCO — so the
/// projection and lookup rules are unit-testable without a window, exactly as in the viewer.</para>
/// </summary>
internal sealed class FleetView
{
    private List<FleetServer> _all = new();
    private List<ServerTag> _tags = new();

    /// <summary>serverId → the set of tag ids assigned to it. Empty/absent = untagged.</summary>
    private Dictionary<int, HashSet<int>> _serverTags = new();

    /// <summary>The groups the user has collapsed. Absence = expanded (the friendly default), so a fresh
    /// fleet shows everything. Keyed by <see cref="FleetGroupKey"/> so state survives reprojection and a
    /// tag rename.</summary>
    private readonly HashSet<FleetGroupKey> _collapsed = new();

    private List<FleetRow> _visible = new();

    /// <summary>Every known server, regardless of what the sidebar is currently showing. This is what a
    /// consumer asking "what is in the fleet?" must read — never the bound list.</summary>
    public IReadOnlyList<FleetServer> All => _all;

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
    public void SetAll(IEnumerable<FleetServer> servers)
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
    public void SetTags(IEnumerable<ServerTag> tags, IEnumerable<ServerTagAssignment> assignments)
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

    private bool IsExpanded(FleetGroupKey key) => !_collapsed.Contains(key);

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

        /* No tags anywhere → behaviour-neutral flat list, exactly as before tags existed. */
        if (_tags.Count == 0)
        {
            foreach (var s in _all)
            {
                rows.Add(new FleetServerRow(s, depth: 0));
            }

            _visible = rows;
            ProjectionChanged?.Invoke(this, EventArgs.Empty);
            return;
        }

        /* Favorites float to the top so collapsing a tag can never hide a starred server. _all arrives
           favourites-first + name sorted, so this preserves that order. Favourites still also appear
           under their own tags below. */
        var favorites = _all.Where(s => s.IsFavorite).ToList();
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

        var serversByTag = new Dictionary<int, List<FleetServer>>();
        foreach (var s in _all)
        {
            if (!_serverTags.TryGetValue(s.ServerId, out var tagIds))
            {
                continue;
            }

            foreach (var tid in tagIds)
            {
                if (!serversByTag.TryGetValue(tid, out var list))
                {
                    list = new List<FleetServer>();
                    serversByTag[tid] = list;
                }

                list.Add(s);
            }
        }

        foreach (var list in serversByTag.Values)
        {
            list.Sort(CompareFavoriteThenName);
        }

        /* A visited guard makes the DFS cycle-proof: CRUD forbids cycles and enforces the 4-level cap,
           but a hand-edited store must not be able to hang the window. */
        var visited = new HashSet<int>();

        void Emit(ServerTag tag, int depth)
        {
            if (!visited.Add(tag.Id))
            {
                return;
            }

            var expanded = IsExpanded(FleetGroupKey.ForTag(tag.Id));
            var kids = childrenByParent.TryGetValue(tag.Id, out var k) ? k : EmptyTags;
            var servers = serversByTag.TryGetValue(tag.Id, out var sv) ? sv : EmptyServers;
            var hasChildren = kids.Count > 0 || servers.Count > 0;

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

        /* Untagged: servers with no assignment at all. Shown last so the tree always accounts for the
           whole fleet. */
        var untagged = _all
            .Where(s => !_serverTags.TryGetValue(s.ServerId, out var t) || t.Count == 0)
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
    /// The server with this id, or null. Resolves against <see cref="All"/> ON PURPOSE: a card click or a
    /// selection restore must still find a server the current view happens to be hiding (collapsed, or
    /// filtered) — otherwise the action silently does nothing, which is exactly the class of bug this type
    /// exists to prevent.
    /// </summary>
    public FleetServer? Find(int serverId) => _all.Find(s => s.ServerId == serverId);

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

    private static int CompareFavoriteThenName(FleetServer a, FleetServer b)
    {
        var byFavorite = b.IsFavorite.CompareTo(a.IsFavorite);
        return byFavorite != 0
            ? byFavorite
            : string.Compare(a.DisplayName, b.DisplayName, StringComparison.OrdinalIgnoreCase);
    }

    private static readonly List<ServerTag> EmptyTags = new();
    private static readonly List<FleetServer> EmptyServers = new();
}
