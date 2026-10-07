/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite;

/// <summary>
/// A server as the fleet projection sees it: the deterministic storage id the tag map is keyed on (NOT the
/// local <see cref="ServerConnection.Id"/> GUID), the display fields the grouping sorts by, and the backing
/// <see cref="ServerConnection"/> the UI and the sidebar's context-menu actions reach through
/// <see cref="Connection"/>.
///
/// <para>This thin adapter is why Lite's <see cref="FleetView"/> can stay WPF-free and unit-testable the way
/// the Darling viewer's twin is: the projection keys and sorts on <see cref="ServerId"/> / <see cref="IsFavorite"/>
/// / <see cref="DisplayName"/> handed to it, never on <see cref="ServerConnection"/>'s shape or the
/// <c>RemoteCollectorService</c> hash helper. Tests build a <see cref="FleetServer"/> directly with a
/// <see cref="Connection"/> of null; only the window ever dereferences it.</para>
/// </summary>
internal sealed record FleetServer(int ServerId, string DisplayName, bool IsFavorite, ServerConnection? Connection);

/// <summary>
/// One rendered row in the fleet sidebar: either a group header (<see cref="FleetHeaderRow"/>) or a
/// server (<see cref="FleetServerRow"/>). Deliberately a plain POCO with no WPF dependency, so
/// <see cref="FleetView"/>'s tree projection can be asserted from unit tests without a window — the same
/// reason <see cref="FleetView"/> itself is WPF-free. Ported from the Darling viewer's identical type.
///
/// <para>Rows are immutable snapshots the projection rebuilds wholesale: expanding or collapsing a header,
/// assigning a tag, or pinning a favourite re-runs <see cref="FleetView.Rebuild"/> and produces a fresh
/// list, rather than mutating a row in place. A ~100-row rebuild is trivial, and it keeps these types free
/// of change notification — the bound list is simply reassigned.</para>
/// </summary>
internal abstract class FleetRow
{
    /// <summary>
    /// Indent level for rendering: 0 = a root group header. A server sits one level deeper than the
    /// header it appears under, and a nested tag one level deeper than its parent. The sidebar multiplies
    /// this by a fixed step to produce the left margin.
    /// </summary>
    public int Depth { get; protected init; }

    /// <summary>
    /// What a screen reader announces for this row (<c>AutomationProperties.Name</c> on the sidebar's list item):
    /// the server's display name or the group's title, instead of the row class's type name.
    /// </summary>
    public virtual string AutomationName => string.Empty;
}

/// <summary>
/// What a <see cref="FleetHeaderRow"/> stands for. A real user <see cref="FleetHeaderRow.Tag"/> carries a
/// <see cref="ServerTag"/>; the two synthetic groups the projection always frames the fleet with do not.
/// </summary>
internal enum FleetGroupKind
{
    /// <summary>A user-authored tag (possibly nested). <see cref="FleetHeaderRow.Tag"/> is non-null.</summary>
    Tag,

    /// <summary>The pinned-favourites group, floated to the top so a collapsed tag can never hide a
    /// starred server. A favourite ALSO appears under each of its tags — the same duplication as any
    /// multi-tagged server.</summary>
    Favorites,

    /// <summary>The catch-all for servers carrying no tag, shown at the bottom so the tree always accounts
    /// for the whole fleet.</summary>
    Untagged
}

/// <summary>
/// Stable identity for a group across rebuilds, so expand/collapse state survives reprojection. A real
/// tag keys on its id; the two pseudo-groups key on their <see cref="FleetGroupKind"/> alone (their
/// <see cref="TagId"/> is unused and left zero). Deliberately a value type so it works as a dictionary /
/// set key.
/// </summary>
internal readonly record struct FleetGroupKey(FleetGroupKind Kind, int TagId)
{
    public static FleetGroupKey ForTag(int tagId) => new(FleetGroupKind.Tag, tagId);
    public static readonly FleetGroupKey Favorites = new(FleetGroupKind.Favorites, 0);
    public static readonly FleetGroupKey Untagged = new(FleetGroupKind.Untagged, 0);

    /// <summary>A stable, human-readable string for persisting this key in the Lite settings.</summary>
    public string ToStorageString() => Kind switch
    {
        FleetGroupKind.Favorites => "Favorites",
        FleetGroupKind.Untagged => "Untagged",
        _ => $"Tag:{TagId}"
    };

    /// <summary>Parses a <see cref="ToStorageString"/> value; false (and <paramref name="key"/> = default)
    /// for null, empty, or an unrecognized/forward-version form.</summary>
    public static bool TryParse(string? value, out FleetGroupKey key)
    {
        key = default;
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        switch (value)
        {
            case "Favorites":
                key = Favorites;
                return true;
            case "Untagged":
                key = Untagged;
                return true;
            default:
                if (value.StartsWith("Tag:", StringComparison.Ordinal)
                    && int.TryParse(value.AsSpan(4), out var id))
                {
                    key = ForTag(id);
                    return true;
                }

                return false;
        }
    }
}

/// <summary>
/// A group header row: a real tag, or the Favorites / Untagged pseudo-group. Clicking it expands or
/// collapses rather than selecting a server (see <see cref="FleetView.ResolveSelection"/>, which never
/// returns a header).
/// </summary>
internal sealed class FleetHeaderRow : FleetRow
{
    public FleetHeaderRow(
        FleetGroupKind kind,
        string title,
        int depth,
        int serverCount,
        bool hasChildren,
        bool isExpanded,
        ServerTag? tag = null)
    {
        Kind = kind;
        Title = title;
        Depth = depth;
        ServerCount = serverCount;
        HasChildren = hasChildren;
        IsExpanded = isExpanded;
        Tag = tag;
    }

    /// <summary>Which kind of group this header stands for.</summary>
    public FleetGroupKind Kind { get; }

    /// <summary>The backing tag when <see cref="Kind"/> is <see cref="FleetGroupKind.Tag"/>; null for the
    /// Favorites and Untagged pseudo-groups.</summary>
    public ServerTag? Tag { get; }

    /// <summary>Display label: the tag name, or "Favorites" / "Untagged".</summary>
    public string Title { get; }

    public override string AutomationName => Title;

    /// <summary>Servers shown directly under this header (its own directly-assigned servers; for the
    /// pseudo-groups, the favourites or untagged count). Rendered as a "(n)" badge.</summary>
    public int ServerCount { get; }

    /// <summary>True when the header has anything beneath it — child tags OR servers — so a disclosure
    /// chevron is drawn and the row is expandable. A childless tag shows no chevron.</summary>
    public bool HasChildren { get; }

    /// <summary>Whether the group is currently expanded. A collapsed header renders alone; its descendants
    /// are omitted from the projection entirely.</summary>
    public bool IsExpanded { get; }

    /// <summary>The stable key used to track this group's expand/collapse state across rebuilds.</summary>
    public FleetGroupKey Key => Kind == FleetGroupKind.Tag
        ? FleetGroupKey.ForTag(Tag?.Id ?? throw new InvalidOperationException("A Tag header must carry a tag."))
        : Kind == FleetGroupKind.Favorites
            ? FleetGroupKey.Favorites
            : FleetGroupKey.Untagged;
}

/// <summary>A server row: one monitored server, indented under whichever group header precedes it. The
/// same server can appear in more than one <see cref="FleetServerRow"/> when it carries multiple tags (or
/// is a favourite), which is the intended label-style model, not a bug.</summary>
internal sealed class FleetServerRow : FleetRow
{
    public FleetServerRow(FleetServer server, int depth)
    {
        Server = server;
        Depth = depth;
    }

    /// <summary>The monitored server this row renders.</summary>
    public FleetServer Server { get; }

    public override string AutomationName => Server.DisplayName;
}
