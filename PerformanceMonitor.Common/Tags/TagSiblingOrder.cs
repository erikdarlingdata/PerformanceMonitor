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

namespace PerformanceMonitor.Common.Tags;

/// <summary>Which way to move a tag among its siblings: toward the top of the list (earlier) or the bottom.</summary>
public enum TagMoveDirection
{
    /// <summary>One place earlier in the sibling order.</summary>
    Up,

    /// <summary>One place later in the sibling order.</summary>
    Down,
}

/// <summary>One tag among the siblings under a single parent, with the sort order it has now.</summary>
public sealed record TagSibling(int Id, string Name, int SortOrder);

/// <summary>The sort order a tag should be written with.</summary>
public sealed record TagSortAssignment(int TagId, int SortOrder);

/// <summary>
/// The sibling-reorder rule for tags (#6094), pure and shared by every writer (the Darling viewer, the MCP tool and
/// the web page through their store, and Lite's Manage Tags window). Every reader shows siblings by sort order, then
/// name (case-insensitive), so a move is computed on that same display order: the siblings are put in display order,
/// the tag swaps places with its neighbour, and EVERY sibling is renumbered densely, 0 to n-1. Dense renumbering is
/// what makes the move work from the all-zero start (every tag was created with sort order 0, so a bare swap of two
/// zeros would change nothing) and from duplicated values. The caller must pass the siblings it read inside the
/// write's own transaction, so a concurrent reorder is never renumbered from a stale snapshot.
/// </summary>
public static class TagSiblingOrder
{
    /// <summary>The siblings in display order: sort order, then name ignoring case, then id so the order is total.
    /// Identical to what the readers show for any sibling set whose names are unique under the parent, which the
    /// per-parent unique index guarantees.</summary>
    public static IReadOnlyList<TagSibling> InDisplayOrder(IEnumerable<TagSibling> siblings) =>
        siblings
            .OrderBy(s => s.SortOrder)
            .ThenBy(s => s.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(s => s.Id)
            .ToList();

    /// <summary>Whether <paramref name="tagId"/> can move one place in <paramref name="direction"/>: it is among the
    /// siblings and is not already first (up) or last (down).</summary>
    public static bool CanMove(IEnumerable<TagSibling> siblings, int tagId, TagMoveDirection direction)
    {
        var ordered = InDisplayOrder(siblings);
        var index = IndexOf(ordered, tagId);
        return index >= 0 && TargetIndex(index, direction, ordered.Count) is not null;
    }

    /// <summary>The new sort order for every sibling after moving <paramref name="tagId"/> one place in
    /// <paramref name="direction"/>, dense from 0 in the new display order. Empty when nothing moves: the tag is
    /// not among the siblings, or it is already at that edge.</summary>
    public static IReadOnlyList<TagSortAssignment> Move(IEnumerable<TagSibling> siblings, int tagId, TagMoveDirection direction)
    {
        var ordered = InDisplayOrder(siblings).ToList();
        var index = IndexOf(ordered, tagId);
        if (index < 0 || TargetIndex(index, direction, ordered.Count) is not int target)
        {
            return Array.Empty<TagSortAssignment>();
        }

        (ordered[index], ordered[target]) = (ordered[target], ordered[index]);
        return ordered.Select((s, i) => new TagSortAssignment(s.Id, i)).ToList();
    }

    private static int IndexOf(IReadOnlyList<TagSibling> ordered, int tagId)
    {
        for (var i = 0; i < ordered.Count; i++)
        {
            if (ordered[i].Id == tagId)
            {
                return i;
            }
        }

        return -1;
    }

    private static int? TargetIndex(int index, TagMoveDirection direction, int count)
    {
        var target = direction == TagMoveDirection.Up ? index - 1 : index + 1;
        return target >= 0 && target < count ? target : null;
    }
}
