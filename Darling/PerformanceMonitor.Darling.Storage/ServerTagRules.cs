/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The fleet-tag write rules, pure and shared by every writer (the viewer and the MCP tools through
/// <see cref="ServerTagStore"/>): name, duplicate, depth, cycle, parent and colour. Each check returns null when
/// the write may proceed, or the typed refusal to hand back. The tree is a few dozen rows read whole, so every
/// walk is in memory and guarded at <see cref="MaxWalkSteps"/> steps, the same bound as the alert rules'
/// tag-subtree read, so a malformed cycle in the data cannot hang a check.
/// </summary>
public static class ServerTagRules
{
    /// <summary>The deepest nesting allowed: a tag has a 0-based depth from the root and at most
    /// <c>MaxDepth - 1</c>.</summary>
    public const int MaxDepth = 4;

    /// <summary>The longest tag name, in characters, after trimming.</summary>
    public const int MaxNameLength = 100;

    /// <summary>The bound on every parent-chain or child-frontier walk.</summary>
    public const int MaxWalkSteps = 64;

    /// <summary>Refusal code: the name is empty, too long or holds a control character.</summary>
    public const string BadName = "bad_name";

    /// <summary>Refusal code: the colour is not <c>#RRGGBB</c>.</summary>
    public const string BadColour = "bad_colour";

    /// <summary>Refusal code: the write would nest tags deeper than <see cref="MaxDepth"/> levels.</summary>
    public const string DepthLimit = "depth_limit";

    /// <summary>Refusal code: the move would put a tag under itself or one of its own descendants.</summary>
    public const string Cycle = "cycle";

    /// <summary>Refusal code: a sibling already carries the name.</summary>
    public const string DuplicateName = "duplicate_name";

    /// <summary>Refusal code: the requested parent is not a tag.</summary>
    public const string UnknownParent = "unknown_parent";

    /// <summary>The colour refusal text.</summary>
    public const string BadColourMessage = "colour must be #RRGGBB, e.g. #416FA6, or null to clear it.";

    /// <summary>The create-depth refusal text.</summary>
    public const string DepthLimitMessage = "Tags can nest at most four levels deep.";

    /// <summary>The cycle refusal text.</summary>
    public const string CycleMessage = "A tag cannot move under itself or one of its own child tags.";

    /// <summary>The 0-based depth of a tag: a root is 0. A missing tag, or a parent chain longer than the walk
    /// bound, reads as the bound reached so far.</summary>
    public static int Depth(IReadOnlyList<ServerTagRow> tags, int tagId)
    {
        var byId = tags.ToDictionary(t => t.Id);
        var depth = 0;
        var current = tagId;
        while (depth < MaxWalkSteps && byId.TryGetValue(current, out var row) && row.ParentId is int parent)
        {
            depth++;
            current = parent;
        }

        return depth;
    }

    /// <summary>The deepest descendant's depth relative to the tag: a leaf is 0.</summary>
    public static int Height(IReadOnlyList<ServerTagRow> tags, int tagId)
    {
        var frontier = new List<int> { tagId };
        var seen = new HashSet<int> { tagId };
        var height = 0;
        for (var step = 0; step < MaxWalkSteps; step++)
        {
            var next = tags
                .Where(t => t.ParentId is int p && frontier.Contains(p) && seen.Add(t.Id))
                .Select(t => t.Id)
                .ToList();
            if (next.Count == 0)
            {
                break;
            }

            height++;
            frontier = next;
        }

        return height;
    }

    /// <summary>Every tag under <paramref name="tagId"/> (not the tag itself), walked breadth-first and bounded
    /// at <see cref="MaxWalkSteps"/> levels.</summary>
    public static HashSet<int> Descendants(IReadOnlyList<ServerTagRow> tags, int tagId)
    {
        var result = new HashSet<int>();
        var frontier = new List<int> { tagId };
        for (var step = 0; step < MaxWalkSteps && frontier.Count > 0; step++)
        {
            var next = tags
                .Where(t => t.ParentId is int p && frontier.Contains(p) && t.Id != tagId && result.Add(t.Id))
                .Select(t => t.Id)
                .ToList();
            frontier = next;
        }

        return result;
    }

    /// <summary>Trims and validates a tag name: required, at most <see cref="MaxNameLength"/> characters, no
    /// control character. Returns null and the trimmed name when valid.</summary>
    public static ServerTagWriteResult.Refused? CheckName(string? name, out string cleanName)
    {
        cleanName = (name ?? string.Empty).Trim();
        if (cleanName.Length == 0)
        {
            return new ServerTagWriteResult.Refused(BadName, "Tag name is required.");
        }

        if (cleanName.Length > MaxNameLength)
        {
            return new ServerTagWriteResult.Refused(BadName, "Tag name exceeds 100 characters.");
        }

        if (cleanName.Any(char.IsControl))
        {
            return new ServerTagWriteResult.Refused(BadName, "Tag name contains a control character.");
        }

        // Format characters (zero-width, bidi overrides) render as nothing, so two names could look identical.
        if (cleanName.Any(c => char.GetUnicodeCategory(c) == UnicodeCategory.Format))
        {
            return new ServerTagWriteResult.Refused(BadName, "Tag name contains an invisible formatting character.");
        }

        return null;
    }

    /// <summary>Trims, validates and upper-cases a colour. Null passes through as null (clear, or the palette
    /// default on create); an empty string or anything but <c>#RRGGBB</c> is refused.</summary>
    public static ServerTagWriteResult.Refused? CheckColour(string? colour, out string? cleanColour)
    {
        cleanColour = null;
        if (colour is null)
        {
            return null;
        }

        colour = colour.Trim();
        if (colour.Length == 0 || !TagColours.IsValidStoredColour(colour))
        {
            return new ServerTagWriteResult.Refused(BadColour, BadColourMessage);
        }

        cleanColour = colour.ToUpperInvariant();
        return null;
    }

    /// <summary>The duplicate-name text: "A tag named 'X' already exists under 'Parent'" (or "at the root").</summary>
    public static string DuplicateNameMessage(string name, string parentName) =>
        parentName == "the root"
            ? string.Create(CultureInfo.InvariantCulture, $"A tag named '{name}' already exists at the root.")
            : string.Create(CultureInfo.InvariantCulture, $"A tag named '{name}' already exists under '{parentName}'.");

    /// <summary>The duplicate-name text for a 23505 the store caught after the in-memory check passed (a writer
    /// that does not take the lock, or a case fold the two comparers disagree on).</summary>
    public static string ConcurrentDuplicateMessage(string? name) =>
        name is null
            ? "A tag with that name already exists under the target parent."
            : string.Create(CultureInfo.InvariantCulture, $"A tag named '{name}' already exists under the target parent.");

    /// <summary>Whether a sibling under <paramref name="parentId"/> (null = root) already has
    /// <paramref name="cleanName"/>, ignoring case, other than the tag <paramref name="selfId"/>.</summary>
    public static bool IsDuplicate(IReadOnlyList<ServerTagRow> tags, string cleanName, int? parentId, int? selfId)
    {
        var folded = Fold(cleanName);
        return tags.Any(t => t.ParentId == parentId && t.Id != selfId && Fold(t.Name) == folded);
    }

    private static string Fold(string name) => name.ToLowerInvariant();

    /// <summary>The checks for a create: name, colour, parent exists, depth, duplicate. Returns the refusal, or
    /// null with the cleaned name and colour.</summary>
    public static ServerTagWriteResult? CheckCreate(
        IReadOnlyList<ServerTagRow> tags,
        string? name,
        int? parentId,
        string? colour,
        out string cleanName,
        out string? cleanColour)
    {
        cleanColour = null;
        var nameRefusal = CheckName(name, out cleanName);
        if (nameRefusal is not null)
        {
            return nameRefusal;
        }

        var colourRefusal = CheckColour(colour, out cleanColour);
        if (colourRefusal is not null)
        {
            return colourRefusal;
        }

        string parentName = "the root";
        if (parentId is int parent)
        {
            var parentRow = tags.FirstOrDefault(t => t.Id == parent);
            if (parentRow is null)
            {
                return UnknownParentResult(parent);
            }

            if (Depth(tags, parent) + 1 > MaxDepth - 1)
            {
                return new ServerTagWriteResult.Refused(DepthLimit, DepthLimitMessage);
            }

            parentName = parentRow.Name;
        }

        return IsDuplicate(tags, cleanName, parentId, null)
            ? new ServerTagWriteResult.Conflict(DuplicateNameMessage(cleanName, parentName))
            : null;
    }

    /// <summary>The checks for an update: the tag exists, then name, colour, and for a move the parent, cycle,
    /// depth, and finally the duplicate check against the target parent's children. Returns the refusal, or null
    /// with the cleaned name and colour.</summary>
    public static ServerTagWriteResult? CheckUpdate(
        IReadOnlyList<ServerTagRow> tags,
        int tagId,
        ServerTagEdit edit,
        out string? cleanName,
        out string? cleanColour)
    {
        cleanName = null;
        cleanColour = null;
        var tag = tags.FirstOrDefault(t => t.Id == tagId);
        if (tag is null)
        {
            return new ServerTagWriteResult.NotFound(string.Create(CultureInfo.InvariantCulture, $"Tag {tagId} does not exist."));
        }

        if (edit.HasName)
        {
            var nameRefusal = CheckName(edit.Name, out var cleaned);
            if (nameRefusal is not null)
            {
                return nameRefusal;
            }

            cleanName = cleaned;
        }

        if (edit.HasColour)
        {
            var colourRefusal = CheckColour(edit.Colour, out cleanColour);
            if (colourRefusal is not null)
            {
                return colourRefusal;
            }
        }

        var targetParent = tag.ParentId;
        var targetParentName = ParentNameOf(tags, tag.ParentId);
        if (edit.HasParent)
        {
            targetParent = edit.ParentId;
            if (edit.ParentId is int parent)
            {
                var parentRow = tags.FirstOrDefault(t => t.Id == parent);
                if (parentRow is null)
                {
                    return UnknownParentResult(parent);
                }

                if (parent == tagId || Descendants(tags, tagId).Contains(parent))
                {
                    return new ServerTagWriteResult.Refused(Cycle, CycleMessage);
                }

                var levels = Depth(tags, parent) + 1 + Height(tags, tagId);
                if (levels > MaxDepth - 1)
                {
                    return new ServerTagWriteResult.Refused(
                        DepthLimit,
                        string.Create(
                            CultureInfo.InvariantCulture,
                            $"Moving '{tag.Name}' under '{parentRow.Name}' would nest its subtree {levels + 1} levels deep; tags nest at most four levels."));
                }

                targetParentName = parentRow.Name;
            }
            else
            {
                targetParentName = "the root";
            }
        }

        if (edit.HasName || edit.HasParent)
        {
            var effectiveName = cleanName ?? tag.Name;
            if (IsDuplicate(tags, effectiveName, targetParent, tagId))
            {
                return new ServerTagWriteResult.Conflict(DuplicateNameMessage(effectiveName, targetParentName));
            }
        }

        return null;
    }

    /// <summary>The check for a delete: the tag must exist.</summary>
    public static ServerTagWriteResult? CheckDelete(IReadOnlyList<ServerTagRow> tags, int tagId) =>
        tags.Any(t => t.Id == tagId)
            ? null
            : new ServerTagWriteResult.NotFound(string.Create(CultureInfo.InvariantCulture, $"Tag {tagId} does not exist."));

    private static ServerTagWriteResult.NotFound UnknownParentResult(int parentId) =>
        new(string.Create(CultureInfo.InvariantCulture, $"Parent tag {parentId} does not exist."), UnknownParent);

    private static string ParentNameOf(IReadOnlyList<ServerTagRow> tags, int? parentId) =>
        parentId is int id ? tags.FirstOrDefault(t => t.Id == id)?.Name ?? "the parent tag" : "the root";
}
