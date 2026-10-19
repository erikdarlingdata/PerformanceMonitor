/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Common.Tags;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The fleet-tag write rules for every writer (the viewer and the MCP tools through <see cref="ServerTagStore"/>):
/// a thin adapter over the shared <see cref="TagTreeRules"/> (#5978), which holds the limits, codes, messages and
/// walks so Lite and query tags use the same copy. This class maps <see cref="ServerTagRow"/> in and the shared
/// refusal out to <see cref="ServerTagWriteResult"/>. Each check returns null when the write may proceed, or the
/// typed refusal to hand back.
/// </summary>
public static class ServerTagRules
{
    /// <summary>The deepest nesting allowed: a tag has a 0-based depth from the root and at most
    /// <c>MaxDepth - 1</c>.</summary>
    public const int MaxDepth = TagTreeRules.MaxDepth;

    /// <summary>The longest tag name, in characters, after trimming.</summary>
    public const int MaxNameLength = TagTreeRules.MaxNameLength;

    /// <summary>The bound on every parent-chain or child-frontier walk.</summary>
    public const int MaxWalkSteps = TagTreeRules.MaxWalkSteps;

    /// <summary>Refusal code: the name is empty, too long or holds a control character.</summary>
    public const string BadName = TagTreeRules.BadName;

    /// <summary>Refusal code: the colour is not <c>#RRGGBB</c>.</summary>
    public const string BadColour = TagTreeRules.BadColour;

    /// <summary>Refusal code: the write would nest tags deeper than <see cref="MaxDepth"/> levels.</summary>
    public const string DepthLimit = TagTreeRules.DepthLimit;

    /// <summary>Refusal code: the move would put a tag under itself or one of its own descendants.</summary>
    public const string Cycle = TagTreeRules.Cycle;

    /// <summary>Refusal code: a sibling already carries the name.</summary>
    public const string DuplicateName = TagTreeRules.DuplicateName;

    /// <summary>Refusal code: the requested parent is not a tag.</summary>
    public const string UnknownParent = TagTreeRules.UnknownParent;

    /// <summary>The colour refusal text.</summary>
    public const string BadColourMessage = TagTreeRules.BadColourMessage;

    /// <summary>The create-depth refusal text.</summary>
    public const string DepthLimitMessage = TagTreeRules.DepthLimitMessage;

    /// <summary>The cycle refusal text.</summary>
    public const string CycleMessage = TagTreeRules.CycleMessage;

    /// <summary>The 0-based depth of a tag: a root is 0. A missing tag, or a parent chain longer than the walk
    /// bound, reads as the bound reached so far.</summary>
    public static int Depth(IReadOnlyList<ServerTagRow> tags, int tagId) =>
        TagTreeRules.Depth(Nodes(tags), tagId);

    /// <summary>The deepest descendant's depth relative to the tag: a leaf is 0.</summary>
    public static int Height(IReadOnlyList<ServerTagRow> tags, int tagId) =>
        TagTreeRules.Height(Nodes(tags), tagId);

    /// <summary>Every tag under <paramref name="tagId"/> (not the tag itself), walked breadth-first and bounded
    /// at <see cref="MaxWalkSteps"/> levels.</summary>
    public static HashSet<int> Descendants(IReadOnlyList<ServerTagRow> tags, int tagId) =>
        TagTreeRules.Descendants(Nodes(tags), tagId);

    /// <summary>Trims and validates a tag name: required, at most <see cref="MaxNameLength"/> characters, no
    /// control character. Returns null and the trimmed name when valid.</summary>
    public static ServerTagWriteResult.Refused? CheckName(string? name, out string cleanName)
    {
        var refusal = TagTreeRules.CheckName(name, out cleanName);
        return refusal is null ? null : new ServerTagWriteResult.Refused(refusal.Code!, refusal.Message);
    }

    /// <summary>Trims, validates and upper-cases a colour. Null passes through as null (clear, or the palette
    /// default on create); an empty string or anything but <c>#RRGGBB</c> is refused.</summary>
    public static ServerTagWriteResult.Refused? CheckColour(string? colour, out string? cleanColour)
    {
        var refusal = TagTreeRules.CheckColour(colour, out cleanColour);
        return refusal is null ? null : new ServerTagWriteResult.Refused(refusal.Code!, refusal.Message);
    }

    /// <summary>The duplicate-name text: "A tag named 'X' already exists under 'Parent'" (or "at the root").</summary>
    public static string DuplicateNameMessage(string name, string parentName) =>
        TagTreeRules.DuplicateNameMessage(name, parentName);

    /// <summary>The duplicate-name text for a 23505 the store caught after the in-memory check passed (a writer
    /// that does not take the lock, or a case fold the two comparers disagree on).</summary>
    public static string ConcurrentDuplicateMessage(string? name) =>
        TagTreeRules.ConcurrentDuplicateMessage(name);

    /// <summary>Whether a sibling under <paramref name="parentId"/> (null = root) already has
    /// <paramref name="cleanName"/>, ignoring case, other than the tag <paramref name="selfId"/>.</summary>
    public static bool IsDuplicate(IReadOnlyList<ServerTagRow> tags, string cleanName, int? parentId, int? selfId) =>
        TagTreeRules.IsDuplicate(Nodes(tags), cleanName, parentId, selfId);

    /// <summary>The checks for a create: name, colour, parent exists, depth, duplicate. Returns the refusal, or
    /// null with the cleaned name and colour.</summary>
    public static ServerTagWriteResult? CheckCreate(
        IReadOnlyList<ServerTagRow> tags,
        string? name,
        int? parentId,
        string? colour,
        out string cleanName,
        out string? cleanColour) =>
        ToResult(TagTreeRules.CheckCreate(Nodes(tags), name, parentId, colour, out cleanName, out cleanColour));

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
        var shared = new TagTreeEdit(edit.HasName, edit.Name, edit.HasColour, edit.Colour, edit.HasParent, edit.ParentId);
        return ToResult(TagTreeRules.CheckUpdate(Nodes(tags), tagId, shared, out cleanName, out cleanColour));
    }

    /// <summary>The check for a delete: the tag must exist.</summary>
    public static ServerTagWriteResult? CheckDelete(IReadOnlyList<ServerTagRow> tags, int tagId) =>
        ToResult(TagTreeRules.CheckDelete(Nodes(tags), tagId));

    /// <summary>The check for a reorder: the tag must exist (an edge move is not a refusal; it is a no-op).</summary>
    public static ServerTagWriteResult? CheckMove(IReadOnlyList<ServerTagRow> tags, int tagId) =>
        ToResult(TagTreeRules.CheckDelete(Nodes(tags), tagId));

    /// <summary>The tags that share <paramref name="tagId"/>'s parent, itself included, as the shared reorder rule
    /// reads them. Empty when the tag does not exist.</summary>
    public static List<TagSibling> SiblingsOf(IReadOnlyList<ServerTagRow> tags, int tagId)
    {
        var tag = tags.FirstOrDefault(t => t.Id == tagId);
        return tag is null
            ? new List<TagSibling>()
            : tags.Where(t => t.ParentId == tag.ParentId).Select(t => new TagSibling(t.Id, t.Name, t.SortOrder)).ToList();
    }

    private static List<TagTreeNode> Nodes(IReadOnlyList<ServerTagRow> tags) =>
        tags.Select(t => new TagTreeNode(t.Id, t.Name, t.ParentId)).ToList();

    private static ServerTagWriteResult? ToResult(TagRefusal? refusal) => refusal?.Kind switch
    {
        null => null,
        TagRefusalKind.Conflict => new ServerTagWriteResult.Conflict(refusal.Message),
        TagRefusalKind.NotFound => new ServerTagWriteResult.NotFound(refusal.Message, refusal.Code),
        _ => new ServerTagWriteResult.Refused(refusal.Code!, refusal.Message),
    };
}
