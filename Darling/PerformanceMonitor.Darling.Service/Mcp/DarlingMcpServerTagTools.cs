/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The fleet server-tag WRITE tools (#5085): create, update (rename, colour, move), delete, assign and unassign.
/// Tags organize the server list and scope custom alert rules, so every result names the custom alert rules the
/// change affects (<c>affected_rules</c>).
///
/// <para><b>One implementation.</b> Every tool hands off to a <c>...Core</c> over <see cref="IServerTagStore"/>.
/// <see cref="ServerTagStore"/> is the same store the Viewer writes through, and the checks in
/// <see cref="ServerTagRules"/> (depth, cycle, duplicate, name, colour) run in the core before the write and again
/// in the store under its lock, so a writer that races this one cannot slip past them.</para>
///
/// <para><b>The flow of every core.</b> Validate the arguments, read a <c>before</c> snapshot (tags,
/// assignments, tag-scoped rules), run the pure checks, write, read an <c>after</c> snapshot, and report
/// <see cref="ServerTagCoverage.Diff"/> of the two. The tag a result returns is the row re-read from
/// <c>after</c>, never the row the write call handed back. A refusal returns before any write.</para>
///
/// <para><b>The admin gate is a grant.</b> Every MCP caller connects as the <c>mcp</c> role, which holds
/// INSERT/UPDATE/DELETE on only <c>config.server_tags</c> and <c>config.server_tag_map</c>
/// (<see cref="DarlingManagedRoles"/>). Without those grants the write fails with 42501 and the tool reports it
/// as an error.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpServerTagTools
{
    /// <summary>The most server ids one assign or unassign call takes.</summary>
    internal const int MaxServerIds = 1000;

    private const string UnknownFieldCode = "unknown_field";
    private const string UnknownServerCode = "unknown_server";
    private const string RulesAffectedCode = "rules_affected";

    /// <summary>The per-command deadline the write tools use, the same as the mute-rule store's.</summary>
    private const int WriteCommandSeconds = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds;

    [McpServerTool(Name = "create_server_tag"), Description(
        "Creates a fleet server tag, at the root or under parent_id. Tags organize the server list and scope custom alert rules: a rule scoped to a tag covers every server under its whole subtree. Names are trimmed, 1-100 characters, unique among siblings ignoring case; tags nest at most four levels. colour is #RRGGBB; omit it for the palette colour the Viewer assigns. Returns the stored tag, or invalid/conflict/not_found with nothing written. Find tag ids and server ids in get_fleet_overview.")]
    public static Task<string> CreateServerTag(
        NpgsqlDataSource postgres,
        [Description("The tag name (1-100 characters, trimmed, unique among siblings).")] string name,
        [Description("The parent tag id (from get_fleet_overview tags); omit for a root tag.")] int? parent_id = null,
        [Description("The colour as #RRGGBB; omit for the palette colour.")] string? colour = null,
        CancellationToken cancellationToken = default) =>
        CreateServerTagCore(new ServerTagStore(postgres, WriteCommandSeconds), name, parent_id, colour, cancellationToken);

    [McpServerTool(Name = "update_server_tag"), Description(
        "PARTIAL edit of a fleet server tag: send only name, colour or parent_id in changes_json; an explicit null clears colour or moves the tag to the root. A move carries the subtree and its assignments, is refused if it would create a cycle or exceed four levels, and changes which servers custom alert rules scoped to old or new ancestors cover: affected_rules names them. Returns updated (re-read from the store), unchanged, or invalid/conflict/not_found with nothing written.")]
    public static Task<string> UpdateServerTag(
        NpgsqlDataSource postgres,
        [Description("The tag id (from get_fleet_overview tags).")] int tag_id,
        [Description("A JSON object with ONLY the fields to change: name, colour, parent_id (e.g. {\"name\":\"East\"}). An explicit null clears colour or moves the tag to the root.")] string changes_json,
        CancellationToken cancellationToken = default) =>
        UpdateServerTagCore(new ServerTagStore(postgres, WriteCommandSeconds), tag_id, changes_json, cancellationToken);

    [McpServerTool(Name = "delete_server_tag"), Description(
        "Deletes a fleet server tag AND its whole subtree of child tags and their server assignments; servers and collected data are untouched. Permanent. If a custom alert rule is scoped to the tag or a descendant, that rule would match no server: the call answers confirm_required naming those rules and deletes nothing; repeat with confirm=true to proceed. Rules scoped to an ancestor are reported as losing servers. Returns deleted with the removed tag ids and assignment count.")]
    public static Task<string> DeleteServerTag(
        NpgsqlDataSource postgres,
        [Description("The tag id (from get_fleet_overview tags).")] int tag_id,
        [Description("True to proceed when custom alert rules are scoped to the subtree; default false.")] bool confirm = false,
        CancellationToken cancellationToken = default) =>
        DeleteServerTagCore(new ServerTagStore(postgres, WriteCommandSeconds), tag_id, confirm, cancellationToken);

    [McpServerTool(Name = "assign_server_tag"), Description(
        "Assigns one fleet server tag to one or more servers (server_ids from get_fleet_overview) in one write; servers already carrying it are a no-op. Every server_id must be a monitored server or nothing is written. Custom alert rules scoped to the tag or an ancestor start covering the added servers: affected_rules names them. Returns assigned with added and already-assigned ids, unchanged, not_found or invalid.")]
    public static Task<string> AssignServerTag(
        NpgsqlDataSource postgres,
        [Description("The tag id (from get_fleet_overview tags).")] int tag_id,
        [Description("1-1000 server ids (from get_fleet_overview); duplicates are ignored.")] int[] server_ids,
        CancellationToken cancellationToken = default) =>
        AssignServerTagCore(new ServerTagStore(postgres, WriteCommandSeconds), tag_id, server_ids, cancellationToken);

    [McpServerTool(Name = "unassign_server_tag"), Description(
        "Removes one fleet server tag from one or more servers (server_ids) in one write; servers not carrying it are a no-op. Custom alert rules scoped to the tag or an ancestor stop covering a removed server unless another tag under the rule's scope still holds it: affected_rules names each rule and the servers it loses. No confirm step; assign again to undo. Returns unassigned, unchanged, not_found or invalid.")]
    public static Task<string> UnassignServerTag(
        NpgsqlDataSource postgres,
        [Description("The tag id (from get_fleet_overview tags).")] int tag_id,
        [Description("1-1000 server ids (from get_fleet_overview); duplicates are ignored.")] int[] server_ids,
        CancellationToken cancellationToken = default) =>
        UnassignServerTagCore(new ServerTagStore(postgres, WriteCommandSeconds), tag_id, server_ids, cancellationToken);

    // ---------------------------------------------------------------- cores

    /// <summary>create_server_tag's body over the store seam.</summary>
    internal static async Task<string> CreateServerTagCore(
        IServerTagStore store, string? name, int? parentId, string? colour, CancellationToken ct = default)
    {
        try
        {
            var before = await store.ReadSnapshotAsync(ct);
            var refusal = ServerTagRules.CheckCreate(before.Tags, name, parentId, colour, out var cleanName, out var cleanColour);
            if (refusal is not null)
            {
                return FromWriteResult(refusal, null)!;
            }

            var written = await store.CreateAsync(cleanName, parentId, cleanColour, ct);
            if (written is not ServerTagWriteResult.Ok ok || ok.Tag is null)
            {
                return FromWriteResult(written, null)!;
            }

            var after = await store.ReadSnapshotAsync(ct);
            var tag = after.Tags.FirstOrDefault(t => t.Id == ok.Tag.Id);
            if (tag is null)
            {
                return ConcurrentlyGone(ok.Tag.Id);
            }

            return Serialize(new JsonObject
            {
                ["status"] = "created",
                ["tag"] = TagNode(after, tag),
                ["affected_rules"] = RulesNode(ServerTagCoverage.Diff(before, after), before, after),
            });
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("create_server_tag", ex);
        }
    }

    /// <summary>update_server_tag's body over the store seam.</summary>
    internal static async Task<string> UpdateServerTagCore(
        IServerTagStore store, int tagId, string? changesJson, CancellationToken ct = default)
    {
        try
        {
            var (edit, error, errorCode) = ParseChanges(changesJson);
            if (error is not null)
            {
                return Refusal("invalid", errorCode ?? "invalid", error);
            }

            var before = await store.ReadSnapshotAsync(ct);
            var refusal = ServerTagRules.CheckUpdate(before.Tags, tagId, edit!, out var cleanName, out var cleanColour);
            if (refusal is not null)
            {
                return FromWriteResult(refusal, null)!;
            }

            var stored = before.Tags.First(t => t.Id == tagId);
            var changed = new List<string>();
            var reduced = new ServerTagEdit(false, null, false, null, false, null);
            if (edit!.HasName && !string.Equals(cleanName, stored.Name, StringComparison.Ordinal))
            {
                changed.Add("name");
                reduced = reduced with { HasName = true, Name = cleanName };
            }

            if (edit.HasColour && !string.Equals(cleanColour, stored.Colour, StringComparison.OrdinalIgnoreCase))
            {
                changed.Add("colour");
                reduced = reduced with { HasColour = true, Colour = cleanColour };
            }

            if (edit.HasParent && edit.ParentId != stored.ParentId)
            {
                changed.Add("parent_id");
                reduced = reduced with { HasParent = true, ParentId = edit.ParentId };
            }

            if (changed.Count == 0)
            {
                return Serialize(new JsonObject
                {
                    ["status"] = "unchanged",
                    ["tag"] = TagNode(before, stored),
                    ["affected_rules"] = new JsonArray(),
                });
            }

            var written = await store.UpdateAsync(tagId, reduced, ct);
            if (written is not ServerTagWriteResult.Ok)
            {
                return FromWriteResult(written, null)!;
            }

            var after = await store.ReadSnapshotAsync(ct);
            var tag = after.Tags.FirstOrDefault(t => t.Id == tagId);
            if (tag is null)
            {
                return ConcurrentlyGone(tagId);
            }

            return Serialize(new JsonObject
            {
                ["status"] = "updated",
                ["updated_fields"] = new JsonArray(changed.Select(f => (JsonNode?)JsonValue.Create(f)).ToArray()),
                ["tag"] = TagNode(after, tag),
                ["affected_rules"] = RulesNode(ServerTagCoverage.Diff(before, after), before, after),
            });
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("update_server_tag", ex);
        }
    }

    /// <summary>delete_server_tag's body over the store seam.</summary>
    internal static async Task<string> DeleteServerTagCore(
        IServerTagStore store, int tagId, bool confirm, CancellationToken ct = default)
    {
        try
        {
            var before = await store.ReadSnapshotAsync(ct);
            var refusal = ServerTagRules.CheckDelete(before.Tags, tagId);
            if (refusal is not null)
            {
                return FromWriteResult(refusal, null)!;
            }

            var scoped = ServerTagCoverage.RulesScopedInSubtree(before, tagId);
            if (scoped.Count > 0 && !confirm)
            {
                var names = string.Join(", ", scoped.Select(r => $"'{r.Name}'"));
                return Serialize(new JsonObject
                {
                    ["status"] = "confirm_required",
                    ["refusal"] = RulesAffectedCode,
                    ["message"] = string.Create(
                        CultureInfo.InvariantCulture,
                        $"Deleting tag {tagId} and its subtree would leave {scoped.Count} custom alert rule(s) scoped to it matching no server: {names}. Nothing was deleted. Repeat with confirm=true to proceed."),
                    ["tag_id"] = tagId,
                    ["affected_rules"] = RulesNode(ServerTagCoverage.SimulateDelete(before, tagId), before, before),
                });
            }

            var written = await store.DeleteAsync(tagId, ct);
            if (written is not ServerTagWriteResult.Ok ok)
            {
                return FromWriteResult(written, null)!;
            }

            var after = await store.ReadSnapshotAsync(ct);
            if (after.Tags.Any(t => t.Id == tagId))
            {
                return Serialize(new JsonObject
                {
                    ["status"] = "error",
                    ["message"] = string.Create(CultureInfo.InvariantCulture, $"Tag {tagId} is still in the store after the delete was reported as done."),
                });
            }

            return Serialize(new JsonObject
            {
                ["status"] = "deleted",
                ["tag_id"] = tagId,
                ["deleted_tag_ids"] = IntArray(ok.RemovedTagIds ?? new[] { tagId }),
                ["removed_assignments"] = ok.RemovedAssignments,
                ["affected_rules"] = RulesNode(ServerTagCoverage.Diff(before, after), before, after),
            });
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("delete_server_tag", ex);
        }
    }

    /// <summary>assign_server_tag's body over the store seam.</summary>
    internal static async Task<string> AssignServerTagCore(
        IServerTagStore store, int tagId, IReadOnlyList<int>? serverIds, CancellationToken ct = default)
    {
        try
        {
            var ids = Normalize(serverIds, out var idsError);
            if (idsError is not null)
            {
                return Refusal("invalid", "invalid", idsError);
            }

            var before = await store.ReadSnapshotAsync(ct);
            var refusal = ServerTagRules.CheckDelete(before.Tags, tagId);
            if (refusal is not null)
            {
                return FromWriteResult(refusal, null)!;
            }

            var registered = await store.FindRegisteredServerIdsAsync(ids, ct);
            var unknown = ids.Where(id => !registered.Contains(id)).ToList();
            if (unknown.Count > 0)
            {
                return Refusal(
                    "not_found",
                    UnknownServerCode,
                    string.Create(CultureInfo.InvariantCulture, $"Server id(s) {string.Join(", ", unknown)} are not monitored servers. Nothing was written."),
                    new JsonObject { ["unknown_server_ids"] = IntArray(unknown) });
            }

            var held = before.Assignments.Where(a => a.TagId == tagId).Select(a => a.ServerId).ToHashSet();
            var pending = ids.Where(id => !held.Contains(id)).ToList();
            IReadOnlyList<int> added = Array.Empty<int>();
            if (pending.Count > 0)
            {
                added = await store.AssignAsync(tagId, pending, ct);
            }

            var after = await store.ReadSnapshotAsync(ct);
            var tag = after.Tags.FirstOrDefault(t => t.Id == tagId);
            if (tag is null)
            {
                return ConcurrentlyGone(tagId);
            }

            var already = ids.Where(id => !added.Contains(id)).ToList();
            var result = new JsonObject
            {
                ["status"] = added.Count == 0 ? "unchanged" : "assigned",
                ["tag"] = TagNode(after, tag),
            };
            if (added.Count > 0)
            {
                result["added_server_ids"] = IntArray(added.OrderBy(i => i));
            }

            result["already_assigned_server_ids"] = IntArray(already);
            result["affected_rules"] = RulesNode(ServerTagCoverage.Diff(before, after), before, after);
            return Serialize(result);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("assign_server_tag", ex);
        }
    }

    /// <summary>unassign_server_tag's body over the store seam. The registered-server check is NOT applied: a
    /// removed server can still hold assignment rows, and those must stay removable.</summary>
    internal static async Task<string> UnassignServerTagCore(
        IServerTagStore store, int tagId, IReadOnlyList<int>? serverIds, CancellationToken ct = default)
    {
        try
        {
            var ids = Normalize(serverIds, out var idsError);
            if (idsError is not null)
            {
                return Refusal("invalid", "invalid", idsError);
            }

            var before = await store.ReadSnapshotAsync(ct);
            var refusal = ServerTagRules.CheckDelete(before.Tags, tagId);
            if (refusal is not null)
            {
                return FromWriteResult(refusal, null)!;
            }

            var held = before.Assignments.Where(a => a.TagId == tagId).Select(a => a.ServerId).ToHashSet();
            var pending = ids.Where(held.Contains).ToList();
            IReadOnlyList<int> removed = Array.Empty<int>();
            if (pending.Count > 0)
            {
                removed = await store.UnassignAsync(tagId, pending, ct);
            }

            var after = await store.ReadSnapshotAsync(ct);
            var tag = after.Tags.FirstOrDefault(t => t.Id == tagId);
            if (tag is null)
            {
                return ConcurrentlyGone(tagId);
            }

            var notAssigned = ids.Where(id => !removed.Contains(id)).ToList();
            var result = new JsonObject
            {
                ["status"] = removed.Count == 0 ? "unchanged" : "unassigned",
                ["tag"] = TagNode(after, tag),
            };
            if (removed.Count > 0)
            {
                result["removed_server_ids"] = IntArray(removed.OrderBy(i => i));
            }

            result["not_assigned_server_ids"] = IntArray(notAssigned);
            result["affected_rules"] = RulesNode(ServerTagCoverage.Diff(before, after), before, after);
            return Serialize(result);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("unassign_server_tag", ex);
        }
    }

    // ------------------------------------------------------------ arguments

    /// <summary>De-duplicates and bounds a server id list; the error is non-null when the list cannot be used.</summary>
    private static List<int> Normalize(IReadOnlyList<int>? serverIds, out string? error)
    {
        error = null;
        var ids = (serverIds ?? Array.Empty<int>()).Distinct().OrderBy(i => i).ToList();
        if (ids.Count == 0)
        {
            error = "Send at least one server id.";
        }
        else if (ids.Count > MaxServerIds)
        {
            error = string.Create(CultureInfo.InvariantCulture, $"At most {MaxServerIds} server ids per call; got {ids.Count}.");
        }
        else if (ids.Any(i => i <= 0))
        {
            error = "Server ids are positive whole numbers.";
        }

        return ids;
    }

    /// <summary>Parses update_server_tag's <c>changes_json</c> the way update_mute_rule parses its body: a
    /// whitelist, the first error wins, and nothing is written when any field is bad.</summary>
    private static (ServerTagEdit? Edit, string? Error, string? Code) ParseChanges(string? changesJson)
    {
        if (string.IsNullOrWhiteSpace(changesJson))
        {
            return (null, "changes_json is required: a JSON object with the fields to change, e.g. {\"name\":\"East\"}.", null);
        }

        JsonNode? root;
        try
        {
            root = JsonNode.Parse(changesJson);
        }
        catch (Exception ex) when (ex is JsonException or ArgumentException)
        {
            return (null, "changes_json is not valid JSON: " + ex.Message, null);
        }

        if (root is not JsonObject body)
        {
            return (null, "changes_json must be a JSON object holding any of name, colour and parent_id.", null);
        }

        bool hasName = false, hasColour = false, hasParent = false;
        string? name = null, colour = null;
        int? parentId = null;
        foreach (var prop in body)
        {
            switch (prop.Key)
            {
                case "name":
                    if (prop.Value is JsonValue nameValue && nameValue.TryGetValue<string>(out var n))
                    {
                        if (string.IsNullOrWhiteSpace(n))
                        {
                            return (null, "'name' is blank. A tag needs a name; send a non-blank value.", ServerTagRules.BadName);
                        }

                        hasName = true;
                        name = n;
                    }
                    else
                    {
                        return (null, "'name' must be a non-blank string; it cannot be cleared.", ServerTagRules.BadName);
                    }

                    break;
                case "colour":
                    if (prop.Value is null)
                    {
                        hasColour = true;
                        colour = null;
                    }
                    else if (prop.Value is JsonValue colourValue && colourValue.TryGetValue<string>(out var c))
                    {
                        if (string.IsNullOrWhiteSpace(c))
                        {
                            return (null, "'colour' is blank. Pass null to clear it, or a #RRGGBB value.", ServerTagRules.BadColour);
                        }

                        hasColour = true;
                        colour = c.Trim();
                    }
                    else
                    {
                        return (null, "'colour' must be a #RRGGBB string, or null to clear it.", ServerTagRules.BadColour);
                    }

                    break;
                case "parent_id":
                    if (prop.Value is null)
                    {
                        hasParent = true;
                        parentId = null;
                    }
                    else if (prop.Value is JsonValue parentValue && parentValue.TryGetValue<int>(out var p))
                    {
                        hasParent = true;
                        parentId = p;
                    }
                    else
                    {
                        return (null, "'parent_id' must be a whole-number tag id, or null to move the tag to the root.", null);
                    }

                    break;
                case "tag_id":
                case "id":
                    return (null, $"'{prop.Key}' is the tag's identity and cannot be edited. Pass the tag to edit as tag_id; send only the fields to change.", UnknownFieldCode);
                case "sort_order":
                    return (null, "'sort_order' is not editable here; the Viewer orders siblings.", UnknownFieldCode);
                default:
                    return (null, $"Unknown field '{prop.Key}'. Editable fields: name, colour, parent_id.", UnknownFieldCode);
            }
        }

        if (!hasName && !hasColour && !hasParent)
        {
            return (null, "No editable fields were provided. Send only the fields to change (name, colour, parent_id).", null);
        }

        return (new ServerTagEdit(hasName, name, hasColour, colour, hasParent, parentId), null, null);
    }

    // -------------------------------------------------------------- results

    /// <summary>Maps a non-Ok store or rules result to its envelope; null for <c>Ok</c>.</summary>
    private static string? FromWriteResult(ServerTagWriteResult result, JsonObject? extra) => result switch
    {
        ServerTagWriteResult.Refused r => Refusal("invalid", r.Code, r.Message, extra),
        ServerTagWriteResult.Conflict c => Refusal("conflict", ServerTagRules.DuplicateName, c.Message, extra),
        ServerTagWriteResult.NotFound n when n.Code is not null => Refusal("not_found", n.Code, n.Message, extra),
        ServerTagWriteResult.NotFound n => Refusal("not_found", null, n.Message, extra),
        _ => null,
    };

    private static string ConcurrentlyGone(int tagId) =>
        Refusal(
            "not_found",
            null,
            string.Create(CultureInfo.InvariantCulture, $"The write to tag {tagId} landed and the tag is no longer in the store: it was deleted concurrently."));

    /// <summary>A <c>{status, refusal, message, ...}</c> envelope for an outcome that wrote nothing.</summary>
    private static string Refusal(string status, string? code, string message, JsonObject? extra = null)
    {
        var node = new JsonObject { ["status"] = status };
        if (code is not null)
        {
            node["refusal"] = code;
        }

        node["message"] = message;
        if (extra is not null)
        {
            foreach (var prop in extra.ToList())
            {
                extra.Remove(prop.Key);
                node[prop.Key] = prop.Value;
            }
        }

        return Serialize(node);
    }

    private static string Serialize(JsonNode node) => node.ToJsonString(McpHelpers.JsonOptions);

    private static JsonArray IntArray(IEnumerable<int> values) =>
        new(values.Select(v => (JsonNode?)JsonValue.Create(v)).ToArray());

    private static JsonObject TagNode(ServerTagSnapshot snapshot, ServerTagRow tag)
    {
        var names = new List<string> { tag.Name };
        var parent = tag.ParentId;
        for (var steps = 0; parent is int id && steps < ServerTagRules.MaxWalkSteps; steps++)
        {
            var row = snapshot.Tags.FirstOrDefault(t => t.Id == id);
            if (row is null)
            {
                break;
            }

            names.Insert(0, row.Name);
            parent = row.ParentId;
        }

        return new JsonObject
        {
            ["tag_id"] = tag.Id,
            ["name"] = tag.Name,
            ["parent_id"] = tag.ParentId,
            ["colour"] = tag.Colour,
            ["sort_order"] = tag.SortOrder,
            ["depth"] = ServerTagRules.Depth(snapshot.Tags, tag.Id),
            ["path"] = string.Join(" / ", names),
        };
    }

    private static JsonArray RulesNode(IReadOnlyList<RuleCoverageEffect> effects, ServerTagSnapshot before, ServerTagSnapshot after)
    {
        var array = new JsonArray();
        foreach (var effect in effects)
        {
            var scope = after.Tags.FirstOrDefault(t => t.Id == effect.ScopeTagId)
                ?? before.Tags.FirstOrDefault(t => t.Id == effect.ScopeTagId);
            array.Add(new JsonObject
            {
                ["rule_id"] = effect.RuleId,
                ["name"] = effect.Name,
                ["enabled"] = effect.Enabled,
                ["scope_tag_id"] = effect.ScopeTagId,
                ["scope_tag_name"] = scope?.Name,
                ["effect"] = effect.Effect,
                ["servers_lost"] = IntArray(effect.ServersLost),
                ["servers_lost_count"] = effect.ServersLostCount,
                ["servers_gained"] = IntArray(effect.ServersGained),
                ["servers_gained_count"] = effect.ServersGainedCount,
            });
        }

        return array;
    }
}
