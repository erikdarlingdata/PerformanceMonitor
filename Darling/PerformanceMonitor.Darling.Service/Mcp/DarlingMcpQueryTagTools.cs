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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Common.Tags;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The query-tag tools (#5981, wave 2 of #5971): five writes (create, update, delete, assign, unassign) and the read
/// <c>list_query_tags</c>. A query tag labels queries by <c>query_hash</c> or by object so the resource roll-ups can group by it;
/// it is never a server tag. The writes are served only with <c>mcp.allowWrites</c> (<see cref="DarlingMcpWritePolicy"/>) and run as
/// the <c>mcp</c> role, which holds INSERT/UPDATE/DELETE on the tag and assignment tables (<see cref="DarlingManagedRoles"/>).
///
/// <para>Every name, colour, depth, duplicate and cycle check is the shared <see cref="TagTreeRules"/>, run in the core before the
/// write and again in <see cref="QueryTagStore"/> under its lock. A write does not rebuild the tag map: it bumps
/// <c>config.query_tag_state.rules_version</c> (a trigger) and the collector's next map pass builds it, and every write result says so.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpQueryTagTools
{
    /// <summary>The per-command deadline the write tools use, the same as the server-tag tools'.</summary>
    internal const int WriteCommandSeconds = DarlingMcpServerTagTools.WriteCommandSeconds;

    /// <summary>The longest hash, database, schema or object text one key part takes.</summary>
    internal const int MaxKeyPartLength = 256;

    /// <summary>The most hand assignments one <c>list_query_tags</c> answer carries.</summary>
    internal const int MaxListedAssignments = 2000;

    /// <summary>The creator stamped on an assignment made through MCP.</summary>
    internal const string CreatedBy = "mcp";

    /// <summary>What every write result says about the tag map (#5985).</summary>
    internal const string MapNote = "The tag map is not rebuilt by this write: it takes effect on the collector's next map pass, not at once.";

    private const string KindHash = "hash";
    private const string KindObject = "object";

    [McpServerTool(Name = "create_query_tag"), Description(
        "Creates a query tag, at the root or under parent_id. A query tag labels queries (by query_hash or by object, see assign_query_tag) so resource use can be grouped by it; it is not a server tag. Names are trimmed, 1-100 characters, unique among siblings ignoring case (the same name under two different parents is fine); tags nest at most four levels. Needs mcp.allowWrites. The tag map takes effect on the collector's next map pass. The answer carries previous (null) and undo (the delete_query_tag call for the new id).")]
    public static Task<string> CreateQueryTag(
        NpgsqlDataSource postgres,
        [Description("The tag name (1-100 characters, trimmed, unique among siblings).")] string name,
        [Description("The parent tag id (from list_query_tags); omit for a root tag.")] int? parent_id = null,
        [Description("The colour as #RRGGBB; omit for the palette colour.")] string? colour = null,
        CancellationToken cancellationToken = default) =>
        CreateQueryTagCore(new QueryTagStore(postgres, WriteCommandSeconds), name, parent_id, colour, cancellationToken);

    [McpServerTool(Name = "update_query_tag"), Description(
        "PARTIAL edit of a query tag: send only name, colour or parent_id in changes_json; an explicit null clears colour or moves the tag to the root. A move carries the subtree, its assignments and its rules; it is refused with cycle if the new parent is the tag or one of its descendants, and with depth_limit past four levels. Needs mcp.allowWrites. The tag map takes effect on the collector's next map pass. The answer carries previous (the tag's name, colour and parent_id before) and undo (the update_query_tag call that puts the changed fields back).")]
    public static Task<string> UpdateQueryTag(
        NpgsqlDataSource postgres,
        [Description("The tag id (from list_query_tags).")] int tag_id,
        [Description("A JSON object with ONLY the fields to change: name, colour, parent_id (e.g. {\"name\":\"Reports\"}). An explicit null clears colour or moves the tag to the root.")] string changes_json,
        CancellationToken cancellationToken = default) =>
        UpdateQueryTagCore(new QueryTagStore(postgres, WriteCommandSeconds), tag_id, changes_json, cancellationToken);

    [McpServerTool(Name = "delete_query_tag"), Description(
        "Deletes a query tag AND its child tags, their hand assignments and their rules. Permanent. A tag that has children, assignments or rules answers confirm_required with the counts and deletes nothing; send it again with confirm=true to proceed. Collected data is untouched. Needs mcp.allowWrites. The tag map takes effect on the collector's next map pass. The answer carries previous (the deleted tag); a tag that had no children, assignments or rules also carries undo (the create_query_tag call, which makes a new id), and any other delete carries an undo_note instead, because no single call restores the subtree.")]
    public static Task<string> DeleteQueryTag(
        NpgsqlDataSource postgres,
        [Description("The tag id (from list_query_tags).")] int tag_id,
        [Description("True to delete a tag that has children, assignments or rules; default false.")] bool confirm = false,
        CancellationToken cancellationToken = default) =>
        DeleteQueryTagCore(new QueryTagStore(postgres, WriteCommandSeconds), tag_id, confirm, cancellationToken);

    [McpServerTool(Name = "assign_query_tag"), Description(
        "Hand-assigns a query tag to one key: kind 'hash' takes query_hash; kind 'object' takes database_name, schema_name AND object_name (all three). server_id limits the assignment to one monitored server; omit it for every server. One tag per key and server scope, with a hash and the object names compared ignoring case: a key that already has a hand tag in that scope moves to this tag, and the result says so (status moved). A hand assignment beats any rule. query_id is kept for display only and never matched on. Needs mcp.allowWrites. The tag map takes effect on the collector's next map pass. The answer carries previous (the tag the key held before in this scope, or null) and undo (unassign_query_tag for a new assignment, assign_query_tag with the earlier tag for a move).")]
    public static Task<string> AssignQueryTag(
        NpgsqlDataSource postgres,
        [Description("The tag id (from list_query_tags).")] int tag_id,
        [Description("The key kind: 'hash' or 'object'.")] string kind,
        [Description("kind 'hash': the query_hash (as the query tools show it, e.g. 0x1A2B...).")] string? query_hash = null,
        [Description("kind 'object': the database name.")] string? database_name = null,
        [Description("kind 'object': the schema name.")] string? schema_name = null,
        [Description("kind 'object': the object (procedure, function, trigger) name.")] string? object_name = null,
        [Description("A monitored server id (from get_fleet_overview) to limit the assignment to; omit for every server.")] int? server_id = null,
        [Description("A Query Store query_id as last seen, kept for display only.")] long? query_id = null,
        CancellationToken cancellationToken = default) =>
        AssignQueryTagCore(new QueryTagStore(postgres, WriteCommandSeconds), tag_id, kind, query_hash, database_name, schema_name, object_name, server_id, query_id, null, cancellationToken);

    [McpServerTool(Name = "unassign_query_tag"), Description(
        "Removes the hand tag from one key in exactly one server scope: the same kind, key parts and server_id that assign_query_tag took (omit server_id for the every-server assignment; a server-limited assignment is a different row). A key with no hand tag in that scope is a no-op. Needs mcp.allowWrites. The tag map takes effect on the collector's next map pass. The answer carries previous (the tag that was removed) and undo (the assign_query_tag call that restores it).")]
    public static Task<string> UnassignQueryTag(
        NpgsqlDataSource postgres,
        [Description("The key kind: 'hash' or 'object'.")] string kind,
        [Description("kind 'hash': the query_hash.")] string? query_hash = null,
        [Description("kind 'object': the database name.")] string? database_name = null,
        [Description("kind 'object': the schema name.")] string? schema_name = null,
        [Description("kind 'object': the object name.")] string? object_name = null,
        [Description("The server id the assignment was limited to; omit for the every-server assignment.")] int? server_id = null,
        CancellationToken cancellationToken = default) =>
        UnassignQueryTagCore(new QueryTagStore(postgres, WriteCommandSeconds), kind, query_hash, database_name, schema_name, object_name, server_id, cancellationToken);

    [McpServerTool(Name = "list_query_tags"), Description(
        "Lists the query tag tree: every tag (tag_id, name, parent_id, depth, path, colour, rule_count, and enabled_rule_count for the rules the resolver considers) with its hand assignments (kind hash or object, key, server_id or null for every server, query_id for display). Each assignment states its precedence: a hand assignment by hash for the server beats one for every server, which beats an object assignment for the server, which beats an object assignment for every server, which beats any rule (rules go by priority then id; precedence_rank is 1 to 5 in that order). map shows whether the tag map has caught up with the last tag, assignment or rule write. Read-only.")]
    public static Task<string> ListQueryTags(
        NpgsqlDataSource postgres,
        CancellationToken cancellationToken = default) =>
        ListQueryTagsCore(new QueryTagStore(postgres, WriteCommandSeconds), cancellationToken);

    // ---------------------------------------------------------------- cores

    /// <summary>create_query_tag's body over the store.</summary>
    internal static async Task<string> CreateQueryTagCore(
        QueryTagStore store, string? name, int? parentId, string? colour, CancellationToken ct = default)
    {
        try
        {
            var before = await store.ReadSnapshotAsync(ct);
            var refusal = TagTreeRules.CheckCreate(QueryTagStore.Nodes(before.Tags), name, parentId, colour, out var cleanName, out var cleanColour);
            if (refusal is not null)
            {
                return FromWriteResult(QueryTagWriteResult.From(refusal));
            }

            var written = await store.CreateAsync(cleanName, parentId, cleanColour, ct);
            if (written is not QueryTagWriteResult.Ok ok || ok.Tag is null)
            {
                return FromWriteResult(written);
            }

            var after = await store.ReadSnapshotAsync(ct);
            var tag = after.Tags.FirstOrDefault(t => t.Id == ok.Tag.Id);
            return tag is null
                ? ConcurrentlyGone(ok.Tag.Id)
                : Serialize(new JsonObject
                {
                    ["status"] = "created",
                    ["tag"] = TagNode(after, tag),
                    ["previous"] = null,
                    ["undo"] = DarlingMcpUndoShape.Call("delete_query_tag", new JsonObject { ["tag_id"] = tag.Id }),
                    ["note"] = MapNote,
                });
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("create_query_tag", ex);
        }
    }

    /// <summary>update_query_tag's body over the store.</summary>
    internal static async Task<string> UpdateQueryTagCore(
        QueryTagStore store, int tagId, string? changesJson, CancellationToken ct = default)
    {
        try
        {
            var (parsed, error, errorCode) = DarlingMcpServerTagTools.ParseChanges(changesJson);
            if (error is not null)
            {
                return Refusal("invalid", errorCode ?? "invalid", error);
            }

            var edit = new TagTreeEdit(parsed!.HasName, parsed.Name, parsed.HasColour, parsed.Colour, parsed.HasParent, parsed.ParentId);
            var before = await store.ReadSnapshotAsync(ct);
            var refusal = TagTreeRules.CheckUpdate(QueryTagStore.Nodes(before.Tags), tagId, edit, out var cleanName, out var cleanColour);
            if (refusal is not null)
            {
                return FromWriteResult(QueryTagWriteResult.From(refusal));
            }

            var stored = before.Tags.First(t => t.Id == tagId);
            var changed = new List<string>();
            var reduced = new TagTreeEdit(false, null, false, null, false, null);
            if (edit.HasName && !string.Equals(cleanName, stored.Name, StringComparison.Ordinal))
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
                    ["previous"] = PreviousTagValues(stored),
                    ["undo"] = UpdateUndo(tagId, stored, edit),
                });
            }

            var written = await store.UpdateAsync(tagId, reduced, ct);
            if (written is not QueryTagWriteResult.Ok)
            {
                return FromWriteResult(written);
            }

            var after = await store.ReadSnapshotAsync(ct);
            var tag = after.Tags.FirstOrDefault(t => t.Id == tagId);
            return tag is null
                ? ConcurrentlyGone(tagId)
                : Serialize(new JsonObject
                {
                    ["status"] = "updated",
                    ["updated_fields"] = new JsonArray(changed.Select(f => (JsonNode?)JsonValue.Create(f)).ToArray()),
                    ["tag"] = TagNode(after, tag),
                    ["previous"] = PreviousTagValues(stored),
                    ["undo"] = UpdateUndo(tagId, stored, reduced),
                    ["note"] = MapNote,
                });
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("update_query_tag", ex);
        }
    }

    /// <summary>delete_query_tag's body over the store.</summary>
    internal static async Task<string> DeleteQueryTagCore(
        QueryTagStore store, int tagId, bool confirm, CancellationToken ct = default)
    {
        try
        {
            var snapshotBefore = await store.ReadSnapshotAsync(ct);
            var storedTag = snapshotBefore.Tags.FirstOrDefault(t => t.Id == tagId);
            var written = await store.DeleteAsync(tagId, confirm, ct);
            if (written is QueryTagWriteResult.ConfirmRequired needed)
            {
                return Serialize(new JsonObject
                {
                    ["status"] = "confirm_required",
                    ["refusal"] = "tag_in_use",
                    ["message"] = string.Create(
                        CultureInfo.InvariantCulture,
                        $"Tag {tagId} has {needed.Impact.ChildTags} child tag(s), {needed.Impact.Assignments} hand assignment(s) and {needed.Impact.Rules} rule(s) in its subtree, and deleting it deletes them all. Nothing was deleted. Repeat with confirm=true to proceed."),
                    ["tag_id"] = tagId,
                    ["child_tags"] = needed.Impact.ChildTags,
                    ["assignments"] = needed.Impact.Assignments,
                    ["rules"] = needed.Impact.Rules,
                });
            }

            if (written is not QueryTagWriteResult.Ok ok)
            {
                return FromWriteResult(written);
            }

            var removed = ok.Removed ?? new QueryTagDeleteImpact(0, 0, 0);
            var deleted = new JsonObject
            {
                ["status"] = "deleted",
                ["tag_id"] = tagId,
                ["deleted_tag_ids"] = IntArray(ok.RemovedTagIds ?? new[] { tagId }),
                ["removed_child_tags"] = removed.ChildTags,
                ["removed_assignments"] = removed.Assignments,
                ["removed_rules"] = removed.Rules,
                ["note"] = MapNote,
            };
            if (storedTag is not null)
            {
                deleted["previous"] = TagNode(snapshotBefore, storedTag);
                if (removed.ChildTags == 0 && removed.Assignments == 0 && removed.Rules == 0)
                {
                    var create = new JsonObject { ["name"] = storedTag.Name };
                    if (storedTag.ParentId is int parent)
                    {
                        create["parent_id"] = parent;
                    }

                    if (storedTag.Colour is not null)
                    {
                        create["colour"] = storedTag.Colour;
                    }

                    deleted["undo"] = DarlingMcpUndoShape.Call("create_query_tag", create);
                    deleted["undo_note"] = "The undo recreates the tag under a NEW id.";
                }
                else
                {
                    deleted["undo_note"] = "No single call undoes this: the delete also removed the child tags, hand assignments and rules listed above, and the previous tag is only the one you named. Recreate the tag with create_query_tag, then re-create what was under it.";
                }
            }

            return Serialize(deleted);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("delete_query_tag", ex);
        }
    }

    /// <summary>assign_query_tag's body over the store.</summary>
    internal static async Task<string> AssignQueryTagCore(
        QueryTagStore store,
        int tagId,
        string? kind,
        string? queryHash,
        string? databaseName,
        string? schemaName,
        string? objectName,
        int? serverId,
        long? queryId,
        string? createdBy,
        CancellationToken ct = default)
    {
        try
        {
            var key = BuildKey(kind, queryHash, databaseName, schemaName, objectName, serverId, out var keyRefusal, requireHashShape: true);
            if (keyRefusal is not null)
            {
                return keyRefusal;
            }

            var before = await store.ReadSnapshotAsync(ct);
            var refusal = TagTreeRules.CheckDelete(QueryTagStore.Nodes(before.Tags), tagId);
            if (refusal is not null)
            {
                return FromWriteResult(QueryTagWriteResult.From(refusal));
            }

            if (key!.ServerId is int server)
            {
                var registered = await store.FindRegisteredServerIdsAsync(new[] { server }, ct);
                if (!registered.Contains(server))
                {
                    return Refusal(
                        "not_found",
                        "unknown_server",
                        string.Create(CultureInfo.InvariantCulture, $"Server id {server} is not a monitored server. Nothing was written."),
                        new JsonObject { ["unknown_server_ids"] = IntArray(new[] { server }) });
                }
            }

            var result = await store.AssignAsync(tagId, key, queryId, createdBy ?? CreatedBy, ct);
            if (result.Outcome == QueryTagAssignOutcome.TagGone || result.Assignment is null)
            {
                return ConcurrentlyGone(tagId);
            }

            var after = await store.ReadSnapshotAsync(ct);
            var tag = after.Tags.FirstOrDefault(t => t.Id == tagId);
            if (tag is null)
            {
                return ConcurrentlyGone(tagId);
            }

            var node = new JsonObject
            {
                ["status"] = result.Outcome switch
                {
                    QueryTagAssignOutcome.Added => "assigned",
                    QueryTagAssignOutcome.Replaced => "moved",
                    _ => "unchanged",
                },
                ["assignment"] = AssignmentNode(result.Assignment, after, false),
                ["tag"] = TagNode(after, tag),
            };
            if (result.Outcome == QueryTagAssignOutcome.Replaced && result.PreviousTagId is int previousId)
            {
                var previous = after.Tags.FirstOrDefault(t => t.Id == previousId);
                node["replaced_tag"] = previous is null ? new JsonObject { ["tag_id"] = previousId } : TagNode(after, previous);
                node["message"] = string.Create(
                    CultureInfo.InvariantCulture,
                    $"The key already had hand tag {previousId} in this server scope (one tag per key); it now has tag {tagId}.");
            }

            node["previous"] = new JsonObject
            {
                ["tag_id"] = result.Outcome switch
                {
                    QueryTagAssignOutcome.Replaced => result.PreviousTagId,
                    QueryTagAssignOutcome.Unchanged => tagId,
                    _ => null,
                },
            };
            node["undo"] = result.Outcome switch
            {
                QueryTagAssignOutcome.Added => DarlingMcpUndoShape.Call("unassign_query_tag", KeyArguments(new JsonObject(), key)),
                QueryTagAssignOutcome.Replaced when result.PreviousTagId is int previousTag =>
                    DarlingMcpUndoShape.Call("assign_query_tag", KeyArguments(new JsonObject { ["tag_id"] = previousTag }, key)),
                _ => DarlingMcpUndoShape.Call("assign_query_tag", KeyArguments(new JsonObject { ["tag_id"] = tagId }, key)),
            };

            if (result.Outcome != QueryTagAssignOutcome.Unchanged)
            {
                node["note"] = MapNote;
            }

            return Serialize(node);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("assign_query_tag", ex);
        }
    }

    /// <summary>unassign_query_tag's body over the store. The registered-server check is not applied: a removed server's rows are cleared
    /// with it, but an id that is no longer registered must not make a leftover row unremovable.</summary>
    internal static async Task<string> UnassignQueryTagCore(
        QueryTagStore store,
        string? kind,
        string? queryHash,
        string? databaseName,
        string? schemaName,
        string? objectName,
        int? serverId,
        CancellationToken ct = default)
    {
        try
        {
            var key = BuildKey(kind, queryHash, databaseName, schemaName, objectName, serverId, out var keyRefusal);
            if (keyRefusal is not null)
            {
                return keyRefusal;
            }

            var removed = await store.UnassignAsync(key!, ct);
            if (removed is null)
            {
                return Serialize(new JsonObject
                {
                    ["status"] = "unchanged",
                    ["previous"] = null,
                    ["undo"] = "None needed: nothing was removed.",
                    ["message"] = "That key has no hand tag in this server scope. Nothing was removed. A server-limited assignment and the every-server assignment are different rows; send server_id to address the first.",
                });
            }

            var after = await store.ReadSnapshotAsync(ct);
            return Serialize(new JsonObject
            {
                ["status"] = "unassigned",
                ["removed_assignment"] = AssignmentNode(removed, after, false),
                ["previous"] = new JsonObject { ["tag_id"] = removed.TagId },
                ["undo"] = DarlingMcpUndoShape.Call("assign_query_tag", AssignmentArguments(removed)),
                ["note"] = MapNote,
            });
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("unassign_query_tag", ex);
        }
    }

    /// <summary>list_query_tags' body over the store.</summary>
    internal static async Task<string> ListQueryTagsCore(QueryTagStore store, CancellationToken ct = default)
    {
        try
        {
            var snapshot = await store.ReadSnapshotAsync(ct);
            var tags = new JsonArray();
            var listed = 0;
            var truncated = false;
            foreach (var tag in InTreeOrder(snapshot.Tags))
            {
                var node = TagNode(snapshot, tag);
                node["rule_count"] = snapshot.RuleCountByTag.GetValueOrDefault(tag.Id);
                node["enabled_rule_count"] = snapshot.EnabledRuleCountByTag.GetValueOrDefault(tag.Id);
                var mine = snapshot.Assignments.Where(a => a.TagId == tag.Id).ToList();
                node["assignment_count"] = mine.Count;
                var assignments = new JsonArray();
                foreach (var assignment in mine)
                {
                    if (listed >= MaxListedAssignments)
                    {
                        truncated = true;
                        break;
                    }

                    listed++;
                    assignments.Add(AssignmentNode(assignment, snapshot, true));
                }

                node["assignments"] = assignments;
                tags.Add(node);
            }

            var result = new JsonObject
            {
                ["status"] = snapshot.Tags.Count == 0 ? "empty" : "ok",
                ["tag_count"] = snapshot.Tags.Count,
                ["tags"] = tags,
                ["precedence"] = new JsonArray(QueryTagResolver.Precedence.Select(step => (JsonNode?)step.Description).ToArray()),
                ["map"] = new JsonObject
                {
                    ["rules_version"] = snapshot.State.RulesVersion,
                    ["built_version"] = snapshot.State.BuiltVersion,
                    ["current"] = snapshot.State.BuiltVersion >= snapshot.State.RulesVersion,
                    ["note"] = "A tag, assignment or rule write takes effect on the collector's next map pass; the map is current when built_version has caught up with rules_version.",
                },
            };
            result["assignments_returned"] = listed;
            result["truncated"] = truncated;
            if (truncated)
            {
                result["message"] = string.Create(CultureInfo.InvariantCulture, $"Only the first {MaxListedAssignments} hand assignments are listed; assignment_count on each tag is complete.");
            }

            if (snapshot.Tags.Count == 0)
            {
                result["message"] = "No query tags exist. create_query_tag adds one.";
            }

            return Serialize(result);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* A cancelled request must surface as cancellation, not as an "error" envelope: the web read dispatch relies on it
               (WebReadCancellationPinTests), the same way every other converted read does. */
            return McpHelpers.FormatError("list_query_tags", ex);
        }
    }

    // ------------------------------------------------------------ arguments

    /// <summary>Validates and trims a key's arguments. Null key plus a ready envelope when the arguments cannot be used.</summary>
    private static QueryTagKey? BuildKey(
        string? kind, string? queryHash, string? databaseName, string? schemaName, string? objectName, int? serverId, out string? refusal,
        bool requireHashShape = false)
    {
        refusal = null;
        var cleanKind = kind?.Trim().ToLowerInvariant();
        if (cleanKind is not (KindHash or KindObject))
        {
            refusal = Refusal("invalid", "bad_kind", "kind must be 'hash' or 'object'.");
            return null;
        }

        /* No sign test on serverId: it is an FNV hash cast to a signed int (ServerIdHelper), so about half of all real ids are
           negative. The registry check in assign_query_tag is what refuses an id that is not a monitored server (#6020). */
        var hash = Clean(queryHash);
        var database = Clean(databaseName);
        var schema = Clean(schemaName);
        var obj = Clean(objectName);
        foreach (var (field, value) in new[] { ("query_hash", hash), ("database_name", database), ("schema_name", schema), ("object_name", obj) })
        {
            if (value is not null && (value.Length > MaxKeyPartLength || value.Any(char.IsControl)))
            {
                refusal = Refusal("invalid", "bad_key", string.Create(CultureInfo.InvariantCulture, $"{field} must be at most {MaxKeyPartLength} characters with no control characters."));
                return null;
            }
        }

        if (cleanKind == KindHash)
        {
            if (hash is null)
            {
                refusal = Refusal("invalid", "missing_key", "kind 'hash' needs query_hash.");
                return null;
            }

            if (database is not null || schema is not null || obj is not null)
            {
                refusal = Refusal("invalid", "bad_key", "kind 'hash' takes query_hash only; database_name, schema_name and object_name belong to kind 'object'.");
                return null;
            }

            /* #6455: a NEW assignment must name something a query hash can be; unassign skips this so a key stored before the check
               can still be removed. */
            if (requireHashShape && !QueryHashIdentity.IsAssignableHash(hash))
            {
                refusal = Refusal("invalid", "bad_key", "query_hash must be hexadecimal: 0x followed by 1 to 16 hex digits (for example 0x1A2B3C4D5E6F7081), or the decimal number the long-query trace stores.");
                return null;
            }

            /* The resolver matches a hash ignoring case, so the key stores one spelling of it (#6020). Unassign skips the shape
               check above, so a key stored before it (all zeros, or neither hex nor decimal) is kept as typed and can be removed. */
            return new QueryTagKey(KindHash, serverId, QueryHashIdentity.CanonicalKey(hash) ?? hash, null, null, null);
        }

        var missing = new[] { ("database_name", database), ("schema_name", schema), ("object_name", obj) }.Where(p => p.Item2 is null).Select(p => p.Item1).ToList();
        if (missing.Count > 0)
        {
            refusal = Refusal("invalid", "missing_key", $"kind 'object' needs database_name, schema_name and object_name; missing: {string.Join(", ", missing)}.");
            return null;
        }

        if (hash is not null)
        {
            refusal = Refusal("invalid", "bad_key", "kind 'object' takes database_name, schema_name and object_name only; query_hash belongs to kind 'hash'.");
            return null;
        }

        return new QueryTagKey(KindObject, serverId, null, database, schema, obj);
    }

    private static string? Clean(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();

    // -------------------------------------------------------------- results

    /// <summary>Maps a non-Ok result to its envelope.</summary>
    private static string FromWriteResult(QueryTagWriteResult result) => result switch
    {
        QueryTagWriteResult.Refused r => Refusal("invalid", r.Code, r.Message),
        QueryTagWriteResult.Conflict c => Refusal("conflict", TagTreeRules.DuplicateName, c.Message),
        QueryTagWriteResult.NotFound n => Refusal("not_found", n.Code, n.Message),
        _ => Serialize(new JsonObject { ["status"] = "error", ["message"] = "The write returned an unexpected result." }),
    };

    private static string ConcurrentlyGone(int tagId) =>
        Refusal(
            "not_found",
            null,
            string.Create(CultureInfo.InvariantCulture, $"Tag {tagId} is no longer in the store: it was deleted concurrently."));

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

    /// <summary>The tags depth-first: roots by name, each followed by its subtree, so a reader sees the tree in order.</summary>
    private static IEnumerable<QueryTagRow> InTreeOrder(IReadOnlyList<QueryTagRow> tags)
    {
        var byParent = tags.ToLookup(t => t.ParentId);
        var seen = new HashSet<int>();
        var stack = new Stack<QueryTagRow>(byParent[null].OrderByDescending(t => t.Name, StringComparer.OrdinalIgnoreCase).ThenByDescending(t => t.Id));
        while (stack.Count > 0)
        {
            var tag = stack.Pop();
            if (!seen.Add(tag.Id))
            {
                continue;
            }

            yield return tag;
            foreach (var child in byParent[tag.Id].OrderByDescending(t => t.Name, StringComparer.OrdinalIgnoreCase).ThenByDescending(t => t.Id))
            {
                stack.Push(child);
            }
        }
    }

    /// <summary>What a tag held before an update (#6403): every field the update tool can change.</summary>
    private static JsonObject PreviousTagValues(QueryTagRow stored) =>
        new() { ["name"] = stored.Name, ["colour"] = stored.Colour, ["parent_id"] = stored.ParentId };

    /// <summary>The update that puts back the fields <paramref name="applied"/> touched, as update_query_tag's own changes_json.</summary>
    private static JsonObject UpdateUndo(int tagId, QueryTagRow stored, TagTreeEdit applied)
    {
        var changes = new JsonObject();
        if (applied.HasName)
        {
            changes["name"] = stored.Name;
        }

        if (applied.HasColour)
        {
            changes["colour"] = stored.Colour;
        }

        if (applied.HasParent)
        {
            changes["parent_id"] = stored.ParentId;
        }

        return DarlingMcpUndoShape.Call("update_query_tag", new JsonObject { ["tag_id"] = tagId, ["changes_json"] = changes.ToJsonString() });
    }

    /// <summary>The key arguments assign_query_tag and unassign_query_tag both take.</summary>
    private static JsonObject KeyArguments(JsonObject arguments, QueryTagKey key)
    {
        arguments["kind"] = key.Kind;
        if (key.QueryHash is not null)
        {
            arguments["query_hash"] = key.QueryHash;
        }

        if (key.DatabaseName is not null)
        {
            arguments["database_name"] = key.DatabaseName;
            arguments["schema_name"] = key.SchemaName;
            arguments["object_name"] = key.ObjectName;
        }

        if (key.ServerId is int server)
        {
            arguments["server_id"] = server;
        }

        return arguments;
    }

    /// <summary>The assign call that recreates a removed hand assignment.</summary>
    private static JsonObject AssignmentArguments(QueryTagAssignmentRow a)
    {
        var arguments = KeyArguments(
            new JsonObject { ["tag_id"] = a.TagId }, new QueryTagKey(a.Kind, a.ServerId, a.QueryHash, a.DatabaseName, a.SchemaName, a.ObjectName));
        if (a.QueryId is long queryId)
        {
            arguments["query_id"] = queryId;
        }

        return arguments;
    }

    private static JsonObject TagNode(QueryTagSnapshot snapshot, QueryTagRow tag)
    {
        var names = new List<string> { tag.Name };
        var parent = tag.ParentId;
        for (var steps = 0; parent is int id && steps < TagTreeRules.MaxWalkSteps; steps++)
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
            ["depth"] = TagTreeRules.Depth(QueryTagStore.Nodes(snapshot.Tags), tag.Id),
            ["path"] = string.Join(" / ", names),
        };
    }

    private static JsonObject AssignmentNode(QueryTagAssignmentRow a, QueryTagSnapshot snapshot, bool withPrecedence)
    {
        var node = new JsonObject
        {
            ["assignment_id"] = a.Id,
            ["tag_id"] = a.TagId,
            ["kind"] = a.Kind,
            ["server_id"] = a.ServerId,
            ["server_name"] = a.ServerId is int s && snapshot.ServerNames.TryGetValue(s, out var name) ? name : null,
            ["scope"] = a.ServerId is null ? "every server" : "one server",
            ["query_hash"] = a.QueryHash,
            ["database_name"] = a.DatabaseName,
            ["schema_name"] = a.SchemaName,
            ["object_name"] = a.ObjectName,
            ["query_id"] = a.QueryId,
            ["created_at"] = a.CreatedAt.ToString("O", CultureInfo.InvariantCulture),
            ["created_by"] = a.CreatedBy,
        };
        if (withPrecedence)
        {
            node["source"] = a.Kind == KindHash ? "hand_hash" : "hand_object";
            node["precedence_rank"] = QueryTagResolver.RankOfAssignment(a.Kind == KindHash ? QueryTagSource.HandHash : QueryTagSource.HandObject, a.ServerId is not null);
            node["beats_rules"] = true;
        }

        return node;
    }
}
