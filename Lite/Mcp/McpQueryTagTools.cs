/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections;
using System.ComponentModel;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Common.Tags;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The query-tag tools (#5991): Lite's twin of Darling's query-tag and query-tag-rule tools (#5981, #5982), under the same
/// names, parameters, refusal codes and result shapes. Eight writes (create, update, delete, assign and unassign a tag;
/// create, update and delete a rule) are served only with <c>mcp_allow_writes</c> (<see cref="McpWritePolicy"/>); the three
/// reads (<c>list_query_tags</c>, <c>list_query_tag_rules</c>, <c>test_query_tag_rule</c>) are ordinary reads.
///
/// <para>Thin over <c>LocalDataService.QueryTags</c> (#5989): every name, colour, depth, duplicate and cycle check is the
/// shared <see cref="TagTreeRules"/> and every rule check the shared <see cref="QueryTagRuleValidator"/>, run in the store.
/// <c>test_query_tag_rule</c> hands Lite's sightings to the shared <see cref="QueryTagResolver"/>; there is no second matcher.
/// A write does not rebuild the tag map: it bumps <c>rules_version</c> and the map's next pass picks it up, and every write
/// result says so.</para>
/// </summary>
[McpServerToolType]
public sealed class McpQueryTagTools
{
    internal const int MaxKeyPartLength = 256;
    internal const int MaxListedAssignments = 2000;
    internal const int MaxSamples = QueryTagRuleTester.MaxSamples;
    internal const int MaxHeldSamples = QueryTagRuleTester.MaxHeldSamples;
    internal const int MaxSightingRows = QueryTagRuleTester.MaxSightingRows;
    internal const int DefaultWindowHours = QueryTagRuleTester.DefaultWindowHours;
    internal const int MaxWindowHours = QueryTagRuleTester.MaxWindowHours;
    internal const int DefaultPriority = 100;
    internal const string CreatedBy = "mcp";
    internal const string MapNote = "The tag map is not rebuilt by this write: it takes effect on the next map pass, not at once.";
    private const string RuleMapNote = "The tag map is rebuilt on the next map pass; this call changed no map row.";

    private const string KindHash = "hash";
    private const string KindObject = "object";

    // ------------------------------------------------------------------ tag tools

    [McpServerTool(Name = "create_query_tag"), Description(
        "Creates a query tag, at the root or under parent_id. A query tag labels queries (by query_hash or by object, see assign_query_tag) so resource use can be grouped by it; it is not a server tag. Names are trimmed, 1-100 characters, unique among siblings ignoring case (the same name under two different parents is fine); tags nest at most four levels. Needs mcp_allow_writes. The tag map takes effect on its next pass. The answer carries previous (null) and undo (the delete_query_tag call for the new id).")]
    public static Task<string> CreateQueryTag(
        LocalDataService dataService,
        [Description("The tag name (1-100 characters, trimmed, unique among siblings).")] string name,
        [Description("The parent tag id (from list_query_tags); omit for a root tag.")] int? parent_id = null,
        [Description("The colour as #RRGGBB; omit for the palette colour.")] string? colour = null) =>
        CreateQueryTagCore(dataService, name, parent_id, colour);

    [McpServerTool(Name = "update_query_tag"), Description(
        "PARTIAL edit of a query tag: send only name, colour or parent_id in changes_json; an explicit null clears colour or moves the tag to the root. A move carries the subtree, its assignments and its rules; it is refused with cycle if the new parent is the tag or one of its descendants, and with depth_limit past four levels. Needs mcp_allow_writes. The tag map takes effect on its next pass. The answer carries previous (the name, colour and parent_id before) and undo (the update_query_tag call that puts the changed fields back).")]
    public static Task<string> UpdateQueryTag(
        LocalDataService dataService,
        [Description("The tag id (from list_query_tags).")] int tag_id,
        [Description("A JSON object with ONLY the fields to change: name, colour, parent_id (e.g. {\"name\":\"Reports\"}). An explicit null clears colour or moves the tag to the root.")] string changes_json) =>
        UpdateQueryTagCore(dataService, tag_id, changes_json);

    [McpServerTool(Name = "delete_query_tag"), Description(
        "Deletes a query tag AND its child tags, their hand assignments and their rules. Permanent. A tag that has children, assignments or rules answers confirm_required with the counts and deletes nothing; send it again with confirm=true to proceed. Collected data is untouched. Needs mcp_allow_writes. The tag map takes effect on its next pass. The answer carries previous (the deleted tag); a tag that had no children, assignments or rules also carries undo (the create_query_tag call, which makes a new id), and any other delete carries an undo_note instead, because no single call restores the subtree.")]
    public static Task<string> DeleteQueryTag(
        LocalDataService dataService,
        [Description("The tag id (from list_query_tags).")] int tag_id,
        [Description("True to delete a tag that has children, assignments or rules; default false.")] bool confirm = false) =>
        DeleteQueryTagCore(dataService, tag_id, confirm);

    [McpServerTool(Name = "assign_query_tag"), Description(
        "Hand-assigns a query tag to one key: kind 'hash' takes query_hash; kind 'object' takes database_name, schema_name AND object_name (all three). server_id limits the assignment to one monitored server; omit it for every server. One tag per key and server scope: a key that already has a hand tag in that scope moves to this tag, and the result says so (status moved). A hand assignment beats any rule. query_id is kept for display only and never matched on. Needs mcp_allow_writes. The tag map takes effect on its next pass. The answer carries previous (the tag the key held before in this scope, or null) and undo (unassign_query_tag for a new assignment, assign_query_tag with the earlier tag for a move).")]
    public static Task<string> AssignQueryTag(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("The tag id (from list_query_tags).")] int tag_id,
        [Description("The key kind: 'hash' or 'object'.")] string kind,
        [Description("kind 'hash': the query_hash (as the query tools show it, e.g. 0x1A2B...).")] string? query_hash = null,
        [Description("kind 'object': the database name.")] string? database_name = null,
        [Description("kind 'object': the schema name.")] string? schema_name = null,
        [Description("kind 'object': the object (procedure, function, trigger) name.")] string? object_name = null,
        [Description("A monitored server id (from list_servers) to limit the assignment to; omit for every server.")] int? server_id = null,
        [Description("A Query Store query_id as last seen, kept for display only.")] long? query_id = null) =>
        AssignQueryTagCore(dataService, serverManager.GetKnownServerIds(), tag_id, kind, query_hash, database_name, schema_name, object_name, server_id, query_id);

    [McpServerTool(Name = "unassign_query_tag"), Description(
        "Removes the hand tag from one key in exactly one server scope: the same kind, key parts and server_id that assign_query_tag took (omit server_id for the every-server assignment; a server-limited assignment is a different row). A key with no hand tag in that scope is a no-op. Needs mcp_allow_writes. The tag map takes effect on its next pass. The answer carries previous (the tag that was removed) and undo (the assign_query_tag call that restores it).")]
    public static Task<string> UnassignQueryTag(
        LocalDataService dataService,
        [Description("The key kind: 'hash' or 'object'.")] string kind,
        [Description("kind 'hash': the query_hash.")] string? query_hash = null,
        [Description("kind 'object': the database name.")] string? database_name = null,
        [Description("kind 'object': the schema name.")] string? schema_name = null,
        [Description("kind 'object': the object name.")] string? object_name = null,
        [Description("The server id the assignment was limited to; omit for the every-server assignment.")] int? server_id = null) =>
        UnassignQueryTagCore(dataService, kind, query_hash, database_name, schema_name, object_name, server_id);

    [McpServerTool(Name = "list_query_tags"), Description(
        "Lists the query tag tree: every tag (tag_id, name, parent_id, depth, path, colour, rule_count) with its hand assignments (kind hash or object, key, server_id or null for every server, query_id for display). Each assignment states its precedence: a hand assignment by hash for the server beats one for every server, which beats one by object, which beats any rule (rules go by priority then id). map shows whether the tag map has caught up with the last tag, assignment or rule write. Read-only.")]
    public static Task<string> ListQueryTags(LocalDataService dataService, ServerManager serverManager) =>
        ListQueryTagsCore(dataService, serverManager.GetAllServers());

    // ------------------------------------------------------------------ rule tools

    [McpServerTool(Name = "create_query_tag_rule"), Description(
        "Creates a query tag rule: new queries whose application, login, host, database, object or query text matches the pattern get the rule's tag " +
        "the next time the tag map is rebuilt (this call changes no map row). field is one of application, login, host, database, object, text, text_tag. " +
        "A pattern ignores case and uses * as its only wildcard (no regular expressions); application, login, host, database and object match the whole value " +
        "(an object is matched as database.schema.name, so start with *. to ignore the database), text matches anywhere in the first 4,000 characters of the query text. " +
        "text_tag reads a /* tag:NAME */ or -- tag:NAME comment in the query text and uses NAME as the tag: it takes no tag_id and its pattern is unused; every other field needs a tag_id " +
        "(from the query tag tools) and a pattern of 1-400 characters. Rules apply by priority (lower first), then id; the first match wins, but a hand assignment beats any rule. " +
        "server_id limits the rule to one server (omit it for every server). Test a rule first with test_query_tag_rule. Needs mcp_allow_writes. The answer carries previous (null) and undo (the delete_query_tag_rule call for the new id).")]
    public static Task<string> CreateQueryTagRule(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("What the rule matches on: application, login, host, database, object, text or text_tag.")] string field,
        [Description("The pattern (1-400 characters, * is the only wildcard). Omit for text_tag.")] string? pattern = null,
        [Description("The tag the rule applies. Required for every field except text_tag, which must not have one.")] int? tag_id = null,
        [Description("Lower runs first; default 100. Ties go to the lower rule id.")] int priority = DefaultPriority,
        [Description("Whether the rule applies; default true.")] bool enabled = true,
        [Description("The server the rule applies to (from list_servers); omit for every server.")] int? server_id = null) =>
        CreateRuleCore(dataService, serverManager.GetKnownServerIds(), field, pattern, tag_id, priority, enabled, server_id);

    [McpServerTool(Name = "update_query_tag_rule"), Description(
        "PARTIAL edit of a query tag rule: send only the fields to change in changes_json, from pattern, priority, enabled, tag_id and server_id. An explicit null for server_id " +
        "makes the rule apply to every server. The rule's field cannot change (delete it and create another). The edit is validated like a new rule. " +
        "Setting enabled to false keeps the rule in list_query_tag_rules with enabled false. The change reaches the tag map on its next pass. Needs mcp_allow_writes. The answer carries previous (the pattern, priority, enabled, tag_id and server_id before) and undo (the update_query_tag_rule call that puts the changed fields back).")]
    public static Task<string> UpdateQueryTagRule(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("The rule id (from list_query_tag_rules).")] int rule_id,
        [Description("A JSON object with ONLY the fields to change: pattern, priority, enabled, tag_id, server_id (e.g. {\"enabled\":false}).")] string changes_json) =>
        UpdateRuleCore(dataService, serverManager.GetKnownServerIds(), rule_id, changes_json);

    [McpServerTool(Name = "delete_query_tag_rule"), Description(
        "Deletes a query tag rule. Permanent; queries it tagged lose that tag at the next map pass (unless another rule or a hand assignment holds them). " +
        "To keep the rule but stop it applying, set enabled to false with update_query_tag_rule. Needs mcp_allow_writes. The answer carries previous (the deleted rule) and undo (the create_query_tag_rule call, which makes a new id).")]
    public static Task<string> DeleteQueryTagRule(
        LocalDataService dataService,
        [Description("The rule id (from list_query_tag_rules).")] int rule_id) =>
        DeleteRuleCore(dataService, rule_id);

    [McpServerTool(Name = "list_query_tag_rules"), Description(
        "Lists the query tag rules in the order they apply (priority, then id): id, field, pattern, priority, enabled, tag_id and tag path, server_id, who created it and when. " +
        "Disabled rules are listed with enabled false. Also returns rules_version and built_version: the tag map is stale while built_version is below rules_version, until the next map pass. " +
        "server_id keeps only the rules that apply to that server (its own and the every-server rules).")]
    public static Task<string> ListQueryTagRules(
        LocalDataService dataService,
        [Description("Only rules that apply to this server (from list_servers); omit for all.")] int? server_id = null) =>
        ListRulesCore(dataService, server_id);

    [McpServerTool(Name = "test_query_tag_rule"), Description(
        "Dry run, writes nothing: runs a saved rule (rule_id) or a draft rule (field, pattern, tag_id, priority) over the recent query sightings of one server or every server and reports " +
        "what it would tag: how many keys the rule matches, how many of those a hand assignment or a rule that ranks ahead already holds (so the rule would not change them), " +
        "and up to 25 samples with the matched field and value. It uses the same matcher the tag map builder runs. A disabled saved rule is tested as if it were enabled. " +
        "A draft is tested as a new rule would rank: its priority, and last among equals. Sightings are what the collectors saw in the window; keys not yet sighted are not tested.")]
    public static Task<string> TestQueryTagRule(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("A saved rule to test (from list_query_tag_rules). Omit to test a draft built from field, pattern and tag_id.")] int? rule_id = null,
        [Description("Draft: what the rule matches on (application, login, host, database, object, text, text_tag).")] string? field = null,
        [Description("Draft: the pattern. Omit for text_tag.")] string? pattern = null,
        [Description("Draft: the tag the rule applies; omit for text_tag.")] int? tag_id = null,
        [Description("Draft: the rule priority (lower first); default 100.")] int priority = DefaultPriority,
        [Description("Test against this server's sightings (from list_servers); omit for every server.")] int? server_id = null,
        [Description("How far back to look at sightings, in hours (1-8760); default 168.")] int window_hours = DefaultWindowHours)
    {
        return TestRuleCore(dataService, serverManager.GetKnownServerIds(), rule_id, field, pattern, tag_id, priority, server_id, window_hours);
    }

    // ------------------------------------------------------------------ tag cores

    internal static Task<string> CreateQueryTagCore(LocalDataService data, string? name, int? parentId, string? colour) =>
        GuardAsync("create_query_tag", async () =>
        {
            var id = await data.CreateQueryTagAsync(name!, parentId, colour);
            var snapshot = await ReadAsync(data);
            var tag = snapshot.Tags.FirstOrDefault(t => t.Id == id);
            return tag is null
                ? ConcurrentlyGone(id)
                : Serialize(new JsonObject
                {
                    ["status"] = "created",
                    ["tag"] = TagNode(snapshot, tag),
                    ["previous"] = null,
                    ["undo"] = UndoCall("delete_query_tag", new JsonObject { ["tag_id"] = tag.Id }),
                    ["note"] = MapNote,
                });
        });

    internal static Task<string> UpdateQueryTagCore(LocalDataService data, int tagId, string? changesJson) =>
        GuardAsync("update_query_tag", async () =>
        {
            var (edit, error, code) = ParseTagChanges(changesJson);
            if (error is not null)
            {
                return Refusal("invalid", code ?? "invalid", error);
            }

            var before = await ReadAsync(data);
            var nodes = Nodes(before.Tags);
            var refusal = TagTreeRules.CheckUpdate(nodes, tagId, edit!, out var cleanName, out var cleanColour);
            if (refusal is not null)
            {
                return FromRefusal(refusal.Kind, refusal.Code, refusal.Message);
            }

            var stored = before.Tags.First(t => t.Id == tagId);
            var changed = new List<string>();
            var reduced = new TagTreeEdit(false, null, false, null, false, null);
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
                    ["previous"] = PreviousTagValues(stored),
                    ["undo"] = TagUpdateUndo(tagId, stored, edit),
                });
            }

            await data.UpdateQueryTagAsync(tagId, reduced);
            var after = await ReadAsync(data);
            var tag = after.Tags.FirstOrDefault(t => t.Id == tagId);
            return tag is null
                ? ConcurrentlyGone(tagId)
                : Serialize(new JsonObject
                {
                    ["status"] = "updated",
                    ["updated_fields"] = new JsonArray(changed.Select(f => (JsonNode?)JsonValue.Create(f)).ToArray()),
                    ["tag"] = TagNode(after, tag),
                    ["previous"] = PreviousTagValues(stored),
                    ["undo"] = TagUpdateUndo(tagId, stored, reduced),
                    ["note"] = MapNote,
                });
        });

    internal static Task<string> DeleteQueryTagCore(LocalDataService data, int tagId, bool confirm) =>
        GuardAsync("delete_query_tag", async () =>
        {
            var snapshot = await ReadAsync(data);
            var nodes = Nodes(snapshot.Tags);
            var refusal = TagTreeRules.CheckDelete(nodes, tagId);
            if (refusal is not null)
            {
                return FromRefusal(refusal.Kind, refusal.Code, refusal.Message);
            }

            var subtree = TagTreeRules.Descendants(nodes, tagId);
            var children = subtree.Count;
            subtree.Add(tagId);
            var assignments = snapshot.Assignments.Count(a => subtree.Contains(a.TagId));
            var rules = snapshot.Rules.Count(r => r.TagId is int t && subtree.Contains(t));
            if (!confirm && (children > 0 || assignments > 0 || rules > 0))
            {
                return Serialize(new JsonObject
                {
                    ["status"] = "confirm_required",
                    ["refusal"] = "tag_in_use",
                    ["message"] = string.Create(
                        CultureInfo.InvariantCulture,
                        $"Tag {tagId} has {children} child tag(s), {assignments} hand assignment(s) and {rules} rule(s) in its subtree, and deleting it deletes them all. Nothing was deleted. Repeat with confirm=true to proceed."),
                    ["tag_id"] = tagId,
                    ["child_tags"] = children,
                    ["assignments"] = assignments,
                    ["rules"] = rules,
                });
            }

            await data.DeleteQueryTagAsync(tagId);
            var deleted = new JsonObject
            {
                ["status"] = "deleted",
                ["tag_id"] = tagId,
                ["deleted_tag_ids"] = new JsonArray(subtree.OrderBy(i => i).Select(i => (JsonNode?)JsonValue.Create(i)).ToArray()),
                ["removed_child_tags"] = children,
                ["removed_assignments"] = assignments,
                ["removed_rules"] = rules,
                ["note"] = MapNote,
            };
            var storedTag = snapshot.Tags.FirstOrDefault(t => t.Id == tagId);
            if (storedTag is not null)
            {
                deleted["previous"] = TagNode(snapshot, storedTag);
                if (children == 0 && assignments == 0 && rules == 0)
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

                    deleted["undo"] = UndoCall("create_query_tag", create);
                    deleted["undo_note"] = "The undo recreates the tag under a NEW id.";
                }
                else
                {
                    deleted["undo_note"] = "No single call undoes this: the delete also removed the child tags, hand assignments and rules listed above, and the previous tag is only the one you named. Recreate the tag with create_query_tag, then re-create what was under it.";
                }
            }

            return Serialize(deleted);
        });

    internal static Task<string> AssignQueryTagCore(
        LocalDataService data, IReadOnlyCollection<int> knownServers, int tagId, string? kind, string? queryHash,
        string? databaseName, string? schemaName, string? objectName, int? serverId, long? queryId) =>
        GuardAsync("assign_query_tag", async () =>
        {
            var key = BuildKey(kind, queryHash, databaseName, schemaName, objectName, serverId, out var keyRefusal, requireHashShape: true);
            if (keyRefusal is not null)
            {
                return keyRefusal;
            }

            var before = await ReadAsync(data);
            var refusal = TagTreeRules.CheckDelete(Nodes(before.Tags), tagId);
            if (refusal is not null)
            {
                return FromRefusal(refusal.Kind, refusal.Code, refusal.Message);
            }

            if (serverId is int server && !knownServers.Contains(server))
            {
                return UnknownServer(server);
            }

            var existing = before.Assignments.FirstOrDefault(a => SameKey(a, key!));
            var status = "assigned";
            int? previousTagId = null;
            if (existing is not null)
            {
                if (existing.TagId == tagId)
                {
                    return Serialize(new JsonObject
                    {
                        ["status"] = "unchanged",
                        ["assignment"] = AssignmentNode(existing, before, false),
                        ["tag"] = TagNode(before, before.Tags.First(t => t.Id == tagId)),
                        ["previous"] = new JsonObject { ["tag_id"] = tagId },
                        ["undo"] = UndoCall("assign_query_tag", KeyArguments(new JsonObject { ["tag_id"] = tagId }, key!)),
                    });
                }

                // One tag per key and scope: the store refuses a second row, so the move is a delete then an add.
                await data.DeleteQueryTagAssignmentAsync(existing.Id);
                previousTagId = existing.TagId;
                status = "moved";
            }

            var newId = await data.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(
                tagId, key!.Kind == KindHash ? QueryTagKeyKind.Hash : QueryTagKeyKind.Object, serverId, key.Hash,
                key.Database, key.Schema, key.Name, queryId, CreatedBy));
            var after = await ReadAsync(data);
            var row = after.Assignments.FirstOrDefault(a => a.Id == newId);
            var tag = after.Tags.FirstOrDefault(t => t.Id == tagId);
            if (row is null || tag is null)
            {
                return ConcurrentlyGone(tagId);
            }

            var node = new JsonObject
            {
                ["status"] = status,
                ["assignment"] = AssignmentNode(row, after, false),
                ["tag"] = TagNode(after, tag),
            };
            if (previousTagId is int previousId)
            {
                var previous = after.Tags.FirstOrDefault(t => t.Id == previousId);
                node["replaced_tag"] = previous is null ? new JsonObject { ["tag_id"] = previousId } : TagNode(after, previous);
                node["message"] = string.Create(
                    CultureInfo.InvariantCulture,
                    $"The key already had hand tag {previousId} in this server scope (one tag per key); it now has tag {tagId}.");
            }

            node["previous"] = new JsonObject { ["tag_id"] = previousTagId };
            node["undo"] = previousTagId is int earlierTag
                ? UndoCall("assign_query_tag", KeyArguments(new JsonObject { ["tag_id"] = earlierTag }, key!))
                : UndoCall("unassign_query_tag", KeyArguments(new JsonObject(), key!));
            node["note"] = MapNote;
            return Serialize(node);
        });

    internal static Task<string> UnassignQueryTagCore(
        LocalDataService data, string? kind, string? queryHash, string? databaseName, string? schemaName, string? objectName, int? serverId) =>
        GuardAsync("unassign_query_tag", async () =>
        {
            var key = BuildKey(kind, queryHash, databaseName, schemaName, objectName, serverId, out var keyRefusal);
            if (keyRefusal is not null)
            {
                return keyRefusal;
            }

            var snapshot = await ReadAsync(data);
            var existing = snapshot.Assignments.FirstOrDefault(a => SameKey(a, key!));
            if (existing is null)
            {
                return Serialize(new JsonObject
                {
                    ["status"] = "unchanged",
                    ["previous"] = null,
                    ["undo"] = "None needed: nothing was removed.",
                    ["message"] = "That key has no hand tag in this server scope. Nothing was removed. A server-limited assignment and the every-server assignment are different rows; send server_id to address the first.",
                });
            }

            await data.DeleteQueryTagAssignmentAsync(existing.Id);
            return Serialize(new JsonObject
            {
                ["status"] = "unassigned",
                ["removed_assignment"] = AssignmentNode(existing, snapshot, false),
                ["previous"] = new JsonObject { ["tag_id"] = existing.TagId },
                ["undo"] = UndoCall("assign_query_tag", AssignmentArguments(existing)),
                ["note"] = MapNote,
            });
        });

    internal static Task<string> ListQueryTagsCore(LocalDataService data, IReadOnlyCollection<ServerConnection> servers) =>
        GuardAsync("list_query_tags", async () =>
        {
            var snapshot = await ReadAsync(data, ServerNameMap(servers));
            var tags = new JsonArray();
            var listed = 0;
            var truncated = false;
            foreach (var tag in InTreeOrder(snapshot.Tags))
            {
                var node = TagNode(snapshot, tag);
                node["rule_count"] = snapshot.Rules.Count(r => r.TagId == tag.Id);
                node["enabled_rule_count"] = snapshot.Rules.Count(r => r.TagId == tag.Id && r.Enabled);
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
                    ["note"] = "A tag, assignment or rule write takes effect on the next map pass; the map is current when built_version has caught up with rules_version.",
                },
                ["assignments_returned"] = listed,
                ["truncated"] = truncated,
            };
            if (truncated)
            {
                result["message"] = string.Create(CultureInfo.InvariantCulture, $"Only the first {MaxListedAssignments} hand assignments are listed; assignment_count on each tag is complete.");
            }

            if (snapshot.Tags.Count == 0)
            {
                result["message"] = "No query tags exist. create_query_tag adds one.";
            }

            return Serialize(result);
        });

    // ------------------------------------------------------------------ rule cores

    internal static Task<string> CreateRuleCore(
        LocalDataService data, IReadOnlyCollection<int> knownServers, string? field, string? pattern, int? tagId, int priority, bool enabled, int? serverId) =>
        GuardAsync("create_query_tag_rule", async () =>
        {
            if (!TryParseField(field, out var parsed))
            {
                return Refusal("invalid", "invalid_field", "field must be one of application, login, host, database, object, text, text_tag.");
            }

            var stored = parsed == QueryTagRuleField.TextTag ? null : pattern;
            var tags = await data.GetQueryTagsAsync();
            var refusal = CheckRule(tags, knownServers, parsed, stored, tagId, serverId, priority, enabled, true, true);
            if (refusal is not null)
            {
                return refusal;
            }

            var id = await data.AddQueryTagRuleAsync(new QueryTagRuleInput(parsed, stored, tagId, priority, enabled, serverId, CreatedBy));
            var row = (await data.GetQueryTagRulesAsync()).FirstOrDefault(r => r.Id == id);
            var state = await data.GetQueryTagStateAsync();
            return row is null
                ? NotFoundRule(id)
                : Serialize(new JsonObject
                {
                    ["status"] = "created",
                    ["rule"] = RuleNode(row, tags),
                    ["previous"] = null,
                    ["undo"] = UndoCall("delete_query_tag_rule", new JsonObject { ["rule_id"] = row.Id }),
                    ["rules_version"] = state.RulesVersion,
                    ["map_refresh"] = RuleMapNote,
                });
        });

    internal static Task<string> UpdateRuleCore(LocalDataService data, IReadOnlyCollection<int> knownServers, int ruleId, string? changesJson) =>
        GuardAsync("update_query_tag_rule", async () =>
        {
            var (changes, error) = ParseRuleChanges(changesJson);
            if (error is not null)
            {
                return error;
            }

            var current = (await data.GetQueryTagRulesAsync()).FirstOrDefault(r => r.Id == ruleId);
            if (current is null)
            {
                return NotFoundRule(ruleId);
            }

            if (!TryParseField(current.MatchField, out var field))
            {
                return Refusal("invalid", "invalid_field", "The stored rule has an unknown field.");
            }

            var pattern = current.Pattern;
            if (changes!.HasPattern)
            {
                if (field == QueryTagRuleField.TextTag)
                {
                    return Refusal("invalid", "text_tag_has_no_pattern", "A text_tag rule reads its tag from the query text and has no pattern to change.");
                }

                pattern = changes.Pattern!;
            }

            var priority = changes.HasPriority ? changes.Priority : current.Priority;
            var enabled = changes.HasEnabled ? changes.Enabled : current.Enabled;
            var tagId = changes.HasTagId ? changes.TagId : current.TagId;
            var serverId = changes.HasServerId ? changes.ServerId : current.ServerId;
            var tags = await data.GetQueryTagsAsync();

            if (pattern == current.Pattern && priority == current.Priority && enabled == current.Enabled && tagId == current.TagId && serverId == current.ServerId)
            {
                return Serialize(new JsonObject
                {
                    ["status"] = "unchanged",
                    ["rule"] = RuleNode(current, tags),
                    ["previous"] = PreviousRuleValues(current),
                    ["undo"] = RuleUpdateUndo(current, changes),
                });
            }

            // A tag or server this edit does not touch is not re-checked: a rule must stay pausable after its tag was removed elsewhere.
            var refusal = CheckRule(
                tags, knownServers, field, field == QueryTagRuleField.TextTag ? null : pattern, tagId, serverId, priority, enabled,
                changes.HasTagId && tagId != current.TagId, changes.HasServerId && serverId != current.ServerId);
            if (refusal is not null)
            {
                return refusal;
            }

            await data.UpdateQueryTagRuleAsync(ruleId, new QueryTagRuleInput(
                field, field == QueryTagRuleField.TextTag ? current.Pattern : pattern, tagId, priority, enabled, serverId, current.CreatedBy));
            var row = (await data.GetQueryTagRulesAsync()).FirstOrDefault(r => r.Id == ruleId);
            var state = await data.GetQueryTagStateAsync();
            return row is null
                ? NotFoundRule(ruleId)
                : Serialize(new JsonObject
                {
                    ["status"] = "updated",
                    ["rule"] = RuleNode(row, tags),
                    ["previous"] = PreviousRuleValues(current),
                    ["undo"] = RuleUpdateUndo(current, changes),
                    ["rules_version"] = state.RulesVersion,
                    ["map_refresh"] = RuleMapNote,
                });
        });

    internal static Task<string> DeleteRuleCore(LocalDataService data, int ruleId) =>
        GuardAsync("delete_query_tag_rule", async () =>
        {
            var before = (await data.GetQueryTagRulesAsync()).FirstOrDefault(r => r.Id == ruleId);
            if (before is null)
            {
                return NotFoundRule(ruleId);
            }

            await data.DeleteQueryTagRuleAsync(ruleId);
            var state = await data.GetQueryTagStateAsync();
            return Serialize(new JsonObject
            {
                ["status"] = "deleted",
                ["rule_id"] = ruleId,
                ["previous"] = RuleNode(before, await data.GetQueryTagsAsync()),
                ["undo"] = UndoCall("create_query_tag_rule", RuleCreateArguments(before)),
                ["undo_note"] = "The undo recreates the rule under a NEW id.",
                ["rules_version"] = state.RulesVersion,
                ["map_refresh"] = RuleMapNote,
            });
        });

    internal static Task<string> ListRulesCore(LocalDataService data, int? serverId) =>
        GuardAsync("list_query_tag_rules", async () =>
        {
            var rules = await data.GetQueryTagRulesAsync();
            var tags = await data.GetQueryTagsAsync();
            var state = await data.GetQueryTagStateAsync();
            var shown = rules.Where(r => serverId is null || r.ServerId is null || r.ServerId == serverId).ToList();
            return Serialize(new JsonObject
            {
                ["status"] = shown.Count == 0 ? "empty" : "ok",
                ["count"] = shown.Count,
                ["rules_version"] = state.RulesVersion,
                ["built_version"] = state.BuiltVersion,
                ["rules"] = new JsonArray(shown.Select(r => (JsonNode?)RuleNode(r, tags)).ToArray()),
            });
        });

    internal static Task<string> TestRuleCore(
        LocalDataService data, IReadOnlyCollection<int> knownServers, int? ruleId, string? field, string? pattern, int? tagId, int priority,
        int? serverId, int windowHours) =>
        GuardAsync("test_query_tag_rule", async () =>
        {
            if (windowHours < 1 || windowHours > MaxWindowHours)
            {
                return Refusal("invalid", "invalid_window", string.Create(CultureInfo.InvariantCulture, $"window_hours must be between 1 and {MaxWindowHours}."));
            }

            var allRules = await data.GetQueryTagRulesAsync();
            var tags = await data.GetQueryTagsAsync();
            QueryTagRule testRule;
            JsonObject ruleNode;
            var draft = ruleId is null;
            if (ruleId is int id)
            {
                var saved = allRules.FirstOrDefault(r => r.Id == id);
                if (saved is null)
                {
                    return NotFoundRule(id);
                }

                // A paused rule is tested as if it were on: the question is what it would tag.
                testRule = ToRule(saved) with { Enabled = true };
                ruleNode = RuleNode(saved, tags);
            }
            else
            {
                if (!TryParseField(field, out var parsed))
                {
                    return Refusal("invalid", "invalid_field", "Give rule_id, or a draft with field one of application, login, host, database, object, text, text_tag.");
                }

                var stored = parsed == QueryTagRuleField.TextTag ? null : pattern;
                var refusal = CheckRule(tags, knownServers, parsed, stored, tagId, null, priority, true, true, false);
                if (refusal is not null)
                {
                    return refusal;
                }

                // A new rule gets the highest id, so it ranks last among equals.
                testRule = new QueryTagRule(QueryTagRuleTester.DraftRuleId, parsed, stored, priority, true, tagId);
                ruleNode = new JsonObject
                {
                    ["draft"] = true,
                    ["field"] = FieldName(parsed),
                    ["pattern"] = stored,
                    ["priority"] = priority,
                    ["tag_id"] = tagId,
                    ["tag"] = tagId is int t ? TagPath(tags, t) : null,
                };
            }

            if (serverId is int sid && !knownServers.Contains(sid))
            {
                return UnknownServer(sid);
            }

            var result = await QueryTagRuleTester.RunAsync(data, testRule, allRules, tags, serverId, windowHours);
            var tagById = tags.ToDictionary(t => t.Id);
            JsonObject SampleNode(QueryTagTestSample s)
            {
                var node = new JsonObject
                {
                    ["server_id"] = s.ServerId,
                    ["key_kind"] = s.KeyKind,
                    ["key"] = s.Key,
                    ["tag_id"] = s.TagId,
                    ["tag"] = s.TagId is long tid && tagById.ContainsKey((int)tid) ? TagPath(tags, (int)tid) : null,
                    ["matched_field"] = s.MatchedField is QueryTagRuleField f ? FieldName(f) : null,
                    ["matched_value"] = s.MatchedValue,
                };
                node["outcome"] = s.Outcome switch
                {
                    QueryTagTestOutcome.WouldTag => "would_tag",
                    QueryTagTestOutcome.HeldByRule => "held_by_rule",
                    QueryTagTestOutcome.HeldByHandHash => "held_by_hand_hash",
                    _ => "held_by_hand_object",
                };
                if (s.Held)
                {
                    if (s.Outcome == QueryTagTestOutcome.HeldByRule)
                    {
                        node["held_by_rule_id"] = s.HeldByRuleId;
                    }

                    node["held_tag_id"] = s.HeldTagId;
                    node["held_tag"] = s.HeldTagId is long ht && tagById.ContainsKey((int)ht) ? TagPath(tags, (int)ht) : null;
                }

                return node;
            }

            var truncated = result.SightingsTruncated;
            var keys = result.KeysTested;
            var cutNote = truncated
                ? string.Create(CultureInfo.InvariantCulture, $" The newest {MaxSightingRows:N0} sighting rows were read; the counts are of those rows.")
                : string.Empty;
            return Serialize(new JsonObject
            {
                ["status"] = keys == 0 && !truncated ? "empty" : "ok",
                ["message"] = keys == 0 && !truncated
                    ? "No query sightings in the window, so there is nothing to test against. Sightings are recorded by the collectors as they see queries."
                    : "Nothing was written. Saving the rule changes the tag map on the next map pass." + cutNote,
                ["draft"] = draft,
                ["rule"] = ruleNode,
                ["server_id"] = serverId,
                ["window_hours"] = windowHours,
                ["keys_examined"] = keys,
                ["sightings_read"] = result.SightingsRead,
                ["sightings_truncated"] = truncated,
                ["keys_matched"] = result.Matches,
                ["would_tag"] = result.WouldTag,
                ["held_by_hand_assignment"] = result.HeldByAssignment,
                ["held_by_higher_rule"] = result.HeldByRule,
                ["samples_truncated"] = result.SamplesTruncated,
                ["samples"] = new JsonArray(result.Samples.Select(x => (JsonNode?)SampleNode(x)).ToArray()),
            });
        });

    // ------------------------------------------------------------------ checks and mapping

    private sealed record Snapshot(
        List<QueryTagRow> Tags, List<QueryTagAssignmentRow> Assignments, List<QueryTagRuleRow> Rules, QueryTagState State,
        IReadOnlyDictionary<int, string> ServerNames);

    private static async Task<Snapshot> ReadAsync(LocalDataService data, IReadOnlyDictionary<int, string>? serverNames = null) =>
        new(await data.GetQueryTagsAsync(), await data.GetQueryTagAssignmentsAsync(), await data.GetQueryTagRulesAsync(),
            await data.GetQueryTagStateAsync(), serverNames ?? new Dictionary<int, string>());

    private static List<TagTreeNode> Nodes(IEnumerable<QueryTagRow> tags) => tags.Select(t => new TagTreeNode(t.Id, t.Name, t.ParentId)).ToList();

    private static Dictionary<int, string> ServerNameMap(IReadOnlyCollection<ServerConnection> servers)
    {
        var names = new Dictionary<int, string>();
        foreach (var s in servers)
        {
            names[RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(s))] = ServerManager.NameForMessage(s);
        }

        return names;
    }

    private sealed record QueryTagKey(string Kind, int? ServerId, string? Hash, string? Database, string? Schema, string? Name);

    private static bool SameKey(QueryTagAssignmentRow a, QueryTagKey key) =>
        a.Kind == key.Kind && a.ServerId == key.ServerId
        && PerformanceMonitor.Common.QueryTags.QueryTagKeyRule.SameKey(
            a.QueryHash, new PerformanceMonitor.Common.QueryTags.QueryTagObject(a.DatabaseName, a.SchemaName, a.ObjectName),
            key.Hash, new PerformanceMonitor.Common.QueryTags.QueryTagObject(key.Database, key.Schema, key.Name));

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

        /* No sign test on server_id: ids are FNV hashes and half of them are negative. The registry decides (UnknownServer). */
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

            /* #6455: a NEW assignment must name something a query hash can be, the same refusal the Darling twin gives; unassign
               skips this so a key stored before the check can still be removed. */
            if (requireHashShape && !QueryHashIdentity.IsAssignableHash(hash))
            {
                refusal = Refusal("invalid", "bad_key", "query_hash must be hexadecimal: 0x followed by 1 to 16 hex digits (for example 0x1A2B3C4D5E6F7081), or the decimal number the long-query trace stores.");
                return null;
            }

            /* The resolver matches a hash ignoring case, so the key stores one spelling of it. Unassign skips the shape check
               above, so a key stored before it is kept as typed and can be removed. */
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

    /// <summary>The shared validation, then the references: null when the rule may be saved, else the refusal envelope.</summary>
    private static string? CheckRule(
        IReadOnlyList<QueryTagRow> tags, IReadOnlyCollection<int> knownServers, QueryTagRuleField field, string? pattern, int? tagId,
        int? serverId, int priority, bool enabled, bool checkTag, bool checkServer)
    {
        var error = QueryTagRuleValidator.Validate(new QueryTagRule(0, field, pattern, priority, enabled, tagId, serverId));
        if (error != QueryTagRuleError.None)
        {
            return Refusal("invalid", CodeOf(error), MessageOf(error));
        }

        if (checkTag && tagId is int tag && !tags.Any(t => t.Id == tag))
        {
            return Refusal(
                "not_found", "unknown_tag", string.Create(CultureInfo.InvariantCulture, $"Tag {tag} does not exist. Nothing was written."),
                new JsonObject { ["tag_id"] = tag });
        }

        if (checkServer && serverId is int server && !knownServers.Contains(server))
        {
            return UnknownServer(server);
        }

        return null;
    }

    internal static string CodeOf(QueryTagRuleError error) => error switch
    {
        QueryTagRuleError.EmptyPattern => "empty_pattern",
        QueryTagRuleError.PatternTooLong => "pattern_too_long",
        QueryTagRuleError.TextTagNamesTag => "text_tag_names_tag",
        QueryTagRuleError.TagMissing => "tag_missing",
        _ => "invalid",
    };

    private static string MessageOf(QueryTagRuleError error) => error switch
    {
        QueryTagRuleError.EmptyPattern => "The pattern is empty. Give 1-400 characters; * is the only wildcard.",
        QueryTagRuleError.PatternTooLong => string.Create(CultureInfo.InvariantCulture, $"The pattern is longer than {QueryTagLimits.MaxPatternLength} characters."),
        QueryTagRuleError.TextTagNamesTag => "A text_tag rule reads its tag from the query text, so it must not have a tag_id.",
        QueryTagRuleError.TagMissing => "This field needs a tag_id: the tag the rule applies. Only text_tag takes none.",
        _ => "The rule is not valid.",
    };

    internal static bool TryParseField(string? text, out QueryTagRuleField field) => QueryTagRuleTester.TryParseField(text, out field);

    internal static string FieldName(QueryTagRuleField field) => QueryTagRuleTester.FieldName(field);

    private static QueryTagRule ToRule(QueryTagRuleRow r) => QueryTagRuleTester.ToRule(r);

    // ------------------------------------------------------------------ changes parsing

    private const string UnknownFieldCode = "unknown_field";

    private static (TagTreeEdit? Edit, string? Error, string? Code) ParseTagChanges(string? changesJson)
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

        try
        {
            _ = body.Count;
        }
        catch (ArgumentException)
        {
            return (null, "changes_json has a duplicate field.", null);
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
                            return (null, "'name' is blank. A tag needs a name; send a non-blank value.", TagTreeRules.BadName);
                        }

                        hasName = true;
                        name = n;
                    }
                    else
                    {
                        return (null, "'name' must be a non-blank string; it cannot be cleared.", TagTreeRules.BadName);
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
                            return (null, "'colour' is blank. Pass null to clear it, or a #RRGGBB value.", TagTreeRules.BadColour);
                        }

                        hasColour = true;
                        colour = c.Trim();
                    }
                    else
                    {
                        return (null, "'colour' must be a #RRGGBB string, or null to clear it.", TagTreeRules.BadColour);
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

        return (new TagTreeEdit(hasName, name, hasColour, colour, hasParent, parentId), null, null);
    }

    private sealed record RuleChanges(
        bool HasPattern, string? Pattern, bool HasPriority, int Priority, bool HasEnabled, bool Enabled,
        bool HasTagId, int? TagId, bool HasServerId, int? ServerId);

    private static (RuleChanges? Changes, string? Error) ParseRuleChanges(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return (null, Refusal("invalid", "invalid", "changes_json must be a JSON object naming the fields to change: pattern, priority, enabled, tag_id, server_id."));
        }

        JsonDocument doc;
        try
        {
            doc = JsonDocument.Parse(json);
        }
        catch (JsonException)
        {
            return (null, Refusal("invalid", "invalid", "changes_json is not valid JSON."));
        }

        using (doc)
        {
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
            {
                return (null, Refusal("invalid", "invalid", "changes_json must be a JSON object."));
            }

            var c = new RuleChanges(false, null, false, 0, false, false, false, null, false, null);
            var seen = 0;
            foreach (var prop in doc.RootElement.EnumerateObject())
            {
                seen++;
                var v = prop.Value;
                switch (prop.Name)
                {
                    case "pattern" when v.ValueKind == JsonValueKind.String:
                        c = c with { HasPattern = true, Pattern = v.GetString() };
                        break;
                    case "priority" when v.ValueKind == JsonValueKind.Number && v.TryGetInt32(out var p):
                        c = c with { HasPriority = true, Priority = p };
                        break;
                    case "enabled" when v.ValueKind is JsonValueKind.True or JsonValueKind.False:
                        c = c with { HasEnabled = true, Enabled = v.GetBoolean() };
                        break;
                    case "tag_id" when v.ValueKind == JsonValueKind.Null:
                        c = c with { HasTagId = true, TagId = null };
                        break;
                    case "tag_id" when v.ValueKind == JsonValueKind.Number && v.TryGetInt32(out var t):
                        c = c with { HasTagId = true, TagId = t };
                        break;
                    case "server_id" when v.ValueKind == JsonValueKind.Null:
                        c = c with { HasServerId = true, ServerId = null };
                        break;
                    case "server_id" when v.ValueKind == JsonValueKind.Number && v.TryGetInt32(out var s):
                        c = c with { HasServerId = true, ServerId = s };
                        break;
                    case "pattern" or "priority" or "enabled" or "tag_id" or "server_id":
                        return (null, Refusal("invalid", "invalid_value", $"changes_json: '{prop.Name}' has the wrong type."));
                    default:
                        return (null, Refusal(
                            "invalid", "unknown_field",
                            $"changes_json: '{prop.Name}' is not an editable field. Use pattern, priority, enabled, tag_id or server_id."));
                }
            }

            return seen == 0
                ? (null, Refusal("invalid", "invalid", "changes_json names no field to change."))
                : (c, null);
        }
    }

    // ------------------------------------------------------------------ output

    internal static async Task<string> GuardAsync(string tool, Func<Task<string>> body)
    {
        try
        {
            return await body();
        }
        catch (QueryTagRefusedException ex)
        {
            return FromRefusal(ex.Kind, ex.Code, ex.Message);
        }
        catch (PendingRestoreException ex)
        {
            return McpHelpers.Status("unavailable", ex.Message);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(tool, ex);
        }
    }

    private static string FromRefusal(TagRefusalKind kind, string? code, string message) => kind switch
    {
        TagRefusalKind.Conflict => Refusal("conflict", code ?? TagTreeRules.DuplicateName, message),
        TagRefusalKind.NotFound => Refusal("not_found", code, message),
        _ => Refusal("invalid", code, message),
    };

    private static string ConcurrentlyGone(int tagId) =>
        Refusal("not_found", null, string.Create(CultureInfo.InvariantCulture, $"Tag {tagId} is no longer in the store: it was deleted concurrently."));

    private static string NotFoundRule(int ruleId) =>
        Refusal(
            "not_found", "rule_not_found", string.Create(CultureInfo.InvariantCulture, $"No query tag rule with id {ruleId}. Nothing was written."),
            new JsonObject { ["rule_id"] = ruleId });

    private static string UnknownServer(int serverId) =>
        Refusal(
            "not_found", "unknown_server", string.Create(CultureInfo.InvariantCulture, $"Server id {serverId} is not a monitored server. Nothing was written."),
            new JsonObject { ["unknown_server_ids"] = new JsonArray(JsonValue.Create(serverId)) });

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

    private static JsonObject TagNode(Snapshot snapshot, QueryTagRow tag)
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
            ["depth"] = TagTreeRules.Depth(Nodes(snapshot.Tags), tag.Id),
            ["path"] = string.Join(" / ", names),
        };
    }

    /// <summary>The one shape of an undo call (#6403): <c>{tool, arguments}</c>, the same as Darling's write tools.</summary>
    private static JsonObject UndoCall(string tool, JsonObject arguments) => new() { ["tool"] = tool, ["arguments"] = arguments };

    private static JsonObject PreviousTagValues(QueryTagRow stored) =>
        new() { ["name"] = stored.Name, ["colour"] = stored.Colour, ["parent_id"] = stored.ParentId };

    /// <summary>The update that puts back the fields <paramref name="applied"/> touched, as update_query_tag's own changes_json.</summary>
    private static JsonObject TagUpdateUndo(int tagId, QueryTagRow stored, TagTreeEdit applied)
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

        return UndoCall("update_query_tag", new JsonObject { ["tag_id"] = tagId, ["changes_json"] = changes.ToJsonString() });
    }

    private static JsonObject KeyArguments(JsonObject arguments, QueryTagKey key)
    {
        arguments["kind"] = key.Kind;
        if (key.Hash is not null)
        {
            arguments["query_hash"] = key.Hash;
        }

        if (key.Database is not null)
        {
            arguments["database_name"] = key.Database;
            arguments["schema_name"] = key.Schema;
            arguments["object_name"] = key.Name;
        }

        if (key.ServerId is int server)
        {
            arguments["server_id"] = server;
        }

        return arguments;
    }

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

    private static JsonObject PreviousRuleValues(QueryTagRuleRow row) => new()
    {
        ["pattern"] = row.MatchField == "text_tag" ? null : row.Pattern,
        ["priority"] = row.Priority,
        ["enabled"] = row.Enabled,
        ["tag_id"] = row.TagId,
        ["server_id"] = row.ServerId,
    };

    /// <summary>The update that puts back the fields <paramref name="applied"/> named, as update_query_tag_rule's own changes_json.</summary>
    private static JsonObject RuleUpdateUndo(QueryTagRuleRow row, RuleChanges applied)
    {
        var changes = new JsonObject();
        if (applied.HasPattern)
        {
            changes["pattern"] = row.Pattern;
        }

        if (applied.HasPriority)
        {
            changes["priority"] = row.Priority;
        }

        if (applied.HasEnabled)
        {
            changes["enabled"] = row.Enabled;
        }

        if (applied.HasTagId)
        {
            changes["tag_id"] = row.TagId;
        }

        if (applied.HasServerId)
        {
            changes["server_id"] = row.ServerId;
        }

        return UndoCall("update_query_tag_rule", new JsonObject { ["rule_id"] = row.Id, ["changes_json"] = changes.ToJsonString() });
    }

    /// <summary>The create that recreates a deleted rule; a text_tag rule takes no pattern and no tag.</summary>
    private static JsonObject RuleCreateArguments(QueryTagRuleRow row)
    {
        var arguments = new JsonObject { ["field"] = row.MatchField };
        if (row.MatchField != "text_tag")
        {
            arguments["pattern"] = row.Pattern;
            arguments["tag_id"] = row.TagId;
        }

        arguments["priority"] = row.Priority;
        arguments["enabled"] = row.Enabled;
        if (row.ServerId is int server)
        {
            arguments["server_id"] = server;
        }

        return arguments;
    }

    private static JsonObject AssignmentNode(QueryTagAssignmentRow a, Snapshot snapshot, bool withPrecedence)
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

    private static JsonObject RuleNode(QueryTagRuleRow rule, IReadOnlyList<QueryTagRow> tags) => new()
    {
        ["id"] = rule.Id,
        ["field"] = rule.MatchField,
        ["pattern"] = rule.MatchField == "text_tag" ? null : rule.Pattern,
        ["priority"] = rule.Priority,
        ["enabled"] = rule.Enabled,
        ["tag_id"] = rule.TagId,
        ["tag"] = rule.TagId is int t ? TagPath(tags, t) : null,
        ["server_id"] = rule.ServerId,
        ["created_by"] = rule.CreatedBy,
        ["created_at"] = rule.CreatedAt.ToString("o", CultureInfo.InvariantCulture),
    };

    /// <summary>The tag's path from the root (<c>Parent/Child</c>), or null when the tag is gone.</summary>
    private static string? TagPath(IReadOnlyList<QueryTagRow> tags, int tagId)
    {
        var byId = tags.ToDictionary(t => t.Id);
        var names = new List<string>();
        int? at = tagId;
        for (var steps = 0; at is int id && byId.TryGetValue(id, out var row) && steps < 16; steps++)
        {
            names.Insert(0, row.Name);
            at = row.ParentId;
        }

        return names.Count == 0 ? null : string.Join('/', names);
    }
}
