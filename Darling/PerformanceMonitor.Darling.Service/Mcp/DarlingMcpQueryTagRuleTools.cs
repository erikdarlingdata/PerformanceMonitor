/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
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
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The query tag RULE tools (#5982, wave 2 of #5971): create, update, delete, list and test the rules that tag new queries as they
/// appear. The three writes are served only with <c>mcp.allowWrites</c> and a token, like the server-tag tools (see
/// <see cref="DarlingMcpWritePolicy"/>); the two reads are ordinary reads.
///
/// <para><b>One matcher.</b> A rule is validated by <see cref="QueryTagRuleValidator"/> and applied by
/// <see cref="QueryTagResolver"/>, the same code the map builder runs. <c>test_query_tag_rule</c> hands the stored sightings to that
/// resolver and reports what it returns; there is no second matcher here.</para>
///
/// <para><b>A write does not rebuild the map.</b> The V192 trigger bumps <c>rules_version</c>; the next collector pass rebuilds the
/// map. Every write result says so and carries the new <c>rules_version</c>.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpQueryTagRuleTools
{
    /// <summary>The most samples <c>test_query_tag_rule</c> returns.</summary>
    internal const int MaxSamples = 25;

    /// <summary>The most sighting rows one test reads: the per-pass key cap the collectors share. A longer window is cut, newest kept.</summary>
    internal const int MaxSightingRows = QueryTagLimits.MaxKeysPerServerPerPass;

    internal const int DefaultWindowHours = 168;
    internal const int MaxWindowHours = 8760;
    internal const int DefaultPriority = 100;

    /// <summary>The pattern a <c>text_tag</c> rule stores: the column is NOT NULL and non-blank, and the matcher ignores it for that field.</summary>
    internal const string TextTagPlaceholderPattern = "tag:*";

    private const string CreatedBy = "mcp";
    private const string TakesEffectNote = "The tag map is rebuilt on the next collector pass; this call changed no map row.";

    /// <summary>The per-command deadline the tools use, the same as the other store writes.</summary>
    internal const int CommandSeconds = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds;

    [McpServerTool(Name = "create_query_tag_rule"), Description(
        "Creates a query tag rule: new queries whose application, login, host, database, object or query text matches the pattern get the rule's tag " +
        "the next time the collector rebuilds the tag map (this call changes no map row). field is one of application, login, host, database, object, text, text_tag. " +
        "A pattern ignores case and uses * as its only wildcard (no regular expressions); application, login, host, database and object match the whole value " +
        "(an object is matched as database.schema.name, so start with *. to ignore the database), text matches anywhere in the first 4,000 characters of the query text. " +
        "text_tag reads a /* tag:NAME */ or -- tag:NAME comment in the query text and uses NAME as the tag: it takes no tag_id and its pattern is unused; every other field needs a tag_id " +
        "(from the query tag tools) and a pattern of 1-400 characters. Rules apply by priority (lower first), then id; the first match wins, but a hand assignment beats any rule. " +
        "server_id limits the rule to one server (omit it for every server). Test a rule first with test_query_tag_rule.")]
    public static Task<string> CreateQueryTagRule(
        NpgsqlDataSource postgres,
        [Description("What the rule matches on: application, login, host, database, object, text or text_tag.")] string field,
        [Description("The pattern (1-400 characters, * is the only wildcard). Omit for text_tag.")] string? pattern = null,
        [Description("The tag the rule applies. Required for every field except text_tag, which must not have one.")] int? tag_id = null,
        [Description("Lower runs first; default 100. Ties go to the lower rule id.")] int priority = DefaultPriority,
        [Description("Whether the rule applies; default true.")] bool enabled = true,
        [Description("The server the rule applies to (from get_fleet_overview); omit for every server.")] int? server_id = null,
        CancellationToken cancellationToken = default) =>
        CreateCore(new QueryTagRuleStore(postgres, CommandSeconds), field, pattern, tag_id, priority, enabled, server_id, cancellationToken);

    [McpServerTool(Name = "update_query_tag_rule"), Description(
        "PARTIAL edit of a query tag rule: send only the fields to change in changes_json, from pattern, priority, enabled, tag_id and server_id. An explicit null for server_id " +
        "makes the rule apply to every server. The rule's field cannot change (delete it and create another). The edit is validated like a new rule. " +
        "Setting enabled to false keeps the rule in list_query_tag_rules with enabled false. The change reaches the tag map on the next collector pass.")]
    public static Task<string> UpdateQueryTagRule(
        NpgsqlDataSource postgres,
        [Description("The rule id (from list_query_tag_rules).")] int rule_id,
        [Description("A JSON object with ONLY the fields to change: pattern, priority, enabled, tag_id, server_id (e.g. {\"enabled\":false}).")] string changes_json,
        CancellationToken cancellationToken = default) =>
        UpdateCore(new QueryTagRuleStore(postgres, CommandSeconds), rule_id, changes_json, cancellationToken);

    [McpServerTool(Name = "delete_query_tag_rule"), Description(
        "Deletes a query tag rule. Permanent; queries it tagged lose that tag at the next collector pass (unless another rule or a hand assignment holds them). " +
        "To keep the rule but stop it applying, set enabled to false with update_query_tag_rule.")]
    public static Task<string> DeleteQueryTagRule(
        NpgsqlDataSource postgres,
        [Description("The rule id (from list_query_tag_rules).")] int rule_id,
        CancellationToken cancellationToken = default) =>
        DeleteCore(new QueryTagRuleStore(postgres, CommandSeconds), rule_id, cancellationToken);

    [McpServerTool(Name = "list_query_tag_rules"), Description(
        "Lists the query tag rules in the order they apply (priority, then id): id, field, pattern, priority, enabled, tag_id and tag path, server_id, who created it and when. " +
        "Disabled rules are listed with enabled false. Also returns rules_version and built_version: the tag map is stale while built_version is below rules_version, until the next collector pass. " +
        "server_id keeps only the rules that apply to that server (its own and the every-server rules).")]
    public static Task<string> ListQueryTagRules(
        NpgsqlDataSource postgres,
        [Description("Only rules that apply to this server (from get_fleet_overview); omit for all.")] int? server_id = null,
        CancellationToken cancellationToken = default) =>
        ListCore(new QueryTagRuleStore(postgres, CommandSeconds), server_id, cancellationToken);

    [McpServerTool(Name = "test_query_tag_rule"), Description(
        "Dry run, writes nothing: runs a saved rule (rule_id) or a draft rule (field, pattern, tag_id, priority) over the recent query sightings of one server or every server and reports " +
        "what it would tag: how many keys the rule matches, how many of those a hand assignment or a rule that ranks ahead already holds (so the rule would not change them), " +
        "and up to 25 samples with the matched field and value. It uses the same matcher the tag map builder runs. A disabled saved rule is tested as if it were enabled. " +
        "A draft is tested as a new rule would rank: its priority, and last among equals. Sightings are what the collectors saw in the window; keys the collectors have not yet sighted are not tested.")]
    public static Task<string> TestQueryTagRule(
        NpgsqlDataSource postgres,
        [Description("A saved rule to test (from list_query_tag_rules). Omit to test a draft built from field, pattern and tag_id.")] int? rule_id = null,
        [Description("Draft: what the rule matches on (application, login, host, database, object, text, text_tag).")] string? field = null,
        [Description("Draft: the pattern. Omit for text_tag.")] string? pattern = null,
        [Description("Draft: the tag the rule applies; omit for text_tag.")] int? tag_id = null,
        [Description("Draft: the rule priority (lower first); default 100.")] int priority = DefaultPriority,
        [Description("Test against this server's sightings (from get_fleet_overview); omit for every server.")] int? server_id = null,
        [Description("How far back to look at sightings, in hours (1-8760); default 168.")] int window_hours = DefaultWindowHours,
        CancellationToken cancellationToken = default) =>
        TestCore(new QueryTagRuleStore(postgres, CommandSeconds), rule_id, field, pattern, tag_id, priority, server_id, window_hours, TimeProvider.System, cancellationToken);

    // ---------------------------------------------------------------- cores

    internal static async Task<string> CreateCore(
        QueryTagRuleStore store, string? field, string? pattern, int? tagId, int priority, bool enabled, int? serverId, CancellationToken ct = default)
    {
        try
        {
            if (!QueryTagRuleStore.TryParseField(field, out var parsed))
            {
                return Refusal("invalid", "invalid_field", "field must be one of application, login, host, database, object, text, text_tag.");
            }

            var stored = parsed == QueryTagRuleField.TextTag ? TextTagPlaceholderPattern : pattern;
            var refusal = await CheckAsync(store, parsed, stored, tagId, serverId, priority, enabled, ct);
            if (refusal is not null)
            {
                return refusal.Json;
            }

            var row = await store.InsertRuleAsync(parsed, stored!, priority, enabled, tagId, serverId, CreatedBy, ct);
            var tags = await store.ReadTagsAsync(ct);
            var state = await store.ReadStateAsync(ct);
            return Serialize(new JsonObject
            {
                ["status"] = "created",
                ["rule"] = RuleNode(row, tags),
                ["rules_version"] = state.RulesVersion,
                ["map_refresh"] = TakesEffectNote,
            });
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("create_query_tag_rule", ex);
        }
    }

    internal static async Task<string> UpdateCore(QueryTagRuleStore store, long ruleId, string? changesJson, CancellationToken ct = default)
    {
        try
        {
            var (changes, error) = ParseChanges(changesJson);
            if (error is not null)
            {
                return error.Json;
            }

            var current = await store.GetRuleAsync(ruleId, ct);
            if (current is null)
            {
                return NotFound(ruleId);
            }

            var pattern = current.Pattern;
            if (changes!.HasPattern)
            {
                if (current.Field == QueryTagRuleField.TextTag)
                {
                    return Refusal("invalid", "text_tag_has_no_pattern", "A text_tag rule reads its tag from the query text and has no pattern to change.");
                }

                pattern = changes.Pattern!;
            }

            var priority = changes.HasPriority ? changes.Priority : current.Priority;
            var enabled = changes.HasEnabled ? changes.Enabled : current.Enabled;
            var tagId = changes.HasTagId ? changes.TagId : current.TagId;
            var serverId = changes.HasServerId ? changes.ServerId : current.ServerId;

            if (pattern == current.Pattern && priority == current.Priority && enabled == current.Enabled && tagId == current.TagId && serverId == current.ServerId)
            {
                var tagsNow = await store.ReadTagsAsync(ct);
                return Serialize(new JsonObject { ["status"] = "unchanged", ["rule"] = RuleNode(current, tagsNow) });
            }

            // A tag or server this edit does not touch is not re-checked: a rule must stay editable (pausable) after its tag was removed elsewhere.
            var refusal = await CheckAsync(
                store, current.Field, pattern, tagId, serverId, priority, enabled, ct,
                checkTag: changes.HasTagId && tagId != current.TagId,
                checkServer: changes.HasServerId && serverId != current.ServerId);
            if (refusal is not null)
            {
                return refusal.Json;
            }

            var row = await store.UpdateRuleAsync(ruleId, pattern, priority, enabled, tagId, serverId, ct);
            if (row is null)
            {
                return NotFound(ruleId);
            }

            var tags = await store.ReadTagsAsync(ct);
            var state = await store.ReadStateAsync(ct);
            return Serialize(new JsonObject
            {
                ["status"] = "updated",
                ["rule"] = RuleNode(row, tags),
                ["rules_version"] = state.RulesVersion,
                ["map_refresh"] = TakesEffectNote,
            });
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("update_query_tag_rule", ex);
        }
    }

    internal static async Task<string> DeleteCore(QueryTagRuleStore store, long ruleId, CancellationToken ct = default)
    {
        try
        {
            if (!await store.DeleteRuleAsync(ruleId, ct))
            {
                return NotFound(ruleId);
            }

            var state = await store.ReadStateAsync(ct);
            return Serialize(new JsonObject
            {
                ["status"] = "deleted",
                ["rule_id"] = ruleId,
                ["rules_version"] = state.RulesVersion,
                ["map_refresh"] = TakesEffectNote,
            });
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("delete_query_tag_rule", ex);
        }
    }

    internal static async Task<string> ListCore(QueryTagRuleStore store, int? serverId, CancellationToken ct = default)
    {
        try
        {
            var rules = await store.ListRulesAsync(ct);
            var tags = await store.ReadTagsAsync(ct);
            var state = await store.ReadStateAsync(ct);
            var shown = rules.Where(r => serverId is null || r.ServerId is null || r.ServerId == serverId).ToList();
            return Serialize(new JsonObject
            {
                ["status"] = shown.Count == 0 ? "empty" : "ok",
                ["count"] = shown.Count,
                ["rules_version"] = state.RulesVersion,
                ["built_version"] = state.BuiltVersion,
                ["rules"] = new JsonArray(shown.Select(r => (JsonNode?)RuleNode(r, tags)).ToArray()),
            });
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("list_query_tag_rules", ex);
        }
    }

    internal static async Task<string> TestCore(
        QueryTagRuleStore store, int? ruleId, string? field, string? pattern, int? tagId, int priority, int? serverId, int windowHours,
        TimeProvider clock, CancellationToken ct = default)
    {
        try
        {
            if (windowHours < 1 || windowHours > MaxWindowHours)
            {
                return Refusal("invalid", "invalid_window", string.Create(CultureInfo.InvariantCulture, $"window_hours must be between 1 and {MaxWindowHours}."));
            }

            var allRules = await store.ListRulesAsync(ct);
            QueryTagRule testRule;
            JsonObject ruleNode;
            var tags = await store.ReadTagsAsync(ct);
            var draft = ruleId is null;
            if (ruleId is int id)
            {
                var saved = allRules.FirstOrDefault(r => r.Id == id);
                if (saved is null)
                {
                    return NotFound(id);
                }

                // A paused rule is tested as if it were on: the question is what it would tag.
                testRule = saved.ToRule() with { Enabled = true };
                ruleNode = RuleNode(saved, tags);
            }
            else
            {
                if (!QueryTagRuleStore.TryParseField(field, out var parsed))
                {
                    return Refusal("invalid", "invalid_field", "Give rule_id, or a draft with field one of application, login, host, database, object, text, text_tag.");
                }

                var stored = parsed == QueryTagRuleField.TextTag ? TextTagPlaceholderPattern : pattern;
                var refusal = await CheckAsync(store, parsed, stored, tagId, null, priority, true, ct);
                if (refusal is not null)
                {
                    return refusal.Json;
                }

                // A new rule gets the highest id, so it ranks last among equals.
                testRule = new QueryTagRule(long.MaxValue, parsed, stored, priority, true, tagId);
                ruleNode = new JsonObject
                {
                    ["draft"] = true,
                    ["field"] = QueryTagRuleStore.FieldName(parsed),
                    ["pattern"] = parsed == QueryTagRuleField.TextTag ? null : stored,
                    ["priority"] = priority,
                    ["tag_id"] = tagId,
                    ["tag"] = tagId is int t ? TagPath(tags, t) : null,
                };
            }

            if (serverId is int sid && (await store.FindRegisteredServerIdsAsync([sid], ct)).Count == 0)
            {
                return UnknownServer(sid);
            }

            var since = clock.GetUtcNow().AddHours(-windowHours);
            var rows = await store.ReadSightingsAsync(serverId, since, MaxSightingRows, ct);
            var assignments = await store.ReadAssignmentsAsync(ct);
            var names = new TagNames(tags);
            var tagById = tags.ToDictionary(t => t.Id);

            // Only the other rules compete with the one under test; the tested rule is the tested copy.
            var competing = allRules.Where(r => r.Id != testRule.Id).Select(r => r.ToRule()).ToList();
            var withTest = competing.Append(testRule).ToList();
            var alone = new[] { testRule };
            var none = Array.Empty<QueryTagHandAssignment>();

            var matched = 0;
            var wouldTag = 0;
            var heldByAssignment = 0;
            var heldByRule = 0;
            var keys = 0;
            var tagged = new List<JsonObject>();
            var held = new List<JsonObject>();

            foreach (var group in rows.GroupBy(r => (r.ServerId, r.KeyKind, r.Key)))
            {
                keys++;
                var sightings = group.Select(QueryTagRuleStore.ToSighting).ToList();
                var hit = QueryTagResolver.Resolve(group.Key.ServerId, sightings, none, alone, names);
                if (!hit.HasTag)
                {
                    continue;
                }

                matched++;
                var full = QueryTagResolver.Resolve(group.Key.ServerId, sightings, assignments, withTest, names);
                var sample = new JsonObject
                {
                    ["server_id"] = group.Key.ServerId,
                    ["key_kind"] = group.Key.KeyKind,
                    ["key"] = group.Key.Key,
                    ["tag_id"] = hit.TagId,
                    ["tag"] = hit.TagId is long tid && tagById.ContainsKey((int)tid) ? TagPath(tags, (int)tid) : null,
                    ["matched_field"] = hit.MatchedField is QueryTagRuleField f ? QueryTagRuleStore.FieldName(f) : null,
                    ["matched_value"] = hit.MatchedValue,
                };

                if (full.Source == QueryTagSource.Rule && full.RuleId == testRule.Id)
                {
                    wouldTag++;
                    sample["outcome"] = "would_tag";
                    tagged.Add(sample);
                    continue;
                }

                if (full.Source == QueryTagSource.Rule)
                {
                    heldByRule++;
                    sample["outcome"] = "held_by_rule";
                    sample["held_by_rule_id"] = full.RuleId;
                }
                else
                {
                    heldByAssignment++;
                    sample["outcome"] = full.Source == QueryTagSource.HandHash ? "held_by_hand_hash" : "held_by_hand_object";
                }

                sample["held_tag_id"] = full.TagId;
                sample["held_tag"] = full.TagId is long ht && tagById.ContainsKey((int)ht) ? TagPath(tags, (int)ht) : null;
                held.Add(sample);
            }

            var all = tagged.Concat(held).ToList();
            return Serialize(new JsonObject
            {
                ["status"] = keys == 0 ? "empty" : "ok",
                ["message"] = keys == 0
                    ? "No query sightings in the window, so there is nothing to test against. Sightings are recorded by the collectors as they see queries."
                    : "Nothing was written. Saving the rule changes the tag map on the next collector pass.",
                ["draft"] = draft,
                ["rule"] = ruleNode,
                ["server_id"] = serverId,
                ["window_hours"] = windowHours,
                ["keys_examined"] = keys,
                ["sightings_read"] = rows.Count,
                ["sightings_truncated"] = rows.Count >= MaxSightingRows,
                ["keys_matched"] = matched,
                ["would_tag"] = wouldTag,
                ["held_by_hand_assignment"] = heldByAssignment,
                ["held_by_higher_rule"] = heldByRule,
                ["samples_truncated"] = all.Count > MaxSamples,
                ["samples"] = new JsonArray(all.Take(MaxSamples).Select(s => (JsonNode?)s).ToArray()),
            });
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("test_query_tag_rule", ex);
        }
    }

    // ---------------------------------------------------------------- checks

    /// <summary>The shared validation, then the references: null when the rule may be saved, else the refusal envelope.</summary>
    private static async Task<Refused?> CheckAsync(
        QueryTagRuleStore store, QueryTagRuleField field, string? pattern, int? tagId, int? serverId, int priority, bool enabled,
        CancellationToken ct, bool checkTag = true, bool checkServer = true)
    {
        var error = QueryTagRuleValidator.Validate(new QueryTagRule(0, field, pattern, priority, enabled, tagId, serverId));
        if (error != QueryTagRuleError.None)
        {
            return new Refused(Refusal("invalid", CodeOf(error), MessageOf(error)));
        }

        if (checkTag && tagId is int tag && !(await store.ReadTagsAsync(ct)).Any(t => t.Id == tag))
        {
            return new Refused(Refusal(
                "not_found", "unknown_tag", string.Create(CultureInfo.InvariantCulture, $"Tag {tag} does not exist. Nothing was written."),
                new JsonObject { ["tag_id"] = tag }));
        }

        if (checkServer && serverId is int server && (await store.FindRegisteredServerIdsAsync([server], ct)).Count == 0)
        {
            return new Refused(UnknownServer(server));
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

    // ---------------------------------------------------------------- the update's changes

    /// <summary>A refusal already rendered as the tool's JSON answer.</summary>
    private sealed record Refused(string Json);

    private sealed record Changes(
        bool HasPattern, string? Pattern, bool HasPriority, int Priority, bool HasEnabled, bool Enabled,
        bool HasTagId, int? TagId, bool HasServerId, int? ServerId);

    private static (Changes? Changes, Refused? Error) ParseChanges(string? json)
    {
        if (string.IsNullOrWhiteSpace(json))
        {
            return (null, new Refused(Refusal("invalid", "invalid", "changes_json must be a JSON object naming the fields to change: pattern, priority, enabled, tag_id, server_id.")));
        }

        JsonDocument doc;
        try
        {
            doc = JsonDocument.Parse(json);
        }
        catch (JsonException)
        {
            return (null, new Refused(Refusal("invalid", "invalid", "changes_json is not valid JSON.")));
        }

        using (doc)
        {
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
            {
                return (null, new Refused(Refusal("invalid", "invalid", "changes_json must be a JSON object.")));
            }

            var c = new Changes(false, null, false, 0, false, false, false, null, false, null);
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
                        return (null, new Refused(Refusal("invalid", "invalid_value", $"changes_json: '{prop.Name}' has the wrong type.")));
                    default:
                        return (null, new Refused(Refusal(
                            "invalid", "unknown_field",
                            $"changes_json: '{prop.Name}' is not an editable field. Use pattern, priority, enabled, tag_id or server_id.")));
                }
            }

            return seen == 0
                ? (null, new Refused(Refusal("invalid", "invalid", "changes_json names no field to change.")))
                : (c, null);
        }
    }

    // ---------------------------------------------------------------- output

    private static JsonObject RuleNode(QueryTagRuleRow rule, IReadOnlyList<QueryTagNameRow> tags) => new()
    {
        ["id"] = rule.Id,
        ["field"] = QueryTagRuleStore.FieldName(rule.Field),
        ["pattern"] = rule.Field == QueryTagRuleField.TextTag ? null : rule.Pattern,
        ["priority"] = rule.Priority,
        ["enabled"] = rule.Enabled,
        ["tag_id"] = rule.TagId,
        ["tag"] = rule.TagId is int t ? TagPath(tags, t) : null,
        ["server_id"] = rule.ServerId,
        ["created_by"] = rule.CreatedBy,
        ["created_at"] = rule.CreatedAt.UtcDateTime.ToString("o", CultureInfo.InvariantCulture),
    };

    /// <summary>The tag's path from the root (<c>Parent/Child</c>), or null when the tag is gone.</summary>
    private static string? TagPath(IReadOnlyList<QueryTagNameRow> tags, int tagId)
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

    private static string NotFound(long ruleId) =>
        Refusal(
            "not_found", "rule_not_found", string.Create(CultureInfo.InvariantCulture, $"No query tag rule with id {ruleId}. Nothing was written."),
            new JsonObject { ["rule_id"] = ruleId });

    private static string UnknownServer(int serverId) =>
        Refusal(
            "not_found", "unknown_server", string.Create(CultureInfo.InvariantCulture, $"Server id {serverId} is not a monitored server. Nothing was written."),
            new JsonObject { ["unknown_server_ids"] = new JsonArray(JsonValue.Create(serverId)) });

    private static string Refusal(string status, string code, string message, JsonObject? extra = null)
    {
        var node = new JsonObject { ["status"] = status, ["refusal"] = code, ["message"] = message };
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

    /// <summary>
    /// The tag names the resolver reads for a <c>/* tag:NAME */</c> comment. A list, not a dictionary: the store lets the same
    /// name stand under two parents, and the resolver treats a name that folds to two entries as ambiguous and matches no tag.
    /// A dictionary would drop the second entry and hide that.
    /// </summary>
    private sealed class TagNames : IReadOnlyDictionary<string, long>
    {
        private readonly List<KeyValuePair<string, long>> _entries;

        public TagNames(IEnumerable<QueryTagNameRow> tags) =>
            _entries = tags.Select(t => new KeyValuePair<string, long>(t.Name, t.Id)).ToList();

        public int Count => _entries.Count;

        public IEnumerable<string> Keys => _entries.Select(e => e.Key);

        public IEnumerable<long> Values => _entries.Select(e => e.Value);

        public long this[string key] => TryGetValue(key, out var v) ? v : throw new KeyNotFoundException(key);

        public bool ContainsKey(string key) => TryGetValue(key, out _);

        public bool TryGetValue(string key, out long value)
        {
            foreach (var e in _entries)
            {
                if (string.Equals(e.Key, key, StringComparison.OrdinalIgnoreCase))
                {
                    value = e.Value;
                    return true;
                }
            }

            value = 0;
            return false;
        }

        public IEnumerator<KeyValuePair<string, long>> GetEnumerator() => _entries.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
