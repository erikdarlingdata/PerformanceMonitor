/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// #6403 group B: each older write tool (custom alert rules, custom views, query tags, query tag rules) answers with the previous
/// value and an undo call in the {tool, arguments} shape <c>enable_server</c> uses. Every test calls the tool, REPLAYS the returned
/// undo through the real tool, and checks the store is back where it started.
/// </summary>
public sealed class McpWriteToolUndoLiveTests
{
    private const int Server = 7701;
    private const string ServerName = "undo-alpha";
    private const string Hash = "0xA1B2C3D4E5F60718";

    private const string Definition =
        "{\"metric\":{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"hours\":0.25}," +
        "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":25}," +
        "\"hysteresis\":{\"breachSamples\":1,\"clearSamples\":1}," +
        "\"scope\":{\"mode\":\"servers\",\"servers\":[\"" + ServerName + "\"]}}";

    private const string OtherDefinition =
        "{\"metric\":{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"max\",\"hours\":0.5}," +
        "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":80}," +
        "\"hysteresis\":{\"breachSamples\":2,\"clearSamples\":2}," +
        "\"scope\":{\"mode\":\"servers\",\"servers\":[\"" + ServerName + "\"]}}";

    private const string ViewDefinition =
        "{\"panels\":[{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}]}";

    private const string OtherViewDefinition =
        "{\"panels\":[{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"avg\",\"timeBucket\":\"hour\",\"viz\":\"table\"}]}";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task WithStoreAsync(Func<NpgsqlDataSource, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the write-tool undo tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var owner = new NpgsqlConnection(connectionString))
        {
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            await using var insert = new NpgsqlCommand(
                $"INSERT INTO servers (server_id, server_name, display_name) VALUES ({Server}, '{ServerName}', '{ServerName}')", owner);
            await insert.ExecuteNonQueryAsync(ct);
        }

        await using var source = NpgsqlDataSource.Create(connectionString);
        var bodySucceeded = false;
        try
        {
            await body(source, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static JsonObject Parse(string json) => (JsonObject)JsonNode.Parse(json)!;

    /// <summary>Runs the undo call an answer returned, through the real tool, and returns that tool's answer.</summary>
    private static async Task<JsonObject> ReplayAsync(NpgsqlDataSource source, JsonNode? undo, CancellationToken ct)
    {
        Assert.NotNull(undo);
        var tool = (string)undo!["tool"]!;
        var a = undo["arguments"]!.AsObject();
        string answer = tool switch
        {
            "delete_custom_alert_rule" => await DarlingMcpCustomAlertTools.DeleteCustomAlertRule(source, (long)a["rule_id"]!),
            "create_custom_alert_rule" => await DarlingMcpCustomAlertTools.CreateCustomAlertRule(
                source, (string)a["name"]!, (string)a["definition"]!, (string?)a["description"], (bool?)a["enabled"] ?? true,
                expires_at_utc: (string?)a["expires_at_utc"]),
            "update_custom_alert_rule" => await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(
                source, (long)a["rule_id"]!, (int)a["version"]!, (string?)a["name"], (string?)a["definition"], (string?)a["description"],
                (bool?)a["enabled"], expires_at_utc: (string?)a["expires_at_utc"]),
            "delete_custom_view" => await DarlingMcpCustomViewTools.DeleteCustomView(source, (long)a["view_id"]!),
            "create_custom_view" => await DarlingMcpCustomViewTools.CreateCustomView(
                source, (string)a["name"]!, (string)a["definition"]!, (string?)a["description"], (string?)a["folder"]),
            "update_custom_view" => await DarlingMcpCustomViewTools.UpdateCustomView(
                source, (long)a["view_id"]!, (string)a["name"]!, (string)a["definition"]!, (int)a["version"]!, (string?)a["description"], (string?)a["folder"]),
            "delete_query_tag" => await DarlingMcpQueryTagTools.DeleteQueryTag(source, (int)a["tag_id"]!, (bool?)a["confirm"] ?? false, ct),
            "create_query_tag" => await DarlingMcpQueryTagTools.CreateQueryTag(source, (string)a["name"]!, (int?)a["parent_id"], (string?)a["colour"], ct),
            "update_query_tag" => await DarlingMcpQueryTagTools.UpdateQueryTag(source, (int)a["tag_id"]!, (string)a["changes_json"]!, ct),
            "assign_query_tag" => await DarlingMcpQueryTagTools.AssignQueryTag(
                source, (int)a["tag_id"]!, (string)a["kind"]!, (string?)a["query_hash"], (string?)a["database_name"], (string?)a["schema_name"],
                (string?)a["object_name"], (int?)a["server_id"], (long?)a["query_id"], ct),
            "unassign_query_tag" => await DarlingMcpQueryTagTools.UnassignQueryTag(
                source, (string)a["kind"]!, (string?)a["query_hash"], (string?)a["database_name"], (string?)a["schema_name"],
                (string?)a["object_name"], (int?)a["server_id"], ct),
            "delete_query_tag_rule" => await DarlingMcpQueryTagRuleTools.DeleteQueryTagRule(source, (int)a["rule_id"]!, ct),
            "create_query_tag_rule" => await DarlingMcpQueryTagRuleTools.CreateQueryTagRule(
                source, (string)a["field"]!, (string?)a["pattern"], (int?)a["tag_id"], (int?)a["priority"] ?? 100, (bool?)a["enabled"] ?? true,
                (int?)a["server_id"], ct),
            "update_query_tag_rule" => await DarlingMcpQueryTagRuleTools.UpdateQueryTagRule(source, (int)a["rule_id"]!, (string)a["changes_json"]!, ct),
            _ => throw new InvalidOperationException("No replay for undo tool " + tool),
        };
        return Parse(answer);
    }

    private static void AssertUndoShape(JsonObject answer, string tool)
    {
        Assert.True(answer.ContainsKey("previous"));
        Assert.Equal(tool, (string?)answer["undo"]!["tool"]);
        Assert.IsType<JsonObject>(answer["undo"]!["arguments"]);
    }

    private static async Task<CustomAlertRule> RuleAsync(NpgsqlDataSource source, long id, CancellationToken ct)
    {
        var result = await new CustomAlertRuleStore(source, StorageCommandDeadlines.McpReadSeconds).GetAsync(id, ct);
        return Assert.IsType<CustomAlertRuleResult.Ok>(result).Rule!;
    }

    // ------------------------------------------------------------------ custom alert rules

    [Fact]
    public Task CustomAlertRule_CreateUndo_DeletesTheNewRule() => WithStoreAsync(async (source, ct) =>
    {
        var created = Parse(await DarlingMcpCustomAlertTools.CreateCustomAlertRule(source, "undo-rule", Definition, "first", enabled: false));
        Assert.Null(created["previous"]);
        AssertUndoShape(created, "delete_custom_alert_rule");
        var id = (long)created["id"]!;

        var undone = await ReplayAsync(source, created["undo"], ct);
        Assert.Equal("deleted", (string?)undone["status"]);
        Assert.IsType<CustomAlertRuleResult.NotFound>(await new CustomAlertRuleStore(source, StorageCommandDeadlines.McpReadSeconds).GetAsync(id, ct));
    });

    [Fact]
    public Task CustomAlertRule_UpdateUndo_RestoresEveryChangedField() => WithStoreAsync(async (source, ct) =>
    {
        var created = Parse(await DarlingMcpCustomAlertTools.CreateCustomAlertRule(source, "undo-rule", Definition, null, enabled: true));
        var id = (long)created["id"]!;
        var original = await RuleAsync(source, id, ct);

        var updated = Parse(await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(
            source, id, original.Version, name: "undo-rule-renamed", definition: OtherDefinition, description: "now described", enabled: false,
            expires_at_utc: DateTime.UtcNow.AddDays(2).ToString("o")));
        Assert.Equal("undo-rule-renamed", (string?)updated["name"]);
        AssertUndoShape(updated, "update_custom_alert_rule");
        Assert.Equal("undo-rule", (string?)updated["previous"]!["name"]);

        var undone = await ReplayAsync(source, updated["undo"], ct);
        Assert.Equal("undo-rule", (string?)undone["name"]);

        var restored = await RuleAsync(source, id, ct);
        Assert.Equal(original.Name, restored.Name);
        Assert.Equal(original.Description, restored.Description);
        Assert.Equal(original.Enabled, restored.Enabled);
        Assert.Equal(original.ExpiresAtUtc, restored.ExpiresAtUtc);
        Assert.Equal(JsonNode.Parse(original.DefinitionJson)!.ToJsonString(), JsonNode.Parse(restored.DefinitionJson)!.ToJsonString());
    });

    [Fact]
    public Task CustomAlertRule_DeleteUndo_RecreatesTheRuleFromTheDeletedDefinition() => WithStoreAsync(async (source, ct) =>
    {
        var created = Parse(await DarlingMcpCustomAlertTools.CreateCustomAlertRule(source, "undo-rule", Definition, "described", enabled: false));
        var id = (long)created["id"]!;
        var original = await RuleAsync(source, id, ct);

        var deleted = Parse(await DarlingMcpCustomAlertTools.DeleteCustomAlertRule(source, id));
        Assert.Equal("deleted", (string?)deleted["status"]);
        Assert.Equal("undo-rule", (string?)deleted["previous"]!["name"]);
        AssertUndoShape(deleted, "create_custom_alert_rule");
        Assert.False(string.IsNullOrEmpty((string?)deleted["undo_note"]));

        var undone = await ReplayAsync(source, deleted["undo"], ct);
        var restored = await RuleAsync(source, (long)undone["id"]!, ct);
        Assert.Equal(original.Name, restored.Name);
        Assert.Equal(original.Description, restored.Description);
        Assert.Equal(original.Enabled, restored.Enabled);
        Assert.Equal(JsonNode.Parse(original.DefinitionJson)!.ToJsonString(), JsonNode.Parse(restored.DefinitionJson)!.ToJsonString());
    });

    // ------------------------------------------------------------------ custom views

    private static async Task<CustomView> ViewAsync(NpgsqlDataSource source, long id, CancellationToken ct)
    {
        var result = await new CustomViewStore(source, StorageCommandDeadlines.McpReadSeconds).GetAsync(id, null, ct);
        return Assert.IsType<CustomViewResult.Ok>(result).View!;
    }

    [Fact]
    public Task CustomView_CreateUndo_DeletesTheNewView() => WithStoreAsync(async (source, ct) =>
    {
        var created = Parse(await DarlingMcpCustomViewTools.CreateCustomView(source, "undo-view", ViewDefinition));
        Assert.Null(created["previous"]);
        AssertUndoShape(created, "delete_custom_view");
        var id = (long)created["id"]!;

        Assert.Equal("deleted", (string?)(await ReplayAsync(source, created["undo"], ct))["status"]);
        Assert.IsNotType<CustomViewResult.Ok>(await new CustomViewStore(source, StorageCommandDeadlines.McpReadSeconds).GetAsync(id, null, ct));
    });

    [Fact]
    public Task CustomView_UpdateUndo_RestoresNameDefinitionDescriptionAndFolder() => WithStoreAsync(async (source, ct) =>
    {
        var created = Parse(await DarlingMcpCustomViewTools.CreateCustomView(source, "undo-view", ViewDefinition, "described", "ops"));
        var id = (long)created["id"]!;
        var original = await ViewAsync(source, id, ct);

        var updated = Parse(await DarlingMcpCustomViewTools.UpdateCustomView(
            source, id, "undo-view-renamed", OtherViewDefinition, original.Version, description: string.Empty, folder: string.Empty));
        AssertUndoShape(updated, "update_custom_view");
        Assert.Equal("undo-view", (string?)updated["previous"]!["name"]);

        await ReplayAsync(source, updated["undo"], ct);
        var restored = await ViewAsync(source, id, ct);
        Assert.Equal(original.Name, restored.Name);
        Assert.Equal(original.Description, restored.Description);
        Assert.Equal(original.Folder, restored.Folder);
        Assert.Equal(JsonNode.Parse(original.DefinitionJson)!.ToJsonString(), JsonNode.Parse(restored.DefinitionJson)!.ToJsonString());
    });

    [Fact]
    public Task CustomView_DeleteUndo_RecreatesTheView() => WithStoreAsync(async (source, ct) =>
    {
        var created = Parse(await DarlingMcpCustomViewTools.CreateCustomView(source, "undo-view", ViewDefinition, "described", "ops"));
        var id = (long)created["id"]!;
        var original = await ViewAsync(source, id, ct);

        var deleted = Parse(await DarlingMcpCustomViewTools.DeleteCustomView(source, id));
        Assert.Equal("undo-view", (string?)deleted["previous"]!["name"]);
        AssertUndoShape(deleted, "create_custom_view");

        var undone = await ReplayAsync(source, deleted["undo"], ct);
        var restored = await ViewAsync(source, (long)undone["id"]!, ct);
        Assert.Equal(original.Name, restored.Name);
        Assert.Equal(original.Description, restored.Description);
        Assert.Equal(original.Folder, restored.Folder);
        Assert.Equal(JsonNode.Parse(original.DefinitionJson)!.ToJsonString(), JsonNode.Parse(restored.DefinitionJson)!.ToJsonString());
    });

    // ------------------------------------------------------------------ query tags

    private static async Task<int> NewTagAsync(NpgsqlDataSource source, string name, int? parent, CancellationToken ct) =>
        (int)Parse(await DarlingMcpQueryTagTools.CreateQueryTag(source, name, parent, null, ct))["tag"]!["tag_id"]!;

    private static async Task<(string, string, string?)[]> TagsAsync(NpgsqlDataSource source, CancellationToken ct) =>
        (await new QueryTagStore(source, 30).ReadSnapshotAsync(ct)).Tags
            .OrderBy(t => t.Id).Select(t => (t.Id + ":" + t.Name, t.ParentId?.ToString() ?? "-", t.Colour)).ToArray();

    private static async Task<string[]> AssignmentsAsync(NpgsqlDataSource source, CancellationToken ct) =>
        (await new QueryTagStore(source, 30).ReadSnapshotAsync(ct)).Assignments
            .OrderBy(a => a.QueryHash).Select(a => $"{a.TagId}|{a.Kind}|{a.ServerId}|{a.QueryHash}|{a.DatabaseName}|{a.SchemaName}|{a.ObjectName}").ToArray();

    [Fact]
    public Task QueryTag_CreateUndo_DeletesTheNewTag() => WithStoreAsync(async (source, ct) =>
    {
        var created = Parse(await DarlingMcpQueryTagTools.CreateQueryTag(source, "undo-tag", null, "#336699", ct));
        Assert.Null(created["previous"]);
        AssertUndoShape(created, "delete_query_tag");

        Assert.Equal("deleted", (string?)(await ReplayAsync(source, created["undo"], ct))["status"]);
        Assert.Empty(await TagsAsync(source, ct));
    });

    [Fact]
    public Task QueryTag_UpdateUndo_RestoresNameColourAndParent() => WithStoreAsync(async (source, ct) =>
    {
        var parent = await NewTagAsync(source, "parent", null, ct);
        var other = await NewTagAsync(source, "other-parent", null, ct);
        var tag = Parse(await DarlingMcpQueryTagTools.CreateQueryTag(source, "child", parent, "#336699", ct));
        var id = (int)tag["tag"]!["tag_id"]!;
        var original = await TagsAsync(source, ct);

        var updated = Parse(await DarlingMcpQueryTagTools.UpdateQueryTag(
            source, id, "{\"name\":\"child-renamed\",\"colour\":null,\"parent_id\":" + other + "}", ct));
        Assert.Equal("updated", (string?)updated["status"]);
        AssertUndoShape(updated, "update_query_tag");
        Assert.NotEqual(original, await TagsAsync(source, ct));

        await ReplayAsync(source, updated["undo"], ct);
        Assert.Equal(original, await TagsAsync(source, ct));
    });

    [Fact]
    public Task QueryTag_DeleteOfALeaf_UndoRecreatesIt_AndOfASubtree_SaysNoSingleCallUndoesIt() => WithStoreAsync(async (source, ct) =>
    {
        var root = await NewTagAsync(source, "root", null, ct);
        var leaf = Parse(await DarlingMcpQueryTagTools.CreateQueryTag(source, "leaf", root, "#336699", ct));
        var leafId = (int)leaf["tag"]!["tag_id"]!;
        var original = (await TagsAsync(source, ct)).Select(t => (t.Item1.Split(':')[1], t.Item2 == "-" ? "-" : "parent", t.Item3)).ToArray();

        var deleted = Parse(await DarlingMcpQueryTagTools.DeleteQueryTag(source, leafId, false, ct));
        Assert.Equal("deleted", (string?)deleted["status"]);
        AssertUndoShape(deleted, "create_query_tag");
        await ReplayAsync(source, deleted["undo"], ct);
        var restored = (await TagsAsync(source, ct)).Select(t => (t.Item1.Split(':')[1], t.Item2 == "-" ? "-" : "parent", t.Item3)).ToArray();
        Assert.Equal(original.OrderBy(t => t.Item1), restored.OrderBy(t => t.Item1));

        // The root now has a child again: deleting it takes the subtree, which no single call restores.
        var subtree = Parse(await DarlingMcpQueryTagTools.DeleteQueryTag(source, root, true, ct));
        Assert.Equal("deleted", (string?)subtree["status"]);
        Assert.Equal("root", (string?)subtree["previous"]!["name"]);
        Assert.Null(subtree["undo"]);
        Assert.False(string.IsNullOrEmpty((string?)subtree["undo_note"]));
    });

    [Fact]
    public Task QueryTag_AssignUndo_RemovesTheAssignment_AndAMoveUndoPutsTheOldTagBack() => WithStoreAsync(async (source, ct) =>
    {
        var one = await NewTagAsync(source, "one", null, ct);
        var two = await NewTagAsync(source, "two", null, ct);

        var assigned = Parse(await DarlingMcpQueryTagTools.AssignQueryTag(source, one, "hash", Hash, null, null, null, null, null, ct));
        Assert.Equal("assigned", (string?)assigned["status"]);
        Assert.Null(assigned["previous"]!["tag_id"]);
        AssertUndoShape(assigned, "unassign_query_tag");
        await ReplayAsync(source, assigned["undo"], ct);
        Assert.Empty(await AssignmentsAsync(source, ct));

        await DarlingMcpQueryTagTools.AssignQueryTag(source, one, "object", null, "db1", "dbo", "proc1", Server, null, ct);
        var before = await AssignmentsAsync(source, ct);
        var moved = Parse(await DarlingMcpQueryTagTools.AssignQueryTag(source, two, "object", null, "db1", "dbo", "proc1", Server, null, ct));
        Assert.Equal("moved", (string?)moved["status"]);
        Assert.Equal(one, (int)moved["previous"]!["tag_id"]!);
        AssertUndoShape(moved, "assign_query_tag");
        Assert.NotEqual(before, await AssignmentsAsync(source, ct));
        await ReplayAsync(source, moved["undo"], ct);
        Assert.Equal(before, await AssignmentsAsync(source, ct));
    });

    [Fact]
    public Task QueryTag_UnassignUndo_AssignsTheTagBack() => WithStoreAsync(async (source, ct) =>
    {
        var one = await NewTagAsync(source, "one", null, ct);
        await DarlingMcpQueryTagTools.AssignQueryTag(source, one, "hash", Hash, null, null, null, Server, 99, ct);
        var before = await AssignmentsAsync(source, ct);

        var removed = Parse(await DarlingMcpQueryTagTools.UnassignQueryTag(source, "hash", Hash, null, null, null, Server, ct));
        Assert.Equal("unassigned", (string?)removed["status"]);
        Assert.Equal(one, (int)removed["previous"]!["tag_id"]!);
        AssertUndoShape(removed, "assign_query_tag");
        Assert.Empty(await AssignmentsAsync(source, ct));

        await ReplayAsync(source, removed["undo"], ct);
        Assert.Equal(before, await AssignmentsAsync(source, ct));
    });

    // ------------------------------------------------------------------ query tag rules

    private static async Task<string[]> RulesAsync(NpgsqlDataSource source, CancellationToken ct) =>
        (await new QueryTagRuleStore(source, 30).ListRulesAsync(ct)).OrderBy(r => r.Pattern)
            .Select(r => $"{QueryTagRuleStore.FieldName(r.Field)}|{r.Pattern}|{r.Priority}|{r.Enabled}|{r.TagId}|{r.ServerId}").ToArray();

    [Fact]
    public Task QueryTagRule_CreateUndo_DeletesTheNewRule() => WithStoreAsync(async (source, ct) =>
    {
        var tag = await NewTagAsync(source, "reports", null, ct);
        var created = Parse(await DarlingMcpQueryTagRuleTools.CreateQueryTagRule(source, "application", "Report*", tag, 50, true, null, ct));
        Assert.Equal("created", (string?)created["status"]);
        Assert.Null(created["previous"]);
        AssertUndoShape(created, "delete_query_tag_rule");

        Assert.Equal("deleted", (string?)(await ReplayAsync(source, created["undo"], ct))["status"]);
        Assert.Empty(await RulesAsync(source, ct));
    });

    [Fact]
    public Task QueryTagRule_UpdateUndo_RestoresTheChangedFields() => WithStoreAsync(async (source, ct) =>
    {
        var tag = await NewTagAsync(source, "reports", null, ct);
        var other = await NewTagAsync(source, "etl", null, ct);
        var created = Parse(await DarlingMcpQueryTagRuleTools.CreateQueryTagRule(source, "application", "Report*", tag, 50, true, Server, ct));
        var id = (int)created["rule"]!["id"]!;
        var before = await RulesAsync(source, ct);

        var updated = Parse(await DarlingMcpQueryTagRuleTools.UpdateQueryTagRule(
            source, id, "{\"pattern\":\"Etl*\",\"priority\":7,\"enabled\":false,\"tag_id\":" + other + ",\"server_id\":null}", ct));
        Assert.Equal("updated", (string?)updated["status"]);
        AssertUndoShape(updated, "update_query_tag_rule");
        Assert.NotEqual(before, await RulesAsync(source, ct));

        await ReplayAsync(source, updated["undo"], ct);
        Assert.Equal(before, await RulesAsync(source, ct));
    });

    [Fact]
    public Task QueryTagRule_DeleteUndo_RecreatesTheRule() => WithStoreAsync(async (source, ct) =>
    {
        var tag = await NewTagAsync(source, "reports", null, ct);
        var created = Parse(await DarlingMcpQueryTagRuleTools.CreateQueryTagRule(source, "login", "svc_*", tag, 60, false, Server, ct));
        var id = (int)created["rule"]!["id"]!;
        var before = await RulesAsync(source, ct);

        var deleted = Parse(await DarlingMcpQueryTagRuleTools.DeleteQueryTagRule(source, id, ct));
        Assert.Equal("deleted", (string?)deleted["status"]);
        Assert.Equal("svc_*", (string?)deleted["previous"]!["pattern"]);
        AssertUndoShape(deleted, "create_query_tag_rule");
        Assert.Empty(await RulesAsync(source, ct));

        await ReplayAsync(source, deleted["undo"], ct);
        Assert.Equal(before, await RulesAsync(source, ct));
    });
}
