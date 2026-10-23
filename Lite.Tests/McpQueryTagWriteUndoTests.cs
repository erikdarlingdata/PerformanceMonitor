/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #6403 group B, Lite parity: the query-tag and query-tag-rule write tools answer with the previous value and an undo call in the
/// {tool, arguments} shape Darling uses. Every test calls the tool, replays the returned undo through the real tool core against a temp
/// DuckDB, and checks the store is back where it started.
/// </summary>
public sealed class McpQueryTagWriteUndoTests : IDisposable
{
    private const int Server = 4242;
    private const string Hash = "0xA1B2C3D4E5F60718";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _data;
    private readonly int[] _known = { Server };

    public McpQueryTagWriteUndoTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "QueryTagUndo_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "undo.duckdb"));
        _duckDb.InitializeAsync().GetAwaiter().GetResult();
        _data = new LocalDataService(_duckDb);
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    private static JsonObject Parse(string json) => (JsonObject)JsonNode.Parse(json)!;

    private async Task<JsonObject> ReplayAsync(JsonNode? undo)
    {
        Assert.NotNull(undo);
        var tool = (string)undo!["tool"]!;
        var a = undo["arguments"]!.AsObject();
        string answer = tool switch
        {
            "delete_query_tag" => await McpQueryTagTools.DeleteQueryTagCore(_data, (int)a["tag_id"]!, (bool?)a["confirm"] ?? false),
            "create_query_tag" => await McpQueryTagTools.CreateQueryTagCore(_data, (string)a["name"]!, (int?)a["parent_id"], (string?)a["colour"]),
            "update_query_tag" => await McpQueryTagTools.UpdateQueryTagCore(_data, (int)a["tag_id"]!, (string)a["changes_json"]!),
            "assign_query_tag" => await McpQueryTagTools.AssignQueryTagCore(
                _data, _known, (int)a["tag_id"]!, (string)a["kind"]!, (string?)a["query_hash"], (string?)a["database_name"], (string?)a["schema_name"],
                (string?)a["object_name"], (int?)a["server_id"], (long?)a["query_id"]),
            "unassign_query_tag" => await McpQueryTagTools.UnassignQueryTagCore(
                _data, (string)a["kind"]!, (string?)a["query_hash"], (string?)a["database_name"], (string?)a["schema_name"],
                (string?)a["object_name"], (int?)a["server_id"]),
            "delete_query_tag_rule" => await McpQueryTagTools.DeleteRuleCore(_data, (int)a["rule_id"]!),
            "create_query_tag_rule" => await McpQueryTagTools.CreateRuleCore(
                _data, _known, (string)a["field"]!, (string?)a["pattern"], (int?)a["tag_id"], (int?)a["priority"] ?? 100, (bool?)a["enabled"] ?? true,
                (int?)a["server_id"]),
            "update_query_tag_rule" => await McpQueryTagTools.UpdateRuleCore(_data, _known, (int)a["rule_id"]!, (string)a["changes_json"]!),
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

    private async Task<int> NewTagAsync(string name, int? parent) =>
        (int)Parse(await McpQueryTagTools.CreateQueryTagCore(_data, name, parent, null))["tag"]!["tag_id"]!;

    private async Task<string[]> TagsAsync() =>
        (await _data.GetQueryTagsAsync()).OrderBy(t => t.Id).Select(t => $"{t.Id}:{t.Name}:{t.ParentId}:{t.Colour}").ToArray();

    private async Task<string[]> AssignmentsAsync() =>
        (await _data.GetQueryTagAssignmentsAsync()).OrderBy(a => a.QueryHash).ThenBy(a => a.ObjectName)
            .Select(a => $"{a.TagId}|{a.Kind}|{a.ServerId}|{a.QueryHash}|{a.DatabaseName}|{a.SchemaName}|{a.ObjectName}").ToArray();

    private async Task<string[]> RulesAsync() =>
        (await _data.GetQueryTagRulesAsync()).OrderBy(r => r.Pattern)
            .Select(r => $"{r.MatchField}|{r.Pattern}|{r.Priority}|{r.Enabled}|{r.TagId}|{r.ServerId}").ToArray();

    [Fact]
    public async Task QueryTag_CreateUndo_DeletesTheNewTag()
    {
        var created = Parse(await McpQueryTagTools.CreateQueryTagCore(_data, "undo-tag", null, "#336699"));
        Assert.Null(created["previous"]);
        AssertUndoShape(created, "delete_query_tag");

        Assert.Equal("deleted", (string?)(await ReplayAsync(created["undo"]))["status"]);
        Assert.Empty(await TagsAsync());
    }

    [Fact]
    public async Task QueryTag_UpdateUndo_RestoresNameColourAndParent()
    {
        var parent = await NewTagAsync("parent", null);
        var other = await NewTagAsync("other-parent", null);
        var id = (int)Parse(await McpQueryTagTools.CreateQueryTagCore(_data, "child", parent, "#336699"))["tag"]!["tag_id"]!;
        var original = await TagsAsync();

        var updated = Parse(await McpQueryTagTools.UpdateQueryTagCore(
            _data, id, "{\"name\":\"child-renamed\",\"colour\":null,\"parent_id\":" + other + "}"));
        Assert.Equal("updated", (string?)updated["status"]);
        AssertUndoShape(updated, "update_query_tag");
        Assert.NotEqual(original, await TagsAsync());

        await ReplayAsync(updated["undo"]);
        Assert.Equal(original, await TagsAsync());
    }

    [Fact]
    public async Task QueryTag_DeleteOfALeaf_UndoRecreatesIt_AndOfASubtree_SaysNoSingleCallUndoesIt()
    {
        var root = await NewTagAsync("root", null);
        var leafId = await NewTagAsync("leaf", root);

        var deleted = Parse(await McpQueryTagTools.DeleteQueryTagCore(_data, leafId, false));
        Assert.Equal("deleted", (string?)deleted["status"]);
        AssertUndoShape(deleted, "create_query_tag");
        await ReplayAsync(deleted["undo"]);
        var tags = await _data.GetQueryTagsAsync();
        Assert.Equal(root, tags.Single(t => t.Name == "leaf").ParentId);

        var subtree = Parse(await McpQueryTagTools.DeleteQueryTagCore(_data, root, true));
        Assert.Equal("deleted", (string?)subtree["status"]);
        Assert.Equal("root", (string?)subtree["previous"]!["name"]);
        Assert.Null(subtree["undo"]);
        Assert.False(string.IsNullOrEmpty((string?)subtree["undo_note"]));
    }

    [Fact]
    public async Task QueryTag_AssignUndo_RemovesTheAssignment_AndAMoveUndoPutsTheOldTagBack()
    {
        var one = await NewTagAsync("one", null);
        var two = await NewTagAsync("two", null);

        var assigned = Parse(await McpQueryTagTools.AssignQueryTagCore(_data, _known, one, "hash", Hash, null, null, null, null, null));
        Assert.Equal("assigned", (string?)assigned["status"]);
        Assert.Null(assigned["previous"]!["tag_id"]);
        AssertUndoShape(assigned, "unassign_query_tag");
        await ReplayAsync(assigned["undo"]);
        Assert.Empty(await AssignmentsAsync());

        await McpQueryTagTools.AssignQueryTagCore(_data, _known, one, "object", null, "db1", "dbo", "proc1", Server, null);
        var before = await AssignmentsAsync();
        var moved = Parse(await McpQueryTagTools.AssignQueryTagCore(_data, _known, two, "object", null, "db1", "dbo", "proc1", Server, null));
        Assert.Equal("moved", (string?)moved["status"]);
        Assert.Equal(one, (int)moved["previous"]!["tag_id"]!);
        AssertUndoShape(moved, "assign_query_tag");
        Assert.NotEqual(before, await AssignmentsAsync());
        await ReplayAsync(moved["undo"]);
        Assert.Equal(before, await AssignmentsAsync());
    }

    [Fact]
    public async Task QueryTag_UnassignUndo_AssignsTheTagBack()
    {
        var one = await NewTagAsync("one", null);
        await McpQueryTagTools.AssignQueryTagCore(_data, _known, one, "hash", Hash, null, null, null, Server, 99);
        var before = await AssignmentsAsync();

        var removed = Parse(await McpQueryTagTools.UnassignQueryTagCore(_data, "hash", Hash, null, null, null, Server));
        Assert.Equal("unassigned", (string?)removed["status"]);
        Assert.Equal(one, (int)removed["previous"]!["tag_id"]!);
        AssertUndoShape(removed, "assign_query_tag");
        Assert.Empty(await AssignmentsAsync());

        await ReplayAsync(removed["undo"]);
        Assert.Equal(before, await AssignmentsAsync());
    }

    [Fact]
    public async Task QueryTagRule_CreateUndo_DeletesTheNewRule()
    {
        var tag = await NewTagAsync("reports", null);
        var created = Parse(await McpQueryTagTools.CreateRuleCore(_data, _known, "application", "Report*", tag, 50, true, null));
        Assert.Equal("created", (string?)created["status"]);
        Assert.Null(created["previous"]);
        AssertUndoShape(created, "delete_query_tag_rule");

        Assert.Equal("deleted", (string?)(await ReplayAsync(created["undo"]))["status"]);
        Assert.Empty(await RulesAsync());
    }

    [Fact]
    public async Task QueryTagRule_UpdateUndo_RestoresTheChangedFields()
    {
        var tag = await NewTagAsync("reports", null);
        var other = await NewTagAsync("etl", null);
        var created = Parse(await McpQueryTagTools.CreateRuleCore(_data, _known, "application", "Report*", tag, 50, true, Server));
        var id = (int)created["rule"]!["id"]!;
        var before = await RulesAsync();

        var updated = Parse(await McpQueryTagTools.UpdateRuleCore(
            _data, _known, id, "{\"pattern\":\"Etl*\",\"priority\":7,\"enabled\":false,\"tag_id\":" + other + ",\"server_id\":null}"));
        Assert.Equal("updated", (string?)updated["status"]);
        AssertUndoShape(updated, "update_query_tag_rule");
        Assert.NotEqual(before, await RulesAsync());

        await ReplayAsync(updated["undo"]);
        Assert.Equal(before, await RulesAsync());
    }

    [Fact]
    public async Task QueryTagRule_DeleteUndo_RecreatesTheRule()
    {
        var tag = await NewTagAsync("reports", null);
        var created = Parse(await McpQueryTagTools.CreateRuleCore(_data, _known, "login", "svc_*", tag, 60, false, Server));
        var id = (int)created["rule"]!["id"]!;
        var before = await RulesAsync();

        var deleted = Parse(await McpQueryTagTools.DeleteRuleCore(_data, id));
        Assert.Equal("deleted", (string?)deleted["status"]);
        Assert.Equal("svc_*", (string?)deleted["previous"]!["pattern"]);
        AssertUndoShape(deleted, "create_query_tag_rule");
        Assert.Empty(await RulesAsync());

        await ReplayAsync(deleted["undo"]);
        Assert.Equal(before, await RulesAsync());
    }
}
