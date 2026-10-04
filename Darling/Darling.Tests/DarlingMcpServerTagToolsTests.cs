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
using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>An in-memory <see cref="IServerTagStore"/> that applies the same <see cref="ServerTagRules"/> as the
/// real store and counts every write, so a test can prove a refusal wrote nothing.</summary>
internal sealed class InMemoryServerTagStore : IServerTagStore
{
    private int _nextId = 1;
    public List<ServerTagRow> Tags { get; } = new();
    public List<ServerTagAssignmentRow> Assignments { get; } = new();
    public List<TagScopedRule> Rules { get; } = new();
    public HashSet<int> Servers { get; } = new();
    public int Writes { get; private set; }

    /// <summary>Runs once, right after a write lands and before the next snapshot read.</summary>
    public Action? AfterWrite { get; set; }

    public int Seed(string name, int? parentId = null)
    {
        var id = _nextId++;
        Tags.Add(new ServerTagRow(id, name, parentId, 0, "#416FA6"));
        return id;
    }

    public Task<ServerTagSnapshot> ReadSnapshotAsync(CancellationToken ct = default) =>
        Task.FromResult(new ServerTagSnapshot(Tags.ToList(), Assignments.ToList(), Rules.ToList()));

    public Task<IReadOnlySet<int>> FindRegisteredServerIdsAsync(IReadOnlyCollection<int> serverIds, CancellationToken ct = default) =>
        Task.FromResult<IReadOnlySet<int>>(serverIds.Where(Servers.Contains).ToHashSet());

    public Task<ServerTagWriteResult> CreateAsync(string name, int? parentId, string? colour, CancellationToken ct = default)
    {
        var refusal = ServerTagRules.CheckCreate(Tags, name, parentId, colour, out var cleanName, out var cleanColour);
        if (refusal is not null)
        {
            return Task.FromResult(refusal);
        }

        var id = Seed(cleanName, parentId);
        Tags[^1] = Tags[^1] with { Colour = cleanColour ?? "#416FA6" };
        Landed();
        return Task.FromResult<ServerTagWriteResult>(new ServerTagWriteResult.Ok(Tags.Single(t => t.Id == id)));
    }

    public Task<ServerTagWriteResult> UpdateAsync(int tagId, ServerTagEdit edit, CancellationToken ct = default)
    {
        var refusal = ServerTagRules.CheckUpdate(Tags, tagId, edit, out var cleanName, out var cleanColour);
        if (refusal is not null)
        {
            return Task.FromResult(refusal);
        }

        var index = Tags.FindIndex(t => t.Id == tagId);
        var row = Tags[index];
        if (edit.HasName)
        {
            row = row with { Name = cleanName! };
        }

        if (edit.HasColour)
        {
            row = row with { Colour = cleanColour };
        }

        if (edit.HasParent)
        {
            row = row with { ParentId = edit.ParentId };
        }

        Tags[index] = row;
        Landed();
        return Task.FromResult<ServerTagWriteResult>(new ServerTagWriteResult.Ok(row));
    }

    public Task<ServerTagWriteResult> DeleteAsync(int tagId, CancellationToken ct = default)
    {
        var refusal = ServerTagRules.CheckDelete(Tags, tagId);
        if (refusal is not null)
        {
            return Task.FromResult(refusal);
        }

        var gone = ServerTagRules.Descendants(Tags, tagId);
        gone.Add(tagId);
        var removed = Assignments.RemoveAll(a => gone.Contains(a.TagId));
        Tags.RemoveAll(t => gone.Contains(t.Id));
        Landed();
        return Task.FromResult<ServerTagWriteResult>(new ServerTagWriteResult.Ok(null, removed, gone.OrderBy(i => i).ToList()));
    }

    public Task<IReadOnlyList<int>> AssignAsync(int tagId, IReadOnlyList<int> serverIds, CancellationToken ct = default)
    {
        var added = serverIds.Where(s => !Assignments.Any(a => a.TagId == tagId && a.ServerId == s)).ToList();
        Assignments.AddRange(added.Select(s => new ServerTagAssignmentRow(s, tagId)));
        Landed();
        return Task.FromResult<IReadOnlyList<int>>(added);
    }

    public Task<IReadOnlyList<int>> UnassignAsync(int tagId, IReadOnlyList<int> serverIds, CancellationToken ct = default)
    {
        var removed = serverIds.Where(s => Assignments.Any(a => a.TagId == tagId && a.ServerId == s)).ToList();
        Assignments.RemoveAll(a => a.TagId == tagId && removed.Contains(a.ServerId));
        Landed();
        return Task.FromResult<IReadOnlyList<int>>(removed);
    }

    public Task<bool> HasChildrenAsync(int tagId, CancellationToken ct = default) =>
        Task.FromResult(Tags.Any(t => t.ParentId == tagId));

    public Task ClearForServerAsync(int serverId, CancellationToken ct = default) => Task.CompletedTask;

    private void Landed()
    {
        Writes++;
        AfterWrite?.Invoke();
    }
}

/// <summary>The cores of the five server-tag write tools over <see cref="InMemoryServerTagStore"/> (#5085).</summary>
public sealed class DarlingMcpServerTagToolsTests
{
    private static readonly string[] ExpectedSurface =
    {
        "assign_server_tag", "create_server_tag", "delete_server_tag", "unassign_server_tag", "update_server_tag",
    };

    private static JsonObject Parse(string json) => (JsonObject)JsonNode.Parse(json)!;

    private static InMemoryServerTagStore Store()
    {
        var store = new InMemoryServerTagStore();
        store.Servers.UnionWith(new[] { 1, 2, 3 });
        return store;
    }

    [Fact]
    public async Task Create_ReturnsTheStoredTag_AndEveryRefusalWritesNothing()
    {
        var store = Store();
        var prod = store.Seed("Prod");
        var created = Parse(await DarlingMcpServerTagTools.CreateServerTagCore(store, " East ", prod, "#aabbcc"));
        Assert.Equal("created", (string?)created["status"]);
        Assert.Equal("East", (string?)created["tag"]!["name"]);
        Assert.Equal("#AABBCC", (string?)created["tag"]!["colour"]);
        Assert.Equal("Prod / East", (string?)created["tag"]!["path"]);
        Assert.Equal(1, store.Writes);

        var cases = new (string? Name, int? Parent, string? Colour, string Status, string Code)[]
        {
            ("  ", null, null, "invalid", "bad_name"),
            ("X", null, "red", "invalid", "bad_colour"),
            ("east", prod, null, "conflict", "duplicate_name"),
            ("X", 999, null, "not_found", "unknown_parent"),
        };
        foreach (var c in cases)
        {
            var result = Parse(await DarlingMcpServerTagTools.CreateServerTagCore(store, c.Name, c.Parent, c.Colour));
            Assert.Equal(c.Status, (string?)result["status"]);
            Assert.Equal(c.Code, (string?)result["refusal"]);
        }

        var l2 = store.Seed("L2", prod);
        var l3 = store.Seed("L3", l2);
        var l4 = store.Seed("L4", l3);
        var depth = Parse(await DarlingMcpServerTagTools.CreateServerTagCore(store, "L5", l4, null));
        Assert.Equal("depth_limit", (string?)depth["refusal"]);
        Assert.Equal(1, store.Writes);
    }

    [Fact]
    public async Task Update_ReReadsTheStoredRow_NotTheWriteResult()
    {
        var store = Store();
        var tag = store.Seed("Prod");
        /* The fake rewrites the row after the write lands: a core that echoed the write result would report the
           requested name; one that re-reads reports what the store holds. */
        store.AfterWrite = () => store.Tags[0] = store.Tags[0] with { Name = "Prod (stored)" };
        var result = Parse(await DarlingMcpServerTagTools.UpdateServerTagCore(store, tag, "{\"name\":\"Production\"}"));
        Assert.Equal("updated", (string?)result["status"]);
        Assert.Equal("Prod (stored)", (string?)result["tag"]!["name"]);
        Assert.Equal("name", (string?)result["updated_fields"]![0]);
    }

    [Fact]
    public async Task Update_NothingDiffers_IsUnchanged_AndCaseOnlyRenameIsAChange()
    {
        var store = Store();
        var tag = store.Seed("Prod");
        var same = Parse(await DarlingMcpServerTagTools.UpdateServerTagCore(store, tag, "{\"name\":\"Prod\",\"parent_id\":null}"));
        Assert.Equal("unchanged", (string?)same["status"]);
        Assert.Equal(0, store.Writes);

        var cased = Parse(await DarlingMcpServerTagTools.UpdateServerTagCore(store, tag, "{\"name\":\"PROD\"}"));
        Assert.Equal("updated", (string?)cased["status"]);
    }

    [Theory]
    [InlineData("{\"nope\":1}", "unknown_field")]
    [InlineData("{\"tag_id\":3}", "unknown_field")]
    [InlineData("{\"id\":3}", "unknown_field")]
    [InlineData("{\"sort_order\":3}", "unknown_field")]
    [InlineData("{\"name\":\"  \"}", "bad_name")]
    [InlineData("{\"name\":null}", "bad_name")]
    [InlineData("{\"colour\":\"\"}", "bad_colour")]
    [InlineData("{}", "invalid")]
    [InlineData("[1]", "invalid")]
    [InlineData("not json", "invalid")]
    public async Task Update_ChangesJsonRefusals_WriteNothing(string json, string code)
    {
        var store = Store();
        var tag = store.Seed("Prod");
        var result = Parse(await DarlingMcpServerTagTools.UpdateServerTagCore(store, tag, json));
        Assert.Equal("invalid", (string?)result["status"]);
        Assert.Equal(code, (string?)result["refusal"]);
        Assert.Equal(0, store.Writes);
    }

    [Fact]
    public async Task Update_CycleDepthDuplicateAndMissing_WriteNothing()
    {
        var store = Store();
        var a = store.Seed("A");
        var b = store.Seed("B", a);
        var c = store.Seed("C", b);
        store.Seed("A2");
        Assert.Equal("cycle", (string?)Parse(await DarlingMcpServerTagTools.UpdateServerTagCore(store, a, $"{{\"parent_id\":{c}}}"))["refusal"]);
        Assert.Equal("not_found", (string?)Parse(await DarlingMcpServerTagTools.UpdateServerTagCore(store, 99, "{\"name\":\"Z\"}"))["status"]);
        Assert.Equal("unknown_parent", (string?)Parse(await DarlingMcpServerTagTools.UpdateServerTagCore(store, a, "{\"parent_id\":99}"))["refusal"]);
        Assert.Equal("conflict", (string?)Parse(await DarlingMcpServerTagTools.UpdateServerTagCore(store, a, "{\"name\":\"a2\"}"))["status"]);
        Assert.Equal(0, store.Writes);
    }

    [Fact]
    public async Task Update_AMoveNamesTheRulesOnTheOldAndNewAncestors()
    {
        var store = Store();
        var east = store.Seed("East");
        var west = store.Seed("West");
        var leaf = store.Seed("Leaf", east);
        store.Assignments.Add(new ServerTagAssignmentRow(1, leaf));
        store.Rules.Add(new TagScopedRule(10, "east-rule", true, east));
        store.Rules.Add(new TagScopedRule(11, "west-rule", true, west));
        var result = Parse(await DarlingMcpServerTagTools.UpdateServerTagCore(store, leaf, $"{{\"parent_id\":{west}}}"));
        var rules = result["affected_rules"]!.AsArray().ToDictionary(r => (long)r!["rule_id"]!, r => (string?)r!["effect"]);
        Assert.Equal("loses_servers", rules[10]);
        Assert.Equal("gains_servers", rules[11]);
    }

    [Fact]
    public async Task Delete_WithRulesInTheSubtree_NeedsConfirm_ThenDeletes()
    {
        var store = Store();
        var parent = store.Seed("Prod");
        var child = store.Seed("East", parent);
        store.Assignments.Add(new ServerTagAssignmentRow(1, child));
        store.Rules.Add(new TagScopedRule(7, "east-rule", true, child));

        var refused = Parse(await DarlingMcpServerTagTools.DeleteServerTagCore(store, parent, false));
        Assert.Equal("confirm_required", (string?)refused["status"]);
        Assert.Equal("rules_affected", (string?)refused["refusal"]);
        Assert.Equal(7, (long)refused["affected_rules"]![0]!["rule_id"]!);
        Assert.Equal("orphaned", (string?)refused["affected_rules"]![0]!["effect"]);
        Assert.Equal(0, store.Writes);
        Assert.Equal(2, store.Tags.Count);

        var done = Parse(await DarlingMcpServerTagTools.DeleteServerTagCore(store, parent, true));
        Assert.Equal("deleted", (string?)done["status"]);
        Assert.Equal(1, (int)done["removed_assignments"]!);
        Assert.Equal(2, done["deleted_tag_ids"]!.AsArray().Count);
        Assert.Equal("orphaned", (string?)done["affected_rules"]![0]!["effect"]);
        Assert.Empty(store.Tags);
    }

    [Fact]
    public async Task Delete_WithNoRules_NeedsNoConfirm_AndAMissingTagIsNotFound()
    {
        var store = Store();
        var tag = store.Seed("Prod");
        Assert.Equal("not_found", (string?)Parse(await DarlingMcpServerTagTools.DeleteServerTagCore(store, 99, true))["status"]);
        Assert.Equal(0, store.Writes);
        Assert.Equal("deleted", (string?)Parse(await DarlingMcpServerTagTools.DeleteServerTagCore(store, tag, false))["status"]);
    }

    [Fact]
    public async Task Assign_ReportsAddedAndAlreadyAssigned_AndUnknownServerWritesNothing()
    {
        var store = Store();
        var tag = store.Seed("Prod");
        store.Assignments.Add(new ServerTagAssignmentRow(1, tag));
        store.Rules.Add(new TagScopedRule(5, "r", true, tag));

        var unknown = Parse(await DarlingMcpServerTagTools.AssignServerTagCore(store, tag, new[] { 2, 77 }));
        Assert.Equal("not_found", (string?)unknown["status"]);
        Assert.Equal("unknown_server", (string?)unknown["refusal"]);
        Assert.Equal(77, (int)unknown["unknown_server_ids"]![0]!);
        Assert.Equal(0, store.Writes);

        var result = Parse(await DarlingMcpServerTagTools.AssignServerTagCore(store, tag, new[] { 1, 2, 2, 3 }));
        Assert.Equal("assigned", (string?)result["status"]);
        Assert.Equal(new[] { 2, 3 }, result["added_server_ids"]!.AsArray().Select(n => (int)n!).ToArray());
        Assert.Equal(new[] { 1 }, result["already_assigned_server_ids"]!.AsArray().Select(n => (int)n!).ToArray());
        Assert.Equal("gains_servers", (string?)result["affected_rules"]![0]!["effect"]);

        var again = Parse(await DarlingMcpServerTagTools.AssignServerTagCore(store, tag, new[] { 1, 2 }));
        Assert.Equal("unchanged", (string?)again["status"]);
        Assert.Equal(1, store.Writes);
    }

    [Fact]
    public async Task Unassign_ReportsRemovedAndNotAssigned_AndNamesTheRulesLosingServers()
    {
        var store = Store();
        var tag = store.Seed("Prod");
        store.Assignments.Add(new ServerTagAssignmentRow(1, tag));
        store.Rules.Add(new TagScopedRule(5, "r", true, tag));

        var result = Parse(await DarlingMcpServerTagTools.UnassignServerTagCore(store, tag, new[] { 1, 2 }));
        Assert.Equal("unassigned", (string?)result["status"]);
        Assert.Equal(new[] { 1 }, result["removed_server_ids"]!.AsArray().Select(n => (int)n!).ToArray());
        Assert.Equal(new[] { 2 }, result["not_assigned_server_ids"]!.AsArray().Select(n => (int)n!).ToArray());
        Assert.Equal("loses_servers", (string?)result["affected_rules"]![0]!["effect"]);

        var none = Parse(await DarlingMcpServerTagTools.UnassignServerTagCore(store, tag, new[] { 2 }));
        Assert.Equal("unchanged", (string?)none["status"]);
    }

    [Fact]
    public async Task AssignAndUnassign_RefuseBadIdLists_AndAMissingTag_WithoutWriting()
    {
        var store = Store();
        var tag = store.Seed("Prod");
        Assert.Equal("invalid", (string?)Parse(await DarlingMcpServerTagTools.AssignServerTagCore(store, tag, Array.Empty<int>()))["status"]);
        Assert.Equal("invalid", (string?)Parse(await DarlingMcpServerTagTools.UnassignServerTagCore(store, tag, Enumerable.Range(1, 1001).ToArray()))["status"]);
        Assert.Equal("not_found", (string?)Parse(await DarlingMcpServerTagTools.AssignServerTagCore(store, 99, new[] { 1 }))["status"]);
        Assert.Equal("not_found", (string?)Parse(await DarlingMcpServerTagTools.UnassignServerTagCore(store, 99, new[] { 1 }))["status"]);
        Assert.Equal(0, store.Writes);
    }

    [Fact]
    public async Task ATagDeletedBetweenWriteAndReadBack_IsReportedAsDeletedConcurrently()
    {
        var store = Store();
        var tag = store.Seed("Prod");
        store.AfterWrite = () => store.Tags.Clear();
        var result = Parse(await DarlingMcpServerTagTools.UpdateServerTagCore(store, tag, "{\"name\":\"Production\"}"));
        Assert.Equal("not_found", (string?)result["status"]);
        Assert.Contains("deleted concurrently", (string?)result["message"], StringComparison.Ordinal);

        var store2 = Store();
        store2.AfterWrite = () => store2.Tags.Clear();
        var assign = Parse(await DarlingMcpServerTagTools.AssignServerTagCore(store2, store2.Seed("X"), new[] { 1 }));
        Assert.Equal("not_found", (string?)assign["status"]);
    }

    [Fact]
    public void TheSurface_IsExactlyTheFiveNamedTools_WithTheirParameters()
    {
        var methods = typeof(DarlingMcpServerTagTools)
            .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
            .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
            .ToDictionary(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name!, StringComparer.Ordinal);
        Assert.Equal(ExpectedSurface, methods.Keys.OrderBy(n => n, StringComparer.Ordinal).ToArray());
        Assert.NotNull(typeof(DarlingMcpServerTagTools).GetCustomAttribute<McpServerToolTypeAttribute>());

        string[] Names(string tool) => methods[tool].GetParameters().Select(p => p.Name!).ToArray();
        Assert.Equal(new[] { "postgres", "name", "parent_id", "colour", "cancellationToken" }, Names("create_server_tag"));
        Assert.Equal(new[] { "postgres", "tag_id", "changes_json", "cancellationToken" }, Names("update_server_tag"));
        Assert.Equal(new[] { "postgres", "tag_id", "confirm", "cancellationToken" }, Names("delete_server_tag"));
        Assert.Equal(new[] { "postgres", "tag_id", "server_ids", "cancellationToken" }, Names("assign_server_tag"));
        Assert.Equal(new[] { "postgres", "tag_id", "server_ids", "cancellationToken" }, Names("unassign_server_tag"));
        Assert.All(methods.Values, m => Assert.True(m.GetCustomAttribute<System.ComponentModel.DescriptionAttribute>()!.Description.Length <= 600));
    }

    [Fact]
    public void TheHost_RegistersTheClass_RightAfterTheCustomAlertTools()
    {
        var source = System.IO.File.ReadAllText(System.IO.Path.Combine(AppContext.BaseDirectory, "Fixtures", "DarlingMcpHostService.cs"));
        var alert = source.IndexOf("WithGeminiCompatibleTools<DarlingMcpCustomAlertTools>()", StringComparison.Ordinal);
        var tags = source.IndexOf("WithGeminiCompatibleTools<DarlingMcpServerTagTools>()", StringComparison.Ordinal);
        Assert.True(alert > 0 && tags > alert, "DarlingMcpServerTagTools must be registered after DarlingMcpCustomAlertTools.");
    }
}
