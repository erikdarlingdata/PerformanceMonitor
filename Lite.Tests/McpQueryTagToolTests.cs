/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Behaviour pins for Lite's query-tag MCP tools (#5991) over a real DuckDB: the write gate, the shared refusal code,
/// a hash assignment shown by the list, the delete confirmation, and the rule test over seeded sightings.
/// </summary>
public sealed class McpQueryTagToolTests : IClassFixture<SharedDuckDbFixture>
{
    private static readonly string[] s_writes =
    [
        "create_query_tag", "update_query_tag", "delete_query_tag", "assign_query_tag", "unassign_query_tag",
        "create_query_tag_rule", "update_query_tag_rule", "delete_query_tag_rule",
    ];

    private static readonly string[] s_reads = ["list_query_tags", "list_query_tag_rules", "test_query_tag_rule"];

    private readonly SharedDuckDbFixture _fixture;
    private readonly LocalDataService _data;

    public McpQueryTagToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _fixture = fixture;
        _data = new LocalDataService(fixture.DuckDb);
    }

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    private async Task ExecAsync(string sql)
    {
        using var connection = _fixture.DuckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public void WritesOff_TheEightWritesAreNotServed_AndTheThreeReadsAre()
    {
        var (tools, _, _) = McpToolsListBudgetTests.BuildServedTools();
        var all = new McpServerPrimitiveCollection<McpServerTool>();
        foreach (var tool in tools)
        {
            all.Add(tool);
        }

        var off = McpWritePolicy.FilterToolCollection(all, allowWrites: false).Select(t => t.ProtocolTool.Name).ToHashSet();
        var on = McpWritePolicy.FilterToolCollection(all, allowWrites: true).Select(t => t.ProtocolTool.Name).ToHashSet();

        Assert.All(s_writes, w => Assert.DoesNotContain(w, off));
        Assert.All(s_writes, w => Assert.Contains(w, on));
        Assert.All(s_reads, r => Assert.Contains(r, off));
    }

    [Fact]
    public async Task Create_WithABadName_IsRefusedWithTheSharedCode_AndWritesNothing()
    {
        var result = Parse(await McpQueryTagTools.CreateQueryTagCore(_data, "   ", null, null));

        Assert.Equal("invalid", result.GetProperty("status").GetString());
        Assert.Equal("bad_name", result.GetProperty("refusal").GetString());
        Assert.Empty(await _data.GetQueryTagsAsync());
    }

    [Fact]
    public async Task AssignByHash_ThenListShowsIt_AndTheWriteSaysTheMapIsNotRebuilt()
    {
        var created = Parse(await McpQueryTagTools.CreateQueryTagCore(_data, "Reports", null, null));
        var tagId = created.GetProperty("tag").GetProperty("tag_id").GetInt32();

        var assigned = Parse(await McpQueryTagTools.AssignQueryTagCore(_data, [], tagId, "hash", "0xABCD", null, null, null, null, null));
        Assert.Equal("assigned", assigned.GetProperty("status").GetString());
        Assert.Contains("not rebuilt", assigned.GetProperty("note").GetString(), StringComparison.Ordinal);

        var list = Parse(await McpQueryTagTools.ListQueryTagsCore(_data, []));
        var tag = list.GetProperty("tags").EnumerateArray().Single();
        Assert.Equal(1, tag.GetProperty("assignment_count").GetInt32());
        var assignment = tag.GetProperty("assignments").EnumerateArray().Single();
        Assert.Equal("hash", assignment.GetProperty("kind").GetString());
        Assert.Equal("0xABCD", assignment.GetProperty("query_hash").GetString());
    }

    [Fact]
    public async Task DeleteATagWithAssignments_WithoutConfirm_AnswersConfirmRequired_AndChangesNothing()
    {
        var tagId = await _data.CreateQueryTagAsync("Billing", null);
        await McpQueryTagTools.AssignQueryTagCore(_data, [], tagId, "hash", "0x01", null, null, null, null, null);

        var refused = Parse(await McpQueryTagTools.DeleteQueryTagCore(_data, tagId, confirm: false));

        Assert.Equal("confirm_required", refused.GetProperty("status").GetString());
        Assert.Equal(1, refused.GetProperty("assignments").GetInt32());
        Assert.Single(await _data.GetQueryTagsAsync());
        Assert.Single(await _data.GetQueryTagAssignmentsAsync());

        var done = Parse(await McpQueryTagTools.DeleteQueryTagCore(_data, tagId, confirm: true));
        Assert.Equal("deleted", done.GetProperty("status").GetString());
        Assert.Empty(await _data.GetQueryTagsAsync());
    }

    [Fact]
    public async Task TestRule_OnSeededSightings_ReportsTheMatchingCount()
    {
        var tagId = await _data.CreateQueryTagAsync("Reporting", null);
        await ExecAsync(@"INSERT INTO query_tag_sightings (server_id, key_kind, key, application_name) VALUES
            (1, 'hash', '0xA1', 'ReportBuilder'), (1, 'hash', '0xA2', 'reportbuilder'), (1, 'hash', '0xA3', 'WebApp')");

        var result = Parse(await McpQueryTagTools.TestRuleCore(_data, [1], null, "application", "Report*", tagId, 100, null, 168));

        Assert.Equal("ok", result.GetProperty("status").GetString());
        Assert.True(result.GetProperty("draft").GetBoolean());
        Assert.Equal(3, result.GetProperty("keys_examined").GetInt32());
        Assert.Equal(2, result.GetProperty("keys_matched").GetInt32());
        Assert.Equal(2, result.GetProperty("would_tag").GetInt32());
    }
}
