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
using PerformanceMonitor.Common.QueryTags;
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

    /* #6455: Lite's assign_query_tag refuses exactly what the Darling twin refuses (the shared QueryHashIdentity.IsAssignableHash),
       so a key that is not a hash never lands in the map; the refusal comes before any write. */
    [Theory]
    [InlineData("0xG6G6G6G6G6G6G6G6")]
    [InlineData("0x")]
    [InlineData("hello")]
    [InlineData("0x1A2B3C4D5E6F70811")]
    [InlineData("12.5")]
    [InlineData("-5")]
    [InlineData("99999999999999999999999")]
    [InlineData("0")]
    [InlineData("0x0000000000000000")]
    public async Task AssignQueryTag_RefusesAHashThatIsNotHexadecimal_AndStoresNothing(string hash)
    {
        var created = Parse(await McpQueryTagTools.CreateQueryTagCore(_data, "Reports", null, null));
        var tagId = created.GetProperty("tag").GetProperty("tag_id").GetInt32();

        var answer = Parse(await McpQueryTagTools.AssignQueryTagCore(_data, [], tagId, "hash", hash, null, null, null, null, null));

        Assert.Equal("invalid", answer.GetProperty("status").GetString());
        Assert.Contains("hexadecimal", answer.GetProperty("message").GetString(), StringComparison.Ordinal);
        var list = Parse(await McpQueryTagTools.ListQueryTagsCore(_data, []));
        Assert.Equal(0, list.GetProperty("tags").EnumerateArray().Single().GetProperty("assignment_count").GetInt32());
    }

    [Fact]
    public async Task UnassignQueryTag_DoesNotApplyTheHashShapeRefusal_SoAnOlderKeyCanStillBeRemoved()
    {
        var answer = Parse(await McpQueryTagTools.UnassignQueryTagCore(_data, "hash", "hello", null, null, null, null));

        Assert.DoesNotContain("hexadecimal", answer.GetProperty("message").GetString() ?? "", StringComparison.Ordinal);
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

    [Fact]
    public async Task Assign_ToANegativeServerId_IsJudgedByTheRegistry_NotBySign()
    {
        var tagId = await _data.CreateQueryTagAsync("Reports", null);

        var known = Parse(await McpQueryTagTools.AssignQueryTagCore(_data, [-12345], tagId, "hash", "0xAB", null, null, null, -12345, null));
        var unknown = Parse(await McpQueryTagTools.AssignQueryTagCore(_data, [7], tagId, "hash", "0xCD", null, null, null, -12345, null));

        Assert.Equal("assigned", known.GetProperty("status").GetString());
        Assert.Equal(-12345, known.GetProperty("assignment").GetProperty("server_id").GetInt32());
        Assert.Equal("not_found", unknown.GetProperty("status").GetString());
    }

    [Theory]
    [InlineData("Caf\u00e9", "CAF\u00c9", "DB.DBO.CAF\u00c9")]
    // Final sigma and sigma share an upper case (one key to the resolver) but ToLowerInvariant keeps them apart.
    [InlineData("Orders\u03c2", "Orders\u03c3", "DB.DBO.ORDERS\u03a3")]
    public async Task Assign_ANonAsciiObjectCaseTwin_MovesIt_AndUnassignRemovesIt(string first, string twin, string unassignAs)
    {
        var tagA = await _data.CreateQueryTagAsync("A", null);
        var tagB = await _data.CreateQueryTagAsync("B", null);
        await McpQueryTagTools.AssignQueryTagCore(_data, [], tagA, "object", null, "db", "dbo", first, null, null);
        var moved = Parse(await McpQueryTagTools.AssignQueryTagCore(_data, [], tagB, "object", null, "db", "dbo", twin, null, null));

        Assert.Equal("moved", moved.GetProperty("status").GetString());
        Assert.Single(await _data.GetQueryTagAssignmentsAsync());

        var parts = unassignAs.Split('.');
        await McpQueryTagTools.UnassignQueryTagCore(_data, "object", null, parts[0], parts[1], parts[2], null);
        Assert.Empty(await _data.GetQueryTagAssignmentsAsync());
    }

    [Fact]
    public async Task Assign_AKelvinSignAndALowerKAreTwoKeys_BecauseTheResolverUpperCasesThem()
    {
        var tagA = await _data.CreateQueryTagAsync("A", null);
        var tagB = await _data.CreateQueryTagAsync("B", null);
        await McpQueryTagTools.AssignQueryTagCore(_data, [], tagA, "object", null, "db", "dbo", "\u212Aey", null, null);
        var added = Parse(await McpQueryTagTools.AssignQueryTagCore(_data, [], tagB, "object", null, "db", "dbo", "key", null, null));

        Assert.NotEqual("moved", added.GetProperty("status").GetString());
        Assert.Equal(2, (await _data.GetQueryTagAssignmentsAsync()).Count);
    }

    [Fact]
    public async Task Assign_ACaseTwinOfAnExistingKey_MovesIt_AndStoresTheCanonicalHash()
    {
        var first = await _data.CreateQueryTagAsync("First", null);
        var second = await _data.CreateQueryTagAsync("Second", null);
        await McpQueryTagTools.AssignQueryTagCore(_data, [], first, "hash", "0xabcdef", null, null, null, null, null);
        var movedHash = Parse(await McpQueryTagTools.AssignQueryTagCore(_data, [], second, "hash", "0xABCDEF", null, null, null, null, null));

        await McpQueryTagTools.AssignQueryTagCore(_data, [], first, "object", null, "Sales", "dbo", "Orders", null, null);
        var movedObject = Parse(await McpQueryTagTools.AssignQueryTagCore(_data, [], second, "object", null, "SALES", "DBO", "orders", null, null));

        Assert.Equal("moved", movedHash.GetProperty("status").GetString());
        Assert.Equal("moved", movedObject.GetProperty("status").GetString());
        var stored = await _data.GetQueryTagAssignmentsAsync();
        Assert.Equal(2, stored.Count);
        Assert.Contains(stored, a => a.QueryHash == "0xABCDEF");
        Assert.Contains(stored, a => a.ObjectName == "orders" && a.TagId == second);
    }

    [Fact]
    public async Task List_CarriesEnabledRuleCount_AndTheSharedPrecedenceRanks()
    {
        var tagId = await _data.CreateQueryTagAsync("Reports", null);
        await McpQueryTagTools.CreateRuleCore(_data, [], "application", "Rep*", tagId, 100, true, null);
        await McpQueryTagTools.CreateRuleCore(_data, [], "login", "bob", tagId, 100, false, null);
        await McpQueryTagTools.AssignQueryTagCore(_data, [], tagId, "object", null, "Sales", "dbo", "Orders", null, null);

        var listed = Parse(await McpQueryTagTools.ListQueryTagsCore(_data, []));

        var tag = listed.GetProperty("tags")[0];
        Assert.Equal(2, tag.GetProperty("rule_count").GetInt32());
        Assert.Equal(1, tag.GetProperty("enabled_rule_count").GetInt32());
        Assert.Equal(4, tag.GetProperty("assignments")[0].GetProperty("precedence_rank").GetInt32());
        Assert.Equal(5, listed.GetProperty("precedence").GetArrayLength());
    }

    [Fact]
    public async Task ACancelledRequest_PropagatesInsteadOfAnErrorEnvelope()
    {
        using var cts = new System.Threading.CancellationTokenSource();
        var tagId = await _data.CreateQueryTagAsync("Reports", null);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            McpQueryTagTools.GuardAsync("create_query_tag", () => Task.FromCanceled<string>(new System.Threading.CancellationToken(true))));
        Assert.True(tagId > 0);
    }

    [Fact]
    public async Task Sightings_ReadBackEmptyCallerFieldsAndDatabaseAsNull()
    {
        await ExecAsync(@"INSERT INTO query_tag_sightings (server_id, key_kind, key, application_name) VALUES (1, 'hash', '0xA1', 'App')");

        var rows = await _data.GetQueryTagSightingsAsync(null, 168, 10);

        var row = Assert.Single(rows);
        Assert.Equal("App", row.ApplicationName);
        Assert.Null(row.LoginName);
        Assert.Null(row.HostName);
        Assert.Null(row.DatabaseName);
    }

    [Fact]
    public async Task TestRule_CutAtTheCap_SaysTruncated_ExactlyTheCapDoesNot()
    {
        var tagId = await _data.CreateQueryTagAsync("Reports", null);
        var cap = McpQueryTagTools.MaxSightingRows;
        await ExecAsync($@"INSERT INTO query_tag_sightings (server_id, key_kind, key, application_name)
            SELECT 1, 'hash', 'k' || i::VARCHAR, 'App' FROM range({cap}) t(i)");

        var exact = Parse(await McpQueryTagTools.TestRuleCore(_data, [1], null, "application", "App", tagId, 100, null, 168));
        await ExecAsync("INSERT INTO query_tag_sightings (server_id, key_kind, key, application_name) VALUES (1, 'hash', 'extra', 'App')");
        var cut = Parse(await McpQueryTagTools.TestRuleCore(_data, [1], null, "application", "App", tagId, 100, null, 168));

        Assert.False(exact.GetProperty("sightings_truncated").GetBoolean());
        Assert.Equal(cap, exact.GetProperty("sightings_read").GetInt32());
        Assert.True(cut.GetProperty("sightings_truncated").GetBoolean());
        Assert.True(cut.GetProperty("sightings_read").GetInt32() <= cap);
        Assert.Contains("counts are of those rows", cut.GetProperty("message").GetString());
    }
}
