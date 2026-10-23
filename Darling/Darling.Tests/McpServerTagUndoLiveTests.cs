/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
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
/// #6403 group C: the five server-tag write tools answer with the previous value and an undo call in the {tool, arguments} shape
/// <c>enable_server</c> uses. Every test calls the tool, REPLAYS the returned undo through the real tool, and checks the stored
/// tags and assignments are back where they started.
/// </summary>
public sealed class McpServerTagUndoLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task WithStoreAsync(Func<NpgsqlDataSource, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the server-tag undo tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var owner = new NpgsqlConnection(connectionString))
        {
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            await using var insert = new NpgsqlCommand(
                "INSERT INTO servers (server_id, server_name, display_name) VALUES (7801, 'tag-alpha', 'tag-alpha'), (7802, 'tag-beta', 'tag-beta')", owner);
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

    private static async Task<string?> ScalarAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    /// <summary>Every tag as name, colour and parent NAME, in name order: ids differ after a delete-and-recreate, so the shape is compared, not the ids.</summary>
    private const string TagShape =
        "SELECT coalesce(string_agg(t.name || '|' || coalesce(t.colour, '-') || '|' || coalesce(p.name, '-'), ',' ORDER BY t.name), '') " +
        "FROM server_tags t LEFT JOIN server_tags p ON p.id = t.parent_id";

    private const string MapShape =
        "SELECT coalesce(string_agg(t.name || ':' || m.server_id, ',' ORDER BY t.name, m.server_id), '') FROM server_tag_map m JOIN server_tags t ON t.id = m.tag_id";

    private static int Id(JsonObject answer) => (int)answer["tag"]!["tag_id"]!;

    /// <summary>The undo call an answer returned, checked for the shared {tool, arguments} shape, run through the real tool.</summary>
    private static async Task<JsonObject> ReplayAsync(NpgsqlDataSource source, JsonNode? undo, string expectedTool, CancellationToken ct)
    {
        Assert.NotNull(undo);
        Assert.Equal(expectedTool, (string)undo!["tool"]!);
        var a = undo["arguments"]!.AsObject();
        int[] Servers() => a["server_ids"]!.AsArray().Select(n => (int)n!).ToArray();
        string answer = expectedTool switch
        {
            "delete_server_tag" => await DarlingMcpServerTagTools.DeleteServerTag(source, (int)a["tag_id"]!, false, ct),
            "create_server_tag" => await DarlingMcpServerTagTools.CreateServerTag(source, (string)a["name"]!, (int?)a["parent_id"], (string?)a["colour"], ct),
            "update_server_tag" => await DarlingMcpServerTagTools.UpdateServerTag(source, (int)a["tag_id"]!, (string)a["changes_json"]!, ct),
            "assign_server_tag" => await DarlingMcpServerTagTools.AssignServerTag(source, (int)a["tag_id"]!, Servers(), ct),
            "unassign_server_tag" => await DarlingMcpServerTagTools.UnassignServerTag(source, (int)a["tag_id"]!, Servers(), ct),
            _ => throw new InvalidOperationException(expectedTool),
        };
        return Parse(answer);
    }

    [Fact]
    public async Task CreateServerTag_AnswersWithAnUndoThatDeletesTheTag()
    {
        await WithStoreAsync(async (source, ct) =>
        {
            var created = Parse(await DarlingMcpServerTagTools.CreateServerTag(source, "East", null, "#112233", ct));
            Assert.Equal("created", (string)created["status"]!);
            Assert.Null(created["previous"]);
            Assert.Equal("East|#112233|-", await ScalarAsync(source, TagShape, ct));

            var undone = await ReplayAsync(source, created["undo"], "delete_server_tag", ct);
            Assert.Equal("deleted", (string)undone["status"]!);
            Assert.Equal("", await ScalarAsync(source, TagShape, ct));
        });
    }

    [Theory]
    [InlineData("{\"name\":\"West\",\"colour\":null}", "updated")]
    [InlineData("{\"name\":\"East\"}", "unchanged")]
    public async Task UpdateServerTag_AnswersWithThePreviousFields_AndAnUndoThatPutsThemBack(string changes, string status)
    {
        await WithStoreAsync(async (source, ct) =>
        {
            var root = Parse(await DarlingMcpServerTagTools.CreateServerTag(source, "Root", null, null, ct));
            var created = Parse(await DarlingMcpServerTagTools.CreateServerTag(source, "East", Id(root), "#112233", ct));
            var before = await ScalarAsync(source, TagShape, ct);

            var updated = Parse(await DarlingMcpServerTagTools.UpdateServerTag(source, Id(created), changes, ct));
            Assert.Equal(status, (string)updated["status"]!);
            Assert.Equal("East", (string)updated["previous"]!["name"]!);
            Assert.Equal("#112233", (string)updated["previous"]!["colour"]!);
            Assert.Equal(Id(root), (int)updated["previous"]!["parent_id"]!);
            Assert.Equal(status == "updated", before != await ScalarAsync(source, TagShape, ct));

            await ReplayAsync(source, updated["undo"], "update_server_tag", ct);
            Assert.Equal(before, await ScalarAsync(source, TagShape, ct));
        });
    }

    [Fact]
    public async Task AMoveOfAServerTag_AnswersWithTheOldOrder_AndAnUndoThatMovesItBack()
    {
        await WithStoreAsync(async (source, ct) =>
        {
            var a = Id(Parse(await DarlingMcpServerTagTools.CreateServerTag(source, "A", null, null, ct)));
            var b = Id(Parse(await DarlingMcpServerTagTools.CreateServerTag(source, "B", null, null, ct)));
            var c = Id(Parse(await DarlingMcpServerTagTools.CreateServerTag(source, "C", null, null, ct)));

            var moved = Parse(await DarlingMcpServerTagTools.UpdateServerTag(source, c, "{\"move\":\"up\"}", ct));
            Assert.Equal("updated", (string)moved["status"]!);
            Assert.Equal(new[] { a, b, c }, moved["previous"]!["order"]!.AsArray().Select(n => (int)n!).ToArray());
            Assert.Equal(new[] { a, c, b }, moved["order"]!.AsArray().Select(n => (int)n!).ToArray());

            var undone = await ReplayAsync(source, moved["undo"], "update_server_tag", ct);
            Assert.Equal(new[] { a, b, c }, undone["order"]!.AsArray().Select(n => (int)n!).ToArray());

            /* A tag already at the edge writes nothing, so there is nothing to undo. */
            var edge = Parse(await DarlingMcpServerTagTools.UpdateServerTag(source, a, "{\"move\":\"up\"}", ct));
            Assert.Equal("unchanged", (string)edge["status"]!);
            Assert.StartsWith("None needed", (string)edge["undo"]!, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task DeleteServerTag_OfALeaf_AnswersWithTheTag_AndAnUndoThatCreatesItAgain()
    {
        await WithStoreAsync(async (source, ct) =>
        {
            var root = Parse(await DarlingMcpServerTagTools.CreateServerTag(source, "Root", null, null, ct));
            var leaf = Parse(await DarlingMcpServerTagTools.CreateServerTag(source, "Leaf", Id(root), "#abcdef", ct));
            var before = await ScalarAsync(source, TagShape, ct);

            var deleted = Parse(await DarlingMcpServerTagTools.DeleteServerTag(source, Id(leaf), false, ct));
            Assert.Equal("deleted", (string)deleted["status"]!);
            Assert.Equal("Leaf", (string)deleted["previous"]!["name"]!);
            Assert.NotEqual(before, await ScalarAsync(source, TagShape, ct));

            var recreated = await ReplayAsync(source, deleted["undo"], "create_server_tag", ct);
            Assert.Equal("created", (string)recreated["status"]!);
            Assert.Equal(before, await ScalarAsync(source, TagShape, ct));
        });
    }

    [Fact]
    public async Task DeleteServerTag_WithChildrenOrAssignments_AnswersWithNoUndo_AndANoteSayingWhy()
    {
        await WithStoreAsync(async (source, ct) =>
        {
            var parent = Id(Parse(await DarlingMcpServerTagTools.CreateServerTag(source, "Parent", null, null, ct)));
            await DarlingMcpServerTagTools.CreateServerTag(source, "Child", parent, null, ct);
            var held = Id(Parse(await DarlingMcpServerTagTools.CreateServerTag(source, "Held", null, null, ct)));
            await DarlingMcpServerTagTools.AssignServerTag(source, held, new[] { 7801 }, ct);

            var withChild = Parse(await DarlingMcpServerTagTools.DeleteServerTag(source, parent, false, ct));
            Assert.Equal("deleted", (string)withChild["status"]!);
            Assert.Null(withChild["undo"]);
            Assert.Contains("No single call", (string)withChild["undo_note"]!, StringComparison.Ordinal);
            Assert.Equal("Parent", (string)withChild["previous"]!["name"]!);

            var withAssignment = Parse(await DarlingMcpServerTagTools.DeleteServerTag(source, held, false, ct));
            Assert.Equal("deleted", (string)withAssignment["status"]!);
            Assert.Equal(1, (int)withAssignment["removed_assignments"]!);
            Assert.Null(withAssignment["undo"]);
            Assert.Contains("assign_server_tag", (string)withAssignment["undo_note"]!, StringComparison.Ordinal);
            Assert.Equal("", await ScalarAsync(source, TagShape, ct));
        });
    }

    [Fact]
    public async Task AssignServerTag_AnswersWithAnUndoThatUnassignsOnlyWhatItAdded()
    {
        await WithStoreAsync(async (source, ct) =>
        {
            var tag = Id(Parse(await DarlingMcpServerTagTools.CreateServerTag(source, "Fleet", null, null, ct)));
            await DarlingMcpServerTagTools.AssignServerTag(source, tag, new[] { 7801 }, ct);
            Assert.Equal("Fleet:7801", await ScalarAsync(source, MapShape, ct));

            var assigned = Parse(await DarlingMcpServerTagTools.AssignServerTag(source, tag, new[] { 7801, 7802 }, ct));
            Assert.Equal("assigned", (string)assigned["status"]!);
            Assert.Equal(new[] { 7801 }, assigned["previous"]!["assigned_server_ids"]!.AsArray().Select(n => (int)n!).ToArray());
            Assert.Equal("Fleet:7801,Fleet:7802", await ScalarAsync(source, MapShape, ct));

            await ReplayAsync(source, assigned["undo"], "unassign_server_tag", ct);
            Assert.Equal("Fleet:7801", await ScalarAsync(source, MapShape, ct));

            var again = Parse(await DarlingMcpServerTagTools.AssignServerTag(source, tag, new[] { 7801 }, ct));
            Assert.Equal("unchanged", (string)again["status"]!);
            Assert.StartsWith("None needed", (string)again["undo"]!, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task UnassignServerTag_AnswersWithAnUndoThatAssignsThemAgain()
    {
        await WithStoreAsync(async (source, ct) =>
        {
            var tag = Id(Parse(await DarlingMcpServerTagTools.CreateServerTag(source, "Fleet", null, null, ct)));
            await DarlingMcpServerTagTools.AssignServerTag(source, tag, new[] { 7801, 7802 }, ct);
            var before = await ScalarAsync(source, MapShape, ct);

            var removed = Parse(await DarlingMcpServerTagTools.UnassignServerTag(source, tag, new[] { 7801 }, ct));
            Assert.Equal("unassigned", (string)removed["status"]!);
            Assert.Equal(new[] { 7801 }, removed["previous"]!["assigned_server_ids"]!.AsArray().Select(n => (int)n!).ToArray());
            Assert.Equal("Fleet:7802", await ScalarAsync(source, MapShape, ct));

            await ReplayAsync(source, removed["undo"], "assign_server_tag", ct);
            Assert.Equal(before, await ScalarAsync(source, MapShape, ct));

            var none = Parse(await DarlingMcpServerTagTools.UnassignServerTag(source, tag, new[] { 9999 }, ct));
            Assert.Equal("unchanged", (string)none["status"]!);
            Assert.StartsWith("None needed", (string)none["undo"]!, StringComparison.Ordinal);
        });
    }
}
