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
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #9642: the MCP tools that manage web access groups and the advanced web access switch run the Viewer's own write code
/// (<see cref="WebAccessGroupAdmin"/>) on a live store. A create and a rotate answer with the token once, sealed to the service's key
/// for the group and generation, and the token is in no log line and no journal row.
/// </summary>
public sealed class DarlingMcpWebAccessToolsLiveTests
{
    private const string Actor = "mcp:test-caller";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AGroupLifecycle_RunsThroughTheTools_SealingEachTokenToItsGeneration_AndJournalingTheCaller()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var ring = TestKeyRings.Healthy;
            var store = new WebAccessGroupStore(owner);

            var empty = JsonNode.Parse(await DarlingMcpWebAccessTools.ListAsync(owner, ct))!;
            Assert.Equal(0, (int)empty["groups_returned"]!);
            Assert.False((bool)empty["advanced_web_access_enabled"]!);

            var on = JsonNode.Parse(await DarlingMcpWebAccessTools.SetAdvancedAsync(owner, true, Actor, null, ct))!;
            Assert.Equal("set", (string)on["status"]!);
            Assert.NotNull(await store.ReadAdvancedSessionEpochAsync(ct));
            Assert.Equal("unchanged", (string)JsonNode.Parse(await DarlingMcpWebAccessTools.SetAdvancedAsync(owner, true, Actor, null, ct))!["status"]!);

            var created = JsonNode.Parse(await DarlingMcpWebAccessTools.CreateAsync(owner, ring, "  Support  ", "all_servers", null, Actor, null, ct))!;
            Assert.Equal("created", (string)created["status"]!);
            var id = Guid.Parse((string)created["group"]!["group_id"]!);
            Assert.Equal("Support", (string)created["group"]!["name"]!);
            var firstToken = (string)created["token"]!;
            Assert.True((bool)created["advanced_web_access_enabled"]!);

            /* The stored copy opens to the token the answer gave, bound to this group and its generation. */
            var credential = (await store.ReadCredentialsAsync(ct)).Single(c => c.Group.GroupId == id);
            Assert.Equal(firstToken, ring.Open(credential.SealedToken, PasswordBinding.ForWebAccessToken(id, credential.Group.TokenGeneration)));

            var tag = (int)(await ScalarAsync(owner, "INSERT INTO config.server_tags(name) VALUES ('web-tag') RETURNING id", ct))!;
            var updated = JsonNode.Parse(await DarlingMcpWebAccessTools.UpdateAsync(owner, "Support", "tags", [tag], false, Actor, null, ct))!;
            Assert.Equal("updated", (string)updated["status"]!);
            var group = (await store.ListAsync(ct)).Single();
            Assert.Equal(WebAccessGroupScope.Tags, group.Scope);
            Assert.Equal([tag], group.TagIds);
            Assert.False(group.Enabled);

            var refusedRotate = JsonNode.Parse(await DarlingMcpWebAccessTools.RotateAsync(owner, ring, id.ToString(), false, Actor, null, ct))!;
            Assert.Equal("confirm_required", (string)refusedRotate["code"]!);
            Assert.Null(refusedRotate["token"]);
            Assert.Equal(group.TokenGeneration, (await store.ReadAsync(id, ct))!.TokenGeneration);

            var rotated = JsonNode.Parse(await DarlingMcpWebAccessTools.RotateAsync(owner, ring, id.ToString(), true, Actor, null, ct))!;
            var secondToken = (string)rotated["token"]!;
            Assert.NotEqual(firstToken, secondToken);
            var after = (await store.ReadCredentialsAsync(ct)).Single(c => c.Group.GroupId == id);
            Assert.NotEqual(group.TokenGeneration, after.Group.TokenGeneration);
            Assert.Equal(secondToken, ring.Open(after.SealedToken, PasswordBinding.ForWebAccessToken(id, after.Group.TokenGeneration)));

            var refusedDelete = JsonNode.Parse(await DarlingMcpWebAccessTools.DeleteAsync(owner, "Support", false, Actor, null, ct))!;
            Assert.Equal("confirm_required", (string)refusedDelete["code"]!);
            Assert.Single(await store.ListAsync(ct));
            var deleted = JsonNode.Parse(await DarlingMcpWebAccessTools.DeleteAsync(owner, "Support", true, Actor, null, ct))!;
            Assert.Equal("deleted", (string)deleted["status"]!);
            Assert.Empty(await store.ListAsync(ct));

            Assert.Equal(Actor, await ScalarAsync(owner, "SELECT actor FROM config.web_access_group_events WHERE event = 'created'", ct));
            Assert.Equal("rotated", await ScalarAsync(owner, "SELECT event FROM config.web_access_group_events WHERE event = 'rotated'", ct));
        });
    }

    [Fact]
    public async Task ACreateAndARotate_KeepTheTokenOutOfEveryLogLine_AndEveryJournalRow()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var logger = new CapturingTestLogger();
            var created = JsonNode.Parse(await DarlingMcpWebAccessTools.CreateAsync(owner, TestKeyRings.Healthy, "Ops", "all_servers", null, Actor, logger, ct))!;
            var id = (string)created["group"]!["group_id"]!;
            var rotated = JsonNode.Parse(await DarlingMcpWebAccessTools.RotateAsync(owner, TestKeyRings.Healthy, id, true, Actor, logger, ct))!;
            var tokens = new[] { (string)created["token"]!, (string)rotated["token"]! };
            Assert.All(tokens, t => Assert.True(t.Length >= 32));

            Assert.True(logger.CountAtLevel(LogLevel.Warning) >= 2);
            var journal = (string)(await ScalarAsync(owner, "SELECT coalesce(string_agg(e::text, ' '), '') FROM config.web_access_group_events e", ct))!;
            foreach (var token in tokens)
            {
                Assert.DoesNotContain(token, logger.Joined, StringComparison.Ordinal);
                Assert.DoesNotContain(token, journal, StringComparison.Ordinal);
            }
        });
    }

    [Fact]
    public async Task BadInput_IsRefusedOrReportedWithoutDatabaseDetail_AndWritesNothing()
    {
        await WithStoreAsync(async (owner, ct) =>
        {
            var ring = TestKeyRings.Healthy;
            var store = new WebAccessGroupStore(owner);

            Assert.Equal("invalid", Code(await DarlingMcpWebAccessTools.CreateAsync(owner, ring, "x", "everyone", null, Actor, null, ct)));
            Assert.Equal("invalid", Code(await DarlingMcpWebAccessTools.CreateAsync(owner, ring, "x", "tags", null, Actor, null, ct)));
            Assert.Equal("invalid", Code(await DarlingMcpWebAccessTools.CreateAsync(owner, ring, "x", "all_servers", [1], Actor, null, ct)));
            Assert.Equal("invalid", Code(await DarlingMcpWebAccessTools.CreateAsync(owner, ring, " ", "all_servers", null, Actor, null, ct)));
            Assert.Equal("key_unavailable", Code(await DarlingMcpWebAccessTools.CreateAsync(owner, TestKeyRings.NotReady, "x", "all_servers", null, Actor, null, ct)));
            Assert.Equal("key_unavailable", Code(await DarlingMcpWebAccessTools.CreateAsync(owner, null, "x", "all_servers", null, Actor, null, ct)));
            Assert.Equal("no_owner_source", Code(await DarlingMcpWebAccessTools.CreateAsync(null, ring, "x", "all_servers", null, Actor, null, ct)));
            Assert.Equal("not_found", Code(await DarlingMcpWebAccessTools.DeleteAsync(owner, "nobody", true, Actor, null, ct)));
            Assert.Empty(await store.ListAsync(ct));

            /* A tag the store does not know is the store's own refusal, reported in the Viewer's words and with no SQL state. */
            var unknownTag = await DarlingMcpWebAccessTools.CreateAsync(owner, ring, "x", "tags", [987654], Actor, null, ct);
            Assert.Contains("Check the group name and selected tags", unknownTag, StringComparison.Ordinal);
            Assert.DoesNotContain("WG400", unknownTag, StringComparison.Ordinal);
            Assert.Empty(await store.ListAsync(ct));

            await DarlingMcpWebAccessTools.CreateAsync(owner, ring, "dup", "all_servers", null, Actor, null, ct);
            var duplicate = await DarlingMcpWebAccessTools.CreateAsync(owner, ring, "dup", "all_servers", null, Actor, null, ct);
            Assert.Contains("A group with this name already exists", duplicate, StringComparison.Ordinal);
            Assert.Single(await store.ListAsync(ct));
        });
    }

    private static string? Code(string answer) => (string?)JsonNode.Parse(answer)!["code"];

    private static async Task<object?> ScalarAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task WithStoreAsync(Func<NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the MCP web access live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await body(owner, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
