/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the
   shared one, so it is not serialized against the live-postgres collection. It seeds fixed tags, servers and
   rules and reads no wall clock. */

/// <summary>The fleet-tag store against a real Postgres (#5085): the colour stamp, the lock, the 23505 backstop,
/// the delete cascade, the rule read and the assignment RETURNING.</summary>
public sealed class ServerTagStoreLiveTests
{
    private async Task<(ScratchPostgres Scratch, NpgsqlDataSource Source, ServerTagStore Store)?> OpenAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live server-tag store test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        var source = NpgsqlDataSource.Create(connectionString);
        return (scratch, source, new ServerTagStore(source, 30));
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task CreateStampsThePaletteColour_AndTwoConcurrentSameNameCreatesGiveOneOkAndOneConflict()
    {
        var opened = await OpenAsync();
        var (scratch, source, store) = opened!.Value;
        await using var _ = scratch;
        await using var __ = source;
        var ct = TestContext.Current.CancellationToken;

        var created = Assert.IsType<ServerTagWriteResult.Ok>(await store.CreateAsync("Prod", null, null, ct));
        Assert.Equal(TagColours.ForTagId(created.Tag!.Id), created.Tag.Colour);

        var results = await Task.WhenAll(
            store.CreateAsync("Twin", null, null, ct),
            store.CreateAsync("TWIN", null, null, ct));
        Assert.Equal(1, results.Count(r => r is ServerTagWriteResult.Ok));
        var conflict = Assert.Single(results.OfType<ServerTagWriteResult.Conflict>());
        /* The lock makes the loser see the winner's committed row, so the in-memory duplicate check answers with
           the precise root message; without it the loser would only be caught by the unique index. */
        Assert.Equal("A tag named 'Twin' already exists at the root.", conflict.Message.Replace("'TWIN'", "'Twin'"));
    }

    [Fact]
    public async Task ARawDuplicateThatBypassedTheLock_SurfacesAsConflict_OnRename()
    {
        var opened = await OpenAsync();
        var (scratch, source, store) = opened!.Value;
        await using var _ = scratch;
        await using var __ = source;
        var ct = TestContext.Current.CancellationToken;

        var a = Assert.IsType<ServerTagWriteResult.Ok>(await store.CreateAsync("Alpha", null, null, ct));
        Assert.IsType<ServerTagWriteResult.Ok>(await store.CreateAsync("Beta", null, null, ct));

        /* The in-memory check compares with ToLowerInvariant; a name that differs only by a character the two
           folds treat differently would reach the index. Simulate a writer that skipped the check by renaming
           straight onto the sibling's name through the unique index. */
        var edit = new ServerTagEdit(true, "Beta", false, null, false, null);
        Assert.IsType<ServerTagWriteResult.Conflict>(await store.UpdateAsync(a.Tag!.Id, edit, ct));
        await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(source, "UPDATE server_tags SET name = 'Beta' WHERE name = 'Alpha'"));
    }

    [Fact]
    public async Task Delete_CascadesTheSubtree_AndReportsTheIdsAndAssignments()
    {
        var opened = await OpenAsync();
        var (scratch, source, store) = opened!.Value;
        await using var _ = scratch;
        await using var __ = source;
        var ct = TestContext.Current.CancellationToken;

        var root = Assert.IsType<ServerTagWriteResult.Ok>(await store.CreateAsync("Prod", null, null, ct)).Tag!;
        var child = Assert.IsType<ServerTagWriteResult.Ok>(await store.CreateAsync("East", root.Id, null, ct)).Tag!;
        await store.AssignAsync(root.Id, [9001], ct);
        await store.AssignAsync(child.Id, [9001, 9002], ct);

        var deleted = Assert.IsType<ServerTagWriteResult.Ok>(await store.DeleteAsync(root.Id, confirm: true, ct));
        Assert.Equal(3, deleted.RemovedAssignments);
        Assert.Equal(new[] { root.Id, child.Id }.OrderBy(i => i), deleted.RemovedTagIds!);

        var snapshot = await store.ReadSnapshotAsync(ct);
        Assert.Empty(snapshot.Tags);
        Assert.Empty(snapshot.Assignments);
        Assert.IsType<ServerTagWriteResult.NotFound>(await store.DeleteAsync(root.Id, confirm: true, ct));
    }

    [Fact]
    public async Task ACreateColourWithSpaces_IsStoredTrimmedAndUpperCased()
    {
        var opened = await OpenAsync();
        var (scratch, source, store) = opened!.Value;
        await using var _ = scratch;
        await using var __ = source;
        var ct = TestContext.Current.CancellationToken;

        var created = Assert.IsType<ServerTagWriteResult.Ok>(await store.CreateAsync("Padded", null, " #416fa6 ", ct));
        Assert.Equal("#416FA6", created.Tag!.Colour);
    }

    [Fact]
    public async Task AnUnconfirmedDelete_ReReadsTheRulesUnderTheLock_AndDeletesNothing()
    {
        var opened = await OpenAsync();
        var (scratch, source, store) = opened!.Value;
        await using var _ = scratch;
        await using var __ = source;
        var ct = TestContext.Current.CancellationToken;

        var root = Assert.IsType<ServerTagWriteResult.Ok>(await store.CreateAsync("Prod", null, null, ct)).Tag!;
        var child = Assert.IsType<ServerTagWriteResult.Ok>(await store.CreateAsync("East", root.Id, null, ct)).Tag!;
        await store.AssignAsync(child.Id, [9001], ct);

        /* A rule scoped into the subtree after the caller's own pre-check: the store call is made directly, so
           only the check under the delete lock can stop it. */
        await ExecAsync(source, $@"INSERT INTO custom_alert_rules (name, definition, enabled, version, created_at, updated_at, updated_by) VALUES
            ('late-rule', '{{""scope"":{{""mode"":""tag"",""tagId"":{child.Id}}}}}', true, 1, now(), now(), 'test')");

        var refused = Assert.IsType<ServerTagWriteResult.ConfirmRequired>(await store.DeleteAsync(root.Id, confirm: false, ct));
        Assert.Equal("late-rule", Assert.Single(refused.Rules).Name);
        Assert.Contains("'late-rule'", refused.Message, StringComparison.Ordinal);

        var snapshot = await store.ReadSnapshotAsync(ct);
        Assert.Equal(2, snapshot.Tags.Count);
        Assert.Single(snapshot.Assignments);

        Assert.IsType<ServerTagWriteResult.Ok>(await store.DeleteAsync(root.Id, confirm: true, ct));
        Assert.Empty((await store.ReadSnapshotAsync(ct)).Tags);
    }

    [Fact]
    public async Task AnAssignToATagDeletedInTheMeantime_IsReportedGone_NotAGenericError()
    {
        var opened = await OpenAsync();
        var (scratch, source, store) = opened!.Value;
        await using var _ = scratch;
        await using var __ = source;
        var ct = TestContext.Current.CancellationToken;

        var tag = Assert.IsType<ServerTagWriteResult.Ok>(await store.CreateAsync("Doomed", null, null, ct)).Tag!;
        Assert.IsType<ServerTagWriteResult.Ok>(await store.DeleteAsync(tag.Id, confirm: true, ct));

        /* The core's pre-read is bypassed: the store is asked to assign to a tag id that is already gone, which
           is what an assign racing a delete sees (a foreign-key violation). */
        var gone = await Assert.ThrowsAsync<ServerTagGoneException>(() => store.AssignAsync(tag.Id, [9001], ct));
        Assert.Equal(tag.Id, gone.TagId);

        /* And through the core, the same race (the snapshot still lists the tag) answers not_found. */
        var racing = new RacingAssignStore(store, tag);
        var json = await DarlingMcpServerTagTools.AssignServerTagCore(racing, tag.Id, [9001], ct);
        Assert.Contains("\"status\":\"not_found\"", json.Replace(" ", string.Empty), StringComparison.Ordinal);
    }

    /// <summary>A store whose snapshot still lists a tag that is gone and that registers every server: the state
    /// an assign sees when a delete lands between its pre-read and its insert.</summary>
    private sealed class RacingAssignStore(ServerTagStore inner, ServerTagRow stale) : IServerTagStore
    {
        public async Task<ServerTagSnapshot> ReadSnapshotAsync(System.Threading.CancellationToken ct = default)
        {
            var real = await inner.ReadSnapshotAsync(ct);
            return real with { Tags = real.Tags.Append(stale).ToList() };
        }

        public Task<System.Collections.Generic.IReadOnlySet<int>> FindRegisteredServerIdsAsync(System.Collections.Generic.IReadOnlyCollection<int> serverIds, System.Threading.CancellationToken ct = default) =>
            Task.FromResult<System.Collections.Generic.IReadOnlySet<int>>(serverIds.ToHashSet());

        public Task<ServerTagWriteResult> CreateAsync(string name, int? parentId, string? colour, System.Threading.CancellationToken ct = default) => inner.CreateAsync(name, parentId, colour, ct);

        public Task<ServerTagWriteResult> UpdateAsync(int tagId, ServerTagEdit edit, System.Threading.CancellationToken ct = default) => inner.UpdateAsync(tagId, edit, ct);

        public Task<ServerTagWriteResult> DeleteAsync(int tagId, bool confirm, System.Threading.CancellationToken ct = default) => inner.DeleteAsync(tagId, confirm, ct);

        public Task<System.Collections.Generic.IReadOnlyList<int>> AssignAsync(int tagId, System.Collections.Generic.IReadOnlyList<int> serverIds, System.Threading.CancellationToken ct = default) => inner.AssignAsync(tagId, serverIds, ct);

        public Task<System.Collections.Generic.IReadOnlyList<int>> UnassignAsync(int tagId, System.Collections.Generic.IReadOnlyList<int> serverIds, System.Threading.CancellationToken ct = default) => inner.UnassignAsync(tagId, serverIds, ct);

        public Task<bool> HasChildrenAsync(int tagId, System.Threading.CancellationToken ct = default) => inner.HasChildrenAsync(tagId, ct);

        public Task ClearForServerAsync(int serverId, System.Threading.CancellationToken ct = default) => inner.ClearForServerAsync(serverId, ct);
    }

    [Fact]
    public async Task TagScopedRules_AreReadSkippingBadIds_AndRegisteredServersFilter()
    {
        var opened = await OpenAsync();
        var (scratch, source, store) = opened!.Value;
        await using var _ = scratch;
        await using var __ = source;
        var ct = TestContext.Current.CancellationToken;

        await ExecAsync(source, @"INSERT INTO custom_alert_rules (name, definition, enabled, version, created_at, updated_at, updated_by) VALUES
            ('tag-rule', '{""scope"":{""mode"":""tag"",""tagId"":7}}', true, 1, now(), now(), 'test'),
            ('bad-rule', '{""scope"":{""mode"":""tag"",""tagId"":""x""}}', true, 1, now(), now(), 'test'),
            ('fleet-rule', '{""scope"":{""mode"":""fleet""}}', true, 1, now(), now(), 'test')");

        var snapshot = await store.ReadSnapshotAsync(ct);
        var rule = Assert.Single(snapshot.Rules);
        Assert.Equal("tag-rule", rule.Name);
        Assert.Equal(7, rule.ScopeTagId);

        var found = await store.FindRegisteredServerIdsAsync([1, 2, 3], ct);
        Assert.Empty(found);
    }

    /// <summary>The five tool cores against a real <see cref="ServerTagStore"/> (the product path, no fake): the
    /// depth and cycle refusals, then the delete that needs confirm because a tag-scoped rule points at the
    /// subtree, then the confirmed delete that leaves the rule orphaned.</summary>
    [Fact]
    public async Task ServerTagToolCores_OverTheRealStore_RefuseDepthAndCycle_AndGateTheDeleteOnTheRules()
    {
        var opened = await OpenAsync();
        var (scratch, source, store) = opened!.Value;
        await using var _ = scratch;
        await using var __ = source;
        var ct = TestContext.Current.CancellationToken;

        static System.Text.Json.Nodes.JsonObject Parse(string json) => (System.Text.Json.Nodes.JsonObject)System.Text.Json.Nodes.JsonNode.Parse(json)!;

        var ids = new System.Collections.Generic.List<int>();
        int? parent = null;
        foreach (var name in new[] { "Level1", "Level2", "Level3", "Level4" })
        {
            var made = Parse(await DarlingMcpServerTagTools.CreateServerTagCore(store, name, parent, null, ct));
            Assert.Equal("created", (string?)made["status"]);
            parent = (int)made["tag"]!["tag_id"]!;
            ids.Add(parent.Value);
        }

        var tooDeep = Parse(await DarlingMcpServerTagTools.CreateServerTagCore(store, "Level5", ids[3], null, ct));
        Assert.Equal("depth_limit", (string?)tooDeep["refusal"]);

        var cycle = Parse(await DarlingMcpServerTagTools.UpdateServerTagCore(store, ids[0], $"{{\"parent_id\":{ids[2]}}}", ct));
        Assert.Equal("cycle", (string?)cycle["refusal"]);

        await ExecAsync(source, "INSERT INTO custom_alert_rules (name, definition, enabled, version, created_at, updated_at, updated_by) VALUES "
            + $"('tag-rule', '{{\"scope\":{{\"mode\":\"tag\",\"tagId\":{ids[2]}}}}}', true, 1, now(), now(), 'test')");

        var needsConfirm = Parse(await DarlingMcpServerTagTools.DeleteServerTagCore(store, ids[1], false, ct));
        Assert.Equal("confirm_required", (string?)needsConfirm["status"]);
        Assert.Equal("tag-rule", (string?)needsConfirm["affected_rules"]![0]!["name"]);
        Assert.Equal(4, (await store.ReadTagsAsync(ct)).Count);

        var deleted = Parse(await DarlingMcpServerTagTools.DeleteServerTagCore(store, ids[1], true, ct));
        Assert.Equal("deleted", (string?)deleted["status"]);
        Assert.Equal("orphaned", (string?)deleted["affected_rules"]![0]!["effect"]);
        Assert.Single(await store.ReadTagsAsync(ct));
    }

    [Fact]
    public async Task AssignAndUnassign_ReturnExactlyTheChangedIds()
    {
        var opened = await OpenAsync();
        var (scratch, source, store) = opened!.Value;
        await using var _ = scratch;
        await using var __ = source;
        var ct = TestContext.Current.CancellationToken;

        var tag = Assert.IsType<ServerTagWriteResult.Ok>(await store.CreateAsync("Prod", null, null, ct)).Tag!;
        Assert.Equal([1, 2], await store.AssignAsync(tag.Id, [1, 2], ct));
        Assert.Equal([3], await store.AssignAsync(tag.Id, [2, 3], ct));
        Assert.Equal([1, 3], await store.UnassignAsync(tag.Id, [1, 3, 4], ct));
        Assert.True(await store.HasChildrenAsync(tag.Id, ct) == false);
    }
}
