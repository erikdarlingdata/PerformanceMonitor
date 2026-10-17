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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Common.Tags;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Behaviour pins for the Lite query-tag store (#5989) against a real DuckDB: the shared refusal codes, the subtree
/// delete, the rules_version bump, one assignment per key, and the server clear.
/// </summary>
public sealed class LiteQueryTagStoreTests : IClassFixture<SharedDuckDbFixture>
{
    private readonly LocalDataService _service;
    private readonly SharedDuckDbFixture _fixture;

    public LiteQueryTagStoreTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _fixture = fixture;
        _service = new LocalDataService(fixture.DuckDb);
    }

    private static async Task<string?> CodeOfAsync(Func<Task> action)
    {
        var ex = await Assert.ThrowsAsync<QueryTagRefusedException>(action);
        return ex.Code;
    }

    private async Task<long> CountAsync(string sql)
    {
        using var connection = _fixture.DuckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Create_AssignsAscendingIds_AColourFromTheId_AndKeepsAGivenColour()
    {
        var a = await _service.CreateQueryTagAsync("  Reporting ", null);
        var b = await _service.CreateQueryTagAsync("Billing", null, "#416fa6");

        Assert.Equal(1, a);
        Assert.Equal(2, b);
        var tags = await _service.GetQueryTagsAsync();
        Assert.Equal("Reporting", tags.Single(t => t.Id == a).Name);
        Assert.Equal(TagColours.ForTagId(a), tags.Single(t => t.Id == a).Colour);
        Assert.Equal("#416FA6", tags.Single(t => t.Id == b).Colour);
    }

    [Fact]
    public async Task Create_RefusesBadNameBadColourDuplicateAndUnknownParent_WithTheSharedCodes()
    {
        var root = await _service.CreateQueryTagAsync("Root", null);

        Assert.Equal(TagTreeRules.BadName, await CodeOfAsync(() => _service.CreateQueryTagAsync("   ", null)));
        Assert.Equal(TagTreeRules.BadName, await CodeOfAsync(() => _service.CreateQueryTagAsync(new string('x', 101), null)));
        Assert.Equal(TagTreeRules.BadColour, await CodeOfAsync(() => _service.CreateQueryTagAsync("Ok", null, "red")));
        Assert.Equal(TagTreeRules.DuplicateName, await CodeOfAsync(() => _service.CreateQueryTagAsync("ROOT", null)));
        Assert.Equal(TagTreeRules.UnknownParent, await CodeOfAsync(() => _service.CreateQueryTagAsync("Child", 999)));

        /* The same name under another parent is allowed (the resolver, not the store, treats it as ambiguous). */
        var other = await _service.CreateQueryTagAsync("Other", null);
        await _service.CreateQueryTagAsync("Root", other);
        Assert.Equal(3, (await _service.GetQueryTagsAsync()).Count);
    }

    [Fact]
    public async Task Create_RefusesTheDepthLimit_AndUpdateRefusesACycle_WithTheSharedCodes()
    {
        var parent = (int?)null;
        var ids = new System.Collections.Generic.List<int>();
        for (var i = 0; i < TagTreeRules.MaxDepth; i++)
        {
            parent = await _service.CreateQueryTagAsync("L" + i, parent);
            ids.Add(parent.Value);
        }

        Assert.Equal(TagTreeRules.DepthLimit, await CodeOfAsync(() => _service.CreateQueryTagAsync("TooDeep", ids[^1])));

        Assert.Equal(TagTreeRules.Cycle, await CodeOfAsync(() =>
            _service.UpdateQueryTagAsync(ids[0], new TagTreeEdit(false, null, false, null, true, ids[1]))));
        Assert.Equal(TagTreeRules.Cycle, await CodeOfAsync(() =>
            _service.UpdateQueryTagAsync(ids[0], new TagTreeEdit(false, null, false, null, true, ids[0]))));
    }

    [Fact]
    public async Task Update_RenamesRecoloursAndMoves_AndRefusesADuplicateOnTheMove()
    {
        var a = await _service.CreateQueryTagAsync("A", null);
        var b = await _service.CreateQueryTagAsync("B", null);
        var child = await _service.CreateQueryTagAsync("Same", a);
        await _service.CreateQueryTagAsync("Same", b);

        Assert.Equal(TagTreeRules.DuplicateName, await CodeOfAsync(() =>
            _service.UpdateQueryTagAsync(child, new TagTreeEdit(false, null, false, null, true, b))));

        await _service.UpdateQueryTagAsync(child, new TagTreeEdit(true, "Renamed", true, "#112233", true, b));
        var row = (await _service.GetQueryTagsAsync()).Single(t => t.Id == child);
        Assert.Equal(("Renamed", "#112233", (int?)b), (row.Name, row.Colour, row.ParentId));
    }

    [Fact]
    public async Task DeleteTag_DeletesItsSubtree_AssignmentsRulesAndMapRows_AndNothingElse()
    {
        var root = await _service.CreateQueryTagAsync("Root", null);
        var child = await _service.CreateQueryTagAsync("Child", root);
        var keep = await _service.CreateQueryTagAsync("Keep", null);

        await _service.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(root, QueryTagKeyKind.Hash, QueryHash: "0xAA"));
        await _service.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(child, QueryTagKeyKind.Object, DatabaseName: "d", SchemaName: "s", ObjectName: "o"));
        var keptAssignment = await _service.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(keep, QueryTagKeyKind.Hash, QueryHash: "0xBB"));
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Login, "app%", child));
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.TextTag, null, null));
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Host, "h", keep));
        await ExecAsync($"INSERT INTO query_tag_map (server_id, key_kind, key, tag_id, source) VALUES (1, 'hash', '0xAA', {child}, 'hand_hash'), (1, 'hash', '0xBB', {keep}, 'hand_hash')");

        await _service.DeleteQueryTagAsync(root);

        Assert.Equal(new[] { keep }, (await _service.GetQueryTagsAsync()).Select(t => t.Id).ToArray());
        Assert.Equal(new[] { keptAssignment }, (await _service.GetQueryTagAssignmentsAsync()).Select(a => a.Id).ToArray());
        /* The kept tag's rule and the text_tag rule (it names no tag) stay. */
        Assert.Equal(2, (await _service.GetQueryTagRulesAsync()).Count);
        Assert.Equal(1L, await CountAsync("SELECT COUNT(*) FROM query_tag_map"));
        var gone = await Assert.ThrowsAsync<QueryTagRefusedException>(() => _service.DeleteQueryTagAsync(root));
        Assert.Equal(TagRefusalKind.NotFound, gone.Kind);
    }

    [Fact]
    public async Task Assignment_IsOnePerKeyPerServer_AndRefusesAMalformedKeyOrAnUnknownTag()
    {
        var t1 = await _service.CreateQueryTagAsync("T1", null);
        var t2 = await _service.CreateQueryTagAsync("T2", null);

        await _service.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(t1, QueryTagKeyKind.Hash, ServerId: 7, QueryHash: "0x1", QueryId: 42));
        Assert.Equal(QueryTagStoreCodes.DuplicateAssignment, await CodeOfAsync(() =>
            _service.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(t2, QueryTagKeyKind.Hash, ServerId: 7, QueryHash: " 0x1 "))));
        /* Another server, and every server, are different keys. */
        await _service.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(t2, QueryTagKeyKind.Hash, ServerId: 8, QueryHash: "0x1"));
        await _service.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(t2, QueryTagKeyKind.Hash, ServerId: null, QueryHash: "0x1"));
        Assert.Equal(QueryTagStoreCodes.DuplicateAssignment, await CodeOfAsync(() =>
            _service.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(t1, QueryTagKeyKind.Hash, ServerId: null, QueryHash: "0x1"))));

        Assert.Equal(QueryTagStoreCodes.BadAssignment, await CodeOfAsync(() =>
            _service.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(t1, QueryTagKeyKind.Hash))));
        Assert.Equal(QueryTagStoreCodes.BadAssignment, await CodeOfAsync(() =>
            _service.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(t1, QueryTagKeyKind.Object, DatabaseName: "d", SchemaName: "s"))));
        Assert.Equal(QueryTagStoreCodes.BadAssignment, await CodeOfAsync(() =>
            _service.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(t1, QueryTagKeyKind.Caller, QueryHash: "x"))));
        Assert.Equal(QueryTagStoreCodes.UnknownTag, await CodeOfAsync(() =>
            _service.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(999, QueryTagKeyKind.Hash, QueryHash: "0x9"))));

        var first = (await _service.GetQueryTagAssignmentsAsync(t1)).Single();
        Assert.Equal(42L, first.QueryId);
        await _service.DeleteQueryTagAssignmentAsync(first.Id);
        Assert.Empty(await _service.GetQueryTagAssignmentsAsync(t1));
        Assert.Equal(QueryTagStoreCodes.NotFound, await CodeOfAsync(() => _service.DeleteQueryTagAssignmentAsync(first.Id)));
    }

    [Fact]
    public async Task Rules_AreCheckedByTheSharedValidator_AndRoundTrip()
    {
        var tag = await _service.CreateQueryTagAsync("T", null);

        Assert.Equal(QueryTagStoreCodes.BadRule, await CodeOfAsync(() => _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Login, " ", tag))));
        Assert.Equal(QueryTagStoreCodes.BadRule, await CodeOfAsync(() => _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Login, new string('p', QueryTagLimits.MaxPatternLength + 1), tag))));
        Assert.Equal(QueryTagStoreCodes.BadRule, await CodeOfAsync(() => _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Login, "p", null))));
        Assert.Equal(QueryTagStoreCodes.BadRule, await CodeOfAsync(() => _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.TextTag, null, tag))));
        Assert.Equal(QueryTagStoreCodes.UnknownTag, await CodeOfAsync(() => _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Login, "p", 999))));

        var late = await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Application, "etl%", tag, Priority: 200, ServerId: 5));
        var early = await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.TextTag, null, null, Priority: 50));

        var rules = await _service.GetQueryTagRulesAsync();
        Assert.Equal(new[] { early, late }, rules.Select(r => r.Id).ToArray());
        Assert.Null(rules[0].TagId);
        Assert.Equal("text_tag", rules[0].MatchField);
        Assert.Equal(("application", 5, true), (rules[1].MatchField, rules[1].ServerId, rules[1].Enabled));

        await _service.SetQueryTagRuleEnabledAsync(late, false);
        await _service.UpdateQueryTagRuleAsync(late, new QueryTagRuleInput(QueryTagRuleField.Host, "box%", tag, Priority: 10, Enabled: false));
        var changed = (await _service.GetQueryTagRulesAsync()).First();
        Assert.Equal((late, "host", "box%", 10, false, (int?)null), (changed.Id, changed.MatchField, changed.Pattern, changed.Priority, changed.Enabled, changed.ServerId));

        await _service.DeleteQueryTagRuleAsync(late);
        Assert.Equal(QueryTagStoreCodes.NotFound, await CodeOfAsync(() => _service.DeleteQueryTagRuleAsync(late)));
        Assert.Equal(QueryTagStoreCodes.NotFound, await CodeOfAsync(() => _service.UpdateQueryTagRuleAsync(late, new QueryTagRuleInput(QueryTagRuleField.Host, "x", tag))));
    }

    [Fact]
    public async Task EveryWrite_BumpsRulesVersion_AndARefusedWriteDoesNot()
    {
        var before = (await _service.GetQueryTagStateAsync()).RulesVersion;
        Assert.Equal(1, before);

        var tag = await _service.CreateQueryTagAsync("T", null);
        var assignment = await _service.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(tag, QueryTagKeyKind.Hash, QueryHash: "0x1"));
        var rule = await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Login, "p", tag));
        Assert.Equal(before + 3, (await _service.GetQueryTagStateAsync()).RulesVersion);

        await _service.UpdateQueryTagAsync(tag, new TagTreeEdit(true, "T2", false, null, false, null));
        await _service.SetQueryTagRuleEnabledAsync(rule, false);
        await _service.DeleteQueryTagRuleAsync(rule);
        await _service.DeleteQueryTagAssignmentAsync(assignment);
        await _service.DeleteQueryTagAsync(tag);
        var state = await _service.GetQueryTagStateAsync();
        Assert.Equal(before + 8, state.RulesVersion);
        Assert.Equal(0, state.BuiltVersion);
        Assert.Equal(1L, await CountAsync("SELECT COUNT(*) FROM query_tag_state"));

        await CodeOfAsync(() => _service.CreateQueryTagAsync("", null));
        Assert.Equal(before + 8, (await _service.GetQueryTagStateAsync()).RulesVersion);
    }

    [Fact]
    public async Task ClearForServer_RemovesThatServersRows_AndKeepsEveryServerRowsAndOtherServers()
    {
        var tag = await _service.CreateQueryTagAsync("T", null);
        await _service.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(tag, QueryTagKeyKind.Hash, ServerId: 1, QueryHash: "0x1"));
        await _service.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(tag, QueryTagKeyKind.Hash, ServerId: 2, QueryHash: "0x1"));
        await _service.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(tag, QueryTagKeyKind.Hash, ServerId: null, QueryHash: "0x1"));
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Login, "a", tag, ServerId: 1));
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Login, "b", tag, ServerId: 2));
        await ExecAsync("INSERT INTO query_tag_sightings (server_id, key_kind, key) VALUES (1, 'hash', 'k'), (2, 'hash', 'k')");
        await ExecAsync($"INSERT INTO query_tag_map (server_id, key_kind, key, tag_id, source) VALUES (1, 'hash', 'k', {tag}, 'rule'), (2, 'hash', 'k', {tag}, 'rule')");
        var version = (await _service.GetQueryTagStateAsync()).RulesVersion;

        await _service.ClearQueryTagsForServerAsync(1);

        var left = (await _service.GetQueryTagAssignmentsAsync()).Select(a => a.ServerId).OrderBy(x => x ?? -1).ToArray();
        Assert.Equal(new int?[] { null, 2 }, left);
        Assert.Equal(new[] { 2 }, (await _service.GetQueryTagRulesAsync()).Select(r => r.ServerId!.Value).ToArray());
        Assert.Equal(1L, await CountAsync("SELECT COUNT(*) FROM query_tag_sightings WHERE server_id = 2"));
        Assert.Equal(1L, await CountAsync("SELECT COUNT(*) FROM query_tag_sightings"));
        Assert.Equal(1L, await CountAsync("SELECT COUNT(*) FROM query_tag_map"));
        Assert.Equal(1L, await CountAsync("SELECT COUNT(*) FROM query_tags"));
        Assert.True((await _service.GetQueryTagStateAsync()).RulesVersion > version);
    }

    [Fact]
    public async Task SightingsRetention_RemovesOnlyRowsNotSeenWithinTheHorizon()
    {
        /* One clock anchors both the seed and the cutoff, so a cutoff wrong by hours or a day cannot hide (#3676). */
        var now = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc);
        var horizon = now.AddDays(-LocalDataService.QueryTagSightingRetentionDays);
        await ExecAsync($@"INSERT INTO query_tag_sightings (server_id, key_kind, key, first_seen, last_seen) VALUES
(1, 'hash', 'old', TIMESTAMP '{horizon.AddDays(-1):yyyy-MM-dd HH:mm:ss}', TIMESTAMP '{horizon.AddDays(-1):yyyy-MM-dd HH:mm:ss}'),
(1, 'hash', 'recent', TIMESTAMP '{horizon.AddDays(-1):yyyy-MM-dd HH:mm:ss}', TIMESTAMP '{horizon.AddDays(1):yyyy-MM-dd HH:mm:ss}')");
        var tag = await _service.CreateQueryTagAsync("T", null);

        Assert.Equal(1, await _service.PurgeOldQueryTagSightingsAsync(utcNow: now));

        Assert.Equal(1L, await CountAsync("SELECT COUNT(*) FROM query_tag_sightings WHERE key = 'recent'"));
        Assert.Equal(0L, await CountAsync("SELECT COUNT(*) FROM query_tag_sightings WHERE key = 'old'"));
        Assert.Equal(1L, await CountAsync("SELECT COUNT(*) FROM query_tags WHERE id = " + tag));
    }

    [Fact]
    public async Task SightingsTable_HoldsTheSharedSampleCap_AndNoMore()
    {
        await ExecAsync($"INSERT INTO query_tag_sightings (server_id, key_kind, key, text_sample) VALUES (1, 'hash', 'k', '{new string('x', QueryTagLimits.TextSampleCap)}')");
        await Assert.ThrowsAnyAsync<Exception>(() => ExecAsync(
            $"INSERT INTO query_tag_sightings (server_id, key_kind, key, text_sample) VALUES (1, 'hash', 'k2', '{new string('x', QueryTagLimits.TextSampleCap + 1)}')"));
    }

    [Fact]
    public async Task SightingsTable_KeysOnTheSevenPartIdentity_SoADifferentApplicationIsANewRow_AndNoCallerFoldsToOneRow()
    {
        /* Darling's ux_query_tag_sightings_key folds the caller fields to ''; here the fold is the column default. */
        const string upsert = @"INSERT INTO query_tag_sightings (server_id, key_kind, key, application_name) VALUES (1, 'hash', '0xAB', 'SSMS')
ON CONFLICT (server_id, key_kind, key, database_name, application_name, login_name, host_name) DO UPDATE SET seen_count = query_tag_sightings.seen_count + 1";
        await ExecAsync(upsert);
        await ExecAsync(upsert);
        await ExecAsync("INSERT INTO query_tag_sightings (server_id, key_kind, key, application_name) VALUES (1, 'hash', '0xAB', 'ETL.exe')");
        await ExecAsync("INSERT INTO query_tag_sightings (server_id, key_kind, key) VALUES (1, 'hash', '0xAB')");
        await ExecAsync(@"INSERT INTO query_tag_sightings (server_id, key_kind, key) VALUES (1, 'hash', '0xAB')
ON CONFLICT (server_id, key_kind, key, database_name, application_name, login_name, host_name) DO UPDATE SET seen_count = query_tag_sightings.seen_count + 1");

        Assert.Equal(2L, await CountAsync("SELECT seen_count FROM query_tag_sightings WHERE key = '0xAB' AND application_name = 'SSMS'"));
        Assert.Equal(1L, await CountAsync("SELECT seen_count FROM query_tag_sightings WHERE key = '0xAB' AND application_name = 'ETL.exe'"));
        Assert.Equal(2L, await CountAsync("SELECT seen_count FROM query_tag_sightings WHERE key = '0xAB' AND application_name = '' AND login_name = '' AND host_name = ''"));
        Assert.Equal(3L, await CountAsync("SELECT COUNT(*) FROM query_tag_sightings"));
        await Assert.ThrowsAnyAsync<Exception>(() => ExecAsync("INSERT INTO query_tag_sightings (server_id, key_kind, key, application_name) VALUES (1, 'hash', '0xAB', 'SSMS')"));
    }

    [Fact]
    public async Task ADismissedAlertsGate_AdvancesEvenWhenTheSightingsPurgeThrows()
    {
        var advanced = false;
        var alertsRan = false;
        await CollectionBackgroundService.RunDismissedAlertsPurgesAsync(
            () => { alertsRan = true; return Task.CompletedTask; },
            () => throw new InvalidOperationException("write lock timeout"),
            () => advanced = true,
            null);

        Assert.True(alertsRan);
        Assert.True(advanced);

        var sightingsRan = false;
        advanced = false;
        await CollectionBackgroundService.RunDismissedAlertsPurgesAsync(
            () => throw new InvalidOperationException("boom"),
            () => { sightingsRan = true; return Task.CompletedTask; },
            () => advanced = true,
            null);

        Assert.True(sightingsRan);
        Assert.False(advanced);
    }

    private async Task ExecAsync(string sql)
    {
        using var connection = _fixture.DuckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}

/// <summary>
/// Schema, upgrade and archive-reset pins for the query-tag tables (#5989): a fresh and an upgraded database both have them,
/// a second start changes nothing, a reset keeps tags, assignments and rules (not the derived tables), and a write is refused
/// while a restore is pending.
/// </summary>
[Collection("CollectionResetGate")]
public sealed class QueryTagSchemaAndResetTests : IDisposable
{
    private static readonly string[] AllTables =
        ["query_tags", "query_tag_assignments", "query_tag_rules", "query_tag_sightings", "query_tag_map", "query_tag_state"];

    private readonly string _tempDir;
    private readonly string _archiveDir;
    private readonly string _dbPath;

    public QueryTagSchemaAndResetTests()
    {
        CollectionResetGate.ResetForTests();
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
    }

    public void Dispose()
    {
        CollectionResetGate.ResetForTests();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort */ }
    }

    private static async Task<long> CountAsync(string path, string sql)
    {
        using var conn = new DuckDBConnection($"Data Source={path}");
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    private static async Task ExecAsync(string path, string sql)
    {
        using var conn = new DuckDBConnection($"Data Source={path}");
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task AssertAllTablesAsync(string path)
    {
        foreach (var table in AllTables)
        {
            Assert.Equal(1L, await CountAsync(path, $"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table}'"));
        }
    }

    [Fact]
    public async Task AFreshDatabase_HasTheTables_AndASecondStartChangesNothing()
    {
        using (var first = new DuckDbInitializer(_dbPath))
        {
            await first.InitializeAsync();
            var data = new LocalDataService(first);
            await data.CreateQueryTagAsync("Kept", null);
        }

        await AssertAllTablesAsync(_dbPath);

        using (var second = new DuckDbInitializer(_dbPath))
        {
            await second.InitializeAsync();
        }

        Assert.Equal(1L, await CountAsync(_dbPath, "SELECT COUNT(*) FROM query_tags WHERE name = 'Kept'"));
        Assert.Equal((long)DuckDbInitializer.CurrentSchemaVersion, await CountAsync(_dbPath, "SELECT MAX(version) FROM schema_version"));
    }

    [Fact]
    public async Task AV69Database_ClimbsToV70_AndGainsTheEmptyTables()
    {
        using (var first = new DuckDbInitializer(_dbPath))
        {
            await first.InitializeAsync();
        }

        foreach (var table in AllTables)
        {
            await ExecAsync(_dbPath, $"DROP TABLE {table}");
        }

        await ExecAsync(_dbPath, "DELETE FROM schema_version");
        await ExecAsync(_dbPath, "INSERT INTO schema_version (version) VALUES (69)");

        using (var upgraded = new DuckDbInitializer(_dbPath))
        {
            await upgraded.InitializeAsync();
        }

        await AssertAllTablesAsync(_dbPath);
        Assert.Equal((long)DuckDbInitializer.CurrentSchemaVersion, await CountAsync(_dbPath, "SELECT MAX(version) FROM schema_version"));
        Assert.Equal(0L, await CountAsync(_dbPath, "SELECT COUNT(*) FROM query_tags"));
    }

    [Fact]
    public async Task AnArchiveReset_KeepsTagsAssignmentsAndRules_AndRebuildsTheDerivedTables()
    {
        using var duckDb = new DuckDbInitializer(_dbPath);
        await duckDb.InitializeFromTemplateAsync();
        var data = new LocalDataService(duckDb);
        var tag = await data.CreateQueryTagAsync("Kept", null, "#112233");
        await data.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(tag, QueryTagKeyKind.Hash, ServerId: 3, QueryHash: "0xAB", QueryId: 9));
        await data.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Login, "svc%", tag, Priority: 7));
        await ExecAsync(_dbPath, "INSERT INTO query_tag_sightings (server_id, key_kind, key) VALUES (3, 'hash', '0xAB')");
        await ExecAsync(_dbPath, $"INSERT INTO query_tag_map (server_id, key_kind, key, tag_id, source) VALUES (3, 'hash', '0xAB', {tag}, 'hand_hash')");

        await new ArchiveService(duckDb, _archiveDir, NullLogger<ArchiveService>.Instance).ArchiveAllAndResetAsync();

        Assert.Equal(1L, await CountAsync(_dbPath, "SELECT COUNT(*) FROM query_tags WHERE name = 'Kept' AND colour = '#112233'"));
        Assert.Equal(1L, await CountAsync(_dbPath, "SELECT COUNT(*) FROM query_tag_assignments WHERE query_hash = '0xAB' AND server_id = 3 AND query_id = 9"));
        Assert.Equal(1L, await CountAsync(_dbPath, "SELECT COUNT(*) FROM query_tag_rules WHERE pattern = 'svc%' AND priority = 7"));
        Assert.Equal(0L, await CountAsync(_dbPath, "SELECT COUNT(*) FROM query_tag_sightings"));
        Assert.Equal(0L, await CountAsync(_dbPath, "SELECT COUNT(*) FROM query_tag_map"));
    }

    [Fact]
    public async Task WhileATagRestoreIsPending_EveryQueryTagWriteIsRefused_ThenARestartRestoresTheRows()
    {
        using var duckDb = new DuckDbInitializer(_dbPath);
        await duckDb.InitializeFromTemplateAsync();
        var data = new LocalDataService(duckDb);
        await data.CreateQueryTagAsync("Kept", null);

        ArchiveService.BeforePreservedTableRestoreForTests = t =>
        {
            if (t == "query_tags") throw new InvalidOperationException("restore failed");
        };
        try
        {
            await new ArchiveService(duckDb, _archiveDir, NullLogger<ArchiveService>.Instance).ArchiveAllAndResetAsync();
        }
        finally
        {
            ArchiveService.BeforePreservedTableRestoreForTests = null;
        }

        Assert.Equal(0L, await CountAsync(_dbPath, "SELECT COUNT(*) FROM query_tags"));
        await Assert.ThrowsAsync<PendingRestoreException>(() => data.CreateQueryTagAsync("Mine", null));
        await Assert.ThrowsAsync<PendingRestoreException>(() => data.DeleteQueryTagAsync(1));
        await Assert.ThrowsAsync<PendingRestoreException>(() => data.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(1, QueryTagKeyKind.Hash, QueryHash: "0x1")));
        await Assert.ThrowsAsync<PendingRestoreException>(() => data.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Login, "p", 1)));
        await Assert.ThrowsAsync<PendingRestoreException>(() => data.ClearQueryTagsForServerAsync(1));

        await duckDb.InitializeAsync();

        Assert.Equal(1L, await CountAsync(_dbPath, "SELECT COUNT(*) FROM query_tags WHERE name = 'Kept'"));
        Assert.Equal(2, await data.CreateQueryTagAsync("Mine", null));
    }
}
