/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Behaviour pins for the Lite query-tag sightings write and tag map (#5990) against a real DuckDB.
/// </summary>
public sealed class LiteQueryTagMapTests : IClassFixture<SharedDuckDbFixture>
{
    private const string HashA = "0x00000000000000AA";
    private const string HashB = "0x00000000000000BB";
    private readonly LocalDataService _service;
    private readonly SharedDuckDbFixture _fixture;

    public LiteQueryTagMapTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _fixture = fixture;
        _service = new LocalDataService(fixture.DuckDb);
    }

    private static QuerySnapshotsCollector.Row Snapshot(string? hash, string text = "SELECT 1", string app = "Reports", string db = "Sales") => new()
    {
        QueryHash = hash,
        QueryText = text,
        DatabaseName = db,
        ProgramName = app,
        LoginName = "svc",
        HostName = "web1",
    };

    private void Sight(params QuerySnapshotsCollector.Row[] rows)
    {
        var buffer = new QueryTagSightingBuffer();
        buffer.Append(QuerySnapshotsCollector.Instance, rows.ToList(), serverId: 7);
        buffer.Flush(_fixture.DuckDb);
    }

    private async Task<List<(string Kind, string Key, int TagId, string Source)>> MapAsync()
    {
        using var connection = _fixture.DuckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT key_kind, key, tag_id, source FROM query_tag_map WHERE server_id = 7 ORDER BY key";
        var rows = new List<(string, string, int, string)>();
        using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            rows.Add((reader.GetString(0), reader.GetString(1), reader.GetInt32(2), reader.GetString(3)));
        }

        return rows;
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
    public async Task ActiveQueryRows_WriteTheirSightings_AndTheSameKeyTwiceBumpsSeenCount()
    {
        Sight(Snapshot(HashA), Snapshot(HashA), Snapshot(HashB), Snapshot(null));
        Sight(Snapshot(HashA));

        Assert.Equal(2, await CountAsync("SELECT COUNT(*) FROM query_tag_sightings WHERE server_id = 7"));
        Assert.Equal(3, await CountAsync($"SELECT seen_count FROM query_tag_sightings WHERE key = '{HashA}'"));
        Assert.Equal(1, await CountAsync($"SELECT seen_count FROM query_tag_sightings WHERE key = '{HashB}'"));
        Assert.Equal(1, await CountAsync($"SELECT COUNT(*) FROM query_tag_sightings WHERE key = '{HashA}' AND text_sample = 'SELECT 1' AND application_name = 'Reports'"));
    }

    [Fact]
    public async Task ADifferentApplicationOnTheSameKey_IsANewSightingRow_AndOneTagStillResolvesTheKey()
    {
        Sight(Snapshot(HashA, "SELECT 1", "Reports"), Snapshot(HashA, "SELECT 1", "Admin"), Snapshot(HashA, "SELECT 1", "Admin"));
        Assert.Equal(2, await CountAsync($"SELECT COUNT(*) FROM query_tag_sightings WHERE key = '{HashA}'"));
        Assert.Equal(2, await CountAsync($"SELECT seen_count FROM query_tag_sightings WHERE key = '{HashA}' AND application_name = 'Admin'"));

        var tag = await _service.CreateQueryTagAsync("Admin", null);
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Application, "admin", tag));
        await _service.BuildQueryTagMapAsync();

        Assert.Equal(tag, Assert.Single(await MapAsync()).TagId);
    }

    [Fact]
    public async Task AFailingSightingsWrite_DoesNotThrow_AndTheCollectionIsUnaffected()
    {
        using (var connection = _fixture.DuckDb.CreateConnection())
        {
            connection.Open();
            using var drop = connection.CreateCommand();
            drop.CommandText = "DROP TABLE query_tag_sightings";
            drop.ExecuteNonQuery();

            var buffer = new QueryTagSightingBuffer();
            buffer.Append(QuerySnapshotsCollector.Instance, [Snapshot(HashA)], serverId: 7);
            var ex = Record.Exception(() => buffer.Flush(_fixture.DuckDb));
            Assert.Null(ex);

            using var create = connection.CreateCommand();
            create.CommandText = Schema.CreateQueryTagSightingsTable;
            create.ExecuteNonQuery();
        }

        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM query_tag_sightings"));
    }

    [Fact]
    public async Task ANewRule_TagsEverySightedKeyItMatches_AfterOnePass()
    {
        Sight(Snapshot(HashA, "SELECT * FROM Orders"), Snapshot(HashB, "SELECT * FROM Users"));
        var tag = await _service.CreateQueryTagAsync("Orders", null);
        await _service.BuildQueryTagMapAsync();
        Assert.Empty(await MapAsync());

        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Text, "*from orders*", tag));
        Assert.Empty(await MapAsync());

        var pass = await _service.BuildQueryTagMapAsync();

        Assert.True(pass.Rebuilt);
        var map = await MapAsync();
        Assert.Equal([("hash", HashA, tag, "rule")], map);
        var state = await _service.GetQueryTagStateAsync();
        Assert.Equal(state.RulesVersion, state.BuiltVersion);
    }

    [Fact]
    public async Task DeletingARule_RemovesItsMapRows_AfterOnePass()
    {
        Sight(Snapshot(HashA, "SELECT * FROM Orders"));
        var tag = await _service.CreateQueryTagAsync("Orders", null);
        var rule = await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Text, "*orders*", tag));
        await _service.BuildQueryTagMapAsync();
        Assert.Single(await MapAsync());

        await _service.DeleteQueryTagRuleAsync(rule);
        await _service.BuildQueryTagMapAsync();

        Assert.Empty(await MapAsync());
    }

    [Fact]
    public async Task AHandAssignment_OverridesARule_AfterOnePass()
    {
        Sight(Snapshot(HashA, "SELECT * FROM Orders"));
        var byRule = await _service.CreateQueryTagAsync("ByRule", null);
        var byHand = await _service.CreateQueryTagAsync("ByHand", null);
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Text, "*orders*", byRule));
        await _service.BuildQueryTagMapAsync();
        Assert.Equal(byRule, Assert.Single(await MapAsync()).TagId);

        await _service.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(byHand, QueryTagKeyKind.Hash, QueryHash: HashA));
        await _service.BuildQueryTagMapAsync();

        var row = Assert.Single(await MapAsync());
        Assert.Equal(byHand, row.TagId);
        Assert.Equal("hand_hash", row.Source);
    }

    [Fact]
    public async Task ASightingAfterTheBuild_IsTaggedByTheNextExtendingPass()
    {
        var tag = await _service.CreateQueryTagAsync("Orders", null);
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Text, "*orders*", tag));
        Sight(Snapshot(HashA, "SELECT * FROM Orders"));
        var cursors = new Dictionary<int, QueryTagMapCursor>();
        await _service.BuildQueryTagMapAsync(cursors);

        Sight(Snapshot(HashB, "SELECT * FROM Orders"));
        var pass = await _service.BuildQueryTagMapAsync(cursors);

        Assert.False(pass.Rebuilt);
        Assert.Equal(2, (await MapAsync()).Count);
    }

    [Fact]
    public async Task TheMap_EqualsWhatTheSharedResolverAnswersForEachKey()
    {
        Sight(
            Snapshot(HashA, "SELECT * FROM Orders -- /* tag:Billing */", "Reports"),
            Snapshot(HashB, "SELECT * FROM Users", "Admin"),
            Snapshot("0x00000000000000CC", "SELECT 3", "Reports"));
        var orders = await _service.CreateQueryTagAsync("Orders", null);
        var billing = await _service.CreateQueryTagAsync("Billing", null);
        var admin = await _service.CreateQueryTagAsync("Admin", null);
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Text, "*orders*", orders, Priority: 50));
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.TextTag, null, null, Priority: 10));
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Application, "admin", admin, Priority: 20));
        await _service.AddQueryTagAssignmentAsync(new QueryTagAssignmentInput(admin, QueryTagKeyKind.Hash, ServerId: 7, QueryHash: "0x00000000000000CC"));

        await _service.BuildQueryTagMapAsync();

        var names = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase)
        {
            ["Orders"] = orders, ["Billing"] = billing, ["Admin"] = admin,
        };
        var assignments = new List<QueryTagHandAssignment>
        {
            new(admin, QueryTagKeyKind.Hash, "0x00000000000000CC", null, 7),
        };
        var rules = new List<QueryTagRule>
        {
            new(1, QueryTagRuleField.Text, "*orders*", 50, true, orders),
            new(2, QueryTagRuleField.TextTag, "/* tag:NAME */", 10, true, null),
            new(3, QueryTagRuleField.Application, "admin", 20, true, admin),
        };
        var fixtures = new[]
        {
            (HashA, Snapshot(HashA, "SELECT * FROM Orders -- /* tag:Billing */", "Reports")),
            (HashB, Snapshot(HashB, "SELECT * FROM Users", "Admin")),
            ("0x00000000000000CC", Snapshot("0x00000000000000CC", "SELECT 3", "Reports")),
        };
        var expected = new List<(string Key, long? TagId)>();
        foreach (var (key, row) in fixtures)
        {
            var result = QueryTagResolver.Resolve(7, QuerySnapshotsCollector.Instance.DescribeQueryTagSightings(row)!, assignments, rules, names);
            expected.Add((key, result.TagId));
        }

        var actual = (await MapAsync()).ToDictionary(r => r.Key, r => (long?)r.TagId);
        foreach (var (key, tagId) in expected)
        {
            Assert.Equal(tagId, actual.TryGetValue(key, out var t) ? t : null);
        }

        Assert.Equal(expected.Count(e => e.TagId is not null), actual.Count);
    }

    private async Task SetLastSeenAsync(string key, DateTime lastSeen)
    {
        using var connection = _fixture.DuckDb.CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE query_tag_sightings SET last_seen = $1 WHERE key = $2";
        command.Parameters.Add(new DuckDBParameter { Value = lastSeen });
        command.Parameters.Add(new DuckDBParameter { Value = key });
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task ASightingSeenJustBeforeThePassClock_IsStillPickedUpByTheNextExtendPass()
    {
        var tag = await _service.CreateQueryTagAsync("Orders", null);
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Text, "*orders*", tag));
        Sight(Snapshot(HashB, "SELECT 2"));
        await SetLastSeenAsync(HashB, DateTime.UtcNow.AddHours(-1));
        var cursors = new Dictionary<int, QueryTagMapCursor>();
        await _service.BuildQueryTagMapAsync(cursors);
        await _service.BuildQueryTagMapAsync(cursors);

        /* Stamped a minute before the previous pass ended: inside the two-minute overlap, so the next pass still reads it. */
        Sight(Snapshot(HashA, "SELECT * FROM Orders"));
        await SetLastSeenAsync(HashA, DateTime.UtcNow.AddMinutes(-1));
        var pass = await _service.BuildQueryTagMapAsync(cursors);

        Assert.False(pass.Rebuilt);
        Assert.Single(await MapAsync());
    }

    [Fact]
    public async Task AnExtendPassThatHitsTheCap_ContinuesAfterItsLastKey_SoTwoPassesCoverEveryKeyOnce()
    {
        var tag = await _service.CreateQueryTagAsync("Orders", null);
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Text, "*orders*", tag));
        Sight(Snapshot("0x00000000000000F0", "SELECT 0"));
        await SetLastSeenAsync("0x00000000000000F0", DateTime.UtcNow.AddHours(-1));
        var cursors = new Dictionary<int, QueryTagMapCursor>();
        await _service.BuildQueryTagMapAsync(cursors, keyCap: 3);

        var keys = Enumerable.Range(1, 5).Select(n => $"0x{n:X16}").ToList();
        Sight(keys.Select(k => Snapshot(k, "SELECT * FROM Orders")).ToArray());
        var start = DateTime.UtcNow.AddSeconds(-30);
        for (var i = 0; i < keys.Count; i++)
        {
            await SetLastSeenAsync(keys[i], start.AddSeconds(i));
        }

        var first = await _service.BuildQueryTagMapAsync(cursors, keyCap: 3);
        var afterFirst = (await MapAsync()).Select(r => r.Key).OrderBy(k => k).ToList();
        var second = await _service.BuildQueryTagMapAsync(cursors, keyCap: 3);

        Assert.False(first.Rebuilt);
        Assert.Equal(3, first.KeysResolved);
        Assert.Equal(keys.Take(3), afterFirst);
        Assert.Equal(2, second.KeysResolved);
        Assert.Equal(keys, (await MapAsync()).Select(r => r.Key).OrderBy(k => k).ToList());
    }

    [Fact]
    public async Task AServerWithNoCursor_GetsTheWholeMapInOnePass_EvenOverTheCap()
    {
        var tag = await _service.CreateQueryTagAsync("Orders", null);
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Text, "*orders*", tag));
        await _service.BuildQueryTagMapAsync();
        var keys = Enumerable.Range(1, 5).Select(n => $"0x{n:X16}").ToList();
        Sight(keys.Select(k => Snapshot(k, "SELECT * FROM Orders")).ToArray());

        var cursors = new Dictionary<int, QueryTagMapCursor>();
        var pass = await _service.BuildQueryTagMapAsync(cursors, keyCap: 3);

        Assert.Equal(keys, (await MapAsync()).Select(r => r.Key).OrderBy(k => k).ToList());
        Assert.Equal(5, pass.KeysResolved);
        Assert.True(cursors.ContainsKey(7));
    }

    [Fact]
    public async Task OneHashSeenInTwoDatabases_IsTwoSightingRows_AndADatabaseRuleTagsIt()
    {
        Sight(Snapshot(HashA, "SELECT 1", "Reports", "Sales"), Snapshot(HashA, "SELECT 1", "Reports", "Archive"));
        Assert.Equal(2, await CountAsync($"SELECT COUNT(*) FROM query_tag_sightings WHERE key = '{HashA}'"));

        var tag = await _service.CreateQueryTagAsync("Archive", null);
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Database, "archive", tag));
        await _service.BuildQueryTagMapAsync();

        Assert.Equal(tag, Assert.Single(await MapAsync()).TagId);
    }

    [Fact]
    public async Task ACollectionBatch_WritesNothingToTheStore_UntilTheBufferFlushes()
    {
        var buffer = new QueryTagSightingBuffer();
        buffer.Append(QuerySnapshotsCollector.Instance, [Snapshot(HashA, "SELECT * FROM Orders"), Snapshot(HashA, "SELECT * FROM Orders")], serverId: 7);

        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM query_tag_sightings"));

        var tag = await _service.CreateQueryTagAsync("Orders", null);
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Text, "*orders*", tag));
        buffer.Flush(_fixture.DuckDb);

        await _service.BuildQueryTagMapAsync();
        Assert.Equal(2, await CountAsync($"SELECT seen_count FROM query_tag_sightings WHERE key = '{HashA}'"));
        Assert.Equal(tag, Assert.Single(await MapAsync()).TagId);
    }

    private const string OldSightingsTable = @"CREATE TABLE query_tag_sightings (server_id INTEGER NOT NULL, key_kind VARCHAR NOT NULL, key VARCHAR NOT NULL, database_name VARCHAR,
        object_name VARCHAR, application_name VARCHAR NOT NULL DEFAULT '', login_name VARCHAR NOT NULL DEFAULT '', host_name VARCHAR NOT NULL DEFAULT '',
        text_sample VARCHAR, first_seen TIMESTAMP NOT NULL DEFAULT current_timestamp, last_seen TIMESTAMP NOT NULL DEFAULT current_timestamp,
        seen_count BIGINT NOT NULL DEFAULT 1, PRIMARY KEY (server_id, key_kind, key, application_name, login_name, host_name))";

    /* A v70 file built from raw statements, the way AgedDatabaseMigrationTests builds an old one: a schema_version table stamped 70 and
       the six-part sightings table, and nothing else. The upgrade creates the rest. */
    private static async Task SeedV70Async(string dbPath)
    {
        using var conn = new DuckDBConnection($"Data Source={dbPath}");
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        foreach (var sql in new[]
        {
            "CREATE TABLE schema_version (version INTEGER NOT NULL)",
            "INSERT INTO schema_version (version) VALUES (70)",
            OldSightingsTable,
            "INSERT INTO query_tag_sightings (server_id, key_kind, key) VALUES (7, 'hash', '0x1')",
        })
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    private static async Task<long> ScalarAsync(string dbPath, string sql)
    {
        using var conn = new DuckDBConnection($"Data Source={dbPath}");
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    private static async Task WithV70Async(Func<string, Task> body)
    {
        var dir = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "LiteTagV71_" + Guid.NewGuid().ToString("N")[..8]);
        System.IO.Directory.CreateDirectory(dir);
        try
        {
            var dbPath = System.IO.Path.Combine(dir, "lite-v70.duckdb");
            await SeedV70Async(dbPath);
            await body(dbPath);
        }
        finally
        {
            try { System.IO.Directory.Delete(dir, true); } catch (Exception) { /* temp cleanup */ }
        }
    }

    private const string TwoDatabasesInsert = "INSERT INTO query_tag_sightings (server_id, key_kind, key, database_name) VALUES (7, 'hash', '0x1', 'A'), (7, 'hash', '0x1', 'B')";

    [Fact]
    public async Task AV70Database_ClimbsToV71_WithTheDatabaseInTheSightingsKey()
    {
        await WithV70Async(async dbPath =>
        {
            using (var upgraded = new DuckDbInitializer(dbPath))
            {
                await upgraded.InitializeAsync();
            }

            Assert.Equal((long)DuckDbInitializer.CurrentSchemaVersion, await ScalarAsync(dbPath, "SELECT MAX(version) FROM schema_version"));
            await ScalarAsync(dbPath, TwoDatabasesInsert.Replace("INSERT INTO", "INSERT INTO") + " RETURNING 1");
            Assert.Equal(2L, await ScalarAsync(dbPath, "SELECT COUNT(*) FROM query_tag_sightings"));
        });
    }

    [Fact]
    public async Task AFailingV71Drop_LeavesTheVersionAtV70_AndTheNextStartRetriesTheStep()
    {
        await WithV70Async(async dbPath =>
        {
            using (var failing = new DuckDbInitializer(dbPath))
            {
                failing.BeforeV71SightingsDropForTests = () => throw new InvalidOperationException("drop failed");
                await failing.InitializeAsync();
            }

            Assert.Equal(70L, await ScalarAsync(dbPath, "SELECT MAX(version) FROM schema_version"));
            Assert.Equal(1L, await ScalarAsync(dbPath, "SELECT COUNT(*) FROM query_tag_sightings"));

            using (var retry = new DuckDbInitializer(dbPath))
            {
                await retry.InitializeAsync();
            }

            Assert.Equal((long)DuckDbInitializer.CurrentSchemaVersion, await ScalarAsync(dbPath, "SELECT MAX(version) FROM schema_version"));
            await ScalarAsync(dbPath, TwoDatabasesInsert + " RETURNING 1");
            Assert.Equal(2L, await ScalarAsync(dbPath, "SELECT COUNT(*) FROM query_tag_sightings"));
        });
    }

    [Fact]
    public async Task AV71CreateThatFailsAfterTheDrop_LeavesTheVersionAtV70_AndTheNextStartRetries()
    {
        await WithV70Async(async dbPath =>
        {
            using (var failing = new DuckDbInitializer(dbPath))
            {
                failing.BeforeV71SightingsCreateForTests = () => throw new InvalidOperationException("create failed");
                await failing.InitializeAsync();
            }

            Assert.Equal(70L, await ScalarAsync(dbPath, "SELECT MAX(version) FROM schema_version"));

            using (var retry = new DuckDbInitializer(dbPath))
            {
                await retry.InitializeAsync();
            }

            Assert.Equal((long)DuckDbInitializer.CurrentSchemaVersion, await ScalarAsync(dbPath, "SELECT MAX(version) FROM schema_version"));
            await ScalarAsync(dbPath, TwoDatabasesInsert + " RETURNING 1");
        });
    }

    [Fact]
    public async Task ARowKeptByAnEarlierFlush_IsStampedAtTheFlushThatWritesIt()
    {
        var buffer = new QueryTagSightingBuffer();
        buffer.Append(QuerySnapshotsCollector.Instance, [Snapshot(HashA)], serverId: 7);
        await Task.Delay(50, TestContext.Current.CancellationToken);
        var beforeFlush = DateTime.UtcNow;

        buffer.Flush(_fixture.DuckDb);

        Assert.Equal(1L, await CountAsync($"SELECT COUNT(*) FROM query_tag_sightings WHERE key = '{HashA}' AND last_seen >= TIMESTAMP '{beforeFlush:yyyy-MM-dd HH:mm:ss.ffffff}'"));
    }

    [Fact]
    public async Task WhileTheV71MigrationIsHeldBack_TheFlushWritesNothing_AndWarnsOnce()
    {
        await WithV70Async(async dbPath =>
        {
            using var failing = new DuckDbInitializer(dbPath);
            failing.BeforeV71SightingsDropForTests = () => throw new InvalidOperationException("drop failed");
            await failing.InitializeAsync();
            var buffer = new QueryTagSightingBuffer();

            buffer.Append(QuerySnapshotsCollector.Instance, [Snapshot(HashA)], serverId: 7);
            buffer.Flush(failing);
            buffer.Append(QuerySnapshotsCollector.Instance, [Snapshot(HashB)], serverId: 7);
            buffer.Flush(failing);

            Assert.Equal(0, buffer.Count);
            Assert.Equal(1, buffer.HeldBackWarningCount);
            Assert.Equal(1L, await ScalarAsync(dbPath, "SELECT COUNT(*) FROM query_tag_sightings"));
        });
    }

    [Fact]
    public async Task AResetAfterAHeldBackStart_StampsTheCurrentVersion()
    {
        await WithV70Async(async dbPath =>
        {
            using var failing = new DuckDbInitializer(dbPath);
            failing.BeforeV71SightingsDropForTests = () => throw new InvalidOperationException("drop failed");
            await failing.InitializeAsync();
            Assert.Equal(70L, await ScalarAsync(dbPath, "SELECT MAX(version) FROM schema_version"));

            await failing.ResetDatabaseAsync();

            Assert.Equal((long)DuckDbInitializer.CurrentSchemaVersion, await ScalarAsync(dbPath, "SELECT MAX(version) FROM schema_version"));
        });
    }

    [Fact]
    public async Task AServerRemovedDuringAPass_HasNoMapRowsOnceTheNextPassRuns()
    {
        var tag = await _service.CreateQueryTagAsync("Orders", null);
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Text, "*orders*", tag));
        Sight(Snapshot(HashA, "SELECT * FROM Orders"));
        var cursors = new Dictionary<int, QueryTagMapCursor>();
        await _service.BuildQueryTagMapAsync(cursors);

        _service.AfterQueryTagMapReadForTests = () => _service.ClearQueryTagsForServerAsync(7).GetAwaiter().GetResult();
        await _service.BuildQueryTagMapAsync(cursors);
        _service.AfterQueryTagMapReadForTests = null;
        var next = await _service.BuildQueryTagMapAsync(cursors);

        Assert.True(next.Rebuilt);
        Assert.Empty(await MapAsync());
    }

    private async Task<string> ScalarTextAsync(string sql)
    {
        using var connection = _fixture.DuckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToString(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken)) ?? string.Empty;
    }

    [Fact]
    public async Task ASampleLongerThanTheCap_IsCutAtTheSharedCap_AndNeverSplitsASurrogatePair()
    {
        var buffer = new QueryTagSightingBuffer();
        var cap = QueryTagLimits.TextSampleCap;
        var split = new string('x', cap - 1) + "\uD83D\uDE00" + new string('y', 50);
        buffer.Append(QuerySnapshotsCollector.Instance, [Snapshot(HashA, new string('a', cap + 1000)), Snapshot(HashB, split)], serverId: 7);
        buffer.Flush(_fixture.DuckDb);

        Assert.Equal(cap, await CountAsync($"SELECT length(text_sample) FROM query_tag_sightings WHERE key = '{HashA}'"));
        Assert.Equal(cap - 1, await CountAsync($"SELECT length(text_sample) FROM query_tag_sightings WHERE key = '{HashB}'"));
    }

    [Fact]
    public async Task AFullBuffer_FlushesAtTheNextAppend()
    {
        var buffer = new QueryTagSightingBuffer();
        var full = Enumerable.Range(1, QueryTagSightingBuffer.MaxEntries).Select(n => Snapshot($"0x{n:X16}")).ToList();
        buffer.Append(QuerySnapshotsCollector.Instance, full, serverId: 7, _fixture.DuckDb);

        Assert.Equal(QueryTagSightingBuffer.MaxEntries, buffer.Count);
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM query_tag_sightings"));

        buffer.Append(QuerySnapshotsCollector.Instance, [Snapshot("0xFFFFFFFFFFFFFFFF")], serverId: 7, _fixture.DuckDb);

        Assert.Equal(QueryTagSightingBuffer.MaxEntries, await CountAsync("SELECT COUNT(*) FROM query_tag_sightings"));
        Assert.Equal(1, buffer.Count);
    }

    [Fact]
    public async Task AFlushCancelledBeforeItsWrite_PutsNothingInTheStore_AndKeepsItsRows()
    {
        var buffer = new QueryTagSightingBuffer();
        buffer.Append(QuerySnapshotsCollector.Instance, [Snapshot(HashA), Snapshot(HashB)], serverId: 7);
        using var cancelled = new System.Threading.CancellationTokenSource();
        await cancelled.CancelAsync();

        buffer.Flush(_fixture.DuckDb, cancellationToken: cancelled.Token);

        Assert.Equal(2, buffer.Count);
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM query_tag_sightings"));

        buffer.Flush(_fixture.DuckDb);
        Assert.Equal(0, buffer.Count);
        Assert.Equal(2, await CountAsync("SELECT COUNT(*) FROM query_tag_sightings"));
    }

    [Fact]
    public async Task AFlushThatCannotGetTheWriteLockInTime_KeepsItsRows()
    {
        var buffer = new QueryTagSightingBuffer();
        buffer.Append(QuerySnapshotsCollector.Instance, [Snapshot(HashA)], serverId: 7);
        var held = new System.Threading.ManualResetEventSlim();
        var release = new System.Threading.ManualResetEventSlim();
        var holder = Task.Run(() =>
        {
            using var writeLock = _fixture.DuckDb.AcquireWriteLock(timeout: TimeSpan.FromSeconds(30));
            held.Set();
            release.Wait(TestContext.Current.CancellationToken);
        }, TestContext.Current.CancellationToken);
        held.Wait(TestContext.Current.CancellationToken);
        try
        {
            buffer.Flush(_fixture.DuckDb, TimeSpan.FromMilliseconds(100));
        }
        finally
        {
            release.Set();
            await holder;
        }

        Assert.Equal(1, buffer.Count);
        Assert.Equal(0, await CountAsync("SELECT COUNT(*) FROM query_tag_sightings"));
    }

    [Fact]
    public async Task TheShutdownDrain_WritesWhatTheLastRoundBuffered()
    {
        var servers = new ServerManager(System.IO.Path.GetTempPath());
        var service = new RemoteCollectorService(_fixture.DuckDb, servers, new ScheduleManager(System.IO.Path.GetTempPath()));
        service.QueryTagSightingBufferForTests = new QueryTagSightingBuffer();
        service.QueryTagSightingBufferForTests.Append(QuerySnapshotsCollector.Instance, [Snapshot(HashA)], serverId: 7);

        service.FlushQueryTagSightingsAtShutdown(System.Threading.CancellationToken.None);

        Assert.Equal(1, await CountAsync($"SELECT COUNT(*) FROM query_tag_sightings WHERE key = '{HashA}'"));
    }

    [Fact]
    public async Task TwoSightingsThatDifferOnlyByDatabase_GiveTheSameTagAndMatchedValue_OnEveryRebuild()
    {
        Sight(Snapshot(HashA, "SELECT 1", "Reports", "Sales"), Snapshot(HashA, "SELECT 1", "Reports", "Archive"));
        var tag = await _service.CreateQueryTagAsync("AnyDb", null);
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Database, "*", tag));
        await _service.BuildQueryTagMapAsync();
        var first = await ScalarTextAsync($"SELECT matched_value FROM query_tag_map WHERE key = '{HashA}'");

        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Login, "nobody", tag));
        await _service.BuildQueryTagMapAsync();
        var second = await ScalarTextAsync($"SELECT matched_value FROM query_tag_map WHERE key = '{HashA}'");

        Assert.Equal("Archive", first);
        Assert.Equal(first, second);
    }

    [Fact]
    public async Task ASightingFlushedBetweenThePassReadAndItsWrite_IsTaggedByTheNextPass()
    {
        var tag = await _service.CreateQueryTagAsync("Orders", null);
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Text, "*orders*", tag));
        var cursors = new Dictionary<int, QueryTagMapCursor>();
        await _service.BuildQueryTagMapAsync(cursors);

        _service.AfterQueryTagMapReadForTests = () => Sight(Snapshot(HashA, "SELECT * FROM Orders"));
        var during = await _service.BuildQueryTagMapAsync(cursors);
        _service.AfterQueryTagMapReadForTests = null;
        Assert.Empty(await MapAsync());

        var next = await _service.BuildQueryTagMapAsync(cursors);

        Assert.False(during.Rebuilt);
        Assert.False(next.Rebuilt);
        Assert.Equal(tag, Assert.Single(await MapAsync()).TagId);
    }

    [Fact]
    public async Task ARuleWrittenBetweenThePassReadAndItsWrite_MakesTheNextPassRebuild()
    {
        var tag = await _service.CreateQueryTagAsync("Orders", null);
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Text, "*orders*", tag));
        Sight(Snapshot(HashA, "SELECT * FROM Orders"), Snapshot(HashB, "SELECT * FROM Items"));

        var items = await _service.CreateQueryTagAsync("Items", null);
        _service.AfterQueryTagMapReadForTests = () => _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Text, "*items*", items)).GetAwaiter().GetResult();
        var during = await _service.BuildQueryTagMapAsync();
        _service.AfterQueryTagMapReadForTests = null;
        var next = await _service.BuildQueryTagMapAsync();

        Assert.True(during.Rebuilt);
        Assert.True(next.Rebuilt);
        Assert.Equal(2, (await MapAsync()).Count);
    }

    [Fact]
    public async Task ADuckDbUtcDateTimeParameter_IsStoredAsThePlainTimestamp_WithNoZoneShift()
    {
        using var connection = _fixture.DuckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT CAST($1 AS VARCHAR)";
        cmd.Parameters.Add(new DuckDBParameter { Value = new DateTime(2026, 1, 2, 3, 4, 5, DateTimeKind.Utc) });
        Assert.Equal("2026-01-02 03:04:05", Convert.ToString(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken)));
    }
}
