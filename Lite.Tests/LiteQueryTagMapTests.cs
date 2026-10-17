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

    private static QuerySnapshotsCollector.Row Snapshot(string? hash, string text = "SELECT 1", string app = "Reports") => new()
    {
        QueryHash = hash,
        QueryText = text,
        DatabaseName = "Sales",
        ProgramName = app,
        LoginName = "svc",
        HostName = "web1",
    };

    private void Sight(params QuerySnapshotsCollector.Row[] rows)
    {
        using var connection = _fixture.DuckDb.CreateConnection();
        connection.Open();
        RemoteCollectorService.WriteQueryTagSightings(connection, QuerySnapshotsCollector.Instance, rows.ToList(), serverId: 7);
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

            var ex = Record.Exception(() => RemoteCollectorService.WriteQueryTagSightings(
                connection, QuerySnapshotsCollector.Instance, [Snapshot(HashA)], serverId: 7));
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
    public async Task APassThatHitsTheCap_ContinuesAfterItsLastKey_SoTwoPassesCoverEveryKeyOnce()
    {
        var tag = await _service.CreateQueryTagAsync("Orders", null);
        await _service.AddQueryTagRuleAsync(new QueryTagRuleInput(QueryTagRuleField.Text, "*orders*", tag));
        await _service.BuildQueryTagMapAsync();
        var keys = Enumerable.Range(1, 5).Select(n => $"0x{n:X16}").ToList();
        Sight(keys.Select(k => Snapshot(k, "SELECT * FROM Orders")).ToArray());
        var start = DateTime.UtcNow.AddHours(-1);
        for (var i = 0; i < keys.Count; i++)
        {
            await SetLastSeenAsync(keys[i], start.AddSeconds(i));
        }

        var cursors = new Dictionary<int, QueryTagMapCursor>();
        var first = await _service.BuildQueryTagMapAsync(cursors, keyCap: 3);
        var afterFirst = (await MapAsync()).Select(r => r.Key).OrderBy(k => k).ToList();
        var second = await _service.BuildQueryTagMapAsync(cursors, keyCap: 3);

        Assert.Equal(3, first.KeysResolved);
        Assert.Equal(keys.Take(3), afterFirst);
        Assert.Equal(2, second.KeysResolved);
        Assert.Equal(keys, (await MapAsync()).Select(r => r.Key).OrderBy(k => k).ToList());
    }
}
