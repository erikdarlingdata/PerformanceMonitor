/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Behaviour pins for the tag on Lite's query reads and their <c>tag</c> filter (#5992 item 3) against a real DuckDB, with
/// <c>query_tag_map</c> seeded directly. The filter is applied in SQL before the top-N cut.
/// </summary>
public sealed class LiteQueryTagReadsTests : IClassFixture<SharedDuckDbFixture>
{
    private const int Server = 9201;
    private const string HotHash = "0x00000000000000AA";
    private const string TaggedHash = "0x00000000000000BB";
    private const string ChildHash = "0x00000000000000CC";

    private readonly LocalDataService _service;
    private readonly SharedDuckDbFixture _fixture;
    private long _nextId = 2_000_000;

    public LiteQueryTagReadsTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _fixture = fixture;
        _service = new LocalDataService(fixture.DuckDb);
    }

    private async Task ExecAsync(string sql, params object[] values)
    {
        using var connection = _fixture.DuckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values)
        {
            cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = v });
        }

        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private Task PlanAsync(string hash, long cpuUs) => ExecAsync(
        "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes) VALUES ($1, $2, $3, 'x', 'db1', $4, $2, 1, $5, $5, 10, 1)",
        _nextId++, DateTime.UtcNow.AddMinutes(-30), Server, hash, cpuUs);

    private Task MapAsync(string hash, int tagId) => ExecAsync(
        "INSERT INTO query_tag_map (server_id, key_kind, key, tag_id, source) VALUES ($1, 'hash', $2, $3, 'hand_hash')", Server, hash, tagId);

    /// <summary>Three plan-cache rows: a hot untagged one, a cooler one tagged A, and the coolest tagged with A's child.</summary>
    private async Task<(int Parent, int Child)> SeedAsync()
    {
        var parent = await _service.CreateQueryTagAsync("A", null);
        var child = await _service.CreateQueryTagAsync("A2", parent);
        await PlanAsync(HotHash, 9_000_000);
        await PlanAsync(TaggedHash, 5_000_000);
        await PlanAsync(ChildHash, 1_000_000);
        await MapAsync(TaggedHash, parent);
        await MapAsync(ChildHash, child);
        return (parent, child);
    }

    private async Task<(string Hash, string? Tag)[]> TopAsync(string? tag, int top)
    {
        var scope = await _service.GetQueryTagReadScopeAsync(Server, tag);
        var rows = await _service.GetTopQueriesByCpuAsync(Server, 24, top, tags: scope);
        return rows.Select(r => (r.QueryHash, scope.TagOfHash(r.QueryHash))).ToArray();
    }

    [Fact]
    public async Task TopQueriesByCpu_CarriesTheTagOnTheTaggedRowAndNullOnTheOther()
    {
        await SeedAsync();

        var rows = await TopAsync(null, 10);

        Assert.Equal("A", rows.Single(r => r.Hash == TaggedHash).Tag);
        Assert.Null(rows.Single(r => r.Hash == HotHash).Tag);
    }

    [Fact]
    public async Task TagFilter_ReturnsATaggedRowThatIsOutsideTheUnfilteredTopN()
    {
        await SeedAsync();

        Assert.Equal([HotHash], (await TopAsync(null, 1)).Select(r => r.Hash).ToArray());
        Assert.Equal([TaggedHash], (await TopAsync("A", 1)).Select(r => r.Hash).ToArray());
        Assert.Equal([ChildHash], (await TopAsync("A2", 1)).Select(r => r.Hash).ToArray());
    }

    [Fact]
    public async Task TagFilter_AParentMatchesItsDescendants_AndUntaggedMatchesRowsWithNoTag()
    {
        await SeedAsync();

        Assert.Equal([TaggedHash, ChildHash], (await TopAsync("a", 10)).Select(r => r.Hash).ToArray());
        Assert.Equal([HotHash], (await TopAsync("untagged", 10)).Select(r => r.Hash).ToArray());
    }

    [Fact]
    public async Task TagFilter_ANameNoTagCarriesIsUnknown()
    {
        await SeedAsync();

        Assert.True((await _service.GetQueryTagReadScopeAsync(Server, "nope")).Unknown);
        Assert.False((await _service.GetQueryTagReadScopeAsync(Server, "untagged")).Unknown);
    }

    [Fact]
    public async Task TagFilter_OverManyMapKeys_ReturnsTheSameRowsAsTheTagCarries()
    {
        var tag = await _service.CreateQueryTagAsync("Bulk", null);
        const int tagged = 300;
        for (var i = 0; i < tagged; i++)
        {
            var hash = "0x" + (0x1000 + i).ToString("X16");
            await PlanAsync(hash, 1_000 + i);
            await MapAsync(hash, tag);
        }

        await PlanAsync("0x00000000000000F1", 50_000);
        await PlanAsync("0x00000000000000F2", 40_000);

        var rows = await TopAsync("Bulk", 1000);
        Assert.Equal(tagged, rows.Length);
        Assert.All(rows, r => Assert.Equal("Bulk", r.Tag));
        Assert.Equal(["0x00000000000000F1", "0x00000000000000F2"], (await TopAsync("untagged", 1000)).Select(r => r.Hash).ToArray());
    }

    [Fact]
    public async Task TagFilter_OnADecimalStoredHash_MatchesTheMapsCanonicalKey()
    {
        var tag = await _service.CreateQueryTagAsync("Dec", null);
        await PlanAsync("170", 1_000);
        await MapAsync("0x00000000000000AA", tag);

        Assert.Equal(["170"], (await TopAsync("Dec", 10)).Select(r => r.Hash).ToArray());
        Assert.Empty(await TopAsync("untagged", 10));
    }
}
