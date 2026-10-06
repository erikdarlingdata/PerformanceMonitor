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
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5313 (Lite twin of Darling's <c>TopFill</c>): <c>GetTopQueriesByCpuAsync</c> and <c>GetQueryStoreTopQueriesAsync</c> rank
/// candidates, drop the WAITFOR shells, then cap at <c>top</c>. They used to over-fetch a fixed five, so six or more shells in the
/// top candidates gave a short page. The page now holds <c>top</c> rows whenever <c>top</c> non-WAITFOR keys qualify, a window with
/// fewer returns all of them, and a key with no text still shows.
/// </summary>
public sealed class TopListWaitforRefillReaderTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 8817;
    private const int Top = 5;
    private static readonly DateTime Collected = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-90), DateTimeKind.Unspecified);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public TopListWaitforRefillReaderTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private async Task ExecAsync(string sql, params object?[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var value in values)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = value });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>One query_stats key whose CPU (the ranking) is <paramref name="cpu"/>; a null text seeds a key with no text.</summary>
    private Task SeedStatsKeyAsync(string hash, long cpu, string? text) => ExecAsync(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_hash, sql_handle, last_execution_time, delta_execution_count,
     delta_worker_time, delta_elapsed_time, query_text)
VALUES ($1, $2, $3, 'TestSrv', 'TestDb', $4, $5, $2, 10, $6, $6, $7)", _nextId++, Collected, ServerId, hash, "0xH" + hash, cpu, text);

    /// <summary>One query_store_stats key whose duration (the ranking) is <paramref name="durationUs"/>.</summary>
    private Task SeedStoreKeyAsync(long queryId, long durationUs, string? text) => ExecAsync(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_text, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
     runtime_stats_interval_id, interval_start_time_utc)
VALUES ($1, $2, $3, 'TestSrv', 'TestDb', $4, $4, 'Regular', $2, $2, $5, $6, 10, 1000, $7, 100, 0, 0, 1, $2)",
        _nextId++, Collected, ServerId, queryId, text, "0xQ" + queryId, durationUs);

    /// <summary>Seeds <paramref name="waitfors"/> WAITFOR keys ranked above <paramref name="normals"/> ordinary ones, both for stats and the store.</summary>
    private async Task SeedAsync(int waitfors, int normals, string? topText = "UNSET")
    {
        var total = waitfors + normals;
        for (var i = 0; i < total; i++)
        {
            var rank = total - i; // the first key ranks highest
            var isWaitfor = i < waitfors;
            var text = isWaitfor ? $"WAITFOR DELAY '00:00:{i:00}'" : $"SELECT {i} FROM dbo.t{i}";
            await SeedStatsKeyAsync($"0xK{i:0000}", rank * 1000L, text);
            await SeedStoreKeyAsync(1000 + i, rank * 1000L, text);
        }

        if (topText != "UNSET")
        {
            // A key with no text that outranks everything: it must still show.
            await SeedStatsKeyAsync("0xKNULL", (total + 5) * 1000L, topText);
            await SeedStoreKeyAsync(9999, (total + 5) * 1000L, topText);
        }
    }

    [Fact]
    public async Task SixShellsInTheTopCandidates_StillGiveAFullPage()
    {
        await SeedAsync(waitfors: 6, normals: Top + 0);
        var service = new LocalDataService(_duckDb);

        var stats = await service.GetTopQueriesByCpuAsync(ServerId, hoursBack: 24, top: Top);
        var store = await service.GetQueryStoreTopQueriesAsync(ServerId, hoursBack: 24, top: Top);

        Assert.Equal(Top, stats.Count);
        Assert.Equal(Top, store.Count);
        Assert.All(stats, r => Assert.DoesNotContain("WAITFOR", r.QueryText));
        Assert.All(store, r => Assert.DoesNotContain("WAITFOR", r.QueryText));
        Assert.Equal(Enumerable.Range(6, Top).Select(i => $"SELECT {i} FROM dbo.t{i}"), stats.Select(r => r.QueryText));
        Assert.Equal(Enumerable.Range(6, Top).Select(i => $"SELECT {i} FROM dbo.t{i}"), store.Select(r => r.QueryText));
    }

    [Fact]
    public async Task FiftyShellsAboveTheOrdinaryKeys_StillGiveAFullPage()
    {
        // 50 shells outrun round one (top + 5) and round two (4x that); round three finds the ordinary keys.
        await SeedAsync(waitfors: 50, normals: 8);
        var service = new LocalDataService(_duckDb);

        var stats = await service.GetTopQueriesByCpuAsync(ServerId, hoursBack: 24, top: Top);
        var store = await service.GetQueryStoreTopQueriesAsync(ServerId, hoursBack: 24, top: Top);

        Assert.Equal(Top, stats.Count);
        Assert.Equal(Top, store.Count);
        Assert.All(stats, r => Assert.DoesNotContain("WAITFOR", r.QueryText));
        Assert.All(store, r => Assert.DoesNotContain("WAITFOR", r.QueryText));
    }

    [Fact]
    public async Task FewerOrdinaryKeysThanTop_ReturnsAllOfThem()
    {
        await SeedAsync(waitfors: 6, normals: 3);
        var service = new LocalDataService(_duckDb);

        var stats = await service.GetTopQueriesByCpuAsync(ServerId, hoursBack: 24, top: Top);
        var store = await service.GetQueryStoreTopQueriesAsync(ServerId, hoursBack: 24, top: Top);

        Assert.Equal(3, stats.Count);
        Assert.Equal(3, store.Count);
        Assert.All(stats, r => Assert.DoesNotContain("WAITFOR", r.QueryText));
        Assert.All(store, r => Assert.DoesNotContain("WAITFOR", r.QueryText));
    }

    [Fact]
    public async Task OnlyShellsInTheWindow_ReturnsAnEmptyPage()
    {
        await SeedAsync(waitfors: 8, normals: 0);
        var service = new LocalDataService(_duckDb);

        Assert.Empty(await service.GetTopQueriesByCpuAsync(ServerId, hoursBack: 24, top: Top));
        Assert.Empty(await service.GetQueryStoreTopQueriesAsync(ServerId, hoursBack: 24, top: Top));
    }

    [Fact]
    public async Task ANullTextKeyAtTheTop_StillShows_AndTheShellsStillRefill()
    {
        await SeedAsync(waitfors: 6, normals: Top, topText: null);
        var service = new LocalDataService(_duckDb);

        var stats = await service.GetTopQueriesByCpuAsync(ServerId, hoursBack: 24, top: Top);
        var store = await service.GetQueryStoreTopQueriesAsync(ServerId, hoursBack: 24, top: Top);

        Assert.Equal(Top, stats.Count);
        Assert.Equal(Top, store.Count);
        Assert.Equal("0xKNULL", stats[0].QueryHash);
        Assert.Equal("", stats[0].QueryText);
        Assert.Equal(9999, store[0].QueryId);
        Assert.Equal("", store[0].QueryText);
        Assert.All(stats.Skip(1), r => Assert.DoesNotContain("WAITFOR", r.QueryText));
        Assert.All(store.Skip(1), r => Assert.DoesNotContain("WAITFOR", r.QueryText));
    }

    [Fact]
    public void NextCandidates_StopsWhenFullShortOfCandidatesOrAtTheBound()
    {
        Assert.Equal(10, TopFill.FirstCandidates(5));
        Assert.Null(TopFill.NextCandidates(5, 1, 10, returned: 5, candidateCount: 10));  // full page
        Assert.Null(TopFill.NextCandidates(5, 1, 10, returned: 2, candidateCount: 7));   // candidates ran out
        Assert.Equal(40, TopFill.NextCandidates(5, 1, 10, returned: 2, candidateCount: 10));
        Assert.Null(TopFill.NextCandidates(5, 3, 160, returned: 2, candidateCount: 160)); // round bound
    }
}
