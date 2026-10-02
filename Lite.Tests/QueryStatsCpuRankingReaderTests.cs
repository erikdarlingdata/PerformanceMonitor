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
/// #3523: Lite's <c>GetTopQueriesByCpuAsync</c> / <c>GetTopProceduresByCpuAsync</c> — the reads behind
/// <c>get_top_queries_by_cpu</c> / <c>get_top_procedures_by_cpu</c> and the Query Performance grids —
/// ranked by summed ELAPSED time. On a wait-bound server the two disagree wildly, so the real CPU
/// consumers could be absent from the page entirely. The queries read has TWO ordering sites: the ranking
/// CTE's <c>ORDER BY ... LIMIT top + 5</c> decides which groups SURVIVE, and the outer post-WAITFOR-trim
/// sort decides the returned order — so the seed makes the CPU king the WORST group by elapsed with more
/// competing groups than the over-fetch admits: under the old key it was cut, not merely mis-sorted.
/// Real-DuckDB round-trip in the shared-fixture harness, like <see cref="QueryStatsModuleAttributionReaderTests"/>.
/// </summary>
public sealed class QueryStatsCpuRankingReaderTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 8815;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    /* Anchor in the recent past so the default 24h window includes it; last_execution_time must be >= the
       window start (the reader's staleness filter, utc offset 0). */
    private static readonly DateTime Collected =
        DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-90), DateTimeKind.Unspecified);

    public QueryStatsCpuRankingReaderTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    /* The wait-bound shape: one CPU king whose elapsed time is the smallest on the box, six lock-sleepers
       whose elapsed dwarfs it while their CPU is noise. */
    private async Task SeedDisagreeingQueriesAsync()
    {
        await SeedQueryStatsAsync("0xCPUKING", "SELECT CpuBurner FROM Numbers", cpuUs: 900_000, elapsedUs: 1_000);
        for (var i = 1; i <= 6; i++)
            await SeedQueryStatsAsync($"0xSLEEPER{i}", $"SELECT Blocked{i} FROM Locked", cpuUs: 1_000 + i, elapsedUs: 800_000 + i * 10_000);
    }

    [Fact]
    public async Task TopQueries_Top1_SurvivesTheCteCut_ByCpuNotElapsed()
    {
        var service = new LocalDataService(_duckDb);
        await SeedDisagreeingQueriesAsync();

        /* Seven groups against top: 1 (CTE over-fetch = 6): an elapsed-keyed CTE cuts the CPU king
           before the outer sort ever sees it. */
        var rows = await service.GetTopQueriesByCpuAsync(ServerId, hoursBack: 24, top: 1);

        Assert.Equal("0xCPUKING", Assert.Single(rows).QueryHash);
    }

    [Fact]
    public async Task TopQueries_ReturnsThePage_InDescendingCpuOrder()
    {
        var service = new LocalDataService(_duckDb);
        await SeedDisagreeingQueriesAsync();

        var rows = await service.GetTopQueriesByCpuAsync(ServerId);

        Assert.Equal(7, rows.Count);
        Assert.Equal("0xCPUKING", rows[0].QueryHash);
        Assert.Equal(rows.Select(r => r.TotalCpuUs).OrderByDescending(v => v), rows.Select(r => r.TotalCpuUs));
    }

    [Fact]
    public async Task TopProcedures_RankByCpu_WhenCpuAndElapsedDisagree()
    {
        var service = new LocalDataService(_duckDb);

        await SeedProcedureStatsAsync("usp_CpuHog", cpuUs: 900_000, elapsedUs: 1_000);
        await SeedProcedureStatsAsync("usp_WaitBound", cpuUs: 5_000, elapsedUs: 900_000);

        var top1 = await service.GetTopProceduresByCpuAsync(ServerId, hoursBack: 24, top: 1);
        Assert.Equal("usp_CpuHog", Assert.Single(top1).ObjectName);

        var page = await service.GetTopProceduresByCpuAsync(ServerId);
        Assert.Equal(new[] { "usp_CpuHog", "usp_WaitBound" }, page.Select(r => r.ObjectName));
    }

    // ── Seeding helpers (column-list INSERT; unset columns default to NULL) ──

    private async Task SeedQueryStatsAsync(string queryHash, string queryText, long cpuUs, long elapsedUs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_hash, sql_handle, last_execution_time, delta_execution_count,
     delta_worker_time, delta_elapsed_time, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestDb" });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xH" + queryHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = Collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10L });
        cmd.Parameters.Add(new DuckDBParameter { Value = cpuUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = elapsedUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryText });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedProcedureStatsAsync(string objectName, long cpuUs, long elapsedUs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     schema_name, object_name, object_type, last_execution_time,
     delta_execution_count, delta_worker_time, delta_elapsed_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestDb" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "dbo" });
        cmd.Parameters.Add(new DuckDBParameter { Value = objectName });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SQL_STORED_PROCEDURE" });
        cmd.Parameters.Add(new DuckDBParameter { Value = Collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10L });
        cmd.Parameters.Add(new DuckDBParameter { Value = cpuUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = elapsedUs });
        await cmd.ExecuteNonQueryAsync();
    }
}
