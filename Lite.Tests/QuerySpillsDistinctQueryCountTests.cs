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
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5633 (review follow-up to item 2), the Lite twin of Darling's <c>QuerySpillsDistinctQueryCountLiveTests</c>:
/// <c>QUERY_SPILLS.spilling_query_count</c> is the number of DISTINCT queries that spilled, not the number of
/// <c>v_query_stats</c> rows (one per query per snapshot) that carried a spill.
/// </summary>
public sealed class QuerySpillsDistinctQueryCountTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 56330;
    private const string ServerName = "SynthSpillsSrv";
    private const string Db = "SynthSpillsDb";

    private static readonly DateTime WindowEnd = DateTime.SpecifyKind(
        new DateTime(DateTime.UtcNow.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond),
        DateTimeKind.Unspecified);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public QuerySpillsDistinctQueryCountTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    [Fact]
    public async Task SpillingQueryCount_CountsDistinctQueries_NotSnapshotRows()
    {
        /* Query A spills in three snapshots, query B in one, query C never: three rows with spills, two distinct queries. */
        await SeedAsync("0xSPILLA", 90, 100);
        await SeedAsync("0xSPILLA", 60, 100);
        await SeedAsync("0xSPILLA", 30, 100);
        await SeedAsync("0xSPILLB", 45, 50);
        await SeedAsync("0xSPILLC", 40, 0);

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = WindowEnd.AddHours(-4),
            TimeRangeEnd = WindowEnd,
        });

        var spills = Assert.Single(facts, f => f.Key == "QUERY_SPILLS");
        Assert.Equal(350.0, spills.Metadata["total_spills"]);
        Assert.Equal(2.0, spills.Metadata["spilling_query_count"]);
    }

    /// <summary>
    /// Review round 2 Medium-4: <c>QUERY_HIGH_DOP.high_dop_queries</c> drives the 5/20 threshold and the "N queries" text, so it counts
    /// DISTINCT (database, query_hash) like the spill count, not one per snapshot row.
    /// </summary>
    [Fact]
    public async Task HighDopQueryCount_CountsDistinctQueries_NotSnapshotRows()
    {
        /* Query A runs at DOP 16 in three snapshots, B in one, C only at DOP 4: four rows above DOP 8, two distinct queries. */
        await SeedAsync("0xDOPA", 90, 0, maxDop: 16);
        await SeedAsync("0xDOPA", 60, 0, maxDop: 16);
        await SeedAsync("0xDOPA", 30, 0, maxDop: 16);
        await SeedAsync("0xDOPB", 45, 0, maxDop: 16);
        await SeedAsync("0xDOPC", 40, 0, maxDop: 4);

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = WindowEnd.AddHours(-4),
            TimeRangeEnd = WindowEnd,
        });

        var dop = Assert.Single(facts, f => f.Key == "QUERY_HIGH_DOP");
        Assert.Equal(2.0, dop.Value);
        Assert.Equal(2.0, dop.Metadata["high_dop_query_count"]);
    }

    private async Task SeedAsync(string queryHash, int minutesBeforeEnd, long spills, int maxDop = 1)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     max_dop, min_dop, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_spills, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $9, 1, 10, 100000, 200000, $8, 'SELECT * FROM dbo.SynthSpillsTable')";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = WindowEnd.AddMinutes(-minutesBeforeEnd) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = Db });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryHash + "PLAN" });
        cmd.Parameters.Add(new DuckDBParameter { Value = spills });
        cmd.Parameters.Add(new DuckDBParameter { Value = maxDop });
        await cmd.ExecuteNonQueryAsync();
    }
}
