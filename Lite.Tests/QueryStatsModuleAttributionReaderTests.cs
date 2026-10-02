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
/// Real-DuckDB round-trip pins for the #1568 module attribution in Top Queries by CPU
/// (<see cref="LocalDataService.GetTopQueriesByCpuAsync"/>): the read-time LEFT JOIN of
/// <c>query_stats.sql_handle</c> to <c>procedure_stats.sql_handle</c> (both stores persist the SAME
/// normalized <c>CONVERT(varchar(130), ..., 1)</c> handle text). A statement whose handle matches a cached
/// procedure/function/trigger inherits its db.schema.object; an unmatched statement is ad hoc; and the join
/// picks ONE module identity per handle (latest collection_time) so a query row never fans out. Mirrors the
/// Darling <c>ViewerQueriesLivePostgresTests</c> attribution round-trip and the Lite reader-test harness
/// (<see cref="BlockingStatsReaderTests"/>).
/// </summary>
public sealed class QueryStatsModuleAttributionReaderTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 8811;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    /* Anchor in the recent past so the default 24h window includes it; last_execution_time must be >= the
       window start (the reader's staleness filter, utc offset 0). */
    private static readonly DateTime Collected = MinuteFloor(DateTime.UtcNow.AddMinutes(-90));

    public QueryStatsModuleAttributionReaderTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>
    /// One connection reused for every seeded row — opening a fresh connection per
    /// single-row INSERT measured ~90ms/row and dominated this class's runtime.
    /// </summary>
    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private static DateTime MinuteFloor(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    [Fact]
    public async Task TopQueries_AttributesMatchedHandleToModule_UnmatchedIsAdHoc()
    {
        var service = new LocalDataService(_duckDb);

        /* Attributed query: sql_handle matches a cached procedure. (Lookups below are by hash — the
           #3523 CPU ranking never matters here, and these seeds carry no worker time.) */
        await SeedQueryStatsAsync("TestDb", "0xATTRIB", "0xMOD", deltaExec: 5, deltaElapsedUs: 300_000, "SELECT attributed");
        await SeedProcedureStatsAsync("TestDb", "dbo", "usp_Thing", "PROCEDURE", "0xMOD");

        /* Ad hoc query: its sql_handle matches no cached module. */
        await SeedQueryStatsAsync("TestDb", "0xADHOC", "0xLOOSE", deltaExec: 3, deltaElapsedUs: 100_000, "SELECT adhoc");

        var rows = await service.GetTopQueriesByCpuAsync(ServerId);

        Assert.Equal(2, rows.Count);

        var attributed = rows.Single(r => r.QueryHash == "0xATTRIB");
        Assert.Equal("usp_Thing", attributed.ModuleObjectName);
        Assert.Equal("dbo", attributed.ModuleSchemaName);
        Assert.Equal("TestDb", attributed.ModuleDatabaseName);
        Assert.Equal("TestDb.dbo.usp_Thing", attributed.ModuleName);

        var adHoc = rows.Single(r => r.QueryHash == "0xADHOC");
        Assert.Equal("", adHoc.ModuleObjectName);
        Assert.Equal("ad hoc", adHoc.ModuleName);
    }

    [Fact]
    public async Task TopQueries_CountDistinctTexts_SoBlendedHashGroupsAreVisible()
    {
        var service = new LocalDataService(_duckDb);

        /* #2012: INSERT...EXEC statements naming DIFFERENT callee procs share a query_hash
           (reproduced live on SQL Server 2022), so a hash group can merge genuinely different
           statements and label them with one representative text. distinct_texts is the tell. */
        await SeedQueryStatsAsync("TestDb", "0xBLEND", "0xH1", deltaExec: 5, deltaElapsedUs: 300_000, "INSERT INTO #items EXEC dbo.inner_v3");
        await SeedQueryStatsAsync("TestDb", "0xBLEND", "0xH2", deltaExec: 4, deltaElapsedUs: 200_000, "INSERT INTO #items EXEC dbo.inner_v4");
        await SeedQueryStatsAsync("TestDb", "0xONETEXT", "0xH3", deltaExec: 3, deltaElapsedUs: 100_000, "SELECT stable");
        await SeedQueryStatsAsync("TestDb", "0xONETEXT", "0xH3", deltaExec: 2, deltaElapsedUs: 90_000, "SELECT stable");

        var rows = await service.GetTopQueriesByCpuAsync(ServerId);

        Assert.Equal(2, rows.Single(r => r.QueryHash == "0xBLEND").DistinctTexts);
        Assert.Equal(1, rows.Single(r => r.QueryHash == "0xONETEXT").DistinctTexts);
    }

    [Fact]
    public async Task TopQueries_ModuleJoinDoesNotFanOut_LatestCollectionWins()
    {
        var service = new LocalDataService(_duckDb);

        await SeedQueryStatsAsync("TestDb", "0xONE", "0xMOD", deltaExec: 4, deltaElapsedUs: 200_000, "SELECT one");

        /* Same sql_handle captured twice with a different object identity — the newer capture wins, and the
           single query row must not multiply across the two procedure_stats rows. */
        await SeedProcedureStatsAsync("TestDb", "dbo", "usp_Old", "PROCEDURE", "0xMOD", collectedOverride: Collected.AddMinutes(-30));
        await SeedProcedureStatsAsync("TestDb", "dbo", "usp_New", "PROCEDURE", "0xMOD", collectedOverride: Collected);

        var rows = await service.GetTopQueriesByCpuAsync(ServerId);

        var only = Assert.Single(rows);          /* exactly one row — the join did not fan out */
        Assert.Equal("TestDb.dbo.usp_New", only.ModuleName);
    }

    // ── Seeding helpers (column-list INSERT; unset columns default to NULL) ──

    private async Task SeedQueryStatsAsync(
        string databaseName, string queryHash, string sqlHandle, long deltaExec, long deltaElapsedUs, string queryText)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_hash, sql_handle, last_execution_time, delta_execution_count, delta_elapsed_time, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = databaseName });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = sqlHandle });
        cmd.Parameters.Add(new DuckDBParameter { Value = Collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = deltaExec });
        cmd.Parameters.Add(new DuckDBParameter { Value = deltaElapsedUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryText });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedProcedureStatsAsync(
        string databaseName, string schemaName, string objectName, string objectType, string sqlHandle,
        DateTime? collectedOverride = null)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     schema_name, object_name, object_type, sql_handle)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectedOverride ?? Collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = databaseName });
        cmd.Parameters.Add(new DuckDBParameter { Value = schemaName });
        cmd.Parameters.Add(new DuckDBParameter { Value = objectName });
        cmd.Parameters.Add(new DuckDBParameter { Value = objectType });
        cmd.Parameters.Add(new DuckDBParameter { Value = sqlHandle });
        await cmd.ExecuteNonQueryAsync();
    }
}

/// <summary>Pure row-model pin for the composed #1568 "Module" display string (no DuckDB).</summary>
public sealed class QueryStatsRowModuleDisplayTests
{
    [Fact]
    public void ModuleName_ComposesDatabaseSchemaObject_WhenAttributed()
    {
        var row = new QueryStatsRow { ModuleDatabaseName = "StackOverflow", ModuleSchemaName = "dbo", ModuleObjectName = "usp_Get" };
        Assert.Equal("StackOverflow.dbo.usp_Get", row.ModuleName);
    }

    [Fact]
    public void ModuleName_IsAdHoc_WhenNoObjectName()
    {
        Assert.Equal("ad hoc", new QueryStatsRow().ModuleName);
        Assert.Equal("ad hoc", new QueryStatsRow { ModuleDatabaseName = "StackOverflow", ModuleSchemaName = "dbo" }.ModuleName);
    }

    [Fact]
    public void ModuleName_PrefersCollectionTimeHostObject_OverHandleStitch()
    {
        /* #2012 stage 2: host_object_name is resolved ON the monitored server at collection, so it
           wins over the #1568 sql_handle stitch (which requires the module to also be in the
           procedure-stats cache); pre-upgrade rows have a NULL host and keep the stitch. */
        var both = new QueryStatsRow
        {
            DatabaseName = "StackOverflow",
            HostObjectName = "dbo.usp_Host",
            ModuleDatabaseName = "OtherDb",
            ModuleSchemaName = "dbo",
            ModuleObjectName = "usp_Stitched",
        };
        Assert.Equal("StackOverflow.dbo.usp_Host", both.ModuleName);

        var hostOnly = new QueryStatsRow { DatabaseName = "StackOverflow", HostObjectName = "dbo.usp_Host" };
        Assert.Equal("StackOverflow.dbo.usp_Host", hostOnly.ModuleName);
    }
}
