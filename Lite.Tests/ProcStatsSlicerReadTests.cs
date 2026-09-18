/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3556: the procedures slicer's reader mapped BOTH read fields to ordinal 4 and never read ordinal 6,
/// so the SELECT's total_physical_reads was computed and dropped on the floor — the #3530 bug's twin, one
/// grid over. As there, the seed's three I/O columns carry values no other column can reproduce: equal
/// fixture values are exactly how the slip stayed invisible, so distinct-per-column is the point of this
/// test, not a nicety.
/// </summary>
public sealed class ProcStatsSlicerReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 8856;
    private const string Db = "ProcSliceDb";

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public ProcStatsSlicerReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /* hoursBack (not fromDate/toDate) on purpose: GetTimeRange applies ServerTimeHelper.UtcOffsetMinutes
       to an explicit range, which would make a fixed timestamp depend on the machine's server-time
       offset. Floored to the hour so the single seeded row sits squarely in one date_trunc bucket. */
    private static readonly DateTime BucketStart = HourFloor(DateTime.UtcNow.AddHours(-3));

    private static DateTime HourFloor(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerHour)), DateTimeKind.Unspecified);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedAsync(
        DateTime collectionTime,
        string objectName,
        long workerUs,
        long elapsedUs,
        long logicalReads,
        long logicalWrites,
        long physicalReads)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, schema_name, object_name, delta_execution_count,
     delta_worker_time, delta_elapsed_time,
     delta_logical_reads, delta_logical_writes, delta_physical_reads)
VALUES ($1, $2, $3, $4, $5, 'dbo', $6, $7, $8, $9, $10, $11, $12)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "ProcSliceSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = Db });
        cmd.Parameters.Add(new DuckDBParameter { Value = objectName });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10L });
        cmd.Parameters.Add(new DuckDBParameter { Value = workerUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = elapsedUs });
        cmd.Parameters.Add(new DuckDBParameter { Value = logicalReads });
        cmd.Parameters.Add(new DuckDBParameter { Value = logicalWrites });
        cmd.Parameters.Add(new DuckDBParameter { Value = physicalReads });
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task SlicerBucket_MapsWritesAndPhysicalReads_ToTheirOwnColumns()
    {
        await SeedAsync(BucketStart.AddMinutes(5), "usp_IoMap",
            workerUs: 1_000, elapsedUs: 2_000, logicalReads: 110, logicalWrites: 30, physicalReads: 50);

        var bucket = Assert.Single(await new LocalDataService(_duckDb).GetProcStatsSlicerDataAsync(ServerId, hoursBack: 24));

        /* All pairwise distinct — 110 / 30 / 50. TotalReads and TotalLogicalReads are deliberate aliases
           of the LOGICAL aggregate (ordinal 4), the same shape the query-stats and Query Store slicers
           map; physical rides its own column at ordinal 6. */
        Assert.Equal(110.0, bucket.TotalReads, precision: 6);
        Assert.Equal(110.0, bucket.TotalLogicalReads, precision: 6);
        Assert.Equal(30.0, bucket.TotalWrites, precision: 6);
        Assert.Equal(50.0, bucket.TotalPhysicalReads, precision: 6);
    }
}
