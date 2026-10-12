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
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Pins Lite's Collection Log window bounding (parity with the Darling #1406 fix): a custom
/// [fromDate,toDate] range set entirely in the past must bound <c>collection_time</c> on BOTH sides —
/// a row after the To bound is EXCLUDED, which the old hours-back-only read (a single now-relative
/// lower bound that ignored fromDate/toDate) could not express. The preset test proves a plain
/// hoursBack read still lower-bounds "now".
/// </summary>
/* Reads the process-wide ServerTimeHelper.UtcOffsetMinutes to build its query window — see the collection
   note on SystemEventsReaderTests, which MUTATES it. */
[Collection("server-time-helper")]
public sealed class CollectionHealthWindowTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 4242;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public CollectionHealthWindowTests(SharedDuckDbFixture fixture)
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

    [Fact]
    public async Task GetRecentCollectionLog_CustomRange_BoundsBothSides_ExcludingRowsOutsideEitherBound()
    {
        var service = new LocalDataService(_duckDb);

        /* A three-hour custom window [start, end] entirely in the past (end an hour ago). A row 30 min
           BEFORE start and a row 30 min AFTER end must BOTH be excluded. The after-end exclusion is the
           behavior the old read (a single now-relative lower bound) could not express — it would have
           swept everything up to "now", pulling in the after-end row. collection_time is stored UTC. */
        var end = Truncate(DateTime.UtcNow.AddHours(-1));
        var start = end.AddHours(-3);
        var beforeStart = start.AddMinutes(-30);
        var inWindowEarly = start.AddMinutes(15);
        var inWindowLate = end.AddMinutes(-15);
        var afterEnd = end.AddMinutes(30);

        await SeedLogAsync("wait_stats", beforeStart, "SUCCESS");
        await SeedLogAsync("wait_stats", inWindowEarly, "SUCCESS");
        await SeedLogAsync("cpu_utilization", inWindowLate, "SUCCESS");
        await SeedLogAsync("wait_stats", afterEnd, "SUCCESS");

        /* A custom from/to is a naive-UTC pair (#4766) and GetTimeRange returns it as it came, so the bounds are
           the UTC instants themselves and the read lands exactly on [start,end] whatever the machine's zone. */
        var fromDate = start;
        var toDate = end;

        /* hoursBack=4 would (old bug) sweep everything >= now-4h, pulling in afterEnd (now-30m); the
           custom To bound must exclude it. */
        var logs = await service.GetRecentCollectionLogAsync(ServerId, hoursBack: 4, fromDate: fromDate, toDate: toDate);

        Assert.Equal(2, logs.Count);
        Assert.All(logs, l => Assert.InRange(l.CollectionTime.Ticks, start.Ticks, end.Ticks));
        Assert.Equal(inWindowLate.Ticks, logs[0].CollectionTime.Ticks);   // newest first
        Assert.Equal(inWindowEarly.Ticks, logs[1].CollectionTime.Ticks);
    }

    [Fact]
    public async Task GetRecentCollectionLog_PresetHoursBack_LowerBoundsFromNow()
    {
        var service = new LocalDataService(_duckDb);

        var recent = Truncate(DateTime.UtcNow.AddMinutes(-30));
        var old = Truncate(DateTime.UtcNow.AddHours(-10));
        await SeedLogAsync("wait_stats", recent, "SUCCESS");
        await SeedLogAsync("wait_stats", old, "SUCCESS");

        /* No custom range → hoursBack lower-bounds "now"; the 10h-old row is excluded, the 30m one kept. */
        var logs = await service.GetRecentCollectionLogAsync(ServerId, hoursBack: 4);

        Assert.Single(logs);
        Assert.Equal(recent.Ticks, logs[0].CollectionTime.Ticks);
    }

    private static DateTime Truncate(DateTime t) =>
        new(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);

    private async Task SeedLogAsync(string collector, DateTime collectionTimeUtc, string status)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = 100 });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        cmd.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 80 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 20 });
        await cmd.ExecuteNonQueryAsync();
    }
}
