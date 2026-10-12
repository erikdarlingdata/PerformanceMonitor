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
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5646, Lite half: the Collection Health read hands the classifier how many of a collector's newest runs in a
/// row succeeded. Newest first on the stored timestamp (DuckDB keeps microseconds), then the higher log_id, so
/// two runs at the same microsecond are ordered the same way every time. Real DuckDB, real reader.
/// </summary>
public sealed class CollectionHealthRecoveryReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 4646;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;

    public CollectionHealthRecoveryReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    [Fact]
    public async Task ARecoveredCollector_ReadsHealthy_WithTheRecoveredNoteAndTheLastFailureTime()
    {
        var service = new LocalDataService(_duckDb);
        var start = Whole(DateTime.UtcNow.AddMinutes(-120));
        long id = 1;

        /* 14 runs: four errors early, then ten successes. 4 of 14 is 28%, over the 20% line. */
        for (var i = 0; i < 4; i++)
        {
            await SeedAsync("query_store", id++, start.AddMinutes(i * 5), "ERROR");
        }

        DateTime lastError = start.AddMinutes(15);
        for (var i = 4; i < 14; i++)
        {
            await SeedAsync("query_store", id++, start.AddMinutes(i * 5), "SUCCESS");
        }

        var row = await ReadAsync(service, "query_store");

        Assert.Equal(14, row.TotalRuns);
        Assert.Equal(4, row.ErrorCount);
        Assert.Equal(3, row.TrailingSuccessRuns);
        Assert.Equal(CollectorHealthClassifier.Healthy, row.HealthStatus);
        Assert.Contains("recovered", row.NoteFormatted, StringComparison.Ordinal);
        Assert.Contains(lastError.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture), row.NoteFormatted, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TwoSuccessesAfterFailures_StaysWarning()
    {
        var service = new LocalDataService(_duckDb);
        var start = Whole(DateTime.UtcNow.AddMinutes(-120));
        long id = 1;

        for (var i = 0; i < 4; i++)
        {
            await SeedAsync("query_store", id++, start.AddMinutes(i * 5), "ERROR");
        }

        await SeedAsync("query_store", id++, start.AddMinutes(20), "SUCCESS");
        await SeedAsync("query_store", id++, start.AddMinutes(25), "SUCCESS");
        for (var i = 6; i < 8; i++)
        {
            await SeedAsync("query_store", id++, start.AddMinutes(i * 5), "ERROR");
        }

        await SeedAsync("query_store", id++, start.AddMinutes(40), "SUCCESS");
        await SeedAsync("query_store", id++, start.AddMinutes(45), "SUCCESS");

        var row = await ReadAsync(service, "query_store");

        Assert.Equal(10, row.TotalRuns);
        Assert.Equal(2, row.TrailingSuccessRuns);
        Assert.Equal(CollectorHealthClassifier.Warning, row.HealthStatus);
    }

    [Fact]
    public async Task AnAlternatingCollector_NeverHasThreeInARow_StaysWarning()
    {
        var service = new LocalDataService(_duckDb);
        var start = Whole(DateTime.UtcNow.AddMinutes(-120));

        for (var i = 0; i < 12; i++)
        {
            await SeedAsync("query_store", i + 1, start.AddMinutes(i * 5), i % 2 == 0 ? "ERROR" : "SUCCESS");
        }

        var row = await ReadAsync(service, "query_store");

        Assert.Equal(1, row.TrailingSuccessRuns);
        Assert.Equal(CollectorHealthClassifier.Warning, row.HealthStatus);
    }

    [Fact]
    public async Task TwoRunsAtTheSameMicrosecond_AreOrderedByTheHigherLogId_EveryTime()
    {
        var service = new LocalDataService(_duckDb);
        var t = Whole(DateTime.UtcNow.AddMinutes(-30)).AddTicks(123_456 * 10L);

        /* Three older successes, then two runs at the SAME microsecond. The row with the higher log_id is the
           newest. The higher id is inserted FIRST so insertion order cannot pass this by accident. */
        await SeedAsync("a_error_newest", 1, t.AddMinutes(-3), "SUCCESS");
        await SeedAsync("a_error_newest", 2, t.AddMinutes(-2), "SUCCESS");
        await SeedAsync("a_error_newest", 3, t.AddMinutes(-1), "SUCCESS");
        await SeedAsync("a_error_newest", 11, t, "ERROR");
        await SeedAsync("a_error_newest", 10, t, "SUCCESS");

        await SeedAsync("b_success_newest", 21, t.AddMinutes(-3), "SUCCESS");
        await SeedAsync("b_success_newest", 22, t.AddMinutes(-2), "SUCCESS");
        await SeedAsync("b_success_newest", 23, t.AddMinutes(-1), "SUCCESS");
        await SeedAsync("b_success_newest", 31, t, "SUCCESS");
        await SeedAsync("b_success_newest", 30, t, "ERROR");

        /* One microsecond apart: time decides, whatever the ids say. */
        await SeedAsync("c_micro", 41, t.AddMinutes(-1), "SUCCESS");
        await SeedAsync("c_micro", 42, t, "SUCCESS");
        await SeedAsync("c_micro", 40, t.AddTicks(10), "ERROR");

        var rows = await service.GetCollectionHealthAsync(ServerId);

        Assert.Equal(0, rows.Single(r => r.CollectorName == "a_error_newest").TrailingSuccessRuns);
        Assert.Equal(1, rows.Single(r => r.CollectorName == "b_success_newest").TrailingSuccessRuns);
        Assert.Equal(0, rows.Single(r => r.CollectorName == "c_micro").TrailingSuccessRuns);
    }

    private async Task<CollectorHealthRow> ReadAsync(LocalDataService service, string collector) =>
        (await service.GetCollectionHealthAsync(ServerId)).Single(h => h.CollectorName == collector);

    private static DateTime Whole(DateTime t) =>
        new(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);

    private async Task SeedAsync(string collector, long logId, DateTime collectionTimeUtc, string status)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _seedConn ??= _duckDb.CreateConnection();
        if (_seedConn.State != System.Data.ConnectionState.Open)
        {
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, 'TestSrv', $3, $4, 100, $5, $6, 1, 50, 50)";
        cmd.Parameters.Add(new DuckDBParameter { Value = logId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        cmd.Parameters.Add(new DuckDBParameter { Value = status == "ERROR" ? "boom" : (object)DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }
}
