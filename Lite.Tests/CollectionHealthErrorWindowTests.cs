/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8533: the Collection Cycles card's connection-failure sentence says "in the last 7 days" of each collector's error count and
/// newest error. Those columns come from the Collection Health rows read, and this pins that they stop at seven days: an ERROR
/// run just inside the window reaches <c>ErrorCount</c>, <c>LastErrorTime</c> and <c>LastError</c>, and one just outside it
/// reaches none of them.
/// </summary>
public sealed class CollectionHealthErrorWindowTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 8533;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;

    public CollectionHealthErrorWindowTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    [Fact]
    public async Task AnErrorRunJustInsideSevenDays_ReachesTheErrorColumns_AndOneJustOutside_DoesNot()
    {
        var service = new LocalDataService(_duckDb);
        var now = Whole(DateTime.UtcNow);
        var inside = now.AddDays(-7).AddHours(1);
        var outside = now.AddDays(-7).AddHours(-1);

        await SeedAsync("wait_stats", 1, outside, "ERROR", "the run outside the window");
        await SeedAsync("wait_stats", 2, inside, "ERROR", "the run inside the window");
        await SeedAsync("wait_stats", 3, now.AddMinutes(-5), "SUCCESS", null);
        await SeedAsync("query_stats", 4, outside, "ERROR", "the run outside the window");
        await SeedAsync("query_stats", 5, now.AddMinutes(-5), "SUCCESS", null);

        var rows = await service.GetCollectionHealthAsync(ServerId);

        var both = rows.Single(row => row.CollectorName == "wait_stats");
        Assert.Equal(1L, both.ErrorCount);
        Assert.Equal(inside, both.LastErrorTime);
        Assert.Equal("the run inside the window", both.LastError);

        var outsideOnly = rows.Single(row => row.CollectorName == "query_stats");
        Assert.Equal(0L, outsideOnly.ErrorCount);
        Assert.Null(outsideOnly.LastErrorTime);
        Assert.True(string.IsNullOrEmpty(outsideOnly.LastError), $"an error from outside the window is still the row's last error: {outsideOnly.LastError}");
    }

    private static DateTime Whole(DateTime t) =>
        new(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);

    private async Task SeedAsync(string collector, long logId, DateTime collectionTimeUtc, string status, string? error)
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
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)error ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }
}
