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
/// #7242, Lite half: the activity collectors' 72 hour quiet bar is observed time, the sum of the real gaps between the zero-row runs,
/// each gap counted at most three intervals (an hour at least). Real DuckDB, real reader.
/// A collector that ran the whole 75 hours with its runs spaced out to 2.5 times its interval is flagged (runs times the interval
/// read it as 30 hours and hid the stop). A service stopped for 56 of the 75 hours has one long gap, which counts only the cap, so
/// it is not flagged.
/// </summary>
public sealed class CollectionHealthObservedQuietGapsReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 7242;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;

    public CollectionHealthObservedQuietGapsReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    [Fact]
    public async Task SpacedOutRuns_AreFlagged_AStoppedStretchIsNot()
    {
        var service = new LocalDataService(_duckDb);
        var now = Whole(DateTime.UtcNow);

        /* query_stats runs every minute. Runs 150 seconds apart (2.5 times the interval) for the whole 75 hours, no gap over the cap. */
        await SeedAsync("query_stats", 1_000_000, now.AddHours(-76), count: 1, stepSeconds: 150, rowsCollected: 5);
        await SeedAsync("query_stats", 2_000_000, now.AddHours(-75), count: 75 * 3600 / 150, stepSeconds: 150, rowsCollected: 0);

        /* waiting_tasks runs every minute. The service stopped for 56 hours, then ran 19 hours of one minute runs. */
        await SeedAsync("waiting_tasks", 3_000_000, now.AddHours(-75), count: 1, stepSeconds: 60, rowsCollected: 5);
        await SeedAsync("waiting_tasks", 4_000_000, now.AddHours(-19), count: 19 * 60, stepSeconds: 60, rowsCollected: 0);

        var rows = await service.GetCollectionHealthAsync(ServerId);
        var spaced = rows.Single(r => r.CollectorName == "query_stats");
        var stopped = rows.Single(r => r.CollectorName == "waiting_tasks");

        /* The run count alone times the interval is 30 hours for the spaced collector, the defect. */
        Assert.Equal(1_800, spaced.TrailingZeroRowSuccessRuns);
        Assert.True(spaced.ObservedQuietGaps!.Minutes(spaced.FrequencyMinutes) > 74 * 60);
        Assert.True(spaced.ProducedThenStopped);

        Assert.Equal(19 * 60, stopped.TrailingZeroRowSuccessRuns);
        Assert.InRange(stopped.ObservedQuietGaps!.Minutes(stopped.FrequencyMinutes), 19 * 60, 20 * 60 + 2);
        Assert.False(stopped.ProducedThenStopped);
    }

    private static DateTime Whole(DateTime t) =>
        new(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);

    /// <summary><paramref name="count"/> SUCCESS runs, <paramref name="stepSeconds"/> apart, the first at <paramref name="firstUtc"/>.</summary>
    private async Task SeedAsync(string collector, long firstLogId, DateTime firstUtc, int count, int stepSeconds, int rowsCollected)
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
SELECT $1::BIGINT + i, $2, 'TestSrv', $3, CAST($4 AS TIMESTAMP) + to_seconds(CAST(i * $5 AS BIGINT)), 100, 'SUCCESS', NULL, $6, 50, 50
FROM range($7::BIGINT) AS t(i)";
        cmd.Parameters.Add(new DuckDBParameter { Value = firstLogId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = firstUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = (long)stepSeconds });
        cmd.Parameters.Add(new DuckDBParameter { Value = rowsCollected });
        cmd.Parameters.Add(new DuckDBParameter { Value = (long)count });
        await cmd.ExecuteNonQueryAsync();
    }
}
