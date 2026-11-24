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
using PerformanceMonitorLite.Tests.Helpers;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8570: the readable-secondary facts count the databases of the newest <c>query_store_health</c> capture only. An older capture
/// does not count, tempdb and a database whose Query Store is OFF are left out of both counts, and a server with no capture reads
/// as no role and no databases.
/// </summary>
public sealed class ReadableSecondaryFactsReadTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 85701;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private long _nextId = -1;

    public ReadableSecondaryFactsReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
    }

    [Fact]
    public async Task TheFacts_CountTheNewestCaptureOnly_AndLeaveOutTempdbAndOffDatabases()
    {
        var older = DateTime.UtcNow.AddHours(-2);
        var newest = DateTime.UtcNow.AddMinutes(-5);
        foreach (var name in new[] { "Old1", "Old2", "Old3" })
        {
            await SeedHealthAsync(older, name, "READ_ONLY", 8);
        }

        await SeedHealthAsync(newest, "tempdb", "READ_WRITE", 8);
        await SeedHealthAsync(newest, "RwDb", "READ_WRITE", 0);
        await SeedHealthAsync(newest, "SecDb", "READ_ONLY", 8);
        await SeedHealthAsync(newest, "OffDb", "OFF", 8);

        var (role, databases, secondaries) = await _dataService.GetReadableSecondaryFactsAsync(ServerId);

        Assert.Null(role);
        Assert.Equal(2, databases);
        Assert.Equal(1, secondaries);

        var none = await _dataService.GetReadableSecondaryFactsAsync(ServerId + 1);
        Assert.Equal((null, 0, 0), none);
    }

    private async Task SeedHealthAsync(DateTime captureTime, string databaseName, string actualState, int readonlyReason)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO query_store_health
            (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state,
             readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode,
             stale_query_threshold_days, max_plans_per_query, interval_length_minutes)
            VALUES ($1, $2, $3, 'TestServer', $4, $5, 'READ_WRITE', $6, 0, 1000, 'AUTO', 30, 200, 60)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = captureTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = databaseName });
        cmd.Parameters.Add(new DuckDBParameter { Value = actualState });
        cmd.Parameters.Add(new DuckDBParameter { Value = readonlyReason });
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}
