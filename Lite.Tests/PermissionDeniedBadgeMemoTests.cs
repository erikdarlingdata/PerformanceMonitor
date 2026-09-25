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
/// #4226 lane V1b job 4: <c>LocalDataService.GetPermissionDeniedCollectorCountAsync</c> (the Collection Health
/// badge, run on EVERY server-tab refresh) measured 46-58 ms on a seeded 20-collector/7-day/1-minute-cadence
/// DuckDB — over the ~50 ms bar worth memoizing even without Darling's fleet fan-out to amortize across.
/// Memoized per server for 20 s, the same "benignly racy" TTL shape as Darling's
/// <c>ViewerDataService.GetFleetCollectionHealthByServerAsync</c>.
/// </summary>
public sealed class PermissionDeniedBadgeMemoTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = 55;

    private readonly DuckDbInitializer _duckDb;

    public PermissionDeniedBadgeMemoTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private async Task SeedOnePermissionsRowAsync(int serverId, long logId)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
VALUES ($1, $2, 'srv', 'collector_a', now(), 'PERMISSIONS')";
        cmd.Parameters.Add(new DuckDBParameter { Value = logId });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>A second call within the TTL returns the FIRST call's answer even though the underlying data
    /// changed in between — proof the second call served from the memo rather than re-querying DuckDB. Pin
    /// fails on the pre-fix body: reverting the memo makes the second call see the newly-inserted row.</summary>
    [Fact]
    public async Task SecondCallWithinTtl_ServesTheMemoizedCount_NotAFreshScan()
    {
        var service = new LocalDataService(_duckDb);

        var before = await service.GetPermissionDeniedCollectorCountAsync(ServerId);
        Assert.Equal(0, before);

        await SeedOnePermissionsRowAsync(ServerId, 1);

        var after = await service.GetPermissionDeniedCollectorCountAsync(ServerId);
        Assert.Equal(0, after); // still memoized — a fresh scan would see 1
    }

    /// <summary>A DIFFERENT server_id is never served from another server's memo entry: seeded for server+1
    /// BEFORE either is ever read, so a cache keyed wrong (or not keyed at all) would answer server+1's first
    /// read with server's stale/absent entry instead of its own real count.</summary>
    [Fact]
    public async Task DifferentServer_IsNotServedFromAnotherServersMemo()
    {
        await SeedOnePermissionsRowAsync(ServerId + 1, 1);

        var service = new LocalDataService(_duckDb);
        var thisServer = await service.GetPermissionDeniedCollectorCountAsync(ServerId);
        Assert.Equal(0, thisServer);

        var otherServer = await service.GetPermissionDeniedCollectorCountAsync(ServerId + 1);
        Assert.Equal(1, otherServer);
    }
}
