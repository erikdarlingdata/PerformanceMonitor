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
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7906: the FinOps utilization read's four memory grant figures (peak waiters, timeouts, forced grants, peak granted-over-target)
/// are null when the window holds no <c>memory_grant_stats</c> row, so a server whose grant collector is off, gated by the engine
/// edition or not yet run does not read as the 0 waiters and 0% utilization a healthy idle server reads. A server with a sample keeps
/// its figures. The Darling.Tests twin pins the same contract on the other store and on the get_finops wire.
/// </summary>
public sealed class FinOpsUtilizationGrantFiguresTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -790_601;
    private const string ServerName = "GrantFiguresSrv";

    private readonly SharedDuckDbFixture _fixture;
    private DuckDBConnection? _seedConn;

    public FinOpsUtilizationGrantFiguresTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _fixture = fixture;
    }

    public void Dispose() => _seedConn?.Dispose();

    [Fact]
    public async Task UtilizationRead_WithNoGrantSample_LeavesTheFourGrantFiguresNull()
    {
        await SeedAsync(withGrantSample: false);

        var row = await new LocalDataService(_fixture.DuckDb).GetUtilizationEfficiencyAsync(ServerId);

        Assert.NotNull(row);
        /* Boxed: a nullable figure with no value reads as null here, and a number of any kind does not. */
        object? waiters = row!.MaxGrantWaiters, timeouts = row.GrantTimeouts, forced = row.ForcedGrants, utilization = row.GrantUtilizationPct;
        Assert.Null(waiters);
        Assert.Null(timeouts);
        Assert.Null(forced);
        Assert.Null(utilization);
    }

    [Fact]
    public async Task UtilizationRead_WithAGrantSample_KeepsTheFourGrantFigures()
    {
        await SeedAsync(withGrantSample: true);

        var row = await new LocalDataService(_fixture.DuckDb).GetUtilizationEfficiencyAsync(ServerId);

        Assert.NotNull(row);
        Assert.Equal(3L, Convert.ToInt64(row!.MaxGrantWaiters));
        Assert.Equal(1L, Convert.ToInt64(row.GrantTimeouts));
        Assert.Equal(2L, Convert.ToInt64(row.ForcedGrants));
        /* 250 MB granted of a 1,000 MB target. */
        Assert.Equal(25m, Convert.ToDecimal(row.GrantUtilizationPct));
    }

    // ── seeding ──

    /// <summary>One memory snapshot an hour ago, so the read has a row to answer, and with <paramref name="withGrantSample"/> one
    /// regular-semaphore grant snapshot beside it: 3 waiters, 1 timeout, 2 forced grants, 250 of 1,000 MB granted.</summary>
    private async Task SeedAsync(bool withGrantSample)
    {
        using var readLock = _fixture.DuckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _fixture.DuckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        var at = DateTime.UtcNow.AddHours(-1);
        using (var cmd = _seedConn.CreateCommand())
        {
            cmd.CommandText = @"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb,
     target_server_memory_mb, total_server_memory_mb, buffer_pool_mb, max_workers_count, current_workers_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
            void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
            P(-790_601L); P(at); P(ServerId); P(ServerName); P(8_192L); P(4_000L); P(4_000L); P(3_000L); P(2_000L); P(512); P(50);
            await cmd.ExecuteNonQueryAsync();
        }

        if (!withGrantSample) return;

        using (var cmd = _seedConn.CreateCommand())
        {
            cmd.CommandText = @"
INSERT INTO memory_grant_stats
    (collection_id, collection_time, server_id, server_name, target_memory_mb, granted_memory_mb, used_memory_mb,
     grantee_count, waiter_count, timeout_error_count_delta, forced_grant_count_delta)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
            void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
            P(-790_602L); P(at); P(ServerId); P(ServerName); P(1_000L); P(250L); P(100L); P(2); P(3); P(1L); P(2L);
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
