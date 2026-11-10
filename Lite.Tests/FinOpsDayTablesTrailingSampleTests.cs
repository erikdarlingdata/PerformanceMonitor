/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8503: the FinOps day tables keep a sample stamped past the clock the read took. Each read builds its days through that clock,
/// and the collector can stamp a sample after it was taken; a sample at or after the last day's end would otherwise match no day
/// and leave the table. Each read runs its days through the window's newest sample, so the sample lands in the row its day names,
/// and a day in between with no sample adds no row. The newer sample here is a day past the clock, far enough past the last day's
/// end to stand for any gap, so the rows are decided by the samples' days alone.
/// </summary>
public sealed class FinOpsDayTablesTrailingSampleTests : IDisposable
{
    private const int ServerId = 8503;
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "FinOpsTrailing_" + Guid.NewGuid().ToString("N")[..8]);
    private long _nextId = 1;

    public FinOpsDayTablesTrailingSampleTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task TheProvisioningTrend_KeepsASamplePastTheReadsClock()
    {
        using var duckDb = new DuckDbInitializer(Path.Combine(_dir, "trend.duckdb"));
        await duckDb.InitializeFromTemplateAsync();
        var nowUtc = DateTime.UtcNow;
        var earlier = nowUtc.AddHours(-1);
        var past = nowUtc.AddHours(25);
        await SeedCpuAsync(duckDb, earlier);
        await SeedCpuAsync(duckDb, past);

        var rows = await new LocalDataService(duckDb).GetProvisioningTrendAsync(ServerId);

        Assert.Equal([earlier.Date, past.Date], rows.Select(r => r.Day).ToArray());
    }

    [Fact]
    public async Task MemoryGrantEfficiency_KeepsASamplePastTheReadsClock()
    {
        using var duckDb = new DuckDbInitializer(Path.Combine(_dir, "grants.duckdb"));
        await duckDb.InitializeFromTemplateAsync();
        var nowUtc = DateTime.UtcNow;
        var earlier = nowUtc.AddHours(-1);
        var past = nowUtc.AddHours(25);
        await SeedGrantAsync(duckDb, earlier);
        await SeedGrantAsync(duckDb, past);

        var rows = await new LocalDataService(duckDb).GetMemoryGrantEfficiencyAsync(ServerId);

        Assert.Equal([earlier.Date, past.Date], rows.Select(r => r.Day).ToArray());
    }

    private async Task SeedCpuAsync(DuckDbInitializer duckDb, DateTime at)
    {
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, 'trailing-sample', $2, 40, 5)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedGrantAsync(DuckDbInitializer duckDb, DateTime at)
    {
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO memory_grant_stats
    (collection_id, collection_time, server_id, server_name, resource_semaphore_id, waiter_count, grantee_count,
     timeout_error_count_delta, forced_grant_count_delta, granted_memory_mb, used_memory_mb)
VALUES ($1, $2, $3, 'trailing-sample', 0, 0, 1, 0, 0, 100, 50)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        await cmd.ExecuteNonQueryAsync();
    }
}
