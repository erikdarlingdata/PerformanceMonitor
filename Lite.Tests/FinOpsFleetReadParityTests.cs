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
/// #4227 Lite parity: pins the two FinOps reads this issue merged into one statement each, so the merge cannot
/// silently change which rows a grid shows. Own isolated DuckDB (needs several distinct server_ids, which the
/// shared fixture's single TestServerId doesn't give).
/// </summary>
public class FinOpsFleetReadParityTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;

    public FinOpsFleetReadParityTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteParity_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "parity.duckdb");
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    private static void Exec(DuckDBConnection conn, string sql, params object[] vals)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in vals) cmd.Parameters.Add(new DuckDBParameter { Value = v });
        cmd.ExecuteNonQuery();
    }

    /// <summary>
    /// The merged statement must keep each old grid's own filter: ByTotal drops a NULL database_name (the old
    /// WHERE c.database_name IS NOT NULL) but keeps a zero-execution database; ByAvg keeps a NULL-name database
    /// but drops a zero-execution one (the old HAVING SUM(delta_execution_count) > 0).
    /// </summary>
    [Fact]
    public async Task TopResourceConsumers_KeepsEachGridsOwnFilter()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        const int serverId = 1;
        var now = DateTime.UtcNow;
        long nextId = -1;

        using (var conn = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await conn.OpenAsync();

            // DbActive: 100 executions, real CPU -> both grids.
            Exec(conn, @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash,
                          delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads)
                         VALUES ($1,$2,$3,$4,'DbActive','0xA',100,500000,900000,1000)",
                nextId--, now, serverId, "SRV1");

            // DbZeroExec: delta_worker_time present but 0 executions -> ByTotal keeps it (name not null),
            // ByAvg drops it (old HAVING SUM(delta_execution_count) > 0).
            Exec(conn, @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash,
                          delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads)
                         VALUES ($1,$2,$3,$4,'DbZeroExec','0xB',0,200000,300000,500)",
                nextId--, now, serverId, "SRV1");

            // Unattributed (NULL database_name): ByTotal drops it (old WHERE database_name IS NOT NULL),
            // ByAvg keeps it (old ByAvg statement never filtered NULL names).
            Exec(conn, @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash,
                          delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads)
                         VALUES ($1,$2,$3,$4,NULL,'0xC',50,900000,100000,200)",
                nextId--, now, serverId, "SRV1");
        }

        var svc = new LocalDataService(initializer);
        var (byTotal, byAvg) = await svc.GetTopResourceConsumersAsync(serverId, hoursBack: 24, topN: 10);

        Assert.Equal(new[] { "DbActive", "DbZeroExec" }, byTotal.Select(r => r.DatabaseName).OrderBy(n => n));
        Assert.Equal(new[] { "", "DbActive" }, byAvg.Select(r => r.DatabaseName).OrderBy(n => n));

        // ByTotal ranks by total CPU time descending. delta_worker_time is microseconds; cpu_time_ms = /1000.0.
        Assert.Equal("DbActive", byTotal[0].DatabaseName);
        Assert.Equal(500L, byTotal[0].CpuTimeMs);

        // ByAvg's CpuTimeMs field carries the AVERAGE (500 total ms / 100 executions = 5), not the total.
        var active = byAvg.Single(r => r.DatabaseName == "DbActive");
        Assert.Equal(5L, active.CpuTimeMs);
        Assert.Equal(500L, active.TotalCpuTimeMs);
    }

    /// <summary>
    /// The fleet statement must return every server with a collected <c>server_properties</c> row, not just the
    /// ones with rows in the five source tables — a server with none still gets an all-null-metrics entry (the
    /// old per-server statement's anchor row, which always produced exactly one row). Also pins the idle-database EXCEPT:
    /// a database seen in the latest size snapshot but absent from 7-day query_stats activity counts as idle.
    /// </summary>
    [Fact]
    public async Task ServerMetrics_FleetStatement_CoversEveryServer_AndFlagsIdleDatabases()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        const int busyServerId = 10;
        const int emptyServerId = 20;
        var now = DateTime.UtcNow;
        long nextId = -1;

        using (var conn = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await conn.OpenAsync();
            SeedServerProperties(conn, busyServerId, "BUSY", nextId--, now);
            SeedServerProperties(conn, emptyServerId, "EMPTY", nextId--, now);

            Exec(conn, @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
                         VALUES ($1,$2,$3,$4,$2,55,5)", nextId--, now, busyServerId, "BUSY");

            // Two databases at the latest size snapshot; only DbActive shows up in 7-day query_stats.
            Exec(conn, @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id,
                          file_id, file_type_desc, file_name, physical_name, total_size_mb, used_size_mb)
                         VALUES ($1,$2,$3,$4,'DbActive',1,1,'ROWS','a.mdf','a.mdf',10240,8000)", nextId--, now, busyServerId, "BUSY");
            Exec(conn, @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id,
                          file_id, file_type_desc, file_name, physical_name, total_size_mb, used_size_mb)
                         VALUES ($1,$2,$3,$4,'DbIdle',2,1,'ROWS','b.mdf','b.mdf',51200,40000)", nextId--, now, busyServerId, "BUSY");

            Exec(conn, @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash,
                          delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads)
                         VALUES ($1,$2,$3,$4,'DbActive','0xA',10,1000,2000,100)", nextId--, now.AddDays(-1), busyServerId, "BUSY");
        }

        var svc = new LocalDataService(initializer);
        var metrics = await svc.GetServerMetricsAsync();

        Assert.True(metrics.ContainsKey(busyServerId));
        Assert.True(metrics.ContainsKey(emptyServerId));

        var busy = metrics[busyServerId];
        Assert.Equal(55m, busy.AvgCpuPct);
        Assert.Equal(1, busy.IdleDbCount); // DbIdle only — DbActive is excluded by the 7-day activity check.
        Assert.NotNull(busy.ProvisioningStatus);

        var empty = metrics[emptyServerId];
        Assert.Null(empty.AvgCpuPct);
        Assert.Null(empty.StorageTotalGb);
        Assert.Null(empty.IdleDbCount);
        Assert.NotNull(empty.ProvisioningStatus); // Evaluate() always returns a verdict, even from all zeros.
    }

    /// <summary>
    /// The overlay must not depend on the <c>servers</c> table: Lite never inserts into it, so a read driven
    /// from it returned an empty dictionary on every real store and the Server Inventory never got its
    /// collected CPU, storage and idle-database figures. With CPU, size and <c>server_properties</c> rows and
    /// the <c>servers</c> table EMPTY, the server's metrics come back; a server removed from the monitor
    /// whose rows have not aged out yet comes back too (the inventory looks servers up from its own list,
    /// which <c>FinOpsServerInventoryTests</c> pins).
    /// </summary>
    [Fact]
    public async Task ServerMetrics_AreReadWithNoServersRow_FromCollectedServerProperties()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        const int liveServerId = 30;
        const int removedServerId = 40;
        var now = DateTime.UtcNow;
        long nextId = -1;

        using (var conn = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await conn.OpenAsync();

            foreach (var (serverId, name) in new[] { (liveServerId, "LIVE"), (removedServerId, "REMOVED") })
            {
                SeedServerProperties(conn, serverId, name, nextId--, now);
                Exec(conn, @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
                             VALUES ($1,$2,$3,$4,$2,40,5)", nextId--, now, serverId, name);
                Exec(conn, @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id,
                              file_id, file_type_desc, file_name, physical_name, total_size_mb, used_size_mb)
                             VALUES ($1,$2,$3,$4,'DbOne',1,1,'ROWS','a.mdf','a.mdf',20480,8000)", nextId--, now, serverId, name);
            }

            using var count = conn.CreateCommand();
            count.CommandText = "SELECT COUNT(*) FROM servers";
            Assert.Equal(0L, Convert.ToInt64(count.ExecuteScalar()));
        }

        var metrics = await new LocalDataService(initializer).GetServerMetricsAsync();

        Assert.True(metrics.TryGetValue(liveServerId, out var live));
        Assert.Equal(40m, live.AvgCpuPct);
        Assert.Equal(20m, live.StorageTotalGb); // 20480 MB of one database at the latest snapshot.
        Assert.Equal(1, live.IdleDbCount);      // DbOne has no 7-day query_stats activity.
        Assert.NotNull(live.ProvisioningStatus);

        Assert.True(metrics.ContainsKey(removedServerId));
    }

    /// <summary>One collected <c>server_properties</c> row, which is what makes a server known to the fleet read.
    /// The NOT NULL edition and hardware columns are filled with values the read never looks at.</summary>
    private static void SeedServerProperties(DuckDBConnection conn, int serverId, string serverName, long collectionId, DateTime collectionTime) =>
        Exec(conn, @"INSERT INTO server_properties (collection_id, collection_time, server_id, server_name,
                      edition, product_version, product_level, engine_edition, cpu_count, hyperthread_ratio, physical_memory_mb)
                     VALUES ($1,$2,$3,$4,'Test Edition','16.0.4150.1','RTM',3,8,1,16384)",
            collectionId, collectionTime, serverId, serverName);
}
