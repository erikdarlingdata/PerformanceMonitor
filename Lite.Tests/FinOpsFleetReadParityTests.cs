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

    /// <summary>One query_stats sample per UTC day for the last <paramref name="days"/> days (today first), on a database no size snapshot lists, so the idle-coverage rule is met without making any listed database active.</summary>
    private static void SeedQueryStatsCoverage(DuckDBConnection conn, int serverId, string serverName, DateTime now, ref long nextId, int days = 7)
    {
        /* A full-coverage seed (7 or more days) starts yesterday and reaches 8 days back: the oldest sample must be at least 7 days old and each complete day before today must hold one. A short seed (the gap case) starts now. */
        var start = days >= 7 ? 1 : 0;
        for (var d = start; d < start + (days >= 7 ? days + 1 : days); d++)
            Exec(conn, @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash,
                          delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads)
                         VALUES ($1,$2,$3,$4,'DbElsewhere','0xE',1,10,20,1)", nextId--, now.AddDays(-d), serverId, serverName);
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
        using var initializer = new DuckDbInitializer(_dbPath);
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
        using var initializer = new DuckDbInitializer(_dbPath);
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
            SeedQueryStatsCoverage(conn, busyServerId, "BUSY", now, ref nextId);
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
        Assert.Null(empty.ProvisioningStatus); // No CPU sample in the window: no verdict, not OVER_PROVISIONED from zeros.
    }

    /// <summary>
    /// The fleet "Idle DBs" count follows the recommendation row's rule: a database is idle for 7 days only when query stats hold a
    /// sample on each of the 7 complete UTC days before today AND the oldest sample is at least 7 days old. A store that was closed for
    /// days (samples only today and yesterday) reads NULL (a dash), not a count of every database; so does one whose samples reach every
    /// date but start only 6 days and a few minutes back; a covered server with nothing idle reads 0, not NULL, even with no sample yet
    /// today.
    /// </summary>
    [Fact]
    public async Task ServerMetrics_IdleDbCount_NeedsSevenDaysOfQueryStatsCoverage()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        const int gapServerId = 50;
        const int busyServerId = 60;
        const int youngServerId = 70;
        var now = DateTime.UtcNow;
        long nextId = -1;

        using (var conn = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await conn.OpenAsync();
            foreach (var (serverId, name) in new[] { (gapServerId, "GAP"), (busyServerId, "BUSYALL"), (youngServerId, "YOUNG") })
            {
                SeedServerProperties(conn, serverId, name, nextId--, now);
                Exec(conn, @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id,
                              file_id, file_type_desc, file_name, physical_name, total_size_mb, used_size_mb)
                             VALUES ($1,$2,$3,$4,'DbOne',1,1,'ROWS','a.mdf','a.mdf',20480,8000)", nextId--, now, serverId, name);
            }

            // GAP: samples today and yesterday only (the app was closed before that), DbOne never ran in them.
            SeedQueryStatsCoverage(conn, gapServerId, "GAP", now, ref nextId, days: 2);

            // BUSYALL: the oldest sample is 8 days old and each of the 7 complete days before today holds one (none yet today), and DbOne
            // is the database that ran, so nothing is idle.
            for (var d = 1; d <= 8; d++)
                Exec(conn, @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash,
                              delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads)
                             VALUES ($1,$2,$3,$4,'DbOne','0xB',5,10,20,1)", nextId--, now.AddDays(-d), busyServerId, "BUSYALL");

            // YOUNG: a sample on each of the 7 dates from 7 days back to yesterday, but the first is at 23:59:30 of the 7th day back, so the
            // oldest sample is not yet 7 days old (6 days and a few minutes at most times of day). Seven dates are covered; the history is not.
            Exec(conn, @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash,
                          delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads)
                         VALUES ($1,$2,$3,$4,'DbOne','0xY',5,10,20,1)", nextId--, now.Date.AddDays(-7).AddHours(23).AddMinutes(59).AddSeconds(30), youngServerId, "YOUNG");
            for (var d = 1; d <= 6; d++)
                Exec(conn, @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash,
                              delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads)
                             VALUES ($1,$2,$3,$4,'DbOne','0xY',5,10,20,1)", nextId--, now.Date.AddDays(-d).AddHours(12), youngServerId, "YOUNG");
        }

        var metrics = await new LocalDataService(initializer).GetServerMetricsAsync();

        Assert.Null(metrics[gapServerId].IdleDbCount);
        Assert.Equal(0, metrics[busyServerId].IdleDbCount);
        Assert.Null(metrics[youngServerId].IdleDbCount);
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
        using var initializer = new DuckDbInitializer(_dbPath);
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
                SeedQueryStatsCoverage(conn, serverId, name, now, ref nextId);
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

    /// <summary>
    /// A server with collected properties and database sizes but NO CPU sample in the 24-hour window gets no
    /// verdict. The read used to turn its missing average into 0% CPU, and <c>Evaluate</c> called it
    /// OVER_PROVISIONED — on real data every such server was told to shrink. A CPU sample older than the
    /// window does not count, and a server with low CPU inside the window still gets OVER_PROVISIONED.
    /// </summary>
    [Fact]
    public async Task ServerMetrics_ServerWithNoCpuSampleInTheWindow_GetsNoVerdict_ALowCpuServerStillGetsOverProvisioned()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        const int quietServerId = 50;
        const int noCpuServerId = 60;
        var now = DateTime.UtcNow;
        long nextId = -1;

        using (var conn = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await conn.OpenAsync();

            foreach (var (serverId, name) in new[] { (quietServerId, "QUIET"), (noCpuServerId, "NOCPU") })
            {
                SeedServerProperties(conn, serverId, name, nextId--, now);
                Exec(conn, @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id,
                              file_id, file_type_desc, file_name, physical_name, total_size_mb, used_size_mb)
                             VALUES ($1,$2,$3,$4,'DbOne',1,1,'ROWS','a.mdf','a.mdf',20480,8000)", nextId--, now, serverId, name);
            }

            foreach (var cpu in new[] { 4, 6, 8 })
            {
                Exec(conn, @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
                             VALUES ($1,$2,$3,$4,$2,$5,1)", nextId--, now.AddHours(-1), quietServerId, "QUIET", cpu);
            }

            // The no-CPU server's only sample is three days old: outside the window, so it is not a sample here.
            Exec(conn, @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
                         VALUES ($1,$2,$3,$4,$2,5,1)", nextId--, now.AddDays(-3), noCpuServerId, "NOCPU");
        }

        var metrics = await new LocalDataService(initializer).GetServerMetricsAsync();

        var quiet = metrics[quietServerId];
        Assert.Equal(6m, quiet.AvgCpuPct);
        Assert.Equal(PerformanceMonitor.Common.ProvisioningVerdict.OverProvisioned, quiet.ProvisioningStatus);

        var noCpu = metrics[noCpuServerId];
        Assert.Null(noCpu.AvgCpuPct);
        Assert.Equal(20m, noCpu.StorageTotalGb); // The size rows are still read; only the verdict needs CPU.
        Assert.Null(noCpu.ProvisioningStatus);
    }

    /// <summary>
    /// The drill-down's verdict follows the same rule. A server with a memory sample but no CPU sample in
    /// the window comes back with an EMPTY status (the tab shows "No Data"), where it used to read 0% CPU
    /// and say OVER_PROVISIONED; a server with low CPU still gets OVER_PROVISIONED.
    /// </summary>
    [Fact]
    public async Task UtilizationEfficiency_ServerWithNoCpuSampleInTheWindow_GetsNoVerdict_ALowCpuServerStillGetsOverProvisioned()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        const int quietServerId = 70;
        const int noCpuServerId = 80;
        var now = DateTime.UtcNow;
        long nextId = -1;

        using (var conn = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await conn.OpenAsync();

            foreach (var (serverId, name) in new[] { (quietServerId, "QUIET"), (noCpuServerId, "NOCPU") })
            {
                Exec(conn, @"INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name,
                              total_physical_memory_mb, available_physical_memory_mb, target_server_memory_mb, total_server_memory_mb, buffer_pool_mb)
                             VALUES ($1,$2,$3,$4,16384,8192,12288,12000,10000)", nextId--, now.AddHours(-1), serverId, name);
            }

            foreach (var cpu in new[] { 4, 6, 8 })
            {
                Exec(conn, @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
                             VALUES ($1,$2,$3,$4,$2,$5,1)", nextId--, now.AddHours(-1), quietServerId, "QUIET", cpu);
            }
        }

        var svc = new LocalDataService(initializer);

        var quiet = await svc.GetUtilizationEfficiencyAsync(quietServerId);
        Assert.NotNull(quiet);
        Assert.Equal(3L, quiet.CpuSamples);
        Assert.Equal(PerformanceMonitor.Common.ProvisioningVerdict.OverProvisioned, quiet.ProvisioningStatus);

        var noCpu = await svc.GetUtilizationEfficiencyAsync(noCpuServerId);
        Assert.NotNull(noCpu);
        Assert.Equal(0L, noCpu.CpuSamples);
        Assert.Equal("", noCpu.ProvisioningStatus);
    }

    /// <summary>One collected <c>server_properties</c> row, which is what makes a server known to the fleet read.
    /// The NOT NULL edition and hardware columns are filled with values the read never looks at.</summary>
    /// <summary>
    /// One server, one health score: the Server Inventory's Health column (the fleet read) and the Utilization tab's badge score the same server
    /// from the same inputs. The inventory used to score the 24-hour average CPU against a fixed memory and storage term, so servers with
    /// different buffer pools and free space all read the same number beside a Utilization score that differed.
    /// </summary>
    [Fact]
    public async Task ServerMetrics_HealthScore_EqualsTheUtilizationScore_PerServer()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        var now = DateTime.UtcNow;
        long nextId = -1;
        // (id, name, cpu samples, physical MB, buffer pool MB, total MB, used MB)
        var servers = new (int Id, string Name, int[] Cpu, int PhysMb, int BpMb, int TotalMb, int UsedMb)[]
        {
            (101, "HEALTHY", [10, 12, 14], 65536, 40000, 100000, 40000),
            (102, "STRAINED", [60, 85, 95], 16384, 15800, 100000, 97000),
        };

        using (var conn = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await conn.OpenAsync();
            foreach (var sv in servers)
            {
                SeedServerProperties(conn, sv.Id, sv.Name, nextId--, now);
                foreach (var cpu in sv.Cpu)
                    Exec(conn, @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
                                 VALUES ($1,$2,$3,$4,$2,$5,1)", nextId--, now.AddHours(-1), sv.Id, sv.Name, cpu);
                Exec(conn, @"INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name,
                              total_physical_memory_mb, available_physical_memory_mb, target_server_memory_mb, total_server_memory_mb, buffer_pool_mb)
                             VALUES ($1,$2,$3,$4,$5,1000,$5,$5,$6)", nextId--, now, sv.Id, sv.Name, sv.PhysMb, sv.BpMb);
                Exec(conn, @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id,
                              file_id, file_type_desc, file_name, physical_name, total_size_mb, used_size_mb)
                             VALUES ($1,$2,$3,$4,'DbOne',1,1,'ROWS','a.mdf','a.mdf',$5,$6)", nextId--, now, sv.Id, sv.Name, sv.TotalMb, sv.UsedMb);
            }
        }

        var svc = new LocalDataService(initializer);
        var fleet = await svc.GetServerMetricsAsync();
        var inventoryScores = new System.Collections.Generic.List<int?>();

        foreach (var sv in servers)
        {
            var util = (await svc.GetUtilizationEfficiencyAsync(sv.Id))!;
            var sizes = await svc.GetDatabaseSizeLatestAsync(sv.Id);
            util.FreeSpacePct = FinOpsHealthCalculator.FreeSpacePct(DatabaseSizeRow.AllocatedTotalMb(sizes), DatabaseSizeRow.FreeTotalMb(sizes));

            Assert.Equal(util.ComputeHealthScore(), fleet[sv.Id].HealthScore);
            inventoryScores.Add(fleet[sv.Id].HealthScore);
        }

        Assert.NotEqual(inventoryScores[0], inventoryScores[1]);
    }

    private static void SeedServerProperties(DuckDBConnection conn, int serverId, string serverName, long collectionId, DateTime collectionTime) =>
        Exec(conn, @"INSERT INTO server_properties (collection_id, collection_time, server_id, server_name,
                      edition, product_version, product_level, engine_edition, cpu_count, hyperthread_ratio, physical_memory_mb)
                     VALUES ($1,$2,$3,$4,'Test Edition','16.0.4150.1','RTM',3,8,1,16384)",
            collectionId, collectionTime, serverId, serverName);
}
