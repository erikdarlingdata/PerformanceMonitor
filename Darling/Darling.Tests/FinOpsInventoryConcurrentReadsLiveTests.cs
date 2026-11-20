/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9149 LIVE: the Server Inventory fleet read (<see cref="DarlingFinOpsInventoryReader.GetServerMetricsAsync"/>) used to be one statement, so a cold
/// first call paid the sum of its inputs' page reads one after the other. It now runs the independent inputs (the CPU window, the server list with
/// its memory and grant columns, the sizes with the idle count, the idle coverage) at the same time on separate pooled connections and joins them by
/// server id. The answer must not change: the first test compares it to the single statement
/// (<see cref="DarlingFinOpsInventoryReader.ServerMetricsSql"/>) on a store with ties and NULLs. The second holds two inputs' tables locked and
/// checks that both reads reach the lock together, which one statement cannot do.
/// </summary>
/* #1776 own-store: every test here boots its own database through ScratchPostgres and works entirely inside
   it, so it cannot race the shared store's live collection and is left out of [Collection("live-postgres")]
   deliberately (the next sweep should not "fix" that). */
public sealed class FinOpsInventoryConcurrentReadsLiveTests
{
    private const int TimeoutSeconds = 60;

    [Fact]
    public async Task GetServerMetricsAsync_JoinedAnswer_EqualsTheSingleStatement_TiesAndNullsIncluded()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #9149 inventory equality test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
        await SeedAsync(connection, now, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);

        var joined = await DarlingFinOpsInventoryReader.GetServerMetricsAsync(dataSource, rollups, coverage, TimeoutSeconds, now, ct);
        var single = await SingleStatementAsync(connection, now, ct);

        Assert.Equal(4, single.Count);
        Assert.Equal(single.OrderBy(kv => kv.Key).ToList(), joined.OrderBy(kv => kv.Key).ToList());

        /* The fixture really holds the shapes the join has to keep: a server with every figure, one with none (all NULL), one whose databases are all
           active (idle 0, not NULL), and one without coverage (idle NULL) beside a storage total. */
        Assert.NotNull(joined[1].AvgCpuPct);
        Assert.Equal(1, joined[1].IdleDbCount);
        Assert.Null(joined[2].AvgCpuPct);
        Assert.Null(joined[2].StorageTotalGb);
        Assert.Null(joined[2].IdleDbCount);
        Assert.Equal(0, joined[3].IdleDbCount);
        Assert.NotNull(joined[4].StorageTotalGb);
        Assert.Null(joined[4].IdleDbCount);
    }

    /// <summary>
    /// The routed form: a field store has the hourly rollups built, so the idle reads go through the stitched rollup
    /// (<see cref="DarlingFinOpsInventoryReader.FleetQueriesFor"/>) and not through the raw fallback the first test covers. The four joined
    /// reads must answer what the routed single statement (<see cref="DarlingFinOpsInventoryReader.ServerMetricsSqlFor"/>) answers, with the
    /// same ties and NULLs, and the idle counts must come out of the routed reads (a database that is active only in the rollup, and one active only after the watermark).
    /// </summary>
    [Fact]
    public async Task GetServerMetricsAsync_RoutedThroughTheRollups_JoinedAnswerEqualsTheRoutedSingleStatement()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #9149 routed equality test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The routed read needs TimescaleDB: the stitched rollup only exists once the continuous aggregates are built.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await DarlingMcpTestData.ExecAsync(connection, ct, "SELECT _timescaledb_functions.stop_background_workers()");

        /* A fixed instant, so the seed does not depend on the wall clock: the idle cutoff (now - 7 days) sits 27 minutes into 2020-06-01 07:00. */
        var now = new DateTime(2020, 6, 8, 7, 33, 0, DateTimeKind.Unspecified);
        var idleCutoff = now.AddDays(-7);
        await SeedAsync(connection, now, ct);

        /* Server 1 also has a database active only in the leading partial hour (raw), server 3 one active only after the watermark (raw tail). Both
           have their size row beside the others', so they are known databases the routed reads have to find active. */
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, total_size_mb, used_size_mb) VALUES ($1, $2, 1, 'inv-a', 'DbLead', 10, 5), ($3, $4, 3, 'inv-c', 'DbTail', 10, 5)",
            CollectionIdGenerator.Next(), now.AddHours(-3), CollectionIdGenerator.Next(), now.AddHours(-5));
        await InsertActivityAsync(connection, 1, "inv-a", "DbLead", idleCutoff.AddMinutes(5), ct);
        await InsertActivityAsync(connection, 3, "inv-c", "DbTail", now.AddMinutes(-20), ct);

        var ready = await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        Assert.True(ready > 0);

        /* The legacy family covers the first days after the cutoff, the successor the days after that, up to a watermark an hour and a half short of now. */
        var legacyFloor = new DateTime(2020, 6, 1, 8, 0, 0, DateTimeKind.Unspecified);
        var successorFloor = new DateTime(2020, 6, 4, 0, 0, 0, DateTimeKind.Unspecified);
        var watermarkTarget = new DateTime(2020, 6, 8, 6, 0, 0, DateTimeKind.Unspecified);
        var disclosure = new RefreshDisclosure(message => Assert.Fail($"the refresh degraded unexpectedly: {message}"));
        await RollupBackfill.RunSliceAsync(connection, TimescaleSupport.QueryStatsDbHourlyView, legacyFloor, successorFloor, disclosure, ct);
        await RollupBackfill.RunSliceAsync(connection, TimescaleSupport.QueryStatsDbIntervalHourlyView, successorFloor, watermarkTarget, disclosure, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        Assert.True(rollups.DbGrainHourly);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);

        var liveRelation = rollups.DbGrainIntervalHourly ? TimescaleSupport.QueryStatsDbIntervalHourlyView : TimescaleSupport.QueryStatsDbHourlyView;
        var watermark = await RollupMaterializationWatermark.GetAsync(dataSource, liveRelation, TimescaleSupport.HourlyBucket, TimeoutSeconds, ct);
        Assert.NotNull(watermark);

        var joined = await DarlingFinOpsInventoryReader.GetServerMetricsAsync(dataSource, rollups, coverage, TimeoutSeconds, now, ct);
        var routedSql = DarlingFinOpsInventoryReader.ServerMetricsSqlFor(coverage, idleCutoff, watermark!.Value);
        var single = await SingleStatementAsync(connection, now, ct, routedSql);

        Assert.Equal(4, single.Count);
        Assert.Equal(single.OrderBy(kv => kv.Key).ToList(), joined.OrderBy(kv => kv.Key).ToList());

        /* The routed idle counts: server 1 keeps exactly one idle database (DbIdle) with DbBusy active in the rollup and DbLead in the leading edge,
           server 3 has none (DbOnly in the rollup, DbTail after the watermark), server 4 has no coverage (NULL), server 2 has nothing at all. */
        Assert.Equal(1, joined[1].IdleDbCount);
        Assert.Null(joined[2].IdleDbCount);
        Assert.Equal(0, joined[3].IdleDbCount);
        Assert.Null(joined[4].IdleDbCount);
        Assert.NotNull(joined[1].AvgCpuPct);
        Assert.Null(joined[2].AvgCpuPct);
        Assert.NotNull(joined[4].StorageTotalGb);
    }

    [Fact]
    public async Task GetServerMetricsAsync_StartsItsInputsTogether_TwoReadsWaitOnTheirTablesAtOnce()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #9149 concurrency test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
        await SeedAsync(connection, now, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);

        /* Hold the CPU table and the grant table locked. The CPU read and the server-list read each need one of them, so each waits on a lock. One
           statement reaches only the first lock it needs and waits there, with no second backend at all. */
        await using var holder = new NpgsqlConnection(scratch.ConnectionString);
        await holder.OpenAsync(ct);
        await DarlingMcpTestData.ExecAsync(holder, ct, "BEGIN; LOCK TABLE cpu_utilization_stats, memory_grant_stats IN ACCESS EXCLUSIVE MODE");

        var released = false;
        try
        {
            var read = DarlingFinOpsInventoryReader.GetServerMetricsAsync(dataSource, rollups, coverage, TimeoutSeconds, now, ct);

            var waiting = 0L;
            var deadline = DateTime.UtcNow.AddSeconds(15);
            while (waiting < 2 && DateTime.UtcNow < deadline)
            {
                await Task.Delay(100, ct);
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock' AND pid <> pg_backend_pid()", connection);
                waiting = (long)(await probe.ExecuteScalarAsync(ct))!;
            }

            await DarlingMcpTestData.ExecAsync(holder, ct, "ROLLBACK");
            released = true;
            var metrics = await read;

            Assert.True(waiting >= 2, $"Only {waiting} read(s) were waiting on the locked tables; the inputs are not running at the same time.");
            Assert.Equal(4, metrics.Count);
        }
        finally
        {
            if (!released) await DarlingMcpTestData.ExecAsync(holder, CancellationToken.None, "ROLLBACK");
        }
    }

    /// <summary>The fleet read as the old single statement answers it, mapped by the same row readers the joined read uses.</summary>
    private static async Task<Dictionary<int, ServerMetricsDto>> SingleStatementAsync(
        NpgsqlConnection connection, DateTime now, CancellationToken ct, string? sql = null)
    {
        await using var command = new NpgsqlCommand(sql ?? DarlingFinOpsInventoryReader.ServerMetricsSql, connection);
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now.AddHours(-24), NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now.AddDays(-7), NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DarlingFinOpsOptimizationReader.IdleCoverageStartUtc(now), NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = DarlingFinOpsOptimizationReader.IdleCoverageDays });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DarlingFinOpsOptimizationReader.IdleCoverageEndUtc(now), NpgsqlDbType = NpgsqlDbType.Timestamp });

        var results = new Dictionary<int, ServerMetricsDto>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            results[reader.GetInt32(0)] = new ServerMetricsDto(
                reader.IsDBNull(1) ? null : Convert.ToDecimal(reader.GetValue(1)),
                reader.IsDBNull(2) ? null : Convert.ToDecimal(reader.GetValue(2)),
                reader.IsDBNull(3) ? null : Convert.ToInt32(reader.GetValue(3)),
                DarlingFinOpsInventoryReader.FleetProvisioningStatusFor(reader),
                DarlingFinOpsInventoryReader.FleetHealthScoreFor(reader));
        }

        return results;
    }

    /// <summary>
    /// Four servers. 1: every figure, CPU samples that tie, two databases tied on the newest size snapshot (one active, one idle), covered.
    /// 2: registered and nothing else. 3: all its databases active, covered, a NULL current-workers count. 4: CPU and a size snapshot but no
    /// query-stats history, so no coverage and no idle count.
    /// </summary>
    private static async Task SeedAsync(NpgsqlConnection c, DateTime now, CancellationToken ct)
    {
        string[] names = ["", "inv-a", "inv-b", "inv-c", "inv-d"];
        for (var id = 1; id <= 4; id++)
            await DarlingMcpTestData.RegisterServerAsync(c, id, names[id], ct);

        /* Server 1: 14 samples with repeated values (the percentile lands on a tie), then an outlier. */
        var cpuA = new[] { 10, 10, 20, 20, 20, 20, 35, 35, 35, 50, 50, 50, 50, 95 };
        for (var i = 0; i < cpuA.Length; i++) await InsertCpuAsync(c, 1, names[1], now.AddMinutes(-3 * (i + 1)), cpuA[i], ct);
        /* Server 3: twelve identical samples. Server 4: three, so the verdict is NOT_ENOUGH_DATA. */
        for (var i = 0; i < 12; i++) await InsertCpuAsync(c, 3, names[3], now.AddMinutes(-4 * (i + 1)), 40, ct);
        for (var i = 0; i < 3; i++) await InsertCpuAsync(c, 4, names[4], now.AddMinutes(-4 * (i + 1)), 60, ct);

        await DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, max_workers_count, current_workers_count, total_physical_memory_mb, buffer_pool_mb, total_server_memory_mb) VALUES ($1, $2, $3, $4, 512, 100, 65536, 40000, 50000)",
            CollectionIdGenerator.Next(), now.AddHours(-2), 1, names[1]);
        await DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, max_workers_count, current_workers_count, total_physical_memory_mb, buffer_pool_mb, total_server_memory_mb) VALUES ($1, $2, $3, $4, 256, NULL, 32768, 20000, 25000)",
            CollectionIdGenerator.Next(), now.AddHours(-2), 3, names[3]);
        foreach (var (id, waiters) in new[] { (1, 3), (1, 5), (3, 0) })
            await DarlingMcpTestData.ExecAsync(c, ct,
                "INSERT INTO memory_grant_stats (collection_id, collection_time, server_id, server_name, waiter_count, timeout_error_count_delta, forced_grant_count_delta, granted_memory_mb, target_memory_mb) VALUES ($1, $2, $3, $4, $5, 1, 2, 150, 1000)",
                CollectionIdGenerator.Next(), now.AddHours(-1 - waiters), id, names[id], waiters);

        /* Server 1's newest snapshot holds two databases on the same instant, with an older, different snapshot behind it. */
        foreach (var (id, db, size, at) in new (int, string, decimal, DateTime)[]
        {
            (1, "DbBusy", 2048m, now.AddHours(-3)), (1, "DbIdle", 1024m, now.AddHours(-3)), (1, "master", 50m, now.AddHours(-3)), (1, "DbBusy", 1000m, now.AddDays(-2)),
            (3, "DbOnly", 4096m, now.AddHours(-5)),
            (4, "DbLone", 512m, now.AddHours(-5)),
        })
            await DarlingMcpTestData.ExecAsync(c, ct,
                "INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, total_size_mb, used_size_mb) VALUES ($1, $2, $3, $4, $5, $6, $6 / 2)",
                CollectionIdGenerator.Next(), at, id, names[id], db, size);

        await InsertActivityAsync(c, 1, names[1], "DbBusy", now.AddDays(-3), ct);
        await InsertActivityAsync(c, 3, names[3], "DbOnly", now.AddDays(-2), ct);
        await FinOpsIdleCoverageSeed.SeedAsync(c, ct, 1, names[1], now);
        await FinOpsIdleCoverageSeed.SeedAsync(c, ct, 3, names[3], now);

        foreach (var id in new[] { 1, 3 })
            await DarlingMcpTestData.ExecAsync(c, ct,
                "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition, cpu_count, physical_memory_mb) VALUES ($1, $2, $3, $4, 'Enterprise Edition', '16.0.4100.1', 'RTM', 3, 8, 65536)",
                CollectionIdGenerator.Next(), now.AddDays(-1), id, names[id]);
    }

    private static Task InsertCpuAsync(NpgsqlConnection c, int id, string name, DateTime at, int cpu, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $2, $5, 1)",
            CollectionIdGenerator.Next(), at, id, name, cpu);

    private static Task InsertActivityAsync(NpgsqlConnection c, int id, string name, string database, DateTime at, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle, delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds) VALUES ($1, $2, $3, $4, $5, '0xHASH9149', '0xHANDLE9149', 1000, 1000, 5, 300)",
            CollectionIdGenerator.Next(), at, id, name, database);
}
