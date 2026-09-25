/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4227 LIVE: the two claims <see cref="ServerInventoryFleetMetricsTests"/> can only pin as SQL text —
/// that the routed idle check finds every one of its three regions (stitched rollup, partial leading hour,
/// unmaterialized tail) and that the fleet statement answers exactly what the old per-server loop answered —
/// proven against real PostgreSQL/TimescaleDB.
/// </summary>
/* #1776 own-store: every test here boots its own database through ScratchPostgres and works entirely inside
   it, so it cannot race the shared store's live collection and is left out of [Collection("live-postgres")]
   deliberately (the next sweep should not "fix" that). */
public sealed class ServerInventoryFleetMetricsLiveTests
{
    /// <summary>The pre-#4227 per-server statement, frozen verbatim from <c>origin/dev</c> at the commit
    /// before this PR's own change, so a drift in <c>ViewerDataService.ServerMetricsSql</c> cannot quietly
    /// pull this comparison's baseline along with it.</summary>
    private const string LegacyPerServerSql = @"
WITH cpu_24h AS (
    SELECT
        AVG(CAST(sqlserver_cpu_utilization AS DECIMAL(5,2))) AS avg_cpu_pct,
        MAX(sqlserver_cpu_utilization) AS max_cpu_pct,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY sqlserver_cpu_utilization) AS p95_cpu_pct
    FROM v_cpu_utilization_stats
    WHERE server_id = $1
    AND   collection_time >= $2
),
mem_latest AS (
    SELECT
        max_workers_count,
        current_workers_count
    FROM v_memory_stats
    WHERE server_id = $1
    AND   (server_id, collection_time) IN (
        SELECT server_id, MAX(collection_time)
        FROM v_memory_stats
        WHERE server_id = $1
        GROUP BY server_id
    )
),
grants AS (
    SELECT
        MAX(waiter_count) AS max_grant_waiters,
        SUM(COALESCE(timeout_error_count_delta, 0)) AS grant_timeouts,
        SUM(COALESCE(forced_grant_count_delta, 0)) AS forced_grants,
        MAX(100.0 * granted_memory_mb / NULLIF(target_memory_mb, 0)) AS grant_utilization_pct
    FROM v_memory_grant_stats
    WHERE server_id = $1
    AND   collection_time >= $2
),
storage_totals AS (
    SELECT
        SUM(total_size_mb) / 1024.0 AS total_storage_gb
    FROM v_database_size_stats
    WHERE server_id = $1
    AND   (server_id, collection_time) IN (
        SELECT server_id, MAX(collection_time)
        FROM v_database_size_stats
        WHERE server_id = $1
        GROUP BY server_id
    )
),
idle_dbs AS (
    SELECT
        COUNT(DISTINCT database_name) AS idle_db_count
    FROM (
        SELECT database_name
        FROM v_database_size_stats
        WHERE server_id = $1
        AND   (server_id, collection_time) IN (
            SELECT server_id, MAX(collection_time)
            FROM v_database_size_stats
            WHERE server_id = $1
            GROUP BY server_id
        )
        AND database_name NOT IN ('master', 'model', 'msdb', 'tempdb', 'PerformanceMonitor')
        EXCEPT
        SELECT DISTINCT database_name
        FROM v_query_stats
        WHERE server_id = $1
        AND   collection_time >= $3
        AND   delta_execution_count > 0
    ) AS idle
)
SELECT
    c.avg_cpu_pct,
    st.total_storage_gb,
    id.idle_db_count,
    c.max_cpu_pct,
    c.p95_cpu_pct,
    COALESCE(m.max_workers_count, 0),
    COALESCE(m.current_workers_count, 0),
    COALESCE(g.max_grant_waiters, 0),
    COALESCE(g.grant_timeouts, 0),
    COALESCE(g.forced_grants, 0),
    COALESCE(g.grant_utilization_pct, 0)
FROM (SELECT 1) AS anchor
LEFT JOIN cpu_24h c ON true
LEFT JOIN mem_latest m ON true
LEFT JOIN storage_totals st ON true
LEFT JOIN idle_dbs id ON true
LEFT JOIN grants g ON true";

    // ── the idle check's three regions, against a seeded TimescaleDB rollup ──

    [Fact]
    public async Task ServerMetricsSqlFor_FindsActiveDatabasesInEveryRegion_AndOnlyTheNeverExecutedOneIsIdle()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4227 routing test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled,
            "The live #4227 routing test needs TimescaleDB: the stitched-rollup arm it measures only exists once a CAGG is materialized.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        const int ServerId = -940201;
        const string ServerName = "finops-routing-e2e";

        /* A fixed historical hour, so nothing about this test depends on wall-clock "now": idleCutoff sits
           27 minutes into the hour (a real partial-leading-hour), legacy covers the whole hour after it,
           successor covers the two hours after THAT (so its measured floor is anchorHour + 1h), and the
           watermark (everything materialized) sits at anchorHour + 3h. */
        var anchorHour = new DateTime(2020, 6, 1, 8, 0, 0, DateTimeKind.Unspecified);
        var idleCutoff = anchorHour.AddMinutes(-27);
        var successorFloorTarget = anchorHour.AddHours(1);
        var watermarkTarget = anchorHour.AddHours(3);

        await ExecAsync(connection, "INSERT INTO servers (server_id, server_name) VALUES ($1, $2)", ct, ServerId, ServerName);

        /* Five candidate databases, one execution each (or none), planted so that exactly ONE — NeverExecutedDb
           — has no evidence anywhere in the query_stats history. Every other one is active in exactly one of
           the three regions the routed read must cover. */
        await InsertQueryStatsAsync(connection, ServerId, ServerName, "LeadingHourDb", idleCutoff.AddMinutes(5), 3, ct);
        await InsertQueryStatsAsync(connection, ServerId, ServerName, "LegacyHourDb", anchorHour.AddMinutes(15), 4, ct);
        await InsertQueryStatsAsync(connection, ServerId, ServerName, "SuccessorHourDb", anchorHour.AddMinutes(90), 6, ct);
        /* A second row in the LAST successor bucket, purely so that bucket is non-empty: an empty bucket
           materializes no row at all, and cagg_watermark (like the max(bucket) fallback) can only report
           the newest bucket that actually HAS one — this is what caught the watermark short of
           watermarkTarget on the first run of this test. */
        await InsertQueryStatsAsync(connection, ServerId, ServerName, "SuccessorHourDb", anchorHour.AddMinutes(135), 1, ct);
        await InsertQueryStatsAsync(connection, ServerId, ServerName, "TailDb", watermarkTarget.AddMinutes(10), 2, ct);
        // NeverExecutedDb: no query_stats rows at all.

        /* The "known databases" universe: one size snapshot per candidate, all at the SAME collection_time so
           the latest-snapshot join (exact timestamp equality) picks up every one of them together. */
        var snapshotTime = watermarkTarget.AddMinutes(20);
        foreach (var db in new[] { "LeadingHourDb", "LegacyHourDb", "SuccessorHourDb", "TailDb", "NeverExecutedDb" })
        {
            await InsertDatabaseSizeAsync(connection, ServerId, ServerName, db, snapshotTime, ct);
        }

        var ready = await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        Assert.True(ready > 0);

        /* Legacy materializes only the first hour; the successor materializes the two after it — so the
           successor's measured floor is anchorHour + 1h, and the stitch must cross that boundary correctly. */
        await RollupBackfill.RunSliceAsync(connection, TimescaleSupport.QueryStatsDbHourlyView, anchorHour, successorFloorTarget, SilentDisclosure(), ct);
        await RollupBackfill.RunSliceAsync(connection, TimescaleSupport.QueryStatsDbIntervalHourlyView, successorFloorTarget, watermarkTarget, SilentDisclosure(), ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        Assert.True(rollups.DbGrainHourly && rollups.DbGrainIntervalHourly);

        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);
        Assert.Equal(successorFloorTarget, coverage.FloorOf(TimescaleSupport.QueryStatsDbIntervalHourlyView));

        /* The engine's own watermark answer, read the same way GetServerMetricsAsync reads it. */
        var watermark = await RollupMaterializationWatermark.GetAsync(
            dataSource, TimescaleSupport.QueryStatsDbIntervalHourlyView, TimescaleSupport.HourlyBucket, 30, ct);
        Assert.Equal(watermarkTarget, watermark);

        /* THE MEASUREMENT: with all four seeded-active databases correctly found active and only
           NeverExecutedDb genuinely idle, idle_db_count must read exactly 1 — over a "known" universe of 5,
           that number alone proves every region (leading hour, legacy, successor, raw tail) pulled its weight,
           since a miss in ANY of the four active regions would push this above 1. */
        var idleCount = await ReadIdleDbCountAsync(dataSource, coverage, idleCutoff, watermark!.Value, ServerId, ct);
        Assert.Equal(1, idleCount);

        // ── the watermark fallback path: same answer, taken the slower way ──

        var fallbackMaxBucket = await ReadFallbackMaxBucketAsync(connection, TimescaleSupport.QueryStatsDbIntervalHourlyView, ct);
        var fallbackWatermark = fallbackMaxBucket + TimescaleSupport.HourlyBucket;
        Assert.Equal(watermarkTarget, fallbackWatermark);

        var idleCountViaFallback = await ReadIdleDbCountAsync(dataSource, coverage, idleCutoff, fallbackWatermark, ServerId, ct);
        Assert.Equal(idleCount, idleCountViaFallback);
    }

    // ── the fleet statement vs. the old per-server loop, server by server ──

    [Fact]
    public async Task GetServerMetricsAsync_FleetResult_MatchesTheOldPerServerStatement_ServerByServer()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #4227 fleet-parity test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* Deliberately NOT converted to hypertables: a plain-Postgres store has no rollups, so
           GetServerMetricsAsync falls back to ServerMetricsSql unrouted — the exact predicate the old
           per-server statement ran, just fleet-wide. That is the comparison this test makes: does the
           GROUP BY / LATERAL rewrite (#4227) change the answer for identical data? */
        const int ServerA = -940301;
        const int ServerB = -940302;
        const int ServerC = -940303;

        await ExecAsync(connection, "INSERT INTO servers (server_id, server_name) VALUES ($1, $2), ($3, $4), ($5, $6)",
            ct, ServerA, "old-new-A", ServerB, "old-new-B", ServerC, "old-new-C");

        var now = DateTime.UtcNow;

        // Server A: mixed — one active database, one idle database, a superseded (non-latest) size snapshot.
        await InsertCpuAsync(connection, ServerA, now.AddHours(-1), 10, ct);
        await InsertCpuAsync(connection, ServerA, now.AddHours(-2), 50, ct);
        await InsertCpuAsync(connection, ServerA, now.AddHours(-3), 90, ct);
        await InsertMemoryAsync(connection, ServerA, now.AddHours(-5), 999, 999, ct); // superseded — must be ignored
        await InsertMemoryAsync(connection, ServerA, now.AddHours(-1), 300, 250, ct); // latest — must win
        await InsertMemoryGrantAsync(connection, ServerA, now.AddHours(-1), 5, 1, 0, 800, 1000, ct);
        await InsertMemoryGrantAsync(connection, ServerA, now.AddHours(-2), 2, 2, 1, 400, 1000, ct);
        await InsertDatabaseSizeAsync(connection, ServerA, "old-new-A", "AppDb1", now.AddMinutes(-30), 500, ct);
        await InsertDatabaseSizeAsync(connection, ServerA, "old-new-A", "AppDb2", now.AddMinutes(-30), 300, ct);
        await InsertDatabaseSizeAsync(connection, ServerA, "old-new-A", "StaleDb", now.AddDays(-2), 9999, ct); // not latest — must be ignored
        await InsertQueryStatsAsync(connection, ServerA, "old-new-A", "AppDb1", now.AddDays(-2), 5, ct);
        // AppDb2: no executions -> idle.

        // Server B: both databases active, no memory-grant evidence at all (grants CTE empty, must COALESCE to 0).
        await InsertCpuAsync(connection, ServerB, now.AddHours(-1), 20, ct);
        await InsertCpuAsync(connection, ServerB, now.AddHours(-2), 40, ct);
        await InsertMemoryAsync(connection, ServerB, now.AddHours(-1), 128, 64, ct);
        await InsertDatabaseSizeAsync(connection, ServerB, "old-new-B", "Db1", now.AddMinutes(-15), 200, ct);
        await InsertDatabaseSizeAsync(connection, ServerB, "old-new-B", "Db2", now.AddMinutes(-15), 150, ct);
        await InsertQueryStatsAsync(connection, ServerB, "old-new-B", "Db1", now.AddDays(-1), 2, ct);
        await InsertQueryStatsAsync(connection, ServerB, "old-new-B", "Db2", now.AddDays(-6), 7, ct); // inside the 7d window

        // Server C: both databases idle, no CPU/memory evidence at all (every left-joined CTE empty).
        await InsertDatabaseSizeAsync(connection, ServerC, "old-new-C", "Db1", now.AddMinutes(-45), 50, ct);
        await InsertDatabaseSizeAsync(connection, ServerC, "old-new-C", "Db2", now.AddMinutes(-45), 75, ct);

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var metrics = await viewer.GetServerMetricsAsync(ct);

        /* The SAME cutoffs feed both sides of the comparison, captured once, so a few milliseconds of
           real elapsed time between the fleet call above and the per-server calls below cannot move a
           row across a boundary — none of the seeded data sits within minutes of either cutoff. */
        var cpuCutoff = now.AddHours(-24);
        var idleCutoff = now.AddDays(-7);

        foreach (var serverId in new[] { ServerA, ServerB, ServerC })
        {
            Assert.True(metrics.ContainsKey(serverId), $"server {serverId} did not appear in the fleet result.");
            var fleetRow = metrics[serverId];
            var oldRow = await RunLegacyPerServerAsync(connection, serverId, cpuCutoff, idleCutoff, ct);

            Assert.Equal(oldRow.AvgCpuPct, fleetRow.AvgCpuPct);
            Assert.Equal(oldRow.StorageTotalGb, fleetRow.StorageTotalGb);
            Assert.Equal(oldRow.IdleDbCount, fleetRow.IdleDbCount);
            Assert.Equal(oldRow.ProvisioningStatus, fleetRow.ProvisioningStatus);
        }

        // The specific shapes seeded above, so a future edit that breaks this test explains what regressed.
        Assert.Equal(1, metrics[ServerA].IdleDbCount);
        Assert.Equal(0, metrics[ServerB].IdleDbCount);
        Assert.Equal(2, metrics[ServerC].IdleDbCount);
    }

    // ── SQL execution helpers ─────────────────────────────────────────────────────────

    private static async Task<int> ReadIdleDbCountAsync(
        NpgsqlDataSource dataSource, RollupCoverage coverage, DateTime idleCutoff, DateTime watermark, int serverId, CancellationToken ct)
    {
        var sql = ViewerDataService.ServerMetricsSqlFor(coverage, idleCutoff, watermark);
        await using var command = dataSource.CreateCommand(sql);
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = idleCutoff }); // $1 cpu cutoff — unused by this test
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = idleCutoff }); // $2 idle cutoff
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (reader.GetInt32(0) == serverId)
            {
                return reader.IsDBNull(3) ? 0 : reader.GetInt32(3);
            }
        }

        throw new InvalidOperationException($"server {serverId} did not appear in the fleet result.");
    }

    private static async Task<DateTime> ReadFallbackMaxBucketAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(RollupMaterializationWatermark.FallbackMaxBucketSql(view), connection);
        var result = await command.ExecuteScalarAsync(ct);
        return (DateTime)result!;
    }

    private static async Task<(decimal? AvgCpuPct, decimal? StorageTotalGb, int? IdleDbCount, string? ProvisioningStatus)> RunLegacyPerServerAsync(
        NpgsqlConnection connection, int serverId, DateTime cpuCutoff, DateTime idleCutoff, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(LegacyPerServerSql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cpuCutoff, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(idleCutoff, DateTimeKind.Unspecified) });

        await using var reader = await command.ExecuteReaderAsync(ct);
        if (await reader.ReadAsync(ct))
        {
            var status = ProvisioningVerdict.Evaluate(
                avgCpuPercent: reader.IsDBNull(0) ? 0m : Convert.ToDecimal(reader.GetValue(0)),
                maxCpuPercent: reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                p95CpuPercent: reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                maxGrantWaiters: reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7)),
                grantTimeouts: reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
                forcedGrants: reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9)),
                grantUtilizationPercent: reader.IsDBNull(10) ? 0m : Convert.ToDecimal(reader.GetValue(10)),
                maxWorkers: reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5)),
                currentWorkers: reader.IsDBNull(6) ? 0 : Convert.ToInt32(reader.GetValue(6)));

            return (
                reader.IsDBNull(0) ? null : Convert.ToDecimal(reader.GetValue(0)),
                reader.IsDBNull(1) ? null : Convert.ToDecimal(reader.GetValue(1)),
                reader.IsDBNull(2) ? null : Convert.ToInt32(reader.GetValue(2)),
                status);
        }

        return (null, null, null, null);
    }

    private static RefreshDisclosure SilentDisclosure() =>
        new(message => Assert.Fail($"the refresh degraded unexpectedly on a 2.28.1 store: {message}"));

    // ── seeding helpers ────────────────────────────────────────────────────────────────

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var p in parameters)
        {
            command.Parameters.AddWithValue(p);
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertQueryStatsAsync(
        NpgsqlConnection connection, int serverId, string serverName, string databaseName, DateTime collectionTime, long executions, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8, $9, $10)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(databaseName);
        command.Parameters.AddWithValue("0xHASH" + databaseName);
        command.Parameters.AddWithValue("0xHANDLE" + databaseName);
        command.Parameters.AddWithValue(1000L);
        command.Parameters.AddWithValue(executions);
        command.Parameters.AddWithValue(300);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertDatabaseSizeAsync(
        NpgsqlConnection connection, int serverId, string serverName, string databaseName, DateTime collectionTime, CancellationToken ct)
    {
        await InsertDatabaseSizeAsync(connection, serverId, serverName, databaseName, collectionTime, 100m, ct);
    }

    private static async Task InsertDatabaseSizeAsync(
        NpgsqlConnection connection, int serverId, string serverName, string databaseName, DateTime collectionTime, decimal totalSizeMb, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, total_size_mb) VALUES ($1, $2, $3, $4, $5, $6)",
            connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(databaseName);
        command.Parameters.AddWithValue(totalSizeMb);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertCpuAsync(NpgsqlConnection connection, int serverId, DateTime collectionTime, int cpuPercent, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue("fleet-parity");
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(cpuPercent);
        command.Parameters.AddWithValue(0);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertMemoryAsync(NpgsqlConnection connection, int serverId, DateTime collectionTime, int maxWorkers, int currentWorkers, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, max_workers_count, current_workers_count) VALUES ($1, $2, $3, $4, $5, $6)",
            connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue("fleet-parity");
        command.Parameters.AddWithValue(maxWorkers);
        command.Parameters.AddWithValue(currentWorkers);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertMemoryGrantAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTime, int waiterCount, long timeoutDelta, long forcedDelta, decimal grantedMb, decimal targetMb, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO memory_grant_stats (collection_id, collection_time, server_id, server_name, waiter_count, timeout_error_count_delta, forced_grant_count_delta, granted_memory_mb, target_memory_mb) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue("fleet-parity");
        command.Parameters.AddWithValue(waiterCount);
        command.Parameters.AddWithValue(timeoutDelta);
        command.Parameters.AddWithValue(forcedDelta);
        command.Parameters.AddWithValue(grantedMb);
        command.Parameters.AddWithValue(targetMb);
        await command.ExecuteNonQueryAsync(ct);
    }
}
