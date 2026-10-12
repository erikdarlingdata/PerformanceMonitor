/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5329: the viewer's Top Queries and Top Procedures reads on the hourly route show BLANK (null) for the columns
/// the hourly rollups keep no copy of, instead of a false 0, and the raw route still returns the values.
///
/// <para>The rollups keep worker time, elapsed time and execution counts and nothing else. Before this change the
/// hourly arm left every other measure at the row's default, so the grid showed 0 reads, 0 writes and 0 spills for
/// a window that did a lot of I/O, and a reads sort ranked by those zeros. The MCP reads already answer null with a
/// precision note; this pins the viewer to the same promise, against real rollups: one query and one procedure
/// seeded with nonzero reads, writes, physical reads, rows and spills, read once while raw holds them and once
/// after raw is purged and the refreshed hourly rollup is all that is left.</para>
///
/// <para><b>#1776 own-store</b> — the test mints a scratch database (it materializes continuous aggregates the
/// shared fixture must never inherit), so the class is deliberately NOT in the <c>live-postgres</c>
/// collection.</para>
/// </summary>
public sealed class ViewerHourlyRouteBlankColumnsLiveTests
{
    private const int ServerId = -947201;
    private const string ServerName = "viewer-hourly-blank";
    private const string Db = "HourlyBlankDb";
    private const string QueryHash = "0xHBQ1";
    private const string ProcName = "usp_HourlyBlank";

    /// <summary>A fixed anchor, never wall-clock relative: the purge of raw's rows (and the refresh of the rollup)
    /// is what moves the router, not calendar time.</summary>
    private static readonly DateTime WindowStart = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public async Task HourlyRoute_ReadsBlankForTheColumnsTheRollupLacks_AndRawRouteReadsValues()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live hourly-route blank-column test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live hourly-route blank-column test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var windowEnd = WindowStart.AddDays(1);
        var end = WindowStart.AddHours(4);
        var bodySucceeded = false;
        try
        {
            /* Two hours, 10 and 20 executions, each stamped 10 minutes past its hour. Every I/O measure is nonzero. */
            await PlantQueryAsync(connection, WindowStart.AddHours(1).AddMinutes(10), 10, ct);
            await PlantQueryAsync(connection, WindowStart.AddHours(2).AddMinutes(10), 20, ct);
            await PlantProcedureAsync(connection, WindowStart.AddHours(1).AddMinutes(10), 10, ct);
            await PlantProcedureAsync(connection, WindowStart.AddHours(2).AddMinutes(10), 20, ct);

            /* ── raw route: the values are there. ── */
            await using (var rawViewer = new ViewerDataService(scratch.ConnectionString))
            {
                var (queries, queriesTier) = await rawViewer.GetTopQueriesByCpuTierAsync(ServerId, WindowStart, end, cancellationToken: ct);
                Assert.Equal("raw", queriesTier);
                var q = Assert.Single(queries);
                Assert.Equal(30L, q.TotalExecutions);
                Assert.Equal(3_000L, q.TotalLogicalReads);
                Assert.Equal(300L, q.TotalLogicalWrites);
                Assert.Equal(30L, q.TotalPhysicalReads);
                Assert.Equal(90L, q.TotalRows);
                Assert.Equal(60L, q.TotalSpills);
                Assert.Equal(MinWorkerUs, q.MinCpuUs);
                Assert.Equal(MaxWorkerUs, q.MaxCpuUs);
                Assert.Equal(MinElapsedUs, q.MinElapsedUs);
                Assert.Equal(MaxElapsedUs, q.MaxElapsedUs);
                Assert.Equal(0.85, q.MinElapsedMs);

                var (procedures, proceduresTier) = await rawViewer.GetTopProceduresByCpuTierAsync(ServerId, WindowStart, end, cancellationToken: ct);
                Assert.Equal("raw", proceduresTier);
                var p = Assert.Single(procedures);
                Assert.Equal(30L, p.TotalExecutions);
                Assert.Equal(3_000L, p.TotalLogicalReads);
                Assert.Equal(300L, p.TotalLogicalWrites);
                Assert.Equal(30L, p.TotalPhysicalReads);
                Assert.Equal(60L, p.TotalSpills);
                Assert.Equal(100.0, p.AvgReads);
                Assert.Equal(MinWorkerUs, p.MinWorkerTimeUs);
                Assert.Equal(MaxWorkerUs, p.MaxWorkerTimeUs);
                Assert.Equal(MinElapsedUs, p.MinElapsedTimeUs);
                Assert.Equal(MaxElapsedUs, p.MaxElapsedTimeUs);
            }

            /* Refresh the hourly rollups over the window BEFORE deleting raw (the product's own refresh path), then purge raw. */
            await RefreshAsync(connection, TimescaleSupport.QueryStatsIntervalHourlyView, WindowStart, windowEnd.AddHours(1), ct);
            await RefreshAsync(connection, TimescaleSupport.ProcedureStatsIntervalHourlyView, WindowStart, windowEnd.AddHours(1), ct);
            foreach (var table in new[] { "query_stats", "procedure_stats" })
            {
                await using var purge = new NpgsqlCommand(
                    $"DELETE FROM collect.{table} WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", connection);
                purge.Parameters.AddWithValue(ServerId);
                purge.Parameters.AddWithValue(WindowStart);
                purge.Parameters.AddWithValue(windowEnd);
                await purge.ExecuteNonQueryAsync(ct);
            }

            /* ── hourly route: a FRESH viewer, since the rollup probe is cached per instance. ── */
            await using var hourlyViewer = new ViewerDataService(scratch.ConnectionString);

            var (hourlyQueries, hourlyQueriesTier) = await hourlyViewer.GetTopQueriesByCpuTierAsync(ServerId, WindowStart, end, cancellationToken: ct);
            Assert.Equal("hourly", hourlyQueriesTier);
            var hq = Assert.Single(hourlyQueries);
            /* What the rollup keeps still reads as numbers. */
            Assert.Equal(30L, hq.TotalExecutions);
            Assert.Equal(30_000L, hq.TotalCpuUs);
            Assert.Equal(27_000L, hq.TotalElapsedUs);
            /* What it does not keep reads as blank, never 0. The rollup holds min/max of per-collection deltas (sums over many
               executions), not per-execution extremes, so Min/Max CPU and elapsed are blank too: the planted executions all ran 850 to
               950 us, and the rollup would have shown 9,000 and 18,000. */
            Assert.Null(hq.MinCpuUs);
            Assert.Null(hq.MaxCpuUs);
            Assert.Null(hq.MinElapsedUs);
            Assert.Null(hq.MaxElapsedUs);
            Assert.Null(hq.MinCpuMs);
            Assert.Null(hq.MaxCpuMs);
            Assert.Null(hq.MinElapsedMs);
            Assert.Null(hq.MaxElapsedMs);
            Assert.Null(hq.TotalLogicalReads);
            Assert.Null(hq.AvgReads);
            Assert.Null(hq.TotalLogicalWrites);
            Assert.Null(hq.TotalPhysicalReads);
            Assert.Null(hq.TotalRows);
            Assert.Null(hq.TotalSpills);
            Assert.Null(hq.MinPhysicalReads);
            Assert.Null(hq.MaxPhysicalReads);
            Assert.Null(hq.MinRows);
            Assert.Null(hq.MaxRows);
            Assert.Null(hq.MinSpills);
            Assert.Null(hq.MaxSpills);
            Assert.Null(hq.MinDop);
            Assert.Null(hq.MaxDop);
            Assert.Null(hq.MinGrantKb);
            Assert.Null(hq.MaxGrantKb);
            Assert.Null(hq.MinUsedGrantKb);
            Assert.Null(hq.MaxUsedGrantKb);
            Assert.Null(hq.MinIdealGrantKb);
            Assert.Null(hq.MaxIdealGrantKb);
            Assert.Null(hq.MinReservedThreads);
            Assert.Null(hq.MaxReservedThreads);
            Assert.Null(hq.MinUsedThreads);
            Assert.Null(hq.MaxUsedThreads);
            Assert.Null(hq.TotalClrUs);
            Assert.Null(hq.TotalClrMs);
            Assert.Null(hq.PlanGenerationNum);
            Assert.Null(hq.WorkerTimePerSecond);

            var (hourlyProcedures, hourlyProceduresTier) = await hourlyViewer.GetTopProceduresByCpuTierAsync(ServerId, WindowStart, end, cancellationToken: ct);
            Assert.Equal("hourly", hourlyProceduresTier);
            var hp = Assert.Single(hourlyProcedures);
            Assert.Equal(30L, hp.TotalExecutions);
            Assert.Equal(30_000L, hp.TotalCpuUs);
            Assert.Null(hp.MinWorkerTimeUs);
            Assert.Null(hp.MaxWorkerTimeUs);
            Assert.Null(hp.MinElapsedTimeUs);
            Assert.Null(hp.MaxElapsedTimeUs);
            Assert.Null(hp.MinCpuMs);
            Assert.Null(hp.MaxCpuMs);
            Assert.Null(hp.MinElapsedMs);
            Assert.Null(hp.MaxElapsedMs);
            Assert.Null(hp.TotalLogicalReads);
            Assert.Null(hp.AvgReads);
            Assert.Null(hp.TotalLogicalWrites);
            Assert.Null(hp.TotalPhysicalReads);
            Assert.Null(hp.MinLogicalReads);
            Assert.Null(hp.MaxLogicalReads);
            Assert.Null(hp.MinPhysicalReads);
            Assert.Null(hp.MaxPhysicalReads);
            Assert.Null(hp.MinLogicalWrites);
            Assert.Null(hp.MaxLogicalWrites);
            Assert.Null(hp.TotalSpills);
            Assert.Null(hp.AvgSpills);
            Assert.Null(hp.MinSpills);
            Assert.Null(hp.MaxSpills);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                    "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
                var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
                Assert.Equal(0L, schedulers);
            });
        }
    }

    /// <summary>One query_stats row with nonzero I/O: per execution 100 reads, 10 writes, 1 physical read, 3 rows, 2 spills.</summary>
    private static async Task PlantQueryAsync(NpgsqlConnection connection, DateTime at, long executions, CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds,
     delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_rows, delta_spills,
     min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)", connection);
        insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
        insert.Parameters.AddWithValue(DarlingMcpTestData.TruncateToSeconds(at));
        insert.Parameters.AddWithValue(ServerId);
        insert.Parameters.AddWithValue(ServerName);
        insert.Parameters.AddWithValue(Db);
        insert.Parameters.AddWithValue(QueryHash);
        insert.Parameters.AddWithValue("0xHBQ1H");
        insert.Parameters.AddWithValue(executions * 1_000L);
        insert.Parameters.AddWithValue(executions * 900L);
        insert.Parameters.AddWithValue(executions);
        insert.Parameters.AddWithValue(3600);
        insert.Parameters.AddWithValue(executions * 100L);
        insert.Parameters.AddWithValue(executions * 10L);
        insert.Parameters.AddWithValue(executions);
        insert.Parameters.AddWithValue(executions * 3L);
        insert.Parameters.AddWithValue(executions * 2L);
        AddPerExecutionExtremes(insert);
        await insert.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One procedure_stats row with the same per-execution I/O (no rows column on procedures).</summary>
    private static async Task PlantProcedureAsync(NpgsqlConnection connection, DateTime at, long executions, CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds,
     delta_logical_reads, delta_logical_writes, delta_physical_reads, delta_spills,
     min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time)
VALUES ($1, $2, $3, $4, $5, 'dbo', $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)", connection);
        insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
        insert.Parameters.AddWithValue(DarlingMcpTestData.TruncateToSeconds(at));
        insert.Parameters.AddWithValue(ServerId);
        insert.Parameters.AddWithValue(ServerName);
        insert.Parameters.AddWithValue(Db);
        insert.Parameters.AddWithValue(ProcName);
        insert.Parameters.AddWithValue("0x" + ProcName);
        insert.Parameters.AddWithValue(executions * 1_000L);
        insert.Parameters.AddWithValue(executions * 900L);
        insert.Parameters.AddWithValue(executions);
        insert.Parameters.AddWithValue(3600);
        insert.Parameters.AddWithValue(executions * 100L);
        insert.Parameters.AddWithValue(executions * 10L);
        insert.Parameters.AddWithValue(executions);
        insert.Parameters.AddWithValue(executions * 2L);
        AddPerExecutionExtremes(insert);
        await insert.ExecuteNonQueryAsync(ct);
    }

    /* #5329: the DMV's per-execution extremes, the numbers the raw route shows. They are the same on every planted row, so they
       never equal a per-collection sum (executions * 900 is 9,000 or 18,000), which is what the rollup would hand back. */
    private const long MinWorkerUs = 800L, MaxWorkerUs = 1_200L, MinElapsedUs = 850L, MaxElapsedUs = 950L;

    private static void AddPerExecutionExtremes(NpgsqlCommand insert)
    {
        insert.Parameters.AddWithValue(MinWorkerUs);
        insert.Parameters.AddWithValue(MaxWorkerUs);
        insert.Parameters.AddWithValue(MinElapsedUs);
        insert.Parameters.AddWithValue(MaxElapsedUs);
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }
}
