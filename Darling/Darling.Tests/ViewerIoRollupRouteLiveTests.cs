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
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5329 lane C: the viewer's Top Queries and Top Procedures hourly route reads the io hourly rollup, and fills
/// reads, physical reads and writes from its sums, when the store has the view and its first bucket is at or
/// before the window's start. Against real rollups: one query and one procedure, 30 executions over two hours with
/// nonzero I/O, rolled up and then purged from raw so the hourly rollups are all that is left.
///
/// <para>Three reads, each from a fresh viewer (the rollup probe is cached per instance): a window that starts at
/// the io view's first bucket (covering: reads filled), a window that starts an hour before it (the view starts
/// after the window start: today's route, reads blank), and the covering window again after the io views are
/// dropped (a store without io: no error, reads blank). The columns other than reads are compared between the
/// three reads for the same seed, so the io route shows exactly what the interval route shows.</para>
///
/// <para><b>#1776 own-store</b> — the test mints a scratch database (it materializes continuous aggregates the
/// shared fixture must never inherit), so the class is deliberately NOT in the <c>live-postgres</c>
/// collection.</para>
/// </summary>
public sealed class ViewerIoRollupRouteLiveTests
{
    private const int ServerId = -947202;
    private const string ServerName = "viewer-io-route";
    private const string Db = "IoRouteDb";
    private const string QueryHash = "0xIOQ1";
    private const string ProcName = "usp_IoRoute";

    /// <summary>A fixed anchor, never wall-clock relative.</summary>
    private static readonly DateTime WindowStart = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public async Task HourlyRoute_FillsReadsFromTheIoRollupOnlyWhenItCoversTheWindowStart()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live io-rollup route test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live io-rollup route test needs TimescaleDB.");
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
        var firstBucket = WindowStart.AddHours(1);
        var bodySucceeded = false;
        try
        {
            /* Two hours, 10 and 20 executions, each stamped 10 minutes past its hour: the first bucket is 01:00. */
            await PlantQueryAsync(connection, firstBucket.AddMinutes(10), 10, ct);
            await PlantQueryAsync(connection, firstBucket.AddHours(1).AddMinutes(10), 20, ct);
            await PlantProcedureAsync(connection, firstBucket.AddMinutes(10), 10, ct);
            await PlantProcedureAsync(connection, firstBucket.AddHours(1).AddMinutes(10), 20, ct);

            /* Refresh every hourly rollup the viewer could read BEFORE deleting raw, then purge raw. */
            foreach (var view in new[]
            {
                TimescaleSupport.QueryStatsIntervalHourlyView, TimescaleSupport.ProcedureStatsIntervalHourlyView,
                TimescaleSupport.QueryStatsIoHourlyView, TimescaleSupport.ProcedureStatsIoHourlyView,
            })
            {
                await RefreshAsync(connection, view, WindowStart, windowEnd.AddHours(1), ct);
            }

            foreach (var table in new[] { "query_stats", "procedure_stats" })
            {
                await using var purge = new NpgsqlCommand(
                    $"DELETE FROM collect.{table} WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", connection);
                purge.Parameters.AddWithValue(ServerId);
                purge.Parameters.AddWithValue(WindowStart);
                purge.Parameters.AddWithValue(windowEnd);
                await purge.ExecuteNonQueryAsync(ct);
            }

            /* ── covering: the window starts AT the io view's first bucket. ── */
            ViewerQueryStatsRow coveringQuery;
            ViewerProcedureStatsRow coveringProcedure;
            await using (var viewer = new ViewerDataService(scratch.ConnectionString))
            {
                var (queries, queriesTier) = await viewer.GetTopQueriesByCpuTierAsync(ServerId, firstBucket, end, cancellationToken: ct);
                Assert.Equal("hourly", queriesTier);
                coveringQuery = Assert.Single(queries);
                Assert.Equal(3_000L, coveringQuery.TotalLogicalReads);
                Assert.Equal(30L, coveringQuery.TotalPhysicalReads);
                Assert.Equal(300L, coveringQuery.TotalLogicalWrites);
                Assert.Equal(100.0, coveringQuery.AvgReads);
                Assert.Equal(30L, coveringQuery.TotalExecutions);
                Assert.Equal(30_000L, coveringQuery.TotalCpuUs);
                Assert.Equal(27_000L, coveringQuery.TotalElapsedUs);
                /* What no hourly rollup keeps stays blank: rows, spills, and the per-execution extremes. */
                Assert.Null(coveringQuery.TotalRows);
                Assert.Null(coveringQuery.TotalSpills);
                Assert.Null(coveringQuery.MinCpuUs);
                Assert.Null(coveringQuery.MaxElapsedUs);

                var (procedures, proceduresTier) = await viewer.GetTopProceduresByCpuTierAsync(ServerId, firstBucket, end, cancellationToken: ct);
                Assert.Equal("hourly", proceduresTier);
                coveringProcedure = Assert.Single(procedures);
                Assert.Equal(3_000L, coveringProcedure.TotalLogicalReads);
                Assert.Equal(30L, coveringProcedure.TotalPhysicalReads);
                Assert.Equal(300L, coveringProcedure.TotalLogicalWrites);
                Assert.Equal(100.0, coveringProcedure.AvgReads);
                Assert.Equal(30L, coveringProcedure.TotalExecutions);
                Assert.Equal(30_000L, coveringProcedure.TotalCpuUs);
                Assert.Null(coveringProcedure.TotalSpills);
                Assert.Null(coveringProcedure.MinWorkerTimeUs);
            }

            /* ── the io view starts after the window start (an hour earlier): today's route and blank reads. ── */
            await using (var viewer = new ViewerDataService(scratch.ConnectionString))
            {
                var (queries, queriesTier) = await viewer.GetTopQueriesByCpuTierAsync(ServerId, WindowStart, end, cancellationToken: ct);
                Assert.Equal("hourly", queriesTier);
                var q = Assert.Single(queries);
                AssertReadsBlank(q);
                AssertSameOtherColumns(coveringQuery, q);

                var (procedures, proceduresTier) = await viewer.GetTopProceduresByCpuTierAsync(ServerId, WindowStart, end, cancellationToken: ct);
                Assert.Equal("hourly", proceduresTier);
                var p = Assert.Single(procedures);
                AssertReadsBlank(p);
                AssertSameOtherColumns(coveringProcedure, p);
            }

            /* ── a store without io: drop the two views; the covering window reads exactly like the interval route. ── */
            foreach (var view in new[] { TimescaleSupport.QueryStatsIoHourlyView, TimescaleSupport.ProcedureStatsIoHourlyView })
            {
                await using var drop = new NpgsqlCommand($"DROP MATERIALIZED VIEW collect.{view}", connection);
                await drop.ExecuteNonQueryAsync(ct);
            }

            await using (var viewer = new ViewerDataService(scratch.ConnectionString))
            {
                var (queries, queriesTier) = await viewer.GetTopQueriesByCpuTierAsync(ServerId, firstBucket, end, cancellationToken: ct);
                Assert.Equal("hourly", queriesTier);
                var q = Assert.Single(queries);
                AssertReadsBlank(q);
                AssertSameOtherColumns(coveringQuery, q);

                var (procedures, proceduresTier) = await viewer.GetTopProceduresByCpuTierAsync(ServerId, firstBucket, end, cancellationToken: ct);
                Assert.Equal("hourly", proceduresTier);
                var p = Assert.Single(procedures);
                AssertReadsBlank(p);
                AssertSameOtherColumns(coveringProcedure, p);
            }

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

    private static void AssertReadsBlank(ViewerQueryStatsRow row)
    {
        Assert.Null(row.TotalLogicalReads);
        Assert.Null(row.AvgReads);
        Assert.Null(row.TotalPhysicalReads);
        Assert.Null(row.TotalLogicalWrites);
    }

    private static void AssertReadsBlank(ViewerProcedureStatsRow row)
    {
        Assert.Null(row.TotalLogicalReads);
        Assert.Null(row.AvgReads);
        Assert.Null(row.TotalPhysicalReads);
        Assert.Null(row.TotalLogicalWrites);
    }

    /// <summary>Every column but reads, physical reads and writes comes out the same on the io route as on the interval route.</summary>
    private static void AssertSameOtherColumns(ViewerQueryStatsRow expected, ViewerQueryStatsRow actual)
    {
        Assert.Equal(expected.DatabaseName, actual.DatabaseName);
        Assert.Equal(expected.QueryHash, actual.QueryHash);
        Assert.Equal(expected.TotalExecutions, actual.TotalExecutions);
        Assert.Equal(expected.TotalCpuUs, actual.TotalCpuUs);
        Assert.Equal(expected.TotalElapsedUs, actual.TotalElapsedUs);
        Assert.Equal(expected.MinCpuUs, actual.MinCpuUs);
        Assert.Equal(expected.MaxCpuUs, actual.MaxCpuUs);
        Assert.Equal(expected.MinElapsedUs, actual.MinElapsedUs);
        Assert.Equal(expected.MaxElapsedUs, actual.MaxElapsedUs);
        Assert.Equal(expected.TotalRows, actual.TotalRows);
        Assert.Equal(expected.TotalSpills, actual.TotalSpills);
        Assert.Equal(expected.QueryText, actual.QueryText);
        Assert.Equal(expected.HostObjectName, actual.HostObjectName);
    }

    private static void AssertSameOtherColumns(ViewerProcedureStatsRow expected, ViewerProcedureStatsRow actual)
    {
        Assert.Equal(expected.DatabaseName, actual.DatabaseName);
        Assert.Equal(expected.SchemaName, actual.SchemaName);
        Assert.Equal(expected.ObjectName, actual.ObjectName);
        Assert.Equal(expected.ObjectType, actual.ObjectType);
        Assert.Equal(expected.TotalExecutions, actual.TotalExecutions);
        Assert.Equal(expected.TotalCpuUs, actual.TotalCpuUs);
        Assert.Equal(expected.TotalElapsedUs, actual.TotalElapsedUs);
        Assert.Equal(expected.MinWorkerTimeUs, actual.MinWorkerTimeUs);
        Assert.Equal(expected.MaxWorkerTimeUs, actual.MaxWorkerTimeUs);
        Assert.Equal(expected.MinElapsedTimeUs, actual.MinElapsedTimeUs);
        Assert.Equal(expected.MaxElapsedTimeUs, actual.MaxElapsedTimeUs);
        Assert.Equal(expected.TotalSpills, actual.TotalSpills);
        Assert.Equal(expected.SqlHandle, actual.SqlHandle);
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
        insert.Parameters.AddWithValue("0xIOQ1H");
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

    /* The DMV's per-execution extremes, the numbers the raw route shows; the rollups cannot reproduce them. */
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
