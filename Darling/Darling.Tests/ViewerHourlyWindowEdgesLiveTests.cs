/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5329 lane C2: the viewer's hourly Top Queries and Top Procedures reads stop at the materialization ceiling of the
/// relation that answers the window's end and answer the window's real edges, against real rollups. One query and one
/// procedure in three hours (H0 = 1 execution, H1 = 10, H2 = 100, so a total names its hours); the hourly rollups are
/// refreshed through H1 only, so the rollups' ceiling is H2's start while raw held an H2 row until the purge. A window
/// that ends well after H2 must count only H0 and H1 (11), name the ceiling in its note, and, when the rollup starts
/// after the window, name the floor. Read through both relations: the io rollup (a window that starts at its first
/// bucket) and the interval rollup (a window that starts an hour before it).
///
/// <para><b>#1776 own-store</b> — the test mints a scratch database (it materializes continuous aggregates the shared
/// fixture must never inherit), so the class is deliberately NOT in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class ViewerHourlyWindowEdgesLiveTests
{
    private const int ServerId = -947301;
    private const string ServerName = "viewer-hourly-edges";
    private const string Db = "HourlyEdgesDb";
    private const string QueryHash = "0xHEQ1";
    private const string ProcName = "usp_HourlyEdges";

    /// <summary>A fixed anchor, never wall-clock relative.</summary>
    private static readonly DateTime WindowStart = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime H0 = WindowStart.AddHours(1);
    private static readonly DateTime H1 = WindowStart.AddHours(2);
    private static readonly DateTime H2 = WindowStart.AddHours(3);

    [Fact]
    public async Task HourlyRoute_StopsAtTheCeiling_AndNamesTheWindowsEdges_OnBothRelationsAndBothGrids()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live hourly window-edges test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The live hourly window-edges test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        var end = WindowStart.AddHours(8);
        var bodySucceeded = false;
        try
        {
            var hours = new[] { H0, H1, H2 };
            var executions = new[] { 1L, 10L, 100L };
            for (var i = 0; i < hours.Length; i++)
            {
                await PlantQueryAsync(connection, hours[i].AddMinutes(10), executions[i], ct);
                await PlantProcedureAsync(connection, hours[i].AddMinutes(10), executions[i], ct);
            }

            /* Refresh through H1's bucket only: H2 is never materialized, so every rollup's ceiling is H2's start. */
            foreach (var view in new[]
            {
                TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsIntervalHourlyView, TimescaleSupport.QueryStatsIoHourlyView,
                TimescaleSupport.ProcedureStatsHourlyView, TimescaleSupport.ProcedureStatsIntervalHourlyView, TimescaleSupport.ProcedureStatsIoHourlyView,
            })
            {
                await RefreshAsync(connection, view, WindowStart, H2, ct);
            }

            /* Purge raw's H0 and H1 rows so the hourly route answers (the router sends a window raw no longer holds to the
               rollup). H2's raw row stays: a continuous aggregate reads past its watermark from raw, so without the
               ceiling bound the unmaterialized hour would be counted (111, not 11). */
            foreach (var table in new[] { "query_stats", "procedure_stats" })
            {
                await using var purge = new NpgsqlCommand(
                    $"DELETE FROM collect.{table} WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", connection);
                purge.Parameters.AddWithValue(ServerId);
                purge.Parameters.AddWithValue(WindowStart);
                purge.Parameters.AddWithValue(H2);
                await purge.ExecuteNonQueryAsync(ct);
            }

            /* The ceiling the stores measured: H2's start, on every relation the reads can name. */
            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
            var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);
            Assert.Equal(H2, coverage.HourlyEndCeiling(TimescaleSupport.QueryStatsIoHourlyView, H0));
            var ceilingText = "materialized only to " + DateTime.SpecifyKind(H2, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);
            var floorText = "served from " + DateTime.SpecifyKind(H0, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);

            /* ── io relation: the window starts AT the io view's first bucket, so the start edge did not move. ── */
            await using (var viewer = new ViewerDataService(scratch.ConnectionString))
            {
                var queries = await viewer.GetTopQueriesByCpuRoutedAsync(ServerId, H0, end, cancellationToken: ct);
                Assert.Equal("hourly", queries.Tier);
                Assert.Equal(11L, queries.Rows.Sum(r => r.TotalExecutions));
                Assert.Equal(1_100L, queries.Rows.Sum(r => r.TotalLogicalReads));
                Assert.Contains(ceilingText, queries.HourlyEdgesNote, StringComparison.Ordinal);
                Assert.Contains("nothing after it was read", queries.HourlyEdgesNote, StringComparison.Ordinal);
                /* The banner's inputs: the io route (reads and writes are filled) and this server's first bucket. */
                Assert.True(queries.IoRoute);
                Assert.Equal(H0, queries.HourlyFirstBucket);

                var procedures = await viewer.GetTopProceduresByCpuRoutedAsync(ServerId, H0, end, cancellationToken: ct);
                Assert.Equal("hourly", procedures.Tier);
                Assert.Equal(11L, procedures.Rows.Sum(r => r.TotalExecutions));
                Assert.Equal(1_100L, procedures.Rows.Sum(r => r.TotalLogicalReads));
                Assert.Contains(ceilingText, procedures.HourlyEdgesNote, StringComparison.Ordinal);
                Assert.True(procedures.IoRoute);
                Assert.Equal(H0, procedures.HourlyFirstBucket);
            }

            /* ── interval relation: the window starts an hour before the first bucket (io does not cover), so the note
               names where the served data starts as well as the ceiling. ── */
            await using (var viewer = new ViewerDataService(scratch.ConnectionString))
            {
                var queries = await viewer.GetTopQueriesByCpuRoutedAsync(ServerId, WindowStart, end, cancellationToken: ct);
                Assert.Equal("hourly", queries.Tier);
                Assert.Null(queries.Rows.Single().TotalLogicalReads);
                Assert.Equal(11L, queries.Rows.Sum(r => r.TotalExecutions));
                Assert.Contains(ceilingText, queries.HourlyEdgesNote, StringComparison.Ordinal);
                Assert.Contains(floorText, queries.HourlyEdgesNote, StringComparison.Ordinal);
                Assert.False(queries.IoRoute);
                Assert.Equal(H0, queries.HourlyFirstBucket);

                var procedures = await viewer.GetTopProceduresByCpuRoutedAsync(ServerId, WindowStart, end, cancellationToken: ct);
                Assert.Equal("hourly", procedures.Tier);
                Assert.Null(procedures.Rows.Single().TotalLogicalReads);
                Assert.Equal(11L, procedures.Rows.Sum(r => r.TotalExecutions));
                Assert.Contains(ceilingText, procedures.HourlyEdgesNote, StringComparison.Ordinal);
                Assert.Contains(floorText, procedures.HourlyEdgesNote, StringComparison.Ordinal);
                Assert.False(procedures.IoRoute);
                Assert.Equal(H0, procedures.HourlyFirstBucket);
            }

            /* ── a server added AFTER the store's oldest one: the start edge is per SERVER. Server B's first bucket is H1 while the
               store-wide floor (server A's) is H0, which is before the window's start; a store-wide floor would say nothing about
               B's late start, and the banner would name nothing. The probe is the one the service's tools run. ── */
            const int OtherServerId = ServerId - 1;
            const string OtherServerName = ServerName + "-later";
            await DarlingMcpTestData.RegisterServerAsync(connection, OtherServerId, OtherServerName, ct);
            await PlantQueryAsync(connection, H1.AddMinutes(10), 4, ct, OtherServerId, OtherServerName);
            await PlantProcedureAsync(connection, H1.AddMinutes(10), 4, ct, OtherServerId, OtherServerName);
            foreach (var view in new[] { TimescaleSupport.QueryStatsIntervalHourlyView, TimescaleSupport.ProcedureStatsIntervalHourlyView })
            {
                await RefreshAsync(connection, view, H1, H2, ct);
            }

            foreach (var table in new[] { "query_stats", "procedure_stats" })
            {
                await using var purgeOther = new NpgsqlCommand($"DELETE FROM collect.{table} WHERE server_id = $1 AND collection_time < $2", connection);
                purgeOther.Parameters.AddWithValue(OtherServerId);
                purgeOther.Parameters.AddWithValue(H2);
                await purgeOther.ExecuteNonQueryAsync(ct);
            }

            await using (var viewer = new ViewerDataService(scratch.ConnectionString))
            {
                var later = await viewer.GetTopQueriesByCpuRoutedAsync(OtherServerId, WindowStart, end, cancellationToken: ct);
                Assert.Equal("hourly", later.Tier);
                Assert.Equal(4L, later.Rows.Sum(r => r.TotalExecutions));
                Assert.Equal(H1, later.HourlyFirstBucket);
                Assert.Contains("served from " + DateTime.SpecifyKind(H1, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture),
                    later.HourlyEdgesNote, StringComparison.Ordinal);

                var laterProcedures = await viewer.GetTopProceduresByCpuRoutedAsync(OtherServerId, WindowStart, end, cancellationToken: ct);
                Assert.Equal(4L, laterProcedures.Rows.Sum(r => r.TotalExecutions));
                Assert.Equal(H1, laterProcedures.HourlyFirstBucket);
                Assert.Contains("served from " + DateTime.SpecifyKind(H1, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture),
                    laterProcedures.HourlyEdgesNote, StringComparison.Ordinal);

                /* Server A, in the same store and the same viewer, still starts at its own first bucket. */
                var first = await viewer.GetTopQueriesByCpuRoutedAsync(ServerId, WindowStart, end, cancellationToken: ct);
                Assert.Equal(H0, first.HourlyFirstBucket);

                /* The probe's window end is exclusive, as the grids' is: server B's ONLY bucket is H1, so a window that ENDS at H1
                   reads nothing, and its note and banner must not name a start for that empty grid. */
                var emptyEnd = await viewer.GetTopQueriesByCpuRoutedAsync(OtherServerId, WindowStart, H1, cancellationToken: ct);
                Assert.Equal("hourly", emptyEnd.Tier);
                Assert.Empty(emptyEnd.Rows);
                Assert.Null(emptyEnd.HourlyFirstBucket);
                Assert.DoesNotContain("served from", emptyEnd.HourlyEdgesNote ?? string.Empty, StringComparison.Ordinal);

                var emptyEndProcedures = await viewer.GetTopProceduresByCpuRoutedAsync(OtherServerId, WindowStart, H1, cancellationToken: ct);
                Assert.Empty(emptyEndProcedures.Rows);
                Assert.Null(emptyEndProcedures.HourlyFirstBucket);
            }

            /* ── a bucket materializes AFTER the viewer measured its ceiling (the coverage snapshot is cached per viewer):
               the rollups now hold H2, and neither grid may read it, as the service's reads do not. ── */
            await using (var viewer = new ViewerDataService(scratch.ConnectionString))
            {
                var before = await viewer.GetTopQueriesByCpuRoutedAsync(ServerId, H0, end, cancellationToken: ct);
                Assert.Equal(11L, before.Rows.Sum(r => r.TotalExecutions));

                foreach (var view in new[]
                {
                    TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsIntervalHourlyView, TimescaleSupport.QueryStatsIoHourlyView,
                    TimescaleSupport.ProcedureStatsHourlyView, TimescaleSupport.ProcedureStatsIntervalHourlyView, TimescaleSupport.ProcedureStatsIoHourlyView,
                })
                {
                    await RefreshAsync(connection, view, H2, end, ct);
                }

                foreach (var table in new[] { "query_stats", "procedure_stats" })
                {
                    await using var purge = new NpgsqlCommand(
                        $"DELETE FROM collect.{table} WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3", connection);
                    purge.Parameters.AddWithValue(ServerId);
                    purge.Parameters.AddWithValue(WindowStart);
                    purge.Parameters.AddWithValue(WindowStart.AddDays(1));
                    await purge.ExecuteNonQueryAsync(ct);
                }

                var queries = await viewer.GetTopQueriesByCpuRoutedAsync(ServerId, H0, end, cancellationToken: ct);
                Assert.Equal(11L, queries.Rows.Sum(r => r.TotalExecutions));
                Assert.Contains(ceilingText, queries.HourlyEdgesNote, StringComparison.Ordinal);
                var procedures = await viewer.GetTopProceduresByCpuRoutedAsync(ServerId, H0, end, cancellationToken: ct);
                Assert.Equal(11L, procedures.Rows.Sum(r => r.TotalExecutions));
                Assert.Contains(ceilingText, procedures.HourlyEdgesNote, StringComparison.Ordinal);
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

    /// <summary>One query_stats row: per execution 1,000 us CPU, 900 us elapsed, 100 reads, 10 writes, 1 physical read.</summary>
    private static async Task PlantQueryAsync(
        NpgsqlConnection connection, DateTime at, long executions, CancellationToken ct, int serverId = ServerId, string serverName = ServerName)
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
        insert.Parameters.AddWithValue(serverId);
        insert.Parameters.AddWithValue(serverName);
        insert.Parameters.AddWithValue(Db);
        insert.Parameters.AddWithValue(QueryHash);
        insert.Parameters.AddWithValue("0xHEQ1H");
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

    private static async Task PlantProcedureAsync(
        NpgsqlConnection connection, DateTime at, long executions, CancellationToken ct, int serverId = ServerId, string serverName = ServerName)
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
        insert.Parameters.AddWithValue(serverId);
        insert.Parameters.AddWithValue(serverName);
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

    private static void AddPerExecutionExtremes(NpgsqlCommand insert)
    {
        insert.Parameters.AddWithValue(800L);
        insert.Parameters.AddWithValue(1_200L);
        insert.Parameters.AddWithValue(850L);
        insert.Parameters.AddWithValue(950L);
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(from);
        refresh.Parameters.AddWithValue(to);
        await refresh.ExecuteNonQueryAsync(ct);
    }
}
