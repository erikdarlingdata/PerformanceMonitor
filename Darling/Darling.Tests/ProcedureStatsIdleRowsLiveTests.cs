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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5449: the procedure_stats collector stores no row for a procedure that did no work in a cycle. A missing minute is
/// "no work", not "no data", and an older store still holds the idle rows (deltas 0, interval 60). A reader must give the
/// same answer on both. Each test seeds two servers with the same work: <see cref="OldServer"/> keeps the idle rows,
/// <see cref="NewServer"/> leaves them out.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres, then works entirely inside it. */
public sealed class ProcedureStatsIdleRowsLiveTests
{
    private const int OldServer = -544901;
    private const int NewServer = -544902;

    [Fact]
    public async Task BucketedRawTrend_IdleMinutesNotStored_ReadsTheSameAsStoredIdleRows()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);

        var oldRows = await ReadTrendAsync(store, OldServer, ct);
        var newRows = await ReadTrendAsync(store, NewServer, ct);

        Assert.Equal(3, oldRows.Count);
        Assert.Equal(oldRows.Count, newRows.Count);
        for (var i = 0; i < oldRows.Count; i++)
        {
            Assert.Equal(oldRows[i].Bucket, newRows[i].Bucket);
            Assert.Equal(oldRows[i].ElapsedMsPerSecond, newRows[i].ElapsedMsPerSecond, precision: 6);
            Assert.Equal(oldRows[i].ExecutionsPerSecond, newRows[i].ExecutionsPerSecond, precision: 6);
        }

        /* 1,200 ms of work over the bucket's 600 s, not over the two stored one-minute collections' 120 s (which read 10). */
        Assert.Equal(2.0, newRows[0].ElapsedMsPerSecond, precision: 6);
        Assert.Equal(1.0, newRows[1].ElapsedMsPerSecond, precision: 6);
        /* The last bucket: 600 ms over the six collections (minute 20 and five quiet runs) it holds, 360 s. */
        Assert.Equal(600.0 / 360.0, newRows[2].ElapsedMsPerSecond, precision: 6);
    }

    [Fact]
    public async Task ChartPoints_AQuietRunPlotsZero_OnBothStores()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);

        var oldRows = await ReadTrendAsync(store, OldServer, ct, bucketMinutes: 1);
        var newRows = await ReadTrendAsync(store, NewServer, ct, bucketMinutes: 1);

        /* Every run from minute 0 to 25 is a point (26), the quiet ones at 0: no line drawn across a gap. */
        Assert.Equal(26, oldRows.Count);
        Assert.Equal(oldRows.Select(r => (r.Bucket, r.ElapsedMsPerSecond)), newRows.Select(r => (r.Bucket, r.ElapsedMsPerSecond)));
        Assert.Equal(0.0, newRows.Single(r => r.Bucket == store.WorkStart.AddMinutes(3)).ElapsedMsPerSecond, precision: 6);
        Assert.Equal(10.0, newRows.Single(r => r.Bucket == store.WorkStart.AddMinutes(15)).ElapsedMsPerSecond, precision: 6);
    }

    [Fact]
    public async Task ProcedureSlicer_ProcedureCountIsOfProceduresWithWork_OnBothStores()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);

        var oldCounts = await ReadSlicerAsync(store, OldServer, ct);
        var newCounts = await ReadSlicerAsync(store, NewServer, ct);

        Assert.NotEmpty(newCounts);
        Assert.Equal(oldCounts, newCounts);
        Assert.All(newCounts, c => Assert.Equal(1L, c.Count));
    }

    [Fact]
    public async Task ProcedureWindowFloor_AWindowOfOnlyIdleRuns_IsCoveredByTheCollectorsRuns()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        /* The new store's work is long ago; the window holds only the collector's runs (every procedure was idle). */
        var start = store.End.AddHours(-2);

        var floor = await RawWindowFloor.GetAsync(store.DataSource, RawWindowFloor.Table.ProcedureStats, NewServer, start, store.End, cancellationToken: ct);

        Assert.NotNull(floor);
        /* The collector's first run, a minute before the first stored row: coverage starts with the runs. */
        Assert.Equal(store.WorkStart, floor);
    }

    [Fact]
    public async Task ProcedureWindowFloor_ARunsOnlyStoreHasAFloor_AsItsFirstRun()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        /* A server the collector has run against but that has no procedure_stats row at all yet. */
        await using (var delete = store.DataSource.CreateCommand("DELETE FROM collect.procedure_stats WHERE server_id = $1"))
        {
            delete.Parameters.AddWithValue(NewServer);
            await delete.ExecuteNonQueryAsync(ct);
        }

        var floor = await RawWindowFloor.GetAsync(store.DataSource, RawWindowFloor.Table.ProcedureStats, NewServer, store.WorkStart.AddHours(1), store.End, cancellationToken: ct);

        Assert.Equal(store.WorkStart, floor);
    }

    [Fact]
    public async Task ProcedureHistoryChart_AQuietRunPlotsZero_OnBothStores()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        await using var viewer = new ViewerDataService(store.ConnectionString);

        var (oldGrid, oldChart) = await ReadHistoryAsync(viewer, store, OldServer, "idle-rows-old", ct);
        var (newGrid, newChart) = await ReadHistoryAsync(viewer, store, NewServer, "idle-rows-new", ct);

        /* The runs span six hours, the procedure's rows minutes 1 to 20: a 0 is plotted only between them. */
        Assert.Equal(20, oldChart.Count);
        Assert.Equal(oldChart.Select(r => r.CollectionTime), newChart.Select(r => r.CollectionTime));
        Assert.Equal(oldChart.Select(r => r.DeltaExecutions), newChart.Select(r => r.DeltaExecutions));
        Assert.Equal(oldChart.Select(r => r.AvgCpuMs), newChart.Select(r => r.AvgCpuMs));
        Assert.Equal(0, newChart.Single(r => r.CollectionTime == store.WorkStart.AddMinutes(3)).DeltaExecutions);
        Assert.Equal(10, newChart.Single(r => r.CollectionTime == store.WorkStart.AddMinutes(15)).DeltaExecutions);
        /* The grid still lists only the minutes with work. */
        Assert.Equal(4, newGrid.Count);
        Assert.Equal(20, oldGrid.Count);
    }

    [Fact]
    public async Task ProcedureHistoryChart_AFailedRunIsNotAQuietPoint()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);
        await using (var fail = store.DataSource.CreateCommand("UPDATE collect.collection_log SET status = 'ERROR' WHERE server_id = $1 AND collection_time = $2"))
        {
            fail.Parameters.AddWithValue(NewServer);
            fail.Parameters.AddWithValue(DateTime.SpecifyKind(store.WorkStart.AddMinutes(3), DateTimeKind.Unspecified));
            await fail.ExecuteNonQueryAsync(ct);
        }
        await using var viewer = new ViewerDataService(store.ConnectionString);

        var (_, chart) = await ReadHistoryAsync(viewer, store, NewServer, "idle-rows-new", ct);

        Assert.Equal(19, chart.Count);
        Assert.DoesNotContain(chart, r => r.CollectionTime == store.WorkStart.AddMinutes(3));
    }

    [Fact]
    public void IdleRunTimes_ARunOwnsTheRowsUpToTheNextRun_WhetherStampedBeforeOrAtTheRows()
    {
        var t = new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Unspecified);
        var runs = Enumerable.Range(0, 8).Select(m => t.AddMinutes(m)).ToList();
        /* Rows land two seconds after their run's stamp (minutes 1 and 6) or on it (minute 4). */
        var rows = new List<DateTime> { t.AddMinutes(1).AddSeconds(2), t.AddMinutes(4), t.AddMinutes(6).AddSeconds(2) };

        var idle = ViewerProcedureHistoryIdleRuns.IdleRunTimes(rows, runs);

        /* Minutes 2, 3 and 5 own no row. Minute 1 owns the first row, 4 and 6 their rows; 0 and 7 lie outside the first and last row. */
        Assert.Equal(new[] { 2, 3, 5 }, idle.Select(i => (int)(i - t).TotalMinutes).ToArray());
        Assert.Empty(ViewerProcedureHistoryIdleRuns.IdleRunTimes(new List<DateTime>(), runs));
    }

    private static async Task<(List<ViewerProcedureStatsHistoryRow> Grid, List<ViewerProcedureStatsHistoryRow> Chart)> ReadHistoryAsync(
        ViewerDataService viewer, SeededStore store, int serverId, string serverName, CancellationToken ct)
    {
        _ = serverName;
        var grid = await viewer.GetProcedureStatsHistoryAsync(serverId, "AppDb", "dbo", "usp_Work", store.WorkStart.AddMinutes(-5), store.WorkStart.AddHours(1), ct);
        var chart = await viewer.GetProcedureStatsHistoryChartRowsAsync(serverId, grid, ct);
        return (grid, chart);
    }

    private static async Task<List<(DateTime Bucket, double ElapsedMsPerSecond, double ExecutionsPerSecond)>> ReadTrendAsync(
        SeededStore store, int serverId, CancellationToken ct, int bucketMinutes = 10)
    {
        await using var command = store.DataSource.CreateCommand(DurationTrendRouting.BuildBucketedRawTrendSql("procedure_stats", withDatabaseFilter: false));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(store.WorkStart.AddMinutes(-5), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(store.WorkStart.AddMinutes(25), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(bucketMinutes);
        var rows = new List<(DateTime, double, double)>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetDateTime(0), reader.GetDouble(1), reader.GetDouble(2)));
        }
        return rows;
    }

    private static async Task<List<(DateTime Bucket, long Count)>> ReadSlicerAsync(SeededStore store, int serverId, CancellationToken ct)
    {
        await using var command = store.DataSource.CreateCommand(ViewerDataService.ProcStatsSlicerSql);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(store.WorkStart.AddHours(-1), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(store.WorkStart.AddHours(2), DateTimeKind.Unspecified));
        command.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text });
        var rows = new List<(DateTime, long)>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetDateTime(0), reader.GetInt64(1)));
        }
        return rows;
    }

    private sealed class SeededStore : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;

        private SeededStore(ScratchPostgres scratch, NpgsqlDataSource dataSource, DateTime workStart, DateTime end)
        {
            _scratch = scratch;
            DataSource = dataSource;
            WorkStart = workStart;
            End = end;
        }

        public NpgsqlDataSource DataSource { get; }

        public string ConnectionString => _scratch.ConnectionString;

        /// <summary>A 10-minute boundary, 6 hours ago: minute 0 of the seeded twenty collections.</summary>
        public DateTime WorkStart { get; }

        public DateTime End { get; }

        public static async Task<SeededStore> CreateAsync(CancellationToken ct)
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
                "Set DARLING_TEST_PG to a Postgres connection string to run the live procedure_stats idle-row tests.");

            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            try
            {
                await using var connection = new NpgsqlConnection(scratch.ConnectionString);
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
                await DarlingMcpTestData.RegisterServerAsync(connection, OldServer, "idle-rows-old", ct);
                await DarlingMcpTestData.RegisterServerAsync(connection, NewServer, "idle-rows-new", ct);

                var now = DateTime.UtcNow.AddHours(-6);
                var workStart = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute - (now.Minute % 10), 0, DateTimeKind.Utc);
                var end = workStart.AddHours(6);
                var work = new HashSet<int> { 1, 2, 15, 20 };
                for (var k = 1; k <= 20; k++)
                {
                    var at = workStart.AddMinutes(k);
                    if (work.Contains(k))
                    {
                        await InsertAsync(connection, OldServer, "idle-rows-old", at, 10, 600_000, ct);
                        await InsertAsync(connection, NewServer, "idle-rows-new", at, 10, 600_000, ct);
                    }
                    else
                    {
                        await InsertAsync(connection, OldServer, "idle-rows-old", at, 0, 0, ct);
                    }
                }

                /* A second procedure that never worked: a row of zeros on the old store, nothing on the new. */
                await using (var never = new NpgsqlCommand(@"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ((SELECT COALESCE(MAX(collection_id), 0) + 1 FROM collect.procedure_stats), $1, $2, 'idle-rows-old', 'AppDb', 'dbo', 'usp_NeverWorked', '0x02', 0, 0, 0, 60)", connection))
                {
                    never.Parameters.AddWithValue(DateTime.SpecifyKind(workStart.AddMinutes(3), DateTimeKind.Unspecified));
                    never.Parameters.AddWithValue(OldServer);
                    await never.ExecuteNonQueryAsync(ct);
                }

                /* The collector's runs for the new server across the whole span, every one of them SUCCESS storing 0 rows
                   after the work: so the last two hours hold runs and no procedure_stats row. */
                await DarlingMcpTestData.ExecAsync(
                    connection, ct,
                    "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) "
                    + "SELECT row_number() OVER (), $1, $2, 'procedure_stats', t, 12, 'SUCCESS', 0 FROM generate_series($3::timestamp, $4::timestamp, interval '1 minute') AS t",
                    NewServer, "idle-rows-new", DateTime.SpecifyKind(workStart, DateTimeKind.Unspecified), DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
                /* The old store ran the collector on the same cadence; it just kept a row for every run. */
                await DarlingMcpTestData.ExecAsync(
                    connection, ct,
                    "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) "
                    + "SELECT 1000000 + row_number() OVER (), $1, $2, 'procedure_stats', t, 12, 'SUCCESS', 1 FROM generate_series($3::timestamp, $4::timestamp, interval '1 minute') AS t",
                    OldServer, "idle-rows-old", DateTime.SpecifyKind(workStart, DateTimeKind.Unspecified), DateTime.SpecifyKind(end, DateTimeKind.Unspecified));

                return new SeededStore(scratch, NpgsqlDataSource.Create(scratch.ConnectionString), workStart, end);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        private static async Task InsertAsync(
            NpgsqlConnection connection, int serverId, string serverName, DateTime at, long executions, long elapsedUs, CancellationToken ct)
        {
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ((SELECT COALESCE(MAX(collection_id), 0) + 1 FROM collect.procedure_stats), $1, $2, $3, 'AppDb', 'dbo', 'usp_Work', '0x01', $4, $4, $5, 60)", connection);
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(at, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(serverName);
            insert.Parameters.AddWithValue(elapsedUs);
            insert.Parameters.AddWithValue(executions);
            await insert.ExecuteNonQueryAsync(ct);
        }

        public async ValueTask DisposeAsync()
        {
            await DataSource.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}
