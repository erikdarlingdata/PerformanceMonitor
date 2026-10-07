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

        /* 1,200 ms of work over the nine rated one-minute points (minutes 1 to 9: minute 0, the series' first run, has no previous
           point, so it carries no seconds), not over the two stored collections' 120 s (which read 10). */
        Assert.Equal(1200.0 / 540.0, newRows[0].ElapsedMsPerSecond, precision: 6);
        Assert.Equal(1.0, newRows[1].ElapsedMsPerSecond, precision: 6);
        /* The last bucket: 600 ms over the six points (minute 20 and five quiet runs) it holds, 360 s. */
        Assert.Equal(600.0 / 360.0, newRows[2].ElapsedMsPerSecond, precision: 6);
    }

    [Fact]
    public async Task ChartPoints_AQuietRunPlotsZero_OnBothStores()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await SeededStore.CreateAsync(ct);

        var oldRows = await ReadTrendAsync(store, OldServer, ct, bucketMinutes: 1);
        var newRows = await ReadTrendAsync(store, NewServer, ct, bucketMinutes: 1);

        /* Every run from minute 0 to 25 is a point (26), the quiet ones at 0: no line drawn across a gap. Minute 0 is the first
           run in the series, so it has no rate to plot (the read keeps it as an unrated point). */
        Assert.Equal(26, oldRows.Count);
        Assert.Equal(oldRows.Select(r => (r.Bucket, r.ElapsedMsPerSecond)), newRows.Select(r => (r.Bucket, r.ElapsedMsPerSecond)));
        Assert.Equal(Enumerable.Range(3, 12).Concat(Enumerable.Range(16, 4)).Concat(Enumerable.Range(21, 5)),
            newRows.Where(r => r.ElapsedMsPerSecond == 0.0).Select(r => (int)(r.Bucket - store.WorkStart).TotalMinutes).OrderBy(m => m));
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

    private static async Task<List<(DateTime Bucket, double ElapsedMsPerSecond, double ExecutionsPerSecond)>> ReadTrendAsync(
        SeededStore store, int serverId, CancellationToken ct, int bucketMinutes = 10)
    {
        await using var command = store.DataSource.CreateCommand(DurationTrendRouting.BuildBucketedRawTrendSql("procedure_stats", withDatabaseFilter: false));
        command.Parameters.AddWithValue(serverId);
        /* From the first run to the run at minute 25 (its log row lands three seconds after its start, so the window ends a moment later). */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(store.WorkStart, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(store.WorkStart.AddMinutes(25).AddSeconds(30), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(bucketMinutes);
        var rows = new List<(DateTime, double, double)>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            /* A NULL rate (the series' first point has no previous point) reads as NaN so two reads compare equal. */
            rows.Add((reader.GetDateTime(0), reader.IsDBNull(1) ? double.NaN : reader.GetDouble(1), reader.IsDBNull(2) ? double.NaN : reader.GetDouble(2)));
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

                /* The collector's runs for the new server across the whole span, in production order: a run's rows are stamped at its
                   start T and its log row lands three seconds later (the run's SQL and storage time, 3000 ms), SUCCESS, with the rows
                   it stored. The busy minutes (1, 2, 15, 20) stored one row; every other run stored none and logs rows_collected 0.
                   So the last hours hold runs and no procedure_stats row. */
                await DarlingMcpTestData.ExecAsync(
                    connection, ct,
                    "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) "
                    + "SELECT row_number() OVER (), $1, $2, 'procedure_stats', t + interval '3 seconds', 3000, 'SUCCESS', "
                    + "CASE WHEN extract(epoch FROM t - $3::timestamp) / 60 IN (1, 2, 15, 20) THEN 1 ELSE 0 END "
                    + "FROM generate_series($3::timestamp, $4::timestamp, interval '1 minute') AS t",
                    NewServer, "idle-rows-new", DateTime.SpecifyKind(workStart, DateTimeKind.Unspecified), DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
                /* The old store ran the collector on the same cadence; it just kept a row for every run (minutes 1 to 20 here). */
                await DarlingMcpTestData.ExecAsync(
                    connection, ct,
                    "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) "
                    + "SELECT 1000000 + row_number() OVER (), $1, $2, 'procedure_stats', t + interval '3 seconds', 3000, 'SUCCESS', "
                    + "CASE WHEN extract(epoch FROM t - $3::timestamp) / 60 BETWEEN 1 AND 20 THEN 1 ELSE 0 END "
                    + "FROM generate_series($3::timestamp, $4::timestamp, interval '1 minute') AS t",
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
