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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5449: the procedure raw-tier trend reads the collector's SUCCESS runs as its time axis. A run that stored rows is a point at
/// its stored collection time; a run that stored none is a zero-work point at the instant it began. A point's seconds are the
/// gap to the previous point (unrated past the collector's one-hour policy), and a bucket's seconds are the sum of those gaps.
/// Every test seeds in production order: rows at the run's start T, the run's log row about three seconds later (the run's
/// SQL and storage time), idle runs log with no rows, a failed run logs an ERROR and an outage logs nothing.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres, then works entirely inside it. */
[Trait("Cost", "Slow")]
public sealed class ProcedureTrendRunModelLiveTests
{
    private static readonly TimeSpan RunLogLag = TimeSpan.FromSeconds(3);

    [Fact]
    public async Task RawTrend_BusyIdleBusy_PlotsZeroAtTheIdleRunsOnly()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await RunModelStore.CreateAsync(ct);
        const int server = -544911;
        await store.RegisterAsync(server, ct);
        var h = store.Hour;
        foreach (var k in new[] { 0, 1, 2, 5 })
        {
            await store.RowAsync(server, "usp_Work", h.AddMinutes(k), elapsedUs: 600_000, executions: 10, interval: 60, ct);
        }
        for (var k = 0; k <= 5; k++)
        {
            await store.RunAsync(server, h.AddMinutes(k) + RunLogLag, durationMs: 3000, "SUCCESS", rowsCollected: k is 0 or 1 or 2 or 5 ? 1 : 0, ct);
        }

        var buckets = await store.ReadAsync(server, h, h.AddMinutes(5).AddSeconds(30), widthMinutes: 1, ct);

        Assert.Equal(Enumerable.Range(0, 6).Select(k => h.AddMinutes(k)), buckets.Select(b => b.Bucket));
        /* A zero is plotted at the two idle runs and nowhere else: minutes 3 and 4. */
        Assert.Equal(new[] { h.AddMinutes(3), h.AddMinutes(4) }, buckets.Where(b => b.ElapsedMsPerSecond == 0.0).Select(b => b.Bucket));
        Assert.All(buckets.Where(b => b.Bucket != h.AddMinutes(3) && b.Bucket != h.AddMinutes(4)), b => Assert.Equal(10.0, b.ElapsedMsPerSecond!.Value, precision: 6));
        Assert.All(buckets, b => Assert.Equal(1, b.CollectionCount));
    }

    [Fact]
    public async Task RawTrend_AFailedRunAddsNoPoint_AndTheNextPointsGapCoversIt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await RunModelStore.CreateAsync(ct);
        const int server = -544912;
        await store.RegisterAsync(server, ct);
        var h = store.Hour;
        /* Minute 1 fails and stores nothing; minute 2's delta measured the two minutes since minute 0. */
        await store.RowAsync(server, "usp_Work", h, elapsedUs: 600_000, executions: 10, interval: 60, ct);
        await store.RowAsync(server, "usp_Work", h.AddMinutes(2), elapsedUs: 1_200_000, executions: 20, interval: 120, ct);
        await store.RunAsync(server, h + RunLogLag, 3000, "SUCCESS", 1, ct);
        await store.RunAsync(server, h.AddMinutes(1) + RunLogLag, 3000, "ERROR", 0, ct);
        await store.RunAsync(server, h.AddMinutes(2) + RunLogLag, 3000, "SUCCESS", 1, ct);

        var buckets = await store.ReadAsync(server, h, h.AddMinutes(2).AddSeconds(30), widthMinutes: 10, ct);

        var bucket = Assert.Single(buckets);
        Assert.Equal(2, bucket.CollectionCount);
        Assert.Equal(1800.0 / 180.0, bucket.ElapsedMsPerSecond!.Value, precision: 6);
    }

    /// <summary>
    /// A 240-minute bucket: a point just before it, 60 busy minutes, an outage, a restart collection (0 work, interval 0) at
    /// minute 180, then 59 busy minutes. The gap from minute 59 to 180 is past the one-hour policy, so minute 180 is unrated, and
    /// the bucket's seconds are the rated gaps (3600 + 3540 = 7140), never the bucket's span.
    /// </summary>
    [Fact]
    public async Task RawTrend_AnOutageIsOutOfTheBucketsSeconds()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await RunModelStore.CreateAsync(ct);
        const int server = -544913;
        await store.RegisterAsync(server, ct);
        var h = store.Hour;
        var minutes = new List<int> { -1 };
        minutes.AddRange(Enumerable.Range(0, 60));
        minutes.Add(180);
        minutes.AddRange(Enumerable.Range(181, 59));
        foreach (var k in minutes)
        {
            var restart = k == 180;
            await store.RowAsync(server, "usp_Work", h.AddMinutes(k), elapsedUs: restart ? 0 : 600_000, executions: restart ? 0 : 1, interval: restart ? 0 : 60, ct);
            await store.RunAsync(server, h.AddMinutes(k) + RunLogLag, 3000, "SUCCESS", 1, ct);
        }

        var buckets = await store.ReadAsync(server, h.AddMinutes(-1), h.AddMinutes(239).AddSeconds(30), widthMinutes: 240, ct);

        var bucket = buckets.Single(b => b.Bucket == h);
        Assert.Equal(1, bucket.UnratedCollections);
        Assert.Equal(120, bucket.CollectionCount);
        Assert.Equal(119.0 / 7140.0, bucket.ExecutionsPerSecond!.Value, precision: 9);
    }

    /// <summary>
    /// One collection stored two procedures: P1 (measured over 60 s) and P2 (a plan first seen 30 minutes ago, 1800 s). The whole
    /// collection's deltas land in the run that stored them, so they divide by the gap to the previous run (60 s), and never by the
    /// largest interval any one row carried.
    /// </summary>
    [Fact]
    public async Task RawTrend_AReturningProceduresDeltaDividesByTheRunsGap()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await RunModelStore.CreateAsync(ct);
        const int server = -544914;
        await store.RegisterAsync(server, ct);
        var h = store.Hour;
        await store.RowAsync(server, "usp_P1", h, elapsedUs: 600_000, executions: 10, interval: 60, ct);
        await store.RowAsync(server, "usp_P1", h.AddMinutes(1), elapsedUs: 600_000, executions: 10, interval: 60, ct);
        await store.RowAsync(server, "usp_P2", h.AddMinutes(1), elapsedUs: 3_000_000, executions: 10, interval: 1800, ct);

        var buckets = await store.ReadAsync(server, h, h.AddMinutes(1).AddSeconds(30), widthMinutes: 1, ct);

        Assert.Equal((600.0 + 3000.0) / 60.0, buckets.Single(b => b.Bucket == h.AddMinutes(1)).ElapsedMsPerSecond!.Value, precision: 6);
    }

    [Fact]
    public async Task RawTrend_OneBusyAndTwoIdleRuns_ReadOver180Seconds()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await RunModelStore.CreateAsync(ct);
        const int server = -544915;
        await store.RegisterAsync(server, ct);
        var h = store.Hour;
        await store.RowAsync(server, "usp_Work", h, elapsedUs: 900_000, executions: 5, interval: 60, ct);
        await store.RunAsync(server, h + RunLogLag, 3000, "SUCCESS", 1, ct);
        await store.RunAsync(server, h.AddMinutes(1) + RunLogLag, 3000, "SUCCESS", 0, ct);
        await store.RunAsync(server, h.AddMinutes(2) + RunLogLag, 3000, "SUCCESS", 0, ct);

        var buckets = await store.ReadAsync(server, h, h.AddMinutes(2).AddSeconds(30), widthMinutes: 10, ct);

        var bucket = Assert.Single(buckets);
        Assert.Equal(3, bucket.CollectionCount);
        Assert.Equal(900.0 / 180.0, bucket.ElapsedMsPerSecond!.Value, precision: 6);
        Assert.Equal(5.0 / 180.0, bucket.ExecutionsPerSecond!.Value, precision: 9);
    }

    [Fact]
    public async Task RawTrend_AFirstGapThatReachesBeforeTheWindowIsTheGapNotTheStoredInterval()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await RunModelStore.CreateAsync(ct);
        const int server = -544916;
        await store.RegisterAsync(server, ct);
        var h = store.Hour;
        /* The previous run is five minutes before the window opens: still inside the hour's policy, so the first point's seconds are 300. */
        await store.RowAsync(server, "usp_Work", h.AddMinutes(-5), elapsedUs: 600_000, executions: 1, interval: 60, ct);
        await store.RowAsync(server, "usp_Work", h, elapsedUs: 600_000, executions: 3, interval: 60, ct);

        var buckets = await store.ReadAsync(server, h, h.AddSeconds(30), widthMinutes: 10, ct);

        var bucket = Assert.Single(buckets);
        Assert.Equal(1, bucket.CollectionCount);
        Assert.Equal(600.0 / 300.0, bucket.ElapsedMsPerSecond!.Value, precision: 6);
    }

    [Fact]
    public async Task RawTrend_AGapPastTheHourIsAnUnratedPoint_AndStillCounted()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await RunModelStore.CreateAsync(ct);
        const int server = -544917;
        await store.RegisterAsync(server, ct);
        var h = store.Hour;
        await store.RowAsync(server, "usp_Work", h, elapsedUs: 600_000, executions: 1, interval: 60, ct);
        await store.RowAsync(server, "usp_Work", h.AddSeconds(4000), elapsedUs: 600_000, executions: 1, interval: 60, ct);

        var buckets = await store.ReadAsync(server, h, h.AddSeconds(4030), widthMinutes: 10, ct);

        Assert.Equal(2, buckets.Count);
        Assert.NotNull(buckets[0].ElapsedMsPerSecond);
        Assert.Null(buckets[1].ElapsedMsPerSecond);
        Assert.Equal(1, buckets[1].UnratedCollections);
        Assert.Equal(1, buckets[1].CollectionCount);
    }

    [Fact]
    public async Task RawTrend_ARestartCollectionIsUnrated_EvenAtAOneMinuteGap()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await RunModelStore.CreateAsync(ct);
        const int server = -544918;
        await store.RegisterAsync(server, ct);
        var h = store.Hour;
        await store.RowAsync(server, "usp_Work", h, elapsedUs: 600_000, executions: 1, interval: 60, ct);
        await store.RowAsync(server, "usp_Work", h.AddMinutes(1), elapsedUs: 0, executions: 0, interval: 0, ct);
        await store.RowAsync(server, "usp_Work", h.AddMinutes(2), elapsedUs: 600_000, executions: 1, interval: 60, ct);

        var buckets = await store.ReadAsync(server, h, h.AddMinutes(2).AddSeconds(30), widthMinutes: 10, ct);

        var bucket = Assert.Single(buckets);
        Assert.Equal(1, bucket.UnratedCollections);
        Assert.Equal(3, bucket.CollectionCount);
        Assert.Equal(1200.0 / 120.0, bucket.ElapsedMsPerSecond!.Value, precision: 6);
    }

    [Fact]
    public async Task RawTrend_TheStoresFirstCollectionUsesItsOwnInterval()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await RunModelStore.CreateAsync(ct);
        const int server = -544919;
        await store.RegisterAsync(server, ct);
        var h = store.Hour;
        await store.RowAsync(server, "usp_Work", h, elapsedUs: 900_000, executions: 9, interval: 45, ct);

        var buckets = await store.ReadAsync(server, h, h.AddSeconds(30), widthMinutes: 10, ct);

        Assert.Equal(900.0 / 45.0, Assert.Single(buckets).ElapsedMsPerSecond!.Value, precision: 6);
    }

    /// <summary>
    /// An idle run that started inside the window but was logged just after its end still plots its 0: runs are read an hour past
    /// the window's end, and the points stop at it. The run began at minute 2 (logged at minute 2 + 3 s, 3,000 ms of work), and the
    /// window ends one second after it began.
    /// </summary>
    [Fact]
    public async Task RawTrend_AnIdleRunThatStartedBeforeTheWindowEndAndWasLoggedAfterItPlotsItsZero()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await RunModelStore.CreateAsync(ct);
        const int server = -544920;
        await store.RegisterAsync(server, ct);
        var h = store.Hour;
        foreach (var k in new[] { 0, 1 })
        {
            await store.RowAsync(server, "usp_Work", h.AddMinutes(k), elapsedUs: 600_000, executions: 10, interval: 60, ct);
            await store.RunAsync(server, h.AddMinutes(k) + RunLogLag, 3000, "SUCCESS", 1, ct);
        }
        await store.RunAsync(server, h.AddMinutes(2) + RunLogLag, 3000, "SUCCESS", 0, ct);

        var buckets = await store.ReadAsync(server, h, h.AddMinutes(2).AddSeconds(1), widthMinutes: 1, ct);

        Assert.Equal(Enumerable.Range(0, 3).Select(k => h.AddMinutes(k)), buckets.Select(b => b.Bucket));
        Assert.Equal(0.0, buckets[2].ElapsedMsPerSecond!.Value, precision: 6);
        Assert.Equal(10.0, buckets[1].ElapsedMsPerSecond!.Value, precision: 6);
    }

    /// <summary>
    /// The same edge for a busy run: its rows (stamped at its start) are inside the window and its log row is just after the end.
    /// The run read past the window's end must not make that run an idle one: the minute has its one point, the stored work.
    /// </summary>
    [Fact]
    public async Task RawTrend_ABusyRunLoggedAfterTheWindowEndIsNotAnIdleRun()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var store = await RunModelStore.CreateAsync(ct);
        const int server = -544921;
        await store.RegisterAsync(server, ct);
        var h = store.Hour;
        foreach (var k in new[] { 0, 1, 2 })
        {
            await store.RowAsync(server, "usp_Work", h.AddMinutes(k), elapsedUs: 600_000, executions: 10, interval: 60, ct);
            await store.RunAsync(server, h.AddMinutes(k) + RunLogLag, 3000, "SUCCESS", 1, ct);
        }
        /* The next run's rows are past the window's end, so they are not read; its log row, an hour of reads past the end, is. */
        await store.RowAsync(server, "usp_Work", h.AddMinutes(3), elapsedUs: 600_000, executions: 10, interval: 60, ct);
        await store.RunAsync(server, h.AddMinutes(3) + RunLogLag, 3000, "SUCCESS", 1, ct);

        var buckets = await store.ReadAsync(server, h, h.AddMinutes(2).AddSeconds(1), widthMinutes: 1, ct);

        Assert.Equal(Enumerable.Range(0, 3).Select(k => h.AddMinutes(k)), buckets.Select(b => b.Bucket));
        Assert.All(buckets, b => Assert.Equal(10.0, b.ElapsedMsPerSecond!.Value, precision: 6));
        Assert.All(buckets, b => Assert.Equal(1, b.CollectionCount));
    }

    internal sealed record TrendPoint(
        DateTime Bucket, double? ElapsedMsPerSecond, double? ExecutionsPerSecond, double? PeakElapsedMsPerSecond,
        DateTime FirstCollection, int UnratedCollections, int CollectionCount);

    private sealed class RunModelStore : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;
        private readonly NpgsqlDataSource _dataSource;
        private int _logId;

        private RunModelStore(ScratchPostgres scratch, NpgsqlDataSource dataSource, DateTime hour)
        {
            _scratch = scratch;
            _dataSource = dataSource;
            Hour = hour;
        }

        /// <summary>A 4-hour boundary in the past, so every bucket width used here (1, 10 and 240 minutes) starts on it.</summary>
        public DateTime Hour { get; }

        public static async Task<RunModelStore> CreateAsync(CancellationToken ct)
        {
            var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
                "Set DARLING_TEST_PG to a Postgres connection string to run the live procedure_stats run-model tests.");

            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            try
            {
                await using var connection = new NpgsqlConnection(scratch.ConnectionString);
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
                var now = DateTime.UtcNow.AddDays(-1);
                var hour = new DateTime(now.Year, now.Month, now.Day, now.Hour - (now.Hour % 4), 0, 0, DateTimeKind.Unspecified);
                return new RunModelStore(scratch, NpgsqlDataSource.Create(scratch.ConnectionString), hour);
            }
            catch
            {
                await scratch.DisposeAsync();
                throw;
            }
        }

        public async Task RegisterAsync(int serverId, CancellationToken ct)
        {
            await using var connection = await _dataSource.OpenConnectionAsync(ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, serverId, $"run-model-{-serverId}", ct);
        }

        public async Task RowAsync(int serverId, string procedure, DateTime at, long elapsedUs, long executions, int interval, CancellationToken ct)
        {
            await using var insert = _dataSource.CreateCommand(@"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ((SELECT COALESCE(MAX(collection_id), 0) + 1 FROM collect.procedure_stats), $1, $2, 'run-model', 'AppDb', 'dbo', $3, $3, $4, $4, $5, $6)");
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(at, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(procedure);
            insert.Parameters.AddWithValue(elapsedUs);
            insert.Parameters.AddWithValue(executions);
            insert.Parameters.AddWithValue(interval);
            await insert.ExecuteNonQueryAsync(ct);
        }

        public async Task RunAsync(int serverId, DateTime loggedAt, long durationMs, string status, int rowsCollected, CancellationToken ct)
        {
            await using var insert = _dataSource.CreateCommand(@"
INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
VALUES ($1, $2, 'run-model', 'procedure_stats', $3, $4, $5, $6)");
            insert.Parameters.AddWithValue(++_logId);
            insert.Parameters.AddWithValue(serverId);
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(loggedAt, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(durationMs);
            insert.Parameters.AddWithValue(status);
            insert.Parameters.AddWithValue(rowsCollected);
            await insert.ExecuteNonQueryAsync(ct);
        }

        public async Task<List<TrendPoint>> ReadAsync(int serverId, DateTime start, DateTime end, int widthMinutes, CancellationToken ct)
        {
            await using var command = _dataSource.CreateCommand(DurationTrendRouting.BuildBucketedRawTrendSql("procedure_stats", withDatabaseFilter: false));
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(widthMinutes);
            var rows = new List<TrendPoint>();
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                rows.Add(new TrendPoint(
                    reader.GetDateTime(0),
                    reader.IsDBNull(1) ? null : reader.GetDouble(1),
                    reader.IsDBNull(2) ? null : reader.GetDouble(2),
                    reader.IsDBNull(3) ? null : reader.GetDouble(3),
                    reader.GetDateTime(4),
                    (int)reader.GetInt64(5),
                    (int)reader.GetInt64(6)));
            }
            return rows;
        }

        public async ValueTask DisposeAsync()
        {
            await _dataSource.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}
