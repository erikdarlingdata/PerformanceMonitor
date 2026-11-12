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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8156: the PostgreSQL-target rates divide by the seconds the collector OBSERVED, so the counter deltas they
/// divide must be the deltas the collector observed too. The coverage witness drops any interval longer than
/// <see cref="CollectorDeltaCalculator.DefaultMaxGapSeconds"/> from the observed time; a difference taken across
/// that same interval spans the outage and is left out of the sums. Otherwise a two-hour hole in a four-hour window
/// doubles every per-second and per-hour figure the pass states, and the trend halves read the hole as a busy half.
///
/// <para>The seed: one-minute rows for the first and the last hour of a four-hour window, nothing in between. Every
/// counter climbs by a fixed step a minute, so the figures outside the hole are 120 intervals' worth and the hole's
/// one interval, if counted, holds the other 120 minutes' worth. Observed time is 7,200 s either way; only the
/// numerators tell the two readings apart. The window is anchored through <see cref="LiveClock"/>.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetRatesAcrossGapLiveTests
{
    private const string ServerName = "darling-pg-target-gap-rates-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /// <summary>Rows at minutes 0..60 and 180..240 of the window: the interval from minute 60 to minute 180 is the hole.</summary>
    private const string SeededMinutes = "FROM generate_series(0, 240) AS n WHERE n <= 60 OR n >= 180";

    /// <summary>The intervals outside the hole: 60 in the first hour, 60 in the last.</summary>
    private const int ObservedIntervals = 120;

    private const double ObservedSeconds = ObservedIntervals * 60.0;

    [Fact]
    public async Task TheCounterRates_LeaveTheHoleOutOfTheNumerator_AsTheWitnessLeavesItOutOfTheSeconds()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the PostgreSQL-target gap-rate test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, ServerId, ServerName, "postgres", 18, ct);

            var end = LiveClock.AnchoredEnd();
            var start = end.AddHours(-4);

            /* 100 commits, 1 rollback and 1 deadlock a minute; 1,000 block hits a minute. */
            await PlantAsync(connection, @"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, 'appdb',
       100 * n, n, 0, 1000 * n, 0, 0, n, NULL
" + SeededMinutes, start, ct);

            /* 1,000 reads a minute at 1 ms each, 10 shared_buffers evictions a minute; no write counter. */
            await PlantAsync(connection, @"
INSERT INTO pg_io_stats
    (collection_id, collection_time, server_id, server_name, backend_type, object_type, context,
     reads, read_time_ms, writes, write_time_ms, evictions, stats_reset)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, 'client backend', 'relation', 'normal',
       1000 * n, 1000 * n, NULL, NULL, 10 * n, NULL
" + SeededMinutes, start, ct);

            /* One timed checkpoint a minute, no requested ones, WAL not reported. */
            await PlantAsync(connection, @"
INSERT INTO pg_write_stats
    (collection_id, collection_time, server_id, server_name,
     num_timed, num_requested, num_done, checkpoint_write_time_ms, checkpoint_sync_time_ms, buffers_written_checkpoint, checkpointer_stats_reset,
     buffers_clean, maxwritten_clean, buffers_alloc, buffers_backend, buffers_backend_fsync, bgwriter_stats_reset,
     wal_records, wal_fpi, wal_bytes, wal_buffers_full, wal_write, wal_sync, wal_write_time_ms, wal_sync_time_ms, wal_stats_reset)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4,
       500 + n, 1000, 1500 + n, 0, 0, 0, NULL,
       0, 0, 0, NULL, NULL, NULL,
       NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
" + SeededMinutes, start, ct);

            var collector = new PgTargetFactCollector(postgres);
            var context = new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = start,
                TimeRangeEnd = end,
                ServerUtcOffset = TimeSpan.Zero,
            };
            var facts = await collector.CollectFactsAsync(context);

            /* The witness: two observed hours of four, the hole discarded, and the gap fact beside the rates. */
            Assert.True(context.Coverage!.IsPartial);
            Assert.Equal(ObservedSeconds * 1000, context.ObservedDurationMs, precision: 3);
            Assert.Contains(facts, f => f.Key == WindowCoverage.FactKey);

            /* pg_database_stats: 120 observed intervals of 101 transactions and one deadlock. */
            var tps = Assert.Single(facts, f => f.Key == PgTargetFactKeys.Tps);
            Assert.Equal(101 * ObservedIntervals / ObservedSeconds, tps.Value, precision: 6);
            Assert.Equal(100 * ObservedIntervals, tps.Metadata[PgTargetScorer.TpsCommitsKey]);
            Assert.Equal(ObservedIntervals, tps.Metadata[PgTargetScorer.TpsRollbacksKey]);
            Assert.Equal(ObservedIntervals, tps.Metadata[PgTargetScorer.CounterIntervalsKey]);
            Assert.Equal(ObservedIntervals + 2, tps.Metadata[PgTargetScorer.CounterSampleCountKey]);

            /* The halves: the midpoint falls inside the hole, so each half is 60 intervals over 3,600 observed
               seconds, and the trend is flat. Apportioning observed seconds by interval count would hand the hole's
               interval to the second half and read it as a busy half. */
            Assert.Equal(101 * 60 / 3_600.0, tps.Metadata[PgTargetScorer.TpsFirstHalfKey], precision: 6);
            Assert.Equal(101 * 60 / 3_600.0, tps.Metadata[PgTargetScorer.TpsSecondHalfKey], precision: 6);
            Assert.Equal(0.0, tps.Metadata[PgTargetScorer.TpsTrendKey], precision: 6);

            var deadlock = Assert.Single(facts, f => f.Key == PgTargetFactKeys.DeadlockRate);
            Assert.Equal(ObservedIntervals, deadlock.Metadata[PgTargetScorer.DeadlockCounterCountKey]);
            Assert.Equal(ObservedIntervals / (ObservedSeconds / 3_600.0), deadlock.Value, precision: 6);

            /* pg_io_stats: the read fact's operations and their rate. */
            var ioRead = Assert.Single(facts, f => f.Key == PgTargetFactKeys.IoReadLatencyMs);
            Assert.Equal(1000 * ObservedIntervals, ioRead.Metadata[PgTargetScorer.IoOpsKey]);
            Assert.Equal(1000 * ObservedIntervals / ObservedSeconds, ioRead.Metadata[PgTargetScorer.IoOpsPerSecKey], precision: 6);

            /* pg_write_stats: the checkpoint count and its hourly rate. */
            var checkpoint = Assert.Single(facts, f => f.Key == PgTargetFactKeys.CheckpointPressure);
            Assert.Equal(ObservedIntervals, checkpoint.Metadata["checkpoints_timed"]);
            Assert.Equal(ObservedIntervals / (ObservedSeconds / 3_600.0), checkpoint.Metadata["checkpoints_per_hour"], precision: 6);

            /* The buffer composite: the block requests of the database arm and the evictions of the I/O arm. */
            var buffer = Assert.Single(facts, f => f.Key == PgTargetFactKeys.BufferCachePressure);
            Assert.Equal(1000 * ObservedIntervals, buffer.Metadata["blocks_total"]);
            Assert.Equal(1000 * ObservedIntervals / ObservedSeconds, buffer.Metadata["block_requests_per_sec"], precision: 6);
            Assert.Equal(10 * ObservedIntervals, buffer.Metadata["evictions"]);
            Assert.Equal(10 * ObservedIntervals / ObservedSeconds, buffer.Metadata["evictions_per_sec"], precision: 6);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task PlantAsync(NpgsqlConnection connection, string sql, DateTime start, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(start);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_io_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_write_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
