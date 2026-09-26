/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4299 (d') rig probe 1: does <c>lock_timeout</c> apply INSIDE <c>run_job</c>, or only around a plain
/// <c>SELECT</c>? <see cref="TimescaleSupport.RunRetentionPurgeJobSql"/> is the service's own trigger for a
/// raw retention job under the never-scheduled design, and its whole safety argument depends on the answer
/// being yes: a purge blocked behind a live reader must give up within the bound and let the NEXT hourly
/// pass retry it, never hold the target chunk range's lock request open indefinitely.
///
/// <para>Proven on the rig separately from this xUnit pin (#4299): a second session holds
/// <c>ACCESS EXCLUSIVE</c> on the oldest chunk, then <c>CALL run_job(id)</c> wrapped in
/// <c>BEGIN; SET LOCAL lock_timeout; ...; COMMIT;</c> raised <c>55P03 lock_not_available</c> at ~5s, not at
/// PostgreSQL's default `run_job` machinery, and the chunk count was unchanged afterward. This test holds
/// the same claim as a repeatable in-repo pin.</para>
///
/// <para><b>#1776 own-store</b>: deliberately NOT <c>[Collection("live-postgres")]</c>. Goes through
/// <see cref="ScratchPostgres.CreateAsync"/> and never touches the shared database's tables.</para>
/// </summary>
public sealed class RawRetentionPurgeJobLiveTests
{
    private const string Raw = "query_stats";
    private const int SetupTimeoutSeconds = 60;

    [Fact]
    public async Task RunRetentionPurgeJob_BlockedOnAConflictingLock_FailsWithinLockTimeout_InsteadOfBlocking()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4299 lock_timeout probe.");

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, default);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync();
        await PgMigrations.MigrateAsync(connection, default);

        var enabled = await TimescaleSupport.TryEnableAsync(connection, null, default);
        Assert.SkipWhen(!enabled, "The live #4299 lock_timeout probe needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, default);

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection) { CommandTimeout = SetupTimeoutSeconds })
        {
            await stop.ExecuteNonQueryAsync();
        }

        var bodySucceeded = false;
        NpgsqlConnection? locker = null;
        try
        {
            /* Seed a chunk old enough that add_retention_policy's drop_after would target it, so run_job has
               something to try to drop rather than a no-op pass. Same column shape as the other live seeds
               in this project (e.g. MaterializationHoleScanShapeTests' restart row). */
            await using (var seed = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES (-1, now() - interval '30 days', 1, 'probe-server', 'ProbeDb', '0xPROBEHASH', '0xPROBEHANDLE', 0, 0, 0, 0)", connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                await seed.ExecuteNonQueryAsync();
            }

            long jobId;
            await using (var arm = new NpgsqlCommand(
                "SELECT add_retention_policy('collect.query_stats', drop_after => interval '7 days') AS job_id", connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                jobId = (long)(int)(await arm.ExecuteScalarAsync())!;
            }

            string? oldestChunk;
            await using (var chunkRead = new NpgsqlCommand(
                "SELECT chunk_name FROM timescaledb_information.chunks WHERE hypertable_name = 'query_stats' ORDER BY range_start LIMIT 1", connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                oldestChunk = (string?)await chunkRead.ExecuteScalarAsync();
            }
            Assert.NotNull(oldestChunk);

            var beforeChunks = await CountChunksAsync(connection);

            /* Second session: hold ACCESS EXCLUSIVE on the target chunk in an open transaction, well longer
               than the lock_timeout RunRetentionPurgeJobSql sets, so a leak (lock_timeout not applying inside
               run_job) would hang this test rather than fail it fast — deliberate, so a regression is loud. */
            locker = new NpgsqlConnection(scratch.ConnectionString);
            await locker.OpenAsync();
            await using (var beginLock = new NpgsqlCommand("BEGIN", locker) { CommandTimeout = SetupTimeoutSeconds })
            {
                await beginLock.ExecuteNonQueryAsync();
            }
            await using (var takeLock = new NpgsqlCommand(
                $"LOCK TABLE _timescaledb_internal.{oldestChunk} IN ACCESS EXCLUSIVE MODE", locker) { CommandTimeout = SetupTimeoutSeconds })
            {
                await takeLock.ExecuteNonQueryAsync();
            }

            var stopwatch = System.Diagnostics.Stopwatch.StartNew();
            var outcome = await TimescaleSupport.RunRetentionPurgeJobAsync(connection, jobId, null, default);
            stopwatch.Stop();

            Assert.False(outcome.Ran, "a run blocked behind a conflicting lock must fail (and be retried the next pass), not succeed");
            /* #4299: RunRetentionPurgeJobSql sent BEGIN/SET LOCAL/CALL/COMMIT as ONE NpgsqlCommand, which
               Npgsql's positional-parameter mode always rejects with 42601 "cannot insert multiple commands
               into a prepared statement" — so this assertion previously passed on EVERY run, blocked or not,
               because it only checked the bool and the elapsed time, and 42601 fails fast too. Asserting the
               SqlState pins the REAL reason: a lock timeout, not a syntax error the CALL never reached. */
            Assert.Equal("55P03", outcome.SqlState);
            Assert.True(
                stopwatch.Elapsed < TimeSpan.FromSeconds(20),
                $"run_job took {stopwatch.Elapsed.TotalSeconds:F1}s to fail — lock_timeout is not bounding it inside run_job " +
                "(it blocked instead of erroring near the bound)");

            /* Release the lock, then confirm the chunk count did not change: the failed run must not have
               partially dropped anything before it hit the lock. */
            await using (var rollbackLock = new NpgsqlCommand("ROLLBACK", locker) { CommandTimeout = SetupTimeoutSeconds })
            {
                await rollbackLock.ExecuteNonQueryAsync();
            }

            var afterChunks = await CountChunksAsync(connection);
            Assert.Equal(beforeChunks, afterChunks);

            bodySucceeded = true;
        }
        finally
        {
            if (locker is not null)
            {
                await locker.DisposeAsync();
            }

            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);
                await batch.RemoveRetentionPolicyAsync(Raw, cleanupCt);
            });

            await connection.DisposeAsync();
        }
    }

    /// <summary>
    /// #4299: the success path. The original one-command SQL always raised 42601 and
    /// <see cref="TimescaleSupport.RunRetentionPurgeJobAsync"/> swallowed it into <c>false</c> every time,
    /// blocked or not. Unblocked, with a chunk older than <c>drop_after</c>: the CALL must actually run and
    /// the chunk must actually drop.
    /// </summary>
    [Fact]
    public async Task RunRetentionPurgeJob_Unblocked_RunsAndDropsTheOldChunk()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4299 purge-success probe.");

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, default);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync();
        await PgMigrations.MigrateAsync(connection, default);

        var enabled = await TimescaleSupport.TryEnableAsync(connection, null, default);
        Assert.SkipWhen(!enabled, "The live #4299 purge-success probe needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, default);

        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection) { CommandTimeout = SetupTimeoutSeconds })
        {
            await stop.ExecuteNonQueryAsync();
        }

        var bodySucceeded = false;
        try
        {
            await using (var seed = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES (-1, now() - interval '30 days', 1, 'probe-server', 'ProbeDb', '0xPROBEHASH', '0xPROBEHANDLE', 0, 0, 0, 0)", connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                await seed.ExecuteNonQueryAsync();
            }

            long jobId;
            await using (var arm = new NpgsqlCommand(
                "SELECT add_retention_policy('collect.query_stats', drop_after => interval '7 days') AS job_id", connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                jobId = (long)(int)(await arm.ExecuteScalarAsync())!;
            }

            var beforeChunks = await CountChunksAsync(connection);
            Assert.True(beforeChunks > 0, "the seed row must have created at least one chunk for run_job to have something to drop");

            var outcome = await TimescaleSupport.RunRetentionPurgeJobAsync(connection, jobId, null, default);

            Assert.True(outcome.Ran, $"the unblocked run must succeed; SqlState={outcome.SqlState ?? "(none)"}");
            Assert.Null(outcome.SqlState);

            var afterChunks = await CountChunksAsync(connection);
            Assert.True(afterChunks < beforeChunks, $"the old chunk must have been dropped: before={beforeChunks}, after={afterChunks}");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                var batch = new LiveCleanupBatch(cleanup);
                await batch.RemoveRetentionPolicyAsync(Raw, cleanupCt);
            });

            await connection.DisposeAsync();
        }
    }

    private static async Task<long> CountChunksAsync(NpgsqlConnection connection)
    {
        await using var count = new NpgsqlCommand(
            "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'query_stats'", connection) { CommandTimeout = SetupTimeoutSeconds };
        return (long)(await count.ExecuteScalarAsync())!;
    }
}
