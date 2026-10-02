/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4247: a guarded <c>DROP INDEX</c> blocked by a concurrent reader must give up at its 5 s
/// <c>lock_timeout</c> rather than hold up the sweep, and it must leave the connection USABLE for the
/// statements after it in <see cref="PgTableTuning.Statements"/> — the <c>ROLLBACK</c> recovery this PR adds
/// to <see cref="PgTableTuning.ApplyAsync"/>. A lock_timeout failure aborts the guarded drop's own explicit
/// <c>BEGIN</c>, and Npgsql's simple-query protocol never reaches the trailing <c>COMMIT</c> text in the same
/// command, so without a recovery <c>ROLLBACK</c> the connection is left "in failed transaction" for every
/// later statement in the sweep — proven below by removing the recovery locally and watching the
/// later-statement assertion fail (not committed; see the PR body for the observed failure text).
///
/// <para><b>#1776 own-store</b>: mints its own scratch database (recreates one of the five now-dropped
/// indexes by hand, holds a real table lock against it from a second connection, and asserts on the recovery
/// path), so it cannot race the shared store and is not serialized against the <c>live-postgres</c>
/// collection.</para>
/// </summary>
public sealed class TuningDropLockRecoveryLiveTests
{
    [Fact]
    public async Task BlockedDrop_GivesUpAtItsTimeout_AndTheSweepRecoversForLaterStatements()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #4247 blocked-drop live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        await using var body = new NpgsqlConnection(scratch.ConnectionString);
        await body.OpenAsync(ct);
        await PgMigrations.MigrateAsync(body, ct);

        /* The pre-#4247 shape of one of the five dropped indexes, recreated by hand: PgTableTuning no longer
           creates it, so a store still carrying it (a field box from before this PR) is exactly the case the
           guarded drop exists for. */
        await using (var create = new NpgsqlCommand(
            "CREATE INDEX idx_query_stats_server_handle_time ON collect.query_stats (server_id, sql_handle, collection_time DESC)",
            body))
        {
            await create.ExecuteNonQueryAsync(ct);
        }

        /* The blocker: an open transaction holding ACCESS SHARE on the table via a real LOCK TABLE statement
           (not an advisory lock). A guarded DROP INDEX needs ACCESS EXCLUSIVE, which ACCESS SHARE conflicts
           with, so the drop of the index above queues behind this and must wait out its lock_timeout. */
        await using var blocker = new NpgsqlConnection(scratch.ConnectionString);
        await blocker.OpenAsync(ct);
        await using (var begin = new NpgsqlCommand("BEGIN", blocker))
        {
            await begin.ExecuteNonQueryAsync(ct);
        }

        await using (var @lock = new NpgsqlCommand("LOCK TABLE collect.query_stats IN ACCESS SHARE MODE", blocker))
        {
            await @lock.ExecuteNonQueryAsync(ct);
        }

        try
        {
            var clock = Stopwatch.StartNew();
            var applied = await PgTableTuning.ApplyAsync(body, NullLogger.Instance, ct);
            clock.Stop();

            /* Well under the 300 s SetupTimeoutSeconds ApplyAsync would otherwise be willing to spend — the
               whole point of the 5 s lock_timeout on the guarded drop is that one blocked statement costs
               about 5 s, not the command's full budget. */
            Assert.True(clock.Elapsed < TimeSpan.FromSeconds(60),
                $"ApplyAsync took {clock.Elapsed.TotalSeconds:F1}s against a single blocked drop — the guarded " +
                "5s lock_timeout should have bounded it well under 60s.");

            /* Every statement except the one deliberately blocked applies cleanly — the sweep continues past
               a single failure rather than aborting the whole batch. */
            Assert.Equal(PgTableTuning.Statements.Count - 1, applied);

            /* Read the outcome on a FRESH connection: if the ROLLBACK recovery were missing, `body` itself
               would still be stuck "in failed transaction" and even a SELECT against it would throw 25P02 —
               which would make these assertions fail for the wrong reason instead of reporting cleanly on
               what actually happened in the store. */
            await using var observer = new NpgsqlConnection(scratch.ConnectionString);
            await observer.OpenAsync(ct);

            /* The blocked drop's own index: still there, because the drop gave up rather than eventually
               succeeding once the blocker committed (it never got a second attempt inside this ApplyAsync
               call — the next idempotent start is what retries it). */
            Assert.True(await IndexExistsAsync(observer, "idx_query_stats_server_handle_time", ct),
                "the index whose DROP was blocked should still exist — the guarded drop must give up, not wait out the block.");

            /* A statement AFTER the blocked drop in PgTableTuning.Statements, on the SAME table
               (idx_query_stats_server_hash_time is a plain CREATE INDEX, which takes SHARE — compatible with
               the blocker's held ACCESS SHARE, so it applies normally): this is the assertion a missing
               ROLLBACK recovery breaks, because without it `body` stays "in failed transaction" for every
               statement after the blocked drop and this CREATE INDEX never runs. */
            Assert.True(await IndexExistsAsync(observer, "idx_query_stats_server_hash_time", ct),
                "a statement listed after the blocked drop failed to apply — the post-failure ROLLBACK recovery regressed.");

            /* The connection ApplyAsync actually used is itself still usable afterward — the ROLLBACK fix's
               other half: a caller that reuses this connection for the rest of a service start must not
               inherit a broken session. */
            await using (var probe = new NpgsqlCommand("SELECT 1", body))
            {
                await probe.ExecuteScalarAsync(ct);
            }
        }
        finally
        {
            await using var rollbackBlocker = new NpgsqlCommand("ROLLBACK", blocker);
            await rollbackBlocker.ExecuteNonQueryAsync(CancellationToken.None);
        }
    }

    private static async Task<bool> IndexExistsAsync(NpgsqlConnection connection, string indexName, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT 1 FROM pg_indexes WHERE schemaname = 'collect' AND indexname = $1", connection);
        command.Parameters.AddWithValue(indexName);
        var result = await command.ExecuteScalarAsync(ct);
        return result is not null;
    }
}
