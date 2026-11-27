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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// #8949: on a store whose settings are already in place the tuning pass issues no <c>ALTER TABLE ... SET</c>. The ALTER takes
/// SHARE UPDATE EXCLUSIVE even when it changes nothing, which cancels an autovacuum on the table and, on a hypertable, on every
/// chunk. The proof that none is issued is a lock: another session holds ACCESS EXCLUSIVE on the table, where an issued ALTER
/// waits out its 5 s lock_timeout and the pass logs the table as busy. A pass that logs nothing of the kind issued no ALTER and
/// read nothing from the table. The other tests show a changed or reset setting is still put back, on the table and on a chunk.
/// </summary>
public sealed class TuningConvergedAlterLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the converged-ALTER tuning live tests.";

    private const string NeedsTimescale = "TimescaleDB is not available on this cluster, so there is no hypertable or chunk to test.";

    private static string StatementFor(string table) =>
        PgTableTuning.Statements.Single(s => s.StartsWith("ALTER TABLE " + table + " SET ", StringComparison.Ordinal));

    [Fact]
    public async Task AConvergedStore_IssuesNoAlter_OnAPlainTableLockedExclusively()
    {
        await RunAsync(async (scratch, connection, ct) =>
        {
            await TuningStartPasses.ConvergeAsync(connection, ct);
            Assert.True(await PgTableTuning.AlterIsConvergedAsync(connection, StatementFor("collect.query_plan_dim"), null, ct));

            await AssertNoAlterIsIssuedWhileLockedAsync(scratch.ConnectionString, connection, "collect.query_plan_dim", ct);
        });
    }

    [Fact]
    public async Task AConvergedStore_IssuesNoAlter_OnAHypertableLockedExclusivelyWithItsChunks()
    {
        await RunAsync(async (scratch, connection, ct) =>
        {
            Assert.SkipUnless(scratch.TimescaleEnabled, NeedsTimescale);
            await ConvertToHypertablesAsync(connection, ct);
            await SeedQueryStatsRowAsync(connection, ct);
            await TuningStartPasses.ConvergeAsync(connection, ct);

            var chunks = await ReadChildOptionsAsync(connection, "collect.query_stats", ct);
            Assert.NotEmpty(chunks);
            Assert.True(await PgTableTuning.AlterIsConvergedAsync(connection, StatementFor("collect.query_stats"), null, ct));

            /* LOCK TABLE on the root takes the same lock on every chunk. */
            await AssertNoAlterIsIssuedWhileLockedAsync(scratch.ConnectionString, connection, "collect.query_stats", ct);

            /* A chunk that lost a setting makes the statement due again, and the pass puts it back through the root. */
            var chunk = chunks.Keys.First();
            await ExecAsync(connection, "ALTER TABLE " + chunk + " RESET (autovacuum_vacuum_insert_scale_factor)", ct);
            Assert.DoesNotContain("autovacuum_vacuum_insert_scale_factor=0.02", (await ReadChildOptionsAsync(connection, "collect.query_stats", ct))[chunk]);
            Assert.False(await PgTableTuning.AlterIsConvergedAsync(connection, StatementFor("collect.query_stats"), null, ct));

            await PgTableTuning.ApplyAsync(connection, NullLogger.Instance, hourly: true, ct);
            Assert.Contains("autovacuum_vacuum_insert_scale_factor=0.02", (await ReadChildOptionsAsync(connection, "collect.query_stats", ct))[chunk]);
            Assert.True(await PgTableTuning.AlterIsConvergedAsync(connection, StatementFor("collect.query_stats"), null, ct));
        });
    }

    [Fact]
    public async Task AChangedOrResetSetting_IsPutBackByTheNextPass()
    {
        await RunAsync(async (_, connection, ct) =>
        {
            const string table = "collect.query_plan_dim";
            var statement = StatementFor(table);
            await TuningStartPasses.ConvergeAsync(connection, ct);
            Assert.True(await PgTableTuning.AlterIsConvergedAsync(connection, statement, null, ct));

            /* The key stays, the value moves: a check on the key alone would leave this as it is. */
            await ExecAsync(connection, "ALTER TABLE " + table + " SET (autovacuum_vacuum_scale_factor = 0.2)", ct);
            Assert.Contains("autovacuum_vacuum_scale_factor=0.2", await ReadOptionsAsync(connection, table, ct));
            Assert.False(await PgTableTuning.AlterIsConvergedAsync(connection, statement, null, ct));

            await PgTableTuning.ApplyAsync(connection, NullLogger.Instance, hourly: true, ct);
            var afterHourly = await ReadOptionsAsync(connection, table, ct);
            Assert.Contains("autovacuum_vacuum_scale_factor=0.02", afterHourly);
            Assert.Contains("autovacuum_vacuum_threshold=10000", afterHourly);
            Assert.DoesNotContain("autovacuum_vacuum_scale_factor=0.2", afterHourly);

            await ExecAsync(connection, "ALTER TABLE " + table + " RESET (autovacuum_vacuum_threshold)", ct);
            Assert.DoesNotContain("autovacuum_vacuum_threshold=10000", await ReadOptionsAsync(connection, table, ct));
            Assert.False(await PgTableTuning.AlterIsConvergedAsync(connection, statement, null, ct));

            await PgTableTuning.ApplyAsync(connection, NullLogger.Instance, ct);
            var afterStart = await ReadOptionsAsync(connection, table, ct);
            Assert.Contains("autovacuum_vacuum_scale_factor=0.02", afterStart);
            Assert.Contains("autovacuum_vacuum_threshold=10000", afterStart);

            /* A setting the statement does not name is not the pass's to judge: the table is still converged and keeps it. */
            await ExecAsync(connection, "ALTER TABLE " + table + " SET (autovacuum_analyze_threshold = 500)", ct);
            Assert.True(await PgTableTuning.AlterIsConvergedAsync(connection, statement, null, ct));
            await PgTableTuning.ApplyAsync(connection, NullLogger.Instance, hourly: true, ct);
            Assert.Contains("autovacuum_analyze_threshold=500", await ReadOptionsAsync(connection, table, ct));

            /* A table that is not there is not converged, so its ALTER is issued and fails with the pass's usual Warning. */
            Assert.False(await PgTableTuning.AlterIsConvergedAsync(connection, "ALTER TABLE collect.no_such_table_8949 SET (fillfactor = 80)", null, ct));
        });
    }

    /// <summary>
    /// What the chunk half of the check rests on, on a hypertable of the test's own: an <c>ALTER TABLE ... SET</c> on the root
    /// reaches the chunks that exist, a chunk created afterwards starts with the root's settings, and a chunk that then loses
    /// one makes the root not converged while the root itself still carries every setting.
    /// </summary>
    [Fact]
    public async Task ANewChunk_StartsWithTheRootsSettings_AndAChunkThatLosesOne_MakesTheRootNotConverged()
    {
        await RunAsync(async (scratch, connection, ct) =>
        {
            Assert.SkipUnless(scratch.TimescaleEnabled, NeedsTimescale);
            await StopBackgroundWorkersAsync(connection, ct);

            const string table = "collect.tuning_alter_probe";
            const string statement = "ALTER TABLE " + table + " SET (" + PgTableTuning.InsertTuningOptions + ")";
            string[] wanted = { "autovacuum_vacuum_insert_scale_factor=0.02", "autovacuum_vacuum_insert_threshold=10000" };

            await ExecAsync(connection, "CREATE TABLE " + table + " (collection_time timestamp NOT NULL, n integer NOT NULL)", ct);
            await ExecAsync(connection, "SELECT create_hypertable('" + table + "', by_range('collection_time', INTERVAL '1 day'))", ct);
            await ExecAsync(connection, "INSERT INTO " + table + " VALUES (TIMESTAMP '2026-03-01 12:00:00', 1)", ct);
            var before = await ReadChildOptionsAsync(connection, table, ct);
            var existing = Assert.Single(before).Key;
            Assert.False(await PgTableTuning.AlterIsConvergedAsync(connection, statement, null, ct));

            await ExecAsync(connection, statement, ct);
            await ExecAsync(connection, "INSERT INTO " + table + " VALUES (TIMESTAMP '2026-03-05 12:00:00', 2)", ct);

            var chunks = await ReadChildOptionsAsync(connection, table, ct);
            Assert.Equal(2, chunks.Count);
            var created = chunks.Keys.Single(c => c != existing);
            foreach (var option in wanted)
            {
                Assert.Contains(option, chunks[existing]);
                Assert.Contains(option, chunks[created]);
                Assert.Contains(option, await ReadOptionsAsync(connection, table, ct));
            }

            Assert.True(await PgTableTuning.AlterIsConvergedAsync(connection, statement, null, ct));

            await ExecAsync(connection, "ALTER TABLE " + created + " RESET (autovacuum_vacuum_insert_scale_factor)", ct);
            foreach (var option in wanted)
            {
                Assert.Contains(option, await ReadOptionsAsync(connection, table, ct));
                Assert.Contains(option, (await ReadChildOptionsAsync(connection, table, ct))[existing]);
            }

            Assert.False(await PgTableTuning.AlterIsConvergedAsync(connection, statement, null, ct));

            /* The statement the pass then issues on the root reaches the chunk again. */
            await ExecAsync(connection, statement, ct);
            Assert.Contains(wanted[0], (await ReadChildOptionsAsync(connection, table, ct))[created]);
            Assert.True(await PgTableTuning.AlterIsConvergedAsync(connection, statement, null, ct));
        });
    }

    /// <summary>
    /// Runs the hourly pass and then the start pass on <paramref name="connection"/> while another session holds ACCESS EXCLUSIVE
    /// on <paramref name="lockedTable"/>. Each pass has to finish (the token is cut at 60 s), log no busy table and no Warning,
    /// and count as many statements applied as the same pass does with no lock held.
    /// </summary>
    private static async Task AssertNoAlterIsIssuedWhileLockedAsync(string connectionString, NpgsqlConnection connection, string lockedTable, CancellationToken ct)
    {
        foreach (var hourly in new[] { true, false })
        {
            var unlocked = await PgTableTuning.ApplyAsync(connection, NullLogger.Instance, hourly, ct);

            await using var holder = new NpgsqlConnection(connectionString);
            await holder.OpenAsync(ct);
            await ExecAsync(holder, "BEGIN", ct);
            try
            {
                await ExecAsync(holder, "LOCK TABLE " + lockedTable + " IN ACCESS EXCLUSIVE MODE", ct);
                var log = new CapturingTestLogger();
                var locked = -1;

                using var cut = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cut.CancelAfter(TimeSpan.FromSeconds(60));
                try
                {
                    locked = await PgTableTuning.ApplyAsync(connection, log, hourly, cut.Token);
                }
                catch (OperationCanceledException) when (cut.IsCancellationRequested && !ct.IsCancellationRequested)
                {
                }

                var pass = (hourly ? "the hourly pass" : "the start pass") + " with " + lockedTable + " locked: " + log.Joined;
                Assert.False(cut.IsCancellationRequested, "still waiting after 60 s, " + pass);
                Assert.DoesNotContain(log.Lines, l => l.Contains("not applied this pass", StringComparison.Ordinal));
                Assert.DoesNotContain(log.Lines, l => l.Contains("table was busy", StringComparison.Ordinal));
                Assert.Equal(0, log.CountAtLevel(LogLevel.Warning));
                Assert.True(unlocked == locked, $"{unlocked} applied with no lock and {locked} in {pass}");
            }
            finally
            {
                await ExecAsync(holder, "ROLLBACK", CancellationToken.None);
            }
        }
    }

    /// <summary>The service's own order: the hypertable conversion, then the tuning pass. Background jobs are stopped so none takes a lock mid-test.</summary>
    private static async Task ConvertToHypertablesAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await StopBackgroundWorkersAsync(connection, ct);
    }

    private static Task StopBackgroundWorkersAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);

    /// <summary>One row, so the hypertable has a chunk.</summary>
    private static Task SeedQueryStatsRowAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ExecAsync(connection, @"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES (8949001, now() AT TIME ZONE 'UTC', -8949, 'Seed', 'Db', 'QH8949', '0x01', 1, 1, 1, 300)", ct);

    /// <summary>The table's own reloptions, empty when it has none.</summary>
    private static async Task<string[]> ReadOptionsAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT reloptions FROM pg_class WHERE oid = $1::regclass", connection);
        command.Parameters.AddWithValue(table);
        return await command.ExecuteScalarAsync(ct) as string[] ?? Array.Empty<string>();
    }

    /// <summary>The reloptions of every relation that inherits from the table (a hypertable's chunks), by quoted name.</summary>
    private static async Task<Dictionary<string, string[]>> ReadChildOptionsAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        var result = new Dictionary<string, string[]>(StringComparer.Ordinal);
        await using var command = new NpgsqlCommand(
            "SELECT format('%I.%I', n.nspname, c.relname), c.reloptions FROM pg_inherits AS i JOIN pg_class AS c ON c.oid = i.inhrelid " +
            "JOIN pg_namespace AS n ON n.oid = c.relnamespace WHERE i.inhparent = $1::regclass ORDER BY c.oid",
            connection);
        command.Parameters.AddWithValue(table);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result[reader.GetString(0)] = reader.IsDBNull(1) ? Array.Empty<string>() : reader.GetFieldValue<string[]>(1);
        }

        return result;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task RunAsync(Func<ScratchPostgres, NpgsqlConnection, CancellationToken, Task> body)
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), SkipReason);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await body(scratch, connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
