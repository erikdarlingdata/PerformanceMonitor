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
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A service start builds at most one of the large start-path indexes: once one build has been issued, the other
/// missing ones wait for the next start and the log names each. Also pins how a start-path build that runs out
/// of time is told apart from any other failure, and the text of the warning it gets. Own scratch database per live test.
/// </summary>
/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres. */
public sealed class TuningStartPathOneBuildTests
{
    private static readonly string[] Deferred =
    {
        PgTableTuning.LegacyRowIndexName,
        PgTableTuning.QueryStatsRestartRowIndexName,
        PgTableTuning.ProcedureStatsRestartRowIndexName,
    };

    [Fact]
    public async Task AStartBuildsOneMissingIndex_DefersTheRest_AndTheNextStartsBuildThem()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the one-build-per-start live test.");

        var ct = TestContext.Current.CancellationToken;
        PgTableTuning.ResetHourlyMissingWarnings();
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var body = new NpgsqlConnection(scratch.ConnectionString);
        await body.OpenAsync(ct);
        await PgMigrations.MigrateAsync(body, ct);
        await TuningStartPasses.ConvergeAsync(body, ct);
        foreach (var name in Deferred)
        {
            Assert.True(await IndexExistsAsync(body, name, ct), name + " should exist after the start passes");
        }

        /* A table with rows is what the budget guards: an empty table builds at once and uses none (#6460), so give both a row. */
        await SeedStatsRowsAsync(body, ct);

        /* The hourly pass never builds any of them, however many are missing. */
        await Exec(body, "DROP INDEX collect." + PgTableTuning.QueryStatsRestartRowIndexName, ct);
        await Exec(body, "DROP INDEX collect." + PgTableTuning.ProcedureStatsRestartRowIndexName, ct);
        await PgTableTuning.ApplyAsync(body, new CapturingTestLogger(), hourly: true, ct);
        Assert.False(await IndexExistsAsync(body, PgTableTuning.QueryStatsRestartRowIndexName, ct));
        Assert.False(await IndexExistsAsync(body, PgTableTuning.ProcedureStatsRestartRowIndexName, ct));

        /* Start 1: builds exactly the first missing one in list order and defers the other, by name. */
        var start1 = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(body, start1, ct);
        Assert.True(await IndexExistsAsync(body, PgTableTuning.QueryStatsRestartRowIndexName, ct), start1.Joined);
        Assert.False(await IndexExistsAsync(body, PgTableTuning.ProcedureStatsRestartRowIndexName, ct), start1.Joined);
        Assert.Single(start1.Lines, l => l.StartsWith("Information:", StringComparison.Ordinal)
            && l.Contains(PgTableTuning.ProcedureStatsRestartRowIndexName, StringComparison.Ordinal)
            && l.Contains("built at the next start", StringComparison.Ordinal));
        Assert.DoesNotContain(start1.Lines, l => l.Contains(PgTableTuning.QueryStatsRestartRowIndexName, StringComparison.Ordinal)
            && l.Contains("built at the next start", StringComparison.Ordinal));

        /* Start 2: builds the other. */
        var start2 = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(body, start2, ct);
        Assert.True(await IndexExistsAsync(body, PgTableTuning.ProcedureStatsRestartRowIndexName, ct), start2.Joined);
        Assert.DoesNotContain(start2.Lines, l => l.Contains("built at the next start", StringComparison.Ordinal));

        /* Start 3: nothing is missing, nothing is built or deferred, and no index OID changes. */
        var oids = await OidsAsync(body, ct);
        var start3 = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(body, start3, ct);
        Assert.Equal(oids, await OidsAsync(body, ct));
        Assert.DoesNotContain(start3.Lines, l => l.Contains("built at the next start", StringComparison.Ordinal));
        Assert.Equal(0, start3.CountAtLevel(LogLevel.Warning));
    }

    /// <summary>
    /// #6460: a first start on a brand-new, empty store builds all three large start-path indexes in ONE pass (an empty table
    /// builds instantly, so none of them uses the one-build budget), so the start logs no deferral and the first hourly pass
    /// finds nothing missing to warn about. Before, the first start built one and deferred the other two, and the hourly pass
    /// 30 s later logged two Warnings that read like a fault.
    /// </summary>
    [Fact]
    public async Task AFirstStartOnAnEmptyStore_BuildsEveryLargeIndex_AndLogsNothingThatReadsLikeAFault()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the first-start live test.");

        var ct = TestContext.Current.CancellationToken;
        PgTableTuning.ResetHourlyMissingWarnings();
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var body = new NpgsqlConnection(scratch.ConnectionString);
        await body.OpenAsync(ct);
        await PgMigrations.MigrateAsync(body, ct);
        foreach (var name in Deferred)
        {
            await Exec(body, "DROP INDEX IF EXISTS collect." + name, ct);
        }

        var start = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(body, start, ct);
        foreach (var name in Deferred)
        {
            Assert.True(await IndexExistsAsync(body, name, ct), name + " should be built by the one start pass: " + start.Joined);
        }

        var hourly = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(body, hourly, hourly: true, ct);
        Assert.Equal(0, start.CountAtLevel(LogLevel.Warning));
        Assert.Equal(0, hourly.CountAtLevel(LogLevel.Warning));
        Assert.DoesNotContain(start.Lines, l => l.Contains("built at the next start", StringComparison.Ordinal));
        Assert.DoesNotContain(hourly.Lines, l => l.Contains("is missing", StringComparison.Ordinal));
        Assert.DoesNotContain(start.Lines, l => l.Contains("is missing", StringComparison.Ordinal));
    }

    /// <summary>
    /// #6460 review: the start-path emptiness check is a bare SELECT and queues behind an ACCESS EXCLUSIVE holder
    /// (drop_chunks, decompress_chunk, VACUUM FULL). It must give up at the create lock bound, not at the 300 s setup
    /// timeout, then fail closed onto the one-build budget (with a Warning naming the table), and the connection must stay
    /// usable: the sweep goes on to the CREATE on the same connection, which finds the table busy too, and a later start builds the index.
    /// #6539: the ALTER TABLE ... SET statements on the same table are bounded the same way, so the WHOLE start returns in well
    /// under a minute (it sat on the first ALTER for the full 300 s setup timeout before), logs the skipped table at Information,
    /// and a later pass applies the setting.
    /// </summary>
    [Fact]
    public async Task AnEmptinessCheck_ThatFindsTheTableLockedExclusively_GivesUpQuickly_AndALaterSweepBuildsTheIndex()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the locked-table emptiness-check live test.");

        var ct = TestContext.Current.CancellationToken;
        PgTableTuning.ResetHourlyMissingWarnings();
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var body = new NpgsqlConnection(scratch.ConnectionString);
        await body.OpenAsync(ct);
        await PgMigrations.MigrateAsync(body, ct);
        await TuningStartPasses.ConvergeAsync(body, ct);
        await SeedStatsRowsAsync(body, ct);
        await Exec(body, "DROP INDEX collect." + PgTableTuning.QueryStatsRestartRowIndexName, ct);
        await Exec(body, "ALTER TABLE collect.query_stats RESET (autovacuum_vacuum_insert_scale_factor, autovacuum_vacuum_insert_threshold)", ct);
        Assert.False(await InsertTuningAppliedAsync(body, ct));

        await using var holder = new NpgsqlConnection(scratch.ConnectionString);
        await holder.OpenAsync(ct);
        await Exec(holder, "BEGIN", ct);
        try
        {
            await Exec(holder, "LOCK TABLE collect.query_stats IN ACCESS EXCLUSIVE MODE", ct);
            var busy = new CapturingTestLogger();

            /* The whole start is bounded: the emptiness check (5 s), the CREATE (5 s) and each ALTER TABLE ... SET on the
               locked table (5 s apiece) all give up, about 20 s in all. The token is cut at 60 s: a start that is still
               waiting then (the ALTER waited the full 300 s before #6539) fails the cut assertion below. */
            using var cut = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cut.CancelAfter(TimeSpan.FromSeconds(60));
            try
            {
                await PgTableTuning.ApplyAsync(body, busy, cut.Token);
            }
            catch (OperationCanceledException) when (cut.IsCancellationRequested && !ct.IsCancellationRequested)
            {
            }

            Assert.False(cut.IsCancellationRequested, "the whole start should give up on the locked table in well under 60 s: " + busy.Joined);
            Assert.Contains(busy.Lines, l => l.StartsWith("Information:", StringComparison.Ordinal)
                && l.Contains("collect.query_stats", StringComparison.Ordinal)
                && l.Contains("not applied this pass", StringComparison.Ordinal));
            Assert.False(await InsertTuningAppliedAsync(body, ct), busy.Joined);

            Assert.Contains(busy.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal)
                && l.Contains("collect.query_stats", StringComparison.Ordinal)
                && l.Contains("Could not tell whether", StringComparison.Ordinal));
            Assert.Contains(busy.Lines, l => l.StartsWith("Information:", StringComparison.Ordinal)
                && l.Contains(PgTableTuning.QueryStatsRestartRowIndexName, StringComparison.Ordinal)
                && l.Contains("table was busy", StringComparison.Ordinal));
        }
        finally
        {
            await Exec(holder, "ROLLBACK", CancellationToken.None);
        }

        /* A later start (a fresh connection: the cut-off one is not reused) builds the index once the lock is gone. */
        await using var restart = new NpgsqlConnection(scratch.ConnectionString);
        await restart.OpenAsync(ct);
        var later = new CapturingTestLogger();
        await PgTableTuning.ApplyAsync(restart, later, ct);
        Assert.True(await IndexExistsAsync(restart, PgTableTuning.QueryStatsRestartRowIndexName, ct), later.Joined);
        Assert.True(await InsertTuningAppliedAsync(restart, ct), later.Joined);
    }

    private static async Task<bool> InsertTuningAppliedAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT COALESCE(array_to_string(c.reloptions, ','), '') LIKE '%insert_scale_factor%' FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            + "WHERE n.nspname = 'collect' AND c.relname = 'query_stats'", c);
        return (bool)(await cmd.ExecuteScalarAsync(ct))!;
    }

    /// <summary>The first-missing index in list order is the earliest start-path index, so the legacy-row index goes before the restart-row ones.</summary>
    [Fact]
    public void TheLegacyRowIndex_IsListedBeforeTheRestartRowIndexes_AndAllThreeShareTheBudget()
    {
        var order = PgTableTuning.Statements.Select((s, i) => (s, i)).ToList();
        int At(string name) => order.First(o => o.s.Contains(name + " ON", StringComparison.Ordinal)).i;
        Assert.True(At(PgTableTuning.LegacyRowIndexName) < At(PgTableTuning.QueryStatsRestartRowIndexName));
        Assert.True(At(PgTableTuning.QueryStatsRestartRowIndexName) < At(PgTableTuning.ProcedureStatsRestartRowIndexName));
        foreach (var name in Deferred)
        {
            Assert.True(PgTableTuning.IsOneBuildPerStart(name), name);
        }
    }

    [Fact]
    public void ABuildThatRanOutOfTime_IsToldApartFromAnyOtherFailure()
    {
        Assert.True(PgTableTuning.IsBuildTimeout(new NpgsqlException("Exception while reading from stream", new TimeoutException("Timeout during reading attempt"))));
        Assert.True(PgTableTuning.IsBuildTimeout(new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", PostgresErrorCodes.QueryCanceled)));
        Assert.False(PgTableTuning.IsBuildTimeout(new PostgresException("relation does not exist", "ERROR", "ERROR", PostgresErrorCodes.UndefinedTable)));
        Assert.False(PgTableTuning.IsBuildTimeout(new NpgsqlException("connection reset", new System.IO.IOException("reset"))));
        Assert.False(PgTableTuning.IsBuildTimeout(new InvalidOperationException("boom")));
    }

    [Fact]
    public void TheTimeoutWarning_NamesTheIndexTheTableTheElapsedTimeAndTheRetry()
    {
        var message = PgTableTuning.BuildTimeoutMessage("idx_demo", "collect.query_stats", 301.4);
        Assert.Equal(
            "Index collect.idx_demo on collect.query_stats was not built: the build ran out of time after 301 s (limit 300 s) and was rolled back; it is retried at the next start, and each try pauses collection at start for up to 300 s",
            message);
    }

    [Fact]
    public void TheStartPathCatch_UsesTheTimeoutWarning_OnlyForABuildThatTimedOut()
    {
        var src = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "PgTableTuning.cs").ReplaceLineEndings("\n");
        Assert.Contains("statement.StartsWith(CreateIndexPrefix, StringComparison.Ordinal) && IsBuildTimeout(ex)", src, StringComparison.Ordinal);
        Assert.Contains("BuildTimeoutMessage(", src, StringComparison.Ordinal);
        Assert.Contains("Composer performance-tuning statement failed", src, StringComparison.Ordinal);
    }

    private static async Task SeedStatsRowsAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await Exec(c, @"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES (6460000, now() AT TIME ZONE 'UTC', -6460, 'Seed', 'Db', 'dbo', 'usp_Seed', '0x01', 1, 1, 1, 300)", ct);
        await Exec(c, @"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES (6460001, now() AT TIME ZONE 'UTC', -6460, 'Seed', 'Db', 'QH6460', '0x01', 1, 1, 1, 300)", ct);
    }

    private static async Task<string> OidsAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT string_agg(c.relname || ':' || c.oid::text, ',' ORDER BY c.relname) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            + "WHERE n.nspname = 'collect' AND c.relname = ANY($1)", c);
        cmd.Parameters.AddWithValue(Deferred);
        return (string)(await cmd.ExecuteScalarAsync(ct))!;
    }

    private static async Task Exec(NpgsqlConnection c, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<bool> IndexExistsAsync(NpgsqlConnection c, string name, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("SELECT 1 FROM pg_indexes WHERE schemaname = 'collect' AND indexname = $1", c);
        cmd.Parameters.AddWithValue(name);
        return await cmd.ExecuteScalarAsync(ct) is not null;
    }
}
