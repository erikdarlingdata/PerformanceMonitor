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
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4605: <see cref="DarlingWebEndpoints.ResolveHourlyEdgesVerdictAsync"/> over the hour ledger, against a real store and in
/// the run's own transaction shape (REPEATABLE READ, then READ ONLY). Two states the count guard meets that are not a count:
/// <list type="bullet">
/// <item>A ledger that does not cover the window (its <c>counted_since</c> is after the window start): the guard answers -1,
/// the runner returns no verdict and notes <see cref="ReadFallback.LedgerUncovered"/> with no log line, and the transaction is
/// usable for the raw panel statement with the statement timeout put back.</item>
/// <item>A store before V164 (no ledger table): the guard cannot plan and faults inside its savepoint; the runner undoes it,
/// returns no verdict and notes <see cref="ReadFallback.GateFailed"/> with no Warning, and the transaction is usable.</item>
/// </list>
/// Each fact also says what is NOT recorded, because a fallback that logged a Warning per panel run would flood a store whose
/// ledger simply has not caught up yet.
///
/// <para><b>#1776 own-store</b>: mints a scratch database because it needs TimescaleDB's continuous aggregate and drops a
/// table, so it is deliberately NOT in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class ComposeHourlyEdgesLedgerGuardLiveTests
{
    private static readonly DateTime H1 = new(2026, 1, 5, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime H2 = H1.AddHours(3);

    private static readonly ComposeHourlyEdgesCandidate Candidate =
        new("query_stats", TimescaleSupport.QueryStatsIntervalHourlyView, H1, H2);

    [Fact]
    public async Task ALedgerThatDoesNotCoverTheWindow_GivesNoVerdict_NotesLedgerUncovered_LogsNothing_AndLeavesTheTransactionUsable()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await CreateScratchAsync(ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PrepareStoreAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            var timeoutOutside = await TextAsync(connection, "SHOW statement_timeout", ct);

            /* Control: counted_since far below the window and nothing in either count, so the comparison finds no pair that
               disagrees and the same candidate, transaction shape and scope give a verdict and note nothing. */
            await QueryStatsLedgerSeed.SetCountedSinceAsync(connection, QueryStatsLedgerSeed.Low, ct);
            await using (var covered = await BeginSnapshotAsync(connection, ct))
            {
                var logger = new CapturingTestLogger();
                using var scope = ReadScope.Open(logger);
                var verdict = await DarlingWebEndpoints.ResolveHourlyEdgesVerdictAsync(connection, Candidate, null, 30, ct);

                Assert.NotNull(verdict);
                Assert.Equal("query_stats", verdict!.SourceTable);
                Assert.Null(scope.Fallback);
                await covered.RollbackAsync(ct);
            }

            /* The ledger starts counting one hour after the window starts: the window is uncovered. */
            await QueryStatsLedgerSeed.SetCountedSinceAsync(connection, H1.AddHours(1), ct);
            await using (var uncovered = await BeginSnapshotAsync(connection, ct))
            {
                var logger = new CapturingTestLogger();
                using var scope = ReadScope.Open(logger);
                var verdict = await DarlingWebEndpoints.ResolveHourlyEdgesVerdictAsync(connection, Candidate, null, 30, ct);

                Assert.Null(verdict);
                Assert.Equal(ReadFallback.LedgerUncovered, scope.Fallback);
                Assert.Equal(ReadOutcome.FallbackRaw, ReadScope.Resolve(ReadOutcome.Ok, scope.Fallback));
                Assert.True(logger.Lines.Count == 0, "an uncovered ledger is an expected state and logs nothing: " + logger.Joined);

                /* The guard's 15 s timeout was put back before the early return, and the transaction still runs statements. */
                Assert.Equal(timeoutOutside, await TextAsync(connection, "SHOW statement_timeout", ct));
                Assert.Equal(1L, await ScalarAsync(connection, "SELECT 1", ct));
                Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_stats", ct));
                await uncovered.RollbackAsync(ct);
            }

            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, bodySucceeded);
        }
    }

    [Fact]
    public async Task AStoreBeforeTheLedger_GivesNoVerdict_NotesGateFailed_WithNoWarning_AndLeavesTheTransactionUsable()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await CreateScratchAsync(ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PrepareStoreAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            /* A store that has not reached V164 has neither ledger table. */
            await ExecuteAsync(connection, $"DROP TABLE {QueryStatsHourLedger.StateTable}", ct);
            await ExecuteAsync(connection, $"DROP TABLE {QueryStatsHourLedger.LedgerTable}", ct);

            await using var snapshot = await BeginSnapshotAsync(connection, ct);
            var logger = new CapturingTestLogger();
            using var scope = ReadScope.Open(logger);
            var verdict = await DarlingWebEndpoints.ResolveHourlyEdgesVerdictAsync(connection, Candidate, null, 30, ct);

            Assert.Null(verdict);
            Assert.Equal(ReadFallback.GateFailed, scope.Fallback);
            Assert.Equal(ReadOutcome.GateFailed, ReadScope.Resolve(ReadOutcome.Ok, scope.Fallback));
            Assert.True(logger.CountAtLevel(LogLevel.Warning) == 0, "a store before V164 falls back quietly: " + logger.Joined);

            /* The failed statement aborted the transaction; the rollback to the savepoint made it usable again, so the
               panel's raw statement can run on it. */
            Assert.Equal(1L, await ScalarAsync(connection, "SELECT 1", ct));
            Assert.Equal(0L, await ScalarAsync(connection, "SELECT count(*) FROM collect.query_stats", ct));
            await snapshot.RollbackAsync(ct);

            bodySucceeded = true;
        }
        finally
        {
            await CleanupAsync(scratch, bodySucceeded);
        }
    }

    private static async Task<ScratchPostgres> CreateScratchAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live hour-ledger runner tests.");
        return await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
    }

    /// <summary>Migrates the scratch store, makes the hourly rollup the guard reads and stops the TimescaleDB scheduler.</summary>
    private static async Task PrepareStoreAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The hour-ledger runner tests need TimescaleDB: the guard reads a continuous aggregate.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        await ExecuteAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
    }

    /// <summary>The run's transaction shape: REPEATABLE READ, then READ ONLY.</summary>
    private static async Task<NpgsqlTransaction> BeginSnapshotAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, ct);
        await ExecuteAsync(connection, "SET TRANSACTION READ ONLY", ct);
        return transaction;
    }

    private static Task CleanupAsync(ScratchPostgres scratch, bool bodySucceeded) =>
        LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
        {
            await using var probe = new NpgsqlCommand(
                "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE datname = pg_catalog.current_database() " +
                "AND backend_type LIKE 'TimescaleDB Background Worker Scheduler%'", cleanup);
            var schedulers = Convert.ToInt64(await probe.ExecuteScalarAsync(cleanupCt));
            Assert.Equal(0L, schedulers);
        });

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }

    private static async Task<string?> TextAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct));
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
