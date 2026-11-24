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

/* #1776 own-store: this class mints its own scratch databases through ScratchPostgres and never touches another test's rows. */
/// <summary>
/// #9343: the Query Store rollup index blocks open with <c>SET LOCAL lock_timeout</c>. Sent on a plain connection that statement is outside any
/// transaction block and PostgreSQL raises "SET LOCAL can only be used in transaction blocks". The fresh-store continuous aggregate setup and every
/// convergence pass after it ran them that way. On the PostgreSQL 17 the compose file and the Helm chart ship, the fresh-store test below fails on the
/// plain command with three warnings.
/// </summary>
public sealed class SetLocalOutsideTransactionLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #9343 tests.";
    private const string SetLocalWarning = "can only be used in transaction blocks";

    private static async Task Exec(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>A fresh store runs the continuous aggregate setup (which creates each view and then its index blocks), and a second pass runs the
    /// convergence path over views that already exist. Neither raises the SET LOCAL warning.</summary>
    [Fact]
    public async Task AFreshStoresAggregateSetupAndConvergencePass_RaiseNoSetLocalOutsideTransactionWarning()
    {
        var ct = TestContext.Current.CancellationToken;
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            var notices = new List<string>();
            connection.Notice += (_, e) => notices.Add(e.Notice.MessageText);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
            Assert.SkipWhen(!timescaleEnabled, "The live #9343 test needs TimescaleDB.");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
            await Exec(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);

            var created = await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            Assert.True(created > 0, "the fresh-store setup created no aggregate, so it ran none of the index blocks");
            var afterFresh = notices.Where(n => n.Contains(SetLocalWarning, StringComparison.Ordinal)).ToList();
            Assert.True(afterFresh.Count == 0, "fresh-store setup: " + string.Join(" | ", afterFresh));

            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            var afterSecond = notices.Where(n => n.Contains(SetLocalWarning, StringComparison.Ordinal)).ToList();
            Assert.True(afterSecond.Count == 0, "convergence pass: " + string.Join(" | ", afterSecond));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>Every index block that opens with <c>SET LOCAL lock_timeout</c> runs through the helper the setup uses without the warning, and the
    /// block's own lock timeout is in force for the statements after it and gone after the commit. As the first statement of a plain command each of
    /// these blocks raises the warning.</summary>
    [Fact]
    public async Task ExecuteInTransaction_RunsEveryLockTimeoutIndexBlockWithoutTheWarning_AndTheTimeoutEndsWithTheTransaction()
    {
        var ct = TestContext.Current.CancellationToken;
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            var notices = new List<string>();
            connection.Notice += (_, e) => notices.Add(e.Notice.MessageText);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            var expected = (PgMigrations.MigrationCommandTimeoutSeconds - 20) + "s";
            var probe = "\nDO $$ BEGIN IF current_setting('lock_timeout') <> '" + expected + "' THEN "
                + "RAISE EXCEPTION 'lock_timeout is %, expected " + expected + "', current_setting('lock_timeout'); END IF; END $$;";
            var blocks = new[]
            {
                TimescaleSupport.CreateQueryStoreCorrectedHourlyHashIndexSql,
                TimescaleSupport.CreateQueryStoreCorrectedHourlyServerCoveringIndexSql,
                TimescaleSupport.CreateQueryStoreCorrectedHourlyHashCoveringIndexSql,
                TimescaleSupport.CreateQueryStoreCorrectedDailyHashCoveringIndexSql,
                TimescaleSupport.CreateQueryStatsHourlyHashCoveringIndexesSql,
            };
            foreach (var block in blocks)
            {
                await TimescaleSupport.ExecuteInTransactionAsync(connection, block + probe, ct);
                await using var read = new NpgsqlCommand("SELECT current_setting('lock_timeout')", connection);
                Assert.Equal("0", (string)(await read.ExecuteScalarAsync(ct))!);
            }

            var warned = notices.Where(n => n.Contains(SetLocalWarning, StringComparison.Ordinal)).ToList();
            Assert.True(warned.Count == 0, string.Join(" | ", warned));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
