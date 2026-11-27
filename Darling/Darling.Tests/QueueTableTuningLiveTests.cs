/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// #8790: the queue-like plain store tables get their own fillfactor and autovacuum settings from the tuning pass. The first test
/// runs the pass on a fresh store and reads the settings back from <c>pg_class.reloptions</c>, then runs it again and checks nothing
/// moved. The second test shows what the fillfactor is for: the same updates on the same rows make more HOT updates with the tuned
/// fillfactor than with the stock one.
/// </summary>
public sealed class QueueTableTuningLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipReason = "Set DARLING_TEST_PG to a Postgres connection string to run the queue-table tuning live tests.";

    private static readonly (string Table, string[] Options)[] Expected =
    {
        ("collect.oversized_plan_backlog", new[] { "fillfactor=80", "autovacuum_vacuum_scale_factor=0.02", "autovacuum_vacuum_threshold=1000" }),
        ("collect.query_stats_hour_ledger", new[] { "fillfactor=80", "autovacuum_vacuum_scale_factor=0.02", "autovacuum_vacuum_threshold=1000" }),
        ("collect.collector_state", new[] { "fillfactor=80" }),
        ("collect.query_text_dim", new[] { "autovacuum_vacuum_scale_factor=0.02", "autovacuum_vacuum_threshold=1000" }),
        ("collect.pg_statement_text", new[] { "autovacuum_vacuum_scale_factor=0.02", "autovacuum_vacuum_threshold=1000" }),
    };

    [Fact]
    public async Task TheTuningPass_SetsEachQueueTablesOptions_AndASecondRunChangesNothing()
    {
        await RunAsync(async (connection, ct) =>
        {
            await PgTableTuning.ApplyAsync(connection, logger: null, ct);
            var first = await ReadOptionsAsync(connection, ct);

            foreach (var (table, options) in Expected)
            {
                Assert.True(first.TryGetValue(table, out var actual), table + " holds no reloptions after the pass");
                foreach (var option in options)
                {
                    Assert.Contains(option, actual!);
                }

                Assert.True(await IsPlainTableAsync(connection, table, ct), table + " is a hypertable or partitioned");
            }

            await PgTableTuning.ApplyAsync(connection, logger: null, ct);
            var second = await ReadOptionsAsync(connection, ct);
            Assert.Equal(first.Keys.OrderBy(k => k, StringComparer.Ordinal), second.Keys.OrderBy(k => k, StringComparer.Ordinal));
            foreach (var table in first.Keys)
            {
                Assert.Equal(first[table].OrderBy(o => o, StringComparer.Ordinal), second[table].OrderBy(o => o, StringComparer.Ordinal));
            }
        });
    }

    [Fact]
    public async Task TheTunedFillfactor_LetsMoreOfTheSameUpdatesGoHot_ThanTheStockFillfactor()
    {
        await RunAsync(async (connection, ct) =>
        {
            await PgTableTuning.ApplyAsync(connection, logger: null, ct);
            var tuned = await MeasureHotUpdatesAsync(connection, ct);

            await ExecAsync(connection, "ALTER TABLE collect.collector_state RESET (fillfactor)", ct);
            var stock = await MeasureHotUpdatesAsync(connection, ct);

            Assert.True(tuned > stock, $"tuned fillfactor made {tuned} HOT updates, the stock one {stock}");
        });
    }

    /// <summary>
    /// Fills <c>collect.collector_state</c> with 2,000 rows under whatever fillfactor it has now, then updates every row 5 times in
    /// one transaction (the columns it changes are not indexed) and returns how many of those updates were HOT. The count is read inside the
    /// transaction as a difference, so it needs no wait for the statistics to flush.
    /// </summary>
    private static async Task<long> MeasureHotUpdatesAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, "TRUNCATE collect.collector_state", ct);
        await ExecAsync(
            connection,
            "INSERT INTO collect.collector_state (server_id, collector_name, state_key, state_value, updated_at) " +
            "SELECT 1, 'tuning_probe', 'key_' || g, 'value_' || g, now() AT TIME ZONE 'UTC' FROM generate_series(1, 2000) AS g",
            ct);

        await using var transaction = await connection.BeginTransactionAsync(ct);
        var before = await ReadHotUpdatesAsync(connection, transaction, ct);
        for (var round = 0; round < 5; round++)
        {
            await using var update = new NpgsqlCommand(
                "UPDATE collect.collector_state SET state_value = state_value || 'x', updated_at = now() AT TIME ZONE 'UTC' WHERE collector_name = 'tuning_probe'",
                connection, transaction);
            await update.ExecuteNonQueryAsync(ct);
        }

        var hot = await ReadHotUpdatesAsync(connection, transaction, ct) - before;
        await transaction.RollbackAsync(ct);
        return hot;
    }

    /// <summary>The session's running HOT update count for the table, including counts not yet flushed. It is a running total, so a caller takes a difference.</summary>
    private static async Task<long> ReadHotUpdatesAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand("SELECT pg_stat_get_xact_tuples_hot_updated('collect.collector_state'::regclass)", connection, transaction);
        return Convert.ToInt64(await read.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task<Dictionary<string, string[]>> ReadOptionsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var result = new Dictionary<string, string[]>(StringComparer.Ordinal);
        await using var command = new NpgsqlCommand(
            "SELECT n.nspname || '.' || c.relname, c.reloptions FROM pg_class AS c JOIN pg_namespace AS n ON n.oid = c.relnamespace " +
            "WHERE n.nspname = 'collect' AND c.relkind = 'r' AND c.reloptions IS NOT NULL",
            connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result[reader.GetString(0)] = reader.GetFieldValue<string[]>(1);
        }

        return result;
    }

    /// <summary>True when the table is an ordinary relation and, if TimescaleDB is installed, no hypertable.</summary>
    private static async Task<bool> IsPlainTableAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        await using var kind = new NpgsqlCommand("SELECT relkind = 'r' FROM pg_class WHERE oid = $1::regclass", connection);
        kind.Parameters.AddWithValue(table);
        if (await kind.ExecuteScalarAsync(ct) is not true)
        {
            return false;
        }

        await using var catalog = new NpgsqlCommand("SELECT to_regclass('_timescaledb_catalog.hypertable') IS NOT NULL", connection);
        if (await catalog.ExecuteScalarAsync(ct) is not true)
        {
            return true;
        }

        await using var hyper = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM _timescaledb_catalog.hypertable WHERE schema_name || '.' || table_name = $1)", connection);
        hyper.Parameters.AddWithValue(table);
        return await hyper.ExecuteScalarAsync(ct) is false;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task RunAsync(Func<NpgsqlConnection, CancellationToken, Task> body)
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
            await body(connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
