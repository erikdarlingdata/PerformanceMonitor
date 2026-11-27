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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8570: <see cref="PgFactCollector.ParameterSensitivitySql"/> runs in a transaction of its own with the two parallel-plan
/// costs at zero (<see cref="PgFactCollector.ParameterSensitivityParallelCostsSql"/>). The first test holds the statement's
/// rows under a parallel plan to its rows under a serial one; the second holds the collector to running the statement that
/// way and to the PARAMETER_SENSITIVITY fact the serial rows give.
///
/// <para>#1776 own-store: mints its own scratch database (<see cref="ScratchPostgres"/>).</para>
/// </summary>
public sealed class ParameterSensitivityParallelPlanLiveTests
{
    private const int ServerId = -857_002;
    private const string ServerName = "ParamSensParallelSrv";

    /// <summary>
    /// Sixty queries with two plans each over forty snapshots, so some keys pass every gate and some fail one: the ratio
    /// (max over min worker time) runs from 2 to 60 across the keys, every fifth key has too few executions, every seventh
    /// was compiled inside the window, and every third snapshot ran nothing.
    /// </summary>
    private const string SeedSql = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     sql_handle, plan_handle, query_text, creation_time, execution_count, delta_execution_count,
     min_worker_time, max_worker_time, min_grant_kb, max_grant_kb, min_spills, max_spills)
SELECT
    900000000 + k * 1000 + p * 100 + s,
    $3::timestamp - make_interval(mins => 5 * s),
    $1, $2,
    'Db' || (k % 3),
    '0xQ' || lpad(k::text, 3, '0'),
    '0xP' || lpad(k::text, 3, '0') || '_' || p,
    '0xS' || k, '0xH' || k || '_' || p, 'SELECT 1',
    CASE WHEN k % 7 = 0 THEN $3::timestamp - interval '1 hour' ELSE $3::timestamp - interval '30 hours' END,
    CASE WHEN k % 5 = 0 THEN 10 ELSE 100 + s END,
    CASE WHEN s % 3 = 0 THEN 0 ELSE 4 END,
    20000,
    20000 * (2 + k) + p * 7 + s,
    1024, 1024 * (1 + k % 4), 0, k % 2
FROM generate_series(1, 60) AS k
CROSS JOIN generate_series(0, 1) AS p
CROSS JOIN generate_series(1, 40) AS s";

    private static async Task<(string Connection, ScratchPostgres Scratch, DateTime PeriodEnd)> SeedStoreAsync(string baseConnection, CancellationToken ct)
    {
        var periodEnd = LiveClock.AnchoredEndNotAfterNow();
        var scratch = await ScratchPostgres.CreateAsync(baseConnection, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using (var seed = new NpgsqlCommand(SeedSql, connection))
        {
            seed.Parameters.AddWithValue(ServerId);
            seed.Parameters.AddWithValue(ServerName);
            seed.Parameters.AddWithValue(periodEnd);
            await seed.ExecuteNonQueryAsync(ct);
        }

        await using (var analyze = new NpgsqlCommand("ANALYZE query_stats", connection))
            await analyze.ExecuteNonQueryAsync(ct);

        return (scratch.ConnectionString, scratch, periodEnd);
    }

    private static void AddWindow(NpgsqlCommand command, DateTime periodEnd)
    {
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(periodEnd.AddHours(-4));
        command.Parameters.AddWithValue(periodEnd);
    }

    /// <summary>Every row of the statement, each column rendered invariant, in the statement's own order.</summary>
    private static async Task<List<string>> ReadRowsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string settings, DateTime periodEnd, CancellationToken ct)
    {
        await using (var set = new NpgsqlCommand(settings, connection, transaction))
            await set.ExecuteNonQueryAsync(ct);

        await using var command = new NpgsqlCommand(PgFactCollector.ParameterSensitivitySql, connection, transaction);
        AddWindow(command, periodEnd);
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var columns = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
                columns[i] = reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture)!;
            rows.Add(string.Join("|", columns));
        }

        return rows;
    }

    private static async Task<string> ExplainAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, DateTime periodEnd, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN " + PgFactCollector.ParameterSensitivitySql, connection, transaction);
        AddWindow(command, periodEnd);
        var lines = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            lines.Add(reader.GetString(0));
        return string.Join("\n", lines);
    }

    /// <summary>
    /// The lab table is far under the planner's parallel scan floors, so the parallel arm lowers them as well: the rows
    /// are then read through a Gather, which the plan shows, and compared with a read that has no worker to use.
    /// </summary>
    [Fact]
    public async Task ParameterSensitivitySql_ParallelPlan_ReturnsTheSerialRows()
    {
        var baseConnection = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnection),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live parameter sensitivity parallel plan test.");
        var ct = TestContext.Current.CancellationToken;

        var (connectionString, scratch, periodEnd) = await SeedStoreAsync(baseConnection!, ct);
        await using var _ = scratch;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);

        List<string> serial;
        await using (var transaction = await connection.BeginTransactionAsync(ct))
            serial = await ReadRowsAsync(connection, transaction, "SET LOCAL max_parallel_workers_per_gather TO 0", periodEnd, ct);

        List<string> parallel;
        string plan;
        await using (var transaction = await connection.BeginTransactionAsync(ct))
        {
            parallel = await ReadRowsAsync(connection, transaction,
                PgFactCollector.ParameterSensitivityParallelCostsSql
                + "; SET LOCAL min_parallel_table_scan_size TO 0; SET LOCAL min_parallel_index_scan_size TO 0; SET LOCAL max_parallel_workers_per_gather TO 2",
                periodEnd, ct);
            plan = await ExplainAsync(connection, transaction, periodEnd, ct);
        }

        Assert.Contains("Gather", plan, StringComparison.Ordinal);
        Assert.True(serial.Count >= 10, $"the seed must give the statement rows to compare, it gave {serial.Count}");
        Assert.Equal(serial, parallel);
    }

    [Fact]
    public async Task ParameterSensitivityFact_ReadUnderZeroParallelCosts_IsTheFactOfTheSerialRows()
    {
        var baseConnection = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnection),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live parameter sensitivity parallel plan test.");
        var ct = TestContext.Current.CancellationToken;

        var (connectionString, scratch, periodEnd) = await SeedStoreAsync(baseConnection!, ct);
        await using var _ = scratch;

        List<string> serial;
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await using var transaction = await connection.BeginTransactionAsync(ct);
            serial = await ReadRowsAsync(connection, transaction, "SET LOCAL max_parallel_workers_per_gather TO 0", periodEnd, ct);
        }

        var log = new NpgsqlMessages();
        await using var countedStore = new NpgsqlDataSourceBuilder(connectionString).UseLoggerFactory(log).Build();
        var context = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = periodEnd.AddHours(-4),
            TimeRangeEnd = periodEnd,
            ServerUtcOffset = TimeSpan.Zero,
        };
        var facts = await new PgFactCollector(countedStore).CollectFactsAsync(context);

        /* The two costs are set once, in the transaction the statement then runs in. Npgsql logs a command of two statements
           as a batch of the two, so the message is matched on each statement. */
        var settings = log.Containing("SET LOCAL parallel_setup_cost TO 0").Where(m => m.Contains("SET LOCAL parallel_tuple_cost TO 0", StringComparison.Ordinal)).ToList();
        var statement = log.Containing(PgFactCollector.ParameterSensitivitySql.Trim());
        Assert.Single(settings);
        Assert.Single(statement);

        /* The serial rows, columns as the statement lists them: 0 min_worker_time, 1 max_worker_time, 2 worker_ratio,
           3 grant_ratio, 4 spill_divergence, 8 database_name, 9 query_hash, 10 query_plan_hash. */
        var rows = serial.Select(r => r.Split('|')).ToList();
        var fact = Assert.Single(facts, f => f.Key == "PARAMETER_SENSITIVITY");
        var worst = rows[0];
        Assert.Equal(Math.Min(rows.Count, FactThresholds.ParameterSensitivity.OffenderCap), fact.Metadata["offender_count"]);
        Assert.Equal(double.Parse(worst[2], CultureInfo.InvariantCulture), fact.Value);
        Assert.Equal(double.Parse(worst[0], CultureInfo.InvariantCulture), fact.Metadata["worst_min_worker_us"]);
        Assert.Equal(double.Parse(worst[1], CultureInfo.InvariantCulture), fact.Metadata["worst_max_worker_us"]);
        Assert.Equal(double.Parse(worst[3], CultureInfo.InvariantCulture), fact.Metadata["worst_grant_ratio"]);
        Assert.Equal(double.Parse(worst[4], CultureInfo.InvariantCulture), fact.Metadata["spill_divergence"]);
        Assert.Equal(
            rows.Take(FactRanked.MaxObjects).Select(r => FactRanked.QueryName(r[8], r[9], r[10])),
            fact.Ranked!.Select(o => o.ObjectName));
    }

    /// <summary>Every Npgsql log message of a data source, for a test that asks which statements ran (Npgsql logs a command
    /// of several statements as a batch, which <see cref="CommandCountingLoggerProvider"/> does not count).</summary>
    private sealed class NpgsqlMessages : Microsoft.Extensions.Logging.ILoggerFactory, Microsoft.Extensions.Logging.ILogger
    {
        private readonly System.Collections.Concurrent.ConcurrentQueue<string> _messages = new();

        /// <summary>The "Executing command" and "Executing batch" messages, one per execution, that carry the text.</summary>
        public List<string> Containing(string text) =>
            _messages.Where(m => m.StartsWith("Executing ", StringComparison.Ordinal) && m.Contains(text, StringComparison.Ordinal)).ToList();

        public void AddProvider(Microsoft.Extensions.Logging.ILoggerProvider provider) { }

        public Microsoft.Extensions.Logging.ILogger CreateLogger(string categoryName) => this;

        public void Dispose() { }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => logLevel >= Microsoft.Extensions.Logging.LogLevel.Debug;

        public void Log<TState>(Microsoft.Extensions.Logging.LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (IsEnabled(logLevel))
                _messages.Enqueue(formatter(state, exception));
        }
    }
}
