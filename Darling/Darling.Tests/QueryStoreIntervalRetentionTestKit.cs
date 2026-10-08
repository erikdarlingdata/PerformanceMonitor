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
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace Darling.Tests;

/// <summary>
/// Shared helpers for the #5571 retention and read-shape live tests on the day-partitioned Query Store interval tables:
/// a migrated scratch store, a promoted table, one-row inserts, and a logger that records what it was told. The test
/// classes using it each own their scratch database, so none of them can race another's DDL.
/// </summary>
internal static class QueryStoreIntervalRetentionTestKit
{
    internal static readonly QueryStoreIntervalPartitions.IntervalTable Wide = QueryStoreIntervalPartitions.Wide;
    internal static readonly QueryStoreIntervalPartitions.IntervalTable Latest = QueryStoreIntervalPartitions.Latest;

    internal const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the #5571 live tests.";

    internal static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>The fixed "now" the retention tests use: not midnight, so a day that straddles the cutoff exists.</summary>
    internal static readonly DateTime Now = new(2026, 10, 8, 14, 55, 30, DateTimeKind.Utc);

    /// <summary>A logger that records every line, so a test can assert what was warned and what was not.</summary>
    internal sealed class ListLogger : ILogger
    {
        internal List<(LogLevel Level, string Message)> Lines { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (Lines)
            {
                Lines.Add((logLevel, formatter(state, exception)));
            }
        }

        internal bool HasAtLeast(LogLevel level)
        {
            lock (Lines)
            {
                return Lines.Any(l => l.Level >= level);
            }
        }

        internal string Dump()
        {
            lock (Lines)
            {
                return string.Join(" | ", Lines.Select(l => $"{l.Level}: {l.Message}"));
            }
        }
    }

    /// <summary>A scratch database run through every migration, so both interval tables are partitioned with an attached legacy table.</summary>
    internal static async Task<NpgsqlConnection> OpenStoreAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    internal static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    internal static async Task<long> CountAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    /// <summary>Whether a schema-qualified relation exists.</summary>
    internal static async Task<bool> ExistsAsync(NpgsqlConnection connection, string qualifiedName, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT to_regclass($1) IS NOT NULL", connection);
        command.Parameters.AddWithValue(qualifiedName);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>One row with the given <c>first_execution_time</c>; the query id keys it.</summary>
    internal static async Task InsertAsync(
        NpgsqlConnection connection, QueryStoreIntervalPartitions.IntervalTable table, DateTime first, long query, CancellationToken ct)
    {
        var sql = table == Wide
            ? "INSERT INTO collect.query_store_interval_wide (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, query_text, execution_count, avg_duration_us, runtime_stats_interval_id, interval_start_time_utc) "
              + "VALUES ($1, 1, 'db', $2, $2, 'Regular', $1, $1, 'select 1', 1, 1000, 1, $1)"
            : "INSERT INTO collect.query_store_interval_latest (collection_time, server_id, database_name, query_id, plan_id, first_execution_time, last_execution_time, query_text, execution_count, avg_duration_us, runtime_stats_interval_id) "
              + "VALUES ($1, 1, 'db', $2, $2, $1, $1, 'select 1', 1, 1000, 1)";
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(first, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = query });
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The leaf a row lives in, schema-qualified, found by its query id.</summary>
    internal static async Task<string> PartitionOfAsync(
        NpgsqlConnection connection, QueryStoreIntervalPartitions.IntervalTable table, long query, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT 'collect.' || (SELECT c.relname FROM pg_class AS c WHERE c.oid = t.tableoid) FROM {table.Parent} AS t WHERE query_id = {query}",
            connection);
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>The leaves of a table by name, from the catalog.</summary>
    internal static async Task<IReadOnlyList<string>> PartitionNamesAsync(
        NpgsqlConnection connection, QueryStoreIntervalPartitions.IntervalTable table, CancellationToken ct) =>
        (await QueryStoreIntervalPartitions.ReadPartitionsAsync(connection, table, ct)).Select(p => p.Name).ToList();

    /// <summary>Arms, validates and promotes a table as of <paramref name="promotedAt"/> (legacy bound S = that day + 2), with no waits between retries.</summary>
    internal static async Task PromoteAsync(
        NpgsqlConnection connection, QueryStoreIntervalPartitions.IntervalTable table, DateTime promotedAt, ILogger logger, CancellationToken ct)
    {
        var step = await QueryStoreIntervalPartitions.RunPromotionAsync(
            connection, table, promotedAt, logger, TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(50), ct);
        if (step.Outcome is not (QueryStoreIntervalPartitions.StepOutcome.Done or QueryStoreIntervalPartitions.StepOutcome.NothingToDo))
        {
            throw new InvalidOperationException($"{table.Name} did not promote: {step.Outcome} {step.Detail}");
        }
    }
}
