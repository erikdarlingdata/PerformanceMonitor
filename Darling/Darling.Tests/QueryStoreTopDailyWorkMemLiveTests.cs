/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8570: the daily-summary read of <c>get_query_store_top</c> (<see cref="DarlingDataReader.QueryStoreTopDailyTableSql"/>) runs with
/// enough <c>work_mem</c> that its final aggregate stays in memory. The plant is a busy server's shape: a hundred thousand distinct
/// plans across five built days. The first fact proves the plant would spill at the server's own <c>work_mem</c> (so the second cannot
/// pass for want of data). The second runs the read through the product's own entry point and counts the temp bytes the database
/// wrote. Temp bytes are a per-database counter that a backend adds at exit, so each fact reads it through a fresh connection after
/// the working one has closed.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every fact reaches DARLING_TEST_PG only to CREATE and DROP its own
   database through ScratchPostgres and works entirely inside it, so it cannot race live collection. */
public sealed class QueryStoreTopDailyWorkMemLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the daily read's work_mem pin (it mints its own scratch database).";
    private const int ServerId = 1;
    private const int Top = 30;
    private const int Plans = 100_000;

    private static readonly DateTime WindowStart = new(2026, 1, 6, 18, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowEnd = new(2026, 1, 10, 6, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime BuildNow = new(2026, 1, 15, 12, 0, 0, DateTimeKind.Unspecified);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* One interval row per plan per day, collected at noon from 2026-01-05 to 2026-01-09. The interval start is the collection time, as
       the seed of QueryStoreTopDailyReadLiveTests has it, so the table read and the daily summary place each row in the same window. */
    private const string SeedSql = @"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes,
 avg_physical_io_reads, avg_rowcount, query_plan_hash, replica_role, runtime_stats_interval_id, interval_start_time_utc)
SELECT
    ct, 1, 'db' || (q % 3), q, q, 'Regular', ct - interval '10 minutes', ct, NULL, 'h' || q,
    1 + (q * 13 + d) % 23, 1000 + (q * 7919 + d * 104729) % 90001, 10 + (q * 31 + d * 17) % 5003,
    100 + (q * 101 + d * 13) % 500009, (q * 3 + d) % 97, (q + d * 5) % 211, 1 + (q * 29 + d * 3) % 1009,
    'ph' || (q % 5), NULL, q * 100 + d, ct
FROM
(
    SELECT q, d, TIMESTAMP '2026-01-05 12:00:00' + make_interval(days => d) AS ct
    FROM generate_series(1, @plans) AS q
    CROSS JOIN generate_series(0, 4) AS d
) AS s;";

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 600 };
        await command.ExecuteNonQueryAsync(ct);
    }

    /* The store with the plant, the days built, and the coverage row the gate needs. */
    private static async Task PlantAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using (var seed = new NpgsqlCommand(SeedSql, connection) { CommandTimeout = 600 })
        {
            seed.Parameters.Add(new NpgsqlParameter("plans", NpgsqlDbType.Integer) { Value = Plans });
            await seed.ExecuteNonQueryAsync(ct);
        }

        await ExecAsync(connection, "INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES (1, 'srv1', TRUE)", ct);
        await ExecAsync(connection,
            "INSERT INTO collect.query_store_interval_wide_coverage (server_id, filled_since, applied_through) VALUES (1, TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-01-01 00:00:00')", ct);

        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        var built = await QueryStoreTopDaily.RunTickAsync(source, BuildNow, 9, NullLogger.Instance, ct);
        Assert.Equal(0, built.Failed);
        await ExecAsync(connection, "ANALYZE collect.query_store_interval_wide", ct);
        await ExecAsync(connection, "ANALYZE collect.query_store_top_daily", ct);
    }

    /* Temp bytes the database has written, through a fresh unpooled connection. */
    private static async Task<long> TempBytesAsync(string connectionString, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(Unpooled(connectionString));
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand("SELECT temp_bytes FROM pg_stat_database WHERE datname = current_database()", connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static string Unpooled(string connectionString) => new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false }.ConnectionString;

    private static async Task RunLiveAsync(Func<string, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await PlantAsync(scratch, ct);
            await body(scratch.ConnectionString, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public Task ThePlant_SpillsTheDailyStatement_AtTheServersOwnWorkMem() => RunLiveAsync(async (store, ct) =>
    {
        var before = await TempBytesAsync(store, ct);
        await using (var connection = new NpgsqlConnection(Unpooled(store)))
        {
            await connection.OpenAsync(ct);
            await using var command = new NpgsqlCommand(DarlingDataReader.QueryStoreTopDailyTableSql, connection) { CommandTimeout = 600 };
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = WindowStart });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = WindowEnd });
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = Top });
            command.Parameters.Add(DatabaseFilter.One(null).Parameter());
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter<DateOnly> { TypedValue = new DateOnly(2026, 1, 7), NpgsqlDbType = NpgsqlDbType.Date });
            command.Parameters.Add(new NpgsqlParameter<DateOnly> { TypedValue = new DateOnly(2026, 1, 10), NpgsqlDbType = NpgsqlDbType.Date });
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TopFill.FirstCandidates(Top) });
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
            }
        }

        var spilled = await TempBytesAsync(store, ct) - before;
        Assert.True(spilled > 0, "the plant must outgrow the server's own work_mem, or the next fact proves nothing");
    });

    [Fact]
    public Task TheRead_StaysInMemory_OnTheSamePlant() => RunLiveAsync(async (store, ct) =>
    {
        var before = await TempBytesAsync(store, ct);
        await using (var source = NpgsqlDataSource.Create(Unpooled(store)))
        {
            var read = await DarlingDataReader.GetQueryStoreTopWithReachAsync(source, ServerId, WindowStart, WindowEnd, Top, (string?)null, null, null, ct);
            Assert.NotNull(read.Table);
            Assert.Equal(3, read.DailyDaysUsed);
            Assert.Equal(Top, read.Rows.Count);
        }

        var spilled = await TempBytesAsync(store, ct) - before;
        Assert.True(spilled == 0, $"the daily read wrote {spilled} temp bytes");
    });
}
