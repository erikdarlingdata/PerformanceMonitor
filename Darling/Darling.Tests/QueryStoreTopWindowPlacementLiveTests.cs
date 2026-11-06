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
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7814: the web Query Store grid and the MCP get_query_store_top read place a window by the interval's own start
/// (<c>COALESCE(interval_start_time_utc, collection_time)</c>), the expression the duration strip's bars are keyed on. They
/// placed it by <c>collection_time</c>, which trails the interval's start, so on a window of a few hours the grid showed an
/// interval that started before the window (collected inside it) and lacked one that started inside it (collected after its
/// end). The same three rows go through the raw read and the interval-table read.
/// </summary>
/* #1776 own-store: reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and then
   works entirely inside it, the same shape as QueryStoreTopEdgeMarginLiveTests. */
public sealed class QueryStoreTopWindowPlacementLiveTests
{
    private const int Server = 1;
    private const long StartedBefore = 1;
    private const long CollectedAfter = 2;
    private const long Inside = 3;

    private static readonly DateTime Day = new(2026, 10, 14, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowStart = Day.AddHours(12);
    private static readonly DateTime WindowEnd = Day.AddHours(16);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheRawRead_PlacesTheWindowByTheIntervalStart_NotByCollectionTime()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #7814 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        foreach (var (q, started, collected) in Rows())
        {
            await ExecAsync(connection, $"""
                INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
                    execution_type_desc, first_execution_time, module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us,
                    max_duration_us, max_cpu_time_us, replica_role, runtime_stats_interval_id, interval_start_time_utc)
                VALUES ({q}, timestamp '{collected:yyyy-MM-dd HH:mm}', {Server}, 'srv1', 'db0', {q}, {q}, 'Regular',
                        timestamp '{started.AddMinutes(5):yyyy-MM-dd HH:mm}', 'mod', md5('h{q}'), 10, 500, 300, 900, 700, NULL, {q},
                        timestamp '{started:yyyy-MM-dd HH:mm}')
                """, ct);
        }

        var ids = await RunAsync(connection, DarlingDataReader.QueryStoreTopSql, ct);

        Assert.Equal(new long[] { CollectedAfter, Inside }, ids.OrderBy(i => i).ToArray());
    }

    [Fact]
    public async Task TheIntervalTableRead_PlacesTheWindowByTheIntervalStart_NotByCollectionTime()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #7814 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        foreach (var (q, started, collected) in Rows())
        {
            await ExecAsync(connection, $"""
                INSERT INTO collect.query_store_interval_wide
                    (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
                     last_execution_time, query_hash, query_plan_hash, execution_count, avg_duration_us, avg_cpu_time_us,
                     avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, avg_rowcount, runtime_stats_interval_id,
                     interval_start_time_utc)
                VALUES (timestamp '{collected:yyyy-MM-dd HH:mm}', {Server}, 'db0', {q}, {q}, 'Regular',
                        timestamp '{started.AddMinutes(5):yyyy-MM-dd HH:mm}', timestamp '{started.AddMinutes(55):yyyy-MM-dd HH:mm}',
                        md5('h{q}'), 'ph{q}', 10, 500, 300, 100, 0, 0, 1, {q}, timestamp '{started:yyyy-MM-dd HH:mm}')
                """, ct);
        }

        var ids = await RunAsync(connection, DarlingDataReader.QueryStoreTopTableSql, ct);

        Assert.Equal(new long[] { CollectedAfter, Inside }, ids.OrderBy(i => i).ToArray());
    }

    /// <summary>The window is 12:00 to 16:00. One interval started before it and was collected inside it; one started
    /// inside it (the last hour) and was collected after its end; one is plainly inside.</summary>
    private static (long Query, DateTime Started, DateTime Collected)[] Rows() =>
    [
        (StartedBefore, Day.AddHours(10), Day.AddHours(12).AddMinutes(30)),
        (CollectedAfter, Day.AddHours(15), Day.AddHours(16).AddMinutes(30)),
        (Inside, Day.AddHours(13), Day.AddHours(14).AddMinutes(5)),
    ];

    private static async Task<List<long>> RunAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = Server });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = WindowStart, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = WindowEnd, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 20 });
        command.Parameters.Add(DatabaseFilter.All.Parameter());
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = TopFill.FinalCandidates(20) });
        await using var reader = await command.ExecuteReaderAsync(ct);
        var (rows, _) = await TopFill.ReadPageAsync(reader, reader.GetOrdinal("page_ord"), r => r.GetInt64(r.GetOrdinal("query_id")), ct);
        Assert.NotEmpty(rows);   /* the fixture produced rows: an empty answer is a bug in the read, not a pass */
        return rows;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
