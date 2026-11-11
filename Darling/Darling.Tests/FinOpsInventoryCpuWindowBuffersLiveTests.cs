/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8570 LIVE: the Server Inventory fleet read (<see cref="DarlingFinOpsInventoryReader.ServerMetricsSql"/>) grouped the 24 hour CPU window straight
/// off <c>cpu_utilization_stats</c>, and the planner fed the ordered-set 95th percentile through the <c>(server_id, collection_time)</c> index to
/// skip a sort. That walks the heap one server at a time, so every server revisits the whole chunk's pages in a scattered order and the index's own
/// pages come with it: a cold first call paid a disk wait for each. The window is now read on its own, in file order, and sorted in memory. The bound
/// here is a page count, not a time: the pages the statement's scans touch must stay near one pass over the window's rows.
/// </summary>
/* #1776 own-store: every test here boots its own database through ScratchPostgres and works entirely inside
   it, so it cannot race the shared store's live collection and is left out of [Collection("live-postgres")]
   deliberately (the next sweep should not "fix" that). */
public sealed class FinOpsInventoryCpuWindowBuffersLiveTests
{
    private const int Servers = 12;
    private const int HistoryHours = 30;

    [Fact]
    public async Task ServerMetricsSql_ReadsTheCpuWindowOnce_NotOnceThroughTheIndexPerServer()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8570 CPU window buffer test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled,
            "The live #8570 CPU window test needs TimescaleDB: the history it measures is a hypertable's chunks.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        await ExecAsync(connection,
            "INSERT INTO servers (server_id, server_name) SELECT s, 'cpu-window-' || s FROM generate_series(1, $1) AS s", ct, Servers);

        /* One sample per server per minute for thirty hours: two day chunks, the newest 24 hours of which are the window. */
        var newest = new DateTime(2026, 6, 20, 12, 0, 0, DateTimeKind.Unspecified);
        await ExecAsync(connection,
            @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
              SELECT (s::bigint * 100000000) + m, $1 - make_interval(mins => m), s, 'cpu-window-' || s, $1 - make_interval(mins => m), (m * 7 + s) % 100, 1
              FROM generate_series(1, $2) AS s, generate_series(0, $3 * 60 - 1) AS m",
            ct, newest, Servers, HistoryHours);
        await ExecAsync(connection, "ANALYZE cpu_utilization_stats", ct);

        var now = newest.AddMinutes(10);
        await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + DarlingFinOpsInventoryReader.ServerMetricsSql, connection);
        explain.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now.AddHours(-24), NpgsqlDbType = NpgsqlDbType.Timestamp });
        explain.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now.AddDays(-7), NpgsqlDbType = NpgsqlDbType.Timestamp });
        explain.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now.Date.AddDays(-7), NpgsqlDbType = NpgsqlDbType.Timestamp });
        explain.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 7 });
        explain.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now.Date, NpgsqlDbType = NpgsqlDbType.Timestamp });

        var pages = ExplainBuffers.DataPages(await explain.ExecuteScalarAsync(ct));
        var tablePages = (long)(await ScalarAsync(connection,
            "SELECT COALESCE(SUM(pg_relation_size(format('%I.%I', chunk_schema, chunk_name)::regclass)), 0)::bigint / 8192 "
            + "FROM timescaledb_information.chunks WHERE hypertable_name = 'cpu_utilization_stats'", ct))!;

        /* The window is 24 of the 30 hours, so one pass over it is about 224 x 0.8 = 180 heap pages, and the rest of the statement's scans add 40 or so
           (227 on the fix). Walking the heap once per server through the index costs 353 on the old statement, which fails the bound. */
        Assert.True(pages < tablePages * 5 / 4,
            $"The inventory fleet read touched {pages} pages for a CPU table of {tablePages} heap pages; reading the window once in file order stays near one pass over the table.");
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct, params object[] values)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values) command.Parameters.Add(new NpgsqlParameter { Value = value });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }
}
