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
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9172 LIVE: a cold first Server Inventory call paid mostly for the 24 hour CPU window, which read each row of the window from the heap
/// (an index scan with a heap fetch per row, or a scan of the day's whole chunk). The window needs three columns, so the covering index
/// <see cref="PgTableTuning.CpuWindowCoverIndexName"/> lets each chunk answer from the index alone. Two claims here: the pages the statement
/// reads fall by more than half once the tuning pass has built the index, and the answer is the same rows and columns before and after, for
/// servers with samples in the window, with none, with samples that start part way through it, and with samples only before it.
/// </summary>
/* #1776 own-store: every test here boots its own database through ScratchPostgres and works entirely inside
   it, so it cannot race the shared store's live collection and is left out of [Collection("live-postgres")]
   deliberately (the next sweep should not "fix" that). */
public sealed class FinOpsInventoryCpuWindowCoverLiveTests
{
    private const int Servers = 12;
    private const int HistoryHours = 30;
    private const int NoSamplesServer = 13;
    private const int MidWindowServer = 14;
    private const int OlderOnlyServer = 15;

    [Fact]
    public async Task TheCoverIndex_CutsTheCpuWindowPages_AndTheInventoryAnswersTheSame()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #9172 CPU window cover test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled,
            "The live #9172 CPU window test needs TimescaleDB: the window it measures spans a hypertable's chunks.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        /* The planner cost the managed store runs with: at the stock 4 the newest chunk is read with a scan of its whole heap, which the index
           cannot beat on cost. */
        await ExecAsync(connection, "SET random_page_cost = 1.1", ct);

        await ExecAsync(connection,
            "INSERT INTO servers (server_id, server_name) SELECT s, 'prod-sqlserver-eastus-cpu-' || s FROM generate_series(1, $1) AS s", ct, OlderOnlyServer);

        /* One sample per server per minute for thirty hours: two day chunks, the newest 24 hours of which are the window. */
        var newest = new DateTime(2026, 6, 20, 12, 0, 0, DateTimeKind.Unspecified);
        await ExecAsync(connection,
            @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
              SELECT (s::bigint * 100000000) + m, $1 - make_interval(mins => m), s, 'prod-sqlserver-eastus-cpu-' || s, $1 - make_interval(mins => m), (m * 7 + s) % 100, 1
              FROM generate_series(1, $2) AS s, generate_series(0, $3 * 60 - 1) AS m",
            ct, newest, Servers, HistoryHours);
        /* A server whose samples start ten hours ago (the window is 24), and one whose samples all end before the window starts. */
        await ExecAsync(connection,
            @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
              SELECT ($2::bigint * 100000000) + m, $1 - make_interval(mins => m), $2, 'prod-sqlserver-eastus-cpu-' || $2, $1 - make_interval(mins => m), (m * 3) % 100, 1
              FROM generate_series(0, 599) AS m",
            ct, newest, MidWindowServer);
        await ExecAsync(connection,
            @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
              SELECT ($2::bigint * 100000000) + m, $1 - make_interval(mins => m), $2, 'prod-sqlserver-eastus-cpu-' || $2, $1 - make_interval(mins => m), (m * 5) % 100, 1
              FROM generate_series(26 * 60, 28 * 60) AS m",
            ct, newest, OlderOnlyServer);
        await ExecAsync(connection, "VACUUM (ANALYZE) cpu_utilization_stats", ct);

        var now = newest.AddMinutes(10);
        var rowsBefore = await ReadFleetRowsAsync(connection, now, ct);
        var pagesBefore = await WindowPagesAsync(connection, now, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);
        var metricsBefore = await DarlingFinOpsInventoryReader.GetServerMetricsAsync(dataSource, rollups, coverage, 30, now, ct);

        /* The product's own pass builds the index, and a vacuum sets the visibility map the index only scan reads, as the store's insert autovacuum does. */
        await PgTableTuning.ApplyAsync(connection, null, ct);
        await ExecAsync(connection, "VACUUM (ANALYZE) cpu_utilization_stats", ct);

        var rowsAfter = await ReadFleetRowsAsync(connection, now, ct);
        var pagesAfter = await WindowPagesAsync(connection, now, ct);
        var metricsAfter = await DarlingFinOpsInventoryReader.GetServerMetricsAsync(dataSource, rollups, coverage, 30, now, ct);

        /* The fixture covers each case: samples in the whole window, samples from part way through it, none at all, and only before it. */
        Assert.NotNull(metricsAfter[1].AvgCpuPct);
        Assert.NotNull(metricsAfter[MidWindowServer].AvgCpuPct);
        Assert.Null(metricsAfter.TryGetValue(NoSamplesServer, out var none) ? none.AvgCpuPct : null);
        Assert.Null(metricsAfter.TryGetValue(OlderOnlyServer, out var older) ? older.AvgCpuPct : null);
        Assert.Equal(rowsBefore, rowsAfter);
        Assert.Equal(metricsBefore.OrderBy(kv => kv.Key).ToList(), metricsAfter.OrderBy(kv => kv.Key).ToList());

        Assert.True(pagesAfter * 2 < pagesBefore,
            $"The CPU window read {pagesBefore} pages before the cover index and {pagesAfter} after; the index only scan should read less than half.");
    }

    /// <summary>Every row and column of the fleet statement, as text, in a fixed order.</summary>
    private static async Task<List<string>> ReadFleetRowsAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        await using var command = FleetCommand(connection, DarlingFinOpsInventoryReader.ServerMetricsSql, now);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = new List<string>();
        while (await reader.ReadAsync(ct))
        {
            var values = new object[reader.FieldCount];
            reader.GetValues(values);
            rows.Add(string.Join("|", values.Select(v => v is DBNull ? "NULL" : Convert.ToString(v, CultureInfo.InvariantCulture))));
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    private static async Task<long> WindowPagesAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        await using var explain = FleetCommand(connection, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + DarlingFinOpsInventoryReader.FleetQueries.Raw.Cpu, now);
        return ExplainBuffers.DataPages(await explain.ExecuteScalarAsync(ct));
    }

    private static NpgsqlCommand FleetCommand(NpgsqlConnection connection, string sql, DateTime now)
    {
        var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now.AddHours(-24), NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now.AddDays(-7), NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now.Date.AddDays(-7), NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 7 });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now.Date, NpgsqlDbType = NpgsqlDbType.Timestamp });
        return command;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] values)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values) command.Parameters.Add(new NpgsqlParameter { Value = value });
        await command.ExecuteNonQueryAsync(ct);
    }
}
