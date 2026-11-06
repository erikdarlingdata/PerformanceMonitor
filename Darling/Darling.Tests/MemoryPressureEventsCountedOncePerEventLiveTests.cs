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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7691: a server with several memory nodes stores one memory pressure record per node for the same event, so the rows list it
/// once per node and every count doubled. One event is one (second, notification): two records of one notification in one second
/// are one event, 1 ms apart in one second are one, and two notifications at one second are two. Read through the product's own
/// statements: the daily summary, the get_memory_pressure_events summary and the analysis fact. Gated on DARLING_TEST_PG.
/// </summary>
/* #1776 own-store: every fact plants its rows under one dedicated server id and deletes them in cleanup. */
[Collection("live-postgres")]
public sealed class MemoryPressureEventsCountedOncePerEventLiveTests
{
    private const string ServerName = "darling-memory-pressure-nodes";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection c, string sql, CancellationToken ct, params object[] p)
    {
        using var cmd = new NpgsqlCommand(sql, c);
        foreach (var v in p)
            cmd.Parameters.AddWithValue(v);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private const string Low = "RESOURCE_MEMPHYSICAL_LOW";
    private const string Steady = "RESOURCE_MEM_STEADY";

    /// <summary>Eight records, five events: A is one LOW event on two nodes, B is a LOW and a STEADY event at one second,
    /// C is one severe LOW event on two nodes 1 ms apart, D is a STEADY event on two nodes that never reached medium.</summary>
    private static async Task PlantAsync(NpgsqlConnection c, DateTime day, CancellationToken ct)
    {
        var collected = day.AddHours(6);
        var rows = new (int Minute, int Milli, string Notification, int Process, int System)[]
        {
            (10, 0, Low, 2, 0), (10, 0, Low, 2, 0),
            (20, 0, Low, 2, 0), (20, 0, Steady, 2, 0),
            (30, 0, Low, 3, 0), (30, 1, Low, 3, 0),
            (40, 0, Steady, 1, 0), (40, 0, Steady, 1, 0),
        };
        foreach (var r in rows)
            await ExecAsync(c, @"INSERT INTO memory_pressure_events (collection_id, collection_time, server_id, server_name, sample_time, memory_notification, memory_indicators_process, memory_indicators_system)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                ct, CollectionIdGenerator.Next(), collected, ServerId, ServerName,
                collected.AddMinutes(r.Minute).AddMilliseconds(r.Milli), r.Notification, r.Process, r.System);
    }

    private static async Task RunAsync(Func<NpgsqlConnection, DateTime, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(connection, @"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 16, now()::timestamp, now()::timestamp)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ct, ServerId, ServerName);
            var day = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-3), DateTimeKind.Unspecified);
            await PlantAsync(connection, day, ct);
            await body(connection, day, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static Task DeleteRowsAsync(NpgsqlConnection c, CancellationToken ct) =>
        ExecAsync(c, $"DELETE FROM memory_pressure_events WHERE server_id = {ServerId};", ct);

    [Fact]
    public async Task TheDailySummary_CountsAnEventOnce_AcrossNodes_AgainstDevPostgres() =>
        await RunAsync(async (connection, day, ct) =>
        {
            using var read = new NpgsqlCommand("SELECT memory_pressure_events, memory_critical_events FROM (" + DailySummarySql.RangeSql + ") AS calendar WHERE day = $5", connection);
            read.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = day });
            read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = day.AddDays(1) });
            read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(day) });
            read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = day });
            using var reader = await read.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            /* A, B (two notifications) and C are 4 events at medium or worse; only C is severe. */
            Assert.Equal(4, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
            Assert.Equal(1, Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture));
        });

    [Fact]
    public async Task TheMemoryPressureSummary_CountsAnEventOnce_PerNotification_AgainstDevPostgres() =>
        await RunAsync(async (connection, day, ct) =>
        {
            using var read = new NpgsqlCommand(DarlingMemoryGrantReader.MemoryPressureSummarySql, connection);
            read.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = day });
            read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = day.AddDays(1) });
            var counts = new Dictionary<string, (long Events, long MediumOrWorse)>();
            using (var reader = await read.ExecuteReaderAsync(ct))
                while (await reader.ReadAsync(ct))
                    counts[reader.GetString(0)] = (reader.GetInt64(1), reader.GetInt64(2));

            Assert.NotEmpty(counts);
            Assert.Equal((3L, 3L), counts[Low]);     // A, B and C: one each
            Assert.Equal((2L, 1L), counts[Steady]);  // B, and D that stayed normal
        });

    [Fact]
    public async Task TheAnalysisFact_CountsAnEventOnce_AcrossNodes_AgainstDevPostgres() =>
        await RunAsync(async (connection, day, ct) =>
        {
            using var read = new NpgsqlCommand(PgFactCollector.MemoryPressureEventsSql, connection);
            read.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = day });
            read.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = day.AddDays(1) });
            using var reader = await read.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal(4, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
        });
}
