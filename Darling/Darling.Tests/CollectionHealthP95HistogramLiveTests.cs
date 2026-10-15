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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5810: <see cref="DarlingDataReader.CollectionHealthSql"/> computes its p95 from a per-(collector, duration)
/// histogram instead of a <c>PERCENTILE_DISC ... WITHIN GROUP</c> in the main aggregate, which had made the whole
/// aggregate a serial GroupAggregate feeding every run of the window into a per-collector sort. These tests hold the
/// histogram to the exact discrete percentile computed here from the planted durations, every other column to the
/// pre-change oracle, and the statement to no ordered-set aggregate.
/// </summary>
/* #1776 own-store: each live test mints its own scratch database, so no other class's rows or chunks shape what it
   reads or how its plans come out. */
[Collection("live-postgres")]
public sealed class CollectionHealthP95HistogramLiveTests
{
    private const int ServerId = -5810001;
    private const string ServerName = "p95-histogram";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public void TheStatementHasNoOrderedSetAggregate()
    {
        Assert.DoesNotContain("WITHIN GROUP", DarlingDataReader.CollectionHealthSql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("WITHIN GROUP", CollectionHealthOracleSql.Mcp, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("running_runs * 20 >= timed_runs * 19", DarlingDataReader.CollectionHealthSql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task P95_IsTheExactDiscretePercentile_AndEveryOtherColumnMatchesTheOracle_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live collection-health p95 test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc);
            now = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerMillisecond), DateTimeKind.Utc);
            var windowStart = DarlingMcpTestData.Naive(now.AddDays(-7));
            var planted = await PlantWeekAsync(connection, now, ct);

            var current = await ReadAsync(connection, DarlingDataReader.CollectionHealthSql, windowStart, ct);
            var oracle = await ReadAsync(connection, CollectionHealthOracleSql.Mcp, windowStart, ct);

            Assert.Equal(planted.Keys.OrderBy(k => k, StringComparer.Ordinal), current.Select(r => (string)r[0]!));
            var p95Ordinal = 6;
            foreach (var row in current)
            {
                var collector = (string)row[0]!;
                var timed = planted[collector].Where(d => d.HasValue).Select(d => d!.Value).OrderBy(d => d).ToList();
                /* PERCENTILE_DISC(0.95): the value at 1-based sorted position ceil(0.95 * n); NULL with no timed run. */
                int? expected = timed.Count == 0 ? null : timed[(19 * timed.Count + 19) / 20 - 1];
                Assert.Equal(expected, row[p95Ordinal] is null ? null : Convert.ToInt32(row[p95Ordinal]));
            }

            /* Every column the pre-change oracle has (it lacks only #5646's trailing_success_runs, appended last)
               is the same value, so the bands and the fleet card's roll-up read what they read before. */
            Assert.Equal(oracle.Count, current.Count);
            for (var r = 0; r < oracle.Count; r++)
            {
                for (var i = 0; i < oracle[r].Length; i++)
                {
                    Assert.True(Equals(oracle[r][i], current[r][i]),
                        $"collector {oracle[r][0]}, column {i}: oracle <{oracle[r][i] ?? "NULL"}> but current <{current[r][i] ?? "NULL"}>");
                }
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, DeleteRowsAsync);
        }
    }

    /// <summary>A week for six collectors: duplicates, NULL durations, a one-run collector, an untimed one, two
    /// runs older than the window, and enough distinct durations that the 95th position falls between ties.</summary>
    private static async Task<Dictionary<string, List<int?>>> PlantWeekAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        var planted = new Dictionary<string, List<int?>>(StringComparer.Ordinal);
        var collectors = new List<string>();
        var times = new List<DateTime>();
        var durations = new List<int?>();
        var statuses = new List<string>();
        var rows = new List<int?>();
        var random = new Random(5810);

        void Add(string collector, double minutesAgo, int? duration, string status = "SUCCESS", bool inWindow = true)
        {
            collectors.Add(collector);
            times.Add(DarlingMcpTestData.Naive(now.AddMinutes(-minutesAgo)));
            durations.Add(duration);
            statuses.Add(status);
            rows.Add(status == "SUCCESS" ? random.Next(0, 3) : null);
            if (inWindow)
            {
                if (!planted.TryGetValue(collector, out var list))
                    planted[collector] = list = new List<int?>();
                list.Add(duration);
            }
        }

        for (var i = 0; i < 2000; i++)
            Add("wide", 3 + i * 5, random.Next(10, 5000), i % 97 == 0 ? "ERROR" : i % 31 == 0 ? "SKIPPED" : "SUCCESS");
        for (var i = 0; i < 400; i++)
            Add("ties", 4 + i * 20, i % 10 < 9 ? 50 : 900);
        for (var i = 0; i < 37; i++)
            Add("with_nulls", 5 + i * 200, i % 3 == 0 ? null : 100 + i);
        Add("single", 60, 4242);
        Add("untimed", 70, null);
        Add("untimed", 80, null, "ERROR");
        for (var i = 0; i < 21; i++)
            Add("bimodal", 6 + i * 400, i < 19 ? 36 : 80_900);
        /* Older than the window: neither may move any p95. */
        Add("wide", 7 * 24 * 60 + 30, 999_999, inWindow: false);
        Add("single", 7 * 24 * 60 + 30, 1, inWindow: false);

        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT $1 + u.ord, $2, $3, u.collector_name, u.collection_time, u.duration_ms, u.status, u.rows_collected
FROM unnest($4::text[], $5::timestamp[], $6::integer[], $7::text[], $8::integer[])
     WITH ORDINALITY AS u (collector_name, collection_time, duration_ms, status, rows_collected, ord)",
            CollectionIdGenerator.Next(), ServerId, ServerName,
            collectors.ToArray(), times.ToArray(), durations.ToArray(), statuses.ToArray(), rows.ToArray());
        return planted;
    }

    private static NpgsqlCommand Bind(NpgsqlConnection connection, string sql, DateTime windowStart)
    {
        var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = windowStart });
        return command;
    }

    private static async Task<List<object?[]>> ReadAsync(NpgsqlConnection connection, string sql, DateTime windowStart, CancellationToken ct)
    {
        await using var command = Bind(connection, sql, windowStart);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var result = new List<object?[]>();
        while (await reader.ReadAsync(ct))
        {
            var values = new object?[reader.FieldCount];
            for (var i = 0; i < values.Length; i++)
                values[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            result.Add(values);
        }

        return result;
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
