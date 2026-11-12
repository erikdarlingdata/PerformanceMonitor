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
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8570 LIVE: the Server Inventory fleet read looked the newest database-size snapshot of every server up three times: its time, then the
/// totals and the databases each joined back on that time. The time comes from each server's own newest row, so the planner cannot drop a
/// chunk of the size history while it plans, and each of the three references planned every chunk (133 each on a busy primary, 399 chunk
/// scans for one table, planning read waits included). The newest snapshot's rows are now read once. The bound here is the number of the
/// size history's chunks the plan names, and the answer must not move: the newest snapshot's rows are all counted (they tie on the time)
/// and nothing older is.
/// </summary>
/* #1776 own-store: every test here boots its own database through ScratchPostgres and works entirely inside
   it, so it cannot race the shared store's live collection and is left out of [Collection("live-postgres")]
   deliberately (the next sweep should not "fix" that). */
public sealed class FinOpsInventorySizeHistoryPlanLiveTests
{
    private const int Servers = 6;
    private const int HistoryDays = 40;

    [Fact]
    public async Task ServerMetricsSql_PlansEachSizeHistoryChunkOnce_AndCountsEveryRowOfTheNewestSnapshotOnly()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #8570 size history plan test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled,
            "The live #8570 size history plan test needs TimescaleDB: what it counts is a hypertable's chunks in the plan.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

        await ExecAsync(connection,
            "INSERT INTO servers (server_id, server_name) SELECT s, 'size-plan-' || s FROM generate_series(1, $1) AS s", ct, Servers);

        var now = new DateTime(2026, 6, 20, 12, 0, 0, DateTimeKind.Unspecified);

        /* A snapshot a day for forty days, so forty chunks. The older ones are large and carry other databases: a read that counted any of them
           would show it in the totals and the idle check's database list. */
        await ExecAsync(connection,
            @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, total_size_mb, used_size_mb)
              SELECT (s::bigint * 100000) + (d * 10) + k, $1 - make_interval(days => d), s, 'size-plan-' || s, 'old' || k, 1000, 500
              FROM generate_series(1, $2) AS s, generate_series(1, $3) AS d, generate_series(1, 3) AS k",
            ct, now, Servers, HistoryDays);

        /* The newest snapshot: three rows that tie on the time, one with no used size, and one system database that never counts as a database. */
        await ExecAsync(connection,
            @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, total_size_mb, used_size_mb)
              SELECT (s::bigint * 100000) + v.k, $1, s, 'size-plan-' || s, v.name, v.total, v.used
              FROM generate_series(1, $2) AS s,
                   (VALUES (1, 'sales', 100, 50), (2, 'hr', 200, 100), (3, 'archive', 300, NULL), (4, 'master', 400, 400)) AS v(k, name, total, used)",
            ct, now, Servers);
        await ExecAsync(connection, "ANALYZE database_size_stats", ct);

        var chunkNames = new HashSet<string>(StringComparer.Ordinal);
        await using (var chunks = new NpgsqlCommand(
            "SELECT chunk_name FROM timescaledb_information.chunks WHERE hypertable_name = 'database_size_stats'", connection))
        await using (var chunkReader = await chunks.ExecuteReaderAsync(ct))
        {
            while (await chunkReader.ReadAsync(ct)) chunkNames.Add(chunkReader.GetString(0));
        }

        Assert.True(chunkNames.Count >= HistoryDays, $"The seed should span at least {HistoryDays} chunks, it spans {chunkNames.Count}.");

        /* The plan, not the run: the chunks the statement names while it plans are the cost a cold first call pays for in catalog reads. */
        await using (var explain = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + DarlingFinOpsInventoryReader.ServerMetricsSql, connection))
        {
            AddParameters(explain, now);
            using var plan = JsonDocument.Parse(Convert.ToString(await explain.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!);
            var planned = CountRelations(plan.RootElement[0].GetProperty("Plan"), chunkNames);
            Assert.True(planned <= chunkNames.Count,
                $"The inventory fleet read planned {planned} chunk scans of the size history, which has {chunkNames.Count} chunks: the newest snapshot should be one reference, not one for its time and one each for the totals and the databases.");
        }

        /* The answer: the newest snapshot's four rows, ties included (sales, hr, archive and the system database all count toward the totals), and none of the older rows. */
        await using var statement = new NpgsqlCommand(DarlingFinOpsInventoryReader.ServerMetricsSql, connection);
        AddParameters(statement, now);
        var seen = 0;
        await using (var reader = await statement.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                seen++;
                /* Ordinals as the fleet read selects them: 2 total storage in GB, 17 allocated MB, 18 free MB. */
                Assert.Equal(1000m / 1024m, Convert.ToDecimal(reader.GetValue(2), CultureInfo.InvariantCulture), 6);
                Assert.Equal(1000m, Convert.ToDecimal(reader.GetValue(17), CultureInfo.InvariantCulture));
                Assert.Equal(150m, Convert.ToDecimal(reader.GetValue(18), CultureInfo.InvariantCulture));
            }
        }

        Assert.Equal(Servers, seen);
    }

    private static void AddParameters(NpgsqlCommand command, DateTime now)
    {
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now.AddHours(-24), NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now.AddDays(-7), NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now.Date.AddDays(-7), NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = 7 });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = now.Date, NpgsqlDbType = NpgsqlDbType.Timestamp });
    }

    private static int CountRelations(JsonElement plan, ISet<string> relations)
    {
        var count = plan.TryGetProperty("Relation Name", out var name) && relations.Contains(name.GetString() ?? string.Empty) ? 1 : 0;
        if (plan.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray()) count += CountRelations(child, relations);
        }

        return count;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct, params object[] values)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values) command.Parameters.Add(new NpgsqlParameter { Value = value });
        await command.ExecuteNonQueryAsync(ct);
    }
}
