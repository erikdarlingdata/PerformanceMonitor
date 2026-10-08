/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4974: the latest-collector-outcome read is a chunk-orderable <c>LIMIT 1</c>. On a hypertable with several
/// 1-day chunks it executes only the newest chunk for a collector that has a row there. A revert to
/// <c>ORDER BY log_id DESC</c> (no index on <c>log_id</c>) executes every chunk.
/// </summary>
/* #1776 own-store: the test mints its own scratch database, so no other class's chunks shape the plan. */
[Collection("live-postgres")]
public sealed class LatestCollectorOutcomePlanTests
{
    private const int TestServerId = -497401;
    private const string TestServerName = "latest-outcome-plan-4974";
    private const string Collector = "running_jobs";

    [Fact]
    public async Task TheShippedRead_ExecutesOnlyTheNewestChunk_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live latest-outcome access-path test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = scratch.ConnectionString;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(connectionString!, ct);
        Assert.SkipUnless(timescaleEnabled, "TimescaleDB is not available on this instance; the chunk walk cannot be asserted.");
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));

        /* No background policy job reshapes the chunks under the plan assertion. */
        await using (var stop = new NpgsqlCommand("SELECT _timescaledb_functions.stop_background_workers()", connection))
        {
            await stop.ExecuteNonQueryAsync(ct);
        }

        var utcNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        await SeedHistoryAsync(connection, utcNow, ct);

        using (var analyze = new NpgsqlCommand("ANALYZE collect.collection_log", connection))
        {
            await analyze.ExecuteNonQueryAsync(ct);
        }

        var plan = await ExplainShippedReadAsync(connection, ct);
        using (var chunkCount = new NpgsqlCommand("SELECT count(*) FROM show_chunks('collect.collection_log')", connection))
        {
            var chunks = Convert.ToInt32(await chunkCount.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
            Assert.True(chunks == 8, "expected the seed to make exactly eight 1-day chunks, got " + chunks + ":\n" + plan);
        }

        var lines = plan.Split('\n');
        var chunkScans = lines.Where(PlanChunkScans.IsChunkScan).ToList();
        var visited = lines
            .Select(l => Regex.Match(l, @"Chunks Visited: (\d+)"))
            .Where(m => m.Success)
            .Select(m => int.Parse(m.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture))
            .ToList();
        if (visited.Count > 0)
        {
            /* TimescaleDB 2.30+: DeferredChunkAppend lists only the chunks it visited and counts them. */
            Assert.True(visited.Count == 1 && visited[0] == 1,
                "expected the deferred chunk append to visit exactly the newest chunk:\n" + plan);
        }
        else
        {
            Assert.True(chunkScans.Count >= 8,
                "expected the plan to carry a chunk scan per seeded day:\n" + plan);
        }

        var executed = chunkScans.Where(l => !l.Contains("never executed", StringComparison.Ordinal)).ToList();
        Assert.True(executed.Count <= 1,
            "more than one chunk executed: the read is walking history again:\n" + plan);

        /* And the product's own read answers: the newest run succeeded, so no precondition. */
        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        Assert.Null(await DarlingRuntimePrecondition.StatusAsync(postgres, TestServerId, TestServerName, Collector, ct));
    }

    /// <summary>
    /// Eight 1-day chunks aligned to the UTC day: one collector every five minutes from 00:00 eight days ago to 23:55
    /// yesterday. Every chunk is in the past, the newest is yesterday's with 288 rows, at any time of day. The ids
    /// follow time order.
    /// </summary>
    private static async Task SeedHistoryAsync(NpgsqlConnection connection, DateTime utcNow, CancellationToken ct)
    {
        using var insert = new NpgsqlCommand(
            "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) " +
            "SELECT 9_000_000_000 + EXTRACT(EPOCH FROM t)::bigint, $1, $2, $3, t, 20, 'SUCCESS', 0 " +
            "FROM generate_series($4::timestamp, $4::timestamp + interval '8 days' - interval '5 minutes', interval '5 minutes') AS t", connection);
        insert.Parameters.AddWithValue(TestServerId);
        insert.Parameters.AddWithValue(TestServerName);
        insert.Parameters.AddWithValue(Collector);
        insert.Parameters.AddWithValue(utcNow.Date.AddDays(-8));
        await insert.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> ExplainShippedReadAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var explain = new NpgsqlCommand(
            "EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) " + DarlingRuntimePrecondition.LatestCollectorOutcomeSql, connection);
        explain.Parameters.AddWithValue(TestServerId);
        explain.Parameters.AddWithValue(Collector);
        var plan = new StringBuilder();
        using var reader = await explain.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            plan.AppendLine(reader.GetString(0));
        }

        return plan.ToString();
    }
}
