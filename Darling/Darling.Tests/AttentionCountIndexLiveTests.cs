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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so none shares a table with another class. */

/// <summary>
/// #9277 (V293): the partial counter-row index on <c>collect.attention_events</c>. The counter rows (one per collector run) sit among many
/// sampled events, and the per-run counts read of <c>get_client_timeouts</c> fetched every row of the time range from the heap to keep them. With
/// the index the same read touches no sampled row. The facts check the plan the statement gets (rows dropped after the heap fetch, the index
/// the plan reads), never a time, and that the rows it returns do not change.
/// </summary>
public sealed class AttentionCountIndexLiveTests
{
    private const int ServerA = -927701;
    private const int Runs = 400;
    private const int SamplesPerRun = 25;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string RequireLivePostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #9277 attention count index tests (each mints its own scratch database).");
        return ConnectionString!;
    }

    private sealed record PlanFacts(bool ReadsTheCountIndexOnly, long RowsRemovedByFilter, bool ReadsTheHeapForTheRange);

    /// <summary>A fresh store has the index, and the counts read is an index-only scan that drops no row after a fetch. Red on a store without V293.</summary>
    [Fact]
    public async Task FreshStore_CountsRead_IsAnIndexOnlyScan_AndDropsNoSampledRow_AndReturnsTheSameRows()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var bodySucceeded = false;
        try
        {
            Assert.True(await IndexExistsAsync(connection, ct), "a fresh store has no " + PgMigrations.AttentionCountIndexName);
            var end = await SeedAsync(connection, ct);

            var plan = await ReadPlanAsync(connection, end, ct);
            Assert.True(plan.ReadsTheCountIndexOnly, "the counts read does not use the counter-row index only");
            Assert.Equal(0, plan.RowsRemovedByFilter);
            Assert.False(plan.ReadsTheHeapForTheRange, "the counts read still fetches table rows for the range");

            await AssertCountsAreTheSeededOnesAsync(connection, end, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>An upgraded store (no index) reads every sampled row of the range and drops it after the fetch, the cost this rung removes. The rung builds the index,
    /// a second run changes nothing, and the rows the read returns are the same before and after.</summary>
    [Fact]
    public async Task UpgradedStore_DropsSampledRowsAfterTheFetch_UntilTheRungBuildsTheIndex_AndTheRowsDoNotChange()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(RequireLivePostgres(), ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(connection, $"DROP INDEX collect.{PgMigrations.AttentionCountIndexName}", ct);
            Assert.False(await IndexExistsAsync(connection, ct));
            var end = await SeedAsync(connection, ct);

            var before = await ReadPlanAsync(connection, end, ct);
            Assert.False(before.ReadsTheCountIndexOnly);
            Assert.True(before.RowsRemovedByFilter >= Runs * SamplesPerRun, "without the index the read should drop the sampled rows after the fetch, dropped " + before.RowsRemovedByFilter);
            await AssertCountsAreTheSeededOnesAsync(connection, end, ct);

            var rung = PgMigrations.Scripts.Single(m => m.Version == 293).Sql;
            await RunRungAsync(connection, rung, ct);
            var built = await IndexOidAsync(connection, ct);
            Assert.NotNull(built);
            await RunRungAsync(connection, rung, ct);
            Assert.Equal(built, await IndexOidAsync(connection, ct));
            await ExecAsync(connection, "VACUUM (ANALYZE) collect.attention_events", ct);

            var after = await ReadPlanAsync(connection, end, ct);
            Assert.True(after.ReadsTheCountIndexOnly);
            Assert.Equal(0, after.RowsRemovedByFilter);
            await AssertCountsAreTheSeededOnesAsync(connection, end, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    private static async Task RunRungAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var tx = await connection.BeginTransactionAsync(ct);
        await ExecAsync(connection, sql, ct, tx);
        await tx.CommitAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, NpgsqlTransaction? tx = null)
    {
        await using var command = new NpgsqlCommand(sql, connection, tx);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<bool> IndexExistsAsync(NpgsqlConnection connection, CancellationToken ct)
        => await IndexOidAsync(connection, ct) is not null;

    private static async Task<uint?> IndexOidAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'collect' AND c.relkind = 'i' AND c.relname = @name", connection);
        command.Parameters.AddWithValue("name", PgMigrations.AttentionCountIndexName);
        return await command.ExecuteScalarAsync(ct) is uint oid ? oid : null;
    }

    /// <summary>One counter row per run (an attention count that follows the run), and sampled events per run, for two servers over the last four days.</summary>
    private static async Task<DateTime> SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var end = DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Unspecified);
        const string sql = @"
INSERT INTO collect.attention_events
    (attention_event_id, collection_time, server_id, server_name, event_time, event_kind, attention_count,
     database_name, client_app_name, client_hostname, session_id, request_id, interrupt_duration_us, statement_fingerprint, statement_text)
SELECT
    -927700000000 - s.srv::bigint * 1000000 - r.n * 1000 - k.n,
    date_trunc('minute', $1::timestamp) - make_interval(mins => r.n * 12) - make_interval(secs => 5),
    -s.srv,
    'COUNT-INDEX-SRV',
    NULL,
    CASE WHEN k.n = 0 THEN 'count' ELSE 'sample' END,
    CASE WHEN k.n = 0 THEN 1 + (r.n % 7) ELSE 0 END,
    CASE WHEN k.n = 0 THEN NULL ELSE 'db' || (k.n % 3) END,
    CASE WHEN k.n = 0 THEN NULL ELSE 'app' || (k.n % 4) END,
    'host',
    k.n,
    NULL,
    CASE WHEN k.n = 0 THEN NULL ELSE 2000000 + k.n END,
    CASE WHEN k.n = 0 THEN NULL ELSE 'fp' || (k.n % 5) END,
    CASE WHEN k.n = 0 THEN NULL ELSE 'SELECT ' || k.n END
FROM (VALUES (927701), (927702)) AS s(srv)
CROSS JOIN generate_series(0, 399) AS r(n)
CROSS JOIN generate_series(0, 25) AS k(n)";
        await using (var command = new NpgsqlCommand(sql, connection))
        {
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, end);
            await command.ExecuteNonQueryAsync(ct);
        }

        await ExecAsync(connection, "VACUUM (ANALYZE) collect.attention_events", ct);
        return end;
    }

    private static NpgsqlCommand CountsCommand(NpgsqlConnection connection, DateTime end, string prefix)
    {
        var command = new NpgsqlCommand(prefix + AttentionWindowSql.Counts("collect.attention_events", bounded: true), connection);
        command.Parameters.AddWithValue(ServerA);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end.AddDays(-4), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        return command;
    }

    private static async Task<PlanFacts> ReadPlanAsync(NpgsqlConnection connection, DateTime end, CancellationToken ct)
    {
        await using var command = CountsCommand(connection, end, "EXPLAIN (ANALYZE, FORMAT JSON) ");
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var doc = JsonDocument.Parse(json);
        var nodes = new List<JsonElement>();
        void Walk(JsonElement node)
        {
            nodes.Add(node);
            if (node.TryGetProperty("Plans", out var children))
            {
                foreach (var child in children.EnumerateArray())
                {
                    Walk(child);
                }
            }
        }

        Walk(doc.RootElement[0].GetProperty("Plan"));

        /* The statement reads one table, so a node with a relation is a scan of it (a chunk's name, on a hypertable). */
        static bool OnTheTable(JsonElement n) => n.TryGetProperty("Relation Name", out _);
        static string Type(JsonElement n) => n.GetProperty("Node Type").GetString()!;
        var indexOnly = nodes.Any(n => Type(n) == "Index Only Scan" && n.TryGetProperty("Index Name", out var idx)
            && idx.GetString()!.Contains(PgMigrations.AttentionCountIndexName, StringComparison.Ordinal));
        var removed = nodes.Where(OnTheTable).Sum(n => n.TryGetProperty("Rows Removed by Filter", out var r) ? r.GetInt64() : 0L);
        var heap = nodes.Any(n => OnTheTable(n) && (Type(n) == "Seq Scan" || Type(n) == "Bitmap Heap Scan" || Type(n) == "Index Scan"));
        return new PlanFacts(indexOnly, removed, heap);
    }

    private static async Task AssertCountsAreTheSeededOnesAsync(NpgsqlConnection connection, DateTime end, CancellationToken ct)
    {
        await using var command = CountsCommand(connection, end, "");
        var got = new List<(DateTime Time, long Count)>();
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                got.Add((reader.GetDateTime(0), reader.GetInt64(1)));
            }
        }

        var floor = end.AddDays(-4);
        var minute = new DateTime(end.Year, end.Month, end.Day, end.Hour, end.Minute, 0, DateTimeKind.Unspecified);
        var expected = Enumerable.Range(0, Runs)
            .Select(n => (Time: minute.AddMinutes(-12 * n).AddSeconds(-5), Count: (long)(1 + (n % 7))))
            .Where(r => r.Time >= floor)
            .OrderByDescending(r => r.Time)
            .ToList();
        Assert.NotEmpty(expected);
        Assert.Equal(expected.Count, got.Count);
        Assert.Equal(expected, got);
    }
}
