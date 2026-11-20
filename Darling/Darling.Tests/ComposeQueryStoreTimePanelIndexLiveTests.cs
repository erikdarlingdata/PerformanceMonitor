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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9151: the covering index <see cref="PgTableTuning.QueryStoreTimePanelIndexName"/> that the fleet Custom View Query Store panel reads its
/// window from. The ensure pass builds it, and a second pass changes nothing. On the same seeded rows the panel's answer is the same with and
/// without it. Its INCLUDE list holds every column the panel's de-duplication reads: with the other scan types switched off, every Query Store
/// measure runs as an Index Only Scan with no heap fetches. Whether the planner PREFERS the index on a given store is not asserted: it depends on
/// the heap's row order and the chunk's size, so the page counts are in the PR body, not here.
/// </summary>
/* #1776 own-store: each live fact mints its own scratch database through ScratchPostgres. */
public sealed class ComposeQueryStoreTimePanelIndexLiveTests
{
    private const string SkipMessage = "Set DARLING_TEST_PG to a Postgres connection string to run the #9151 panel index live tests.";

    private const string NarrowFilter = "[{\"dimension\":\"module_name\",\"op\":\"like\",\"value\":\"%Order%\"}]";

    /// <summary>Every Query Store measure the panel offers, with the aggregate each takes.</summary>
    private static readonly (string Measure, string Aggregate)[] AllMeasures =
    {
        ("qs_executions", "sum"), ("qs_max_duration_us", "max"), ("qs_max_cpu_us", "max"), ("qs_avg_duration_us", ""), ("qs_avg_cpu_us", ""),
        ("qs_total_duration_us", ""), ("qs_total_cpu_us", ""),
    };

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 600 };
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The panel indexes on the table, by name and index oid. A rebuilt index has a new oid.</summary>
    private static async Task<List<string>> IndexesAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var list = new List<string>();
        await using var command = new NpgsqlCommand(
            @"SELECT c.relname || '#' || c.oid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
              WHERE i.indrelid = 'collect.query_store_stats'::regclass ORDER BY c.relname", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            list.Add(reader.GetString(0));
        }

        return list;
    }

    /// <summary>The panel index has INCLUDE columns: more attributes than key attributes.</summary>
    private static async Task<bool> IsCoveringAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            @"SELECT bool_or(i.indnatts > i.indnkeyatts) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
              WHERE i.indrelid = 'collect.query_store_stats'::regclass AND c.relname = @n", connection);
        command.Parameters.AddWithValue("n", PgTableTuning.QueryStoreTimePanelIndexName);
        return (await command.ExecuteScalarAsync(ct)) is true;
    }

    /// <summary>The plan's heap fetches on the panel index, scanning every chunk (the index is built per chunk).</summary>
    private static async Task<long?> HeapFetchesAsync(NpgsqlConnection connection, ComposeCompiled compiled, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + compiled.Sql, connection) { CommandTimeout = 300 };
        foreach (var p in compiled.Parameters)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = p.NpgsqlDbType, Value = p.Value });
        }

        using var document = JsonDocument.Parse((string)(await command.ExecuteScalarAsync(ct))!);
        LastPlanShape = PlanShape(document.RootElement[0].GetProperty("Plan"));
        return PanelIndexHeapFetchesByChunk(document.RootElement[0].GetProperty("Plan"));
    }

    private static string LastPlanShape = string.Empty;

    private static string PlanShape(JsonElement node)
    {
        var text = node.GetProperty("Node Type").GetString() + (node.TryGetProperty("Index Name", out var n) ? "(" + n.GetString() + ")" : node.TryGetProperty("Relation Name", out var rn) ? "(" + rn.GetString() + ")" + (node.TryGetProperty("Filter", out var fl) ? "{" + fl.GetString() + "}" : string.Empty) : string.Empty);
        return node.TryGetProperty("Plans", out var children) ? text + "[" + string.Join(", ", children.EnumerateArray().Select(PlanShape)) + "]" : text;
    }

    /// <summary>The chunk indexes carry the hypertable index's name inside a longer one, so the match is by suffix.</summary>
    private static long? PanelIndexHeapFetchesByChunk(JsonElement node)
    {
        long? total = null;
        if (node.TryGetProperty("Node Type", out var type) && type.GetString() == "Index Only Scan"
            && node.TryGetProperty("Index Name", out var name) && (name.GetString() ?? string.Empty).EndsWith(PgTableTuning.QueryStoreTimePanelIndexName, StringComparison.Ordinal))
        {
            total = node.GetProperty("Heap Fetches").GetInt64();
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                var sub = PanelIndexHeapFetchesByChunk(child);
                if (sub is not null)
                {
                    total = (total ?? 0) + sub.Value;
                }
            }
        }

        return total;
    }

    /// <summary>A start builds at most one large missing index, so a store missing several builds the panel index at a later start. Returns the indexes once it exists.</summary>
    private static async Task<List<string>> StartsUntilBuiltAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var log = new CapturingTestLogger();
        for (var start = 1; start <= 5; start++)
        {
            await PgTableTuning.ApplyAsync(connection, log, hourly: false, ct);
            var indexes = await IndexesAsync(connection, ct);
            if (indexes.Any(i => i.StartsWith(PgTableTuning.QueryStoreTimePanelIndexName + "#", StringComparison.Ordinal)))
            {
                return indexes;
            }
        }

        Assert.Fail("the panel index was not built in five starts: " + log.Joined);
        return new List<string>();
    }

    [Fact]
    public async Task TheEnsurePass_BuildsThePanelIndexOnce_AndTheAnswerIsUnchanged()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await ComposeQueryStoreSharedDedupeLiveTests.OpenSeededAsync(scratch, ct);
            await ExecAsync(connection, "DROP INDEX IF EXISTS collect." + PgTableTuning.QueryStoreTimePanelIndexName, ct);
            await ExecAsync(connection, "VACUUM (ANALYZE) collect.query_store_stats", ct);
            var context = ComposeQueryStoreSharedDedupeLiveTests.Context(null);
            var panel = ComposeQueryStoreSharedDedupeLiveTests.PanelJson("qs_executions", "sum", new[] { "module_name" }, 25, false, NarrowFilter);
            var compiled = ComposeQueryStoreSharedDedupeLiveTests.Compile(panel, context, shared: true);

            var rowsBefore = await ComposeQueryStoreSharedDedupeLiveTests.RowsAsync(connection, compiled, ct);
            Assert.NotEmpty(rowsBefore);
            Assert.DoesNotContain(await IndexesAsync(connection, ct), i => i.StartsWith(PgTableTuning.QueryStoreTimePanelIndexName + "#", StringComparison.Ordinal));

            PgTableTuning.ResetHourlyMissingWarnings();
            var afterFirst = await StartsUntilBuiltAsync(connection, ct);

            /* a second pass: no index is rebuilt, dropped or added */
            await PgTableTuning.ApplyAsync(connection, null, hourly: false, ct);
            Assert.Equal(afterFirst, await IndexesAsync(connection, ct));
            await PgTableTuning.ApplyAsync(connection, null, hourly: true, ct);
            Assert.Equal(afterFirst, await IndexesAsync(connection, ct));

            await ExecAsync(connection, "VACUUM (ANALYZE) collect.query_store_stats", ct);
            var rowsAfter = await ComposeQueryStoreSharedDedupeLiveTests.RowsAsync(connection, compiled, ct);

            var difference = ComposeQueryStoreSharedDedupeLiveTests.Difference(rowsAfter, rowsBefore);
            Assert.True(difference is null, "the answer changed with the index: " + difference);
            Assert.True(await IsCoveringAsync(connection, ct), "the panel index has no INCLUDE columns");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task ThePanelIndex_HoldsEveryColumnThePanelReads_ForEveryQueryStoreMeasure()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await ComposeQueryStoreSharedDedupeLiveTests.OpenSeededAsync(scratch, ct);
            PgTableTuning.ResetHourlyMissingWarnings();
            await StartsUntilBuiltAsync(connection, ct);
            await ExecAsync(connection, "VACUUM (ANALYZE) collect.query_store_stats", ct);
            /* The planner's choice is not under test. Leave it only the panel index (the others, and the seq and bitmap scans, are out of the way), so a
               column missing from INCLUDE shows as a plan that is not an Index Only Scan, or as heap fetches. */
            await ExecAsync(connection, @"DO $$ DECLARE r record; BEGIN
                FOR r IN SELECT c.oid::regclass AS ix FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
                         WHERE i.indrelid = 'collect.query_store_stats'::regclass AND c.relname <> '" + PgTableTuning.QueryStoreTimePanelIndexName + @"'
                         AND NOT EXISTS (SELECT 1 FROM pg_constraint k WHERE k.conindid = i.indexrelid)
                LOOP EXECUTE format('DROP INDEX %s', r.ix); END LOOP; END $$", ct);
            await ExecAsync(connection, "SET enable_seqscan = off; SET enable_bitmapscan = off", ct);
            var context = ComposeQueryStoreSharedDedupeLiveTests.Context(null);

            foreach (var (measure, aggregate) in AllMeasures)
            {
                foreach (var groups in new[] { new[] { "module_name" }, new[] { "database_name", "query_hash" } })
                {
                    var panel = ComposeQueryStoreSharedDedupeLiveTests.PanelJson(measure, aggregate, groups, 10, true, NarrowFilter);
                    var compiled = ComposeQueryStoreSharedDedupeLiveTests.Compile(panel, context, shared: true);
                    var fetches = await HeapFetchesAsync(connection, compiled, ct);
                    Assert.True(fetches is not null, $"{measure} by {string.Join(",", groups)}: the plan has no Index Only Scan on the panel index: " + LastPlanShape);
                    Assert.True(fetches == 0, $"{measure} by {string.Join(",", groups)}: {fetches} heap fetches, so a column the panel reads is not in the index");
                }
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}
