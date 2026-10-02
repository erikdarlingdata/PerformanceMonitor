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
using System.Text.RegularExpressions;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4605's BRIN index on <c>collect.query_store_interval_wide (collection_time)</c>, on a real store: the
/// ensure builds a valid BRIN index once, the upsert's <c>collection_time</c> rewrite stays HOT with it (and
/// does not with a btree), the planner picks it for a 12-hour window at a low <c>random_page_cost</c>, and an
/// INVALID leftover from an interrupted <c>CONCURRENTLY</c> build is dropped and rebuilt.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it, so it cannot race
   live collection. */
public sealed class QueryStoreIntervalWideBrinIndexLiveTests
{
    private const string Table = "collect.query_store_interval_wide";
    private const string Index = QueryStoreIntervalWideBrinIndex.IndexName;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<NpgsqlConnection> OpenMigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    internal static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    internal static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task EnsureAsync(NpgsqlConnection connection, CancellationToken ct) =>
        QueryStoreIntervalWideBrinIndex.EnsureAsync(connection, NullLogger.Instance, ct);

    /// <summary>Rows in collection_time order, one per minute-ish step over <paramref name="days"/> days ending at 2026-09-30.</summary>
    private static Task SeedAsync(NpgsqlConnection connection, int rows, double days, int textLength, CancellationToken ct) =>
        ExecAsync(connection, string.Create(CultureInfo.InvariantCulture, $@"
INSERT INTO {Table} (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc,
    first_execution_time, last_execution_time, query_text, execution_count, avg_duration_us, runtime_stats_interval_id)
SELECT
    timestamp '2026-09-30 00:00:00' - make_interval(secs => ({days} * 86400.0) * (1 - g::numeric / {rows})),
    -4605, 'db', g, g, 'Regular',
    timestamp '2026-09-01 00:00:00' + make_interval(secs => g),
    timestamp '2026-09-01 00:00:00' + make_interval(secs => g),
    repeat('x', {textLength}), 1, 1, g
FROM generate_series(1, {rows}) AS g
ORDER BY g;"), ct);

    /// <summary>The upsert's DO UPDATE SET shape: collection_time rewritten with the changing measures.</summary>
    private const string UpsertShapedSet =
        "collection_time = collection_time + interval '1 minute', last_execution_time = last_execution_time + interval '1 minute', "
        + "execution_count = execution_count + 1, avg_duration_us = avg_duration_us + 1";

    internal static async Task<(long Updated, long Hot)> UpdateAndReadHotAsync(
        NpgsqlConnection connection, string setList, string where, CancellationToken ct)
    {
        /* The pg_stat_xact_* counters are the session's still-pending counts, so an earlier measurement on this
           connection is still in them after its rollback: take the delta around the update. */
        await using var transaction = await connection.BeginTransactionAsync(ct);
        var (updatedBefore, hotBefore) = await ReadXactCountersAsync(connection, transaction, ct);
        await using (var update = new NpgsqlCommand($"UPDATE {Table} SET {setList} WHERE {where}", connection, transaction) { CommandTimeout = 300 })
        {
            await update.ExecuteNonQueryAsync(ct);
        }

        var (updatedAfter, hotAfter) = await ReadXactCountersAsync(connection, transaction, ct);
        await transaction.RollbackAsync(ct);
        return (updatedAfter - updatedBefore, hotAfter - hotBefore);
    }

    private static async Task<(long Updated, long Hot)> ReadXactCountersAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT COALESCE(SUM(n_tup_upd), 0)::bigint, COALESCE(SUM(n_tup_hot_upd), 0)::bigint FROM pg_stat_xact_all_tables WHERE schemaname = 'collect' AND relname = 'query_store_interval_wide'",
            connection, transaction);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    [Fact]
    public async Task Ensure_BuildsAValidBrinIndexOnce_AndASecondCallChangesNothing()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4605 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);

        Assert.Null(await ScalarAsync(connection, $"SELECT to_regclass('{Index}')::text", ct) is string existing ? existing : null);

        await EnsureAsync(connection, ct);

        Assert.True((bool)(await ScalarAsync(connection,
            $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{Index}'::regclass", ct))!);
        var definition = (string)(await ScalarAsync(connection, $"SELECT pg_get_indexdef('{Index}'::regclass)", ct))!;
        Assert.Contains("USING brin (collection_time)", definition);
        Assert.Contains("autosummarize='on'", definition);

        var oid = await ScalarAsync(connection, $"SELECT '{Index}'::regclass::oid", ct);
        await EnsureAsync(connection, ct);
        Assert.Equal(oid, await ScalarAsync(connection, $"SELECT '{Index}'::regclass::oid", ct));
    }

    [Fact]
    public async Task TheUpsertsCollectionTimeRewrite_StaysHotWithTheBrin_AndNotWithABtree()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4605 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await SeedAsync(connection, 2000, 9, 50, ct);
        await EnsureAsync(connection, ct);

        var (updated, hot) = await UpdateAndReadHotAsync(connection, UpsertShapedSet, "runtime_stats_interval_id <= 200", ct);
        Assert.Equal(200, updated);
        Assert.Equal(updated, hot);

        /* The control: first_execution_time is btree-indexed (V154 and the identity index), so this update can
           never be HOT. If it were, the counters above would prove nothing. */
        var (controlUpdated, controlHot) = await UpdateAndReadHotAsync(
            connection, "first_execution_time = first_execution_time + interval '1 minute'",
            "runtime_stats_interval_id > 200 AND runtime_stats_interval_id <= 400", ct);
        Assert.Equal(200, controlUpdated);
        Assert.Equal(0, controlHot);

        /* The mutation this pin exists for: a btree on collection_time makes the same update non-HOT. */
        await ExecAsync(connection, $"DROP INDEX {Index}", ct);
        await ExecAsync(connection, $"CREATE INDEX ix_btree_probe ON {Table} (collection_time)", ct);
        var (btreeUpdated, btreeHot) = await UpdateAndReadHotAsync(connection, UpsertShapedSet, "runtime_stats_interval_id <= 200", ct);
        Assert.Equal(200, btreeUpdated);
        Assert.Equal(0, btreeHot);
    }

    /// <summary>The column names the product's upsert sets in <c>DO UPDATE SET</c>, parsed from its own SQL.</summary>
    internal static HashSet<string> UpsertSetColumns()
    {
        var sql = QueryStoreIntervalWide.UpsertSql;
        var start = sql.IndexOf("DO UPDATE SET", StringComparison.Ordinal);
        Assert.True(start >= 0, "the upsert has no DO UPDATE SET; the parse below would read nothing");
        var end = sql.IndexOf("WHERE (EXCLUDED.collection_time", start, StringComparison.Ordinal);
        Assert.True(end > start, "the upsert's DO UPDATE SET has no closing WHERE (EXCLUDED.collection_time ...)");

        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (Match match in Regex.Matches(
            sql.Substring(start, end - start), @"^\s*(?<name>[a-z_][a-z0-9_]*)\s*=", RegexOptions.Multiline | RegexOptions.CultureInvariant))
        {
            names.Add(match.Groups["name"].Value);
        }

        return names;
    }

    /* Every column any non-BRIN index on the table covers: key and INCLUDE columns (indkey) plus the columns an
       index expression or predicate references (pg_depend records those against the table's columns). */
    private const string NonBrinIndexedColumnsSql = @"
SELECT DISTINCT a.attname
FROM pg_index i
JOIN pg_class ic ON ic.oid = i.indexrelid
JOIN pg_am am ON am.oid = ic.relam
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum > 0 AND NOT a.attisdropped
WHERE i.indrelid = 'collect.query_store_interval_wide'::regclass
  AND am.amname <> 'brin'
  AND (a.attnum = ANY (i.indkey::int2[])
       OR EXISTS (SELECT 1 FROM pg_depend d
                  WHERE d.classid = 'pg_class'::regclass AND d.objid = i.indexrelid
                    AND d.refclassid = 'pg_class'::regclass AND d.refobjid = i.indrelid
                    AND d.refobjsubid = a.attnum))";

    private static async Task<HashSet<string>> NonBrinIndexedColumnsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var columns = new HashSet<string>(StringComparer.Ordinal);
        await using var command = new NpgsqlCommand(NonBrinIndexedColumnsSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            columns.Add(reader.GetString(0));
        }

        return columns;
    }

    [Fact]
    public async Task NoNonBrinIndex_CoversAColumnTheUpsertSets_SoTheUpsertStaysHotForItsWholeSetList()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4605 live test.");
        var ct = TestContext.Current.CancellationToken;

        var setColumns = UpsertSetColumns();
        Assert.True(setColumns.Count >= 50, $"the parse found {setColumns.Count} SET columns; the upsert sets about 55");
        Assert.Contains("collection_time", setColumns);
        Assert.Contains("execution_count", setColumns);
        foreach (var identity in QueryStoreIntervalWide.IdentityColumns.Split(',', StringSplitOptions.TrimEntries))
        {
            Assert.DoesNotContain(identity, setColumns);
        }

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await EnsureAsync(connection, ct);

        /* Data-independent: the catalog, not a seeded update. The scan must see the identity index and the
           first_execution_time index, or it reads nothing. */
        var indexed = await NonBrinIndexedColumnsAsync(connection, ct);
        Assert.Contains("server_id", indexed);
        Assert.Contains("first_execution_time", indexed);
        Assert.Empty(indexed.Intersect(setColumns));

        /* The scan can fail: a btree on a SET column is reported ... */
        await ExecAsync(connection, $"CREATE INDEX ix_btree_probe ON {Table} (execution_count)", ct);
        Assert.Equal(new[] { "execution_count" }, (await NonBrinIndexedColumnsAsync(connection, ct)).Intersect(setColumns));
        await ExecAsync(connection, "DROP INDEX collect.ix_btree_probe", ct);

        /* ... including a column that only an INCLUDE list or a partial-index predicate mentions ... */
        await ExecAsync(connection, $"CREATE INDEX ix_include_probe ON {Table} (server_id) INCLUDE (query_hash) WHERE plan_type IS NOT NULL", ct);
        Assert.Equal(
            new[] { "plan_type", "query_hash" },
            (await NonBrinIndexedColumnsAsync(connection, ct)).Intersect(setColumns).OrderBy(name => name, StringComparer.Ordinal));
        await ExecAsync(connection, "DROP INDEX collect.ix_include_probe", ct);

        /* ... and a BRIN on a SET column is not, because it keeps the update HOT. */
        await ExecAsync(connection, $"CREATE INDEX ix_brin_probe ON {Table} USING brin (last_execution_time)", ct);
        Assert.Empty((await NonBrinIndexedColumnsAsync(connection, ct)).Intersect(setColumns));
    }

    [Fact]
    public async Task ATwelveHourWindow_PlansAsABitmapScanOverTheBrin_AtALowRandomPageCost()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4605 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await SeedAsync(connection, 150000, 9, 150, ct);
        await ExecAsync(connection, $"ANALYZE {Table}", ct);

        const string window = "collection_time >= timestamp '2026-09-29 12:00:00' AND collection_time <= timestamp '2026-09-30 00:00:00'";

        /* Without the index the same read is a sequential scan: the pin can fail. */
        var before = await PlanNodesAsync(connection, window, ct);
        Assert.Contains("Seq Scan", before);
        Assert.DoesNotContain("Bitmap Index Scan", before);

        await EnsureAsync(connection, ct);
        await ExecAsync(connection, $"ANALYZE {Table}", ct);

        var after = await PlanNodesAsync(connection, window, ct);
        Assert.Contains("Bitmap Heap Scan", after);
        Assert.Contains("Bitmap Index Scan", after);
        Assert.DoesNotContain("Seq Scan", after);
    }

    private static async Task<HashSet<string>> PlanNodesAsync(NpgsqlConnection connection, string window, CancellationToken ct)
    {
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await ExecAsync(connection, "SET LOCAL random_page_cost = 1.1", ct);
        await using var command = new NpgsqlCommand(
            $"EXPLAIN (FORMAT JSON) SELECT server_id, query_id, execution_count FROM {Table} WHERE {window}", connection, transaction);
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        await transaction.RollbackAsync(ct);

        var nodes = new HashSet<string>();
        void Walk(JsonElement element)
        {
            if (element.ValueKind == JsonValueKind.Object)
            {
                foreach (var property in element.EnumerateObject())
                {
                    if (property.Name == "Node Type")
                    {
                        nodes.Add(property.Value.GetString()!);
                    }

                    Walk(property.Value);
                }
            }
            else if (element.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in element.EnumerateArray())
                {
                    Walk(item);
                }
            }
        }

        using var document = JsonDocument.Parse(json);
        Walk(document.RootElement);
        return nodes;
    }

    [Fact]
    public async Task AnInvalidLeftover_IsDroppedAndRebuilt()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4605 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await EnsureAsync(connection, ct);
        var oldOid = await ScalarAsync(connection, $"SELECT '{Index}'::regclass::oid", ct);

        /* An interrupted CONCURRENTLY build leaves exactly this catalog state; the rig and CI roles are
           superuser, so flipping indisvalid reproduces it deterministically. */
        await ExecAsync(connection, $"UPDATE pg_index SET indisvalid = false WHERE indexrelid = '{Index}'::regclass", ct);
        Assert.False((bool)(await ScalarAsync(connection, $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{Index}'::regclass", ct))!);

        await EnsureAsync(connection, ct);

        Assert.True((bool)(await ScalarAsync(connection, $"SELECT indisvalid FROM pg_index WHERE indexrelid = '{Index}'::regclass", ct))!);
        Assert.NotEqual(oldOid, await ScalarAsync(connection, $"SELECT '{Index}'::regclass::oid", ct));
        var definition = (string)(await ScalarAsync(connection, $"SELECT pg_get_indexdef('{Index}'::regclass)", ct))!;
        Assert.Contains("USING brin (collection_time)", definition);
        Assert.Contains("autosummarize='on'", definition);
    }
}
