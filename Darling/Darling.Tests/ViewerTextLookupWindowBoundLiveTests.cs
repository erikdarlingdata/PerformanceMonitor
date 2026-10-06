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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5420: the viewer's procedure comparison and the Query Store top reads look text up with a time bound.
/// <list type="bullet">
/// <item>The comparison used to pick each procedure's statement with a per-procedure lookup that had no time bound (no index
/// covers <c>sql_handle</c>, so a procedure with no text walked every retained chunk), and joined the period rows to the top
/// procedures with <c>IS NOT DISTINCT FROM</c>, which cannot hash and ran as a nested loop that threw away millions of rows.</item>
/// <item>The Query Store top reads' inline-text fallback (<c>query_store_stats</c>, one lookup per candidate row with no text
/// row) had the same unbounded shape, in the viewer and in Darling's MCP reader, which keep one tail text each.</item>
/// </list>
/// Every fact runs on a scratch store with the collector tables converted to hypertables (1-day chunks) over 12 days, so
/// "reads only the window's chunks" is something the plan can show. The one documented change is pinned by name: a procedure
/// or query whose only text is older than the window shows none.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every live fact here reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and then works entirely inside it, so it cannot race live
   collection. */
public sealed class ViewerTextLookupWindowBoundLiveTests
{
    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the #5420 text-lookup window-bound live pins (each mints its own scratch database).";
    private const int ServerId = 5420;

    /// <summary>The window's end. History runs 12 days back from here, so the 1-day chunks number 13.</summary>
    private static readonly DateTime End = new(2026, 2, 20, 12, 0, 0, DateTimeKind.Unspecified);

    /// <summary>Comparison windows: current = the last 24 h, baseline = the 24 h before it. The text lookup's window is
    /// baseline start to current end: 48 h, which touches at most 3 one-day chunks.</summary>
    private static readonly DateTime CurrentStart = End.AddHours(-24);
    private static readonly DateTime BaselineStart = End.AddHours(-48);
    private const int WindowChunkCeiling = 3;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// <see cref="ViewerDataService.ProcedureStatsComparisonSql"/> as it stood before #5420, verbatim: the oracle the new
    /// statement is compared with. Do not edit it to follow the live statement.
    /// </summary>
    private const string OldProcedureStatsComparisonSql = """
        WITH top_current AS (
            SELECT database_name, schema_name, object_name
            FROM procedure_stats
            WHERE server_id = $1
            AND   collection_time >= $2 AND collection_time <= $3
            AND   ($6::text[] IS NULL OR database_name = ANY($6))
            AND   delta_execution_count > 0
            GROUP BY database_name, schema_name, object_name
            ORDER BY SUM(delta_execution_count) DESC
            LIMIT 100
        ),
        top_baseline AS (
            SELECT database_name, schema_name, object_name
            FROM procedure_stats
            WHERE server_id = $1
            AND   collection_time >= $4 AND collection_time <= $5
            AND   ($6::text[] IS NULL OR database_name = ANY($6))
            AND   delta_execution_count > 0
            GROUP BY database_name, schema_name, object_name
            ORDER BY SUM(delta_execution_count) DESC
            LIMIT 100
        ),
        top_procs AS (
            SELECT DISTINCT database_name, schema_name, object_name
            FROM (
                SELECT * FROM top_current
                UNION ALL
                SELECT * FROM top_baseline
            ) AS combined
        ),
        current_period AS (
            SELECT tp.database_name, tp.schema_name, tp.object_name,
                   SUM(ps.delta_execution_count) AS exec_count,
                   SUM(ps.delta_elapsed_time)::double precision / NULLIF(SUM(ps.delta_execution_count), 0) / 1000.0 AS avg_duration_ms,
                   SUM(ps.delta_worker_time)::double precision / NULLIF(SUM(ps.delta_execution_count), 0) / 1000.0 AS avg_cpu_ms,
                   SUM(ps.delta_physical_reads)::double precision / NULLIF(SUM(ps.delta_execution_count), 0) AS avg_reads,
                   MAX(ps.sql_handle) AS sql_handle
            FROM top_procs tp
            INNER JOIN procedure_stats ps
              ON  ps.database_name IS NOT DISTINCT FROM tp.database_name
              AND ps.schema_name IS NOT DISTINCT FROM tp.schema_name
              AND ps.object_name IS NOT DISTINCT FROM tp.object_name
            WHERE ps.server_id = $1
            AND   ps.collection_time >= $2 AND ps.collection_time <= $3
            AND   ps.delta_execution_count > 0
            GROUP BY tp.database_name, tp.schema_name, tp.object_name
        ),
        baseline_period AS (
            SELECT tp.database_name, tp.schema_name, tp.object_name,
                   SUM(ps.delta_execution_count) AS exec_count,
                   SUM(ps.delta_elapsed_time)::double precision / NULLIF(SUM(ps.delta_execution_count), 0) / 1000.0 AS avg_duration_ms,
                   SUM(ps.delta_worker_time)::double precision / NULLIF(SUM(ps.delta_execution_count), 0) / 1000.0 AS avg_cpu_ms,
                   SUM(ps.delta_physical_reads)::double precision / NULLIF(SUM(ps.delta_execution_count), 0) AS avg_reads,
                   MAX(ps.sql_handle) AS sql_handle
            FROM top_procs tp
            INNER JOIN procedure_stats ps
              ON  ps.database_name IS NOT DISTINCT FROM tp.database_name
              AND ps.schema_name IS NOT DISTINCT FROM tp.schema_name
              AND ps.object_name IS NOT DISTINCT FROM tp.object_name
            WHERE ps.server_id = $1
            AND   ps.collection_time >= $4 AND ps.collection_time <= $5
            AND   ps.delta_execution_count > 0
            GROUP BY tp.database_name, tp.schema_name, tp.object_name
        )
        SELECT COALESCE(c.database_name, b.database_name) AS database_name,
               COALESCE(c.schema_name, b.schema_name) AS schema_name,
               COALESCE(c.object_name, b.object_name) AS object_name,
               c.exec_count, c.avg_duration_ms, c.avg_cpu_ms, c.avg_reads,
               b.exec_count AS baseline_exec_count,
               b.avg_duration_ms AS baseline_avg_duration_ms,
               b.avg_cpu_ms AS baseline_avg_cpu_ms,
               b.avg_reads AS baseline_avg_reads,
               t.query_text
        FROM current_period c
        FULL OUTER JOIN baseline_period b
          ON  COALESCE(c.database_name, '') = COALESCE(b.database_name, '')
          AND COALESCE(c.schema_name, '') = COALESCE(b.schema_name, '')
          AND COALESCE(c.object_name, '') = COALESCE(b.object_name, '')
        /* #1981: a REPRESENTATIVE statement of the procedure via the same normalized sql_handle
           join #1568's module attribution relies on (both stores persist the identical
           CONVERT(varchar(130), ..., 1) text). procedure_stats captures no text of its own, so
           this is the latest captured statement from inside the module — parity with the other
           two comparison grids, labeled a statement rather than the definition. v_query_stats
           resolves the #1767 payload dimension. */
        LEFT JOIN LATERAL (
            SELECT qs.query_text
            FROM v_query_stats qs
            WHERE qs.server_id = $1
            AND   qs.sql_handle = COALESCE(c.sql_handle, b.sql_handle)
            AND   qs.query_text IS NOT NULL
            ORDER BY qs.collection_time DESC
            LIMIT 1
        ) t ON TRUE
        """;

    /* ───────────────────────────── seeds ───────────────────────────── */

    /// <summary>
    /// 202 procedures, one row per hour for 12 days. Keys with NULL parts (database NULL for every 50th, schema for every
    /// 40th, object for every 60th), two keys that differ only by an empty-string versus NULL object (201, 202), procedures
    /// 150-159 present only before the current window (GONE) and 160-169 only inside it (NEW). The ones the facts care about
    /// carry large counts so they rank inside each period's top 100.
    /// </summary>
    private const string ProcedureSeedSql = @"
INSERT INTO collect.procedure_stats
(collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
 delta_execution_count, delta_worker_time, delta_elapsed_time, delta_physical_reads)
SELECT row_number() OVER ()::bigint, g.t, 5420, 'srv',
       CASE WHEN p % 50 = 0 THEN NULL ELSE 'db' || (p % 4) END,
       CASE WHEN p % 40 = 0 THEN NULL ELSE 'dbo' END,
       CASE WHEN p = 201 THEN '' WHEN p = 202 OR p % 60 = 0 THEN NULL ELSE 'proc_' || p END,
       '0xH' || p,
       CASE WHEN (p + extract(epoch FROM g.t)::bigint / 3600) % 7 = 0 THEN 0
            ELSE (CASE WHEN p <= 10 OR p % 50 = 0 OR p % 40 = 0 OR p % 60 = 0 OR p >= 150 THEN 1000 ELSE 1 END)
                 * (1 + (p * 31 + extract(epoch FROM g.t)::bigint / 3600) % 50) END,
       1000 + (p * 7919) % 90000 + (extract(epoch FROM g.t)::bigint / 3600) % 13,
       2000 + (p * 104729) % 90000 + (extract(epoch FROM g.t)::bigint / 3600) % 17,
       (p * 3 + extract(epoch FROM g.t)::bigint / 3600) % 97
FROM generate_series(1, 202) AS p
CROSS JOIN generate_series(TIMESTAMP '2026-02-08 12:00:00', TIMESTAMP '2026-02-20 12:00:00', INTERVAL '1 hour') AS g(t)
WHERE NOT (p BETWEEN 150 AND 159 AND g.t >= TIMESTAMP '2026-02-19 12:00:00')
AND   NOT (p BETWEEN 160 AND 169 AND g.t <= TIMESTAMP '2026-02-19 12:00:00');";

    /// <summary>
    /// <c>query_stats</c>, one row per handle every 2 hours, the text different on every row so the newest pick matters, and
    /// never two rows of one handle on one instant (a tie on the time is arbitrary in both statements). Handles 1-5 carry text
    /// only older than 3 days (before the 48 h window) and NULL-text rows inside it; 6-10 carry no inline text but a digest that
    /// resolves through <c>query_text_dim</c> (the #1767 payload dimension); 11-160 carry text throughout; 161-180 only NULL
    /// text; 181+ have no row at all.
    /// </summary>
    private const string QueryStatsSeedSql = @"
INSERT INTO collect.query_text_dim (digest, query_text, last_seen)
SELECT decode(md5('dim' || p), 'hex'), 'SELECT ' || p || ' /* from the payload dimension */', TIMESTAMP '2026-02-20 12:00:00'
FROM generate_series(6, 10) AS p;
INSERT INTO collect.query_stats
(collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle, query_text, query_text_digest, delta_execution_count)
SELECT 1000000 + row_number() OVER ()::bigint, g.t, 5420, 'srv', 'db' || (p % 4), '0xQ' || p, '0xH' || p,
       CASE WHEN p <= 5 THEN CASE WHEN g.t < TIMESTAMP '2026-02-17 12:00:00' THEN 'SELECT ' || p || ' /* only before the window */' END
            WHEN p BETWEEN 6 AND 10 THEN NULL
            WHEN p <= 160 THEN 'SELECT ' || p || ' /* ' || to_char(g.t, 'YYYY-MM-DD HH24:MI') || ' */'
            ELSE NULL END,
       CASE WHEN p BETWEEN 6 AND 10 THEN decode(md5('dim' || p), 'hex') END,
       1
FROM generate_series(1, 180) AS p
CROSS JOIN generate_series(TIMESTAMP '2026-02-08 12:00:00', TIMESTAMP '2026-02-20 12:00:00', INTERVAL '2 hours') AS g(t);";

    /// <summary>
    /// <c>query_store_stats</c>: 60 queries, one raw row per hour for 12 days, each its own interval so none dedupes. 1-30 carry
    /// text on every row (different each time); 31-40 only older than the 48 h window; 41-60 none anywhere. No
    /// <c>query_store_text</c> row exists, so every candidate takes the inline-text fallback.
    /// </summary>
    private const string QueryStoreSeedSql = @"
INSERT INTO collect.query_store_stats
(collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
 last_execution_time, query_text, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads,
 avg_logical_io_writes, avg_physical_io_reads, avg_rowcount, query_plan_hash, runtime_stats_interval_id)
SELECT 2000000 + row_number() OVER ()::bigint, g.t, 5420, 'srv', 'db' || (q % 3), q, q, 'Regular', g.t - INTERVAL '10 minutes',
       g.t,
       CASE WHEN q <= 30 THEN 'SELECT ' || q || ' /* ' || to_char(g.t, 'YYYY-MM-DD HH24:MI') || ' */'
            WHEN q <= 40 THEN CASE WHEN g.t < TIMESTAMP '2026-02-18 12:00:00' THEN 'SELECT ' || q || ' /* only before the window */' END
            ELSE NULL END,
       'h' || q, 10 + q, 1000 + q * 37, 500 + q, 5, 5, 5, 5, 'ph' || q,
       q * 100000 + (extract(epoch FROM g.t)::bigint / 3600)
FROM generate_series(1, 60) AS q
CROSS JOIN generate_series(TIMESTAMP '2026-02-08 12:00:00', TIMESTAMP '2026-02-20 12:00:00', INTERVAL '1 hour') AS g(t);";

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.CommandTimeout = 120;
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Migrates a scratch store, converts the collector tables to hypertables BEFORE any row lands (so the
    /// seed creates real 1-day chunks), runs the seeds and hands the body a connection.</summary>
    private static async Task RunLiveAsync(string[] seeds, Func<NpgsqlConnection, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct), "the scratch store needs TimescaleDB for the chunked shape");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, NullLogger.Instance, ct);
        await ExecAsync(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);   /* no policy job reshapes the chunks under a plan assertion */
        var bodySucceeded = false;
        try
        {
            foreach (var seed in seeds)
            {
                await ExecAsync(connection, seed, ct);
            }

            await ExecAsync(connection, "ANALYZE collect.procedure_stats; ANALYZE collect.query_stats; ANALYZE collect.query_store_stats;", ct);
            await body(connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /* ───────────────────────────── plan helpers ───────────────────────────── */

    private static NpgsqlParameter Timestamp(DateTime value) =>
        new NpgsqlParameter<DateTime> { TypedValue = value, NpgsqlDbType = NpgsqlDbType.Timestamp };

    private static void AddComparisonParameters(NpgsqlCommand command)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(Timestamp(CurrentStart));
        command.Parameters.Add(Timestamp(End));
        command.Parameters.Add(Timestamp(BaselineStart));
        command.Parameters.Add(Timestamp(CurrentStart));
        command.Parameters.Add(DatabaseFilter.All.Parameter());
    }

    /// <summary>EXPLAIN (ANALYZE, FORMAT JSON) of <paramref name="sql"/> with the parameters <paramref name="bind"/> adds.</summary>
    private static async Task<JsonElement> ExplainAsync(NpgsqlConnection connection, string sql, Action<NpgsqlCommand> bind, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, connection);
        command.CommandTimeout = 120;
        bind(command);
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        return JsonDocument.Parse(json).RootElement[0].GetProperty("Plan");
    }

    private static IEnumerable<JsonElement> Nodes(JsonElement node)
    {
        yield return node;
        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                foreach (var descendant in Nodes(child))
                {
                    yield return descendant;
                }
            }
        }
    }

    private static string Text(JsonElement node, string property) =>
        node.TryGetProperty(property, out var value) && value.ValueKind == JsonValueKind.String ? value.GetString()! : "";

    private static async Task<HashSet<string>> ChunkNamesAsync(NpgsqlConnection connection, string table, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT chunk_name FROM timescaledb_information.chunks WHERE hypertable_schema = 'collect' AND hypertable_name = @t", connection);
        command.Parameters.AddWithValue("t", table);
        var names = new HashSet<string>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }

    /// <summary>The distinct chunks of <paramref name="table"/> the plan actually read (a node that never ran has 0 loops).</summary>
    private static async Task<(int Scanned, int Total)> ChunksReadAsync(NpgsqlConnection connection, JsonElement plan, string table, CancellationToken ct)
    {
        var chunks = await ChunkNamesAsync(connection, table, ct);
        var read = Nodes(plan)
            .Where(n => chunks.Contains(Text(n, "Relation Name")) && n.TryGetProperty("Actual Loops", out var loops) && loops.GetDouble() > 0)
            .Select(n => Text(n, "Relation Name"))
            .ToHashSet(StringComparer.Ordinal);
        return (read.Count, chunks.Count);
    }

    /* ───────────────────────────── the procedure comparison ───────────────────────────── */

    private static async Task<SortedDictionary<string, string[]>> RunComparisonAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        AddComparisonParameters(command);
        var rows = new SortedDictionary<string, string[]>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var cells = new string[reader.FieldCount];
            for (var i = 0; i < cells.Length; i++)
            {
                cells[i] = reader.IsDBNull(i) ? "<null>" : reader.GetValue(i) is double d
                    ? BitConverter.DoubleToInt64Bits(d).ToString("x", CultureInfo.InvariantCulture)
                    : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture)!;
            }

            rows.Add(string.Join('|', cells[0], cells[1], cells[2]), cells);
        }

        return rows;
    }

    [Fact]
    public async Task TheComparison_ReadsTheTextFromTheWindowsChunksOnly_AndItsPeriodJoinsHaveNoNullSafeJoinFilter()
    {
        await RunLiveAsync(new[] { ProcedureSeedSql, QueryStatsSeedSql }, async (connection, ct) =>
        {
            var plan = await ExplainAsync(connection, ViewerDataService.ProcedureStatsComparisonSql, AddComparisonParameters, ct);

            var (scanned, total) = await ChunksReadAsync(connection, plan, "query_stats", ct);
            Assert.True(total >= 12, $"the seed must span many chunks (found {total}) or this proves nothing.");
            Assert.True(scanned is > 0 and <= WindowChunkCeiling,
                $"the text lookup read {scanned} of {total} query_stats chunks; a 48 h window touches at most {WindowChunkCeiling}.");

            /* IS NOT DISTINCT FROM is not hashable: the old period joins were nested loops with that as a join filter
               over every window row. A hash or merge join on plain equalities has no such filter. */
            Assert.DoesNotContain(Nodes(plan), n => Text(n, "Join Filter").Contains("DISTINCT FROM", StringComparison.Ordinal));
            var keyedJoins = Nodes(plan).Count(n => Text(n, "Node Type") is "Hash Join" or "Merge Join"
                && (Text(n, "Hash Cond") + Text(n, "Merge Cond")).Contains("object_name", StringComparison.Ordinal));
            Assert.True(keyedJoins >= 2, $"expected both period joins as hash or merge joins on the keys, found {keyedJoins}.");
        });
    }

    [Fact]
    public async Task TheOldStatement_ReadsEveryChunk_WhichIsWhatTheBoundRemoves()
    {
        /* The oracle's own shape, so the shape test above cannot pass vacuously: the same seed under the pre-#5420 statement. */
        await RunLiveAsync(new[] { ProcedureSeedSql, QueryStatsSeedSql }, async (connection, ct) =>
        {
            var plan = await ExplainAsync(connection, OldProcedureStatsComparisonSql, AddComparisonParameters, ct);
            var (scanned, total) = await ChunksReadAsync(connection, plan, "query_stats", ct);
            Assert.True(scanned > WindowChunkCeiling, $"the old lookup read only {scanned} of {total} chunks; the seed must make it walk the history.");
            Assert.Contains(Nodes(plan), n => Text(n, "Join Filter").Contains("DISTINCT FROM", StringComparison.Ordinal));
        });
    }

    [Fact]
    public async Task TheComparison_AnswersTheOldStatementsAnswer_OnEveryColumn_ExceptTheDocumentedTextChange()
    {
        await RunLiveAsync(new[] { ProcedureSeedSql, QueryStatsSeedSql }, async (connection, ct) =>
        {
            var old = await RunComparisonAsync(connection, OldProcedureStatsComparisonSql, ct);
            var current = await RunComparisonAsync(connection, ViewerDataService.ProcedureStatsComparisonSql, ct);

            /* the seed has to hold what the facts claim, or equality proves nothing. */
            Assert.True(old.Count >= 100, $"the old statement returned {old.Count} rows.");
            Assert.Contains(old.Keys, k => k.StartsWith("<null>|", StringComparison.Ordinal));           /* a NULL database */
            Assert.Contains(old.Keys, k => k.Contains("|<null>|", StringComparison.Ordinal));            /* a NULL schema */
            Assert.Contains(old.Keys, k => k.EndsWith("|<null>", StringComparison.Ordinal));             /* a NULL object */
            Assert.Contains(old.Keys, k => k.EndsWith("|dbo|", StringComparison.Ordinal));               /* an empty-string object */
            Assert.Contains(old.Values, r => r[3] == "<null>" && r[7] != "<null>");                      /* GONE */
            Assert.Contains(old.Values, r => r[3] != "<null>" && r[7] == "<null>");                      /* NEW */

            Assert.Equal(old.Keys, current.Keys);

            var changed = new List<string>();
            foreach (var (key, oldRow) in old)
            {
                var newRow = current[key];
                for (var i = 0; i < 11; i++)
                {
                    Assert.True(oldRow[i] == newRow[i], $"{key}: column {i} was {oldRow[i]}, is now {newRow[i]}.");
                }

                if (oldRow[11] != newRow[11])
                {
                    changed.Add(key);
                    Assert.Equal("<null>", newRow[11]);                                                  /* only ever text -> none */
                    Assert.Contains("only before the window", oldRow[11], StringComparison.Ordinal);
                }
            }

            /* procedures 1-5 are the only ones whose sole text predates the window; every other text (the dim-resolved
               6-10 included) is the old statement's, character for character. */
            Assert.Equal(5, changed.Count);
            Assert.Contains(current.Values, r => r[11].Contains("payload dimension", StringComparison.Ordinal));
        });
    }

    /* ───────────────────────────── the Query Store top fallback ───────────────────────────── */

    private static void AddViewerQueryStoreParameters(NpgsqlCommand command)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(Timestamp(BaselineStart));
        command.Parameters.Add(Timestamp(End));
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 100 });
        command.Parameters.Add(DatabaseFilter.All.Parameter());
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 100 });
    }

    private static void AddMcpQueryStoreParameters(NpgsqlCommand command)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(Timestamp(BaselineStart));
        command.Parameters.Add(Timestamp(End));
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 100 });
        command.Parameters.Add(DatabaseFilter.All.Parameter());
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 100 });
    }

    private static async Task<Dictionary<long, string?>> QueryStoreTextsAsync(NpgsqlConnection connection, string sql, Action<NpgsqlCommand> bind, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        bind(command);
        var texts = new Dictionary<long, string?>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        var query = reader.GetOrdinal("query_id");
        var text = reader.GetOrdinal("query_text");
        while (await reader.ReadAsync(ct))
        {
            if (reader.IsDBNull(query))
            {
                continue;                                                                                /* the candidate-count row */
            }

            texts[reader.GetInt64(query)] = reader.IsDBNull(text) ? null : reader.GetString(text);
        }

        return texts;
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TheQueryStoreTopFallback_ReadsOnlyTheWindowsChunks_InTheViewerAndTheMcpCopy(bool viewer)
    {
        var (sql, bind) = viewer
            ? (ViewerDataService.QueryStoreTopSql, (Action<NpgsqlCommand>)AddViewerQueryStoreParameters)
            : (DarlingDataReader.QueryStoreTopSql, AddMcpQueryStoreParameters);
        await RunLiveAsync(new[] { QueryStoreSeedSql }, async (connection, ct) =>
        {
            var plan = await ExplainAsync(connection, sql, bind, ct);
            var (scanned, total) = await ChunksReadAsync(connection, plan, "query_store_stats", ct);
            Assert.True(total >= 12, $"the seed must span many chunks (found {total}) or this proves nothing.");
            Assert.True(scanned is > 0 and <= WindowChunkCeiling,
                $"the read took {scanned} of {total} query_store_stats chunks; a 48 h window touches at most {WindowChunkCeiling}, text fallback included.");
        });
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TheQueryStoreTopFallback_KeepsTextFromInsideTheWindow_AndShowsNoneForTextOnlyBeforeIt(bool viewer)
    {
        var (sql, bind) = viewer
            ? (ViewerDataService.QueryStoreTopSql, (Action<NpgsqlCommand>)AddViewerQueryStoreParameters)
            : (DarlingDataReader.QueryStoreTopSql, AddMcpQueryStoreParameters);
        await RunLiveAsync(new[] { QueryStoreSeedSql }, async (connection, ct) =>
        {
            var texts = await QueryStoreTextsAsync(connection, sql, bind, ct);
            Assert.Equal(60, texts.Count);
            for (var q = 1; q <= 30; q++)
            {
                /* the newest in-window row's text: collection_time DESC, and the window's last row is End itself. */
                Assert.Equal($"SELECT {q} /* 2026-02-20 12:00 */", texts[q]);
            }

            for (var q = 31; q <= 60; q++)
            {
                Assert.Null(texts[q]);
            }
        });
    }

    [Fact]
    public void EveryQueryStoreTopStatement_CarriesTheWindowBoundOnItsInlineTextFallback()
    {
        /* The viewer's raw and table reads share one tail; the MCP reader's raw, table and daily reads share another. Both
           carry the bound on the fallback's own scan. */
        foreach (var sql in new[]
                 {
                     ViewerDataService.QueryStoreTopSql, ViewerDataService.QueryStoreTopTableSql,
                     DarlingDataReader.QueryStoreTopSql, DarlingDataReader.QueryStoreTopTableSql, DarlingDataReader.QueryStoreTopDailyTableSql,
                 })
        {
            var normalized = sql.ReplaceLineEndings("\n");
            var fallback = normalized[normalized.IndexOf("FROM query_store_stats AS s", StringComparison.Ordinal)..];
            fallback = fallback[..fallback.IndexOf("LIMIT 1", StringComparison.Ordinal)];
            Assert.Contains("s.collection_time >= $2", fallback, StringComparison.Ordinal);
            Assert.Contains("s.collection_time <= $3", fallback, StringComparison.Ordinal);
        }
    }
}
