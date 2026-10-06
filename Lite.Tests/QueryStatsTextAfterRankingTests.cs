/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5381: <c>GetTopQueriesByCpuAsync</c> and <c>GetProcedureStatsComparisonAsync</c> picked a group's representative query
/// text with a LATERAL "newest text per key" over the WHOLE <c>v_query_stats</c> archive, no time filter. DuckDB runs that as a
/// window over every archived row with <c>query_text</c> carried through it, so the read's memory grew with the archive and a
/// three-day archive of wide text ran out of memory at Lite's 1 GB limit, with a 24 hour window, alone. Both reads now rank on
/// the narrow columns and take the text in one aggregate over the read's own window, for the ranked groups only.
/// <para>Three kinds of pin, per read. PARITY: the pre-#5381 statement, kept verbatim below as a test-only oracle, and the new
/// statement return every column of every row alike on a seeded hot + parquet store. MEMORY: on a small synthetic archive of
/// wide high-entropy text with the memory limit set low, the oracle fails with out of memory and the new statement passes.
/// PLAN: every parquet scan of <c>query_stats</c> in the new statements carries the window's <c>collection_time</c> filter.</para>
/// </summary>
public sealed class QueryStatsTextAfterRankingTests : IDisposable
{
    private const int ServerId = 5381;
    private const int OtherServerId = 5382;

    /* The memory pins' archive: 12 daily files of 1,000 rows, each row a 16,000-character hex text (about 16 MB decoded per
       file, 192 MB for the archive), read at 128 MB with 4 threads. The old statements carry the text of the whole archive
       through a window and cannot fit; the new ones touch the window's file(s) only. */
    private const int ArchiveFiles = 12;
    private const int KeysPerFile = 500;
    private const int SnapshotsPerKey = 2;
    private const int TextChars = 16000;
    private const string LowMemoryLimit = "128MB";
    private static readonly DateTime FirstArchiveDay = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Unspecified);

    private readonly string _tempDir;
    private readonly string _archiveDir;
    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public QueryStatsTextAfterRankingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _duckDb.InitializeAsync().GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        _duckDb.Dispose();
        try
        {
            if (Directory.Exists(_tempDir))
            {
                Directory.Delete(_tempDir, recursive: true);
            }
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    /* ---- the pre-#5381 statements, verbatim, as oracles ---- */

    private static string LegacyTopQueriesByCpuSql(string dbClause, int candidates) => @"
WITH ranked AS (
    SELECT
        database_name,
        query_hash,
        MAX(last_execution_time) AS last_execution_time,
        MAX(creation_time) AS creation_time,
        SUM(delta_execution_count) AS total_executions,
        SUM(delta_worker_time) AS total_cpu_us,
        SUM(delta_elapsed_time) AS total_elapsed_us,
        SUM(delta_logical_reads) AS total_reads,
        SUM(delta_rows) AS total_rows,
        SUM(delta_logical_writes) AS total_writes,
        SUM(delta_physical_reads) AS total_physical_reads,
        SUM(delta_spills) AS total_spills,
        MIN(min_dop) AS min_dop,
        MAX(max_dop) AS max_dop,
        MIN(min_worker_time) AS min_worker_time,
        MAX(max_worker_time) AS max_worker_time,
        MIN(min_elapsed_time) AS min_elapsed_time,
        MAX(max_elapsed_time) AS max_elapsed_time,
        MIN(min_physical_reads) AS min_physical_reads,
        MAX(max_physical_reads) AS max_physical_reads,
        MIN(min_rows) AS min_rows,
        MAX(max_rows) AS max_rows,
        MIN(min_grant_kb) AS min_grant_kb,
        MAX(max_grant_kb) AS max_grant_kb,
        MIN(min_spills) AS min_spills,
        MAX(max_spills) AS max_spills,
        MAX(query_plan_hash) AS query_plan_hash,
        MAX(sql_handle) AS sql_handle,
        MAX(plan_handle) AS plan_handle,
        MIN(min_used_grant_kb) AS min_used_grant_kb,
        MAX(max_used_grant_kb) AS max_used_grant_kb,
        MIN(min_ideal_grant_kb) AS min_ideal_grant_kb,
        MAX(max_ideal_grant_kb) AS max_ideal_grant_kb,
        MIN(min_reserved_threads) AS min_reserved_threads,
        MAX(max_reserved_threads) AS max_reserved_threads,
        MIN(min_used_threads) AS min_used_threads,
        MAX(max_used_threads) AS max_used_threads,
        MAX(total_clr_time) AS total_clr_time,
        MAX(plan_generation_num) AS plan_generation_num,
        MAX(CAST(delta_worker_time AS DOUBLE PRECISION) / NULLIF(sample_interval_seconds, 0) / 1000.0) AS worker_time_per_second,
        /* #2012: distinct statement texts merged into this group. query_hash is a SHAPE hash, so
           ad-hoc literal variants collapse - > 1 means the representative text below labels a blend
           (stage 2 folded host_object_name into the key, so INSERT...EXEC statements hosted by
           DIFFERENT procs no longer merge; only ad-hoc blends and pre-upgrade NULL-host history
           can still count > 1). DuckDB's 64-bit hash() stands in for Darling's #1767 content
           digest: comparing fixed-size hashes instead of arbitrarily long batch texts (a review
           note on the twin's asymmetry); a same-group 64-bit collision is negligible for a
           display count. */
        COUNT(DISTINCT hash(query_text)) AS distinct_texts,
        host_object_name
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   last_execution_time >= $2 + $5 * INTERVAL '1' MINUTE" + dbClause + @"
    GROUP BY database_name, query_hash, host_object_name
    HAVING (SUM(delta_execution_count) > 0 OR SUM(delta_elapsed_time) > 0)
    /* #3541 A13: the parallelism floor is part of the QUERY, on the grouped population before the ranking
       and the cap — see the method's note. $6 = 0 admits every group; NULL max_dop (never captured) reads
       as 0 and stays out of a filtered page, as the C# arm it replaces did. */
    AND   COALESCE(MAX(max_dop), 0) >= $6
    /* #5299 round 3 (O16): the ranking ends on the whole group key, so a tie at the candidate cut picks the same keys in every
       refill round, as the Darling and Viewer twins do. */
    ORDER BY SUM(delta_worker_time) DESC, database_name, query_hash, host_object_name
    LIMIT " + candidates + @"
),
module AS (
    /* #1568 module attribution: one procedure_stats identity per sql_handle (latest collection_time
       wins) so a statement whose sql_handle matches a cached procedure/function/trigger inherits its
       db.schema.object and the query_stats row never fans out. Both stores persist the SAME normalized
       CONVERT(varchar(130), ..., 1) handle text, so the join key lines up; unmatched -> ad hoc. */
    SELECT
        sql_handle,
        object_name,
        schema_name,
        database_name
    FROM
    (
        SELECT
            sql_handle,
            object_name,
            schema_name,
            database_name,
            ROW_NUMBER() OVER (PARTITION BY sql_handle ORDER BY collection_time DESC, collection_id DESC) AS rn
        FROM v_procedure_stats
        WHERE server_id = $1
        AND   sql_handle IS NOT NULL
        AND   sql_handle <> ''
    ) ranked_modules
    WHERE rn = 1
),
page AS (
SELECT
    r.*,
    t.query_text,
    t.query_plan_xml AS query_plan,
    m.object_name AS module_object_name,
    m.schema_name AS module_schema_name,
    m.database_name AS module_database_name,
    ROW_NUMBER() OVER (ORDER BY r.total_cpu_us DESC, r.database_name, r.query_hash, r.host_object_name) AS page_ord
FROM ranked r
LEFT JOIN LATERAL (
    SELECT query_text, query_plan_xml
    FROM v_query_stats
    WHERE server_id = $1
    AND   query_hash = r.query_hash
    AND   database_name = r.database_name
    /* #2012 stage 2: the representative text must come from THIS group's own rows - without the
       host constraint a hash shared across host objects could label one caller's row with
       another caller's text (NOT DISTINCT FROM so ad-hoc NULL hosts still match ad-hoc rows). */
    AND   host_object_name IS NOT DISTINCT FROM r.host_object_name
    AND   query_text IS NOT NULL
    /* #5299 round 2 (N3): two rows of one key at one collection_time (two plans, one collection) must give the
       same text on every read, so a tie on the time breaks on the row collected last. */
    ORDER BY collection_time DESC, collection_id DESC
    LIMIT 1
) t ON TRUE
LEFT JOIN module m ON m.sql_handle = r.sql_handle
WHERE t.query_text IS NULL OR t.query_text NOT LIKE 'WAITFOR%'
ORDER BY r.total_cpu_us DESC, r.database_name, r.query_hash, r.host_object_name
LIMIT $4
)
/* #5313: the count row rides beside the page so a round trimmed to nothing still reports whether more candidates exist. */
SELECT p.*, c.candidate_count
FROM (SELECT COUNT(*) AS candidate_count FROM ranked) c
LEFT JOIN page p ON TRUE
ORDER BY p.page_ord";

    private static string LegacyProcedureStatsComparisonSql(string dbClause) => @"
WITH top_current AS (
    SELECT database_name, schema_name, object_name
    FROM v_procedure_stats
    WHERE server_id = $1
    AND   collection_time >= $2 AND collection_time <= $3" + dbClause + @"
    AND   delta_execution_count > 0
    GROUP BY database_name, schema_name, object_name
    ORDER BY SUM(delta_execution_count) DESC
    LIMIT 100
),
top_baseline AS (
    SELECT database_name, schema_name, object_name
    FROM v_procedure_stats
    WHERE server_id = $1
    AND   collection_time >= $4 AND collection_time <= $5" + dbClause + @"
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
    ) combined
),
current_period AS (
    SELECT tp.database_name, tp.schema_name, tp.object_name,
           SUM(ps.delta_execution_count) AS exec_count,
           SUM(ps.delta_elapsed_time)::DOUBLE PRECISION / NULLIF(SUM(ps.delta_execution_count), 0) / 1000.0 AS avg_duration_ms,
           SUM(ps.delta_worker_time)::DOUBLE PRECISION / NULLIF(SUM(ps.delta_execution_count), 0) / 1000.0 AS avg_cpu_ms,
           SUM(ps.delta_physical_reads)::DOUBLE PRECISION / NULLIF(SUM(ps.delta_execution_count), 0) AS avg_reads,
           MAX(ps.sql_handle) AS sql_handle
    FROM top_procs tp
    INNER JOIN v_procedure_stats ps
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
           SUM(ps.delta_elapsed_time)::DOUBLE PRECISION / NULLIF(SUM(ps.delta_execution_count), 0) / 1000.0 AS avg_duration_ms,
           SUM(ps.delta_worker_time)::DOUBLE PRECISION / NULLIF(SUM(ps.delta_execution_count), 0) / 1000.0 AS avg_cpu_ms,
           SUM(ps.delta_physical_reads)::DOUBLE PRECISION / NULLIF(SUM(ps.delta_execution_count), 0) AS avg_reads,
           MAX(ps.sql_handle) AS sql_handle
    FROM top_procs tp
    INNER JOIN v_procedure_stats ps
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
  ON  c.database_name IS NOT DISTINCT FROM b.database_name
  AND c.schema_name IS NOT DISTINCT FROM b.schema_name
  AND c.object_name IS NOT DISTINCT FROM b.object_name
/* #1981: a REPRESENTATIVE statement of the procedure, resolved through the same normalized
   sql_handle join the #1568 module attribution relies on (both stores persist the identical
   CONVERT(varchar(130), ..., 1) text). procedure_stats captures no text of its own, so this is
   the latest captured statement from inside the module - parity with the other two comparison
   grids' text columns, labeled a statement rather than the definition. */
LEFT JOIN LATERAL (
    SELECT qs.query_text
    FROM v_query_stats qs
    WHERE qs.server_id = $1
    AND   qs.sql_handle = COALESCE(c.sql_handle, b.sql_handle)
    AND   qs.query_text IS NOT NULL
    ORDER BY qs.collection_time DESC
    LIMIT 1
) t ON TRUE;";

    /* ---- plumbing ---- */

    private async Task ExecAsync(string sql, params object?[] values)
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync(TestContext.Current.CancellationToken);
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var value in values)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = value });
        }

        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task<(string[] Names, List<object?[]> Rows)> RunRawAsync(string sql, params object[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var value in values)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = value });
        }

        using var reader = await cmd.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        var names = Enumerable.Range(0, reader.FieldCount).Select(reader.GetName).ToArray();
        var rows = new List<object?[]>();
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            var row = new object?[reader.FieldCount];
            for (var i = 0; i < row.Length; i++)
            {
                row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            rows.Add(row);
        }

        return (names, rows);
    }

    private async Task SetLowMemoryAsync()
    {
        await ExecAsync($"SET memory_limit='{LowMemoryLimit}'");
        await ExecAsync("SET threads=4");
        var seen = (await RunRawAsync("SELECT current_setting('memory_limit')::VARCHAR")).Rows[0][0]?.ToString() ?? string.Empty;
        /* 128MB reads back as 122.0 MiB: the limit really is on the instance the reads share. */
        Assert.StartsWith("122", seen, StringComparison.Ordinal);
    }

    private static void AssertSameRows((string[] Names, List<object?[]> Rows) expected, (string[] Names, List<object?[]> Rows) actual, string what)
    {
        Assert.Equal(expected.Names, actual.Names);
        Assert.True(expected.Rows.Count > 0, $"{what}: the oracle returned no rows, so the comparison proves nothing");
        Assert.Equal(expected.Rows.Count, actual.Rows.Count);
        for (var r = 0; r < expected.Rows.Count; r++)
        {
            for (var c = 0; c < expected.Names.Length; c++)
            {
                Assert.True(
                    Equals(expected.Rows[r][c], actual.Rows[r][c]),
                    $"{what}: row {r} column {expected.Names[c]} differs: old '{expected.Rows[r][c]}' vs new '{actual.Rows[r][c]}'");
            }
        }
    }

    private static async Task<Exception?> TryRunAsync(Func<Task> run)
    {
        try
        {
            await run();
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    private async Task InsertQueryStatsAsync(
        DateTime collected, string db, string hash, string? host, string handle, string? text, long cpu,
        string? plan = null, int server = ServerId)
    {
        await ExecAsync(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle, host_object_name,
     last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time, max_dop, query_text, query_plan_xml)
VALUES ($1, $2, $3, 'TestSrv', $4, $5, $6, $7, $2, 10, $8, $8, 1, $9, $10)",
            _nextId++, collected, server, db, hash, handle, host, cpu, text, plan);
    }

    private async Task InsertProcedureStatsAsync(DateTime collected, string db, string schema, string name, string handle, long execs, long cpu)
    {
        await ExecAsync(@"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type, sql_handle,
     delta_execution_count, delta_elapsed_time, delta_worker_time, delta_physical_reads)
VALUES ($1, $2, $3, 'TestSrv', $4, $5, $6, 'P', $7, $8, $9, $9, 5)",
            _nextId++, collected, ServerId, db, schema, name, handle, execs, cpu);
    }

    /* Moves what is in the hot query_stats table to one archive file, the way archival does. */
    private async Task ArchiveHotRowsAsync(string fileName)
    {
        var path = Path.Combine(_archiveDir, fileName).Replace("\\", "/");
        await ExecAsync($"COPY query_stats TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)");
        await ExecAsync("DELETE FROM query_stats");
    }

    private static string Day(DateTime t) => t.ToString("yyyyMMdd", CultureInfo.InvariantCulture);

    /* The parity store: parquet files hold the older rows, the hot table the newest, every shape the newest-text pick has a rule for. */
    private async Task SeedParityStoreAsync()
    {
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var old1 = now.AddDays(-3);
        var old2 = now.AddDays(-2);
        var old3 = now.AddDays(-1).AddHours(-2);
        var hot = now.AddMinutes(-60);

        /* g1: text changes across the archive and the hot table; the hot (newest) one wins. */
        await InsertQueryStatsAsync(old1, "Db1", "0xG1", null, "0xH1", "g1 oldest text", 1_000);
        /* g2a/g2b: one hash under two host objects (#2012 stage 2): each group takes its own host's text. */
        await InsertQueryStatsAsync(old1, "Db1", "0xG2", "dbo.ProcA", "0xH2", "g2 text of ProcA, older", 5_000);
        await InsertQueryStatsAsync(old1, "Db1", "0xG2", "dbo.ProcB", "0xH2", "g2 text of ProcB, older", 4_000);
        /* g7: the plan comes from the same row as the text. */
        await InsertQueryStatsAsync(old1, "Db3", "0xG7", null, "0xH7", "g7 older text", 700, plan: "<plan>g7 older</plan>");
        /* g8: another server's rows of the same keys are not this server's text. */
        await InsertQueryStatsAsync(old1.AddMinutes(5), "Db1", "0xG1", null, "0xH1", "g1 OTHER SERVER text", 1, server: OtherServerId);
        await ArchiveHotRowsAsync($"{Day(old1)}_2300_query_stats.parquet");

        await InsertQueryStatsAsync(old2, "Db1", "0xG1", null, "0xH1", "g1 middle text", 1_000);
        await ArchiveHotRowsAsync($"{Day(old2)}_2300_query_stats.parquet");

        /* g3: the newest row has NO text; the newest row that has one answers. */
        await InsertQueryStatsAsync(old3, "Db2", "0xG3", null, "0xH3", "g3 text, the newest that has one", 3_000);
        await InsertQueryStatsAsync(old3.AddMinutes(30), "Db2", "0xG3", null, "0xH3", null, 3_000);
        /* g4: no row of the group has a text at all. */
        await InsertQueryStatsAsync(old3, "Db2", "0xG4", null, "0xH4", null, 2_500);
        /* g6: a WAITFOR shell is kept off the page. */
        await InsertQueryStatsAsync(old3, "Db2", "0xG6", null, "0xH6", "WAITFOR DELAY '00:00:30'", 90_000);
        await ArchiveHotRowsAsync($"{Day(old3)}_2300_query_stats.parquet");
        await _duckDb.CreateArchiveViewsAsync();

        await InsertQueryStatsAsync(hot, "Db1", "0xG1", null, "0xH1", "g1 newest text (hot)", 1_000);
        await InsertQueryStatsAsync(hot, "Db1", "0xG2", "dbo.ProcA", "0xH2", "g2 text of ProcA, newest", 5_000);
        await InsertQueryStatsAsync(hot, "Db1", "0xG2", "dbo.ProcB", "0xH2", "g2 text of ProcB, newest", 4_000);
        /* g5: two rows of one key at one time with different texts: the row collected last answers (#5299 round 2 N3). */
        await InsertQueryStatsAsync(hot, "Db3", "0xG5", null, "0xH5", "g5 tie text A (lower collection id)", 1_500);
        await InsertQueryStatsAsync(hot, "Db3", "0xG5", null, "0xH5", "g5 tie text B (higher collection id)", 1_500);
        await InsertQueryStatsAsync(hot, "Db3", "0xG7", null, "0xH7", "g7 newest text", 700, plan: "<plan>g7 newest</plan>");
        await InsertQueryStatsAsync(hot, "Db2", "0xG4", null, "0xH4", null, 2_500);
    }

    /* A wide, high-entropy archive of ArchiveFiles daily files, written straight to parquet (the columns the reads need; the hot
       table supplies the rest as NULL through union_by_name). Day n's rows are collected within that day. */
    private async Task SeedWideArchiveAsync()
    {
        for (var f = 0; f < ArchiveFiles; f++)
        {
            var day = FirstArchiveDay.AddDays(f);
            var path = Path.Combine(_archiveDir, $"{Day(day)}_2300_query_stats.parquet").Replace("\\", "/");
            var dayText = day.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            await ExecAsync($@"
COPY (
    SELECT ({(f + 1) * 10_000_000L}::BIGINT + i * 10 + s) AS collection_id,
           TIMESTAMP '{dayText}' + INTERVAL ((i % 20)) HOUR + INTERVAL ((s + 1) * 10) MINUTE AS collection_time,
           {ServerId}::INTEGER AS server_id,
           'TestSrv' AS server_name,
           'db_' || (i % 5)::VARCHAR AS database_name,
           md5((i % 250)::VARCHAR) AS query_hash,
           CASE WHEN i % 9 = 0 THEN 'dbo.Proc_' || (i % 7)::VARCHAR ELSE NULL END AS host_object_name,
           '0x' || md5('h' || (i % 250)::VARCHAR) AS sql_handle,
           TIMESTAMP '{dayText}' + INTERVAL ((i % 20)) HOUR + INTERVAL ((s + 1) * 10) MINUTE AS last_execution_time,
           100::BIGINT AS delta_execution_count,
           (1000 + i)::BIGINT AS delta_worker_time,
           (i + s)::BIGINT AS delta_elapsed_time,
           list_aggregate(list_transform(range(0, {TextChars / 32}), j -> md5(i::VARCHAR || ':' || s::VARCHAR || ':{dayText}:' || j::VARCHAR)), 'string_agg', '') AS query_text
    FROM range(0, {KeysPerFile}) t(i), range(0, {SnapshotsPerKey}) g(s)
) TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)");
        }

        await _duckDb.CreateArchiveViewsAsync();
    }

    private static DateTime LastArchiveDayStart => FirstArchiveDay.AddDays(ArchiveFiles - 1);

    private async Task SeedWideProceduresAsync()
    {
        /* 30 procedures whose handles are ones the wide archive's statements carry; each ran in the last day and the day before. */
        for (var k = 0; k < 30; k++)
        {
            var handle = (await RunRawAsync($"SELECT '0x' || md5('h' || {k}::VARCHAR)")).Rows[0][0]!.ToString()!;
            await InsertProcedureStatsAsync(LastArchiveDayStart.AddHours(12), "db_0", "dbo", $"Proc{k:D2}", handle, 100 + k, 10_000 + k);
            await InsertProcedureStatsAsync(LastArchiveDayStart.AddDays(-1).AddHours(12), "db_0", "dbo", $"Proc{k:D2}", handle, 90 + k, 9_000 + k);
        }
    }

    /* ---- top queries by CPU ---- */

    private (DateTime Start, DateTime End) ParityWindow()
    {
        var end = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        return (end.AddHours(-24 * 7), end);
    }

    [Fact]
    public async Task TopQueriesByCpu_NewStatementReturnsEveryColumnOfEveryRowTheOldOneDid()
    {
        await SeedParityStoreAsync();
        var (start, end) = ParityWindow();

        var oldRows = await RunRawAsync(LegacyTopQueriesByCpuSql(string.Empty, 100), ServerId, start, end, 50, 0, 0);
        var newRows = await RunRawAsync(LocalDataService.TopQueriesByCpuSql(string.Empty, 100), ServerId, start, end, 50, 0, 0);

        AssertSameRows(oldRows, newRows, "top queries by CPU");
    }

    [Fact]
    public async Task TopQueriesByCpu_PicksTheSameTextsThroughTheRealReader()
    {
        await SeedParityStoreAsync();

        var rows = await new LocalDataService(_duckDb).GetTopQueriesByCpuAsync(ServerId, hoursBack: 24 * 7, top: 50);
        string? TextOf(string hash, string? host = null) => rows.Single(r => r.QueryHash == hash && r.HostObjectName == host).QueryText;

        Assert.Equal("g1 newest text (hot)", TextOf("0xG1"));
        Assert.Equal("g2 text of ProcA, newest", TextOf("0xG2", "dbo.ProcA"));
        Assert.Equal("g2 text of ProcB, newest", TextOf("0xG2", "dbo.ProcB"));
        Assert.Equal("g3 text, the newest that has one", TextOf("0xG3"));
        Assert.Equal(string.Empty, TextOf("0xG4"));
        Assert.Equal("g5 tie text B (higher collection id)", TextOf("0xG5"));
        Assert.DoesNotContain(rows, r => r.QueryHash == "0xG6");
        Assert.Equal("g7 newest text", TextOf("0xG7"));
        Assert.Equal("<plan>g7 newest</plan>", rows.Single(r => r.QueryHash == "0xG7").QueryPlan);
    }

    /// <summary>
    /// The one difference from the old statement, on purpose: the text now comes from the read's own window. A window that ends
    /// before the newest capture shows the window's newest statement; the old lateral showed the newest one ever captured.
    /// </summary>
    [Fact]
    public async Task TopQueriesByCpu_TakesTheTextFromTheWindowNotFromAfterIt()
    {
        var inWindow = new DateTime(2026, 9, 10, 12, 0, 0, DateTimeKind.Unspecified);
        var after = inWindow.AddDays(2);
        await InsertQueryStatsAsync(inWindow, "Db1", "0xWIN", null, "0xHW", "text inside the window", 1_000);
        await InsertQueryStatsAsync(after, "Db1", "0xWIN", null, "0xHW", "text captured after the window", 1_000);

        var from = inWindow.AddHours(-1);
        var to = inWindow.AddHours(1);
        var rows = await new LocalDataService(_duckDb).GetTopQueriesByCpuAsync(ServerId, hoursBack: 24, top: 5, fromDate: from, toDate: to);
        Assert.Equal("text inside the window", Assert.Single(rows).QueryText);

        var oldRows = await RunRawAsync(LegacyTopQueriesByCpuSql(string.Empty, 100), ServerId, from, to, 5, 0, 0);
        Assert.Equal("text captured after the window", oldRows.Rows.Single(r => r[0] is not null)[42]);
    }

    [Fact]
    public async Task TopQueriesByCpu_OldStatementRunsOutOfMemoryOnAWideArchive_NewOneDoesNot()
    {
        await SeedWideArchiveAsync();
        await SetLowMemoryAsync();
        var from = LastArchiveDayStart;
        var to = LastArchiveDayStart.AddDays(1);

        var oldFailure = await TryRunAsync(() => RunRawAsync(LegacyTopQueriesByCpuSql(string.Empty, 100), ServerId, from, to, 50, 0, 0));
        Assert.NotNull(oldFailure);
        Assert.Contains("Out of Memory", oldFailure!.Message, StringComparison.OrdinalIgnoreCase);

        var rows = await new LocalDataService(_duckDb).GetTopQueriesByCpuAsync(ServerId, hoursBack: 24, top: 50, fromDate: from, toDate: to);
        Assert.Equal(50, rows.Count);
        Assert.All(rows, r => Assert.True(r.QueryText.Length >= TextChars, "each row carries its full-width text"));
    }

    [Fact]
    public async Task TopQueriesByCpu_TextScanCarriesTheWindowFilter()
    {
        await SeedWideArchiveAsync();
        var from = LastArchiveDayStart;
        var to = LastArchiveDayStart.AddDays(1);

        AssertEveryParquetScanIsWindowed(await ParquetScanFiltersAsync(LocalDataService.TopQueriesByCpuSql(string.Empty, 100), ServerId, from, to, 50, 0, 0));

        /* The control: the statement it replaced has a scan of the whole archive, so this pin can fail. */
        var oldScans = await ParquetScanFiltersAsync(LegacyTopQueriesByCpuSql(string.Empty, 100), ServerId, from, to, 50, 0, 0);
        Assert.Contains(oldScans, scan => !scan.Contains("collection_time>=", StringComparison.Ordinal));
    }

    /* ---- procedure comparison ---- */

    private (DateTime CurStart, DateTime CurEnd, DateTime BaseStart, DateTime BaseEnd) WideComparisonWindows() =>
        (LastArchiveDayStart, LastArchiveDayStart.AddDays(1), LastArchiveDayStart.AddDays(-1), LastArchiveDayStart);

    [Fact]
    public async Task ProcedureComparison_NewStatementReturnsEveryColumnOfEveryRowTheOldOneDid()
    {
        await SeedParityStoreAsync();
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        /* One procedure per tie-free handle of the parity store, ran in both ranges; 0xHNONE has no statement anywhere (blank on both).
           The old statement ordered by collection_time alone, so a handle with two rows at one time (0xH2, 0xH5) was an arbitrary
           pick there; the tie has its own test below. */
        foreach (var (name, handle) in new[] { ("ProcG1", "0xH1"), ("ProcG3", "0xH3"), ("ProcG7", "0xH7"), ("ProcNone", "0xHNONE") })
        {
            await InsertProcedureStatsAsync(now.AddHours(-3), "Db1", "dbo", name, handle, 50, 5_000);
            await InsertProcedureStatsAsync(now.AddHours(-27), "Db1", "dbo", name, handle, 40, 4_000);
        }

        /* A window that holds every statement: the old and the new statement agree on every column of every row. */
        var wide = new object[] { ServerId, now.AddHours(-24), now, now.AddDays(-5), now.AddHours(-24) };
        var oldWide = await RunRawAsync(LegacyProcedureStatsComparisonSql(string.Empty), wide);
        var newWide = await RunRawAsync(LocalDataService.ProcedureStatsComparisonSql(string.Empty), wide);
        AssertSameRows(Sorted(oldWide), Sorted(newWide), "procedure comparison");
        var texts = newWide.Rows.ToDictionary(r => r[2]!.ToString()!, r => r[11]?.ToString());
        Assert.Equal("g1 newest text (hot)", texts["ProcG1"]);
        Assert.Equal("g3 text, the newest that has one", texts["ProcG3"]);
        Assert.Equal("g7 newest text", texts["ProcG7"]);
        Assert.Null(texts["ProcNone"]);

        /* Windows that span 48 hours: the older files' statements sit outside them, so a handle's text is the newest one INSIDE
           (the one difference from the old statement, on purpose), and 0xH3's only statement is outside so it reads blank. */
        var narrow = new object[] { ServerId, now.AddHours(-24), now, now.AddHours(-48), now.AddHours(-24) };
        var narrowRows = await RunRawAsync(LocalDataService.ProcedureStatsComparisonSql(string.Empty), narrow);
        var narrowTexts = narrowRows.Rows.ToDictionary(r => r[2]!.ToString()!, r => r[11]?.ToString());
        Assert.Equal("g1 newest text (hot)", narrowTexts["ProcG1"]);
        Assert.Equal("g7 newest text", narrowTexts["ProcG7"]);
        Assert.Null(narrowTexts["ProcNone"]);
    }

    [Fact]
    public async Task ProcedureComparison_ATieOnTheTimeTakesTheStatementCollectedLast()
    {
        await SeedParityStoreAsync();
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        await InsertProcedureStatsAsync(now.AddHours(-3), "Db3", "dbo", "ProcTie", "0xH5", 50, 5_000);

        var rows = await new LocalDataService(_duckDb).GetProcedureStatsComparisonAsync(ServerId, now.AddHours(-24), now, now.AddHours(-48), now.AddHours(-24));
        Assert.Equal("g5 tie text B (higher collection id)", Assert.Single(rows).QueryText);
    }

    private static (string[] Names, List<object?[]> Rows) Sorted((string[] Names, List<object?[]> Rows) set) =>
        (set.Names, [.. set.Rows.OrderBy(r => r[0]?.ToString(), StringComparer.Ordinal).ThenBy(r => r[1]?.ToString(), StringComparer.Ordinal).ThenBy(r => r[2]?.ToString(), StringComparer.Ordinal)]);

    [Fact]
    public async Task ProcedureComparison_OldStatementRunsOutOfMemoryOnAWideArchive_NewOneDoesNot()
    {
        await SeedWideArchiveAsync();
        await SeedWideProceduresAsync();
        await SetLowMemoryAsync();
        var (curStart, curEnd, baseStart, baseEnd) = WideComparisonWindows();

        var oldFailure = await TryRunAsync(() => RunRawAsync(LegacyProcedureStatsComparisonSql(string.Empty), ServerId, curStart, curEnd, baseStart, baseEnd));
        Assert.NotNull(oldFailure);
        Assert.Contains("Out of Memory", oldFailure!.Message, StringComparison.OrdinalIgnoreCase);

        var rows = await new LocalDataService(_duckDb).GetProcedureStatsComparisonAsync(ServerId, curStart, curEnd, baseStart, baseEnd);
        Assert.Equal(30, rows.Count);
        Assert.All(rows, r => Assert.True(r.QueryText.Length >= TextChars, "each procedure carries its statement's full-width text"));
    }

    [Fact]
    public async Task ProcedureComparison_TextScanCarriesTheWindowFilter()
    {
        await SeedWideArchiveAsync();
        await SeedWideProceduresAsync();
        var (curStart, curEnd, baseStart, baseEnd) = WideComparisonWindows();

        AssertEveryParquetScanIsWindowed(await ParquetScanFiltersAsync(LocalDataService.ProcedureStatsComparisonSql(string.Empty), ServerId, curStart, curEnd, baseStart, baseEnd));

        /* The control: the statement it replaced has a scan of the whole archive, so this pin can fail. */
        var oldScans = await ParquetScanFiltersAsync(LegacyProcedureStatsComparisonSql(string.Empty), ServerId, curStart, curEnd, baseStart, baseEnd);
        Assert.Contains(oldScans, scan => !scan.Contains("collection_time>=", StringComparison.Ordinal));
    }

    /* ---- the other two wide-text reads: bounded by their window, not by the archive ----
       The query stats comparison takes MAX(query_text) over the rows of its two windows and the heatmap projects a 120-character
       preview per row of its window. Neither carries text through an operator over the whole archive, so neither grew with it:
       what they cost is the text of the window's own files, which no SQL shape avoids (DuckDB decodes a row group's text column
       whole, whatever the filter on top of it keeps). These pins hold that: a window of one or two files reads at the low limit
       on a twelve-file archive. A window that spans the archive does not fit any limit, and is not pinned. */

    [Fact]
    public async Task QueryStatsComparison_ReadsAWindowOfTheWideArchiveAtTheLowLimit()
    {
        await SeedWideArchiveAsync();
        await SetLowMemoryAsync();
        var (curStart, curEnd, baseStart, baseEnd) = WideComparisonWindows();

        var rows = await new LocalDataService(_duckDb).GetQueryStatsComparisonAsync(ServerId, curStart, curEnd, baseStart, baseEnd);

        Assert.NotEmpty(rows);
        Assert.All(rows, r => Assert.True(r.QueryText.Length >= TextChars, "each row carries its full-width text"));
    }

    [Fact]
    public async Task QueryHeatmap_ReadsAWindowOfTheWideArchiveAtTheLowLimit()
    {
        await SeedWideArchiveAsync();
        await SetLowMemoryAsync();

        var result = await new LocalDataService(_duckDb).GetQueryHeatmapAsync(
            ServerId, HeatmapMetric.Cpu, hoursBack: 24, fromDate: LastArchiveDayStart, toDate: LastArchiveDayStart.AddDays(1));

        Assert.NotEmpty(result.TimeBuckets);
    }

    /* Every read_parquet scan in an EXPLAIN (FORMAT JSON) plan lists its pushed-down filters in extra_info; each must hold a
       collection_time bound, or the scan reads the whole archive. */
    private async Task<List<string>> ParquetScanFiltersAsync(string sql, params object[] values)
    {
        var json = (await RunRawAsync("EXPLAIN (FORMAT JSON) " + sql, values)).Rows[0][1]!.ToString()!;
        var scans = new List<string>();
        void Walk(System.Text.Json.JsonElement node)
        {
            if (node.ValueKind == System.Text.Json.JsonValueKind.Array)
            {
                foreach (var child in node.EnumerateArray())
                {
                    Walk(child);
                }
            }
            else if (node.ValueKind == System.Text.Json.JsonValueKind.Object)
            {
                if (node.TryGetProperty("name", out var name) && name.GetString()!.Contains("PARQUET", StringComparison.OrdinalIgnoreCase))
                {
                    var filters = node.TryGetProperty("extra_info", out var info) && info.TryGetProperty("Filters", out var f)
                        ? string.Join(" | ", f.ValueKind == System.Text.Json.JsonValueKind.Array ? f.EnumerateArray().Select(x => x.GetString()) : [f.ToString()])
                        : string.Empty;
                    scans.Add(filters);
                }

                if (node.TryGetProperty("children", out var children))
                {
                    Walk(children);
                }
            }
        }

        using var doc = System.Text.Json.JsonDocument.Parse(json);
        Walk(doc.RootElement);
        return scans;
    }

    private static void AssertEveryParquetScanIsWindowed(List<string> scans)
    {
        Assert.NotEmpty(scans);
        foreach (var scan in scans)
        {
            Assert.True(
                scan.Contains("collection_time>=", StringComparison.Ordinal) && scan.Contains("collection_time<=", StringComparison.Ordinal),
                "a read_parquet scan whose pushed-down filters lack the collection_time window: '" + scan + "'");
        }
    }
}
