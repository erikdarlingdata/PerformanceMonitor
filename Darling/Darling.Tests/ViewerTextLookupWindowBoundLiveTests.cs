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
    /// <see cref="ViewerDataService.QueryStatsComparisonSql"/> as it stood before the #5420 review round, verbatim: the oracle the
    /// derived-table statement is compared with. Do not edit it to follow the live statement.
    /// </summary>
    private const string OldQueryStatsComparisonSql = """
            WITH top_current AS (
                SELECT query_hash, database_name
                FROM query_stats
                WHERE server_id = $1
                AND   collection_time >= $2 AND collection_time <= $3
                AND   ($6::text[] IS NULL OR database_name = ANY($6))
                AND   delta_execution_count > 0
                GROUP BY query_hash, database_name
                ORDER BY SUM(delta_execution_count) DESC
                LIMIT 100
            ),
            top_baseline AS (
                SELECT query_hash, database_name
                FROM query_stats
                WHERE server_id = $1
                AND   collection_time >= $4 AND collection_time <= $5
                AND   ($6::text[] IS NULL OR database_name = ANY($6))
                AND   delta_execution_count > 0
                GROUP BY query_hash, database_name
                ORDER BY SUM(delta_execution_count) DESC
                LIMIT 100
            ),
            top_hashes AS (
                SELECT DISTINCT query_hash, database_name
                FROM (
                    SELECT * FROM top_current
                    UNION ALL
                    SELECT * FROM top_baseline
                ) AS combined
            ),
            current_period AS (
                SELECT th.database_name, th.query_hash,
                       SUM(qs.delta_execution_count) AS exec_count,
                       SUM(qs.delta_elapsed_time)::double precision / NULLIF(SUM(qs.delta_execution_count), 0) / 1000.0 AS avg_duration_ms,
                       SUM(qs.delta_worker_time)::double precision / NULLIF(SUM(qs.delta_execution_count), 0) / 1000.0 AS avg_cpu_ms,
                       SUM(qs.delta_physical_reads)::double precision / NULLIF(SUM(qs.delta_execution_count), 0) AS avg_reads,
                       MAX(qs.query_text) AS query_text
                FROM top_hashes th
                INNER JOIN v_query_stats qs
                  ON  qs.query_hash IS NOT DISTINCT FROM th.query_hash
                  AND qs.database_name IS NOT DISTINCT FROM th.database_name
                WHERE qs.server_id = $1
                AND   qs.collection_time >= $2 AND qs.collection_time <= $3
                AND   qs.delta_execution_count > 0
                GROUP BY th.database_name, th.query_hash
            ),
            baseline_period AS (
                SELECT th.database_name, th.query_hash,
                       SUM(qs.delta_execution_count) AS exec_count,
                       SUM(qs.delta_elapsed_time)::double precision / NULLIF(SUM(qs.delta_execution_count), 0) / 1000.0 AS avg_duration_ms,
                       SUM(qs.delta_worker_time)::double precision / NULLIF(SUM(qs.delta_execution_count), 0) / 1000.0 AS avg_cpu_ms,
                       SUM(qs.delta_physical_reads)::double precision / NULLIF(SUM(qs.delta_execution_count), 0) AS avg_reads,
                       MAX(qs.query_text) AS query_text
                FROM top_hashes th
                INNER JOIN v_query_stats qs
                  ON  qs.query_hash IS NOT DISTINCT FROM th.query_hash
                  AND qs.database_name IS NOT DISTINCT FROM th.database_name
                WHERE qs.server_id = $1
                AND   qs.collection_time >= $4 AND qs.collection_time <= $5
                AND   qs.delta_execution_count > 0
                GROUP BY th.database_name, th.query_hash
            )
            SELECT COALESCE(c.database_name, b.database_name) AS database_name,
                   COALESCE(c.query_hash, b.query_hash) AS query_hash,
                   COALESCE(c.query_text, b.query_text) AS query_text,
                   c.exec_count, c.avg_duration_ms, c.avg_cpu_ms, c.avg_reads,
                   b.exec_count AS baseline_exec_count,
                   b.avg_duration_ms AS baseline_avg_duration_ms,
                   b.avg_cpu_ms AS baseline_avg_cpu_ms,
                   b.avg_reads AS baseline_avg_reads
            FROM current_period c
            FULL OUTER JOIN baseline_period b
              ON  COALESCE(c.database_name, '') = COALESCE(b.database_name, '')
              AND COALESCE(c.query_hash, '') = COALESCE(b.query_hash, '')
            """;

    /// <summary>
    /// <see cref="ViewerDataService.QueryStoreComparisonSql"/> as it stood before the #5420 review round, verbatim (oracle).
    /// </summary>
    private const string OldQueryStoreComparisonSql = """
            WITH deduped_current AS (
                /* LOAD-BEARING (correctness, not just perf) — #1841. The rows are CUMULATIVE per-interval
                   snapshots and the collector re-fetches the OPEN interval every cycle, so the SAME interval
                   (same first_execution_time) is stored repeatedly with a growing execution_count. Keep the
                   LATEST snapshot per interval before aggregating.

                   The execution-count weighting below does NOT rescue this on its own: the repeated snapshots
                   of one interval carry DIFFERENT (growing) weights AND different avg_* values, so an open
                   interval is weighted by the triangular sum of its own growth. That bias is stronger in the
                   recent window than in the baseline window (recent windows hold more still-open intervals),
                   which skews the very delta this comparison exists to compute. One deduped CTE per window,
                   so both arms are treated identically. */
                SELECT
                    database_name,
                    /* #2150: query_id is projected (it was already a partition key) so the period CTEs can
                       resolve text from collect.query_store_text, which is keyed on it. */
                    query_id,
                    query_hash,
                    query_text,
                    execution_count,
                    avg_duration_us,
                    avg_cpu_time_us,
                    avg_logical_io_reads,
                    ROW_NUMBER() OVER
                    (
                        PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                        ORDER BY collection_time DESC, execution_count DESC
                    ) AS rn
                FROM query_store_stats
                WHERE server_id = $1
                AND   collection_time >= $2 AND collection_time <= $3
                AND   ($6::text[] IS NULL OR database_name = ANY($6))
            ),
            deduped_baseline AS (
                SELECT
                    database_name,
                    /* #2150: query_id is projected (it was already a partition key) so the period CTEs can
                       resolve text from collect.query_store_text, which is keyed on it. */
                    query_id,
                    query_hash,
                    query_text,
                    execution_count,
                    avg_duration_us,
                    avg_cpu_time_us,
                    avg_logical_io_reads,
                    ROW_NUMBER() OVER
                    (
                        PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                        ORDER BY collection_time DESC, execution_count DESC
                    ) AS rn
                FROM query_store_stats
                WHERE server_id = $1
                AND   collection_time >= $4 AND collection_time <= $5
                AND   ($6::text[] IS NULL OR database_name = ANY($6))
            ),
            top_current AS (
                SELECT database_name, query_hash
                FROM deduped_current
                WHERE rn = 1
                AND   execution_count > 0
                GROUP BY database_name, query_hash
                ORDER BY SUM(execution_count) DESC
                LIMIT 100
            ),
            top_baseline AS (
                SELECT database_name, query_hash
                FROM deduped_baseline
                WHERE rn = 1
                AND   execution_count > 0
                GROUP BY database_name, query_hash
                ORDER BY SUM(execution_count) DESC
                LIMIT 100
            ),
            top_hashes AS (
                SELECT DISTINCT database_name, query_hash
                FROM (
                    SELECT * FROM top_current
                    UNION ALL
                    SELECT * FROM top_baseline
                ) AS combined
            ),
            current_period AS (
                SELECT th.database_name, th.query_hash,
                       SUM(qs.execution_count) AS exec_count,
                       SUM(qs.execution_count * qs.avg_duration_us::double precision) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_duration_ms,
                       SUM(qs.execution_count * qs.avg_cpu_time_us::double precision) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_cpu_ms,
                       SUM(qs.execution_count * qs.avg_logical_io_reads::double precision) / NULLIF(SUM(qs.execution_count), 0) AS avg_reads,
                       /* #2150: this comparison groups by query_hash, but text is stored per query_id, so the
                          side table is joined on the finer key and MAX still picks one member's text for the
                          group — the same arbitrary-but-deterministic choice MAX(qs.query_text) made before.
                          The join cannot fan out (query_store_text is one row per server/database/query_id, by
                          primary key), so the execution-count SUMs above are unaffected. The COALESCE keeps
                          pre-cutover rows, whose text is still inline, reading exactly as they used to. */
                       MAX(COALESCE(x.query_sql_text, qs.query_text)) AS query_text
                FROM top_hashes th
                INNER JOIN deduped_current qs
                  ON  qs.query_hash IS NOT DISTINCT FROM th.query_hash
                  AND qs.database_name IS NOT DISTINCT FROM th.database_name
                LEFT JOIN query_store_text AS x
                  ON  x.server_id = $1
                  AND x.database_name = qs.database_name
                  AND x.query_id = qs.query_id
                WHERE qs.rn = 1
                AND   qs.execution_count > 0
                GROUP BY th.database_name, th.query_hash
            ),
            baseline_period AS (
                SELECT th.database_name, th.query_hash,
                       SUM(qs.execution_count) AS exec_count,
                       SUM(qs.execution_count * qs.avg_duration_us::double precision) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_duration_ms,
                       SUM(qs.execution_count * qs.avg_cpu_time_us::double precision) / NULLIF(SUM(qs.execution_count), 0) / 1000.0 AS avg_cpu_ms,
                       SUM(qs.execution_count * qs.avg_logical_io_reads::double precision) / NULLIF(SUM(qs.execution_count), 0) AS avg_reads,
                       /* #2150 — same resolution as current_period above. Both arms need it because the final
                          projection takes COALESCE(c.query_text, b.query_text): converting only one arm would
                          leave a GONE row (present in baseline only) with no text to fall back to. */
                       MAX(COALESCE(x.query_sql_text, qs.query_text)) AS query_text
                FROM top_hashes th
                INNER JOIN deduped_baseline qs
                  ON  qs.query_hash IS NOT DISTINCT FROM th.query_hash
                  AND qs.database_name IS NOT DISTINCT FROM th.database_name
                LEFT JOIN query_store_text AS x
                  ON  x.server_id = $1
                  AND x.database_name = qs.database_name
                  AND x.query_id = qs.query_id
                WHERE qs.rn = 1
                AND   qs.execution_count > 0
                GROUP BY th.database_name, th.query_hash
            )
            SELECT COALESCE(c.database_name, b.database_name) AS database_name,
                   COALESCE(c.query_hash, b.query_hash) AS query_hash,
                   COALESCE(c.query_text, b.query_text) AS query_text,
                   c.exec_count, c.avg_duration_ms, c.avg_cpu_ms, c.avg_reads,
                   b.exec_count AS baseline_exec_count,
                   b.avg_duration_ms AS baseline_avg_duration_ms,
                   b.avg_cpu_ms AS baseline_avg_cpu_ms,
                   b.avg_reads AS baseline_avg_reads
            FROM current_period c
            FULL OUTER JOIN baseline_period b
              ON  COALESCE(c.database_name, '') = COALESCE(b.database_name, '')
              AND COALESCE(c.query_hash, '') = COALESCE(b.query_hash, '')
            """;

    /// <summary>
    /// <c>query_stats</c> for the Top Queries comparison twin: 130 hashes, one row every 2 hours for 12 days, with every delta
    /// column set so the averages are real numbers. NULL parts: the database of every 25th, the hash of every 40th and of 125
    /// (so 125 is NULL on both keys, and 40, 80 and 120 merge into one NULL-hash group). Empty-string keys beside them, each against a NULL it must stay apart from: 48
    /// is the one empty hash, in the database that the heavy NULL-hash group (40, 80, 120) sits in, and 127 is the one empty
    /// database while 51 is a NULL database on its hash. 48 and 51 carry one execution per row, so they never make the top 100 and a
    /// join that merged a NULL key with an empty one would fan the heavy row out; 100-109 only before the current window
    /// (GONE) and 110-119 only inside it (NEW). 100 hashes cannot hold all of them, so the top-100 cut trims each period.
    /// </summary>
    private const string QueryStatsTwinSeedSql = @"
INSERT INTO collect.query_stats
(collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle, query_text,
 delta_execution_count, delta_worker_time, delta_elapsed_time, delta_physical_reads)
SELECT 3000000 + row_number() OVER ()::bigint, g.t, 5420, 'srv',
       CASE WHEN p % 25 = 0 OR p = 51 THEN NULL WHEN p = 127 THEN '' ELSE 'db' || (p % 4) END,
       CASE WHEN p % 40 = 0 OR p = 125 THEN NULL WHEN p = 48 THEN '' WHEN p = 51 THEN '0xQ127' ELSE '0xQ' || p END,
       '0xT' || p, 'SELECT ' || p || ' /* ' || to_char(g.t, 'YYYY-MM-DD HH24:MI') || ' */',
       CASE WHEN p IN (48, 51) THEN 1
            WHEN (p + extract(epoch FROM g.t)::bigint / 7200) % 7 = 0 THEN 0
            ELSE (CASE WHEN p <= 10 OR p % 25 = 0 OR p % 40 = 0 OR p >= 100 THEN 1000 ELSE 1 END)
                 * (1 + (p * 31 + extract(epoch FROM g.t)::bigint / 7200) % 50) END,
       1000 + (p * 7919) % 90000 + (extract(epoch FROM g.t)::bigint / 7200) % 13,
       2000 + (p * 104729) % 90000 + (extract(epoch FROM g.t)::bigint / 7200) % 17,
       (p * 3 + extract(epoch FROM g.t)::bigint / 7200) % 97
FROM generate_series(1, 130) AS p
CROSS JOIN generate_series(TIMESTAMP '2026-02-08 12:00:00', TIMESTAMP '2026-02-20 12:00:00', INTERVAL '2 hours') AS g(t)
WHERE NOT (p BETWEEN 100 AND 109 AND g.t >= TIMESTAMP '2026-02-19 12:00:00')
AND   NOT (p BETWEEN 110 AND 119 AND g.t <= TIMESTAMP '2026-02-19 12:00:00');";

    /// <summary>
    /// <c>query_store_stats</c> for the Query Store comparison twin: 130 queries, one snapshot an hour with two snapshots per
    /// 2-hour interval (so the dedupe keeps the later one), the same NULL, empty-string (47: the one empty hash, in the database the NULL-hash group 80 sits in; 127: the one
    /// empty database, 51 is a NULL database on its hash; 47 and 51 never make the top 100) and GONE/NEW shapes as the Top Queries seed, and a
    /// <c>query_store_text</c> row for every query whose inline text is NULL (q divisible by 3, outside the NULL-database ones).
    /// </summary>
    private const string QueryStoreTwinSeedSql = @"
INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen)
SELECT 5420, 'db' || (q % 3), q, 'SELECT ' || q || ' /* side table */', TIMESTAMP '2026-02-20 12:00:00'
FROM generate_series(1, 130) AS q
WHERE q % 3 = 0 AND q % 25 <> 0;
INSERT INTO collect.query_store_stats
(collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
 last_execution_time, query_text, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads,
 avg_logical_io_writes, avg_physical_io_reads, avg_rowcount, query_plan_hash, runtime_stats_interval_id)
SELECT 4000000 + row_number() OVER ()::bigint, g.t, 5420, 'srv',
       CASE WHEN q % 25 = 0 OR q = 51 THEN NULL WHEN q = 127 THEN '' ELSE 'db' || (q % 3) END, q, q, 'Regular',
       date_bin(INTERVAL '2 hours', g.t, TIMESTAMP '2026-02-08 12:00:00'), g.t,
       CASE WHEN q % 3 = 0 THEN NULL ELSE 'SELECT ' || q || ' /* inline */' END,
       CASE WHEN q % 40 = 0 OR q = 125 THEN NULL WHEN q = 47 THEN '' WHEN q = 51 THEN 'h127' ELSE 'h' || q END,
       CASE WHEN q IN (47, 51) THEN 1
            WHEN (q + extract(epoch FROM g.t)::bigint / 3600) % 7 = 0 THEN 0
            ELSE (CASE WHEN q <= 10 OR q % 25 = 0 OR q % 40 = 0 OR q >= 100 THEN 1000 ELSE 1 END)
                 * (1 + (q * 31 + extract(epoch FROM g.t)::bigint / 3600) % 50) END,
       1000 + (q * 7919) % 90000 + (extract(epoch FROM g.t)::bigint / 3600) % 13,
       2000 + (q * 104729) % 90000 + (extract(epoch FROM g.t)::bigint / 3600) % 17,
       (q * 3 + extract(epoch FROM g.t)::bigint / 3600) % 97, 5, 5, 5, 'ph' || q,
       q * 100000 + (extract(epoch FROM g.t)::bigint / 7200)
FROM generate_series(1, 130) AS q
CROSS JOIN generate_series(TIMESTAMP '2026-02-08 12:00:00', TIMESTAMP '2026-02-20 12:00:00', INTERVAL '1 hour') AS g(t)
WHERE NOT (q BETWEEN 100 AND 109 AND g.t >= TIMESTAMP '2026-02-19 12:00:00')
AND   NOT (q BETWEEN 110 AND 119 AND g.t <= TIMESTAMP '2026-02-19 12:00:00');";

    /// <summary>
    /// Two procedures whose statements share one instant: each handle has two <c>query_stats</c> rows at 11:00 on the last day
    /// with different collection ids and texts. The first procedure's rows go in lower id first, the second's higher id first,
    /// so no heap order picks the higher id for both.
    /// </summary>
    private const string TieSeedSql = @"
INSERT INTO collect.procedure_stats
(collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
 delta_execution_count, delta_worker_time, delta_elapsed_time, delta_physical_reads)
VALUES (9000001, TIMESTAMP '2026-02-20 11:00:00', 5420, 'srv', 'db1', 'dbo', 'proc_tie_a', '0xHTIEA', 500000, 1, 1, 1),
       (9000002, TIMESTAMP '2026-02-20 11:00:00', 5420, 'srv', 'db1', 'dbo', 'proc_tie_b', '0xHTIEB', 500000, 1, 1, 1);
INSERT INTO collect.query_stats
(collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle, query_text, delta_execution_count)
VALUES (9100001, TIMESTAMP '2026-02-20 11:00:00', 5420, 'srv', 'db1', '0xQTIEA1', '0xHTIEA', 'tie A: the lower collection id', 1),
       (9100002, TIMESTAMP '2026-02-20 11:00:00', 5420, 'srv', 'db1', '0xQTIEA2', '0xHTIEA', 'tie A: the higher collection id', 1),
       (9100004, TIMESTAMP '2026-02-20 11:00:00', 5420, 'srv', 'db1', '0xQTIEB2', '0xHTIEB', 'tie B: the higher collection id', 1),
       (9100003, TIMESTAMP '2026-02-20 11:00:00', 5420, 'srv', 'db1', '0xQTIEB1', '0xHTIEB', 'tie B: the lower collection id', 1);";

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

    private static void AddComparisonParameters(NpgsqlCommand command) =>
        BindComparison(command, BaselineStart, CurrentStart, DatabaseFilter.All);

    /// <summary>The comparison's six parameters: the last 24 h as the current window, <paramref name="baselineStart"/> to
    /// <paramref name="baselineEnd"/> as the baseline, and <paramref name="filter"/> as the database filter.</summary>
    private static void BindComparison(NpgsqlCommand command, DateTime baselineStart, DateTime baselineEnd, DatabaseFilter filter)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(Timestamp(CurrentStart));
        command.Parameters.Add(Timestamp(End));
        command.Parameters.Add(Timestamp(baselineStart));
        command.Parameters.Add(Timestamp(baselineEnd));
        command.Parameters.Add(filter.Parameter());
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

    private static Task<SortedDictionary<string, string[]>> RunComparisonAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        RunKeyedAsync(connection, sql, AddComparisonParameters, 3, ct);

    /// <summary>Every row of <paramref name="sql"/> as text cells (doubles by their exact bits), keyed on the first
    /// <paramref name="keyColumns"/> columns.</summary>
    private static async Task<SortedDictionary<string, string[]>> RunKeyedAsync(
        NpgsqlConnection connection, string sql, Action<NpgsqlCommand> bind, int keyColumns, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        bind(command);
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

            rows.Add(string.Join('|', cells.Take(keyColumns)), cells);
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

    /* ───────────────────────────── review round 1 (#5420): the gap, the tie, the twins, the filter ───────────────────────────── */

    /// <summary>Two 24 h windows that each touch at most 2 one-day chunks.</summary>
    private const int TwoWindowChunkCeiling = 4;

    [Fact]
    public async Task TheComparison_WithABaselineAWeekBack_ReadsTheTwoWindowsChunksAndNotTheGapBetweenThem()
    {
        await RunLiveAsync(new[] { ProcedureSeedSql, QueryStatsSeedSql }, async (connection, ct) =>
        {
            var baselineStart = End.AddDays(-8);
            var baselineEnd = End.AddDays(-7);
            void Bind(NpgsqlCommand c) => BindComparison(c, baselineStart, baselineEnd, DatabaseFilter.All);

            /* the baseline must hold procedures, or a read that skips it proves nothing. */
            var rows = await RunKeyedAsync(connection, ViewerDataService.ProcedureStatsComparisonSql, Bind, 3, ct);
            Assert.Contains(rows.Values, r => r[7] != "<null>" && r[3] != "<null>");

            var plan = await ExplainAsync(connection, ViewerDataService.ProcedureStatsComparisonSql, Bind, ct);
            var (scanned, total) = await ChunksReadAsync(connection, plan, "query_stats", ct);
            Assert.True(total >= 12, $"the seed must span many chunks (found {total}) or this proves nothing.");
            Assert.True(scanned is > 0 and <= TwoWindowChunkCeiling,
                $"the text lookup read {scanned} of {total} query_stats chunks; two 24 h windows touch at most {TwoWindowChunkCeiling}, and the span between them 9.");
        });
    }

    [Fact]
    public async Task WhenOneProceduresStatementsShareAnInstant_TheRowCollectedLastSuppliesTheText()
    {
        await RunLiveAsync(new[] { ProcedureSeedSql, QueryStatsSeedSql, TieSeedSql }, async (connection, ct) =>
        {
            var rows = await RunComparisonAsync(connection, ViewerDataService.ProcedureStatsComparisonSql, ct);
            Assert.Equal("tie A: the higher collection id", rows["db1|dbo|proc_tie_a"][11]);
            Assert.Equal("tie B: the higher collection id", rows["db1|dbo|proc_tie_b"][11]);
        });
    }

    [Fact]
    public async Task TheComparison_UnderADatabaseFilter_AnswersTheOldStatementsAnswer_ExceptTheDocumentedTextChange()
    {
        await RunLiveAsync(new[] { ProcedureSeedSql, QueryStatsSeedSql }, async (connection, ct) =>
        {
            var filter = DatabaseFilter.One("db1");
            void Bind(NpgsqlCommand c) => BindComparison(c, BaselineStart, CurrentStart, filter);
            var old = await RunKeyedAsync(connection, OldProcedureStatsComparisonSql, Bind, 3, ct);
            var current = await RunKeyedAsync(connection, ViewerDataService.ProcedureStatsComparisonSql, Bind, 3, ct);

            Assert.True(old.Count >= 20, $"the filtered old statement returned {old.Count} rows.");
            Assert.All(old.Keys, k => Assert.StartsWith("db1|", k, StringComparison.Ordinal));
            Assert.Equal(old.Keys, current.Keys);
            foreach (var (key, oldRow) in old)
            {
                for (var i = 0; i < 11; i++)
                {
                    Assert.True(oldRow[i] == current[key][i], $"{key}: column {i} was {oldRow[i]}, is now {current[key][i]}.");
                }

                if (oldRow[11] != current[key][11])
                {
                    Assert.Equal("<null>", current[key][11]);                                            /* only ever text -> none */
                    Assert.Contains("only before the window", oldRow[11], StringComparison.Ordinal);
                }
            }
        });
    }

    /// <summary>Hash or merge joins (any join type) whose condition names <paramref name="key"/>.</summary>
    private static int KeyedJoins(JsonElement plan, string key) =>
        Nodes(plan).Count(n => Text(n, "Node Type") is "Hash Join" or "Merge Join"
            && (Text(n, "Hash Cond") + Text(n, "Merge Cond")).Contains(key, StringComparison.Ordinal));

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TheTwinComparison_JoinsItsPeriodsOnPlainEqualities_NotANullSafeJoinFilter(bool queryStats)
    {
        await RunLiveAsync(new[] { queryStats ? QueryStatsTwinSeedSql : QueryStoreTwinSeedSql }, async (connection, ct) =>
        {
            var sql = queryStats ? ViewerDataService.QueryStatsComparisonSql : ViewerDataService.QueryStoreComparisonSql;
            var plan = await ExplainAsync(connection, sql, AddComparisonParameters, ct);
            Assert.DoesNotContain(Nodes(plan), n => Text(n, "Join Filter").Contains("DISTINCT FROM", StringComparison.Ordinal));
            var keyedJoins = KeyedJoins(plan, "query_hash");
            Assert.True(keyedJoins >= 2, $"expected both period joins as hash or merge joins on query_hash, found {keyedJoins}.");

            /* the oracle's own shape, so the assertions above cannot pass vacuously on this seed. */
            var oldPlan = await ExplainAsync(connection, queryStats ? OldQueryStatsComparisonSql : OldQueryStoreComparisonSql, AddComparisonParameters, ct);
            Assert.Contains(Nodes(oldPlan), n => Text(n, "Join Filter").Contains("DISTINCT FROM", StringComparison.Ordinal));
        });
    }

    [Theory]
    [InlineData(true, false)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    public async Task TheTwinComparison_AnswersTheOldStatementsAnswer_OnEveryColumn_NullKeysIncluded(bool queryStats, bool filtered)
    {
        await RunLiveAsync(new[] { queryStats ? QueryStatsTwinSeedSql : QueryStoreTwinSeedSql }, async (connection, ct) =>
        {
            var filter = filtered ? DatabaseFilter.One("db1") : DatabaseFilter.All;
            void Bind(NpgsqlCommand c) => BindComparison(c, BaselineStart, CurrentStart, filter);
            var old = await RunKeyedAsync(connection, queryStats ? OldQueryStatsComparisonSql : OldQueryStoreComparisonSql, Bind, 2, ct);
            var current = await RunKeyedAsync(connection, queryStats ? ViewerDataService.QueryStatsComparisonSql : ViewerDataService.QueryStoreComparisonSql, Bind, 2, ct);

            /* the seed has to hold what the facts claim, or equality proves nothing. */
            Assert.True(old.Count >= (filtered ? 20 : 100), $"the old statement returned {old.Count} rows.");
            Assert.Contains(old.Values, r => r[3] == "<null>" && r[7] != "<null>");                       /* GONE */
            Assert.Contains(old.Values, r => r[3] != "<null>" && r[7] == "<null>");                       /* NEW */
            if (!filtered)
            {
                Assert.Contains(old.Keys, k => k.StartsWith("<null>|", StringComparison.Ordinal));        /* a NULL database */
                Assert.Contains(old.Keys, k => k.EndsWith("|<null>", StringComparison.Ordinal));          /* a NULL hash */
                Assert.Contains(old.Keys, k => k == "<null>|<null>");                                     /* both NULL */
                /* an empty key and a NULL one that must stay apart, both in the seed's tables (so the lookup has the temptation) and one
                   of each pair in the answer: the heavy NULL-hash group and the heavy empty database are compared, the light empty
                   hash and the light NULL database are trimmed, and a join that merged them would fan a heavy row out. */
                var (table, emptyHashDb, emptyDbHash) = queryStats ? ("collect.query_stats", "db0", "0xQ127") : ("collect.query_store_stats", "db2", "h127");
                Assert.Contains(old.Keys, k => k == $"{emptyHashDb}|<null>");
                Assert.Contains(old.Keys, k => k == $"|{emptyDbHash}");
                Assert.DoesNotContain(old.Keys, k => k == $"{emptyHashDb}|");
                Assert.DoesNotContain(old.Keys, k => k == $"<null>|{emptyDbHash}");
                await using var seeded = new NpgsqlCommand(
                    $"SELECT count(*) FROM {table} WHERE (database_name = '{emptyHashDb}' AND query_hash = '') OR (database_name IS NULL AND query_hash = '{emptyDbHash}')", connection);
                Assert.True((long)(await seeded.ExecuteScalarAsync(ct))! > 0, "the empty-versus-NULL seed rows are missing.");
            }

            Assert.Equal(old.Keys, current.Keys);
            foreach (var (key, oldRow) in old)
            {
                for (var i = 0; i < oldRow.Length; i++)
                {
                    Assert.True(oldRow[i] == current[key][i], $"{key}: column {i} was {oldRow[i]}, is now {current[key][i]}.");
                }
            }
        });
    }
}
