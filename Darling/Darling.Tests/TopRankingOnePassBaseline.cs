/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// The ONE-PASS raw top-N statements as they stood before #5226's two-pass restructure (the reader at commit
/// d0a486829), kept as the oracle <see cref="TopRankingDifferentialLiveTests"/> compares the shipped reader against.
/// The statement bodies are copied verbatim, comments included, with exactly one edit: the interpolated
/// <c>TimescaleSupport.IntervalHonestSourceFilter</c> is written out as its value, so a later change to that constant
/// cannot move the oracle. They are NOT the shipped statements and must not be edited to track them.
///
/// <para><b>The one deliberate difference.</b> The old statements ranked with a bare <c>SUM(x) DESC</c>, which sorts NULL
/// first in PostgreSQL and leaves ties in plan order. The two-pass work changed that on purpose: every deciding
/// <c>ORDER BY</c> is now <c>DESC NULLS LAST</c> on the ranking metric and then on CPU, ending in the group key.
/// <see cref="Expand"/> applies that same ordering to the old text (and expands the ranking the way the old
/// ranking swap did), so the comparison isolates the restructure and nothing else: a different set of rows, a
/// dropped group or a different totals row is a failure, and so is a different tie order.</para>
/// </summary>
internal static class TopRankingOnePassBaseline
{
    /// <summary>The metric the ranking sorts on: the raw-table sum and the page-level output column.</summary>
    private static (string Sum, string Output) Metric(TopRanking ranking) => ranking switch
    {
        TopRanking.Duration => ("SUM(delta_elapsed_time)", "r.total_elapsed_us"),
        TopRanking.Reads => ("SUM(delta_logical_reads)", "r.total_reads"),
        TopRanking.Executions => ("SUM(delta_execution_count)", "r.total_executions"),
        _ => ("SUM(delta_worker_time)", "r.total_cpu_us"),
    };

    /// <summary>
    /// The old statement for a ranking, with the old inner ORDER BY and the old outer ORDER BY (the procedures statement
    /// has no outer one) rewritten to the total order described on the class. <paramref name="innerKey"/> and
    /// <paramref name="outerKey"/> are the group key columns that end each ORDER BY.
    /// </summary>
    public static string Expand(string oldSql, TopRanking ranking, string innerKey, string? outerKey)
    {
        var (sum, output) = Metric(ranking);
        var sql = ReplaceOnce(oldSql, "ORDER BY SUM(delta_worker_time) DESC",
            $"ORDER BY {sum} DESC NULLS LAST, SUM(delta_worker_time) DESC NULLS LAST, {innerKey}");
        if (outerKey is not null)
        {
            sql = ReplaceOnce(sql, "ORDER BY r.total_cpu_us DESC",
                $"ORDER BY {output} DESC NULLS LAST, r.total_cpu_us DESC NULLS LAST, {outerKey}");
        }

        return sql;
    }

    private static string ReplaceOnce(string sql, string find, string replacement)
    {
        var at = sql.IndexOf(find, StringComparison.Ordinal);
        if (at < 0 || sql.IndexOf(find, at + find.Length, StringComparison.Ordinal) >= 0)
        {
            throw new InvalidOperationException($"Expected exactly one '{find}' in the one-pass baseline text.");
        }

        return string.Concat(sql.AsSpan(0, at), replacement, sql.AsSpan(at + find.Length));
    }

    /// <summary>The old <c>TopQueriesSql</c>, the per-hash grouping, for a ranking.</summary>
    public static string Queries(TopRanking ranking) => Expand(OneQueriesSql, ranking,
        "database_name, query_hash, host_object_name", "r.database_name, r.query_hash, r.host_object_name");

    /// <summary>The old <c>TopQueriesByHostObjectSql</c>, the host-object roll-up, for a ranking.</summary>
    public static string QueriesByHostObject(TopRanking ranking) => Expand(OneHostObjectSql, ranking,
        "database_name, host_object_name, CASE WHEN host_object_name IS NULL THEN query_hash END",
        "r.database_name, r.host_object_name, r.query_hash");

    /// <summary>The old <c>TopProceduresSql</c> for a ranking.</summary>
    public static string Procedures(TopRanking ranking) => Expand(OneProceduresSql, ranking,
        "database_name, schema_name, object_name, object_type", null);

    /// <summary>Old <c>TopQueriesSql</c> at d0a486829, the CPU-ranked text.</summary>
    private const string OneQueriesSql = """
        WITH ranked AS (
            SELECT
                database_name,
                query_hash,
                host_object_name,
                CAST(SUM(delta_execution_count) AS bigint) AS total_executions,
                CAST(SUM(delta_worker_time) AS bigint) AS total_cpu_us,
                CAST(SUM(delta_elapsed_time) AS bigint) AS total_elapsed_us,
                CAST(SUM(delta_logical_reads) AS bigint) AS total_reads,
                CAST(SUM(delta_logical_writes) AS bigint) AS total_writes,
                CAST(SUM(delta_physical_reads) AS bigint) AS total_physical_reads,
                CAST(SUM(delta_rows) AS bigint) AS total_rows,
                CAST(SUM(delta_spills) AS bigint) AS total_spills,
                MIN(min_dop) AS min_dop,
                MAX(max_dop) AS max_dop,
                MIN(min_worker_time) AS min_worker_time,
                MAX(max_worker_time) AS max_worker_time,
                MIN(min_elapsed_time) AS min_elapsed_time,
                MAX(max_elapsed_time) AS max_elapsed_time,
                MAX(query_plan_hash) AS query_plan_hash,
                MAX(sql_handle) AS sql_handle,
                MAX(plan_handle) AS plan_handle,
                MAX(last_execution_time) AS last_execution_time,
                MAX(creation_time) AS creation_time,
                MIN(min_physical_reads) AS min_physical_reads,
                MAX(max_physical_reads) AS max_physical_reads,
                MIN(min_rows) AS min_rows,
                MAX(max_rows) AS max_rows,
                MIN(min_grant_kb) AS min_grant_kb,
                MAX(max_grant_kb) AS max_grant_kb,
                MIN(min_used_grant_kb) AS min_used_grant_kb,
                MAX(max_used_grant_kb) AS max_used_grant_kb,
                MIN(min_ideal_grant_kb) AS min_ideal_grant_kb,
                MAX(max_ideal_grant_kb) AS max_ideal_grant_kb,
                MIN(min_spills) AS min_spills,
                MAX(max_spills) AS max_spills,
                MIN(min_reserved_threads) AS min_reserved_threads,
                MAX(max_reserved_threads) AS max_reserved_threads,
                MIN(min_used_threads) AS min_used_threads,
                MAX(max_used_threads) AS max_used_threads,
                MAX(total_clr_time) AS total_clr_time,
                MAX(plan_generation_num) AS plan_generation_num,
                MAX(CAST(delta_worker_time AS double precision) / NULLIF(sample_interval_seconds, 0) / 1000.0) AS worker_time_per_second,
                /* #2012: how many DISTINCT statement texts this hash group merged. query_hash is a
                   SHAPE hash — INSERT...EXEC statements naming DIFFERENT callee procs share one
                   (reproduced live), and ad-hoc literal variants collapse too — so a group with
                   distinct_texts > 1 is a BLEND whose representative text below is one member, not
                   the statement. Counted over the #1767 content digest already on every row (~free);
                   COUNT(DISTINCT) skips NULLs, so 0 means only pre-dimension legacy rows, which age
                   out with raw retention. */
                COUNT(DISTINCT query_text_digest) AS distinct_texts
            FROM query_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   ($5::text IS NULL OR database_name = $5)
            /* #4394: excludes zero-interval rows (sample_interval_seconds = 0) through
               TimescaleSupport.IntervalHonestSourceFilter, the same filter the hourly successors
               bake into their CREATE, so a raw-served and an hourly-served read of the same window
               agree by construction. The collector writes a zero-interval row with zero deltas
               (CollectorDeltaCalculator's first-sighting, reset and gap cases), so this changes no
               total in practice. It keeps the two tiers from disagreeing if that ever stops holding. */
            AND   sample_interval_seconds IS DISTINCT FROM 0
            /* #2012 stage 2: host_object_name splits INSERT...EXEC callers that share a query_hash
               (each proc-hosted statement groups under its own host object), while ad-hoc rows carry
               NULL and keep collapsing into one group per hash exactly as before. */
            GROUP BY database_name, query_hash, host_object_name
            HAVING (SUM(delta_execution_count) > 0 OR SUM(delta_elapsed_time) > 0)
            /* #3541 A13: the parallelism filter is part of the QUERY, applied to the grouped population
               BEFORE the CPU ranking and the cap. It used to run in C# over the returned top-N page, so
               parallel_only=true on a box whose twenty hottest plans were serial answered an empty page while
               the window held parallel plans further down — and the engine's own CXPACKET advice sends agents
               to exactly that call. $6 is the group's lifetime max_dop floor: 0 admits every group (the
               unfiltered read, byte-identical in result to before), 2 is parallel_only, min_dop is itself. The
               COALESCE keeps a group whose max_dop was never captured (NULL) out of a filtered page, which is
               what the C# arm did too (null read as 0, and 0 > 1 is false). */
            AND COALESCE(MAX(max_dop), 0) >= $6
            ORDER BY SUM(delta_worker_time) DESC
            LIMIT $4 + 5
        )
        SELECT
            r.database_name,
            r.query_hash,
            r.host_object_name,
            r.query_plan_hash,
            r.sql_handle,
            r.plan_handle,
            r.total_executions,
            r.total_cpu_us,
            r.total_elapsed_us,
            r.total_reads,
            r.total_writes,
            r.total_physical_reads,
            r.total_rows,
            r.total_spills,
            r.min_dop,
            r.max_dop,
            r.min_worker_time,
            r.max_worker_time,
            r.min_elapsed_time,
            r.max_elapsed_time,
            t.query_text,
            r.distinct_texts,
            CAST(1 AS bigint) AS distinct_query_hashes,
            r.last_execution_time,
            r.creation_time,
            r.min_physical_reads,
            r.max_physical_reads,
            r.min_rows,
            r.max_rows,
            r.min_grant_kb,
            r.max_grant_kb,
            r.min_used_grant_kb,
            r.max_used_grant_kb,
            r.min_ideal_grant_kb,
            r.max_ideal_grant_kb,
            r.min_spills,
            r.max_spills,
            r.min_reserved_threads,
            r.max_reserved_threads,
            r.min_used_threads,
            r.max_used_threads,
            r.total_clr_time,
            r.plan_generation_num,
            r.worker_time_per_second
        FROM ranked AS r
        LEFT JOIN LATERAL (
            SELECT query_text
            FROM v_query_stats
            WHERE server_id = $1
            AND   query_hash = r.query_hash
            AND   database_name = r.database_name
            /* #2012 stage 2: the representative text must come from THIS group's own rows — before
               this, the lookup could serve one caller's text for another caller's stats, which is
               the exact mis-attribution the issue documents from live triage. */
            AND   host_object_name IS NOT DISTINCT FROM r.host_object_name
            AND   query_text IS NOT NULL
            ORDER BY collection_time DESC
            LIMIT 1
        ) AS t ON TRUE
        WHERE t.query_text IS NULL OR t.query_text NOT LIKE 'WAITFOR%'
        ORDER BY r.total_cpu_us DESC
        LIMIT $4
        """;

    /// <summary>Old <c>TopQueriesByHostObjectSql</c> at d0a486829, the CPU-ranked text.</summary>
    private const string OneHostObjectSql = """
        WITH ranked AS (
            SELECT
                database_name,
                MAX(query_hash) AS query_hash,
                host_object_name,
                CAST(SUM(delta_execution_count) AS bigint) AS total_executions,
                CAST(SUM(delta_worker_time) AS bigint) AS total_cpu_us,
                CAST(SUM(delta_elapsed_time) AS bigint) AS total_elapsed_us,
                CAST(SUM(delta_logical_reads) AS bigint) AS total_reads,
                CAST(SUM(delta_logical_writes) AS bigint) AS total_writes,
                CAST(SUM(delta_physical_reads) AS bigint) AS total_physical_reads,
                CAST(SUM(delta_rows) AS bigint) AS total_rows,
                CAST(SUM(delta_spills) AS bigint) AS total_spills,
                MIN(min_dop) AS min_dop,
                MAX(max_dop) AS max_dop,
                MIN(min_worker_time) AS min_worker_time,
                MAX(max_worker_time) AS max_worker_time,
                MIN(min_elapsed_time) AS min_elapsed_time,
                MAX(max_elapsed_time) AS max_elapsed_time,
                MAX(query_plan_hash) AS query_plan_hash,
                MAX(sql_handle) AS sql_handle,
                MAX(plan_handle) AS plan_handle,
                MAX(last_execution_time) AS last_execution_time,
                MAX(creation_time) AS creation_time,
                MIN(min_physical_reads) AS min_physical_reads,
                MAX(max_physical_reads) AS max_physical_reads,
                MIN(min_rows) AS min_rows,
                MAX(max_rows) AS max_rows,
                MIN(min_grant_kb) AS min_grant_kb,
                MAX(max_grant_kb) AS max_grant_kb,
                MIN(min_used_grant_kb) AS min_used_grant_kb,
                MAX(max_used_grant_kb) AS max_used_grant_kb,
                MIN(min_ideal_grant_kb) AS min_ideal_grant_kb,
                MAX(max_ideal_grant_kb) AS max_ideal_grant_kb,
                MIN(min_spills) AS min_spills,
                MAX(max_spills) AS max_spills,
                MIN(min_reserved_threads) AS min_reserved_threads,
                MAX(max_reserved_threads) AS max_reserved_threads,
                MIN(min_used_threads) AS min_used_threads,
                MAX(max_used_threads) AS max_used_threads,
                MAX(total_clr_time) AS total_clr_time,
                MAX(plan_generation_num) AS plan_generation_num,
                MAX(CAST(delta_worker_time AS double precision) / NULLIF(sample_interval_seconds, 0) / 1000.0) AS worker_time_per_second,
                COUNT(DISTINCT query_text_digest) AS distinct_texts,
                /* #2235: the fragment count IS the finding — 21 here is why a per-hash ranking missed it. */
                COUNT(DISTINCT query_hash) AS distinct_query_hashes
            FROM query_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   ($5::text IS NULL OR database_name = $5)
            /* #4394: same first-collection exclusion as TopQueriesSql — see its note. */
            AND   sample_interval_seconds IS DISTINCT FROM 0
            /* #2235: proc-hosted rows collapse to one row per (database, host object) — every literal
               fragment of one statement lands together. Ad-hoc rows (host_object_name NULL) fall to the
               CASE and stay keyed on their OWN query_hash, so they group exactly as the default read does;
               without that arm every unrelated ad-hoc statement in a database would pool into one row. */
            GROUP BY database_name, host_object_name,
                     CASE WHEN host_object_name IS NULL THEN query_hash END
            HAVING (SUM(delta_execution_count) > 0 OR SUM(delta_elapsed_time) > 0)
            /* #3541 A13: same in-query parallelism floor as TopQueriesSql — see its note. Under the rollup
               the group's max_dop is the max across every fragment, so a procedure whose dynamic SQL went
               parallel in ANY fragment passes parallel_only, which is the question being asked. */
            AND COALESCE(MAX(max_dop), 0) >= $6
            ORDER BY SUM(delta_worker_time) DESC
            LIMIT $4 + 5
        )
        SELECT
            r.database_name,
            r.query_hash,
            r.host_object_name,
            r.query_plan_hash,
            r.sql_handle,
            r.plan_handle,
            r.total_executions,
            r.total_cpu_us,
            r.total_elapsed_us,
            r.total_reads,
            r.total_writes,
            r.total_physical_reads,
            r.total_rows,
            r.total_spills,
            r.min_dop,
            r.max_dop,
            r.min_worker_time,
            r.max_worker_time,
            r.min_elapsed_time,
            r.max_elapsed_time,
            t.query_text,
            r.distinct_texts,
            r.distinct_query_hashes,
            r.last_execution_time,
            r.creation_time,
            r.min_physical_reads,
            r.max_physical_reads,
            r.min_rows,
            r.max_rows,
            r.min_grant_kb,
            r.max_grant_kb,
            r.min_used_grant_kb,
            r.max_used_grant_kb,
            r.min_ideal_grant_kb,
            r.max_ideal_grant_kb,
            r.min_spills,
            r.max_spills,
            r.min_reserved_threads,
            r.max_reserved_threads,
            r.min_used_threads,
            r.max_used_threads,
            r.total_clr_time,
            r.plan_generation_num,
            r.worker_time_per_second
        FROM ranked AS r
        LEFT JOIN LATERAL (
            SELECT query_text
            FROM v_query_stats
            WHERE server_id = $1
            AND   database_name = r.database_name
            /* Mirrors the grouping: for a rolled-up proc any of its fragments' texts is a valid
               representative, but an ad-hoc row must still match its own hash or the text could come from
               an unrelated statement. */
            AND   host_object_name IS NOT DISTINCT FROM r.host_object_name
            AND   (r.host_object_name IS NOT NULL OR query_hash = r.query_hash)
            AND   query_text IS NOT NULL
            ORDER BY collection_time DESC
            LIMIT 1
        ) AS t ON TRUE
        WHERE t.query_text IS NULL OR t.query_text NOT LIKE 'WAITFOR%'
        ORDER BY r.total_cpu_us DESC
        LIMIT $4
        """;

    /// <summary>Old <c>TopProceduresSql</c> at d0a486829, the CPU-ranked text.</summary>
    private const string OneProceduresSql = """
        SELECT
            database_name,
            schema_name,
            object_name,
            object_type,
            MAX(sql_handle) AS sql_handle,
            MAX(plan_handle) AS plan_handle,
            CAST(SUM(delta_execution_count) AS bigint) AS total_executions,
            CAST(SUM(delta_worker_time) AS bigint) AS total_cpu_us,
            CAST(SUM(delta_elapsed_time) AS bigint) AS total_elapsed_us,
            CAST(SUM(delta_logical_reads) AS bigint) AS total_reads,
            CAST(SUM(delta_logical_writes) AS bigint) AS total_writes,
            CAST(SUM(delta_physical_reads) AS bigint) AS total_physical_reads,
            CAST(SUM(delta_spills) AS bigint) AS total_spills,
            MIN(min_worker_time) AS min_worker_time,
            MAX(max_worker_time) AS max_worker_time,
            MIN(min_elapsed_time) AS min_elapsed_time,
            MAX(max_elapsed_time) AS max_elapsed_time,
            MAX(last_execution_time) AS last_execution_time,
            MAX(cached_time) AS cached_time,
            MIN(min_logical_reads) AS min_logical_reads,
            MAX(max_logical_reads) AS max_logical_reads,
            MIN(min_physical_reads) AS min_physical_reads,
            MAX(max_physical_reads) AS max_physical_reads,
            MIN(min_logical_writes) AS min_logical_writes,
            MAX(max_logical_writes) AS max_logical_writes,
            MIN(min_spills) AS min_spills,
            MAX(max_spills) AS max_spills
        FROM procedure_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   ($5::text IS NULL OR database_name = $5)
        /* #4394: same first-collection exclusion as TopQueriesSql — see its note. */
        AND   sample_interval_seconds IS DISTINCT FROM 0
        GROUP BY database_name, schema_name, object_name, object_type
        HAVING SUM(delta_execution_count) > 0 OR SUM(delta_elapsed_time) > 0
        ORDER BY SUM(delta_worker_time) DESC
        LIMIT $4
        """;
}
