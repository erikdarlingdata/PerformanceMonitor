/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/* #9164 part B: the raw top-queries read, served for the whole hours of its window by the hourly host rollup. */
internal static partial class DarlingDataReader
{
    /// <summary>
    /// #9164: the whole hours <c>[MidStart, MidEnd)</c> a raw top-queries read takes from <c>collect.query_stats_host_hourly</c>. The read is
    /// (start, end] on <c>collection_time</c>, so an hour bucket <c>[h, h + 1h)</c> is whole only when <c>h &gt; start</c> and
    /// <c>h + 1h &lt;= end</c>. Every other row, the part of the window before <c>MidStart</c> and the part from <c>MidEnd</c> through the
    /// end, is read from raw.
    /// </summary>
    internal sealed record HostHourlyMid(DateTime MidStart, DateTime MidEnd);

    /// <summary>The rollup mid of a raw top-queries window, or null to read raw alone: <see cref="QueryStatsHostHourly.PlanMid"/>, which names the rules.</summary>
    internal static HostHourlyMid? PlanTopQueriesHostHourlyMid(DateTime startUtc, DateTime endUtc, DateTime nowUtc) =>
        QueryStatsHostHourly.PlanMid(startUtc, endUtc, nowUtc) is var (midStart, midEnd) ? new HostHourlyMid(midStart, midEnd) : null;

    /// <summary>The raw statement the mid replaces keeps its tail: the representative text, the orphan fallback, the WAITFOR trim, the page order.</summary>
    private const string HostHourlyTailMarker = "latest_text AS MATERIALIZED (";

    /// <summary>#9164: <see cref="TopQueriesSql"/>, ranked and widened from the host-hourly rollup plus the raw edges. A property, not a field: the two raw statements live in the other file of this class, and the order static fields of a partial class run in across files is not defined. Binds $1 to $8 as the raw statement does, then $9 <c>MidStart</c> and $10 <c>MidEnd</c>.</summary>
    public static string TopQueriesHostHourlySql => BuildHostHourlySql(TopQueriesSql, byHostObject: false);

    /// <summary>#9164: <see cref="TopQueriesByHostObjectSql"/>'s host-hourly twin; same parameters as <see cref="TopQueriesHostHourlySql"/>.</summary>
    public static string TopQueriesByHostObjectHostHourlySql => BuildHostHourlySql(TopQueriesByHostObjectSql, byHostObject: true);

    private static string BuildHostHourlySql(string raw, bool byHostObject)
    {
        var tailAt = raw.IndexOf(HostHourlyTailMarker, StringComparison.Ordinal);
        if (tailAt < 0 || tailAt != raw.LastIndexOf(HostHourlyTailMarker, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The raw top-queries statement no longer carries its latest_text step exactly once.");
        }

        var values = QueryStatsHostHourly.ValueColumns;
        /* The peak CPU rate rides through the units under a name of its own: the per-second census wants every column with that suffix to be a
           quotient where it is named, and here it is carried, not computed (the raw read's quotient is in the edge rows' aggregate). */
        static string Unit(string name) => name == "worker_time_per_second" ? "peak_cpu_ms_per_second" : name;
        var edgeSelect = string.Join(",\n                ", values.Select(c => $"CAST(({c.Aggregate}) AS {c.Type}) AS {Unit(c.Name)}"));
        var unitColumns = string.Join(", ", values.Select(c => c.Name == Unit(c.Name) ? c.Name : $"{c.Name} AS {Unit(c.Name)}"));

        /* The key the raw statement groups by, and the winners' key columns it ranks on. */
        var groupBy = byHostObject
            ? "database_name, host_object_name, CASE WHEN host_object_name IS NULL THEN query_hash END"
            : "database_name, query_hash, host_object_name";
        var winSelect = byHostObject
            ? "database_name AS win_database_name, host_object_name AS win_host_object_name, CASE WHEN host_object_name IS NULL THEN query_hash END AS win_group_hash"
            : "database_name AS win_database_name, query_hash AS win_query_hash, host_object_name AS win_host_object_name";
        var winOrder = byHostObject
            ? "win_database_name, win_host_object_name, win_group_hash"
            : "win_database_name, win_query_hash, win_host_object_name";
        var matchWinner = byHostObject
            ? @"COALESCE(u.host_object_name, '') = COALESCE(w.win_host_object_name, '')
                AND COALESCE(CASE WHEN u.host_object_name IS NULL THEN u.query_hash END, '') = COALESCE(w.win_group_hash, '')
                AND u.database_name IS NOT DISTINCT FROM w.win_database_name
                AND u.host_object_name IS NOT DISTINCT FROM w.win_host_object_name
                AND CASE WHEN u.host_object_name IS NULL THEN u.query_hash END IS NOT DISTINCT FROM w.win_group_hash"
            : @"COALESCE(u.query_hash, '') = COALESCE(w.win_query_hash, '')
                AND u.database_name IS NOT DISTINCT FROM w.win_database_name
                AND u.query_hash IS NOT DISTINCT FROM w.win_query_hash
                AND u.host_object_name IS NOT DISTINCT FROM w.win_host_object_name";
        var restartPick = byHostObject
            ? "host_object_name IN (SELECT win_host_object_name FROM winners) OR query_hash IN (SELECT win_group_hash FROM winners) OR (query_hash IS NULL AND EXISTS (SELECT 1 FROM winners WHERE win_group_hash IS NULL AND win_host_object_name IS NULL))"
            : "query_hash IN (SELECT win_query_hash FROM winners) OR (query_hash IS NULL AND EXISTS (SELECT 1 FROM winners WHERE win_query_hash IS NULL))";
        var matchText = byHostObject
            ? @"g.database_name IS NOT DISTINCT FROM t.database_name
                AND g.host_object_name IS NOT DISTINCT FROM t.host_object_name
                AND g.group_hash IS NOT DISTINCT FROM t.group_hash"
            : @"g.database_name IS NOT DISTINCT FROM t.database_name
                AND g.query_hash IS NOT DISTINCT FROM t.query_hash
                AND g.host_object_name IS NOT DISTINCT FROM t.host_object_name";

        /* One unit per (database, hash, host object) and hour, in the rollup's own shape: from the rollup for the whole hours, and
           from raw for the edges, aggregated by the very expressions the tick builds an hour with. So the statement below only has to
           add the units: sums add, minimums and maximums take the minimum and maximum, and a zero-interval-only unit contributes its
           NULL measures and its observations (newest execution, newest text) exactly as its raw rows did. */
        var unitFilters = $@"
              AND   ($5::text[] IS NULL OR database_name = ANY($5))
             {DatabaseFilter.ExcludeClause("database_name", 8)}
              /*TAG:hash:query_hash*/";

        var head = $@"WITH units AS MATERIALIZED (
            /* #9164: the whole hours of the window, from the hourly host rollup. */
            SELECT database_name, query_hash, host_object_name, {unitColumns}
            FROM collect.query_stats_host_hourly
            WHERE server_id = $1
            AND   bucket >= $9
            AND   bucket < $10{unitFilters}
            UNION ALL
            /* The edges: (start, MidStart) and [MidEnd, end] of the raw table. */
            SELECT database_name, query_hash, host_object_name,
                {string.Join(",\n                ", values.Select(c => $"CAST(({c.Aggregate}) AS {c.Type}) AS {Unit(c.Name)}"))}
            FROM query_stats
            WHERE server_id = $1
            AND   ((collection_time > $2 AND collection_time < $9) OR (collection_time >= $10 AND collection_time <= $3)){unitFilters}
            GROUP BY database_name, query_hash, host_object_name
        ),
        winners AS MATERIALIZED (
            /* Pass 1 of the raw statement over the units: the same groups, the same two HAVING predicates, the same parallelism floor
               and the same total order, summed from the honest-row sums the units carry. */
            SELECT
                {winSelect},
                $RANK$ AS rank_metric,
                SUM(total_cpu_us) AS rank_cpu
            FROM units
            GROUP BY {groupBy}
            HAVING (SUM(total_executions) > 0 OR SUM(total_elapsed_us) > 0)
            AND COALESCE(MAX(max_dop), 0) >= $6
            ORDER BY rank_metric DESC NULLS LAST, rank_cpu DESC NULLS LAST, {winOrder}
            LIMIT $7
        ),
        restart_units AS MATERIALIZED (
            /* The hour ledger counts only interval-honest rows, so a late batch of restart rows (interval 0) leaves a built hour 'complete'
               while raw's answer moves: such rows take no part in a sum, but they can hold the newest execution time and the newest text of
               their group. The winners' restart rows of the whole hours are read from raw (restart rows are rare), so those two columns
               are the raw answer whatever the build saw. The same unit shape, so the join below treats them as any other unit. */
            SELECT database_name, query_hash, host_object_name,
                {edgeSelect}
            FROM query_stats
            WHERE server_id = $1
            AND   collection_time >= $9
            AND   collection_time < $10
            AND   sample_interval_seconds = 0
            AND   ({restartPick}){unitFilters}
            GROUP BY database_name, query_hash, host_object_name
        ),
        wide_units AS MATERIALIZED (
            SELECT u.*, w.rank_metric, w.rank_cpu
            FROM (SELECT * FROM units UNION ALL SELECT * FROM restart_units) AS u
            JOIN winners AS w
                ON {matchWinner}
        ),
        wide_texts AS (
            /* The distinct text count is the count of distinct digests across the hours' digest arrays and the edge rows' digests. */
            SELECT u.database_name, u.host_object_name,
                   {(byHostObject ? "CASE WHEN u.host_object_name IS NULL THEN u.query_hash END AS group_hash" : "u.query_hash AS query_hash")},
                   COUNT(DISTINCT d.digest) AS distinct_texts
            FROM wide_units AS u
            CROSS JOIN LATERAL unnest(u.digests) AS d(digest)
            GROUP BY u.database_name, u.host_object_name, {(byHostObject ? "CASE WHEN u.host_object_name IS NULL THEN u.query_hash END" : "u.query_hash")}
        ),
        ranked AS (
            /* Pass 2 of the raw statement over the winners' units. */
            SELECT g.*, COALESCE(t.distinct_texts, 0) AS distinct_texts
            FROM (
                SELECT
                    u.database_name,
                    {(byHostObject ? "MAX(u.query_hash) FILTER (WHERE u.raw_rows > 0) AS query_hash" : "u.query_hash")},
                    u.host_object_name,
                    {(byHostObject ? "CASE WHEN u.host_object_name IS NULL THEN u.query_hash END AS group_hash," : "")}
                    CAST(SUM(u.total_executions) AS bigint) AS total_executions,
                    CAST(SUM(u.total_cpu_us) AS bigint) AS total_cpu_us,
                    CAST(SUM(u.total_elapsed_us) AS bigint) AS total_elapsed_us,
                    CAST(SUM(u.total_reads) AS bigint) AS total_reads,
                    CAST(SUM(u.total_writes) AS bigint) AS total_writes,
                    CAST(SUM(u.total_physical_reads) AS bigint) AS total_physical_reads,
                    CAST(SUM(u.total_rows) AS bigint) AS total_rows,
                    CAST(SUM(u.total_spills) AS bigint) AS total_spills,
                    MIN(u.min_dop) AS min_dop,
                    MAX(u.max_dop) AS max_dop,
                    MIN(u.min_worker_time) AS min_worker_time,
                    MAX(u.max_worker_time) AS max_worker_time,
                    MIN(u.min_elapsed_time) AS min_elapsed_time,
                    MAX(u.max_elapsed_time) AS max_elapsed_time,
                    MAX(u.query_plan_hash) AS query_plan_hash,
                    MAX(u.sql_handle) AS sql_handle,
                    MAX(u.plan_handle) AS plan_handle,
                    MAX(u.last_execution_time) AS last_execution_time,
                    MAX(u.creation_time) AS creation_time,
                    MIN(u.min_physical_reads) AS min_physical_reads,
                    MAX(u.max_physical_reads) AS max_physical_reads,
                    MIN(u.min_rows) AS min_rows,
                    MAX(u.max_rows) AS max_rows,
                    MIN(u.min_grant_kb) AS min_grant_kb,
                    MAX(u.max_grant_kb) AS max_grant_kb,
                    MIN(u.min_used_grant_kb) AS min_used_grant_kb,
                    MAX(u.max_used_grant_kb) AS max_used_grant_kb,
                    MIN(u.min_ideal_grant_kb) AS min_ideal_grant_kb,
                    MAX(u.max_ideal_grant_kb) AS max_ideal_grant_kb,
                    MIN(u.min_spills) AS min_spills,
                    MAX(u.max_spills) AS max_spills,
                    MIN(u.min_reserved_threads) AS min_reserved_threads,
                    MAX(u.max_reserved_threads) AS max_reserved_threads,
                    MIN(u.min_used_threads) AS min_used_threads,
                    MAX(u.max_used_threads) AS max_used_threads,
                    MAX(u.total_clr_time) AS total_clr_time,
                    MAX(u.plan_generation_num) AS plan_generation_num,
                    MAX(u.peak_cpu_ms_per_second) AS worker_time_per_second,
                    {(byHostObject ? "COUNT(DISTINCT u.query_hash) FILTER (WHERE u.raw_rows > 0) AS distinct_query_hashes," : "")}
                    {ByteaMaxSql.Of("u.latest_text_key")} AS latest_text_key,
                    MAX(u.rank_metric) AS rank_metric,
                    MAX(u.rank_cpu) AS rank_cpu
                FROM wide_units AS u
                GROUP BY {(byHostObject ? "u.database_name, u.host_object_name, CASE WHEN u.host_object_name IS NULL THEN u.query_hash END" : "u.database_name, u.query_hash, u.host_object_name")}
            ) AS g
            LEFT JOIN wide_texts AS t
                ON {matchText}
        ),
        ";

        return head + raw[tailAt..];
    }

    /// <summary>
    /// #9164: reads a raw top-queries window through the hourly host rollup, or returns null when the read must run the raw statement:
    /// a store without the rollup (below V287), an hour of the mid that the rollup does not hold whole anywhere but at the end, or an end that
    /// shrinks the mid below <see cref="QueryStatsHostHourly.MinMid"/> (<see cref="QueryStatsHostHourly.UsableMidEndAsync"/>). Hours at the end of the mid the
    /// tick has not built yet move to the raw edge: the mid ends at the last built hour. The coverage check and the statement run in one repeatable-read
    /// transaction, so a build or a late row between them cannot change what the check answered.
    /// </summary>
    private static async Task<List<TopQueryRow>?> TryReadTopQueriesHostHourlyAsync(
        NpgsqlDataSource postgres, HostHourlyMid mid, string sql, int serverId, DateTime startUtc, DateTime endUtc, int top, DatabaseFilter databases,
        int minMaxDop, ServerClock clock, CancellationToken cancellationToken)
    {
        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead, cancellationToken);
        /* The newest closed hours may not be built yet (the tick builds an hour on its first hourly run after the grace): those move to
           the raw edge. Any other hole or stale hour, or a store without the rollup, sends the whole read to raw. */
        if (await QueryStatsHostHourly.UsableMidEndAsync(connection, transaction, serverId, mid.MidStart, mid.MidEnd, McpCommandDeadlines.ReadSeconds, cancellationToken) is not DateTime midEnd)
        {
            return null;
        }

        mid = mid with { MidEnd = midEnd };

        /* #5313: the WAITFOR trim can leave the page short; TopFill asks again with a larger candidate limit ($7), as the raw read does. */
        return await TopFill.RunAsync(top, async candidates =>
        {
            await using var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = McpCommandDeadlines.ReadSeconds };
            BindTopQueriesParameters(command, serverId, startUtc, endUtc, top, databases, minMaxDop, candidates);
            AddTimestamp(command, mid.MidStart);
            AddTimestamp(command, mid.MidEnd);
            return await ReadTopQueryPageAsync(command, clock, cancellationToken);
        });
    }
}
