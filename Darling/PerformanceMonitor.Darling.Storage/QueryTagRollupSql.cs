/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The resource-use-by-tag roll-up SQL (#5983, wave 2 of #5971), one statement per source. It lives in the storage layer so the
/// MCP read <c>get_resource_use_by_tag</c> and the Darling viewer's by-tag view (#5988) run the same text. Every statement joins
/// the materialised map <c>config.query_tag_map</c> (the map holds one tag per key, so a fact lands on at most one tag) and
/// returns <c>server_id</c>, <c>tag_id</c> (NULL = no map row, the "untagged" bucket) and then the section's measures in the
/// order <see cref="QueryTagRollupSection"/> lists them. Nothing evaluates a rule here: the map is the only thing joined.
///
/// <para>Parameters: $1 server ids (<c>integer[]</c>), $2 window start and $3 window end (naive UTC <c>timestamp</c>), except <see cref="QueryStoreSql"/>, which binds a start per server. The hourly
/// statements also take $4, the rollup's materialisation ceiling, through <see cref="CeilingPlaceholder"/>.</para>
/// <para>The sections are never added together: the plan cache and Query Store count the same executions, and procedures overlap
/// both. A caller key (blocking and deadlock rows) is <c>application, login, host, database</c> joined by <c>U+001F</c>, the key
/// the collectors' sighting hook builds (#5980); an object key is <c>database.schema.name</c>.</para>
/// </summary>
public static class QueryTagRollupSql
{
    /// <summary>Where the hourly statements carry the ceiling bound; replaced by <see cref="CeilingClause"/> or the empty string.</summary>
    public const string CeilingPlaceholder = "$CEIL$";

    /// <summary>The ceiling bound, with the shape the hourly top-N reads bind.</summary>
    public const string CeilingClause = " AND f.bucket < $4";

    /// <summary>Every tag: id, name, parent. The tree maths (own figure, subtree figure) runs over this in C#.</summary>
    public const string TagTreeSql = "SELECT id, name, parent_id FROM config.query_tags ORDER BY id";

    /// <summary>Plan cache, raw tier: <c>collect.query_stats</c> deltas keyed by <c>query_hash</c>. Every additive statement aliases its measure columns with the section's measure names (unscaled: microseconds are divided in C#), and the reader reads them by name.</summary>
    public static readonly string PlanCacheRawSql = $"""
        SELECT q.server_id, m.tag_id,
               CAST(COALESCE(SUM(q.delta_worker_time), 0) AS double precision) AS cpu_ms,
               CAST(COALESCE(SUM(q.delta_elapsed_time), 0) AS double precision) AS duration_ms,
               CAST(COALESCE(SUM(q.delta_logical_reads), 0) AS double precision) AS logical_reads,
               CAST(COALESCE(SUM(q.delta_logical_writes), 0) AS double precision) AS logical_writes,
               CAST(COALESCE(SUM(q.delta_execution_count), 0) AS double precision) AS executions
        FROM collect.query_stats AS q
        LEFT JOIN config.query_tag_map AS m
               ON m.server_id = q.server_id AND m.key_kind = 'hash' AND m.key = q.query_hash
        WHERE q.server_id = ANY($1)
        AND   q.collection_time >= $2
        AND   q.collection_time <= $3
        AND   q.{TimescaleSupport.IntervalHonestSourceFilter}
        GROUP BY q.server_id, m.tag_id
        """;

    /// <summary>Plan cache, hourly tier: <c>collect.query_stats_io_hourly</c>, the rollup that keeps logical reads and writes.</summary>
    public const string PlanCacheHourlySql = """
        SELECT f.server_id, m.tag_id,
               CAST(COALESCE(SUM(f.worker_time_sum), 0) AS double precision) AS cpu_ms,
               CAST(COALESCE(SUM(f.elapsed_time_sum), 0) AS double precision) AS duration_ms,
               CAST(COALESCE(SUM(f.logical_reads_sum), 0) AS double precision) AS logical_reads,
               CAST(COALESCE(SUM(f.logical_writes_sum), 0) AS double precision) AS logical_writes,
               CAST(COALESCE(SUM(f.execution_count_sum), 0) AS double precision) AS executions
        FROM collect.query_stats_io_hourly AS f
        LEFT JOIN config.query_tag_map AS m
               ON m.server_id = f.server_id AND m.key_kind = 'hash' AND m.key = f.query_hash
        WHERE f.server_id = ANY($1)
        AND   f.bucket >= $2
        AND   f.bucket < $3$CEIL$
        GROUP BY f.server_id, m.tag_id
        """;

    /// <summary>
    /// Query Store: <c>collect.query_store_interval_wide</c> (the latest snapshot per interval), averages weighted by executions. ONE
    /// statement for every server the gate allowed (#6794): it took one statement per server before, run in sequence, 42 of them on a
    /// 42-server store. Each server keeps its own lower bound, the gate's <c>ReadStart</c>, so a server's rows are exactly the rows the
    /// per-server statement returned. Parameters differ from the other statements: $1 the allowed server ids (<c>integer[]</c>), $2 each
    /// server's read start in the same order (<c>timestamp[]</c>), $3 window end, $4 the earliest read start, which repeats the
    /// <c>first_execution_time</c> floor as a constant so the day partitions below it are pruned (the table is partitioned on that column).
    /// <para>The per-server start on <c>collection_time</c> is written <c>collection_time - read_start &gt;= interval '0'</c> on purpose, and
    /// there is no constant <c>collection_time</c> floor: either one lets the planner AND the partition's <c>collection_time</c> index into
    /// every server's scan, and that index is not keyed by server, so each of the 42 loops walked the whole day's entries (about 10 million
    /// per loop). Measured on one production store with 42 servers over 24 hours, warm: 13.0 s with the index, 6.0 s without; each server's
    /// scan is the <c>(server_id, first_execution_time)</c> range instead. The constant upper bound <c>q.collection_time &lt;= $3</c> stays
    /// and is harmless: $3 is the window end, at or near now, so it keeps almost every row of the window's partitions and the planner has no
    /// reason to scan the <c>collection_time</c> index for it. It filters the rows the per-server range already found.</para>
    /// </summary>
    public static readonly string QueryStoreSql = $$"""
        SELECT q.server_id, m.tag_id,
               CAST(COALESCE(SUM(q.avg_cpu_time_us * q.execution_count), 0) AS double precision) AS cpu_ms,
               CAST(COALESCE(SUM(q.avg_duration_us * q.execution_count), 0) AS double precision) AS duration_ms,
               CAST(COALESCE(SUM(q.avg_logical_io_reads * q.execution_count), 0) AS double precision) AS logical_reads,
               CAST(COALESCE(SUM(q.avg_logical_io_writes * q.execution_count), 0) AS double precision) AS logical_writes,
               CAST(COALESCE(SUM(q.execution_count), 0) AS double precision) AS executions,
               CAST(COALESCE(MAX(q.max_query_max_used_memory), 0) AS double precision) AS peak_memory_pages
        FROM unnest($1::integer[], $2::timestamp[]) AS s(server_id, read_start)
        JOIN collect.query_store_interval_wide AS q
               ON q.server_id = s.server_id
              AND q.collection_time - s.read_start >= interval '0'
              AND q.first_execution_time >= s.read_start - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
        LEFT JOIN config.query_tag_map AS m
               ON m.server_id = q.server_id AND m.key_kind = 'hash' AND m.key = q.query_hash
        WHERE q.server_id = ANY($1)
        AND   q.collection_time <= $3
        AND   q.first_execution_time >= $4 - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
        AND   q.first_execution_time <= $3 + {{QueryStoreIntervalWide.FirstExecUpperSlackSql}}
        GROUP BY q.server_id, m.tag_id
        """;

    /// <summary>
    /// The fleet form of <see cref="QueryStoreSql"/> (#6800 part 2, step 2): the same six measures per <c>(server_id, tag_id)</c>, with the
    /// whole hours the hourly builder has done read from <c>collect.query_store_hash_hourly</c> and the rest from the wide table.
    /// <see cref="QueryTagRollupHashRoute"/> decides which is which. Parameters: $1 and $2 the table pairs as parallel arrays (hour start
    /// <c>timestamp[]</c>, server <c>integer[]</c>), $3, $4 and $5 the wide ranges as parallel arrays (server, from, to, each range
    /// <c>[from, to)</c>), $6 the window end.
    /// <para>Both arms yield six partials per <c>(server_id, query_hash)</c> (sums of the weighted products and of the counts, the maximum of
    /// the memory), exact <c>numeric</c> sums, so adding the hours gives the wide statement's total. The tag map is joined once, on the
    /// partials, and the final GROUP BY is by <c>(server_id, tag_id)</c>: the aggregate stays at that size. The COALESCE and the cast to
    /// <c>double precision</c> are today's, applied after the hours are added.</para>
    /// <para>The table arm is one probe of <c>(hour, server_id)</c> per listed pair. The wide arm runs once per distinct <c>[from, to)</c>
    /// (the ranges are grouped, so every server that shares the tail shares one scan of it, not one scan per server): inside each, the
    /// bounds on <c>collection_time</c> are written plainly so a scan by <c>collection_time</c> is possible, which a per-server loop must
    /// not use (<see cref="QueryStoreSql"/> explains the 13.0 s it cost). The <c>first_execution_time</c> floor is the range's own start
    /// less the purge margin, the same floor the builder uses for the hour, and the wide statement's own for the first range.</para>
    /// </summary>
    public static readonly string QueryStoreRoutedSql = $$"""
        WITH wide_groups AS
        (
            SELECT r.from_time, r.to_time, array_agg(r.server_id) AS servers
            FROM unnest($3::integer[], $4::timestamp[], $5::timestamp[]) AS r(server_id, from_time, to_time)
            GROUP BY r.from_time, r.to_time
        ),
        partials AS
        (
            SELECT t.server_id, t.query_hash, t.ec_sum, t.cpu_wsum, t.dur_wsum, t.reads_wsum, t.writes_wsum, t.mem_max
            FROM unnest($1::timestamp[], $2::integer[]) AS p(hour, server_id)
            JOIN collect.query_store_hash_hourly AS t
                   ON t.hour = p.hour
                  AND t.server_id = p.server_id
            UNION ALL
            SELECT w.server_id, w.query_hash, w.ec_sum, w.cpu_wsum, w.dur_wsum, w.reads_wsum, w.writes_wsum, w.mem_max
            FROM wide_groups AS g
            CROSS JOIN LATERAL
            (
                SELECT q.server_id, q.query_hash,
                       SUM(q.execution_count) AS ec_sum,
                       SUM(q.avg_cpu_time_us * q.execution_count) AS cpu_wsum,
                       SUM(q.avg_duration_us * q.execution_count) AS dur_wsum,
                       SUM(q.avg_logical_io_reads * q.execution_count) AS reads_wsum,
                       SUM(q.avg_logical_io_writes * q.execution_count) AS writes_wsum,
                       MAX(q.max_query_max_used_memory) AS mem_max
                FROM collect.query_store_interval_wide AS q
                WHERE q.server_id = ANY(g.servers)
                AND   q.collection_time >= g.from_time
                AND   q.collection_time < g.to_time
                AND   q.collection_time <= $6
                AND   q.first_execution_time >= g.from_time - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
                AND   q.first_execution_time <= $6 + {{QueryStoreIntervalWide.FirstExecUpperSlackSql}}
                GROUP BY q.server_id, q.query_hash
            ) AS w
        )
        SELECT p.server_id, m.tag_id,
               CAST(COALESCE(SUM(p.cpu_wsum), 0) AS double precision) AS cpu_ms,
               CAST(COALESCE(SUM(p.dur_wsum), 0) AS double precision) AS duration_ms,
               CAST(COALESCE(SUM(p.reads_wsum), 0) AS double precision) AS logical_reads,
               CAST(COALESCE(SUM(p.writes_wsum), 0) AS double precision) AS logical_writes,
               CAST(COALESCE(SUM(p.ec_sum), 0) AS double precision) AS executions,
               CAST(COALESCE(MAX(p.mem_max), 0) AS double precision) AS peak_memory_pages
        FROM partials AS p
        LEFT JOIN config.query_tag_map AS m
               ON m.server_id = p.server_id AND m.key_kind = 'hash' AND m.key = p.query_hash
        GROUP BY p.server_id, m.tag_id
        """;

    /// <summary>Procedures, raw tier: <c>collect.procedure_stats</c> deltas keyed by <c>database.schema.name</c>.</summary>
    public static readonly string ProceduresRawSql = $"""
        SELECT q.server_id, m.tag_id,
               CAST(COALESCE(SUM(q.delta_worker_time), 0) AS double precision) AS cpu_ms,
               CAST(COALESCE(SUM(q.delta_elapsed_time), 0) AS double precision) AS duration_ms,
               CAST(COALESCE(SUM(q.delta_logical_reads), 0) AS double precision) AS logical_reads,
               CAST(COALESCE(SUM(q.delta_logical_writes), 0) AS double precision) AS logical_writes,
               CAST(COALESCE(SUM(q.delta_execution_count), 0) AS double precision) AS executions
        FROM collect.procedure_stats AS q
        LEFT JOIN config.query_tag_map AS m
               ON m.server_id = q.server_id AND m.key_kind = 'object'
              AND m.key = COALESCE(q.database_name, '') || '.' || COALESCE(q.schema_name, '') || '.' || COALESCE(q.object_name, '')
        WHERE q.server_id = ANY($1)
        AND   q.collection_time >= $2
        AND   q.collection_time <= $3
        AND   q.{TimescaleSupport.IntervalHonestSourceFilter}
        GROUP BY q.server_id, m.tag_id
        """;

    /// <summary>Procedures, hourly tier: <c>collect.procedure_stats_io_hourly</c>.</summary>
    public const string ProceduresHourlySql = """
        SELECT f.server_id, m.tag_id,
               CAST(COALESCE(SUM(f.worker_time_sum), 0) AS double precision) AS cpu_ms,
               CAST(COALESCE(SUM(f.elapsed_time_sum), 0) AS double precision) AS duration_ms,
               CAST(COALESCE(SUM(f.logical_reads_sum), 0) AS double precision) AS logical_reads,
               CAST(COALESCE(SUM(f.logical_writes_sum), 0) AS double precision) AS logical_writes,
               CAST(COALESCE(SUM(f.execution_count_sum), 0) AS double precision) AS executions
        FROM collect.procedure_stats_io_hourly AS f
        LEFT JOIN config.query_tag_map AS m
               ON m.server_id = f.server_id AND m.key_kind = 'object'
              AND m.key = COALESCE(f.database_name, '') || '.' || COALESCE(f.schema_name, '') || '.' || COALESCE(f.object_name, '')
        WHERE f.server_id = ANY($1)
        AND   f.bucket >= $2
        AND   f.bucket < $3$CEIL$
        GROUP BY f.server_id, m.tag_id
        """;

    private const string BlockedKey =
        "COALESCE(r.blocked_client_app, '') || chr(31) || COALESCE(r.blocked_login_name, '') || chr(31) || COALESCE(r.blocked_host_name, '') || chr(31) || COALESCE(r.database_name, '')";

    private const string BlockingKey =
        "COALESCE(r.blocking_client_app, '') || chr(31) || COALESCE(r.blocking_login_name, '') || chr(31) || COALESCE(r.blocking_host_name, '') || chr(31) || COALESCE(r.database_name, '')";

    private static string BlockingSql(string source, string window, string blockedCount, string causedCount) => $"""
        SELECT s.server_id, s.tag_id,
               CAST(SUM(s.blocked_ms) AS double precision) AS blocked_ms, CAST(SUM(s.caused_ms) AS double precision) AS caused_ms,
               CAST(SUM(s.blocked_n) AS double precision) AS {blockedCount}, CAST(SUM(s.caused_n) AS double precision) AS {causedCount}
        FROM (
            SELECT r.server_id, m.tag_id, COALESCE(r.wait_time_ms, 0) AS blocked_ms, 0 AS caused_ms, 1 AS blocked_n, 0 AS caused_n
            FROM {source} AS r
            LEFT JOIN config.query_tag_map AS m
                   ON m.server_id = r.server_id AND m.key_kind = 'caller' AND m.key = {BlockedKey}
            WHERE r.server_id = ANY($1) AND {window}
            UNION ALL
            SELECT r.server_id, m.tag_id, 0, COALESCE(r.wait_time_ms, 0), 0, 1
            FROM {source} AS r
            LEFT JOIN config.query_tag_map AS m
                   ON m.server_id = r.server_id AND m.key_kind = 'caller' AND m.key = {BlockingKey}
            WHERE r.server_id = ANY($1) AND {window}
        ) AS s
        GROUP BY s.server_id, s.tag_id
        """;

    /// <summary>Blocking, blocked-process reports: the wait lands on the blocked side's tag (blocked) and on the blocking side's tag (caused).</summary>
    public static readonly string BlockingReportsSql = BlockingSql("collect.blocked_process_reports", "r.event_time >= $2 AND r.event_time <= $3", "blocked_reports", "caused_reports");

    /// <summary>Blocking, DMV snapshots: the same two sides over the always-on DMV sample.</summary>
    public static readonly string BlockingSnapshotsSql = BlockingSql("collect.dmv_blocking_snapshots", "r.collection_time >= $2 AND r.collection_time <= $3", "blocked_snapshots", "caused_snapshots");

    /// <summary>The deadlock graphs of the window, newest first, up to $4; the processes are parsed in C# (a graph is XML).</summary>
    public const string DeadlockGraphsSql = """
        SELECT server_id, deadlock_id, deadlock_graph_xml, deadlock_time
        FROM collect.deadlocks
        WHERE server_id = ANY($1)
        AND   deadlock_time >= $2
        AND   deadlock_time <= $3
        ORDER BY deadlock_time DESC, deadlock_id DESC
        LIMIT $4
        """;

    /// <summary>The map rows for the deadlock processes: only the servers in the call ($1) and only the caller keys the window's processes carry ($2), never every caller row of the fleet.</summary>
    public const string CallerMapSql = """
        SELECT server_id, key, tag_id
        FROM config.query_tag_map
        WHERE server_id = ANY($1) AND key_kind = 'caller' AND key = ANY($2)
        """;

    /// <summary>The caller key of one process, as the sighting hook builds it.</summary>
    public static string CallerKey(string? application, string? login, string? host, string? database) =>
        (application ?? string.Empty) + '\u001F' + (login ?? string.Empty) + '\u001F' + (host ?? string.Empty) + '\u001F' + (database ?? string.Empty);
}

/// <summary>One measure. <paramref name="Scale"/> divides the stored value (microseconds to milliseconds); <paramref name="Peak"/> takes the maximum instead of the sum.</summary>
public sealed record QueryTagRollupMeasure(string Name, double Scale = 1, bool Peak = false);

/// <summary>One source's section: its name, its measures in the SQL's column order, and the measure the rows are ordered by, descending.</summary>
public sealed record QueryTagRollupSection(string Name, IReadOnlyList<QueryTagRollupMeasure> Measures, string OrderMeasure)
{
    /// <summary>Plan cache (query_stats).</summary>
    public static readonly QueryTagRollupSection PlanCache = new("plan_cache",
        [new("cpu_ms", Scale: 1000), new("duration_ms", Scale: 1000), new("logical_reads"), new("logical_writes"), new("executions")], "cpu_ms");

    /// <summary>Query Store.</summary>
    public static readonly QueryTagRollupSection QueryStore = new("query_store",
        [new("cpu_ms", Scale: 1000), new("duration_ms", Scale: 1000), new("logical_reads"), new("logical_writes"), new("executions"),
         new("peak_memory_pages", Peak: true)], "cpu_ms");

    /// <summary>Procedures (procedure_stats).</summary>
    public static readonly QueryTagRollupSection Procedures = new("procedures",
        [new("cpu_ms", Scale: 1000), new("duration_ms", Scale: 1000), new("logical_reads"), new("logical_writes"), new("executions")], "cpu_ms");

    /// <summary>Blocked-process reports.</summary>
    public static readonly QueryTagRollupSection BlockingReports = new("blocking_reports",
        [new("blocked_ms"), new("caused_ms"), new("blocked_reports"), new("caused_reports")], "blocked_ms");

    /// <summary>DMV blocking snapshots.</summary>
    public static readonly QueryTagRollupSection BlockingSnapshots = new("blocking_snapshots",
        [new("blocked_ms"), new("caused_ms"), new("blocked_snapshots"), new("caused_snapshots")], "blocked_ms");

    /// <summary>Deadlocks: the count of graphs holding the tag (not additive) and the processes of the tag.</summary>
    public static readonly QueryTagRollupSection Deadlocks = new("deadlocks",
        [new("deadlocks"), new("processes")], "deadlocks");
}
