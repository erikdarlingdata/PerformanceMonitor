/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Common;

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
    /// scan is the <c>(server_id, first_execution_time)</c> range instead. The upper bound is written the same way,
    /// <c>$3 - collection_time &gt;= interval '0'</c> (#6800 part 2, step 3a). It was a plain <c>collection_time &lt;= $3</c> once, on the
    /// belief that $3 is at or near now and keeps almost every row, so the planner has no reason to use the index for it. Step 2b measured
    /// otherwise: with the window end 6 hours back the planner chose the <c>collection_time</c> index for that bound alone, the bitmap went
    /// lossy, and the run took 367 ms against 330 ms at now on a 30-server seed. That index inside the per-server loop is the 13.0 s case
    /// above, and this statement serves every one-server read and every fleet read the route falls back from (a store before the backfill,
    /// an <c>as_of</c> older than the built hours). The two forms keep the same rows.</para>
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
        AND   $3 - q.collection_time >= interval '0'
        AND   q.first_execution_time >= $4 - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
        AND   q.first_execution_time <= $3 + {{QueryStoreIntervalWide.FirstExecUpperSlackSql}}
        GROUP BY q.server_id, m.tag_id
        """;

    /// <summary>
    /// The fleet form of <see cref="QueryStoreSql"/> (#6800 part 2, step 2): the same six measures per <c>(server_id, tag_id)</c>, with the
    /// whole hours the hourly builder has done read from <c>collect.query_store_hash_hourly</c> and the rest from the wide table.
    /// <see cref="QueryTagRollupHashRoute"/> decides which is which. Parameters: the hourly arm's six first (#7240 step 4): $1 the servers it reads
    /// (<c>integer[]</c>), $2 and $3 the hour ranges (parallel <c>timestamp[]</c> arrays of starts and ends, each range <c>[from, to)</c>), and $4, $5, $6 the gaps inside those ranges that
    /// the arm must not read, as parallel arrays (server <c>integer[]</c>, gap start and gap end <c>timestamp[]</c>, each gap <c>[start, end)</c>); then $7 the
    /// window end, then three per wide range: the servers that share it (<c>integer[]</c>), its <c>from</c> and its <c>to</c> (each range
    /// <c>[from, to)</c>), the first range at $8, $9 and $10.
    /// With <paramref name="days"/> (#6800 step 3c) the day pairs come next as $8 (day start <c>timestamp[]</c>) and $9 (server <c>integer[]</c>), probed in
    /// <c>collect.query_store_hash_daily</c> with the same partials, and the first range moves to $10, $11 and $12.
    /// <para>Both arms yield six partials per <c>(server_id, query_hash)</c> (sums of the weighted products and of the counts, the maximum of
    /// the memory), exact <c>numeric</c> sums, so adding the hours gives the wide statement's total. The tag map is joined once, on the
    /// partials, and the final GROUP BY is by <c>(server_id, tag_id)</c>: the aggregate stays at that size. The COALESCE and the cast to
    /// <c>double precision</c> are today's, applied after the hours are added.</para>
    /// <para>The hourly arm (#7240 step 4) is one range on <c>hour</c> for each run of hours some server reads from the table (so the whole days the day table answers are never scanned), for the servers in scope, less the gaps: <see cref="QueryTagRollupHashRoute.HourlyRead"/>
    /// derives the gaps from the plan's table pairs, so a gap is every hour of a server inside the range that is not one of its table
    /// pairs (an hour not built, a stale pair, an hour a day row answers, an hour before the server's own start), and an anti-join removes
    /// them. It replaces the <c>unnest</c> of every <c>(hour, server)</c> pair, which the planner merge-joined against
    /// <c>ix_query_store_hash_hourly_hour_server</c>. The arm is grouped by <c>(server_id, query_hash)</c> before the <c>UNION ALL</c>, as the wide
    /// arms are, so the tag join below sees one row per hash, not one per hash and hour: sums stay sums and the memory maximum stays a maximum, so the final
    /// totals are the same. A pair is read by the hourly arm only when it is a table pair and by a wide arm only when its hour lies in a
    /// wide range, and the plan keeps those two sets apart. The wide side is one arm per DISTINCT <c>[from, to)</c>
    /// (<paramref name="wideRanges"/>: every server that shares the tail shares one scan of it), and each arm carries its own numbered
    /// parameters. Its bounds on <c>collection_time</c> are written plainly so a scan by <c>collection_time</c> is possible, which a
    /// per-server loop must not use (<see cref="QueryStoreSql"/> explains the 13.0 s it cost), and its <c>first_execution_time</c> floor is
    /// the range's own start less the purge margin, the same floor the builder uses for the hour and the wide statement's own for the first
    /// range. Why one arm each and not one <c>CROSS JOIN LATERAL</c> over the ranges: inside a lateral the bounds are the outer row's values,
    /// so the planner cannot see how wide a range is (the head of a window and its tail look alike), takes the range for selective, and adds
    /// the <c>first_execution_time</c> index to the scan by <c>collection_time</c>; that index returns every row of the window and its
    /// servers, so each range paid for the whole window's index entries and the bitmap went lossy. With the values in the arm the planner
    /// sees the real range and scans <c>collection_time</c> alone. A constant floor shared by all ranges was tried first and did not stop
    /// it (PR notes).</para>
    ///
    /// <para>The wide arms bound <c>first_execution_time</c> above by the earlier of the range's own end and the window end, plus
    /// <see cref="QueryStoreIntervalWide.FirstExecUpperSlackSql"/> (#6847). <see cref="QueryStoreSql"/> bounds by the window end. The slack's
    /// reasons (#5523) are about a row's <c>collection_time</c>, not the window: a row collected before the range's end holds a first
    /// execution at most the monitored clock's lead plus a collection cycle after that, which the 12 hours cover. A bound at the window end
    /// let a range early in the window run its <c>(server_id, first_execution_time)</c> index range up to 12 hours past the WINDOW's end,
    /// which PostgreSQL could then AND into the scan of <c>collection_time</c> and read the window's index entries to serve one range.
    /// The table arm and the day arm have no such bound, because the hour and day rows do not carry the column. So the routed answer
    /// equals <see cref="QueryStoreSql"/> except for a row whose first execution is more than 12 hours after its range's end (a monitored
    /// server whose clock runs more than 12 hours fast, which the slack already leaves out): the statement drops it from a window it would
    /// count in a wider one, and the tables keep it every time.</para>
    /// </summary>
    public static string QueryStoreRoutedSqlFor(int wideRanges, bool days = false)
    {
        /* $1 to $6 are the hourly arm's, $7 the window end, then the day pair arrays (when days) and the wide arms' triples. */
        const string windowEnd = "$7";
        var first = days ? 10 : 8;
        var text = new System.Text.StringBuilder();
        text.Append("""
            WITH partials AS
            (
                SELECT t.server_id, t.query_hash,
                       SUM(t.ec_sum) AS ec_sum,
                       SUM(t.cpu_wsum) AS cpu_wsum,
                       SUM(t.dur_wsum) AS dur_wsum,
                       SUM(t.reads_wsum) AS reads_wsum,
                       SUM(t.writes_wsum) AS writes_wsum,
                       MAX(t.mem_max) AS mem_max
                FROM unnest($2::timestamp[], $3::timestamp[]) AS r(range_from, range_to)
                JOIN collect.query_store_hash_hourly AS t
                       ON t.hour >= r.range_from
                      AND t.hour < r.range_to
                WHERE t.server_id = ANY($1::integer[])
                AND   NOT EXISTS
                      (
                          SELECT 1
                          FROM unnest($4::integer[], $5::timestamp[], $6::timestamp[]) AS g(server_id, gap_from, gap_to)
                          WHERE g.server_id = t.server_id
                          AND   t.hour >= g.gap_from
                          AND   t.hour < g.gap_to
                      )
                GROUP BY t.server_id, t.query_hash

            """);
        if (days)
        {
            text.Append("""
                    UNION ALL
                    SELECT t.server_id, t.query_hash, t.ec_sum, t.cpu_wsum, t.dur_wsum, t.reads_wsum, t.writes_wsum, t.mem_max
                    FROM unnest($8::timestamp[], $9::integer[]) AS p(day, server_id)
                    JOIN collect.query_store_hash_daily AS t
                           ON t.day = p.day
                          AND t.server_id = p.server_id

                """);
        }

        for (var arm = 0; arm < wideRanges; arm++)
        {
            var servers = "$" + (first + (3 * arm)).ToString(System.Globalization.CultureInfo.InvariantCulture);
            var from = "$" + (first + 1 + (3 * arm)).ToString(System.Globalization.CultureInfo.InvariantCulture);
            var to = "$" + (first + 2 + (3 * arm)).ToString(System.Globalization.CultureInfo.InvariantCulture);
            text.Append($$"""
                    UNION ALL
                    SELECT q.server_id, q.query_hash,
                           SUM(q.execution_count) AS ec_sum,
                           SUM(q.avg_cpu_time_us * q.execution_count) AS cpu_wsum,
                           SUM(q.avg_duration_us * q.execution_count) AS dur_wsum,
                           SUM(q.avg_logical_io_reads * q.execution_count) AS reads_wsum,
                           SUM(q.avg_logical_io_writes * q.execution_count) AS writes_wsum,
                           MAX(q.max_query_max_used_memory) AS mem_max
                    FROM collect.query_store_interval_wide AS q
                    WHERE q.server_id = ANY({{servers}}::integer[])
                    AND   q.collection_time >= {{from}}::timestamp
                    AND   q.collection_time < {{to}}::timestamp
                    AND   q.collection_time <= {{windowEnd}}
                    AND   q.first_execution_time >= {{from}}::timestamp - {{QueryStoreIntervalWide.PurgeEdgeMarginSql}}
                    AND   q.first_execution_time <= LEAST({{to}}::timestamp, {{windowEnd}}) + {{QueryStoreIntervalWide.FirstExecUpperSlackSql}}
                    GROUP BY q.server_id, q.query_hash

                """);
        }

        text.Append("""
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
            """);
        return text.ToString();
    }

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

    /// <summary>
    /// The two arms over one relation <c>r</c>: a row lands on the blocked side's tag (blocked) and on the blocking side's tag
    /// (caused). <paramref name="with"/> is empty or a WITH list ending in a comma-free CTE; <paramref name="armWhere"/> is the
    /// extra predicate on <c>r</c> (empty when the CTE already applied the window).
    /// </summary>
    private static string BlockingSql(string with, string source, string armWhere, string blockedCount, string causedCount) => $"""
        {with}SELECT s.server_id, s.tag_id,
               CAST(SUM(s.blocked_ms) AS double precision) AS blocked_ms, CAST(SUM(s.caused_ms) AS double precision) AS caused_ms,
               CAST(SUM(s.blocked_n) AS double precision) AS {blockedCount}, CAST(SUM(s.caused_n) AS double precision) AS {causedCount}
        FROM (
            SELECT r.server_id, m.tag_id, COALESCE(r.wait_time_ms, 0) AS blocked_ms, 0 AS caused_ms, 1 AS blocked_n, 0 AS caused_n
            FROM {source} AS r
            LEFT JOIN config.query_tag_map AS m
                   ON m.server_id = r.server_id AND m.key_kind = 'caller' AND m.key = {BlockedKey}
            WHERE r.server_id = ANY($1){armWhere}
            UNION ALL
            SELECT r.server_id, m.tag_id, 0, COALESCE(r.wait_time_ms, 0), 0, 1
            FROM {source} AS r
            LEFT JOIN config.query_tag_map AS m
                   ON m.server_id = r.server_id AND m.key_kind = 'caller' AND m.key = {BlockingKey}
            WHERE r.server_id = ANY($1){armWhere}
        ) AS s
        GROUP BY s.server_id, s.tag_id
        """;

    /// <summary>
    /// Blocking, blocked-process reports (#7163): the wait lands on the blocked side's tag (blocked) and on the blocking side's
    /// tag (caused). The reports are collapsed to WAITS first (<see cref="BlockingWaits.WaitsCteSql"/>, the definition the
    /// alert, the fleet tile and the daily summary share): SQL Server writes a report every threshold interval, each carrying
    /// the wait so far, so summing the rows added a wait up about eleven times over a one-minute wait. <c>blocked_ms</c> and
    /// <c>caused_ms</c> are the longest report of each wait and the counts are waits; a parallel query's thread waiting on its own
    /// session is no wait (<see cref="BlockingWaits.NotSelfWaitSql"/>). The tag keys are the longest report's caller columns.
    /// The columns keep the names <c>blocked_reports</c> and <c>caused_reports</c>, the section's contract.
    /// </summary>
    public static readonly string BlockingReportsSql = BlockingSql(
        "WITH " + BlockingWaits.WaitsCteSql(
            BlockingWaits.SqlDialect.Postgres, "rw",
            """
            SELECT server_id, blocked_spid, blocking_spid, event_time, wait_time_ms, database_name,
                   blocked_client_app, blocked_login_name, blocked_host_name, blocking_client_app, blocking_login_name, blocking_host_name
            FROM collect.blocked_process_reports
            WHERE server_id = ANY($1) AND event_time >= $2 AND event_time <= $3
            """,
            ["server_id"]) + "\n",
        "rw", string.Empty, "blocked_reports", "caused_reports");

    /// <summary>Blocking, DMV snapshots: the same two sides over the always-on DMV sample, one row per sample (a still-open wait repeats once per sample); a parallel query's self-wait rows are left out.</summary>
    public static readonly string BlockingSnapshotsSql = BlockingSql(
        string.Empty, "collect.dmv_blocking_snapshots",
        " AND r.collection_time >= $2 AND r.collection_time <= $3 AND " + BlockingWaits.NotSelfWaitSql, "blocked_snapshots", "caused_snapshots");

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
