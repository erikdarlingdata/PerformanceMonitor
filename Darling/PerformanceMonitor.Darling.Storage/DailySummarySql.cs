/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The Performance Calendar / Daily Summary aggregate SQL, and its #1661 retention-tier routing.
///
/// <para>This lives in Storage because it has TWO consumers in projects that cannot see each other: the viewer's
/// <c>ViewerDataService</c> and the service's <c>DarlingHealthReader</c> (behind the <c>get_daily_health</c> MCP
/// tool). Until #1661 each kept its own hand-copied literal, described as "verbatim" with nothing enforcing it.
/// They had not drifted in SQL — only in comments — but nothing would have caught it if they had, and routing one
/// without the other would have made the calendar and the MCP tool disagree about the same day.</para>
/// </summary>
public static class DailySummarySql
{
    /// <summary>
    /// Grouped per-day daily-summary aggregate. $1 server_id, $2 range start, $3 range end (naive UTC,
    /// half-open <c>[start, end)</c>). Postgres dialect: <c>date_trunc('day', ...)</c> day bucketing,
    /// <c>(array_agg(wait_type ORDER BY ...))[1]</c> for the per-day top wait, <c>FILTER</c> conditional
    /// counts, and a day spine (UNION of every source's days) LEFT JOINed so a quiet-but-collected day still
    /// appears (Healthy, not No-Data). The high-CPU rule (total host CPU = SQL + other-process &gt;= 80, Linux
    /// NULL-other-process fallback) mirrors the alert engine and the Overview headline.
    /// </summary>
    public const string RangeSql = """
        WITH wait_per_type AS (
            SELECT date_trunc('day', collection_time) AS d, wait_type, SUM(delta_wait_time_ms) AS ms
            FROM v_wait_stats
            WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3 AND delta_wait_time_ms > 0
            GROUP BY 1, 2
        ),
        wait_totals AS (
            SELECT d, SUM(ms) / 1000.0 AS total_wait_sec
            FROM wait_per_type
            GROUP BY d
        ),
        wait_top AS (
            /* Per-day top wait type = the wait with the most delta time that day (Postgres DISTINCT ON). */
            SELECT DISTINCT ON (d) d, wait_type AS top_wait_type
            FROM wait_per_type
            ORDER BY d, ms DESC
        ),
        waits AS (
            SELECT t.d, t.total_wait_sec, tp.top_wait_type
            FROM wait_totals t
            LEFT JOIN wait_top tp ON tp.d = t.d
        ),
        queries AS (
            SELECT date_trunc('day', collection_time) AS d, COUNT(DISTINCT query_hash) AS c
            FROM v_query_stats
            WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
            GROUP BY 1
        ),
        deadlocks AS (
            SELECT date_trunc('day', collection_time) AS d, COUNT(*) AS c
            FROM v_deadlocks
            WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
            GROUP BY 1
        ),
        bpr AS (
            SELECT date_trunc('day', collection_time) AS d, COUNT(*) AS c, MAX(wait_time_ms) AS max_wait_ms
            FROM v_blocked_process_reports
            WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
            GROUP BY 1
        ),
        dmv AS (
            SELECT date_trunc('day', collection_time) AS d, COUNT(*) AS c, MAX(wait_time_ms) AS max_wait_ms
            FROM v_dmv_blocking_snapshots
            WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
            GROUP BY 1
        ),
        cpu AS (
            /* Total host CPU = SQL + other-process (NULL on Linux -> 0), matching the Overview headline. The
               80 is ServerHealthThresholds.CpuWarningPercent, the card band's Warning bar, restated as a
               literal because this is a SQL string and pinned equal by both suites (#3539 A2). Deliberately
               NOT the alert engine's configurable CPU threshold: this statement re-counts at read time, so
               binding it to a knob would recolour every past day the moment the knob moved. The count feeds
               a bar that scales with the window (DailyHealthThresholds.HighCpuCriticalSamplesFor). */
            SELECT date_trunc('day', collection_time) AS d,
                   COUNT(*) FILTER (WHERE (sqlserver_cpu_utilization + COALESCE(other_process_cpu_utilization, 0)) >= 80) AS c
            FROM v_cpu_utilization_stats
            WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
            GROUP BY 1
        ),
        coll AS (
            /* Any run (all statuses) marks the day as collected -> it appears even if every metric is quiet
               (a quiet monitored day is Healthy/green, not No-Data/grey). runs is also the denominator the
               error SHARE bands on (#3539 A2); errs alone used to make the day Critical on presence. */
            SELECT date_trunc('day', collection_time) AS d,
                   COUNT(*) AS runs,
                   COUNT(*) FILTER (WHERE status = 'ERROR') AS errs
            FROM v_collection_log
            WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
            GROUP BY 1
        ),
        mem AS (
            SELECT date_trunc('day', collection_time) AS d,
                   COUNT(*) FILTER (WHERE memory_indicators_process >= 2 OR memory_indicators_system >= 2) AS pressure,
                   COUNT(*) FILTER (WHERE memory_indicators_process >= 3) AS critical
            FROM v_memory_pressure_events
            WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
            GROUP BY 1
        ),
        alerts AS (
            /* Actionable alerts only: exclude dismissed rows and resolution/good-news notices, mirroring
               AlertMetricClassifier.IsResolution. The suffix list must match that method exactly — it is a
               hand-maintained SQL copy, so widening one without the other silently counts recoveries
               ("Collection Resumed", "Agent Restarted", "AG Sync Recovered") as actionable alerts. */
            SELECT date_trunc('day', alert_time) AS d, COUNT(*) AS c
            FROM config_alert_log
            WHERE server_id = $1 AND alert_time >= $2 AND alert_time < $3
              AND dismissed = FALSE
              AND metric_name NOT LIKE '%Cleared%'
              AND metric_name NOT LIKE '%Resolved%'
              AND metric_name NOT LIKE '%Restored%'
              AND metric_name NOT LIKE '%Resumed%'
              AND metric_name NOT LIKE '%Restarted%'
              AND metric_name NOT LIKE '%Recovered%'
              AND metric_name NOT LIKE '%Reconnected%'
            GROUP BY 1
        ),
        day_spine AS (
            SELECT d FROM waits
            UNION SELECT d FROM queries
            UNION SELECT d FROM deadlocks
            UNION SELECT d FROM bpr
            UNION SELECT d FROM dmv
            UNION SELECT d FROM cpu
            UNION SELECT d FROM coll
            UNION SELECT d FROM mem
            UNION SELECT d FROM alerts
        )
        SELECT
            s.d AS day,
            COALESCE(w.total_wait_sec, 0) AS total_wait_sec,
            w.top_wait_type,
            COALESCE(q.c, 0) AS unique_queries,
            COALESCE(dl.c, 0) AS deadlock_count,
            COALESCE(NULLIF(b.c, 0), dm.c, 0) AS blocking_events,
            COALESCE(cp.c, 0) AS high_cpu_events,
            COALESCE(cl.errs, 0) AS collection_errors,
            COALESCE(m.pressure, 0) AS memory_pressure_events,
            COALESCE(m.critical, 0) AS memory_critical_events,
            COALESCE(al.c, 0) AS alert_count,
            /* Peak block wait (ms) from the SAME source the blocking count came from (BPR preferred, DMV-snapshot
               fallback), so the day-detail blocking reason ('N blocking events (peak block X)') reconciles with
               the count. 0 when the blocking came from a source without a wait time. */
            COALESCE(CASE WHEN COALESCE(b.c, 0) > 0 THEN b.max_wait_ms ELSE dm.max_wait_ms END, 0) AS peak_block_wait_ms,
            /* Every collector run in the window (#3539 A2): the denominator that turns collection_errors
               into a share. Appended after peak_block_wait_ms so every existing ordinal read stays where it was. */
            COALESCE(cl.runs, 0) AS collection_runs,
            /* #3541 A9: how many of the seven per-signal sources hold at least one row for the day — the
               retention arm's PRESENCE fact. The spine is a UNION over nine sources aging out at different
               horizons, each LEFT JOINed and COALESCEd to zero, so a day the collection log (60 days) or the
               alert log (90) still names can have every signal (30) purged and read as measured-zero-Healthy.
               The reader judges such a day by this count against the store's retention horizon: zero sources
               past the horizon is a purged shell, some sources past it is a day the purge has not reached.
               A source counts as present when its grouped CTE produced a row for the day, which for cpu is
               "any sample" (the FILTER is inside the aggregate) and for waits is "any positive delta".
               Appended LAST, after collection_runs, for the same ordinal reason. */
            (CASE WHEN w.d IS NULL THEN 0 ELSE 1 END)
              + (CASE WHEN q.d IS NULL THEN 0 ELSE 1 END)
              + (CASE WHEN dl.d IS NULL THEN 0 ELSE 1 END)
              + (CASE WHEN b.d IS NULL THEN 0 ELSE 1 END)
              + (CASE WHEN dm.d IS NULL THEN 0 ELSE 1 END)
              + (CASE WHEN cp.d IS NULL THEN 0 ELSE 1 END)
              + (CASE WHEN m.d IS NULL THEN 0 ELSE 1 END) AS signal_sources_present
        FROM day_spine s
        LEFT JOIN waits w ON w.d = s.d
        LEFT JOIN queries q ON q.d = s.d
        LEFT JOIN deadlocks dl ON dl.d = s.d
        LEFT JOIN bpr b ON b.d = s.d
        LEFT JOIN dmv dm ON dm.d = s.d
        LEFT JOIN cpu cp ON cp.d = s.d
        LEFT JOIN coll cl ON cl.d = s.d
        LEFT JOIN mem m ON m.d = s.d
        LEFT JOIN alerts al ON al.d = s.d
        ORDER BY s.d
        """;

    /// <summary>
    /// The <c>queries</c> CTE exactly as it appears in <see cref="RangeSql"/> — the ONE part of the
    /// daily summary that reads a table subject to the 4-day raw drop. Every other source here (wait_stats,
    /// deadlocks, alerts, memory) keeps <c>DarlingRetention</c>'s 30-day default, so they need no routing.
    /// </summary>
    private const string QueriesCteRaw = """
        queries AS (
            SELECT date_trunc('day', collection_time) AS d, COUNT(DISTINCT query_hash) AS c
            FROM v_query_stats
            WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
            GROUP BY 1
        ),
        """;

    /// <summary>
    /// The same CTE against a rollup. <c>COUNT(DISTINCT query_hash)</c> is exact over a CAGG because query_hash is
    /// one of its GROUP BY columns — the rollup preserves every distinct hash per bucket, so counting them per day
    /// gives the identical answer raw would. The time column becomes <c>bucket</c>; parameter positions are
    /// unchanged, so the caller binds the same three values either way.
    /// </summary>
    private static string QueriesCteForCagg(string relation) => $"""
        queries AS (
            SELECT date_trunc('day', bucket) AS d, COUNT(DISTINCT query_hash) AS c
            FROM collect.{relation}
            WHERE server_id = $1 AND bucket >= $2 AND bucket < $3
            GROUP BY 1
        ),
        """;

    /// <summary>
    /// #1661: the daily summary SQL for <paramref name="tier"/>. Raw returns the frozen constant untouched; a
    /// rollup tier swaps only the <c>queries</c> CTE. Throws if the swap finds nothing, so editing
    /// <see cref="RangeSql"/> without updating <see cref="QueriesCteRaw"/> fails loudly
    /// instead of silently leaving the tab on raw — which is precisely how this bug went unnoticed.
    /// </summary>
    public static string RangeSqlFor(RetentionTier tier)
    {
        if (tier == RetentionTier.Raw)
        {
            return RangeSql;
        }

        var relation = tier == RetentionTier.Hourly
            ? TimescaleSupport.QueryStatsHourlyView
            : TimescaleSupport.QueryStatsDailyView;

        var routed = RangeSql.Replace(
            QueriesCteRaw, QueriesCteForCagg(relation), StringComparison.Ordinal);

        if (string.Equals(routed, RangeSql, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "Daily-summary CAGG routing found no queries CTE to replace — QueriesCteRaw has drifted from RangeSql (#1661).");
        }

        return routed;
    }

}
