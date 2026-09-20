/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;

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
            /* #3653 A6: the count when the queries CTE holds the day with one, 0 when it does not hold the day,
               and NULL when it holds the day WITHOUT a count — the routed form's "not carried at this tier" row
               (QueriesCteForCagg's third member). On raw the CTE never produces a NULL count (a COUNT is never
               NULL), so here this is the old COALESCE-to-zero by another spelling, and the spelling is what lets one
               outer select serve every tier without a second projection swap. Measurement contract rule 1
               (MeasurementContractCensusTests): what was not measured is NULL, never 0 — the daily calendar
               is an instance of it. The readers keep the NULL (long?) and name the day in days_missing[]. */
            CASE WHEN q.d IS NULL THEN 0 ELSE q.c END AS unique_queries,
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
               Appended LAST, after collection_runs, for the same ordinal reason.
               #3653 A6: the queries arm reads the COUNT rather than the day, because the routed form's
               not-carried row (QueriesCteForCagg) carries the day with a NULL count — the rollup holds no row
               for that day, so it is not a source that holds the day, and counting it present would turn a
               purged shell whose one surviving "signal" is a day the tier never carried into past_horizon.
               On raw a queries row always carries a count, so the two spellings agree there. */
            (CASE WHEN w.d IS NULL THEN 0 ELSE 1 END)
              + (CASE WHEN q.c IS NULL THEN 0 ELSE 1 END)
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
    ///
    /// <para><b>The days the rollup has not reached yet come from raw (#3653 A6, the <c>unique_queries = 0</c>
    /// clause).</b> Both query rollups are materialized-only continuous aggregates (no
    /// <c>materialized_only = false</c> on their CREATE, and TimescaleDB 2.13+ defaults real-time OFF), and
    /// their refresh policies run behind the clock — the hourly's <c>end_offset</c> is an hour, the daily's a
    /// day with a daily cadence, so the daily's newest complete bucket is one to two days old. A calendar whose
    /// window starts past the hourly route's ceiling is routed to the DAILY rollup for the WHOLE window,
    /// yesterday and today included, and there the LEFT JOIN found no row for those days: the outer
    /// <c>COALESCE(q.c, 0)</c> then printed <c>unique_queries = 0</c> beside that same day's fresh wait, CPU
    /// and deadlock numbers, which read the 30-day raw tables. Zero is not what was measured; "not
    /// materialized yet" is. The CTE now takes the rollup for every day up to and including the rollup's last
    /// materialized DAY for this server and raw <c>query_stats</c> for the days after it — raw keeps four days
    /// and the lag is at most two, so those days are answered exactly, from the rows the rollup will
    /// materialize tomorrow. The two halves are partitioned on the ceiling DAY, never on the hour, so no day
    /// has two rows in <c>queries</c> (a <c>COUNT(DISTINCT)</c> cannot be summed across halves) and the
    /// hourly tier's ceiling day reads its own partial rollup rather than a mix. Bounded by shape: the ceiling
    /// is one indexed <c>max(bucket)</c> per server and the raw half is at most two days of one server.</para>
    ///
    /// <para><b>The days the rollup skipped BELOW its ceiling are NULL, not 0 (#3653 A6, the last clause).</b>
    /// A day at or below this server's ceiling for which the rollup holds no row was, until this, a day the
    /// LEFT JOIN missed and the outer select printed as <c>unique_queries = 0</c> beside that day's real wait,
    /// CPU and deadlock numbers. Zero says "measured, nothing ran"; what happened is that the tier never
    /// materialized the day. The shape that leaves such a day is the one <see cref="RetentionTierRouter"/>'s
    /// essay and <see cref="TimescaleSupport.RepairMaterializationHolesAsync"/> describe: a service down longer
    /// than a refresh policy's <c>start_offset</c> resumes with a refresh that opens at <c>now - start_offset</c>,
    /// and every bucket between the last pre-outage refresh's window end and the outage is skipped for good
    /// unless something refreshes it by hand. #3731 does exactly that at the next service start, so the case is
    /// rarer than it was and not gone: between the first post-resume policy refresh (which moves the ceiling
    /// past the skipped days and makes them "below" it) and the next start, and for any hole past the
    /// per-start cap, the calendar still reads the skipped day. The honest answer is NULL — "not carried at
    /// this tier" — which is the measurement contract's rule 1 (what was not measured is NULL, never 0;
    /// <c>MeasurementContractCensusTests</c>) applied to a calendar cell.</para>
    ///
    /// <para><b>The witness is the hole scan's own definition, per server, at day grain — not a second
    /// one.</b> <see cref="TimescaleSupport.MaterializationHoleScanSql"/> calls a bucket a hole when the
    /// materialization holds NO row for it AND the aggregate's SOURCE holds at least one row in it that the
    /// aggregate's own <c>WHERE</c> admits. The third member of the <c>queries</c> CTE asks the same two
    /// questions of every day at or below the ceiling, with <c>server_id = $1</c> on both probes (this is one
    /// server's calendar) and the day as the bucket: no rollup row for this server that day, and a source row
    /// for this server that day the aggregate would have produced output from. The source, its time column
    /// and its filter are read off <see cref="TimescaleSupport.MaterializationHoleTargets"/> — the repair's own
    /// target list — so the daily tier probes <c>query_stats_hourly</c> (its hierarchical source, 90 days),
    /// the hourly tier probes raw <c>query_stats</c> (4 days), and the interval-honest successor's probe
    /// carries <c>sample_interval_seconds IS DISTINCT FROM 0</c> (<see cref="TimescaleSupport.MaterializationHoleSourceFilterFor"/>)
    /// so a day holding only restart rows is not called a hole. The second probe is what keeps the disclosure
    /// honest where the raw table was simply empty: a server that ran nothing that day has no source row, no
    /// rollup row, and is not named — its 0 is what raw would have said. It also bounds what this can see: a
    /// day whose source rows the purge has already taken is undecidable by any witness and reads as it did;
    /// on the daily tier that is a day older than the hourly's 90 days, which is also the alert log's horizon —
    /// the longest-lived spine source — so such a day is at most a boundary-day shell, already banded NoData
    /// by its retention state. A day BELOW the relation's floor that the source still holds
    /// (the straddle the router's essay accepts "with no signal to the caller") answers the same two probes
    /// the same way, so this is also that reader's partial-coverage notice, for as long as the source holds
    /// the day. Each probe is one index range per day — the materialization's <c>(server_id, bucket)</c>
    /// group index, the source's <c>(server_id, time)</c> index — over a series of at most a year of days, so
    /// the cost follows the window's length and not the tables' weight, the scan's own argument.</para>
    ///
    /// <para><b>A bucket holding zero distinct hashes is not a case.</b> The rollup groups by <c>query_hash</c>,
    /// so a bucket exists only where a source row did, and every bucket carries at least one hash: a row in the
    /// rollup half of this CTE always has <c>c &gt;= 1</c>. "Carried with a count of 0" cannot occur, which is
    /// why the not-carried row can be told apart by <c>c IS NULL</c> alone and needs no flag column. The outer
    /// select projects <c>CASE WHEN q.d IS NULL THEN 0 ELSE q.c END</c> so the NULL survives the join; the
    /// readers keep it as <c>long?</c> and list the day in <c>days_missing[]</c>. The day joins the spine
    /// through <c>queries</c> like any other, so it is printed — its source proves it was collected — and its
    /// presence arm reads the count, not the day, so it is not counted as a source that holds the day.</para>
    ///
    /// <para>Lite's twin has no rollup tier: its calendar reads raw DuckDB, where every day inside retention is
    /// the source itself, and is untouched.</para>
    /// </summary>
    private static string QueriesCteForCagg(string relation)
    {
        /* The repair's own target row for this relation: the source it aggregates, that source's time column and
           the CREATE its filter is read from. Looked up rather than restated so the calendar's idea of "what the
           rollup reads" cannot drift from the scan's. A relation the registry does not know is refused here,
           before it reaches the store as a 42P01. */
        var target = TimescaleSupport.MaterializationHoleTargets
            .FirstOrDefault(t => string.Equals(t.View, relation, StringComparison.Ordinal));
        if (target.View is null)
        {
            throw new ArgumentException(
                $"'{relation}' is not a registered continuous aggregate (TimescaleSupport.MaterializationHoleTargets), so the daily summary cannot name its source or the days it did not carry (#3653 A6).",
                nameof(relation));
        }

        var filter = TimescaleSupport.MaterializationHoleSourceFilterFor(target.CreateSql);
        var sourceFilter = filter.Length == 0 ? string.Empty : $"\n          AND {filter}";

        return $"""
        queries_ceiling AS (
            SELECT date_trunc('day', max(bucket)) AS last_day
            FROM collect.{relation}
            WHERE server_id = $1
        ),
        queries AS (
            SELECT date_trunc('day', bucket) AS d, COUNT(DISTINCT query_hash) AS c
            FROM collect.{relation}
            WHERE server_id = $1 AND bucket >= $2 AND bucket < $3
            GROUP BY 1
            UNION ALL
            SELECT date_trunc('day', collection_time) AS d, COUNT(DISTINCT query_hash) AS c
            FROM v_query_stats
            WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
              AND collection_time >= COALESCE((SELECT last_day + INTERVAL '1 day' FROM queries_ceiling), $2)
            GROUP BY 1
            UNION ALL
            /* #3653 A6: the days at or below this server's ceiling that the rollup holds NO row for while its
               source still holds rows the rollup's own WHERE admits -- "not carried at this tier". One row per
               such day with a NULL count; the outer select passes it through as unique_queries = NULL and the
               readers list the day in days_missing[]. The two probes are the hole scan's
               (TimescaleSupport.MaterializationHoleScanSql, the #3731 repair), per server, at day grain. A day
               the source has no admitted row for is not named: the rollup is honestly empty there. */
            SELECT b.d, NULL::bigint AS c
            FROM generate_series(date_trunc('day', $2::timestamp), date_trunc('day', $3::timestamp), INTERVAL '1 day') AS b(d)
            WHERE b.d < $3
              AND b.d < COALESCE((SELECT last_day + INTERVAL '1 day' FROM queries_ceiling), $2)
              AND NOT EXISTS (
                SELECT 1 FROM collect.{relation} AS r
                WHERE r.server_id = $1 AND r.bucket >= b.d AND r.bucket < b.d + INTERVAL '1 day')
              AND EXISTS (
                SELECT 1 FROM collect.{target.Source} AS s
                WHERE s.server_id = $1 AND s.{target.SourceTimeColumn} >= b.d AND s.{target.SourceTimeColumn} < b.d + INTERVAL '1 day'{sourceFilter})
        ),
        """;
    }

    /// <summary>
    /// #1661: the daily summary SQL for <paramref name="tier"/>. Raw returns the frozen constant untouched; a
    /// rollup tier swaps only the <c>queries</c> CTE. Throws if the swap finds nothing, so editing
    /// <see cref="RangeSql"/> without updating <see cref="QueriesCteRaw"/> fails loudly
    /// instead of silently leaving the tab on raw — which is precisely how this bug went unnoticed.
    /// </summary>
    public static string RangeSqlFor(RetentionTier tier)
        => RangeSqlFor(tier, TimescaleSupport.QueryStatsHourlyView);

    /// <summary>
    /// <see cref="RangeSqlFor(RetentionTier)"/> with the HOURLY relation the caller resolved (#3653, Q12):
    /// <paramref name="hourlyRelation"/> is <c>query_stats_hourly</c> or its interval-honest successor
    /// <c>query_stats_interval_hourly</c>, by <see cref="RollupCoverage.HourlyRelationFor"/> over the window's
    /// start. The successor carries the same <c>query_hash</c> and <c>bucket</c> the queries CTE reads, and
    /// leaves out the restart row the legacy counted as a query seen that day. The daily tier is not
    /// parameterised: the daily has no successor (it is hierarchical from the legacy hourly —
    /// <see cref="TimescaleSupport.SupersededHourlyRollups"/>). The one-argument form keeps the legacy for
    /// callers that have not probed, which is what every pre-#3653 pin reads.
    ///
    /// <para>#3653 A6: the routed text differs between the legacy and the successor in the relation name AND in
    /// the not-carried probe's source filter — the successor's own <c>WHERE</c>, read off its CREATE — so the
    /// two are no longer one text with a name swapped; <c>IntervalHonestHourlyRollupTests</c> pins both
    /// differences and nothing else. A <paramref name="hourlyRelation"/> that is not a registered continuous
    /// aggregate is refused with an <see cref="ArgumentException"/> rather than sent to the store.</para>
    /// </summary>
    public static string RangeSqlFor(RetentionTier tier, string hourlyRelation)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(hourlyRelation);

        if (tier == RetentionTier.Raw)
        {
            return RangeSql;
        }

        var relation = tier == RetentionTier.Hourly
            ? hourlyRelation
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
