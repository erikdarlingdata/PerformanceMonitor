/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;

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
    /// half-open <c>[start, end)</c>), $4 the <see cref="EventWindowFloor"/> for $2 (the deadlock and blocked-report
    /// CTEs window on the event's own time, so they need the partition-column floor beside it; bind it at every
    /// caller). Deadlocks and blocked-process reports count on the day they happened; every other source on the
    /// day it was collected. Postgres dialect: <c>date_trunc('day', ...)</c> day bucketing,
    /// <c>DISTINCT ON (d)</c> for the per-day top wait, <c>FILTER</c> conditional
    /// counts, and a day spine (UNION of every source's days) LEFT JOINed so a quiet-but-collected day still
    /// appears (Healthy, not No-Data). The high-CPU rule (total host CPU = SQL + other-process &gt;= 80, Linux
    /// NULL-other-process fallback) mirrors the alert engine and the Overview headline.
    /// The per-wait sums merge spellings on <c>rtrim(wait_type)</c>: SQL Server reports a few wait names with a
    /// trailing space, which the collector stores trimmed from #4884 on, so a day that spans the upgrade sums both
    /// spellings under the clean name before it picks the top wait. <c>wait_per_spelling</c> is the per-day,
    /// per-stored-name aggregation this read always ran; <c>wait_per_type</c> merges its groups, so the trim runs
    /// once per group rather than once per row.
    /// </summary>
    private const string RangeSqlTemplate = $"""
        WITH wait_per_spelling AS (
            SELECT date_trunc('day', collection_time) AS d, wait_type, SUM(delta_wait_time_ms) AS ms
            FROM v_wait_stats
            WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3 AND delta_wait_time_ms > 0
            GROUP BY 1, 2
        ),
        wait_per_type AS (
            SELECT d, rtrim(wait_type) AS wait_type, SUM(ms) AS ms
            FROM wait_per_spelling
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
            /* #3905: distinct (day, hash) pairs, then counted -- the distinct-hash count per day, in a shape
               the planner can hash rather than sort. See DailySummarySql.QueriesCteRaw. */
            SELECT x.d, COUNT(x.query_hash) AS c
            FROM (
                SELECT DISTINCT date_trunc('day', collection_time) AS d, query_hash
                /* #7147: collect.query_stats, not the v_query_stats view: the view's payload joins keep the planner off an
                   index-only scan even though this read needs only server_id, collection_time and query_hash.
                   #8651: unique queries counts queries with credited executions in the window, so a restart row
                   (sample_interval_seconds = 0: a first sighting, an uncredited reset or a gap) does not count its hash. A NULL
                   interval (a row from before the column) passes, the rule the interval rollups' own WHERE states. */
                FROM collect.query_stats
                WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
                  AND sample_interval_seconds IS DISTINCT FROM 0
            ) AS x
            GROUP BY x.d
        ),
        deadlocks AS (
            /* Windowed and day-bucketed on when the deadlock HAPPENED, so a deadlock at 23:50 that was collected
               at 00:10 counts on the day it happened. $4 is the EventWindowFloor for $2: a partition-column bound
               with no upper limit, so an event collected late still counts. */
            SELECT date_trunc('day', deadlock_time) AS d, COUNT(*) AS c
            FROM v_deadlocks
            WHERE server_id = $1 AND deadlock_time >= $2 AND deadlock_time < $3
            AND   collection_time >= $4
            GROUP BY 1
        ),
        /*blocking-waits*/
        bpr AS (
            /* #7028: the day's blocking WAITS, chosen per hour as the blocking alerts choose them for their rolling
               hour (BlockingWaits.HourlyChosenWaitsSql): each hour takes its blocked-process-report waits when it has
               any, else its DMV snapshot waits, so an hour that paged on DMV waits alone still counts in a day that
               holds reports in another hour. c is the count the band bands and every surface shows, max_wait_ms
               the longest of those waits. A wait lands on the day of its representative report (XE: event_time,
               DMV: collection_time). xe_c counts the waits that came from reports: an hour that holds any report
               waits chooses them, so a day holds blocked-process reports exactly when xe_c > 0, whatever else the
               hour-choice picked. */
            SELECT date_trunc('day', event_time) AS d, COUNT(*) AS c, MAX(wait_time_ms) AS max_wait_ms,
                   COUNT(*) FILTER (WHERE from_xe) AS xe_c
            FROM blk
            GROUP BY 1
        ),
        dmv AS (
            /* #7028: the snapshot arm's value per day, from the gated chain above (dmv_gated). */
            SELECT d, peak_snapshot_wait_ms FROM dmv_gated
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
        /* #8549: coll counts runs and errors per day, and it read the raw log for the whole window. On a busy store the
           uncompressed chunks hold every server's rows on shared pages, so one server's rows sit two to a heap block and
           a bitmap scan of one chunk read about 4,000 blocks (5.5 s cold for the 30-day window, 19,800 blocks for 40,400
           rows). The hourly rollup (V186, collect.collection_log_hourly, the table the 7-day health read uses) holds
           the same two counts per closed hour, from the same cycle-row exclusion. It keeps 31 days plus 3 hours
           (CollectionLogHourly.RetentionHours), exactly this window, so closed hours come from it, summed per day, and the raw
           log is read only for the hours it does not hold: the window's partial edge hours, any closed hour the rollup pass has
           not reached or failed on (the viewer reads the store directly, so that happens, and a store catches up a day per pass
           after an upgrade), and the open hour. Until the table kept the month, the older hours were read raw on the
           assumption that compressed chunks are cheap: on a field store they were not (109 chunks decompressed, 735,000 rows,
           771 ms of a 2.6 s read). The same island shape as CollectionHealthStatements.ComposedSql,
           bounded by the window on both ends. A rolled hour's row holds every collector that ran in it, so an hour with
           no row is read raw and found empty, which is the raw answer. */
        coll_rolled AS MATERIALIZED (
            SELECT hr.hour_start, SUM(hr.total_runs) AS runs, SUM(hr.error_count) AS errs
            FROM collect.collection_log_hourly AS hr
            WHERE hr.server_id = $1 AND hr.hour_start >= $2 AND hr.hour_start <= $3 - INTERVAL '1 hour'
            GROUP BY hr.hour_start
        ),
        coll_raw_ranges AS (
            /* The window's hours the rollup does not hold, as islands of consecutive hours, clipped to the window. */
            SELECT GREATEST(MIN(g.hour_start), $2) AS range_start,
                   LEAST(MAX(g.hour_start) + INTERVAL '1 hour', $3) AS range_end
            FROM (
                SELECT s.hour_start,
                       s.hour_start - ROW_NUMBER() OVER (ORDER BY s.hour_start) * INTERVAL '1 hour' AS island
                FROM generate_series(date_trunc('hour', $2::timestamp), date_trunc('hour', $3::timestamp - INTERVAL '1 microsecond'), INTERVAL '1 hour') AS s(hour_start)
                WHERE NOT EXISTS (SELECT 1 FROM coll_rolled AS r WHERE r.hour_start = s.hour_start)
            ) AS g
            GROUP BY g.island
        ),
        coll_raw AS (
            /* #5506's bounded probe per range: OFFSET 0 keeps the planner from flattening the LATERAL into a join over
               the server's whole window, the plan that read every uncompressed chunk. */
            SELECT date_trunc('day', l.collection_time) AS d,
                   COUNT(*) AS runs,
                   COUNT(*) FILTER (WHERE status = 'ERROR') AS errs
            FROM coll_raw_ranges AS rr
            CROSS JOIN LATERAL (
                SELECT x.collection_time, x.status
                FROM v_collection_log AS x
                WHERE x.server_id = $1
                AND   x.collection_time >= rr.range_start AND x.collection_time < rr.range_end
                AND   x.{CollectionCycleRow.NotCycleRowSql}
                OFFSET 0
            ) AS l
            GROUP BY 1
        ),
        coll AS (
            /* Any run (all statuses) marks the day as collected -> it appears even if every metric is quiet
               (a quiet monitored day is Healthy/green, not No-Data/grey). runs is also the denominator the
               error SHARE bands on (#3539 A2); errs alone used to make the day Critical on presence. */
            SELECT p.d, SUM(p.runs)::bigint AS runs, SUM(p.errs)::bigint AS errs
            FROM (
                SELECT date_trunc('day', hour_start) AS d, runs, errs FROM coll_rolled
                UNION ALL
                SELECT d, runs, errs FROM coll_raw
            ) AS p
            GROUP BY p.d
        ),
        mem AS (
            /* #7691: one event is one (second, notification), however many memory nodes reported it. The distinct pairs come from one GROUP BY
               (hashed), never from a DISTINCT aggregate, which PostgreSQL sorts. An event counts in a bar when any of its records meets the bar. */
            SELECT ev.d,
                   COUNT(*) FILTER (WHERE ev.pressured) AS pressure,
                   COUNT(*) FILTER (WHERE ev.criticalp) AS critical
            FROM (
                SELECT date_trunc('day', collection_time) AS d,
                       BOOL_OR(memory_indicators_process >= 2 OR memory_indicators_system >= 2) AS pressured,
                       BOOL_OR(memory_indicators_process >= 3) AS criticalp
                FROM v_memory_pressure_events
                WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
                GROUP BY date_trunc('day', collection_time), date_trunc('second', sample_time), memory_notification
            ) ev
            GROUP BY ev.d
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
            COALESCE(b.c, 0) AS blocking_events,
            COALESCE(cp.c, 0) AS high_cpu_events,
            COALESCE(cl.errs, 0) AS collection_errors,
            COALESCE(m.pressure, 0) AS memory_pressure_events,
            COALESCE(m.critical, 0) AS memory_critical_events,
            COALESCE(al.c, 0) AS alert_count,
            /* Peak block wait (ms): the longest of the waits blocking_events counts (#7028), so the day-detail blocking
               reason ('N blocking events (peak block X)') reconciles with the count. 0 when the day has none. */
            COALESCE(b.max_wait_ms, 0) AS peak_block_wait_ms,
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
              /* Blocking is TWO sources, as the retention notes' "N of 7" counts them: the blocked-process reports and the
                 DMV snapshots are separate tables with their own retention (#7028). The chosen waits (b.d) can come from
                 either side, so the reports arm reads xe_c, the report-sourced waits, and a DMV-only day does not count
                 as holding reports. dm.d is non-null on every day that holds a snapshot, whatever its gated value (the
                 gate keeps one row per day with an in-window snapshot, and a low value is 0, not no row). */
              + (CASE WHEN COALESCE(b.xe_c, 0) = 0 THEN 0 ELSE 1 END)
              + (CASE WHEN dm.d IS NULL THEN 0 ELSE 1 END)
              + (CASE WHEN cp.d IS NULL THEN 0 ELSE 1 END)
              + (CASE WHEN m.d IS NULL THEN 0 ELSE 1 END) AS signal_sources_present,
            /* #7028: the blocking band's snapshot arm, the Blocking Wait Time alert's own measure: the largest total
               blocked wait of one DMV snapshot in the day after the alert's persistence gate (three adjacent snapshots,
               or one at three times the bar), in milliseconds, 0 when the day has no snapshot. Replaces
               blocking_reports (#5644), which the band no longer reads: it bands on blocking_events (waits). Appended
               LAST, after signal_sources_present, so every existing ordinal read stays where it was. */
            COALESCE(CAST(ROUND(dm.peak_snapshot_wait_ms) AS bigint), 0) AS peak_snapshot_wait_ms
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
    /// <see cref="RangeSqlTemplate"/> with the blocking chains spliced in. The day's <c>blocking_events</c> counts
    /// blocking WAITS, as the alert value, the fleet "Blocking" tile and the MCP counts do
    /// (<see cref="BlockingWaits.WaitsCteSql"/>), not blocked-process report rows (SQL Server writes about eleven per
    /// one-minute wait). #7028: the waits are chosen per HOUR (<see cref="BlockingWaits.HourlyChosenWaitsSql"/>,
    /// buckets anchored at the day start), the way the blocking alerts choose them for their rolling hour, and the
    /// day also carries the <c>Blocking Wait Time</c> alert's measure, the largest gated DMV snapshot total
    /// (<see cref="BlockingWaits.GatedSnapshotWaitSql"/>). The blocked-process side carries <c>database_name</c> for
    /// <see cref="DailySummaryAzureMasterScope"/>'s filter; the DMV side keeps its collected-day bucket.
    /// </summary>
    public static readonly string RangeSql = RangeSqlTemplate.Replace(
        "/*blocking-waits*/",
        /* The DMV rows are read once, from the lookback floor (a streak that began before $2 is seen), and the waits
           and the gate both take them from here. */
        @"dmv_src AS (
    SELECT server_id, blocked_spid, blocking_spid, event_time, collection_time, wait_time_ms FROM v_dmv_blocking_snapshots
    WHERE server_id = $1 AND collection_time >= " + BlockingSnapshotFloorSql("$2", "$1") + @" AND collection_time < $3
),"
        + BlockingWaits.WaitsCteSql(
            BlockingWaits.SqlDialect.Postgres, "bpr_waits",
            @"SELECT blocked_spid, blocking_spid, event_time, wait_time_ms, database_name FROM v_blocked_process_reports
    " + BprSourceWhere) + "," + BlockingWaits.WaitsCteSql(
            BlockingWaits.SqlDialect.Postgres, "dmv_waits",
            @"SELECT blocked_spid, blocking_spid, event_time, collection_time, wait_time_ms FROM dmv_src
    WHERE collection_time >= $2") + ","
        /* Hourly buckets from a fixed midnight, so every bucket lies inside one day (date_bin on a day-aligned origin
           is date_trunc('hour')); the DMV side buckets on collection_time, as this statement always has. */
        + BlockingWaits.HourlyChosenWaitsSql(
            "blk", "bpr_waits", "dmv_waits", "TIMESTAMP '2000-01-01'", false, null, "event_time", "collection_time") + ","
        + BlockingWaits.GatedSnapshotWaitSql(
            "dmv_gated", "SELECT server_id, collection_time, wait_time_ms, blocked_spid, blocking_spid FROM dmv_src",
            "$2", "$3", false,
            AlertEngine.BlockingWaitBreachSamples, AlertEngine.BlockingWaitSingleSnapshotMultiplier,
            AlertEngine.BlockingWaitEpisodeGapFactor, DefaultSnapshotCadenceMinutes, byDay: true) + ",",
        StringComparison.Ordinal);

    /// <summary>The blocked-process-report source's window, one line: <see cref="DailySummaryAzureMasterScope"/> appends
    /// its keep predicate to it.</summary>
    internal const string BprSourceWhere =
        "WHERE server_id = $1 AND event_time >= $2 AND event_time < $3 AND collection_time >= $4";

    /* A property, not a static readonly field: RangeSql is initialised above it and would read a zero. */
    private static int DefaultSnapshotCadenceMinutes =>
        PerformanceMonitor.Collectors.CollectorScheduleDefaults.All["dmv_blocking_snapshot"].FrequencyMinutes;

    /// <summary>
    /// #7028: where the snapshot reads start for a window that starts at <paramref name="windowStartSql"/>, so a streak
    /// that began before the window is seen (<see cref="BlockingWaits.SnapshotLookbackFloorSql"/> with the alert's own
    /// constants). <paramref name="serverIdSql"/> is the one server's id, or null for the fleet.
    /// </summary>
    public static string BlockingSnapshotFloorSql(string windowStartSql, string? serverIdSql) =>
        BlockingWaits.SnapshotLookbackFloorSql(
            windowStartSql, serverIdSql, AlertEngine.BlockingWaitBreachSamples, AlertEngine.BlockingWaitEpisodeGapFactor,
            DefaultSnapshotCadenceMinutes);

    /// <summary>
    /// #7028: the snapshot arm for a window with the alert's own gate constants and the shipped default cadence, one
    /// definition for the fleet reads and the Viewer Overview (<see cref="BlockingWaits.GatedSnapshotWaitSql"/>).
    /// <paramref name="serverIdSql"/> is the one server's id (<c>$1</c>), or null for every server. The snapshots are read
    /// from the lookback floor up to the window end when one is given (the gate only looks backwards, so a snapshot after
    /// the window can never change a value inside it, and a window in the past would otherwise read every snapshot
    /// through now), bounded the way the gate keeps its rows (<c>&lt;=</c> with <paramref name="endInclusive"/>, else
    /// <c>&lt;</c>). <paramref name="floorSql"/> is the statement's partition-column floor for the
    /// window start (<see cref="EventWindowFloor"/>), which the lookback floor never goes below.
    /// <paramref name="sourceRelation"/> names a relation the caller already reads the snapshots into (<c>server_id</c>,
    /// <c>collection_time</c>, <c>wait_time_ms</c>, <c>blocked_spid</c>, <c>blocking_spid</c>, already scoped to the server
    /// and read from at or before <paramref name="floorSql"/>), so the statement's wait count and this gate share one scan
    /// of the snapshots instead of reading them twice (#7028: the second scan took the fleet card to 1.7 times the
    /// buffers of the statement before the gate).
    /// </summary>
    public static string BlockingSnapshotGateSql(
        string name, string windowStartSql, string? windowEndSql, bool endInclusive, string? serverIdSql, string floorSql,
        string? sourceRelation = null) =>
        BlockingWaits.GatedSnapshotWaitSql(
            name,
            (sourceRelation is null
                ? @"SELECT server_id, collection_time, wait_time_ms, blocked_spid, blocking_spid FROM v_dmv_blocking_snapshots
    WHERE collection_time >= " + floorSql + " AND collection_time >= " + BlockingSnapshotFloorSql(windowStartSql, serverIdSql)
                    + (serverIdSql is null ? string.Empty : " AND server_id = " + serverIdSql)
                : "SELECT server_id, collection_time, wait_time_ms, blocked_spid, blocking_spid FROM " + sourceRelation
                    + " WHERE collection_time >= " + BlockingSnapshotFloorSql(windowStartSql, serverIdSql))
            + (windowEndSql is null ? string.Empty : " AND collection_time " + (endInclusive ? "<=" : "<") + " " + windowEndSql),
            windowStartSql, windowEndSql, endInclusive,
            AlertEngine.BlockingWaitBreachSamples, AlertEngine.BlockingWaitSingleSnapshotMultiplier,
            AlertEngine.BlockingWaitEpisodeGapFactor, DefaultSnapshotCadenceMinutes);

    /// <summary>
    /// The <c>queries</c> CTE exactly as it appears in <see cref="RangeSql"/> — the ONE part of the
    /// daily summary that reads a table subject to the 4-day raw drop. Every other source here (wait_stats,
    /// deadlocks, alerts, memory) keeps <c>DarlingRetention</c>'s 30-day default, so they need no routing.
    ///
    /// <para><b>No other CTE has a tier to route to (#3905, checked rather than assumed).</b> The continuous
    /// aggregates over this statement's other sources are the anomaly baseline's per-collection supplies and
    /// the fleet card's collection-health rollup, and none of them answers its CTE exactly: the wait supply
    /// sums every wait type into one figure (no top wait), the blocked-process supply counts reports but
    /// carries no wait time (no peak block), the deadlock supply's 35-day horizon is shorter than a raw
    /// retention an operator may lengthen, and <c>collection_health_hourly</c> keeps eight days of a month.
    /// CPU, memory pressure and the DMV blocking snapshots have no aggregate at all. The eight raw CTEs are a
    /// bounded per-server read of the window; what made the month read slow was this CTE's routed form (the
    /// not-carried probes, <see cref="QueriesCteForCagg"/>) and the sort its <c>COUNT(DISTINCT)</c> forced,
    /// both fixed here.</para>
    ///
    /// <para><b>Distinct pairs, then counted (#3905).</b> <c>COUNT(x.query_hash)</c> over the distinct
    /// <c>(day, query_hash)</c> pairs is <c>COUNT(DISTINCT query_hash)</c> per day, NULL hash included (both
    /// skip it), but PostgreSQL evaluates a DISTINCT aggregate by SORTING every input row of its group, while
    /// a <c>SELECT DISTINCT</c> can be hashed. On a busy server's month of hourly rollup rows that sort was
    /// the calendar's largest remaining cost once the probes were fixed: 1.10 s for 573,600 rows on the rig
    /// against 0.24 s for the same rows hashed, and 140 ms against 48 ms for a single raw day. The same shape
    /// is used on every tier, so every tier still counts the same thing.</para>
    /// </summary>
    private const string QueriesCteRaw = """
        queries AS (
            /* #3905: distinct (day, hash) pairs, then counted -- the distinct-hash count per day, in a shape
               the planner can hash rather than sort. See DailySummarySql.QueriesCteRaw. */
            SELECT x.d, COUNT(x.query_hash) AS c
            FROM (
                SELECT DISTINCT date_trunc('day', collection_time) AS d, query_hash
                /* #7147: collect.query_stats, not the v_query_stats view: the view's payload joins keep the planner off an
                   index-only scan even though this read needs only server_id, collection_time and query_hash.
                   #8651: unique queries counts queries with credited executions in the window, so a restart row
                   (sample_interval_seconds = 0: a first sighting, an uncredited reset or a gap) does not count its hash. A NULL
                   interval (a row from before the column) passes, the rule the interval rollups' own WHERE states. */
                FROM collect.query_stats
                WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
                  AND sample_interval_seconds IS DISTINCT FROM 0
            ) AS x
            GROUP BY x.d
        ),
        """;

    /// <summary>
    /// The row predicate of the full-window raw queries read, shared by the credited-pairs shape below.
    /// </summary>
    private const string RawRowPredicate = "server_id = $1 AND collection_time >= $2 AND collection_time < $3";

    /// <summary>
    /// The distinct <c>(day, query_hash)</c> pairs of <paramref name="relation"/> under <paramref name="rowPredicate"/> that have a
    /// credited execution, in a shape that keeps the pair read index-only (#8651).
    ///
    /// <para>The simple form, <c>AND sample_interval_seconds IS DISTINCT FROM 0</c> on the pair read, names a column
    /// <c>idx_query_stats_server_hash_time</c> does not hold, so the read left its index-only scan for heap fetches (a busy server's
    /// open day: 252 ms warm to 432 ms, 2,080 heap fetches to a bitmap heap scan over 39,228 heap blocks). This shape reads the
    /// pairs with no interval column, as the count did before, and takes away the restart-only pairs: a pair is restart-only when
    /// the day holds a restart row for the hash (<c>sample_interval_seconds = 0</c>, read through the partial index
    /// <c>idx_query_stats_restart_rows_time</c>, about 1,600 rows a day across 42 servers) and no credited row for it under the same
    /// <paramref name="rowPredicate"/>. The credited probe seeks <c>idx_query_stats_server_hash_time</c> by hash and day. A NULL
    /// interval counts as credited, as before. <paramref name="rowPredicate"/> is written once and used in all three reads, and its
    /// columns are unqualified so each read binds them to its own table. It must hold <c>server_id = $1</c>.</para>
    ///
    /// <para>A compressed chunk has no partial index, so the restart read would decompress every day again. The reads that span
    /// compressed chunks keep the simple form (<see cref="QueriesCteRaw"/>).</para>
    /// </summary>
    private static string CreditedPairs(string relation, string rowPredicate) => $"""
        SELECT p.d, p.query_hash
                FROM (
                    SELECT DISTINCT date_trunc('day', collection_time) AS d, query_hash
                    FROM {relation}
                    WHERE {rowPredicate}
                ) AS p
                LEFT JOIN (
                    SELECT r0.d, r0.query_hash
                    FROM (
                        SELECT DISTINCT date_trunc('day', collection_time) AS d, query_hash
                        FROM collect.query_stats
                        WHERE {rowPredicate}
                          AND sample_interval_seconds = 0
                    ) AS r0
                    LEFT JOIN LATERAL (
                        SELECT 1 AS credited
                        FROM collect.query_stats AS k
                        WHERE k.server_id = $1 AND k.query_hash = r0.query_hash
                          AND k.collection_time >= r0.d AND k.collection_time < r0.d + INTERVAL '1 day'
                          AND {rowPredicate}
                          AND k.sample_interval_seconds IS DISTINCT FROM 0
                        LIMIT 1
                    ) AS c ON TRUE
                    WHERE c.credited IS NULL
                ) AS ro ON ro.d = p.d AND ro.query_hash = p.query_hash
                WHERE ro.d IS NULL
        """;

    /// <summary>
    /// The queries CTE for a store with no TimescaleDB rollups, where every chunk is uncompressed and the restart rows have their
    /// partial index (#8651). Same result as <see cref="QueriesCteRaw"/>, with the index-only pair read of <see cref="CreditedPairs"/>.
    /// </summary>
    private static readonly string QueriesCteRawUncompressed = $"""
        queries AS (
            /* #3905: distinct (day, hash) pairs, then counted. #8651: the pairs with a credited execution, see CreditedPairs. */
            SELECT x.d, COUNT(x.query_hash) AS c
            FROM (
                {CreditedPairs("collect.query_stats", RawRowPredicate)}
            ) AS x
            GROUP BY x.d
        ),
        """;

    /// <summary>
    /// <see cref="RangeSql"/> for a store with no rollups at all (plain PostgreSQL, so no compressed chunk): its queries CTE reads
    /// the pairs through <see cref="CreditedPairs"/>. The same text as <see cref="RangeSql"/> otherwise.
    /// </summary>
    internal static readonly string RangeSqlPlainStore = RangeSql.Replace(QueriesCteRaw, QueriesCteRawUncompressed, StringComparison.Ordinal);

    /// <summary>
    /// The same CTE against a rollup. <c>COUNT(DISTINCT query_hash)</c> is exact over a CAGG because query_hash is
    /// one of its GROUP BY columns — the rollup preserves every distinct hash per bucket, so counting them per day
    /// gives the count of distinct hashes the rollup's source rows produced. Raw skips restart rows
    /// (<c>sample_interval_seconds IS DISTINCT FROM 0</c>) in this count, as the interval-honest successor's own
    /// <c>WHERE</c> does (#8651), so raw and the successor agree. The legacy rollups cannot skip them: they carry no
    /// per-hash column that tells a restart row apart after aggregation, so a legacy rollup still counts a hash whose only
    /// rows that day are restart rows (369 against 362 on one store, #7505). The time column becomes <c>bucket</c>; parameter positions are
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
    /// materialize tomorrow. No day has two rows in <c>queries</c> (a <c>COUNT(DISTINCT)</c> cannot be summed
    /// across halves). Bounded by shape: the ceiling is one indexed <c>max(bucket)</c> per server and the raw
    /// half is a day or two of one server.</para>
    ///
    /// <para><b>A day the rollup holds only part of reads raw too, and the larger count wins (#7505).</b> An
    /// hourly rollup runs behind the clock: its refresh runs once an hour and its <c>end_offset</c> is an hour,
    /// so its newest bucket can be two hours old. Split on the ceiling DAY alone, today read that partial
    /// rollup: after a burst of new queries the month read showed 1 where the three-day read, which answers
    /// from raw, showed 2,445. The ceiling now also carries <c>raw_from</c>, the day in which the newest bucket
    /// ENDS. It is the day after the ceiling day when the newest bucket is the day's last (a daily rollup always,
    /// an hourly one at 23:00), so a whole-bucket tier is read exactly as before. It is the ceiling day itself
    /// when the newest bucket ends inside the day, and raw is then read from that day too. The two halves meet
    /// on that one day, and <c>MAX</c> per day keeps one row: each half counts hashes that did appear, so the
    /// larger is the closer. Raw's wins when the rollup has not caught up. The rollup's wins when raw has lost
    /// the day, either all of it (a rollup that stopped refreshing for longer than raw's four days keeps its
    /// partial count and never prints 0) or part of it (a chunk dropped by retention). A quiet server whose
    /// last closed hour has no bucket reads raw for that day while raw holds it, and keeps the rollup's count
    /// after.</para>
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
    /// and its filter are read off <see cref="TimescaleSupport.RollupCoverageProbeTargets"/> — the repair's own
    /// target list, plus the six the freeze (#3653 LC) took off it, since a frozen legacy rollup is still a
    /// valid relation for this probe to name — so the daily tier probes <c>query_stats_hourly</c> (its
    /// hierarchical source, 90 days),
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
    /// the day.</para>
    ///
    /// <para><b>Each probe is one index range per day, and the SQL has to say so (#3905).</b> This paragraph
    /// used to end "each probe is one index range per day, so the cost follows the window's length and not the
    /// tables' weight". That was the intent, not the plan. Written as a bare <c>EXISTS</c> and <c>NOT EXISTS</c>,
    /// PostgreSQL pulls both up into a semi-join and an anti-join, and neither join can push its per-day range
    /// into the scan, so the planner materialized the server's ENTIRE history in the probed relation (the
    /// source's four days of raw rows, or the rollup's 90 days) and rescanned it once per day of the window.
    /// That is O(days × retained rows), and past <c>work_mem</c> it spills and rereads a temp file per day. On
    /// the rig (one busy server: 570,600 raw rows, 880,800 hourly) this member alone was 4.5 s of a cold 6.1 s
    /// month read, 32,678 temp blocks read, and a 120-day read at the daily tier, whose source probe runs over
    /// the hourly's 90 days, took 12.4 s. On DARLING01's busiest server it was 629 of 873 ms. So each probe now
    /// carries <c>OFFSET 0</c>. PostgreSQL refuses to pull a sub-select with an OFFSET into a join
    /// (<c>simplify_EXISTS_query</c>: "OFFSET 0 ... traditionally is used as an optimization fence"), which
    /// leaves each probe a SubPlan run once per candidate day: an index range on the materialization's
    /// <c>(server_id, bucket)</c> index and on the source's time index, pruned to one chunk at run time. The
    /// candidate days are fenced the same way, so the source probe runs only for the days the rollup probe
    /// found empty, which on a covered window is none. At that scale the member is now 10 ms, the month read
    /// 186 ms and the 120-day read 161 ms, whatever the relations weigh.</para>
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
    private static string QueriesCteForCagg(string relation, string? heldDaily = null)
    {
        /* The repair's own target row for this relation: the source it aggregates, that source's time column and
           the CREATE its filter is read from. Looked up rather than restated so the calendar's idea of "what the
           rollup reads" cannot drift from the scan's. A relation the registry does not know is refused here,
           before it reaches the store as a 42P01. */
        var target = TimescaleSupport.RollupCoverageProbeTargets
            .FirstOrDefault(t => string.Equals(t.View, relation, StringComparison.Ordinal));
        if (target.View is null)
        {
            throw new ArgumentException(
                $"'{relation}' is not a registered continuous aggregate (TimescaleSupport.RollupCoverageProbeTargets), so the daily summary cannot name its source or the days it did not carry (#3653 A6).",
                nameof(relation));
        }

        var filter = TimescaleSupport.MaterializationHoleSourceFilterFor(target.CreateSql);
        var sourceFilter = filter.Length == 0 ? string.Empty : $"\n          AND {filter}";

        var width = (long)target.BucketWidth.TotalSeconds;

        /* #8549: a daily whose source is itself a rollup (the hourly it is hierarchical from) reads the days it lacks from that
           hourly for the hours the hourly holds, and from raw only for the rest. The hourly tier's relations read raw as their
           source, so there is nothing between them and raw and their text is unchanged. */
        var hourlySource = IsRollup(target.Source) ? target.Source : null;
        var closedHours = hourlySource is null
            ? string.Empty
            : ClosedHoursMember(
                "(SELECT raw_from FROM queries_ceiling)",
                $"EXISTS (SELECT 1 FROM collect.{hourlySource} AS s WHERE s.server_id = $1 AND s.bucket = g.h OFFSET 0)",
                (hourlySource, string.Empty));
        var rawTailGate = hourlySource is null ? string.Empty : "\n                  AND (SELECT raw_from FROM queries_ceiling) IS NULL";

        var tailRowPredicate =
            $"server_id = $1 AND collection_time >= $2 AND collection_time < $3 AND collection_time >= COALESCE((SELECT raw_from FROM queries_ceiling), $2){rawTailGate}";

        /* #8549: the rollup half's (day, query_hash) pairs. With a daily to read, the closed days it holds come from it. */
        var rollupPairs = heldDaily is null
            ? $"""
                SELECT DISTINCT date_trunc('day', bucket) AS d, query_hash
                        FROM collect.{relation}
                        WHERE server_id = $1 AND bucket >= $2 AND bucket < $3
                """
            : HeldDailyPairs(relation, heldDaily, boundary: null).Pairs;
        var dailyCtes = heldDaily is null ? string.Empty : HeldDailyPairs(relation, heldDaily, boundary: null).Ctes;

        return $"""
        /* #7505: last_day is the newest day the rollup holds a bucket for. raw_from is the first day the rollup
           does NOT hold whole: the day in which the newest bucket ENDS. For a daily rollup, or an hourly one whose
           newest bucket is 23:00, that is the day after last_day. For an hourly rollup that is still behind the
           clock (its refresh runs an hour apart and stops an hour short of now, so it can lag two hours), the newest
           bucket ends inside last_day, so raw_from = last_day and that day is partial. */
        {dailyCtes}queries_ceiling AS (
            SELECT date_trunc('day', max(bucket)) AS last_day,
                   date_trunc('day', max(bucket) + INTERVAL '{width} seconds') AS raw_from
            FROM collect.{relation}
            WHERE server_id = $1
        ),
        queries AS (
            /* One row per day. The rollup half and the raw half overlap on exactly one day, the partial one, and
               two distinct counts of the halves cannot be added. Each half is a count of hashes that really
               appeared that day, so the larger is the closer: the rollup's when raw has lost part of the day
               (retention), raw's when the rollup has not caught up (#7505). */
            SELECT u.d, MAX(u.c) AS c
            FROM (
            SELECT x.d, COUNT(x.query_hash) AS c
            FROM (
                {rollupPairs}
            ) AS x
            GROUP BY x.d
            UNION ALL
            SELECT x.d, COUNT(x.query_hash) AS c
            FROM (
                /* #8651: raw counts only queries with credited executions, as the interval rollups do. The open day and the hours
                   neither rollup holds are uncompressed, so the pairs are read index-only and the restart-only ones taken away.
                   See CreditedPairs. */
                {CreditedPairs("collect.query_stats", tailRowPredicate)}{closedHours}
            ) AS x
            GROUP BY x.d
            UNION ALL
            /* #3653 A6: the days at or below this server's ceiling that the rollup holds NO row for while its
               source still holds rows the rollup's own WHERE admits -- "not carried at this tier". One row per
               such day with a NULL count; the outer select passes it through as unique_queries = NULL and the
               readers list the day in days_missing[]. The two probes are the hole scan's
               (TimescaleSupport.MaterializationHoleScanSql, the #3731 repair), per server, at day grain. A day
               the source has no admitted row for is not named: the rollup is honestly empty there.
               #3905: every OFFSET 0 below is load-bearing. It keeps each probe a per-day index probe instead of
               a join over the server's whole history, and the source is probed only on the days the rollup
               probe found empty. */
            SELECT b.d, NULL::bigint AS c
            FROM (
                SELECT g.d
                FROM generate_series(date_trunc('day', $2::timestamp), date_trunc('day', $3::timestamp), INTERVAL '1 day') AS g(d)
                WHERE g.d < $3
                  AND g.d < COALESCE((SELECT last_day + INTERVAL '1 day' FROM queries_ceiling), $2)
                  AND NOT EXISTS (
                    SELECT 1 FROM collect.{relation} AS r
                    WHERE r.server_id = $1 AND r.bucket >= g.d AND r.bucket < g.d + INTERVAL '1 day'
                    OFFSET 0)
                OFFSET 0
            ) AS b
            WHERE EXISTS (
                SELECT 1 FROM collect.{target.Source} AS s
                WHERE s.server_id = $1 AND s.{target.SourceTimeColumn} >= b.d AND s.{target.SourceTimeColumn} < b.d + INTERVAL '1 day'{sourceFilter}
                OFFSET 0)
            ) AS u
            GROUP BY u.d
        ),
        """;
    }

    /// <summary>
    /// The (day, query_hash) pairs of an interval-honest hourly relation, counting each CLOSED day from its daily relation instead (#8549). A
    /// distinct over a month of the hourly rollup sorted about 6.3 million rows and spilled to disk on the busiest server of a test store, most
    /// of the month read. The daily holds the same pairs in about 24 times fewer rows, because it is built from the hourly by
    /// <c>time_bucket('1 day')</c> over the same columns. Returns the CTE that reads the daily and the pairs that read the CTE.
    ///
    /// <para><b>When a day comes from the daily.</b> A day is held when it lies wholly inside <c>[$2, $3)</c> and the daily has a bucket for the
    /// server at that midnight, the same coverage rule <c>collect.wait_stats_daily</c> follows for the wait totals. A daily bucket is written
    /// whole, from the hourly buckets of the day, so one row proves the day. The daily refresh ends a day short of now, so yesterday is not held
    /// until the next UTC midnight: it is read from the hourly with the open day, and the raw tail that follows is unchanged. A window edge that
    /// cuts a day, a hole in the middle and a day the daily has not reached all read the hourly, which gives that day exactly the pairs it gave
    /// before.</para>
    ///
    /// <para><b>Shape.</b> One range read of the daily over the window gives every held day's pairs at once (the closed days are all one range, so a
    /// probe per day would scan the daily's chunk once a day, as a first attempt did: 215 ms of a 275 ms month read on a test store). The days that
    /// range holds are the held days, a small list. The day list over the window, with those days taken out, drives one lateral read of the hourly
    /// per remaining day, clamped to the window, with <c>OFFSET 0</c> so the planner keeps the list as it is. <paramref name="boundary"/> (a
    /// day-aligned timestamp literal) keeps the stitched successor half to the days from the stitch boundary on, as its hourly read already was.
    /// The bucket's upper bound is written <c>bucket &lt;= $3 - 1 day</c>, the same test as the bucket plus a day ending inside the window, so the
    /// store's index serves it.</para>
    /// </summary>
    private static (string Ctes, string Pairs) HeldDailyPairs(string hourly, string daily, string? boundary)
    {
        var dayStart = boundary is null ? "$2::timestamp" : $"GREATEST($2::timestamp, {boundary})";
        var bucketBound = boundary is null ? string.Empty : $" AND bucket >= {boundary}";
        var hourlyBound = boundary is null ? string.Empty : $" AND h.bucket >= {boundary}";

        var ctes = $"""
        /* #8549: the closed days the daily holds, as (day, query_hash) pairs, and the days they cover. See HeldDailyPairs. */
        queries_daily_pairs AS (
            SELECT DISTINCT bucket AS d, query_hash
            FROM collect.{daily}
            WHERE server_id = $1 AND bucket >= $2 AND bucket <= $3 - INTERVAL '1 day'{bucketBound}
        ),
        queries_daily_days AS (
            SELECT DISTINCT d FROM queries_daily_pairs
        ),

        """;

        var pairs = $"""
                SELECT d, query_hash FROM queries_daily_pairs
                        UNION ALL
                        SELECT r.d, r.query_hash
                        FROM (
                            SELECT g.d
                            FROM generate_series(date_trunc('day', {dayStart}), $3::timestamp - INTERVAL '1 microsecond', INTERVAL '1 day') AS g(d)
                            WHERE NOT EXISTS (SELECT 1 FROM queries_daily_days AS k WHERE k.d = g.d)
                            OFFSET 0
                        ) AS p
                        CROSS JOIN LATERAL (
                            SELECT DISTINCT p.d AS d, h.query_hash
                            FROM collect.{hourly} AS h
                            WHERE h.server_id = $1 AND h.bucket >= GREATEST(p.d, $2) AND h.bucket < LEAST(p.d + INTERVAL '1 day', $3){hourlyBound}
                        ) AS r
                """;

        return (ctes, pairs);
    }

    /// <summary>Whether <paramref name="view"/> is a registered rollup, so a daily whose source is that name is hierarchical from it (#8549).</summary>
    private static bool IsRollup(string view)
        => TimescaleSupport.RollupCoverageProbeTargets.Any(t => string.Equals(t.View, view, StringComparison.Ordinal));

    /// <summary>
    /// The member of the daily <c>queries</c> CTE that counts the days at and after the daily rollup's ceiling (#8549). The daily's refresh
    /// leaves yesterday out until the next UTC midnight, so for a day it lacks, a closed day, the old raw tail read every raw row of
    /// the day: 381 of the 482 buffers of a 30-day read on a test store. This member reads the HOURLY rollup for the hours it holds and raw
    /// only for the hours neither rollup holds, plus the open day's last hours (the hourly stops an hour short of now).
    ///
    /// <para><b>The grid and the held test.</b> One row per hour from <paramref name="rawFrom"/>, the first day the daily does not hold whole,
    /// to the end of the window. An hour is held when it lies wholly inside <c>[$2, $3)</c> and <paramref name="heldProbe"/> finds an
    /// hourly bucket for the server at that hour. A window edge that cuts an hour reads that hour from raw, clamped to the window, because
    /// the rollup cannot split an hour. Every other hour with a bucket is read from the hourly by one range, so the hourly is never
    /// probed per row. The probe carries <c>OFFSET 0</c> so each hour is one index probe and the grid is not joined into the hourly's
    /// history, the same fence as the not-carried probes (#3905).</para>
    ///
    /// <para><b>One count per day over both halves.</b> A day can hold held hours and raw hours, and two counts of distinct hashes cannot
    /// be added, so the two halves are combined with <c>UNION</c> (which removes a hash seen in both) before the one <c>COUNT</c> per day.
    /// The member returns only those pairs, to be placed inside the raw tail's pair set: the tail's gate is closed whenever this grid is
    /// not empty, so the two never overlap, and the tail's one <c>COUNT(x.query_hash)</c> per day (a count of distinct pairs, never a
    /// <c>COUNT(DISTINCT</c>) counts them. Days past the ceiling never meet the daily half, which ends at the ceiling, or the not-carried
    /// member, which stops there too.</para>
    ///
    /// <para>With no ceiling (<paramref name="rawFrom"/> NULL, an empty daily) the grid is empty and the raw tail reads the window as before:
    /// reading a month of the hourly for a daily that holds nothing is the read this member avoids.</para>
    /// </summary>
    private static string ClosedHoursMember(string rawFrom, string heldProbe, params (string Relation, string Predicate)[] hourlyReads)
    {
        var hourly = string.Concat(hourlyReads.Select(read => $"""
                SELECT date_trunc('day', s.bucket) AS d, s.query_hash
                FROM collect.{read.Relation} AS s
                WHERE s.server_id = $1 AND s.bucket >= $2 AND s.bucket + INTERVAL '1 hour' <= $3
                  AND s.bucket >= {rawFrom}{read.Predicate}
                UNION

            """));

        /* #8651: the raw hours' rows, as one predicate for CreditedPairs. Each hour sits inside one day, so the pairs an hour
           yields are that hour's own, and the union of the hours is the day's credited pairs. */
        const string hourRowPredicate = "server_id = $1 AND collection_time >= GREATEST(q.h, $2) AND collection_time < LEAST(q.h + INTERVAL '1 hour', $3)";

        return "\n" + $"""
            /* #8549: the hours at and after the daily's ceiling. A held hour is read from the hourly rollup, every other hour from raw,
               and the pairs are UNIONed with the raw tail's (empty here, its gate is closed) under the tail's one count per day. See ClosedHoursMember.
               The interval hourly's count skips restart rows (sample_interval_seconds = 0) by design: unique queries counts queries with credited
               executions in the window (#8651). */
            UNION
            {hourly}                SELECT r.d, r.query_hash
                FROM (
                    SELECT g.h,
                           (g.h >= $2 AND g.h + INTERVAL '1 hour' <= $3 AND {heldProbe}) AS held
                    FROM generate_series(date_trunc('hour', GREATEST($2::timestamp, {rawFrom})), $3::timestamp - INTERVAL '1 microsecond', INTERVAL '1 hour') AS g(h)
                    WHERE {rawFrom} IS NOT NULL
                    OFFSET 0
                ) AS q
                CROSS JOIN LATERAL (
                    {CreditedPairs("collect.query_stats", hourRowPredicate)}
                ) AS r
                WHERE NOT q.held
            """;
    }

    /// <summary>
    /// THE STITCHED not-carried probe (#3653 A6, lane LA-3b2, decision 3): the same <see cref="QueriesCteForCagg"/>
    /// CTE, but the rollup half AND the not-carried half each run TWICE — once against <paramref name="legacy"/>
    /// restricted to <c>bucket &lt; F</c>, once against <paramref name="successor"/> restricted to
    /// <c>bucket &gt;= F</c> — and every member is UNION ALL'd into one <c>queries</c> CTE, exactly the shape
    /// decision 3 specifies: "run the existing probe TWICE, once per relation NAME, each restricted to its side
    /// of F ... and UNION ALL the two results". <paramref name="successorFloor"/> is the stitch boundary
    /// (<see cref="RollupCoverage.StitchFloor"/>) — the SAME F <see cref="RollupCoverage.StitchedRelationSql"/>
    /// would split the FROM clause at, so the probe's split agrees with the read's split by construction. The
    /// ceiling used to gate the raw fallback is read per side, from each relation's own <c>max(bucket)</c>: the
    /// legacy side's ceiling governs raw fallback for days below F, the successor side's for days at or above F.
    /// Each side keeps its own not-carried source probe (the legacy's has no filter; the successor's carries
    /// its restart-row exclusion, per <see cref="RangeSqlFor(RetentionTier, string)"/>'s own note) — a day's
    /// hole is judged against whichever relation actually owns that day's bucket.
    ///
    /// <para>The split boundary used everywhere below is DAY-ALIGNED, not F itself: <c>boundaryDay</c> is the
    /// first whole day at or after F (F unchanged if F already falls on a day start, else F's date plus one day
    /// — the same rule the daily tier uses for its own F_d). F can land mid-day (production: the successor's
    /// first bucket is whatever hour it started), and every member of this CTE partitions on the boundary DAY,
    /// never on the hour: if the split ran at F's hour, the floor day would supply a row from EACH side
    /// (legacy for its early hours, successor for its later ones) and the calendar would print that day twice
    /// with split counts, since a COUNT(DISTINCT) computed separately on each half cannot be summed across
    /// halves without double-counting hashes seen on both sides of the same day. Aligning to the day means the
    /// legacy alone supplies every hour of the floor day (it is live and holds that day whole, before LC
    /// freezes it), and the successor's first live day is the NEXT calendar day.</para>
    /// </summary>
    private static string QueriesCteForStitchedCagg(string legacy, string successor, DateTime successorFloor, string? successorDaily = null)
    {
        var legacyTarget = TimescaleSupport.RollupCoverageProbeTargets
            .FirstOrDefault(t => string.Equals(t.View, legacy, StringComparison.Ordinal));
        if (legacyTarget.View is null)
        {
            throw new ArgumentException(
                $"'{legacy}' is not a registered continuous aggregate (TimescaleSupport.RollupCoverageProbeTargets), so the daily summary cannot name its source or the days it did not carry (#3653 A6).",
                nameof(legacy));
        }

        var successorTarget = TimescaleSupport.RollupCoverageProbeTargets
            .FirstOrDefault(t => string.Equals(t.View, successor, StringComparison.Ordinal));
        if (successorTarget.View is null)
        {
            throw new ArgumentException(
                $"'{successor}' is not a registered continuous aggregate (TimescaleSupport.RollupCoverageProbeTargets), so the daily summary cannot name its source or the days it did not carry (#3653 A6).",
                nameof(successor));
        }

        var legacyFilter = TimescaleSupport.MaterializationHoleSourceFilterFor(legacyTarget.CreateSql);
        var legacySourceFilter = legacyFilter.Length == 0 ? string.Empty : $"\n          AND {legacyFilter}";
        var successorFilter = TimescaleSupport.MaterializationHoleSourceFilterFor(successorTarget.CreateSql);
        var successorSourceFilter = successorFilter.Length == 0 ? string.Empty : $"\n          AND {successorFilter}";
        var boundaryDay = successorFloor == successorFloor.Date ? successorFloor : successorFloor.Date.AddDays(1);
        /* #8604: invariant culture, so a host whose time separator is not a colon cannot spoil the literal. */
        var boundary = string.Create(CultureInfo.InvariantCulture, $"TIMESTAMP '{boundaryDay:yyyy-MM-dd HH:mm:ss.ffffff}'");
        var legacyWidth = (long)legacyTarget.BucketWidth.TotalSeconds;
        var successorWidth = (long)successorTarget.BucketWidth.TotalSeconds;

        /* #8549: the closed-hours member, when both dailies read an hourly rollup. Hours before the boundary day read the legacy
           hourly and hours from it the successor's, the split the daily halves already make. See QueriesCteForCagg. */
        const string stitchedRawFrom = "COALESCE((SELECT raw_from FROM queries_ceiling_successor), (SELECT raw_from FROM queries_ceiling_legacy))";
        var hierarchical = IsRollup(legacyTarget.Source) && IsRollup(successorTarget.Source);
        var closedHours = hierarchical
            ? ClosedHoursMember(
                stitchedRawFrom,
                $"CASE WHEN g.h < {boundary} THEN EXISTS (SELECT 1 FROM collect.{legacyTarget.Source} AS s WHERE s.server_id = $1 AND s.bucket = g.h OFFSET 0) "
                + $"ELSE EXISTS (SELECT 1 FROM collect.{successorTarget.Source} AS s WHERE s.server_id = $1 AND s.bucket = g.h OFFSET 0) END",
                (legacyTarget.Source, $" AND s.bucket < {boundary}"),
                (successorTarget.Source, $" AND s.bucket >= {boundary}"))
            : string.Empty;
        var rawTailGate = hierarchical ? $"\n                  AND {stitchedRawFrom} IS NULL" : string.Empty;
        var tailRowPredicate =
            $"server_id = $1 AND collection_time >= $2 AND collection_time < $3 AND collection_time >= COALESCE((SELECT raw_from FROM queries_ceiling_successor), (SELECT raw_from FROM queries_ceiling_legacy), $2){rawTailGate}";

        /* #8549: the successor half's (day, query_hash) pairs. The legacy half keeps its hourly read: the successor daily is built from the
           successor hourly and the legacy daily from the legacy one, and only the successor's count leaves out a restart row (#8651). */
        var successorPairs = successorDaily is null
            ? $"""
                SELECT DISTINCT date_trunc('day', bucket) AS d, query_hash
                        FROM collect.{successor}
                        WHERE server_id = $1 AND bucket >= $2 AND bucket < $3 AND bucket >= {boundary}
                """
            : HeldDailyPairs(successor, successorDaily, boundary).Pairs;
        var dailyCtes = successorDaily is null ? string.Empty : HeldDailyPairs(successor, successorDaily, boundary).Ctes;

        return $"""
        /* #7505: raw_from is the day in which each side's newest bucket ends, the first day that side does not
           hold whole. See QueriesCteForCagg. */
        {dailyCtes}queries_ceiling_legacy AS (
            SELECT date_trunc('day', max(bucket)) AS last_day,
                   date_trunc('day', max(bucket) + INTERVAL '{legacyWidth} seconds') AS raw_from
            FROM collect.{legacy}
            WHERE server_id = $1 AND bucket < {boundary}
        ),
        queries_ceiling_successor AS (
            SELECT date_trunc('day', max(bucket)) AS last_day,
                   date_trunc('day', max(bucket) + INTERVAL '{successorWidth} seconds') AS raw_from
            FROM collect.{successor}
            WHERE server_id = $1 AND bucket >= {boundary}
        ),
        queries AS (
            /* One row per day: the larger of the rollup's and raw's count on the one day both can hold. See
               QueriesCteForCagg. */
            SELECT u.d, MAX(u.c) AS c
            FROM (
            SELECT x.d, COUNT(x.query_hash) AS c
            FROM (
                SELECT DISTINCT date_trunc('day', bucket) AS d, query_hash
                FROM collect.{legacy}
                WHERE server_id = $1 AND bucket >= $2 AND bucket < $3 AND bucket < {boundary}
            ) AS x
            GROUP BY x.d
            UNION ALL
            SELECT x.d, COUNT(x.query_hash) AS c
            FROM (
                {successorPairs}
            ) AS x
            GROUP BY x.d
            UNION ALL
            SELECT x.d, COUNT(x.query_hash) AS c
            FROM (
                /* #8651: raw counts only queries with credited executions, as the interval rollups do. See QueriesCteForCagg. */
                {CreditedPairs("collect.query_stats", tailRowPredicate)}{closedHours}
            ) AS x
            GROUP BY x.d
            UNION ALL
            SELECT b.d, NULL::bigint AS c
            FROM (
                SELECT g.d
                FROM generate_series(date_trunc('day', $2::timestamp), date_trunc('day', $3::timestamp), INTERVAL '1 day') AS g(d)
                WHERE g.d < $3
                  AND g.d < {boundary}
                  AND g.d < COALESCE((SELECT last_day + INTERVAL '1 day' FROM queries_ceiling_legacy), $2)
                  AND NOT EXISTS (
                    SELECT 1 FROM collect.{legacy} AS r
                    WHERE r.server_id = $1 AND r.bucket >= g.d AND r.bucket < g.d + INTERVAL '1 day'
                    OFFSET 0)
                OFFSET 0
            ) AS b
            WHERE EXISTS (
                SELECT 1 FROM collect.{legacyTarget.Source} AS s
                WHERE s.server_id = $1 AND s.{legacyTarget.SourceTimeColumn} >= b.d AND s.{legacyTarget.SourceTimeColumn} < b.d + INTERVAL '1 day'{legacySourceFilter}
                OFFSET 0)
            UNION ALL
            SELECT b.d, NULL::bigint AS c
            FROM (
                SELECT g.d
                FROM generate_series(date_trunc('day', $2::timestamp), date_trunc('day', $3::timestamp), INTERVAL '1 day') AS g(d)
                WHERE g.d < $3
                  AND g.d >= {boundary}
                  AND g.d < COALESCE((SELECT last_day + INTERVAL '1 day' FROM queries_ceiling_successor), $2)
                  AND NOT EXISTS (
                    SELECT 1 FROM collect.{successor} AS r
                    WHERE r.server_id = $1 AND r.bucket >= g.d AND r.bucket < g.d + INTERVAL '1 day'
                    OFFSET 0)
                OFFSET 0
            ) AS b
            WHERE EXISTS (
                SELECT 1 FROM collect.{successorTarget.Source} AS s
                WHERE s.server_id = $1 AND s.{successorTarget.SourceTimeColumn} >= b.d AND s.{successorTarget.SourceTimeColumn} < b.d + INTERVAL '1 day'{successorSourceFilter}
                OFFSET 0)
            ) AS u
            GROUP BY u.d
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
    /// leaves out the restart row the legacy counted as a query seen that day, as raw's count now does (#8651). This overload's OWN daily tier is
    /// not parameterised — it always reads the legacy <c>query_stats_daily</c> unstitched, whatever
    /// <paramref name="hourlyRelation"/> carries; the daily's own stitch lives in the three-argument overload
    /// below, which does not call this one for that case at all — it builds its own queries CTE directly off
    /// <see cref="QueriesCteForCagg"/>, so a successor-only daily keeps its own name instead of being downgraded
    /// back to the legacy here. The one-argument form keeps the legacy for callers that have not probed, which
    /// is what every pre-#3653 pin reads.
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
                "Daily-summary CAGG routing found no queries CTE to replace. QueriesCteRaw has drifted from RangeSql (#1661).");
        }

        return routed;
    }

    /// <summary>
    /// #3653 A6, lane LA-3b2 (hourly) / lane LA-8 (daily): <see cref="RangeSqlFor(RetentionTier, string)"/>,
    /// stitch-aware, on EITHER rollup tier. When <paramref name="coverage"/> reports a stitch boundary for the
    /// tier's own legacy rollup over <paramref name="windowStartUtc"/> (<see cref="RollupCoverage.StitchFloor"/>
    /// — F for hourly, F_d for daily), the not-carried probe runs TWICE, once per relation name split at that
    /// boundary (decision 3, <see cref="QueriesCteForStitchedCagg"/>); with no boundary — no successor, an
    /// empty one, or one that already covers the whole window — this is byte-identical to
    /// <see cref="RangeSqlFor(RetentionTier, string)"/> over the legacy name, because <c>StitchFloor</c> answers
    /// null in exactly the cases <see cref="RollupCoverage.StitchedRelationSql"/> would also answer legacy-only.
    /// The daily tier's successor name comes off <see cref="TimescaleSupport.SupersededDailyRollups"/>, the same
    /// registry <see cref="RollupCoverage.StitchedRelationSql"/> reads, rather than a second lookup that could
    /// drift from it.
    /// </summary>
    private static string RangeSqlForQueries(RetentionTier tier, RollupCoverage coverage, DateTime windowStartUtc)
    {
        ArgumentNullException.ThrowIfNull(coverage);

        if (tier != RetentionTier.Hourly && tier != RetentionTier.Daily)
        {
            /* #8651: a store with no rollups has no compressed chunk and has the restart-row index, so its raw read keeps the
               index-only pair read. A store with rollups may have compressed chunks under a raw read, which the simple
               predicate serves without decompressing more. */
            return tier == RetentionTier.Raw && !coverage.HasAnyRollup ? RangeSqlPlainStore : RangeSqlFor(tier);
        }

        var stitchTier = tier == RetentionTier.Hourly ? RollupCoverage.StitchTier.Hourly : RollupCoverage.StitchTier.Daily;
        var legacy = tier == RetentionTier.Hourly ? TimescaleSupport.QueryStatsHourlyView : TimescaleSupport.QueryStatsDailyView;

        var successorFloor = coverage.StitchFloor(legacy, stitchTier, windowStartUtc);
        if (successorFloor is null)
        {
            /* #3653 A6, lane LA-6 (coordinator ruling on #4182): this branch already runs only when
               StitchFloor answered null, which per StitchedRelationSql's own remarks means it would splice a
               SINGLE relation — "collect.{legacy} AS f" (no successor, empty successor) or
               "collect.{successor} AS f" (successor covers the whole window) — never a stitch. Reading the name
               back off the builder's own splice (rather than re-deriving "which one won" here) keeps every SQL
               splice on the one path (decision 1): this call and StitchedRelationSql agree by construction.
               Built with QueriesCteForCagg directly (not the two-argument overload above, whose OWN daily
               branch always names the legacy — it exists for callers with no coverage to probe a successor
               with) so a successor-only daily answer is not silently downgraded back to the legacy. */
            var singleSplice = coverage.StitchedRelationSql(legacy, "f", windowStartUtc, stitchTier);
            var relationStart = "collect.".Length;
            var relationEnd = singleSplice.IndexOf(" AS ", relationStart, StringComparison.Ordinal);
            var relationName = singleSplice[relationStart..relationEnd];

            /* #8549: the hourly tier's interval-honest relation counts its closed days from the interval daily when the store holds it.
               The daily tier reads the daily already, and the legacy hourly keeps its own path. */
            var heldDaily = tier == RetentionTier.Hourly
                && coverage.HasQueryStatsIntervalDaily
                && string.Equals(relationName, TimescaleSupport.QueryStatsIntervalHourlyView, StringComparison.Ordinal)
                    ? TimescaleSupport.QueryStatsIntervalDailyView
                    : null;
            var singleRouted = RangeSql.Replace(
                QueriesCteRaw, QueriesCteForCagg(relationName, heldDaily), StringComparison.Ordinal);
            if (string.Equals(singleRouted, RangeSql, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    "Daily-summary CAGG routing found no queries CTE to replace. QueriesCteRaw has drifted from RangeSql (#1661).");
            }

            return singleRouted;
        }

        var successor = tier == RetentionTier.Hourly
            ? TimescaleSupport.SuccessorOf(legacy)!
            : TimescaleSupport.SupersededDailyRollups.First(p => string.Equals(p.LegacyDaily, legacy, StringComparison.Ordinal)).SuccessorDaily;
        var successorDaily = tier == RetentionTier.Hourly && coverage.HasQueryStatsIntervalDaily
            ? TimescaleSupport.QueryStatsIntervalDailyView
            : null;
        var routed = RangeSql.Replace(
            QueriesCteRaw, QueriesCteForStitchedCagg(legacy, successor, successorFloor.Value, successorDaily), StringComparison.Ordinal);

        if (string.Equals(routed, RangeSql, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "Daily-summary stitched CAGG routing found no queries CTE to replace. QueriesCteRaw has drifted from RangeSql (#1661).");
        }

        return routed;
    }

    /// <summary>
    /// The <c>wait_per_spelling</c> part exactly as it appears in <see cref="RangeSql"/>: the per-day, per-stored-name wait totals, summed
    /// from raw <c>wait_stats</c>. The part a 30-day window pays for most (#8549): it reads and groups a month of raw rows, 2.2 s of a
    /// 2.7 s statement on a test store of 11.76 M rows.
    /// </summary>
    private const string WaitPerSpellingRaw = """
        WITH wait_per_spelling AS (
            SELECT date_trunc('day', collection_time) AS d, wait_type, SUM(delta_wait_time_ms) AS ms
            FROM v_wait_stats
            WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3 AND delta_wait_time_ms > 0
            GROUP BY 1, 2
        ),
        """;

    /// <summary>
    /// <see cref="WaitPerSpellingRaw"/> over the daily wait totals (#8549). The rollup answers a day when it holds that day for the server
    /// and the day lies WHOLLY inside <c>[$2, $3)</c>; every other day reads raw, one lateral probe each: the open day, the day or two the
    /// refresh has not reached, a hole in the middle, and the first or last day of a window that does not start or end at midnight (a
    /// partial day must sum only the rows inside the window, which the rollup cannot split).
    ///
    /// <para><b>The day boundary.</b> Daily Summary days are <c>date_trunc('day', collection_time)</c> over the naive-UTC column, so they are
    /// UTC days, and <c>time_bucket('1 day')</c> on that column buckets to the same midnight. A day the window covers whole therefore
    /// has the same rows in both places. A window of any other shape reads those days from raw, so the result does not depend on the
    /// rollup's day boundary matching the window's.</para>
    ///
    /// <para>The day list is <c>generate_series</c> over the window, <c>OFFSET 0</c> so the planner keeps it as one list, and the
    /// last instant of the window is <c>$3</c> less a microsecond, the finest step a <c>timestamp</c> holds, so a window ending a hair
    /// past midnight still reads that day. The raw lateral term groups by the stored spelling as the raw statement does, so the
    /// <c>rtrim</c> merge in <c>wait_per_type</c> is unchanged and the rows of both branches have the same shape.</para>
    /// </summary>
    private const string WaitPerSpellingFromRollup = """
        WITH wait_covered AS (
            SELECT bucket AS d
            FROM collect.wait_stats_daily
            WHERE server_id = $1 AND bucket >= $2 AND bucket + INTERVAL '1 day' <= $3
            GROUP BY bucket
        ),
        wait_per_spelling AS (
            SELECT bucket AS d, wait_type, delta_wait_time_ms AS ms
            FROM collect.wait_stats_daily
            WHERE server_id = $1 AND bucket >= $2 AND bucket + INTERVAL '1 day' <= $3
            UNION ALL
            SELECT x.d, r.wait_type, r.ms
            FROM (
                SELECT g.d
                FROM generate_series(date_trunc('day', $2::timestamp), $3::timestamp - INTERVAL '1 microsecond', INTERVAL '1 day') AS g(d)
                WHERE NOT EXISTS (SELECT 1 FROM wait_covered AS c WHERE c.d = g.d)
                OFFSET 0
            ) AS x
            CROSS JOIN LATERAL (
                SELECT w.wait_type, SUM(w.delta_wait_time_ms) AS ms
                FROM v_wait_stats AS w
                WHERE w.server_id = $1
                AND   w.collection_time >= GREATEST(x.d, $2)
                AND   w.collection_time < LEAST(x.d + INTERVAL '1 day', $3)
                AND   w.delta_wait_time_ms > 0
                GROUP BY w.wait_type
            ) AS r
        ),
        """;

    /// <summary>
    /// <see cref="RangeSqlFor(RetentionTier, string)"/>'s coverage-aware form (#3653), with the wait totals read from
    /// <c>collect.wait_stats_daily</c> for the closed days it holds when the store has it (#8549,
    /// <see cref="RollupCoverage.HasWaitStatsDaily"/>), on the hourly and daily tiers. The raw tier is a window of a few days and keeps the
    /// raw part. A store without the relation (an older build's store, or one whose sweep has not made it yet) gets exactly the text it
    /// got before: the existence probe decides, so the read can never name a relation that is not there.
    /// </summary>
    public static string RangeSqlFor(RetentionTier tier, RollupCoverage coverage, DateTime windowStartUtc)
    {
        var routed = RangeSqlForQueries(tier, coverage, windowStartUtc);
        if (tier == RetentionTier.Raw || !coverage.HasWaitStatsDaily)
        {
            return routed;
        }

        var withRollup = routed.Replace(WaitPerSpellingRaw, WaitPerSpellingFromRollup, StringComparison.Ordinal);
        if (string.Equals(withRollup, routed, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "Daily-summary wait rollup routing found no wait_per_spelling CTE to replace: WaitPerSpellingRaw has drifted from RangeSql (#8549).");
        }

        return withRollup;
    }

}
