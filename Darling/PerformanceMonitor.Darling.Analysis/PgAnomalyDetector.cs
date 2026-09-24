/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using static PerformanceMonitor.Analysis.Baselines.AnomalyThresholds;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// Detects anomalies by comparing the analysis window's metrics against
/// time-bucketed baselines (hour-of-day x day-of-week, 30-day rolling window) —
/// Lite's AnomalyDetector (Lite/Analysis/AnomalyDetector.cs) ported for the
/// analysis slice AN2b onto Darling's Postgres store: the same nine detector
/// methods plus the HasBaselineDataAsync gate, the same fact keys, metadata
/// keys, thresholds, and severity inputs, and the same per-detector try/catch
/// tolerance (a failing detector logs and contributes nothing; it never fails
/// the run).
///
/// Two detection patterns:
/// - Z-score: (observed - mean) / stddev — used for continuous metrics
///   (CPU, batch requests, I/O latency, session counts, query duration, memory)
/// - Ratio: currentRate / baselineRate — used for rate/event metrics
///   (wait stats, blocking, deadlocks)
///
/// Baseline computation and caching are handled by <see cref="PgBaselineProvider"/>.
///
/// <para>
/// #3653 (A8, first slice): every z-score family hands the shared <see cref="AnomalyGate"/> the
/// window PEAK and the window MEAN as a pair, and the gate fires only when BOTH clear the existing
/// cutoffs — the peak alone tested a window MAX against a per-sample distribution, so the verdict's
/// null expectation grew with the number of samples in the window (a 24-hour anchored pass reported
/// more anomalies than the 4-hour scheduled pass on identical behaviour; the post-install pages ran
/// 11 → 24/h on the measured population). Every window read here already computed AVG beside MAX
/// except <see cref="IoWindowSql"/>, which read AVG ALONE while its siblings read the peak under the
/// same shared cutoffs; it now reads both and reports the peak like the rest. The reported deviation
/// (Value, deviation_sigma) stays the PEAK's; the mean's rides beside it as mean_deviation_sigma.
/// Lite's AnomalyDetector carries the identical reads, verbatim.
/// </para>
///
/// <para>
/// #3741 (the last leg of that slice): the WAIT-PROFILE detector, which never went through the gate,
/// gets the same pair. Its window read computed a peak ms/sec and nothing else, and its inline gate
/// tested that peak alone — modified z on the median/MAD frame at the heavy-tail 5.0 cutoff AND the
/// 250 ms/sec floor — so one hot collection in an otherwise quiet window fired the one metric the
/// fleet measured as heavy-tailed by nature. <see cref="WaitRateWindowSql"/> now reads AVG beside MAX
/// and the trustworthy robust arm is ONE <see cref="AnomalyGate"/> pair call: peak and mean both clear
/// the 5.0 cutoff, the floor stays on the peak. The ratio arm (a trustworthy bucket without robust
/// statistics) asks the same of both ratios; the no-baseline arm stays on the peak's absolute bar, as
/// the ruling says. <c>current_ms_per_sec</c> stays the peak; <c>avg_ms_per_sec</c> and
/// <c>mean_modified_z</c> ride beside it.
/// </para>
///
/// <para>
/// Postgres discipline (see PgFindingStore): the SQL is Lite-verbatim against the
/// V4 passthrough views (already dialect-shared — no QUALIFY in the detector
/// queries), with every window bound a naive-UTC Kind-Unspecified parameter.
/// Lite's SQL contains no bare NOW()/CURRENT_TIMESTAMP; the 30-day baseline-data
/// gate in <see cref="HasBaselineDataSql"/>'s $2 is bound as a naive-UTC parameter
/// exactly like Lite binds it, and since #2506 comes off the window's end rather
/// than off the clock, so every bound in this class is window-derived.
/// SQL is exposed const so Darling.Tests can pin the dialect ungated
/// ($N positional parameters, no QUALIFY, no bare now(), no N'' literals) —
/// the PgFindingStore/DarlingAlertReadAdapter pattern.
/// </para>
/// </summary>
public class PgAnomalyDetector : IAnomalyDetector
{
    private readonly NpgsqlDataSource _postgres;
    private readonly PgBaselineProvider _baselineProvider;
    private readonly ILogger? _logger;

    /// <summary>
    /// Per-metric deviation thresholds. Metrics not listed use DefaultDeviationThreshold.
    /// </summary>
    private readonly Dictionary<string, double> _deviationThresholds = new();

    public PgAnomalyDetector(NpgsqlDataSource postgres, PgBaselineProvider baselineProvider, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _baselineProvider = baselineProvider ?? throw new ArgumentNullException(nameof(baselineProvider));
        _logger = logger;
    }

    /// <summary>
    /// Sets a custom deviation threshold for a specific metric.
    /// </summary>
    public void SetDeviationThreshold(string metricName, double threshold)
    {
        _deviationThresholds[metricName] = threshold;
    }

    private double GetDeviationThreshold(string metricName)
    {
        return _deviationThresholds.TryGetValue(metricName, out var threshold)
            ? threshold
            : DefaultDeviationThreshold;
    }

    /// <summary>
    /// Adds baseline context metadata to an anomaly fact's metadata dictionary.
    /// </summary>
    private static void AddBaselineContext(Dictionary<string, double> metadata, BaselineBucket baseline)
    {
        metadata["baseline_hour"] = baseline.HourOfDay;
        metadata["baseline_dow"] = baseline.DayOfWeek;
        metadata["baseline_tier"] = (double)baseline.Tier;
        /* #1743: the robust frame the modified-z was judged in (zeros for a rollup-bound metric
           still on the classical path), and the honest confidence the FactScorer multiplies by —
           derived from tier + sample density, no longer a hardcoded 1.0. */
        metadata["baseline_median"] = baseline.Median;
        metadata["baseline_mad"] = baseline.Mad;
        metadata["confidence"] = baseline.Confidence;
        /* #3691 lane 41: the DISTINCT-DAY count, which the bucket has always carried and no fact ever showed.
           The zero-history extremity's claim is "N samples across D days of this hour, never once non-zero", and
           a sample count alone cannot say it — 250 samples from two busy afternoons is not a month. Stamped on
           every z-family fact, not only the zero-history ones: it is the same quality signal IsTrustworthy's day
           floor reads, and an operator reading any baseline fact wants it beside baseline_samples. */
        metadata["baseline_distinct_days"] = baseline.DistinctDays;
        /* #3859: on the FLAT tier that count is a CEILING PROXY, not a measurement, and the fact says so.
           CollapseToFlat takes MAX(DistinctDays) over the hour buckets rather than a global DISTINCT-days
           query - a calendar day recurs across the 24 buckets so summing would double-count - and each
           (hour, dow) bucket holds at most ~5 same-weekday dates in a 30-day window, so a Flat bucket
           reports about 5 however much history it actually pooled. The admission lives at the CollapseToFlat
           site, where it is a comment nothing downstream can read; this stamp is what stops a
           get_analysis_facts reader treating the ceiling as a measurement, which is the whole defect. Stamped
           only where it is true - the Full and HourOnly tiers count their own days, and an unconditional flag
           would say nothing. */
        if (baseline.Tier == BaselineTier.Flat)
            metadata["baseline_distinct_days_is_proxy"] = 1;
    }

    /// <summary>
    /// Detects anomalies by comparing the analysis window against time-bucketed baselines.
    /// Returns anomaly facts to be merged into the main fact list.
    /// </summary>
    public async Task<List<Fact>> DetectAnomaliesAsync(AnalysisContext context)
    {
        var anomalies = new List<Fact>();

        // Check if baseline period has any data at all — if not, skip all anomaly detection.
        /* #2506: the gate's 30 days are measured back from the WINDOW's end, not from the clock. Every
           other bound in this class already comes off context.TimeRangeStart/End, and the baseline this
           gate is guarding is computed at context.TimeRangeStart too — so asking "was anything collected
           in the 30 days before now" while the baseline reads the 30 days before an anchored window was
           the one place the two could disagree. Identical for an unanchored pass, whose TimeRangeEnd IS
           now. */
        if (!await HasBaselineDataAsync(context.ServerId, context.TimeRangeEnd, context.CancellationToken))
            return anomalies;

        // Existing detection methods (upgraded to time-bucketed baselines)
        await DetectCpuAnomalies(context, anomalies);
        await DetectWaitAnomalies(context, anomalies);
        await DetectBlockingAnomalies(context, anomalies);
        await DetectIoAnomalies(context, anomalies);

        // New detection methods
        await DetectBatchRequestAnomalies(context, anomalies);
        await DetectSessionAnomalies(context, anomalies);
        await DetectQueryDurationAnomalies(context, anomalies);
        await DetectMemoryAnomalies(context, anomalies);
        await DetectObjectStatsAnomalies(context, anomalies);

        return anomalies;
    }

    /* ---------------- SQL (Lite-verbatim, PG dialect only where forced) ---------------- */

    /// <summary>
    /// Baseline-data gate: wait_stats as canary — if waits are collected, other data is too.
    /// $2 is <c>context.TimeRangeEnd.AddDays(-30)</c>, bound naive-UTC Kind-Unspecified — never a bare
    /// now(), which would be timestamptz. It was <c>DateTime.UtcNow.AddDays(-30)</c> until #2506; the
    /// two are the same value on every unanchored pass, and only the window-derived form stays correct
    /// when the pass is anchored at a past instant.
    /// </summary>
    public const string HasBaselineDataSql = @"
SELECT (SELECT COUNT(*) FROM v_wait_stats
        WHERE server_id = $1 AND collection_time >= $2)
     + (SELECT COUNT(*) FROM v_cpu_utilization_stats
        WHERE server_id = $1 AND collection_time >= $2)";

    public const string CpuWindowSql = @"
SELECT MAX(sqlserver_cpu_utilization) AS peak_cpu,
       AVG(sqlserver_cpu_utilization) AS avg_cpu,
       COUNT(*) AS sample_count,
       (SELECT collection_time FROM v_cpu_utilization_stats
        WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
        ORDER BY sqlserver_cpu_utilization DESC LIMIT 1) AS peak_time
FROM v_cpu_utilization_stats
WHERE server_id = $1
AND   collection_time >= $2 AND collection_time < $3";

    /* #3653 A8 option B (lane L2a): the tiled CPU window read — one row per target-local hour
       (WindowTiles.LocalHourSql, $4..$6 bound from the ANALYSIS window's clock, never the cached
       baseline clock — see the recipe doc). The peak-time subquery is dropped for a per-tile
       array_agg ORDER BY, which the correlated LIMIT-1 subquery cannot express per group. Column
       order (0 local_hour, 1 peak, 2 avg, 3 count, 4 peak_time) is the reader's ordinal contract. */
    public const string CpuTileWindowSql = @"
SELECT " + WindowTiles.LocalHourSql + @" AS local_hour,
       MAX(sqlserver_cpu_utilization) AS peak_cpu,
       AVG(sqlserver_cpu_utilization) AS avg_cpu,
       COUNT(*) AS sample_count,
       (array_agg(collection_time ORDER BY sqlserver_cpu_utilization DESC))[1] AS peak_time
FROM v_cpu_utilization_stats
WHERE server_id = $1
AND   collection_time >= $2 AND collection_time < $3
GROUP BY local_hour
ORDER BY local_hour";

    /* Wait-profile current window: all-types wait ms/sec per collection (the collection's STORED interval
       since V127, the LAG for pre-V127 collections — never an assumed cadence; mirrors the WaitMsPerSec
       baseline), then PEAK and MEAN across collections. A restart collection (every row's stored interval
       0) has a NULL interval, so it is neither a peak candidate, nor a term of the mean, nor counted as a
       sample — before #3540 it was a sample worth 0.00 ms/sec. Window bound aligned to the baseline
       (>= $2 AND < $3).

       #3741: the MEAN beside the peak, same arm, same guard — AVG over exactly the collections MAX ranges
       over (a NULL-interval collection contributes NULL to both, and AVG ignores NULL as MAX does), so the
       two statistics describe the same sample set and the pair gate compares like with like. The peak
       alone tested a window MAX against a per-collection distribution, so one hot collection in a quiet
       window read as a profile shift; the mean of N collections drawn from the baseline sits at the
       baseline whatever N is, and requiring it to clear the same bar removes that bias without a new
       number. Column ORDER is the reader's ordinal contract (0 peak, 1 avg, 2 total, 3 count) — pinned. */
    public const string WaitRateWindowSql = @"
WITH per_collection AS (
    SELECT collection_time,
           SUM(delta_wait_time_ms)::DOUBLE PRECISION AS total_wait_ms,
           /* #3540: the collection's STORED interval (MAX over its rows — a wait type first seen in an otherwise
              steady pass carries 0 beside its siblings' real interval and adds 0 to the sum; MAX is 0 only when
              EVERY row was unknowable, a restart) mapped through NULLIF so that collection is NOT a sample; a
              pre-V127 collection (NULL) falls back to the LAG this read always used. */
           CASE WHEN MAX(sample_interval_seconds) IS NULL
                THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time))))
                ELSE NULLIF(MAX(sample_interval_seconds), 0)
           END AS interval_sec
    FROM v_wait_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   delta_wait_time_ms >= 0
    GROUP BY collection_time
)
SELECT MAX(CASE WHEN interval_sec > 0 THEN total_wait_ms / interval_sec END) AS peak_ms_per_sec,
       AVG(CASE WHEN interval_sec > 0 THEN total_wait_ms / interval_sec END) AS avg_ms_per_sec,
       SUM(total_wait_ms) AS total_wait_ms,
       COUNT(*) FILTER (WHERE interval_sec IS NOT NULL) AS sample_count
FROM per_collection";

    /* #3653 A8 option B (lane L2a): the tiled wait-rate window read. The per_collection CTE is
       unchanged — it must expose collection_time under that name for WindowTiles.LocalHourSql to key
       on — and the OUTER select groups by local_hour instead of collapsing to one row. Column order
       (0 local_hour, 1 peak, 2 avg, 3 total, 4 count) is the reader's ordinal contract. $4..$6 bind
       from the ANALYSIS window's clock. */
    public const string WaitRateTileWindowSql = @"
WITH per_collection AS (
    SELECT collection_time,
           SUM(delta_wait_time_ms)::DOUBLE PRECISION AS total_wait_ms,
           CASE WHEN MAX(sample_interval_seconds) IS NULL
                THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time))))
                ELSE NULLIF(MAX(sample_interval_seconds), 0)
           END AS interval_sec
    FROM v_wait_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   delta_wait_time_ms >= 0
    GROUP BY collection_time
)
SELECT " + WindowTiles.LocalHourSql + @" AS local_hour,
       MAX(CASE WHEN interval_sec > 0 THEN total_wait_ms / interval_sec END) AS peak_ms_per_sec,
       AVG(CASE WHEN interval_sec > 0 THEN total_wait_ms / interval_sec END) AS avg_ms_per_sec,
       SUM(total_wait_ms) AS total_wait_ms,
       COUNT(*) FILTER (WHERE interval_sec IS NOT NULL) AS sample_count
FROM per_collection
GROUP BY local_hour
ORDER BY local_hour";

    /* Top 6 wait-type contributors in the window (named in the metadata KEY). */
    public const string WaitContribWindowSql = @"
SELECT wait_type,
       SUM(delta_wait_time_ms)::BIGINT AS total_ms
FROM v_wait_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
AND   delta_wait_time_ms > 0
GROUP BY wait_type
ORDER BY total_ms DESC
LIMIT 6";

    /* current_blocking: prefer the blocked-process-report; fall back to the always-on DMV
       snapshot so RDS (where the BPR session is empty) still counts blocking. Mirrors the
       overview/alert path (Lite LocalDataService.Overview.cs / LocalDataService.Blocking.cs). */
    public const string BlockingWindowSql = @"
SELECT
    COALESCE(NULLIF(
        (SELECT COUNT(*) FROM v_blocked_process_reports
         WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3), 0),
        (SELECT COUNT(*) FROM v_dmv_blocking_snapshots
         WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3)) AS current_blocking,
    (SELECT COUNT(*) FROM v_deadlocks
     WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3) AS current_deadlocks";

    /* #3653 (A8): the I/O window read hands the gate the PEAK and the MEAN per-file-row latency, like every
       sibling family. Until this slice it read AVG ALONE — the one z-score detector judging a window average
       against cutoffs its siblings met with a window MAX, so a single file's stall burst that would have fired
       on CPU or batch requests was diluted here, and an averaged window carried none of the per-sample peak the
       ReadLatencyFloorMs / IoLatencyFallbackMs bars were sized for. The baseline is the per-file-row read ratio
       at this same grain (PgBaselineProvider's io_latency arm), so MAX over the same rows is the statistic the
       other families test. Lite-verbatim. */
    public const string IoWindowSql = @"
SELECT MAX(delta_stall_read_ms * 1.0 / NULLIF(delta_reads, 0)) AS peak_read_lat,
       AVG(delta_stall_read_ms * 1.0 / NULLIF(delta_reads, 0)) AS avg_read_lat,
       MAX(delta_stall_write_ms * 1.0 / NULLIF(delta_writes, 0)) AS peak_write_lat,
       AVG(delta_stall_write_ms * 1.0 / NULLIF(delta_writes, 0)) AS avg_write_lat
FROM v_file_io_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
AND   (delta_reads > 0 OR delta_writes > 0)";

    /* #3653 A8 option B (lane L2a): the tiled I/O window read — ONE read feeds both the read-latency
       and write-latency gates, each scored through EvaluateTiles with its own WindowTile list built
       from this one row set (design's I/O row: "ONE tiled read feeds both gates"). Column order
       (0 local_hour, 1 peak_read, 2 avg_read, 3 peak_write, 4 avg_write, 5 count) is the reader's
       ordinal contract. $4..$6 bind from the ANALYSIS window's clock. */
    public const string IoTileWindowSql = @"
SELECT " + WindowTiles.LocalHourSql + @" AS local_hour,
       MAX(delta_stall_read_ms * 1.0 / NULLIF(delta_reads, 0)) AS peak_read_lat,
       AVG(delta_stall_read_ms * 1.0 / NULLIF(delta_reads, 0)) AS avg_read_lat,
       MAX(delta_stall_write_ms * 1.0 / NULLIF(delta_writes, 0)) AS peak_write_lat,
       AVG(delta_stall_write_ms * 1.0 / NULLIF(delta_writes, 0)) AS avg_write_lat,
       COUNT(*) AS sample_count
FROM v_file_io_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
AND   (delta_reads > 0 OR delta_writes > 0)
GROUP BY local_hour
ORDER BY local_hour";

    /* Batch-request window: per-second rate per sample (#3527) — delta_cntr_value spans one collection
       interval, so divide by the row's MEASURED sample_interval_seconds (#2234). Interval <= 0 marks an
       unknowable delta (first sighting/reset/gap) and the row is skipped, never read as 0. Keeps the
       window statistic in the same requests/sec unit as the baseline and the BatchRequestFloor/Fallback
       thresholds. */
    public const string BatchRequestWindowSql = @"
SELECT AVG(delta_cntr_value * 1.0 / NULLIF(sample_interval_seconds, 0)) AS avg_batch,
       MAX(delta_cntr_value * 1.0 / NULLIF(sample_interval_seconds, 0)) AS peak_batch,
       COUNT(*) AS sample_count
FROM v_perfmon_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
AND   counter_name = 'Batch Requests/sec'
AND   delta_cntr_value >= 0
AND   sample_interval_seconds > 0";

    public const string SessionWindowSql = @"
WITH per_collection AS (
    SELECT collection_time,
           SUM(connection_count)::DOUBLE PRECISION AS total_connections
    FROM v_session_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
    GROUP BY collection_time
)
SELECT AVG(total_connections) AS avg_connections,
       MAX(total_connections) AS peak_connections,
       COUNT(*) AS sample_count
FROM per_collection";

    public const string QueryDurationWindowSql = @"
WITH per_collection AS (
    SELECT collection_time,
           SUM(delta_elapsed_time)::DOUBLE PRECISION AS total_elapsed
    FROM v_query_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
    AND   delta_execution_count > 0
    AND   delta_elapsed_time >= 0
    GROUP BY collection_time
)
SELECT AVG(total_elapsed) AS avg_elapsed,
       MAX(total_elapsed) AS peak_elapsed,
       COUNT(*) AS sample_count
FROM per_collection";

    public const string MemoryWindowSql = @"
SELECT AVG(total_server_memory_mb::DOUBLE PRECISION / NULLIF(target_server_memory_mb::DOUBLE PRECISION, 0) * 100) AS avg_pressure,
       MAX(total_server_memory_mb::DOUBLE PRECISION / NULLIF(target_server_memory_mb::DOUBLE PRECISION, 0) * 100) AS peak_pressure,
       COUNT(*) AS sample_count
FROM v_memory_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
AND   target_server_memory_mb > 0";

    public const string ObjectGrowthSql = @"
WITH snaps AS (SELECT DISTINCT collection_time FROM v_index_object_stats WHERE server_id = $1 ORDER BY collection_time DESC LIMIT 2),
latest AS (SELECT MAX(collection_time) t FROM snaps),
prior AS (SELECT MIN(collection_time) t FROM snaps),
cur AS (SELECT database_name, object_id, MAX(schema_name) schema_name, MAX(table_name) table_name, SUM(reserved_mb) mb
        FROM v_index_object_stats WHERE server_id = $1 AND collection_time = (SELECT t FROM latest) GROUP BY database_name, object_id),
prv AS (SELECT database_name, object_id, SUM(reserved_mb) mb
        FROM v_index_object_stats WHERE server_id = $1 AND collection_time = (SELECT t FROM prior) GROUP BY database_name, object_id)
SELECT cur.database_name, cur.schema_name, cur.table_name, prv.mb AS prior_mb, cur.mb AS current_mb,
       cur.mb - prv.mb AS growth_mb,
       CASE WHEN prv.mb > 0 THEN (cur.mb - prv.mb) * 100.0 / prv.mb ELSE 0 END AS growth_pct
FROM cur JOIN prv ON cur.database_name = prv.database_name AND cur.object_id = prv.object_id
WHERE (SELECT t FROM latest) <> (SELECT t FROM prior)
AND   cur.mb - prv.mb >= $2
AND   (CASE WHEN prv.mb > 0 THEN (cur.mb - prv.mb) * 100.0 / prv.mb ELSE 0 END) >= $3
ORDER BY growth_mb DESC LIMIT 1";

    public const string ObjectContentionSql = @"
WITH snaps AS (SELECT DISTINCT collection_time FROM v_index_object_stats WHERE server_id = $1 ORDER BY collection_time DESC LIMIT 2),
latest AS (SELECT MAX(collection_time) t FROM snaps),
prior AS (SELECT MIN(collection_time) t FROM snaps),
cur AS (SELECT database_name, object_id, index_id, schema_name, table_name, index_name,
               COALESCE(row_lock_wait_in_ms,0) ms, COALESCE(index_lock_promotion_count,0) esc
        FROM v_index_object_stats WHERE server_id = $1 AND collection_time = (SELECT t FROM latest)),
prv AS (SELECT database_name, object_id, index_id, COALESCE(row_lock_wait_in_ms,0) ms, COALESCE(index_lock_promotion_count,0) esc
        FROM v_index_object_stats WHERE server_id = $1 AND collection_time = (SELECT t FROM prior))
SELECT cur.database_name, cur.schema_name, cur.table_name, cur.index_name,
       cur.ms - prv.ms AS ms_delta, cur.esc - prv.esc AS esc_delta
FROM cur JOIN prv ON cur.database_name = prv.database_name AND cur.object_id = prv.object_id AND cur.index_id = prv.index_id
WHERE (SELECT t FROM latest) <> (SELECT t FROM prior)
AND   cur.ms >= prv.ms
AND   cur.ms - prv.ms >= $2
ORDER BY ms_delta DESC LIMIT 1";

    /* ---------------- detectors (Lite-verbatim logic) ---------------- */

    /// <summary>
    /// Day-over-day object/index detection (delta-based, not stddev-baseline) since the
    /// index_object_stats collector runs daily and its counters are cumulative.
    /// Emits ANOMALY_OBJECT_GROWTH for the biggest table grower over threshold and
    /// ANOMALY_OBJECT_CONTENTION for the index with the largest new lock-wait time.
    /// </summary>
    private async Task DetectObjectStatsAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            // Growth: biggest day-over-day table grower (indexes rolled up) over threshold.
            using (var cmd = new NpgsqlCommand(ObjectGrowthSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds })
            {
                cmd.Parameters.AddWithValue(context.ServerId);
                cmd.Parameters.AddWithValue(ObjectGrowthMbThreshold);
                cmd.Parameters.AddWithValue(ObjectGrowthPctThreshold);

                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                if (await reader.ReadAsync(context.CancellationToken))
                {
                    var db = reader.GetString(0);
                    var gSchema = reader.IsDBNull(1) ? null : reader.GetValue(1)?.ToString();
                    var gTable = reader.IsDBNull(2) ? null : reader.GetValue(2)?.ToString();
                    var growthMb = Convert.ToDouble(reader.GetValue(5));
                    var growthPct = Convert.ToDouble(reader.GetValue(6));
                    anomalies.Add(new Fact
                    {
                        Source = "anomaly",
                        Key = "ANOMALY_OBJECT_GROWTH",
                        Value = growthMb,
                        ServerId = context.ServerId,
                        DatabaseName = db,
                        ObjectName = string.IsNullOrEmpty(gTable) ? null : string.IsNullOrEmpty(gSchema) ? gTable : $"{gSchema}.{gTable}",
                        Metadata = new Dictionary<string, double>
                        {
                            ["prior_mb"] = Convert.ToDouble(reader.GetValue(3)),
                            ["current_mb"] = Convert.ToDouble(reader.GetValue(4)),
                            ["growth_mb"] = growthMb,
                            ["growth_pct"] = growthPct,
                            ["growth_ratio"] = growthPct / ObjectGrowthPctThreshold
                        }
                    });
                }
            }

            // Contention: index with the largest new row-lock wait time (no reset).
            using (var cmd = new NpgsqlCommand(ObjectContentionSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds })
            {
                cmd.Parameters.AddWithValue(context.ServerId);
                cmd.Parameters.AddWithValue(ObjectLockWaitMsDeltaThreshold);

                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                if (await reader.ReadAsync(context.CancellationToken))
                {
                    var db = reader.GetString(0);
                    var cSchema = reader.IsDBNull(1) ? null : reader.GetValue(1)?.ToString();
                    var cTable = reader.IsDBNull(2) ? null : reader.GetValue(2)?.ToString();
                    var cIndex = reader.IsDBNull(3) ? null : reader.GetValue(3)?.ToString();
                    var msDelta = Convert.ToDouble(reader.GetValue(4));
                    string? contendedObject = null;
                    if (!string.IsNullOrEmpty(cTable))
                    {
                        contendedObject = string.IsNullOrEmpty(cSchema) ? cTable : $"{cSchema}.{cTable}";
                        if (!string.IsNullOrEmpty(cIndex))
                            contendedObject += $", index {cIndex}";
                    }
                    anomalies.Add(new Fact
                    {
                        Source = "anomaly",
                        Key = "ANOMALY_OBJECT_CONTENTION",
                        Value = msDelta,
                        ServerId = context.ServerId,
                        DatabaseName = db,
                        ObjectName = contendedObject,
                        Metadata = new Dictionary<string, double>
                        {
                            ["lock_wait_ms_delta"] = msDelta,
                            ["escalation_delta"] = Convert.ToDouble(reader.GetValue(5)),
                            ["contention_ratio"] = msDelta / ObjectLockWaitMsDeltaThreshold
                        }
                    });
                }
            }
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgAnomalyDetector] Object stats anomaly detection failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Checks if the server has enough historical data for meaningful baselines.
    /// Uses wait_stats as canary — if waits are collected, other data is too.
    /// </summary>
    private async Task<bool> HasBaselineDataAsync(int serverId, DateTime windowEnd, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

            using var cmd = new NpgsqlCommand(HasBaselineDataSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(serverId);
            /* Lite binds the same window end minus 30 days here — the parameterized "now" (or the
               #2506 anchor), made Kind-Unspecified for the naive-UTC timestamp columns. */
            cmd.Parameters.AddWithValue(AsNaive(windowEnd.AddDays(-30)));

            var count = Convert.ToInt64(await cmd.ExecuteScalarAsync(cancellationToken) ?? 0);
            return count > 0;
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, cancellationToken))
        {
            /* Silent on a genuine fault BY DESIGN (Lite's gate posture: an unreadable canary reads
               as "no baseline data" and detection just sits out the pass) — but shutdown residue is
               excluded (#2299), or a stop mid-gate would masquerade as an empty baseline instead of
               unwinding to the pass's single Information line like every other read here. */
            return false;
        }
    }

    /// <summary>
    /// Detects CPU utilization anomalies using z-score against time-bucketed baseline.
    /// </summary>
    private async Task DetectCpuAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var window = context.TimeRangeEnd - context.TimeRangeStart;
            var map = await _baselineProvider.GetBucketMapAsync(
                context.ServerId, MetricNames.Cpu, context.TimeRangeStart, context.TimeRangeEnd, context.CancellationToken);

            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            List<WindowTile> tiles;
            using (var cmd = new NpgsqlCommand(CpuTileWindowSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds })
            {
                BindTiledWindow(cmd, context, map);
                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                tiles = await ReadTilesAsync(reader, localHourOrdinal: 0, peakOrdinal: 1, meanOrdinal: 2, samplesOrdinal: 3, peakTimeOrdinal: 4, context.CancellationToken);
            }

            var whole = WindowTiles.WholeWindow(tiles);
            if (whole.Samples == 0) return;

            var cpuThreshold = GetDeviationThreshold(MetricNames.Cpu);
            var tv = AnomalyGate.EvaluateTiles(
                tiles, map,
                cpuThreshold, ModifiedZThresholdFor(MetricNames.Cpu, cpuThreshold), CpuFloorPct, CpuFallbackPct, SigmaDisplayCap,
                window);

            AnomalyGate.ZDecision decision;
            BaselineBucket baseline;
            double peakCpu, avgCpu;
            long windowSamples;
            DateTime? peakTime;
            Dictionary<string, double>? tileMetadata = null;

            if (tv is { } verdict)
            {
                decision = verdict.Decision;
                baseline = verdict.Bucket;
                peakCpu = verdict.Tile.Peak;
                avgCpu = verdict.Tile.Mean;
                windowSamples = verdict.Tile.Samples;
                peakTime = verdict.Tile.PeakTimeUtc;
                tileMetadata = new Dictionary<string, double>();
                WindowTiles.AddTileMetadata(tileMetadata, verdict, tiles, map.WindowClock);
            }
            else
            {
                // Never-blind fallback (design §1): no tile scored — today's whole-window path, unchanged.
                baseline = await _baselineProvider.GetBaselineAsync(
                    context.ServerId, MetricNames.Cpu, context.TimeRangeStart, context.CancellationToken);
                if (baseline.SampleCount == 0) return;
                peakCpu = whole.Peak;
                avgCpu = whole.Mean;
                windowSamples = whole.Samples;
                peakTime = whole.PeakTimeUtc;
                decision = AnomalyGate.EvaluateZScore(
                    baseline, peakCpu, avgCpu,
                    cpuThreshold, ModifiedZThresholdFor(MetricNames.Cpu, cpuThreshold), CpuFloorPct, CpuFallbackPct, SigmaDisplayCap,
                    window: window);
            }

            if (!decision.Fire) return;

            var effectiveStdDev = baseline.EffectiveStdDev;
            var metadata = new Dictionary<string, double>
            {
                ["peak_cpu"] = peakCpu,
                ["avg_cpu_in_window"] = avgCpu,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_stddev"] = effectiveStdDev,
                ["deviation_sigma"] = decision.Sigma,
                ["mean_deviation_sigma"] = decision.MeanSigma ?? 0,
                ["fire_threshold"] = decision.ThresholdUsed,
                ["baseline_low_quality"] = decision.LowQualityBaseline ? 1 : 0,
                ["fallback_exceedance"] = decision.FallbackExceedance,
                ["baseline_zero_history"] = decision.ZeroHistory ? 1 : 0,
                ["baseline_samples"] = baseline.SampleCount,
                ["window_samples"] = windowSamples,
                ["peak_time_ticks"] = peakTime?.Ticks ?? 0
            };
            AddBaselineContext(metadata, baseline);
            if (tileMetadata is not null)
            {
                foreach (var (k, v) in tileMetadata) metadata[k] = v;
            }

            anomalies.Add(new Fact
            {
                Source = "anomaly",
                Key = "ANOMALY_CPU_SPIKE",
                Value = peakCpu,
                ServerId = context.ServerId,
                Metadata = metadata
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgAnomalyDetector] CPU anomaly detection failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Detects a shift in the wait PROFILE — the whole-server all-types wait rate (ms/sec) running
    /// significantly above its time-bucketed baseline — and emits ONE ANOMALY_WAIT_PROFILE fact with
    /// the top wait types as contrib_&lt;TYPE&gt; metadata. Replaces the old per-type
    /// ANOMALY_WAIT_&lt;type&gt; facts, which compared a per-hour per-type value to a per-interval
    /// all-types baseline (a ~240x unit inflation) and missed a minority-but-real wait. Comparing
    /// all-types-vs-all-types on the honest per-second scale fixes units, aggregation, and the
    /// per-type cascade together.
    /// </summary>
    private async Task DetectWaitAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var window = context.TimeRangeEnd - context.TimeRangeStart;
            var map = await _baselineProvider.GetBucketMapAsync(
                context.ServerId, MetricNames.WaitMsPerSec, context.TimeRangeStart, context.TimeRangeEnd, context.CancellationToken);

            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            // Current window: all-types wait ms/sec per collection (interval via LAG), tiled by target-local
            // hour (#3653 A8 option B, lane L2a) — the ordinals are WaitRateTileWindowSql's column order, pinned.
            List<WindowTile> tiles;
            using (var rateCmd = new NpgsqlCommand(WaitRateTileWindowSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds })
            {
                BindTiledWindow(rateCmd, context, map);
                using var rateReader = await rateCmd.ExecuteReaderAsync(context.CancellationToken);
                tiles = await ReadTilesAsync(rateReader, localHourOrdinal: 0, peakOrdinal: 1, meanOrdinal: 2, samplesOrdinal: 4, peakTimeOrdinal: -1, context.CancellationToken);
            }

            var whole = WindowTiles.WholeWindow(tiles);
            if (whole.Samples == 0) return; // no rated collection in the window

            var peakRate = whole.Peak;
            var avgRate = whole.Mean;
            /* total_wait_ms is not one of WindowTile's fields (it isn't a peak/mean/sample statistic), so it is
               summed across tiles here, once, from the same rows the tiles came from. */
            double totalWaitMs = 0;
            using (var totalCmd = new NpgsqlCommand(WaitRateTileWindowSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds })
            {
                BindTiledWindow(totalCmd, context, map);
                using var totalReader = await totalCmd.ExecuteReaderAsync(context.CancellationToken);
                while (await totalReader.ReadAsync(context.CancellationToken))
                {
                    totalWaitMs += totalReader.IsDBNull(3) ? 0.0 : Convert.ToDouble(totalReader.GetValue(3));
                }
            }

            // The coordinator's ruling for this family (#3653 A8 option B, lane L2a): the start-bucket arm
            // choice stays exactly as it is today (robust z / classical ratio / no-baseline). Only the
            // robust-z arm moves onto EvaluateTiles; the ratio and no-baseline arms stay whole-window,
            // unchanged, fed from WholeWindow(tiles) — no second SQL read for them.
            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.WaitMsPerSec, context.TimeRangeStart, context.CancellationToken);

            /* #1743: the modified z-score replaces the ratio as the trusted-baseline trigger —
               fleet-measured, it strictly CONTAINS the ratio's catches at every cutoff (nothing the
               ratio fired on scored under it), and it sees the masked-surge class the ratio cannot:
               a burst-scarred baseline's inflated mean pulled real sustained deviations under 4x
               (worst prod server: stddev 67.9x its robust sigma). The magnitude floor stays AND-ed
               exactly as the ratio path had it — the wait family is heavy-tailed by nature, and the
               floor is what was measured WITH the 5.0 cutoff. The ratio still rides the metadata
               for display and for scoring pre-#1743 facts; a bucket without robust stats keeps the
               classical ratio trigger; an untrustworthy baseline keeps the absolute peak-rate bar
               (NOT silence) so a genuinely heavy profile still surfaces on a young store (is_new).

               #3741 (the last leg of #3653 Q1 / #3724): the trusted robust arm is no longer an inline
               test of the peak alone — it is the shared AnomalyGate PAIR call every other baseline
               detector took in #3724, so the window PEAK and the window MEAN must BOTH clear the 5.0
               modified-z cutoff, with the 250 ms/sec floor on the peak only (the gate's rule: the floor
               is the "trivial value" ceiling for the value the finding reports, sized for a peak). The
               gate speaks the same frame this arm always spoke: DecideRobustFirst judges both statistics
               as modified z against the bucket's median and EffectiveRobustSigma — the exact arithmetic
               of BaselineMath.ModifiedZScore, which still stamps the uncapped modified_z the scorer
               grades — and the EffectiveRobustSigma > 0 guard on this arm means the classical frame
               inside the gate is unreachable here, so the same 5.0 is passed as both cutoffs and the
               absolute bar is the family's one bar. The mean's z is judged against the SAME per-collection
               median/MAD as the peak's, as #3724 did for every other family (one centre, one dispersion,
               one body). Said honestly for a heavy-tailed family: the mean of N draws converges to the
               distribution's MEAN, which sits above its median when the tail is to the right, so the
               mean clause's null expectation is a little above zero and the clause is a little LESS
               strict than a symmetric reading would suggest — a bias toward firing, never toward
               silence, and bounded, because it can only admit a window the peak already admitted; the
               median-centred alternative (option b in #3741) was weighed and the ruling took the engine's
               precedent. The ratio arm asks the same of both ratios (peak/mean AND window-mean/mean over
               DefaultRatioThreshold). The no-baseline arm stays on the peak's absolute bar alone — there
               is no z to trust on either statistic there, and the ruling keeps that bar where it was. */
            bool isNew;
            double ratio;
            double scoredPeakRate = peakRate;
            double scoredAvgRate = avgRate;
            var scoredBaseline = baseline;
            Dictionary<string, double>? tileMetadata = null;
            if (baseline.IsTrustworthy && baseline.EffectiveRobustSigma > 0)
            {
                isNew = false;
                ratio = baseline.Mean > 0 ? peakRate / baseline.Mean : 0;

                /* #3653 A8 option B (lane L2a), coordinator ruling: only the robust-z arm moves onto
                   EvaluateTiles. A null verdict (no tile scored) falls back to today's whole-window
                   EvaluateZScore over WholeWindow(tiles) — the never-blind rule. */
                var tv = AnomalyGate.EvaluateTiles(
                    tiles, map,
                    HeavyTailModifiedZThreshold, HeavyTailModifiedZThreshold, WaitProfileFallbackMsPerSec, WaitProfileFallbackMsPerSec, SigmaDisplayCap,
                    window);

                AnomalyGate.ZDecision decision;
                if (tv is { } verdict)
                {
                    decision = verdict.Decision;
                    scoredBaseline = verdict.Bucket;
                    scoredPeakRate = verdict.Tile.Peak;
                    scoredAvgRate = verdict.Tile.Mean;
                    tileMetadata = new Dictionary<string, double>();
                    WindowTiles.AddTileMetadata(tileMetadata, verdict, tiles, map.WindowClock);
                }
                else
                {
                    decision = AnomalyGate.EvaluateZScore(
                        baseline, peakRate, avgRate,
                        HeavyTailModifiedZThreshold, HeavyTailModifiedZThreshold, WaitProfileFallbackMsPerSec, WaitProfileFallbackMsPerSec, SigmaDisplayCap,
                        window: window);
                }
                if (!decision.Fire) return;
            }
            else if (baseline.IsTrustworthy && baseline.Mean > 0)
            {
                isNew = false;
                ratio = peakRate / baseline.Mean;
                var meanRatio = avgRate / baseline.Mean;
                if (ratio < DefaultRatioThreshold || meanRatio < DefaultRatioThreshold) return;
            }
            else
            {
                isNew = true;
                ratio = peakRate >= WaitProfileFallbackMsPerSec ? NoBaselineRatio : 0;
                if (ratio < DefaultRatioThreshold) return;
            }

            var modifiedZ = BaselineMath.ModifiedZScore(scoredBaseline, scoredPeakRate);
            var meanModifiedZ = BaselineMath.ModifiedZScore(scoredBaseline, scoredAvgRate);

            /* current_ms_per_sec stays the PEAK (the value the story leads with and the ratio is taken
               on); avg_ms_per_sec is the window mean beside it, mean_modified_z its deviation in the
               same uncapped frame as modified_z — the wait profile's own vocabulary, where the other
               families' deviation_sigma / mean_deviation_sigma pair is the capped one (#3741). Both are
               stamped on every arm: 0 modified z on a robust-less bucket, exactly as modified_z is.
               #3653 A8 option B (lane L2a): on the robust-z arm these are the WORST TILE's values (or
               the whole window's, on the never-blind fallback); the ratio and no-baseline arms below
               keep reporting the whole-window peak/mean, unchanged. */
            var metadata = new Dictionary<string, double>
            {
                ["current_ms_per_sec"] = scoredPeakRate,
                ["avg_ms_per_sec"] = scoredAvgRate,
                ["baseline_mean"] = scoredBaseline.Mean,
                ["total_wait_ms"] = totalWaitMs,
                ["ratio"] = ratio,
                ["modified_z"] = modifiedZ,
                ["mean_modified_z"] = meanModifiedZ,
                ["is_new"] = isNew ? 1 : 0,
                /* #3871 rider: the DefaultRatioThreshold this detector gates on is measured now (its own
                   distribution, 3,655 windows / 14 d / p99 3.14), and the stamp is how a reader tells a
                   measured bar from an inherited one without opening the source. The frozen Dashboard
                   mirror does NOT stamp it: its private copy of the constant is pinned as unmeasured on
                   that tier, and a lineage stamp there would claim what its own comment denies. */
                ["threshold_lineage"] = 1
            };
            AddBaselineContext(metadata, scoredBaseline);
            if (tileMetadata is not null)
            {
                foreach (var (k, v) in tileMetadata) metadata[k] = v;
            }

            // Top 6 contributors — named in the metadata KEY (a Dictionary<string,double> can't hold
            // the type name in the value), value = the type's total wait ms in the window.
            using (var contribCmd = new NpgsqlCommand(WaitContribWindowSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds })
            {
                contribCmd.Parameters.AddWithValue(context.ServerId);
                contribCmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
                contribCmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

                using var contribReader = await contribCmd.ExecuteReaderAsync(context.CancellationToken);
                while (await contribReader.ReadAsync(context.CancellationToken))
                {
                    var waitType = contribReader.GetString(0);
                    metadata[$"contrib_{waitType}"] = Convert.ToDouble(contribReader.GetValue(1));
                }
            }

            anomalies.Add(new Fact
            {
                Source = "anomaly",
                Key = "ANOMALY_WAIT_PROFILE",
                Value = totalWaitMs,
                ServerId = context.ServerId,
                Metadata = metadata
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgAnomalyDetector] Wait anomaly detection failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Detects blocking/deadlock anomalies — event rates significantly above
    /// baseline for this time bucket. Uses ratio-based scoring.
    /// </summary>
    private async Task DetectBlockingAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var blockingBaseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.Blocking, context.TimeRangeStart, context.CancellationToken);
            var deadlockBaseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.Deadlock, context.TimeRangeStart, context.CancellationToken);

            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(BlockingWindowSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var currentBlocking = Convert.ToInt64(reader.GetValue(0));
            var currentDeadlocks = Convert.ToInt64(reader.GetValue(1));

            /* Baseline mean is events per hour-of-day/dow bucket (≈ events per hour at this time of
               day). current_* are raw counts over the whole analysis window (hoursBack, default 4),
               so normalize them to per-hour before the ratio — otherwise the ratio scales with the
               window length, not the workload, and a steady event rate trips the spike threshold. */
            var windowHours = (context.TimeRangeEnd - context.TimeRangeStart).TotalHours;
            if (windowHours <= 0) windowHours = 1;
            var currentBlockingPerHour = currentBlocking / windowHours;
            var currentDeadlocksPerHour = currentDeadlocks / windowHours;

            // Baseline mean = events per hour for this hour+dow bucket. Gate on IsTrustworthy (not just
            // SampleCount>0): a thin/zero-history baseline falls back to the absolute event count rather
            // than an inflated ratio. is_new marks that fallback so the composer renders it honestly as
            // a first occurrence — never the dishonest "spiked to 100×" the sentinel used to render.
            var blockingTrust = blockingBaseline.IsTrustworthy;
            var deadlockTrust = deadlockBaseline.IsTrustworthy;
            var baselineBlockingRate = blockingBaseline.SampleCount > 0 ? blockingBaseline.Mean : 0;
            var baselineDeadlockRate = deadlockBaseline.SampleCount > 0 ? deadlockBaseline.Mean : 0;

            // Blocking spike: at least 5 events in the window AND (trustworthy → per-hour rate >= 3x
            // baseline; untrustworthy → fire on the count alone).
            if (currentBlocking >= 5 && (!blockingTrust || currentBlockingPerHour / Math.Max(baselineBlockingRate, 1) >= DefaultEventRatioThreshold))
            {
                var isNew = !blockingTrust;
                var metadata = new Dictionary<string, double>
                {
                    ["current_count"] = currentBlocking,
                    ["baseline_rate"] = baselineBlockingRate,
                    ["ratio"] = isNew ? NoBaselineRatio : currentBlockingPerHour / baselineBlockingRate,
                    ["is_new"] = isNew ? 1 : 0
                };
                AddBaselineContext(metadata, blockingBaseline);

                anomalies.Add(new Fact
                {
                    Source = "anomaly",
                    Key = "ANOMALY_BLOCKING_SPIKE",
                    Value = currentBlocking,
                    ServerId = context.ServerId,
                    Metadata = metadata
                });
            }

            // Deadlock spike: at least 3 events in the window AND (trustworthy → per-hour rate >= 3x
            // baseline; untrustworthy → fire on the count alone).
            if (currentDeadlocks >= 3 && (!deadlockTrust || currentDeadlocksPerHour / Math.Max(baselineDeadlockRate, 1) >= DefaultEventRatioThreshold))
            {
                var isNew = !deadlockTrust;
                var metadata = new Dictionary<string, double>
                {
                    ["current_count"] = currentDeadlocks,
                    ["baseline_rate"] = baselineDeadlockRate,
                    ["ratio"] = isNew ? NoBaselineRatio : currentDeadlocksPerHour / baselineDeadlockRate,
                    ["is_new"] = isNew ? 1 : 0
                };
                AddBaselineContext(metadata, deadlockBaseline);

                anomalies.Add(new Fact
                {
                    Source = "anomaly",
                    Key = "ANOMALY_DEADLOCK_SPIKE",
                    Value = currentDeadlocks,
                    ServerId = context.ServerId,
                    Metadata = metadata
                });
            }
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgAnomalyDetector] Blocking anomaly detection failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Detects I/O latency anomalies using z-score against time-bucketed baseline.
    /// </summary>
    private async Task DetectIoAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.IoLatency, context.TimeRangeStart, context.CancellationToken);

            if (baseline.SampleCount == 0) return;
            var effectiveStdDev = baseline.EffectiveStdDev;

            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(IoWindowSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var peakReadLat = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var avgReadLat = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var peakWriteLat = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2));
            var avgWriteLat = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3));

            var ioThreshold = GetDeviationThreshold(MetricNames.IoLatency);

            // Read latency anomaly — the reported value and sigma are the PEAK's, the pair decides (#3653).
            var readDecision = AnomalyGate.EvaluateZScore(
                baseline, peakReadLat, avgReadLat,
                ioThreshold, ModifiedZThresholdFor(MetricNames.IoLatency, ioThreshold), ReadLatencyFloorMs, IoLatencyFallbackMs, SigmaDisplayCap,
                window: context.TimeRangeEnd - context.TimeRangeStart);
            if (readDecision.Fire)
            {
                var metadata = new Dictionary<string, double>
                {
                    ["current_latency_ms"] = peakReadLat,
                    ["avg_latency_ms"] = avgReadLat,
                    ["baseline_mean_ms"] = baseline.Mean,
                    ["baseline_stddev_ms"] = effectiveStdDev,
                    ["deviation_sigma"] = readDecision.Sigma,
                    ["mean_deviation_sigma"] = readDecision.MeanSigma ?? 0,
                    ["fire_threshold"] = readDecision.ThresholdUsed,
                    ["baseline_low_quality"] = readDecision.LowQualityBaseline ? 1 : 0,
                    ["fallback_exceedance"] = readDecision.FallbackExceedance,
                    ["baseline_zero_history"] = readDecision.ZeroHistory ? 1 : 0,
                    ["baseline_samples"] = baseline.SampleCount
                };
                AddBaselineContext(metadata, baseline);

                anomalies.Add(new Fact
                {
                    Source = "anomaly",
                    Key = "ANOMALY_READ_LATENCY",
                    Value = peakReadLat,
                    ServerId = context.ServerId,
                    Metadata = metadata
                });
            }

            // Write latency anomaly
            var writeDecision = AnomalyGate.EvaluateZScore(
                baseline, peakWriteLat, avgWriteLat,
                ioThreshold, ModifiedZThresholdFor(MetricNames.IoLatency, ioThreshold), WriteLatencyFloorMs, IoLatencyFallbackMs, SigmaDisplayCap,
                window: context.TimeRangeEnd - context.TimeRangeStart);
            if (writeDecision.Fire)
            {
                var metadata = new Dictionary<string, double>
                {
                    ["current_latency_ms"] = peakWriteLat,
                    ["avg_latency_ms"] = avgWriteLat,
                    ["baseline_mean_ms"] = baseline.Mean,
                    ["baseline_stddev_ms"] = effectiveStdDev,
                    ["deviation_sigma"] = writeDecision.Sigma,
                    ["mean_deviation_sigma"] = writeDecision.MeanSigma ?? 0,
                    ["fire_threshold"] = writeDecision.ThresholdUsed,
                    ["baseline_low_quality"] = writeDecision.LowQualityBaseline ? 1 : 0,
                    ["fallback_exceedance"] = writeDecision.FallbackExceedance,
                    ["baseline_zero_history"] = writeDecision.ZeroHistory ? 1 : 0,
                    ["baseline_samples"] = baseline.SampleCount
                };
                AddBaselineContext(metadata, baseline);

                anomalies.Add(new Fact
                {
                    Source = "anomaly",
                    Key = "ANOMALY_WRITE_LATENCY",
                    Value = peakWriteLat,
                    ServerId = context.ServerId,
                    Metadata = metadata
                });
            }
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgAnomalyDetector] I/O anomaly detection failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Detects batch requests/sec anomalies using z-score against time-bucketed baseline.
    /// </summary>
    private async Task DetectBatchRequestAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.BatchRequests, context.TimeRangeStart, context.CancellationToken);

            if (baseline.SampleCount == 0) return;
            var effectiveStdDev = baseline.EffectiveStdDev;

            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(BatchRequestWindowSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var avgBatch = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var peakBatch = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));

            if (windowSamples == 0) return;

            var decision = AnomalyGate.EvaluateZScore(
                baseline, peakBatch, avgBatch,
                GetDeviationThreshold(MetricNames.BatchRequests), ModifiedZThresholdFor(MetricNames.BatchRequests, GetDeviationThreshold(MetricNames.BatchRequests)), BatchRequestFloor, BatchRequestFallback, SigmaDisplayCap,
                window: context.TimeRangeEnd - context.TimeRangeStart);
            if (!decision.Fire) return;

            var metadata = new Dictionary<string, double>
            {
                ["peak_batch_requests"] = peakBatch,
                ["avg_batch_requests"] = avgBatch,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_stddev"] = effectiveStdDev,
                ["deviation_sigma"] = decision.Sigma,
                ["mean_deviation_sigma"] = decision.MeanSigma ?? 0,
                ["fire_threshold"] = decision.ThresholdUsed,
                ["baseline_low_quality"] = decision.LowQualityBaseline ? 1 : 0,
                ["fallback_exceedance"] = decision.FallbackExceedance,
                ["baseline_zero_history"] = decision.ZeroHistory ? 1 : 0,
                ["baseline_samples"] = baseline.SampleCount,
                ["window_samples"] = windowSamples
            };
            AddBaselineContext(metadata, baseline);

            anomalies.Add(new Fact
            {
                Source = "anomaly",
                Key = "ANOMALY_BATCH_REQUESTS",
                Value = peakBatch,
                ServerId = context.ServerId,
                Metadata = metadata
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgAnomalyDetector] Batch request anomaly detection failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Detects session/connection count anomalies using z-score against time-bucketed baseline.
    /// </summary>
    private async Task DetectSessionAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.SessionCount, context.TimeRangeStart, context.CancellationToken);

            if (baseline.SampleCount == 0) return;
            var effectiveStdDev = baseline.EffectiveStdDev;

            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(SessionWindowSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var avgConnections = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var peakConnections = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));

            if (windowSamples == 0) return;

            var decision = AnomalyGate.EvaluateZScore(
                baseline, peakConnections, avgConnections,
                GetDeviationThreshold(MetricNames.SessionCount), ModifiedZThresholdFor(MetricNames.SessionCount, GetDeviationThreshold(MetricNames.SessionCount)), SessionCountFloor, SessionCountFallback, SigmaDisplayCap,
                window: context.TimeRangeEnd - context.TimeRangeStart);
            if (!decision.Fire) return;

            var metadata = new Dictionary<string, double>
            {
                ["peak_connections"] = peakConnections,
                ["avg_connections"] = avgConnections,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_stddev"] = effectiveStdDev,
                ["deviation_sigma"] = decision.Sigma,
                ["mean_deviation_sigma"] = decision.MeanSigma ?? 0,
                ["fire_threshold"] = decision.ThresholdUsed,
                ["baseline_low_quality"] = decision.LowQualityBaseline ? 1 : 0,
                ["fallback_exceedance"] = decision.FallbackExceedance,
                ["baseline_zero_history"] = decision.ZeroHistory ? 1 : 0,
                ["baseline_samples"] = baseline.SampleCount,
                ["window_samples"] = windowSamples
            };
            AddBaselineContext(metadata, baseline);

            anomalies.Add(new Fact
            {
                Source = "anomaly",
                Key = "ANOMALY_SESSION_SPIKE",
                Value = peakConnections,
                ServerId = context.ServerId,
                Metadata = metadata
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgAnomalyDetector] Session anomaly detection failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Detects query duration aggregate anomalies using z-score against time-bucketed baseline.
    /// Measures total elapsed time across all queries per collection interval.
    /// </summary>
    private async Task DetectQueryDurationAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.QueryDuration, context.TimeRangeStart, context.CancellationToken);

            if (baseline.SampleCount == 0) return;
            var effectiveStdDev = baseline.EffectiveStdDev;

            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(QueryDurationWindowSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var avgElapsed = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var peakElapsed = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));

            if (windowSamples == 0) return;

            var decision = AnomalyGate.EvaluateZScore(
                baseline, peakElapsed, avgElapsed,
                GetDeviationThreshold(MetricNames.QueryDuration), ModifiedZThresholdFor(MetricNames.QueryDuration, GetDeviationThreshold(MetricNames.QueryDuration)), QueryDurationFloorUs, QueryDurationFallbackUs, SigmaDisplayCap,
                window: context.TimeRangeEnd - context.TimeRangeStart);
            if (!decision.Fire) return;

            var metadata = new Dictionary<string, double>
            {
                ["peak_total_elapsed_us"] = peakElapsed,
                ["avg_total_elapsed_us"] = avgElapsed,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_stddev"] = effectiveStdDev,
                ["deviation_sigma"] = decision.Sigma,
                ["mean_deviation_sigma"] = decision.MeanSigma ?? 0,
                ["fire_threshold"] = decision.ThresholdUsed,
                ["baseline_low_quality"] = decision.LowQualityBaseline ? 1 : 0,
                ["fallback_exceedance"] = decision.FallbackExceedance,
                ["baseline_zero_history"] = decision.ZeroHistory ? 1 : 0,
                ["baseline_samples"] = baseline.SampleCount,
                ["window_samples"] = windowSamples
            };
            AddBaselineContext(metadata, baseline);

            anomalies.Add(new Fact
            {
                Source = "anomaly",
                Key = "ANOMALY_QUERY_DURATION",
                Value = peakElapsed,
                ServerId = context.ServerId,
                Metadata = metadata
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgAnomalyDetector] Query duration anomaly detection failed: {Message}", ex.Message);
        }
    }

    /// <summary>
    /// Detects memory utilization anomalies using z-score against time-bucketed baseline.
    /// Ported from Lite (Darling, Lite, and Dashboard all collect memory metrics).
    /// Measures total_server_memory_mb / target_server_memory_mb as memory pressure %.
    /// </summary>
    private async Task DetectMemoryAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.Memory, context.TimeRangeStart, context.CancellationToken);

            if (baseline.SampleCount == 0) return;
            var effectiveStdDev = baseline.EffectiveStdDev;

            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(MemoryWindowSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var avgPressure = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var peakPressure = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));

            if (windowSamples == 0) return;

            var decision = AnomalyGate.EvaluateZScore(
                baseline, peakPressure, avgPressure,
                GetDeviationThreshold(MetricNames.Memory), ModifiedZThresholdFor(MetricNames.Memory, GetDeviationThreshold(MetricNames.Memory)), MemoryPressureFloorPct, MemoryPressureFallbackPct, SigmaDisplayCap,
                window: context.TimeRangeEnd - context.TimeRangeStart);
            if (!decision.Fire) return;

            var metadata = new Dictionary<string, double>
            {
                ["peak_memory_pressure_pct"] = peakPressure,
                ["avg_memory_pressure_pct"] = avgPressure,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_stddev"] = effectiveStdDev,
                ["deviation_sigma"] = decision.Sigma,
                ["mean_deviation_sigma"] = decision.MeanSigma ?? 0,
                ["fire_threshold"] = decision.ThresholdUsed,
                ["baseline_low_quality"] = decision.LowQualityBaseline ? 1 : 0,
                ["fallback_exceedance"] = decision.FallbackExceedance,
                ["baseline_zero_history"] = decision.ZeroHistory ? 1 : 0,
                ["baseline_samples"] = baseline.SampleCount,
                ["window_samples"] = windowSamples
            };
            AddBaselineContext(metadata, baseline);

            anomalies.Add(new Fact
            {
                Source = "anomaly",
                Key = "ANOMALY_MEMORY_PRESSURE",
                Value = peakPressure,
                ServerId = context.ServerId,
                Metadata = metadata
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            _logger?.LogError("[PgAnomalyDetector] Memory anomaly detection failed: {Message}", ex.Message);
        }
    }

    /// <summary>Kind-Unspecified for query bounds — Npgsql 6+ rejects Kind-Utc against <c>timestamp</c>.</summary>
    private static DateTime AsNaive(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>
    /// #3653 A8 option B (lane L2a): binds a tiled window statement's six parameters — $1 server id, $2/$3 the
    /// analysis window bounds (naive UTC, exactly as every non-tiled read here binds them), then $4..$6 from
    /// <paramref name="map"/>'s <see cref="BaselineBucketMap.WindowClock"/> in the SAME order
    /// <c>PgBaselineProvider.ComputeBucketsAsync</c> binds them (transition instant, then
    /// <c>OffsetBeforeMinutes</c>, then <c>OffsetAfterMinutes</c>) — the map's clock is resolved over the
    /// ANALYSIS window (design §1), never the cached 30-day baseline window, so a caller must not substitute
    /// its own. Shared by every tiled family in this file (CPU, waits, I/O; batch/sessions/query/memory follow
    /// in lane L2b) so the bind order cannot drift between them.
    /// </summary>
    private static void BindTiledWindow(NpgsqlCommand cmd, AnalysisContext context, BaselineBucketMap map)
    {
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        cmd.Parameters.AddWithValue(AsNaive(map.WindowClock.TransitionAtUtc));
        cmd.Parameters.AddWithValue(map.WindowClock.OffsetBeforeMinutes);
        cmd.Parameters.AddWithValue(map.WindowClock.OffsetAfterMinutes);
    }

    /// <summary>
    /// #3653 A8 option B (lane L2a): reads every row of a tiled window statement into a
    /// <see cref="WindowTile"/> list, through <see cref="WindowTiles.ReadTile"/> — shared by every tiled
    /// family in this file so the ordinal wiring cannot drift between them. <paramref name="peakTimeOrdinal"/>
    /// defaults to -1 (no peak time column) for families that don't track one.
    /// </summary>
    private static async Task<List<WindowTile>> ReadTilesAsync(
        NpgsqlDataReader reader,
        int localHourOrdinal,
        int peakOrdinal,
        int meanOrdinal,
        int samplesOrdinal,
        int peakTimeOrdinal,
        CancellationToken cancellationToken)
    {
        var tiles = new List<WindowTile>();
        while (await reader.ReadAsync(cancellationToken))
        {
            tiles.Add(WindowTiles.ReadTile(reader, localHourOrdinal, peakOrdinal, meanOrdinal, samplesOrdinal, peakTimeOrdinal));
        }
        return tiles;
    }
}
