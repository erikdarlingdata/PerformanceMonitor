using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Analysis;
using PerformanceMonitorDashboard.Helpers;

namespace PerformanceMonitorDashboard.Analysis;

/// <summary>
/// Detects anomalies by comparing the analysis window's metrics against
/// time-bucketed baselines (hour-of-day x day-of-week, 30-day rolling window).
///
/// Two detection patterns:
/// - Z-score: (observed - mean) / stddev — used for continuous metrics
///   (CPU, batch requests, I/O latency, session counts, query duration, memory)
/// - Ratio: currentRate / baselineRate — used for rate/event metrics
///   (wait stats, blocking, deadlocks)
///
/// Baseline computation and caching are handled by SqlServerBaselineProvider.
///
/// Port of Lite's AnomalyDetector — uses SQL Server collect.* tables instead of DuckDB views.
/// No server_id filtering — Dashboard monitors one server per database.
/// </summary>
public class SqlServerAnomalyDetector
{
    private readonly string _connectionString;
    private readonly SqlServerBaselineProvider _baselineProvider;

    /// <summary>
    /// Default number of standard deviations above baseline mean to flag as anomalous.
    /// </summary>
    private const double DefaultDeviationThreshold = 2.0;

    /// <summary>
    /// Default ratio threshold for the wait-profile detector (peak window all-types ms/sec ÷ baseline
    /// mean). On the HONEST per-second scale now, so far below the old 5.0 that assumed a ~240x-inflated
    /// input; matches the FactScorer WaitProfileRatioFloor. Still uncalibrated as of the 2026-09 dogfood
    /// measurement (#3538 A5, #3616), which read each wait TYPE's fraction of a 4-hour window on the
    /// Darling store and not the all-types ms/sec peak-over-baseline ratio this cutoff gates; unmeasured
    /// on the Dashboard tier altogether (no Full-edition store was in that pass). The read that would
    /// calibrate it is that ratio's own distribution over a fleet, one more column on the same pass.
    /// Mirrors PerformanceMonitor.Analysis.Baselines.AnomalyThresholds.DefaultRatioThreshold (#3653).
    /// </summary>
    private const double DefaultRatioThreshold = 4.0;

    /// <summary>
    /// Default ratio threshold for event-based anomaly detection (blocking/deadlocks).
    /// </summary>
    private const double DefaultEventRatioThreshold = 3.0;

    // #1486 absolute-magnitude floors (the z-path sanity ceiling) so a z-score against a thin
    // baseline can't surface a trivial value; sigma display cap so a variance-collapsed baseline
    // can't render millions-of-sigma.
    private const double CpuFloorPct = 50.0;                // %
    private const double ReadLatencyFloorMs = 10.0;         // ms
    private const double BatchRequestFloor = 500.0;         // requests/sec
    private const double SessionCountFloor = 50.0;          // connections
    private const double QueryDurationFloorUs = 1_000_000;  // total elapsed us = 1 second
    private const double MemoryPressureFloorPct = 90.0;     // total/target %
    private const double WriteLatencyFloorMs = 20.0;        // ms, was 5
    private const double SigmaDisplayCap = 25.0;

    // Low-quality-baseline ABSOLUTE-FALLBACK bars: when the baseline is too thin to trust a z-score
    // (BaselineBucket.IsTrustworthy false), the detector fires on these instead of going silent.
    // Each is deliberately HIGHER than the matching #1486 magnitude floor above (the interaction
    // trap: a young store fires only on the higher bar, never on both-AND-ed into blindness).
    private const double CpuFallbackPct = 90.0;                 // %
    private const double MemoryPressureFallbackPct = 95.0;      // total/target %
    private const double BatchRequestFallback = 5000.0;        // requests/sec
    private const double SessionCountFallback = 500.0;         // connections
    private const double QueryDurationFallbackUs = 5_000_000;  // total elapsed us = 5 seconds
    private const double IoLatencyFallbackMs = 50.0;           // ms (read and write)

    // Wait-profile detector (DetectWaitAnomalies → one ANOMALY_WAIT_PROFILE): the current window's
    // all-types wait ms/sec (PEAK across collections, matching the z-detectors) is compared to the
    // WaitMsPerSec baseline. DefaultRatioThreshold and the FactScorer wait slope are on the HONEST
    // per-second scale now (the old 5×/20× was calibrated to a ~240×-inflated per-hour-vs-per-interval
    // input) — a sensible starting point, still uncalibrated: see DefaultRatioThreshold for what the
    // 2026-09 fleet pass measured instead, that it never reached the Dashboard tier, and which read would
    // calibrate these. The fallback exceedance is what a YOUNG store produces, and no measured store was one.
    private const double WaitProfileFallbackMsPerSec = 250.0;  // untrustworthy-baseline absolute bar
    private const double NoBaselineRatio = 100.0;             // scoring sentinel for a first-occurrence (is_new)

    /// <summary>
    /// Per-metric deviation thresholds. Metrics not listed use DefaultDeviationThreshold.
    /// </summary>
    private readonly Dictionary<string, double> _deviationThresholds = new();

    public SqlServerAnomalyDetector(string connectionString, SqlServerBaselineProvider baselineProvider)
    {
        _connectionString = connectionString;
        _baselineProvider = baselineProvider;
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
    }

    /// <summary>
    /// Detects anomalies by comparing the analysis window against time-bucketed baselines.
    /// Returns anomaly facts to be merged into the main fact list.
    /// </summary>
    public async Task<List<Fact>> DetectAnomaliesAsync(AnalysisContext context)
    {
        var anomalies = new List<Fact>();

        // Check if baseline period has any data at all — if not, skip all anomaly detection.
        if (!await HasBaselineDataAsync())
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

    /// <summary>
    /// Day-over-day object/index detection (delta-based, not stddev-baseline) since the
    /// index_object_stats collector runs daily and its counters are cumulative. Emits
    /// ANOMALY_OBJECT_GROWTH for the biggest table grower over threshold and
    /// ANOMALY_OBJECT_CONTENTION for the index with the largest new lock-wait time.
    /// </summary>
    private const decimal ObjectGrowthMbThreshold = 100m;
    private const double ObjectGrowthPctThreshold = 20.0;
    private const long ObjectLockWaitMsDeltaThreshold = 60000;

    private async Task DetectObjectStatsAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            // Growth: biggest day-over-day table grower (indexes rolled up) over threshold.
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    snaps AS
    (
        SELECT TOP (2)
            collection_time
        FROM
        (
            SELECT DISTINCT
                collection_time
            FROM collect.index_object_stats
        ) AS d
        ORDER BY
            collection_time DESC
    ),
    boundaries AS
    (
        SELECT
            latest_time = MAX(collection_time),
            prior_time = MIN(collection_time)
        FROM snaps
    ),
    cur AS
    (
        SELECT
            database_name,
            object_id,
            schema_name = MAX(schema_name),
            table_name = MAX(table_name),
            mb = SUM(reserved_mb)
        FROM collect.index_object_stats
        WHERE collection_time =
        (
            SELECT b.latest_time
            FROM boundaries AS b
        )
        GROUP BY
            database_name,
            object_id
    ),
    prv AS
    (
        SELECT
            database_name,
            object_id,
            mb = SUM(reserved_mb)
        FROM collect.index_object_stats
        WHERE collection_time =
        (
            SELECT b.prior_time
            FROM boundaries AS b
        )
        GROUP BY
            database_name,
            object_id
    )
SELECT TOP (1)
    cur.database_name,
    cur.schema_name,
    cur.table_name,
    prior_mb = prv.mb,
    current_mb = cur.mb,
    growth_mb = cur.mb - prv.mb,
    growth_pct =
        CASE
            WHEN prv.mb > 0
            THEN (cur.mb - prv.mb) * 100.0 / prv.mb
            ELSE 0
        END
FROM cur
JOIN prv
  ON  prv.database_name = cur.database_name
  AND prv.object_id = cur.object_id
CROSS JOIN boundaries AS b
WHERE b.latest_time <> b.prior_time
AND   cur.mb - prv.mb >= @growthMb
AND   CASE WHEN prv.mb > 0 THEN (cur.mb - prv.mb) * 100.0 / prv.mb ELSE 0 END >= @growthPct
ORDER BY
    cur.mb - prv.mb DESC
OPTION(MAXDOP 1, RECOMPILE);";
                cmd.Parameters.Add(new SqlParameter("@growthMb", ObjectGrowthMbThreshold));
                cmd.Parameters.Add(new SqlParameter("@growthPct", ObjectGrowthPctThreshold));

                using var reader = await cmd.ExecuteReaderAsync();
                if (await reader.ReadAsync())
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
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH
    snaps AS
    (
        SELECT TOP (2)
            collection_time
        FROM
        (
            SELECT DISTINCT
                collection_time
            FROM collect.index_object_stats
        ) AS d
        ORDER BY
            collection_time DESC
    ),
    boundaries AS
    (
        SELECT
            latest_time = MAX(collection_time),
            prior_time = MIN(collection_time)
        FROM snaps
    ),
    cur AS
    (
        SELECT
            database_name,
            object_id,
            index_id,
            schema_name,
            table_name,
            index_name,
            ms = ISNULL(row_lock_wait_in_ms, 0),
            esc = ISNULL(index_lock_promotion_count, 0)
        FROM collect.index_object_stats
        WHERE collection_time =
        (
            SELECT b.latest_time
            FROM boundaries AS b
        )
    ),
    prv AS
    (
        SELECT
            database_name,
            object_id,
            index_id,
            ms = ISNULL(row_lock_wait_in_ms, 0),
            esc = ISNULL(index_lock_promotion_count, 0)
        FROM collect.index_object_stats
        WHERE collection_time =
        (
            SELECT b.prior_time
            FROM boundaries AS b
        )
    )
SELECT TOP (1)
    cur.database_name,
    cur.schema_name,
    cur.table_name,
    cur.index_name,
    ms_delta = cur.ms - prv.ms,
    esc_delta = cur.esc - prv.esc
FROM cur
JOIN prv
  ON  prv.database_name = cur.database_name
  AND prv.object_id = cur.object_id
  AND prv.index_id = cur.index_id
CROSS JOIN boundaries AS b
WHERE b.latest_time <> b.prior_time
AND   cur.ms >= prv.ms
AND   cur.ms - prv.ms >= @msDelta
ORDER BY
    cur.ms - prv.ms DESC
OPTION(MAXDOP 1, RECOMPILE);";
                cmd.Parameters.Add(new SqlParameter("@msDelta", ObjectLockWaitMsDeltaThreshold));

                using var reader = await cmd.ExecuteReaderAsync();
                if (await reader.ReadAsync())
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
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerAnomalyDetector] Object stats anomaly detection failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Checks if the server has enough historical data for meaningful baselines.
    /// Uses wait_stats and cpu_utilization_stats as canary.
    /// </summary>
    private async Task<bool> HasBaselineDataAsync()
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    (SELECT COUNT(*) FROM collect.wait_stats
     WHERE collection_time >= @cutoff)
  + (SELECT COUNT(*) FROM collect.cpu_utilization_stats
     WHERE collection_time >= @cutoff);";

            cmd.Parameters.Add(new SqlParameter("@cutoff", DateTime.UtcNow.AddDays(-30)));

            var count = Convert.ToInt64(await cmd.ExecuteScalarAsync() ?? 0);
            return count > 0;
        }
        catch { return false; }
    }

    /// <summary>
    /// Detects CPU utilization anomalies using z-score against time-bucketed baseline.
    /// </summary>
    private async Task DetectCpuAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                SqlServerMetricNames.Cpu, context.TimeRangeStart);

            if (baseline.SampleCount == 0) return;
            // No effectiveStdDev<=0 early return — an untrustworthy/zero-dispersion baseline falls
            // back to the absolute bar (below) rather than going silent.
            var effectiveStdDev = baseline.EffectiveStdDev;

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    MAX(sqlserver_cpu_utilization) AS peak_cpu,
    AVG(CAST(sqlserver_cpu_utilization AS FLOAT)) AS avg_cpu,
    COUNT(*) AS sample_count,
    (SELECT TOP 1 collection_time FROM collect.cpu_utilization_stats
     WHERE collection_time >= @windowStart AND collection_time < @windowEnd
     ORDER BY sqlserver_cpu_utilization DESC) AS peak_time
FROM collect.cpu_utilization_stats
WHERE collection_time >= @windowStart
AND   collection_time < @windowEnd;";

            cmd.Parameters.Add(new SqlParameter("@windowStart", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@windowEnd", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var peakCpu = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var avgCpu = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            var peakTime = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3);

            if (windowSamples == 0) return;

            var decision = AnomalyGate.EvaluateZScore(
                baseline.Mean, effectiveStdDev, baseline.IsTrustworthy, peakCpu,
                GetDeviationThreshold(SqlServerMetricNames.Cpu), CpuFloorPct, CpuFallbackPct, SigmaDisplayCap);
            if (!decision.Fire) return;

            var metadata = new Dictionary<string, double>
            {
                ["peak_cpu"] = peakCpu,
                ["avg_cpu_in_window"] = avgCpu,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_stddev"] = effectiveStdDev,
                ["deviation_sigma"] = decision.Sigma,
                ["baseline_low_quality"] = decision.LowQualityBaseline ? 1 : 0,
                ["fallback_exceedance"] = decision.FallbackExceedance,
                ["baseline_samples"] = baseline.SampleCount,
                ["window_samples"] = windowSamples,
                ["confidence"] = 1.0,
                ["peak_time_ticks"] = peakTime?.Ticks ?? 0
            };
            AddBaselineContext(metadata, baseline);

            anomalies.Add(new Fact
            {
                Source = "anomaly",
                Key = "ANOMALY_CPU_SPIKE",
                Value = peakCpu,
                ServerId = context.ServerId,
                Metadata = metadata
            });
        }
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerAnomalyDetector] CPU anomaly detection failed: {ex.Message}");
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
            var baseline = await _baselineProvider.GetBaselineAsync(
                SqlServerMetricNames.WaitMsPerSec, context.TimeRangeStart);

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            // Current window: all-types wait ms/sec per collection (interval via LAG, never an assumed
            // cadence — mirrors the WaitMsPerSec baseline), then PEAK across collections.
            double peakRate;
            double totalWaitMs;
            long collectionCount;
            using (var rateCmd = connection.CreateCommand())
            {
                rateCmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH per_collection AS (
    SELECT collection_time,
           CAST(SUM(wait_time_ms_delta) AS FLOAT) AS total_wait_ms,
           DATEDIFF(SECOND, LAG(collection_time) OVER (ORDER BY collection_time), collection_time) AS interval_sec
    FROM collect.wait_stats
    WHERE collection_time >= @windowStart AND collection_time < @windowEnd
    AND   wait_time_ms_delta >= 0
    GROUP BY collection_time
)
SELECT MAX(CASE WHEN interval_sec > 0 THEN total_wait_ms / interval_sec ELSE 0 END) AS peak_ms_per_sec,
       SUM(total_wait_ms) AS total_wait_ms,
       SUM(CASE WHEN interval_sec IS NOT NULL THEN 1 ELSE 0 END) AS sample_count
FROM per_collection;";
                rateCmd.Parameters.Add(new SqlParameter("@windowStart", context.TimeRangeStart));
                rateCmd.Parameters.Add(new SqlParameter("@windowEnd", context.TimeRangeEnd));

                using var rateReader = await rateCmd.ExecuteReaderAsync();
                if (!await rateReader.ReadAsync()) return;
                peakRate = rateReader.IsDBNull(0) ? 0.0 : Convert.ToDouble(rateReader.GetValue(0));
                totalWaitMs = rateReader.IsDBNull(1) ? 0.0 : Convert.ToDouble(rateReader.GetValue(1));
                collectionCount = rateReader.IsDBNull(2) ? 0L : Convert.ToInt64(rateReader.GetValue(2));
            }

            if (collectionCount == 0) return; // no rated collection in the window

            // Trustworthy baseline → honest per-second ratio; else fall back to the absolute peak-rate
            // bar (NOT silence) so a genuinely heavy profile still surfaces on a young store (is_new).
            bool isNew;
            double ratio;
            if (baseline.IsTrustworthy && baseline.Mean > 0)
            {
                isNew = false;
                ratio = peakRate / baseline.Mean;
            }
            else
            {
                isNew = true;
                ratio = peakRate >= WaitProfileFallbackMsPerSec ? NoBaselineRatio : 0;
            }

            if (ratio < DefaultRatioThreshold) return;

            var metadata = new Dictionary<string, double>
            {
                ["current_ms_per_sec"] = peakRate,
                ["baseline_mean"] = baseline.Mean,
                ["total_wait_ms"] = totalWaitMs,
                ["ratio"] = ratio,
                ["is_new"] = isNew ? 1 : 0
            };
            AddBaselineContext(metadata, baseline);

            // Top 6 contributors — named in the metadata KEY (a Dictionary<string,double> can't hold
            // the type name in the value), value = the type's total wait ms in the window.
            using (var contribCmd = connection.CreateCommand())
            {
                contribCmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 6
    wait_type,
    CAST(SUM(wait_time_ms_delta) AS BIGINT) AS total_ms
FROM collect.wait_stats
WHERE collection_time >= @windowStart AND collection_time < @windowEnd
AND   wait_time_ms_delta > 0
GROUP BY wait_type
ORDER BY total_ms DESC;";
                contribCmd.Parameters.Add(new SqlParameter("@windowStart", context.TimeRangeStart));
                contribCmd.Parameters.Add(new SqlParameter("@windowEnd", context.TimeRangeEnd));

                using var contribReader = await contribCmd.ExecuteReaderAsync();
                while (await contribReader.ReadAsync())
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
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerAnomalyDetector] Wait anomaly detection failed: {ex.Message}");
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
                SqlServerMetricNames.Blocking, context.TimeRangeStart);
            var deadlockBaseline = await _baselineProvider.GetBaselineAsync(
                SqlServerMetricNames.Deadlock, context.TimeRangeStart);

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    (SELECT COUNT(*) FROM collect.blocking_BlockedProcessReport
     WHERE collection_time >= @windowStart AND collection_time <= @windowEnd) AS current_blocking,
    (SELECT COUNT(*) FROM collect.deadlocks
     WHERE collection_time >= @windowStart AND collection_time <= @windowEnd) AS current_deadlocks;";

            cmd.Parameters.Add(new SqlParameter("@windowStart", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@windowEnd", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

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
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerAnomalyDetector] Blocking anomaly detection failed: {ex.Message}");
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
                SqlServerMetricNames.IoLatency, context.TimeRangeStart);

            if (baseline.SampleCount == 0) return;
            var effectiveStdDev = baseline.EffectiveStdDev;

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    AVG(io_stall_read_ms_delta * 1.0 / NULLIF(num_of_reads_delta, 0)) AS avg_read_lat,
    AVG(io_stall_write_ms_delta * 1.0 / NULLIF(num_of_writes_delta, 0)) AS avg_write_lat
FROM collect.file_io_stats
WHERE collection_time >= @windowStart AND collection_time <= @windowEnd
AND   (num_of_reads_delta > 0 OR num_of_writes_delta > 0);";

            cmd.Parameters.Add(new SqlParameter("@windowStart", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@windowEnd", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var currentReadLat = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var currentWriteLat = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));

            var ioThreshold = GetDeviationThreshold(SqlServerMetricNames.IoLatency);

            // Read latency anomaly
            var readDecision = AnomalyGate.EvaluateZScore(
                baseline.Mean, effectiveStdDev, baseline.IsTrustworthy, currentReadLat,
                ioThreshold, ReadLatencyFloorMs, IoLatencyFallbackMs, SigmaDisplayCap);
            if (readDecision.Fire)
            {
                var metadata = new Dictionary<string, double>
                {
                    ["current_latency_ms"] = currentReadLat,
                    ["baseline_mean_ms"] = baseline.Mean,
                    ["baseline_stddev_ms"] = effectiveStdDev,
                    ["deviation_sigma"] = readDecision.Sigma,
                    ["baseline_low_quality"] = readDecision.LowQualityBaseline ? 1 : 0,
                    ["fallback_exceedance"] = readDecision.FallbackExceedance,
                    ["baseline_samples"] = baseline.SampleCount
                };
                AddBaselineContext(metadata, baseline);

                anomalies.Add(new Fact
                {
                    Source = "anomaly",
                    Key = "ANOMALY_READ_LATENCY",
                    Value = currentReadLat,
                    ServerId = context.ServerId,
                    Metadata = metadata
                });
            }

            // Write latency anomaly
            var writeDecision = AnomalyGate.EvaluateZScore(
                baseline.Mean, effectiveStdDev, baseline.IsTrustworthy, currentWriteLat,
                ioThreshold, WriteLatencyFloorMs, IoLatencyFallbackMs, SigmaDisplayCap);
            if (writeDecision.Fire)
            {
                var metadata = new Dictionary<string, double>
                {
                    ["current_latency_ms"] = currentWriteLat,
                    ["baseline_mean_ms"] = baseline.Mean,
                    ["baseline_stddev_ms"] = effectiveStdDev,
                    ["deviation_sigma"] = writeDecision.Sigma,
                    ["baseline_low_quality"] = writeDecision.LowQualityBaseline ? 1 : 0,
                    ["fallback_exceedance"] = writeDecision.FallbackExceedance,
                    ["baseline_samples"] = baseline.SampleCount
                };
                AddBaselineContext(metadata, baseline);

                anomalies.Add(new Fact
                {
                    Source = "anomaly",
                    Key = "ANOMALY_WRITE_LATENCY",
                    Value = currentWriteLat,
                    ServerId = context.ServerId,
                    Metadata = metadata
                });
            }
        }
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerAnomalyDetector] I/O anomaly detection failed: {ex.Message}");
        }
    }

    // Batch-request window: per-second rate per sample (#3527) — cntr_value_delta spans one collection
    // interval, so divide by the row's MEASURED sample_interval_seconds. Interval <= 0 marks an
    // unknowable delta (first sighting/reset/gap) and the row is skipped, never read as 0. Keeps the
    // window statistic in the same requests/sec unit as the baseline and the
    // BatchRequestFloor/Fallback thresholds.
    public const string BatchRequestWindowSql = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    AVG(cntr_value_delta * 1.0 / NULLIF(sample_interval_seconds, 0)) AS avg_batch,
    MAX(cntr_value_delta * 1.0 / NULLIF(sample_interval_seconds, 0)) AS peak_batch,
    COUNT(*) AS sample_count
FROM collect.perfmon_stats
WHERE collection_time >= @windowStart AND collection_time <= @windowEnd
AND   counter_name = 'Batch Requests/sec'
AND   cntr_value_delta >= 0
AND   sample_interval_seconds > 0;";

    /// <summary>
    /// Detects batch requests/sec anomalies using z-score against time-bucketed baseline.
    /// </summary>
    private async Task DetectBatchRequestAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                SqlServerMetricNames.BatchRequests, context.TimeRangeStart);

            if (baseline.SampleCount == 0) return;
            var effectiveStdDev = baseline.EffectiveStdDev;

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = BatchRequestWindowSql;

            cmd.Parameters.Add(new SqlParameter("@windowStart", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@windowEnd", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var avgBatch = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var peakBatch = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));

            if (windowSamples == 0) return;

            var decision = AnomalyGate.EvaluateZScore(
                baseline.Mean, effectiveStdDev, baseline.IsTrustworthy, peakBatch,
                GetDeviationThreshold(SqlServerMetricNames.BatchRequests), BatchRequestFloor, BatchRequestFallback, SigmaDisplayCap);
            if (!decision.Fire) return;

            var metadata = new Dictionary<string, double>
            {
                ["peak_batch_requests"] = peakBatch,
                ["avg_batch_requests"] = avgBatch,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_stddev"] = effectiveStdDev,
                ["deviation_sigma"] = decision.Sigma,
                ["baseline_low_quality"] = decision.LowQualityBaseline ? 1 : 0,
                ["fallback_exceedance"] = decision.FallbackExceedance,
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
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerAnomalyDetector] Batch request anomaly detection failed: {ex.Message}");
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
                SqlServerMetricNames.SessionCount, context.TimeRangeStart);

            if (baseline.SampleCount == 0) return;
            var effectiveStdDev = baseline.EffectiveStdDev;

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH per_collection AS (
    SELECT collection_time,
           SUM(total_sessions) AS total_connections
    FROM collect.session_stats
    WHERE collection_time >= @windowStart AND collection_time <= @windowEnd
    GROUP BY collection_time
)
SELECT AVG(CAST(total_connections AS FLOAT)) AS avg_connections,
       MAX(total_connections) AS peak_connections,
       COUNT(*) AS sample_count
FROM per_collection;";

            cmd.Parameters.Add(new SqlParameter("@windowStart", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@windowEnd", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var avgConnections = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var peakConnections = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));

            if (windowSamples == 0) return;

            var decision = AnomalyGate.EvaluateZScore(
                baseline.Mean, effectiveStdDev, baseline.IsTrustworthy, peakConnections,
                GetDeviationThreshold(SqlServerMetricNames.SessionCount), SessionCountFloor, SessionCountFallback, SigmaDisplayCap);
            if (!decision.Fire) return;

            var metadata = new Dictionary<string, double>
            {
                ["peak_connections"] = peakConnections,
                ["avg_connections"] = avgConnections,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_stddev"] = effectiveStdDev,
                ["deviation_sigma"] = decision.Sigma,
                ["baseline_low_quality"] = decision.LowQualityBaseline ? 1 : 0,
                ["fallback_exceedance"] = decision.FallbackExceedance,
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
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerAnomalyDetector] Session anomaly detection failed: {ex.Message}");
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
                SqlServerMetricNames.QueryDuration, context.TimeRangeStart);

            if (baseline.SampleCount == 0) return;
            var effectiveStdDev = baseline.EffectiveStdDev;

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH per_collection AS (
    SELECT collection_time,
           SUM(total_elapsed_time_delta) AS total_elapsed
    FROM collect.query_stats
    WHERE collection_time >= @windowStart AND collection_time <= @windowEnd
    AND   execution_count_delta > 0
    AND   total_elapsed_time_delta >= 0
    GROUP BY collection_time
)
SELECT AVG(CAST(total_elapsed AS FLOAT)) AS avg_elapsed,
       MAX(total_elapsed) AS peak_elapsed,
       COUNT(*) AS sample_count
FROM per_collection;";

            cmd.Parameters.Add(new SqlParameter("@windowStart", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@windowEnd", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var avgElapsed = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var peakElapsed = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));

            if (windowSamples == 0) return;

            var decision = AnomalyGate.EvaluateZScore(
                baseline.Mean, effectiveStdDev, baseline.IsTrustworthy, peakElapsed,
                GetDeviationThreshold(SqlServerMetricNames.QueryDuration), QueryDurationFloorUs, QueryDurationFallbackUs, SigmaDisplayCap);
            if (!decision.Fire) return;

            var metadata = new Dictionary<string, double>
            {
                ["peak_total_elapsed_us"] = peakElapsed,
                ["avg_total_elapsed_us"] = avgElapsed,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_stddev"] = effectiveStdDev,
                ["deviation_sigma"] = decision.Sigma,
                ["baseline_low_quality"] = decision.LowQualityBaseline ? 1 : 0,
                ["fallback_exceedance"] = decision.FallbackExceedance,
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
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerAnomalyDetector] Query duration anomaly detection failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Detects memory utilization anomalies using z-score against time-bucketed baseline.
    /// Measures total_memory_mb / committed_target_memory_mb as memory pressure %
    /// (the SQL Server analog of Lite's total_server_memory_mb / target_server_memory_mb).
    /// </summary>
    private async Task DetectMemoryAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                SqlServerMetricNames.Memory, context.TimeRangeStart);

            if (baseline.SampleCount == 0) return;
            var effectiveStdDev = baseline.EffectiveStdDev;

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    AVG(CAST(total_memory_mb AS FLOAT) / NULLIF(committed_target_memory_mb, 0) * 100) AS avg_pressure,
    MAX(CAST(total_memory_mb AS FLOAT) / NULLIF(committed_target_memory_mb, 0) * 100) AS peak_pressure,
    COUNT(*) AS sample_count
FROM collect.memory_stats
WHERE collection_time >= @windowStart AND collection_time <= @windowEnd
AND   committed_target_memory_mb > 0;";

            cmd.Parameters.Add(new SqlParameter("@windowStart", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@windowEnd", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var avgPressure = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var peakPressure = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));

            if (windowSamples == 0) return;

            var decision = AnomalyGate.EvaluateZScore(
                baseline.Mean, effectiveStdDev, baseline.IsTrustworthy, peakPressure,
                GetDeviationThreshold(SqlServerMetricNames.Memory), MemoryPressureFloorPct, MemoryPressureFallbackPct, SigmaDisplayCap);
            if (!decision.Fire) return;

            var metadata = new Dictionary<string, double>
            {
                ["peak_memory_pressure_pct"] = peakPressure,
                ["avg_memory_pressure_pct"] = avgPressure,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_stddev"] = effectiveStdDev,
                ["deviation_sigma"] = decision.Sigma,
                ["baseline_low_quality"] = decision.LowQualityBaseline ? 1 : 0,
                ["fallback_exceedance"] = decision.FallbackExceedance,
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
        catch (Exception ex)
        {
            Logger.Error($"[SqlServerAnomalyDetector] Memory anomaly detection failed: {ex.Message}");
        }
    }
}
