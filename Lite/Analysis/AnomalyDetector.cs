using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using static PerformanceMonitor.Analysis.Baselines.AnomalyThresholds;

namespace PerformanceMonitorLite.Analysis;

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
/// Baseline computation and caching are handled by BaselineProvider.
/// </summary>
public class AnomalyDetector
{
    private readonly DuckDbInitializer _duckDb;
    private readonly BaselineProvider _baselineProvider;

    /// <summary>
    /// Per-metric deviation thresholds. Metrics not listed use DefaultDeviationThreshold.
    /// </summary>
    private readonly Dictionary<string, double> _deviationThresholds = new();

    public AnomalyDetector(DuckDbInitializer duckDb, BaselineProvider baselineProvider)
    {
        _duckDb = duckDb;
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
        /* #1743: the robust frame the modified-z was judged in (zeros for an event-family metric
           still on the classical path), and the honest confidence the FactScorer multiplies by —
           derived from tier + sample density, no longer a hardcoded 1.0. */
        metadata["baseline_median"] = baseline.Median;
        metadata["baseline_mad"] = baseline.Mad;
        metadata["confidence"] = baseline.Confidence;
    }

    /// <summary>
    /// Detects anomalies by comparing the analysis window against time-bucketed baselines.
    /// Returns anomaly facts to be merged into the main fact list.
    /// </summary>
    public async Task<List<Fact>> DetectAnomaliesAsync(AnalysisContext context)
    {
        var anomalies = new List<Fact>();

        /* Check if baseline period has any data at all — if not, skip all anomaly detection.

           #2506: the gate's 30 days are measured back from the WINDOW's end, not from the clock. Every
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
            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            // Growth: biggest day-over-day table grower (indexes rolled up) over threshold.
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = @"
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
                cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
                cmd.Parameters.Add(new DuckDBParameter { Value = ObjectGrowthMbThreshold });
                cmd.Parameters.Add(new DuckDBParameter { Value = ObjectGrowthPctThreshold });

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
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = @"
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
                cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
                cmd.Parameters.Add(new DuckDBParameter { Value = ObjectLockWaitMsDeltaThreshold });

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
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            AppLogger.Error("AnomalyDetector", $"Object stats anomaly detection failed: {ex.Message}");
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
            using var readLock = _duckDb.AcquireReadLock(cancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT (SELECT COUNT(*) FROM v_wait_stats
        WHERE server_id = $1 AND collection_time >= $2)
     + (SELECT COUNT(*) FROM v_cpu_utilization_stats
        WHERE server_id = $1 AND collection_time >= $2)";
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = windowEnd.AddDays(-30) });

            var count = Convert.ToInt64(await cmd.ExecuteScalarAsync(cancellationToken) ?? 0);
            return count > 0;
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, cancellationToken))
        {
            /* No baseline data reads as "skip anomaly detection", which is right — but an
               abandonment is NOT that answer, and swallowing it here would let the pass go on
               through nine detectors under a token that had already fired (#2443). */
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
            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.Cpu, context.TimeRangeStart, context.CancellationToken);

            if (baseline.SampleCount == 0) return;
            // No effectiveStdDev<=0 early return — an untrustworthy/zero-dispersion baseline falls
            // back to the absolute bar (below) rather than going silent.
            var effectiveStdDev = baseline.EffectiveStdDev;

            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT MAX(sqlserver_cpu_utilization) AS peak_cpu,
       AVG(sqlserver_cpu_utilization) AS avg_cpu,
       COUNT(*) AS sample_count,
       (SELECT collection_time FROM v_cpu_utilization_stats
        WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
        ORDER BY sqlserver_cpu_utilization DESC LIMIT 1) AS peak_time
FROM v_cpu_utilization_stats
WHERE server_id = $1
AND   collection_time >= $2 AND collection_time < $3";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var peakCpu = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var avgCpu = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            var peakTime = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3);

            if (windowSamples == 0) return;

            var decision = AnomalyGate.EvaluateZScore(
                baseline, peakCpu,
                GetDeviationThreshold(MetricNames.Cpu), ModifiedZThresholdFor(MetricNames.Cpu, GetDeviationThreshold(MetricNames.Cpu)), CpuFloorPct, CpuFallbackPct, SigmaDisplayCap);
            if (!decision.Fire) return;

            var metadata = new Dictionary<string, double>
            {
                ["peak_cpu"] = peakCpu,
                ["avg_cpu_in_window"] = avgCpu,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_stddev"] = effectiveStdDev,
                ["deviation_sigma"] = decision.Sigma,
                ["fire_threshold"] = decision.ThresholdUsed,
                ["baseline_low_quality"] = decision.LowQualityBaseline ? 1 : 0,
                ["fallback_exceedance"] = decision.FallbackExceedance,
                ["baseline_samples"] = baseline.SampleCount,
                ["window_samples"] = windowSamples,
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
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            AppLogger.Error("AnomalyDetector", $"CPU anomaly detection failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Detects a shift in the wait PROFILE — the whole-server all-types wait rate (ms/sec) running
    /// significantly above its time-bucketed baseline — and emits ONE ANOMALY_WAIT_PROFILE fact with
    /// the top wait types as contrib_&lt;TYPE&gt; metadata. This replaces the old per-type
    /// ANOMALY_WAIT_&lt;type&gt; facts, which (a) compared a per-hour per-type value to a per-interval
    /// all-types baseline — a ~240x unit inflation — and (b) missed a minority-but-real wait (e.g.
    /// RESOURCE_SEMAPHORE while CX* dominate the summed baseline). Comparing all-types-vs-all-types on
    /// the honest per-second scale fixes units, aggregation, and the per-type cascade together.
    /// </summary>
    private async Task DetectWaitAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.WaitMsPerSec, context.TimeRangeStart, context.CancellationToken);

            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            // Current window: all-types wait ms/sec per collection (interval via LAG, never an assumed
            // cadence — mirrors the WaitMsPerSec baseline), then PEAK across collections (matching the
            // z-detectors' peak-representative value). Window bound aligned to the baseline: >= $2 AND < $3.
            double peakRate;
            double totalWaitMs;
            long collectionCount;
            using (var rateCmd = connection.CreateCommand())
            {
                rateCmd.CommandText = @"
WITH per_collection AS (
    SELECT collection_time,
           SUM(delta_wait_time_ms)::DOUBLE PRECISION AS total_wait_ms,
           extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_sec
    FROM v_wait_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   delta_wait_time_ms >= 0
    GROUP BY collection_time
)
SELECT MAX(CASE WHEN interval_sec > 0 THEN total_wait_ms / interval_sec ELSE 0 END) AS peak_ms_per_sec,
       SUM(total_wait_ms) AS total_wait_ms,
       COUNT(*) FILTER (WHERE interval_sec IS NOT NULL) AS sample_count
FROM per_collection";
                rateCmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
                rateCmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
                rateCmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

                using var rateReader = await rateCmd.ExecuteReaderAsync(context.CancellationToken);
                if (!await rateReader.ReadAsync(context.CancellationToken)) return;
                peakRate = rateReader.IsDBNull(0) ? 0.0 : Convert.ToDouble(rateReader.GetValue(0));
                totalWaitMs = rateReader.IsDBNull(1) ? 0.0 : Convert.ToDouble(rateReader.GetValue(1));
                collectionCount = rateReader.IsDBNull(2) ? 0L : Convert.ToInt64(rateReader.GetValue(2));
            }

            if (collectionCount == 0) return; // no rated collection in the window

            /* #1743: the modified z-score replaces the ratio as the trusted-baseline trigger —
               fleet-measured, it strictly CONTAINS the ratio's catches at every cutoff (nothing the
               ratio fired on scored under it), and it sees the masked-surge class the ratio cannot:
               a burst-scarred baseline's inflated mean pulled real sustained deviations under 4x
               (worst prod server: stddev 67.9x its robust sigma). The magnitude floor stays AND-ed
               exactly as the ratio path had it — the wait family is heavy-tailed by nature, and the
               floor is what was measured WITH the 5.0 cutoff. The ratio still rides the metadata
               for display and for scoring pre-#1743 facts; a bucket without robust stats keeps the
               classical ratio trigger; an untrustworthy baseline keeps the absolute peak-rate bar
               (NOT silence) so a genuinely heavy profile still surfaces on a young store (is_new). */
            bool isNew;
            double ratio;
            var modifiedZ = BaselineMath.ModifiedZScore(baseline, peakRate);
            if (baseline.IsTrustworthy && baseline.EffectiveRobustSigma > 0)
            {
                isNew = false;
                ratio = baseline.Mean > 0 ? peakRate / baseline.Mean : 0;
                if (modifiedZ < HeavyTailModifiedZThreshold || peakRate < WaitProfileFallbackMsPerSec) return;
            }
            else if (baseline.IsTrustworthy && baseline.Mean > 0)
            {
                isNew = false;
                ratio = peakRate / baseline.Mean;
                if (ratio < DefaultRatioThreshold) return;
            }
            else
            {
                isNew = true;
                ratio = peakRate >= WaitProfileFallbackMsPerSec ? NoBaselineRatio : 0;
                if (ratio < DefaultRatioThreshold) return;
            }

            var metadata = new Dictionary<string, double>
            {
                ["current_ms_per_sec"] = peakRate,
                ["baseline_mean"] = baseline.Mean,
                ["total_wait_ms"] = totalWaitMs,
                ["ratio"] = ratio,
                ["modified_z"] = modifiedZ,
                ["is_new"] = isNew ? 1 : 0
            };
            AddBaselineContext(metadata, baseline);

            // Top 6 contributors — named in the metadata KEY (a Dictionary<string,double> can't hold
            // the type name in the value), value = the type's total wait ms in the window.
            using (var contribCmd = connection.CreateCommand())
            {
                contribCmd.CommandText = @"
SELECT wait_type,
       SUM(delta_wait_time_ms)::BIGINT AS total_ms
FROM v_wait_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
AND   delta_wait_time_ms > 0
GROUP BY wait_type
ORDER BY total_ms DESC
LIMIT 6";
                contribCmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
                contribCmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
                contribCmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

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
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            AppLogger.Error("AnomalyDetector", $"Wait anomaly detection failed: {ex.Message}");
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

            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            /* current_blocking: prefer the blocked-process-report; fall back to the always-on DMV
               snapshot so RDS (where the BPR session is empty) still counts blocking. Mirrors the
               overview/alert path (LocalDataService.Overview.cs / LocalDataService.Blocking.cs). */
            cmd.CommandText = @"
SELECT
    COALESCE(NULLIF(
        (SELECT COUNT(*) FROM v_blocked_process_reports
         WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3), 0),
        (SELECT COUNT(*) FROM v_dmv_blocking_snapshots
         WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3)) AS current_blocking,
    (SELECT COUNT(*) FROM v_deadlocks
     WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3) AS current_deadlocks";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

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
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            AppLogger.Error("AnomalyDetector", $"Blocking anomaly detection failed: {ex.Message}");
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

            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT AVG(delta_stall_read_ms * 1.0 / NULLIF(delta_reads, 0)) AS avg_read_lat,
       AVG(delta_stall_write_ms * 1.0 / NULLIF(delta_writes, 0)) AS avg_write_lat
FROM v_file_io_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
AND   (delta_reads > 0 OR delta_writes > 0)";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var currentReadLat = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var currentWriteLat = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));

            var ioThreshold = GetDeviationThreshold(MetricNames.IoLatency);

            // Read latency anomaly
            var readDecision = AnomalyGate.EvaluateZScore(
                baseline, currentReadLat,
                ioThreshold, ModifiedZThresholdFor(MetricNames.IoLatency, ioThreshold), ReadLatencyFloorMs, IoLatencyFallbackMs, SigmaDisplayCap);
            if (readDecision.Fire)
            {
                var metadata = new Dictionary<string, double>
                {
                    ["current_latency_ms"] = currentReadLat,
                    ["baseline_mean_ms"] = baseline.Mean,
                    ["baseline_stddev_ms"] = effectiveStdDev,
                    ["deviation_sigma"] = readDecision.Sigma,
                ["fire_threshold"] = readDecision.ThresholdUsed,
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
                baseline, currentWriteLat,
                ioThreshold, ModifiedZThresholdFor(MetricNames.IoLatency, ioThreshold), WriteLatencyFloorMs, IoLatencyFallbackMs, SigmaDisplayCap);
            if (writeDecision.Fire)
            {
                var metadata = new Dictionary<string, double>
                {
                    ["current_latency_ms"] = currentWriteLat,
                    ["baseline_mean_ms"] = baseline.Mean,
                    ["baseline_stddev_ms"] = effectiveStdDev,
                    ["deviation_sigma"] = writeDecision.Sigma,
                ["fire_threshold"] = writeDecision.ThresholdUsed,
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
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            AppLogger.Error("AnomalyDetector", $"I/O anomaly detection failed: {ex.Message}");
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

            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT AVG(delta_cntr_value) AS avg_batch,
       MAX(delta_cntr_value) AS peak_batch,
       COUNT(*) AS sample_count
FROM v_perfmon_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
AND   counter_name = 'Batch Requests/sec'
AND   delta_cntr_value >= 0";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var avgBatch = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var peakBatch = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));

            if (windowSamples == 0) return;

            var decision = AnomalyGate.EvaluateZScore(
                baseline, peakBatch,
                GetDeviationThreshold(MetricNames.BatchRequests), ModifiedZThresholdFor(MetricNames.BatchRequests, GetDeviationThreshold(MetricNames.BatchRequests)), BatchRequestFloor, BatchRequestFallback, SigmaDisplayCap);
            if (!decision.Fire) return;

            var metadata = new Dictionary<string, double>
            {
                ["peak_batch_requests"] = peakBatch,
                ["avg_batch_requests"] = avgBatch,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_stddev"] = effectiveStdDev,
                ["deviation_sigma"] = decision.Sigma,
                ["fire_threshold"] = decision.ThresholdUsed,
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
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            AppLogger.Error("AnomalyDetector", $"Batch request anomaly detection failed: {ex.Message}");
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

            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
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

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var avgConnections = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var peakConnections = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));

            if (windowSamples == 0) return;

            var decision = AnomalyGate.EvaluateZScore(
                baseline, peakConnections,
                GetDeviationThreshold(MetricNames.SessionCount), ModifiedZThresholdFor(MetricNames.SessionCount, GetDeviationThreshold(MetricNames.SessionCount)), SessionCountFloor, SessionCountFallback, SigmaDisplayCap);
            if (!decision.Fire) return;

            var metadata = new Dictionary<string, double>
            {
                ["peak_connections"] = peakConnections,
                ["avg_connections"] = avgConnections,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_stddev"] = effectiveStdDev,
                ["deviation_sigma"] = decision.Sigma,
                ["fire_threshold"] = decision.ThresholdUsed,
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
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            AppLogger.Error("AnomalyDetector", $"Session anomaly detection failed: {ex.Message}");
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

            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
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

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var avgElapsed = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var peakElapsed = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));

            if (windowSamples == 0) return;

            var decision = AnomalyGate.EvaluateZScore(
                baseline, peakElapsed,
                GetDeviationThreshold(MetricNames.QueryDuration), ModifiedZThresholdFor(MetricNames.QueryDuration, GetDeviationThreshold(MetricNames.QueryDuration)), QueryDurationFloorUs, QueryDurationFallbackUs, SigmaDisplayCap);
            if (!decision.Fire) return;

            var metadata = new Dictionary<string, double>
            {
                ["peak_total_elapsed_us"] = peakElapsed,
                ["avg_total_elapsed_us"] = avgElapsed,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_stddev"] = effectiveStdDev,
                ["deviation_sigma"] = decision.Sigma,
                ["fire_threshold"] = decision.ThresholdUsed,
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
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            AppLogger.Error("AnomalyDetector", $"Query duration anomaly detection failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Detects memory utilization anomalies using z-score against time-bucketed baseline.
    /// Dashboard and Darling collect memory metrics too; mirror any change across all three.
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

            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT AVG(total_server_memory_mb::DOUBLE PRECISION / NULLIF(target_server_memory_mb::DOUBLE PRECISION, 0) * 100) AS avg_pressure,
       MAX(total_server_memory_mb::DOUBLE PRECISION / NULLIF(target_server_memory_mb::DOUBLE PRECISION, 0) * 100) AS peak_pressure,
       COUNT(*) AS sample_count
FROM v_memory_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
AND   target_server_memory_mb > 0";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var avgPressure = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var peakPressure = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));

            if (windowSamples == 0) return;

            var decision = AnomalyGate.EvaluateZScore(
                baseline, peakPressure,
                GetDeviationThreshold(MetricNames.Memory), ModifiedZThresholdFor(MetricNames.Memory, GetDeviationThreshold(MetricNames.Memory)), MemoryPressureFloorPct, MemoryPressureFallbackPct, SigmaDisplayCap);
            if (!decision.Fire) return;

            var metadata = new Dictionary<string, double>
            {
                ["peak_memory_pressure_pct"] = peakPressure,
                ["avg_memory_pressure_pct"] = avgPressure,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_stddev"] = effectiveStdDev,
                ["deviation_sigma"] = decision.Sigma,
                ["fire_threshold"] = decision.ThresholdUsed,
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
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            AppLogger.Error("AnomalyDetector", $"Memory anomaly detection failed: {ex.Message}");
        }
    }
}
