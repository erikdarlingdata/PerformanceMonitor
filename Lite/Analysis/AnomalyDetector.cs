using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;

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
    /// Default number of standard deviations above baseline mean to flag as anomalous.
    /// </summary>
    private const double DefaultDeviationThreshold = 2.0;

    /// <summary>
    /// Default ratio threshold for rate-based anomaly detection (wait stats).
    /// </summary>
    private const double DefaultRatioThreshold = 5.0;

    /// <summary>
    /// Default ratio threshold for event-based anomaly detection (blocking/deadlocks).
    /// </summary>
    private const double DefaultEventRatioThreshold = 3.0;

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
    }

    /// <summary>
    /// Detects anomalies by comparing the analysis window against time-bucketed baselines.
    /// Returns anomaly facts to be merged into the main fact list.
    /// </summary>
    public async Task<List<Fact>> DetectAnomaliesAsync(AnalysisContext context)
    {
        var anomalies = new List<Fact>();

        // Check if baseline period has any data at all — if not, skip all anomaly detection.
        if (!await HasBaselineDataAsync(context.ServerId))
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

        return anomalies;
    }

    /// <summary>
    /// Checks if the server has enough historical data for meaningful baselines.
    /// Uses wait_stats as canary — if waits are collected, other data is too.
    /// </summary>
    private async Task<bool> HasBaselineDataAsync(int serverId)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT (SELECT COUNT(*) FROM v_wait_stats
        WHERE server_id = $1 AND collection_time >= $2)
     + (SELECT COUNT(*) FROM v_cpu_utilization_stats
        WHERE server_id = $1 AND collection_time >= $2)";
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddDays(-30) });

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
                context.ServerId, MetricNames.Cpu, context.TimeRangeStart);

            if (baseline.SampleCount == 0) return;
            var effectiveStdDev = baseline.EffectiveStdDev;
            if (effectiveStdDev <= 0) return; // Zero mean + zero stddev — skip

            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

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

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var peakCpu = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var avgCpu = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            var peakTime = reader.IsDBNull(3) ? (DateTime?)null : reader.GetDateTime(3);

            if (windowSamples == 0) return;

            var deviation = (peakCpu - baseline.Mean) / effectiveStdDev;
            if (deviation < GetDeviationThreshold(MetricNames.Cpu) || peakCpu < 50) return;

            var metadata = new Dictionary<string, double>
            {
                ["peak_cpu"] = peakCpu,
                ["avg_cpu_in_window"] = avgCpu,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_stddev"] = effectiveStdDev,
                ["deviation_sigma"] = deviation,
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
            AppLogger.Error("AnomalyDetector", $"CPU anomaly detection failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Detects wait stat anomalies — total wait time significantly above
    /// baseline rate for this time bucket. Uses ratio-based scoring.
    /// </summary>
    private async Task DetectWaitAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.WaitStats, context.TimeRangeStart);

            // No baseline data at all — can't distinguish "new" waits from "always present."
            // Skip rather than flagging everything as anomalous.
            if (baseline.SampleCount == 0) return;

            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            // Get per-wait-type totals in the analysis window
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT wait_type,
       SUM(delta_wait_time_ms)::BIGINT AS total_ms
FROM v_wait_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
AND   delta_wait_time_ms > 0
GROUP BY wait_type
HAVING SUM(delta_wait_time_ms) > 10000
ORDER BY total_ms DESC
LIMIT 10";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            var currentHours = (context.TimeRangeEnd - context.TimeRangeStart).TotalHours;
            if (currentHours <= 0) currentHours = 1;

            // Baseline mean is total wait ms per collection interval for this time bucket.
            // If no baseline, use ratio=100 for significant new waits.
            var baselineRate = baseline.SampleCount > 0 ? baseline.Mean : 0;

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var waitType = reader.GetString(0);
                var currentMs = Convert.ToInt64(reader.GetValue(1));
                var currentRate = currentMs / currentHours;

                double ratio;
                string anomalyType;

                if (baselineRate <= 0 || baseline.SampleCount == 0)
                {
                    ratio = currentMs > 60_000 ? 100.0 : 0;
                    anomalyType = "new";
                }
                else
                {
                    ratio = currentRate / baselineRate;
                    anomalyType = "spike";
                }

                if (ratio < DefaultRatioThreshold) continue;

                var metadata = new Dictionary<string, double>
                {
                    ["current_ms"] = currentMs,
                    ["baseline_mean"] = baseline.Mean,
                    ["ratio"] = ratio,
                    ["is_new"] = anomalyType == "new" ? 1 : 0
                };
                AddBaselineContext(metadata, baseline);

                anomalies.Add(new Fact
                {
                    Source = "anomaly",
                    Key = $"ANOMALY_WAIT_{waitType}",
                    Value = currentMs,
                    ServerId = context.ServerId,
                    Metadata = metadata
                });
            }
        }
        catch (Exception ex)
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
                context.ServerId, MetricNames.Blocking, context.TimeRangeStart);
            var deadlockBaseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.Deadlock, context.TimeRangeStart);

            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT
    (SELECT COUNT(*) FROM v_blocked_process_reports
     WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3) AS current_blocking,
    (SELECT COUNT(*) FROM v_deadlocks
     WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3) AS current_deadlocks";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var currentBlocking = Convert.ToInt64(reader.GetValue(0));
            var currentDeadlocks = Convert.ToInt64(reader.GetValue(1));

            // Baseline mean = events per day for this hour+dow bucket
            var baselineBlockingRate = blockingBaseline.SampleCount > 0 ? blockingBaseline.Mean : 0;
            var baselineDeadlockRate = deadlockBaseline.SampleCount > 0 ? deadlockBaseline.Mean : 0;

            // Blocking spike: at least 5 events AND 3x baseline rate (or no baseline)
            if (currentBlocking >= 5 && (baselineBlockingRate <= 0 || currentBlocking / Math.Max(baselineBlockingRate, 1) >= DefaultEventRatioThreshold))
            {
                var metadata = new Dictionary<string, double>
                {
                    ["current_count"] = currentBlocking,
                    ["baseline_rate"] = baselineBlockingRate,
                    ["ratio"] = baselineBlockingRate > 0 ? currentBlocking / baselineBlockingRate : 100.0
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

            // Deadlock spike: at least 3 events AND 3x baseline rate (or no baseline)
            if (currentDeadlocks >= 3 && (baselineDeadlockRate <= 0 || currentDeadlocks / Math.Max(baselineDeadlockRate, 1) >= DefaultEventRatioThreshold))
            {
                var metadata = new Dictionary<string, double>
                {
                    ["current_count"] = currentDeadlocks,
                    ["baseline_rate"] = baselineDeadlockRate,
                    ["ratio"] = baselineDeadlockRate > 0 ? currentDeadlocks / baselineDeadlockRate : 100.0
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
                context.ServerId, MetricNames.IoLatency, context.TimeRangeStart);

            if (baseline.SampleCount == 0) return;
            var effectiveStdDev = baseline.EffectiveStdDev;
            if (effectiveStdDev <= 0) return;

            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

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

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var currentReadLat = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var currentWriteLat = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));

            var ioThreshold = GetDeviationThreshold(MetricNames.IoLatency);

            // Read latency anomaly
            if (currentReadLat > 10)
            {
                var readDeviation = (currentReadLat - baseline.Mean) / effectiveStdDev;
                if (readDeviation >= ioThreshold)
                {
                    var metadata = new Dictionary<string, double>
                    {
                        ["current_latency_ms"] = currentReadLat,
                        ["baseline_mean_ms"] = baseline.Mean,
                        ["baseline_stddev_ms"] = effectiveStdDev,
                        ["deviation_sigma"] = readDeviation,
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
            }

            // Write latency anomaly
            if (currentWriteLat > 5)
            {
                var writeDeviation = (currentWriteLat - baseline.Mean) / effectiveStdDev;
                if (writeDeviation >= ioThreshold)
                {
                    var metadata = new Dictionary<string, double>
                    {
                        ["current_latency_ms"] = currentWriteLat,
                        ["baseline_mean_ms"] = baseline.Mean,
                        ["baseline_stddev_ms"] = effectiveStdDev,
                        ["deviation_sigma"] = writeDeviation,
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
        }
        catch (Exception ex)
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
                context.ServerId, MetricNames.BatchRequests, context.TimeRangeStart);

            if (baseline.SampleCount == 0) return;
            var effectiveStdDev = baseline.EffectiveStdDev;
            if (effectiveStdDev <= 0) return;

            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

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

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var avgBatch = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var peakBatch = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));

            if (windowSamples == 0) return;

            var deviation = (peakBatch - baseline.Mean) / effectiveStdDev;
            if (deviation < GetDeviationThreshold(MetricNames.BatchRequests)) return;

            var metadata = new Dictionary<string, double>
            {
                ["peak_batch_requests"] = peakBatch,
                ["avg_batch_requests"] = avgBatch,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_stddev"] = effectiveStdDev,
                ["deviation_sigma"] = deviation,
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
                context.ServerId, MetricNames.SessionCount, context.TimeRangeStart);

            if (baseline.SampleCount == 0) return;
            var effectiveStdDev = baseline.EffectiveStdDev;
            if (effectiveStdDev <= 0) return;

            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
WITH per_collection AS (
    SELECT collection_time,
           SUM(connection_count)::DOUBLE AS total_connections
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

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var avgConnections = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var peakConnections = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));

            if (windowSamples == 0) return;

            var deviation = (peakConnections - baseline.Mean) / effectiveStdDev;
            if (deviation < GetDeviationThreshold(MetricNames.SessionCount)) return;

            var metadata = new Dictionary<string, double>
            {
                ["peak_connections"] = peakConnections,
                ["avg_connections"] = avgConnections,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_stddev"] = effectiveStdDev,
                ["deviation_sigma"] = deviation,
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
                context.ServerId, MetricNames.QueryDuration, context.TimeRangeStart);

            if (baseline.SampleCount == 0) return;
            var effectiveStdDev = baseline.EffectiveStdDev;
            if (effectiveStdDev <= 0) return;

            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
WITH per_collection AS (
    SELECT collection_time,
           SUM(delta_elapsed_time)::DOUBLE AS total_elapsed
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

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var avgElapsed = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var peakElapsed = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));

            if (windowSamples == 0) return;

            var deviation = (peakElapsed - baseline.Mean) / effectiveStdDev;
            if (deviation < GetDeviationThreshold(MetricNames.QueryDuration)) return;

            var metadata = new Dictionary<string, double>
            {
                ["peak_total_elapsed_us"] = peakElapsed,
                ["avg_total_elapsed_us"] = avgElapsed,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_stddev"] = effectiveStdDev,
                ["deviation_sigma"] = deviation,
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
            AppLogger.Error("AnomalyDetector", $"Query duration anomaly detection failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Detects memory utilization anomalies using z-score against time-bucketed baseline.
    /// Lite-only — Dashboard does not collect memory metrics.
    /// Measures total_server_memory_mb / target_server_memory_mb as memory pressure %.
    /// </summary>
    private async Task DetectMemoryAnomalies(AnalysisContext context, List<Fact> anomalies)
    {
        try
        {
            var baseline = await _baselineProvider.GetBaselineAsync(
                context.ServerId, MetricNames.Memory, context.TimeRangeStart);

            if (baseline.SampleCount == 0) return;
            var effectiveStdDev = baseline.EffectiveStdDev;
            if (effectiveStdDev <= 0) return;

            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT AVG(total_server_memory_mb::DOUBLE / NULLIF(target_server_memory_mb::DOUBLE, 0) * 100) AS avg_pressure,
       MAX(total_server_memory_mb::DOUBLE / NULLIF(target_server_memory_mb::DOUBLE, 0) * 100) AS peak_pressure,
       COUNT(*) AS sample_count
FROM v_memory_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
AND   target_server_memory_mb > 0";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var avgPressure = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var peakPressure = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));

            if (windowSamples == 0) return;

            var deviation = (peakPressure - baseline.Mean) / effectiveStdDev;
            if (deviation < GetDeviationThreshold(MetricNames.Memory)) return;

            var metadata = new Dictionary<string, double>
            {
                ["peak_memory_pressure_pct"] = peakPressure,
                ["avg_memory_pressure_pct"] = avgPressure,
                ["baseline_mean"] = baseline.Mean,
                ["baseline_stddev"] = effectiveStdDev,
                ["deviation_sigma"] = deviation,
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
            AppLogger.Error("AnomalyDetector", $"Memory anomaly detection failed: {ex.Message}");
        }
    }
}
