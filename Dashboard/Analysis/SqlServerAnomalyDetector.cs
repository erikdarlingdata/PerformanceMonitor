using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitorDashboard.Helpers;

namespace PerformanceMonitorDashboard.Analysis;

/// <summary>
/// Detects anomalies by comparing the analysis window's metrics against
/// time-bucketed baselines (hour-of-day x day-of-week, 30-day rolling window).
///
/// Two detection patterns:
/// - Z-score: (observed - mean) / stddev — used for continuous metrics
///   (CPU, batch requests, I/O latency, session counts, query duration)
/// - Ratio: currentRate / baselineRate — used for rate/event metrics
///   (wait stats, blocking, deadlocks)
///
/// Baseline computation and caching are handled by SqlServerBaselineProvider.
///
/// Port of Lite's AnomalyDetector — uses SQL Server collect.* tables instead of DuckDB views.
/// No server_id filtering — Dashboard monitors one server per database.
/// No memory metric — Dashboard doesn't collect memory stats.
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

        return anomalies;
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
            var effectiveStdDev = baseline.EffectiveStdDev;
            if (effectiveStdDev <= 0) return;

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

            var deviation = (peakCpu - baseline.Mean) / effectiveStdDev;
            if (deviation < GetDeviationThreshold(SqlServerMetricNames.Cpu) || peakCpu < 50) return;

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
            Logger.Error($"[SqlServerAnomalyDetector] CPU anomaly detection failed: {ex.Message}");
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
                SqlServerMetricNames.WaitStats, context.TimeRangeStart);

            if (baseline.SampleCount == 0) return;

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 10
    wait_type,
    CAST(SUM(wait_time_ms_delta) AS BIGINT) AS total_ms
FROM collect.wait_stats
WHERE collection_time >= @windowStart AND collection_time <= @windowEnd
AND   wait_time_ms_delta > 0
GROUP BY wait_type
HAVING SUM(wait_time_ms_delta) > 10000
ORDER BY total_ms DESC;";

            cmd.Parameters.Add(new SqlParameter("@windowStart", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@windowEnd", context.TimeRangeEnd));

            var currentHours = (context.TimeRangeEnd - context.TimeRangeStart).TotalHours;
            if (currentHours <= 0) currentHours = 1;

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
            if (effectiveStdDev <= 0) return;

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
            Logger.Error($"[SqlServerAnomalyDetector] I/O anomaly detection failed: {ex.Message}");
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
                SqlServerMetricNames.BatchRequests, context.TimeRangeStart);

            if (baseline.SampleCount == 0) return;
            var effectiveStdDev = baseline.EffectiveStdDev;
            if (effectiveStdDev <= 0) return;

            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    AVG(cntr_value_delta) AS avg_batch,
    MAX(cntr_value_delta) AS peak_batch,
    COUNT(*) AS sample_count
FROM collect.perfmon_stats
WHERE collection_time >= @windowStart AND collection_time <= @windowEnd
AND   counter_name = 'Batch Requests/sec'
AND   cntr_value_delta >= 0;";

            cmd.Parameters.Add(new SqlParameter("@windowStart", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@windowEnd", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var avgBatch = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var peakBatch = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var windowSamples = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));

            if (windowSamples == 0) return;

            var deviation = (peakBatch - baseline.Mean) / effectiveStdDev;
            if (deviation < GetDeviationThreshold(SqlServerMetricNames.BatchRequests)) return;

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
            if (effectiveStdDev <= 0) return;

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

            var deviation = (peakConnections - baseline.Mean) / effectiveStdDev;
            if (deviation < GetDeviationThreshold(SqlServerMetricNames.SessionCount)) return;

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
            if (effectiveStdDev <= 0) return;

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

            var deviation = (peakElapsed - baseline.Mean) / effectiveStdDev;
            if (deviation < GetDeviationThreshold(SqlServerMetricNames.QueryDuration)) return;

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
            Logger.Error($"[SqlServerAnomalyDetector] Query duration anomaly detection failed: {ex.Message}");
        }
    }
}
