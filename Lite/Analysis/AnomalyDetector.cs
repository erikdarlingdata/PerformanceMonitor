using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// Detects anomalies by comparing the analysis window's metrics against a
/// baseline period. When a metric deviates significantly from baseline
/// (mean + standard deviation), an ANOMALY fact is emitted.
///
/// This is the "oh shit" mode — detecting acute deviations that don't show
/// up in aggregate analysis because they're brief. A 5-minute CPU spike
/// that averages out over 4 hours is invisible to aggregate scoring but
/// obvious when compared against "what was this metric doing before?"
///
/// Baseline selection: uses the 24 hours preceding the analysis window.
/// If less data is available, uses whatever exists with lower confidence.
/// </summary>
public class AnomalyDetector
{
    private readonly DuckDbInitializer _duckDb;

    /// <summary>
    /// Minimum number of baseline samples needed for reliable detection.
    /// Below this, anomalies are still detected but with reduced confidence.
    /// </summary>
    private const int MinBaselineSamples = 10;

    /// <summary>
    /// Number of standard deviations above baseline mean to flag as anomalous.
    /// </summary>
    private const double DeviationThreshold = 2.0;

    public AnomalyDetector(DuckDbInitializer duckDb)
    {
        _duckDb = duckDb;
    }

    /// <summary>
    /// Detects anomalies by comparing the analysis window against a baseline period.
    /// Returns anomaly facts to be merged into the main fact list.
    /// </summary>
    public async Task<List<Fact>> DetectAnomaliesAsync(AnalysisContext context)
    {
        var anomalies = new List<Fact>();

        // Baseline: 24 hours preceding the analysis window
        var baselineEnd = context.TimeRangeStart;
        var baselineStart = baselineEnd.AddHours(-24);

        // Check if baseline period has any data at all — if not, skip all anomaly detection.
        // Without baseline data, everything looks anomalous.
        if (!await HasBaselineDataAsync(context.ServerId, baselineStart, baselineEnd))
            return anomalies;

        await DetectCpuAnomalies(context, baselineStart, baselineEnd, anomalies);
        await DetectWaitAnomalies(context, baselineStart, baselineEnd, anomalies);
        await DetectBlockingAnomalies(context, baselineStart, baselineEnd, anomalies);
        await DetectIoAnomalies(context, baselineStart, baselineEnd, anomalies);

        return anomalies;
    }

    /// <summary>
    /// Checks if the baseline period has any collected data.
    /// Uses wait_stats as canary — if waits are collected, other data is too.
    /// </summary>
    private async Task<bool> HasBaselineDataAsync(int serverId, DateTime baselineStart, DateTime baselineEnd)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT (SELECT COUNT(*) FROM v_wait_stats
        WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3)
     + (SELECT COUNT(*) FROM v_cpu_utilization_stats
        WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3)";
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = baselineStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = baselineEnd });

            var count = Convert.ToInt64(await cmd.ExecuteScalarAsync() ?? 0);
            return count > 0;
        }
        catch { return false; }
    }

    /// <summary>
    /// Detects CPU utilization anomalies by comparing per-sample values
    /// against the baseline distribution.
    /// </summary>
    private async Task DetectCpuAnomalies(AnalysisContext context,
        DateTime baselineStart, DateTime baselineEnd, List<Fact> anomalies)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            // Get baseline stats
            using var baselineCmd = connection.CreateCommand();
            baselineCmd.CommandText = @"
SELECT AVG(sqlserver_cpu_utilization) AS mean_cpu,
       STDDEV_SAMP(sqlserver_cpu_utilization) AS stddev_cpu,
       COUNT(*) AS sample_count
FROM v_cpu_utilization_stats
WHERE server_id = $1
AND   collection_time >= $2 AND collection_time < $3";

            baselineCmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            baselineCmd.Parameters.Add(new DuckDBParameter { Value = baselineStart });
            baselineCmd.Parameters.Add(new DuckDBParameter { Value = baselineEnd });

            double baselineMean = 0, baselineStdDev = 0;
            long baselineSamples = 0;

            using (var reader = await baselineCmd.ExecuteReaderAsync())
            {
                if (await reader.ReadAsync())
                {
                    baselineMean = reader.IsDBNull(0) ? 0 : Convert.ToDouble(reader.GetValue(0));
                    baselineStdDev = reader.IsDBNull(1) ? 0 : Convert.ToDouble(reader.GetValue(1));
                    baselineSamples = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2));
                }
            }

            if (baselineSamples < 3 || baselineStdDev <= 0) return;

            // Get peak and average in the analysis window
            using var windowCmd = connection.CreateCommand();
            windowCmd.CommandText = @"
SELECT MAX(sqlserver_cpu_utilization) AS peak_cpu,
       AVG(sqlserver_cpu_utilization) AS avg_cpu,
       COUNT(*) AS sample_count,
       (SELECT collection_time FROM v_cpu_utilization_stats
        WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
        ORDER BY sqlserver_cpu_utilization DESC LIMIT 1) AS peak_time
FROM v_cpu_utilization_stats
WHERE server_id = $1
AND   collection_time >= $2 AND collection_time < $3";

            windowCmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            windowCmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            windowCmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var windowReader = await windowCmd.ExecuteReaderAsync();
            if (!await windowReader.ReadAsync()) return;

            var peakCpu = windowReader.IsDBNull(0) ? 0.0 : Convert.ToDouble(windowReader.GetValue(0));
            var avgCpu = windowReader.IsDBNull(1) ? 0.0 : Convert.ToDouble(windowReader.GetValue(1));
            var windowSamples = windowReader.IsDBNull(2) ? 0L : Convert.ToInt64(windowReader.GetValue(2));
            var peakTime = windowReader.IsDBNull(3) ? (DateTime?)null : windowReader.GetDateTime(3);

            if (windowSamples == 0) return;

            // Check if peak deviates significantly from baseline
            var deviation = (peakCpu - baselineMean) / baselineStdDev;
            if (deviation < DeviationThreshold || peakCpu < 50) return; // Don't flag low absolute values

            var confidence = baselineSamples >= MinBaselineSamples ? 1.0 : (double)baselineSamples / MinBaselineSamples;

            anomalies.Add(new Fact
            {
                Source = "anomaly",
                Key = "ANOMALY_CPU_SPIKE",
                Value = peakCpu,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["peak_cpu"] = peakCpu,
                    ["avg_cpu_in_window"] = avgCpu,
                    ["baseline_mean"] = baselineMean,
                    ["baseline_stddev"] = baselineStdDev,
                    ["deviation_sigma"] = deviation,
                    ["baseline_samples"] = baselineSamples,
                    ["window_samples"] = windowSamples,
                    ["confidence"] = confidence,
                    ["peak_time_ticks"] = peakTime?.Ticks ?? 0
                }
            });
        }
        catch (Exception ex)
        {
            AppLogger.Error("AnomalyDetector", $"CPU anomaly detection failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Detects wait stat anomalies — significant waits in the analysis window
    /// that were absent or much lower in the baseline.
    /// </summary>
    private async Task DetectWaitAnomalies(AnalysisContext context,
        DateTime baselineStart, DateTime baselineEnd, List<Fact> anomalies)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            // Check if baseline has any wait data at all — if not, skip
            using var checkCmd = connection.CreateCommand();
            checkCmd.CommandText = @"
SELECT COUNT(*) FROM v_wait_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3";
            checkCmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            checkCmd.Parameters.Add(new DuckDBParameter { Value = baselineStart });
            checkCmd.Parameters.Add(new DuckDBParameter { Value = baselineEnd });
            var baselineCount = Convert.ToInt64(await checkCmd.ExecuteScalarAsync() ?? 0);
            if (baselineCount == 0) return;

            // Get per-wait-type totals in both windows
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
WITH baseline AS (
    SELECT wait_type,
           SUM(delta_wait_time_ms)::BIGINT AS total_ms
    FROM v_wait_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   delta_wait_time_ms > 0
    GROUP BY wait_type
),
current_window AS (
    SELECT wait_type,
           SUM(delta_wait_time_ms)::BIGINT AS total_ms
    FROM v_wait_stats
    WHERE server_id = $1 AND collection_time >= $4 AND collection_time <= $5
    AND   delta_wait_time_ms > 0
    GROUP BY wait_type
)
SELECT c.wait_type,
       c.total_ms AS current_ms,
       COALESCE(b.total_ms, 0) AS baseline_ms
FROM current_window c
LEFT JOIN baseline b ON c.wait_type = b.wait_type
WHERE c.total_ms > 10000  -- At least 10 seconds of wait time
ORDER BY c.total_ms DESC
LIMIT 10";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = baselineStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = baselineEnd });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var waitType = reader.GetString(0);
                var currentMs = Convert.ToInt64(reader.GetValue(1));
                var baselineMs = Convert.ToInt64(reader.GetValue(2));

                // Normalize to per-hour rates before comparing (windows are different lengths)
                var baselineHours = (baselineEnd - baselineStart).TotalHours;
                var currentHours = (context.TimeRangeEnd - context.TimeRangeStart).TotalHours;
                if (baselineHours <= 0) baselineHours = 1;
                if (currentHours <= 0) currentHours = 1;

                double ratio;
                string anomalyType;

                if (baselineMs == 0)
                {
                    ratio = currentMs > 60_000 ? 100.0 : 0; // Only flag if > 1 minute total
                    anomalyType = "new";
                }
                else
                {
                    var baselineRate = baselineMs / baselineHours;
                    var currentRate = currentMs / currentHours;
                    ratio = baselineRate > 0 ? currentRate / baselineRate : 100.0;
                    anomalyType = "spike";
                }

                if (ratio < 5.0) continue; // Need at least 5x increase

                anomalies.Add(new Fact
                {
                    Source = "anomaly",
                    Key = $"ANOMALY_WAIT_{waitType}",
                    Value = currentMs,
                    ServerId = context.ServerId,
                    Metadata = new Dictionary<string, double>
                    {
                        ["current_ms"] = currentMs,
                        ["baseline_ms"] = baselineMs,
                        ["ratio"] = ratio,
                        ["is_new"] = anomalyType == "new" ? 1 : 0
                    }
                });
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("AnomalyDetector", $"Wait anomaly detection failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Detects blocking/deadlock anomalies — events in the analysis window
    /// that are significantly above baseline rates.
    /// </summary>
    private async Task DetectBlockingAnomalies(AnalysisContext context,
        DateTime baselineStart, DateTime baselineEnd, List<Fact> anomalies)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            // Check if baseline period has any data at all
            using var checkCmd = connection.CreateCommand();
            checkCmd.CommandText = @"
SELECT (SELECT COUNT(*) FROM v_blocked_process_reports
        WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3)
     + (SELECT COUNT(*) FROM v_deadlocks
        WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3)
     + (SELECT COUNT(*) FROM v_wait_stats
        WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3)";
            checkCmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            checkCmd.Parameters.Add(new DuckDBParameter { Value = baselineStart });
            checkCmd.Parameters.Add(new DuckDBParameter { Value = baselineEnd });
            var baselineDataCount = Convert.ToInt64(await checkCmd.ExecuteScalarAsync() ?? 0);
            if (baselineDataCount == 0) return; // No baseline data = can't detect anomaly

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT
    (SELECT COUNT(*) FROM v_blocked_process_reports
     WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3) AS baseline_blocking,
    (SELECT COUNT(*) FROM v_blocked_process_reports
     WHERE server_id = $1 AND collection_time >= $4 AND collection_time <= $5) AS current_blocking,
    (SELECT COUNT(*) FROM v_deadlocks
     WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3) AS baseline_deadlocks,
    (SELECT COUNT(*) FROM v_deadlocks
     WHERE server_id = $1 AND collection_time >= $4 AND collection_time <= $5) AS current_deadlocks";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = baselineStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = baselineEnd });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var baselineBlocking = Convert.ToInt64(reader.GetValue(0));
            var currentBlocking = Convert.ToInt64(reader.GetValue(1));
            var baselineDeadlocks = Convert.ToInt64(reader.GetValue(2));
            var currentDeadlocks = Convert.ToInt64(reader.GetValue(3));

            // Normalize to per-hour rates (windows are different lengths)
            var baselineHours = (baselineEnd - baselineStart).TotalHours;
            var currentHours = (context.TimeRangeEnd - context.TimeRangeStart).TotalHours;
            if (baselineHours <= 0) baselineHours = 1;
            if (currentHours <= 0) currentHours = 1;

            var baselineBlockingRate = baselineBlocking / baselineHours;
            var currentBlockingRate = currentBlocking / currentHours;
            var blockingRatio = baselineBlocking > 0 ? currentBlockingRate / baselineBlockingRate : 100.0;

            var baselineDeadlockRate = baselineDeadlocks / baselineHours;
            var currentDeadlockRate = currentDeadlocks / currentHours;
            var deadlockRatio = baselineDeadlocks > 0 ? currentDeadlockRate / baselineDeadlockRate : 100.0;

            // Blocking spike: at least 5 events AND 3x baseline rate (or new)
            if (currentBlocking >= 5 && (baselineBlocking == 0 || blockingRatio >= 3))
            {
                anomalies.Add(new Fact
                {
                    Source = "anomaly",
                    Key = "ANOMALY_BLOCKING_SPIKE",
                    Value = currentBlocking,
                    ServerId = context.ServerId,
                    Metadata = new Dictionary<string, double>
                    {
                        ["current_count"] = currentBlocking,
                        ["baseline_count"] = baselineBlocking,
                        ["ratio"] = blockingRatio
                    }
                });
            }

            // Deadlock spike: at least 3 events AND 3x baseline rate (or new)
            if (currentDeadlocks >= 3 && (baselineDeadlocks == 0 || deadlockRatio >= 3))
            {
                anomalies.Add(new Fact
                {
                    Source = "anomaly",
                    Key = "ANOMALY_DEADLOCK_SPIKE",
                    Value = currentDeadlocks,
                    ServerId = context.ServerId,
                    Metadata = new Dictionary<string, double>
                    {
                        ["current_count"] = currentDeadlocks,
                        ["baseline_count"] = baselineDeadlocks,
                        ["ratio"] = deadlockRatio
                    }
                });
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("AnomalyDetector", $"Blocking anomaly detection failed: {ex.Message}");
        }
    }

    /// <summary>
    /// Detects I/O latency anomalies — significant increase in read/write latency
    /// compared to baseline.
    /// </summary>
    private async Task DetectIoAnomalies(AnalysisContext context,
        DateTime baselineStart, DateTime baselineEnd, List<Fact> anomalies)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
WITH baseline AS (
    SELECT AVG(delta_stall_read_ms * 1.0 / NULLIF(delta_reads, 0)) AS avg_read_lat,
           AVG(delta_stall_write_ms * 1.0 / NULLIF(delta_writes, 0)) AS avg_write_lat,
           STDDEV_SAMP(delta_stall_read_ms * 1.0 / NULLIF(delta_reads, 0)) AS stddev_read,
           STDDEV_SAMP(delta_stall_write_ms * 1.0 / NULLIF(delta_writes, 0)) AS stddev_write,
           COUNT(*) AS samples
    FROM v_file_io_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   (delta_reads > 0 OR delta_writes > 0)
),
current_window AS (
    SELECT AVG(delta_stall_read_ms * 1.0 / NULLIF(delta_reads, 0)) AS avg_read_lat,
           AVG(delta_stall_write_ms * 1.0 / NULLIF(delta_writes, 0)) AS avg_write_lat
    FROM v_file_io_stats
    WHERE server_id = $1 AND collection_time >= $4 AND collection_time <= $5
    AND   (delta_reads > 0 OR delta_writes > 0)
)
SELECT b.avg_read_lat, b.stddev_read, c.avg_read_lat,
       b.avg_write_lat, b.stddev_write, c.avg_write_lat,
       b.samples
FROM baseline b, current_window c";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = baselineStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = baselineEnd });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            using var reader = await cmd.ExecuteReaderAsync();
            if (!await reader.ReadAsync()) return;

            var baselineReadLat = reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0));
            var stddevRead = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
            var currentReadLat = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2));
            var baselineWriteLat = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3));
            var stddevWrite = reader.IsDBNull(4) ? 0.0 : Convert.ToDouble(reader.GetValue(4));
            var currentWriteLat = reader.IsDBNull(5) ? 0.0 : Convert.ToDouble(reader.GetValue(5));
            var samples = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6));

            if (samples < 3) return;

            // Read latency anomaly
            if (stddevRead > 0 && currentReadLat > 10) // At least 10ms to matter
            {
                var readDeviation = (currentReadLat - baselineReadLat) / stddevRead;
                if (readDeviation >= DeviationThreshold)
                {
                    anomalies.Add(new Fact
                    {
                        Source = "anomaly",
                        Key = "ANOMALY_READ_LATENCY",
                        Value = currentReadLat,
                        ServerId = context.ServerId,
                        Metadata = new Dictionary<string, double>
                        {
                            ["current_latency_ms"] = currentReadLat,
                            ["baseline_mean_ms"] = baselineReadLat,
                            ["baseline_stddev_ms"] = stddevRead,
                            ["deviation_sigma"] = readDeviation,
                            ["baseline_samples"] = samples
                        }
                    });
                }
            }

            // Write latency anomaly
            if (stddevWrite > 0 && currentWriteLat > 5) // At least 5ms to matter
            {
                var writeDeviation = (currentWriteLat - baselineWriteLat) / stddevWrite;
                if (writeDeviation >= DeviationThreshold)
                {
                    anomalies.Add(new Fact
                    {
                        Source = "anomaly",
                        Key = "ANOMALY_WRITE_LATENCY",
                        Value = currentWriteLat,
                        ServerId = context.ServerId,
                        Metadata = new Dictionary<string, double>
                        {
                            ["current_latency_ms"] = currentWriteLat,
                            ["baseline_mean_ms"] = baselineWriteLat,
                            ["baseline_stddev_ms"] = stddevWrite,
                            ["deviation_sigma"] = writeDeviation,
                            ["baseline_samples"] = samples
                        }
                    });
                }
            }
        }
        catch (Exception ex)
        {
            AppLogger.Error("AnomalyDetector", $"I/O anomaly detection failed: {ex.Message}");
        }
    }
}
