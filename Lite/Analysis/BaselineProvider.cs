using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// Provides time-bucketed baselines (hour-of-day x day-of-week) computed from
/// 30-day rolling history in DuckDB. Replaces the flat 24-hour lookback used
/// by the previous anomaly detection implementation.
///
/// Each baseline bucket contains mean, stddev, and sample count for a metric
/// at a specific (hour, day-of-week) combination. When a bucket has insufficient
/// samples, the provider collapses to less-specific tiers:
///   Full (hour+dow) -> Hour-only -> Flat (global mean/stddev)
///
/// Baselines are cached in memory with a 1-hour TTL to avoid redundant
/// recomputation during rapid re-analysis.
/// </summary>
public class BaselineProvider
{
    private readonly DuckDbInitializer _duckDb;

    /// <summary>Rolling window for baseline computation.</summary>
    private const int BaselineWindowDays = 30;

    /// <summary>Collapse to hour-only when full bucket has fewer than this many samples.</summary>
    private const int CollapseThreshold = 10;

    /// <summary>Restore to full bucket when sample count reaches this level (hysteresis).</summary>
    private const int RestoreThreshold = 15;

    /// <summary>Cache TTL — baselines are recomputed after this interval.</summary>
    public static TimeSpan CacheTtl { get; set; } = TimeSpan.FromHours(1);

    private readonly ConcurrentDictionary<string, CachedBaseline> _cache = new();

    public BaselineProvider(DuckDbInitializer duckDb)
    {
        _duckDb = duckDb;
    }

    /// <summary>
    /// Gets the baseline for a specific metric, server, and time bucket.
    /// Returns the most specific bucket available, collapsing as needed.
    /// </summary>
    public async Task<BaselineBucket> GetBaselineAsync(
        int serverId, string metricName, DateTime analysisTime)
    {
        var hourOfDay = analysisTime.Hour;
        var dayOfWeek = (int)analysisTime.DayOfWeek; // Sunday=0

        var baselines = await GetOrComputeBaselinesAsync(serverId, metricName, analysisTime);
        if (baselines == null || baselines.Count == 0)
            return BaselineBucket.Empty;

        // Try full bucket (hour + day-of-week)
        var fullKey = (hourOfDay, dayOfWeek);
        if (baselines.TryGetValue(fullKey, out var fullBucket) && fullBucket.SampleCount >= RestoreThreshold)
            return fullBucket;

        // If full bucket exists but below restore threshold, check if it's above collapse threshold
        // (hysteresis: don't collapse if we're between 10-14 samples and were previously using full)
        if (fullBucket != null && fullBucket.SampleCount >= CollapseThreshold)
            return fullBucket;

        // Collapse to hour-only: aggregate all days for this hour
        var hourBuckets = baselines
            .Where(kvp => kvp.Key.HourOfDay == hourOfDay)
            .Select(kvp => kvp.Value)
            .ToList();

        if (hourBuckets.Count > 0)
        {
            var collapsed = CollapseToHourOnly(hourBuckets);
            if (collapsed.SampleCount >= CollapseThreshold)
                return collapsed;
        }

        // Collapse to flat: aggregate everything
        var allBuckets = baselines.Values.ToList();
        if (allBuckets.Count > 0)
        {
            var flat = CollapseToFlat(allBuckets);
            if (flat.SampleCount >= 3) // Minimum viable baseline
                return flat;
        }

        return BaselineBucket.Empty;
    }

    /// <summary>
    /// Gets all baseline buckets for a metric/server. Used by UI for rendering
    /// expected-range bands across all time slots.
    /// </summary>
    public async Task<Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>?> GetAllBaselinesAsync(
        int serverId, string metricName, DateTime analysisTime)
    {
        return await GetOrComputeBaselinesAsync(serverId, metricName, analysisTime);
    }

    /// <summary>Forces cache eviction for a server — used during testing.</summary>
    public void InvalidateCache(int serverId)
    {
        var keysToRemove = _cache.Keys.Where(k => k.StartsWith($"{serverId}:", StringComparison.Ordinal)).ToList();
        foreach (var key in keysToRemove)
            _cache.TryRemove(key, out _);
    }

    /// <summary>Forces full cache clear — used during testing.</summary>
    public void ClearCache() => _cache.Clear();

    private async Task<Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>?> GetOrComputeBaselinesAsync(
        int serverId, string metricName, DateTime analysisTime)
    {
        var cacheKey = $"{serverId}:{metricName}";
        var roundedHour = new DateTime(analysisTime.Year, analysisTime.Month, analysisTime.Day, analysisTime.Hour, 0, 0);

        if (_cache.TryGetValue(cacheKey, out var cached) &&
            cached.ComputedAt == roundedHour &&
            (DateTime.UtcNow - cached.RealTime) < CacheTtl)
        {
            return cached.Buckets;
        }

        var buckets = await ComputeBaselinesAsync(serverId, metricName, analysisTime);

        _cache[cacheKey] = new CachedBaseline
        {
            ComputedAt = roundedHour,
            RealTime = DateTime.UtcNow,
            Buckets = buckets
        };

        return buckets;
    }

    private async Task<Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>?> ComputeBaselinesAsync(
        int serverId, string metricName, DateTime analysisTime)
    {
        var query = GetBaselineQuery(metricName);
        if (query == null) return null;

        var windowStart = analysisTime.AddDays(-BaselineWindowDays);

        try
        {
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = query;
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = windowStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = analysisTime });

            var buckets = new Dictionary<(int, int), BaselineBucket>();

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var hour = Convert.ToInt32(reader.GetValue(0));
                var dow = Convert.ToInt32(reader.GetValue(1));
                var mean = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2));
                var stddev = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3));
                var count = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4));

                buckets[(hour, dow)] = new BaselineBucket
                {
                    HourOfDay = hour,
                    DayOfWeek = dow,
                    Mean = mean,
                    StdDev = stddev,
                    SampleCount = count,
                    Tier = count >= RestoreThreshold ? BaselineTier.Full
                         : count >= CollapseThreshold ? BaselineTier.Full
                         : BaselineTier.HourOnly
                };
            }

            return buckets;
        }
        catch (Exception ex)
        {
            AppLogger.Error("BaselineProvider", $"Failed to compute baselines for {metricName}: {ex.Message}");
            return null;
        }
    }

    private static string? GetBaselineQuery(string metricName)
    {
        // All queries return: hour_of_day, day_of_week, mean_val, stddev_val, sample_count
        // Cumulative metrics (batch requests, wait stats, query duration) use CTEs for
        // restart poisoning exclusion — exclude samples where value drops to near-zero
        // when the prior sample was significantly higher.
        // Multi-row-per-collection metrics (waits, sessions, queries) aggregate per
        // collection_time first, then bucket by hour+dow.
        return metricName switch
        {
            // Point-in-time metric — no restart exclusion needed
            MetricNames.Cpu => @"
SELECT EXTRACT(HOUR FROM collection_time)::INT AS hour_of_day,
       EXTRACT(DOW FROM collection_time)::INT AS day_of_week,
       AVG(sqlserver_cpu_utilization) AS mean_val,
       STDDEV_SAMP(sqlserver_cpu_utilization) AS stddev_val,
       COUNT(*) AS sample_count
FROM v_cpu_utilization_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
GROUP BY hour_of_day, day_of_week",

            // Cumulative counter — restart exclusion via subquery with QUALIFY.
            // Excludes samples where delta drops to 0 when prior sample was > 1000
            // (restart signature for cumulative counters).
            MetricNames.BatchRequests => @"
SELECT EXTRACT(HOUR FROM collection_time)::INT AS hour_of_day,
       EXTRACT(DOW FROM collection_time)::INT AS day_of_week,
       AVG(delta_cntr_value) AS mean_val,
       STDDEV_SAMP(delta_cntr_value) AS stddev_val,
       COUNT(*) AS sample_count
FROM (
    SELECT collection_time, delta_cntr_value
    FROM v_perfmon_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   counter_name = 'Batch Requests/sec'
    AND   delta_cntr_value >= 0
    QUALIFY NOT (delta_cntr_value = 0
        AND COALESCE(LAG(delta_cntr_value) OVER (ORDER BY collection_time), 0) > 1000)
)
GROUP BY hour_of_day, day_of_week",

            // Cumulative counter, multiple rows per collection (per wait type) —
            // aggregate to total wait ms per collection first, then QUALIFY for restart exclusion
            MetricNames.WaitStats => @"
WITH per_collection AS (
    SELECT collection_time,
           SUM(delta_wait_time_ms) AS total_wait_ms
    FROM v_wait_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   delta_wait_time_ms >= 0
    GROUP BY collection_time
    QUALIFY NOT (total_wait_ms = 0
        AND COALESCE(LAG(total_wait_ms) OVER (ORDER BY collection_time), 0) > 10000)
)
SELECT EXTRACT(HOUR FROM collection_time)::INT AS hour_of_day,
       EXTRACT(DOW FROM collection_time)::INT AS day_of_week,
       AVG(total_wait_ms) AS mean_val,
       STDDEV_SAMP(total_wait_ms) AS stddev_val,
       COUNT(*) AS sample_count
FROM per_collection
GROUP BY hour_of_day, day_of_week",

            // Point-in-time, multiple rows per collection (per program_name) —
            // aggregate to total connections per collection first
            MetricNames.SessionCount => @"
WITH per_collection AS (
    SELECT collection_time,
           SUM(connection_count) AS total_connections
    FROM v_session_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    GROUP BY collection_time
)
SELECT EXTRACT(HOUR FROM collection_time)::INT AS hour_of_day,
       EXTRACT(DOW FROM collection_time)::INT AS day_of_week,
       AVG(total_connections) AS mean_val,
       STDDEV_SAMP(total_connections) AS stddev_val,
       COUNT(*) AS sample_count
FROM per_collection
GROUP BY hour_of_day, day_of_week",

            // Cumulative (plan cache), multiple rows per collection (per query) —
            // use delta columns, aggregate total elapsed per collection, QUALIFY for restart exclusion
            MetricNames.QueryDuration => @"
WITH per_collection AS (
    SELECT collection_time,
           SUM(delta_elapsed_time) AS total_elapsed
    FROM v_query_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   delta_execution_count > 0
    AND   delta_elapsed_time >= 0
    GROUP BY collection_time
    QUALIFY NOT (total_elapsed = 0
        AND COALESCE(LAG(total_elapsed) OVER (ORDER BY collection_time), 0) > 100000)
)
SELECT EXTRACT(HOUR FROM collection_time)::INT AS hour_of_day,
       EXTRACT(DOW FROM collection_time)::INT AS day_of_week,
       AVG(total_elapsed) AS mean_val,
       STDDEV_SAMP(total_elapsed) AS stddev_val,
       COUNT(*) AS sample_count
FROM per_collection
GROUP BY hour_of_day, day_of_week",

            // Point-in-time metric — no restart exclusion needed
            MetricNames.IoLatency => @"
SELECT EXTRACT(HOUR FROM collection_time)::INT AS hour_of_day,
       EXTRACT(DOW FROM collection_time)::INT AS day_of_week,
       AVG(delta_stall_read_ms * 1.0 / NULLIF(delta_reads, 0)) AS mean_val,
       STDDEV_SAMP(delta_stall_read_ms * 1.0 / NULLIF(delta_reads, 0)) AS stddev_val,
       COUNT(*) AS sample_count
FROM v_file_io_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
AND   (delta_reads > 0 OR delta_writes > 0)
GROUP BY hour_of_day, day_of_week",

            // Event-based — mean = events per day for this bucket, sample_count = distinct days observed.
            // No restart exclusion needed (event counts, not cumulative).
            MetricNames.Blocking => @"
SELECT EXTRACT(HOUR FROM collection_time)::INT AS hour_of_day,
       EXTRACT(DOW FROM collection_time)::INT AS day_of_week,
       COUNT(*)::DOUBLE / GREATEST(COUNT(DISTINCT collection_time::DATE), 1) AS mean_val,
       0::DOUBLE AS stddev_val,
       COUNT(DISTINCT collection_time::DATE) AS sample_count
FROM v_blocked_process_reports
WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
GROUP BY hour_of_day, day_of_week",

            // Event-based — same approach as blocking
            MetricNames.Deadlock => @"
SELECT EXTRACT(HOUR FROM collection_time)::INT AS hour_of_day,
       EXTRACT(DOW FROM collection_time)::INT AS day_of_week,
       COUNT(*)::DOUBLE / GREATEST(COUNT(DISTINCT collection_time::DATE), 1) AS mean_val,
       0::DOUBLE AS stddev_val,
       COUNT(DISTINCT collection_time::DATE) AS sample_count
FROM v_deadlocks
WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
GROUP BY hour_of_day, day_of_week",

            // Point-in-time metric (memory pressure %) — no restart exclusion needed
            MetricNames.Memory => @"
SELECT EXTRACT(HOUR FROM collection_time)::INT AS hour_of_day,
       EXTRACT(DOW FROM collection_time)::INT AS day_of_week,
       AVG(total_server_memory_mb::DOUBLE / NULLIF(target_server_memory_mb::DOUBLE, 0) * 100) AS mean_val,
       STDDEV_SAMP(total_server_memory_mb::DOUBLE / NULLIF(target_server_memory_mb::DOUBLE, 0) * 100) AS stddev_val,
       COUNT(*) AS sample_count
FROM v_memory_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
AND   target_server_memory_mb > 0
GROUP BY hour_of_day, day_of_week",

            // ── Chart-unit baselines (for UI bands — units match what the chart displays) ──

            // Buffer pool MB (chart shows this, not pressure %)
            MetricNames.MemoryBufferPoolMb => @"
SELECT EXTRACT(HOUR FROM collection_time)::INT AS hour_of_day,
       EXTRACT(DOW FROM collection_time)::INT AS day_of_week,
       AVG(buffer_pool_mb::DOUBLE) AS mean_val,
       STDDEV_SAMP(buffer_pool_mb::DOUBLE) AS stddev_val,
       COUNT(*) AS sample_count
FROM v_memory_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
AND   buffer_pool_mb > 0
GROUP BY hour_of_day, day_of_week",

            // Wait ms per second (chart shows this, not total ms per collection)
            MetricNames.WaitMsPerSec => @"
WITH per_collection AS (
    SELECT collection_time,
           SUM(delta_wait_time_ms)::DOUBLE AS total_wait_ms,
           date_diff('second', LAG(collection_time) OVER (ORDER BY collection_time), collection_time) AS interval_sec
    FROM v_wait_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   delta_wait_time_ms >= 0
    GROUP BY collection_time
),
with_rate AS (
    SELECT collection_time,
           CASE WHEN interval_sec > 0 THEN total_wait_ms / interval_sec ELSE 0 END AS ms_per_sec
    FROM per_collection
    WHERE interval_sec IS NOT NULL
    QUALIFY NOT (ms_per_sec = 0
        AND COALESCE(LAG(ms_per_sec) OVER (ORDER BY collection_time), 0) > 100)
)
SELECT EXTRACT(HOUR FROM collection_time)::INT AS hour_of_day,
       EXTRACT(DOW FROM collection_time)::INT AS day_of_week,
       AVG(ms_per_sec) AS mean_val,
       STDDEV_SAMP(ms_per_sec) AS stddev_val,
       COUNT(*) AS sample_count
FROM with_rate
GROUP BY hour_of_day, day_of_week",

            // Blocking events per minute (chart shows event bars bucketed by minute)
            MetricNames.BlockingPerMinute => @"
WITH per_minute AS (
    SELECT DATE_TRUNC('minute', collection_time) AS minute_bucket,
           COUNT(*)::DOUBLE AS event_count
    FROM v_blocked_process_reports
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    GROUP BY minute_bucket
)
SELECT EXTRACT(HOUR FROM minute_bucket)::INT AS hour_of_day,
       EXTRACT(DOW FROM minute_bucket)::INT AS day_of_week,
       AVG(event_count) AS mean_val,
       STDDEV_SAMP(event_count) AS stddev_val,
       COUNT(*) AS sample_count
FROM per_minute
GROUP BY hour_of_day, day_of_week",

            _ => null
        };
    }

    /// <summary>
    /// Collapses multiple day-of-week buckets for the same hour into a single
    /// hour-only bucket using pooled statistics.
    /// </summary>
    private static BaselineBucket CollapseToHourOnly(List<BaselineBucket> hourBuckets)
    {
        var totalSamples = hourBuckets.Sum(b => b.SampleCount);
        if (totalSamples == 0)
            return BaselineBucket.Empty;

        // Weighted mean across all day-of-week buckets for this hour
        var weightedMean = hourBuckets.Sum(b => b.Mean * b.SampleCount) / totalSamples;

        // Pooled standard deviation
        var pooledVariance = PoolVariance(hourBuckets, weightedMean);

        return new BaselineBucket
        {
            HourOfDay = hourBuckets[0].HourOfDay,
            DayOfWeek = -1, // Indicates hour-only
            Mean = weightedMean,
            StdDev = Math.Sqrt(pooledVariance),
            SampleCount = totalSamples,
            Tier = BaselineTier.HourOnly
        };
    }

    /// <summary>
    /// Collapses all buckets into a single flat baseline (equivalent to old 24h behavior).
    /// </summary>
    private static BaselineBucket CollapseToFlat(List<BaselineBucket> allBuckets)
    {
        var totalSamples = allBuckets.Sum(b => b.SampleCount);
        if (totalSamples == 0)
            return BaselineBucket.Empty;

        var weightedMean = allBuckets.Sum(b => b.Mean * b.SampleCount) / totalSamples;
        var pooledVariance = PoolVariance(allBuckets, weightedMean);

        return new BaselineBucket
        {
            HourOfDay = -1,
            DayOfWeek = -1,
            Mean = weightedMean,
            StdDev = Math.Sqrt(pooledVariance),
            SampleCount = totalSamples,
            Tier = BaselineTier.Flat
        };
    }

    /// <summary>
    /// Computes pooled variance from multiple buckets, accounting for both
    /// within-bucket variance and between-bucket mean differences.
    /// </summary>
    private static double PoolVariance(List<BaselineBucket> buckets, double grandMean)
    {
        var totalSamples = buckets.Sum(b => b.SampleCount);
        if (totalSamples <= 1) return 0;

        double totalSumSq = 0;
        foreach (var b in buckets)
        {
            if (b.SampleCount <= 0) continue;
            // Within-bucket variance contribution
            totalSumSq += (b.StdDev * b.StdDev) * (b.SampleCount - 1);
            // Between-bucket mean difference contribution
            totalSumSq += b.SampleCount * (b.Mean - grandMean) * (b.Mean - grandMean);
        }

        return totalSumSq / (totalSamples - 1);
    }

    private class CachedBaseline
    {
        public DateTime ComputedAt { get; init; }
        public DateTime RealTime { get; init; }
        public Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>? Buckets { get; init; }
    }
}

/// <summary>
/// Represents the computed baseline statistics for a single time bucket.
/// </summary>
public class BaselineBucket
{
    public int HourOfDay { get; init; }
    public int DayOfWeek { get; init; }
    public double Mean { get; init; }
    public double StdDev { get; init; }
    public long SampleCount { get; init; }
    public BaselineTier Tier { get; init; }

    public static BaselineBucket Empty => new()
    {
        HourOfDay = -1, DayOfWeek = -1, Mean = 0, StdDev = 0,
        SampleCount = 0, Tier = BaselineTier.Flat
    };

    /// <summary>
    /// Returns the effective stddev with a proportional minimum floor to prevent
    /// division-by-zero in z-score calculations. When both mean and stddev are 0
    /// (zero activity), returns 0 — callers should skip scoring.
    /// </summary>
    public double EffectiveStdDev
    {
        get
        {
            if (Mean == 0 && StdDev <= 0) return 0; // Zero activity — skip scoring
            return Math.Max(StdDev, Mean * 0.01);
        }
    }
}

public enum BaselineTier
{
    Full,     // hour + day-of-week (168 buckets)
    HourOnly, // hour only (24 buckets)
    Flat      // global mean/stddev
}

/// <summary>Metric name constants used as baseline cache keys.</summary>
public static class MetricNames
{
    public const string Cpu = "cpu";
    public const string BatchRequests = "batch_requests";
    public const string WaitStats = "wait_stats";
    public const string SessionCount = "session_count";
    public const string QueryDuration = "query_duration";
    public const string IoLatency = "io_latency";
    public const string Blocking = "blocking";
    public const string Deadlock = "deadlock";
    public const string Memory = "memory";

    // Chart-unit metrics (for UI bands — units match what the chart displays)
    public const string MemoryBufferPoolMb = "memory_buffer_pool_mb";
    public const string WaitMsPerSec = "wait_ms_per_sec";
    public const string BlockingPerMinute = "blocking_per_minute";
}
