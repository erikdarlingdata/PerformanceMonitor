using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitorDashboard.Helpers;

namespace PerformanceMonitorDashboard.Analysis;

/// <summary>
/// Provides time-bucketed baselines (hour-of-day x day-of-week) computed from
/// 30-day rolling history in SQL Server collect.* tables.
///
/// Port of Lite's BaselineProvider — uses SQL Server instead of DuckDB.
/// No server_id filtering — Dashboard monitors one server per database.
///
/// Each baseline bucket contains mean, stddev, and sample count for a metric
/// at a specific (hour, day-of-week) combination. When a bucket has insufficient
/// samples, the provider collapses to less-specific tiers:
///   Full (hour+dow) -> Hour-only -> Flat (global mean/stddev)
///
/// Baselines are cached in memory with a 1-hour TTL to avoid redundant
/// recomputation during rapid re-analysis.
/// </summary>
public class SqlServerBaselineProvider
{
    private readonly string _connectionString;

    /// <summary>Rolling window for baseline computation.</summary>
    private const int BaselineWindowDays = 30;

    /// <summary>Collapse to hour-only when full bucket has fewer than this many samples.</summary>
    private const int CollapseThreshold = 10;

    /// <summary>Restore to full bucket when sample count reaches this level (hysteresis).</summary>
    private const int RestoreThreshold = 15;

    /// <summary>Cache TTL — baselines are recomputed after this interval.</summary>
    public static TimeSpan CacheTtl { get; set; } = TimeSpan.FromHours(1);

    private readonly ConcurrentDictionary<string, CachedBaseline> _cache = new();

    public SqlServerBaselineProvider(string connectionString)
    {
        _connectionString = connectionString;
    }

    /// <summary>
    /// Gets the baseline for a specific metric and time bucket.
    /// Returns the most specific bucket available, collapsing as needed.
    /// </summary>
    public async Task<BaselineBucket> GetBaselineAsync(string metricName, DateTime analysisTime)
    {
        var hourOfDay = analysisTime.Hour;
        var dayOfWeek = (int)analysisTime.DayOfWeek; // Sunday=0

        var baselines = await GetOrComputeBaselinesAsync(metricName, analysisTime);
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
    /// Gets all baseline buckets for a metric. Used by UI for rendering
    /// expected-range bands across all time slots.
    /// </summary>
    public async Task<Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>?> GetAllBaselinesAsync(
        string metricName, DateTime analysisTime)
    {
        return await GetOrComputeBaselinesAsync(metricName, analysisTime);
    }

    /// <summary>Forces full cache clear — used during testing.</summary>
    public void ClearCache() => _cache.Clear();

    private async Task<Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>?> GetOrComputeBaselinesAsync(
        string metricName, DateTime analysisTime)
    {
        var cacheKey = metricName;
        var roundedHour = new DateTime(analysisTime.Year, analysisTime.Month, analysisTime.Day, analysisTime.Hour, 0, 0);

        if (_cache.TryGetValue(cacheKey, out var cached) &&
            cached.ComputedAt == roundedHour &&
            (DateTime.UtcNow - cached.RealTime) < CacheTtl)
        {
            return cached.Buckets;
        }

        var buckets = await ComputeBaselinesAsync(metricName, analysisTime);

        _cache[cacheKey] = new CachedBaseline
        {
            ComputedAt = roundedHour,
            RealTime = DateTime.UtcNow,
            Buckets = buckets
        };

        return buckets;
    }

    private async Task<Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>?> ComputeBaselinesAsync(
        string metricName, DateTime analysisTime)
    {
        var query = GetBaselineQuery(metricName);
        if (query == null) return null;

        var windowStart = analysisTime.AddDays(-BaselineWindowDays);

        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = query;
            cmd.Parameters.Add(new SqlParameter("@windowStart", windowStart));
            cmd.Parameters.Add(new SqlParameter("@windowEnd", analysisTime));

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
            Logger.Error($"[SqlServerBaselineProvider] Failed to compute baselines for {metricName}: {ex.Message}");
            return null;
        }
    }

    private static string? GetBaselineQuery(string metricName)
    {
        // All queries return: hour_of_day, day_of_week, mean_val, stddev_val, sample_count
        // Day-of-week normalization: (DATEPART(weekday, x) + @@DATEFIRST - 1) % 7 gives Sunday=0
        // Cumulative metrics use CTEs for restart poisoning exclusion — exclude samples where
        // value drops near-zero when the prior sample was significantly higher.
        // SQL Server has no QUALIFY — use ROW_NUMBER() in CTEs instead.
        return metricName switch
        {
            // Point-in-time metric — no restart exclusion needed
            SqlServerMetricNames.Cpu => @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT DATEPART(HOUR, collection_time) AS hour_of_day,
       (DATEPART(WEEKDAY, collection_time) + @@DATEFIRST - 1) % 7 AS day_of_week,
       AVG(CAST(sqlserver_cpu_utilization AS FLOAT)) AS mean_val,
       STDEV(CAST(sqlserver_cpu_utilization AS FLOAT)) AS stddev_val,
       COUNT(*) AS sample_count
FROM collect.cpu_utilization_stats
WHERE collection_time >= @windowStart AND collection_time < @windowEnd
GROUP BY DATEPART(HOUR, collection_time),
         (DATEPART(WEEKDAY, collection_time) + @@DATEFIRST - 1) % 7;",

            // Cumulative counter — restart exclusion via CTE with LAG.
            // server_start_time is inline in collect.perfmon_stats.
            // Exclude samples within 5 min of a detected restart.
            SqlServerMetricNames.BatchRequests => @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH filtered AS (
    SELECT collection_time, cntr_value_delta,
           LAG(cntr_value_delta) OVER (ORDER BY collection_time) AS prev_value
    FROM collect.perfmon_stats
    WHERE collection_time >= @windowStart AND collection_time < @windowEnd
    AND   counter_name = 'Batch Requests/sec'
    AND   cntr_value_delta >= 0
)
SELECT DATEPART(HOUR, collection_time) AS hour_of_day,
       (DATEPART(WEEKDAY, collection_time) + @@DATEFIRST - 1) % 7 AS day_of_week,
       AVG(cntr_value_delta) AS mean_val,
       STDEV(cntr_value_delta) AS stddev_val,
       COUNT(*) AS sample_count
FROM filtered
WHERE NOT (cntr_value_delta = 0 AND ISNULL(prev_value, 0) > 1000)
GROUP BY DATEPART(HOUR, collection_time),
         (DATEPART(WEEKDAY, collection_time) + @@DATEFIRST - 1) % 7;",

            // Cumulative counter, multiple rows per collection (per wait type) —
            // aggregate to total wait ms per collection first, then filter restart poisoning
            SqlServerMetricNames.WaitStats => @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH per_collection AS (
    SELECT collection_time,
           SUM(wait_time_ms_delta) AS total_wait_ms
    FROM collect.wait_stats
    WHERE collection_time >= @windowStart AND collection_time < @windowEnd
    AND   wait_time_ms_delta >= 0
    GROUP BY collection_time
),
with_lag AS (
    SELECT collection_time, total_wait_ms,
           LAG(total_wait_ms) OVER (ORDER BY collection_time) AS prev_value
    FROM per_collection
)
SELECT DATEPART(HOUR, collection_time) AS hour_of_day,
       (DATEPART(WEEKDAY, collection_time) + @@DATEFIRST - 1) % 7 AS day_of_week,
       AVG(CAST(total_wait_ms AS FLOAT)) AS mean_val,
       STDEV(CAST(total_wait_ms AS FLOAT)) AS stddev_val,
       COUNT(*) AS sample_count
FROM with_lag
WHERE NOT (total_wait_ms = 0 AND ISNULL(prev_value, 0) > 10000)
GROUP BY DATEPART(HOUR, collection_time),
         (DATEPART(WEEKDAY, collection_time) + @@DATEFIRST - 1) % 7;",

            // Point-in-time, multiple rows per collection (per program_name) —
            // aggregate to total connections per collection first.
            // collect.session_stats does NOT have server_start_time — not needed.
            SqlServerMetricNames.SessionCount => @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH per_collection AS (
    SELECT collection_time,
           SUM(total_sessions) AS total_connections
    FROM collect.session_stats
    WHERE collection_time >= @windowStart AND collection_time < @windowEnd
    GROUP BY collection_time
)
SELECT DATEPART(HOUR, collection_time) AS hour_of_day,
       (DATEPART(WEEKDAY, collection_time) + @@DATEFIRST - 1) % 7 AS day_of_week,
       AVG(CAST(total_connections AS FLOAT)) AS mean_val,
       STDEV(CAST(total_connections AS FLOAT)) AS stddev_val,
       COUNT(*) AS sample_count
FROM per_collection
GROUP BY DATEPART(HOUR, collection_time),
         (DATEPART(WEEKDAY, collection_time) + @@DATEFIRST - 1) % 7;",

            // Cumulative (plan cache), multiple rows per collection (per query) —
            // use delta columns, aggregate total elapsed per collection, filter restart poisoning.
            // server_start_time is inline in collect.query_stats.
            SqlServerMetricNames.QueryDuration => @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH per_collection AS (
    SELECT collection_time,
           SUM(total_elapsed_time_delta) AS total_elapsed
    FROM collect.query_stats
    WHERE collection_time >= @windowStart AND collection_time < @windowEnd
    AND   execution_count_delta > 0
    AND   total_elapsed_time_delta >= 0
    GROUP BY collection_time
),
with_lag AS (
    SELECT collection_time, total_elapsed,
           LAG(total_elapsed) OVER (ORDER BY collection_time) AS prev_value
    FROM per_collection
)
SELECT DATEPART(HOUR, collection_time) AS hour_of_day,
       (DATEPART(WEEKDAY, collection_time) + @@DATEFIRST - 1) % 7 AS day_of_week,
       AVG(CAST(total_elapsed AS FLOAT)) AS mean_val,
       STDEV(CAST(total_elapsed AS FLOAT)) AS stddev_val,
       COUNT(*) AS sample_count
FROM with_lag
WHERE NOT (total_elapsed = 0 AND ISNULL(prev_value, 0) > 100000)
GROUP BY DATEPART(HOUR, collection_time),
         (DATEPART(WEEKDAY, collection_time) + @@DATEFIRST - 1) % 7;",

            // Point-in-time metric — no restart exclusion needed
            SqlServerMetricNames.IoLatency => @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT DATEPART(HOUR, collection_time) AS hour_of_day,
       (DATEPART(WEEKDAY, collection_time) + @@DATEFIRST - 1) % 7 AS day_of_week,
       AVG(io_stall_read_ms_delta * 1.0 / NULLIF(num_of_reads_delta, 0)) AS mean_val,
       STDEV(io_stall_read_ms_delta * 1.0 / NULLIF(num_of_reads_delta, 0)) AS stddev_val,
       COUNT(*) AS sample_count
FROM collect.file_io_stats
WHERE collection_time >= @windowStart AND collection_time < @windowEnd
AND   (num_of_reads_delta > 0 OR num_of_writes_delta > 0)
GROUP BY DATEPART(HOUR, collection_time),
         (DATEPART(WEEKDAY, collection_time) + @@DATEFIRST - 1) % 7;",

            // Event-based — use wait_stats collection intervals as time spine (bucketed to minute),
            // LEFT JOIN event counts so intervals with zero events are included in the baseline.
            // Without this, the baseline only reflects storm periods (when events exist).
            SqlServerMetricNames.Blocking => @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH spine AS (
    SELECT DISTINCT DATEADD(MINUTE, DATEDIFF(MINUTE, 0, collection_time), 0) AS minute_bucket
    FROM collect.wait_stats
    WHERE collection_time >= @windowStart AND collection_time < @windowEnd
),
event_counts AS (
    SELECT DATEADD(MINUTE, DATEDIFF(MINUTE, 0, collection_time), 0) AS minute_bucket,
           CAST(COUNT(*) AS FLOAT) AS cnt
    FROM collect.blocking_BlockedProcessReport
    WHERE collection_time >= @windowStart AND collection_time < @windowEnd
    GROUP BY DATEADD(MINUTE, DATEDIFF(MINUTE, 0, collection_time), 0)
),
per_interval AS (
    SELECT s.minute_bucket, ISNULL(e.cnt, 0) AS event_count
    FROM spine s
    LEFT JOIN event_counts e ON s.minute_bucket = e.minute_bucket
)
SELECT DATEPART(HOUR, minute_bucket) AS hour_of_day,
       (DATEPART(WEEKDAY, minute_bucket) + @@DATEFIRST - 1) % 7 AS day_of_week,
       AVG(event_count) AS mean_val,
       STDEV(event_count) AS stddev_val,
       COUNT(*) AS sample_count
FROM per_interval
GROUP BY DATEPART(HOUR, minute_bucket),
         (DATEPART(WEEKDAY, minute_bucket) + @@DATEFIRST - 1) % 7;",

            // Event-based — same spine approach as blocking
            SqlServerMetricNames.Deadlock => @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

;WITH spine AS (
    SELECT DISTINCT DATEADD(MINUTE, DATEDIFF(MINUTE, 0, collection_time), 0) AS minute_bucket
    FROM collect.wait_stats
    WHERE collection_time >= @windowStart AND collection_time < @windowEnd
),
event_counts AS (
    SELECT DATEADD(MINUTE, DATEDIFF(MINUTE, 0, collection_time), 0) AS minute_bucket,
           CAST(COUNT(*) AS FLOAT) AS cnt
    FROM collect.deadlocks
    WHERE collection_time >= @windowStart AND collection_time < @windowEnd
    GROUP BY DATEADD(MINUTE, DATEDIFF(MINUTE, 0, collection_time), 0)
),
per_interval AS (
    SELECT s.minute_bucket, ISNULL(e.cnt, 0) AS event_count
    FROM spine s
    LEFT JOIN event_counts e ON s.minute_bucket = e.minute_bucket
)
SELECT DATEPART(HOUR, minute_bucket) AS hour_of_day,
       (DATEPART(WEEKDAY, minute_bucket) + @@DATEFIRST - 1) % 7 AS day_of_week,
       AVG(event_count) AS mean_val,
       STDEV(event_count) AS stddev_val,
       COUNT(*) AS sample_count
FROM per_interval
GROUP BY DATEPART(HOUR, minute_bucket),
         (DATEPART(WEEKDAY, minute_bucket) + @@DATEFIRST - 1) % 7;",

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

    private sealed class CachedBaseline
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
public static class SqlServerMetricNames
{
    public const string Cpu = "cpu";
    public const string BatchRequests = "batch_requests";
    public const string WaitStats = "wait_stats";
    public const string SessionCount = "session_count";
    public const string QueryDuration = "query_duration";
    public const string IoLatency = "io_latency";
    public const string Blocking = "blocking";
    public const string Deadlock = "deadlock";
}
