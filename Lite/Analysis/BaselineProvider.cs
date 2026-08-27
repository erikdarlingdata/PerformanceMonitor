using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.Baselines;
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

    /// <summary>Cache TTL — baselines are recomputed after this interval.</summary>
    public static TimeSpan CacheTtl { get; set; } = TimeSpan.FromHours(1);

    private readonly ConcurrentDictionary<string, CachedBaseline> _cache = new();

    /// <summary>
    /// Resolves a collector's configured retention in days, or null when it cannot be determined (#1757).
    /// A SEAM rather than a ScheduleManager dependency: this provider is constructed in two places and has
    /// never needed the schedule, so taking the whole manager to read one integer would couple the analysis
    /// path to the collection path for no other reason. Null disables the warning entirely, which keeps
    /// every existing caller and every test working unchanged.
    /// </summary>
    private readonly Func<string, int?>? _retentionDaysForCollector;

    /// <summary>Families already warned about this process-run — the warning is a configuration statement,
    /// not an event, so it belongs once per family per start rather than once per baseline computation
    /// (which runs hourly, forever).</summary>
    private readonly ConcurrentDictionary<string, bool> _retentionWarned = new(StringComparer.Ordinal);

    public BaselineProvider(DuckDbInitializer duckDb, Func<string, int?>? retentionDaysForCollector = null)
    {
        _duckDb = duckDb;
        _retentionDaysForCollector = retentionDaysForCollector;
    }

    /// <summary>
    /// The source table each baseline family reads, for the retention check below. Lite reads raw directly
    /// (it has no continuous-aggregate tier), so a family's usable history IS its source table's retention.
    /// </summary>
    private static readonly Dictionary<string, string> SourceCollectorFor = new(StringComparer.Ordinal)
    {
        [MetricNames.Cpu] = "cpu_utilization",
        [MetricNames.BatchRequests] = "perfmon_stats",
        [MetricNames.WaitStats] = "wait_stats",
        [MetricNames.WaitMsPerSec] = "wait_stats",
        [MetricNames.SessionCount] = "session_stats",
        [MetricNames.QueryDuration] = "query_stats",
        [MetricNames.IoLatency] = "file_io_stats",
        [MetricNames.Blocking] = "blocked_process_report",
        [MetricNames.BlockingPerMinute] = "blocked_process_report",
        [MetricNames.Deadlock] = "deadlocks",
        [MetricNames.Memory] = "memory_stats",
    };

    /// <summary>
    /// Warns ONCE per family per process-run when a baseline source table is retained for less than the
    /// baseline window (#1757).
    ///
    /// <para>Darling hit this as a product bug: tiered retention shrank raw to 4 days while the baseline
    /// asks for 30, and the thresholds silently degraded — seven day-of-week buckets cannot be filled from
    /// four days. Lite ships every baseline source at 30 days so it is NOT affected by default, but its
    /// retention is user-editable, so the same silent degradation is one settings change away and nothing
    /// would say so. This is what says so.</para>
    ///
    /// <para>Deliberately a warning and not a correction: shortening retention is a legitimate choice for
    /// disk reasons, and the user is entitled to make it — they are just not entitled to be surprised by
    /// what it does to anomaly detection.</para>
    /// </summary>
    private void WarnIfRetentionUndercutsBaselineWindow(string metricName)
    {
        if (_retentionDaysForCollector is null || !SourceCollectorFor.TryGetValue(metricName, out var collector))
        {
            return;
        }

        var retentionDays = _retentionDaysForCollector(collector);
        if (retentionDays is not int days || days >= BaselineMath.BaselineWindowDays)
        {
            return;
        }

        if (!_retentionWarned.TryAdd(metricName, true))
        {
            return;
        }

        AppLogger.Warn(
            "Baselines",
            $"{collector} is retained for {days} days but the {metricName} baseline window is " +
            $"{BaselineMath.BaselineWindowDays} days — baselines for {metricName} are computed from " +
            $"{days} days of history, so anomaly thresholds are degraded. Raise {collector}'s retention to " +
            $"at least {BaselineMath.BaselineWindowDays} days, or expect less reliable detection.");
    }

    /// <summary>
    /// Gets the baseline for a specific metric, server, and time bucket.
    /// Returns the most specific bucket available, collapsing as needed.
    /// </summary>
    public async Task<BaselineBucket> GetBaselineAsync(
        int serverId, string metricName, DateTime analysisTime, CancellationToken cancellationToken = default)
    {
        WarnIfRetentionUndercutsBaselineWindow(metricName);

        var hourOfDay = analysisTime.Hour;
        var dayOfWeek = (int)analysisTime.DayOfWeek; // Sunday=0

        var baselines = await GetOrComputeBaselinesAsync(serverId, metricName, analysisTime, cancellationToken);
        if (baselines == null || baselines.Count == 0)
            return BaselineBucket.Empty;

        return BaselineMath.SelectBucket(baselines, hourOfDay, dayOfWeek);
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
        int serverId, string metricName, DateTime analysisTime, CancellationToken cancellationToken)
    {
        var cacheKey = $"{serverId}:{metricName}";
        var roundedHour = new DateTime(analysisTime.Year, analysisTime.Month, analysisTime.Day, analysisTime.Hour, 0, 0);

        if (_cache.TryGetValue(cacheKey, out var cached) &&
            cached.ComputedAt == roundedHour &&
            (DateTime.UtcNow - cached.RealTime) < CacheTtl)
        {
            return cached.Buckets;
        }

        var buckets = await ComputeBaselinesAsync(serverId, metricName, analysisTime, cancellationToken);

        _cache[cacheKey] = new CachedBaseline
        {
            ComputedAt = roundedHour,
            RealTime = DateTime.UtcNow,
            Buckets = buckets
        };

        return buckets;
    }

    private async Task<Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>?> ComputeBaselinesAsync(
        int serverId, string metricName, DateTime analysisTime, CancellationToken cancellationToken)
    {
        var query = GetBaselineQuery(metricName);
        if (query == null) return null;

        var absStdDevFloor = BaselineMath.AbsStdDevFloorFor(metricName);
        var windowStart = analysisTime.AddDays(-BaselineMath.BaselineWindowDays);

        try
        {
            using var readLock = _duckDb.AcquireReadLock(cancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = query;
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = windowStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = analysisTime });

            var buckets = new Dictionary<(int, int), BaselineBucket>();

            using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            /* #1743: the robust-scaffold metrics return eight columns (…, median_val, mad_val)
               and carry sentinel tier rows — every Lite arm except the two event-family ones,
               which keep the six-column classical shape (detected by column count) and whose
               buckets read Median=0/Mad=0, degrading the robust path to the classical one. */
            var hasRobustColumns = reader.FieldCount >= 8;
            while (await reader.ReadAsync(cancellationToken))
            {
                var hour = Convert.ToInt32(reader.GetValue(0));
                var dow = Convert.ToInt32(reader.GetValue(1));
                var mean = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2));
                var stddev = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3));
                var count = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4));
                var distinctDays = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5));
                var median = hasRobustColumns && !reader.IsDBNull(6) ? Convert.ToDouble(reader.GetValue(6)) : 0.0;
                var mad = hasRobustColumns && !reader.IsDBNull(7) ? Convert.ToDouble(reader.GetValue(7)) : 0.0;

                buckets[(hour, dow)] = new BaselineBucket
                {
                    HourOfDay = hour,
                    DayOfWeek = dow,
                    Mean = mean,
                    StdDev = stddev,
                    SampleCount = count,
                    DistinctDays = distinctDays,
                    AbsStdDevFloor = absStdDevFloor,
                    Median = median,
                    Mad = mad,
                    /* Sentinel rows from the GROUPING SETS scaffold carry their tier in their key:
                       (-1,-1) = the exact flat tier, (hour,-1) = the exact hour-only tier. A real
                       (hour, dow) bucket is Full even when sparse — SelectBucket's thresholds
                       decide whether it is USED, not what it IS. (Was a copy-paste of two identical
                       Full branches that mislabeled sparse buckets HourOnly in baseline_tier.) */
                    Tier = hour < 0 ? BaselineTier.Flat
                         : dow < 0 ? BaselineTier.HourOnly
                         : BaselineTier.Full
                };
            }

            return buckets;
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, cancellationToken))
        {
            /* #2443: a baseline that could not be computed leaves its metric with no anomaly
               detection, which is worth an ERROR. An abandoned one is not that — it is the pass we
               called off, and five of the seven lines #2299 was filed about came from exactly this
               catch on the Darling twin. */
            AppLogger.Error("BaselineProvider", $"Failed to compute baselines for {metricName}: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// #1743: the shared robust-tier scaffold appended to every metric's cleaned rowset. The arm
    /// contributes a CTE chain ending in <c>clean(collection_time, v)</c> — its existing
    /// window-bound/QUALIFY restart-exclusion semantics untouched — and this scaffold computes
    /// mean, stddev, median, and MAD EXACTLY at all three tiers via GROUPING SETS: (hh,dw) = Full,
    /// (hh) = the hour-only sentinel row (day_of_week -1), () = the flat sentinel row (-1,-1).
    /// Medians cannot be pooled from per-bucket medians, which is why the tiers are computed in
    /// SQL — see BaselineMath.SelectBucket's sentinel-key contract. DuckDB's native median()/mad()
    /// make this single-pass (mad() is the RAW median absolute deviation, matching
    /// BaselineBucket.Mad's unscaled contract — pinned by test against a hand-computed set);
    /// Darling's Postgres twin spells the same tiers with percentile_cont and a second pass.
    /// </summary>
    private const string RobustTierScaffold = @"
keyed AS (
    SELECT v,
           EXTRACT(HOUR FROM collection_time)::INT AS hh,
           EXTRACT(DOW FROM collection_time)::INT AS dw,
           collection_time::DATE AS d
    FROM clean
)
SELECT COALESCE(hh, -1) AS hour_of_day,
       COALESCE(dw, -1) AS day_of_week,
       AVG(v) AS mean_val,
       STDDEV_SAMP(v) AS stddev_val,
       COUNT(*) AS sample_count,
       COUNT(DISTINCT d) AS distinct_days,
       median(v) AS median_val,
       mad(v) AS mad_val
FROM keyed
GROUP BY GROUPING SETS ((hh, dw), (hh), ())";

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
WITH clean AS (
    SELECT collection_time, sqlserver_cpu_utilization AS v
    FROM v_cpu_utilization_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
)," + RobustTierScaffold,

            // Cumulative counter — restart exclusion via subquery with QUALIFY.
            // Excludes samples where delta drops to 0 when prior sample was > 1000
            // (restart signature for cumulative counters).
            MetricNames.BatchRequests => @"
WITH clean AS (
    SELECT collection_time, delta_cntr_value AS v
    FROM v_perfmon_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   counter_name = 'Batch Requests/sec'
    AND   delta_cntr_value >= 0
    QUALIFY NOT (delta_cntr_value = 0
        AND COALESCE(LAG(delta_cntr_value) OVER (ORDER BY collection_time), 0) > 1000)
)," + RobustTierScaffold,

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
),
clean AS (
    SELECT collection_time, total_wait_ms AS v
    FROM per_collection
)," + RobustTierScaffold,

            // Point-in-time, multiple rows per collection (per program_name) —
            // aggregate to total connections per collection first
            MetricNames.SessionCount => @"
WITH per_collection AS (
    SELECT collection_time,
           SUM(connection_count) AS total_connections
    FROM v_session_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    GROUP BY collection_time
),
clean AS (
    SELECT collection_time, total_connections AS v
    FROM per_collection
)," + RobustTierScaffold,

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
),
clean AS (
    SELECT collection_time, total_elapsed AS v
    FROM per_collection
)," + RobustTierScaffold,

            // Point-in-time metric — no restart exclusion needed
            /* v stays NULLABLE here (a write-only row has no read latency): AVG/STDDEV always
               ignored those rows and median()/mad() do the same, while COUNT(*) keeps counting
               them — sample_count semantics unchanged from the pre-#1743 shape. */
            MetricNames.IoLatency => @"
WITH clean AS (
    SELECT collection_time, delta_stall_read_ms * 1.0 / NULLIF(delta_reads, 0) AS v
    FROM v_file_io_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   (delta_reads > 0 OR delta_writes > 0)
)," + RobustTierScaffold,

            // Event-based — mean = events per day for this bucket, sample_count = distinct days observed.
            // No restart exclusion needed (event counts, not cumulative).
            MetricNames.Blocking => @"
SELECT EXTRACT(HOUR FROM collection_time)::INT AS hour_of_day,
       EXTRACT(DOW FROM collection_time)::INT AS day_of_week,
       COUNT(*)::DOUBLE PRECISION / GREATEST(COUNT(DISTINCT collection_time::DATE), 1) AS mean_val,
       0::DOUBLE PRECISION AS stddev_val,
       COUNT(DISTINCT collection_time::DATE) AS sample_count,
       COUNT(DISTINCT collection_time::DATE) AS distinct_days
FROM v_blocked_process_reports
WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
GROUP BY hour_of_day, day_of_week",

            // Event-based — same approach as blocking
            MetricNames.Deadlock => @"
SELECT EXTRACT(HOUR FROM collection_time)::INT AS hour_of_day,
       EXTRACT(DOW FROM collection_time)::INT AS day_of_week,
       COUNT(*)::DOUBLE PRECISION / GREATEST(COUNT(DISTINCT collection_time::DATE), 1) AS mean_val,
       0::DOUBLE PRECISION AS stddev_val,
       COUNT(DISTINCT collection_time::DATE) AS sample_count,
       COUNT(DISTINCT collection_time::DATE) AS distinct_days
FROM v_deadlocks
WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
GROUP BY hour_of_day, day_of_week",

            // Point-in-time metric (memory pressure %) — no restart exclusion needed
            MetricNames.Memory => @"
WITH clean AS (
    SELECT collection_time,
           total_server_memory_mb::DOUBLE PRECISION / NULLIF(target_server_memory_mb::DOUBLE PRECISION, 0) * 100 AS v
    FROM v_memory_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   target_server_memory_mb > 0
)," + RobustTierScaffold,

            // ── Chart-unit baselines (for UI bands — units match what the chart displays) ──

            // Wait ms per second (chart shows this, not total ms per collection)
            MetricNames.WaitMsPerSec => @"
WITH per_collection AS (
    SELECT collection_time,
           SUM(delta_wait_time_ms)::DOUBLE PRECISION AS total_wait_ms,
           extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_sec
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
),
clean AS (
    SELECT collection_time, ms_per_sec AS v
    FROM with_rate
)," + RobustTierScaffold,

            // Blocking events per minute (chart shows event bars bucketed by minute)
            MetricNames.BlockingPerMinute => @"
WITH per_minute AS (
    SELECT DATE_TRUNC('minute', collection_time) AS minute_bucket,
           COUNT(*)::DOUBLE PRECISION AS event_count
    FROM v_blocked_process_reports
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    GROUP BY minute_bucket
),
clean AS (
    SELECT minute_bucket AS collection_time, event_count AS v
    FROM per_minute
)," + RobustTierScaffold,

            _ => null
        };
    }

    private class CachedBaseline
    {
        public DateTime ComputedAt { get; init; }
        public DateTime RealTime { get; init; }
        public Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>? Buckets { get; init; }
    }
}
