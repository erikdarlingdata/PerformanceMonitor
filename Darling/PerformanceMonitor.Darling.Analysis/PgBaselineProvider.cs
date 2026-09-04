/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// Provides time-bucketed baselines (hour-of-day x day-of-week) computed from
/// 30-day rolling history in Darling's Postgres store — Lite's BaselineProvider
/// (Lite/Analysis/BaselineProvider.cs) ported for the analysis slice AN2b, reading
/// the V4 passthrough views so the metric SQL stays Lite-verbatim wherever the
/// dialects agree.
///
/// <para>
/// Each baseline bucket contains mean, stddev, and sample count for a metric
/// at a specific (hour, day-of-week) combination. When a bucket has insufficient
/// samples, the provider collapses to less-specific tiers:
///   Full (hour+dow) -> Hour-only -> Flat (global mean/stddev)
/// Baselines are cached in memory with a 1-hour TTL to avoid redundant
/// recomputation during rapid re-analysis. Collapse math, cache keys, and the
/// public surface are Lite's, line-for-line.
/// </para>
///
/// <para>
/// Postgres discipline (see PgFindingStore): the window bounds are bound
/// naive-UTC Kind-Unspecified parameters ($1 server_id, $2 window start,
/// $3 analysis time) — never a bare <c>now()</c>/<c>CURRENT_TIMESTAMP</c>, which
/// would be timestamptz and compare in the PG server's time zone. Lite's SQL
/// already parameterized every bound, so no "now" replacement was needed here.
/// </para>
///
/// <para>
/// The arc's only genuine dialect work lives in <see cref="GetBaselineQuery"/>:
/// DuckDB's QUALIFY clause (used at four sites for restart-poisoning / rate
/// exclusion) does not exist in Postgres. Each site is rewritten as
/// window-function-in-a-CTE + the identical predicate in an OUTER where — the
/// same idiom the Dashboard twin (SqlServerBaselineProvider) uses for T-SQL —
/// with the original DuckDB form preserved in a comment block and the
/// row-selection equivalence argued site-by-site.
/// </para>
/// </summary>
public class PgBaselineProvider
{
    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger? _logger;

    /// <summary>Cache TTL — baselines are recomputed after this interval.</summary>
    public static TimeSpan CacheTtl { get; set; } = TimeSpan.FromHours(1);

    private readonly ConcurrentDictionary<string, CachedBaseline> _cache = new();

    public PgBaselineProvider(NpgsqlDataSource postgres, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logger = logger;
    }

    /// <summary>
    /// Gets the baseline for a specific metric, server, and time bucket.
    /// Returns the most specific bucket available, collapsing as needed.
    /// </summary>
    public async Task<BaselineBucket> GetBaselineAsync(
        int serverId, string metricName, DateTime analysisTime, CancellationToken cancellationToken = default)
    {
        var hourOfDay = analysisTime.Hour;
        var dayOfWeek = (int)analysisTime.DayOfWeek; // Sunday=0 — matches EXTRACT(DOW) in both engines

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

        return await ComputeBucketsAsync(serverId, metricName, analysisTime, query, cancellationToken);
    }

    /// <summary>
    /// Did this failure mean "the statement ran out of time" rather than "the connection broke"?
    ///
    /// <para>Worth a named predicate because the two are indistinguishable in the message Npgsql produces.
    /// Npgsql enforces its command timeout by CANCELLING the statement, so the server logs
    /// <c>canceling statement due to user request</c> and the client is left holding a torn stream, which it
    /// reports as "Exception while reading from stream". Read literally that says the network failed; what
    /// actually happened is a query outgrowing its deadline on a store that grew. On the dogfood box the two
    /// log lines sat 267 ms apart in different files, and correlating them by hand is not a diagnosis the
    /// next person should have to repeat.</para>
    ///
    /// <para>Structural, not message matching: <c>57014</c> is <c>query_canceled</c>, the server saying it
    /// cancelled; a <see cref="TimeoutException"/> anywhere in the chain is Npgsql's own deadline. Everything
    /// else stays "failed", because labelling a genuine connection fault a timeout is the same defect aimed
    /// the other way.</para>
    /// </summary>
    internal static bool IsCommandTimeout(Exception ex) =>
        ex is PostgresException { SqlState: "57014" }
        || ex is TimeoutException
        || ex.InnerException is TimeoutException;

    private async Task<Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>?> ComputeBucketsAsync(
        int serverId, string metricName, DateTime analysisTime, string query, CancellationToken cancellationToken)
    {
        var absStdDevFloor = BaselineMath.AbsStdDevFloorFor(metricName);
        var windowStart = analysisTime.AddDays(-BaselineMath.BaselineWindowDays);

        /* Timed so the failure path can say how long it got, not just that it failed — see the catch. */
        var elapsed = System.Diagnostics.Stopwatch.StartNew();

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

            using var cmd = new NpgsqlCommand(query, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(serverId);
            /* Window bounds arrive as bound naive-UTC parameters (Kind-Unspecified so Npgsql
               maps them to `timestamp`, matching the naive-UTC columns) — never bare now(). */
            cmd.Parameters.AddWithValue(AsNaive(windowStart));
            cmd.Parameters.AddWithValue(AsNaive(analysisTime));

            var buckets = new Dictionary<(int, int), BaselineBucket>();

            using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            /* #1743: the robust-scaffold metrics return eight columns (…, median_val, mad_val)
               and carry sentinel tier rows; the two event-family metrics (blocking, deadlock)
               keep the six-column classical shape — detected by column count, so their buckets
               read Median=0/Mad=0 and the robust path degrades for them. */
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
                       decide whether it is USED, not what it IS. */
                    Tier = hour < 0 ? BaselineTier.Flat
                         : dow < 0 ? BaselineTier.HourOnly
                         : BaselineTier.Full
                };
            }

            return buckets;
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, cancellationToken))
        {
            /* A command TIMEOUT and a genuine connection fault are the same message here, and that cost real
               diagnosis time on the dogfood box: Npgsql surfaces its own client-side timeout as
               "Exception while reading from stream" — it cancels the statement, the server logs
               `canceling statement due to user request`, and the client only sees the torn stream. Read
               literally, that says "the network broke"; what actually happened is that this query outgrew its
               timeout on a store that had grown. Correlating the two logs by timestamp (267 ms apart) is not
               something the next person should have to redo, so the distinction is reported here.

               Detected structurally rather than by message text: 57014 is the server telling us it cancelled,
               and a TimeoutException anywhere in the chain is Npgsql's own deadline. Anything else keeps the
               old wording, because calling a real connection fault a timeout would be the same defect
               pointing the other way. */
            if (IsCommandTimeout(ex))
            {
                _logger?.LogError(
                    "[PgBaselineProvider] Baseline query for {MetricName} did not finish within its command timeout — gave up after {Seconds:F1}s, so this metric has NO baseline this pass and its anomaly detection is silent (the collected data is unaffected). The store side logs this as 'canceling statement due to user request'. If it repeats, the window this query scans has outgrown the timeout: {Message}",
                    metricName, elapsed.Elapsed.TotalSeconds, ex.Message);
            }
            else
            {
                _logger?.LogError(
                    "[PgBaselineProvider] Failed to compute baselines for {MetricName} after {Seconds:F1}s: {Message}",
                    metricName, elapsed.Elapsed.TotalSeconds, ex.Message);
            }

            return null;
        }
    }

    /// <summary>
    /// #1743: the shared robust-tier scaffold appended to every raw-grain metric's cleaned rowset.
    /// The arm contributes a CTE chain ending in <c>clean(collection_time, v)</c> — its existing
    /// window-bound/restart-exclusion semantics untouched — and this scaffold computes mean, stddev,
    /// median, and MAD EXACTLY at all three tiers via GROUPING SETS: (hh,dw) = Full, (hh) = the
    /// hour-only sentinel row (day_of_week -1), () = the flat sentinel row (-1,-1). Medians cannot
    /// be pooled from per-bucket medians, which is why the tiers are computed in SQL rather than
    /// synthesized in BaselineMath — see SelectBucket's sentinel-key contract. MAD is a second
    /// percentile pass: every row joins each tier it belongs to (its full bucket, its hour, and the
    /// flat tier), then median of |v − tier median| per tier. Sentinel tiers also fix the flat
    /// tier's DistinctDays, which the pooled synthesis could only approximate with a MAX proxy.
    /// </summary>
    /*
       #2820: the tier expansion is written as an explicit UNION ALL feeding an EQUI-join, not as the
       obvious `ON (t.hour_of_day = -1 OR t.hour_of_day = k.hh) AND (t.day_of_week = -1 OR ...)`.
       Both express "every row joins each tier it belongs to" and both return byte-identical rows —
       the OR form was the original, and it is the reason io_latency spent a week timing out on the
       dogfood box. Postgres cannot hash an OR'd non-equi predicate, so it degrades to a join whose
       planner estimate reached cost 596,208,924 for a 193-row result and which spilled to temp;
       measured on use2, one server, 476,431 rows in the 30-day window: 23.7s OR-join vs 4.2s
       expanded, same 193 rows and the same checksum. DuckDB never needed this because it has a
       native mad() aggregate (Lite computes the same answer single-pass, no join at all); this
       scaffold is the Postgres emulation of that, so it has to earn its join shape explicitly.

       Expanding rows before an equi-join is deliberately the cheaper side of the trade: the fanout
       is identical either way (each row belongs to exactly three tiers), so this buys the hash join
       without adding a single row to the percentile sorts.
    */
    internal const string RobustTierScaffold = @"
keyed AS (
    SELECT v,
           EXTRACT(HOUR FROM collection_time)::INT AS hh,
           EXTRACT(DOW FROM collection_time)::INT AS dw,
           collection_time::DATE AS d
    FROM clean
),
tier_stats AS (
    SELECT COALESCE(hh, -1) AS hour_of_day,
           COALESCE(dw, -1) AS day_of_week,
           AVG(v) AS mean_val,
           STDDEV_SAMP(v) AS stddev_val,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY v) AS median_val,
           COUNT(*) AS sample_count,
           COUNT(DISTINCT d) AS distinct_days
    FROM keyed
    GROUP BY GROUPING SETS ((hh, dw), (hh), ())
),
keyed_tiers AS (
    SELECT v, hh AS hour_of_day, dw AS day_of_week FROM keyed
    UNION ALL SELECT v, hh, -1 FROM keyed
    UNION ALL SELECT v, -1, -1 FROM keyed
),
tier_mads AS (
    SELECT t.hour_of_day, t.day_of_week,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY ABS(k.v - t.median_val)) AS mad_val
    FROM keyed_tiers AS k
    JOIN tier_stats AS t
      ON t.hour_of_day = k.hour_of_day
     AND t.day_of_week = k.day_of_week
    GROUP BY t.hour_of_day, t.day_of_week
)
SELECT t.hour_of_day, t.day_of_week, t.mean_val, t.stddev_val, t.sample_count, t.distinct_days,
       t.median_val, m.mad_val
FROM tier_stats AS t
JOIN tier_mads AS m
  ON m.hour_of_day = t.hour_of_day
 AND m.day_of_week = t.day_of_week";

    /// <summary>
    /// The eleven per-metric baseline queries — Lite's, verbatim, except the four QUALIFY
    /// sites rewritten for Postgres (no QUALIFY support). Internal (not private like Lite's)
    /// so Darling.Tests can pin every query's dialect and the rewrites' structure ungated.
    /// <para>#1743: the nine non-event metrics route their cleaned rowsets through
    /// <see cref="RobustTierScaffold"/> and return EIGHT columns (…, median_val, mad_val) — CPU
    /// and I/O latency included, reading their RAW hypertables at Lite's grain (their retired
    /// sum/sumsq rollups could not produce a median; both tables carry their own 30-day
    /// service-side retention, so this does not reopen #1757 — see the arms' notes).
    /// Blocking/deadlock are event-family (events/day, stddev 0) evaluated on the event-ratio
    /// path, deliberately untouched; the reader detects their six-column shape by count.</para>
    /// </summary>
    internal static string? GetBaselineQuery(string metricName)
    {
        // All queries return: hour_of_day, day_of_week, mean_val, stddev_val, sample_count
        // Cumulative metrics (batch requests, wait stats, query duration) use CTEs for
        // restart poisoning exclusion — exclude samples where value drops to near-zero
        // when the prior sample was significantly higher.
        // Multi-row-per-collection metrics (waits, sessions, queries) aggregate per
        // collection_time first, then bucket by hour+dow.
        return metricName switch
        {
            /* #1743 follow-up: CPU reads the RAW hypertable, at Lite's exact per-sample grain, so
               the robust scaffold applies — the old sum/sumsq rollup could reconstruct mean/stddev
               but structurally cannot produce a median. Reading raw here does NOT reopen #1757:
               that finding was 4 days of supply under a 30-day window, and cpu_utilization carries
               its own 30-DAY service-side retention (CollectorScheduleDefaults: 1-minute cadence,
               30-day retention; verified on a production store — no TimescaleDB retention policy
               on the table, service-side purge at 30d, compressed after 1 day). The mean/stddev
               this computes are the SAME per-sample statistics the rollup reconstruction produced.
               The now-unused cpu_utilization_baseline aggregate remains registered for upgrade
               compatibility; retiring it is separate cleanup. */
            MetricNames.Cpu => @"
WITH clean AS (
    SELECT collection_time, sqlserver_cpu_utilization::DOUBLE PRECISION AS v
    FROM cpu_utilization_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
)," + RobustTierScaffold,

            /* QUALIFY rewrite 1 of 4 — cumulative counter, restart exclusion.
               Excludes samples where the delta drops to 0 when the prior sample was > 1000
               (restart signature for cumulative counters). Lite's DuckDB original:

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
                   GROUP BY hour_of_day, day_of_week

               QUALIFY evaluates AFTER window computation: LAG runs over every WHERE-surviving
               row (including rows QUALIFY itself is about to drop), THEN the predicate prunes.
               The rewrite computes the SAME LAG over the SAME WHERE-filtered rowset inside a
               CTE and applies the IDENTICAL predicate in the outer WHERE — window-before-filter
               is preserved, so only the FIRST zero after a >1000 sample is dropped, and a zero
               following another zero keeps LAG = 0 and SURVIVES (genuine idle, not a restart).
               Row selection is exactly the original's. */
            MetricNames.BatchRequests => @"
WITH windowed AS (
    SELECT collection_time, delta_cntr_value,
           COALESCE(LAG(delta_cntr_value) OVER (ORDER BY collection_time), 0) AS prior_delta
    FROM perfmon_baseline
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
),
clean AS (
    SELECT collection_time, delta_cntr_value AS v
    FROM windowed
    WHERE NOT (delta_cntr_value = 0 AND prior_delta > 1000)
)," + RobustTierScaffold,

            /* QUALIFY rewrite 2 of 4 — cumulative counter, multiple rows per collection (per
               wait type): aggregate to total wait ms per collection FIRST, then restart
               exclusion. Lite's DuckDB original applied QUALIFY inside the grouped CTE:

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
                   SELECT ... FROM per_collection GROUP BY hour_of_day, day_of_week

               In DuckDB that QUALIFY's LAG runs over the GROUPED rows (one per collection_time)
               before any exclusion. The rewrite splits grouping (per_collection) from windowing
               (with_lag) so LAG still sees EVERY grouped row — a row the filter drops still
               serves as its successor's LAG value — and the outer WHERE applies the identical
               predicate. Only the first 0-total immediately after a >10000ms collection (the
               restart signature) is excluded; consecutive zeros (genuine idle) survive because
               their LAG is 0, not >10000. Row selection is exactly the original's. */
            MetricNames.WaitStats => @"
WITH per_collection AS (
    SELECT collection_time, total_wait_ms
    FROM wait_stats_baseline
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
),
with_lag AS (
    SELECT collection_time, total_wait_ms,
           COALESCE(LAG(total_wait_ms) OVER (ORDER BY collection_time), 0) AS prior_total_wait_ms
    FROM per_collection
),
clean AS (
    SELECT collection_time, total_wait_ms AS v
    FROM with_lag
    WHERE NOT (total_wait_ms = 0 AND prior_total_wait_ms > 10000)
)," + RobustTierScaffold,

            // Point-in-time, multiple rows per collection (per program_name) —
            // aggregate to total connections per collection first
            MetricNames.SessionCount => @"
WITH clean AS (
    SELECT collection_time, total_connections AS v
    FROM session_stats_baseline
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
)," + RobustTierScaffold,

            /* QUALIFY rewrite 3 of 4 — cumulative (plan cache), multiple rows per collection
               (per query): delta columns aggregated to total elapsed per collection, then
               restart exclusion. Lite's DuckDB original:

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
                   SELECT ... FROM per_collection GROUP BY hour_of_day, day_of_week

               Same shape as rewrite 2: group first, window over ALL grouped rows in a separate
               CTE, apply the identical exclusion predicate in the outer WHERE — a 0-total right
               after a >100000us collection is the restart signature and is dropped; zeros after
               zeros survive. Row selection is exactly the original's. */
            MetricNames.QueryDuration => @"
WITH per_collection AS (
    SELECT collection_time, total_elapsed
    FROM query_stats_baseline
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
),
with_lag AS (
    SELECT collection_time, total_elapsed,
           COALESCE(LAG(total_elapsed) OVER (ORDER BY collection_time), 0) AS prior_total_elapsed
    FROM per_collection
),
clean AS (
    SELECT collection_time, total_elapsed AS v
    FROM with_lag
    WHERE NOT (total_elapsed = 0 AND prior_total_elapsed > 100000)
)," + RobustTierScaffold,

            /* #1743 follow-up: same move as CPU — raw hypertable at Lite's per-file-row grain so
               the robust scaffold applies (file_io_stats also carries its own 30-day service-side
               retention; see the CPU arm's note). The stall/reads ratio keeps its DOUBLE PRECISION
               cast so a spurious large delta can't make STDDEV_SAMP produce a numeric that
               overflows System.Decimal when Npgsql materializes the aggregate. v stays NULLABLE
               (a write-only file row has no read latency): AVG/STDDEV/median/mad all ignore those
               rows while COUNT(*) keeps counting them — exactly the row_count-vs-ratio_count
               distinction the retired rollup documented, preserved at the raw grain. */
            MetricNames.IoLatency => @"
WITH clean AS (
    SELECT collection_time, delta_stall_read_ms::DOUBLE PRECISION / NULLIF(delta_reads, 0) AS v
    FROM file_io_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    AND   (delta_reads > 0 OR delta_writes > 0)
)," + RobustTierScaffold,

            // Event-based — mean = events per day for this bucket, sample_count = distinct days observed.
            // No restart exclusion needed (event counts, not cumulative).
            MetricNames.Blocking => @"
SELECT EXTRACT(HOUR FROM collection_time)::INT AS hour_of_day,
       EXTRACT(DOW FROM collection_time)::INT AS day_of_week,
       SUM(event_count)::DOUBLE PRECISION / GREATEST(COUNT(DISTINCT collection_time::DATE), 1) AS mean_val,
       0::DOUBLE PRECISION AS stddev_val,
       COUNT(DISTINCT collection_time::DATE) AS sample_count,
       COUNT(DISTINCT collection_time::DATE) AS distinct_days
FROM blocked_process_baseline
WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
GROUP BY hour_of_day, day_of_week",

            // Event-based — same approach as blocking
            MetricNames.Deadlock => @"
SELECT EXTRACT(HOUR FROM collection_time)::INT AS hour_of_day,
       EXTRACT(DOW FROM collection_time)::INT AS day_of_week,
       SUM(event_count)::DOUBLE PRECISION / GREATEST(COUNT(DISTINCT collection_time::DATE), 1) AS mean_val,
       0::DOUBLE PRECISION AS stddev_val,
       COUNT(DISTINCT collection_time::DATE) AS sample_count,
       COUNT(DISTINCT collection_time::DATE) AS distinct_days
FROM deadlock_baseline
WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
GROUP BY hour_of_day, day_of_week",

            // Point-in-time metric (memory pressure %) — no restart exclusion needed
            MetricNames.Memory => @"
WITH clean AS (
    SELECT collection_time, memory_pressure_pct AS v
    FROM memory_baseline
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
)," + RobustTierScaffold,

            // ── Chart-unit baselines (for UI bands — units match what the chart displays) ──

            /* QUALIFY rewrite 4 of 4 — wait ms per second (chart unit). Lite's DuckDB original:

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
                   )
                   SELECT ... FROM with_rate GROUP BY hour_of_day, day_of_week

               Two things to preserve exactly:
               (a) per_collection's LAG(collection_time) alongside GROUP BY collection_time is a
                   window over the GROUPED rows — standard SQL both engines share; it carries
                   over verbatim (no QUALIFY there).
               (b) In DuckDB, with_rate's WHERE runs BEFORE its QUALIFY window: the window's
                   first row (interval_sec NULL — no prior collection) is removed FIRST, so the
                   QUALIFY LAG is computed over only the rated rows. The rewrite keeps that
                   WHERE in with_rate, then windows in a LATER CTE (with_lag) over exactly the
                   post-WHERE rowset, then applies the identical predicate in the outer WHERE.
                   As in rewrites 1-3, LAG sees rows the filter drops, so only the first 0-rate
                   after a >100 ms/sec sample (restart signature) is excluded and idle zeros
                   after zeros survive. Row selection is exactly the original's. */
            MetricNames.WaitMsPerSec => @"
WITH per_collection AS (
    SELECT collection_time,
           total_wait_ms::DOUBLE PRECISION AS total_wait_ms,
           extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_sec
    FROM wait_stats_baseline
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
),
with_rate AS (
    SELECT collection_time,
           CASE WHEN interval_sec > 0 THEN total_wait_ms / interval_sec ELSE 0 END AS ms_per_sec
    FROM per_collection
    WHERE interval_sec IS NOT NULL
),
with_lag AS (
    SELECT collection_time, ms_per_sec,
           COALESCE(LAG(ms_per_sec) OVER (ORDER BY collection_time), 0) AS prior_ms_per_sec
    FROM with_rate
),
clean AS (
    SELECT collection_time, ms_per_sec AS v
    FROM with_lag
    WHERE NOT (ms_per_sec = 0 AND prior_ms_per_sec > 100)
)," + RobustTierScaffold,

            // Blocking events per minute (chart shows event bars bucketed by minute)
            MetricNames.BlockingPerMinute => @"
WITH per_minute AS (
    SELECT DATE_TRUNC('minute', collection_time) AS minute_bucket,
           SUM(event_count)::DOUBLE PRECISION AS event_count
    FROM blocked_process_baseline
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

    /// <summary>Kind-Unspecified for reads/writes — Npgsql 6+ rejects Kind-Utc against <c>timestamp</c>.</summary>
    private static DateTime AsNaive(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private class CachedBaseline
    {
        public DateTime ComputedAt { get; init; }
        public DateTime RealTime { get; init; }
        public Dictionary<(int HourOfDay, int DayOfWeek), BaselineBucket>? Buckets { get; init; }
    }
}
