/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Phase-5 analysis slice AN2b — PgAnomalyDetector + PgBaselineProvider, Lite's
/// anomaly pipeline ported onto the V4 passthrough views. Ungated: the method surfaces match
/// Lite's classes name-for-name (expected-name lists — the twin assemblies aren't referenced);
/// every SQL string is PG dialect (no QUALIFY anywhere, no bare now()/CURRENT_TIMESTAMP, no
/// N'' literals, $N positional parameters); and the four QUALIFY rewrites carry the DuckDB
/// original's row-selection semantics — window function computed in a CTE over the
/// pre-exclusion rowset, the exclusion predicate applied in the OUTER where — pinned
/// structurally per site with the equivalence argument written out. Gated on DARLING_TEST_PG:
/// migrate, plant an hour×dow wait_stats history containing a counter-reset (restart) row plus
/// a genuine-idle zero row, and prove against live Postgres that BOTH rewritten wait baselines
/// (wait_stats and wait_ms_per_sec) exclude exactly the poisoned sample — sample count AND
/// mean — then plant an anomalous current window and watch the ported detector emit ONE
/// ANOMALY_WAIT_PROFILE fact (change 1) via the thin-baseline is_new fallback, with the planted
/// wait type named as a contrib_&lt;TYPE&gt; contributor.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingAnomalyBaselineTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -626262;
    private const string TestServerName = "anomaly-baseline-e2e";
    private const string TestWaitType = "AN2B_TEST_WAIT";

    /// <summary>
    /// Lite's nine detector methods (Lite/Analysis/AnomalyDetector.cs) — the port must carry
    /// every one, same names, plus the HasBaselineDataAsync gate.
    /// </summary>
    private static readonly string[] LiteDetectorMethods =
    {
        "DetectCpuAnomalies",
        "DetectWaitAnomalies",
        "DetectBlockingAnomalies",
        "DetectIoAnomalies",
        "DetectBatchRequestAnomalies",
        "DetectSessionAnomalies",
        "DetectQueryDurationAnomalies",
        "DetectMemoryAnomalies",
        "DetectObjectStatsAnomalies"
    };

    /// <summary>
    /// Lite's eleven baseline metrics (Lite/Analysis/BaselineProvider.cs GetBaselineQuery) —
    /// the port must serve a query for every one.
    /// </summary>
    private static readonly string[] AllMetricNames =
    {
        MetricNames.Cpu,
        MetricNames.BatchRequests,
        MetricNames.WaitStats,
        MetricNames.SessionCount,
        MetricNames.QueryDuration,
        MetricNames.IoLatency,
        MetricNames.Blocking,
        MetricNames.Deadlock,
        MetricNames.Memory,
        MetricNames.WaitMsPerSec,
        MetricNames.BlockingPerMinute
    };

    private static readonly string[] AllDetectorSql =
    {
        PgAnomalyDetector.HasBaselineDataSql,
        PgAnomalyDetector.CpuWindowSql,
        PgAnomalyDetector.WaitRateWindowSql,
        PgAnomalyDetector.WaitContribWindowSql,
        PgAnomalyDetector.BlockingWindowSql,
        PgAnomalyDetector.IoWindowSql,
        PgAnomalyDetector.BatchRequestWindowSql,
        PgAnomalyDetector.SessionWindowSql,
        PgAnomalyDetector.QueryDurationWindowSql,
        PgAnomalyDetector.MemoryWindowSql,
        PgAnomalyDetector.ObjectGrowthSql,
        PgAnomalyDetector.ObjectContentionSql
    };

    private static IEnumerable<string> AllAnalysisSql =>
        AllDetectorSql.Concat(AllMetricNames.Select(m => PgBaselineProvider.GetBaselineQuery(m)!));

    [Fact]
    public void IoLatencyBaseline_CastsRatioToDoublePrecision_NotNumeric()
    {
        /* The stall/reads ratio must be DOUBLE PRECISION, not numeric (`* 1.0`): STDDEV_SAMP of a
           spurious-large ratio yields a numeric that overflows System.Decimal when Npgsql materializes
           the aggregate, silently failing the io_latency baseline (found live via the error monitor).
           #1757 first pinned the cast inside the file_io_baseline aggregate; #2007 retired that
           aggregate outright (nothing read it after the raw move), so the raw-arm pins below are
           the surviving — and only — guarantee. */

        /* #1743 follow-up moved the arm off the rollup and onto the raw hypertable (the rollup
           cannot produce a median) — the cast pin moves WITH it: the ratio must still be computed
           in float arithmetic at the source. The old SUM(row_count) pin's SEMANTIC survives as the
           nullable-v design: the arm's WHERE keeps write-only rows (delta_reads > 0 OR
           delta_writes > 0) and must NOT filter the NULL ratios out — the scaffold's COUNT(*)
           counts them (the row_count behavior) while AVG/STDDEV/median/mad ignore them (the
           ratio_count behavior), exactly the retired rollup's two-count distinction. */
        var sql = PgBaselineProvider.GetBaselineQuery(MetricNames.IoLatency)!;
        Assert.Contains("FROM file_io_stats", sql, StringComparison.Ordinal);
        Assert.Contains("delta_stall_read_ms::DOUBLE PRECISION / NULLIF(delta_reads, 0)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("delta_stall_read_ms * 1.0", sql, StringComparison.Ordinal);
        Assert.Contains("(delta_reads > 0 OR delta_writes > 0)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v IS NOT NULL", sql, StringComparison.Ordinal);
    }

    /* ---------------- ungated: method-surface pins vs Lite ---------------- */

    [Fact]
    public void AnomalyDetector_CarriesLitesMethodSurface_NineDetectorsPlusBaselineGate()
    {
        var privateMethods = typeof(PgAnomalyDetector)
            .GetMethods(BindingFlags.Instance | BindingFlags.NonPublic)
            .Select(m => m.Name)
            .ToHashSet(StringComparer.Ordinal);

        foreach (var detector in LiteDetectorMethods)
        {
            Assert.Contains(detector, privateMethods);
        }
        Assert.Contains("HasBaselineDataAsync", privateMethods);

        /* Lite's public surface: the orchestration entry point + the threshold override. */
        Assert.NotNull(typeof(PgAnomalyDetector).GetMethod("DetectAnomaliesAsync"));
        Assert.NotNull(typeof(PgAnomalyDetector).GetMethod("SetDeviationThreshold"));
    }

    [Fact]
    public void BaselineProvider_CarriesLitesSurface_AndAllElevenMetricQueries()
    {
        /* Lite's public surface: bucket lookup with tier collapse + the test cache hooks. */
        Assert.NotNull(typeof(PgBaselineProvider).GetMethod("GetBaselineAsync"));
        Assert.NotNull(typeof(PgBaselineProvider).GetMethod("InvalidateCache"));
        Assert.NotNull(typeof(PgBaselineProvider).GetMethod("ClearCache"));
        Assert.NotNull(typeof(PgBaselineProvider).GetProperty("CacheTtl"));

        /* All eleven of Lite's metrics are served; an unknown metric is null (Lite's contract). */
        foreach (var metric in AllMetricNames)
        {
            Assert.NotNull(PgBaselineProvider.GetBaselineQuery(metric));
        }
        Assert.Null(PgBaselineProvider.GetBaselineQuery("no_such_metric"));
    }

    [Fact]
    public void BaselineBucket_EffectiveStdDev_KeepsLitesFloorSemantics()
    {
        /* Zero activity (mean 0, stddev 0) → 0, callers skip scoring. */
        Assert.Equal(0.0, BaselineBucket.Empty.EffectiveStdDev);

        /* Flat-but-nonzero baseline → the proportional 1% floor prevents divide-by-zero. */
        var flatBusy = new BaselineBucket { Mean = 200, StdDev = 0 };
        Assert.Equal(2.0, flatBusy.EffectiveStdDev);

        /* Real spread wins over the floor. */
        var spread = new BaselineBucket { Mean = 200, StdDev = 50 };
        Assert.Equal(50.0, spread.EffectiveStdDev);
    }

    /* ---------------- ungated: SQL hygiene across the whole slice ---------------- */

    [Fact]
    public void AllAnalysisSql_PgDialect_NoQualify_NoBareNow_PositionalParams()
    {
        foreach (var sql in AllAnalysisSql)
        {
            var lower = sql.ToLowerInvariant();

            /* Postgres has no QUALIFY — every DuckDB QUALIFY must have been rewritten. */
            Assert.DoesNotContain("qualify", lower);

            /* Bare now()/CURRENT_TIMESTAMP is timestamptz — the naive-UTC columns would
               compare in the server's time zone. Every "now" must be a bound parameter. */
            Assert.DoesNotContain("now(", lower);
            Assert.DoesNotContain("current_timestamp", lower);

            /* Postgres has no N'' literals and no @named parameters — $N positional only. */
            Assert.DoesNotContain("N'", sql);
            Assert.DoesNotContain("@", sql);
            Assert.Contains("$1", sql);
        }
    }

    /* ---------------- ungated: the four QUALIFY rewrites, semantics pinned ----------------

       DuckDB evaluates QUALIFY AFTER window functions: LAG runs over every row that survived
       WHERE / GROUP BY (including rows the QUALIFY predicate itself is about to drop), and
       only then does the predicate prune. Postgres has no QUALIFY, so each site is rewritten
       as window-function-in-a-CTE + the identical predicate in an OUTER where. That shape is
       equivalent BECAUSE the window is still computed over the full pre-exclusion rowset —
       the outer WHERE cannot change what LAG saw. The failure modes a wrong rewrite invites:
       (a) putting the predicate inside the windowed CTE's WHERE (filters BEFORE the window —
           LAG would skip excluded rows, chaining exclusions through consecutive zeros), or
       (b) recomputing LAG after a first exclusion pass — same wrong chaining.
       Each pin below asserts the window function appears inside a CTE/subselect and the
       exclusion predicate appears only AFTER that CTE is selected FROM — the structural
       guarantee of window-before-filter. The live gated test then proves the row selection
       on real data (poisoned row out, genuine idle zero kept).                                */

    [Fact]
    public void BatchRequestsRewrite_LagInCte_ExclusionInOuterWhere_OnlyFirstZeroAfterHighPriorDrops()
    {
        /* Original (DuckDB): subselect WHERE server/window/counter/delta>=0 with
           QUALIFY NOT (delta_cntr_value = 0 AND COALESCE(LAG(delta_cntr_value) OVER
           (ORDER BY collection_time), 0) > 1000), aggregated by hour+dow outside.
           Rewrite: the SAME LAG over the SAME WHERE-filtered rowset inside the `windowed`
           CTE; the SAME predicate on the outer SELECT that aggregates FROM windowed. A zero
           sample right after a >1000 sample (restart signature) is dropped; a zero after a
           zero has prior_delta = 0 and SURVIVES (genuine idle). */
        var sql = PgBaselineProvider.GetBaselineQuery(MetricNames.BatchRequests)!;

        Assert.DoesNotContain("QUALIFY", sql, StringComparison.OrdinalIgnoreCase);

        var lagAt = sql.IndexOf("COALESCE(LAG(delta_cntr_value) OVER (ORDER BY collection_time), 0) AS prior_delta", StringComparison.Ordinal);
        var fromCteAt = sql.IndexOf("FROM windowed", StringComparison.Ordinal);
        var exclusionAt = sql.IndexOf("WHERE NOT (delta_cntr_value = 0 AND prior_delta > 1000)", StringComparison.Ordinal);

        Assert.True(lagAt >= 0, "the restart LAG must be computed in the windowed CTE");
        Assert.True(fromCteAt > lagAt, "the aggregate must select FROM the windowed CTE");
        Assert.True(exclusionAt > fromCteAt, "the exclusion must filter OUTSIDE the CTE — after the window is computed");

        /* The pre-window row filter is unchanged from Lite. */
        Assert.Contains("counter_name = 'Batch Requests/sec'", TimescaleSupport.CreatePerfmonBaselineSql, StringComparison.Ordinal);
        Assert.Contains("delta_cntr_value >= 0", TimescaleSupport.CreatePerfmonBaselineSql, StringComparison.Ordinal);

        /* #3527: v is the PER-SECOND rate. The perfmon_baseline supply materializes only
           (collection_time, delta_cntr_value) — no stored interval, and a continuous aggregate cannot
           grow a column without forfeiting the history the 4-day raw tier can't refill — so the divisor
           is derived from LAG(collection_time) over the collapsed series (the WaitMsPerSec idiom),
           computed in the SAME windowed CTE (window-before-filter holds for it too), with the
           interval-less first row filtered alongside the restart exclusion. The DOUBLE PRECISION cast
           is the io-arm rule: STDDEV_SAMP over numeric can overflow System.Decimal. */
        var intervalAt = sql.IndexOf("extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_sec", StringComparison.Ordinal);
        Assert.True(intervalAt >= 0 && intervalAt < fromCteAt, "interval_sec must be derived inside the windowed CTE");
        Assert.Contains("delta_cntr_value::DOUBLE PRECISION / interval_sec AS v", sql, StringComparison.Ordinal);
        var intervalFilterAt = sql.IndexOf("interval_sec > 0", StringComparison.Ordinal);
        Assert.True(intervalFilterAt > fromCteAt, "the interval filter must sit OUTSIDE the windowed CTE, with the exclusion");
    }

    /// <summary>
    /// #3527: the batch-request WINDOW statistic must be requests/sec — the per-interval
    /// delta_cntr_value divided by the row's measured sample_interval_seconds — or the
    /// BatchRequestFloor/Fallback thresholds (defined in requests/sec) admit 60-300x-inflated
    /// deltas and the comparison against the per-second baseline is cross-unit. Interval &lt;= 0
    /// rows carry NO knowable delta and must be filtered, never read as a rate of 0 or as the
    /// raw delta.
    /// </summary>
    [Fact]
    public void BatchRequestWindow_DividesByMeasuredInterval_AndSkipsUnknowableRows()
    {
        var sql = PgAnomalyDetector.BatchRequestWindowSql;

        Assert.Contains("AVG(delta_cntr_value * 1.0 / NULLIF(sample_interval_seconds, 0))", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(delta_cntr_value * 1.0 / NULLIF(sample_interval_seconds, 0))", sql, StringComparison.Ordinal);
        Assert.Contains("sample_interval_seconds > 0", sql, StringComparison.Ordinal);

        /* A raw AVG/MAX of the delta is exactly the #3527 defect — pin its absence. */
        Assert.DoesNotContain("AVG(delta_cntr_value)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("MAX(delta_cntr_value)", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3527 proven live, both halves in one place: the BatchRequests BASELINE arm derives its
    /// per-second unit from LAG(collection_time) over the perfmon_baseline supply, and the DETECTOR's
    /// window read divides by the stored measured interval — so the two sides meet in the same
    /// requests/sec unit and the absolute bars judge honest rates.
    ///
    /// <para>History (one Monday-10:00 bucket, 12 collections at 300s spacing, delta 30000 each —
    /// 100 req/sec): c1 has no prior (interval NULL → dropped), c6 is a restart zero (prior 30000 &gt;
    /// 1000 → excluded), c7 is a genuine idle zero (prior 0 → kept at 0/sec). 10 samples, mean
    /// (9x100 + 0)/10 = 90 — in requests/sec, where the raw-delta unit would read 27000.</para>
    ///
    /// <para>Window (the following Monday): three rows at stored interval 60, delta 600000 — 10000
    /// req/sec — plus one interval-0 row with a wild delta that must be SKIPPED, not rated. The one
    /// planted history day leaves the bucket untrustworthy (Full tier needs 3 distinct days), so the
    /// detector fires on the absolute BatchRequestFallback bar (5000 req/sec): peak 10000 clears it
    /// honestly. Pre-#3527 the raw deltas cleared every bar by orders of magnitude regardless of
    /// workload; post-fix the emitted Value, peak/avg metadata, and baseline_mean are all per-second.</para>
    /// </summary>
    [Fact]
    public async Task EndToEnd_BatchRequestArm_PerSecondBaselineAndWindow_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live batch-request test.");

        var ct = TestContext.Current.CancellationToken;
        const int batchServerId = TestServerId + 2; // own id — this test cleans its own rows
        const string batchServerName = "batch-per-second-e2e";

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using (var cleanup = new NpgsqlCommand(
            $"DELETE FROM perfmon_stats WHERE server_id = {batchServerId}; " +
            $"DELETE FROM wait_stats WHERE server_id = {batchServerId};", connection))
        {
            await cleanup.ExecuteNonQueryAsync(ct);
        }

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var day = DateTime.UtcNow.Date.AddDays(-8);
            while (day.DayOfWeek != DayOfWeek.Monday) day = day.AddDays(-1);
            var historyStart = DateTime.SpecifyKind(day.AddHours(10), DateTimeKind.Unspecified);

            for (var i = 0; i < 12; i++)
            {
                var delta = (i == 5 || i == 6) ? 0L : 30000L;
                await InsertAsync(connection,
                    "INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                    (long)(200 + i), historyStart.AddMinutes(5 * i), batchServerId, batchServerName,
                    "SQLServer:SQL Statistics", "Batch Requests/sec", "", delta * 2, delta, 300);
            }

            /* The baseline supply must exist (see the wait test's note) — plain fallback views. */
            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);

            var provider = new PgBaselineProvider(postgres);
            var analysisTime = historyStart.AddDays(7);

            var baseline = await provider.GetBaselineAsync(batchServerId, MetricNames.BatchRequests, analysisTime);
            Assert.Equal(10L, baseline.SampleCount);
            Assert.Equal(90.0, baseline.Mean, 0.001);
            Assert.Equal(BaselineTier.Full, baseline.Tier);
            Assert.Equal(10, baseline.HourOfDay);
            Assert.Equal((int)DayOfWeek.Monday, baseline.DayOfWeek);

            /* Canary for the HasBaselineData gate — OUTSIDE the analysis window so the wait
               detector's own window read stays empty and it emits nothing. */
            await InsertAsync(connection,
                "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                300L, historyStart, batchServerId, batchServerName, TestWaitType, 1L, 100L);

            /* The anomalous current window: 10000 req/sec (delta 600000 over a measured 60s),
               plus one interval-0 row whose wild delta must never be rated. */
            for (var i = 0; i < 3; i++)
            {
                await InsertAsync(connection,
                    "INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                    (long)(400 + i), analysisTime.AddMinutes(5 * (i + 1)), batchServerId, batchServerName,
                    "SQLServer:SQL Statistics", "Batch Requests/sec", "", 1200000L, 600000L, 60);
            }
            await InsertAsync(connection,
                "INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                403L, analysisTime.AddMinutes(20), batchServerId, batchServerName,
                "SQLServer:SQL Statistics", "Batch Requests/sec", "", 0L, 999999999L, 0);

            var detector = new PgAnomalyDetector(postgres, provider);
            var context = new AnalysisContext
            {
                ServerId = batchServerId,
                ServerName = batchServerName,
                TimeRangeStart = analysisTime,
                TimeRangeEnd = analysisTime.AddMinutes(30),
                ServerUtcOffset = TimeSpan.Zero
            };

            var anomalies = await detector.DetectAnomaliesAsync(context);

            var fact = Assert.Single(anomalies);
            Assert.Equal("ANOMALY_BATCH_REQUESTS", fact.Key);
            Assert.Equal(10000.0, fact.Value, 0.001);                              // per-second, not 600000
            Assert.Equal(10000.0, fact.Metadata["peak_batch_requests"], 0.001);
            Assert.Equal(10000.0, fact.Metadata["avg_batch_requests"], 0.001);
            Assert.Equal(3.0, fact.Metadata["window_samples"]);                    // the interval-0 row is NOT a sample
            Assert.Equal(90.0, fact.Metadata["baseline_mean"], 0.001);             // same unit as the window
            Assert.Equal(1.0, fact.Metadata["baseline_low_quality"]);              // one distinct day → absolute bar
            Assert.Equal(2.0, fact.Metadata["fallback_exceedance"], 0.001);        // 10000 / the 5000 req/sec bar

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                using (var command = new NpgsqlCommand(
                    $"DELETE FROM perfmon_stats WHERE server_id = {batchServerId}; " +
                    $"DELETE FROM wait_stats WHERE server_id = {batchServerId};", cleanup))
                {
                    await command.ExecuteNonQueryAsync(cleanupCt);
                }
                await DropBaselineFallbackViewsAsync(cleanup, cleanupCt);
            });
        }
    }

    [Fact]
    public void WaitStatsRewrite_GroupThenLagInCte_ExclusionInOuterWhere_RestartTotalDropsIdleZeroSurvives()
    {
        /* Original (DuckDB): per_collection groups SUM(delta_wait_time_ms) per collection_time
           and applies QUALIFY NOT (total_wait_ms = 0 AND COALESCE(LAG(total_wait_ms) OVER
           (ORDER BY collection_time), 0) > 10000) directly on the grouped CTE — the window
           runs over the GROUPED rows, pre-exclusion. Rewrite: grouping (per_collection) and
           windowing (with_lag) split into successive CTEs so LAG still sees every grouped row
           — a dropped row still serves as its successor's LAG value — and the identical
           predicate moves to the outer WHERE. Only the first 0-total right after a >10000ms
           collection is excluded; consecutive zeros survive (their prior is 0).

           #1757 moved the per-collection collapse into the baseline aggregate, so the grouping no longer
           appears in this query -- the totals arrive already one row per collection_time. The INVARIANT is
           unchanged and is what is pinned: LAG runs over the per-collection series, and the exclusion is
           applied OUTSIDE the windowed CTE so a dropped row still serves as its successor's LAG value. */
        var sql = PgBaselineProvider.GetBaselineQuery(MetricNames.WaitStats)!;

        Assert.DoesNotContain("QUALIFY", sql, StringComparison.OrdinalIgnoreCase);

        var supplyAt = sql.IndexOf("FROM wait_stats_baseline", StringComparison.Ordinal);
        var lagAt = sql.IndexOf("COALESCE(LAG(total_wait_ms) OVER (ORDER BY collection_time), 0) AS prior_total_wait_ms", StringComparison.Ordinal);
        var fromCteAt = sql.IndexOf("FROM with_lag", StringComparison.Ordinal);
        var exclusionAt = sql.IndexOf("WHERE NOT (total_wait_ms = 0 AND prior_total_wait_ms > 10000)", StringComparison.Ordinal);

        Assert.True(supplyAt >= 0 && lagAt > supplyAt, "LAG must run over the per-collection totals supplied by the baseline aggregate");
        Assert.True(fromCteAt > lagAt, "the aggregate must select FROM the with_lag CTE");
        Assert.True(exclusionAt > fromCteAt, "the exclusion must filter OUTSIDE the windowed CTE");
    }

    [Fact]
    public void QueryDurationRewrite_GroupThenLagInCte_ExclusionInOuterWhere_SameShapeAsWaitStats()
    {
        /* Original (DuckDB): identical shape to the wait-stats site — per_collection groups
           SUM(delta_elapsed_time) (only rows with delta_execution_count > 0 and non-negative
           elapsed), QUALIFY NOT (total_elapsed = 0 AND COALESCE(LAG(total_elapsed) OVER
           (ORDER BY collection_time), 0) > 100000). Rewrite: same group → window-CTE → outer
           WHERE split; same argument — the plan-cache restart zero right after a >100000us
           collection drops, idle zeros survive. */
        var sql = PgBaselineProvider.GetBaselineQuery(MetricNames.QueryDuration)!;

        Assert.DoesNotContain("QUALIFY", sql, StringComparison.OrdinalIgnoreCase);

        var groupAt = sql.IndexOf("FROM query_stats_baseline", StringComparison.Ordinal);
        var lagAt = sql.IndexOf("COALESCE(LAG(total_elapsed) OVER (ORDER BY collection_time), 0) AS prior_total_elapsed", StringComparison.Ordinal);
        var fromCteAt = sql.IndexOf("FROM with_lag", StringComparison.Ordinal);
        var exclusionAt = sql.IndexOf("WHERE NOT (total_elapsed = 0 AND prior_total_elapsed > 100000)", StringComparison.Ordinal);

        Assert.True(groupAt >= 0 && lagAt > groupAt, "LAG must run over the GROUPED per-collection totals");
        Assert.True(fromCteAt > lagAt, "the aggregate must select FROM the with_lag CTE");
        Assert.True(exclusionAt > fromCteAt, "the exclusion must filter OUTSIDE the windowed CTE");

        /* Lite's pre-aggregation row filters are unchanged. */
        Assert.Contains("delta_execution_count > 0", TimescaleSupport.CreateQueryStatsBaselineSql, StringComparison.Ordinal);
        Assert.Contains("delta_elapsed_time >= 0", TimescaleSupport.CreateQueryStatsBaselineSql, StringComparison.Ordinal);
    }

    [Fact]
    public void WaitMsPerSecRewrite_RateFilterBeforeLagCte_ExclusionInOuterWhere_FirstRowDropPreserved()
    {
        /* Original (DuckDB): with_rate selects the per-collection ms/sec rate WHERE
           interval_sec IS NOT NULL (dropping the window's FIRST collection, whose LAG-based
           interval is NULL) and then QUALIFYs NOT (ms_per_sec = 0 AND COALESCE(LAG(ms_per_sec)
           OVER (ORDER BY collection_time), 0) > 100). DuckDB runs that WHERE BEFORE the
           QUALIFY window — the restart LAG is computed over only the rated rows. The rewrite
           must keep BOTH orderings: (1) the IS NOT NULL filter stays in with_rate, and the
           restart LAG moves to a LATER CTE (with_lag) over with_rate's output — window over
           the post-WHERE rowset, exactly DuckDB's; (2) the exclusion predicate applies OUTSIDE
           with_lag, after the window — so a 0-rate right after a >100 ms/sec sample drops and
           an idle zero after a zero survives. The per_collection interval computation
           (LAG(collection_time) over the GROUPED rows) is standard SQL in both engines and
           carries over verbatim — it never had a QUALIFY. */
        var sql = PgBaselineProvider.GetBaselineQuery(MetricNames.WaitMsPerSec)!;

        Assert.DoesNotContain("QUALIFY", sql, StringComparison.OrdinalIgnoreCase);

        /* The Lite-verbatim interval spine survives. */
        Assert.Contains("LAG(collection_time) OVER (ORDER BY collection_time)", sql, StringComparison.Ordinal);

        var rateFilterAt = sql.IndexOf("WHERE interval_sec IS NOT NULL", StringComparison.Ordinal);
        var lagAt = sql.IndexOf("COALESCE(LAG(ms_per_sec) OVER (ORDER BY collection_time), 0) AS prior_ms_per_sec", StringComparison.Ordinal);
        var fromCteAt = sql.IndexOf("FROM with_lag", StringComparison.Ordinal);
        var exclusionAt = sql.IndexOf("WHERE NOT (ms_per_sec = 0 AND prior_ms_per_sec > 100)", StringComparison.Ordinal);

        Assert.True(rateFilterAt >= 0 && lagAt > rateFilterAt, "the IS NOT NULL filter must precede the restart LAG (DuckDB's WHERE-before-QUALIFY order)");
        Assert.True(fromCteAt > lagAt, "the aggregate must select FROM the with_lag CTE");
        Assert.True(exclusionAt > fromCteAt, "the exclusion must filter OUTSIDE the windowed CTE");
    }

    /* ---------------- gated: live restart-exclusion + detector proof ---------------- */

    [Fact]
    public async Task EndToEnd_RestartExclusionBaselines_AndWaitSpikeDetector_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live anomaly/baseline test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* Clear leftovers from an earlier aborted run so the assertions below are deterministic. */
        await DeleteTestRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            /* ---- plant the hour×dow history: one bucket (Monday 10:00 UTC), 12 collections
                    at 5-minute spacing, one wait type, 60000ms of wait per collection — except
                    c6 (index 5): 0ms, the COUNTER-RESET row (restart signature: a zero total
                    right after a busy 60000ms collection), and c7 (index 6): 0ms, a GENUINE
                    IDLE zero (zero after zero — must SURVIVE the exclusion).

                    Expected row selection, hand-checked against the DuckDB originals:
                    - wait_stats baseline (rewrite 2): 12 grouped totals; only c6 drops
                      (prior 60000 > 10000). 11 samples, mean = 10*60000/11 = 54545.45.
                      Wrong rewrites give 12 samples/mean 50000 (no exclusion) or 10 samples/
                      mean 60000 (idle zero wrongly dropped via filtered/recomputed LAG).
                    - wait_ms_per_sec baseline (rewrite 4): the first collection has no prior
                      (interval NULL) and is dropped by IS NOT NULL, leaving 11 rated rows at
                      200 ms/sec (60000/300s) except c6=0, c7=0; only c6 drops (prior 200 >
                      100). 10 samples, mean = 9*200/10 = 180. Wrong rewrites give 11/163.6
                      or 9/200.                                                              */
            var day = DateTime.UtcNow.Date.AddDays(-8);
            while (day.DayOfWeek != DayOfWeek.Monday) day = day.AddDays(-1);
            /* Monday 10:00 UTC, 8-14 days back — inside every 30-day window used below.
               Kind-Unspecified: naive-UTC storage, see PgCollectorRowWriter. */
            var historyStart = DateTime.SpecifyKind(day.AddHours(10), DateTimeKind.Unspecified);

            for (var i = 0; i < 12; i++)
            {
                var delta = (i == 5 || i == 6) ? 0L : 60000L;
                await InsertAsync(connection,
                    "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    (long)(i + 1), historyStart.AddMinutes(5 * i), TestServerId, TestServerName,
                    TestWaitType, 10L, delta);
            }

            /* #1757 moved the baseline supply off the raw v_* views and onto named baseline relations, so
               those relations have to EXIST for any of the assertions below to mean anything — a missing one
               throws inside ComputeBaselinesAsync, which swallows it and hands back an empty baseline, and
               every Assert below would then be comparing against zeroes.

               Deliberately the PLAIN-POSTGRESQL fallback views rather than the continuous aggregates: both
               are built from the same select body (that is the point of deriving one from the other), so they
               compute the identical statistic, and an ordinary view is isolated — creating the aggregates here
               would create all seventeen and change compose's tier routing for the live test that asserts a
               10-day window lands on RAW. The continuous-aggregate half of this invariant is proven in
               TimescaleSupportTests, which already owns the snapshot/restore machinery for that.

               Asserted as EXISTENCE of all nine, not as a return count of nine (#1862). The return is how
               many the call CREATED, and it skips any relation that is already there — so a store where a
               sibling class's continuous aggregate is still standing, or where an earlier run's fallback view
               survived, legitimately answers 8 and the old assertion failed on a store that was in every way
               fit for this test. That is the same order-dependence as the fixture bug this shipped with, one
               layer up: a count of a shared mutable store is not a property of this test. Existence is what
               the paragraph above actually needs, and it holds however the nine came to be there. The list
               being nine long is pinned purely in BaselineSupplyTests, where no store can move it. */
            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);

            foreach (var (_, view) in TimescaleSupport.BaselineAggregates)
            {
                using var exists = new NpgsqlCommand(TimescaleSupport.BaselineRelationExistsSql(view), connection);
                Assert.True((bool)(await exists.ExecuteScalarAsync(ct))!,
                    $"Baseline relation collect.{view} does not exist and could not be created as a fallback "
                    + "view. Every assertion below would then read an empty baseline and compare against "
                    + "zeroes rather than failing on the statistic it means to test.");
            }

            /* The FOLLOWING Monday 10:00 UTC — same (hour, dow) bucket, 1-7 days back, still
               in the past. Both baseline reads and the detector window anchor here. */
            var analysisTime = historyStart.AddDays(7);

            var provider = new PgBaselineProvider(postgres);

            /* ---- rewrite 4 proven live: the poisoned rate sample is excluded, the genuine
                    idle zero is kept. Tier Full also proves EXTRACT(HOUR/DOW) agreed with the
                    C# (Hour, DayOfWeek) bucket lookup — a DOW mismatch would miss the bucket. */
            var rateBaseline = await provider.GetBaselineAsync(TestServerId, MetricNames.WaitMsPerSec, analysisTime);
            Assert.Equal(10L, rateBaseline.SampleCount);
            Assert.Equal(180.0, rateBaseline.Mean, 0.001);
            Assert.Equal(BaselineTier.Full, rateBaseline.Tier);
            Assert.Equal(10, rateBaseline.HourOfDay);
            Assert.Equal((int)DayOfWeek.Monday, rateBaseline.DayOfWeek);

            /* ---- rewrite 2 proven live: same dataset, per-collection totals. */
            var waitBaseline = await provider.GetBaselineAsync(TestServerId, MetricNames.WaitStats, analysisTime);
            Assert.Equal(11L, waitBaseline.SampleCount);
            Assert.Equal(600000.0 / 11.0, waitBaseline.Mean, 0.01);
            Assert.Equal(BaselineTier.Full, waitBaseline.Tier);

            /* ---- plant the anomalous current window: a heavy wait profile one week after the
                    history. The history is ONE distinct Monday, so under the change-2 quality gate the
                    WaitMsPerSec baseline (Full tier) is UNtrustworthy (Full needs >= 3 distinct days),
                    and change 1's wait detector falls back to the absolute peak-rate bar (is_new)
                    rather than a ratio. Three 5-minute-spaced collections at 200000ms each: the first
                    is dropped (no prior in-window interval), the second and third rate at
                    200000/300s ≈ 666.7 ms/sec — past the 250 ms/sec WaitProfileFallbackMsPerSec bar. */
            await InsertAsync(connection,
                "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                100L, analysisTime.AddMinutes(5), TestServerId, TestServerName, TestWaitType, 50L, 200000L);
            await InsertAsync(connection,
                "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                101L, analysisTime.AddMinutes(10), TestServerId, TestServerName, TestWaitType, 50L, 200000L);
            await InsertAsync(connection,
                "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                102L, analysisTime.AddMinutes(15), TestServerId, TestServerName, TestWaitType, 50L, 200000L);

            var detector = new PgAnomalyDetector(postgres, provider);
            var context = new AnalysisContext
            {
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = analysisTime,
                TimeRangeEnd = analysisTime.AddMinutes(30),
                ServerUtcOffset = TimeSpan.Zero
            };

            /* Full public-surface run: the HasBaselineData gate passes on the planted waits, every
               other detector no-ops on its empty tables, and the wait detector emits exactly one
               ANOMALY_WAIT_PROFILE fact with the top wait type named as contrib_<TYPE>. */
            var anomalies = await detector.DetectAnomaliesAsync(context);

            var fact = Assert.Single(anomalies);
            Assert.Equal("ANOMALY_WAIT_PROFILE", fact.Key);
            Assert.Equal("anomaly", fact.Source);
            Assert.Equal(TestServerId, fact.ServerId);
            Assert.Equal(600000.0, fact.Value);               // total all-types wait ms in the window
            Assert.Equal(1.0, fact.Metadata["is_new"]);       // thin baseline → absolute-bar fallback
            Assert.Equal(100.0, fact.Metadata["ratio"]);      // NoBaselineRatio sentinel (is_new)
            Assert.True(fact.Metadata.ContainsKey($"contrib_{TestWaitType}"), "the planted wait type must be named as a contributor");
            Assert.Equal(600000.0, fact.Metadata[$"contrib_{TestWaitType}"]);
            /* The Full-tier baseline still resolved (10 samples ≥ collapse threshold) — only its
               DISTINCT-day count (1) disqualifies it from being trusted; its bucket coordinates carry through. */
            Assert.Equal((double)BaselineTier.Full, fact.Metadata["baseline_tier"]);
            Assert.Equal(10.0, fact.Metadata["baseline_hour"]);
            Assert.Equal((double)DayOfWeek.Monday, fact.Metadata["baseline_dow"]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteTestRowsAsync(cleanup, cleanupCt);
                await DropBaselineFallbackViewsAsync(cleanup, cleanupCt);
            });
        }
    }

    /// <summary>
    /// #1995 review follow-through: the IO arm's NULL-ratio semantics, proven against a LIVE
    /// Postgres instead of claimed in comments. The arm's WHERE keeps write-only file rows
    /// (delta_reads = 0, delta_writes &gt; 0), whose stall/reads ratio is NULL through NULLIF — and
    /// the scaffold must COUNT those rows (the retired rollup's row_count behavior) while
    /// AVG/STDDEV/median/mad ignore them (the ratio_count behavior). If percentile_cont ever
    /// counted NULLs the median here would shift off 2.5; if the WHERE dropped write-only rows the
    /// sample count would read 4. Grain note, measured on the production fleet: file_io_stats is
    /// ~216K rows/server/30d (13x CPU's grain) and the full composed arm ran in 1.15 s on the
    /// busiest tenant — fine behind the provider's 1-hour cache.
    /// </summary>
    [Fact]
    public async Task EndToEnd_IoLatencyArm_CountsWriteOnlyRows_StatsIgnoreThem_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live IO-arm test.");

        var ct = TestContext.Current.CancellationToken;
        const int ioServerId = TestServerId + 1; // own id — this test cleans its own rows

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using (var cleanup = new NpgsqlCommand($"DELETE FROM file_io_stats WHERE server_id = {ioServerId};", connection))
        {
            await cleanup.ExecuteNonQueryAsync(ct);
        }

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var day = DateTime.UtcNow.Date.AddDays(-8);
            while (day.DayOfWeek != DayOfWeek.Monday) day = day.AddDays(-1);
            var historyStart = DateTime.SpecifyKind(day.AddHours(10), DateTimeKind.Unspecified);

            /* Four read-bearing rows with hand-checkable ratios 1, 2, 3, 4 ms; one WRITE-ONLY row
               (NULL ratio, must be counted but not averaged); one no-activity row (must be
               filtered by the WHERE entirely). Expected flat tier: 5 samples, median 2.5, MAD 1.0. */
            var rows = new (long Reads, long Writes, long StallReadMs)[]
                { (10, 0, 10), (10, 0, 20), (10, 0, 30), (10, 0, 40), (0, 25, 0), (0, 0, 0) };
            for (var i = 0; i < rows.Length; i++)
            {
                await InsertAsync(connection,
                    "INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, delta_reads, delta_writes, delta_stall_read_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    (long)(100 + i), historyStart.AddMinutes(5 * i), ioServerId, "IO-NULL-SEMANTICS",
                    rows[i].Reads, rows[i].Writes, rows[i].StallReadMs);
            }

            var provider = new PgBaselineProvider(postgres);
            var bucket = await provider.GetBaselineAsync(ioServerId, MetricNames.IoLatency, historyStart.AddDays(7));

            /* One sparse bucket collapses to the exact flat sentinel — robust fields intact. */
            Assert.Equal(BaselineTier.Flat, bucket.Tier);
            Assert.Equal(5, bucket.SampleCount);          // write-only row COUNTED, no-activity row filtered
            Assert.Equal(2.5, bucket.Median, precision: 6); // NULL ratio ignored by the median
            Assert.Equal(1.0, bucket.Mad, precision: 6);
            Assert.Equal(2.5, bucket.Mean, precision: 6);   // and by the mean — both counts' semantics hold

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                using var command = new NpgsqlCommand($"DELETE FROM file_io_stats WHERE server_id = {ioServerId};", cleanup);
                await command.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    private static async Task InsertAsync(NpgsqlConnection connection, string sql, params object[] values)
    {
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values)
        {
            command.Parameters.AddWithValue(value);
        }
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {TestServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Restores the shared fixture by removing the plain baseline views this class creates. Uses the same
    /// continuous-aggregate guard the product does, so it can never drop a real aggregate another live test
    /// planted — a bare DROP VIEW would, because a continuous aggregate is also a relkind='v' view.
    /// </summary>
    private static async Task DropBaselineFallbackViewsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        foreach (var (_, view) in TimescaleSupport.BaselineAggregates)
        {
            using var drop = new NpgsqlCommand(TimescaleSupport.DropBaselineFallbackViewSql(view), connection);
            await drop.ExecuteNonQueryAsync(ct);
        }
    }
}
