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
using System.Threading;
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
        PgAnomalyDetector.CpuTileWindowSql,
        PgAnomalyDetector.WaitRateTileWindowSql,
        PgAnomalyDetector.WaitContribWindowSql,
        PgAnomalyDetector.BlockingWindowSql,
        PgAnomalyDetector.IoTileWindowSql,
        PgAnomalyDetector.BatchRequestTileWindowSql,
        PgAnomalyDetector.SessionTileWindowSql,
        PgAnomalyDetector.QueryDurationTileWindowSql,
        PgAnomalyDetector.MemoryTileWindowSql,
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
        /* Lite's public surface: bucket lookup with tier collapse + the test cache hooks. The lookup is asked for by
           its four-parameter signature since #3691 lane 33 put the KEYED five-parameter overload beside it — a bare
           GetMethod(name) is ambiguous with two overloads and would throw, not fail. Lite's twin has one overload. */
        Assert.NotNull(typeof(PgBaselineProvider).GetMethod("GetBaselineAsync", [typeof(int), typeof(string), typeof(DateTime), typeof(CancellationToken)]));
        Assert.NotNull(typeof(PgBaselineProvider).GetMethod("GetBaselineAsync", [typeof(int), typeof(string), typeof(string), typeof(DateTime), typeof(CancellationToken)]));
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
        var exclusionAt = sql.IndexOf("WHERE NOT (delta_cntr_value = 0 AND prior_delta > 1000 AND sample_interval_seconds IS NULL)", StringComparison.Ordinal);

        Assert.True(lagAt >= 0, "the restart LAG must be computed in the windowed CTE");
        Assert.True(fromCteAt > lagAt, "the aggregate must select FROM the windowed CTE");
        Assert.True(exclusionAt > fromCteAt, "the exclusion must filter OUTSIDE the CTE — after the window is computed");

        /* The pre-window row filter is unchanged from Lite — and since #3653 carries Lite's knowability
           filter too, so the restart's interval-0 row never reaches the heuristic. */
        Assert.Contains("FROM perfmon_interval_baseline", sql, StringComparison.Ordinal);
        Assert.Contains("counter_name = 'Batch Requests/sec'", TimescaleSupport.CreatePerfmonIntervalBaselineSql, StringComparison.Ordinal);
        Assert.Contains("delta_cntr_value >= 0", TimescaleSupport.CreatePerfmonIntervalBaselineSql, StringComparison.Ordinal);
        Assert.Contains("sample_interval_seconds IS DISTINCT FROM 0", TimescaleSupport.CreatePerfmonIntervalBaselineSql, StringComparison.Ordinal);

        /* #3527 re-taken by #3653: v is the PER-SECOND rate over the collection's STORED interval, which the
           perfmon_interval_baseline supply now carries; the LAG(collection_time) gap is only the fallback for a
           pre-column (NULL-interval) collection. Both live in the SAME windowed CTE (window-before-filter
           holds for the fallback too), and the interval filter sits OUTSIDE with the exclusion. The DOUBLE
           PRECISION cast is the io-arm rule: STDDEV_SAMP over numeric can overflow System.Decimal. */
        var intervalAt = sql.IndexOf("COALESCE(sample_interval_seconds::DOUBLE PRECISION,", StringComparison.Ordinal);
        var lagFallbackAt = sql.IndexOf("extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time))))) AS interval_sec", StringComparison.Ordinal);
        Assert.True(intervalAt >= 0 && intervalAt < fromCteAt, "interval_sec must read the stored interval inside the windowed CTE");
        Assert.True(lagFallbackAt > intervalAt && lagFallbackAt < fromCteAt, "the LAG-derived gap must be the COALESCE fallback, inside the same CTE");
        Assert.Contains("delta_cntr_value::DOUBLE PRECISION / interval_sec AS v", sql, StringComparison.Ordinal);
        var intervalFilterAt = sql.IndexOf("interval_sec > 0", StringComparison.Ordinal);
        Assert.True(intervalFilterAt > fromCteAt, "the interval filter must sit OUTSIDE the windowed CTE, with the exclusion");
    }

    /// <summary>
    /// #3653 (A10, the mechanical half): the three arms whose supply became interval-honest apply the
    /// three-state rule — the <c>LAG &gt; N</c> magnitude heuristic is GATED on <c>sample_interval_seconds IS
    /// NULL</c> (pre-column collections, where it is still the only restart guard) and is not consulted for a
    /// collection with a measured interval, whose zero is a real idle sample. The pin is on the gate's presence
    /// in the successor text AND its absence from the legacy text, so neither can be "simplified" into the other.
    /// </summary>
    [Fact]
    public void SuccessorArms_GateTheMagnitudeHeuristicOnANullInterval_LegacyArmsDoNot()
    {
        var gated = new (string Metric, string Predicate)[]
        {
            (MetricNames.BatchRequests, "WHERE NOT (delta_cntr_value = 0 AND prior_delta > 1000 AND sample_interval_seconds IS NULL)"),
            (MetricNames.WaitStats, "WHERE NOT (total_wait_ms = 0 AND prior_total_wait_ms > 10000 AND sample_interval_seconds IS NULL)"),
            (MetricNames.WaitMsPerSec, "WHERE NOT (ms_per_sec = 0 AND prior_ms_per_sec > 100 AND sample_interval_seconds IS NULL)"),
        };
        /* The ungated form closes its parenthesis right after the bar; the gated form continues with AND. */
        var ungated = new (string Metric, string Predicate)[]
        {
            (MetricNames.BatchRequests, "WHERE NOT (delta_cntr_value = 0 AND prior_delta > 1000)"),
            (MetricNames.WaitStats, "WHERE NOT (total_wait_ms = 0 AND prior_total_wait_ms > 10000)"),
            (MetricNames.WaitMsPerSec, "WHERE NOT (ms_per_sec = 0 AND prior_ms_per_sec > 100)"),
        };

        for (var i = 0; i < gated.Length; i++)
        {
            var successor = PgBaselineProvider.GetBaselineQuery(gated[i].Metric)!;
            var legacy = PgBaselineProvider.GetLegacyBaselineQuery(gated[i].Metric)!;

            Assert.Contains(gated[i].Predicate, successor, StringComparison.Ordinal);
            Assert.DoesNotContain(gated[i].Predicate, legacy, StringComparison.Ordinal);
            Assert.Contains(ungated[i].Predicate, legacy, StringComparison.Ordinal);
            Assert.DoesNotContain(ungated[i].Predicate, successor, StringComparison.Ordinal);

            /* The successor reads the successor relation and never the legacy one; the legacy text the reverse.
               The legacy name is a PREFIX of the successor's source line only in the other direction
               (wait_stats_baseline vs wait_stats_interval_baseline), so "FROM <legacy>" followed by a line
               break is what distinguishes a real legacy read from the successor's longer name. */
            var (legacyView, successorView) = PgBaselineProvider.SupersededSupplyFor(gated[i].Metric)!.Value;
            Assert.Contains("FROM " + successorView, successor, StringComparison.Ordinal);
            Assert.False(
                System.Text.RegularExpressions.Regex.IsMatch(successor, "FROM " + System.Text.RegularExpressions.Regex.Escape(legacyView) + @"\s"),
                $"the successor text for {gated[i].Metric} still reads the legacy relation {legacyView}");
            Assert.Contains("FROM " + legacyView, legacy, StringComparison.Ordinal);
            Assert.DoesNotContain("FROM " + successorView, legacy, StringComparison.Ordinal);

            /* Both texts are Postgres: bound window, no QUALIFY, no bare clock, same robust scaffold. */
            foreach (var text in new[] { successor, legacy })
            {
                Assert.DoesNotContain("QUALIFY", text, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("now(", text, StringComparison.OrdinalIgnoreCase);
                Assert.Contains("$1", text, StringComparison.Ordinal);
                Assert.EndsWith(PgBaselineProvider.RobustTierScaffold, text, StringComparison.Ordinal);
            }
        }

        /* The rate arms read the STORED interval first and derive one from LAG only where it is NULL. */
        Assert.Contains("CASE WHEN sample_interval_seconds IS NULL", PgBaselineProvider.GetBaselineQuery(MetricNames.WaitMsPerSec)!, StringComparison.Ordinal);
        Assert.DoesNotContain("sample_interval_seconds", PgBaselineProvider.GetLegacyBaselineQuery(MetricNames.WaitMsPerSec)!, StringComparison.Ordinal);
        Assert.DoesNotContain("sample_interval_seconds", PgBaselineProvider.GetLegacyBaselineQuery(MetricNames.BatchRequests)!, StringComparison.Ordinal);
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
        /* #3653 A8 option B (lane L2b): the SQL Server-store detector reads the TILED const now — the
           production reader moved, so the pin follows it. The old plain BatchRequestWindowSql const had
           no remaining reader and was deleted. */
        var sql = PgAnomalyDetector.BatchRequestTileWindowSql;

        Assert.Contains("AVG(delta_cntr_value * 1.0 / NULLIF(sample_interval_seconds, 0))", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(delta_cntr_value * 1.0 / NULLIF(sample_interval_seconds, 0))", sql, StringComparison.Ordinal);
        Assert.Contains("sample_interval_seconds > 0", sql, StringComparison.Ordinal);

        /* A raw AVG/MAX of the delta is exactly the #3527 defect — pin its absence. */
        Assert.DoesNotContain("AVG(delta_cntr_value)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("MAX(delta_cntr_value)", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3653 (A8, first slice): the I/O window read hands the shared gate the PEAK and the MEAN per-file-row
    /// latency for reads and for writes — it was the one z-score family reading AVG ALONE while its siblings
    /// read the window MAX under the same shared cutoffs. Structural pin on the PG const, and a line-for-line
    /// parity pin against Lite's inline twin (the two SKUs' window reads are Lite-verbatim by contract; the
    /// repo pins no other IO-window parity, so this is the one that keeps them from drifting apart on the
    /// statistic they hand the gate).
    /// </summary>
    [Fact]
    public void IoWindow_ReadsThePeakAndMeanPair_ForReadsAndWrites_LiteVerbatim()
    {
        var sql = PgAnomalyDetector.IoTileWindowSql;
        var expectedColumns = new[]
        {
            "MAX(delta_stall_read_ms * 1.0 / NULLIF(delta_reads, 0)) AS peak_read_lat",
            "AVG(delta_stall_read_ms * 1.0 / NULLIF(delta_reads, 0)) AS avg_read_lat",
            "MAX(delta_stall_write_ms * 1.0 / NULLIF(delta_writes, 0)) AS peak_write_lat",
            "AVG(delta_stall_write_ms * 1.0 / NULLIF(delta_writes, 0)) AS avg_write_lat",
        };
        foreach (var column in expectedColumns)
            Assert.Contains(column, sql, StringComparison.Ordinal);

        /* The column ORDER is the reader's ordinal contract (0 peak read, 1 avg read, 2 peak write, 3 avg write). */
        var positions = expectedColumns.Select(c => sql.IndexOf(c, StringComparison.Ordinal)).ToArray();
        Assert.True(positions.SequenceEqual(positions.OrderBy(p => p)), "peak/avg column order is the reader's ordinal contract");

        /* Same grain and row filter as the io_latency baseline arm (per-file-row, read-or-write-bearing rows). */
        Assert.Contains("FROM v_file_io_stats", sql, StringComparison.Ordinal);
        Assert.Contains("(delta_reads > 0 OR delta_writes > 0)", sql, StringComparison.Ordinal);

        /* Lite's inline twin carries the same four columns, in the same order. */
        var lite = RepoFile.ReadRepoFile("Lite", "Analysis", "AnomalyDetector.cs");
        var litePositions = expectedColumns.Select(c => lite.IndexOf(c, StringComparison.Ordinal)).ToArray();
        Assert.All(litePositions, p => Assert.True(p > 0, "Lite's I/O window read has drifted from the PG twin"));
        Assert.True(litePositions.SequenceEqual(litePositions.OrderBy(p => p)));

        /* And every z-score family in BOTH detectors hands the gate the PAIR — no peak-only call survives
           in the SQL Server detector bodies (the PostgreSQL-target detector's peak-only calls are the
           documented transitional state, owned by its content lanes). Eight since #3741: the wait-profile
           detector's trusted robust arm is the eighth call — it was the one baseline detector #3724 left
           on an inline peak-only gate, and this pin deliberately excluded it until it went through the root. */
        var pg = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgAnomalyDetector.cs"));
        var liteCode = CSharpSourceWalker.StripCommentsAndStrings(lite);
        foreach (var code in new[] { pg, liteCode })
        {
            /* #3653 A8 option B (lane L2a-2): the tiled read/write I/O arms now name their never-blind
               fallback locals readBaseline/writeBaseline (each family reads its own bucket independently
               once tiles fall back), not the shared "baseline" local the other six families still use —
               so the baseline-argument capture is widened to any identifier ending in "Baseline", still
               anchored on "baseline," as a literal for the pin's substring intent. */
            var calls = System.Text.RegularExpressions.Regex.Matches(code, @"AnomalyGate\.EvaluateZScore\(\s*(\w*[Bb]aseline),\s*(\w+),\s*(\w+),");
            Assert.Equal(8, calls.Count); // cpu, wait profile (#3741), read, write, batch, session, query duration, memory
            foreach (System.Text.RegularExpressions.Match call in calls)
            {
                Assert.EndsWith("aseline", call.Groups[1].Value, StringComparison.Ordinal);
                Assert.StartsWith("peak", call.Groups[2].Value, StringComparison.Ordinal);
                Assert.StartsWith("avg", call.Groups[3].Value, StringComparison.Ordinal);
            }
            Assert.DoesNotMatch(@"AnomalyGate\.EvaluateZScore\(\s*\w*[Bb]aseline,\s*\w+,\s*(ioThreshold|GetDeviationThreshold)", code);
        }
    }

    /// <summary>
    /// #3741 (the last leg of #3653 Q1 / #3724): the wait-profile window read hands the detector the PEAK and
    /// the MEAN all-types ms/sec, and the trusted robust arm is the shared gate's PAIR call rather than the
    /// inline peak-only test it kept through #3724. Structural pin on the PG const (the two aggregates share
    /// the interval-guarded arm, so a NULL-interval collection is neither statistic's term; column order is
    /// the reader's ordinal contract), a line-for-line parity pin against Lite's inline twin, and a pin on
    /// the detector bodies: the gate call names <c>HeavyTailModifiedZThreshold</c> and
    /// <c>WaitProfileFallbackMsPerSec</c> (the wait family's cutoff and its one bar), the inline
    /// <c>modifiedZ &lt; HeavyTailModifiedZThreshold</c> gate is gone from both SKUs, the ratio arm tests a
    /// <c>meanRatio</c> beside <c>ratio</c>, and the no-baseline arm still tests <c>peakRate</c> alone against
    /// the bar — the ruling keeps it there.
    /// </summary>
    [Fact]
    public void WaitRateWindow_ReadsThePeakAndMeanPair_AndTheRobustArmIsTheSharedPairGate_LiteVerbatim()
    {
        /* #3653 B: both SQL Server-store reads are now the TILED const — one row per target-local hour,
           local_hour first (peak 1, avg 2, total 3, count 4 in each reader's own read, per DetectWaitAnomalies
           on each SKU, #4172 then #4169). The four Lite-parity columns below are unaffected by that shift:
           they are substring/ordinal pins on the SAME peak/avg/total/count column TEXT, checked to appear in
           the same relative order in both, which the shared local_hour-first shift does not change. */
        var sql = PgAnomalyDetector.WaitRateTileWindowSql;
        var expectedColumns = new[]
        {
            "MAX(CASE WHEN interval_sec > 0 THEN total_wait_ms / interval_sec END) AS peak_ms_per_sec",
            "AVG(CASE WHEN interval_sec > 0 THEN total_wait_ms / interval_sec END) AS avg_ms_per_sec",
            "SUM(total_wait_ms) AS total_wait_ms",
            "COUNT(*) FILTER (WHERE interval_sec IS NOT NULL) AS sample_count",
        };
        foreach (var column in expectedColumns)
            Assert.Contains(column, sql, StringComparison.Ordinal);

        /* The column ORDER among these four is still the reader's ordinal contract, one position later than
           before (local_hour occupies 0): peak 1, avg 2, total 3, count 4 (WaitRateTileWindowSql's doc
           comment, pinned). */
        var positions = expectedColumns.Select(c => sql.IndexOf(c, StringComparison.Ordinal)).ToArray();
        Assert.True(positions.SequenceEqual(positions.OrderBy(p => p)), "peak/avg/total/count column order is the reader's ordinal contract");

        /* Lite's inline twin carries the same four columns, in the same order. */
        var lite = RepoFile.ReadRepoFile("Lite", "Analysis", "AnomalyDetector.cs");
        var litePositions = expectedColumns.Select(c => lite.IndexOf(c, StringComparison.Ordinal)).ToArray();
        Assert.All(litePositions, p => Assert.True(p > 0, "Lite's wait-rate window read has drifted from the PG twin"));
        Assert.True(litePositions.SequenceEqual(litePositions.OrderBy(p => p)));

        var pg = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgAnomalyDetector.cs"));
        var liteCode = CSharpSourceWalker.StripCommentsAndStrings(lite);
        foreach (var code in new[] { pg, liteCode })
        {
            /* The robust arm: ONE pair call, the wait family's cutoff as the modified-z cutoff and its one bar as
               the floor — peak AND mean must clear 5.0, the 250 ms/sec floor stays on the peak inside the gate. */
            /* #3653 B: both detectors' never-blind fallback pass the pre-computed `window` local
               (TimeRangeEnd - TimeRangeStart, computed once at the top of the method for BindTiledWindow/
               GetBucketMapAsync and EvaluateTiles to share) rather than re-deriving it inline at the call site
               (#4172 then #4169). Untiled families still re-derive it inline, so either spelling is accepted
               here rather than pinning one. */
            Assert.Matches(
                @"AnomalyGate\.EvaluateZScore\(\s*baseline,\s*peakRate,\s*avgRate,\s*HeavyTailModifiedZThreshold,\s*HeavyTailModifiedZThreshold,\s*WaitProfileFallbackMsPerSec,\s*WaitProfileFallbackMsPerSec,\s*SigmaDisplayCap,\s*window:\s*(window|context\.TimeRangeEnd\s*-\s*context\.TimeRangeStart)\)",
                code);

            /* The inline peak-only gate is gone. */
            Assert.DoesNotMatch(@"modifiedZ\s*<\s*HeavyTailModifiedZThreshold", code);

            /* The ratio arm asks the same of both ratios. */
            Assert.Matches(@"var\s+meanRatio\s*=\s*avgRate\s*/\s*baseline\.Mean", code);
            Assert.Matches(@"ratio\s*<\s*DefaultRatioThreshold\s*\|\|\s*meanRatio\s*<\s*DefaultRatioThreshold", code);

            /* The no-baseline arm stays on the peak's absolute bar alone (the ruling). */
            Assert.Matches(@"ratio\s*=\s*peakRate\s*>=\s*WaitProfileFallbackMsPerSec\s*\?\s*NoBaselineRatio\s*:\s*0", code);
            Assert.DoesNotMatch(@"avgRate\s*>=\s*WaitProfileFallbackMsPerSec", code);
        }

        /* #3653 B (#4169): both detectors now read peakRate/avgRate off WindowTiles.WholeWindow(tiles).Peak/
           .Mean, fed by a tiled read, rather than off a single-row reader ordinal directly. Pin that shape
           directly, on BOTH SKUs, rather than against a stale reader-ordinal regex that assumed a single
           collapsed row on one side. */
        foreach (var (name, code) in new[] { ("pg", pg), ("lite", liteCode) })
        {
            Assert.True(System.Text.RegularExpressions.Regex.IsMatch(code, @"var\s+whole\s*=\s*WindowTiles\.WholeWindow\(tiles\)"), $"{name}: missing 'var whole = WindowTiles.WholeWindow(tiles)'");
            Assert.True(System.Text.RegularExpressions.Regex.IsMatch(code, @"var\s+peakRate\s*=\s*whole\.Peak"), $"{name}: missing 'var peakRate = whole.Peak'");
            Assert.True(System.Text.RegularExpressions.Regex.IsMatch(code, @"var\s+avgRate\s*=\s*whole\.Mean"), $"{name}: missing 'var avgRate = whole.Mean'");
        }

        /* #3653 A8 hygiene: PG used to re-run WaitRateTileWindowSql a SECOND time, word for word, just to sum
           total_wait_ms (ordinal 3) into totalWaitMs — a real structural difference from Lite's twin (which
           has always summed the same column inside its tile-read loop) that #3653 B's own pin above left
           unresolved pending a coordinator ruling. That ruling is this: read once, like Lite. Both sides now
           sum ordinal 3 off the SAME reader the tiles themselves come from (rateReader), inside the SAME
           loop, so this pin both requires the new shape AND forbids the old one's tell (a second reader
           variable, totalReader, reading the identical SQL again) from coming back. Live-measured with
           pg_stat_statements: this statement ran twice per analysis pass before the fix, once after. */
        Assert.Matches(@"totalWaitMs\s*\+=\s*rateReader\.IsDBNull\(3\)", pg);
        Assert.Matches(@"windowTotalWaitMs\s*\+=\s*rateReader\.IsDBNull\(3\)", liteCode);
        Assert.DoesNotMatch(@"totalReader", pg);
    }

    /// <summary>
    /// #3653 B (#4172 then #4169): a census of <c>AnomalyGate.EvaluateTiles</c> calls in the tiled families
    /// (CPU, wait-profile's robust arm, I/O read, I/O write) — four on each SKU. CPU and wait's calls are
    /// byte-identical between the two detectors (checked literally). I/O's two calls tolerate the SAME two
    /// differences this file already tolerates elsewhere by design, not by omission: the baseline-map
    /// argument (PG re-fetches into <c>readMap</c>/<c>writeMap</c>, each gate scoring independently; Lite
    /// shares one <c>map</c> fetch — same cache key, same value, mirroring how this file already accepts
    /// <c>\w*[Bb]aseline</c> for PG's <c>readBaseline</c>/<c>writeBaseline</c> against Lite's shared
    /// <c>baseline</c>), and the modified-z threshold argument (PG hoists it once into <c>modifiedZThreshold</c>;
    /// Lite calls <c>ModifiedZThresholdFor</c> inline at each site — same pure function, same two inputs —
    /// mirroring how the wait test above already accepts either the hoisted <c>window</c> local or the inline
    /// <c>context.TimeRangeEnd - context.TimeRangeStart</c> expression for the SAME reason).
    /// </summary>
    [Fact]
    public void AnomalyGate_EvaluateTilesCalls_AreFourPerSku_AndMatchBetweenPgAndLite()
    {
        var pg = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgAnomalyDetector.cs"));
        var liteCode = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Lite", "Analysis", "AnomalyDetector.cs"));

        // #3653 B: L4b lands Lite's mirror of L2b's four (batch, sessions, query, memory), so the census is
        // exact again -- eight calls on each side, byte-identical per family.
        foreach (var (name, code) in new[] { ("pg", pg), ("lite", liteCode) })
        {
            var calls = System.Text.RegularExpressions.Regex.Matches(code, @"AnomalyGate\.EvaluateTiles\(");
            Assert.True(calls.Count == 8, $"{name}: expected 8 EvaluateTiles calls (cpu, wait, io-read, io-write, batch, sessions, query, memory), found {calls.Count}");
        }

        // CPU: byte-identical.
        Assert.Matches(@"AnomalyGate\.EvaluateTiles\(\s*tiles,\s*map,\s*cpuThreshold,\s*ModifiedZThresholdFor\(MetricNames\.Cpu,\s*cpuThreshold\),\s*CpuFloorPct,\s*CpuFallbackPct,\s*SigmaDisplayCap,\s*window\)", pg);
        Assert.Matches(@"AnomalyGate\.EvaluateTiles\(\s*tiles,\s*map,\s*cpuThreshold,\s*ModifiedZThresholdFor\(MetricNames\.Cpu,\s*cpuThreshold\),\s*CpuFloorPct,\s*CpuFallbackPct,\s*SigmaDisplayCap,\s*window\)", liteCode);

        // Wait profile's robust arm: byte-identical.
        Assert.Matches(@"AnomalyGate\.EvaluateTiles\(\s*tiles,\s*map,\s*HeavyTailModifiedZThreshold,\s*HeavyTailModifiedZThreshold,\s*WaitProfileFallbackMsPerSec,\s*WaitProfileFallbackMsPerSec,\s*SigmaDisplayCap,\s*window\)", pg);
        Assert.Matches(@"AnomalyGate\.EvaluateTiles\(\s*tiles,\s*map,\s*HeavyTailModifiedZThreshold,\s*HeavyTailModifiedZThreshold,\s*WaitProfileFallbackMsPerSec,\s*WaitProfileFallbackMsPerSec,\s*SigmaDisplayCap,\s*window\)", liteCode);

        // I/O read: the map argument and the modified-z threshold tolerate the two documented differences.
        Assert.Matches(@"AnomalyGate\.EvaluateTiles\(\s*readTiles,\s*\w*[Mm]ap,\s*ioThreshold,\s*(modifiedZThreshold|ModifiedZThresholdFor\(MetricNames\.IoLatency,\s*ioThreshold\)),\s*ReadLatencyFloorMs,\s*IoLatencyFallbackMs,\s*SigmaDisplayCap,\s*window\)", pg);
        Assert.Matches(@"AnomalyGate\.EvaluateTiles\(\s*readTiles,\s*\w*[Mm]ap,\s*ioThreshold,\s*(modifiedZThreshold|ModifiedZThresholdFor\(MetricNames\.IoLatency,\s*ioThreshold\)),\s*ReadLatencyFloorMs,\s*IoLatencyFallbackMs,\s*SigmaDisplayCap,\s*window\)", liteCode);

        // I/O write: same tolerances.
        Assert.Matches(@"AnomalyGate\.EvaluateTiles\(\s*writeTiles,\s*\w*[Mm]ap,\s*ioThreshold,\s*(modifiedZThreshold|ModifiedZThresholdFor\(MetricNames\.IoLatency,\s*ioThreshold\)),\s*WriteLatencyFloorMs,\s*IoLatencyFallbackMs,\s*SigmaDisplayCap,\s*window\)", pg);
        Assert.Matches(@"AnomalyGate\.EvaluateTiles\(\s*writeTiles,\s*\w*[Mm]ap,\s*ioThreshold,\s*(modifiedZThreshold|ModifiedZThresholdFor\(MetricNames\.IoLatency,\s*ioThreshold\)),\s*WriteLatencyFloorMs,\s*IoLatencyFallbackMs,\s*SigmaDisplayCap,\s*window\)", liteCode);

        // Batch requests: byte-identical.
        Assert.Matches(@"AnomalyGate\.EvaluateTiles\(\s*tiles,\s*map,\s*batchThreshold,\s*ModifiedZThresholdFor\(MetricNames\.BatchRequests,\s*batchThreshold\),\s*BatchRequestFloor,\s*BatchRequestFallback,\s*SigmaDisplayCap,\s*window\)", pg);
        Assert.Matches(@"AnomalyGate\.EvaluateTiles\(\s*tiles,\s*map,\s*batchThreshold,\s*ModifiedZThresholdFor\(MetricNames\.BatchRequests,\s*batchThreshold\),\s*BatchRequestFloor,\s*BatchRequestFallback,\s*SigmaDisplayCap,\s*window\)", liteCode);

        // Sessions: byte-identical.
        Assert.Matches(@"AnomalyGate\.EvaluateTiles\(\s*tiles,\s*map,\s*sessionThreshold,\s*ModifiedZThresholdFor\(MetricNames\.SessionCount,\s*sessionThreshold\),\s*SessionCountFloor,\s*SessionCountFallback,\s*SigmaDisplayCap,\s*window\)", pg);
        Assert.Matches(@"AnomalyGate\.EvaluateTiles\(\s*tiles,\s*map,\s*sessionThreshold,\s*ModifiedZThresholdFor\(MetricNames\.SessionCount,\s*sessionThreshold\),\s*SessionCountFloor,\s*SessionCountFallback,\s*SigmaDisplayCap,\s*window\)", liteCode);

        // Query duration: byte-identical.
        Assert.Matches(@"AnomalyGate\.EvaluateTiles\(\s*tiles,\s*map,\s*queryDurationThreshold,\s*ModifiedZThresholdFor\(MetricNames\.QueryDuration,\s*queryDurationThreshold\),\s*QueryDurationFloorUs,\s*QueryDurationFallbackUs,\s*SigmaDisplayCap,\s*window\)", pg);
        Assert.Matches(@"AnomalyGate\.EvaluateTiles\(\s*tiles,\s*map,\s*queryDurationThreshold,\s*ModifiedZThresholdFor\(MetricNames\.QueryDuration,\s*queryDurationThreshold\),\s*QueryDurationFloorUs,\s*QueryDurationFallbackUs,\s*SigmaDisplayCap,\s*window\)", liteCode);

        // Memory: byte-identical.
        Assert.Matches(@"AnomalyGate\.EvaluateTiles\(\s*tiles,\s*map,\s*memoryThreshold,\s*ModifiedZThresholdFor\(MetricNames\.Memory,\s*memoryThreshold\),\s*MemoryPressureFloorPct,\s*MemoryPressureFallbackPct,\s*SigmaDisplayCap,\s*window\)", pg);
        Assert.Matches(@"AnomalyGate\.EvaluateTiles\(\s*tiles,\s*map,\s*memoryThreshold,\s*ModifiedZThresholdFor\(MetricNames\.Memory,\s*memoryThreshold\),\s*MemoryPressureFloorPct,\s*MemoryPressureFallbackPct,\s*SigmaDisplayCap,\s*window\)", liteCode);
    }

    /// <summary>
    /// #3527 proven live, both halves in one place: the BatchRequests BASELINE arm derives its
    /// per-second unit from LAG(collection_time) over the perfmon_baseline supply, and the DETECTOR's
    /// window read divides by the stored measured interval — so the two sides meet in the same
    /// requests/sec unit and the absolute bars judge honest rates.
    ///
    /// <para>History (one Monday-10:00 bucket, 12 collections at 300s spacing, delta 30000 each —
    /// 100 req/sec, every row carrying its measured interval): c1 is rated off its STORED interval
    /// (#3653 — the legacy arm had to drop it for lacking a LAG prior), c6 is the restart row as the
    /// collector actually writes one (delta 0 WITH interval 0 → dropped by the supply's knowability filter
    /// before it can be a sample), c7 is a genuine idle zero over a measured interval (kept at 0/sec — the
    /// magnitude heuristic is gated off for a measured row). 11 samples, mean (10x100 + 0)/11 = 90.909 —
    /// in requests/sec, where the raw-delta unit would read ~27273.</para>
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
                /* c6 is the restart as the collector writes it: interval 0 beside the zero delta. c7 is a
                   measured idle zero. The difference between them is the whole of #3653's A10 half. */
                var interval = i == 5 ? 0 : 300;
                await InsertAsync(connection,
                    "INSERT INTO perfmon_stats (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
                    (long)(200 + i), historyStart.AddMinutes(5 * i), batchServerId, batchServerName,
                    "SQLServer:SQL Statistics", "Batch Requests/sec", "", delta * 2, delta, interval);
            }

            /* The baseline supply must exist (see the wait test's note) — plain fallback views. */
            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);

            var provider = new PgBaselineProvider(postgres);
            var analysisTime = historyStart.AddDays(7);

            var baseline = await provider.GetBaselineAsync(batchServerId, MetricNames.BatchRequests, analysisTime);
            Assert.Equal(11L, baseline.SampleCount);
            Assert.Equal(1000.0 / 11.0, baseline.Mean, 0.001);
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
            Assert.Equal(1000.0 / 11.0, fact.Metadata["baseline_mean"], 0.001);    // same unit as the window
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
           applied OUTSIDE the windowed CTE so a dropped row still serves as its successor's LAG value.

           #3653: the supply is the interval-honest aggregate and the exclusion is gated on a NULL stored
           interval (SuccessorArms_GateTheMagnitudeHeuristicOnANullInterval_LegacyArmsDoNot pins the gate
           itself); the shape pinned here holds for both the successor and the legacy text. */
        foreach (var (sql, supply) in new[]
        {
            (PgBaselineProvider.GetBaselineQuery(MetricNames.WaitStats)!, "FROM wait_stats_interval_baseline"),
            (PgBaselineProvider.GetLegacyBaselineQuery(MetricNames.WaitStats)!, "FROM wait_stats_baseline"),
        })
        {
            Assert.DoesNotContain("QUALIFY", sql, StringComparison.OrdinalIgnoreCase);

            var supplyAt = sql.IndexOf(supply, StringComparison.Ordinal);
            var lagAt = sql.IndexOf("COALESCE(LAG(total_wait_ms) OVER (ORDER BY collection_time), 0) AS prior_total_wait_ms", StringComparison.Ordinal);
            var fromCteAt = sql.IndexOf("FROM with_lag", StringComparison.Ordinal);
            var exclusionAt = sql.IndexOf("WHERE NOT (total_wait_ms = 0 AND prior_total_wait_ms > 10000", StringComparison.Ordinal);

            Assert.True(supplyAt >= 0 && lagAt > supplyAt, "LAG must run over the per-collection totals supplied by the baseline aggregate");
            Assert.True(fromCteAt > lagAt, "the aggregate must select FROM the with_lag CTE");
            Assert.True(exclusionAt > fromCteAt, "the exclusion must filter OUTSIDE the windowed CTE");
        }
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
        /* #3653: the same orderings hold for the successor text (stored interval first, LAG only where it is
           NULL, heuristic gated on NULL) and for the legacy text it falls back to. */
        /* #3653 (#3540 rule 1, readers NULL-not-0 on unknowable) - the rate arm and its WHERE, both texts, both
           halves. The arm ends at END: an interval_sec of 0 (two collections that date_trunc to the same
           second, on the LAG fallback) has no rate, and ELSE 0 rated it 0 ms/sec INTO the sample set - not
           dead text, as the census roster had it: on a PG18 rig three planted collections averaged 66.7 where
           the two rated ones say 100. And with_rate's WHERE carries interval_sec > 0 beside IS NOT NULL
           (Lite's text), because END alone hands clean a NULL ms_per_sec that NOT (NULL = 0 AND ...) lets
           through whenever another conjunct is FALSE, and COUNT(*) AS sample_count counts it (same rig: count
           3, mean 100). The WHERE sits in with_rate, BEFORE the restart LAG, so the LAG window is exactly the
           rated rows - the ordering (b) above already requires. */
        foreach (var sql in new[]
        {
            PgBaselineProvider.GetBaselineQuery(MetricNames.WaitMsPerSec)!,
            PgBaselineProvider.GetLegacyBaselineQuery(MetricNames.WaitMsPerSec)!,
        })
        {
            Assert.DoesNotContain("QUALIFY", sql, StringComparison.OrdinalIgnoreCase);

            /* The Lite-verbatim interval spine survives. */
            Assert.Contains("LAG(collection_time) OVER (ORDER BY collection_time)", sql, StringComparison.Ordinal);

            var rateFilterAt = sql.IndexOf("WHERE interval_sec IS NOT NULL AND interval_sec > 0", StringComparison.Ordinal);
            var lagAt = sql.IndexOf("COALESCE(LAG(ms_per_sec) OVER (ORDER BY collection_time), 0) AS prior_ms_per_sec", StringComparison.Ordinal);
            var fromCteAt = sql.IndexOf("FROM with_lag", StringComparison.Ordinal);
            var exclusionAt = sql.IndexOf("WHERE NOT (ms_per_sec = 0 AND prior_ms_per_sec > 100", StringComparison.Ordinal);

            Assert.True(rateFilterAt >= 0 && lagAt > rateFilterAt, "the IS NOT NULL AND > 0 filter must precede the restart LAG (DuckDB's WHERE-before-QUALIFY order)");
            Assert.True(fromCteAt > lagAt, "the aggregate must select FROM the with_lag CTE");
            Assert.True(exclusionAt > fromCteAt, "the exclusion must filter OUTSIDE the windowed CTE");

            /* The rate arm yields NULL, never 0, for a non-positive interval; the ELSE that rated it 0 is gone
               from both texts. */
            Assert.Contains("CASE WHEN interval_sec > 0 THEN total_wait_ms / interval_sec END AS ms_per_sec", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("ELSE 0", sql, StringComparison.Ordinal);
            Assert.True(rateFilterAt > sql.IndexOf("END AS ms_per_sec", StringComparison.Ordinal), "the > 0 filter is with_rate's own WHERE, under the arm it guards");
        }
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
            /* #3741: the mean rides beside the peak on every arm; on THIS arm the bar stayed on the peak alone
               (both rated collections sit at 666.7, so the two statistics coincide here — the arm's rule is
               pinned structurally, its verdict on a split window in the pair e2e below). */
            Assert.Equal(200000.0 / 300.0, fact.Metadata["current_ms_per_sec"], 0.01);
            Assert.Equal(200000.0 / 300.0, fact.Metadata["avg_ms_per_sec"], 0.01);
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

    /// <summary>
    /// #4248: IoLatency reads the RAW file_io_stats hypertable at per-file grain over the 30-day window, so its
    /// cache key is the UTC DAY, not the hour (PgBaselineProvider.IsDailyCacheMetric) — two calls hours apart on
    /// the same UTC day share the one compute, and a call on the next UTC day recomputes. Proven by counting the
    /// baseline reads Npgsql actually executes (CommandCapture), #3941's own live-pin technique.
    /// </summary>
    [Fact]
    public async Task EndToEnd_IoLatencyArm_TwoCallsHoursApartOnOneDay_ShareOneCompute_NextDayRecomputes_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live IO-arm day-cache test.");

        var ct = TestContext.Current.CancellationToken;
        const int ioServerId = TestServerId + 5; // own id — this test cleans its own rows

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

            for (var i = 0; i < 5; i++)
            {
                await InsertAsync(connection,
                    "INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, delta_reads, delta_writes, delta_stall_read_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    (long)(200 + i), historyStart.AddMinutes(5 * i), ioServerId, "IO-DAILY-CACHE",
                    10L, 0L, (long)(10 * (i + 1)));
            }

            var provider = new PgBaselineProvider(postgres);
            var analysisDay = historyStart.AddDays(7).Date; // the 30-day window's end (#4248: midnight, not the hour)

            var (morning, firstReads) = await CommandCapture.CountBaselineReadsAsync(
                () => provider.GetBaselineAsync(ioServerId, MetricNames.IoLatency, analysisDay.AddHours(1), ct));
            Assert.Equal(1, firstReads);
            Assert.True(morning.SampleCount > 0, "the seed produced no baseline — the comparison would prove nothing");

            /* Nineteen hours later (over CacheTtl's one hour), same UTC day: the #4248 pin — no second read. */
            var (afternoon, secondReads) = await CommandCapture.CountBaselineReadsAsync(
                () => provider.GetBaselineAsync(ioServerId, MetricNames.IoLatency, analysisDay.AddHours(20), ct));
            Assert.Equal(0, secondReads);
            Assert.Equal(morning.SampleCount, afternoon.SampleCount);
            Assert.Equal(morning.Median, afternoon.Median);

            /* The next UTC day is a different window end (midnight moved), so a fresh compute. */
            var (_, nextDayReads) = await CommandCapture.CountBaselineReadsAsync(
                () => provider.GetBaselineAsync(ioServerId, MetricNames.IoLatency, analysisDay.AddDays(1).AddHours(1), ct));
            Assert.Equal(1, nextDayReads);

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

    /// <summary>
    /// #3653 (A8, first slice) proven live through the PG detector: the I/O read hands the shared gate the
    /// per-file-row PEAK and MEAN, and the gate fires only when both clear. History: three Mondays at 10:00,
    /// four read-bearing rows each at exactly 2 ms (20 ms of stall over 10 reads) — 12 samples over 3 distinct
    /// days, so the Full bucket is TRUSTWORTHY and the verdict is the z path, not the absolute bar; the
    /// collapsed dispersion sits on the 2.5 ms I/O floor. Window A (the A8 shape): fifteen 2 ms rows and ONE
    /// 60 ms row — peak 60 ms is 23σ and over the 10 ms floor, the pre-#3653 fire; the window mean 5.625 ms is
    /// 1.45σ, under the 3.5 cutoff — no fact. Window B: fifteen 40 ms rows and one at 60 — peak 60 (23.2σ),
    /// mean 41.25 (15.7σ) — ONE ANOMALY_READ_LATENCY whose Value and current_latency_ms are the PEAK (no
    /// longer the average the read used to emit), avg_latency_ms the mean, both sigmas stamped.
    /// </summary>
    [Fact]
    public async Task EndToEnd_IoDetector_PeakAndMeanPair_OneHotRowStaysQuiet_SustainedWindowFires_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live IO-detector test.");

        var ct = TestContext.Current.CancellationToken;
        const int ioServerId = TestServerId + 3; // own id — this test cleans its own rows
        const string ioServerName = "io-peak-mean-e2e";

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using (var cleanup = new NpgsqlCommand(
            $"DELETE FROM file_io_stats WHERE server_id = {ioServerId}; " +
            $"DELETE FROM wait_stats WHERE server_id = {ioServerId};", connection))
        {
            await cleanup.ExecuteNonQueryAsync(ct);
        }

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var day = DateTime.UtcNow.Date.AddDays(-8);
            while (day.DayOfWeek != DayOfWeek.Monday) day = day.AddDays(-1);
            var lastHistoryMonday = DateTime.SpecifyKind(day.AddHours(10), DateTimeKind.Unspecified);
            var analysisTime = lastHistoryMonday.AddDays(7); // a Monday 10:00, at least a day in the past

            const string insertIo =
                "INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, delta_reads, delta_writes, delta_stall_read_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)";

            var id = 500L;
            foreach (var weeksBack in new[] { 2, 1, 0 })
            {
                var monday = lastHistoryMonday.AddDays(-7 * weeksBack);
                for (var i = 0; i < 4; i++)
                    await InsertAsync(connection, insertIo, id++, monday.AddMinutes(5 * i), ioServerId, ioServerName, 10L, 0L, 20L);
            }

            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);

            var provider = new PgBaselineProvider(postgres);
            var baseline = await provider.GetBaselineAsync(ioServerId, MetricNames.IoLatency, analysisTime);
            Assert.Equal(BaselineTier.Full, baseline.Tier);
            Assert.Equal(12L, baseline.SampleCount);
            Assert.Equal(3L, baseline.DistinctDays);
            Assert.True(baseline.IsTrustworthy, "three Mondays clear the Full-tier day floor: the z path, not the bar");
            Assert.Equal(2.0, baseline.Mean, precision: 6);
            Assert.Equal(2.5, baseline.EffectiveRobustSigma, precision: 6); // MAD 0 → the I/O absolute floor

            /* Canary for the HasBaselineData gate — OUTSIDE the analysis window so the wait detector's own
               window read stays empty and it emits nothing. */
            await InsertAsync(connection,
                "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                600L, lastHistoryMonday, ioServerId, ioServerName, TestWaitType, 1L, 100L);

            var detector = new PgAnomalyDetector(postgres, provider);
            var context = new AnalysisContext
            {
                ServerId = ioServerId,
                ServerName = ioServerName,
                TimeRangeStart = analysisTime,
                TimeRangeEnd = analysisTime.AddMinutes(90),
                ServerUtcOffset = TimeSpan.Zero
            };

            /* Window A: one hot file row in an otherwise-baseline window. */
            for (var i = 0; i < 16; i++)
                await InsertAsync(connection, insertIo, id++, analysisTime.AddMinutes(5 * (i + 1)), ioServerId, ioServerName, 10L, 0L, i == 7 ? 600L : 20L);

            var quiet = await detector.DetectAnomaliesAsync(context);
            Assert.DoesNotContain(quiet, f => f.Key == "ANOMALY_READ_LATENCY");
            Assert.Empty(quiet);

            /* Window B: the whole window ran high. */
            await using (var clearWindow = new NpgsqlCommand(
                $"DELETE FROM file_io_stats WHERE server_id = {ioServerId} AND collection_time > $1;", connection))
            {
                clearWindow.Parameters.AddWithValue(analysisTime);
                await clearWindow.ExecuteNonQueryAsync(ct);
            }
            for (var i = 0; i < 16; i++)
                await InsertAsync(connection, insertIo, id++, analysisTime.AddMinutes(5 * (i + 1)), ioServerId, ioServerName, 10L, 0L, i == 7 ? 600L : 400L);

            var sustained = await detector.DetectAnomaliesAsync(context);
            var fact = Assert.Single(sustained);
            Assert.Equal("ANOMALY_READ_LATENCY", fact.Key);
            Assert.Equal(60.0, fact.Value, 0.001);                                  // the PEAK, not the window average
            Assert.Equal(60.0, fact.Metadata["current_latency_ms"], 0.001);
            /* #3653 A8 option B: the fact reports the WORST TILE, the target-local hour holding the 60 ms row. Its mean is
               that hour's n rows (n - 1 at 40 ms plus the 60 ms row), not the whole window's 41.25. The whole window's 16
               rows ride in window_samples_total. */
            var tileStart = new DateTime((long)fact.Metadata["tile_start_ticks"]);
            var rowTimes = Enumerable.Range(0, 16).Select(i => analysisTime.AddMinutes(5 * (i + 1))).ToList();
            Assert.InRange(rowTimes[7], tileStart, tileStart.AddHours(1).AddTicks(-1));   // the worst hour holds the hot row
            var tileRows = rowTimes.Count(t => t >= tileStart && t < tileStart.AddHours(1));
            var tileMean = ((tileRows - 1) * 40.0 + 60.0) / tileRows;
            Assert.Equal(tileMean, fact.Metadata["avg_latency_ms"], 0.001);
            Assert.Equal(16.0, fact.Metadata["window_samples_total"]);
            Assert.Equal(60.0, fact.Metadata["window_peak"], 0.001);
            Assert.Equal(0.0, fact.Metadata["baseline_low_quality"]);               // the z path
            Assert.Equal((60.0 - 2.0) / 2.5, fact.Metadata["deviation_sigma"], 0.001);         // 23.2σ, the peak's
            Assert.Equal((tileMean - 2.0) / 2.5, fact.Metadata["mean_deviation_sigma"], 0.001); // the worst hour's mean
            Assert.Equal(AnomalyThresholds.ModifiedZThreshold, fact.Metadata["fire_threshold"]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                using (var command = new NpgsqlCommand(
                    $"DELETE FROM file_io_stats WHERE server_id = {ioServerId}; " +
                    $"DELETE FROM wait_stats WHERE server_id = {ioServerId};", cleanup))
                {
                    await command.ExecuteNonQueryAsync(cleanupCt);
                }
                await DropBaselineFallbackViewsAsync(cleanup, cleanupCt);
            });
        }
    }

    /// <summary>
    /// #3741 proven live through the PG detector: the wait-profile read hands the detector the per-collection
    /// PEAK and MEAN ms/sec, and the trusted robust arm — now the shared gate's pair call — fires only when
    /// both clear the heavy-tail 5.0 cutoff. History: three Mondays at 10:00, twelve collections each at
    /// 5-minute spacing, one wait type, totals cycling 30000 / 60000 / 90000 ms so the rates cycle 100 / 200 /
    /// 300 ms/sec (the interval is the LAG — no stored interval, the pre-V127 shape the baseline read still
    /// serves). The first collection of the first Monday has no prior and is dropped; the first of each later
    /// Monday rates against a week-long LAG and lands near zero. 35 samples over 3 distinct days: the Full
    /// bucket is TRUSTWORTHY, median 200, MAD 100 (percentile_cont over the 35, hand-checked: 2 near-zero, 9 at
    /// 100, 12 at 200, 12 at 300 — the 18th of both sorted sets), EffectiveRobustSigma 148.26.
    /// Window A (the A8 shape #3724 removed everywhere else): seventeen collections at 5-minute spacing, the
    /// first unrated (no in-window prior), fifteen at the 60000 ms median rate and ONE at 960000 ms — peak
    /// 3200 ms/sec is 20.2 robust σ and over the 250 ms/sec floor, the pre-#3741 fire; the window mean 387.5
    /// ms/sec is 1.26σ, under 5.0 — no fact. Window B: fifteen at 450000 ms (1500 ms/sec) and one at 960000 —
    /// peak 3200 (20.2σ), mean 1606.25 (9.5σ) — ONE ANOMALY_WAIT_PROFILE, is_new 0, current_ms_per_sec the
    /// PEAK, avg_ms_per_sec the mean, modified_z and mean_modified_z both over the cutoff, and Value the
    /// window's total wait ms including the unrated first collection (the SUM ranges over every collection;
    /// only the rates skip the unrated one).
    /// </summary>
    [Fact]
    public async Task EndToEnd_WaitProfileDetector_PeakAndMeanPair_OneHotCollectionStaysQuiet_SustainedWindowFires_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live wait-profile pair test.");

        var ct = TestContext.Current.CancellationToken;
        const int waitServerId = TestServerId + 4; // own id — this test cleans its own rows
        const string waitServerName = "wait-peak-mean-e2e";
        const string insertWait =
            "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)";

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using (var cleanup = new NpgsqlCommand($"DELETE FROM wait_stats WHERE server_id = {waitServerId};", connection))
        {
            await cleanup.ExecuteNonQueryAsync(ct);
        }

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var day = DateTime.UtcNow.Date.AddDays(-8);
            while (day.DayOfWeek != DayOfWeek.Monday) day = day.AddDays(-1);
            var lastHistoryMonday = DateTime.SpecifyKind(day.AddHours(10), DateTimeKind.Unspecified);
            var analysisTime = lastHistoryMonday.AddDays(7); // a Monday 10:00, at least a day in the past

            var id = 700L;
            foreach (var weeksBack in new[] { 2, 1, 0 })
            {
                var monday = lastHistoryMonday.AddDays(-7 * weeksBack);
                for (var i = 0; i < 12; i++)
                {
                    var totalMs = (i % 3) switch { 0 => 30000L, 1 => 60000L, _ => 90000L };
                    await InsertAsync(connection, insertWait, id++, monday.AddMinutes(5 * i), waitServerId, waitServerName, TestWaitType, 10L, totalMs);
                }
            }

            await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);

            var provider = new PgBaselineProvider(postgres);
            var baseline = await provider.GetBaselineAsync(waitServerId, MetricNames.WaitMsPerSec, analysisTime);
            Assert.Equal(BaselineTier.Full, baseline.Tier);
            Assert.Equal(35L, baseline.SampleCount);
            Assert.Equal(3L, baseline.DistinctDays);
            Assert.True(baseline.IsTrustworthy, "three Mondays clear the Full-tier day floor: the gate's z path, not the is_new bar");
            Assert.Equal(200.0, baseline.Median, 0.001);
            Assert.Equal(100.0, baseline.Mad, 0.001);
            Assert.True(baseline.EffectiveRobustSigma > 0, "the robust frame — the arm the pair gate now owns");
            var robustSigma = baseline.EffectiveRobustSigma;
            Assert.Equal(100.0 / 0.6745, robustSigma, 0.01);

            var detector = new PgAnomalyDetector(postgres, provider);
            var context = new AnalysisContext
            {
                ServerId = waitServerId,
                ServerName = waitServerName,
                TimeRangeStart = analysisTime,
                TimeRangeEnd = analysisTime.AddMinutes(90),
                ServerUtcOffset = TimeSpan.Zero
            };

            /* Window A: one hot collection in an otherwise-median window. i = 0 is the unrated first
               collection (its LAG has no in-window prior); i = 1..16 rate against 300 s. */
            for (var i = 0; i < 17; i++)
                await InsertAsync(connection, insertWait, id++, analysisTime.AddMinutes(5 * (i + 1)), waitServerId, waitServerName, TestWaitType, 50L, i == 8 ? 960000L : 60000L);

            var quiet = await detector.DetectAnomaliesAsync(context);
            Assert.DoesNotContain(quiet, f => f.Key == "ANOMALY_WAIT_PROFILE");
            Assert.Empty(quiet);

            /* The A8 arithmetic, stated so a reader can see the pre-#3741 fire this window used to be: the
               peak alone cleared both halves of the old inline gate. */
            var peakZ = (3200.0 - baseline.Median) / robustSigma;
            var meanAZ = (387.5 - baseline.Median) / robustSigma;
            Assert.True(peakZ >= AnomalyThresholds.HeavyTailModifiedZThreshold && 3200.0 >= AnomalyThresholds.WaitProfileFallbackMsPerSec, "red-first: the peak-only gate fired on window A");
            Assert.True(meanAZ < AnomalyThresholds.HeavyTailModifiedZThreshold, "the window mean is what keeps window A quiet");

            /* Window B: the whole window ran heavy. */
            await using (var clearWindow = new NpgsqlCommand(
                $"DELETE FROM wait_stats WHERE server_id = {waitServerId} AND collection_time > $1;", connection))
            {
                clearWindow.Parameters.AddWithValue(analysisTime);
                await clearWindow.ExecuteNonQueryAsync(ct);
            }
            for (var i = 0; i < 17; i++)
                await InsertAsync(connection, insertWait, id++, analysisTime.AddMinutes(5 * (i + 1)), waitServerId, waitServerName, TestWaitType, 50L, i == 8 ? 960000L : 450000L);

            var sustained = await detector.DetectAnomaliesAsync(context);
            var fact = Assert.Single(sustained);
            Assert.Equal("ANOMALY_WAIT_PROFILE", fact.Key);
            Assert.Equal(0.0, fact.Metadata["is_new"]);                                     // the trusted robust arm
            Assert.Equal(3200.0, fact.Metadata["current_ms_per_sec"], 0.001);                // the PEAK
            /* #3653 A8 option B: the worst tile's mean, the hour holding the 3,200 ms/s collection: its n rated
               collections, n - 1 at 1,500 plus the spike. It is not the whole window's (15 × 1500 + 3200) / 16. */
            var tileStart = new DateTime((long)fact.Metadata["tile_start_ticks"]);
            var collectionTimes = Enumerable.Range(0, 17).Select(i => analysisTime.AddMinutes(5 * (i + 1))).ToList();
            Assert.InRange(collectionTimes[8], tileStart, tileStart.AddHours(1).AddTicks(-1));   // the worst hour holds the spike
            var tileCollections = collectionTimes.Skip(1).Count(t => t >= tileStart && t < tileStart.AddHours(1)); // the first is unrated
            var tileMeanRate = ((tileCollections - 1) * 1500.0 + 3200.0) / tileCollections;
            Assert.Equal(tileMeanRate, fact.Metadata["avg_ms_per_sec"], 0.001);
            Assert.Equal(16.0, fact.Metadata["window_samples_total"]);
            Assert.Equal(16 * 450000.0 + 960000.0, fact.Value, 0.001);                        // every collection's total, the unrated first included
            Assert.Equal(3200.0 / baseline.Mean, fact.Metadata["ratio"], 0.001);             // the ratio stays the peak's
            Assert.Equal(peakZ, fact.Metadata["modified_z"], 0.001);                          // uncapped, the scorer's anchor
            Assert.Equal((tileMeanRate - baseline.Median) / robustSigma, fact.Metadata["mean_modified_z"], 0.001);
            Assert.True(fact.Metadata["mean_modified_z"] >= AnomalyThresholds.HeavyTailModifiedZThreshold, "a fired fact's mean cleared the same cutoff");
            Assert.True(fact.Metadata["modified_z"] >= fact.Metadata["mean_modified_z"], "the reported deviation is the peak's; the mean's is the smaller one");
            Assert.Equal(16 * 450000.0 + 960000.0, fact.Metadata[$"contrib_{TestWaitType}"], 0.001); // the one type carries the whole total

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                using (var command = new NpgsqlCommand($"DELETE FROM wait_stats WHERE server_id = {waitServerId};", cleanup))
                {
                    await command.ExecuteNonQueryAsync(cleanupCt);
                }
                await DropBaselineFallbackViewsAsync(cleanup, cleanupCt);
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
