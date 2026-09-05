/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The baseline supply tier (#1757). The bug these guard is not a slow query — it is that tiered retention
/// shrank raw to 4 days while the baseline asks for 30, so on every tiered store the anomaly thresholds were
/// being computed from a fraction of their intended history with no error raised. The timeout that got it
/// reported was the loud phase a store passes through on its way to that silence.
///
/// <para>What is pinned here is the INVARIANT — "same statistic, different supply" — not the SQL shape. The
/// shape assertions live with the QUALIFY-rewrite tests; these check the relationships that make the swap
/// legitimate and that a later change cannot quietly break.</para>
/// </summary>
public class BaselineSupplyTests
{
    /// <summary>
    /// THE RELATION THAT MAKES #1757 UNABLE TO RETURN. If baseline-tier retention ever drops below the
    /// baseline window, the supply is shorter than the question again and the degradation is silent — exactly
    /// the failure mode being fixed. Storage cannot reference Analysis, so this cross-assembly relation has
    /// no compile-time home; it lives here because Darling.Tests references both.
    /// </summary>
    [Fact]
    public void BaselineRetention_CoversTheBaselineWindow_OrNumber1757Returns()
    {
        Assert.True(
            TimescaleSupport.BaselineRetentionSpan >= TimeSpan.FromDays(BaselineMath.BaselineWindowDays),
            $"baseline-tier retention ({TimescaleSupport.BaselineRetentionSpan.TotalDays}d) must cover the " +
            $"baseline window ({BaselineMath.BaselineWindowDays}d) — below it, baselines silently degrade");

        /* The string and the TimeSpan are two statements of one fact; drift between them would arm a policy
           at a horizon the pin above never checked. */
        Assert.Equal(
            TimescaleSupport.BaselineRetentionInterval,
            $"{TimescaleSupport.BaselineRetentionSpan.TotalDays:0} days");
    }

    /// <summary>
    /// Every baseline aggregate groups by time_bucket AND collection_time. The hourly bucket is purely a
    /// partitioning/retention key; collection_time carries the GRAIN. Dropping the second group column is the
    /// tempting "simplification" that would silently change the unit of observation from one collection
    /// snapshot to one hour — a different statistic at a different scale, whose STDDEV_SAMP cannot be
    /// reconstructed at all because the hourly tier stores no sum-of-squares.
    /// </summary>
    [Fact]
    public void EveryBaselineAggregate_PreservesCollectionGrain_NotJustTheHourlyBucket()
    {
        /* Seven since #2007 retired the unread CPU/IO pair (their arms read the raw hypertables);
           RetiredBaselineAggregateTests pins the retirement half of that count. */
        Assert.Equal(7, TimescaleSupport.BaselineAggregates.Length);

        foreach (var (createSql, view) in TimescaleSupport.BaselineAggregates)
        {
            Assert.Contains("time_bucket('1 hour', collection_time) AS bucket", createSql, StringComparison.Ordinal);
            Assert.Contains("GROUP BY server_id, bucket, collection_time", createSql, StringComparison.Ordinal);
            Assert.Contains("collect." + view, createSql, StringComparison.Ordinal);
            /* Real-time aggregation is opted INTO explicitly. TimescaleDB 2.13+ disables it by default, and
               these views are created WITH NO DATA behind a policy that only refreshes the trailing 3 days —
               without both this flag and the startup backfill they would answer a 30-day question with 3 days
               of supply, which is worse than the raw horizon the tier exists to escape. */
            Assert.Contains("timescaledb.continuous, timescaledb.materialized_only = false", createSql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// THE PRECONDITION FOR PER-TABLE (RATHER THAN PER-FAMILY) AGGREGATES: two families sharing a source must
    /// share its row-level filters, or one family's filter baked into the shared aggregate silently poisons
    /// the other's supply. Two sources are shared today — wait_stats (WaitStats + WaitMsPerSec) and
    /// blocked_process_reports (Blocking + BlockingPerMinute) — and this asserts the filters still match.
    /// A future family-specific filter on a shared source must split the aggregate, not narrow it.
    /// </summary>
    [Fact]
    public void SharedSourceFamilies_ShareTheirRowLevelFilters_OrTheAggregateCannotServeBoth()
    {
        /* wait_stats: both families filter on non-negative deltas and nothing else. */
        var waitStats = PgBaselineProvider.GetBaselineQuery(MetricNames.WaitStats)!;
        var waitRate = PgBaselineProvider.GetBaselineQuery(MetricNames.WaitMsPerSec)!;
        Assert.Contains("FROM wait_stats_baseline", waitStats, StringComparison.Ordinal);
        Assert.Contains("FROM wait_stats_baseline", waitRate, StringComparison.Ordinal);
        Assert.Contains("delta_wait_time_ms >= 0", TimescaleSupport.CreateWaitStatsBaselineSql, StringComparison.Ordinal);

        /* blocked_process_reports: neither family filters, so the shared aggregate carries no WHERE at all.
           A WHERE appearing here later means one family narrowed the other's supply. */
        var blocking = PgBaselineProvider.GetBaselineQuery(MetricNames.Blocking)!;
        var blockingPerMinute = PgBaselineProvider.GetBaselineQuery(MetricNames.BlockingPerMinute)!;
        Assert.Contains("FROM blocked_process_baseline", blocking, StringComparison.Ordinal);
        Assert.Contains("FROM blocked_process_baseline", blockingPerMinute, StringComparison.Ordinal);
        Assert.DoesNotContain("WHERE", TimescaleSupport.CreateBlockedProcessBaselineSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// THE MUTATION CHECK the whole change exists for: no baseline family may read a raw <c>v_*</c>
    /// passthrough. Point one back at raw — the state dev was in — and this goes red. It scans the live SQL
    /// only: the QUALIFY-rewrite doc comments quote Lite's DuckDB originals verbatim and legitimately name
    /// the raw views, so comment lines are stripped before the check rather than the check being weakened.
    /// </summary>
    [Fact]
    public void NoBaselineFamily_StillReadsRawHistory()
    {
        var families = new[]
        {
            MetricNames.Cpu, MetricNames.BatchRequests, MetricNames.WaitStats, MetricNames.SessionCount,
            MetricNames.QueryDuration, MetricNames.IoLatency, MetricNames.Blocking, MetricNames.Deadlock,
            MetricNames.Memory, MetricNames.WaitMsPerSec, MetricNames.BlockingPerMinute,
        };

        foreach (var family in families)
        {
            var sql = PgBaselineProvider.GetBaselineQuery(family);
            Assert.NotNull(sql);

            var live = string.Join('\n', sql!
                .Split('\n')
                .Where(l => !l.TrimStart().StartsWith("--", StringComparison.Ordinal)));

            Assert.False(
                live.Contains("FROM v_", StringComparison.Ordinal),
                $"{family} still reads a raw passthrough — on a tiered store that is 4 days of supply for a " +
                $"{BaselineMath.BaselineWindowDays}-day window, which is #1757");
        }
    }

    /// <summary>
    /// Nine families are served by the baseline aggregates and nothing else — a family pointed at a view
    /// that no longer exists would fail at runtime against a real store, which no unit test would catch.
    /// <para>#1743 follow-up: CPU and I/O latency are the two deliberate EXCEPTIONS — they read their RAW
    /// hypertables (at Lite's grain, so the robust scaffold applies; their retired sum/sumsq rollups could
    /// not produce a median). That is safe from #1757 only because those two collectors carry their own
    /// 30-day service-side retention, which <see cref="RawBaselineFamilies_RetentionCoversTheWindow"/>
    /// pins as the load-bearing invariant.</para>
    /// </summary>
    [Fact]
    public void EveryFamily_ReadsItsIntendedSupply()
    {
        var views = TimescaleSupport.BaselineAggregates.Select(a => a.View).ToArray();
        var aggregateFamilies = new[]
        {
            MetricNames.BatchRequests, MetricNames.WaitStats, MetricNames.SessionCount,
            MetricNames.QueryDuration, MetricNames.Blocking, MetricNames.Deadlock,
            MetricNames.Memory, MetricNames.WaitMsPerSec, MetricNames.BlockingPerMinute,
        };

        foreach (var family in aggregateFamilies)
        {
            var sql = PgBaselineProvider.GetBaselineQuery(family)!;
            Assert.True(
                views.Any(v => sql.Contains("FROM " + v, StringComparison.Ordinal)),
                $"{family} does not read any known baseline aggregate");
        }

        Assert.Contains("FROM cpu_utilization_stats", PgBaselineProvider.GetBaselineQuery(MetricNames.Cpu)!, StringComparison.Ordinal);
        Assert.Contains("FROM file_io_stats", PgBaselineProvider.GetBaselineQuery(MetricNames.IoLatency)!, StringComparison.Ordinal);
    }

    /// <summary>
    /// The invariant that makes the CPU/I-O raw reads safe: both collectors retain at least the full
    /// baseline window. Drop either below <see cref="BaselineMath.BaselineWindowDays"/> and the family
    /// silently regresses to #1757's short-supply shape — this pin is what makes that a red build
    /// instead of a quiet baseline degradation.
    /// </summary>
    [Fact]
    public void RawBaselineFamilies_RetentionCoversTheWindow()
    {
        Assert.True(
            CollectorScheduleDefaults.All["cpu_utilization"].RetentionDays >= BaselineMath.BaselineWindowDays,
            "cpu_utilization retention no longer covers the baseline window");
        Assert.True(
            CollectorScheduleDefaults.All["file_io_stats"].RetentionDays >= BaselineMath.BaselineWindowDays,
            "file_io_stats retention no longer covers the baseline window");
    }

    /// <summary>
    /// The RUNTIME half of the same invariant (review-caught on the follow-up PR): retention for
    /// these collectors is user-editable, so a defaults pin alone leaves a deployed store able to
    /// silently shorten the CPU/I-O baseline supply. DarlingRetention floors the purge horizon for
    /// exactly the raw-reading baseline families at the baseline window — this pins the set's
    /// membership to the provider's raw-reading arms so neither side can drift alone.
    /// </summary>
    [Fact]
    public void BaselineServingRawCollectors_MatchTheRawReadingArms()
    {
        Assert.Equal(2, DarlingRetention.BaselineServingRawCollectors.Count);
        Assert.Contains("cpu_utilization", DarlingRetention.BaselineServingRawCollectors);
        Assert.Contains("file_io_stats", DarlingRetention.BaselineServingRawCollectors);
    }

    /// <summary>
    /// <c>refresh_continuous_aggregate</c>'s window bounds are declared <c>"any"</c>. An untyped literal or an
    /// uncast parameter leaves PostgreSQL with no type to resolve the polymorphic argument against, so the
    /// casts are load-bearing rather than decoration — and the bound parameter keeps a timestamp out of string
    /// interpolation. The forced form is the 4-argument 2.18+ signature; the tunables that the published API
    /// page shows as named arguments are NOT parameters of this procedure, so their absence here is correct.
    /// </summary>
    [Fact]
    public void RefreshSql_BindsAndCastsItsBounds_BecauseTheParametersArePolymorphic()
    {
        var plain = TimescaleSupport.RefreshContinuousAggregateSql(TimescaleSupport.PerfmonBaselineView);
        Assert.Contains("$1::timestamp", plain, StringComparison.Ordinal);
        Assert.Contains("NULL::timestamp", plain, StringComparison.Ordinal);
        Assert.Contains("'collect.perfmon_baseline'::regclass", plain, StringComparison.Ordinal);
        Assert.DoesNotContain("buckets_per_batch", plain, StringComparison.Ordinal);

        /* force is the 4th positional argument, and only ever set deliberately. */
        Assert.EndsWith("NULL::timestamp)", plain, StringComparison.Ordinal);
        var forced = TimescaleSupport.RefreshContinuousAggregateSql(TimescaleSupport.PerfmonBaselineView, force: true);
        Assert.EndsWith("NULL::timestamp, true)", forced, StringComparison.Ordinal);
    }

    /// <summary>
    /// THE BACKFILL MUST NOT BLOCK STARTUP. It is a bulk materialization whose cost scales with whatever
    /// history the store already had, and the composer tuning, the delta re-seed and the collection loop are
    /// all sequenced after the TimescaleDB block — so an <c>await</c> at the launch site takes a restarted
    /// service dark for the backfill's whole duration, precisely when an operator is most likely restarting
    /// it. That regression compiles, passes every functional test, and is invisible except on a big store,
    /// which is why it is pinned at the source. It must still be AWAITED at the shutdown drain: launched and
    /// never observed is a different bug.
    /// </summary>
    [Fact]
    public void BaselineBackfill_IsLaunchedNotAwaited_ButStillDrainedOnShutdown()
    {
        var worker = ReadWorkerSource();

        Assert.Contains("baselineBackfill = RunBaselineBackfillAsync(", worker, StringComparison.Ordinal);
        Assert.DoesNotContain("await RunBaselineBackfillAsync(", worker, StringComparison.Ordinal);
        Assert.Contains("await baselineBackfill;", worker, StringComparison.Ordinal);
    }

    /// <summary>
    /// THE PLAIN-POSTGRESQL HALF OF THE SAME STATISTIC. Darling runs without TimescaleDB whenever the
    /// extension is absent or its setup fails, and the provider reads the baseline relations BY NAME — so
    /// without these views every family throws, <c>ComputeBaselinesAsync</c> swallows it, and the anomaly
    /// baselines silently return empty. The fallback is DERIVED from the aggregate's own select body rather
    /// than written out again, which is what makes "same statistic, different supply" true by construction;
    /// these assertions pin that the derivation stays faithful and drops every TimescaleDB-only construct.
    /// </summary>
    [Fact]
    public void EveryBaselineAggregate_DerivesAPlainPostgresFallbackView_ComputingTheSameStatistic()
    {
        foreach (var (createSql, view) in TimescaleSupport.BaselineAggregates)
        {
            var fallback = TimescaleSupport.CreateBaselineFallbackViewSql(view, createSql);

            Assert.StartsWith($"CREATE OR REPLACE VIEW collect.{view} AS", fallback, StringComparison.Ordinal);

            /* time_bucket is the ONE TimescaleDB-only construct in the body; for a 1-hour bucket date_trunc is
               the same value. Nothing else may survive, or the view cannot be created without the extension. */
            Assert.DoesNotContain("time_bucket", fallback, StringComparison.Ordinal);
            Assert.DoesNotContain("timescaledb", fallback, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("WITH NO DATA", fallback, StringComparison.Ordinal);
            Assert.DoesNotContain("MATERIALIZED", fallback, StringComparison.Ordinal);
            Assert.Contains("date_trunc('hour', collection_time) AS bucket", fallback, StringComparison.Ordinal);

            /* The grain and the source survive the derivation — a fallback that grouped differently, or read a
               different table, would be a different number wearing the same column names. */
            Assert.Contains("GROUP BY server_id, bucket, collection_time", fallback, StringComparison.Ordinal);
            Assert.Contains("FROM collect." + TimescaleSupport.SourceTableFor(view), fallback, StringComparison.Ordinal);
        }

        /* Row-level filters are part of the statistic, so they must survive too. */
        var waits = TimescaleSupport.CreateBaselineFallbackViewSql(
            TimescaleSupport.WaitStatsBaselineView, TimescaleSupport.CreateWaitStatsBaselineSql);
        Assert.Contains("delta_wait_time_ms >= 0", waits, StringComparison.Ordinal);
    }

    /// <summary>
    /// A continuous aggregate IS a <c>relkind='v'</c> view, so an unguarded <c>DROP VIEW</c> by these names
    /// would silently destroy a materialized tier rather than clearing a fallback. The guard is the whole
    /// point of the statement, and it has to survive without TimescaleDB installed too — reaching
    /// <c>timescaledb_information</c> unconditionally raises 42P01 on a store that never created the
    /// extension, which is exactly the store the fallback exists for.
    /// </summary>
    [Fact]
    public void DroppingAFallbackView_RefusesToTouchAContinuousAggregate_AndSurvivesWithoutTheExtension()
    {
        var sql = TimescaleSupport.DropBaselineFallbackViewSql(TimescaleSupport.WaitStatsBaselineView);

        Assert.Contains("continuous_aggregates", sql, StringComparison.Ordinal);
        Assert.Contains("relkind = 'v'", sql, StringComparison.Ordinal);
        /* Probed, not assumed: to_regclass yields NULL instead of erroring when the extension is absent. */
        Assert.Contains("to_regclass('timescaledb_information.continuous_aggregates') IS NOT NULL", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// MUTATION CHECK for the clamp direction — the defect class where every piece is right and one of them
    /// faces the wrong way. Clamping the COVERAGE side (<c>LEAST</c> over coverage and the window) instead of
    /// the NEED side inverts the gate: an empty aggregate collapses to <c>now - window</c>, the skip test
    /// becomes "now - window &lt;= source oldest", and that is unconditionally true on any store whose raw
    /// retention is shorter than the window — so the backfill skips precisely the tiered stores it exists for
    /// and fires only where it is not needed. Restore <c>LEAST</c> and this goes red.
    /// </summary>
    [Fact]
    public void BackfillGate_ClampsTheNeed_NotTheCoverage_OrItSkipsEveryStoreItIsFor()
    {
        var sql = TimescaleSupport.BaselineBackfillProbeSql(
            TimescaleSupport.QueryStatsBaselineView, TimescaleSupport.SourceTableFor(TimescaleSupport.QueryStatsBaselineView));

        /* The clamp is GREATEST, and it is applied to the NEED — source-oldest against the retention horizon. */
        Assert.Contains("GREATEST(", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LEAST(", sql, StringComparison.Ordinal);

        /* And the coverage side is read RAW, never clamped: it is the thing being measured, not a bound. */
        Assert.Contains($"(SELECT min(bucket) FROM collect.{TimescaleSupport.QueryStatsBaselineView}) AS coverage_oldest", sql, StringComparison.Ordinal);

        /* The need is bounded by this tier's own retention, and the bound arrives as a PARAMETER. It used to
           be spelled `now()::timestamp - INTERVAL '35 days'` here, which is LOCALTIMESTAMP arithmetic: the
           horizon rendered in the store session's zone, clamped by GREATEST against a naive-UTC
           min(collection_time). The caller passes BaselineRetentionSpan off the service clock instead, so
           what this can still assert about the SQL is that the local clock is gone and the bound is bound.
           The horizon's VALUE stays pinned by BaselineRetention_CoversTheBaselineWindow_OrNumber1757Returns
           (which holds the string and the TimeSpan equal), and the absence of a bare clock in any store
           predicate by StoreSqlClockDisciplineTests. */
        Assert.Contains("$1)) AS need_from", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now()", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("LOCALTIMESTAMP", sql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// MUTATION CHECK for the plain-PostgreSQL / partial-build gap. The provider reads these relations by
    /// name and its catch swallows the 42P01, so a missing one is a family that silently returns nothing —
    /// no error surfaced, thresholds worthless, which is #1757's own failure shape delivered to a store that
    /// never had #1757. Remove the fallback call from the worker and this goes red.
    ///
    /// <para>The UNGATED part is the half worth pinning. Gating it on "TimescaleDB unavailable" covers the
    /// plain-PostgreSQL store and leaves the harder case broken: the extension present, the sweep's
    /// per-aggregate failure isolation having left one aggregate unbuilt, and exactly one family dead on an
    /// otherwise healthy store.</para>
    /// </summary>
    [Fact]
    public void BaselineRelations_AreGuaranteedToExist_AndTheGuaranteeIsNotGatedOnTimescaleDb()
    {
        var worker = ReadWorkerSource();

        Assert.Contains("TimescaleSupport.EnsureBaselineFallbackViewsAsync(", worker, StringComparison.Ordinal);

        /* Not inside `if (_timescaleAvailable)` and not inside its negation — the call site must be reachable
           on every path. Checked by position: it has to sit AFTER the TimescaleDB block's catch, which is the
           only place all three gap routes have converged. */
        var callAt = worker.IndexOf("TimescaleSupport.EnsureBaselineFallbackViewsAsync(", StringComparison.Ordinal);
        var plainModeAt = worker.IndexOf("continuing in plain-PostgreSQL mode", StringComparison.Ordinal);
        Assert.True(plainModeAt > 0 && callAt > plainModeAt,
            "the fallback must run after the TimescaleDB block, on every path — not inside it");

        var gatedAt = worker.IndexOf("if (!_timescaleAvailable)", StringComparison.Ordinal);
        Assert.True(gatedAt < 0 || gatedAt > callAt,
            "the fallback must NOT be gated on !_timescaleAvailable — that leaves a partially-built store with one dead family");

        /* The probe is what makes running it everywhere safe: a continuous aggregate is also a relkind='v'
           view, so an unconditional CREATE OR REPLACE VIEW by these names would destroy a materialization. */
        Assert.Contains("to_regclass", TimescaleSupport.BaselineRelationExistsSql(TimescaleSupport.PerfmonBaselineView), StringComparison.Ordinal);
    }

    private static string ReadWorkerSource([CallerFilePath] string thisFile = "")
    {
        /* Locate the repo from this file, the AlertFiringLogTests idiom - no build-output copying. */
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
