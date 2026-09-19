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
        /* wait_stats: both families filter on non-negative deltas and on the collector's knowability verdict
           (#3653: sample_interval_seconds IS DISTINCT FROM 0), and nothing else. Both the successor supply
           and the legacy one they still fall back to are shared the same way. */
        var waitStats = PgBaselineProvider.GetBaselineQuery(MetricNames.WaitStats)!;
        var waitRate = PgBaselineProvider.GetBaselineQuery(MetricNames.WaitMsPerSec)!;
        Assert.Contains("FROM wait_stats_interval_baseline", waitStats, StringComparison.Ordinal);
        Assert.Contains("FROM wait_stats_interval_baseline", waitRate, StringComparison.Ordinal);
        Assert.Contains("delta_wait_time_ms >= 0", TimescaleSupport.CreateWaitStatsIntervalBaselineSql, StringComparison.Ordinal);
        Assert.Contains("sample_interval_seconds IS DISTINCT FROM 0", TimescaleSupport.CreateWaitStatsIntervalBaselineSql, StringComparison.Ordinal);

        var legacyWaitStats = PgBaselineProvider.GetLegacyBaselineQuery(MetricNames.WaitStats)!;
        var legacyWaitRate = PgBaselineProvider.GetLegacyBaselineQuery(MetricNames.WaitMsPerSec)!;
        Assert.Contains("FROM wait_stats_baseline", legacyWaitStats, StringComparison.Ordinal);
        Assert.Contains("FROM wait_stats_baseline", legacyWaitRate, StringComparison.Ordinal);
        Assert.Contains("delta_wait_time_ms >= 0", TimescaleSupport.LegacyCreateWaitStatsBaselineSql, StringComparison.Ordinal);
        Assert.DoesNotContain("sample_interval_seconds", TimescaleSupport.LegacyCreateWaitStatsBaselineSql, StringComparison.Ordinal);

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
    /// membership to the provider's raw-reading arms so neither side can drift alone. Since #3691 the
    /// PostgreSQL-target provider's raw sources are members too (the #1757 shape, PG edition): they have
    /// no rollup at all, so their user-editable schedule default was the only thing covering the window.
    /// Four v1 sources, plus the three v2 arms' tables (<c>pg_replication_stats</c> since lane 12; <c>pg_io_stats</c>
    /// and <c>pg_write_stats</c> since the #3691 between-waves batch, lanes 11 and 15 having reported theirs as out
    /// of their files; <c>pg_blocking_edges</c> since lane 17, whose <c>pg_blocked_sessions</c> arm reads it;
    /// <c>pg_wait_sampling</c> since lane 24, whose <c>pg_sampled_wait_ms_per_sec</c> arm reads it). The
    /// count is the SQL Server pair plus whatever the provider reads — the PostgreSQL half is derived
    /// (<see cref="PgTargetBaselineSources"/>), so a new arm moves this pin by itself. Membership is by COLLECTOR
    /// name (the purge resolves a schedule row, not a table), so each derived table is mapped to its collector through
    /// the catalog before the lookup — <c>pg_blocking_edges</c> is the one member whose two names differ.
    /// </summary>
    [Fact]
    public void BaselineServingRawCollectors_MatchTheRawReadingArms()
    {
        var pgSources = PgTargetBaselineSources();
        Assert.Equal(9, pgSources.Count);   /* the four v1 tables + pg_replication_stats + pg_io_stats + pg_write_stats + pg_blocking_edges (lane 17) + pg_wait_sampling (lane 24) */
        Assert.Equal(2 + pgSources.Count, DarlingRetention.BaselineServingRawCollectors.Count);
        Assert.Contains("cpu_utilization", DarlingRetention.BaselineServingRawCollectors);
        Assert.Contains("file_io_stats", DarlingRetention.BaselineServingRawCollectors);
        foreach (var table in pgSources)
        {
            Assert.Contains(CollectorNameFor(table), DarlingRetention.BaselineServingRawCollectors);
        }

        /* Every member is a real collector with a schedule row — a misspelt member would floor nothing and the
           purge would never notice, because Contains() on a name no collector carries is simply false. */
        foreach (var member in DarlingRetention.BaselineServingRawCollectors)
        {
            Assert.True(CollectorScheduleDefaults.All.ContainsKey(member), $"{member} is floored but is not a scheduled collector");
        }
    }

    /// <summary>
    /// The <c>pg_*</c> hypertables <c>PgTargetBaselineProvider</c> reads directly — DERIVED from the provider's own
    /// query text for every <c>MetricNames.Pg*</c> constant (reflection over the registry, so a metric declared
    /// tomorrow is in the sweep the day it is declared), every <c>FROM pg_&lt;table&gt;</c> the arm names. Not a
    /// hand list: the #3691 between-waves batch found two arms (lanes 11 and 15) whose tables a hand list had
    /// missed, and the next lane cannot miss one this way — an arm reading a table the floor lacks fails
    /// <see cref="BaselineServingRawCollectors_MatchTheRawReadingArms"/> by construction. A metric whose arm is
    /// still a stub (null) contributes nothing, which is right: nothing is read, so nothing needs a floor.
    /// </summary>
    private static System.Collections.Generic.List<string> PgTargetBaselineSources()
    {
        var pgNames = typeof(MetricNames).GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string) && f.Name.StartsWith("Pg", StringComparison.Ordinal))
            .Select(f => (string)f.GetRawConstantValue()!)
            .ToList();
        Assert.True(pgNames.Count >= 9, "the MetricNames PostgreSQL registry sweep found too few names");

        var tablesRead = new System.Collections.Generic.HashSet<string>(StringComparer.Ordinal);
        foreach (var name in pgNames)
        {
            var sql = PgTargetBaselineProvider.GetPgTargetBaselineQuery(name);
            if (sql is null) continue;   /* a stub arm reads nothing */
            foreach (System.Text.RegularExpressions.Match m in System.Text.RegularExpressions.Regex.Matches(sql, @"\bFROM\s+(pg_\w+)"))
            {
                tablesRead.Add(m.Groups[1].Value);
            }
        }

        return tablesRead.OrderBy(t => t, StringComparer.Ordinal).ToList();
    }

    /// <summary>The purge floors by COLLECTOR name (<c>CollectorScheduleDefaults</c> keys), the derivation yields TABLE
    /// names; for every member but <c>pg_blocking</c> / <c>pg_blocking_edges</c> the two are the same string. Resolved
    /// through the catalog so the test drives the seam with the name the purge actually passes.</summary>
    private static string CollectorNameFor(string table) =>
        CollectorCatalog.All.Single(c => c.TargetTable == table).Name;

    /// <summary>The derivation itself, pinned: the nine tables the arms read today, by name, so a table quietly
    /// leaving an arm (or a regex that stopped matching) is a visible change and not a smaller floor.</summary>
    [Fact]
    public void PgTargetBaselineProvider_ReadsExactlyTheFlooredPgSources()
    {
        Assert.Equal(
            new[] { "pg_blocking_edges", "pg_cpu_utilization", "pg_database_stats", "pg_io_stats", "pg_replication_stats", "pg_session_states", "pg_wait_sampling", "pg_wait_stats", "pg_write_stats" },
            PgTargetBaselineSources());
        /* Lane 17's arm reads the log for its zero samples too; the log is not a collector table and needs no floor of
           this kind (DarlingRetentionHorizons.CollectionLogRetentionDays, twice the base, already covers the window). */
        Assert.Contains("FROM collection_log", PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgBlockedSessions)!, StringComparison.Ordinal);
        Assert.True(DarlingRetentionHorizons.CollectionLogRetentionDays >= BaselineMath.BaselineWindowDays);
        /* Every derived table is a real collector table (the regex cannot admit a CTE named pg_something). */
        Assert.All(PgTargetBaselineSources(), t => Assert.True(CollectorCatalog.All.Any(c => c.TargetTable == t), $"{t} is not a collector table"));
    }

    /// <summary>
    /// The purge SEAM, driven the way an operator drives it (#3691): a schedule shortened to 7 days yields the
    /// baseline-window horizon for every baseline-serving collector — the SQL Server pair (#1757) and the
    /// PostgreSQL nine the provider's arms read — and yields 7 for a collector that serves no baseline, so the floor is a floor and not a
    /// blanket. A setting ABOVE the window is honoured as given (the floor never shortens), and the destructive
    /// sink's one-day clamp still runs first, so a 0 becomes 1 for an unfloored table and 30 for a floored one.
    /// </summary>
    [Fact]
    public void EffectivePurgeRetentionDays_FloorsEveryBaselineServingCollectorAtTheWindow_AndNothingElse()
    {
        const int shortened = 7;
        Assert.True(shortened < BaselineMath.BaselineWindowDays);

        foreach (var collector in DarlingRetention.BaselineServingRawCollectors)
        {
            Assert.Equal(BaselineMath.BaselineWindowDays, DarlingRetention.EffectivePurgeRetentionDays(collector, shortened));
            Assert.Equal(BaselineMath.BaselineWindowDays, DarlingRetention.EffectivePurgeRetentionDays(collector, 0));
            Assert.Equal(90, DarlingRetention.EffectivePurgeRetentionDays(collector, 90));
        }

        foreach (var table in PgTargetBaselineSources())
        {
            Assert.Equal(BaselineMath.BaselineWindowDays, DarlingRetention.EffectivePurgeRetentionDays(CollectorNameFor(table), shortened));
        }
        /* The one name split: the purge knows the collector, and the table alone would floor nothing. */
        Assert.Equal("pg_blocking", CollectorNameFor("pg_blocking_edges"));
        Assert.Equal(BaselineMath.BaselineWindowDays, DarlingRetention.EffectivePurgeRetentionDays("pg_blocking", shortened));

        /* Not baseline-serving: the operator's number stands, clamped at one. */
        Assert.Equal(shortened, DarlingRetention.EffectivePurgeRetentionDays("deadlocks", shortened));
        /* pg_lock_stats, not pg_blocking: the latter joined the floored set with lane 17's pg_blocked_sessions arm. */
        Assert.Equal(shortened, DarlingRetention.EffectivePurgeRetentionDays("pg_lock_stats", shortened));
        Assert.Equal(1, DarlingRetention.EffectivePurgeRetentionDays("deadlocks", 0));
        Assert.Equal(1, DarlingRetention.EffectivePurgeRetentionDays("deadlocks", -5));
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
        var plain = TimescaleSupport.RefreshContinuousAggregateSql(TimescaleSupport.PerfmonIntervalBaselineView);
        Assert.Contains("$1::timestamp", plain, StringComparison.Ordinal);
        Assert.Contains("NULL::timestamp", plain, StringComparison.Ordinal);
        Assert.Contains("'collect.perfmon_interval_baseline'::regclass", plain, StringComparison.Ordinal);
        Assert.DoesNotContain("buckets_per_batch", plain, StringComparison.Ordinal);

        /* force is the 4th positional argument, and only ever set deliberately. */
        Assert.EndsWith("NULL::timestamp)", plain, StringComparison.Ordinal);
        var forced = TimescaleSupport.RefreshContinuousAggregateSql(TimescaleSupport.PerfmonIntervalBaselineView, force: true);
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

        /* Row-level filters are part of the statistic, so they must survive too — the interval filter and the
           carried interval column included (#3653), or the plain-PostgreSQL store would sum the restart zero
           its TimescaleDB twin no longer does. */
        var waits = TimescaleSupport.CreateBaselineFallbackViewSql(
            TimescaleSupport.WaitStatsIntervalBaselineView, TimescaleSupport.CreateWaitStatsIntervalBaselineSql);
        Assert.Contains("delta_wait_time_ms >= 0", waits, StringComparison.Ordinal);
        Assert.Contains("sample_interval_seconds IS DISTINCT FROM 0", waits, StringComparison.Ordinal);
        Assert.Contains("max(sample_interval_seconds) AS sample_interval_seconds", waits, StringComparison.Ordinal);

        /* The legacy pair still derives (the live retirement test builds its plain-view fixture from it). */
        var legacyWaits = TimescaleSupport.CreateBaselineFallbackViewSql(
            TimescaleSupport.LegacyWaitStatsBaselineView, TimescaleSupport.LegacyCreateWaitStatsBaselineSql);
        Assert.StartsWith("CREATE OR REPLACE VIEW collect.wait_stats_baseline AS", legacyWaits, StringComparison.Ordinal);
    }

    /* ---------------- #3653 A6/A10: the interval-honest supplies and the supersession ---------------- */

    /// <summary>
    /// THE CONTAMINATION FILTER, baked in (#3653 A6). Each interval-honest supply drops the rows the collector
    /// marked unknowable (<c>sample_interval_seconds = 0</c>: first sighting, counter reset — a restart — or a
    /// gap past the policy) BEFORE the per-collection sum, so a restart collection produces no row rather than
    /// a zero that reads as a quiet sample; and each carries the collection's measured interval so the provider
    /// divides by what was measured instead of deriving a gap. <c>IS DISTINCT FROM 0</c> rather than
    /// <c>&gt; 0</c> on purpose: the three-state rule keeps NULL (pre-column) rows, for which the provider's
    /// magnitude heuristic remains the guard.
    /// </summary>
    [Fact]
    public void IntervalHonestSupplies_DropUnknowableRows_AndCarryTheMeasuredInterval()
    {
        foreach (var createSql in new[] { TimescaleSupport.CreatePerfmonIntervalBaselineSql, TimescaleSupport.CreateWaitStatsIntervalBaselineSql })
        {
            Assert.Contains("AND   sample_interval_seconds IS DISTINCT FROM 0", createSql, StringComparison.Ordinal);
            Assert.Contains("max(sample_interval_seconds) AS sample_interval_seconds", createSql, StringComparison.Ordinal);
            Assert.DoesNotContain("sample_interval_seconds > 0", createSql, StringComparison.Ordinal);
        }

        /* The legacy texts are the record of the defect: no filter, no column. Retained verbatim so the live
           retirement test can build the fleet's real shape and prove the restart row is in the old sum and out
           of the new. */
        foreach (var legacy in new[] { TimescaleSupport.LegacyCreatePerfmonBaselineSql, TimescaleSupport.LegacyCreateWaitStatsBaselineSql })
        {
            Assert.DoesNotContain("sample_interval_seconds", legacy, StringComparison.Ordinal);
        }

        /* Registered under the successor names; the legacy names are registered NOWHERE the ensure sweep reads. */
        var registered = TimescaleSupport.BaselineAggregates.Select(a => a.View).ToArray();
        Assert.Contains(TimescaleSupport.PerfmonIntervalBaselineView, registered);
        Assert.Contains(TimescaleSupport.WaitStatsIntervalBaselineView, registered);
        Assert.DoesNotContain(TimescaleSupport.LegacyPerfmonBaselineView, registered);
        Assert.DoesNotContain(TimescaleSupport.LegacyWaitStatsBaselineView, registered);
        Assert.Equal(TimescaleSupport.CreatePerfmonIntervalBaselineSql,
            TimescaleSupport.BaselineAggregates.Single(a => a.View == TimescaleSupport.PerfmonIntervalBaselineView).CreateSql);
        Assert.Equal(TimescaleSupport.CreateWaitStatsIntervalBaselineSql,
            TimescaleSupport.BaselineAggregates.Single(a => a.View == TimescaleSupport.WaitStatsIntervalBaselineView).CreateSql);

        /* Both pairs resolve to their raw table, so the backfill probe and the retirement fixture share one map. */
        Assert.Equal("perfmon_stats", TimescaleSupport.SourceTableFor(TimescaleSupport.PerfmonIntervalBaselineView));
        Assert.Equal("wait_stats", TimescaleSupport.SourceTableFor(TimescaleSupport.WaitStatsIntervalBaselineView));
        Assert.Equal("perfmon_stats", TimescaleSupport.SourceTableFor(TimescaleSupport.LegacyPerfmonBaselineView));
        Assert.Equal("wait_stats", TimescaleSupport.SourceTableFor(TimescaleSupport.LegacyWaitStatsBaselineView));
    }

    /// <summary>
    /// The successors REPLACED the legacy pair in the registry rather than joining it — the count is still
    /// seven and the pair holds the first two positions — because <c>HourlyRefreshPhaseOrder</c>,
    /// <c>AggregateCompressionTargets</c> and <c>RetentionPolicies</c> derive from that list by position and
    /// count. An append would have widened the light band by two minutes and dropped the heaviest refresh's
    /// watch line from 1,050 s to 950 s against its 896 s ceiling (the arithmetic is on
    /// <c>SupersededBaselineRelations</c>); this pin names the reason.
    ///
    /// <para><b>The grid DID move at #3653 (Q12), and not because of this pair.</b> The three interval-honest
    /// HOURLY successors could not replace their legacies (the daily tier is hierarchical from those —
    /// <c>SupersededHourlyRollups</c>), so they were appended to <c>HourlyAggregates</c> and the grid
    /// re-derived by its own method: sixteen policies, the heaviest at :18, the watch line at 900 s. The two
    /// bounded hourly successors are dealt into the bounded class ahead of the baselines, so this pair's
    /// minutes moved 3→6 and 5→7 — the converge re-phases them once. What this pin still holds is the
    /// baseline half: seven members, the pair in front, and the baseline pair adding NOTHING to the count the
    /// grid derives from. The grid's own figures are pinned with their derivation in TimescaleSupportTests
    /// (<c>CompressionPhaseGrid_ClearsEveryRefreshSlotsGuardBand_AndTheHeaviestRefreshsSlotWhole</c> and
    /// <c>TheRefreshGridIsUnchanged_AndTheCompressionGridsOneInputFromItIsPinned</c>), not restated here.</para>
    /// </summary>
    [Fact]
    public void Successors_TookTheLegacyPositions_SoThePhaseGridDidNotMove()
    {
        Assert.Equal(7, TimescaleSupport.BaselineAggregates.Length);
        Assert.Equal(TimescaleSupport.PerfmonIntervalBaselineView, TimescaleSupport.BaselineAggregates[0].View);
        Assert.Equal(TimescaleSupport.WaitStatsIntervalBaselineView, TimescaleSupport.BaselineAggregates[1].View);

        /* The order's count is the hourly registry plus the seven baselines — 9 + 7 = 16 since #3653 — and
           the baseline pair contributes exactly its two positions to it, no more. */
        Assert.Equal(TimescaleSupport.HourlyAggregates.Length + 7, TimescaleSupport.HourlyRefreshPhaseOrder.Count);
        Assert.Equal(16, TimescaleSupport.HourlyRefreshPhaseOrder.Count);
        Assert.Equal(6, TimescaleSupport.RefreshPhaseMinutesFor(TimescaleSupport.PerfmonIntervalBaselineView));
        Assert.Equal(7, TimescaleSupport.RefreshPhaseMinutesFor(TimescaleSupport.WaitStatsIntervalBaselineView));
        Assert.Equal(18, TimescaleSupport.HeaviestRefreshStartMinute);
        Assert.Equal(900, TimescaleSupport.RefreshSlotWarningSeconds);
    }

    /// <summary>
    /// The supersession list is disjoint from both the living registry and the on-sight retirement list: a
    /// legacy name in <c>BaselineAggregates</c> would be re-created by the ensure sweep after every drop; one in
    /// <c>RetiredBaselineRelations</c> would be dropped on sight and forfeit the 35-day history the condition
    /// exists to protect. Each successor is registered, each legacy is not, and the provider's per-metric map
    /// agrees with the storage list pair for pair.
    /// </summary>
    [Fact]
    public void SupersededRelations_AreNeitherLiving_NorRetiredOnSight_AndTheProviderMapAgrees()
    {
        var living = TimescaleSupport.BaselineAggregates.Select(a => a.View).ToHashSet(StringComparer.Ordinal);
        var retired = TimescaleSupport.RetiredBaselineRelations.ToHashSet(StringComparer.Ordinal);

        Assert.Equal(2, TimescaleSupport.SupersededBaselineRelations.Length);
        foreach (var (legacy, successor) in TimescaleSupport.SupersededBaselineRelations)
        {
            Assert.DoesNotContain(legacy, living);
            Assert.DoesNotContain(legacy, retired);
            Assert.Contains(successor, living);
            Assert.DoesNotContain(successor, retired);
            Assert.NotEqual(legacy, successor);
        }

        var pairs = TimescaleSupport.SupersededBaselineRelations.ToHashSet();
        Assert.Contains(PgBaselineProvider.SupersededSupplyFor(MetricNames.BatchRequests)!.Value, pairs);
        Assert.Contains(PgBaselineProvider.SupersededSupplyFor(MetricNames.WaitStats)!.Value, pairs);
        Assert.Contains(PgBaselineProvider.SupersededSupplyFor(MetricNames.WaitMsPerSec)!.Value, pairs);
        foreach (var untouched in new[]
        {
            MetricNames.Cpu, MetricNames.SessionCount, MetricNames.QueryDuration, MetricNames.IoLatency,
            MetricNames.Blocking, MetricNames.Deadlock, MetricNames.Memory, MetricNames.BlockingPerMinute,
        })
        {
            Assert.Null(PgBaselineProvider.SupersededSupplyFor(untouched));
            Assert.Null(PgBaselineProvider.GetLegacyBaselineQuery(untouched));
        }
    }

    /// <summary>
    /// THE RETIREMENT CONDITION walked across time (#3653): a legacy continuous aggregate releases only once the
    /// successor's oldest bucket reaches the tier horizon; an empty successor never releases it; a legacy plain
    /// view releases at once. Day 0 after a backfill from a 30-day raw horizon is the case the brief asked to see
    /// evaluate FALSE; day 5 is where it turns.
    /// </summary>
    [Fact]
    public void RetirementCondition_HoldsOnlyOnceTheSuccessorCoversTheTier()
    {
        var firstStart = new DateTime(2026, 9, 20, 4, 0, 0, DateTimeKind.Unspecified);
        var successorOldest = firstStart.AddDays(-30); // backfilled from raw's default 30-day horizon

        Assert.False(TimescaleSupport.SupersededBaselineRelationDropsAt(true, successorOldest, firstStart));
        Assert.False(TimescaleSupport.SupersededBaselineRelationDropsAt(true, successorOldest, firstStart.AddDays(4).AddHours(23)));
        Assert.True(TimescaleSupport.SupersededBaselineRelationDropsAt(true, successorOldest, firstStart.AddDays(5)));
        Assert.True(TimescaleSupport.SupersededBaselineRelationDropsAt(true, successorOldest, firstStart.AddDays(40)));

        /* Exactly on the horizon counts as coverage (<=). */
        Assert.True(TimescaleSupport.SupersededBaselineRelationDropsAt(true, firstStart - TimescaleSupport.BaselineRetentionSpan, firstStart));

        /* An un-backfilled successor covers nothing, whatever the clock says. */
        Assert.False(TimescaleSupport.SupersededBaselineRelationDropsAt(true, null, firstStart.AddDays(400)));

        /* A legacy PLAIN VIEW has nothing of its own to lose: drops as soon as a successor exists. */
        Assert.True(TimescaleSupport.SupersededBaselineRelationDropsAt(false, null, firstStart));
        Assert.True(TimescaleSupport.SupersededBaselineRelationDropsAt(false, successorOldest, firstStart));
    }

    /// <summary>
    /// THE SUPPLY RULE walked across the states a store passes through (#3653): fresh store (no legacy) →
    /// successor; day 0 after upgrade (legacy 35 days deep, successor 30) → legacy; a day later, once the
    /// successor reaches the window's start → successor, even though the legacy still reaches further back
    /// than the window; a server registered after the upgrade (both relations equally shallow) → successor;
    /// an un-backfilled successor → legacy; an empty legacy → successor.
    /// </summary>
    [Fact]
    public void SupplyRule_PrefersTheSuccessor_ExactlyWhenItReachesAsFarAsTheLegacyCanContribute()
    {
        var analysisTime = new DateTime(2026, 9, 20, 4, 0, 0, DateTimeKind.Unspecified);
        var windowStart = analysisTime.AddDays(-BaselineMath.BaselineWindowDays);
        var legacyOldest = analysisTime.AddDays(-35);

        /* Fresh store, or already retired. */
        Assert.True(PgBaselineProvider.PrefersSuccessor(false, null, null, windowStart));
        Assert.True(PgBaselineProvider.PrefersSuccessor(false, null, analysisTime.AddDays(-2), windowStart));

        /* Day 0: the successor was backfilled from a raw horizon that stops one day short of the window. */
        Assert.False(PgBaselineProvider.PrefersSuccessor(true, legacyOldest, analysisTime.AddDays(-29), windowStart));

        /* Day 1: the successor reaches the window's start; the legacy's extra five days are never read. */
        Assert.True(PgBaselineProvider.PrefersSuccessor(true, legacyOldest, windowStart, windowStart));
        Assert.True(PgBaselineProvider.PrefersSuccessor(true, legacyOldest, analysisTime.AddDays(-31), windowStart));

        /* A server registered three days ago: both relations start three days ago — no reason to read the
           contaminated one. */
        var threeDaysAgo = analysisTime.AddDays(-3);
        Assert.True(PgBaselineProvider.PrefersSuccessor(true, threeDaysAgo, threeDaysAgo, windowStart));
        /* …but if the successor is one bucket shallower than the legacy for that server, the legacy has a row
           the successor lacks. */
        Assert.False(PgBaselineProvider.PrefersSuccessor(true, threeDaysAgo, threeDaysAgo.AddHours(1), windowStart));

        /* Created but not yet backfilled: never preferred while the legacy has rows. */
        Assert.False(PgBaselineProvider.PrefersSuccessor(true, legacyOldest, null, windowStart));

        /* A legacy relation with no rows for this server cannot contribute anything. */
        Assert.True(PgBaselineProvider.PrefersSuccessor(true, null, null, windowStart));
        Assert.True(PgBaselineProvider.PrefersSuccessor(true, null, threeDaysAgo, windowStart));
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
        var sql = TimescaleSupport.DropBaselineFallbackViewSql(TimescaleSupport.WaitStatsIntervalBaselineView);

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
    /// #3653: coverage is read OFF THE MATERIALIZATION when there is one. Through the real-time view a
    /// <c>WITH NO DATA</c> aggregate reports raw's whole reach (watermark <c>-infinity</c>, measured on the
    /// 2.28.1 rig), so the gate above never fires on the start that created the aggregate and the history goes
    /// invisible one policy refresh later, until the next start. The materialization-hypertable form is what
    /// <c>BackfillBaselineAggregatesAsync</c> runs on a TimescaleDB store; the view form remains for plain views.
    /// Everything else about the probe — source, clamp, bound horizon — is identical between the two.
    /// </summary>
    [Fact]
    public void BackfillGate_ReadsCoverageOffTheMaterialization_NotThroughTheRealTimeView()
    {
        var view = TimescaleSupport.WaitStatsIntervalBaselineView;
        var source = TimescaleSupport.SourceTableFor(view);
        var throughView = TimescaleSupport.BaselineBackfillProbeSql(view, source);
        var offMaterialization = TimescaleSupport.BaselineBackfillProbeSql(view, source, ("_timescaledb_internal", "_materialized_hypertable_353"));

        Assert.Contains($"(SELECT min(bucket) FROM collect.{view}) AS coverage_oldest", throughView, StringComparison.Ordinal);
        Assert.Contains("(SELECT min(bucket) FROM \"_timescaledb_internal\".\"_materialized_hypertable_353\") AS coverage_oldest", offMaterialization, StringComparison.Ordinal);
        Assert.DoesNotContain($"FROM collect.{view}", offMaterialization, StringComparison.Ordinal);

        /* Same probe otherwise: source and need are read identically, the horizon stays bound. */
        Assert.Equal(
            throughView.Replace($"collect.{view}", "<coverage>", StringComparison.Ordinal),
            offMaterialization.Replace("\"_timescaledb_internal\".\"_materialized_hypertable_353\"", "<coverage>", StringComparison.Ordinal));
        Assert.Equal(throughView, TimescaleSupport.BaselineBackfillProbeSql(view, source, materialization: null));
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
        Assert.Contains("to_regclass", TimescaleSupport.BaselineRelationExistsSql(TimescaleSupport.PerfmonIntervalBaselineView), StringComparison.Ordinal);
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
