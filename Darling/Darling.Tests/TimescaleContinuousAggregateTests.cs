/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the continuous-aggregate + retention-tier SQL so the tested shape can never silently drift: the three
/// HOURLY rollups (query_stats / procedure_stats / query_store_stats), the two HIERARCHICAL daily rollups (sourced
/// from the hourly CAGGs, not raw), the per-cadence refresh policies, and the tiered retention (raw 4d, hourly
/// history CAGGs 90d since #1937, daily history kept indefinitely, with the interval-dedup and baseline tiers on
/// horizons of their own). All idempotent RUNTIME setup in <see cref="TimescaleSupport"/> (the
/// worker's TimescaleDB block), NOT a versioned migration, so there is no schema-version change to pin.
/// </summary>
public sealed class TimescaleContinuousAggregateTests
{
    [Fact]
    public void QueryStatsHourly_IsAContinuousAggregate_HourBucketed_GroupedByComposerDimensions()
    {
        var sql = TimescaleSupport.CreateQueryStatsHourlySql;

        Assert.Contains("CREATE MATERIALIZED VIEW IF NOT EXISTS collect.query_stats_hourly", sql, StringComparison.Ordinal);
        Assert.Contains("WITH (timescaledb.continuous)", sql, StringComparison.Ordinal);
        Assert.Contains("time_bucket('1 hour', collection_time) AS bucket", sql, StringComparison.Ordinal);
        Assert.Contains("FROM collect.query_stats", sql, StringComparison.Ordinal);
        /* Grouped by the composer's query_stats dimensions, in order, so a panel points here with no remapping. */
        Assert.Contains("GROUP BY server_id, server_name, database_name, query_hash, sql_handle, bucket", sql, StringComparison.Ordinal);

        /* SUM/MIN/MAX on each per-interval delta (so avg composes at query time as sum/count) + a sample_count;
           deliberately NO materialized average (it would not re-aggregate correctly). */
        foreach (var col in new[] { "delta_worker_time", "delta_elapsed_time", "delta_execution_count" })
        {
            Assert.Contains("sum(" + col + ")", sql, StringComparison.Ordinal);
            Assert.Contains("min(" + col + ")", sql, StringComparison.Ordinal);
            Assert.Contains("max(" + col + ")", sql, StringComparison.Ordinal);
        }

        Assert.Contains("count(*) AS sample_count", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("avg(", sql, StringComparison.Ordinal);

        /* WITH NO DATA — no startup backfill; materializing history is the --backfill-rollups operator op. */
        Assert.Contains("WITH NO DATA", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #1759: every query-acceleration rollup stays MATERIALIZED-ONLY, and this pin now carries the opposite
    /// meaning it used to. It was written asserting "real-time aggregation left ON (no materialized_only)" —
    /// false twice over. TimescaleDB 2.13+ defaults the option to TRUE, so naming nothing means real-time
    /// aggregation is OFF (the runtime is pinned at 2.28.1); and turning it on would not surface un-materialized
    /// history anyway, because the watermark is a hard partition rather than a fallback.
    ///
    /// <para>What makes the pin LOAD-BEARING rather than merely accurate: <c>RollupCoverageProbeSql</c> and
    /// <c>RetentionArmSafetySql</c> both read <c>min(bucket)</c> to mean "the oldest bucket this rollup has
    /// MATERIALIZED". Adding <c>materialized_only = false</c> would union the raw branch in, so an EMPTY
    /// materialization would report RAW's oldest row as the rollup's floor. The router would then believe a
    /// rollup covers history it has not materialized (serving silence), and — far worse — the arming gate would
    /// arm a raw purge over history no rollup holds. The nine BASELINE aggregates deliberately do set the
    /// option; they are leaves whose arming coverage is the tier itself, so the same union cannot mislead a
    /// cross-tier gate.</para>
    /// </summary>
    [Fact]
    public void QueryAccelerationRollups_StayMaterializedOnly_BecauseCoverageAndArmingReadMinBucket()
    {
        foreach (var sql in new[]
        {
            TimescaleSupport.CreateQueryStatsHourlySql,
            TimescaleSupport.CreateQueryStatsDailySql,
            TimescaleSupport.CreateQueryStatsDbHourlySql,
            TimescaleSupport.CreateQueryStatsDbDailySql,
            TimescaleSupport.CreateProcedureStatsHourlySql,
            TimescaleSupport.CreateProcedureStatsDailySql,
            TimescaleSupport.CreateQueryStoreStatsHourlySql,
            TimescaleSupport.CreateQueryStoreStatsDailySql,
        })
        {
            Assert.DoesNotContain("materialized_only", sql, StringComparison.Ordinal);
        }

        /* The probe and the arming gate must keep reading the SAME expression off the SAME relation, or
           "covered" would mean two different things to the router and to the purge. */
        var probe = TimescaleSupport.RollupCoverageProbeSql(RollupAvailability.All);
        var arming = TimescaleSupport.RetentionArmSafetySql(
            "query_stats", "collection_time", new[] { TimescaleSupport.QueryStatsHourlyView });

        Assert.Contains($"min(bucket) FROM collect.{TimescaleSupport.QueryStatsHourlyView}", probe, StringComparison.Ordinal);
        Assert.Contains($"min(bucket) FROM collect.{TimescaleSupport.QueryStatsHourlyView}", arming, StringComparison.Ordinal);
        Assert.Contains("min(collection_time) FROM collect.query_stats", probe, StringComparison.Ordinal);
        Assert.Contains("min(collection_time) FROM collect.query_stats", arming, StringComparison.Ordinal);
    }

    [Fact]
    public void ProcedureStatsHourly_GroupedBySchemaAndObject()
    {
        var sql = TimescaleSupport.CreateProcedureStatsHourlySql;

        Assert.Contains("CREATE MATERIALIZED VIEW IF NOT EXISTS collect.procedure_stats_hourly", sql, StringComparison.Ordinal);
        Assert.Contains("WITH (timescaledb.continuous)", sql, StringComparison.Ordinal);
        Assert.Contains("FROM collect.procedure_stats", sql, StringComparison.Ordinal);
        /* schema_name + object_name — both composer dimensions (a panel by schema_name alone re-aggregates). */
        Assert.Contains("GROUP BY server_id, server_name, database_name, schema_name, object_name, bucket", sql, StringComparison.Ordinal);
        Assert.Contains("count(*) AS sample_count", sql, StringComparison.Ordinal);
        Assert.Contains("WITH NO DATA", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ContinuousAggregatePolicy_IsTheConservativeHourlyShape_Idempotent()
    {
        var sql = TimescaleSupport.AddHourlyRefreshPolicySql(TimescaleSupport.QueryStatsHourlyView);

        Assert.Contains("add_continuous_aggregate_policy('collect.query_stats_hourly'", sql, StringComparison.Ordinal);
        Assert.Contains("end_offset => INTERVAL '1 hour'", sql, StringComparison.Ordinal);
        Assert.Contains("schedule_interval => INTERVAL '1 hour'", sql, StringComparison.Ordinal);
        Assert.Contains("if_not_exists => true", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// TREATMENT ONE of #3012, pinned ALONE so that reverting it is red here even if the stagger survives.
    ///
    /// <para>An hourly refresh's cost is set by the window it re-scans, and a 3-day window against a 4-day raw
    /// retention re-materialized roughly three quarters of the hypertable every hour. Measured on the
    /// production store: the heaviest hourly refresh ran 3,301-6,330 s against its own 1-hour cadence — 118-175%
    /// of it, so each run started into the tail of the last — while rows arriving per hour FELL ~3x. On the
    /// narrowed window the same refresh finishes inside its own phase window, which is what makes the
    /// overlap structurally impossible rather than merely absent — by 364 s of a 1,260 s window against
    /// #3166's census and #3174's re-derived grid. The figure and its derivation live on
    /// TimescaleSupport.HeaviestHourlyRefreshObservedCeilingSeconds rather than being restated
    /// here.</para>
    ///
    /// <para>Asserted for EVERY hourly view rather than the heavy one alone: the defect was a shared default,
    /// so a fix that reached only the aggregate named in the incident would leave twelve behind.</para>
    /// </summary>
    [Fact]
    public void HourlyRefreshWindow_IsOneDay_NotTheRawRetentionHorizon()
    {
        Assert.Equal("1 day", TimescaleSupport.HourlyRefreshStartOffset);
        Assert.Equal(TimeSpan.FromDays(1), TimescaleSupport.HourlyRefreshStartSpan);
        Assert.Equal("1 hour", TimescaleSupport.HourlyRefreshScheduleInterval);
        Assert.Equal(TimeSpan.FromHours(1), TimescaleSupport.HourlyRefreshScheduleSpan);

        foreach (var view in TimescaleSupport.HourlyRefreshPhaseOrder)
        {
            var sql = TimescaleSupport.AddHourlyRefreshPolicySql(view);

            Assert.Contains("start_offset => INTERVAL '1 day'", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("start_offset => INTERVAL '3 days'", sql, StringComparison.Ordinal);
        }

        /* The window is now chosen against its own cadence, and raw retention only has to clear it — the
           opposite of the dependency that made the refresh expensive, where the window had to cover a depth
           retention chose. Both directions asserted so neither can be narrowed into the other. */
        Assert.True(TimescaleSupport.HourlyRefreshStartSpan < TimescaleSupport.RawRetentionSpan,
            "raw retention must outlive the hourly refresh window, or a drop outruns the aggregate meant to preserve it");
        Assert.True(TimescaleSupport.HourlyRefreshScheduleSpan < TimescaleSupport.HourlyRefreshStartSpan,
            "a refresh window narrower than its own cadence would leave buckets no run ever covers");
    }

    /// <summary>
    /// TREATMENT TWO of #3012, pinned ALONE so that reverting it is red here even if the narrowing survives.
    ///
    /// <para>The heaviest hourly refresh's recorded ceiling is the only one that does not fit inside
    /// <see cref="TimescaleSupport.CompressionPhaseGuardMinutes"/>, so what it does take has to be invisible
    /// to everything else rather than merely short. Two things do that, and the second is the less obvious
    /// one:</para>
    ///
    /// <para><b>Distinct minutes of the hour — ALL of them, since #3174.</b> A refresh holds
    /// <c>AccessShareLock</c> on what it reads; a compression policy on that same hypertable queues an
    /// <c>AccessExclusiveLock</c> request behind it; and a QUEUED exclusive request blocks every subsequent
    /// shared request, so collector store-writes convoy behind a lock nobody holds. Policies that share a
    /// hypertable therefore have to start at different times. The grid now gives every hourly policy its own
    /// minute, so that holds for every pair rather than for the pairs that happen to contend — which is why
    /// the assertion below is over the WHOLE phase set and the <c>query_store_stats</c> family is kept only as
    /// the measured case.</para>
    ///
    /// <para><b>A fixed schedule.</b> Naming <c>initial_start</c> is what switches TimescaleDB from
    /// finish-to-start to start-to-start scheduling. Under finish-to-start, one bad hour phase-locks a family
    /// permanently — the incident's three jobs, on unrelated schedules with very different workloads, finished
    /// within 119 seconds of each other and then re-started together every hour after that. The offsets without
    /// the fixed schedule would drift back into coincidence the first time a run overran.</para>
    /// </summary>
    [Fact]
    public void HourlyRefreshPolicies_AreStaggeredAcrossTheHour_OnAFixedSchedule()
    {
        Assert.Equal(1, TimescaleSupport.LightRefreshStepMinutes);

        /* Thirteen hourly refreshes: the six named rollups plus the seven baseline aggregates. */
        Assert.Equal(13, TimescaleSupport.HourlyRefreshPhaseOrder.Count);
        Assert.Equal(12, TimescaleSupport.LightHourlyRefreshCount);

        var phases = TimescaleSupport.HourlyRefreshPhaseOrder
            .Select(TimescaleSupport.RefreshPhaseMinutesFor)
            .ToArray();

        /* THE INVARIANT, over the whole set rather than over the pairs that contend (#3174): thirteen
           policies, thirteen distinct minutes. Asserted as a count identity against the list rather than
           against a literal 13, so an aggregate registered tomorrow is covered without editing this. */
        Assert.Equal(TimescaleSupport.HourlyRefreshPhaseOrder.Count, phases.Distinct().Count());

        /* And each minute is inside the hour, on the band its own view belongs to — the heaviest refresh at
           its own start, everything else in the light band ahead of it. A minute past the light band would
           put a refresh inside the heaviest window or the compression band, which is what the tiling
           assertion below and the compression grid both rest on. */
        foreach (var view in TimescaleSupport.HourlyRefreshPhaseOrder)
        {
            var phase = TimescaleSupport.RefreshPhaseMinutesFor(view);
            Assert.InRange(phase, 0, TimescaleSupport.MinutesInHourlyCadence - 1);

            if (string.Equals(view, TimescaleSupport.HeaviestHourlyRefreshView, StringComparison.Ordinal))
            {
                Assert.Equal(TimescaleSupport.HeaviestRefreshStartMinute, phase);
            }
            else
            {
                Assert.InRange(
                    phase,
                    0,
                    (TimescaleSupport.LightHourlyRefreshCount - 1) * TimescaleSupport.LightRefreshStepMinutes);
            }

            var sql = TimescaleSupport.AddHourlyRefreshPolicySql(view);
            Assert.Contains(
                $"initial_start => date_trunc('hour', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' + INTERVAL '1 hour' + INTERVAL '{phase.ToString(CultureInfo.InvariantCulture)} minutes'",
                sql,
                StringComparison.Ordinal);
        }

        /* The lock adjacency the convoy actually formed on, named explicitly because it is the group the
           production measurements were taken against. Generalized below; kept here as the measured case. */
        var family = new[]
        {
            TimescaleSupport.QueryStoreStatsHourlyView,
            TimescaleSupport.QueryStoreStatsIntervalHourlyView,
            TimescaleSupport.QueryStoreStatsCorrectedHourlyView,
        };

        var slots = family.Select(TimescaleSupport.RefreshPhaseMinutesFor).ToArray();
        Assert.Equal(slots.Length, slots.Distinct().Count());

        /* UTC, not the session time zone: a bare date_trunc('hour', now()) lands off-grid on the half-hour and
           quarter-hour zones, which is a store-wide silent skew rather than a local oddity. */
        var anchored = TimescaleSupport.AddHourlyRefreshPolicySql(TimescaleSupport.QueryStatsHourlyView);
        Assert.Contains("now() AT TIME ZONE 'UTC'", anchored, StringComparison.Ordinal);
        Assert.DoesNotContain("date_trunc('hour', now())", anchored, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3060: the seam between the grid and #2136's Store Job Over Cadence knob, asserted in SECONDS —
    /// the unit that alert actually compares — against <see cref="TimescaleSupport.RefreshPhaseSlotSeconds"/>.
    ///
    /// <para><b>Why in seconds as well as in percent.</b> The knob is a percent of a job's own schedule
    /// interval and the grid is a number of minutes, and nothing made those two commensurable: the default
    /// landing exactly on one slot was an accident of two independent decisions, and #3174 broke the
    /// derivation that had made it a decision — see
    /// <see cref="TimescaleSupport.RefreshSlotPercentOfHourlyCadence"/> for why the number cannot move even
    /// though the grid did. What is left to check is the ORDERING, in the unit an operator reads off the
    /// alert text: the knob must fire at or before the window the compression grid assumes a refresh fits
    /// inside. The tightness half is gone with the uniform slot count it was expressed over, so the knob now
    /// fires earlier than it strictly has to — the safe direction, and asserted as an inequality rather than
    /// left implied.</para>
    ///
    /// <para><b>The tiling assertion is not a tautology, and it changed shape with the grid.</b> It used to
    /// multiply one slot by a slot count, which under integer division went SHORT of the cadence for any
    /// step that did not divide 60. The bands have different widths now, so the identity is that they
    /// PARTITION the hour: the light band and its guard, the heaviest refresh's window, and the compression
    /// band sum to the cadence exactly. That is stronger than the old form — it says no minute is
    /// unassigned and none is claimed twice — and it is what makes
    /// <see cref="TimescaleSupport.HeaviestRefreshWindowMinutes"/> a remainder rather than a third
    /// independent number. Pinned against <see cref="TimescaleSupport.HourlyRefreshScheduleSpan"/> rather
    /// than a literal hour so the cadence enters as the product's own value.</para>
    /// </summary>
    [Fact]
    public void TheRefreshGrid_TilesTheHourlyCadence_AndTheCadenceKnobFiresNoLaterThanOneSlot()
    {
        /* THE TILING IDENTITY: the three bands partition the hour. */
        Assert.Equal(
            TimescaleSupport.MinutesInHourlyCadence,
            TimescaleSupport.HeaviestRefreshStartMinute
            + TimescaleSupport.HeaviestRefreshWindowMinutes
            + TimescaleSupport.CompressionPhaseBandMinutes);

        Assert.Equal(
            TimescaleSupport.HourlyRefreshScheduleSpan,
            TimeSpan.FromMinutes(TimescaleSupport.MinutesInHourlyCadence));

        /* And the light band plus its guard IS that first stretch, so the identity above cannot be satisfied
           by a start minute that came from anywhere else. */
        Assert.Equal(
            ((TimescaleSupport.LightHourlyRefreshCount - 1) * TimescaleSupport.LightRefreshStepMinutes)
            + TimescaleSupport.CompressionPhaseGuardMinutes,
            TimescaleSupport.HeaviestRefreshStartMinute);

        /* Every band is strictly positive: a catalog or a policy list grown far enough would drive the
           remainder to zero or negative, and a grid with no window for the heaviest refresh has to be red
           here rather than produce a negative slot width. */
        Assert.True(
            TimescaleSupport.HeaviestRefreshWindowMinutes > 0,
            $"the hour has no minutes left for the heaviest refresh's window: "
            + $"{TimescaleSupport.LightHourlyRefreshCount} light policies at "
            + $"{TimescaleSupport.LightRefreshStepMinutes}-minute spacing, a "
            + $"{TimescaleSupport.CompressionPhaseGuardMinutes}-minute guard and a "
            + $"{TimescaleSupport.CompressionPhaseBandMinutes}-minute compression band already fill it — "
            + "re-derive the grid (#3174) rather than widening any one band");
        Assert.True(TimescaleSupport.CompressionPhaseBandMinutes > 0);
        Assert.True(TimescaleSupport.CompressionPhaseGuardMinutes > 0);

        var cadenceSeconds = (int)TimescaleSupport.HourlyRefreshScheduleSpan.TotalSeconds;
        var knobFiresAtSeconds = cadenceSeconds * TimescaleSupport.RefreshSlotPercentOfHourlyCadence / 100;

        Assert.True(
            knobFiresAtSeconds <= TimescaleSupport.RefreshPhaseSlotSeconds,
            $"the shipped cadence knob fires at {knobFiresAtSeconds}s against a "
            + $"{TimescaleSupport.RefreshPhaseSlotSeconds}s window — past the width the compression phase grid "
            + "assumes a refresh fits inside, so the alert would arrive after #3035's precondition is false. "
            + "The knob cannot move without a rung (V57), so the repair is the grid, not the knob");

        /* #3044's watch line sits inside the same window, so the two signals are ordered rather than
           competing: the grid's own watch speaks first, the cadence alert at the wall. */
        Assert.True(
            TimescaleSupport.RefreshSlotWarningSeconds <= TimescaleSupport.RefreshPhaseSlotSeconds,
            "the refresh slot watch line must sit inside the window it watches");
    }

    /// <summary>
    /// The grid's actual invariant, over EVERY contended relation rather than the one family the incident
    /// evidence named.
    ///
    /// <para><b>Why the family-only check was not enough.</b> <c>collect.query_stats</c> — the other dominant
    /// table — feeds THREE hourly policies: the query-grain rollup, the per-database rollup, and the
    /// query_stats baseline. Under the uniform grid their phases landed on distinct minutes only because of
    /// where they happened to sit in <see cref="TimescaleSupport.HourlyRefreshPhaseOrder"/> — phases collided
    /// whenever two positions differed by a multiple of the slot count, so inserting one
    /// aggregate ahead of the per-database rollup would silently put two <c>query_stats</c> refreshes back on
    /// the same minute — the exact lock-queue adjacency this change exists to remove — and the family-only
    /// assertion would have stayed green. Raised by review rather than found here.</para>
    ///
    /// <para><b>Contended relation, not source table.</b> Two hourly refreshes contend when one SELECTs from
    /// what the other WRITES, so the group key is a view's immediate <c>FROM collect.&lt;x&gt;</c> — and a view
    /// that is itself a source joins the group of its own consumers, because refreshing it writes the
    /// materialization hypertable they read. That is why the corrected Query Store rollup and the interval
    /// layer it reads are checked against each other and not merely against raw.</para>
    ///
    /// <para><b>Derived from the shipped CREATE text</b>, via
    /// <see cref="TimescaleSupport.HourlyRefreshDefinitions"/>, so a new aggregate is covered the moment it is
    /// registered. The extraction is asserted non-empty per view first: a regex that matched nothing would
    /// group every view under "no source" and report a clean sweep for having checked nothing, which is the
    /// failure mode a guard proving an absence has to rule out before its result means anything.</para>
    ///
    /// <para><b>Why this is kept even though #3174 made it unfalsifiable by construction.</b> Every hourly
    /// policy now has its own minute, so a per-relation collision is impossible without a whole-set
    /// collision — and the whole-set claim is asserted first, here, so the per-relation sweep below is the
    /// weaker consequence rather than the load-bearing check. It stays because it is the check that states
    /// WHY distinctness matters: it recovers the contended relations from the shipped CREATE text, so if a
    /// later grid ever went back to sharing minutes between policies, this is the guard that says which
    /// sharing is the harmful kind.</para>
    /// </summary>
    [Fact]
    public void HourlyRefreshPhases_AreDistinctForEveryRelationTwoPoliciesContendFor()
    {
        var definitions = TimescaleSupport.HourlyRefreshDefinitions;
        Assert.Equal(TimescaleSupport.HourlyRefreshPhaseOrder.Count, definitions.Count);

        /* THE STRUCTURAL CLAIM, ahead of the per-relation sweep: no two hourly policies share a minute at
           all, so no contending PAIR can. Asserted over the shipped list rather than over a literal count,
           and it is what makes the sweep below unable to fail for a reason the sweep itself would have to
           discover. */
        var everyPhase = TimescaleSupport.HourlyRefreshPhaseOrder
            .Select(TimescaleSupport.RefreshPhaseMinutesFor)
            .ToArray();
        Assert.Equal(everyPhase.Length, everyPhase.Distinct().Count());

        /* THE DISCRIMINATING CASE, so the claim above is a property of the MAP and not of this list's
           current order. Under the old grid the three collect.query_stats consumers sat at positions 0, 3
           and 9 — all congruent mod the slot count — and inserting one aggregate ahead of the last of them
           put two back on the same minute. Rotating the list is the same class of change, and the map has to
           survive it: every rotation of the phase order still yields as many distinct minutes as there are
           policies. A modulus-based map fails this at once.

           THROUGH THE SHIPPED MAP, via its order-taking overload, and that is the whole point of the
           overload existing. Re-implementing the counting rule here would prove the RULE injective under
           permutation and leave TimescaleSupport.RefreshPhaseMinutesFor exercised at exactly one order — the
           "a test that agrees with any derivation" failure one layer down, and the same failure this grid
           exists to remove. Raised by review. */
        for (var rotation = 1; rotation < TimescaleSupport.HourlyRefreshPhaseOrder.Count; rotation++)
        {
            var rotated = TimescaleSupport.HourlyRefreshPhaseOrder
                .Skip(rotation)
                .Concat(TimescaleSupport.HourlyRefreshPhaseOrder.Take(rotation))
                .ToArray();

            var minutes = rotated
                .Select(view => TimescaleSupport.RefreshPhaseMinutesFor(rotated, view))
                .ToArray();

            Assert.Equal(minutes.Length, minutes.Distinct().Count());

            /* And the map RESPONDED to the order, per VIEW — without this the loop re-asserts the unrotated
               claim thirteen times and an overload that ignored its parameter would pass, which is the
               defect the inline copy this replaced actually embodied. Comparing the two minute SEQUENCES is
               not enough: an overload that ignored `rotated` still returns a permutation of the unrotated
               minutes, so the sequences differ while nothing moved. What has to change is the minute a
               NAMED view gets. */
            Assert.Contains(
                TimescaleSupport.HourlyRefreshPhaseOrder,
                view => TimescaleSupport.RefreshPhaseMinutesFor(rotated, view)
                    != TimescaleSupport.RefreshPhaseMinutesFor(view));
        }

        /* The two overloads agree at the shipped order, so the seam cannot drift from the map the product
           actually uses. */
        foreach (var view in TimescaleSupport.HourlyRefreshPhaseOrder)
        {
            Assert.Equal(
                TimescaleSupport.RefreshPhaseMinutesFor(view),
                TimescaleSupport.RefreshPhaseMinutesFor(TimescaleSupport.HourlyRefreshPhaseOrder, view));
        }

        /* And the seam is as loud as the public overload for a view it does not hold. */
        Assert.Throws<ArgumentOutOfRangeException>(
            () => TimescaleSupport.RefreshPhaseMinutesFor(
                TimescaleSupport.HourlyRefreshPhaseOrder, TimescaleSupport.QueryStatsDailyView));

        /* view -> the relation its own CREATE selects FROM. */
        var sourceOf = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (createSql, view) in definitions)
        {
            var match = Regex.Match(createSql, @"FROM\s+collect\.(\w+)", RegexOptions.IgnoreCase);
            Assert.True(match.Success,
                $"could not recover a source relation from {view}'s CREATE — the guard would group every view under one bucket and report a clean sweep having checked nothing");
            sourceOf[view] = match.Groups[1].Value;
        }

        /* Positive control on the extraction itself, in the identical form: the two relations whose
           multi-consumer shape this test exists for must both come back with the consumers we know they have.
           A control that only proved "some regex matched something" would not exercise the case at issue. */
        Assert.Equal(3, sourceOf.Count(kv => string.Equals(kv.Value, "query_stats", StringComparison.Ordinal)));
        Assert.Equal(2, sourceOf.Count(kv => string.Equals(kv.Value, "query_store_stats", StringComparison.Ordinal)));

        var views = new HashSet<string>(definitions.Select(a => a.View), StringComparer.Ordinal);

        foreach (var relation in sourceOf.Values.Distinct(StringComparer.Ordinal))
        {
            /* Everything that reads the relation, plus the relation itself when it is an hourly view —
               refreshing it WRITES what its consumers read. */
            var contenders = sourceOf.Where(kv => string.Equals(kv.Value, relation, StringComparison.Ordinal))
                .Select(kv => kv.Key)
                .ToList();

            if (views.Contains(relation))
            {
                contenders.Add(relation);
            }

            var phases = contenders.Select(TimescaleSupport.RefreshPhaseMinutesFor).ToArray();

            Assert.True(
                phases.Length == phases.Distinct().Count(),
                $"two hourly refresh policies contending for collect.{relation} start on the same minute: "
                + string.Join(", ", contenders.Zip(phases, (v, m) => $"{v}=:{m.ToString("00", CultureInfo.InvariantCulture)}")));
        }
    }

    /// <summary>
    /// TREATMENT THREE of #3012's family, and the #3185 defect: the light band separates the members whose
    /// runs are long enough for CPU and I/O contention to matter, and it does that WITHOUT widening.
    ///
    /// <para><b>The defect this is written against.</b> Distinct starts is a LOCK guarantee — two refreshes
    /// hold mutually compatible <c>AccessShareLock</c>s, so light refreshes cannot convoy on each other and
    /// the band was sized by member COUNT alone. Sized that way, a one-minute step put
    /// <see cref="TimescaleSupport.QueryStoreStatsHourlyView"/> and
    /// <see cref="TimescaleSupport.QueryStoreStatsCorrectedHourlyView"/> two minutes apart where the previous
    /// grid had them fifteen, and two ~265 s aggregations overlapped for essentially their whole runs at
    /// 5.4x the per-output-group cost, cardinality flat to 1.4%.</para>
    ///
    /// <para><b>Every probe is derived from
    /// <see cref="TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds"/> here, not from
    /// <see cref="TimescaleSupport.UnboundedLightRefreshSeparationMinutes"/>.</b> That member IS the guard,
    /// and asserting a separation against the member that produced it is a tautology that passes at any
    /// value including one minute. The requirement is that the gap covers the recorded light-refresh
    /// CEILING, so the ceiling is what the gap is measured against.</para>
    ///
    /// <para><b>And the band's width is asserted as a SET IDENTITY rather than as a bound.</b> The whole
    /// argument for this shape over a wider step is that the separation is free: a bound like "no light
    /// minute past the span" would pass for a band that had grown and then been re-measured, while the set
    /// identity says the band occupies exactly the positions it always did and the guard, the heaviest
    /// refresh's window and the compression band are therefore untouched.</para>
    /// </summary>
    [Fact]
    public void TheLightBand_SeparatesEveryUnboundedCardinalityRefresh_WithoutWidening()
    {
        var lightViews = TimescaleSupport.HourlyRefreshPhaseOrder
            .Where(view => !string.Equals(
                view, TimescaleSupport.HeaviestHourlyRefreshView, StringComparison.Ordinal))
            .ToArray();

        /* THE BAND DID NOT WIDEN: the light minutes are exactly the band's own positions, so nothing was
           taken from the guard or from the heaviest refresh's window to pay for the separation. */
        Assert.Equal(
            Enumerable
                .Range(0, TimescaleSupport.LightHourlyRefreshCount)
                .Select(index => index * TimescaleSupport.LightRefreshStepMinutes)
                .ToArray(),
            lightViews.Select(TimescaleSupport.RefreshPhaseMinutesFor).OrderBy(minute => minute).ToArray());

        var unbounded = lightViews
            .Where(TimescaleSupport.IsUnboundedCardinalityRefresh)
            .Select(view => (View: view, Minute: TimescaleSupport.RefreshPhaseMinutesFor(view)))
            .OrderBy(placed => placed.Minute)
            .ToArray();

        /* POSITIVE CONTROL, and it is also the one thing that catches a mis-declared static initializer:
           UnboundedCardinalityRefreshViews is a field built from HourlyRefreshDefinitions, and a version of
           it that ran empty would classify every view deployment-bounded and pass every gap assertion below
           for having no gaps to check. */
        Assert.True(
            unbounded.Length > 1,
            "fewer than two light refreshes classify unbounded-cardinality, so the separation this case "
            + "exists for has nothing to separate and every assertion below is vacuous — either the "
            + "registry changed or TimescaleSupport.IsUnboundedCardinalityRefresh has stopped recovering "
            + "GROUP BY terms from the shipped CREATE text (#3185)");

        /* THE REQUIREMENT: consecutive unbounded members cannot overlap at the recorded ceiling. */
        for (var index = 1; index < unbounded.Length; index++)
        {
            var gapSeconds = (unbounded[index].Minute - unbounded[index - 1].Minute) * 60;

            Assert.True(
                gapSeconds >= TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds,
                $"{unbounded[index - 1].View} starts at :{unbounded[index - 1].Minute.ToString("00", CultureInfo.InvariantCulture)} "
                + $"and {unbounded[index].View} at :{unbounded[index].Minute.ToString("00", CultureInfo.InvariantCulture)}, "
                + $"a gap of {gapSeconds.ToString(CultureInfo.InvariantCulture)}s against a recorded light-refresh ceiling of "
                + $"{TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds.ToString(CultureInfo.InvariantCulture)}s — so the two can overlap for "
                + "the difference, which is the #3185 regression");
        }

        /* And the LAST of them clears the heaviest refresh's window, which is the same inequality as the
           band holding them all: the window opens at LightBandSpanMinutes + the guard, and the guard is the
           separation. Asserted in seconds against the ceiling so it is not that identity restated. */
        Assert.True(
            (unbounded[^1].Minute * 60) + TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds
                <= TimescaleSupport.HeaviestRefreshStartMinute * 60,
            $"{unbounded[^1].View} starts at :{unbounded[^1].Minute.ToString("00", CultureInfo.InvariantCulture)} and can run "
            + $"{TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds.ToString(CultureInfo.InvariantCulture)}s, which reaches into the heaviest "
            + $"refresh's window at :{TimescaleSupport.HeaviestRefreshStartMinute.ToString("00", CultureInfo.InvariantCulture)} — the one overlap the "
            + "grid exists to prevent (#3012)");

        /* THE MEASURED CASE, named because it is the pair #3185's readings were taken from. Kept beside the
           general sweep for the reason the query_store_stats family is kept above: the sweep says the
           property holds, this says the property covers the thing that was measured. */
        Assert.True(
            Math.Abs(
                TimescaleSupport.RefreshPhaseMinutesFor(TimescaleSupport.QueryStoreStatsHourlyView)
                - TimescaleSupport.RefreshPhaseMinutesFor(TimescaleSupport.QueryStoreStatsCorrectedHourlyView))
                * 60
            >= TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds,
            "the two views #3185 measured at 5.4x per-group cost are back inside one light-refresh ceiling "
            + "of each other");

        /* THE FEASIBILITY BOUND IS NOT VACUOUS: it admits the shipped count and refuses some larger one, so
           a registry that grew past what the band can separate goes red instead of overlapping quietly. */
        Assert.True(TimescaleSupport.LightBandHoldsUnboundedRefreshCount(unbounded.Length));
        Assert.Contains(
            Enumerable.Range(unbounded.Length + 1, TimescaleSupport.LightHourlyRefreshCount),
            count => !TimescaleSupport.LightBandHoldsUnboundedRefreshCount(count));

        /* AND THE DEGRADED CASE, which is what the map does instead of refusing. Reached through the
           order-taking seam at the registry's unbounded members ALONE: duplicating the whole registry
           cannot reach it, because one light member in four is unbounded today and that is exactly the
           density the separation admits — a doubled list doubles the positions as fast as the members. A
           band that is all unbounded is the shape that does not fit, and it is the shape a run of heavy
           registrations walks toward.

           It degrades rather than throwing because CompressionPhaseMinutes is a static field whose
           initializer reaches this map, and a static initializer that throws costs the whole type for the
           life of the process. Measured, not assumed: mutating the GROUP BY recovery to return nothing
           makes every view classify unbounded, and with a throwing map that took 72 tests red across four
           unrelated files. */
        var allUnbounded = TimescaleSupport.HourlyRefreshPhaseOrder
            .Where(view => !string.Equals(
                view, TimescaleSupport.HeaviestHourlyRefreshView, StringComparison.Ordinal))
            .Where(TimescaleSupport.IsUnboundedCardinalityRefresh)
            .ToArray();

        /* The condition, evaluated over THAT order's band rather than over the shipped one —
           LightBandHoldsUnboundedRefreshCount's public overload reads the shipped registry and answers
           true for three members, because the shipped band has twelve positions for them. A band of three
           has three. */
        Assert.True(
            TimescaleSupport.LightBandIndexForUnboundedRefresh(allUnbounded.Length - 1)
                > allUnbounded.Length - 1,
            "a band of nothing but today's unbounded members would still hold them all, so the degraded "
            + "case below cannot be reached and this half of the case proves nothing");

        var degraded = allUnbounded
            .Select(view => TimescaleSupport.RefreshPhaseMinutesFor(allUnbounded, view))
            .ToArray();

        Assert.Equal(
            Enumerable
                .Range(0, allUnbounded.Length)
                .Select(index => index * TimescaleSupport.LightRefreshStepMinutes)
                .ToArray(),
            degraded);

        /* Distinct starts survive the degradation, which is the guarantee #3012 needed and the one the
           separation is built on top of rather than in place of. */
        Assert.Equal(degraded.Length, degraded.Distinct().Count());

        /* AND THE GAP THE LIVE FINDING JUDGES AGAINST IS THE GAP THE MAP PRODUCED, per class. Raised by
           review: LightRefreshSpacingMinutesFor reads two constants, so on its own it states a PROMISE, and
           a promise nothing measures is what a doc comment says while the code does something else. This
           measures the smallest distance between two of a class's minutes on the shipped map and holds the
           member to it, so a layout change that stopped delivering the gap that member names is red here
           rather than reported against a figure the band never honoured. */
        foreach (var isUnbounded in new[] { true, false })
        {
            var minutes = lightViews
                .Where(view => TimescaleSupport.IsUnboundedCardinalityRefresh(view) == isUnbounded)
                .Select(TimescaleSupport.RefreshPhaseMinutesFor)
                .OrderBy(minute => minute)
                .ToArray();

            Assert.True(minutes.Length > 1, $"the {(isUnbounded ? "unbounded" : "bounded")} class has "
                + "fewer than two members on the band, so it has no gap to measure and this claim is vacuous");

            var achieved = minutes.Zip(minutes.Skip(1), (earlier, later) => later - earlier).Min();
            var member = lightViews.First(view =>
                TimescaleSupport.IsUnboundedCardinalityRefresh(view) == isUnbounded);

            Assert.Equal(TimescaleSupport.LightRefreshSpacingMinutesFor(member), achieved);
        }
    }

    /// <summary>
    /// MEMBERSHIP IS DERIVED FROM THE SHIPPED CREATE TEXT, and every grouping term in every registered
    /// hourly aggregate has to be classified deliberately.
    ///
    /// <para><b>Why the rule is a list of COLUMNS and this is the guard that keeps it honest.</b> A list of
    /// heavy VIEWS is a frozen enumeration — right until the next aggregate is registered, then silently
    /// wrong, which is the positional coupling #3174 removed one level over. A view's one-bucket output
    /// cardinality is decided by its own GROUP BY, so
    /// <see cref="TimescaleSupport.IsUnboundedCardinalityRefresh"/> reads that. What can still go stale is
    /// the COLUMN list: an aggregate registered tomorrow could group by a per-statement column nobody has
    /// classified, and the quiet answer — "not on the per-statement list, therefore bounded" — is the answer
    /// that gives it the least room. So every term is required to appear on one of the two lists, and the
    /// bounded half lives HERE: it has no product consumer, and a new grouping column has to be classified
    /// in a file that is red until someone does it.</para>
    ///
    /// <para><b>The parse is controlled per view before its result is used.</b> A recovery that matched
    /// nothing would return no terms for every view, and no terms contains no per-statement column — so a
    /// broken parse reports every aggregate deployment-bounded and a clean sweep for having checked nothing.
    /// Two named views' exact term lists are asserted, one from each class, which is what discriminates a
    /// working parse from one that merely returned something.</para>
    /// </summary>
    [Fact]
    public void EveryGroupingTerm_IsClassified_AndTheParseIsControlledPerView()
    {
        /* Bounded by the DEPLOYMENT: servers, databases, schema objects, small enumerations, and the
           collection instants a cadence produces. None of these grows with the monitored workload's
           distinct statement population, which is what makes the cost of grouping by them predictable. */
        var deploymentBounded = new[]
        {
            "server_id",
            "server_name",
            "database_name",
            "module_name",
            "schema_name",
            "object_name",
            "execution_type_desc",
            "replica_role",
            "bucket",
            "collection_time",
        };

        foreach (var (createSql, view) in TimescaleSupport.HourlyRefreshDefinitions)
        {
            var terms = TimescaleSupport.RefreshGroupingTermsFor(createSql);

            Assert.True(
                terms.Count > 0,
                $"no GROUP BY terms could be recovered from {view}'s CREATE, so its refresh cost class is "
                + "being decided by an empty list — TimescaleSupport.RefreshGroupingTermsFor has stopped "
                + "matching the shipped text (#3185)");

            foreach (var term in terms)
            {
                Assert.True(
                    TimescaleSupport.PerStatementGroupingColumns.Contains(term, StringComparer.Ordinal)
                    || deploymentBounded.Contains(term, StringComparer.Ordinal)
                    || term.StartsWith("time_bucket(", StringComparison.Ordinal),
                    $"{view} groups by `{term}`, which is on neither TimescaleSupport."
                    + "PerStatementGroupingColumns nor this case's deployment-bounded list. Decide which it "
                    + "is: a column whose distinct count grows with the monitored workload's statement "
                    + "population belongs on the first list and its view gets "
                    + "UnboundedLightRefreshSeparationMinutes of the band; one bounded by the size of the "
                    + "deployment belongs on the second and its view keeps LightRefreshStepMinutes. "
                    + "Unclassified defaults to bounded, which is the answer that gives it the least room "
                    + "(#3185)");
            }
        }

        /* PARSE CONTROL, one view from each class, spelled out. Depth-zero splitting is what makes the
           hierarchical view's time_bucket() come back as ONE term: a plain comma split yields a term of
           `time_bucket('1 hour'` and one of `bucket)`, and the second of those is a real column name. */
        Assert.Equal(
            new[] { "server_id", "server_name", "database_name", "bucket" },
            TimescaleSupport.RefreshGroupingTermsFor(TimescaleSupport.CreateQueryStatsDbHourlySql));

        Assert.Equal(
            new[] { "server_id", "server_name", "database_name", "module_name", "query_hash", "time_bucket('1 hour', bucket)" },
            TimescaleSupport.RefreshGroupingTermsFor(TimescaleSupport.CreateQueryStoreStatsCorrectedHourlySql));

        /* And the two classes both have members, so neither branch of the rule is dead. */
        var byClass = TimescaleSupport.HourlyRefreshPhaseOrder
            .GroupBy(TimescaleSupport.IsUnboundedCardinalityRefresh)
            .ToDictionary(group => group.Key, group => group.Count());

        Assert.True(byClass.TryGetValue(true, out var unboundedCount) && unboundedCount > 0);
        Assert.True(byClass.TryGetValue(false, out var boundedCount) && boundedCount > 0);

        /* A view the registry does not hold has no CREATE to classify, and the answer is loud rather than
           the quiet "bounded" a defaulted lookup would give it. */
        Assert.Throws<ArgumentOutOfRangeException>(
            () => TimescaleSupport.IsUnboundedCardinalityRefresh(TimescaleSupport.QueryStatsDailyView));

        /* THE FAIL-SAFE ARM, exercised at the only surface that can reach it. Nothing in the registry is
           unparseable, so this branch is unreachable through the shipped definitions — and an unreachable
           guard reads as protection while certifying nothing. A CREATE with no GROUP BY at all classifies
           UNBOUNDED, which gives that aggregate the wider gap and makes the band's feasibility bound the
           thing that reports the problem, rather than handing it a one-minute step in silence. */
        Assert.True(TimescaleSupport.GroupKeyIsUnboundedCardinality(
            "CREATE MATERIALIZED VIEW collect.nothing AS SELECT 1 WITH NO DATA"));

        /* And the arm is not a blanket true: a parseable, deployment-bounded key still answers false
           through the same function, so the fail-safe cannot be what every view is riding on. */
        Assert.False(TimescaleSupport.GroupKeyIsUnboundedCardinality(
            TimescaleSupport.CreateQueryStatsDbHourlySql));
        Assert.True(TimescaleSupport.GroupKeyIsUnboundedCardinality(
            TimescaleSupport.CreateQueryStoreStatsHourlySql));
    }

    /// <summary>
    /// THE STEP CANNOT BE WIDENED, which is #3185's first candidate repair answered in the build rather
    /// than in a discussion.
    ///
    /// <para>"Give the light band a step sized by member duration" is the obvious reading of a band that
    /// overlaps its own members, and it reads like a trade someone could choose to make against
    /// <see cref="TimescaleSupport.HeaviestRefreshWindowMinutes"/>. It is not a trade:
    /// <see cref="TimescaleSupport.WidestFeasibleLightRefreshStepMinutes"/> returns the SHIPPED step, so the
    /// band is already as wide as the hour carries and every wider step is red. The binding constraint is
    /// not the hour's sixty minutes — it is the heaviest refresh's watch line, which is why the infeasible
    /// answer arrives two minutes in rather than at the fifty-five a duration-derived step would ask
    /// for.</para>
    ///
    /// <para>Asserted at step + 1 as well, so the bound is TIGHT rather than merely true: a member that
    /// returned the shipped step by reading it would pass the first assertion and fail this one.</para>
    /// </summary>
    [Fact]
    public void TheWidestFeasibleLightRefreshStep_IsTheShippedOne_SoADurationDerivedStepIsRed()
    {
        Assert.Equal(
            TimescaleSupport.LightRefreshStepMinutes,
            TimescaleSupport.WidestFeasibleLightRefreshStepMinutes);

        /* The opened-up chain agrees with the shipped one where they overlap, so it cannot drift into being
           a second and kinder model of the grid — the requirement #3182 put on its guard twin. */
        Assert.Equal(
            TimescaleSupport.RefreshSlotWarningSeconds,
            TimescaleSupport.RefreshSlotWarningSecondsForLightBandAndGuard(
                TimescaleSupport.LightBandSpanMinutes, TimescaleSupport.CompressionPhaseGuardMinutes));

        Assert.Equal(
            TimescaleSupport.RefreshSlotWarningSecondsForGuardMinutes(
                TimescaleSupport.CompressionPhaseGuardMinutes),
            TimescaleSupport.RefreshSlotWarningSecondsForLightBandAndGuard(
                TimescaleSupport.LightBandSpanMinutes, TimescaleSupport.CompressionPhaseGuardMinutes));

        /* TIGHTNESS: one minute wider and the grid's own precondition is already false. */
        var oneWider = (TimescaleSupport.LightHourlyRefreshCount - 1)
            * (TimescaleSupport.LightRefreshStepMinutes + 1);

        Assert.False(
            TimescaleSupport.HeaviestHourlyRefreshObservedCeilingSeconds
                < TimescaleSupport.RefreshSlotWarningSecondsForLightBandAndGuard(
                    oneWider, TimescaleSupport.CompressionPhaseGuardMinutes),
            $"a {(TimescaleSupport.LightRefreshStepMinutes + 1).ToString(CultureInfo.InvariantCulture)}-minute light step still leaves a watch line above "
            + "the heaviest refresh's recorded ceiling, so WidestFeasibleLightRefreshStepMinutes is "
            + "understating what the hour carries and the argument for separating inside the band instead of "
            + "widening it no longer holds (#3185)");

        /* And the step a duration derivation would ask for — the light ceiling rounded up, which is the
           same rounding CompressionPhaseGuardMinutes uses — is infeasible, which is the whole finding. */
        var durationDerivedStep =
            (int)Math.Ceiling(TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds / 60.0);

        Assert.True(
            durationDerivedStep > TimescaleSupport.WidestFeasibleLightRefreshStepMinutes,
            "a light step derived from the light-refresh ceiling now fits inside what the hour carries, so "
            + "the uniform shape #3185 rejected has become available and the interleaved layout is no "
            + "longer the only one that fits");
    }

    /// <summary>
    /// The phase grid is keyed on the VIEW, and nothing shipped carries a job id.
    ///
    /// <para>#3012's evidence names job ids in the 1050s. Those exist only on the store it was measured
    /// against and would name entirely different jobs anywhere else, so a fix that encoded one would be
    /// correct on exactly one deployment. The converge path recovers the view name with a join rather than
    /// reading an id, because a refresh job's own <c>hypertable_name</c> is the aggregate's per-deployment
    /// materialization hypertable.</para>
    /// </summary>
    [Fact]
    public void RefreshPolicyIdentity_IsTheView_NeverAJobId()
    {
        var read = TimescaleSupport.ContinuousAggregateRefreshStateSql;

        Assert.Contains("j.proc_name = 'policy_refresh_continuous_aggregate'", read, StringComparison.Ordinal);
        Assert.Contains("ca.view_name", read, StringComparison.Ordinal);
        /* BOTH identities, because timescaledb_information.jobs resolves a continuous-aggregate job back to
           its USER VIEW rather than reporting the materialization hypertable the bgw_job row actually carries.
           The materialization-only form read back nothing at all against a live store, so matching only the
           name the catalog columns imply is the shape that already failed once. */
        Assert.Contains("ca.view_name = j.hypertable_name", read, StringComparison.Ordinal);
        Assert.Contains("ca.materialization_hypertable_name = j.hypertable_name", read, StringComparison.Ordinal);
        Assert.Contains("ca.view_schema = 'collect'", read, StringComparison.Ordinal);

        /* Compared as SECONDS, so '1 day' / '1 day 00:00:00' / '24:00:00' cannot read as a difference and
           re-alter the same job on every start; and the phase read in UTC for the same reason the write is. */
        Assert.Contains("EXTRACT(EPOCH FROM (j.config->>'start_offset')::interval)::bigint", read, StringComparison.Ordinal);
        Assert.Contains("EXTRACT(MINUTE FROM j.initial_start AT TIME ZONE 'UTC')::int", read, StringComparison.Ordinal);

        var write = TimescaleSupport.SetContinuousAggregateRefreshSql;

        /* The job id reaches alter_job ONLY as a bound parameter, cast ::integer (the #1586 trap). */
        Assert.Contains("j.job_id = $1::integer", write, StringComparison.Ordinal);
        Assert.Contains("jsonb_set(j.config, '{start_offset}', to_jsonb($2::text))", write, StringComparison.Ordinal);
        Assert.Contains("fixed_schedule => true", write, StringComparison.Ordinal);
        Assert.Contains("initial_start =>", write, StringComparison.Ordinal);

        /* Every un-named alter_job parameter means "leave unchanged", so the converge cannot arm a paused job
           or retune a cadence. Naming `scheduled` here would weaken #1680's never-expose-an-armed-window
           discipline through a path that has nothing to do with retention. */
        Assert.DoesNotContain("scheduled =>", write, StringComparison.Ordinal);
        Assert.DoesNotContain("schedule_interval =>", write, StringComparison.Ordinal);
        Assert.DoesNotContain("next_start =>", write, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStoreStatsHourly_GroupedByComposerDims_CarriesWeightedSums()
    {
        var sql = TimescaleSupport.CreateQueryStoreStatsHourlySql;

        Assert.Contains("CREATE MATERIALIZED VIEW IF NOT EXISTS collect.query_store_stats_hourly", sql, StringComparison.Ordinal);
        Assert.Contains("WITH (timescaledb.continuous)", sql, StringComparison.Ordinal);
        Assert.Contains("time_bucket('1 hour', collection_time) AS bucket", sql, StringComparison.Ordinal);
        Assert.Contains("FROM collect.query_store_stats", sql, StringComparison.Ordinal);
        /* The COMPOSER's QS dimensions (module_name / query_hash) so a composed QS panel can route here — NOT
           Query Store's own query_id / plan_id, which the composer never exposes. */
        Assert.Contains("GROUP BY server_id, server_name, database_name, module_name, query_hash, bucket", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("query_id", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("plan_id", sql, StringComparison.Ordinal);
        /* Execution-WEIGHTED sums so the composer's weighted mean = duration_us_weighted_sum / execution_count_sum
           composes EXACTLY (avg*count = the interval total, summed = the true total) — never an avg-of-avgs. */
        Assert.Contains("sum(execution_count) AS execution_count_sum", sql, StringComparison.Ordinal);
        Assert.Contains("sum(avg_duration_us::double precision * execution_count) AS duration_us_weighted_sum", sql, StringComparison.Ordinal);
        Assert.Contains("sum(avg_cpu_time_us::double precision * execution_count) AS cpu_us_weighted_sum", sql, StringComparison.Ordinal);
        Assert.Contains("max(max_duration_us) AS max_duration_us_max", sql, StringComparison.Ordinal);
        Assert.Contains("max(max_cpu_time_us) AS max_cpu_time_us_max", sql, StringComparison.Ordinal);
        /* The imprecise avg-of-avgs shape is gone. */
        Assert.DoesNotContain("avg(avg_duration_us)", sql, StringComparison.Ordinal);
        Assert.Contains("count(*) AS sample_count", sql, StringComparison.Ordinal);
        Assert.Contains("WITH NO DATA", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStoreStatsDaily_IsHierarchical_FromQsHourly_SameColumnsAsHourly()
    {
        var sql = TimescaleSupport.CreateQueryStoreStatsDailySql;

        Assert.Contains("CREATE MATERIALIZED VIEW IF NOT EXISTS collect.query_store_stats_daily", sql, StringComparison.Ordinal);
        Assert.Contains("FROM collect.query_store_stats_hourly", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY server_id, server_name, database_name, module_name, query_hash, time_bucket('1 day', bucket)", sql, StringComparison.Ordinal);
        /* Same column NAMES as the hourly (so ComposeCaggValueMapper reads both unchanged): SUM the weighted sums,
           MAX the peaks, SUM executions + sample_count. */
        Assert.Contains("sum(duration_us_weighted_sum) AS duration_us_weighted_sum", sql, StringComparison.Ordinal);
        Assert.Contains("sum(cpu_us_weighted_sum) AS cpu_us_weighted_sum", sql, StringComparison.Ordinal);
        Assert.Contains("sum(execution_count_sum) AS execution_count_sum", sql, StringComparison.Ordinal);
        Assert.Contains("max(max_duration_us_max) AS max_duration_us_max", sql, StringComparison.Ordinal);
        Assert.Contains("max(max_cpu_time_us_max) AS max_cpu_time_us_max", sql, StringComparison.Ordinal);
        Assert.Contains("sum(sample_count) AS sample_count", sql, StringComparison.Ordinal);
        Assert.Contains("WITH NO DATA", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStatsDaily_IsHierarchical_SourcedFromHourlyCagg_GroupedByExplicitDayBucket()
    {
        var sql = TimescaleSupport.CreateQueryStatsDailySql;

        Assert.Contains("CREATE MATERIALIZED VIEW IF NOT EXISTS collect.query_stats_daily", sql, StringComparison.Ordinal);
        Assert.Contains("WITH (timescaledb.continuous)", sql, StringComparison.Ordinal);
        /* HIERARCHICAL: sourced from the hourly CAGG, NOT raw. */
        Assert.Contains("FROM collect.query_stats_hourly", sql, StringComparison.Ordinal);
        Assert.Contains("time_bucket('1 day', bucket) AS bucket", sql, StringComparison.Ordinal);
        /* GROUP BY uses the explicit time_bucket EXPRESSION, never the bare `bucket` alias: a bare alias binds to
           the hourly source column under Postgres's input-column-wins rule and would group by hour, not day. */
        Assert.Contains("GROUP BY server_id, server_name, database_name, query_hash, sql_handle, time_bucket('1 day', bucket)", sql, StringComparison.Ordinal);
        /* Re-aggregates the hourly rollup: SUM of sums, MIN of mins, MAX of maxes, SUM of the sample_counts. */
        Assert.Contains("sum(worker_time_sum) AS worker_time_sum", sql, StringComparison.Ordinal);
        Assert.Contains("min(worker_time_min) AS worker_time_min", sql, StringComparison.Ordinal);
        Assert.Contains("max(worker_time_max) AS worker_time_max", sql, StringComparison.Ordinal);
        Assert.Contains("sum(sample_count) AS sample_count", sql, StringComparison.Ordinal);
        Assert.Contains("WITH NO DATA", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ProcedureStatsDaily_MirrorsQueryStatsDaily_FromProcedureHourly_ByObjectName()
    {
        var sql = TimescaleSupport.CreateProcedureStatsDailySql;

        Assert.Contains("CREATE MATERIALIZED VIEW IF NOT EXISTS collect.procedure_stats_daily", sql, StringComparison.Ordinal);
        Assert.Contains("FROM collect.procedure_stats_hourly", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY server_id, server_name, database_name, schema_name, object_name, time_bucket('1 day', bucket)", sql, StringComparison.Ordinal);
        Assert.Contains("sum(sample_count) AS sample_count", sql, StringComparison.Ordinal);
        Assert.Contains("WITH NO DATA", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The DAILY tier keeps its 3-day window and stays OFF the phase grid — the regression #3012 invites is a
    /// tidy-up that makes every refresh window "consistent".
    ///
    /// <para>Narrowing this would trade nothing for a correctness risk. A daily job's 3-day window is 3 days
    /// of scan against an 86,400-second cadence rather than a 3,600-second one, so it was never near its own
    /// schedule interval and never appeared in the convoy; and the 3 days are the buffer
    /// <see cref="TimescaleSupport.HourlyRetentionInterval"/> leans on, so the hourly rollups' drop at 90 days
    /// can never outrun the daily aggregate meant to preserve that history.</para>
    ///
    /// <para><see cref="TimescaleSupport.RefreshPhaseMinutesFor"/> is asserted to THROW for every daily view,
    /// which is what stops one being dragged onto the grid by a caller that only knows it has a view name.</para>
    /// </summary>
    [Fact]
    public void DailyPolicy_UsesOneDayEndOffsetAndSchedule_KeepsThreeDayStart()
    {
        Assert.Equal("3 days", TimescaleSupport.DailyRefreshStartOffset);
        Assert.Equal(TimeSpan.FromDays(3), TimescaleSupport.DailyRefreshStartSpan);

        foreach (var view in new[]
        {
            TimescaleSupport.QueryStatsDailyView,
            TimescaleSupport.ProcedureStatsDailyView,
            TimescaleSupport.QueryStoreStatsDailyView,
            TimescaleSupport.QueryStoreStatsCorrectedDailyView,
            TimescaleSupport.QueryStatsDbDailyView,
            TimescaleSupport.QueryStoreStatsIntervalDailyView,
            TimescaleSupport.QueryStoreStatsDayGrainDailyView,
        })
        {
            var sql = TimescaleSupport.AddDailyRefreshPolicySql(view);

            Assert.Contains($"add_continuous_aggregate_policy('collect.{view}'", sql, StringComparison.Ordinal);
            Assert.Contains("start_offset => INTERVAL '3 days'", sql, StringComparison.Ordinal);
            Assert.Contains("end_offset => INTERVAL '1 day'", sql, StringComparison.Ordinal);
            Assert.Contains("schedule_interval => INTERVAL '1 day'", sql, StringComparison.Ordinal);
            Assert.Contains("if_not_exists => true", sql, StringComparison.Ordinal);

            /* No initial_start: the daily tier keeps TimescaleDB's finish-to-start scheduling, untouched. */
            Assert.DoesNotContain("initial_start", sql, StringComparison.Ordinal);

            Assert.Throws<ArgumentOutOfRangeException>(() => TimescaleSupport.RefreshPhaseMinutesFor(view));
        }

        Assert.True(TimescaleSupport.DailyRefreshStartSpan < TimescaleSupport.HourlyRetentionSpan,
            "the hourly rollups' retention must outlive the daily refresh window, or a drop outruns the daily aggregate");
    }

    [Fact]
    public void RetentionPolicy_IsIdempotentChunkDrop()
    {
        var sql = TimescaleSupport.AddRetentionPolicySql("query_stats", TimescaleSupport.RawRetentionInterval);

        Assert.Contains("add_retention_policy('collect.query_stats'", sql, StringComparison.Ordinal);
        Assert.Contains("drop_after => INTERVAL '4 days'", sql, StringComparison.Ordinal);
        Assert.Contains("if_not_exists => true", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void RetentionTiers_RawFourDays_HourlyNinetyDays_StayPastTheNextRefreshWindow()
    {
        /* The buffers the whole tiering rests on: raw's 4d stays past the hourly CAGG's refresh start; the
           hourly CAGGs' 90d stays past the daily CAGG's refresh start. Either horizon dropping below the
           refresh window beneath it would let a drop outrun the aggregate meant to preserve that history —
           asserted against the refresh constants themselves in HourlyRefreshWindow_IsOneDay_... and
           DailyPolicy_UsesOneDayEndOffsetAndSchedule_..., so the pair cannot drift.

           The hourly number is 90 rather than 21 since #1937, and the reason is a READ one rather than a refresh
           one: the viewer offers month-plus windows, and at 21 days a 30-day view could not render at hourly
           grain at all. The refresh-window buffer this test is about is satisfied either way — widening never
           threatened it — so the pin is here to catch a future NARROWING, which would. */
        Assert.Equal("4 days", TimescaleSupport.RawRetentionInterval);
        Assert.Equal("90 days", TimescaleSupport.HourlyRetentionInterval);
    }

    /* ── #1661: the per-database rollup carrying the I/O sums no other aggregate has ── */

    /// <summary>
    /// FinOps' database-grain workload view sums delta_logical_reads / delta_physical_reads /
    /// delta_logical_writes, and NO other continuous aggregate carries any of them — the others were built to the
    /// composer's measure set, which never exposed I/O. Without this rollup that view cannot route past the raw
    /// horizon at all.
    /// </summary>
    [Fact]
    public void QueryStatsDbHourly_CarriesTheIoSumsNoOtherAggregateHas()
    {
        var sql = TimescaleSupport.CreateQueryStatsDbHourlySql;

        Assert.Contains("CREATE MATERIALIZED VIEW IF NOT EXISTS collect.query_stats_db_hourly", sql, StringComparison.Ordinal);
        Assert.Contains("WITH (timescaledb.continuous)", sql, StringComparison.Ordinal);
        Assert.Contains("time_bucket('1 hour', collection_time) AS bucket", sql, StringComparison.Ordinal);
        Assert.Contains("FROM collect.query_stats", sql, StringComparison.Ordinal);

        foreach (var column in new[] { "delta_logical_reads", "delta_physical_reads", "delta_logical_writes", "delta_worker_time", "delta_execution_count" })
        {
            Assert.Contains("sum(" + column + ")", sql, StringComparison.Ordinal);
        }

        Assert.Contains("count(*) AS sample_count", sql, StringComparison.Ordinal);

        /* The I/O columns must exist HERE and nowhere else — if they are ever added to the query-grain aggregate
           this rollup becomes redundant, and that should be a deliberate decision, not a silent duplication. */
        foreach (var io in new[] { "delta_logical_reads", "delta_physical_reads", "delta_logical_writes" })
        {
            Assert.DoesNotContain(io, TimescaleSupport.CreateQueryStatsHourlySql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// Grouped by database_name and deliberately NOT query_hash. That coarser grain is what keeps it small despite
    /// carrying more columns than the query-grain aggregate — one row per database per hour, not one per query.
    /// </summary>
    [Fact]
    public void QueryStatsDbHourly_IsDatabaseGrain_NotQueryGrain()
    {
        var sql = TimescaleSupport.CreateQueryStatsDbHourlySql;

        Assert.Contains("GROUP BY server_id, server_name, database_name, bucket", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("query_hash", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The daily sibling must be HIERARCHICAL — sourced from the hourly rollup, never from raw. Sourcing a daily
    /// aggregate from raw would make it collapse to whatever raw still retains (4 days), which is the entire
    /// failure this tiering exists to prevent.
    /// </summary>
    [Fact]
    public void QueryStatsDbDaily_IsHierarchical_SourcedFromTheHourlyRollup()
    {
        var sql = TimescaleSupport.CreateQueryStatsDbDailySql;

        Assert.Contains("CREATE MATERIALIZED VIEW IF NOT EXISTS collect.query_stats_db_daily", sql, StringComparison.Ordinal);
        Assert.Contains("FROM collect.query_stats_db_hourly", sql, StringComparison.Ordinal);
        Assert.Contains("time_bucket('1 day', bucket) AS bucket", sql, StringComparison.Ordinal);

        /* Re-aggregates the hourly SUMS, never the raw deltas. */
        foreach (var column in new[] { "logical_reads_sum", "physical_reads_sum", "logical_writes_sum", "worker_time_sum", "execution_count_sum" })
        {
            Assert.Contains("sum(" + column + ")", sql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The rollup filters out rows the FinOps view itself excludes (delta_worker_time IS NULL), so the
    /// pre-aggregated totals match what the raw query would have produced rather than including
    /// never-counted rows.
    /// </summary>
    [Fact]
    public void QueryStatsDbHourly_ExcludesNullWorkerTime_MatchingTheReadersFilter() =>
        Assert.Contains("WHERE delta_worker_time IS NOT NULL", TimescaleSupport.CreateQueryStatsDbHourlySql, StringComparison.Ordinal);

    /* -- #1680: retention policies are created paused, and only armed when provably safe -- */

    /// <summary>
    /// TimescaleDB runs a new retention policy's first check IMMEDIATELY at creation, not on its next interval.
    /// A policy created live therefore drops before any external session can pause it - there is no window to
    /// win, and on a field store that cost two days of history permanently. Creation must be paused.
    /// </summary>
    [Fact]
    public void RetentionPolicies_AreCreatedPaused()
    {
        var sql = TimescaleSupport.AddRetentionPolicySql("query_stats", "4 days");

        Assert.Contains("add_retention_policy('collect.query_stats'", sql, StringComparison.Ordinal);
        Assert.Contains("if_not_exists => true", sql, StringComparison.Ordinal);

        /* The pause is a SEPARATE statement run in the same transaction, targeted by job id. */
        Assert.Contains("alter_job($1::integer, scheduled => false)", TimescaleSupport.PauseJobSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #1705: <c>add_retention_policy</c> accepts no <c>scheduled</c> argument on ANY TimescaleDB 2.x — the
    /// parameter belongs to <c>add_job</c> / <c>alter_job</c>. Passing it made this statement fail with 42883 on
    /// every store, and the per-policy catch downgraded that to a warning, so retention silently stopped existing
    /// fleet-wide. The old pin asserted the string contained <c>scheduled =&gt; false</c> and therefore passed
    /// against SQL no version could parse; this asserts the opposite, so the bug cannot come back the same way.
    /// </summary>
    [Fact]
    public void AddRetentionPolicy_NeverPassesScheduled_ItIsNotAnArgumentOfThatFunction()
    {
        var sql = TimescaleSupport.AddRetentionPolicySql("query_stats", "4 days");

        Assert.DoesNotContain("scheduled", sql, StringComparison.OrdinalIgnoreCase);

        /* The accepted 2.28.1 signature is (regclass, "any", boolean, interval, timestamptz, text, interval);
           these are the only named arguments this statement may use. */
        foreach (var illegal in new[] { "scheduled =>", "paused =>", "enabled =>" })
        {
            Assert.DoesNotContain(illegal, sql, StringComparison.OrdinalIgnoreCase);
        }
    }

    /// <summary>
    /// Arming is a separate statement, targeted at the retention job for one relation. It must filter by
    /// proc_name AND the hypertable, so it can never arm some other policy - or every policy - by accident.
    /// </summary>
    [Fact]
    public void ArmRetentionPolicy_TargetsExactlyOneRelationsRetentionJob()
    {
        var sql = TimescaleSupport.ArmRetentionPolicySql("query_stats");

        Assert.Contains("scheduled => true", sql, StringComparison.Ordinal);
        Assert.Contains("proc_name = 'policy_retention'", sql, StringComparison.Ordinal);
        Assert.Contains("hypertable_name = 'query_stats'", sql, StringComparison.Ordinal);
        Assert.Contains("hypertable_schema = 'collect'", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #1877: holding is the exact mirror of arming — same targeting, opposite flag — because a policy whose
    /// coverage list GREW under it has to be stopped, and <c>add_retention_policy(if_not_exists)</c> never
    /// pauses a policy the store already has.
    ///
    /// <para>The two statements are asserted to disagree on the flag and agree on everything else. A hold that
    /// missed the hypertable filter would stop every retention policy in the store on one shallow consumer;
    /// one that said <c>true</c> would arm the very policy the gate just judged unsafe, which is the data-loss
    /// direction and would look identical in a test that only checked the targeting.</para>
    /// </summary>
    [Fact]
    public void HoldRetentionPolicy_IsTheMirrorOfArming_SameTargetOppositeFlag()
    {
        var hold = TimescaleSupport.HoldRetentionPolicySql("query_stats");
        var arm = TimescaleSupport.ArmRetentionPolicySql("query_stats");

        Assert.Contains("scheduled => false", hold, StringComparison.Ordinal);
        Assert.DoesNotContain("scheduled => true", hold, StringComparison.Ordinal);

        foreach (var filter in new[]
        {
            "proc_name = 'policy_retention'",
            "hypertable_schema = 'collect'",
            "hypertable_name = 'query_stats'",
        })
        {
            Assert.Contains(filter, hold, StringComparison.Ordinal);
        }

        /* Identical apart from the flag - the pair is one statement with a boolean, so they cannot drift. */
        Assert.Equal(arm, hold.Replace("scheduled => false", "scheduled => true", StringComparison.Ordinal));
    }

    /// <summary>
    /// The safety probe compares the source's oldest row against the coverage tier's oldest bucket. Raw tables
    /// key on collection_time, rollups on bucket - reading the wrong column would compare against nothing and
    /// silently decide it was safe to arm.
    /// </summary>
    [Theory]
    [InlineData("query_stats", "collection_time", "query_stats_hourly")]
    [InlineData("query_stats_hourly", "bucket", "query_stats_daily")]
    public void RetentionArmSafetySql_ComparesSourceOldestToCoverageOldest(string relation, string timeColumn, string coverage)
    {
        var sql = TimescaleSupport.RetentionArmSafetySql(relation, timeColumn, new[] { coverage });

        Assert.Contains($"min({timeColumn}) FROM collect.{relation}", sql, StringComparison.Ordinal);
        Assert.Contains($"min(bucket) FROM collect.{coverage}", sql, StringComparison.Ordinal);
        Assert.Contains("AS source_oldest", sql, StringComparison.Ordinal);
        Assert.Contains("AS coverage_oldest", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// WATCHED (mutation): make the arming gate emit only the FIRST coverage relation and this goes red.
    /// A raw table can have more than one rollup family reading it (#1849 — query_store_stats feeds both the
    /// original inflated pair and the corrected one), and a purge that satisfied only one of them would drop
    /// raw history the other has never materialized. Every named consumer must appear in the statement, each
    /// as its own column, because the verdict is an AND the READER evaluates — folding them into one
    /// <c>GREATEST</c> would skip NULLs and let an EMPTY rollup vanish from the comparison entirely.
    /// </summary>
    [Fact]
    public void RetentionArmSafetySql_NamesEveryCoverageRelation()
    {
        var coverage = new[]
        {
            TimescaleSupport.QueryStoreStatsHourlyView,
            TimescaleSupport.QueryStoreStatsIntervalHourlyView,
        };

        var sql = TimescaleSupport.RetentionArmSafetySql("query_store_stats", "collection_time", coverage);

        foreach (var view in coverage)
        {
            Assert.Contains($"min(bucket) FROM collect.{view}", sql, StringComparison.Ordinal);
        }

        /* One column per consumer, indexed — the reader walks them positionally. */
        Assert.Contains("AS coverage_oldest_0", sql, StringComparison.Ordinal);
        Assert.Contains("AS coverage_oldest_1", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("GREATEST", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The map both purge paths read (#1784) must name BOTH Query Store rollup families as raw's coverage.
    /// query_store_stats is the only raw table with two consumers; naming just one would let raw purge over
    /// history the other never materialized, which is the #1790-class race the corrected rollups introduce.
    /// </summary>
    [Fact]
    public void RawTierCoverage_QueryStoreStats_RequiresBothRollupFamilies()
    {
        var entry = TimescaleSupport.RawTierCoverage.Single(t =>
            string.Equals(t.Relation, "query_store_stats", StringComparison.Ordinal));

        Assert.Contains(TimescaleSupport.QueryStoreStatsHourlyView, entry.Coverage);
        Assert.Contains(TimescaleSupport.QueryStoreStatsIntervalHourlyView, entry.Coverage);

        /* The interval tier must outlive raw or the gate can never be satisfied in steady state: raw's
           newest-dropped chunk and L1's oldest-kept bucket would sit at the same age. */
        Assert.True(TimescaleSupport.IntervalRetentionSpan > TimescaleSupport.RawRetentionSpan,
            "the interval-grain dedup layer must be retained strictly longer than raw, or arming raw's purge " +
            "races its own coverage gate.");
    }

    /* ─────────────── the DAY-grain corrected daily (#1869) ─────────────── */

    /// <summary>
    /// L2: L1 re-deduped at the DAY grain. The two facts that make it work are the source edge (it reads L1,
    /// not raw — a CAGG on query_store_stats cannot bucket on anything but collection_time) and
    /// <c>last(x, bucket)</c> over the full INTERVAL IDENTITY, which is what collapses an interval's two
    /// hourly rows back to one. A <c>sum</c> here would reproduce exactly the residual this exists to remove.
    /// </summary>
    [Fact]
    public void QueryStoreStatsIntervalDaily_RededupsTheIntervalAcrossTheDay_FromL1()
    {
        var sql = TimescaleSupport.CreateQueryStoreStatsIntervalDailySql;

        Assert.Contains("CREATE MATERIALIZED VIEW IF NOT EXISTS collect.query_store_stats_interval_daily", sql, StringComparison.Ordinal);
        Assert.Contains("WITH (timescaledb.continuous)", sql, StringComparison.Ordinal);
        Assert.Contains($"FROM collect.{TimescaleSupport.QueryStoreStatsIntervalHourlyView}", sql, StringComparison.Ordinal);

        /* WIDENING, and that is what makes the level legal at all: an identity-width hierarchical CAGG is a
           LEAF, so a 1-hour re-dedup over L1 could exist but nothing could be built on it. */
        Assert.Contains("time_bucket('1 day', bucket) AS bucket", sql, StringComparison.Ordinal);

        /* The dedup itself — LAST, never SUM, and ordered by the parent's bucket because that is the only
           time column an L1 row carries. */
        Assert.Contains("last(execution_count, bucket) AS execution_count", sql, StringComparison.Ordinal);
        Assert.Contains("last(avg_duration_us, bucket) AS avg_duration_us", sql, StringComparison.Ordinal);
        Assert.Contains("last(avg_cpu_time_us, bucket) AS avg_cpu_time_us", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("sum(execution_count)", sql, StringComparison.Ordinal);

        /* Grouped on the SAME interval identity L1 keys on, both generations included (#1853): the real id and
           the tier-1 proxy, deliberately not COALESCEd into one column. */
        Assert.Contains("runtime_stats_interval_id, first_execution_time,", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY server_id, server_name, database_name, module_name, query_hash, query_id, plan_id,", sql, StringComparison.Ordinal);

        /* sample_count keeps meaning RAW SNAPSHOTS, so it stays comparable with every other rollup here. */
        Assert.Contains("sum(sample_count) AS sample_count", sql, StringComparison.Ordinal);
        Assert.Contains("WITH NO DATA", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// L3: the composer-grain collapse of L2, carrying <see cref="TimescaleSupport.CreateQueryStoreStatsCorrectedDailySql"/>'s
    /// column set to the byte so ComposeCaggValueMapper reads either one unchanged. The load-bearing
    /// difference is one line — the relation it reads.
    /// </summary>
    [Fact]
    public void QueryStoreStatsDayGrainDaily_CollapsesL2_WithTheCorrectedDailysColumnsExactly()
    {
        var sql = TimescaleSupport.CreateQueryStoreStatsDayGrainDailySql;

        Assert.Contains("CREATE MATERIALIZED VIEW IF NOT EXISTS collect.query_store_stats_daygrain_daily", sql, StringComparison.Ordinal);
        Assert.Contains($"FROM collect.{TimescaleSupport.QueryStoreStatsIntervalDailyView}", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY server_id, server_name, database_name, module_name, query_hash, time_bucket('1 day', bucket)", sql, StringComparison.Ordinal);

        /* Every projected column of the corrected daily, identically named and identically computed — asserted
           against the corrected daily's own SQL rather than a transcription, so the two cannot drift into
           column sets one shared value mapper then mis-reads. */
        foreach (var projection in new[]
        {
            "sum(execution_count) AS execution_count_sum",
            "sum(avg_duration_us::double precision * execution_count) AS duration_us_weighted_sum",
            "sum(avg_cpu_time_us::double precision * execution_count) AS cpu_us_weighted_sum",
            "max(max_duration_us) AS max_duration_us_max",
            "max(max_cpu_time_us) AS max_cpu_time_us_max",
            "sum(sample_count) AS sample_count",
        })
        {
            Assert.Contains(projection, TimescaleSupport.CreateQueryStoreStatsCorrectedDailySql, StringComparison.Ordinal);
            Assert.Contains(projection, sql, StringComparison.Ordinal);
        }

        Assert.Contains("WITH NO DATA", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// EVERY retention policy's consumers must outlive the policy itself, walked over
    /// <see cref="TimescaleSupport.RetentionPolicies"/> rather than asserted against the pairs that happen to
    /// exist today (#1905).
    ///
    /// <para><b>What an inversion costs, post-#1877.</b> Each level's purge waits on its consumer covering it,
    /// so a consumer that expired FIRST would hold its own source's purge forever. That used to be the whole
    /// cost: the source's policy simply never armed and that tier grew, loudly, with a WARN every start. Since
    /// #1877 a positively measured shortfall also RE-HOLDS a policy that is already armed — so an inverted pair
    /// would STOP A PURGE ON A COMPLETELY HEALTHY STORE, and because holding makes the source deeper the gap
    /// widens rather than closes, so it would never self-release. That is a latch, and it is why this moved
    /// from three hand-written comparisons to a walk.</para>
    ///
    /// <para>The three cases, all of them real: a consumer with its own policy is compared; a consumer with NO
    /// policy is kept indefinitely, which is an infinite horizon and always satisfies; and a consumer that IS
    /// the source is the leaf rule (#1757), where the real consumer is the baseline COMPUTATION rather than a
    /// relation, so what has to hold is that the tier outlives the baseline WINDOW.</para>
    ///
    /// <para>The other half of the safety argument is geometry, not ordering, and is not assertable here:
    /// materialization chunks are never finer than their source's (measured at 10 days against the raw tables'
    /// 1, on PostgreSQL 18.4 / TimescaleDB 2.28.1), so a consumer never retains LESS than its source at a chunk
    /// boundary. Ordering plus that is what makes a healthy store unable to read as short.</para>
    ///
    /// <para>WATCHED (mutation): set <c>IntervalDailyRetentionInterval</c> below
    /// <c>IntervalRetentionInterval</c> and this goes red naming both relations — and, unlike the three
    /// comparisons it replaced, it would equally catch an inversion on a policy nobody has written yet.</para>
    /// </summary>
    [Fact]
    public void EveryRetentionPolicysConsumersOutliveIt_WalkedOverTheListItself()
    {
        Assert.NotEmpty(TimescaleSupport.RetentionPolicies);

        var horizons = TimescaleSupport.RetentionPolicies
            .ToDictionary(p => p.Relation, p => ParseInterval(p.DropAfter), StringComparer.Ordinal);

        /* Pairs where BOTH sides carry a finite horizon — the only ones an ordering comparison can be made
           about, and therefore the ones a vacuous walk would silently skip. Counted so a change that made
           every consumer look policy-less cannot pass by comparing nothing. */
        var compared = 0;
        var leaves = 0;

        foreach (var (relation, dropAfter, _, coverage) in TimescaleSupport.RetentionPolicies)
        {
            var sourceHorizon = ParseInterval(dropAfter);
            Assert.True(sourceHorizon > TimeSpan.Zero, $"{relation}'s horizon \"{dropAfter}\" did not parse");
            Assert.NotEmpty(coverage);

            foreach (var consumer in coverage)
            {
                if (string.Equals(consumer, relation, StringComparison.Ordinal))
                {
                    /* Self-coverage is the leaf rule (#1757), not an inversion: the tier cannot outlive itself,
                       and its real consumer is the baseline computation, whose capture requirement is the
                       window. That is the comparison that means something here. */
                    leaves++;
                    Assert.True(
                        sourceHorizon > TimeSpan.FromDays(BaselineMath.BaselineWindowDays),
                        $"{relation} covers ITSELF (the #1757 leaf rule), so its horizon has to exceed the " +
                        $"{BaselineMath.BaselineWindowDays}-day baseline window its real consumer reads — it is " +
                        $"{sourceHorizon.TotalDays}d.");
                    continue;
                }

                if (!horizons.TryGetValue(consumer, out var consumerHorizon))
                {
                    /* No policy at all: kept indefinitely, so its horizon is infinite and nothing to compare. */
                    continue;
                }

                compared++;
                Assert.True(
                    consumerHorizon > sourceHorizon,
                    $"{relation} keeps {sourceHorizon.TotalDays}d but its consumer {consumer} keeps only " +
                    $"{consumerHorizon.TotalDays}d. A consumer that expires FIRST can never cover its source, so " +
                    $"{relation}'s purge is held forever — and since #1877 a policy already armed is STOPPED by " +
                    "that same measurement, on a healthy store, and holding only deepens the source so it never " +
                    "self-releases. Every consumer must outlive the tier whose purge waits on it.");
            }
        }

        /* Floors, not exact counts: both rise as policies are added, and a walk that compared nothing would
           otherwise be indistinguishable from a walk that found everything in order. */
        Assert.True(compared >= 6,
            $"only {compared} finite consumer pair(s) were actually compared; the list has at least 6, so the " +
            "walk is skipping pairs it should be judging and would pass over an inversion.");
        Assert.True(leaves >= TimescaleSupport.BaselineAggregates.Length,
            $"only {leaves} self-covering tier(s) were seen; the {TimescaleSupport.BaselineAggregates.Length} " +
            "baseline aggregates all cover themselves under the #1757 leaf rule.");
    }

    /// <summary>
    /// The three coverage-gated raw tiers must appear in <see cref="TimescaleSupport.RetentionPolicies"/> with
    /// exactly the coverage <see cref="TimescaleSupport.RawTierCoverage"/> names, at the raw horizon (#1905).
    ///
    /// <para>Deliberately a VALUE check standing behind a structural one. The structural guard —
    /// <c>RawTierRetentionPolicies_AreDerivedFromTheCoverageMap_NotRehardcodedBesideIt</c> — is what proves the
    /// rows are DERIVED, because a correct hand-copy produces identical values and no value test can tell the
    /// difference. This one catches the case where someone defeats or outlives that guard and hand-copies the
    /// rows WRONG, which is the harm rather than the mechanism. Both, because the mechanism is what rots and
    /// the harm is what hurts.</para>
    /// </summary>
    [Fact]
    public void EveryCoverageGatedRawTier_HasAPolicyMatchingTheMap()
    {
        foreach (var (relation, _, coverage) in TimescaleSupport.RawTierCoverage)
        {
            var policy = TimescaleSupport.RetentionPolicies
                .SingleOrDefault(p => string.Equals(p.Relation, relation, StringComparison.Ordinal));

            Assert.False(policy.Relation is null,
                $"{relation} is coverage-gated in RawTierCoverage but has no retention policy — the two purge " +
                "paths (#1784) no longer agree about which tables are gated.");
            Assert.Equal(TimescaleSupport.RawRetentionInterval, policy.DropAfter);
            Assert.Equal(coverage, policy.Coverage);
        }
    }

    /// <summary>
    /// Every retention horizon exists TWICE — as the interval literal the policy is actually created with, and
    /// as a <see cref="TimeSpan"/> twin the router and the gates compare against — so every one of them has to
    /// be pinned equal.
    ///
    /// <para>Pinned as a set rather than one pair at a time, which is what closes the hole this replaced:
    /// <c>RawRetentionSpan</c> and <c>HourlyRetentionSpan</c> were pinned in RetentionTierRouterTests,
    /// <c>IntervalDailyRetentionSpan</c> had a hand-written pin here, and <c>IntervalRetentionSpan</c> and
    /// <c>BaselineRetentionSpan</c> had none at all despite their doc comments claiming otherwise. A twin that
    /// disagrees is the worst shape available: the policy drops on one number while every gate and route
    /// reasons about another, and nothing reads wrong until data is already gone.</para>
    /// </summary>
    [Theory]
    [InlineData(nameof(TimescaleSupport.RawRetentionInterval))]
    [InlineData(nameof(TimescaleSupport.IntervalRetentionInterval))]
    [InlineData(nameof(TimescaleSupport.IntervalDailyRetentionInterval))]
    [InlineData(nameof(TimescaleSupport.HourlyRetentionInterval))]
    [InlineData(nameof(TimescaleSupport.BaselineRetentionInterval))]
    public void EveryRetentionIntervalLiteral_EqualsItsTimeSpanTwin(string intervalName)
    {
        /* SUFFIX swap, not Replace: IntervalRetentionInterval contains "Interval" twice, and replacing both
           asks for a field named SpanRetentionSpan — which is how this test first ran red. */
        const string suffix = "Interval";
        Assert.EndsWith(suffix, intervalName, StringComparison.Ordinal);
        var spanName = string.Concat(intervalName.AsSpan(0, intervalName.Length - suffix.Length), "Span");

        var literal = (string)typeof(TimescaleSupport).GetField(intervalName)!.GetValue(null)!;
        var spanField = typeof(TimescaleSupport).GetField(spanName);

        Assert.True(spanField is not null, $"{intervalName} has no {spanName} twin to compare against");
        Assert.Equal(ParseInterval(literal), (TimeSpan)spanField!.GetValue(null)!);
    }

    /// <summary>"4 days" / "21 days" -> a <see cref="TimeSpan"/>. The policies are created with these literals,
    /// so parsing what is actually sent is what keeps the comparison honest — a separate TimeSpan field would
    /// be a second representation to drift.</summary>
    private static TimeSpan ParseInterval(string interval)
    {
        var parts = interval.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 2 || !int.TryParse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out var count))
        {
            return TimeSpan.Zero;
        }

        return parts[1].StartsWith("day", StringComparison.OrdinalIgnoreCase)
            ? TimeSpan.FromDays(count)
            : parts[1].StartsWith("hour", StringComparison.OrdinalIgnoreCase)
                ? TimeSpan.FromHours(count)
                : TimeSpan.Zero;
    }
}
