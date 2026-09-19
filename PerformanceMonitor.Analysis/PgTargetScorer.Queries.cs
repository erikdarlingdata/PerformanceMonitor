/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// <c>pg_queries</c> — top statements (lane 7), keyed through <see cref="PgTargetFactKeys.BadActorKey"/>.
/// Pattern: <c>FactScorer.ScoreBadActorFact</c> — a tier that gets the statement in the door and a grade that
/// says how bad — with ONE decision variable in place of its two: the statement's SHARE of the window's total
/// execution time, computed over the WINDOW (the collector's <c>SUM(…) OVER ()</c>, #3541 A7), never over the
/// page of rows returned.
///
/// <para><b>Why share and not the SQL Server pair (execution-count tier × per-execution impact).</b> The SQL
/// Server family answers "is this query consistently terrible" from <c>query_stats</c>, where every row is a
/// plan-cache entry with CPU and reads per execution, and its tiers (1,000 / 10,000 / 100,000 executions;
/// 50 / 2,000 ms CPU; 5,000 / 250,000 reads) are that engine's inherited constants. None of them may be
/// reused by value here (#3538 A5), and the PostgreSQL source does not offer the same axes: <c>pg_stat_statements</c>
/// has no CPU (that is <c>pg_stat_kcache</c>, an optional extension) and its block counts are cache-tier
/// counters, not a logical-reads figure. What it does offer, exactly, is elapsed time per statement shape and
/// the total across every shape — so the honest question is the one a PostgreSQL operator actually asks of
/// this view: "how much of what the server did was this one statement". A statement holding six tenths of
/// the window's time is the story's query-shaped leaf whatever its per-call cost; the per-call figures
/// (<c>mean_exec_ms</c>, <c>calls_per_sec</c>) are the advice's material, not the gate's.</para>
///
/// <para><b>The idle-server gate.</b> Share alone would root a card on a development box where the whole
/// window's execution time is 100 ms and one statement holds 60 of them. <c>window_busy_fraction</c> — the
/// window's total statement time over <see cref="AnalysisContext.ObservedDurationMs"/>, summed over backends
/// so it may exceed 1.0 — is the "was the server doing anything" gate, a fraction of OBSERVED time (#3538 A7),
/// so the same server reads the same at every <c>hours_back</c>.</para>
///
/// <para><b>Lineage.</b> There is no engine-defined line for "too large a share"; the #3691 fleet calibration
/// (2026-09-19, 7 days × 50 Aurora PostgreSQL clusters of the dogfood fleet, hourly <c>pg_statement_stats</c>)
/// read both quantities. The result is the reason the gate exists: the top-1 share is ROUTINE unconditionally
/// (63 % of server-hours on the median cluster have a statement at or above 0.25), while the busy fraction is
/// tiny almost everywhere (the median cluster is below the 0.05 floor in 100 % of its hours; fleet p25 73 %,
/// minimum 12 %). So the floor is the discriminator and is <b>measured</b> at ≈ the fleet p97 of hourly busy
/// fractions, and the share bars are <b>measured-conditional</b> — their placement on the per-server share
/// distribution is known, and they mean something only given busy ≥ the floor. Every fact this arm grades
/// carries <c>threshold_lineage = 1</c>: no bar here is a bare choice any more. The quantities are
/// engine-neutral; the stock-PostgreSQL population is not yet measured. A second read conditioned on the floor
/// is the follow-up if the fired rate on the fleet reads wrong.</para>
/// </summary>
public static partial class PgTargetScorer
{
    /// <summary>
    /// The share of the window's total execution time at which one statement is CONCERNING (severity 0.5 —
    /// the story threshold) and at which it is CRITICAL (1.0). A quarter of everything the server executed
    /// being one statement shape is a workload with a head; six tenths is a workload that IS one statement.
    /// measured-conditional (given busy ≥ <see cref="BadActorBusyFloor"/>): per-server top-1 share of hourly
    /// execution time over 7 days × 50 Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-19 — p50 median
    /// 0.32 (range 0.14 – 0.67), p90 median 0.42, p99 median 0.71, maximum 0.95. Unconditionally 0.25 is BELOW
    /// the median cluster's median hour (63 % of hours at or above it) and 0.60 is ≈ its p96 (4 % of hours at
    /// or above; on the fleet-p90 cluster 29 %) — routine shapes, which is why the floor below decides and these grade. The
    /// fact carries threshold_lineage = 1; a share read conditioned on the floor is the stated follow-up.
    /// </summary>
    public const double BadActorShareConcerning = 0.25;
    public const double BadActorShareCritical = 0.60;

    /// <summary>
    /// The floor on <c>window_busy_fraction</c> below which no statement can be a bad actor: the server
    /// executed statements for less than this fraction of ONE backend's worth of the observed time (0.05 of
    /// a four-hour window is twelve minutes of statement time), so a large share is a large share of nearly
    /// nothing. measured: ≈ p97 of hourly busy fractions (Σ delta_total_exec_time_ms over observed time) over
    /// 7 days × 50 Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-19 — per-server p50 median 0.7 %,
    /// p90 median 3.3 %, maximum 69 %; hours below the floor: median 100 % of a cluster's hours, fleet p25 73 %,
    /// minimum 12 %. Measured on Aurora; the stock-PostgreSQL population is not yet measured. The fact carries
    /// threshold_lineage = 1.
    /// </summary>
    public const double BadActorBusyFloor = 0.05;

    /// <summary>The boost a co-firing workload symptom adds to a bad actor — see <see cref="QueriesAmplifiers"/>.
    /// unmeasured: chosen, not measured — calibrate against analysis_findings co-fire rates before the next
    /// release; the fact carries threshold_lineage = 0.</summary>
    public const double BadActorCoFireBoost = 0.25;

    /// <summary>
    /// Layer-1 base severity for a <c>PG_BAD_ACTOR_*</c> fact: the share graded through the shared formula
    /// between the two bars above, gated on the idle-server floor. Zero for anything under the
    /// <c>pg_queries</c> source that is not a bad-actor key, for a fact with no share, and for an idle window.
    /// Stamps <c>threshold_lineage = 1</c> on every fact it grades (including the ones it grades to 0 through
    /// the gate), because the number that decided — the floor or the share bars — is fleet-measured either way
    /// (2026-09-19; the share bars conditionally, see their declaration).
    /// </summary>
    private static partial double ScoreQueriesFact(Fact fact)
    {
        if (!fact.Key.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal))
            return 0.0;
        if (!fact.Metadata.TryGetValue("share_of_window_time", out var share) || share <= 0)
            return 0.0;

        /* measured: the floor and both share bars below are the constants declared above with their 2026-09-19
           lineage (the floor ≈ fleet p97 of hourly busy fractions; the shares measured-conditional on it). */
        fact.Metadata["threshold_lineage"] = 1;

        var busy = fact.Metadata.GetValueOrDefault("window_busy_fraction");
        if (busy < BadActorBusyFloor)
            return 0.0;

        return FactScorer.ApplyThresholdFormula(share, BadActorShareConcerning, BadActorShareCritical);
    }

    /// <summary>
    /// Layer-2 amplifiers for a bad actor: the server-level symptoms that make one statement's share URGENT
    /// rather than merely large. Each predicate asks only whether the sibling fact FIRED (its own scorer's bar
    /// put its base severity above zero) and, where the statement's own metadata can corroborate, whether it
    /// does — never a bar of this arm's own.
    /// <list type="bullet">
    /// <item><description><see cref="PgTargetFactKeys.TempSpill"/> fired and THIS statement wrote temp blocks
    /// in the window (<c>temp_blks_written &gt; 0</c>): the server is spilling and this is one of the
    /// spillers — lane 6's <c>PG_TEMP_SPILL → PG_BAD_ACTOR_*</c> edge, seen from the leaf.</description></item>
    /// <item><description><see cref="PgTargetFactKeys.CpuPercent"/> fired: instance CPU is high (Aurora /
    /// Performance Insights only — the fact is absent on stock, so the arm is inert there) while one statement
    /// holds the time. <c>pg_stat_statements</c> measures elapsed, not CPU, so this is corroboration, not
    /// attribution; the advice says so.</description></item>
    /// </list>
    /// </summary>
    private static partial List<AmplifierDefinition> QueriesAmplifiers(string key) =>
    [
        new()
        {
            Description = "The server is spilling to temp files and this statement wrote temp blocks in the window",
            /* unmeasured: BadActorCoFireBoost, chosen, not measured — see its declaration. */
            Boost = BadActorCoFireBoost,
            Predicate = facts =>
                facts.TryGetValue(PgTargetFactKeys.TempSpill, out var spill) && spill.BaseSeverity > 0
                && facts.TryGetValue(key, out var self) && self.Metadata.GetValueOrDefault("temp_blks_written") > 0,
        },
        new()
        {
            Description = "Instance CPU is elevated while this statement holds the window's execution time",
            /* unmeasured: BadActorCoFireBoost, chosen, not measured — see its declaration. */
            Boost = BadActorCoFireBoost,
            Predicate = facts =>
                facts.TryGetValue(PgTargetFactKeys.CpuPercent, out var cpu) && cpu.BaseSeverity > 0,
        },
    ];
}
