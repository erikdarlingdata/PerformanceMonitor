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
/// <para><b>Lineage, and the 2026-09-20 ruling.</b> There is no engine-defined line for "too large a share"; the #3691
/// fleet calibration (2026-09-19, 7 days × 50 Aurora PostgreSQL clusters of the dogfood fleet, hourly
/// <c>pg_statement_stats</c>) read both quantities, and the second read (2026-09-20, §C4) conditioned the share on the
/// floor: GIVEN a busy hour, the top-1 statement's share is at or above 0.25 on 85 % of hours and at or above 0.60 on
/// 44 % — a single statement dominating a busy hour is the NORMAL shape of these concentrated application workloads.
/// So the busy floor is the discriminator and is <b>measured</b> (≈ the fleet p97 of hourly busy fractions), and the
/// share bars are <b>measured-routine</b>: their placement is known, and what it says is that an absolute share
/// cannot be the grade. The ruling (Erik, 2026-09-20): <i>re-grade the bad actor as deviation from the statement's
/// OWN hour-of-week share baseline, absolute share carried as context — the derive-not-constant pattern; the
/// percentile-raise option rejected.</i></para>
///
/// <para><b>What this arm grades now.</b> A fact the floor admits lands in a CONTEXT band —
/// half to all of <see cref="BadActorContextBand"/>, linear in its share, so the band keeps the statements in share order for the alias
/// (<c>PgTargetRelationshipGraph.ResolveBadActor</c> picks the highest-severity bad actor; before this lane that was
/// the largest share, and it still is when no statement is beyond its own normal) while its ceiling stays under
/// the 0.5 story line: no statement roots a card on its share alone any more. The share bars survive as CONTEXT
/// (<c>share_band</c>: 0 under 0.25, 1 at or above the routine line, 2 at or above 0.60), named in the advice for
/// what they are. The GRADE is the deviation: <c>PgTargetAnomalyDetector.Queries.cs</c> reads each candidate's
/// window share against its OWN hour-of-week share bucket (lane 33's keyed <c>pg_statement_share</c> arm) and emits
/// <c>ANOMALY_PG_BAD_ACTOR_SHARE</c> for the statement furthest beyond its normal, graded by the shared deviation
/// ramp; that anomaly roots the story with this card as its leaf (the alias edge in
/// <c>PgTargetRelationshipGraph.Query.cs</c>), and <see cref="QueriesAmplifiers"/>'s own-normal arm lifts the named
/// statement's card so the alias resolves to IT and the card clears the story line on its own when the anomaly's
/// story cannot carry it. Layer 1 sees one fact, never the fact set — which is why the deviation is a detector's
/// fact and an amplifier's predicate rather than a term in this method. A statement with no trustworthy own-normal
/// yet (young store, first seen) gets no anomaly and stays the context band — never the old absolute grade
/// silently; the advice says so.</para>
///
/// <para><b>The lineage stamp.</b> A fact the floor zeroes carries <c>threshold_lineage = 1</c> (a measured bar
/// decided); a fact the floor admits carries <c>threshold_lineage = 0</c> — the band that sets its severity is a
/// chosen number, and the shared ramp that grades the anomaly rests on cutoffs reused by name and unmeasured for
/// statement share (the anomaly says so on its own fact). The quantities are engine-neutral; the stock-PostgreSQL
/// population is not yet measured.</para>
/// </summary>
public static partial class PgTargetScorer
{
    /// <summary>
    /// The share bars, kept as CONTEXT since the 2026-09-20 ruling: <c>share_band</c> 1 at or above the first, 2 at or
    /// above the second — named in the advice, never the grade. A quarter of everything the server executed being one
    /// statement shape is a workload with a head; six tenths is a workload that IS one statement — and on the measured
    /// population BOTH are routine. measured-routine (given busy ≥ <see cref="BadActorBusyFloor"/>): per-server
    /// top-1 share of hourly execution time over 7 days × 50 Aurora PostgreSQL clusters of the dogfood fleet,
    /// 2026-09-19 — p50 median 0.32, p90 median 0.42, p99 median 0.71; and conditioned on the floor, 2026-09-20 (§C4):
    /// given a busy hour, share ≥ 0.25 on 84.9 % of hours (p50 0.55), ≥ 0.60 on 43.5 % (p90 0.85, p99 0.90). A bar
    /// that fires on 85 % of the hours it is asked about is a description of the workload, not a grade — which is why
    /// the grade moved to the statement's own baseline and these stay as the named context lines they are.
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
    /// minimum 12 %. Measured on Aurora; the stock-PostgreSQL population is not yet measured. A fact the floor
    /// zeroes carries threshold_lineage = 1 — the measured bar decided.
    /// </summary>
    public const double BadActorBusyFloor = 0.05;

    /// <summary>
    /// The CONTEXT band an admitted bad actor lands in: base severity runs linearly from HALF of this (a vanishing
    /// share) to this (a statement holding the whole window) — 0.3 × (0.5 + 0.5 × share), so a routine 0.60
    /// statement reads 0.24 and a 0.25 one 0.1875 — every admitted card under the 0.5 story line, in share order
    /// (the alias resolves among them by severity, so a symptom's edge still walks to the largest statement when
    /// none is beyond its own normal), and none rootable on its share alone: a card roots only through the anomaly
    /// that says the statement is beyond its OWN normal (the lift below) or through a symptom's edge into it. The
    /// half-band floor exists so the lifted card of a SMALL deviant statement still outranks every un-lifted card
    /// (see <see cref="BadActorOwnNormalBoost"/>). unmeasured: chosen, not measured — a severity level below the
    /// story line has no fleet distribution to place it on; calibrate against the fired rate of
    /// ANOMALY_PG_BAD_ACTOR_SHARE and how often a context card is walked into as a leaf before the next release.
    /// The fact carries threshold_lineage = 0.
    /// </summary>
    public const double BadActorContextBand = 0.3;

    /// <summary>The boost a co-firing workload symptom adds to a bad actor — see <see cref="QueriesAmplifiers"/>.
    /// unmeasured: chosen, not measured — calibrate against analysis_findings co-fire rates before the next
    /// release. A boost is not a bar the base was graded on, so it does not move the fact's threshold_lineage.</summary>
    public const double BadActorCoFireBoost = 0.25;

    /// <summary>
    /// The boost the own-normal anomaly adds to the card of the statement it NAMES: base × (1 + this), so a 0.60
    /// statement the anomaly names reads 0.24 × 3.5 = 0.84, a 0.25 one 0.1875 × 3.5 = 0.656, and the smallest
    /// admissible share at least 0.15 × 3.5 = 0.525 — the named card ALWAYS clears the 0.5 story line on its own,
    /// and always outranks every un-named context card (whose ceiling with both co-fires is 0.3 × 1.5 = 0.45), so
    /// the alias resolves to the statement the anomaly names and the anomaly's edge opens onto it. The GRADED
    /// severity is the anomaly's (the shared ramp: 0.5 at the cutoff, 1.0 at twice it); the lift is what lets the
    /// card stand when the anomaly's story cannot carry it — the card roots first when it outranks the anomaly,
    /// and the two are one incident through the active alias edge either way, never a lost card. unmeasured:
    /// chosen, not measured — calibrate against the anomaly's fired rate and the share distribution of the
    /// statements it names before the next release. A boost does not move threshold_lineage.
    /// </summary>
    public const double BadActorOwnNormalBoost = 2.5;

    /// <summary>
    /// Layer-1 base severity for a <c>PG_BAD_ACTOR_*</c> fact since the 2026-09-20 ruling: the busy floor still
    /// gates admission (measured), and an admitted fact lands in the CONTEXT band — half to all of
    /// <see cref="BadActorContextBand"/>, linear in its share — with the share bars stamped as <c>share_band</c> context. Zero for anything under the
    /// <c>pg_queries</c> source that is not a bad-actor key, for a fact with no share, and for an idle window.
    /// Stamps <c>threshold_lineage = 1</c> on a fact the measured floor zeroes and <c>0</c> on a fact the chosen
    /// band grades (see the class summary). The deviation grade lives on <c>ANOMALY_PG_BAD_ACTOR_SHARE</c>.
    /// </summary>
    private static partial double ScoreQueriesFact(Fact fact)
    {
        if (!fact.Key.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal))
            return 0.0;
        if (!fact.Metadata.TryGetValue("share_of_window_time", out var share) || share <= 0)
            return 0.0;

        var busy = fact.Metadata.GetValueOrDefault("window_busy_fraction");
        if (busy < BadActorBusyFloor)
        {
            /* measured: the floor is the constant declared above with its 2026-09-19 lineage (≈ fleet p97 of hourly
               busy fractions) — the bar that decided this fact is measured. */
            fact.Metadata["threshold_lineage"] = 1;
            return 0.0;
        }

        /* measured-routine context, never the grade: the two share bars (2026-09-19; conditioned 2026-09-20 §C4). */
        fact.Metadata["share_band"] = share >= BadActorShareCritical ? 2 : share >= BadActorShareConcerning ? 1 : 0;
        /* unmeasured: BadActorContextBand (see its declaration) sets the severity of every admitted fact — a chosen
           number decided, so the flag is 0; the anomaly that grades the deviation stamps its own. */
        fact.Metadata["threshold_lineage"] = 0;

        /* The band's shape (half at a vanishing share, whole at the whole window) is the declaration's; 0.5 is the
           halving, not a bar. */
        return BadActorContextBand * (0.5 + 0.5 * Math.Min(share, 1.0));
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
    /// <item><description><see cref="PgTargetFactKeys.AnomalyBadActorShare"/> fired and NAMES this statement
    /// (<see cref="Fact.ObjectName"/> is the <c>queryid</c> the key carries): the statement's share this window is
    /// beyond its OWN hour-of-week normal — the grade, since the 2026-09-20 ruling (lane 34). The boost is
    /// <see cref="BadActorOwnNormalBoost"/>, sized so the named card outranks every un-named context card and clears
    /// the story line; the anomaly's fact carries the sigma. Read through <see cref="OwnNormalAnomalyNames"/> so the
    /// graph's alias edge asks the same question.</description></item>
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
        new()
        {
            Description = "ANOMALY_PG_BAD_ACTOR_SHARE fired for this statement — its share of the window is beyond its own hour-of-week normal",
            /* unmeasured: BadActorOwnNormalBoost, chosen, not measured — see its declaration. */
            Boost = BadActorOwnNormalBoost,
            Predicate = facts => OwnNormalAnomalyNames(facts, key),
        },
    ];

    /// <summary>
    /// Whether <see cref="PgTargetFactKeys.AnomalyBadActorShare"/> FIRED (base severity above zero — the shared
    /// deviation ramp graded it) and names the statement behind <paramref name="badActorKey"/>: the anomaly's
    /// <see cref="Fact.ObjectName"/> is the <c>queryid</c> as an invariant string, and the key is the prefix plus
    /// that same string (<see cref="PgTargetFactKeys.BadActorKey"/>), so the comparison is on the id's text and never
    /// on a double. One predicate for the amplifier and the graph's alias edge, so "the statement the anomaly names"
    /// is decided once.
    /// </summary>
    public static bool OwnNormalAnomalyNames(IReadOnlyDictionary<string, Fact> facts, string badActorKey) =>
        facts.TryGetValue(PgTargetFactKeys.AnomalyBadActorShare, out var anomaly)
        && anomaly.BaseSeverity > 0
        && !string.IsNullOrEmpty(anomaly.ObjectName)
        && string.Equals(badActorKey, PgTargetFactKeys.BadActorKeyPrefix + anomaly.ObjectName, StringComparison.Ordinal);
}
