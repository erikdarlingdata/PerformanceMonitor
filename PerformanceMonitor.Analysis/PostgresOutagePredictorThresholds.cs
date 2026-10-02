/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The bars the three Tier-0 PostgreSQL outage predictors grade on — transaction-ID wraparound, the xmin
/// horizon, replication-slot WAL retention — declared ONCE, here, and read by both surfaces that judge them:
/// the alert evaluator (<c>PostgresAlertEvaluator</c>, which PAGES) and the PostgreSQL-target analysis
/// scorer (<see cref="PgTargetScorer"/>, which NARRATES — story, advice, occurrences, mute-by-story-path).
/// Design decision D9 of #3542: "share the evaluator's constants so the surfaces agree; alert pages,
/// analysis narrates." Sharing by REFERENCE is what makes agreement structural rather than a convention a
/// future edit can break — a source pin (<c>PgTargetVacuumTests</c>) holds that neither consumer retypes a
/// literal from this file, so there is exactly one place a number can move.
///
/// <para><b>Why this assembly and not <c>PerformanceMonitor.Common</c>.</b> The natural home for a
/// constant two peers share is the lowest library both reference — and <c>PerformanceMonitor.Analysis</c>
/// references NOTHING (no project, no package), by design: it is pure scoring logic that both SKUs and the
/// deprecated Dashboard consume. Giving it a <c>Common</c> reference would drag <c>ModelContextProtocol</c>
/// and <c>CredentialManagement</c> into its closure and re-write six locked <c>packages.lock.json</c> files
/// for one static class. <c>PerformanceMonitor.Alerting</c> already reaches this assembly transitively
/// (Alerting → Notifications → Analysis; <c>AnalysisNotificationService</c> is the existing consumer), so
/// putting the definitions HERE costs no project edge at all. The evaluator keeps its old names as aliases
/// (<c>public const X = PostgresOutagePredictorThresholds.X</c>) — the same shape its poison-wait constants
/// already take from <c>PoisonWaitEvaluator</c> — so the read adapter, the host, the MCP wraparound tool and
/// every test that cites them keep compiling.</para>
///
/// <para><b>Every value is derived from PostgreSQL's own mechanics rather than picked</b> — see each
/// constant — which is also why there is no knob: the product adds configuration when someone wants a
/// different number, not before. In the threshold-lineage vocabulary of the analysis scorer, every bar here
/// is <b>engine-defined</b>.</para>
/// </summary>
public static class PostgresOutagePredictorThresholds
{
    /* Wraparound. The wall is 2 billion transactions, but the number that matters first is the server's
       OWN autovacuum_freeze_max_age: at that age autovacuum force-starts a wraparound-prevention vacuum
       whether or not a table is otherwise due, so crossing it means the server has begun defending itself.
       Critical fires at 2x it, which on a stock 200-million setting is 400 million: comfortably clear of
       the 2-billion stop, but far enough past the engine's own line to mean its defence is not keeping up.
       Both are ratios of a setting the row carries, so a cluster tuned to 1.5 billion gets thresholds
       scaled to its own configuration rather than to a constant that would never fire for it.

       #2689: Warning used to fire at 90% of freeze_max_age unconditionally, and that is a ROUTINE operating
       point — every healthy database climbs to ~that age on every freeze cycle and is reset, the expected
       sawtooth. Firing there paged on essentially every healthy database, permanently (one fleet database
       re-fired the identical alert every ~5-minute cycle at 9% of the way to actual wraparound). The real
       risk is not "approaching freeze_max_age", it is "age has REACHED freeze_max_age and autovacuum is not
       bringing it back down" - i.e. the forced vacuum this crossing itself triggers is not winning. So the
       relative Warning arm now sits AT the setting (1.0x, the crossing point itself) and is gated by
       FreezingIsKeepingUp: a database sitting above its own setting but coming back down each cycle does not
       warn; one stuck at or above it, never seen lower within the window, does. */

    /// <summary>The relative Warning arm: age at (1.0×) <c>autovacuum_freeze_max_age</c>, gated on the
    /// counter NOT having come down from its window peak — the crossing point itself, not an approach.</summary>
    public const double WraparoundWarningFractionOfFreezeMaxAge = 1.0;

    /// <summary>The relative Critical arm: age at twice the setting — far enough past the engine's own
    /// line to mean the forced vacuum it triggered is not keeping up.</summary>
    public const double WraparoundCriticalMultipleOfFreezeMaxAge = 2.0;

    /// <summary>
    /// The 32-bit comparison space both counters age within — the same denominator the collector stores its
    /// percentages against, so the alert and <c>pct_toward_wraparound</c> can never disagree.
    /// </summary>
    public const long WraparoundCeiling = 2_147_483_648L;

    /// <summary>
    /// The absolute Critical arm, as a fraction of <see cref="WraparoundCeiling"/>: PostgreSQL's own
    /// <c>vacuum_failsafe_age</c> (1.6B by default) is ~74.5% of the space, and past it the engine abandons
    /// cost limits and skips index cleanup to catch up. Matching the ladder
    /// <c>DarlingMcpPgWraparoundTools</c> already classifies against.
    /// <para>This exists because the RELATIVE arm alone leaves Critical unreachable on exactly the clusters
    /// most at risk: <c>criticalAt = 2 x setting</c> exceeds the 2^31 wall once the setting passes ~1.07B, and
    /// the setting is tunable to 2B. A tuned cluster would have warned and then never escalated.</para>
    /// </summary>
    public const double WraparoundCriticalFractionOfCeiling = 0.745;

    /// <summary>
    /// #2689: an absolute early-Warning arm, the same reasoning as <see cref="WraparoundCriticalFractionOfCeiling"/>
    /// applied one severity down. On a cluster tuned high enough (past ~1.07B), the RELATIVE Warning arm
    /// (1.0x setting) can land beyond or right at the Critical ceiling arm, robbing the operator of any
    /// early warning at all. Unconditional (no FreezingIsKeepingUp gate) like the Critical ceiling arm,
    /// because half the true wraparound space is already such a rare, high-consequence number that no
    /// healthy database reaches it under a routine sawtooth regardless of oscillation history.
    /// </summary>
    public const double WraparoundWarningFractionOfCeiling = 0.5;

    /* xmin horizon. 50 million transactions of held-back horizon is roughly where bloat becomes visible
       rather than theoretical on a busy database. The persistence gate is what makes it actionable: a
       holder seen in a majority of the window's observations is chronic, while one seen once is a query
       that ran long, and only the first is worth waking anyone for. */

    /// <summary>The held-back age at which an xmin holder becomes a finding, given persistence.</summary>
    public const long XminAgeWarningThreshold = 50_000_000;

    /// <summary>
    /// The majority standard both persistence arms apply (#3537) — one fraction, two denominators. The
    /// IDENTITY arm asks it of the collections that recorded any holder ("of the times something held it,
    /// how often was it this one"); the HORIZON arm asks it of the window's real captures ("of the times we
    /// looked, how often was the horizon pinned past the threshold"). Majority is the line in both readings
    /// because it is where "keeps happening" stops being arguable: below it every fire needs a judgment
    /// call about how much less than half still counts, and there is no mechanics-derived number under 0.5
    /// to anchor one.
    /// </summary>
    public const double XminPersistenceFraction = 0.5;

    /// <summary>
    /// #3537: the floor under BOTH persistence denominators. Without it the identity arm read the first
    /// holder after quiet hours as 1 win in 1 observation — 100%, "chronic", off a single sample — because
    /// its denominator counts only holder-bearing collections and quiet hours contribute none. 5 because it
    /// is the smallest denominator whose majority test cannot be satisfied by fewer than three sightings
    /// (at 1–4 observations, one or two sightings clear 50% — the exact shape of the false fire), and at
    /// the collector's 1-minute cadence three sightings means the condition spanned minutes, not a moment.
    /// Higher would buy little: a holder that has aged the horizon 50 million transactions has existed for
    /// minutes on any workload fast enough for the extra samples to cost real delay.
    /// </summary>
    public const int XminMinimumObservations = 5;

    /* Replication slots. No byte threshold for the terminal states — `lost` and `unreserved` are failures
       that have already happened, at any size. For a slot merely retaining WAL, 10 GB is the point where
       an unbounded pile stops being noise on any volume worth monitoring; growth is what escalates it,
       since max_slot_wal_keep_size defaults to -1 and nothing will stop it. */

    /// <summary>Retained WAL behind a slot at which it becomes a finding; the terminal
    /// <c>wal_status</c> values fire at any size.</summary>
    public const long SlotRetainedWalWarningBytes = 10L * 1024 * 1024 * 1024;
}
