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
/// <c>pg_vacuum</c> — autovacuum backlog, wraparound trend, xmin hold (filled by lane 4 of #3542, design §3.1 /
/// §3.3, doctrine [D3]: workload-triggered maintenance is the named failure class). The wraparound and xmin
/// bars are the SAME symbols the Tier-0 alert evaluator grades on (D9) — <see cref="PostgresOutagePredictorThresholds"/>,
/// engine-defined lineage, referenced, never retyped; a source pin holds that neither file carries the
/// other's literal. Everything ELSE numeric in this file is either the engine's own per-table trigger line
/// (engine-defined) or marked unmeasured, and a fact graded on an unmeasured bar carries
/// <c>threshold_lineage = 0</c> so <c>get_analysis_facts</c> shows it.
///
/// <para><b>Metadata contract with the collector</b> (<c>PgTargetFactCollector.Vacuum.cs</c>): the keys below
/// are the whole interface between the read and the grade, and the advice partial reads the same names. A
/// missing key reads as 0 through <c>GetValueOrDefault</c>, which for every gate here means "does not fire"
/// — the conservative default, never a fabricated positive.</para>
/// </summary>
public static partial class PgTargetScorer
{
    /* ── PG_AUTOVACUUM_BACKLOG ── */

    /// <summary>
    /// How many consecutive hourly <c>pg_autovacuum_stats</c> samples a table must sit past its own trigger
    /// line before the backlog is a finding rather than a vacuum that has not run yet. The collector binds
    /// this as the read's persistence gate. Three, because the collector is hourly and autovacuum's
    /// scheduler re-examines every table each <c>autovacuum_naptime</c> (one minute by default; nobody sane
    /// runs it at hours), so a table still past its line after three hourly captures has been offered to
    /// the launcher a hundred-odd times and not been cleared — either a vacuum on it takes longer than an
    /// hour, or the workers never reached it, or what it vacuumed could not be removed. All three are
    /// findings. Lineage: <b>unmeasured</b> — chosen, not measured; calibrate against
    /// <c>pg_autovacuum_stats</c> before the next release. Consequence a caller should know: an
    /// <c>hours_back</c> below this cannot produce the fact at all — the metadata's
    /// <c>samples_in_window</c> says how many the window held.
    /// </summary>
    public const int BacklogPersistenceSamples = 3;

    /// <summary>
    /// The backlog ratio at which a persistent backlog grades 1.0: ten times the table's own trigger line.
    /// The CONCERNING bar is engine-defined (ratio 1.0 = <c>n_dead_tup</c> equals the table's
    /// <c>autovacuum_vacuum_threshold + autovacuum_vacuum_scale_factor × n_live_tup</c>, reloptions honoured
    /// — past it and not clearing is definitionally starvation, not opinion); this critical multiple is
    /// <b>unmeasured</b> — chosen, not measured; calibrate against <c>pg_autovacuum_stats</c> before the next
    /// release. A fact graded through it carries <c>threshold_lineage = 0</c>.
    /// </summary>
    public const double BacklogCriticalRatio = 10.0;

    /// <summary>Metadata key: the worse of the two engine arms, dead/threshold or inserts/insert-threshold.</summary>
    public const string BacklogRatioKey = "backlog_ratio";
    /// <summary>Metadata key: 0 = the dead-tuple arm won, 1 = the append-only insert arm won.</summary>
    public const string BacklogArmIsInsertKey = "backlog_arm_is_insert";
    /// <summary>Metadata key: hourly samples the table sat past its line, counted back from the latest.</summary>
    public const string BacklogTrailingSamplesKey = "trailing_samples_past_line";
    /// <summary>Metadata key: hourly samples the window held for the table.</summary>
    public const string BacklogSamplesInWindowKey = "samples_in_window";
    /// <summary>Metadata key: dead tuples per hour over the trailing backlog run (gauge differenced across
    /// samples; a level, so differencing is honest). Absent when not computable.</summary>
    public const string BacklogSlopePerHourKey = "dead_tuple_slope_per_hour";
    /// <summary>Metadata key: 1 when the slope key is present, 0 when the run's span was too short to divide.</summary>
    public const string BacklogSlopeComputableKey = "slope_computable";
    /// <summary>Metadata key: autovacuum runs on the table during the backlog run — the CUMULATIVE
    /// <c>autovacuum_count</c> differenced (never summed). Absent when the counter went backwards (a stats
    /// reset), and <see cref="BacklogRunsComputableKey"/> says so.</summary>
    public const string BacklogAutovacuumRunsKey = "autovacuum_runs_in_backlog";
    /// <summary>Metadata key: 1 when <see cref="BacklogAutovacuumRunsKey"/> could be differenced.</summary>
    public const string BacklogRunsComputableKey = "autovacuum_runs_computable";
    /// <summary>Metadata key: 1 when the table's <c>autovacuum_enabled</c> reloption is off.</summary>
    public const string BacklogTableAutovacuumDisabledKey = "autovacuum_disabled";
    /// <summary>Metadata key: how many tables were persistently past their line (the fact names the worst).</summary>
    public const string BacklogTablesKey = "tables_in_backlog";
    public const string BacklogDeadTuplesKey = "dead_tuples";
    public const string BacklogVacuumThresholdKey = "vacuum_threshold";
    public const string BacklogLiveTuplesKey = "live_tuples";
    public const string BacklogInsertsSinceVacuumKey = "inserts_since_vacuum";
    public const string BacklogInsertThresholdKey = "insert_vacuum_threshold";
    public const string BacklogHoursKey = "backlog_hours";
    public const string BacklogTotalBytesKey = "total_bytes";
    /// <summary>Metadata key: hours from the table's <c>last_autovacuum</c> stamp to the window end; absent
    /// when the table has never been autovacuumed.</summary>
    public const string BacklogHoursSinceLastAutovacuumKey = "hours_since_last_autovacuum";

    /// <summary>
    /// How many tables persistently past their line reads as the WORKERS being the bottleneck rather than one
    /// table's shape — the amplifier that redirects the advice from per-table reloptions to
    /// <c>autovacuum_max_workers</c>. Lineage: <b>unmeasured</b> — chosen, not measured; calibrate against
    /// <c>pg_autovacuum_stats</c> before the next release. The stock worker count is three, so five tables
    /// each holding a worker for over an hour is past what the default pool can rotate through.
    /// </summary>
    public const int BacklogManyTables = 5;

    /* ── PG_WRAPAROUND_TREND ── */

    public const string WraparoundXidAgeKey = "xid_age";
    public const string WraparoundMultiXactAgeKey = "multixact_age";
    public const string WraparoundFreezeMaxAgeKey = "freeze_max_age";
    public const string WraparoundMultiXactFreezeMaxAgeKey = "multixact_freeze_max_age";
    public const string WraparoundXidPeakKey = "xid_window_peak";
    public const string WraparoundMultiXactPeakKey = "multixact_window_peak";
    /// <summary>Metadata key: 1 when the counter came DOWN from its window peak (the healthy sawtooth), 0 when
    /// its latest reading IS the peak — the #2689 gate on the relative Warning arm.</summary>
    public const string WraparoundXidKeepingUpKey = "xid_keeping_up";
    public const string WraparoundMultiXactKeepingUpKey = "multixact_keeping_up";
    /// <summary>Metadata key: 1 when the MultiXact counter is the relatively-worse one and the fact's Value is its age.</summary>
    public const string WraparoundCounterIsMultiXactKey = "counter_is_multixact";
    public const string WraparoundXidsRemainingKey = "xids_remaining";
    public const string WraparoundMultiXidsRemainingKey = "multixids_remaining";
    /// <summary>Metadata key: the worst counter's age change per hour across the window (first reading to
    /// latest). Negative or zero when autovacuum clawed it back.</summary>
    public const string WraparoundSlopePerHourKey = "age_slope_per_hour";
    /// <summary>Metadata key: hours until the worst counter reaches the 2^31 wall at the window's slope. Present
    /// ONLY when the slope is positive — <see cref="WraparoundTimeToWallComputableKey"/> carries the branch.</summary>
    public const string WraparoundHoursToWallKey = "hours_to_wall";
    public const string WraparoundTimeToWallComputableKey = "time_to_wall_computable";
    public const string WraparoundDatabasesKey = "databases_in_window";
    public const string WraparoundDatabasesGradedKey = "databases_graded";
    public const string WraparoundSamplesKey = "samples_in_window";
    /// <summary>Metadata key: the arm that graded the worst counter — 0 none, 1 relative Warning (at the
    /// setting, not recovering), 2 ceiling Warning (half the space), 3 relative Critical (2× the setting),
    /// 4 ceiling Critical (past <c>vacuum_failsafe_age</c>).</summary>
    public const string WraparoundArmKey = "graded_arm";

    /* ── PG_XMIN_HOLD ── */

    public const string XminAgeKey = "xmin_age";
    /// <summary>Metadata key: the holder kind, encoded by <see cref="PgTargetAdvice.HolderSourceCode"/> because
    /// metadata is doubles; the fact's <see cref="Fact.ObjectName"/> carries <c>source:holder</c> as text.</summary>
    public const string XminHolderSourceKey = "holder_source";
    /// <summary>Metadata key: distinct collections in the window that recorded ANY holder — the identity
    /// arm's denominator (the alert's <c>observations_total</c>).</summary>
    public const string XminObservationsTotalKey = "observations_total";
    /// <summary>Metadata key: collections in which THIS (source, holder) was the winner.</summary>
    public const string XminObservationsHeldKey = "observations_held";
    /// <summary>Metadata key: collections whose winning age sat at or above the shared warning bar, holder
    /// ignored — the horizon arm's numerator, carried so the reader sees it even though v1 cannot grade the
    /// arm (its denominator, <c>collection_log</c> captures, is outside the analysis read's table set).</summary>
    public const string XminObservationsAboveThresholdKey = "observations_above_threshold";
    public const string XminHeldFractionKey = "held_fraction";
    /// <summary>Metadata key: 1 when the identity arm held (majority of holder-bearing collections, at least
    /// the minimum observations, age at or above the bar) — the alert's chronic-holder shape.</summary>
    public const string XminIdentityArmKey = "identity_arm";
    public const string XminPeakWinningAgeKey = "peak_winning_age";
    public const string XminMinutesSinceLastHolderKey = "minutes_since_last_holder";
    /// <summary>Metadata key: the server's <c>autovacuum_freeze_max_age</c> when the wraparound read supplied
    /// one — the engine-defined critical bar for a held horizon (below).</summary>
    public const string XminFreezeMaxAgeKey = "freeze_max_age";

    /// <summary>
    /// Layer-1 base severity for the vacuum family. One arm per key; an unknown key under this source is 0.
    /// </summary>
    private static partial double ScoreVacuumFact(Fact fact) => fact.Key switch
    {
        PgTargetFactKeys.AutovacuumBacklog => ScoreAutovacuumBacklog(fact),
        PgTargetFactKeys.WraparoundTrend => ScoreWraparoundTrend(fact),
        PgTargetFactKeys.XminHold => ScoreXminHold(fact),
        _ => 0.0,
    };

    /// <summary>
    /// The collector emits the fact only for a table past its line for <see cref="BacklogPersistenceSamples"/>
    /// consecutive samples, so the ratio is at least 1 and the base at least 0.5 — a persistent backlog roots
    /// a story on its own, which is [D3]'s claim: the threshold IS the engine's trigger, so "past it and not
    /// clearing" is starvation by definition. Slope, per-table disable and the xmin hold are amplifiers, not
    /// base — the base answers "how far past its own line", the amplifiers answer "why it is not clearing".
    /// </summary>
    private static double ScoreAutovacuumBacklog(Fact fact)
    {
        var ratio = fact.Metadata.GetValueOrDefault(BacklogRatioKey, fact.Value);
        if (fact.Metadata.GetValueOrDefault(BacklogTrailingSamplesKey) < BacklogPersistenceSamples)
            return 0.0;

        /* Concerning = 1.0 is engine-defined: the table's own autovacuum trigger line (threshold +
           scale_factor × live tuples, reloptions honoured by the collector). Critical is the unmeasured
           multiple above — so the fact carries the lineage flag. */
        fact.Metadata["threshold_lineage"] = 0;
        return FactScorer.ApplyThresholdFormula(ratio, 1.0, BacklogCriticalRatio);
    }

    /// <summary>
    /// The wraparound grade, per counter, against the SHARED bars — the same four arms
    /// <c>PostgresAlertEvaluator.Grade</c> walks, in the same order (absolute arms first, unconditional;
    /// the relative Warning arm gated on the counter not having come down from its peak), so a database the
    /// alert would page at Warning scores at least 0.5 here and one it would page at Critical scores 1.0.
    /// Public because the collector uses the same function to choose WHICH database and counter the one fact
    /// carries — one definition of "worse", two callers. <c>PgTargetVacuumTests</c> pins this against the
    /// evaluator's verdict across a grid so the two walks cannot drift.
    /// </summary>
    /// <returns>Severity on the shared 0–1 base scale (0 = not graded) and the arm that produced it (the
    /// <see cref="WraparoundArmKey"/> encoding).</returns>
    public static (double Severity, int Arm) GradeWraparoundCounter(long age, long setting, bool freezingIsKeepingUp)
    {
        if (setting <= 0 || age <= 0)
            return (0.0, 0);

        /* engine-defined: every bar below is a PostgresOutagePredictorThresholds symbol — vacuum_failsafe_age's
           share of the 2^31 space, half that space, and 1.0× / 2.0× autovacuum_freeze_max_age. */
        var ceilingCriticalAt = (long)(PostgresOutagePredictorThresholds.WraparoundCeiling * PostgresOutagePredictorThresholds.WraparoundCriticalFractionOfCeiling);
        var ceilingWarnAt = (long)(PostgresOutagePredictorThresholds.WraparoundCeiling * PostgresOutagePredictorThresholds.WraparoundWarningFractionOfCeiling);
        var criticalAt = (long)(setting * PostgresOutagePredictorThresholds.WraparoundCriticalMultipleOfFreezeMaxAge);
        var warnAt = (long)(setting * PostgresOutagePredictorThresholds.WraparoundWarningFractionOfFreezeMaxAge);

        if (age >= ceilingCriticalAt)
            return (1.0, 4);
        if (age >= criticalAt)
            return (1.0, 3);

        /* Warning arms ramp from 0.5 at the bar toward 1.0 at the critical bar of the SAME kind, so a database
           at 1.9× its setting outranks one at 1.1× the way the alert's message would read them. engine-defined:
           both ends of each ramp are the shared symbols above. */
        if (age >= ceilingWarnAt)
        {
            var ramp = (double)(age - ceilingWarnAt) / Math.Max(1, ceilingCriticalAt - ceilingWarnAt);
            return (0.5 + 0.5 * Math.Clamp(ramp, 0.0, 1.0), 2);
        }

        if (age >= warnAt && !freezingIsKeepingUp)
        {
            var ramp = (double)(age - warnAt) / Math.Max(1, criticalAt - warnAt);
            return (0.5 + 0.5 * Math.Clamp(ramp, 0.0, 1.0), 1);
        }

        return (0.0, 0);
    }

    /// <summary>
    /// One fact per server, carrying the relatively-worst (database, counter); graded per counter through
    /// <see cref="GradeWraparoundCounter"/> and the WORSE grade taken — never the two counters collapsed
    /// (they have different governing settings; <c>PostgresAlertInfo</c>'s per-counter argument). A database
    /// whose sawtooth is healthy scores 0 and stays visible as context with its slope.
    /// </summary>
    private static double ScoreWraparoundTrend(Fact fact)
    {
        var xid = GradeWraparoundCounter(
            (long)fact.Metadata.GetValueOrDefault(WraparoundXidAgeKey),
            (long)fact.Metadata.GetValueOrDefault(WraparoundFreezeMaxAgeKey),
            fact.Metadata.GetValueOrDefault(WraparoundXidKeepingUpKey) >= 1);
        var multi = GradeWraparoundCounter(
            (long)fact.Metadata.GetValueOrDefault(WraparoundMultiXactAgeKey),
            (long)fact.Metadata.GetValueOrDefault(WraparoundMultiXactFreezeMaxAgeKey),
            fact.Metadata.GetValueOrDefault(WraparoundMultiXactKeepingUpKey) >= 1);

        return Math.Max(xid.Severity, multi.Severity);
    }

    /// <summary>
    /// The alert's IDENTITY arm, on the shared bars: this (source, holder) won a majority of the collections
    /// that recorded any holder, over at least the minimum observations, with the horizon held at least the
    /// warning age back. Base 0.5 at the bar — the alert's Warning — ramping to 1.0 where the held age reaches
    /// the server's own <c>autovacuum_freeze_max_age</c>: a horizon pinned THAT far back means the forced
    /// anti-wraparound vacuum the engine starts at that age cannot advance <c>relfrozenxid</c> either (VACUUM
    /// freezes only what is older than the horizon), which is the §3.3 chain into <c>PG_WRAPAROUND_TREND</c>
    /// stated as a bar. When the wraparound read supplied no setting the ramp has no top and the base stays
    /// flat at 0.5 rather than borrowing a number.
    ///
    /// <para>A holder the arm does NOT hold for — one long query, or a window too young for five
    /// observations — scores 0 and stays visible as context, exactly the shape the alert declines to page.
    /// The HORIZON arm (rotating holders) is not graded in v1: its honest denominator is the collector's
    /// SUCCESS captures in <c>collection_log</c>, which is not a table the analysis read may name; the
    /// numerator rides in the metadata so the reader can see the parade even though it is not scored.</para>
    /// </summary>
    private static double ScoreXminHold(Fact fact)
    {
        var age = (long)fact.Metadata.GetValueOrDefault(XminAgeKey, fact.Value);
        var total = fact.Metadata.GetValueOrDefault(XminObservationsTotalKey);
        var held = fact.Metadata.GetValueOrDefault(XminObservationsHeldKey);

        /* engine-defined: the three gates are the shared PostgresOutagePredictorThresholds symbols the alert's
           identity arm applies (age bar, majority fraction, minimum observations). */
        var identityArm = age >= PostgresOutagePredictorThresholds.XminAgeWarningThreshold
            && total >= PostgresOutagePredictorThresholds.XminMinimumObservations
            && total > 0
            && held / total >= PostgresOutagePredictorThresholds.XminPersistenceFraction;

        fact.Metadata[XminIdentityArmKey] = identityArm ? 1 : 0;
        if (!identityArm)
            return 0.0;

        /* engine-defined: concerning = the shared xmin warning age; critical = the server's own
           autovacuum_freeze_max_age when known and above it. Flat 0.5 (the alert's Warning) otherwise. */
        var freezeMaxAge = fact.Metadata.GetValueOrDefault(XminFreezeMaxAgeKey);
        if (freezeMaxAge <= PostgresOutagePredictorThresholds.XminAgeWarningThreshold)
            return 0.5;

        return FactScorer.ApplyThresholdFormula(age, PostgresOutagePredictorThresholds.XminAgeWarningThreshold, freezeMaxAge);
    }

    /// <summary>
    /// Layer-2 amplifiers for the vacuum family. Every boost is a co-fire read from the fact set — the
    /// chain §3.3 describes, made numeric: a held horizon makes a backlog unremovable, a backlog queues the
    /// forced freeze vacuum, both push the wraparound counter. Boost values are <b>unmeasured</b> — chosen,
    /// not measured; calibrate against the dogfood PostgreSQL fleet's co-fire rates before the next release.
    ///
    /// <para>Every co-fire predicate reads the sibling's <see cref="Fact.BaseSeverity"/>, never its
    /// <see cref="Fact.Severity"/>: <c>FactScorer.ScoreAll</c> assigns amplified severities one fact at a
    /// time, so when the backlog's amplifiers run the hold's <c>Severity</c> may still be 0 from Layer 1 —
    /// a predicate on it would match or not depending on emission ORDER. Base severity is final before any
    /// amplifier runs. (Found by executing the pins, not by reading.)</para>
    /// </summary>
    private static partial List<AmplifierDefinition> VacuumAmplifiers(string key) => key switch
    {
        PgTargetFactKeys.AutovacuumBacklog => AutovacuumBacklogAmplifiers(),
        PgTargetFactKeys.WraparoundTrend => WraparoundTrendAmplifiers(),
        PgTargetFactKeys.XminHold => XminHoldAmplifiers(),
        PgTargetFactKeys.ConfigAutovacuumOff or PgTargetFactKeys.ConfigMaintWorkMem => VacuumConfigCoFireAmplifiers(),
        _ => [],
    };

    private static List<AmplifierDefinition> AutovacuumBacklogAmplifiers() =>
    [
        new()
        {
            Description = "Dead tuples are RISING through the backlog while autovacuum ran on the table — it is running and losing, not merely late",
            /* unmeasured: chosen, not measured — calibrate against pg_autovacuum_stats before the next release. */
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.AutovacuumBacklog, out var f)
                && f.Metadata.GetValueOrDefault(BacklogSlopePerHourKey) > 0
                && f.Metadata.GetValueOrDefault(BacklogAutovacuumRunsKey) > 0,
        },
        new()
        {
            Description = "autovacuum_enabled is OFF on this table (reloption) — the engine will never clear it; only a manual VACUUM can",
            /* unmeasured: chosen, not measured — calibrate against pg_autovacuum_stats before the next release. */
            Boost = 0.5,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.AutovacuumBacklog, out var f)
                && f.Metadata.GetValueOrDefault(BacklogTableAutovacuumDisabledKey) >= 1,
        },
        new()
        {
            Description = "PG_XMIN_HOLD co-fired — a held xmin horizon makes these dead tuples unremovable, so vacuuming cannot clear the backlog",
            /* unmeasured: chosen, not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.XminHold, out var f) && f.BaseSeverity > 0,
        },
        new()
        {
            Description = "CONFIG_PG_AUTOVACUUM_OFF co-fired — autovacuum is off server-wide, so the backlog is by configuration, not by load",
            /* unmeasured: chosen, not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
            Boost = 0.5,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.ConfigAutovacuumOff, out var f) && f.BaseSeverity > 0,
        },
        new()
        {
            Description = "Many tables are persistently past their line — the worker pool, not this table's shape, is the bottleneck",
            /* unmeasured: BacklogManyTables above — chosen, not measured. */
            Boost = 0.2,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.AutovacuumBacklog, out var f)
                && f.Metadata.GetValueOrDefault(BacklogTablesKey) >= BacklogManyTables,
        },
    ];

    private static List<AmplifierDefinition> WraparoundTrendAmplifiers() =>
    [
        new()
        {
            Description = "PG_XMIN_HOLD co-fired — a held horizon stops freezing outright; the forced vacuum cannot advance relfrozenxid past it",
            /* unmeasured: chosen, not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
            Boost = 0.5,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.XminHold, out var f) && f.BaseSeverity > 0,
        },
        new()
        {
            Description = "PG_AUTOVACUUM_BACKLOG co-fired — the anti-wraparound vacuum queues behind tables autovacuum is already failing to clear",
            /* unmeasured: chosen, not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.AutovacuumBacklog, out var f) && f.BaseSeverity > 0,
        },
    ];

    private static List<AmplifierDefinition> XminHoldAmplifiers() =>
    [
        new()
        {
            Description = "PG_WRAPAROUND_TREND co-fired — the held horizon is already showing as a freeze age autovacuum cannot bring down",
            /* unmeasured: chosen, not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
            Boost = 0.5,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.WraparoundTrend, out var f) && f.BaseSeverity > 0,
        },
        new()
        {
            Description = "PG_AUTOVACUUM_BACKLOG co-fired — tables are past their trigger line and staying there: the damage the hold is doing",
            /* unmeasured: chosen, not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.AutovacuumBacklog, out var f) && f.BaseSeverity > 0,
        },
    ];

    /// <summary>
    /// D5 for the two vacuum-side config keys: a setting alone is a convention card (or, for
    /// <c>maintenance_work_mem</c>, nothing at all — it is not an advisory root); the backlog co-fire is what
    /// upgrades it. The base for both keys is lane 2's <c>ScoreConfigFact</c> (their SOURCE is <c>pg_config</c>);
    /// this boost is the vacuum family's half of the contract.
    /// </summary>
    private static List<AmplifierDefinition> VacuumConfigCoFireAmplifiers() =>
    [
        new()
        {
            Description = "PG_AUTOVACUUM_BACKLOG co-fired — tables are persistently past their own trigger line, so this setting has measured consequences",
            /* unmeasured: chosen, not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
            Boost = 0.5,
            Predicate = facts => facts.TryGetValue(PgTargetFactKeys.AutovacuumBacklog, out var f) && f.BaseSeverity > 0,
        },
    ];
}
