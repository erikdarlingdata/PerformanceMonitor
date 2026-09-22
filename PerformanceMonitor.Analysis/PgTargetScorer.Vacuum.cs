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
/// other's literal. Everything ELSE numeric in this file is the engine's own per-table trigger line
/// (engine-defined), a backlog bar the #3691 fleet calibration measured on 2026-09-19 (14 days × 50 Aurora
/// PostgreSQL clusters of the dogfood fleet, hourly <c>pg_autovacuum_stats</c> — the constants carry their
/// percentiles; the quantity is engine-neutral, the stock-PostgreSQL population is not yet measured), a
/// co-fire boost still marked unmeasured, or the §3.1 band for a table that turned autovacuum off and fell
/// behind (<see cref="AutovacuumDisabledBaseSeverity"/>, #3691 step 22 — the argument for its lineage is on
/// the constant). The backlog fact carries <c>threshold_lineage = 1</c> so <c>get_analysis_facts</c> shows
/// that every bar it was graded on is measured or engine-defined; the disabled-table fact carries the same.
///
/// <para><c>CONFIG_PG_AUTOVACUUM_DISABLED</c> has NO amplifier arm: the shared dispatcher's <c>CONFIG_PG_</c>
/// prefix arm carries it to lane 2's <c>ConfigAmplifiers</c>, which answers the empty list, and that is right
/// — the co-fire D5 would amplify it with (the backlog) is the condition of its emission, and the backlog fact
/// already carries the +0.5 "reloption off" amplifier for the same table. Two boosts on one reloption would
/// count it twice.</para>
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
    /// findings. Lineage: <b>measured</b> — ≈ the fleet p90 of each server's longest consecutive run of hourly
    /// samples past the line, over 14 days × 50 Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-19
    /// (median longest run 1 h, fleet p90 3 h, max 8 h; hours with any table past its line: median 2.1 %, max
    /// 21.5 %). Three sits at the top of routine — the WARNING shape. Measured on Aurora; the stock-PostgreSQL
    /// population is not yet measured. Consequence a caller should know: an
    /// <c>hours_back</c> below this cannot produce the fact at all — the metadata's
    /// <c>samples_in_window</c> says how many the window held.
    /// </summary>
    public const int BacklogPersistenceSamples = 3;

    /// <summary>
    /// The backlog ratio at which a persistent backlog grades 1.0: ten times the table's own trigger line.
    /// The CONCERNING bar is engine-defined (ratio 1.0 = <c>n_dead_tup</c> equals the table's
    /// <c>autovacuum_vacuum_threshold + autovacuum_vacuum_scale_factor × n_live_tup</c>, reloptions honoured
    /// — past it and not clearing is definitionally starvation, not opinion); this critical multiple is
    /// <b>measured</b> — ≈ p99.4 of per-server hourly maximum backlog ratios over 14 days × 50 Aurora PostgreSQL
    /// clusters of the dogfood fleet, 2026-09-19 (hours at or above 10×: at most 0.6 % on any cluster; per-server
    /// p50 of the hourly max 0.92 — tables live just under their own line, which is the engine working — p99
    /// median 1.0, fleet p90 of p99 2.2, one table at 350×). Measured on Aurora; the stock-PostgreSQL
    /// population is not yet measured. A fact graded through it carries <c>threshold_lineage = 1</c>.
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
    /// <c>autovacuum_max_workers</c>. Lineage: <b>measured</b> — above the fleet maximum of tables past their
    /// line at once (3) over 14 days × 50 Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-19; five was
    /// never reached, so the amplifier sits in the measured empty interval. Measured on Aurora; the
    /// stock-PostgreSQL population is not yet measured. The stock worker count is three, so five tables
    /// each holding a worker for over an hour is past what the default pool can rotate through.
    /// </summary>
    public const int BacklogManyTables = 5;

    /* ── CONFIG_PG_AUTOVACUUM_DISABLED (#3691 step 22, design §3.1) ── */

    /// <summary>
    /// Base severity for a table whose <c>autovacuum_enabled</c> reloption is off AND which has sat past its
    /// own trigger line for <see cref="BacklogPersistenceSamples"/> consecutive hourly samples. Lineage:
    /// <b>engine-defined</b> — the design's band for "the engine's own maintenance was switched off on an
    /// object that needs it" (§3.1): the number is D5's advisory base raised into the incident band because the
    /// workload co-fire D5 demands is BUILT INTO the emission — the collector emits the fact only for a table
    /// past the line PostgreSQL itself draws (<c>autovacuum_vacuum_threshold + autovacuum_vacuum_scale_factor ×
    /// n_live_tup</c>, reloptions honoured, or the insert arm), so a disabled table nobody needs vacuumed never
    /// scores at all. Below 1.0 because the ratio, slope and run count that say HOW bad live on
    /// <c>PG_AUTOVACUUM_BACKLOG</c>, which grades them and roots the story; this card is the named cause. The
    /// persistence gate is <see cref="BacklogPersistenceSamples"/> by name (measured, 2026-09-19) and the line is
    /// the engine's, so the fact carries <c>threshold_lineage = 1</c>.
    /// </summary>
    public const double AutovacuumDisabledBaseSeverity = 0.9;

    /// <summary>Metadata key: how many tables with <c>autovacuum_enabled = off</c> met the persistence gate
    /// this pass (the fact names the worst by ratio in <see cref="Fact.ObjectName"/> and the top three in
    /// <see cref="Fact.Ranked"/>; any beyond those are <c>get_pg_autovacuum_health</c>'s).</summary>
    public const string AutovacuumDisabledTablesKey = "disabled_tables_in_backlog";

    /// <summary>Metadata key: 1 when <c>CONFIG_PG_AUTOVACUUM_OFF</c> also reads off this pass (the launcher is
    /// off server-wide, so the per-table reloption is moot until it is back on), else 0. Stamped by the collector
    /// off the config fact emitted earlier in the same pass (emission order: Config before Vacuum).</summary>
    public const string AutovacuumDisabledServerOffKey = "server_autovacuum_off";

    /* Ranks 2 and 3 used to ride here as disabled_rank_{n}_ratio / _hours — numbers without names, because
       Fact.Metadata is doubles-only, so the advice could state their shape ("3.1× for 5 h") and not which table.
       #3691 lane 43 moved them onto Fact.Ranked, where an entry carries its NAME with the same two figures
       under BacklogRatioKey / BacklogHoursKey; the two key builders are gone rather than kept beside it,
       because two places to read the same rank is how a card ends up disagreeing with itself. */

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

    /// <summary>Metadata key: the LATEST capture's winning age — the horizon as it stands now, and the fact's
    /// <see cref="Fact.Value"/>. The GRADED age is <see cref="XminHorizonFloorAgeKey"/>: what the horizon stayed
    /// at or above for the whole held run.</summary>
    public const string XminAgeKey = "xmin_age";
    /// <summary>Metadata key: the DOMINANT holder kind across the held run — the source that won at least
    /// <see cref="XminHolderDominanceShare"/> of the run's captures — encoded by
    /// <see cref="PgTargetAdvice.HolderSourceCode"/> because metadata is doubles. 0 ("unknown") when no source
    /// reached that share: with holders of several KINDS taking turns there is no kind to name, and every
    /// consumer that keys on one (the idle-in-transaction leaf, the slot-xmin amplifier, this family's remedy
    /// arms) must close rather than pick. The fact's <see cref="Fact.ObjectName"/> carries <c>source:holder</c>
    /// as text, and only when ONE holder dominated (<see cref="XminHolderAttributedKey"/>).</summary>
    public const string XminHolderSourceKey = "holder_source";
    /// <summary>Metadata key: the kind that won the LATEST capture, encoded the same way. Attribution detail,
    /// never a gate: under alternation the latest winner is one of several and naming it as THE holder is the
    /// defect #3691 step 40 removed.</summary>
    public const string XminLatestHolderSourceKey = "latest_holder_source";
    /// <summary>Metadata key: distinct collections in the window that recorded ANY holder — the persistence
    /// denominator (the alert's <c>observations_total</c>). The collector writes nothing when the horizon is
    /// unheld, so a capture with rows is a capture where something held it.</summary>
    public const string XminObservationsTotalKey = "observations_total";
    /// <summary>Metadata key: the longest run of CONSECUTIVE captures whose winning age sat at or above the
    /// shared warning bar, holder ignored — the horizon's persistence, and the graded numerator. This is the
    /// count v1 carried as <c>observations_above_threshold</c> and declined to grade.</summary>
    public const string XminHeldCapturesKey = "held_captures";
    /// <summary>Metadata key: <see cref="XminHeldCapturesKey"/> over <see cref="XminObservationsTotalKey"/> — a
    /// fraction of holder-bearing captures, NOT of the collector's capture log (that denominator lives in
    /// <c>collection_log</c>, which an analysis read may not name; #3642).</summary>
    public const string XminHeldFractionKey = "held_fraction";
    /// <summary>Metadata key: the LOWEST winning age inside the held run — what the horizon stayed at or above
    /// for every capture of it, and therefore the age the severity ramp reads. A floor is the honest number for
    /// a persistence claim; the peaks are moments.</summary>
    public const string XminHorizonFloorAgeKey = "horizon_floor_age";
    /// <summary>Metadata key: the highest winning age inside the held run.</summary>
    public const string XminHorizonRunPeakAgeKey = "horizon_run_peak_age";
    /// <summary>Metadata key: 1 when the horizon's persistence arm held (a run of at least the minimum
    /// observations, a majority of the window's holder-bearing captures, the run's floor at or above the age
    /// bar). Named for what it grades: the horizon, not an identity.</summary>
    public const string XminPersistenceArmKey = "persistence_arm";
    /// <summary>Metadata key: distinct (source, holder) pairs that won a capture of the held run. 5 is the
    /// lane-36 shape — five equally-old transactions taking turns on one pinned horizon.</summary>
    public const string XminDistinctHoldersKey = "distinct_holders";
    /// <summary>Metadata keys: the share of the held run's captures won by each holder KIND. The four are
    /// disjoint and cover the collector's whole source vocabulary (both slot branches fold into the slot
    /// share), so they sum to 1 over any run the collector wrote.</summary>
    public const string XminWinnerBackendShareKey = "winner_source_backend_share";
    public const string XminWinnerSlotShareKey = "winner_source_slot_share";
    public const string XminWinnerStandbyShareKey = "winner_source_standby_share";
    public const string XminWinnerPreparedShareKey = "winner_source_prepared_share";
    /// <summary>Metadata key: captures of the held run won by the modal (source, holder) pair.</summary>
    public const string XminModalHolderCapturesKey = "modal_holder_captures";
    /// <summary>Metadata key: that pair's share of the held run.</summary>
    public const string XminModalHolderShareKey = "modal_holder_share";
    /// <summary>Metadata key: the modal SOURCE's share of the held run.</summary>
    public const string XminDominantSourceShareKey = "dominant_source_share";
    /// <summary>Metadata key: 1 when one holder's share reached <see cref="XminHolderDominanceShare"/> and the
    /// fact therefore names it; 0 when holders alternated and the fact names none.</summary>
    public const string XminHolderAttributedKey = "holder_attributed";
    /// <summary>Metadata key: the highest winning age anywhere in the window, held run or not.</summary>
    public const string XminPeakWinningAgeKey = "peak_winning_age";
    public const string XminMinutesSinceLastHolderKey = "minutes_since_last_holder";
    /// <summary>Metadata key: the server's <c>autovacuum_freeze_max_age</c> when the wraparound read supplied
    /// one — the engine-defined critical bar for a held horizon (below).</summary>
    public const string XminFreezeMaxAgeKey = "freeze_max_age";

    /// <summary>
    /// The share of a held run's captures one holder (or one holder KIND) must win before the fact names it.
    /// <para>unmeasured: chosen, not measured — a majority is the weakest claim that can still be called
    /// dominance, and it is the same half the alert evaluator's persistence fraction uses for its own arm.
    /// Calibrate against <c>pg_xmin_horizon</c>'s winner distribution before the next release.</para>
    /// <para>It gates ATTRIBUTION only — never the grade. A horizon held by five holders in turn scores
    /// exactly what one holder's would; all this decides is whether the fact says a name.</para>
    /// </summary>
    public const double XminHolderDominanceShare = 0.5;

    /// <summary>
    /// Layer-1 base severity for the vacuum family. One arm per key; an unknown key under this source is 0.
    /// </summary>
    private static partial double ScoreVacuumFact(Fact fact) => fact.Key switch
    {
        PgTargetFactKeys.AutovacuumBacklog => ScoreAutovacuumBacklog(fact),
        PgTargetFactKeys.WraparoundTrend => ScoreWraparoundTrend(fact),
        PgTargetFactKeys.XminHold => ScoreXminHold(fact),
        /* #3691 step 22: a CONFIG_PG_ key under the pg_vacuum SOURCE — the base dispatcher routes by source, so
           this arm (not lane 2's ScoreConfigFact) grades it; the per-table reloption is read from
           pg_autovacuum_stats, not pg_settings. */
        PgTargetFactKeys.ConfigAutovacuumDisabled => ScoreAutovacuumDisabled(fact),
        _ => 0.0,
    };

    /// <summary>
    /// <c>CONFIG_PG_AUTOVACUUM_DISABLED</c>: flat at <see cref="AutovacuumDisabledBaseSeverity"/> once the
    /// same two gates the backlog fact stands on hold — the persistence gate (the collector binds
    /// <see cref="BacklogPersistenceSamples"/> into the read; re-checked here so a hand-built fact under the
    /// gate scores 0, the conservative default) and the ratio at or past the table's own line. No ramp: the
    /// ratio is graded on the backlog fact, which the same pass emits for the same table (the backlog read
    /// ranks disabled tables first), and a second ramp on the same number would double-count it in the story
    /// severity. Not an advisory root and not evidence-gated in D5's sense: it clears the incident line on its
    /// own because the evidence is a precondition of its existence.
    /// </summary>
    private static double ScoreAutovacuumDisabled(Fact fact)
    {
        if (fact.Metadata.GetValueOrDefault(BacklogTrailingSamplesKey) < BacklogPersistenceSamples)
            return 0.0;

        /* engine-defined: 1.0 is the table's own autovacuum trigger line (threshold + scale_factor × live tuples,
           reloptions honoured by the collector) — the same line the backlog fact's concerning bar sits on. The
           persistence gate above is BacklogPersistenceSamples by name (measured, 2026-09-19); so lineage 1. */
        if (fact.Metadata.GetValueOrDefault(BacklogRatioKey, fact.Value) < 1.0)
            return 0.0;

        fact.Metadata["threshold_lineage"] = 1;
        return AutovacuumDisabledBaseSeverity;
    }

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
           scale_factor × live tuples, reloptions honoured by the collector). Critical is the measured multiple
           above (2026-09-19), as is the persistence gate — so the fact carries threshold_lineage = 1: every bar
           it was graded on is engine-defined or measured. */
        fact.Metadata["threshold_lineage"] = 1;
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
    /// The HORIZON's persistence, on the shared bars: the winning age — whoever won — stayed at or above the
    /// warning age for a run of consecutive captures at least the minimum-observations long, and that run is a
    /// majority of the window's holder-bearing captures. Base 0.5 at the bar — the alert's Warning — ramping to
    /// 1.0 where the held age reaches the server's own <c>autovacuum_freeze_max_age</c>: a horizon pinned THAT
    /// far back means the forced anti-wraparound vacuum the engine starts at that age cannot advance
    /// <c>relfrozenxid</c> either (VACUUM freezes only what is older than the horizon), which is the §3.3 chain
    /// into <c>PG_WRAPAROUND_TREND</c> stated as a bar. When the wraparound read supplied no setting the ramp
    /// has no top and the base stays flat at 0.5 rather than borrowing a number.
    ///
    /// <para><b>Why not the identity (#3691 step 40).</b> v1 applied these same three gates to one (source,
    /// holder) pair's win count — the alert evaluator's identity arm. A real stock storm held a 4.3 M-xid
    /// horizon for twenty-five minutes while five equally-old transactions alternated as the winner, so no
    /// identity was ever seen twice and the fact graded <b>0</b> on a horizon that was pinned the whole time.
    /// The bars, their lineage and their values are unchanged; what they are applied to is the horizon's age
    /// across consecutive captures, which is the thing being held. The holder is attributed separately (shares
    /// per kind, the distinct-holder count, the modal holder when one dominates) and attribution never touches
    /// the grade. A single persistent holder therefore grades exactly as it did in v1 — its run IS the
    /// horizon's run — which is pinned.</para>
    ///
    /// <para>The ramp reads the run's FLOOR (<see cref="XminHorizonFloorAgeKey"/>): the age the horizon stayed
    /// at or above for every capture of the run, not the moment it was worst. Absent (a planted fact, or a
    /// window with no run at all) it falls back to the latest winning age, which is what a one-capture claim
    /// can honestly say.</para>
    ///
    /// <para>A horizon the arm does NOT hold for — one long query, a run too short, a window too young —
    /// scores 0 and stays visible as context, exactly the shape the alert declines to page.</para>
    /// </summary>
    private static double ScoreXminHold(Fact fact)
    {
        var latestAge = (long)fact.Metadata.GetValueOrDefault(XminAgeKey, fact.Value);
        var floorAge = (long)fact.Metadata.GetValueOrDefault(XminHorizonFloorAgeKey, latestAge);
        var total = fact.Metadata.GetValueOrDefault(XminObservationsTotalKey);
        var heldCaptures = fact.Metadata.GetValueOrDefault(XminHeldCapturesKey);

        /* engine-defined: the three gates are the shared PostgresOutagePredictorThresholds symbols the alert
           applies (age bar, majority fraction, minimum observations) — same symbols, same values, now read
           against the horizon's consecutive-capture run instead of one holder's win count. */
        var persistenceArm = floorAge >= PostgresOutagePredictorThresholds.XminAgeWarningThreshold
            && heldCaptures >= PostgresOutagePredictorThresholds.XminMinimumObservations
            && total > 0
            && heldCaptures / total >= PostgresOutagePredictorThresholds.XminPersistenceFraction;

        fact.Metadata[XminPersistenceArmKey] = persistenceArm ? 1 : 0;
        if (!persistenceArm)
            return 0.0;

        /* engine-defined: concerning = the shared xmin warning age; critical = the server's own
           autovacuum_freeze_max_age when known and above it. Flat 0.5 (the alert's Warning) otherwise. */
        var freezeMaxAge = fact.Metadata.GetValueOrDefault(XminFreezeMaxAgeKey);
        if (freezeMaxAge <= PostgresOutagePredictorThresholds.XminAgeWarningThreshold)
            return 0.5;

        return FactScorer.ApplyThresholdFormula(floorAge, PostgresOutagePredictorThresholds.XminAgeWarningThreshold, freezeMaxAge);
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
            /* The gate is BacklogManyTables above (measured, 2026-09-19); the boost itself is unmeasured: chosen,
               not measured — calibrate against the dogfood PostgreSQL fleet before the next release. */
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
