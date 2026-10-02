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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// Turns the three Tier 0 PostgreSQL outage predictors into fired alerts.
/// <para>Deliberately NOT folded into <see cref="AlertEngine"/>. That class is 1500 lines shared with Lite
/// and is what SQL Server monitoring alerts through today; adding engine-specific branches inside it would
/// put the highest-blast-radius file on this branch in the path of every PostgreSQL change. This is a pure
/// function of (rows, settings) instead — no I/O, no state — so it is exhaustively testable and the host
/// keeps ownership of delivery and dedup.</para>
/// <para><b>Thresholds are constants, on purpose, for this first cut</b> — the three outage predictors'
/// on <see cref="PostgresOutagePredictorThresholds"/> (shared with the analysis scorer, #3542 D9), the
/// poison-wait ones on <see cref="PoisonWaitEvaluator"/>, aliased here under their original names. Every
/// one is derived from PostgreSQL's own mechanics rather than picked — see each constant — so there is no obvious knob a user
/// would set differently, and the product's stated position is to add configuration when it is genuinely
/// needed rather than speculatively (the same reasoning as having no collection-schedule settings). Making
/// them configurable means new columns on <c>config_alert_settings</c>, a migration, and Settings-window
/// work, and that is worth doing once someone wants a different number, not before.</para>
/// </summary>
public static class PostgresAlertEvaluator
{
    /* #3542 D9: the wraparound, xmin and slot bars below are ALIASES of PostgresOutagePredictorThresholds
       (PerformanceMonitor.Analysis), the one definition both this evaluator and the PostgreSQL-target
       analysis scorer grade on — so the surface that pages and the surface that narrates cannot disagree by
       construction. Each constant's derivation (why 1.0x / 2x the setting, why 74.5% of the ceiling, why 50
       million and a majority over at least five observations, why 10 GB) moved with the definition; the
       names are kept here so the read adapter, the host, the MCP wraparound tool and the tests that cite
       them keep compiling — the same shape the poison-wait constants take from PoisonWaitEvaluator. A
       source pin holds that this file carries none of those literals itself. */

    /// <summary>Alias of <see cref="PostgresOutagePredictorThresholds.WraparoundWarningFractionOfFreezeMaxAge"/>.</summary>
    public const double WraparoundWarningFractionOfFreezeMaxAge = PostgresOutagePredictorThresholds.WraparoundWarningFractionOfFreezeMaxAge;

    /// <summary>Alias of <see cref="PostgresOutagePredictorThresholds.WraparoundCriticalMultipleOfFreezeMaxAge"/>.</summary>
    public const double WraparoundCriticalMultipleOfFreezeMaxAge = PostgresOutagePredictorThresholds.WraparoundCriticalMultipleOfFreezeMaxAge;

    /// <summary>Alias of <see cref="PostgresOutagePredictorThresholds.WraparoundCeiling"/>.</summary>
    public const long WraparoundCeiling = PostgresOutagePredictorThresholds.WraparoundCeiling;

    /// <summary>Alias of <see cref="PostgresOutagePredictorThresholds.WraparoundCriticalFractionOfCeiling"/>.</summary>
    public const double WraparoundCriticalFractionOfCeiling = PostgresOutagePredictorThresholds.WraparoundCriticalFractionOfCeiling;

    /// <summary>Alias of <see cref="PostgresOutagePredictorThresholds.WraparoundWarningFractionOfCeiling"/>.</summary>
    public const double WraparoundWarningFractionOfCeiling = PostgresOutagePredictorThresholds.WraparoundWarningFractionOfCeiling;

    /// <summary>Alias of <see cref="PostgresOutagePredictorThresholds.XminAgeWarningThreshold"/>.</summary>
    public const long XminAgeWarningThreshold = PostgresOutagePredictorThresholds.XminAgeWarningThreshold;

    /// <summary>Alias of <see cref="PostgresOutagePredictorThresholds.XminPersistenceFraction"/>.</summary>
    public const double XminPersistenceFraction = PostgresOutagePredictorThresholds.XminPersistenceFraction;

    /// <summary>Alias of <see cref="PostgresOutagePredictorThresholds.XminMinimumObservations"/>.</summary>
    public const int XminMinimumObservations = PostgresOutagePredictorThresholds.XminMinimumObservations;

    /// <summary>
    /// The stable subject a horizon-arm fire carries when no single holder owns the incident (#3537). A
    /// constant, like the metric names above, because the host's per-subject cooldown and history dedup
    /// key on it: subjecting each parade member in turn would present every rotation as a brand-new
    /// incident and page once per member for one continuously-pinned horizon.
    /// </summary>
    public const string XminRotatingHoldersSubject = "rotating holders";

    /// <summary>Alias of <see cref="PostgresOutagePredictorThresholds.SlotRetainedWalWarningBytes"/>.</summary>
    public const long SlotRetainedWalWarningBytes = PostgresOutagePredictorThresholds.SlotRetainedWalWarningBytes;

    /* Poison waits (#2711). SQL Server's Poison Wait alert fires on THREADPOOL / RESOURCE_SEMAPHORE /
       RESOURCE_SEMAPHORE_QUERY_COMPILE because all three share one defining trait: near-zero in healthy
       operation, so any sustained accumulation is inherently abnormal. The Postgres analogue chosen here is
       the IPC pair — BtreePage (waiting for a B-tree index page another backend holds) and BufferIo
       (waiting on another backend's in-flight page read) — from the fleet research on #2711: exactly zero
       on 4 of 5 production servers over 24h, and a structural inter-backend contention signal on the fifth
       that no other alert covers. The other candidate there, Lock/Relation, was rejected because a
       relation-level lock wait IS blocking and the #2713 Blocking alert already owns that ground.

       The THRESHOLD SHAPE was deliberately not the SQL Server one of the time. That research also showed
       why: these events average 1-2 ms per wait at six-figure volumes, so the avg-ms-per-wait bar SQL
       Server then used (PoisonWaitThresholdMs) is meaningless against them — high-volume tiny waits never
       move an average. What identifies the poison state is TOTAL accumulated wait time crossing a bar,
       normalized to the window so the number reads as "how many backends were continuously stuck, on
       average".

       #3539 A4 ported this shape BACK to SQL Server — the per-wait average had the mirror-image failure
       there (one 600 ms wait paged, a storm of short waits slept) — and the three constants below are now
       ALIASES of the shared definitions on PoisonWaitEvaluator, kept under their old names so the read
       adapter, the host and the tests that cite them keep compiling. One alert name under one mute key
       means one thing on both engines; both engines' calibration margins are documented on the shared
       constants. */

    /// <summary>
    /// The evaluation window — shared with SQL Server; see <see cref="PoisonWaitEvaluator.WindowMinutes"/>
    /// for the coverage/under-fire reasoning. The read side sums deltas whose collection_time falls inside it.
    /// </summary>
    public const int PoisonWaitWindowMinutes = PoisonWaitEvaluator.WindowMinutes;

    /// <summary>
    /// Warning at one backend continuously stuck across the whole window (600 seconds of wait per 10
    /// minutes) — shared with SQL Server; both engines' measured margins (~160x here, ~100x there) are on
    /// <see cref="PoisonWaitEvaluator.WarningAvgWaiters"/>.
    /// </summary>
    public const double PoisonWaitWarningAvgWaiters = PoisonWaitEvaluator.WarningAvgWaiters;

    /// <summary>
    /// Critical at ten backends continuously stuck on average — shared with SQL Server; see
    /// <see cref="PoisonWaitEvaluator.CriticalAvgWaiters"/>.
    /// </summary>
    public const double PoisonWaitCriticalAvgWaiters = PoisonWaitEvaluator.CriticalAvgWaiters;

    /// <summary>
    /// #3444 (V122): the shipped default for <c>deadlocks.pg_count_threshold</c> — how many distinct
    /// deadlocks inside the rolling window fire the PostgreSQL Deadlocks alert.
    ///
    /// <para><b>1 is the value the alert shipped with as a compile-time constant</b>, restated here as a
    /// default rather than changed, so a store that upgrades and is never touched fires exactly where it
    /// did. The knob exists because the number's right value is per-workload; the number itself is not a
    /// product opinion this change is revising.</para>
    ///
    /// <para><b>Its own knob rather than SQL Server's <c>deadlocks.count_threshold</c>.</b> The two
    /// engines' counts are tuned against different evidence: SQL Server's is captured deadlock GRAPHS, this
    /// one is distinct deadlocks parsed from the server log, and an operator tuning one should not silently
    /// move the other (#3444). When V122 shipped this column there was a second reason — a PostgreSQL server
    /// had no deadlock BAND to agree with, because the fleet card read <c>v_deadlocks</c> and nulled the
    /// structural zero — and that reason is gone: since #3539 the PostgreSQL card bands its own
    /// <c>pg_stat_database.deadlocks</c> counter difference through the SAME
    /// <c>health_bands.deadlock_warn_per_hour</c> tiers. So the #3444 move (raise the fire gate to meet the
    /// band's Warning bar, so a page and an amber dot describe the same server) is now available on this
    /// knob too; the knob stays separate so making it is a choice. The <c>enabled</c> switch IS shared,
    /// matching <see cref="PoisonWaitMetric"/>'s own split: whether the condition is worth alerting on at
    /// all is one preference, and the volume at which it is worth a page is not.</para>
    /// </summary>
    public const int DeadlockCountThresholdDefault = 1;

    /// <summary>
    /// #3444 (V122): the shipped default for <c>blocking.pg_count_threshold</c> — how many distinct ROOT
    /// blockers inside the rolling window fire the PostgreSQL Blocking alert. 1, for the same
    /// reproduce-today's-behaviour reason as <see cref="DeadlockCountThresholdDefault"/>.
    ///
    /// <para><b>The denominators differ, which is the second reason this is not SQL Server's
    /// <c>blocking.count_threshold</c>.</b> That figure counts engine-recorded blocked-process reports and
    /// DMV snapshot rows. This one counts distinct root blockers found in a PERIODIC SAMPLE of
    /// <c>pg_stat_activity</c> — PostgreSQL records nothing unless asked, so the same numeral is a bar
    /// against two different measurement processes.</para>
    /// </summary>
    public const int BlockingCountThresholdDefault = 1;

    /// <summary>
    /// The FLOOR both #3444 count knobs clamp to on read, and the lower write bound
    /// <c>update_alert_settings</c> enforces — one value so a bound the write path ACCEPTS cannot be a
    /// value the read path then rewrites.
    ///
    /// <para><b>It is what keeps the knob a threshold.</b> At 0 the gate's <c>count &gt;= threshold</c>
    /// test is true for a count of zero, so a store row hand-edited to 0 would fire "Deadlocks Detected"
    /// on a server with no deadlocks. 1 is the tightest setting that still describes an occurrence. The
    /// SQL Server twins take the same write bound and have no read-side floor; that asymmetry is named
    /// rather than copied — see <c>DarlingAlertSettings</c>.</para>
    /// </summary>
    public const int CountThresholdFloor = 1;

    /// <summary>Metric names, kept as constants because mute rules and history filtering match on them.</summary>
    public const string WraparoundMetric = "PostgreSQL Wraparound Risk";
    public const string XminHorizonMetric = "PostgreSQL Vacuum Horizon Blocked";
    public const string SlotRetentionMetric = "PostgreSQL Replication Slot Retention";

    /// <summary>
    /// Deliberately the EXACT SQL Server metric string, not a "PostgreSQL "-prefixed one like the Tier 0
    /// trio above — the same parity reasoning the #2711 Deadlocks/Blocking alerts documented: a mute rule,
    /// a history filter, or a dashboard built against "Poison Wait" should not have to know which engine a
    /// server runs, and the shared PoisonWaitEnabled switch already governs both engines' versions.
    /// </summary>
    public const string PoisonWaitMetric = "Poison Wait";

    /// <summary>
    /// One evaluated finding, ready for the host to turn into an <see cref="AlertOutcome"/>. Kept separate
    /// from AlertOutcome so this library stays free of the host's mute/dedup concerns.
    /// </summary>
    /// <param name="MetricName">Mute-rule and history key.</param>
    /// <param name="Severity">Graded per condition.</param>
    /// <param name="Subject">The specific database / holder / slot, for the host's dedup fingerprint.</param>
    /// <param name="CurrentValue">Human-readable current value.</param>
    /// <param name="ThresholdValue">Human-readable threshold breached.</param>
    /// <param name="ShortMessage">One-line body, server-name prefix excluded, matching the engine's contract.</param>
    /// <param name="NumericCurrentValue">For history charting.</param>
    /// <param name="NumericThresholdValue">Numeric twin.</param>
    public sealed record Finding(
        string MetricName,
        AlertSeverityLevel Severity,
        string Subject,
        string CurrentValue,
        string ThresholdValue,
        string ShortMessage,
        double? NumericCurrentValue,
        double? NumericThresholdValue);

    /// <summary>
    /// Evaluates every predictor. Returns findings worst-first so a host that caps delivery keeps the ones
    /// that matter. An empty list is the healthy case and must not be confused with "not evaluated".
    /// </summary>
    public static List<Finding> Evaluate(
        IReadOnlyList<PostgresWraparoundAlertInfo>? wraparound,
        PostgresXminHorizonAlertInfo? xmin,
        IReadOnlyList<PostgresSlotAlertInfo>? slots)
    {
        var findings = new List<Finding>();

        if (wraparound is not null)
        {
            foreach (var db in wraparound)
            {
                var finding = EvaluateWraparound(db);
                if (finding is not null)
                {
                    findings.Add(finding);
                }
            }
        }

        var xminFinding = EvaluateXmin(xmin);
        if (xminFinding is not null)
        {
            findings.Add(xminFinding);
        }

        if (slots is not null)
        {
            foreach (var slot in slots)
            {
                var finding = EvaluateSlot(slot);
                if (finding is not null)
                {
                    findings.Add(finding);
                }
            }
        }

        findings.Sort((a, b) => b.Severity.CompareTo(a.Severity));
        return findings;
    }

    public static Finding? EvaluateWraparound(PostgresWraparoundAlertInfo db)
    {
        ArgumentNullException.ThrowIfNull(db);

        /* A non-positive setting would make every derived threshold zero and fire on every database forever,
           so a missing or nonsensical value means "cannot judge" rather than "everything is critical". Judged
           per counter now: a server can have a sane autovacuum_freeze_max_age and a broken multixact one. */
        var xidJudgeable = db.AutovacuumFreezeMaxAge > 0;
        var multiJudgeable = db.AutovacuumMultixactFreezeMaxAge > 0;
        if (!xidJudgeable && !multiJudgeable)
        {
            return null;
        }

        /* Each counter against ITS OWN governing setting. Defaults differ by 2x (200M vs 400M), so grading
           MultiXact age against the XID setting warned 2.2x premature. Each also carries its OWN
           FreezingIsKeepingUp (#2689) - a database can have a healthy XID sawtooth and a stuck MultiXact, or
           the reverse, and gating one counter's Warning off the other's recovery would be exactly backwards. */
        var xid = xidJudgeable ? Grade(db.XidAge, db.AutovacuumFreezeMaxAge, db.XidFreezingIsKeepingUp) : null;
        var multi = multiJudgeable
            ? Grade(db.MultiXactAge, db.AutovacuumMultixactFreezeMaxAge, db.MultiXactFreezingIsKeepingUp)
            : null;

        /* The worse breach wins, and "worse" is the relative position, not the raw age — the only comparison
           that means anything when the denominators differ. Severity first so a Critical MultiXact cannot be
           hidden behind a merely-warning XID that happens to have a bigger number. */
        var multiWins = multi is not null
            && (xid is null
                || multi.Value.Severity > xid.Value.Severity
                || (multi.Value.Severity == xid.Value.Severity
                    && db.MultiXactFractionOfSetting > db.XidFractionOfSetting));

        var winner = multiWins ? multi : xid;
        if (winner is null)
        {
            return null;
        }

        var counter = multiWins ? "MultiXact" : "XID";
        var setting = multiWins ? db.AutovacuumMultixactFreezeMaxAge : db.AutovacuumFreezeMaxAge;
        var settingName = multiWins ? "autovacuum_multixact_freeze_max_age" : "autovacuum_freeze_max_age";
        var age = multiWins ? db.MultiXactAge : db.XidAge;
        var (severity, breached, viaCeiling) = winner.Value;
        var critical = severity == AlertSeverityLevel.Critical;
        var pctOfCeiling = 100.0 * age / WraparoundCeiling;

        /* #2689: viaCeiling now applies to BOTH severities (the absolute early-Warning arm as well as the
           absolute Critical arm), so the ceiling fraction quoted has to match whichever one actually fired -
           quoting the Critical fraction on a Warning message would contradict the severity right next to it. */
        var ceilingFraction = critical ? WraparoundCriticalFractionOfCeiling : WraparoundWarningFractionOfCeiling;

        var thresholdValue = viaCeiling
            ? $"{breached:N0} ({ceilingFraction * 100:0.#}% of the 2^31 wraparound space)"
            : critical
                ? $"{breached:N0} (2x {settingName} {setting:N0})"
                : $"{breached:N0} (at {settingName} {setting:N0}, not recovering)";

        string shortMessage;
        if (critical)
        {
            shortMessage = $"[{db.DatabaseName}] {counter} age {age:N0} is {pctOfCeiling:0.#}% of the way to the "
                + "wraparound wall"
                + (viaCeiling
                    ? " — past vacuum_failsafe_age, where PostgreSQL abandons its cost limits and skips "
                      + "index cleanup trying to catch up."
                    : $" and past twice {settingName} ({setting:N0}) — autovacuum's own wraparound defence "
                      + "is not keeping up.")
                + " At the wall the server stops accepting writes, and the remedy is hours of vacuuming, so act now.";
        }
        else if (viaCeiling)
        {
            shortMessage = $"[{db.DatabaseName}] {counter} age {age:N0} is {pctOfCeiling:0.#}% of the way to the "
                + "wraparound wall — already well past the routine vacuum-forcing point on this cluster's own "
                + $"{settingName}. Vacuum now rather than waiting to see if autovacuum keeps pace.";
        }
        else
        {
            shortMessage = $"[{db.DatabaseName}] {counter} age {age:N0} has reached {settingName} ({setting:N0}) — "
                + "the point where autovacuum forces a wraparound-prevention vacuum — and has not come back down "
                + "since. Confirm autovacuum is actually keeping up rather than merely running.";
        }

        return new Finding(
            WraparoundMetric,
            severity,
            db.DatabaseName,
            $"{counter} age {age:N0} in [{db.DatabaseName}]",
            thresholdValue,
            shortMessage,
            age,
            breached);
    }

    /// <summary>
    /// Grades one counter against its own setting, then OR-s in the absolute arms. Returns the severity, the
    /// threshold actually breached, and whether it was one of the absolute ones — so the message can say which.
    /// </summary>
    /// <param name="freezingIsKeepingUp">#2689: whether this counter has come down from its own recent peak.
    /// Gates the RELATIVE Warning arm only - a database sitting above its own setting but resetting every
    /// cycle is the routine sawtooth, not a risk; the absolute arms stay unconditional, same as Critical's.</param>
    private static (AlertSeverityLevel Severity, long Breached, bool ViaCeiling)? Grade(
        long age, long setting, bool freezingIsKeepingUp)
    {
        var warnAt = (long)(setting * WraparoundWarningFractionOfFreezeMaxAge);
        var criticalAt = (long)(setting * WraparoundCriticalMultipleOfFreezeMaxAge);
        var ceilingCriticalAt = (long)(WraparoundCeiling * WraparoundCriticalFractionOfCeiling);
        var ceilingWarnAt = (long)(WraparoundCeiling * WraparoundWarningFractionOfCeiling);

        /* The absolute arms are checked FIRST and independently: on a cluster tuned past ~1.07B the relative
           arms sit beyond the wall and can never be reached, which is precisely where an alert is needed
           most. Unconditional on FreezingIsKeepingUp - see the constant doc comments for why. */
        if (age >= ceilingCriticalAt)
        {
            return (AlertSeverityLevel.Critical, ceilingCriticalAt, true);
        }

        if (age >= criticalAt)
        {
            return (AlertSeverityLevel.Critical, criticalAt, false);
        }

        if (age >= ceilingWarnAt)
        {
            return (AlertSeverityLevel.Warning, ceilingWarnAt, true);
        }

        return age >= warnAt && !freezingIsKeepingUp
            ? (AlertSeverityLevel.Warning, warnAt, false)
            : null;
    }

    public static Finding? EvaluateXmin(PostgresXminHorizonAlertInfo? xmin)
    {
        if (xmin is null || xmin.XminAge < XminAgeWarningThreshold)
        {
            return null;
        }

        /* The persistence gate, two arms (#3537). Without any gate this fires on any long-running report,
           which is how an alert earns a mute rule instead of a response.

           The IDENTITY arm is the original: THIS holder won a majority of the collections that recorded
           any holder — the chronic-holder shape, and the arm that can name the thing to kill. Its
           denominator counts holder-bearing collections only (deliberately: see the read adapter), which
           is why it also needs the observation floor — the first holder after quiet hours is 1 win in 1
           observation, and 100% of one sample is not "chronic" however the fraction reads.

           The HORIZON arm covers what the identity fraction structurally cannot see: a horizon pinned
           past the age threshold in a majority of the window's REAL captures while the holder identity
           rotates. Each parade member is individually transient, so no identity fraction ever accumulates
           — but the alert's own claim ("vacuum is reclaiming nothing cluster-wide") is about the horizon,
           not the holder, and it is continuously true. Same majority standard, honest denominator for
           each claim: the identity claim is about the collections that had a holder, the horizon claim is
           about every time the collector looked. A capture count of 0 — an adapter that supplied none, or
           a log write that failed — floors the arm out rather than firing, the conservative default. */
        var identityFractionHolds = xmin.ObservationsTotal > 0
            && (double)xmin.ObservationsHeld / xmin.ObservationsTotal >= XminPersistenceFraction;

        var identityArm = identityFractionHolds && xmin.ObservationsTotal >= XminMinimumObservations;

        var horizonArm = xmin.CapturesInWindow >= XminMinimumObservations
            && (double)xmin.ObservationsAboveThreshold / xmin.CapturesInWindow >= XminPersistenceFraction;

        if (!identityArm && !horizonArm)
        {
            return null;
        }

        var holder = string.IsNullOrWhiteSpace(xmin.Identifier)
            ? xmin.Source
            : $"{xmin.Source}:{xmin.Identifier}";

        if (identityArm)
        {
            return new Finding(
                XminHorizonMetric,
                AlertSeverityLevel.Warning,
                holder,
                $"{xmin.XminAge:N0} transactions held by {holder}",
                $"{XminAgeWarningThreshold:N0} transactions, held in at least "
                    + $"{XminPersistenceFraction:P0} of observations",
                $"Vacuum is reclaiming nothing cluster-wide: {holder} is holding the xmin horizon "
                    + $"{xmin.XminAge:N0} transactions back, in {xmin.ObservationsHeld} of "
                    + $"{xmin.ObservationsTotal} observations. {RemedyFor(xmin.Source)}"
                    + (string.IsNullOrWhiteSpace(xmin.Detail) ? string.Empty : $" ({xmin.Detail})"),
                xmin.XminAge,
                XminAgeWarningThreshold);
        }

        /* Horizon-arm fire. Two shapes reach here, told apart by the identity FRACTION alone (the floor
           is what a freshly-started window cannot yet satisfy): when the fraction holds, the latest
           holder has won the collections that recorded one — a chronic holder observed through a window
           still too young for the identity arm, so it keeps the subject and the naming. When it does not,
           the holders are rotating, and the subject must NOT be the latest member: the incident is the
           horizon, and a per-member subject would sidestep the host's per-subject cooldown to page once
           per parade member. The remedy still names the latest holder's cause — it is the one thing
           currently actionable either way. */
        var rotating = !identityFractionHolds;
        var subject = rotating ? XminRotatingHoldersSubject : holder;
        var holderClause = rotating
            ? $"by a succession of different holders rather than one chronic one — the latest is {holder}"
            : $"by {holder}, the winner in {xmin.ObservationsHeld} of the {xmin.ObservationsTotal} "
                + "collections that recorded a holder";

        return new Finding(
            XminHorizonMetric,
            AlertSeverityLevel.Warning,
            subject,
            $"{xmin.XminAge:N0} transactions held by {subject}",
            $"{XminAgeWarningThreshold:N0} transactions, behind in at least "
                + $"{XminPersistenceFraction:P0} of the window's captures",
            $"Vacuum is reclaiming nothing cluster-wide: the xmin horizon has been at least "
                + $"{XminAgeWarningThreshold:N0} transactions behind in {xmin.ObservationsAboveThreshold} of "
                + $"the window's {xmin.CapturesInWindow} collections, held {holderClause}; it currently "
                + $"stands {xmin.XminAge:N0} back. {RemedyFor(xmin.Source)}"
                + (string.IsNullOrWhiteSpace(xmin.Detail) ? string.Empty : $" ({xmin.Detail})"),
            xmin.XminAge,
            XminAgeWarningThreshold);
    }

    /// <summary>
    /// The four causes look identical by symptom and need completely different fixes, so the alert carries
    /// the fix rather than making the reader go and find out which one it is.
    /// </summary>
    public static string RemedyFor(string? source) => source switch
    {
        "session" => "A backend is idle in transaction — end it, and look at why the application left it open.",
        "replication_slot" => "An inactive replication slot is pinning it — if its consumer is gone, the slot must be dropped.",
        "replication_slot_catalog" => "A logical slot's catalog_xmin is pinning it — its consumer is not confirming; check the CDC/logical pipeline.",
        "standby_feedback" => "A standby's hot_standby_feedback is pinning it — a long query on the replica, or the feedback setting itself.",
        "prepared_transaction" => "An orphaned prepared transaction is pinning it — COMMIT PREPARED or ROLLBACK PREPARED it.",
        _ => "Unrecognized holder — read pg_stat_activity, pg_replication_slots and pg_prepared_xacts directly.",
    };

    public static Finding? EvaluateSlot(PostgresSlotAlertInfo slot)
    {
        var terminal = slot.WalStatus is "lost" or "unreserved";
        var growing = slot.RetainedWalGrowthBytes > 0;
        var overBytes = slot.RetainedWalBytes >= SlotRetainedWalWarningBytes;

        if (!terminal && !overBytes)
        {
            return null;
        }

        /* Grading. A terminal state has already failed. An inactive slot over the byte line and still
           growing is the disk-fill emergency — unbounded by default, so nothing will stop it. Anything
           else over the line is a warning. */
        var severity = terminal || (!slot.IsActive && growing && overBytes)
            ? AlertSeverityLevel.Critical
            : AlertSeverityLevel.Warning;

        var gb = slot.RetainedWalBytes / 1024.0 / 1024.0 / 1024.0;
        var growthGb = slot.RetainedWalGrowthBytes / 1024.0 / 1024.0 / 1024.0;

        var body = slot.WalStatus switch
        {
            "lost" => $"Slot [{slot.SlotName}] is LOST — the WAL its consumer needs is gone and the slot "
                    + "cannot resume. It has to be recreated, and its consumer resynchronised.",
            "unreserved" => $"Slot [{slot.SlotName}] is UNRESERVED — required WAL has already been removed. "
                          + "Its consumer will fail on next connect.",
            _ when severity == AlertSeverityLevel.Critical =>
                $"Slot [{slot.SlotName}] is inactive and still accumulating: {gb:N1} GB retained, "
                + $"up {growthGb:N1} GB. WAL retention is unbounded by default "
                + "(max_slot_wal_keep_size = -1), so this fills the volume and stops the server. If the "
                + "consumer is gone, drop the slot.",
            _ => $"Slot [{slot.SlotName}] is retaining {gb:N1} GB of WAL"
               + (growing ? $" and growing ({growthGb:N1} GB this window)" : " (not growing)")
               + $", status {slot.WalStatus ?? "unknown"}, "
               + (slot.IsActive ? "consumer active." : "consumer INACTIVE."),
        };

        return new Finding(
            SlotRetentionMetric,
            severity,
            slot.SlotName,
            terminal
                ? $"wal_status = {slot.WalStatus} on [{slot.SlotName}]"
                : string.Create(CultureInfo.InvariantCulture, $"{gb:N1} GB retained by [{slot.SlotName}]"),
            terminal
                ? "any (wal_status lost/unreserved is a failure that has already happened)"
                : string.Create(CultureInfo.InvariantCulture,
                    $"{SlotRetainedWalWarningBytes / 1024.0 / 1024.0 / 1024.0:N0} GB retained"),
            body,
            slot.RetainedWalBytes,
            terminal ? null : SlotRetainedWalWarningBytes);
    }

    /// <summary>
    /// The Postgres Poison Wait analogue (#2711), one finding per wait event over the bar, worst-first.
    /// <para>Kept OUT of <see cref="Evaluate"/> on purpose: the Tier 0 trio are levels with a fire-only
    /// delivery loop, while this is an accumulation check whose host needs the Detected/Cleared active
    /// flag and the #2704 unrefreshed-source-row guard — state that belongs to the host, not here. Pure
    /// function of the rows, same as everything else in this class.</para>
    /// <para>PER EVENT, not summed across the poison set: BtreePage (a hot index insert point) and
    /// BufferIo (backends stacked behind in-flight reads) are different incidents with different remedies,
    /// and the per-subject cooldown the host keys on (#1140) only works if each is its own finding. The
    /// conservative consequence — two events each just under the bar do not fire — is accepted: an alert
    /// neither earns is worse than one arriving a window later.</para>
    /// </summary>
    public static List<Finding> EvaluatePoisonWaits(IReadOnlyList<PostgresPoisonWaitAlertInfo>? waits)
    {
        var findings = new List<Finding>();
        if (waits is null)
        {
            return findings;
        }

        foreach (var wait in waits)
        {
            var finding = EvaluatePoisonWait(wait);
            if (finding is not null)
            {
                findings.Add(finding);
            }
        }

        findings.Sort((a, b) =>
        {
            var bySeverity = b.Severity.CompareTo(a.Severity);
            return bySeverity != 0
                ? bySeverity
                : Comparer<double?>.Default.Compare(b.NumericCurrentValue, a.NumericCurrentValue);
        });
        return findings;
    }

    public static Finding? EvaluatePoisonWait(PostgresPoisonWaitAlertInfo wait)
    {
        ArgumentNullException.ThrowIfNull(wait);

        /* Accumulated wait time, normalized to the window: "how many backends were continuously stuck,
           on average". NOT avg-ms-per-wait — the #2711 fleet data shows these events averaging 1-2 ms at
           six-figure volumes, a shape a per-wait average can never see (see the constants block above). */
        var windowMs = PoisonWaitWindowMinutes * 60_000.0;
        var avgWaiters = wait.AccumulatedWaitMs / windowMs;
        if (avgWaiters < PoisonWaitWarningAvgWaiters)
        {
            return null;
        }

        var critical = avgWaiters >= PoisonWaitCriticalAvgWaiters;
        var breachedAvg = critical ? PoisonWaitCriticalAvgWaiters : PoisonWaitWarningAvgWaiters;
        var subject = PoisonWaitSubject(wait);
        var accumulatedSeconds = wait.AccumulatedWaitMs / 1000;

        return new Finding(
            PoisonWaitMetric,
            critical ? AlertSeverityLevel.Critical : AlertSeverityLevel.Warning,
            subject,
            string.Create(CultureInfo.InvariantCulture,
                $"{accumulatedSeconds:N0}s of {subject} wait in {PoisonWaitWindowMinutes}m"),
            string.Create(CultureInfo.InvariantCulture,
                $"{breachedAvg * PoisonWaitWindowMinutes * 60:N0}s accumulated over {PoisonWaitWindowMinutes}m "
                + $"(an average of {breachedAvg:0.#} backend(s) continuously waiting)"),
            string.Create(CultureInfo.InvariantCulture,
                $"[{subject}] {accumulatedSeconds:N0}s of wait accumulated in the last "
                + $"{PoisonWaitWindowMinutes} minutes across {wait.AccumulatedWaits:N0} waits — on average "
                + $"{avgWaiters:N1} backend(s) continuously stuck. {PoisonWaitRemedyFor(wait.WaitEvent)}"),
            wait.AccumulatedWaitMs,
            breachedAvg * windowMs);
    }

    /// <summary>
    /// The subject string a poison row alerts under — Postgres's own <c>type:event</c> display convention,
    /// in the server's stored casing. One definition, because the host matches read rows back to findings
    /// by this exact string for the #2704 collection-time guard, and a drifted twin would silently
    /// disconnect the guard from the findings it protects.
    /// </summary>
    public static string PoisonWaitSubject(PostgresPoisonWaitAlertInfo wait) =>
        $"{wait.WaitType}:{wait.WaitEvent}";

    /// <summary>
    /// The two poison events present identically as "everything got slow at once" and have completely
    /// different fixes, so the alert carries the fix — the same reasoning as <see cref="RemedyFor"/>.
    /// Matched case-insensitively because wait-event name casing differs between Aurora majors.
    /// </summary>
    public static string PoisonWaitRemedyFor(string? waitEvent) => waitEvent?.ToLowerInvariant() switch
    {
        "btreepage" => "Backends are queueing on individual B-tree index pages — a hot index insert point "
            + "is serializing them. Sample pg_stat_activity for the statements waiting on this event and "
            + "look at the indexes their writes converge on.",
        "bufferio" => "Backends are stacked behind one another's in-flight page reads — the same pages are "
            + "being demanded faster than storage returns them. Look for a working set outgrowing "
            + "shared_buffers, or a storage latency shift in the I/O statistics.",
        _ => "Sample pg_stat_activity for the statements waiting on this event.",
    };
}
