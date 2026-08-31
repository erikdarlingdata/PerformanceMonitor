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
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// Turns the three Tier 0 PostgreSQL outage predictors into fired alerts.
/// <para>Deliberately NOT folded into <see cref="AlertEngine"/>. That class is 1500 lines shared with Lite
/// and is what SQL Server monitoring alerts through today; adding engine-specific branches inside it would
/// put the highest-blast-radius file on this branch in the path of every PostgreSQL change. This is a pure
/// function of (rows, settings) instead — no I/O, no state — so it is exhaustively testable and the host
/// keeps ownership of delivery and dedup.</para>
/// <para><b>Thresholds are constants here, on purpose, for this first cut.</b> Every one is derived from
/// PostgreSQL's own mechanics rather than picked — see each constant — so there is no obvious knob a user
/// would set differently, and the product's stated position is to add configuration when it is genuinely
/// needed rather than speculatively (the same reasoning as having no collection-schedule settings). Making
/// them configurable means new columns on <c>config_alert_settings</c>, a migration, and Settings-window
/// work, and that is worth doing once someone wants a different number, not before.</para>
/// </summary>
public static class PostgresAlertEvaluator
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
    public const double WraparoundWarningFractionOfFreezeMaxAge = 1.0;
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
    public const long XminAgeWarningThreshold = 50_000_000;
    public const double XminPersistenceFraction = 0.5;

    /* Replication slots. No byte threshold for the terminal states — `lost` and `unreserved` are failures
       that have already happened, at any size. For a slot merely retaining WAL, 10 GB is the point where
       an unbounded pile stops being noise on any volume worth monitoring; growth is what escalates it,
       since max_slot_wal_keep_size defaults to -1 and nothing will stop it. */
    public const long SlotRetainedWalWarningBytes = 10L * 1024 * 1024 * 1024;

    /* Poison waits (#2711). SQL Server's Poison Wait alert fires on THREADPOOL / RESOURCE_SEMAPHORE /
       RESOURCE_SEMAPHORE_QUERY_COMPILE because all three share one defining trait: near-zero in healthy
       operation, so any sustained accumulation is inherently abnormal. The Postgres analogue chosen here is
       the IPC pair — BtreePage (waiting for a B-tree index page another backend holds) and BufferIo
       (waiting on another backend's in-flight page read) — from the fleet research on #2711: exactly zero
       on 4 of 5 production servers over 24h, and a structural inter-backend contention signal on the fifth
       that no other alert covers. The other candidate there, Lock/Relation, was rejected because a
       relation-level lock wait IS blocking and the #2713 Blocking alert already owns that ground.

       The THRESHOLD SHAPE is deliberately not the SQL Server one. That research also showed why: these
       events average 1-2 ms per wait at six-figure volumes, so SQL Server's avg-ms-per-wait bar
       (PoisonWaitThresholdMs) is meaningless against them — high-volume tiny waits never move an average.
       What identifies the poison state is TOTAL accumulated wait time crossing a bar, normalized to the
       window so the number reads as "how many backends were continuously stuck, on average". */

    /// <summary>
    /// The evaluation window, matching the SQL Server poison-wait read's own 10-minute recency window so
    /// the two engines' alerts answer over the same horizon. The read side sums deltas whose
    /// collection_time falls inside it; partial coverage (service just started, collector gap) undercounts
    /// and therefore under-fires — the correct failure direction for an alert that pages.
    /// </summary>
    public const int PoisonWaitWindowMinutes = 10;

    /// <summary>
    /// Warning fires when accumulated wait time averages one backend continuously stuck across the whole
    /// window (600 seconds of wait per 10 minutes). Calibrated against the #2711 fleet data: the WORST
    /// server observed averaged ~0.006 concurrently-waiting backends on IPC:BtreePage over 24h
    /// (538,850 ms / 86,400 s), so this bar sits ~160x above the worst healthy baseline seen anywhere on
    /// the fleet — nothing measured to date would have fired it, which is the point: these events are
    /// near-zero when healthy, and a full backend pinned for ten straight minutes is categorically not that.
    /// </summary>
    public const double PoisonWaitWarningAvgWaiters = 1.0;

    /// <summary>
    /// Critical at ten backends continuously stuck on average — an active contention collapse, where the
    /// pile-up itself is throttling throughput rather than merely taxing it.
    /// </summary>
    public const double PoisonWaitCriticalAvgWaiters = 10.0;

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

        /* The persistence gate. Without it this fires on any long-running report, which is how an alert
           earns a mute rule instead of a response. */
        var persistent = xmin.ObservationsTotal > 0
            && (double)xmin.ObservationsHeld / xmin.ObservationsTotal >= XminPersistenceFraction;

        if (!persistent)
        {
            return null;
        }

        var subject = string.IsNullOrWhiteSpace(xmin.Identifier)
            ? xmin.Source
            : $"{xmin.Source}:{xmin.Identifier}";

        return new Finding(
            XminHorizonMetric,
            AlertSeverityLevel.Warning,
            subject,
            $"{xmin.XminAge:N0} transactions held by {subject}",
            $"{XminAgeWarningThreshold:N0} transactions, held in at least "
                + $"{XminPersistenceFraction:P0} of observations",
            $"Vacuum is reclaiming nothing cluster-wide: {subject} is holding the xmin horizon "
                + $"{xmin.XminAge:N0} transactions back, in {xmin.ObservationsHeld} of "
                + $"{xmin.ObservationsTotal} observations. {RemedyFor(xmin.Source)}"
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
