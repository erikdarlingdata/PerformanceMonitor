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
using System.Text;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Advice for the vacuum family (filled by lane 4 of #3542): dead/threshold ratio and slope per table, the
/// wraparound counter's position against its own setting with the time-to-wall arithmetic and its "not
/// computable" branch, and the xmin holder named per <c>holder_source</c> — because the four causes of a held
/// horizon look identical by symptom and need completely different fixes (<c>pg_xmin_horizon</c>'s own
/// reason for existing).
///
/// <para><b>House rules this file keeps.</b> Every number in the prose is read from the fact — the table's
/// own threshold, the server's own <c>autovacuum_freeze_max_age</c>, the holder's own age — never assumed.
/// Every recommendation names its counter-objective (more autovacuum I/O, a killed session, a dropped slot's
/// consumer). Autovacuum is never to be switched off, and the text says so where an operator might reach for
/// it. The wraparound block states the routine-versus-emergency distinction explicitly: past
/// <c>autovacuum_freeze_max_age</c> the engine is already defending itself and the job is to confirm the
/// defence is winning; past <c>vacuum_failsafe_age</c> or twice the setting it is a manual VACUUM now. No
/// <c>CREATE INDEX</c>, and the per-table <c>ALTER TABLE … SET (…)</c> statements ride in the prose with the
/// table's own figures rather than in <see cref="AdviceBlock.RemediationTsql"/>, which the viewer renders as
/// a copy-paste T-SQL remediation.</para>
/// </summary>
public static partial class PgTargetAdvice
{
    /// <summary>
    /// The <c>pg_xmin_horizon.source</c> vocabulary as numbers, because <see cref="Fact.Metadata"/> is doubles.
    /// The collector encodes with <see cref="HolderSourceCode"/>, the advice decodes with
    /// <see cref="HolderSourceName"/>; the fact's <see cref="Fact.ObjectName"/> carries the text form too.
    /// Order is arbitrary and persisted only in metadata (never in a key or a story path), so it may grow.
    /// </summary>
    private static readonly string[] s_holderSources =
    [
        "unknown",
        "session",
        "replication_slot",
        "replication_slot_catalog",
        "standby_feedback",
        "prepared_transaction",
    ];

    /// <summary>Encodes a <c>pg_xmin_horizon.source</c> value; anything unrecognised is 0 ("unknown").</summary>
    public static int HolderSourceCode(string? source)
    {
        if (string.IsNullOrWhiteSpace(source)) return 0;
        var index = Array.IndexOf(s_holderSources, source.Trim().ToLowerInvariant());
        return index < 0 ? 0 : index;
    }

    /// <summary>Decodes <see cref="HolderSourceCode"/>; an out-of-range code reads as "unknown".</summary>
    public static string HolderSourceName(double code)
    {
        var index = (int)code;
        return index >= 0 && index < s_holderSources.Length ? s_holderSources[index] : s_holderSources[0];
    }

    private static partial AdviceBlock? ComposeVacuum(string key, IReadOnlyDictionary<string, Fact> factsByKey) => key switch
    {
        PgTargetFactKeys.AutovacuumBacklog => ComposeAutovacuumBacklog(factsByKey),
        PgTargetFactKeys.WraparoundTrend => ComposeWraparoundTrend(factsByKey),
        PgTargetFactKeys.XminHold => ComposeXminHold(factsByKey),
        PgTargetFactKeys.ConfigAutovacuumOff => ComposeConfigAutovacuumOff(factsByKey),
        PgTargetFactKeys.ConfigMaintWorkMem => ComposeConfigMaintWorkMem(factsByKey),
        PgTargetFactKeys.ConfigAutovacuumDisabled => ComposeConfigAutovacuumDisabled(factsByKey),
        _ => null,
    };

    /* ── PG_AUTOVACUUM_BACKLOG ── */

    private static readonly AdviceBlock s_backlogStatic = new(
        Headline: "A table has sat past its own autovacuum trigger line for consecutive hourly samples",
        Investigation:
            "PostgreSQL's autovacuum fires on a table when n_dead_tup exceeds autovacuum_vacuum_threshold + " +
            "autovacuum_vacuum_scale_factor × n_live_tup (per-table reloptions honoured), or on PostgreSQL 13+ when " +
            "inserts since the last vacuum exceed the insert threshold. The collector stores each table's own line " +
            "hourly; this finding is a table past it in at least three consecutive samples, ranked by the RATIO to " +
            "its own threshold rather than the raw count, so a small hot table fifty times past its line outranks a " +
            "large one that is merely busy. The slope of dead tuples across the run and the differenced " +
            "autovacuum_count say whether autovacuum is running and losing or never reaching the table.",
        Remediation:
            "Do not turn autovacuum off to quiet it — an unvacuumed table becomes bloat and, unfrozen, a wraparound " +
            "shutdown. If autovacuum ran and lost, give it more work per second on that table (ALTER TABLE … SET " +
            "(autovacuum_vacuum_cost_limit = …, autovacuum_vacuum_cost_delay = …)) at the cost of more I/O while it " +
            "runs; if it never reached the table, the workers are busy elsewhere — raise autovacuum_max_workers " +
            "(restart) or lower the biggest tables' scale factor so the small hot ones get a turn. Check PG_XMIN_HOLD: " +
            "a held horizon makes dead tuples unremovable and no tuning helps until it is released.");

    private static AdviceBlock ComposeAutovacuumBacklog(IReadOnlyDictionary<string, Fact> facts)
    {
        if (!facts.TryGetValue(PgTargetFactKeys.AutovacuumBacklog, out var f))
            return s_backlogStatic;

        var m = f.Metadata;
        var table = string.IsNullOrEmpty(f.ObjectName) ? "A table" : f.ObjectName;
        var db = string.IsNullOrEmpty(f.DatabaseName) ? string.Empty : $" in {f.DatabaseName}";
        var ratio = m.GetValueOrDefault(PgTargetScorer.BacklogRatioKey, f.Value);
        var insertArm = m.GetValueOrDefault(PgTargetScorer.BacklogArmIsInsertKey) >= 1;
        var trailing = m.GetValueOrDefault(PgTargetScorer.BacklogTrailingSamplesKey);
        var hours = m.GetValueOrDefault(PgTargetScorer.BacklogHoursKey);
        var dead = m.GetValueOrDefault(PgTargetScorer.BacklogDeadTuplesKey);
        var threshold = m.GetValueOrDefault(PgTargetScorer.BacklogVacuumThresholdKey);
        var live = m.GetValueOrDefault(PgTargetScorer.BacklogLiveTuplesKey);
        var inserts = m.GetValueOrDefault(PgTargetScorer.BacklogInsertsSinceVacuumKey);
        var insertThreshold = m.GetValueOrDefault(PgTargetScorer.BacklogInsertThresholdKey);
        var disabled = m.GetValueOrDefault(PgTargetScorer.BacklogTableAutovacuumDisabledKey) >= 1;
        var others = Math.Max(0, m.GetValueOrDefault(PgTargetScorer.BacklogTablesKey) - 1);
        var slopeComputable = m.GetValueOrDefault(PgTargetScorer.BacklogSlopeComputableKey) >= 1;
        var slope = m.GetValueOrDefault(PgTargetScorer.BacklogSlopePerHourKey);
        var runsComputable = m.GetValueOrDefault(PgTargetScorer.BacklogRunsComputableKey) >= 1;
        var runs = m.GetValueOrDefault(PgTargetScorer.BacklogAutovacuumRunsKey);
        var xminHeld = facts.TryGetValue(PgTargetFactKeys.XminHold, out var xmin) && xmin.Severity > 0;
        var autovacuumOff = facts.TryGetValue(PgTargetFactKeys.ConfigAutovacuumOff, out var off) && off.BaseSeverity > 0;

        var headline = insertArm
            ? $"{table}{db} has taken {Fmt(inserts)} inserts since its last vacuum, {ratio:0.#}× its insert-vacuum threshold, for {trailing:0} consecutive hourly samples"
            : $"{table}{db} carries {Fmt(dead)} dead tuples, {ratio:0.#}× its own autovacuum trigger line, for {trailing:0} consecutive hourly samples";

        var inv = new StringBuilder();
        if (insertArm)
        {
            inv.Append($"The append-only arm: {Fmt(inserts)} rows inserted since the last vacuum against a per-table insert threshold of {Fmt(insertThreshold)} (autovacuum_vacuum_insert_threshold + autovacuum_vacuum_insert_scale_factor × {Fmt(live)} live tuples). ");
            inv.Append("An append-only table has no dead tuples for the dead-tuple rule to see, and a table that is never vacuumed is never frozen — this is the classic wraparound route, which is why the insert arm exists. ");
        }
        else
        {
            inv.Append($"{Fmt(dead)} dead tuples against this table's own trigger line of {Fmt(threshold)} (autovacuum_vacuum_threshold + autovacuum_vacuum_scale_factor × {Fmt(live)} live tuples, reloptions honoured)");
            if (live > 0 && threshold > 0)
                inv.Append($" — the line sits at {threshold / live:P1} of the table");
            inv.Append(". ");
        }

        inv.Append($"The table has been past its line for {trailing:0} consecutive hourly samples spanning {hours:0.#} h. ");

        if (slopeComputable)
        {
            var verb = slope > 0 ? "rose" : slope < 0 ? "fell" : "held flat";
            inv.Append($"Across that run the {(insertArm ? "insert" : "dead-tuple")} count {verb} at {Fmt(Math.Abs(slope))} per hour (the stored hourly gauge, differenced between samples — a level, not a counter). ");
        }
        else
        {
            inv.Append("The slope across the run is not computable — the run's samples do not span enough time to divide. ");
        }

        if (runsComputable)
        {
            if (runs > 0)
            {
                inv.Append($"autovacuum ran on the table {runs:0} time{(runs == 1 ? string.Empty : "s")} during the run (the cumulative autovacuum_count, differenced — never summed)");
                inv.Append(slope > 0
                    ? ", and the backlog still grew: it is running and LOSING, not merely late. "
                    : ", and did not bring the count under the line. ");
            }
            else
            {
                inv.Append("autovacuum did not run on the table at all during the run (autovacuum_count unchanged) — the launcher never reached it. ");
            }
        }
        else
        {
            inv.Append("Whether autovacuum ran during the run cannot be read: autovacuum_count went backwards across the window, which is a statistics reset, not a run count. ");
        }

        if (m.TryGetValue(PgTargetScorer.BacklogHoursSinceLastAutovacuumKey, out var sinceLast))
            inv.Append($"last_autovacuum was {sinceLast:0.#} h before the window end. ");
        else
            inv.Append("The table has no last_autovacuum stamp — it has never been autovacuumed since its statistics were last reset. ");

        if (disabled)
            inv.Append("autovacuum_enabled is OFF on this table (a reloption), so the engine will never clear it. ");
        if (xminHeld)
            inv.Append("PG_XMIN_HOLD co-fired: the xmin horizon is held back, so dead tuples newer than it are not removable by any vacuum — this backlog cannot clear until the holder is released. ");
        if (autovacuumOff)
            inv.Append("CONFIG_PG_AUTOVACUUM_OFF co-fired: autovacuum is off server-wide, so this backlog is by configuration. ");
        if (others > 0)
            inv.Append($"{others:0} other table{(others == 1 ? " is" : "s are")} persistently past their line too — get_pg_autovacuum lists them all by ratio. ");

        var rem = new StringBuilder();
        rem.Append("Never switch autovacuum off to make this quiet: the backlog becomes bloat, and an unfrozen table becomes a wraparound shutdown. ");
        if (xminHeld)
        {
            rem.Append("Resolve PG_XMIN_HOLD first — while the horizon is pinned, no VACUUM (manual or automatic) can remove these tuples, and any tuning below only burns I/O. ");
        }

        if (autovacuumOff)
        {
            rem.Append("Turn autovacuum back on (autovacuum = on in postgresql.conf, then pg_reload_conf()); until it is, run VACUUM (ANALYZE) on this table by hand. ");
        }
        else if (disabled)
        {
            rem.Append($"Re-enable it for this table — ALTER TABLE {table} SET (autovacuum_enabled = true); — or, if it was disabled for a bulk load still in progress, schedule a manual VACUUM when the load ends. Counter-objective: autovacuum I/O returns to this table. ");
        }
        else if (runsComputable && runs > 0)
        {
            rem.Append($"autovacuum is reaching this table and losing, so let it work unthrottled HERE rather than server-wide: ALTER TABLE {table} SET (autovacuum_vacuum_cost_delay = 0); — 0 is the engine's own 'no throttling' value (the server default is a 2 ms delay per 200 cost units); a per-table reloption applies at the next run without a restart. Counter-objective: more read and write I/O from autovacuum while it runs on this table. ");
            if (!insertArm && live > 0 && threshold > 0)
                rem.Append($"If the line itself is the problem — {Fmt(threshold)} dead rows before a run starts — lower this table's own scale factor so runs start earlier and each does less: ALTER TABLE {table} SET (autovacuum_vacuum_scale_factor = {SuggestedScaleFactor(threshold, live):0.###}); — that halves the trigger line from its current {threshold / live:P1} of the table. Counter-objective: more frequent, smaller vacuums. ");
        }
        else
        {
            rem.Append($"autovacuum is not reaching this table. With {others + 1:0} table{(others == 0 ? string.Empty : "s")} persistently past their line, the worker pool is the likelier limit: autovacuum_max_workers (default 3, restart to change) or the biggest tables' scale factor (ALTER TABLE … SET (autovacuum_vacuum_scale_factor = …)) so long-running vacuums on them do not monopolise the workers. Counter-objective: more concurrent autovacuum I/O. Confirm nothing is blocking a worker (pg_stat_progress_vacuum, pg_locks). ");
        }

        if (insertArm && live > 0 && insertThreshold > 0)
            rem.Append($"For an append-only table the point is freezing: its insert line sits at {insertThreshold / live:P1} of the table; ALTER TABLE {table} SET (autovacuum_vacuum_insert_scale_factor = {SuggestedScaleFactor(insertThreshold, live):0.###}); halves it so autovacuum visits after fewer inserts and freezes the new pages in smaller batches. Counter-objective: a vacuum pass on the table twice as often. ");

        return new AdviceBlock(headline, inv.ToString().TrimEnd(), rem.ToString().TrimEnd());
    }

    /// <summary>Half the table's current effective trigger fraction, floored where the fraction is already
    /// tiny — value-stated from the table's own threshold and live count, never a fixed folklore number.</summary>
    private static double SuggestedScaleFactor(double threshold, double live) =>
        Math.Max(0.001, Math.Round(threshold / live / 2, 3, MidpointRounding.AwayFromZero));

    /* ── PG_WRAPAROUND_TREND ── */

    private static readonly AdviceBlock s_wraparoundStatic = new(
        Headline: "A database's transaction-ID or MultiXact age has reached the engine's own freeze line and is not coming down",
        Investigation:
            "PostgreSQL's 32-bit transaction counter wraps; when a database's oldest unfrozen XID (datfrozenxid) or " +
            "MultiXact ages past autovacuum_freeze_max_age, autovacuum force-starts a wraparound-prevention vacuum " +
            "whether or not the table is otherwise due — reaching that age is ROUTINE (the healthy sawtooth), staying " +
            "at or above it is the finding. Each counter is graded against its OWN setting (the MultiXact default is " +
            "twice the XID one), and the same bars the wraparound alert pages on decide the severity here. The window's " +
            "slope of the age gives a time-to-wall estimate when the age is climbing; when it fell or held, none is stated.",
        Remediation:
            "ROUTINE (at the setting, not recovering): confirm the forced vacuum is running and winning — " +
            "pg_stat_progress_vacuum, and the age falling between collections; find the oldest tables with " +
            "age(relfrozenxid) and check nothing holds the xmin horizon (PG_XMIN_HOLD). EMERGENCY (past twice the " +
            "setting, or past vacuum_failsafe_age at 74.5% of the 2^31 space): run VACUUM (FREEZE) on the oldest " +
            "tables now from a session with vacuum_cost_delay = 0, and keep the database's connections; at the wall " +
            "the server stops accepting writes and the remedy is hours of single-user-mode vacuuming. Never disable " +
            "autovacuum to stop the forced vacuum — it is the only thing standing between the counter and the wall.");

    private static AdviceBlock ComposeWraparoundTrend(IReadOnlyDictionary<string, Fact> facts)
    {
        if (!facts.TryGetValue(PgTargetFactKeys.WraparoundTrend, out var f))
            return s_wraparoundStatic;

        var m = f.Metadata;
        var db = string.IsNullOrEmpty(f.DatabaseName) ? "a database" : f.DatabaseName;
        var multi = m.GetValueOrDefault(PgTargetScorer.WraparoundCounterIsMultiXactKey) >= 1;
        var counter = multi ? "MultiXact" : "XID";
        var settingName = multi ? "autovacuum_multixact_freeze_max_age" : "autovacuum_freeze_max_age";
        var age = multi ? m.GetValueOrDefault(PgTargetScorer.WraparoundMultiXactAgeKey) : m.GetValueOrDefault(PgTargetScorer.WraparoundXidAgeKey);
        var setting = multi ? m.GetValueOrDefault(PgTargetScorer.WraparoundMultiXactFreezeMaxAgeKey) : m.GetValueOrDefault(PgTargetScorer.WraparoundFreezeMaxAgeKey);
        var peak = multi ? m.GetValueOrDefault(PgTargetScorer.WraparoundMultiXactPeakKey) : m.GetValueOrDefault(PgTargetScorer.WraparoundXidPeakKey);
        var keepingUp = (multi ? m.GetValueOrDefault(PgTargetScorer.WraparoundMultiXactKeepingUpKey) : m.GetValueOrDefault(PgTargetScorer.WraparoundXidKeepingUpKey)) >= 1;
        var remaining = multi ? m.GetValueOrDefault(PgTargetScorer.WraparoundMultiXidsRemainingKey) : m.GetValueOrDefault(PgTargetScorer.WraparoundXidsRemainingKey);
        var slope = m.GetValueOrDefault(PgTargetScorer.WraparoundSlopePerHourKey);
        var wallComputable = m.GetValueOrDefault(PgTargetScorer.WraparoundTimeToWallComputableKey) >= 1;
        var hoursToWall = m.GetValueOrDefault(PgTargetScorer.WraparoundHoursToWallKey);
        var arm = (int)m.GetValueOrDefault(PgTargetScorer.WraparoundArmKey);
        var pctOfCeiling = age / PostgresOutagePredictorThresholds.WraparoundCeiling;
        var fractionOfSetting = setting > 0 ? age / setting : 0;
        var xminHeld = facts.TryGetValue(PgTargetFactKeys.XminHold, out var xmin) && xmin.Severity > 0;
        var backlog = facts.TryGetValue(PgTargetFactKeys.AutovacuumBacklog, out var bl) && bl.Severity > 0;
        var emergency = arm is 3 or 4;

        var headline = arm switch
        {
            4 => $"{counter} age {Fmt(age)} in {db} is {pctOfCeiling:P1} of the wraparound space — past vacuum_failsafe_age; EMERGENCY",
            3 => $"{counter} age {Fmt(age)} in {db} is past twice {settingName} ({Fmt(setting)}) — autovacuum's own defence is losing; EMERGENCY",
            2 => $"{counter} age {Fmt(age)} in {db} is {pctOfCeiling:P1} of the wraparound space — far past this cluster's routine freeze point",
            1 => $"{counter} age {Fmt(age)} in {db} has reached {settingName} ({Fmt(setting)}) and has not come back down",
            _ => $"{counter} age {Fmt(age)} in {db} is {fractionOfSetting:P0} of {settingName} — under the engine's freeze line",
        };

        var inv = new StringBuilder();
        inv.Append($"{db}: {counter} age {Fmt(age)}, {fractionOfSetting:P0} of its own {settingName} ({Fmt(setting)}) and {pctOfCeiling:P1} of the 2^31 space; {Fmt(remaining)} {(multi ? "MultiXacts" : "transactions")} remain before the wall. ");
        inv.Append(keepingUp
            ? $"The counter has come DOWN from its window peak of {Fmt(peak)} — autovacuum's freeze cycle is winning (the routine sawtooth). "
            : $"The latest reading IS the window peak ({Fmt(peak)}) — the counter has never been lower inside the window, so the forced anti-wraparound vacuum is not (yet) winning. ");
        inv.Append(wallComputable
            ? $"At the window's slope of {Fmt(slope)} per hour the wall is roughly {FmtHours(hoursToWall)} away; the estimate is a straight line through the window, and a workload change moves it. "
            : slope <= 0
                ? "Time-to-wall is not computable: the age fell or held across the window, so a straight line never reaches it. "
                : "Time-to-wall is not computable from this window. ");
        inv.Append("The other counter is graded against its own setting separately and rides in the fact's metadata; the two are never collapsed. ");
        if (xminHeld)
            inv.Append("PG_XMIN_HOLD co-fired: a held horizon stops freezing outright — VACUUM can freeze only tuples older than the horizon, so relfrozenxid cannot advance past the holder. ");
        if (backlog)
            inv.Append("PG_AUTOVACUUM_BACKLOG co-fired: the forced freeze vacuum is queuing behind tables autovacuum is already failing to clear. ");

        var rem = new StringBuilder();
        if (emergency)
        {
            rem.Append($"EMERGENCY, not routine. Find the oldest tables in {db} (SELECT relname, age(relfrozenxid) FROM pg_class WHERE relkind IN ('r','m','t') ORDER BY 2 DESC) and run VACUUM (FREEZE, VERBOSE) on them now from a session with vacuum_cost_delay = 0 — do not wait for autovacuum, whose per-run cost limits are why it fell behind. ");
            if (arm == 4)
                rem.Append("The engine is already past vacuum_failsafe_age and vacuuming without cost limits or index cleanup; that is the last automatic defence. ");
            rem.Append("If a manual VACUUM cannot advance the age, something holds the horizon — an idle transaction, a replication slot, a prepared transaction (PG_XMIN_HOLD names it) — and that must be released first. Counter-objective: a freeze vacuum reads every page of the table and is I/O-heavy while it runs; at the wall the alternative is a write-refusing server and hours in single-user mode. ");
        }
        else if (arm is 1 or 2)
        {
            rem.Append($"ROUTINE crossing that has not resolved. autovacuum force-started a wraparound-prevention vacuum when the age reached {settingName}; confirm it is running (pg_stat_progress_vacuum) and that the age falls at the next collections. If it is running and the age still climbs, the oldest tables are large enough that the forced vacuum cannot outrun the workload — raise autovacuum_vacuum_cost_limit (reload) or run VACUUM (FREEZE) on them by hand off-peak. Counter-objective: more autovacuum I/O. Do NOT raise {settingName} to make the crossing go away: it only moves the line closer to the wall. ");
        }
        else
        {
            rem.Append($"Nothing to do: the age is under {settingName} and the freeze cycle is behaving. This fact is context for the vacuum chain. ");
        }

        rem.Append("Never disable autovacuum to stop the forced vacuum — it is the only thing between the counter and the wall.");
        return new AdviceBlock(headline, inv.ToString().TrimEnd(), rem.ToString().TrimEnd());
    }

    /* ── PG_XMIN_HOLD ── */

    private static readonly AdviceBlock s_xminStatic = new(
        Headline: "One holder has pinned the xmin horizon for a majority of the window's observations",
        Investigation:
            "VACUUM can remove only tuples older than the cluster's oldest active snapshot (the xmin horizon). " +
            "Four unrelated causes hold it back and look identical by symptom — dead tuples accumulate, autovacuum " +
            "reports success, nothing shrinks — so the collector attributes each capture to its source: a session " +
            "(idle in transaction or a long query), a replication slot's xmin or catalog_xmin, a standby's " +
            "hot_standby_feedback, or a prepared transaction. This finding uses the same age bar and majority-of-" +
            "observations persistence the xmin alert pages on, so a holder seen once (a query that ran long) is context, " +
            "not a finding.",
        Remediation:
            "The fix differs completely by holder kind: end an idle-in-transaction session (pg_terminate_backend) and " +
            "find why the application left it open; drop an inactive replication slot whose consumer is gone " +
            "(pg_drop_replication_slot) or fix the consumer; disable hot_standby_feedback or end the long replica query; " +
            "COMMIT PREPARED or ROLLBACK PREPARED an orphaned prepared transaction. Each has a counter-objective — a " +
            "killed transaction, a re-synced replica, a CDC pipeline that must resnapshot — which is why the holder is named.");

    private static AdviceBlock ComposeXminHold(IReadOnlyDictionary<string, Fact> facts)
    {
        if (!facts.TryGetValue(PgTargetFactKeys.XminHold, out var f))
            return s_xminStatic;

        var m = f.Metadata;
        var holder = string.IsNullOrEmpty(f.ObjectName) ? "an unidentified holder" : f.ObjectName;
        var source = HolderSourceName(m.GetValueOrDefault(PgTargetScorer.XminHolderSourceKey));
        var age = m.GetValueOrDefault(PgTargetScorer.XminAgeKey, f.Value);
        var total = m.GetValueOrDefault(PgTargetScorer.XminObservationsTotalKey);
        var held = m.GetValueOrDefault(PgTargetScorer.XminObservationsHeldKey);
        var above = m.GetValueOrDefault(PgTargetScorer.XminObservationsAboveThresholdKey);
        var identityArm = m.GetValueOrDefault(PgTargetScorer.XminIdentityArmKey) >= 1;
        var freezeMaxAge = m.GetValueOrDefault(PgTargetScorer.XminFreezeMaxAgeKey);
        var sinceLast = m.GetValueOrDefault(PgTargetScorer.XminMinutesSinceLastHolderKey);
        var backlog = facts.TryGetValue(PgTargetFactKeys.AutovacuumBacklog, out var bl) && bl.Severity > 0;
        var wraparound = facts.TryGetValue(PgTargetFactKeys.WraparoundTrend, out var wa) && wa.Severity > 0;

        var kind = source switch
        {
            "session" => "a session",
            "replication_slot" => "a replication slot's xmin",
            "replication_slot_catalog" => "a logical slot's catalog_xmin",
            "standby_feedback" => "a standby's hot_standby_feedback",
            "prepared_transaction" => "a prepared transaction",
            _ => "an unrecognised holder kind",
        };

        var headline = identityArm
            ? $"{holder} ({kind}) is holding the xmin horizon {Fmt(age)} transactions back, in {held:0} of {total:0} holder-bearing collections"
            : $"{holder} ({kind}) held the xmin horizon {Fmt(age)} transactions back — a transient holder, not a chronic one";

        var inv = new StringBuilder();
        inv.Append($"Latest winning holder: {holder}, {kind}, horizon held {Fmt(age)} transactions back (the shared warning bar is {Fmt(PostgresOutagePredictorThresholds.XminAgeWarningThreshold)}), last seen {sinceLast:0} min before the window end. ");
        inv.Append($"Persistence: this holder won {held:0} of the {total:0} collections in the window that recorded ANY holder ({(total > 0 ? held / total : 0):P0}; the alert's majority standard is {PostgresOutagePredictorThresholds.XminPersistenceFraction:P0} over at least {PostgresOutagePredictorThresholds.XminMinimumObservations} observations). ");
        inv.Append(identityArm
            ? "That is the chronic-holder shape the xmin alert pages on; the alert and this finding grade on the same bars. "
            : "That does not meet the chronic-holder standard — a query that ran long and finished looks like this — so it is context, not a finding, exactly as the alert would decline to page it. ");
        if (above > 0)
            inv.Append($"The horizon sat at or above the bar in {above:0} collections regardless of who held it; a parade of DIFFERENT holders (the alert's horizon arm) is not graded here in v1 because its honest denominator is the collector's own capture log, which this read does not consume. ");
        if (freezeMaxAge > 0)
            inv.Append($"Held past this server's autovacuum_freeze_max_age ({Fmt(freezeMaxAge)}) the hold would also stop the anti-wraparound vacuum advancing relfrozenxid; the severity ramps toward critical as the held age approaches it. ");
        if (backlog)
            inv.Append("PG_AUTOVACUUM_BACKLOG co-fired: tables are past their trigger line and staying there — the damage this hold is doing, since dead tuples newer than the horizon cannot be removed. ");
        if (wraparound)
            inv.Append("PG_WRAPAROUND_TREND co-fired: the freeze age is already climbing past the engine's line. ");

        var rem = source switch
        {
            "session" =>
                $"A backend is holding a snapshot — idle in transaction, or a query running for hours. Identify it (pg_stat_activity WHERE backend_xmin IS NOT NULL ORDER BY age(backend_xmin) DESC; the holder text names the pid) and end it with pg_terminate_backend(pid) if it is idle in transaction; then find why the application left the transaction open (a missing COMMIT on an error path, a pooler in transaction mode holding a dead client). Set idle_in_transaction_session_timeout so the next one cannot pin the cluster for hours. Counter-objective: the terminated transaction's work is rolled back.",
            "replication_slot" =>
                $"A replication slot's xmin is pinning the horizon — its consumer has stopped confirming. If the consumer is gone for good, drop the slot (SELECT pg_drop_replication_slot('…'); the holder text names it); if it is merely behind, restart or fix the consumer so it advances. Counter-objective: a dropped slot's consumer must re-initialise from a fresh base copy.",
            "replication_slot_catalog" =>
                $"A logical slot's catalog_xmin is pinning the horizon — the logical decoding consumer (CDC, logical replication subscriber) is not confirming. Check its pipeline; if it is abandoned, drop the slot. Counter-objective: an abandoned CDC pipeline that is later revived must resnapshot.",
            "standby_feedback" =>
                $"A standby with hot_standby_feedback = on is exporting its longest-running query's snapshot to the primary. End the replica query, or set hot_standby_feedback = off on that standby and accept query cancellations there instead (max_standby_streaming_delay). Counter-objective: replica queries may be cancelled by conflicting cleanup.",
            "prepared_transaction" =>
                $"An orphaned prepared transaction (pg_prepared_xacts) is pinning the horizon; its XA coordinator never finished it. COMMIT PREPARED or ROLLBACK PREPARED it by gid — the right choice depends on what the coordinator believes happened. Counter-objective: whichever way it resolves is irreversible.",
            _ =>
                "The holder kind is not one the collector recognises: read pg_stat_activity (backend_xmin), pg_replication_slots (xmin, catalog_xmin) and pg_prepared_xacts directly to find what is pinning the horizon. Counter-objective: none until the holder is identified — every release has a cost only its kind can name.",
        };

        if (!identityArm)
            rem = "No action on this evidence alone — a transient holder is normal; if it recurs: " + rem;

        return new AdviceBlock(headline, inv.ToString().TrimEnd(), rem);
    }

    /* ── CONFIG_PG_AUTOVACUUM_OFF / CONFIG_PG_MAINT_WORK_MEM (collected and base-scored by the config lane;
          the vacuum family owns their advice and their backlog co-fire) ── */

    private static readonly AdviceBlock s_autovacuumOffStatic = new(
        Headline: "autovacuum is OFF server-wide",
        Investigation:
            "autovacuum = off in pg_settings disables the launcher entirely — no table is vacuumed or analysed " +
            "unless someone runs VACUUM by hand, and only the wraparound-prevention vacuum still fires (the engine " +
            "refuses to let that one be disabled). A convention check on its own; when PG_AUTOVACUUM_BACKLOG " +
            "co-fires the consequence is measured.",
        Remediation:
            "Turn it back on: autovacuum = on in postgresql.conf and SELECT pg_reload_conf(); (no restart). If it was " +
            "switched off because a run hurt a workload, the answer is per-table cost settings or a lower scale factor " +
            "on that table, never the global switch. Counter-objective: autovacuum's I/O returns.");

    private static AdviceBlock ComposeConfigAutovacuumOff(IReadOnlyDictionary<string, Fact> facts)
    {
        if (!facts.TryGetValue(PgTargetFactKeys.ConfigAutovacuumOff, out _))
            return s_autovacuumOffStatic;

        if (facts.TryGetValue(PgTargetFactKeys.AutovacuumBacklog, out var bl) && bl.Severity > 0)
        {
            var table = string.IsNullOrEmpty(bl.ObjectName) ? "a table" : bl.ObjectName;
            var ratio = bl.Metadata.GetValueOrDefault(PgTargetScorer.BacklogRatioKey, bl.Value);
            return s_autovacuumOffStatic with
            {
                Headline = $"autovacuum is OFF server-wide, and {table} is {ratio:0.#}× past its own trigger line because of it",
                Investigation = s_autovacuumOffStatic.Investigation +
                    $" Here it has: {table} has sat past the line autovacuum would have fired at for {bl.Metadata.GetValueOrDefault(PgTargetScorer.BacklogTrailingSamplesKey):0} consecutive hourly samples (PG_AUTOVACUUM_BACKLOG).",
            };
        }

        return s_autovacuumOffStatic;
    }

    private static readonly AdviceBlock s_maintWorkMemStatic = new(
        Headline: "maintenance_work_mem bounds how much of a table each autovacuum pass can clean",
        Investigation:
            "Each autovacuum worker collects dead-tuple identifiers into memory bounded by autovacuum_work_mem " +
            "(which defaults to maintenance_work_mem); when the space fills mid-table the run stops to scan every " +
            "index, then resumes — on PostgreSQL 16 and earlier that is six bytes per dead tuple, capped at 1 GB, so a " +
            "table with tens of millions of dead tuples costs several index passes per vacuum. PostgreSQL 17 replaced " +
            "the array with a radix-tree TID store and the limit stops binding in practice. This is evidence-gated: it " +
            "means something only beside PG_AUTOVACUUM_BACKLOG.",
        Remediation:
            "Raise autovacuum_work_mem (reload, not restart) rather than maintenance_work_mem, so manual index builds " +
            "are not affected; size it to the largest backlog's dead-tuple count × 6 bytes on PG ≤ 16. Counter-objective: " +
            "every autovacuum worker may take that much memory at once.");

    private static AdviceBlock ComposeConfigMaintWorkMem(IReadOnlyDictionary<string, Fact> facts)
    {
        if (!facts.TryGetValue(PgTargetFactKeys.ConfigMaintWorkMem, out _))
            return s_maintWorkMemStatic;

        if (facts.TryGetValue(PgTargetFactKeys.AutovacuumBacklog, out var bl) && bl.Severity > 0)
        {
            var table = string.IsNullOrEmpty(bl.ObjectName) ? "the backlogged table" : bl.ObjectName;
            var dead = bl.Metadata.GetValueOrDefault(PgTargetScorer.BacklogDeadTuplesKey);
            var bytesNeeded = dead * 6;
            return s_maintWorkMemStatic with
            {
                Headline = $"maintenance_work_mem is being tested by {table}'s {Fmt(dead)} dead tuples",
                Investigation = s_maintWorkMemStatic.Investigation +
                    $" Here: {table} carries {Fmt(dead)} dead tuples (PG_AUTOVACUUM_BACKLOG), which on PostgreSQL 16 and earlier needs about {FmtBytes(bytesNeeded)} of autovacuum_work_mem to collect in one index pass.",
            };
        }

        return s_maintWorkMemStatic;
    }

    /* ── CONFIG_PG_AUTOVACUUM_DISABLED (#3691 step 22, design §3.1): a table that turned autovacuum off and fell
          behind. Collected and scored in this family (source pg_vacuum — the reloption is per table, read from
          pg_autovacuum_stats); the card is the NAMED CAUSE beside the backlog fact, which carries the grade. ── */

    private static readonly AdviceBlock s_autovacuumDisabledStatic = new(
        Headline: "A table with autovacuum_enabled = off has sat past its own autovacuum trigger line",
        Investigation:
            "ALTER TABLE … SET (autovacuum_enabled = off) tells the launcher to skip one table; PostgreSQL still " +
            "computes that table's trigger line (autovacuum_vacuum_threshold + autovacuum_vacuum_scale_factor × " +
            "n_live_tup, or the insert line on 13+) and the collector stores it hourly, so this finding is a table " +
            "the engine WOULD have vacuumed, in at least three consecutive samples, if it had been allowed to. A " +
            "disabled table with no backlog is not reported — someone may be vacuuming it by hand on a schedule — so " +
            "the fact exists only when the reloption has a measured consequence. The one exception the engine " +
            "keeps: the wraparound-prevention vacuum ignores the reloption and will still run at " +
            "autovacuum_freeze_max_age.",
        Remediation:
            "Re-enable it — ALTER TABLE … SET (autovacuum_enabled = true); takes effect at the launcher's next look, " +
            "no restart — at the cost of the vacuum I/O whoever disabled it was avoiding; if a run hurt the workload, " +
            "a per-table cost delay or a lower scale factor is the lever, not the switch. Or keep it off and " +
            "schedule VACUUM (ANALYZE) on the table yourself, accepting that a manual vacuum reads every page and " +
            "holds SHARE UPDATE EXCLUSIVE while it runs (reads and writes continue; DDL and other vacuums wait). " +
            "Never VACUUM FULL as the first lever: it takes ACCESS EXCLUSIVE and rewrites the table.");

    private static AdviceBlock ComposeConfigAutovacuumDisabled(IReadOnlyDictionary<string, Fact> facts)
    {
        if (!facts.TryGetValue(PgTargetFactKeys.ConfigAutovacuumDisabled, out var f))
            return s_autovacuumDisabledStatic;

        var m = f.Metadata;
        var table = string.IsNullOrEmpty(f.ObjectName) ? "A table" : f.ObjectName;
        var db = string.IsNullOrEmpty(f.DatabaseName) ? string.Empty : $" in {f.DatabaseName}";
        var ratio = m.GetValueOrDefault(PgTargetScorer.BacklogRatioKey, f.Value);
        var insertArm = m.GetValueOrDefault(PgTargetScorer.BacklogArmIsInsertKey) >= 1;
        var hours = m.GetValueOrDefault(PgTargetScorer.BacklogHoursKey);
        var trailing = m.GetValueOrDefault(PgTargetScorer.BacklogTrailingSamplesKey);
        var dead = m.GetValueOrDefault(PgTargetScorer.BacklogDeadTuplesKey);
        var threshold = m.GetValueOrDefault(PgTargetScorer.BacklogVacuumThresholdKey);
        var live = m.GetValueOrDefault(PgTargetScorer.BacklogLiveTuplesKey);
        var inserts = m.GetValueOrDefault(PgTargetScorer.BacklogInsertsSinceVacuumKey);
        var insertThreshold = m.GetValueOrDefault(PgTargetScorer.BacklogInsertThresholdKey);
        var totalBytes = m.GetValueOrDefault(PgTargetScorer.BacklogTotalBytesKey);
        var others = Math.Max(0, m.GetValueOrDefault(PgTargetScorer.AutovacuumDisabledTablesKey) - 1);
        var serverOff = m.GetValueOrDefault(PgTargetScorer.AutovacuumDisabledServerOffKey) >= 1
            || (facts.TryGetValue(PgTargetFactKeys.ConfigAutovacuumOff, out var off) && off.BaseSeverity > 0);
        var hasLastAutovacuum = m.TryGetValue(PgTargetScorer.BacklogHoursSinceLastAutovacuumKey, out var sinceLast);

        var headline = $"{table}{db} has autovacuum_enabled = off and sits at {ratio:0.#}× its own {(insertArm ? "insert-vacuum" : "autovacuum trigger")} line for {FmtHours(hours)}";

        var inv = new StringBuilder();
        inv.Append(insertArm
            ? $"The reloption is off on {table}, and the table has taken {Fmt(inserts)} inserts since its last vacuum against its own insert line of {Fmt(insertThreshold)} (autovacuum_vacuum_insert_threshold + autovacuum_vacuum_insert_scale_factor × {Fmt(live)} live tuples) — {ratio:0.#}× the line, for {trailing:0} consecutive hourly samples spanning {FmtHours(hours)}. "
            : $"The reloption is off on {table}, and the table carries {Fmt(dead)} dead tuples against its own trigger line of {Fmt(threshold)} (autovacuum_vacuum_threshold + autovacuum_vacuum_scale_factor × {Fmt(live)} live tuples, reloptions honoured) — {ratio:0.#}× the line, for {trailing:0} consecutive hourly samples spanning {FmtHours(hours)}. ");
        inv.Append("The engine computed that line and would have fired; the reloption is the only reason it did not. ");
        if (hasLastAutovacuum)
            inv.Append($"autovacuum last ran on this table {FmtHours(sinceLast)} before the window end. ");
        else
            inv.Append("autovacuum has never run on this table as far as the statistics go (last_autovacuum is null). ");
        if (totalBytes > 0)
            inv.Append($"The table is {FmtBytes(totalBytes)} on disk. ");
        if (serverOff)
            inv.Append("CONFIG_PG_AUTOVACUUM_OFF also fired: the launcher is off server-wide, so the per-table reloption is moot until autovacuum = on — the server setting subsumes this card. ");
        if (others > 0)
        {
            inv.Append($"{others:0} more disabled table{(others == 1 ? " has" : "s have")} met the same gate");
            var shapes = new List<string>();
            for (var rank = 2; rank <= 3; rank++)
            {
                if (m.TryGetValue(PgTargetScorer.AutovacuumDisabledRankRatioKey(rank), out var r))
                    shapes.Add($"{r:0.#}× for {FmtHours(m.GetValueOrDefault(PgTargetScorer.AutovacuumDisabledRankHoursKey(rank)))}");
            }
            if (shapes.Count > 0)
                inv.Append($" ({string.Join(", ", shapes)})");
            inv.Append(" — get_pg_autovacuum_health names them, disabled tables first. ");
        }
        if (facts.TryGetValue(PgTargetFactKeys.AutovacuumBacklog, out var bl) && bl.Severity > 0)
            inv.Append("PG_AUTOVACUUM_BACKLOG carries the grade (ratio, slope, tables in backlog); this card names the cause. ");

        var rem = new StringBuilder();
        if (serverOff)
            rem.Append("Turn autovacuum back on server-wide first (autovacuum = on in postgresql.conf, then pg_reload_conf()); the per-table reloption below only matters once the launcher runs. ");
        rem.Append($"Two levers, each with its cost. (1) Re-enable autovacuum on this table — ALTER TABLE {table} SET (autovacuum_enabled = true); — no restart; the launcher picks it up at its next autovacuum_naptime pass and will vacuum it at once because it is already past its line. Counter-objective: the autovacuum I/O whoever disabled it was avoiding returns to this table; if a run was hurting the workload, pair it with ALTER TABLE {table} SET (autovacuum_vacuum_cost_delay = 2); (the server default; the reloption exists so ONE table can be throttled without touching the rest) rather than leaving it off. ");
        rem.Append($"(2) Keep it off and schedule the vacuum yourself — VACUUM (ANALYZE) {table}; off-peak, then on a cadence that beats the {(insertArm ? "insert" : "dead-tuple")} rate. Counter-objective: a manual VACUUM reads every page of the table{(totalBytes > 0 ? $" ({FmtBytes(totalBytes)})" : string.Empty)} and holds SHARE UPDATE EXCLUSIVE while it runs — reads and writes continue, DDL and other vacuums wait — and a schedule that slips becomes this finding again. ");
        rem.Append("Never VACUUM FULL as the first lever: it takes ACCESS EXCLUSIVE, rewrites the whole table and blocks every reader and writer for the duration; it is for reclaiming space AFTER the backlog is under control, if ever. ");
        if (!insertArm && live > 0 && threshold > 0)
            rem.Append($"If the table was disabled because its runs were too big, re-enable it with a lower line so each run is smaller: ALTER TABLE {table} SET (autovacuum_vacuum_scale_factor = {SuggestedScaleFactor(threshold, live):0.###}); halves the trigger from its current {threshold / live:P1} of the table. Counter-objective: more frequent, smaller vacuums. ");

        return new AdviceBlock(headline, inv.ToString().TrimEnd(), rem.ToString().TrimEnd());
    }

    /* ── formatting ── */

    private static string Fmt(double value) => value.ToString("N0", CultureInfo.InvariantCulture);

    private static string FmtHours(double hours) => hours switch
    {
        < 1 => $"{hours * 60:0} minutes",
        < 48 => $"{hours:0.#} hours",
        _ => $"{hours / 24:0.#} days",
    };

    private static string FmtBytes(double bytes) => bytes switch
    {
        >= 1024d * 1024 * 1024 => $"{bytes / (1024d * 1024 * 1024):0.#} GB",
        >= 1024d * 1024 => $"{bytes / (1024d * 1024):0.#} MB",
        _ => $"{bytes / 1024d:0.#} kB",
    };
}
