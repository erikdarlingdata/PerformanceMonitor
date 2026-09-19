/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Advice for connection saturation (filled by lane 3 of #3542, design §3.6): <c>peak / (max_connections −
/// superuser_reserved_connections)</c> stated with all three numbers and the state breakdown of the peak capture,
/// so the operator sees WHICH population fills the pool; <c>PG_MONITORING_PERMISSIONS</c> says what the monitoring
/// login could not see and why no ratio was built on it.
///
/// <para><b>Value-stated, never folklore.</b> The composed block reads the fact the scorer graded —
/// <c>peak_total_sessions</c>, <c>usable_connections</c>, <c>max_connections</c>,
/// <c>superuser_reserved_connections</c>, the active / idle-in-transaction / other split, <c>captures_with_rows</c>,
/// the newest capture — and says those numbers. Where lane 2's <c>CONFIG_PG_WORK_MEM</c> context fact is in the
/// set, the memory cost of raising <c>max_connections</c> is stated in this server's own <c>work_mem</c>, never an
/// assumed one (D4 composition without RAM). The static block (an empty fact set — the read-time fallback for a
/// finding persisted without frozen story text) says what the family concludes without claiming a figure it
/// does not have.</para>
///
/// <para><b>The cliff is stated as an outage, and both levers carry their counter-objective</b> (OtterTune
/// doctrine): a pooler (pgbouncer, RDS Proxy) trades session-level state — transaction pooling does not preserve
/// prepared statements, <c>SET</c>, advisory locks or <c>LISTEN</c> across statements — and adds a hop; raising
/// <c>max_connections</c> costs memory headroom (<c>work_mem</c> is per sort or hash node PER backend, so the
/// ceiling multiplies it) and takes a restart (<c>pg_settings.context = postmaster</c>). Neither is chosen for
/// the reader: the breakdown says which one the evidence points at — parked idle-in-transaction sessions are an
/// application scoping fault a bigger pool would only defer. No DDL is written here (D8).</para>
/// </summary>
public static partial class PgTargetAdvice
{
    private static readonly AdviceBlock s_saturationStatic = new(
        Headline: "Connections peaked near the usable ceiling — PostgreSQL refuses the next one, it does not queue it",
        Investigation:
            "The window's peak session count from pg_session_states, measured against the usable ceiling PostgreSQL " +
            "itself enforces: max_connections minus superuser_reserved_connections, the slots an ordinary role can " +
            "take. Past that line a new connection is refused outright (FATAL: too many clients already / remaining " +
            "connection slots are reserved) — there is no queue, so for the application that asked this is an " +
            "outage, not a slowdown. The ceiling is the engine's own; the 80% / 90% bands on the ratio sit far above " +
            "anything the measured fleet reached (fleet maximum 10.3% of ceiling over 7 days; threshold_lineage = 1). The count includes " +
            "PostgreSQL's own background processes, which hold no connection slot, so the ratio reads a few points " +
            "high — the safe direction for a cliff. The session collector stores a capture only when some session " +
            "had a transaction open past its floor, so the peak is over the captures that had something to report.",
        Remediation:
            "Read the state breakdown of the peak capture first — active, idle in transaction, other — because it " +
            "says which lever applies. If idle-in-transaction sessions hold the slots, the pool is being filled by " +
            "parked connections and the fix is transaction scoping in the application (and idle_in_transaction_" +
            "session_timeout as a backstop); a bigger pool only defers the refusal. If active sessions hold them, " +
            "the two levers are a connection pooler between the applications and the server (pgbouncer in " +
            "transaction mode, or RDS Proxy on AWS), which trades session-level state — prepared statements, SET, " +
            "advisory locks, LISTEN — and adds a hop; or raising max_connections, which costs work_mem × connections " +
            "of memory headroom on the same host and takes a restart. get_pg_session_states shows the sessions " +
            "behind the peak; get_pg_server_config shows the ceiling and whether a change is already pending restart.");

    private static readonly AdviceBlock s_permissionsStatic = new(
        Headline: "The monitoring login cannot see session state on this server",
        Investigation:
            "Most of the pg_session_states rows stored in the window came back state_is_redacted. PostgreSQL does " +
            "not refuse pg_stat_activity to a role without pg_read_all_stats — it returns every other role's " +
            "backend with state, wait_event, xact_start, query_start, backend_type and the query text blanked, " +
            "leaving pid, database, user, application_name and the xid/xmin columns. Measured on a live instance: " +
            "the privileged role saw four idle-in-transaction sessions, the unprivileged one saw zero of the same " +
            "nine backends. So the session totals cannot be read at face value, and this pass emits no " +
            "connection-saturation ratio for the server rather than one built on blank rows — the peak session " +
            "COUNT is stated (a bare count survives redaction); its share of the ceiling is not.",
        Remediation:
            "Grant the monitoring login pg_monitor (or the narrower pg_read_all_stats) on the target — on RDS and " +
            "Aurora the master user can grant it. The counter-objective is visibility, not storage: pg_read_all_stats " +
            "lets the login SEE other roles' query text, which pg_session_states deliberately never stores, so the " +
            "grant widens what the monitoring connection could read, not what this product keeps. Until it is " +
            "granted, the saturation, idle-in-transaction and xmin-holder findings for this server are blind, and " +
            "get_pg_session_states shows the redacted rows as they are.");

    /// <summary>
    /// The composed block for a saturation or permissions root, or the family's static block when the fact set
    /// does not carry the key (the <see cref="Static"/> path, and a story whose facts were not passed).
    /// </summary>
    private static partial AdviceBlock? ComposeSessions(string key, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        switch (key)
        {
            case PgTargetFactKeys.ConnectionSaturation:
                return factsByKey.TryGetValue(key, out var saturation)
                    ? ComposeSaturation(saturation, factsByKey)
                    : s_saturationStatic;

            case PgTargetFactKeys.MonitoringPermissions:
                return factsByKey.TryGetValue(key, out var permissions)
                    ? ComposePermissions(permissions)
                    : s_permissionsStatic;

            default:
                return null;
        }
    }

    private static AdviceBlock ComposeSaturation(Fact fact, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        var ratio = fact.Metadata.GetValueOrDefault("saturation_ratio");
        var peak = fact.Metadata.GetValueOrDefault("peak_total_sessions");
        var active = fact.Metadata.GetValueOrDefault("peak_active_sessions");
        var idleInTransaction = fact.Metadata.GetValueOrDefault("peak_idle_in_transaction_sessions");
        var other = fact.Metadata.GetValueOrDefault("peak_other_sessions");
        var maxConnections = fact.Metadata.GetValueOrDefault("max_connections");
        var reserved = fact.Metadata.GetValueOrDefault("superuser_reserved_connections");
        /* PostgreSQL 16+'s second carve-out (pg_use_reserved_connections); 0 or absent on every earlier major and on
           the 16+ default, in which case the arithmetic reads exactly as it did before the ceiling learned it. */
        var reservedForRole = fact.Metadata.GetValueOrDefault("reserved_connections");
        var usable = fact.Metadata.GetValueOrDefault("usable_connections");
        var captures = fact.Metadata.GetValueOrDefault("captures_with_rows");
        var latest = fact.Metadata.GetValueOrDefault("latest_total_sessions");
        var pendingRestart = fact.Metadata.GetValueOrDefault("max_connections_pending_restart") > 0;
        var hasPeakAge = fact.Metadata.TryGetValue("peak_age_s", out var peakAgeSeconds);
        var idleShare = fact.Metadata.GetValueOrDefault("peak_idle_in_transaction_share");

        var inv = new StringBuilder();
        inv.Append(CultureInfo.InvariantCulture,
            $"At the window's peak capture{(hasPeakAge ? $", {FormatAge(peakAgeSeconds)} before the window's end" : string.Empty)}, {peak:0} sessions were connected against {usable:0} usable connections — max_connections {maxConnections:0} minus superuser_reserved_connections {reserved:0}{(reservedForRole > 0 ? $" minus reserved_connections {reservedForRole:0}" : string.Empty)} — that is {peak:0} / ({maxConnections:0} − {reserved:0}{(reservedForRole > 0 ? $" − {reservedForRole:0}" : string.Empty)}) = {ratio * 100:0}%.");
        inv.Append(" PostgreSQL refuses the connection that would take a reserved slot unless the role is a superuser (FATAL: too many clients already / remaining connection slots are reserved), and it does not queue: for the application that asked, a refusal is an outage, not a slowdown.");
        inv.Append(CultureInfo.InvariantCulture,
            $" State breakdown at the peak: {active:0} active, {idleInTransaction:0} idle in transaction, {other:0} other (idle client sessions and PostgreSQL's own background processes — checkpointer, walwriter, autovacuum — which this series cannot tell apart and which hold no connection slot, so the ratio reads a few points high, the safe direction for a cliff).");
        if (idleShare >= PgTargetScorer.IdleInTransactionShareBar)
            inv.Append(CultureInfo.InvariantCulture,
                $" Idle-in-transaction sessions were {idleShare * 100:0}% of the peak — the pool is being filled by PARKED connections, not work.");
        inv.Append(CultureInfo.InvariantCulture,
            $" The peak was seen over {captures:0} {(captures == 1 ? "capture" : "captures")} that stored session rows (the collector stores a capture only when some session had a transaction open past its floor, so quiet minutes are absent from this series); the newest capture in the window had {latest:0} sessions.");
        if (pendingRestart)
            inv.Append(" A max_connections change is already pending a restart on this server — the ceiling stated here is the RUNNING value, not the pending one.");
        inv.Append(" The ceiling is the engine's own line; the 80% / 90% bands on the ratio sit far above anything the measured fleet reached (fleet maximum 10.3% of ceiling over 7 days; threshold_lineage = 1).");

        var rem = new StringBuilder();
        if (idleShare >= PgTargetScorer.IdleInTransactionShareBar)
        {
            rem.Append(CultureInfo.InvariantCulture,
                $"{idleInTransaction:0} of the {peak:0} peak sessions were idle in transaction: connections holding a slot and doing nothing. The lever is transaction scoping in the application — commit or roll back before returning a connection to its pool — with idle_in_transaction_session_timeout as the server-side backstop (its counter-objective: a legitimately long client-side pause inside a transaction is rolled back). A bigger pool only defers the same refusal. ");
        }

        rem.Append("Two capacity levers, each with its cost.");
        rem.Append(" (1) A connection pooler between the applications and the server — pgbouncer in transaction mode, or RDS Proxy on AWS — so many application connections share a smaller server-side pool; transaction pooling does not preserve session-level state across statements (prepared statements, SET, advisory locks, LISTEN/NOTIFY, temporary tables), and the pooler is one more hop and one more thing to run.");
        rem.Append(CultureInfo.InvariantCulture,
            $" (2) Raise max_connections above {maxConnections:0}: every slot is a backend that may claim work_mem per sort or hash node");
        if (factsByKey.TryGetValue(PgTargetFactKeys.ConfigWorkMem, out var workMem))
        {
            rem.Append(CultureInfo.InvariantCulture,
                $" — work_mem is {workMem.Value:0.#} MB here, so {usable:0} connections each running one such node could claim {workMem.Value * usable:N0} MB before shared_buffers is counted — ");
        }
        else
        {
            rem.Append(", so the ceiling multiplies work_mem against the host's memory before shared_buffers is counted; ");
        }
        rem.Append("raising it lowers the safe work_mem and cache headroom on the same host, and it takes a restart (pg_settings.context = postmaster).");
        rem.Append(" get_pg_session_states shows the sessions behind the peak by database, user and application; get_pg_server_config shows the ceiling and whether a change is pending restart.");

        return s_saturationStatic with
        {
            Headline = string.Format(CultureInfo.InvariantCulture,
                "Connections peaked at {0:0}% of the usable ceiling ({1:0} of {2:0}) — PostgreSQL refuses the next one, it does not queue it",
                ratio * 100, peak, usable),
            Investigation = inv.ToString(),
            Remediation = rem.ToString(),
        };
    }

    private static AdviceBlock ComposePermissions(Fact fact)
    {
        var share = fact.Metadata.GetValueOrDefault("rows_redacted_share");
        var redacted = fact.Metadata.GetValueOrDefault("rows_redacted");
        var stored = fact.Metadata.GetValueOrDefault("rows_stored");
        var captures = fact.Metadata.GetValueOrDefault("captures_with_rows");
        var peak = fact.Metadata.GetValueOrDefault("peak_total_sessions");

        var inv = new StringBuilder();
        inv.Append(CultureInfo.InvariantCulture,
            $"{redacted:0} of the {stored:0} pg_session_states rows stored across {captures:0} {(captures == 1 ? "capture" : "captures")} in the window ({share * 100:0}%) came back state_is_redacted.");
        inv.Append(" PostgreSQL does not refuse pg_stat_activity to a role without pg_read_all_stats — it returns every other role's backend with state, wait_event, xact_start, query_start, backend_type and the query text blanked, leaving pid, database, user, application_name and the xid/xmin columns; measured on a live instance, the privileged role saw four idle-in-transaction sessions and the unprivileged one zero of the same nine backends.");
        inv.Append(CultureInfo.InvariantCulture,
            $" The peak capture counted {peak:0} sessions — a bare count, which survives redaction — but what state they were in, and whether the captures that stored rows describe the pool or the login's own backends, cannot be read, so this pass emits no connection-saturation ratio for this server rather than one built on blank rows.");

        return s_permissionsStatic with
        {
            Headline = string.Format(CultureInfo.InvariantCulture,
                "The monitoring login cannot see session state on this server — {0:0}% of the stored session rows are redacted",
                share * 100),
            Investigation = inv.ToString(),
        };
    }

    /// <summary>Seconds rendered at the scale a reader thinks in — "in the last minute" under sixty seconds,
    /// minutes under an hour, hours above — the figure places the peak inside the window, not on a clock.</summary>
    private static string FormatAge(double seconds) => seconds switch
    {
        < 60 => "under a minute",
        < 3_600 => string.Format(CultureInfo.InvariantCulture, "{0:0} min", seconds / 60),
        _ => string.Format(CultureInfo.InvariantCulture, "{0:0.#} h", seconds / 3_600),
    };
}
