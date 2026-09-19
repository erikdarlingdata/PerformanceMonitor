/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// <c>pg_sessions</c> — connection saturation (filled by lane 3 of #3542, design §3.6):
/// <c>peak_sessions / (max_connections − superuser_reserved_connections)</c>, the one place PostgreSQL is HARSHER
/// than SQL Server. SQL Server queues a worker request when the pool is exhausted (THREADPOOL — a wait); PostgreSQL
/// refuses the connection outright, <c>FATAL: too many clients already</c> / <c>remaining connection slots are
/// reserved for roles with the SUPERUSER attribute</c>, with no queue behind the refusal. A connection refused is an
/// outage for the application that asked, not a slowdown, so the ratio's top band is a full 1.0.
///
/// <para><b>Where the ratio comes from.</b> The collector composes it (<c>PgTargetFactCollector.Sessions.cs</c>):
/// the window's PEAK <c>total_sessions</c> from <c>pg_session_states</c>' denormalised totals over the ceiling read
/// off lane 2's <c>CONFIG_PG_MAX_CONNECTIONS</c> / <c>CONFIG_PG_SUPERUSER_RESERVED</c> context facts, which sit in
/// the in-memory fact list by the time the session family runs (emission order: Config before Sessions). This arm
/// receives the composed fact and grades <c>saturation_ratio</c>; it never re-reads the config snapshot and never
/// sees a fact set — the base-severity seam is one fact, by design.</para>
///
/// <para><b>Lineage.</b> The CEILING is engine-defined: <c>max_connections</c> is the line PostgreSQL refuses at,
/// and <c>superuser_reserved_connections</c> is the engine's own carve-out of slots an ordinary role can never
/// take — there is no judgment in the denominator. The two BANDS on the ratio (0.8 → 0.5, 0.9 → 1.0) are
/// UNMEASURED: no fleet distribution of peak-sessions-over-ceiling has been read yet, so every saturation fact
/// carries <c>threshold_lineage = 0</c>. The calibrating read is the per-server distribution of
/// <c>max(total_sessions)</c> per window over <c>pg_session_states</c> against that server's usable ceiling
/// (p50 / p95 across the dogfood PostgreSQL fleet).</para>
///
/// <para><b>Self-gating below the warning line, THREADPOOL's shape.</b> <see cref="FactScorer.ApplyThresholdFormula"/>
/// grades ANY positive value below the concerning bar as a fraction of it, so a pool at 40% of its ceiling would
/// score 0.25 — "fired" to every sibling predicate that asks <c>BaseSeverity &gt; 0</c> (lane 5's Lock-wait
/// confirmer, lane 9's session-spike fold) on every server that has ever accepted a connection. Saturation scores
/// 0 below the warning band and 0.5 AT it, so a fired saturation fact always means the pool is genuinely near
/// its cliff; the honest amplifier predicate is then presence, not a second bar.</para>
///
/// <para><b><c>PG_MONITORING_PERMISSIONS</c></b> is the family's OTHER fact, emitted INSTEAD of a ratio when the
/// monitoring login could not see session state (the redacted share of the stored rows reached the majority
/// line). It is not a threshold finding but a measured blindness — the collector saw rows and most of them were
/// blank — and it roots a WARNING-band card at the story line so it surfaces on the quietest server; it has no
/// amplifiers and cannot climb.</para>
/// </summary>
public static partial class PgTargetScorer
{
    /// <summary>
    /// The saturation ratio (peak sessions over usable connections) at which the pool is CONCERNING (0.5 — the
    /// story threshold) and at which it is CRITICAL (1.0). Eight tenths of the usable slots taken at the window's
    /// peak leaves one burst of headroom; nine tenths leaves none a deploy or a retry storm would not consume.
    /// unmeasured: chosen, not measured — calibrate against pg_session_states before the next release (the
    /// per-server peak-sessions / ceiling distribution named in the class summary); the fact carries
    /// threshold_lineage = 0.
    /// </summary>
    public const double ConnectionSaturationWarning = 0.8;
    public const double ConnectionSaturationCritical = 0.9;

    /// <summary>
    /// The share of the stored <c>pg_session_states</c> rows that came back <c>state_is_redacted</c> at or above
    /// which the window's session picture is the monitoring login's blindness rather than the server's state, and
    /// the collector emits <see cref="PgTargetFactKeys.MonitoringPermissions"/> in place of a saturation ratio.
    /// One half is the definitional majority the key's own doc names ("share ≥ ½"), not a tuned bar: redaction
    /// is per-backend and ownership-based (the login sees its own backends whole and every other role's blank),
    /// so on a target without <c>pg_read_all_stats</c> the share sits at or near 1.0 and on one with it at 0 — the
    /// line between them is not what decides, and there is nothing to calibrate.
    /// </summary>
    public const double RedactedShareMajority = 0.5;

    /// <summary>
    /// The base severity of <see cref="PgTargetFactKeys.MonitoringPermissions"/>: the story line
    /// (<c>InferenceEngine.MinimumSeverityThreshold</c>, 0.5, by reference in spirit — the constant is private).
    /// A hair BELOW it would be the D5 advisory shape (0.4), which roots a card only for the keys in
    /// <see cref="PgTargetFactKeys.ConfigAdvisoryRoots"/>; this key is not a config convention check and is not
    /// in that list, and a fact that never rooted a card would be the silence the V86 design comment says this
    /// finding exists to replace. At the line it roots a WARNING-band card and, with no amplifiers, never pages.
    /// </summary>
    public const double MonitoringPermissionsBase = 0.5;

    /// <summary>The boost a co-firing sibling adds to a saturation fact — see <see cref="SessionsAmplifiers"/>.
    /// unmeasured: chosen, not measured — calibrate against analysis_findings co-fire rates before the next
    /// release; the fact carries threshold_lineage = 0.</summary>
    public const double ConnectionSaturationCoFireBoost = 0.25;

    /// <summary>
    /// The share of the PEAK capture's sessions that were <c>idle in transaction</c> at or above which the pool is
    /// being filled by PARKED connections rather than work — design §3.6's named amplifier ("parked connections
    /// consuming slots"), read off the fact's own state breakdown because the <c>PG_IDLE_IN_TRANSACTION</c> fact
    /// that would co-fire is v2. unmeasured: chosen, not measured — calibrate against pg_session_states before
    /// the next release (the per-capture idle_in_transaction_sessions / total_sessions distribution); the fact
    /// carries threshold_lineage = 0.
    /// </summary>
    public const double IdleInTransactionShareBar = 0.25;

    /// <summary>
    /// Layer-1 base severity for the <c>pg_sessions</c> source: the saturation ratio graded between the two bands
    /// above and ZERO below the warning line; the permissions advisory at its fixed base. Anything else under the
    /// source (a future session fact without an arm) scores 0. Stamps <c>threshold_lineage = 0</c> on every
    /// saturation fact it sees, including the ones it grades to 0 — the number that decided is unmeasured either
    /// way — and never on the permissions fact, whose only line is definitional.
    /// </summary>
    private static partial double ScoreSessionsFact(Fact fact)
    {
        switch (fact.Key)
        {
            case PgTargetFactKeys.ConnectionSaturation:
            {
                if (!fact.Metadata.TryGetValue("saturation_ratio", out var ratio) || ratio <= 0)
                    return 0.0;

                /* unmeasured: both bands are the constants declared above, chosen, not measured — calibrate
                   against pg_session_states before the next release; the fact carries threshold_lineage = 0.
                   Below the warning band the fact is context (0), never a fraction of the bar — see the class
                   summary on self-gating. */
                fact.Metadata["threshold_lineage"] = 0;
                if (ratio < ConnectionSaturationWarning)
                    return 0.0;

                return FactScorer.ApplyThresholdFormula(ratio, ConnectionSaturationWarning, ConnectionSaturationCritical);
            }

            case PgTargetFactKeys.MonitoringPermissions:
                /* The collector emits this only past RedactedShareMajority; the guard here is against a fact
                   built elsewhere with no share at all — a blank claim scores nothing. */
                return fact.Metadata.TryGetValue("rows_redacted_share", out var share) && share >= RedactedShareMajority
                    ? MonitoringPermissionsBase
                    : 0.0;

            default:
                return 0.0;
        }
    }

    /// <summary>
    /// Layer-2 amplifiers for <see cref="PgTargetFactKeys.ConnectionSaturation"/>. Each asks whether a sibling
    /// FIRED (its own scorer put its base above zero) or reads the saturation fact's own breakdown — never a bar
    /// of another family's.
    /// <list type="bullet">
    /// <item><description><b>Parked connections</b>: the peak capture's idle-in-transaction share reached
    /// <see cref="IdleInTransactionShareBar"/>. The slots are held by sessions doing nothing, so the lever is
    /// transaction scoping in the application, not capacity. Under partial redaction the idle-in-transaction count
    /// UNDER-reports (state is a privileged column), so this can only fail to fire, never fire falsely.</description></item>
    /// <item><description><b><see cref="PgTargetFactKeys.CpuPercent"/> fired</b> (Aurora / Performance Insights
    /// only — absent on stock, so inert there): the instance is CPU-bound while the pool is near its ceiling —
    /// arrivals are stacking up on a saturated CPU, the D7 "queueing at the cliff" shape, and a pooler will not
    /// buy back the CPU.</description></item>
    /// </list>
    /// <para><b>v2 hooks, deliberately commented rather than written against an inert predicate:</b> (a) the
    /// offered-vs-delivered co-fire the design names — sessions climbing across the window while <c>PG_TPS</c>
    /// is flat or falling — needs lane 2's database family to carry a TPS trend, which it does not yet; (b) the
    /// <c>PG_IDLE_IN_TRANSACTION</c> co-fire, once that fact exists, supersedes the self-metadata parked-connections
    /// arm above with a duration-qualified one. Neither is a bar this lane may choose today.</para>
    /// </summary>
    private static partial List<AmplifierDefinition> SessionsAmplifiers(string key) =>
    [
        new()
        {
            Description = "Parked connections fill the pool: idle-in-transaction sessions were a quarter or more of the peak capture",
            /* unmeasured: ConnectionSaturationCoFireBoost, chosen, not measured — see its declaration. */
            Boost = ConnectionSaturationCoFireBoost,
            Predicate = facts =>
                facts.TryGetValue(key, out var self)
                && self.Metadata.GetValueOrDefault("peak_idle_in_transaction_share") >= IdleInTransactionShareBar,
        },
        new()
        {
            Description = "Instance CPU is elevated while the connection pool is near its ceiling — arrivals are queueing on CPU, not just on slots",
            /* unmeasured: ConnectionSaturationCoFireBoost, chosen, not measured — see its declaration. */
            Boost = ConnectionSaturationCoFireBoost,
            Predicate = facts =>
                facts.TryGetValue(PgTargetFactKeys.CpuPercent, out var cpu) && cpu.BaseSeverity > 0,
        },
        /* v2 — offered vs delivered (design §3.6, D7 verbatim): sessions climbing while PG_TPS is flat or falling.
           Needs a trend on the PG_TPS fact (first-half vs second-half rate) from lane 2's database family:
        new()
        {
            Description = "Sessions climbed across the window while transactions per second did not — queueing at the cliff",
            Boost = ConnectionSaturationCoFireBoost,
            Predicate = facts =>
                facts.TryGetValue(key, out var self) && self.Metadata.GetValueOrDefault("peak_is_late") > 0
                && facts.TryGetValue(PgTargetFactKeys.Tps, out var tps) && tps.Metadata.GetValueOrDefault("trend") <= 0,
        },
           v2 — PG_IDLE_IN_TRANSACTION fired: parked connections with a measured duration and horizon claim,
           replacing the self-metadata share arm above:
        new()
        {
            Description = "Long idle-in-transaction sessions are holding the slots the pool is short of",
            Boost = ConnectionSaturationCoFireBoost,
            Predicate = facts =>
                facts.TryGetValue(PgTargetFactKeys.IdleInTransaction, out var parked) && parked.BaseSeverity > 0,
        }, */
    ];
}
