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
/// <c>pg_sessions</c> — connection saturation (filled by lane 3 of #3542, design §3.6):
/// <c>peak_sessions / (max_connections − superuser_reserved_connections)</c>, the one place PostgreSQL is HARSHER
/// than SQL Server. SQL Server queues a worker request when the pool is exhausted (THREADPOOL — a wait); PostgreSQL
/// refuses the connection outright, <c>FATAL: too many clients already</c> / <c>remaining connection slots are
/// reserved for roles with the SUPERUSER attribute</c>, with no queue behind the refusal. A connection refused is an
/// outage for the application that asked, not a slowdown, so the ratio's top band is a full 1.0.
///
/// <para><b>Where the ratio comes from.</b> The collector composes it (<c>PgTargetFactCollector.Sessions.cs</c>):
/// the window's PEAK backend count over the ceiling read off lane 2's <c>CONFIG_PG_MAX_CONNECTIONS</c> /
/// <c>CONFIG_PG_SUPERUSER_RESERVED</c> context facts, which sit in the in-memory fact list by the time the session
/// family runs (emission order: Config before Sessions). Since lane 25 of #3691 the NUMERATOR is
/// <c>pg_stat_database.numbackends</c> (V133) — <c>SUM</c> over the databases at one <c>collection_time</c>, the
/// window's peak of those instants: a universal one-minute LEVEL of backends attached to a database, sampled
/// whether or not any session tripped a capture rule — and the exception-capture peak (<c>pg_session_states</c>'
/// denormalised <c>total_sessions</c>, v1's only numerator) is the stated FALLBACK for a store whose rows do not
/// carry the column yet. Which one decided rides the fact as <see cref="SaturationNumeratorSourceKey"/>, and
/// BOTH readings stay in the metadata (<c>peak_total_sessions</c> for #3713's compare banding and the state
/// breakdown; <see cref="PeakNumbackendsKey"/> for the level). This arm receives the composed fact and grades
/// <c>saturation_ratio</c> the same way from either numerator; it never re-reads the config snapshot and never
/// sees a fact set — the base-severity seam is one fact, by design.</para>
///
/// <para><b>Lineage.</b> The CEILING is engine-defined: <c>max_connections</c> is the line PostgreSQL refuses at,
/// and <c>superuser_reserved_connections</c> is the engine's own carve-out of slots an ordinary role can never
/// take — there is no judgment in the denominator. The two BANDS on the ratio (0.8 → 0.5, 0.9 → 1.0) are
/// fractions of that engine-defined line, and the #3691 fleet calibration (2026-09-19) read where they sit: over
/// 7 days × 50 Aurora PostgreSQL clusters of the dogfood fleet (usable ceilings 1,240 – 5,000, a
/// <c>max_connections</c> snapshot on all 50), sessions-over-ceiling per capture had a per-server p99 median of
/// 0.021 and a fleet MAXIMUM of 0.103 — both bands sit deep in the measured empty interval, which is where a
/// hard-FATAL cliff's bars belong. So every saturation fact carries <c>threshold_lineage = 1</c>: the ceiling is
/// the engine's, the bands are measured against it. Caveat the constants repeat: the numerator that read used was
/// <c>pg_session_states</c>' <c>total_sessions</c> (the exception-capture peak — biased HIGH on a server that
/// captures often and blind on one that never does; the fleet's captures per server ran 59 – 2,100 over 7 days);
/// the fact is now composed from <c>numbackends</c> where the store carries it, whose distribution has not yet
/// been read against the ceiling. The bars are not moved by that switch: 0.8 / 0.9 are fractions of the engine's
/// own line, and a numerator that counts FEWER non-slot backends can only move the measured shape further into
/// the empty interval, never out of it.</para>
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
///
/// <para><b><c>PG_IDLE_IN_TRANSACTION</c></b> (v2 — #3691 lane 14, design §3.10) is the family's THIRD fact: the
/// longest transaction any session held open while <c>idle in transaction</c> in the window, from the same
/// <c>pg_session_states</c> captures, graded on its DURATION alone. A session in that state has finished a
/// statement and not committed or rolled back: it holds its snapshot (so autovacuum cannot remove any tuple
/// deleted since — the xmin horizon), its row and relation locks (so the next writer waits on nothing being
/// done), and its connection slot (so the pool is that much shorter). None of that costs the application
/// anything visible, which is why the shape is chronic — the leaf of three chains (blocking, xmin, saturation)
/// and an application defect in every one of them. The horizon claim escalates the grade one band
/// (<c>horizon_age &gt; 0</c> — the engine's own <c>backend_xmin</c>/<c>backend_xid</c> age; <c>-1</c> is the
/// collector's "pins nothing" sentinel and must never escalate, V86); recurrence of one holder identity across
/// captures is an amplifier, never a bar; a redacted-majority window emits no idle fact and stamps the
/// permissions advisory <c>idle_in_transaction_unobservable = 1</c> in place of a false zero.</para>
/// </summary>
public static partial class PgTargetScorer
{
    /// <summary>Metadata key: the longest <c>xact_duration_ms</c> of any idle-in-transaction row in the window — the
    /// number <see cref="IdleInTransactionWarningMs"/> grades (<see cref="Fact.Value"/> carries the same figure in
    /// seconds for the reader).</summary>
    public const string IdleInTransactionDurationMsKey = "max_xact_duration_ms";

    /// <summary>Metadata key: the longest holder's <c>horizon_age</c> as the V86 collector stored it — the age of
    /// the xid / xmin the session pins, or <c>-1</c> when it pins nothing (a READ COMMITTED reader, an UPDATE that
    /// matched no rows). The escalation asks <c>&gt; 0</c>, so the sentinel never escalates.</summary>
    public const string IdleInTransactionHolderHorizonAgeKey = "holder_horizon_age";

    /// <summary>Metadata key: the most captures in the window any ONE holder identity (<c>application_name</c> +
    /// <c>username</c> + <c>database_name</c>) was seen over the duration floor in — the chronic-holder witness the
    /// recurrence amplifier reads.</summary>
    public const string IdleInTransactionRecurringCapturesKey = "recurring_captures";

    /// <summary>Metadata stamp (0 / 1) the scorer writes: whether the horizon claim lifted the grade one band.</summary>
    public const string IdleInTransactionHorizonEscalatedKey = "horizon_escalated";

    /// <summary>Metadata stamp on <see cref="PgTargetFactKeys.MonitoringPermissions"/>: the idle-in-transaction
    /// read was NOT attempted because the window's stored rows were majority-redacted (state is a privileged
    /// column) — the family's silence on parked transactions is the login's, not the server's.</summary>
    public const string IdleInTransactionUnobservableKey = "idle_in_transaction_unobservable";

    /// <summary>
    /// The idle-in-transaction DURATION — the longest <c>xact_duration_ms</c> any <c>idle in transaction</c> session
    /// in the window had held its transaction open, from <c>pg_session_states</c> — at which
    /// <see cref="PgTargetFactKeys.IdleInTransaction"/> is CONCERNING (60 s → 0.5) and CRITICAL (10 min → 1.0).
    /// Design §3.10's constant floor: one minute is past any statement-to-statement pause an OLTP application
    /// makes on purpose and past the collector's own storage floors (ten seconds idle, thirty open); ten minutes
    /// is a transaction nobody is coming back to. measured: inside the empty interval — zero idle-in-transaction
    /// rows at or over 60 s over 7 days × 50 Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-19
    /// (pg_session_states xact_duration_ms of idle-in-transaction rows: p50 24 ms, p99 50 ms, maximum 27.7 s on
    /// one cluster) — the chronic-holder shape is absent on the measured population, which is exactly what the
    /// bar is for, and an empty interval IS a measurement. Engine-neutral quantity (a duration, not a wait
    /// fraction), so the stock-PostgreSQL population reads the same bar; the fact carries threshold_lineage = 1.
    /// The collector passes the WARNING value as the read's floor (<c>$4</c>), so a fact exists only when a
    /// row crossed it and the scorer never grades a fraction of the bar.
    /// </summary>
    public const double IdleInTransactionWarningMs = 60_000;
    public const double IdleInTransactionCriticalMs = 600_000;

    /// <summary>
    /// One band of <see cref="FactScorer.ApplyThresholdFormula"/>'s own scale (CONCERNING 0.5 → CRITICAL 1.0),
    /// added to the duration grade when the longest holder PINS THE HORIZON (<c>horizon_age &gt; 0</c>) and
    /// clamped at 1.0. engine-defined gate, structural step: whether a session holds back the xmin horizon is
    /// PostgreSQL's own <c>backend_xmin</c> / <c>backend_xid</c> (V86 stores the age; <c>-1</c> pins nothing and
    /// never escalates), and the step is the formula's band width, not a bar anyone chose — so the fact's
    /// threshold_lineage stays 1. Why a band and not an amplifier: a parked transaction that is holding vacuum
    /// back is a worse FACT (dead tuples accumulating server-wide, freezing stalled), not a corroborated one;
    /// amplifiers multiply and cap at 2.0 across the co-fires, while this makes 60 s of pinning read as a
    /// CRITICAL finding on its own.
    /// </summary>
    public const double IdleInTransactionHorizonEscalation = 0.5;

    /// <summary>
    /// The number of captures in the window one holder identity must have been seen over the floor in for the
    /// RECURRENCE amplifier to fire (<c>recurring_captures ≥ 3</c>): the same application, as the same role, in
    /// the same database, parked past a minute in three separate five-minute captures is a code path, not an
    /// incident. unmeasured: chosen, not measured — no holder identity recurred over the floor on the measured
    /// fleet (zero rows at or over 60 s, 2026-09-19), so there was nothing to read a persistence shape from;
    /// calibrate against pg_session_states once a fleet target shows the chronic shape. An amplifier GATE, not a
    /// bar the base is graded on, so the fact's threshold_lineage (1, off the measured duration bars above) does
    /// not move — the same rule <see cref="ConnectionSaturationCoFireBoost"/> rests on.
    /// </summary>
    public const double IdleInTransactionRecurrenceCaptures = 3;

    /// <summary>The boost recurrence adds (×1.2). unmeasured: chosen, not measured — calibrate against
    /// analysis_findings co-fire rates before the next release; not a bar, lineage unaffected.</summary>
    public const double IdleInTransactionRecurrenceBoost = 0.2;

    /// <summary>
    /// The saturation ratio (peak sessions over usable connections) at which the pool is CONCERNING (0.5 — the
    /// story threshold) and at which it is CRITICAL (1.0). Eight tenths of the usable slots taken at the window's
    /// peak leaves one burst of headroom; nine tenths leaves none a deploy or a retry storm would not consume.
    /// engine-defined denominator (max_connections − superuser_reserved_connections); the bands on it are
    /// measured: above the fleet maximum sessions-over-ceiling of 0.103 (per-server p99 median 0.021) over 7 days
    /// × 50 Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-19 (pg_session_states total_sessions per
    /// capture against each server's usable ceiling) — the measured empty interval. Engine-neutral quantity,
    /// Aurora population; the stock-PostgreSQL population is not yet measured. That read's numerator was the
    /// exception-capture peak; since #3691 lane 25 the fact's numerator is pg_stat_database.numbackends (V133)
    /// where the store carries it, and the numbackends distribution against the ceiling is not yet measured —
    /// the capture-peak reading was 10.3 % of ceiling at most, and a level that excludes the database-less
    /// background processes the capture counted sits at or under it on the same servers. The bars do not move
    /// with the numerator. The fact carries threshold_lineage = 1.
    /// </summary>
    public const double ConnectionSaturationWarning = 0.8;
    public const double ConnectionSaturationCritical = 0.9;

    /// <summary>Metadata key on <see cref="PgTargetFactKeys.ConnectionSaturation"/> (#3691 lane 25): WHICH numerator
    /// the collector divided by the ceiling — <see cref="SaturationNumeratorNumbackends"/> (1) for the window's peak
    /// of <c>SUM(numbackends)</c> per <c>collection_time</c> over <c>pg_database_stats</c>, or
    /// <see cref="SaturationNumeratorCapturePeak"/> (0) for the exception-capture peak <c>total_sessions</c> from
    /// <c>pg_session_states</c>, v1's numerator and the fallback for a store whose rows do not carry the V133 column
    /// (or carry it on fewer than <see cref="NumbackendsCoverageFloor"/> of the window's samples — see
    /// <see cref="NumbackendsPartialKey"/>). Stamped on every saturation fact so a reader of <c>get_analysis_facts</c>
    /// knows what the fraction's top was; absent from a fact built before this lane, which is read as 0.</summary>
    public const string SaturationNumeratorSourceKey = "numerator_source";

    /// <summary><see cref="SaturationNumeratorSourceKey"/> value: the numerator is <c>numbackends</c>.</summary>
    public const int SaturationNumeratorNumbackends = 1;

    /// <summary><see cref="SaturationNumeratorSourceKey"/> value: the numerator is the session captures' peak
    /// <c>total_sessions</c> — exception-driven, and biased a few points HIGH by the database-less background
    /// processes it counts.</summary>
    public const int SaturationNumeratorCapturePeak = 0;

    /// <summary>Metadata: the window's peak of <c>SUM(numbackends)</c> over the databases at one <c>collection_time</c>
    /// — the server's backends attached to a database at that instant. Present whenever at least one instant in the
    /// window carried the column, INCLUDING the partial-coverage fallback, so both readings are visible side by side
    /// with <c>peak_total_sessions</c>; absent when no row in the window has it (a pre-V133 store).</summary>
    public const string PeakNumbackendsKey = "peak_numbackends";

    /// <summary>Metadata: how many seconds before the window's end the <see cref="PeakNumbackendsKey"/> instant was
    /// — a different instant from <c>peak_age_s</c>, which stays the CAPTURE peak's age (the breakdown's instant).</summary>
    public const string NumbackendsPeakAgeKey = "numbackends_peak_age_s";

    /// <summary>Metadata: the distinct <c>collection_time</c>s in the window at which at least one database row carried
    /// a non-NULL <c>numbackends</c> (the shared-relations row is always NULL and does not count against an instant).</summary>
    public const string NumbackendsSamplesKey = "numbackends_samples";

    /// <summary>Metadata: the distinct <c>collection_time</c>s the window's <c>pg_database_stats</c> holds at all — the
    /// denominator <see cref="NumbackendsSamplesKey"/> is a coverage of.</summary>
    public const string DatabaseStatsSamplesKey = "database_stats_samples";

    /// <summary>Metadata stamp (0 / 1): 1 when the window had SOME <c>numbackends</c> instants but fewer than
    /// <see cref="NumbackendsCoverageFloor"/> of its samples — a store mid-migration, or a collector restarted onto the
    /// rung part-way through the window — so the collector fell back to the capture peak rather than call the
    /// window's peak off a series that covers less than half of it. 0 otherwise (full coverage, or none at all).</summary>
    public const string NumbackendsPartialKey = "numbackends_partial";

    /// <summary>
    /// The share of the window's <c>pg_database_stats</c> instants that must carry <c>numbackends</c> for it to be the
    /// saturation numerator; under it the collector uses the capture peak and stamps <see cref="NumbackendsPartialKey"/>.
    /// One half is a definitional majority, not a tuned bar (the <see cref="RedactedShareMajority"/> shape): the
    /// column arrives with a rung and from then on every row has it, so a real store reads ~0 or ~1 and the only
    /// windows between are the ones the rung landed inside — and a peak read over the sampled half of such a window
    /// is a peak over half the window, which is the capture-peak lie in a new coat. There is nothing to calibrate;
    /// the coverage rides the fact so a reader can see how close a window came.
    /// </summary>
    public const double NumbackendsCoverageFloor = 0.5;

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
    /// release. A boost is not a bar the base was graded on, so it does not move the fact's threshold_lineage (1, off the
    /// measured bars above).</summary>
    public const double ConnectionSaturationCoFireBoost = 0.25;

    /// <summary>Metadata stamp (0 / 1) the offered-vs-delivered amplifier writes on
    /// <see cref="PgTargetFactKeys.ConnectionSaturation"/>: 1 when <c>ANOMALY_PG_SESSION_SPIKE</c> fired this pass
    /// and <c>ANOMALY_PG_TPS</c> did not — more connections than this hour of the week usually carries, doing no
    /// more work than usual. Stamped on every fired saturation fact (0 otherwise), so <c>get_analysis_facts</c>
    /// shows the verdict either way; absent on a saturation fact the amplifier pass never reached (base 0).</summary>
    public const string OfferedVsDeliveredKey = "offered_vs_delivered";

    /// <summary>Metadata: the session-count anomaly's peak over its bucket's robust centre (median when the bucket
    /// has one, else mean) — "connections rose to N× this hour's norm" in the advice. 0 when the anomaly is absent
    /// or did not fire, or when the bucket's centre is 0 (no ratio to state).</summary>
    public const string SessionSpikeRatioKey = "session_spike_ratio";

    /// <summary>Metadata: the TPS anomaly's peak over its bucket's robust centre, on the same terms as
    /// <see cref="SessionSpikeRatioKey"/>. 0 when no TPS anomaly fired — which is the shape the amplifier fires on.</summary>
    public const string TpsAnomalyRatioKey = "tps_anomaly_ratio";

    /// <summary>
    /// The boost the offered-vs-delivered pair adds to a fired saturation fact (×1.3): the session-count anomaly
    /// fired against this server's own hour-of-week baseline while the transaction-rate anomaly did not. Why the
    /// pair and not a raw trend: design §3.6 / D7 named "sessions climbing while TPS is flat or falling", and lane 3
    /// parked a hook reading the window's first-half-vs-second-half TPS trend for it; the 2026-09-19 calibration
    /// (measurements §A1) read routine 20–50× TPS bursts on every cluster (per-server 5-minute maximum median
    /// 1,710 against a p50 of 31), so an in-window trend would amplify saturation on every batch job's shoulder
    /// and miss real queueing at low absolute TPS. The honest offered-vs-delivered signal is the hour-of-week
    /// pair lane 9 built: sessions anomalous HIGH (the offer rose) while throughput is NOT anomalous high (nothing
    /// more was delivered) — arrivals are queueing at the cliff, not working. Larger than the sibling co-fires'
    /// 0.25 because it is two baselined instruments agreeing, and 1.0 × 1.3 = 1.3 leaves the second co-fire to
    /// carry a CRITICAL pool to the 1.5 notify line — one corroborator is WARNING, two page (#3584's rule).
    /// unmeasured: chosen, not measured — calibrate against analysis_findings co-fire rates before the next
    /// release. A boost is not a bar the base was graded on, so the fact's threshold_lineage (1) does not move —
    /// the rule <see cref="ConnectionSaturationCoFireBoost"/> rests on.
    /// </summary>
    public const double OfferedVsDeliveredBoost = 0.3;

    /// <summary>
    /// The share of the PEAK capture's sessions that were <c>idle in transaction</c> at or above which the pool is
    /// being filled by PARKED connections rather than work — design §3.6's named amplifier ("parked connections
    /// consuming slots"), read off the fact's own state breakdown because the <c>PG_IDLE_IN_TRANSACTION</c> fact
    /// that would co-fire is v2. measured: above the fleet maximum idle-in-transaction share of 0.167 (per-server
    /// maximum median 0.063) over 7 days × 50 Aurora PostgreSQL clusters of the dogfood fleet, 2026-09-19
    /// (pg_session_states, idle_in_transaction_sessions / total_sessions per capture) — a quarter was never
    /// reached, so the amplifier's gate sits in the measured empty interval. Measured on Aurora; the
    /// stock-PostgreSQL population is not yet measured. The boost it applies is a separate, unmeasured constant.
    /// </summary>
    public const double IdleInTransactionShareBar = 0.25;

    /// <summary>
    /// Layer-1 base severity for the <c>pg_sessions</c> source: the saturation ratio graded between the two bands
    /// above and ZERO below the warning line; the idle-in-transaction duration graded between ITS two bands, ZERO
    /// below the floor, lifted one band when the holder pins the horizon; the permissions advisory at its fixed
    /// base. Anything else under the source (a future session fact without an arm) scores 0. Stamps
    /// <c>threshold_lineage = 1</c> on every saturation and idle-in-transaction fact it sees, including the ones
    /// it grades to 0 — the number that decided is the engine's ceiling and bands measured against it, or the
    /// measured duration bars (2026-09-19) either way — and never on the permissions fact, whose only line is
    /// definitional.
    /// </summary>
    private static partial double ScoreSessionsFact(Fact fact)
    {
        switch (fact.Key)
        {
            case PgTargetFactKeys.ConnectionSaturation:
            {
                if (!fact.Metadata.TryGetValue("saturation_ratio", out var ratio) || ratio <= 0)
                    return 0.0;

                /* engine-defined ceiling, measured bands: both are the constants declared above with their
                   2026-09-19 lineage; the fact carries threshold_lineage = 1. Below the warning band the fact is
                   context (0), never a fraction of the bar — see the class summary on self-gating. */
                fact.Metadata["threshold_lineage"] = 1;
                if (ratio < ConnectionSaturationWarning)
                    return 0.0;

                return FactScorer.ApplyThresholdFormula(ratio, ConnectionSaturationWarning, ConnectionSaturationCritical);
            }

            case PgTargetFactKeys.IdleInTransaction:
            {
                if (!fact.Metadata.TryGetValue(IdleInTransactionDurationMsKey, out var heldMs) || heldMs <= 0)
                    return 0.0;

                /* measured duration bars (2026-09-19, the empty interval — see the constants); the fact carries
                   threshold_lineage = 1. Below the warning floor the fact is context (0), never a fraction of the
                   bar — the collector's read floor makes this arm unreachable in practice, and the guard is what
                   makes it true for a fact built elsewhere. */
                fact.Metadata["threshold_lineage"] = 1;
                if (heldMs < IdleInTransactionWarningMs)
                    return 0.0;

                var graded = FactScorer.ApplyThresholdFormula(heldMs, IdleInTransactionWarningMs, IdleInTransactionCriticalMs);

                /* engine-defined gate: horizon_age > 0 is PostgreSQL's own statement that this backend's xmin / xid
                   is holding the horizon back; -1 (pins nothing, V86) and an absent key never escalate. One band
                   (IdleInTransactionHorizonEscalation), clamped at CRITICAL. */
                var horizonAge = fact.Metadata.GetValueOrDefault(IdleInTransactionHolderHorizonAgeKey, -1);
                if (horizonAge > 0)
                {
                    fact.Metadata[IdleInTransactionHorizonEscalatedKey] = 1;
                    return Math.Min(1.0, graded + IdleInTransactionHorizonEscalation);
                }

                fact.Metadata[IdleInTransactionHorizonEscalatedKey] = 0;
                return graded;
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
    /// <item><description><b>Offered vs delivered</b> (v2 — #3691 lane 18, design §3.6 / D7): <c>ANOMALY_PG_SESSION_SPIKE</c>
    /// fired while <c>ANOMALY_PG_TPS</c> did not — more connections than this hour of the week usually carries,
    /// doing no more work than usual. Reads the two ANOMALY facts (the <c>anomaly</c> source is in
    /// <c>FactScorer.ScoreAll</c>'s lookup, the pattern lane 15's checkpoint trigger set), never a raw in-window
    /// trend — see <see cref="OfferedVsDeliveredBoost"/> for why. Both anomalies fired is a load surge (the
    /// anomaly family's own corroboration), not queueing, and the arm stays quiet; a stock target with no
    /// baseline yet has neither anomaly and the arm is inert. This is the ONE amplifier whose predicate writes
    /// metadata (<see cref="OfferedVsDeliveredKey"/> and the two ratios): the verdict needs the fact SET, which
    /// the base-severity seam never sees, and the predicate is the only seam that has both the set and the fact;
    /// the stamp is a pure function of the set, so evaluating it twice writes the same values.</description></item>
    /// </list>
    /// <para><b>The one amplifier on <see cref="PgTargetFactKeys.IdleInTransaction"/></b> (v2, lane 14) is
    /// RECURRENCE: one holder identity seen over the duration floor in <see cref="IdleInTransactionRecurrenceCaptures"/>
    /// or more captures of the window (<see cref="IdleInTransactionRecurringCapturesKey"/>, the fact's own
    /// metadata) — the chronic code path rather than one forgotten session, ×1.2. Persistence is an amplifier and
    /// not a bar on purpose: the duration alone grades, so the fact's lineage is the measured bars' and the
    /// unmeasured capture count can only lift a finding that already exists.</para>
    ///
    /// <para><b>One v2 hook stays deliberately commented rather than written against an inert predicate:</b> the
    /// <c>PG_IDLE_IN_TRANSACTION</c> co-fire is NOT enabled here even though the fact now exists: the
    /// self-metadata parked-connections arm above already fires on the same evidence (the peak capture's
    /// idle-in-transaction share), and a second +0.25 for the same sessions counted twice would be
    /// double-counting, not corroboration. The idle fact reaches the saturation story through the graph edge
    /// (<c>PgTargetRelationshipGraph.Saturation.cs</c>) instead, gated on the same share bar. The OTHER hook lane
    /// 3 parked — the raw <see cref="TpsTrendKey"/> trend — is RETIRED, replaced by the anomaly-pair arm above on
    /// the calibration's evidence; the trend itself is still stamped on <c>PG_TPS</c> by the database family as a
    /// reader's figure, and nothing here reads it.</para>
    /// </summary>
    private static partial List<AmplifierDefinition> SessionsAmplifiers(string key)
    {
        if (key == PgTargetFactKeys.IdleInTransaction)
        {
            return
            [
                new()
                {
                    Description = "The same holder — application, role and database — was parked past the floor in three or more captures of the window: a code path, not one forgotten session",
                    /* unmeasured: IdleInTransactionRecurrenceBoost and the capture gate, chosen, not measured — see the declarations. */
                    Boost = IdleInTransactionRecurrenceBoost,
                    Predicate = facts =>
                        facts.TryGetValue(key, out var self)
                        && self.Metadata.GetValueOrDefault(IdleInTransactionRecurringCapturesKey) >= IdleInTransactionRecurrenceCaptures,
                },
            ];
        }

        return SaturationAmplifiers(key);
    }

    private static List<AmplifierDefinition> SaturationAmplifiers(string key) =>
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
        new()
        {
            Description = "ANOMALY_PG_SESSION_SPIKE fired while ANOMALY_PG_TPS did not — more connections than this hour usually carries, doing no more work than usual: arrivals are queueing at the cliff, not working",
            /* unmeasured: OfferedVsDeliveredBoost, chosen, not measured — see its declaration. */
            Boost = OfferedVsDeliveredBoost,
            Predicate = facts => OfferedVsDeliveredFired(facts, key),
        },
        /* v2 — the raw-trend offered-vs-delivered hook lane 3 parked here (PG_TPS's TpsTrendKey, second half − first
           half of the window's rate, gated on peak_is_late) is RETIRED, not merely still parked: the 2026-09-19
           calibration (§A1) showed the in-window TPS trend is noise on every cluster, and the anomaly-pair arm above
           is its replacement. Do not resurrect it — see OfferedVsDeliveredBoost's declaration for the argument.
           PG_IDLE_IN_TRANSACTION fired (the fact exists since lane 14 of #3691): a duration-qualified parked
           claim. Left parked because the self-metadata share arm above already counts the same sessions —
           enabling this would be the same evidence boosted twice (see the summary); the story-level join is the
           graph edge, gated on IdleInTransactionShareBar. If the share arm is ever retired, this replaces it:
        new()
        {
            Description = "Long idle-in-transaction sessions are holding the slots the pool is short of",
            Boost = ConnectionSaturationCoFireBoost,
            Predicate = facts =>
                facts.TryGetValue(PgTargetFactKeys.IdleInTransaction, out var parked) && parked.BaseSeverity > 0,
        }, */
    ];

    /// <summary>
    /// The offered-vs-delivered verdict for the saturation fact under <paramref name="key"/>, stamped on it and
    /// returned: <see cref="PgTargetFactKeys.AnomalySessionSpike"/> FIRED (its own scorer put its base above zero —
    /// the same "fired" every sibling predicate asks) while <see cref="PgTargetFactKeys.AnomalyTps"/> did not fire,
    /// or fired with its peak at or under its bucket's centre. The second clause is defensive, not a second bar:
    /// <c>AnomalyGate</c> fires only on <c>peak − centre ≥ threshold × dispersion</c>, so a fired TPS anomaly
    /// always sits above its centre and the clause is vacuous today; it is written so a future two-sided TPS
    /// detector (a throughput DROP against the baseline, which is also "delivered less") reads as offered-vs-
    /// delivered rather than silencing the arm. Writes <see cref="OfferedVsDeliveredKey"/> (0 / 1),
    /// <see cref="SessionSpikeRatioKey"/> and <see cref="TpsAnomalyRatioKey"/> every time it runs, so a fired
    /// saturation fact always shows the verdict and the two figures the advice states; a set without the saturation
    /// fact stamps nothing and returns false.
    /// </summary>
    public static bool OfferedVsDeliveredFired(IReadOnlyDictionary<string, Fact> facts, string key)
    {
        ArgumentNullException.ThrowIfNull(facts);
        if (!facts.TryGetValue(key, out var self)) return false;

        var sessionRatio = facts.TryGetValue(PgTargetFactKeys.AnomalySessionSpike, out var spike) && spike.BaseSeverity > 0
            ? AnomalyPeakOverCentre(spike)
            : 0.0;
        var tpsFired = facts.TryGetValue(PgTargetFactKeys.AnomalyTps, out var tps) && tps.BaseSeverity > 0;
        var tpsRatio = tpsFired ? AnomalyPeakOverCentre(tps!) : 0.0;

        /* Sessions HIGH: the anomaly fired (presence is the gate; the ratio is the reader's figure and may be 0 on
           a zero-centred bucket). Throughput NOT high: no TPS anomaly, or one whose computable ratio is at or under
           1.0 — a fired anomaly with no computable ratio (centre 0) is high by construction (peak > 0 = centre). */
        var sessionsHigh = spike is not null && spike.BaseSeverity > 0;
        var throughputHigh = tpsFired && !(tpsRatio > 0 && tpsRatio <= 1.0);
        var fired = sessionsHigh && !throughputHigh;

        self.Metadata[SessionSpikeRatioKey] = sessionRatio;
        self.Metadata[TpsAnomalyRatioKey] = tpsRatio;
        self.Metadata[OfferedVsDeliveredKey] = fired ? 1 : 0;
        return fired;
    }

    /// <summary>
    /// A z-score anomaly's peak (<see cref="Fact.Value"/>) over its bucket's robust centre — <c>baseline_median</c>
    /// when the bucket carries one, else <c>baseline_mean</c>: the centre the deviation prose names, so the advice's
    /// "N× this hour's norm" and its "σ above the median/mean" are one number (lane 15's <c>baseline_ratio</c>
    /// convention). 0 when the centre is 0 or negative — there is no multiple of nothing to state, and a metadata
    /// double must stay finite (the finding is persisted as JSON).
    /// </summary>
    public static double AnomalyPeakOverCentre(Fact anomaly)
    {
        ArgumentNullException.ThrowIfNull(anomaly);
        var median = anomaly.Metadata.GetValueOrDefault("baseline_median");
        var centre = median > 0 ? median : anomaly.Metadata.GetValueOrDefault("baseline_mean");
        return centre > 0 ? anomaly.Value / centre : 0.0;
    }
}
