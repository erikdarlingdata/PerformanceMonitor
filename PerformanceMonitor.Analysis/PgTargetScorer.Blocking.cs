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
/// <c>pg_blocking</c> — the blocking / active-query family (filled by lane 17 of #3691, design §2a): <c>PG_BLOCKING_CHAIN</c>
/// from the window's sampled <c>pg_blocking_edges</c> reconstructed into chains with the root attributed (the
/// <c>BlockingChainReconstructor</c> port), <c>PG_LOCK_WAIT_EVENTS</c> from the engine-written <c>lock_wait</c> family
/// of <c>pg_log_events</c> (event grain — what the sample between two captures cannot see), and
/// <c>PG_LONG_RUNNING_QUERY</c> from <c>pg_session_states</c>' active rows. Bars carry their lineage marker within six
/// lines; unmeasured ones carry <c>threshold_lineage = 0</c>; gates are rates or fractions of OBSERVED time, never
/// absolute totals. The chain fact's grade must state that its source is a SAMPLE (a chain shorter than the capture
/// interval was never seen) — the collector's own caveat, carried into the finding.
///
/// <para><b>What grades, and what does not.</b> Every duration graded here is the BLOCKED (or running) session's OWN
/// clock as PostgreSQL reported it at the capture — <c>blocked_query_duration_ms</c> on the edge row, <c>after N ms</c>
/// on the engine's own log line, <c>query_duration_ms</c> on the session row — never "captures seen × cadence". A
/// blocked session sampled in three consecutive one-minute captures was blocked for at least what its own duration
/// says, and the sample cannot say more; multiplying sightings by the cadence would invent a duration from the
/// collector's schedule. Persistence of the same head across captures is therefore an AMPLIFIER, never a bar.</para>
///
/// <para><b>One vocabulary for the two blocking facts.</b> The sampled chain and the written event are two readings
/// of one contention, so they are graded on the same two duration bars (<see cref="BlockingWaitWarningMs"/> /
/// <see cref="BlockingWaitCriticalMs"/>): the chain on the longest blocked statement it sampled, the events on the
/// longest wait the engine wrote. A long runner is a different question (a statement that runs, not one that waits)
/// and has its own pair. The one engine-defined line in the family is <c>deadlock_timeout</c> — the engine's own "a
/// lock wait this long is worth a log line" — carried on the facts as metadata for the advice, not used as a bar
/// (it is a logging threshold, one second by default, and grading blocking at one second would page on every
/// routine lock handoff).</para>
///
/// <para><b>Lineage.</b> The 2026-09-19 fleet calibration did not read <c>pg_blocking_edges</c> or <c>pg_log_events</c>
/// (their approximate row counts read 0 — the unanalysed-table trap — so no distribution was taken) and the only
/// blocking-adjacent number it produced is a WAIT FRACTION (<c>lock</c> ms/ms, fleet p99 0.034, maximum 2.0), which is
/// lane 5's bar, not a duration. Every bar in this file is UNMEASURED and says so; every fact carries
/// <c>threshold_lineage = 0</c>; the calibrating reads are named on each constant.</para>
/// </summary>
public static partial class PgTargetScorer
{
    /* filled by lane 17 of #3691 — the family's bars, keys, base grade and amplifiers; the marker stays, as v1's did. */

    /* ── Metadata keys the collector writes and the scorer / advice / graph read, by constant. ── */

    /// <summary>Metadata key: the longest <c>blocked_query_duration_ms</c> of any blocked edge in the window — the
    /// number the chain fact is GRADED on (<see cref="Fact.Value"/> carries the same figure in seconds).</summary>
    public const string BlockingMaxBlockedQueryMsKey = "max_blocked_query_ms";
    /// <summary>Metadata key: the longest <c>blocked_xact_duration_ms</c> of any blocked edge in the window — stated,
    /// not graded (a transaction can be old for reasons other than this wait).</summary>
    public const string BlockingMaxBlockedXactMsKey = "max_blocked_xact_ms";
    /// <summary>Metadata key: the captures in the window the NAMED head (the one <see cref="Fact.ObjectName"/> carries)
    /// was the root of.</summary>
    public const string BlockingHeadCapturesKey = "head_captures";
    /// <summary>Metadata key: the most captures in the window any ONE head blocker (pid + application + role) was the
    /// root of — the persistence witness the recurrence amplifier reads (the chronic head need not be the most-blocked
    /// one, the sessions family's recurrence rule).</summary>
    public const string BlockingRecurringHeadCapturesKey = "recurring_head_captures";
    /// <summary>Metadata key: 1 when the named head was <c>idle in transaction</c> at its worst sighting — the
    /// application-defect root the graph routes to <c>PG_IDLE_IN_TRANSACTION</c>; 0 when it was running a statement.</summary>
    public const string BlockingHeadIsIdleInTransactionKey = "head_is_idle_in_transaction";
    /// <summary>Metadata key: the named head's backend pid (the join to <c>PG_LONG_RUNNING_QUERY</c>'s
    /// <see cref="LongRunningQueryPidKey"/> — same pid in the same window is the same backend).</summary>
    public const string BlockingHeadPidKey = "head_pid";
    /// <summary>Metadata key: the total sessions the named head had behind it, summed over the captures it headed
    /// (the "most-blocked head" ranking key).</summary>
    public const string BlockingHeadBlockedSessionsKey = "head_blocked_sessions";
    /// <summary>Metadata key: the deepest chain (edges from head to the furthest waiter) any capture recorded.</summary>
    public const string BlockingLongestChainDepthKey = "longest_chain_depth";
    /// <summary>Metadata key: the most distinct blocked sessions any one capture recorded.</summary>
    public const string BlockingPeakBlockedSessionsKey = "peak_blocked_sessions";
    /// <summary>Metadata key: captures in the window that recorded at least one edge.</summary>
    public const string BlockingCapturesWithBlockingKey = "captures_with_blocking";
    /// <summary>Metadata key: SUCCESS captures of the <c>pg_blocking</c> collector in the window (from
    /// <c>collection_log</c>) — the sample's honest denominator; 0 when the log has none.</summary>
    public const string BlockingCapturesTotalKey = "captures_total";
    /// <summary>Metadata key: captures whose edge set contained a component with no head — a cycle in flight, which
    /// the deadlock detector resolves within <c>deadlock_timeout</c>; counted, never attributed to a head.</summary>
    public const string BlockingCycleCapturesKey = "cycle_captures";
    /// <summary>Metadata key: 1 when the named head's stored statement text was truncated by the collector.</summary>
    public const string BlockingHeadQueryTruncatedKey = "head_query_truncated";
    /// <summary>Metadata key: <c>deadlock_timeout</c> in milliseconds from the latest <c>pg_server_config</c> snapshot
    /// at or before the window's end; the engine default (1000) when no snapshot names it, and
    /// <see cref="BlockingDeadlockTimeoutFromSnapshotKey"/> says which.</summary>
    public const string BlockingDeadlockTimeoutMsKey = "deadlock_timeout_ms";
    /// <summary>Metadata key: 1 when <see cref="BlockingDeadlockTimeoutMsKey"/> came from a snapshot, 0 when it is the
    /// engine default assumed.</summary>
    public const string BlockingDeadlockTimeoutFromSnapshotKey = "deadlock_timeout_from_snapshot";

    /// <summary>Metadata key: the count of <c>still waiting</c> lines — one per wait that outlived
    /// <c>deadlock_timeout</c> — in the window's <c>lock_wait</c> events.</summary>
    public const string LockWaitEventsCountKey = "wait_events";
    /// <summary>Metadata key: the longest <c>after N ms</c> any <c>lock_wait</c> line in the window carried — the
    /// engine's own end-to-end length of a wait (the <c>acquired … after N ms</c> line), GRADED on the chain's bars.</summary>
    public const string LockWaitEventsMaxMsKey = "max_event_ms";
    /// <summary>Metadata key: the sum of <c>after N ms</c> over the <c>acquired</c> lines — waited time the engine
    /// wrote down, stated beside observed time; never a gate.</summary>
    public const string LockWaitEventsAcquiredMsKey = "acquired_wait_ms";
    /// <summary>Metadata key: <c>detected deadlock</c> lines in the window's <c>lock_wait</c> family.</summary>
    public const string LockWaitEventsDeadlocksKey = "deadlocks_detected";
    /// <summary>Metadata key: <c>still waiting</c> lines per OBSERVED hour (<c>context.ObservedDurationMs</c>).</summary>
    public const string LockWaitEventsPerHourKey = "wait_events_per_hour";
    /// <summary>Metadata key: 1 when the family cannot know (see the two reason flags); the fact then makes no claim.</summary>
    public const string LockWaitUnavailableKey = "unavailable";
    /// <summary>Reason flag: the latest <c>pg_server_config</c> snapshot says <c>log_lock_waits = off</c> — the engine
    /// writes nothing, so "no events" is not "no waits".</summary>
    public const string LockWaitReasonLogLockWaitsOffKey = "reason_log_lock_waits_off";
    /// <summary>Reason flag: no <c>pg_server_config</c> snapshot names <c>log_lock_waits</c> AND no event arrived, so
    /// whether the engine was writing cannot be told apart from whether anything waited.</summary>
    public const string LockWaitReasonSettingUnknownKey = "reason_log_lock_waits_unknown";
    /// <summary>Reason flag: <c>log_lock_waits</c> is on but the <c>pg_log_events</c> collector recorded no SUCCESS run
    /// in the window (<c>collection_log</c>) — the log was not being read, so its silence is the collector's.</summary>
    public const string LockWaitReasonLogCollectorSilentKey = "reason_log_collector_silent";
    /// <summary>1 when <c>log_lock_waits</c> is on, the collector ran, and no <c>lock_wait</c> line arrived — a measured
    /// zero, which is a fact and not an absence.</summary>
    public const string LockWaitNoEventsKey = "no_events";
    /// <summary>Metadata key: 1 when <c>log_lock_waits</c> read <c>on</c> in the snapshot, 0 when <c>off</c>, absent when unknown.</summary>
    public const string LockWaitLogLockWaitsOnKey = "log_lock_waits_on";

    /// <summary>Metadata key: the longest <c>query_duration_ms</c> of any qualifying active client row in the window —
    /// the number <c>PG_LONG_RUNNING_QUERY</c> is graded on (<see cref="Fact.Value"/> is the same in seconds).</summary>
    public const string LongRunningQueryMaxMsKey = "max_query_ms";
    /// <summary>Metadata key: the longest runner's backend pid (see <see cref="BlockingHeadPidKey"/>).</summary>
    public const string LongRunningQueryPidKey = "runner_pid";
    /// <summary>Metadata key: the longest runner's <c>query_id</c> (PostgreSQL's normalised fingerprint; 0 when the
    /// row carried none — pre-14, <c>compute_query_id</c> off, or redacted).</summary>
    public const string LongRunningQueryIdKey = "runner_query_id";
    /// <summary>Metadata key: the most captures any ONE runner (pid + query_id) was seen over the floor in — the
    /// persistence witness.</summary>
    public const string LongRunningQueryCapturesKey = "runner_captures";
    /// <summary>Metadata key: 1 when the longest runner's latest sighting had a wait event (<c>wait_event_type</c>
    /// other than <c>Lock</c> — Lock waits are the chain's and are excluded from this fact by the read).</summary>
    public const string LongRunningQueryWaitingKey = "runner_waiting";

    /* ── Bars. Every one unmeasured; every fact stamps threshold_lineage = 0. ── */

    /// <summary>
    /// The longest BLOCKED statement duration (<c>blocked_query_duration_ms</c> on a sampled edge; <c>after N ms</c>
    /// on an engine-written lock-wait line) at which the chain / event fact is CONCERNING (30 s → 0.5) and CRITICAL
    /// (5 min → 1.0). unmeasured: chosen, not measured — the 2026-09-19 calibration took no distribution over
    /// pg_blocking_edges or pg_log_events (approximate row counts read 0, the unanalysed-table trap) and its one
    /// lock number is a wait FRACTION (lane 5's bar), so calibrate against pg_blocking_edges.blocked_query_duration_ms
    /// and the lock_wait family's `after N ms` before the next release. Thirty seconds is past any statement
    /// timeout an OLTP application sets on purpose and thirty deadlock_timeouts at the engine default; five minutes
    /// is a waiter nobody is coming back for. Engine-neutral quantity (a duration). The fact carries threshold_lineage = 0.
    /// </summary>
    public const double BlockingWaitWarningMs = 30_000;
    public const double BlockingWaitCriticalMs = 300_000;

    /// <summary>
    /// The number of captures in the window one head blocker (pid + application + role) must have been the root of
    /// for the PERSISTENCE amplifier to fire (<c>head_captures ≥ 3</c>): the same backend heading a chain in three
    /// one-minute samples is a holder, not a handoff. unmeasured: chosen, not measured — no head persistence shape
    /// was read on 2026-09-19; calibrate against pg_blocking_edges before the next release. An amplifier GATE, not a
    /// bar the base is graded on — the duration alone grades, and this can only lift a finding that already exists.
    /// The same gate serves the long runner (one pid + query_id over the floor in three captures).
    /// </summary>
    public const double BlockingPersistenceCaptures = 3;

    /// <summary>The boost persistence and each corroborating sibling add (×1.2). unmeasured: chosen, not measured —
    /// calibrate against analysis_findings co-fire rates before the next release; not a bar, lineage unaffected.</summary>
    public const double BlockingCoFireBoost = 0.2;

    /// <summary>
    /// The longest ACTIVE statement duration (<c>query_duration_ms</c> on a <c>pg_session_states</c> client-backend row
    /// in the <c>active</c> state, not waiting on a Lock) at which <c>PG_LONG_RUNNING_QUERY</c> is CONCERNING (10 min →
    /// 0.5) and CRITICAL (six times that, one hour → 1.0). unmeasured: chosen, not measured — the calibration read
    /// no active-statement duration distribution; calibrate against pg_session_states.query_duration_ms where
    /// state = 'active' before the next release. Ten minutes is past the collector's thirty-second storage floor by
    /// a wide margin and past any interactive statement; an hour is a batch that should have been a job. A statement's
    /// own duration is a per-row quantity, not a total over the window, so it is not the absolute-ms-total gate the
    /// A7 rule forbids — the CRITICAL bar is spelled as a multiple of the WARNING bar so the file states the ratio
    /// rather than an hour-in-milliseconds constant. Engine-neutral quantity. The fact carries threshold_lineage = 0.
    /// The collector passes the WARNING value as the read's floor (<c>$4</c>), so a fact exists only when a row crossed it.
    /// </summary>
    public const double LongRunningQueryWarningMs = 600_000;
    public const double LongRunningQueryCriticalMs = 6 * LongRunningQueryWarningMs;

    /// <summary>The engine default for <c>deadlock_timeout</c> (one second), assumed when no snapshot names it —
    /// engine-defined: PostgreSQL's own shipped value, stated on the fact with the from-snapshot flag at 0.</summary>
    public const double DeadlockTimeoutDefaultMs = 1_000;

    /// <summary>
    /// Layer-1 base severity for the <c>pg_blocking</c> source: the chain on its longest blocked statement, the
    /// events on their longest written wait (one vocabulary — the same two bars), the long runner on its longest
    /// active statement; each ZERO below its warning bar (self-gating, the sessions family's rule: a fired blocking
    /// fact always means a wait past the bar, so a sibling predicate can ask presence and not a second bar).
    /// An <c>unavailable</c> or <c>no_events</c> event fact makes no claim and scores 0 with its stamp. Every fact
    /// here stamps <c>threshold_lineage = 0</c> — all bars unmeasured, stated on the constants.
    /// </summary>
    private static partial double ScoreBlockingFact(Fact fact)
    {
        switch (fact.Key)
        {
            case PgTargetFactKeys.BlockingChain:
            {
                /* unmeasured: BlockingWaitWarningMs / BlockingWaitCriticalMs (see the constants); threshold_lineage = 0. */
                fact.Metadata["threshold_lineage"] = 0;
                if (!fact.Metadata.TryGetValue(BlockingMaxBlockedQueryMsKey, out var blockedMs) || blockedMs <= 0)
                    return 0.0;
                if (blockedMs < BlockingWaitWarningMs)
                    return 0.0;
                return FactScorer.ApplyThresholdFormula(blockedMs, BlockingWaitWarningMs, BlockingWaitCriticalMs);
            }

            case PgTargetFactKeys.LockWaitEvents:
            {
                /* unmeasured: the chain's two bars, reused by name (one vocabulary); threshold_lineage = 0. */
                fact.Metadata["threshold_lineage"] = 0;
                if (fact.Metadata.GetValueOrDefault(LockWaitUnavailableKey) >= 1 || fact.Metadata.GetValueOrDefault(LockWaitNoEventsKey) >= 1)
                    return 0.0;
                if (!fact.Metadata.TryGetValue(LockWaitEventsMaxMsKey, out var eventMs) || eventMs <= 0)
                    return 0.0;
                if (eventMs < BlockingWaitWarningMs)
                    return 0.0;
                return FactScorer.ApplyThresholdFormula(eventMs, BlockingWaitWarningMs, BlockingWaitCriticalMs);
            }

            case PgTargetFactKeys.LongRunningQuery:
            {
                /* unmeasured: LongRunningQueryWarningMs / LongRunningQueryCriticalMs (see the constants); threshold_lineage = 0. */
                fact.Metadata["threshold_lineage"] = 0;
                if (!fact.Metadata.TryGetValue(LongRunningQueryMaxMsKey, out var queryMs) || queryMs <= 0)
                    return 0.0;
                if (queryMs < LongRunningQueryWarningMs)
                    return 0.0;
                return FactScorer.ApplyThresholdFormula(queryMs, LongRunningQueryWarningMs, LongRunningQueryCriticalMs);
            }

            default:
                return 0.0;
        }
    }

    /// <summary>
    /// Layer-2 amplifiers for the family. Every predicate reads a sibling's <see cref="Fact.BaseSeverity"/> (its own
    /// verdict, never the amplified <see cref="Fact.Severity"/> — emission-order independence, lane 4's finding) or
    /// the fact's OWN metadata.
    /// <list type="bullet">
    /// <item><description><b>Chain — persistence</b>: one head was the root in <see cref="BlockingPersistenceCaptures"/>
    /// or more captures (<see cref="BlockingRecurringHeadCapturesKey"/>). A holder, not a handoff. Persistence is an amplifier
    /// and not a bar because the sample cannot turn sightings into a duration (class summary).</description></item>
    /// <item><description><b>Chain — the engine wrote it down</b>: <c>PG_LOCK_WAIT_EVENTS</c> fired. An independent
    /// source (the server log, event grain) agrees with the sample — waits past the bar happened, whether or not the
    /// sample caught them.</description></item>
    /// <item><description><b>Chain — the anomaly co-fired</b>: <c>ANOMALY_PG_BLOCKING</c> fired — more sessions were
    /// blocked this hour than this server's own baseline for the hour-of-week says; the fold puts it in this
    /// story (<c>PgTargetFactKeys.AnomalyToFamilies</c>) and the boost says the chain is unusual for THIS server,
    /// not its routine.</description></item>
    /// <item><description><b>Events — the sample named the root</b>: <c>PG_BLOCKING_CHAIN</c> fired. The log says how
    /// long; the sample says who.</description></item>
    /// <item><description><b>Long runner — persistence</b>: the same pid + query_id over the floor in
    /// <see cref="BlockingPersistenceCaptures"/> or more captures (<see cref="LongRunningQueryCapturesKey"/>) — one
    /// statement still running, not a series of long ones.</description></item>
    /// </list>
    /// </summary>
    private static partial List<AmplifierDefinition> BlockingAmplifiers(string key)
    {
        switch (key)
        {
            case PgTargetFactKeys.BlockingChain:
                return
                [
                    new()
                    {
                        Description = "The same head blocker was the root of a chain in three or more captures of the window — a holder, not a lock handoff",
                        /* unmeasured: BlockingCoFireBoost and BlockingPersistenceCaptures, chosen, not measured — see the declarations. */
                        Boost = BlockingCoFireBoost,
                        Predicate = facts =>
                            facts.TryGetValue(key, out var self)
                            && self.Metadata.GetValueOrDefault(BlockingRecurringHeadCapturesKey) >= BlockingPersistenceCaptures,
                    },
                    new()
                    {
                        Description = "The engine's own log_lock_waits lines fired past the same bar — the written record agrees with the sample",
                        /* unmeasured: BlockingCoFireBoost, chosen, not measured — see its declaration. */
                        Boost = BlockingCoFireBoost,
                        Predicate = facts =>
                            facts.TryGetValue(PgTargetFactKeys.LockWaitEvents, out var events) && events.BaseSeverity > 0,
                    },
                    new()
                    {
                        Description = "More sessions were blocked per capture than this server's hour-of-week baseline — the chain is unusual for this server, not its routine",
                        /* unmeasured: BlockingCoFireBoost, chosen, not measured — see its declaration. */
                        Boost = BlockingCoFireBoost,
                        Predicate = facts =>
                            facts.TryGetValue(PgTargetFactKeys.AnomalyBlocking, out var anomaly) && anomaly.BaseSeverity > 0,
                    },
                ];

            case PgTargetFactKeys.LockWaitEvents:
                return
                [
                    new()
                    {
                        Description = "A sampled blocking chain fired in the same window — the sample names the root the log line cannot",
                        /* unmeasured: BlockingCoFireBoost, chosen, not measured — see its declaration. */
                        Boost = BlockingCoFireBoost,
                        Predicate = facts =>
                            facts.TryGetValue(PgTargetFactKeys.BlockingChain, out var chain) && chain.BaseSeverity > 0,
                    },
                ];

            case PgTargetFactKeys.LongRunningQuery:
                return
                [
                    new()
                    {
                        Description = "The same backend was still running the same statement over the floor in three or more captures — one statement, not a series",
                        /* unmeasured: BlockingCoFireBoost and BlockingPersistenceCaptures, chosen, not measured — see the declarations. */
                        Boost = BlockingCoFireBoost,
                        Predicate = facts =>
                            facts.TryGetValue(key, out var self)
                            && self.Metadata.GetValueOrDefault(LongRunningQueryCapturesKey) >= BlockingPersistenceCaptures,
                    },
                ];

            default:
                return [];
        }
    }
}
