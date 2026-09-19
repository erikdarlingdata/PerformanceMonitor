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
/// Advice for the blocking / active-query family (design §2a). Value-stated from the facts — the root blocker's state, application and role, how many sessions were behind it and in how many captures, the lock-wait event rate, the long-running statement's duration against its bars — and a remedy per ROOT STATE, because an <c>idle in transaction</c> root is an application defect while an <c>active</c> root is a query-tuning problem and the two need opposite responses (the <c>get_pg_blocking</c> tool's own rule). Never a <c>CREATE INDEX</c> statement (D8); the sample caveat is stated on every chain card (filled by lane 17 of #3691).
///
/// <para><b>Levers, each with its counter-objective (OtterTune doctrine).</b> <c>lock_timeout</c> makes the WAITER
/// fail its statement rather than wait — the application must handle the error. <c>idle_in_transaction_session_timeout</c>
/// ends a parked holder and ROLLS ITS WORK BACK. <c>deadlock_timeout</c> is a logging-versus-detection trade: lower
/// it and deadlocks are found sooner and more waits are logged, at the cost of more frequent deadlock checks on
/// every wait. <c>log_lock_waits = on</c> costs one log line per wait past the timeout — cheap on any server that
/// is not already blocking constantly, and the only way to see waits the one-minute sample misses. The ordering
/// lever — every code path takes its locks in the same order — is the application's and the only one that removes
/// the wait instead of moving it. None of these is a performance setting.</para>
///
/// <para><b>What the card cannot state.</b> A fact's metadata is doubles; the head's statement text, the
/// lock-wait line's relation and fingerprint are strings. The head's application and role ride the fact's two
/// string seams (<see cref="Fact.ObjectName"/> / <see cref="Fact.DatabaseName"/>); the statement text is one
/// <c>get_pg_blocking</c> call away and the card says so instead of paraphrasing a query it does not have.</para>
/// </summary>
public static partial class PgTargetAdvice
{
    /* filled by lane 17 of #3691 — the family's four composers and their static blocks. The marker stays, as v1's did. */

    private static readonly AdviceBlock s_blockingChainStatic = new(
        Headline: "A blocking chain was sampled — sessions queued behind one head blocker's locks",
        Investigation:
            "pg_blocking_edges stores, once a minute, every (blocked, blocking) pair pg_blocking_pids() reported, " +
            "with both sides' state and duration. The analysis reconstructs each capture into chains: a HEAD is a " +
            "blocker that is not itself blocked, the sessions behind it are every waiter reachable through the " +
            "edges, depth is the longest such path. The chain is graded on the blocked side's OWN statement duration " +
            "(30 s WARNING, 5 min CRITICAL — unmeasured bars, chosen not read from the fleet; threshold_lineage = 0) " +
            "and never on how many captures saw it: the collector SAMPLES, so a wait shorter than the cadence is " +
            "invisible and one seen three times was blocked for at least what its own clock says, not three " +
            "minutes. The same head across three or more captures amplifies; the engine's own log_lock_waits lines " +
            "and the blocked-session anomaly corroborate.",
        Remediation:
            "The remedy depends on the head's STATE. Idle in transaction: an application finished a statement and " +
            "never committed — fix the code path (commit or roll back before the connection returns to its pool); " +
            "idle_in_transaction_session_timeout ends such a session and ROLLS ITS WORK BACK. Active: a statement is " +
            "holding locks while it runs — shorten it, break the batch up, or take locks in a consistent order across " +
            "code paths (the one lever that removes the wait rather than moving it). For the waiters, lock_timeout " +
            "makes a statement fail instead of queueing, which the application must then handle. get_pg_blocking " +
            "shows each captured chain with the head's statement text; get_pg_lock_stats shows which lock modes and " +
            "relations were contended; get_pg_log_events (family lock_wait) shows the waits the engine wrote down " +
            "between samples, where log_lock_waits is on.");

    private static readonly AdviceBlock s_lockWaitEventsStatic = new(
        Headline: "The engine logged lock waits past deadlock_timeout — the written record of contention between samples",
        Investigation:
            "With log_lock_waits = on, PostgreSQL writes one 'process N still waiting for <mode> on <resource> after " +
            "N ms' line for every lock wait that outlives deadlock_timeout (1 s by default), and a matching 'acquired " +
            "… after N ms' line when the wait ends — the wait's true end-to-end length. pg_log_events stores those " +
            "lines as the lock_wait family, at EVENT grain: complete where the one-minute pg_blocking sample is not, " +
            "and one line deep where the sample has the blocker's statement. The fact counts the waits, rates them " +
            "per observed hour, and grades the longest wait the engine wrote on the same bars as the sampled chain " +
            "(30 s / 5 min — unmeasured; threshold_lineage = 0). With log_lock_waits off, or no config snapshot to " +
            "say, or the log collector not running in the window, the fact is unavailable and says which — silence " +
            "is never read as zero waits.",
        Remediation:
            "Read the lines with get_pg_log_events (family lock_wait): the DETAIL names the holder's pid and the " +
            "queue, the CONTEXT names the tuple and relation. Pair them with get_pg_blocking for the holder's " +
            "statement. Turning log_lock_waits on where it is off costs one log line per wait past deadlock_timeout " +
            "— cheap on a server that is not already blocking constantly, and the only way to see the waits the " +
            "sample misses. Lowering deadlock_timeout logs shorter waits and finds deadlocks sooner, at the cost of " +
            "running the deadlock check on every wait that long. lock_timeout makes the waiter fail rather than " +
            "queue, which the application must handle.");

    private static readonly AdviceBlock s_longRunningQueryStatic = new(
        Headline: "A statement ran far longer than any interactive request should — a batch that should have been a job, or a plan that went wrong",
        Investigation:
            "pg_session_states stored an active client-backend row whose query_duration_ms crossed the floor. " +
            "Autovacuum workers and every other engine process are excluded (a walsender is 'active' for the life of " +
            "the standby); a session waiting on a Lock is excluded too — it is a waiter, and the blocking-chain fact " +
            "names the head it is behind. The bars are 10 min (WARNING) and 1 h (CRITICAL) of statement duration " +
            "— unmeasured, chosen not read from the fleet; threshold_lineage = 0. The same backend still running the " +
            "same statement in three or more captures amplifies: one statement, not a series.",
        Remediation:
            "get_pg_session_states shows the active sessions with their duration, wait event and query_id; " +
            "pg_statement_stats joins on query_id for the statement's normalised text and its plan-level counters. " +
            "A long runner holds every lock it has taken for its whole life and pins the xmin horizon for its whole " +
            "life, so shortening it (smaller batches, a committed loop, an index the plan is missing — checked, not " +
            "written blind) pays twice. statement_timeout ends a statement past the interval and ROLLS IT BACK, so " +
            "set it above the longest statement the application runs on purpose. No DDL is written here.");

    /// <summary>
    /// Value-stated advice for the three blocking-family keys, or the family's static block when the fact set does
    /// not carry the key (the <see cref="Static"/> path). Null for a key this family does not own.
    /// </summary>
    private static partial AdviceBlock? ComposeBlocking(string key, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        switch (key)
        {
            case PgTargetFactKeys.BlockingChain:
                return factsByKey.TryGetValue(key, out var chain)
                    ? ComposeBlockingChain(chain, factsByKey)
                    : s_blockingChainStatic;

            case PgTargetFactKeys.LockWaitEvents:
                return factsByKey.TryGetValue(key, out var events)
                    ? ComposeLockWaitEvents(events, factsByKey)
                    : s_lockWaitEventsStatic;

            case PgTargetFactKeys.LongRunningQuery:
                return factsByKey.TryGetValue(key, out var runner)
                    ? ComposeLongRunningQuery(runner, factsByKey)
                    : s_longRunningQueryStatic;

            default:
                return null;
        }
    }

    private static AdviceBlock ComposeBlockingChain(Fact fact, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        var blockedMs = fact.Metadata.GetValueOrDefault(PgTargetScorer.BlockingMaxBlockedQueryMsKey);
        var blockedXactMs = fact.Metadata.GetValueOrDefault(PgTargetScorer.BlockingMaxBlockedXactMsKey);
        var headIdle = fact.Metadata.GetValueOrDefault(PgTargetScorer.BlockingHeadIsIdleInTransactionKey) > 0;
        var headPid = fact.Metadata.GetValueOrDefault(PgTargetScorer.BlockingHeadPidKey);
        var headBlocked = fact.Metadata.GetValueOrDefault(PgTargetScorer.BlockingHeadBlockedSessionsKey);
        var headCaptures = fact.Metadata.GetValueOrDefault(PgTargetScorer.BlockingHeadCapturesKey);
        var recurring = fact.Metadata.GetValueOrDefault(PgTargetScorer.BlockingRecurringHeadCapturesKey);
        var headDepth = fact.Metadata.GetValueOrDefault("head_depth");
        var headQueryMs = fact.Metadata.GetValueOrDefault("head_query_ms");
        var headXactMs = fact.Metadata.GetValueOrDefault("head_xact_ms");
        var truncated = fact.Metadata.GetValueOrDefault(PgTargetScorer.BlockingHeadQueryTruncatedKey) > 0;
        var distinctHeads = fact.Metadata.GetValueOrDefault("distinct_heads");
        var longestDepth = fact.Metadata.GetValueOrDefault(PgTargetScorer.BlockingLongestChainDepthKey);
        var peakBlocked = fact.Metadata.GetValueOrDefault(PgTargetScorer.BlockingPeakBlockedSessionsKey);
        var capturesWithBlocking = fact.Metadata.GetValueOrDefault(PgTargetScorer.BlockingCapturesWithBlockingKey);
        var capturesTotal = fact.Metadata.GetValueOrDefault(PgTargetScorer.BlockingCapturesTotalKey);
        var cycles = fact.Metadata.GetValueOrDefault(PgTargetScorer.BlockingCycleCapturesKey);
        var deadlockTimeoutMs = fact.Metadata.GetValueOrDefault(PgTargetScorer.BlockingDeadlockTimeoutMsKey, PgTargetScorer.DeadlockTimeoutDefaultMs);
        var deadlockTimeoutFromSnapshot = fact.Metadata.GetValueOrDefault(PgTargetScorer.BlockingDeadlockTimeoutFromSnapshotKey) > 0;
        var hasPeakAge = fact.Metadata.TryGetValue("peak_age_s", out var peakAgeSeconds);
        var hasLastSeen = fact.Metadata.TryGetValue("head_last_seen_age_s", out var lastSeenSeconds);
        var head = string.IsNullOrEmpty(fact.ObjectName) ? "an unnamed application" : fact.ObjectName;
        var database = string.IsNullOrEmpty(fact.DatabaseName) ? string.Empty : $" in {fact.DatabaseName}";
        var isRecurring = recurring >= PgTargetScorer.BlockingPersistenceCaptures;
        var state = headIdle ? "idle in transaction" : "running a statement";

        var inv = new StringBuilder();
        inv.Append(CultureInfo.InvariantCulture,
            $"The head blocker with the most sessions behind it — {head}{database}, pid {headPid:0}, {state} — had {headBlocked:0} blocked {(headBlocked == 1 ? "session" : "sessions")} behind it summed over the {headCaptures:0} {(headCaptures == 1 ? "capture" : "captures")} it headed{(hasLastSeen ? $", last seen {FormatAge(lastSeenSeconds)} before the window's end" : string.Empty)}; its chain reached {headDepth:0} deep.");
        inv.Append(CultureInfo.InvariantCulture,
            $" The longest any blocked statement had been waiting when sampled was {FormatDuration(blockedMs)} (its transaction {FormatDuration(blockedXactMs)} old) — the number this finding is graded on, read from the blocked session's own clock, never from captures × cadence.");
        inv.Append(CultureInfo.InvariantCulture,
            $" Blocking was caught in {capturesWithBlocking:0} of {(capturesTotal > 0 ? $"the {capturesTotal:0} pg_blocking {(capturesTotal == 1 ? "capture" : "captures")} in the window" : "the window's captures (the collection log recorded no run count)")}; at the peak{(hasPeakAge ? $", {FormatAge(peakAgeSeconds)} before the window's end" : string.Empty)}, {peakBlocked:0} {(peakBlocked == 1 ? "session was" : "sessions were")} blocked at once, the deepest chain was {longestDepth:0} deep, and {distinctHeads:0} distinct {(distinctHeads == 1 ? "head was" : "heads were")} seen.");
        if (cycles > 0)
            inv.Append(CultureInfo.InvariantCulture,
                $" {cycles:0} {(cycles == 1 ? "capture" : "captures")} contained a cycle with no head — a deadlock in flight, which the engine's detector breaks within deadlock_timeout; the deadlock family reports those.");
        if (headIdle)
        {
            inv.Append(" The head was IDLE IN TRANSACTION: it had finished its last statement and was waiting on the client, holding every row and relation lock it had taken — the waiters are queued behind work nobody is doing, and the lever is the application, not the query.");
            if (factsByKey.TryGetValue(PgTargetFactKeys.IdleInTransaction, out var parked) && parked.BaseSeverity > 0)
                inv.Append(CultureInfo.InvariantCulture,
                    $" PG_IDLE_IN_TRANSACTION fired in the same window ({(string.IsNullOrEmpty(parked.ObjectName) ? "an unnamed application" : parked.ObjectName)}, {FormatDuration(parked.Metadata.GetValueOrDefault(PgTargetScorer.IdleInTransactionDurationMsKey))}): this chain is that parked transaction's damage.");
        }
        else
        {
            inv.Append(CultureInfo.InvariantCulture,
                $" The head was ACTIVE — running a statement for {FormatDuration(headQueryMs)} in a transaction {FormatDuration(headXactMs)} old at its worst sighting — so the locks are held by work in progress and the lever is the statement.");
            if (factsByKey.TryGetValue(PgTargetFactKeys.LongRunningQuery, out var runner) && runner.BaseSeverity > 0
                && runner.Metadata.GetValueOrDefault(PgTargetScorer.LongRunningQueryPidKey) == headPid)
                inv.Append(CultureInfo.InvariantCulture,
                    $" PG_LONG_RUNNING_QUERY fired on the same pid ({FormatDuration(runner.Metadata.GetValueOrDefault(PgTargetScorer.LongRunningQueryMaxMsKey))} at its longest sighting): the head IS the long runner.");
        }
        if (truncated)
            inv.Append(" The head's stored statement text was truncated by the collector; get_pg_blocking shows what was kept.");
        if (isRecurring)
            inv.Append(CultureInfo.InvariantCulture,
                $" One head was the root in {recurring:0} captures — a holder, not a lock handoff (persistence amplifies the finding; it is not what graded it).");
        if (factsByKey.TryGetValue(PgTargetFactKeys.LockWaitEvents, out var events) && events.BaseSeverity > 0)
            inv.Append(CultureInfo.InvariantCulture,
                $" The engine's own log agrees: PG_LOCK_WAIT_EVENTS fired with {events.Metadata.GetValueOrDefault(PgTargetScorer.LockWaitEventsCountKey):0} {(events.Metadata.GetValueOrDefault(PgTargetScorer.LockWaitEventsCountKey) == 1 ? "wait" : "waits")} logged past deadlock_timeout, the longest {FormatDuration(events.Metadata.GetValueOrDefault(PgTargetScorer.LockWaitEventsMaxMsKey))}.");
        if (factsByKey.TryGetValue(PgTargetFactKeys.AnomalyBlocking, out var anomaly) && anomaly.BaseSeverity > 0)
            inv.Append(CultureInfo.InvariantCulture,
                $" ANOMALY_PG_BLOCKING co-fired: {anomaly.Metadata.GetValueOrDefault("peak_blocked_sessions"):0} blocked at the peak capture against this server's hour-of-week baseline — unusual for this server, not its routine.");
        var lockWaitFired = (factsByKey.TryGetValue(PgTargetFactKeys.WaitKey("Lock", "relation"), out var relation) && relation.BaseSeverity > 0)
            || (factsByKey.TryGetValue(PgTargetFactKeys.WaitKey("Lock", null), out var lockWait) && lockWait.BaseSeverity > 0);
        if (lockWaitFired)
            inv.Append(" Lock waits crossed their own bar in the wait profile for the same window — the measured time waited and the sampled chain are two readings of one contention.");
        inv.Append(CultureInfo.InvariantCulture,
            $" The bars are {FormatDuration(PgTargetScorer.BlockingWaitWarningMs)} and {FormatDuration(PgTargetScorer.BlockingWaitCriticalMs)} of blocked statement duration — unmeasured, chosen and not read from the fleet (threshold_lineage = 0); deadlock_timeout on this server is {FormatDuration(deadlockTimeoutMs)}{(deadlockTimeoutFromSnapshot ? " (from the config snapshot)" : " (the engine default — no snapshot named it)")}, so every wait this finding describes outlived it many times over.");

        var rem = new StringBuilder();
        if (headIdle)
        {
            rem.Append(CultureInfo.InvariantCulture,
                $"The lever is the application behind {head}: commit or roll back before the connection returns to its pool — an ORM session open across a request, a job waiting on another service inside a transaction, autocommit off. idle_in_transaction_session_timeout ends such a session past the interval and ROLLS ITS WORK BACK, so set it above the longest pause the application makes on purpose; pg_terminate_backend({headPid:0}) does the same to this one session now.");
        }
        else
        {
            rem.Append(CultureInfo.InvariantCulture,
                $"The lever is the statement {head} was running: shorten it, break the batch into committed pieces so each holds its locks briefly, and take locks in the same order on every code path — the one change that removes the wait instead of moving it. A statement holds every lock it takes until its transaction ends, so a {FormatDuration(headXactMs)} transaction is {FormatDuration(headXactMs)} of everyone else waiting.");
        }
        rem.Append(" For the waiters, lock_timeout makes a statement fail instead of queueing past the interval — the application must handle the error, and a retry storm is the counter-objective.");
        rem.Append(" get_pg_blocking shows each captured chain with the head's statement text; get_pg_lock_stats shows which lock modes and relations were contended; get_pg_log_events (family lock_wait) shows the waits the engine wrote down between samples where log_lock_waits is on.");

        return s_blockingChainStatic with
        {
            Headline = string.Format(CultureInfo.InvariantCulture,
                "{0} headed a blocking chain {1} — {2:0} {3} queued behind it, the longest blocked for {4}",
                head, state == "idle in transaction" ? "while idle in transaction" : "while running a statement",
                headBlocked, headBlocked == 1 ? "session" : "sessions", FormatDuration(blockedMs)),
            Investigation = inv.ToString(),
            Remediation = rem.ToString(),
        };
    }

    private static AdviceBlock ComposeLockWaitEvents(Fact fact, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        var deadlockTimeoutMs = fact.Metadata.GetValueOrDefault(PgTargetScorer.BlockingDeadlockTimeoutMsKey, PgTargetScorer.DeadlockTimeoutDefaultMs);
        var unavailable = fact.Metadata.GetValueOrDefault(PgTargetScorer.LockWaitUnavailableKey) > 0;
        var noEvents = fact.Metadata.GetValueOrDefault(PgTargetScorer.LockWaitNoEventsKey) > 0;
        var logCaptures = fact.Metadata.GetValueOrDefault("log_captures");

        if (unavailable)
        {
            var reason = fact.Metadata.GetValueOrDefault(PgTargetScorer.LockWaitReasonLogLockWaitsOffKey) > 0
                ? "log_lock_waits is OFF in the latest config snapshot, so the engine wrote no lock_wait lines — this pass cannot know how long anything waited between samples"
                : fact.Metadata.GetValueOrDefault(PgTargetScorer.LockWaitReasonLogCollectorSilentKey) > 0
                    ? $"log_lock_waits is on but the pg_log_events collector recorded no successful run in the window, so nobody was reading the log — its silence is the collector's, not the server's"
                    : "no pg_server_config snapshot names log_lock_waits and no lock_wait line arrived, so whether the engine was writing cannot be told apart from whether anything waited";
            return s_lockWaitEventsStatic with
            {
                Headline = "Lock-wait events are unavailable on this server — " + (fact.Metadata.GetValueOrDefault(PgTargetScorer.LockWaitReasonLogLockWaitsOffKey) > 0 ? "log_lock_waits is off" : fact.Metadata.GetValueOrDefault(PgTargetScorer.LockWaitReasonLogCollectorSilentKey) > 0 ? "the log collector did not run" : "the setting is unknown"),
                Investigation = $"No lock_wait event in the window, and that is not a measured zero: {reason}. The sampled chain (PG_BLOCKING_CHAIN) is the only blocking evidence this pass has." + $" deadlock_timeout is {FormatDuration(deadlockTimeoutMs)}.",
                Remediation = fact.Metadata.GetValueOrDefault(PgTargetScorer.LockWaitReasonLogLockWaitsOffKey) > 0
                    ? "Set log_lock_waits = on (reloadable, no restart): one log line per lock wait past deadlock_timeout, which is cheap on any server not already blocking constantly and the only record of the waits the one-minute sample misses. Counter-objective: log volume on a server that IS blocking constantly — which is the server you want the lines from."
                    : "Check that the pg_log_events collector is enabled and can reach the server log (the RDS log API or file access); get_pg_log_events shows what it has stored.",
            };
        }

        if (noEvents)
        {
            return s_lockWaitEventsStatic with
            {
                Headline = "No lock wait outlived deadlock_timeout in the window — the engine logged none, with log_lock_waits on",
                Investigation = string.Format(CultureInfo.InvariantCulture,
                    "log_lock_waits is on and the log collector ran {0:0} {1} in the window; no lock_wait line arrived, so no lock wait outlived deadlock_timeout ({2}). A measured zero — a finding about the window, not an absence of data.",
                    logCaptures, logCaptures == 1 ? "time" : "times", FormatDuration(deadlockTimeoutMs)),
                Remediation = "Nothing to do here. If PG_BLOCKING_CHAIN fired in the same window, the sampled waits were all shorter than deadlock_timeout at the engine's own reckoning, or the log collector's window did not yet include their lines.",
            };
        }

        var waits = fact.Metadata.GetValueOrDefault(PgTargetScorer.LockWaitEventsCountKey);
        var acquired = fact.Metadata.GetValueOrDefault("acquired_events");
        var deadlocks = fact.Metadata.GetValueOrDefault(PgTargetScorer.LockWaitEventsDeadlocksKey);
        var maxMs = fact.Metadata.GetValueOrDefault(PgTargetScorer.LockWaitEventsMaxMsKey);
        var acquiredMs = fact.Metadata.GetValueOrDefault(PgTargetScorer.LockWaitEventsAcquiredMsKey);
        var perHour = fact.Metadata.GetValueOrDefault(PgTargetScorer.LockWaitEventsPerHourKey);
        var topRelationWaits = fact.Metadata.GetValueOrDefault("top_relation_waits");
        var hasLastEvent = fact.Metadata.TryGetValue("last_event_age_s", out var lastEventSeconds);
        var relation = string.IsNullOrEmpty(fact.ObjectName) ? null : fact.ObjectName;

        var inv = new StringBuilder();
        inv.Append(CultureInfo.InvariantCulture,
            $"The engine logged {waits:0} lock {(waits == 1 ? "wait" : "waits")} that outlived deadlock_timeout ({FormatDuration(deadlockTimeoutMs)}) in the window — {perHour:0.#} per observed hour — and {acquired:0} of them {(acquired == 1 ? "was" : "were")} later acquired, for {FormatDuration(acquiredMs)} of waiting the engine wrote down; the longest single wait was {FormatDuration(maxMs)}{(hasLastEvent ? $", the last line {FormatAge(lastEventSeconds)} before the window's end" : string.Empty)}.");
        if (deadlocks > 0)
            inv.Append(CultureInfo.InvariantCulture, $" {deadlocks:0} of the lines {(deadlocks == 1 ? "was" : "were")} a detected deadlock — the deadlock family grades those.");
        if (relation is not null)
            inv.Append(CultureInfo.InvariantCulture, $" The relation most waited on was {relation} ({topRelationWaits:0} {(topRelationWaits == 1 ? "wait" : "waits")}, from the lines' CONTEXT).");
        inv.Append(" These are the waits the one-minute sample cannot see: the engine writes the line when the wait crosses the timeout and again when it ends, so the count is complete and the duration is the engine's own.");
        if (factsByKey.TryGetValue(PgTargetFactKeys.BlockingChain, out var chain) && chain.BaseSeverity > 0)
            inv.Append(CultureInfo.InvariantCulture,
                $" PG_BLOCKING_CHAIN fired in the same window and names the head: {(string.IsNullOrEmpty(chain.ObjectName) ? "an unnamed application" : chain.ObjectName)}, {(chain.Metadata.GetValueOrDefault(PgTargetScorer.BlockingHeadIsIdleInTransactionKey) > 0 ? "idle in transaction" : "running a statement")} — the log says how long, the sample says who.");
        inv.Append(CultureInfo.InvariantCulture,
            $" Graded on the longest wait against the chain's bars — {FormatDuration(PgTargetScorer.BlockingWaitWarningMs)} and {FormatDuration(PgTargetScorer.BlockingWaitCriticalMs)}, unmeasured (threshold_lineage = 0) — one vocabulary for the two readings of one contention.");

        var rem = new StringBuilder();
        rem.Append("get_pg_log_events (family lock_wait) shows each line: the DETAIL names the holder's pid and the queue behind it, the CONTEXT the tuple and relation, and the statement fingerprint joins to pg_statement_stats. Pair with get_pg_blocking for the holder's statement text.");
        rem.Append(" lock_timeout makes the waiter fail instead of queueing past the interval — the application must handle it. Lowering deadlock_timeout logs shorter waits and detects deadlocks sooner, at the cost of the deadlock check running on every wait that long; raising it hides the waits this finding is made of.");
        rem.Append(" The ordering lever — every code path takes its locks in the same order — is the application's and the only one that removes the wait.");

        return s_lockWaitEventsStatic with
        {
            Headline = string.Format(CultureInfo.InvariantCulture,
                "The engine logged {0:0} lock {1} past deadlock_timeout{2} — the longest {3}",
                waits, waits == 1 ? "wait" : "waits", relation is null ? string.Empty : $" (most on {relation})", FormatDuration(maxMs)),
            Investigation = inv.ToString(),
            Remediation = rem.ToString(),
        };
    }

    private static AdviceBlock ComposeLongRunningQuery(Fact fact, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        var queryMs = fact.Metadata.GetValueOrDefault(PgTargetScorer.LongRunningQueryMaxMsKey);
        var pid = fact.Metadata.GetValueOrDefault(PgTargetScorer.LongRunningQueryPidKey);
        var queryId = fact.Metadata.GetValueOrDefault(PgTargetScorer.LongRunningQueryIdKey);
        var runnerCaptures = fact.Metadata.GetValueOrDefault("runner_captures_seen");
        var recurring = fact.Metadata.GetValueOrDefault(PgTargetScorer.LongRunningQueryCapturesKey);
        var waiting = fact.Metadata.GetValueOrDefault(PgTargetScorer.LongRunningQueryWaitingKey) > 0;
        var runners = fact.Metadata.GetValueOrDefault("runners");
        var capturesWithRunners = fact.Metadata.GetValueOrDefault("captures_with_runners");
        var hasLastSeen = fact.Metadata.TryGetValue("runner_last_seen_age_s", out var lastSeenSeconds);
        var runner = string.IsNullOrEmpty(fact.ObjectName) ? "an unnamed application" : fact.ObjectName;
        var database = string.IsNullOrEmpty(fact.DatabaseName) ? string.Empty : $" in {fact.DatabaseName}";
        var isRecurring = recurring >= PgTargetScorer.BlockingPersistenceCaptures;

        var inv = new StringBuilder();
        inv.Append(CultureInfo.InvariantCulture,
            $"The longest-running active statement in the window — {runner}{database}, pid {pid:0}{(queryId != 0 ? $", query_id {queryId:0}" : ", no query_id on the row")} — had been running for {FormatDuration(queryMs)} at its longest sighting{(hasLastSeen ? $", last seen {FormatAge(lastSeenSeconds)} before the window's end" : string.Empty)}, over the {FormatDuration(PgTargetScorer.LongRunningQueryWarningMs)} floor in {runnerCaptures:0} {(runnerCaptures == 1 ? "capture" : "captures")}; {runners:0} distinct {(runners == 1 ? "runner was" : "runners were")} over the floor across {capturesWithRunners:0} {(capturesWithRunners == 1 ? "capture" : "captures")}.");
        inv.Append(waiting
            ? " Its sightings carried a wait event other than Lock — it was waiting on I/O, IPC or a lightweight lock at least once, not only burning CPU."
            : " Its sightings carried no wait event — it was on CPU each time it was sampled.");
        inv.Append(" Sessions waiting on a Lock are not counted here: they are the blocking chain's waiters, not runners.");
        if (isRecurring)
            inv.Append(CultureInfo.InvariantCulture,
                $" The same backend was still running the same statement in {recurring:0} captures — one statement, not a series (persistence amplifies the finding; it is not what graded it).");
        if (factsByKey.TryGetValue(PgTargetFactKeys.BlockingChain, out var chain) && chain.BaseSeverity > 0
            && chain.Metadata.GetValueOrDefault(PgTargetScorer.BlockingHeadPidKey) == pid)
            inv.Append(CultureInfo.InvariantCulture,
                $" PG_BLOCKING_CHAIN fired with THIS pid as its head: {chain.Metadata.GetValueOrDefault(PgTargetScorer.BlockingHeadBlockedSessionsKey):0} {(chain.Metadata.GetValueOrDefault(PgTargetScorer.BlockingHeadBlockedSessionsKey) == 1 ? "session" : "sessions")} queued behind the locks this statement holds while it runs.");
        inv.Append(CultureInfo.InvariantCulture,
            $" The bars are {FormatDuration(PgTargetScorer.LongRunningQueryWarningMs)} and {FormatDuration(PgTargetScorer.LongRunningQueryCriticalMs)} of statement duration — unmeasured, chosen and not read from the fleet (threshold_lineage = 0).");

        var rem = new StringBuilder();
        rem.Append(CultureInfo.InvariantCulture,
            $"Find the statement: get_pg_session_states shows pid {pid:0}'s row with its wait event and query_id{(queryId != 0 ? ", and pg_statement_stats joins on that query_id for the normalised text and the plan-level counters" : string.Empty)}. A statement holds every lock it has taken and pins the xmin horizon for its whole life, so shortening it pays twice: smaller batches with a commit between them, a plan check for the scan it should not be doing (an index is a possibility to verify, not DDL to paste), or a job scheduler for work that was never interactive.");
        rem.Append(" statement_timeout ends any statement past the interval and ROLLS IT BACK, so set it above the longest statement the application runs on purpose; pg_cancel_backend(pid) does the same to this one now.");
        if (chain is not null && chain.BaseSeverity > 0)
            rem.Append(" get_pg_blocking shows the chain behind it.");

        return s_longRunningQueryStatic with
        {
            Headline = string.Format(CultureInfo.InvariantCulture,
                "{0} ran one statement for {1}{2}",
                runner, FormatDuration(queryMs),
                chain is not null && chain.BaseSeverity > 0 && chain.Metadata.GetValueOrDefault(PgTargetScorer.BlockingHeadPidKey) == pid
                    ? " — and headed a blocking chain while it ran"
                    : string.Empty),
            Investigation = inv.ToString(),
            Remediation = rem.ToString(),
        };
    }

    /* ── ANOMALY_PG_BLOCKING ── */

    /// <summary>The anomaly's composed block (called from <c>ComposeAnomaly</c>'s <c>ANOMALY_PG_BLOCKING</c> arm, which
    /// the between-waves batch declared so this lane never edits that file): the peak blocked-session count, sigmas
    /// above the hour-of-week baseline, the baseline itself — or the first-occurrence rendering on a low-quality
    /// bucket. The static block claims no figure.</summary>
    private static partial AdviceBlock? ComposeBlockingAnomaly(IReadOnlyDictionary<string, Fact> factsByKey)
    {
        var fallback = BlockingAnomalyStatic();
        return factsByKey.TryGetValue(PgTargetFactKeys.AnomalyBlocking, out var anomaly)
            ? ComposeDeviation(anomaly, fallback, "Blocked sessions per capture", "peak_blocked_sessions", v => v.ToString("0", CultureInfo.InvariantCulture) + (v == 1 ? " session" : " sessions"))
            : fallback;
    }

    /// <summary>Built per call (not <c>static readonly</c>), for the reason <c>PgTargetAdvice.Io.cs</c> states: it
    /// composes the hedge and remediation strings declared in <c>PgTargetAdvice.Anomaly.cs</c>, and static
    /// initialisers across partial files have no defined order.</summary>
    private static AdviceBlock BlockingAnomalyStatic() => new(
        Headline: "More sessions were blocked at once than this server's normal for this time of week",
        Investigation:
            "The window's peak count of distinct blocked sessions per pg_blocking capture (COUNT(DISTINCT blocked_pid) " +
            "from pg_blocking_edges, and ZERO for every capture the collection log says ran and found no edge — a " +
            "capture that looked and saw nothing is a measured zero, a capture that did not run is not a sample) was " +
            "judged against this server's hour-of-week baseline of the same count over the last 30 days. Both the " +
            "peak and the window's mean had to clear the bar, so one hot minute does not fire it. A blocking anomaly " +
            "says more sessions were queued than this hour usually has — not that the waits were long; " +
            "PG_BLOCKING_CHAIN grades the duration and names the head, and this anomaly folds into that story." + s_anomalyHedge,
        Remediation:
            "get_pg_blocking shows the captured chains with the head attributed; get_pg_lock_stats shows the lock " +
            "modes and relations contended. " + s_anomalyRemediation);
}
