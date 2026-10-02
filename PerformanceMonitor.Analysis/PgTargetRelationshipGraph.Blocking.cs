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
/// The blocking chain (filled by lane 17 of #3691, design §2a / §3.10): <c>PG_BLOCKING_CHAIN</c> → <c>PG_IDLE_IN_TRANSACTION</c>
/// (a chain whose root is idle in transaction IS the parked holder the idle fact names — the blocking leaf of §3.10,
/// reached by name), <c>PG_BLOCKING_CHAIN</c> ↔ <c>PG_LONG_RUNNING_QUERY</c> (the same pid: the head IS the long
/// runner, both directions so whichever outranks the other roots one story — lane 5's lesson that one direction
/// splits stories), <c>PG_LOCK_WAIT_EVENTS</c> → <c>PG_BLOCKING_CHAIN</c> (the written event leads to the sampled chain
/// that names the root), and <c>ANOMALY_PG_BLOCKING</c> → <c>PG_BLOCKING_CHAIN</c> (the deviation folds onto the fact
/// that grades the duration). Predicates read the destination fact's <see cref="Fact.BaseSeverity"/>, never a bar of
/// their own and never <see cref="Fact.Severity"/> (an amplifier must not open the edge that would then corroborate
/// it). The Lock waits' own edges live in <c>PgTargetRelationshipGraph.Saturation.cs</c> (one source node's edges in
/// one place): the edges FROM a Lock wait and FROM the idle fact INTO the chain are declared there, not here.
///
/// <para><b>Why no edge from the chain to the Lock waits.</b> The wait profile grades one wait once and a Lock wait's
/// story already walks to saturation and to the idle holder; a chain → Lock edge would make the chain's story
/// re-enter the saturation mesh through a node whose own edges are declared elsewhere, and the sampled chain and the
/// measured wait fraction are two readings of the same contention (the advice says so) rather than cause and
/// symptom. The Lock wait reaches the chain (declared in the Lock waits' file) because the chain is the reading
/// that names the root; the chain does not need the wait to say anything it does not already say.</para>
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    private const string BlockingCategory = "blocking";

    /* filled by lane 17 of #3691 — the chain's edges. The marker stays, as v1's did. */
    private partial void BuildBlockingEdges()
    {
        /* Cause by name: the head was idle in transaction AND the idle fact fired — the chain is that parked
           transaction's damage. Gated on the chain's OWN head state, not merely on both being present: a chain
           under an ACTIVE head and an unrelated parked holder elsewhere are two findings. */
        AddEdge(PgTargetFactKeys.BlockingChain, PgTargetFactKeys.IdleInTransaction, BlockingCategory,
            "The chain's head blocker was idle in transaction and PG_IDLE_IN_TRANSACTION fired — the waiters are queued behind a parked transaction the application never closed",
            facts => facts.TryGetValue(PgTargetFactKeys.BlockingChain, out var chain)
                && chain.Metadata.GetValueOrDefault(PgTargetScorer.BlockingHeadIsIdleInTransactionKey) > 0
                && facts.TryGetValue(PgTargetFactKeys.IdleInTransaction, out var parked) && parked.BaseSeverity > 0);

        /* Cause by pid: the head IS the long runner. Both directions so the higher-ranked of the two roots the one
           story and walks to the other; the pid seam is exact within one window on one server (a pid is one backend's
           lifetime; the long-runner read and the edge read are the same captures). */
        AddEdge(PgTargetFactKeys.BlockingChain, PgTargetFactKeys.LongRunningQuery, BlockingCategory,
            "PG_LONG_RUNNING_QUERY fired on the chain's head pid — the locks are held by a statement still running",
            facts => HeadIsTheRunner(facts));
        AddEdge(PgTargetFactKeys.LongRunningQuery, PgTargetFactKeys.BlockingChain, BlockingCategory,
            "PG_BLOCKING_CHAIN fired with this runner's pid as its head — sessions are queued behind the locks this statement holds",
            facts => HeadIsTheRunner(facts));

        /* The written event leads to the sampled chain that names the root. */
        AddEdge(PgTargetFactKeys.LockWaitEvents, PgTargetFactKeys.BlockingChain, BlockingCategory,
            "PG_BLOCKING_CHAIN fired in the same window — the sample names the head the engine's log line cannot",
            facts => facts.TryGetValue(PgTargetFactKeys.BlockingChain, out var chain) && chain.BaseSeverity > 0);

        /* The deviation folds onto the fact that grades the duration (PgTargetFactKeys.AnomalyToFamilies names the
           same fold for the reconciler); the edge is what puts both in one story when the anomaly outranks. */
        AddEdge(PgTargetFactKeys.AnomalyBlocking, PgTargetFactKeys.BlockingChain, BlockingCategory,
            "PG_BLOCKING_CHAIN fired — the unusual blocked-session count has a sampled chain with a named head behind it",
            facts => facts.TryGetValue(PgTargetFactKeys.BlockingChain, out var chain) && chain.BaseSeverity > 0);
    }

    /// <summary>Both blocking facts fired and the chain's head pid is the long runner's pid.</summary>
    private static bool HeadIsTheRunner(IReadOnlyDictionary<string, Fact> facts) =>
        facts.TryGetValue(PgTargetFactKeys.BlockingChain, out var chain) && chain.BaseSeverity > 0
        && facts.TryGetValue(PgTargetFactKeys.LongRunningQuery, out var runner) && runner.BaseSeverity > 0
        && chain.Metadata.TryGetValue(PgTargetScorer.BlockingHeadPidKey, out var headPid)
        && runner.Metadata.TryGetValue(PgTargetScorer.LongRunningQueryPidKey, out var runnerPid)
        && headPid == runnerPid;
}
