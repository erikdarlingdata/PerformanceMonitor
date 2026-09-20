/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The kernel chain (filled by lane 28 of #3691): <c>PG_CPU_DECOMPOSITION</c> → the side that dominates it. Compute-
/// bound, it leads to the statement burning the CPU — <c>PG_BAD_ACTOR_&lt;queryid&gt;</c> through the
/// <see cref="PgTargetFactKeys.BadActorFamily"/> alias, the first edge in the tree to name it (the alias resolves to
/// the highest-severity bad actor the pass emitted, lane 7's exec-time ordering; the CPU bad actor by
/// <c>query_id</c> is stated in the advice from the <c>PG_CPU_BURN_CORES</c> fact, and when the two are one statement
/// the story and the sentence agree). Wait-bound, it leads to whichever <c>PG_WAIT_*</c> fact is itself a finding —
/// the eight-key v1 vocabulary, standouts before rollups (a rollup yields to its fired standout in the scorer, so
/// the standout is the one with a verdict). Predicates read the destination fact's <see cref="Fact.BaseSeverity"/>
/// and the decomposition's own share, never a bar of their own beyond the one dominant-share line the scorer
/// declares with its lineage, and never <see cref="Fact.Severity"/>.
///
/// <para><b>Why the source node is the decomposition and not <c>PG_CPU_BURN_CORES</c>.</b> The proxy is context —
/// base 0 always (<c>PgTargetScorer.Kernel.cs</c>: no core count, so no bar) — and a base-0 fact is outside
/// <c>InferenceEngine</c>'s lookup: it can neither root a story nor be walked to, so an edge from it would be dead by
/// construction (the write chain removed its <c>PG_WAL_VOLUME_SHIFT</c> edge for exactly this reason). The graded
/// instrument is <c>ANOMALY_PG_CPU_BURN</c>, and no anomaly has a graph edge: the shared pipeline relates an anomaly
/// to its parent through <see cref="PgTargetFactKeys.AnomalyToFamilies"/> (the fold, registered by the v3 plumbing
/// against <c>PG_CPU_BURN_CORES</c>). The decomposition is the family's one fact that can cross the incident line
/// (0.6 on the co-fire), so it is the node a story can start from; its edges then say where to look next.</para>
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    private const string KernelCategory = "cpu_burn";

    /* filled by lane 28 */
    private partial void BuildKernelEdges()
    {
        /* Compute-bound → the statement. Requires a fired bad actor in the pass (the alias resolution drops the
           edge when none is emitted, never throws) and the decomposition's burn share at or past the dominant line.
           No edge to PG_CPU_BURN_CORES: base 0, unreachable (class summary). */
        AddEdge(PgTargetFactKeys.CpuDecomposition, PgTargetFactKeys.BadActorFamily, KernelCategory,
            "Compute-bound — the CPU this window is burning is attributable: a top statement crossed its own threshold in the same pass",
            facts => PgTargetScorer.IsDominant(facts, PgTargetScorer.KernelBurnShareKey) && AnyBadActorFired(facts));

        /* Wait-bound → the fired wait. Standouts first, then the rollups: InferenceEngine orders a node's active
           edges by the destination's severity, so the walk lands on the wait with the strongest verdict, and a
           rollup whose standout fired scores 0 (PgTargetScorer.Waits.cs) and is never chosen over it. */
        foreach (var (type, waitEvent) in new (string, string?)[]
                 {
                     ("Lock", "relation"), ("LWLock", "WALWrite"), ("IO", "DataFileRead"), ("IO", "WALSync"),
                     ("Lock", null), ("LWLock", null), ("IO", null), ("IPC", null),
                 })
        {
            var waitKey = PgTargetFactKeys.WaitKey(type, waitEvent);
            AddEdge(PgTargetFactKeys.CpuDecomposition, waitKey, KernelCategory,
                $"Wait-bound — backend time this window was parked, and {(waitEvent is null ? type : type + ":" + waitEvent)} is itself a finding",
                facts => PgTargetScorer.IsDominant(facts, PgTargetScorer.KernelWaitShareKey)
                    && facts.TryGetValue(waitKey, out var wait) && wait.BaseSeverity > 0);
        }
    }

    /// <summary>Whether any <c>PG_BAD_ACTOR_*</c> fact in the lookup is a finding — the alias then resolves to the
    /// strongest of them.</summary>
    private static bool AnyBadActorFired(System.Collections.Generic.IReadOnlyDictionary<string, Fact> facts)
    {
        foreach (var (key, fact) in facts)
        {
            if (key.StartsWith(PgTargetFactKeys.BadActorKeyPrefix, StringComparison.Ordinal) && fact.BaseSeverity > 0)
                return true;
        }
        return false;
    }
}
