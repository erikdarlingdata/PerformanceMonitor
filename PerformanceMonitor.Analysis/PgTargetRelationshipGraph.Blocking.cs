/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The blocking chain (filled by lane 17 of #3691, design §2a / §3.10): <c>PG_BLOCKING_CHAIN</c> → <c>PG_IDLE_IN_TRANSACTION</c>
/// (a chain whose root is idle in transaction IS the parked holder the idle fact names — the blocking leaf of §3.10,
/// reached by name), <c>PG_BLOCKING_CHAIN</c> ↔ the <c>Lock</c>-type waits (the sampled chain and the measured wait are
/// two readings of one contention; one wait is graded once, so at most one of the two wait keys is fired),
/// <c>PG_LOCK_WAIT_EVENTS</c> → <c>PG_BLOCKING_CHAIN</c> (the written event leads to the sampled chain that names the
/// root), and <c>PG_LONG_RUNNING_QUERY</c> → <c>PG_BLOCKING_CHAIN</c> when the long runner is the chain's root.
/// Predicates read the destination fact's <see cref="Fact.BaseSeverity"/>, never a bar of their own and never
/// <see cref="Fact.Severity"/> (an amplifier must not open the edge that would then corroborate it). The Lock waits'
/// own edges live in <c>PgTargetRelationshipGraph.Saturation.cs</c> (one source node's edges in one place): an edge
/// FROM a Lock wait INTO the chain is declared there, not here.
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    /* filled by lane 17 */
    private partial void BuildBlockingEdges()
    {
    }
}
