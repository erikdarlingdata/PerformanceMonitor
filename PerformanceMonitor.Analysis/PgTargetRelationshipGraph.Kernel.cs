/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The kernel chain (filled by lane 28 of #3691): <c>PG_CPU_BURN_CORES</c> → the statements burning it
/// (<c>PG_BAD_ACTOR_&lt;queryid&gt;</c> through the <see cref="PgTargetFactKeys.BadActorFamily"/> alias — the same
/// leaf the Aurora <c>PG_CPU_PERCENT</c> reaches in <c>PgTargetRelationshipGraph.Saturation.cs</c>, so a stock
/// target's CPU story ends where an Aurora target's does), and <c>PG_CPU_DECOMPOSITION</c> as the context beside it,
/// never a source node (base 0 cannot open an edge). Predicates read the destination fact's
/// <see cref="Fact.BaseSeverity"/>, never a bar of their own and never <see cref="Fact.Severity"/>. An edge FROM
/// <c>PG_CPU_PERCENT</c> INTO this family, if lane 28 wants one, is declared in the saturation partial that owns
/// that source node, not here.
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    /* filled by lane 28 */
    private partial void BuildKernelEdges()
    {
    }
}
