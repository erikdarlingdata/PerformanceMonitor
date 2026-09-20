/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The plan chain (filled by lane 27 of #3691, design §6): <c>PG_PLAN_REGRESSION</c> → the statement's
/// <c>PG_BAD_ACTOR_&lt;queryid&gt;</c> (the regression names the statement the queries family already grades; the
/// alias <see cref="PgTargetFactKeys.BadActorFamily"/> resolves it), <c>PG_PARAMETER_SENSITIVITY</c> →
/// <c>PG_PLAN_REGRESSION</c> (a skewed parameter is one way a plan flips), and <c>PG_SEQ_SCAN_ADVISORY</c> →
/// the bad actor it was captured under (lane 30). Predicates read the destination fact's
/// <see cref="Fact.BaseSeverity"/>, never a bar of their own and never <see cref="Fact.Severity"/> (an amplifier
/// must not open the edge that would then corroborate it). A context fact at base 0 cannot open an edge — an
/// anomaly's co-fire is declared as an amplifier reading its <c>fired</c> metadata (lane 15's pattern), never as
/// an edge from the anomaly. The bad-actor edges' own destinations live in <c>PgTargetRelationshipGraph.Query.cs</c>
/// (one source node's edges in one place): an edge FROM a bad actor INTO this family is declared there, not here.
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    /* filled by lane 27 */
    private partial void BuildPlanEdges()
    {
    }
}
