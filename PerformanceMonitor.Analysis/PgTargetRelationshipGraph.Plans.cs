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
/// The plan chain (filled by lane 27 of #3691, design §6): <c>PG_PLAN_REGRESSION</c> → the statement's
/// <c>PG_BAD_ACTOR_&lt;queryid&gt;</c> (the regression names the statement the queries family already grades; the
/// alias <see cref="PgTargetFactKeys.BadActorFamily"/> resolves it), <c>PG_PARAMETER_SENSITIVITY</c> →
/// <c>PG_PLAN_REGRESSION</c> (a skewed parameter is one way a plan flips), <c>ANOMALY_PG_PLAN_REGRESSION</c> →
/// <c>PG_PLAN_REGRESSION</c> (the server-wide deviation folds onto the fact that names the flip), and
/// <c>PG_SEQ_SCAN_ADVISORY</c> → the bad actor it was captured under (lane 30). Predicates read the destination
/// fact's <see cref="Fact.BaseSeverity"/>, never a bar of their own and never <see cref="Fact.Severity"/> (an
/// amplifier must not open the edge that would then corroborate it). A context fact at base 0 cannot open an edge —
/// an anomaly's co-fire is declared as an amplifier reading its <c>fired</c> metadata (lane 15's pattern), never as
/// an edge from the anomaly. The bad-actor edges' own destinations live in <c>PgTargetRelationshipGraph.Query.cs</c>
/// (one source node's edges in one place): an edge FROM a bad actor INTO this family is declared there, not here.
///
/// <para><b>The alias resolves to the HIGHEST-severity bad actor, not to "this statement's" — and the edge says
/// so.</b> <see cref="PgTargetRelationshipGraph.ResolveBadActor"/> is the one resolution rule the graph performs
/// (declared once in the root so lanes 6, 9 and this one do not each invent one), and it has no per-statement
/// input: it picks the top bad actor of the pass. The regression's edge therefore opens ONLY when that top bad
/// actor IS the regressed statement (<see cref="PgTargetScorer.SameStatementFired"/> against the resolved key) —
/// when the regressed statement is a bad actor but not the top one, the story ends at the regression and the
/// scorer's same-id amplifier still lifts it (amplifiers read the fact set directly and need no edge). A same-id
/// resolution would be a shared-file change to the alias seam, reported to the coordinator rather than made
/// here. The alternative — opening the edge whenever any bad actor with the same id fired and letting the alias
/// point at a different statement — would put the wrong statement's text under the regression's card.</para>
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    private const string PlanCategory = "plans";

    /* filled by lane 27 of #3691 — the regression, sensitivity and anomaly edges. Lane 30 adds the Seq-Scan edge below
       the marker; the marker stays, as v1's did. */
    private partial void BuildPlanEdges()
    {
        /* The regression walks to the statement the queries family graded — when the pass's top bad actor is the
           regressed statement (see the class summary for why the two must be the same fact). */
        AddEdge(PgTargetFactKeys.PlanRegression, PgTargetFactKeys.BadActorFamily, PlanCategory,
            "PG_BAD_ACTOR fired for the same queryid — the statement whose plan flipped is the one holding the window's execution time",
            facts => facts.TryGetValue(PgTargetFactKeys.PlanRegression, out var regression) && regression.BaseSeverity > 0
                && !string.IsNullOrEmpty(regression.ObjectName)
                && string.Equals(ResolveBadActor(facts), PgTargetFactKeys.BadActorKeyPrefix + regression.ObjectName, StringComparison.Ordinal)
                && PgTargetScorer.SameStatementFired(facts, PgTargetFactKeys.PlanRegression, PgTargetFactKeys.BadActorKeyPrefix));

        /* Mechanism to cost: the skew names WHY the plan flips; the regression says what the flip cost. Same statement,
           by ObjectName (the query_id), both fired. */
        AddEdge(PgTargetFactKeys.ParameterSensitivity, PgTargetFactKeys.PlanRegression, PlanCategory,
            "PG_PLAN_REGRESSION fired on the same queryid — the plan variance the skewed column explains stepped the statement's per-call time",
            facts => PgTargetScorer.SameStatementFired(facts, PgTargetFactKeys.ParameterSensitivity, PgTargetFactKeys.PlanRegression));

        /* The deviation folds onto the fact that names the flip (PgTargetFactKeys.AnomalyToFamilies names the same fold
           for the reconciler); the edge is what puts both in one story when the anomaly outranks. */
        AddEdge(PgTargetFactKeys.AnomalyPlanRegression, PgTargetFactKeys.PlanRegression, PlanCategory,
            "PG_PLAN_REGRESSION fired — the server-wide per-call slowdown has a named statement with a captured plan flip behind it",
            facts => facts.TryGetValue(PgTargetFactKeys.PlanRegression, out var regression) && regression.BaseSeverity > 0);

        /* lane 30: PG_SEQ_SCAN_ADVISORY → PgTargetFactKeys.BadActorFamily (the statement the scan was captured under). */
    }
}
