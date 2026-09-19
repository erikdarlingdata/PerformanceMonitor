/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The query chain (lane 7, with lane 6): <c>PG_BAD_ACTOR_*</c> is the leaf wherever a statement is the answer;
/// <c>PG_TEMP_SPILL</c> → { <c>CONFIG_PG_WORK_MEM</c>, <c>PG_BAD_ACTOR_*</c> } is lane 6's edge.
///
/// <para><b>Empty in v1, deliberately (lane 7).</b> A bad actor is a LEAF: nothing in the v1 vocabulary is
/// downstream of "this one statement holds the time" — the regression fact that would be
/// (<c>PG_QUERY_REGRESSION</c>, window-over-window <c>mean_exec_ms</c> step corroborated by a
/// <c>pg_plan_capture</c> plan-hash change) is v2 content and has no key yet. The edges INTO a bad actor
/// belong to the families whose symptom a statement explains (<c>PG_TEMP_SPILL</c>, lane 6;
/// <c>PG_CPU_PERCENT</c>, lane 9), and those lanes own them. Until an edge reaches it, a bad actor that
/// clears the story threshold roots its own card, exactly as the SQL Server <c>BAD_ACTOR_*</c> family does
/// (that graph declares no edge for it either).</para>
///
/// <para><b>What an edge into this family has to solve.</b> <see cref="RelationshipGraph.AddEdge"/> takes an
/// exact destination key and the traversal looks it up in the fact set by that string; this family's keys
/// are dynamic (<see cref="PgTargetFactKeys.BadActorKey"/>, one per <c>queryid</c>), so a static edge cannot
/// name one. The lane that writes the first edge INTO a bad actor either resolves the destination at
/// build-story time (the top-share bad actor present in the fact set) or declares a stable alias key the
/// collector also stamps — a shared-vocabulary decision, recorded here so it is made once.</para>
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    private partial void BuildQueryEdges()
    {
        /* No edges out of a bad actor in v1 — see the class summary. */
    }
}
