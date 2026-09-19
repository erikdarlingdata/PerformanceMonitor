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
/// <para><b>Lane 6's edge: <c>PG_TEMP_SPILL → CONFIG_PG_WORK_MEM</c>.</b> The spill is the workload evidence and
/// the knob is its leaf (D5): the edge fires when the knob FIRED — <c>Severity &gt; 0</c>, which for this
/// evidence-gated key means the collector stamped this very spill onto it at or above the floor — so the story
/// reads spill → knob and the knob never roots a card of its own. No edge into <c>PG_BAD_ACTOR_*</c> from the
/// spill tonight: the destination problem below is unsolved (an exact key is needed and the family's keys are
/// dynamic), so the offending statements reach the reader through the drill-down
/// (<c>pg_temp_spill_statements</c>, <c>PgTargetDrillDownCollector.Queries.cs</c>) and the spill's amplifier
/// names that a temp-writing bad actor exists, rather than through a path node.</para>
///
/// <para><b>No edges OUT of a bad actor in v1, deliberately (lane 7).</b> A bad actor is a LEAF: nothing in the v1 vocabulary is
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
/// collector also stamps — a shared-vocabulary decision, recorded here so it is made once. <b>Decided between
/// waves:</b> both halves of the first option — an edge names <see cref="PgTargetFactKeys.BadActorFamily"/>
/// (<c>PG_BAD_ACTOR</c>, never a fact's key) and the root graph's <c>GetActiveEdges</c> override resolves it
/// to the highest-severity bad actor present, or drops the edge. Lanes 6 and 9 write
/// <c>AddEdge(PG_TEMP_SPILL, PgTargetFactKeys.BadActorFamily, …)</c> and nothing else changes.</para>
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    private partial void BuildQueryEdges()
    {
        /* Lane 6: the spill leads to the knob when the knob fired on the spill's own evidence. The predicate reads
           the knob's verdict, never a size of its own; the bar is PgTargetScorer.Temp.cs's with its lineage. */
        AddEdge(PgTargetFactKeys.TempSpill, PgTargetFactKeys.ConfigWorkMem, TempCategory,
            "work_mem is the per-sort budget these temp files exceeded — the knob fired on this spill's evidence (D5)",
            facts => facts.TryGetValue(PgTargetFactKeys.ConfigWorkMem, out var knob) && knob.Severity > 0);

        /* No edges out of a bad actor in v1 — see the class summary. */
    }

    /// <summary>The edge category of the temp chain (the audit label; a story's category is its root fact's
    /// source, so a spill-rooted finding files under <c>pg_temp</c> regardless).</summary>
    private const string TempCategory = "temp_spill";
}
