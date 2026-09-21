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
/// <para><b>Lane 32's edge, declared here between waves: <c>PG_TEMP_SPILL → CONFIG_PG_MEMORY_OVERCOMMIT</c>.</b>
/// The composition check (<c>shared_buffers + max_connections × work_mem …</c> against the host) names the spill as
/// one of its two D5 co-fires — a spill says the per-backend <c>work_mem</c> term is being SPENT, not merely budgeted
/// — and lane 32's amplifier in <c>PgTargetScorer.Memory.cs</c> already lifts the sum when the spill fires. What
/// it could not do from its own file was open the STORY: this graph's rule is that one source node's edges live in
/// one place, and the spill's edges are this file's, so lane 32 reported the edge as an out-of-lane item (#3809)
/// and the third between-waves batch declares it here, beside the <c>work_mem</c> edge. The predicate reads the
/// sum's <see cref="Fact.BaseSeverity"/> — positive only when the configured worst case exceeds this host, the
/// scorer's verdict with its lineage — never a ratio of its own, matching the two edges into the same leaf in
/// <c>PgTargetRelationshipGraph.HostMemory.cs</c>. Traversal picks the higher-severity destination and skips a leaf
/// an earlier story consumed, so a spilling host whose <c>PG_HOST_MEMORY_PRESSURE</c> outranks the spill keeps the
/// pressure → sum story and the spill walks to <c>work_mem</c> as before (the live memory e2e pins that outcome);
/// the sum appears in a spill's path only where nothing higher has claimed it. A stock target has no sum fact at
/// all (no host-memory source), so the edge is inert there by construction.</para>
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
///
/// <para><b>Lane 34 (ruled 2026-09-20): the family's own anomaly walks INTO the statement it names.</b>
/// <c>ANOMALY_PG_BAD_ACTOR_SHARE → PG_BAD_ACTOR</c> — the deviation from the statement's OWN hour-of-week share is
/// the grade now, and the card is its context leaf (the share, the calls, the mean, the drill-down's text). The edge
/// names the alias and opens only when the alias resolves to the statement the anomaly names
/// (<see cref="PgTargetScorer.OwnNormalAnomalyNames"/> — the same predicate the card's lift uses, so the
/// resolution and the lift agree by construction: the lift is what makes the named card the highest-severity bad
/// actor). When the anomaly outranks the card it roots and consumes it — one story; when the lifted card outranks
/// (a large share at a modest sigma) the card roots first and the anomaly stays its own story, and
/// <c>InferenceEngine.ClusterIntoIncidents</c> unions the two across this active edge — one incident. The static
/// <c>AnomalyToFamilies</c> entry names the same alias for the reconciler and cannot fold on it (an exact-key lookup;
/// no path carries the alias) — recorded on that map; this edge is the mechanism.</para>
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

        /* Lane 32's co-fire, as a story (#3809): the spill leads to the composition check when the configured worst
           case exceeds this host. Base-gated on the sum's own verdict — the arithmetic is the scorer's, with its
           lineage — and inert wherever the sum fact is absent (a stock target has no host-memory source). */
        AddEdge(PgTargetFactKeys.TempSpill, PgTargetFactKeys.ConfigMemoryOvercommit, TempCategory,
            "The configured memory worst case exceeds this host, and these spills say the per-backend work_mem term is being spent, not merely budgeted (D5)",
            facts => facts.TryGetValue(PgTargetFactKeys.ConfigMemoryOvercommit, out var sum) && sum.BaseSeverity > 0);

        /* Lane 34: the own-normal deviation leads to the statement it names — see the class summary. Predicate reads
           the anomaly's verdict (BaseSeverity > 0, the shared deviation ramp's) and the alias resolution, never a bar. */
        AddEdge(PgTargetFactKeys.AnomalyBadActorShare, PgTargetFactKeys.BadActorFamily, QueriesCategory,
            "PG_BAD_ACTOR fired for the statement the anomaly names — its share of the window is beyond its own hour-of-week normal",
            facts =>
            {
                var resolved = ResolveBadActor(facts);
                return resolved is not null && PgTargetScorer.OwnNormalAnomalyNames(facts, resolved);
            });

        /* No edges OUT of a bad actor — see the class summary; the family's anomaly is an edge INTO it. */
    }

    /// <summary>The edge category of the queries chain — the anomaly into its statement.</summary>
    private const string QueriesCategory = "queries";

    /// <summary>The edge category of the temp chain (the audit label; a story's category is its root fact's
    /// source, so a spill-rooted finding files under <c>pg_temp</c> regardless).</summary>
    private const string TempCategory = "temp_spill";
}
