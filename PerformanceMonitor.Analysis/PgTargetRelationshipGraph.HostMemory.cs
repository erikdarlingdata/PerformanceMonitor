/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The host-memory chain (filled by lane 32 of #3691, design §4b) — named <c>HostMemory</c> because
/// <c>PgTargetRelationshipGraph.Memory.cs</c> is lane 2's v1 buffer chain (<c>PG_BUFFER_CACHE_PRESSURE →
/// CONFIG_PG_SHARED_BUFFERS</c>), and one source node's edges live in one place. Here: <c>PG_HOST_MEMORY_PRESSURE</c>
/// → <c>CONFIG_PG_MEMORY_OVERCOMMIT</c> (the measured shortage leads to the arithmetic that predicted it, when the
/// arithmetic's verdict is positive — the buffer chain's knob-edge shape), and <c>CONFIG_PG_SHARED_BUFFERS</c> →
/// <c>CONFIG_PG_MEMORY_OVERCOMMIT</c> (lane 2's knob is one term of the sum: a cache at the initdb default beside a
/// configuration that does not fit the host is the same sizing conversation, and the story should say so in one card).
/// <c>PG_TEMP_SPILL</c> → <c>CONFIG_PG_MEMORY_OVERCOMMIT</c> is NOT declared here: the spill is
/// <c>PgTargetRelationshipGraph.Query.cs</c>'s source node and this graph's rule is that one source node's edges live in
/// one place, so lane 32 relies on the amplifier that reads the spill's verdict (<c>PgTargetScorer.Memory.cs</c>) for the
/// D5 lift and reports the edge as an out-of-lane item for the query chain's file.
/// Predicates read the destination fact's <see cref="Fact.BaseSeverity"/>, never a bar of their own and never
/// <see cref="Fact.Severity"/>; the overcommit fact is a config advisory (0.4 base), so its own base is positive
/// on a quiet server and an edge INTO it opens on the arithmetic alone — which is the intent (D5: the co-fire
/// lifts, the edge explains).
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    /* filled by lane 32 of #3691 — the marker stays, as v1's did. */
    private partial void BuildHostMemoryEdges()
    {
        /* The measured shortage leads to the arithmetic that predicted it. The predicate reads the sum's verdict
           (BaseSeverity > 0: the configured worst case exceeds this host), never a ratio of its own; the bar is
           PgTargetScorer.Memory.cs's with its lineage. When the sum fits the box, the pressure roots alone and its
           advice says the memory is going to something the configuration did not budget. */
        AddEdge(PgTargetFactKeys.HostMemoryPressure, PgTargetFactKeys.ConfigMemoryOvercommit, HostMemoryCategory,
            "The configured memory worst case exceeds this host — the arithmetic predicted the shortage the OS is reporting",
            facts => facts.TryGetValue(PgTargetFactKeys.ConfigMemoryOvercommit, out var sum) && sum.BaseSeverity > 0);

        /* Lane 2's knob is one term of the sum. Both are 0.4 advisories on a quiet server; joined, the knob's card carries
           the composition as its leaf rather than two sizing cards standing beside each other. */
        AddEdge(PgTargetFactKeys.ConfigSharedBuffers, PgTargetFactKeys.ConfigMemoryOvercommit, HostMemoryCategory,
            "shared_buffers is one term of a configured worst case that exceeds this host's memory",
            facts => facts.TryGetValue(PgTargetFactKeys.ConfigMemoryOvercommit, out var sum) && sum.BaseSeverity > 0);
    }

    /// <summary>The edge category of the host-memory chain (the audit label; a story's category is its root fact's
    /// source, so a pressure-rooted finding files under <c>pg_memory</c> regardless).</summary>
    private const string HostMemoryCategory = "host_memory";
}
