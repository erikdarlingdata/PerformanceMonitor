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
/// arithmetic's verdict is positive — the buffer chain's knob-edge shape), and <c>PG_TEMP_SPILL</c> →
/// <c>CONFIG_PG_MEMORY_OVERCOMMIT</c> is declared in <c>PgTargetRelationshipGraph.Query.cs</c> beside the spill's
/// existing <c>CONFIG_PG_WORK_MEM</c> edge if lane 32 wants it, because the spill is that file's source node.
/// Predicates read the destination fact's <see cref="Fact.BaseSeverity"/>, never a bar of their own and never
/// <see cref="Fact.Severity"/>; the overcommit fact is a config advisory (0.4 base), so its own base is positive
/// on a quiet server and an edge INTO it opens on the arithmetic alone — which is the intent (D5: the co-fire
/// lifts, the edge explains).
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    /* filled by lane 32 */
    private partial void BuildHostMemoryEdges()
    {
    }
}
