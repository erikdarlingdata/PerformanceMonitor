/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The memory / I-O chain (lane 2): <c>PG_BUFFER_CACHE_PRESSURE</c> → <c>CONFIG_PG_SHARED_BUFFERS</c>; lane 5
/// adds <c>IO:DataFileRead</c> waits → the composite. <c>PG_TEMP_SPILL</c> → <c>CONFIG_PG_WORK_MEM</c> is lane 6, in
/// the query chain file, because its leaf is a statement.
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    /// <summary>
    /// The memory / I-O chain, filled by lane 2: <c>PG_BUFFER_CACHE_PRESSURE → CONFIG_PG_SHARED_BUFFERS</c> —
    /// the composite's measured shortage leads to the knob, when the knob is at the initdb default. The
    /// predicate reads the knob's verdict (<c>BaseSeverity &gt; 0</c>), never a size of its own; the bar is
    /// <c>PgTargetScorer.Config.cs</c>'s with its lineage. Lane 5 adds <c>IO:DataFileRead</c> → the composite.
    /// </summary>
    private partial void BuildMemoryEdges()
    {
        AddEdge(PgTargetFactKeys.BufferCachePressure, PgTargetFactKeys.ConfigSharedBuffers, "buffer_cache_pressure",
            "shared_buffers at the initdb default — the cache under pressure was never sized for this host",
            facts => facts.TryGetValue(PgTargetFactKeys.ConfigSharedBuffers, out var knob) && knob.BaseSeverity > 0);
    }
}
