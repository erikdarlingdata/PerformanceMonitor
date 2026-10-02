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

        /* Lane 5's wait edge into this chain (#3542 step 5): IO:DataFileRead is the wait a backend feels on a
           buffer-cache miss, so a fired data-file-read wait leads to PG_BUFFER_CACHE_PRESSURE and on to the knob
           — IO:DataFileRead → PG_BUFFER_CACHE_PRESSURE → CONFIG_PG_SHARED_BUFFERS. The IO rollup has no edge: it
           scores 0 whenever a named IO standout fired (PgTargetScorer.Waits.cs — one wait is graded once), so
           the standout is the root. The predicate reads the destination's verdict; the bars are the scorer's. */
        AddEdge(PgTargetFactKeys.WaitKey("IO", "DataFileRead"), PgTargetFactKeys.BufferCachePressure, "buffer_cache_pressure",
            "The buffer cache is measurably short for this working set — these data file reads are misses",
            facts => facts.TryGetValue(PgTargetFactKeys.BufferCachePressure, out var pressure) && pressure.BaseSeverity > 0);
    }
}
