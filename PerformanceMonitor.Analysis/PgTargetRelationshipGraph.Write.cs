/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The write chain (lane 2): <c>PG_WAL_VOLUME_SHIFT</c> → <c>PG_CHECKPOINT_PRESSURE</c> → <c>CONFIG_PG_MAX_WAL_SIZE</c>;
/// lane 5 adds <c>IO:WALSync</c> / <c>LWLock:WALWrite</c> waits into the same chain.
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    /// <summary>
    /// The write chain, filled by lane 2. Every predicate asks only whether the destination FIRED
    /// (<c>BaseSeverity &gt; 0</c>) — the bars live in <c>PgTargetScorer.Write.cs</c> with their lineage, and the
    /// graph reads their verdict. <c>BaseSeverity</c> rather than <c>Severity</c> so an edge cannot be opened by
    /// an amplifier that the very edge would then corroborate.
    ///
    /// <para><c>PG_WAL_VOLUME_SHIFT → PG_CHECKPOINT_PRESSURE</c>: the leading edge — WAL volume moved against
    /// its baseline, and checkpoints followed. Inert until the v2 baseline lane emits the shift fact (the key is
    /// declared so the edge is written against a constant). <c>PG_CHECKPOINT_PRESSURE →
    /// CONFIG_PG_MAX_WAL_SIZE</c>: the engine's own exhaustion report leads to the ceiling it exhausted, when
    /// that ceiling is the shipped default — the story a checkpoint-churning target returns. Lane 5 adds the
    /// <c>IO:WALSync</c> / <c>LWLock:WALWrite</c> wait edges into this chain.</para>
    /// </summary>
    private partial void BuildWriteEdges()
    {
        AddEdge(PgTargetFactKeys.WalVolumeShift, PgTargetFactKeys.CheckpointPressure, "checkpoint_pressure",
            "Requested checkpoints dominate — the WAL volume shift is forcing checkpoints ahead of checkpoint_timeout",
            facts => facts.TryGetValue(PgTargetFactKeys.CheckpointPressure, out var pressure) && pressure.BaseSeverity > 0);

        AddEdge(PgTargetFactKeys.CheckpointPressure, PgTargetFactKeys.ConfigMaxWalSize, "checkpoint_pressure",
            "max_wal_size at the shipped default — the ceiling the requested checkpoints are hitting was never sized",
            facts => facts.TryGetValue(PgTargetFactKeys.ConfigMaxWalSize, out var knob) && knob.BaseSeverity > 0);
    }
}
