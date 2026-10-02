/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The write chain (lane 2): <c>PG_CHECKPOINT_PRESSURE</c> → <c>CONFIG_PG_MAX_WAL_SIZE</c>; lane 5 adds
/// <c>IO:WALSync</c> / <c>LWLock:WALWrite</c> waits into the same chain. The WAL-volume leading edge (lane 15,
/// #3691 §3.11) is NOT an edge here — the class summary below says why.
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    /// <summary>
    /// The write chain, filled by lane 2. Every predicate asks only whether the destination FIRED
    /// (<c>BaseSeverity &gt; 0</c>) — the bars live in <c>PgTargetScorer.Write.cs</c> with their lineage, and the
    /// graph reads their verdict. <c>BaseSeverity</c> rather than <c>Severity</c> so an edge cannot be opened by
    /// an amplifier that the very edge would then corroborate.
    ///
    /// <para><c>PG_CHECKPOINT_PRESSURE → CONFIG_PG_MAX_WAL_SIZE</c>: the engine's own exhaustion report leads to
    /// the ceiling it exhausted, when that ceiling is the shipped default — the story a checkpoint-churning
    /// target returns. Lane 5 adds the <c>IO:WALSync</c> / <c>LWLock:WALWrite</c> wait edges into this chain.
    /// On Aurora neither end can fire (both facts are <c>not_applicable</c>, base 0 — #3691 §A4), so the edge's
    /// predicate is false and the story cannot form there even from planted requested-dominant counters; that
    /// is one gate (the scorer's), read by this predicate, not a second one here.</para>
    ///
    /// <para><b>Why the WAL-volume leading edge is an AMPLIFIER and a FOLD, not an edge (lane 15, #3691 §3.11).</b>
    /// Lane 2 declared <c>PG_WAL_VOLUME_SHIFT → PG_CHECKPOINT_PRESSURE</c> against the v2 key, inert until the
    /// shift existed. Lane 15 made the shift a CONTEXT fact (base 0 — WAL volume has no absolute bar; it is
    /// graded only through <c>ANOMALY_PG_WAL_VOLUME</c>, the peak against the server's own hour-of-week bucket),
    /// and a base-0 fact can neither root a story nor be walked to, so an edge FROM it was dead by construction
    /// and is removed rather than left with a doc line that had become false. Re-keying it onto the anomaly was
    /// considered and rejected: <c>InferenceEngine</c> roots by descending severity and CONSUMES the walked path,
    /// so on a pass where the anomaly outranked the pressure fact the persisted finding would root on
    /// <c>ANOMALY_PG_WAL_VOLUME</c> with the pressure as its leaf — capped at the anomaly's 1.49 tuning-class
    /// ceiling however hard the pressure paged, and flapping its root key from pass to pass with whichever
    /// outranked. The relationship therefore lives where the shared pipeline already puts anomaly → parent
    /// relationships: <c>PgTargetFactKeys.AnomalyToFamilies</c> folds the anomaly's story onto the checkpoint
    /// incident (P2), and <c>PgTargetScorer.Write.cs</c>'s trigger amplifier lifts the pressure fact when the
    /// anomaly fired. No v1 anomaly has a graph edge for the same reason.</para>
    /// </summary>
    private partial void BuildWriteEdges()
    {
        AddEdge(PgTargetFactKeys.CheckpointPressure, PgTargetFactKeys.ConfigMaxWalSize, "checkpoint_pressure",
            "max_wal_size at the shipped default — the ceiling the requested checkpoints are hitting was never sized",
            facts => facts.TryGetValue(PgTargetFactKeys.ConfigMaxWalSize, out var knob) && knob.BaseSeverity > 0);

        /* Lane 5's wait edges into this chain (#3542 step 5). The WAL waits are the SYMPTOM a backend feels of
           the pressure above: IO:WALSync is fsync on the segment, LWLock:WALWrite is the queue behind the WAL
           writer, and both inflate with the full-page images every forced checkpoint re-arms — so a fired WAL
           wait leads to PG_CHECKPOINT_PRESSURE, which leads to the ceiling: IO:WALSync → PG_CHECKPOINT_PRESSURE
           → CONFIG_PG_MAX_WAL_SIZE. The IO and LWLock ROLLUPS have no edge here: a rollup scores 0 whenever its
           named standout fired (PgTargetScorer.Waits.cs — one wait is graded once), so the standout is the
           root and the rollup never competes with it. The edges run symptom → cause only: when the pressure
           OUTRANKS the wait (a near-total requested share amplified by its knob), the pressure roots first,
           walks to the knob, and the wait roots a one-fact story whose advice names the co-fire — accepted
           and pinned, because a reverse pressure → wait edge would make the walk prefer the higher-severity
           wait over the knob and leave the knob to root a third card. Predicates read the destination's verdict
           (BaseSeverity > 0 — a PostgreSQL wait fact scores 0 below its concerning bar, so "fired" means "a
           finding"); the bars are PgTargetScorer.Waits.cs's with their lineage. */
        AddEdge(PgTargetFactKeys.WaitKey("IO", "WALSync"), PgTargetFactKeys.CheckpointPressure, "checkpoint_pressure",
            "Requested checkpoints dominate — the full-page images forced checkpoints re-arm are the WAL these fsyncs are waiting on",
            facts => facts.TryGetValue(PgTargetFactKeys.CheckpointPressure, out var pressure) && pressure.BaseSeverity > 0);
        AddEdge(PgTargetFactKeys.WaitKey("LWLock", "WALWrite"), PgTargetFactKeys.CheckpointPressure, "checkpoint_pressure",
            "Requested checkpoints dominate — WAL volume is forcing checkpoints, and every commit is queued behind the same WAL",
            facts => facts.TryGetValue(PgTargetFactKeys.CheckpointPressure, out var pressure) && pressure.BaseSeverity > 0);
    }
}
