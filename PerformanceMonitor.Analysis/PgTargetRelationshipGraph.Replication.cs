/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The replication chain (filled by lane 12 of #3691, design §3.5 / §3.10): <c>PG_SLOT_XMIN</c> ↔ <c>PG_XMIN_HOLD</c>
/// (a slot's horizon is one of the causes the vacuum family names — "vacuum cannot advance because a slot holds
/// xmin", declared as a MESH for the reason the vacuum chain is one: whichever of the two roots first must reach
/// the other or the one incident becomes two cards), <c>PG_REPLICATION_LAG</c> → <c>PG_SLOT_RETENTION</c> → <c>PG_SLOT_XMIN</c>
/// (a standby's unreplayed WAL is its slot's retained WAL; a retaining slot may be pinning the horizon too — the
/// two independent harms of one abandoned slot, walked in the order an operator meets them), <c>PG_WAL_VOLUME_SHIFT</c>
/// → <c>PG_SLOT_RETENTION</c> (lane 15's fact: the primary writing more WAL than its baseline is a faster-growing
/// pile behind a slot; declared against the constant, inert until that fact exists — the lag fact takes the shift
/// as an amplifier, not an edge), and <c>ANOMALY_PG_REPLICATION_LAG</c> → <c>PG_REPLICATION_LAG</c> (the
/// baseline-relative judgment leads to the fact that names the standby and the stage).
///
/// <para>Predicates read the destination fact's <see cref="Fact.BaseSeverity"/>, never a bar of their own and
/// never <see cref="Fact.Severity"/> — an edge must not be opened by an amplifier the edge itself would then
/// corroborate (the write chain's rule). The bars live in <see cref="PgTargetScorer"/> with their lineage.</para>
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    private const string ReplicationCategory = "replication_risk";

    private partial void BuildReplicationEdges()
    {
        /* slot xmin ↔ xmin hold: one horizon, two reads of it (the slot's own row, the cluster's winning holder). */
        AddEdge(PgTargetFactKeys.SlotXmin, PgTargetFactKeys.XminHold, ReplicationCategory,
            "PG_XMIN_HOLD fired — the cluster's winning horizon holder is graded chronic; vacuum cannot advance past what this slot exports",
            facts => FiredBase(facts, PgTargetFactKeys.XminHold));
        AddEdge(PgTargetFactKeys.XminHold, PgTargetFactKeys.SlotXmin, ReplicationCategory,
            "PG_SLOT_XMIN fired — a replication slot's xmin or catalog_xmin is the horizon being held, read from the slot's own rows",
            facts => FiredBase(facts, PgTargetFactKeys.SlotXmin));

        /* lag → retention → slot xmin: the standby's unreplayed WAL is retained WAL; a retaining slot may also pin the horizon. */
        AddEdge(PgTargetFactKeys.ReplicationLag, PgTargetFactKeys.SlotRetention, ReplicationCategory,
            "PG_SLOT_RETENTION fired — the WAL this standby has not replayed is WAL its slot makes the primary keep; the lag is also a disk-fill path",
            facts => FiredBase(facts, PgTargetFactKeys.SlotRetention));
        AddEdge(PgTargetFactKeys.SlotRetention, PgTargetFactKeys.SlotXmin, ReplicationCategory,
            "PG_SLOT_XMIN fired — a slot is pinning the vacuum horizon as well as the disk: the second independent harm of an abandoned slot",
            facts => FiredBase(facts, PgTargetFactKeys.SlotXmin));

        /* WAL volume shift (lane 15) → the slot retaining that WAL. Inert until the fact exists; the lag fact reads
           the shift as an amplifier instead of an edge (the write chain already owns the shift's checkpoint walk,
           and one replication destination keeps that walk from forking three ways). */
        AddEdge(PgTargetFactKeys.WalVolumeShift, PgTargetFactKeys.SlotRetention, ReplicationCategory,
            "PG_SLOT_RETENTION fired — the primary is writing more WAL than its baseline and a slot is making it keep that WAL",
            facts => FiredBase(facts, PgTargetFactKeys.SlotRetention));

        /* anomaly → the fact it folds into: the baseline-relative judgment leads to the standby and stage. */
        AddEdge(PgTargetFactKeys.AnomalyReplicationLag, PgTargetFactKeys.ReplicationLag, ReplicationCategory,
            "PG_REPLICATION_LAG fired — the anomalous lag is a named standby's drift or a peak past the slot alert's bar, not only a statistical shift",
            facts => FiredBase(facts, PgTargetFactKeys.ReplicationLag));
    }

    /// <summary>Whether <paramref name="key"/> is in the scored set with a positive BASE severity — the write
    /// chain's predicate shape (<see cref="Fact.BaseSeverity"/>, so an amplifier cannot open the edge that would
    /// corroborate it). The vacuum chain's <c>Fired</c> reads <see cref="Fact.Severity"/>; this family keeps to
    /// the stricter reading because two of its edges point at a fact its own amplifiers lift.</summary>
    private static bool FiredBase(System.Collections.Generic.IReadOnlyDictionary<string, Fact> facts, string key) =>
        facts.TryGetValue(key, out var fact) && fact.BaseSeverity > 0;
}
