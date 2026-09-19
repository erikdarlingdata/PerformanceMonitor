/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The bloat chain (filled by lane 13 of #3691, design §3.4): <c>PG_BLOAT_TREND</c> ↔ <c>PG_AUTOVACUUM_BACKLOG</c>
/// on the SAME table — bloat is the DAMAGE whose cause is §3.1's backlog (dead tuples autovacuum did not
/// reclaim are where the growth comes from) — and <c>PG_INDEX_BLOAT_TREND</c> → <c>PG_BLOAT_TREND</c> when the
/// index's parent table is one the table trend names. Predicates read the destination fact's verdict and the
/// two facts' object names, never a bar of their own.
///
/// <para><b>Why the table intersection, and what happens without names.</b> A backlog on <c>orders</c> says
/// nothing about bloat growing on <c>events</c>; an edge on co-presence alone would tell a story whose two
/// halves are about different tables. So the predicate intersects the backlog fact's one table
/// (<see cref="Fact.ObjectName"/>) with the set the bloat fact names (its worst table plus the
/// <c>growth_bytes_&lt;schema.table&gt;</c> metadata keys, through <see cref="PgTargetScorer.BloatNamedObjects"/>).
/// When the backlog fact carries no name at all the intersection cannot be tested and the edge falls back
/// to co-presence — and the description says "a table this trend names", which in that one case is the
/// weaker claim; <see cref="PgTargetScorer.BloatAndBacklogShareATable"/> is the single definition both the
/// edge and the amplifier use, so they cannot disagree about which it was.</para>
///
/// <para><b>Why both directions between trend and backlog.</b> The inference engine roots at the highest
/// amplified severity and walks outward, consuming what it reaches — the vacuum chain's lesson
/// (<c>PgTargetRelationshipGraph.Vacuum.cs</c>). Which of the two leads is not fixed: a 900 MB growth over
/// fourteen days grades higher than a table just past its trigger line, and a 40× backlog grades higher
/// than a growth just over the 256 MiB line. One direction would leave the other fact rooting a second story
/// about the same table. The backlog → trend edge is declared HERE, in the bloat partial, because it exists
/// only when the trend does; the vacuum mesh's own pins are untouched (they have no bloat fact, so the edge
/// never fires there).</para>
///
/// <para><b>What it does not do.</b> No edge into <c>PG_XMIN_HOLD</c> (the hold reaches bloat through the
/// backlog, which already meshes with it — a direct edge would make the story a triangle told twice; the
/// hold's role in the growth is the amplifier's sentence). No buffer-cache leaf (a bloated heap IS more pages
/// for the same rows, but the buffer fact grades a server-wide miss share and cannot be attributed to one
/// table). No config co-fire: <c>fillfactor</c> is per-object and no server knob makes a table bloat.</para>
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    private const string BloatCategory = "bloat_growth";

    private partial void BuildBloatEdges()
    {
        /* filled by lane 13 of #3691. */

        /* trend ↔ backlog, same table: the damage and its cause, either one leading. */
        AddEdge(PgTargetFactKeys.BloatTrend, PgTargetFactKeys.AutovacuumBacklog, BloatCategory,
            "PG_AUTOVACUUM_BACKLOG fired on a table this trend names — the dead tuples autovacuum is not clearing are where the growth is coming from",
            facts => BloatTrendAndBacklogCoFire(facts));
        AddEdge(PgTargetFactKeys.AutovacuumBacklog, PgTargetFactKeys.BloatTrend, BloatCategory,
            "PG_BLOAT_TREND fired on this table — the backlog's damage: the bloat estimate has grown past the measured line across the lookback",
            facts => BloatTrendAndBacklogCoFire(facts));

        /* index trend → table trend, same parent table: the heap and its index churning together. */
        AddEdge(PgTargetFactKeys.IndexBloatTrend, PgTargetFactKeys.BloatTrend, BloatCategory,
            "PG_BLOAT_TREND fired on this index's table — heap and index are growing together, which is churn on the table, not an index-only shape",
            facts => Fired(facts, PgTargetFactKeys.BloatTrend)
                && facts.TryGetValue(PgTargetFactKeys.IndexBloatTrend, out var index)
                && facts.TryGetValue(PgTargetFactKeys.BloatTrend, out var table)
                && PgTargetScorer.BloatNamedObjects(index, parentTables: true).Overlaps(PgTargetScorer.BloatNamedObjects(table)));
    }

    /// <summary>Both fired, and they share a table (or the backlog carries no name to test — see the class summary).</summary>
    private static bool BloatTrendAndBacklogCoFire(IReadOnlyDictionary<string, Fact> facts) =>
        Fired(facts, PgTargetFactKeys.BloatTrend)
        && Fired(facts, PgTargetFactKeys.AutovacuumBacklog)
        && PgTargetScorer.BloatAndBacklogShareATable(facts[PgTargetFactKeys.BloatTrend], facts[PgTargetFactKeys.AutovacuumBacklog]);
}
