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
/// The object-growth chain (filled by lane 38 of #3691): <c>PG_DATABASE_GROWTH</c> → <c>PG_BLOAT_TREND</c> on the
/// SAME database — growth that is bloat is a different remedy than growth that is data, and the operator must learn
/// which before a storage decision — and <c>ANOMALY_PG_DATABASE_GROWTH</c> → <c>PG_DATABASE_GROWTH</c>, the
/// deviation folding onto the trend that names the database (<c>PgTargetFactKeys.AnomalyToFamilies</c> names the
/// same fold for the reconciler; the edge is what puts both in one story when the anomaly outranks). Predicates read
/// the destination fact's verdict and the two facts' database names, never a bar of their own.
///
/// <para><b>Why the database intersection, and what happens without a name.</b> A bloat trend on a table in
/// <c>reporting</c> says nothing about <c>appdb</c> growing; an edge on co-presence alone would tell a story whose
/// two halves are about different databases. So the predicate intersects the bloat fact's one database
/// (<see cref="Fact.DatabaseName"/>, its worst table's) with the set the growth fact names (its worst database plus
/// the <c>growth_bytes_&lt;database&gt;</c> metadata keys, through <see cref="PgTargetScorer.GrowthNamedDatabases"/>).
/// When the bloat fact carries no database name the intersection cannot be tested and the edge falls back to
/// co-presence; <see cref="PgTargetScorer.GrowthAndBloatShareADatabase"/> is the single definition the edge and the
/// amplifier use, so they cannot disagree about which it was.</para>
///
/// <para><b>One direction only, growth → bloat.</b> The bloat trend is the more specific finding (a table, a cause
/// in the vacuum mesh) and already meshes with the backlog both ways; a bloat → growth edge would make every bloat
/// story carry a database-size paragraph whose only content is "and the database it is in got bigger", which the
/// bloat card's own heap size already says. When the growth fact outranks, the story walks to the bloat trend and
/// on into the vacuum mesh; when the bloat trend outranks, the growth fact stays its own context card.</para>
///
/// <para><b>What it does not do.</b> No edge into the write family (WAL volume is a rate of change, not a level,
/// and a database can grow on a quiet WAL through a COPY that is mostly checkpointed away), no edge into the
/// sessions or CPU families, no disk-free leaf (not collected). No config co-fire: no server knob makes a database
/// grow.</para>
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    private const string GrowthCategory = "object_growth";

    private partial void BuildGrowthEdges()
    {
        /* filled by lane 38 of #3691. */

        /* growth → bloat trend, same database: is the growth dead space? */
        AddEdge(PgTargetFactKeys.DatabaseGrowth, PgTargetFactKeys.BloatTrend, GrowthCategory,
            "PG_BLOAT_TREND fired on a table in a database this trend names — part of the growth is dead space autovacuum has not reclaimed, so the bloat card's levers come before any storage decision",
            facts => Fired(facts, PgTargetFactKeys.DatabaseGrowth)
                && Fired(facts, PgTargetFactKeys.BloatTrend)
                && PgTargetScorer.GrowthAndBloatShareADatabase(facts[PgTargetFactKeys.DatabaseGrowth], facts[PgTargetFactKeys.BloatTrend]));

        /* the deviation → the trend that names the database. */
        AddEdge(PgTargetFactKeys.AnomalyDatabaseGrowth, PgTargetFactKeys.DatabaseGrowth, GrowthCategory,
            "PG_DATABASE_GROWTH fired — the unusual growth rate this window sits on a fortnight's trend that names the database",
            facts => Fired(facts, PgTargetFactKeys.DatabaseGrowth));
    }
}
