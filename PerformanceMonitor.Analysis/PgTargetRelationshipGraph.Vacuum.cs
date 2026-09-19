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
/// The vacuum chain (filled by lane 4 of #3542): <c>PG_WRAPAROUND_TREND</c>, <c>PG_AUTOVACUUM_BACKLOG</c> and
/// <c>PG_XMIN_HOLD</c> as a MESH, with <c>CONFIG_PG_AUTOVACUUM_OFF</c> and <c>CONFIG_PG_MAINT_WORK_MEM</c> as
/// config leaves under the backlog. The causal story §3.3 tells runs one way — a held horizon makes dead
/// tuples unremovable, so the backlog cannot clear, so the forced anti-wraparound vacuum cannot advance
/// <c>relfrozenxid</c> and the age climbs — and every edge's description states its hop in that story; but
/// the edges are declared in BOTH directions between the three, for the reason the SQL Server CPU chain
/// (<c>SOS_SCHEDULER_YIELD</c> ↔ <c>CXPACKET</c> ↔ <c>THREADPOOL</c>) is a mesh too.
///
/// <para><b>Why a mesh, and what "one incident" means here.</b> The inference engine roots stories at the
/// HIGHEST-severity fact first, walks from each node to the highest-severity active destination, and marks
/// every node on a path consumed. Which of the three is highest after amplification is not fixed: each
/// amplifies the others (a hold at the alert's Warning is lifted by the wraparound and backlog co-fires past
/// a wraparound that only just crossed its setting), so a one-directional chain rooted at the wraparound
/// would see the hold root FIRST, form a one-fact story, and leave the wraparound unable to reach it — three
/// stories about one incident. That is exactly what executing the first draft's pins showed. With the mesh,
/// whichever fact leads roots the story and the walk reaches the other two; the story PATH's order follows
/// the severities (the same property the SQL Server mesh has), and every edge description reads correctly in
/// either direction because it names the hop's mechanism, not "cause" or "effect".</para>
///
/// <para><c>PG_XMIN_HOLD</c> → idle-in-transaction / slot xmin are v2 leaves; in v1 the hold fact carries
/// <c>holder_source</c> metadata and the advice names the fix per source, so no edge is declared to a
/// destination that cannot yet exist. The config leaves are one-directional (a setting is never a root of
/// this chain) and D5 holds for <c>maintenance_work_mem</c>: its own base stays under the story threshold
/// and the backlog co-fire amplifier (<c>PgTargetScorer.Vacuum.cs</c>) is what lifts it. Every predicate
/// reads the fact set only — a destination is followed when it fired; the bars live in
/// <see cref="PgTargetScorer"/> with their lineage.</para>
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    private const string VacuumCategory = "vacuum_starvation";

    private partial void BuildVacuumEdges()
    {
        /* wraparound ↔ backlog: the forced freeze vacuum queues behind tables autovacuum is already losing. */
        AddEdge(PgTargetFactKeys.WraparoundTrend, PgTargetFactKeys.AutovacuumBacklog, VacuumCategory,
            "PG_AUTOVACUUM_BACKLOG fired — tables are persistently past their own trigger line, so the forced anti-wraparound vacuum queues behind work autovacuum is already losing",
            facts => Fired(facts, PgTargetFactKeys.AutovacuumBacklog));
        AddEdge(PgTargetFactKeys.AutovacuumBacklog, PgTargetFactKeys.WraparoundTrend, VacuumCategory,
            "PG_WRAPAROUND_TREND fired — the backlog is already showing as a freeze age at or past the engine's own line",
            facts => Fired(facts, PgTargetFactKeys.WraparoundTrend));

        /* backlog ↔ hold: dead tuples newer than the held horizon are unremovable, however often autovacuum runs. */
        AddEdge(PgTargetFactKeys.AutovacuumBacklog, PgTargetFactKeys.XminHold, VacuumCategory,
            "PG_XMIN_HOLD fired — dead tuples newer than the held horizon are unremovable, so the backlog cannot clear however often autovacuum runs",
            facts => Fired(facts, PgTargetFactKeys.XminHold));
        AddEdge(PgTargetFactKeys.XminHold, PgTargetFactKeys.AutovacuumBacklog, VacuumCategory,
            "PG_AUTOVACUUM_BACKLOG fired — the damage the hold is doing: tables past their line and staying there",
            facts => Fired(facts, PgTargetFactKeys.AutovacuumBacklog));

        /* wraparound ↔ hold: a held horizon stops freezing outright — VACUUM freezes only what is older than it. */
        AddEdge(PgTargetFactKeys.WraparoundTrend, PgTargetFactKeys.XminHold, VacuumCategory,
            "PG_XMIN_HOLD fired — a held horizon stops freezing outright, so the forced vacuum cannot advance relfrozenxid past the holder",
            facts => Fired(facts, PgTargetFactKeys.XminHold));
        AddEdge(PgTargetFactKeys.XminHold, PgTargetFactKeys.WraparoundTrend, VacuumCategory,
            "PG_WRAPAROUND_TREND fired — the held horizon is already showing as a freeze age autovacuum cannot bring down",
            facts => Fired(facts, PgTargetFactKeys.WraparoundTrend));

        /* config leaves under the backlog. */
        AddEdge(PgTargetFactKeys.AutovacuumBacklog, PgTargetFactKeys.ConfigAutovacuumOff, VacuumCategory,
            "CONFIG_PG_AUTOVACUUM_OFF — autovacuum is off server-wide; the backlog is by configuration",
            facts => Fired(facts, PgTargetFactKeys.ConfigAutovacuumOff));
        AddEdge(PgTargetFactKeys.AutovacuumBacklog, PgTargetFactKeys.ConfigMaintWorkMem, VacuumCategory,
            "CONFIG_PG_MAINT_WORK_MEM — the dead-tuple memory each autovacuum pass may use bounds how much of the backlog one run can clear",
            facts => Fired(facts, PgTargetFactKeys.ConfigMaintWorkMem));
    }

    /// <summary>Whether <paramref name="key"/> is in the scored set with a positive severity — the one
    /// predicate shape this chain uses. (<see cref="RelationshipGraph"/>'s <c>HasFact</c> is private to it.)</summary>
    private static bool Fired(IReadOnlyDictionary<string, Fact> facts, string key) =>
        facts.TryGetValue(key, out var fact) && fact.Severity > 0;
}
