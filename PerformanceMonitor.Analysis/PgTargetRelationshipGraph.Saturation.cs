/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The saturation chain (filled by lane 3 of #3542, design §3.6): <c>PG_CONNECTION_SATURATION</c> ↔ <c>Lock</c>-type
/// waits (parked holders block; lane 5 wrote both directions when the wait family landed — see the body);
/// saturation ↔ <c>PG_IDLE_IN_TRANSACTION</c> is a v2 hook and stays a comment until that fact exists.
///
/// <para><b>Lane 3 added no edge of its own, deliberately.</b> Both destinations the chain names were facts other
/// lanes emit — the <c>PG_WAIT_LOCK*</c> family is lane 5's and <c>PG_IDLE_IN_TRANSACTION</c> is v2 — and an edge
/// whose predicate asks <c>BaseSeverity &gt; 0</c> of a fact nobody emits is inert by construction but reads as a
/// finished chain to the next person. The two context facts the saturation ratio is composed from
/// (<c>CONFIG_PG_MAX_CONNECTIONS</c>, <c>CONFIG_PG_SUPERUSER_RESERVED</c>) score 0 and are stated IN the
/// saturation advice, so an edge to them would never be followed and would say nothing the card does not.
/// The saturation fact therefore rooted a one-card story until the wait family landed, with the
/// parked-connections population named in its own state breakdown (the <c>peak_idle_in_transaction_share</c>
/// amplifier) rather than reached through an edge; with lane 5 in, it meshes with the Lock waits.</para>
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    private partial void BuildSaturationEdges()
    {
        /* Lane 5 (#3542 step 5) — the saturation ↔ Lock-wait MESH. Both directions are live, on purpose (lane 4's
           lesson): the inference engine walks roots in severity order along one linear path, so a chain written
           in one direction splits into two stories whenever the other end outranks the root. Here it is safe to
           write both — saturation has no other leaf in v1 (lane 3's remarks above), so a saturation root walking
           to the wait steals nothing, and a wait root walking to saturation ends where lane 3's one-card story
           ended anyway. Whichever fires harder roots; the other is consumed; one story either way.

           The wait side is Lock:relation (the standout) or the Lock ROLLUP, never both: a rollup scores 0 whenever
           its named standout fired (PgTargetScorer.Waits.cs — one wait is graded once), so at most one of the two
           is in the fired set. Predicates read the destination's verdict (BaseSeverity > 0 — a PostgreSQL wait
           fact scores 0 below its concerning bar, so "fired" means "a finding"); the bars are the scorer's.

           Symptom → cause: parked and queued sessions hold the heavyweight locks the rest are waiting for. */
        AddEdge(PgTargetFactKeys.WaitKey("Lock", "relation"), PgTargetFactKeys.ConnectionSaturation, "connection_saturation",
            "Sessions are at the connection ceiling — parked or queued sessions are the usual relation-lock holders",
            facts => facts.TryGetValue(PgTargetFactKeys.ConnectionSaturation, out var saturation) && saturation.BaseSeverity > 0);
        AddEdge(PgTargetFactKeys.WaitKey("Lock", null), PgTargetFactKeys.ConnectionSaturation, "connection_saturation",
            "Sessions are at the connection ceiling — parked or queued sessions hold what these backends wait for",
            facts => facts.TryGetValue(PgTargetFactKeys.ConnectionSaturation, out var saturation) && saturation.BaseSeverity > 0);

        /* Cause → symptom (lane 3's shape, written by lane 5 against its keys): when the pool is near its ceiling
           AND Lock waits fired, the slots are held by sessions waiting on each other's locks (a blocked holder
           keeps its connection), so the pool is short because of contention, not arrivals. */
        AddEdge(PgTargetFactKeys.ConnectionSaturation, PgTargetFactKeys.WaitKey("Lock", "relation"), "connection_saturation",
            "Lock:relation waits fired while the pool was near its ceiling — blocked sessions are holding the slots",
            facts => facts.TryGetValue(PgTargetFactKeys.WaitKey("Lock", "relation"), out var relation) && relation.BaseSeverity > 0);
        AddEdge(PgTargetFactKeys.ConnectionSaturation, PgTargetFactKeys.WaitKey("Lock", null), "connection_saturation",
            "Lock waits fired while the pool was near its ceiling — blocked sessions are holding the slots",
            facts => facts.TryGetValue(PgTargetFactKeys.WaitKey("Lock", null), out var lockWait) && lockWait.BaseSeverity > 0);

        /* v2 — saturation → PG_IDLE_IN_TRANSACTION: the parked-connections leaf with a measured duration and a
           horizon claim (design §3.10 names it "the app-defect leaf of three chains (blocking, xmin, saturation)"):

        AddEdge(PgTargetFactKeys.ConnectionSaturation, PgTargetFactKeys.IdleInTransaction, "connection_saturation",
            "Long idle-in-transaction sessions are holding the slots the pool is short of",
            facts => facts.TryGetValue(PgTargetFactKeys.IdleInTransaction, out var parked) && parked.BaseSeverity > 0); */
    }
}
