/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The saturation chain (filled by lane 3 of #3542, design §3.6): <c>PG_CONNECTION_SATURATION</c> → <c>Lock</c>-type
/// waits (parked holders block; lane 5 adds the wait edge when the wait family exists); saturation ↔
/// <c>PG_IDLE_IN_TRANSACTION</c> is a v2 hook and stays a comment until that fact exists.
///
/// <para><b>No edge is added tonight, deliberately.</b> Both destinations the chain names are facts other lanes
/// emit — the <c>PG_WAIT_LOCK*</c> rollup is lane 5's and <c>PG_IDLE_IN_TRANSACTION</c> is v2 — and an edge whose
/// predicate asks <c>BaseSeverity &gt; 0</c> of a fact nobody emits is inert by construction but reads as a
/// finished chain to the next person. The two context facts the saturation ratio is composed from
/// (<c>CONFIG_PG_MAX_CONNECTIONS</c>, <c>CONFIG_PG_SUPERUSER_RESERVED</c>) score 0 and are stated IN the
/// saturation advice, so an edge to them would never be followed and would say nothing the card does not.
/// The saturation fact therefore roots a one-card story in v1, with the parked-connections population named
/// in its own state breakdown (the <c>peak_idle_in_transaction_share</c> amplifier) rather than reached
/// through an edge.</para>
/// </summary>
public sealed partial class PgTargetRelationshipGraph
{
    private partial void BuildSaturationEdges()
    {
        /* Lane 5 — saturation → Lock-type waits: when the pool is near its ceiling AND the Lock rollup fired, the
           slots are held by sessions waiting on each other's locks (a blocked holder keeps its connection), so
           the pool is short because of contention, not arrivals. Written by lane 5 against its rollup key,
           because the "fired" line is that family's bar:

        AddEdge(PgTargetFactKeys.ConnectionSaturation, PgTargetFactKeys.WaitKey("Lock", null), "connection_saturation",
            "Lock waits fired while the pool was near its ceiling — blocked sessions are holding the slots",
            facts => facts.TryGetValue(PgTargetFactKeys.WaitKey("Lock", null), out var lockWait) && lockWait.BaseSeverity > 0);

           v2 — saturation → PG_IDLE_IN_TRANSACTION: the parked-connections leaf with a measured duration and a
           horizon claim (design §3.10 names it "the app-defect leaf of three chains (blocking, xmin, saturation)"):

        AddEdge(PgTargetFactKeys.ConnectionSaturation, PgTargetFactKeys.IdleInTransaction, "connection_saturation",
            "Long idle-in-transaction sessions are holding the slots the pool is short of",
            facts => facts.TryGetValue(PgTargetFactKeys.IdleInTransaction, out var parked) && parked.BaseSeverity > 0); */
    }
}
