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
/// The saturation chain (filled by lane 3 of #3542, design §3.6): <c>PG_CONNECTION_SATURATION</c> ↔ <c>Lock</c>-type
/// waits (parked holders block; lane 5 wrote both directions when the wait family landed — see the body);
/// saturation ↔ <c>PG_IDLE_IN_TRANSACTION</c> — the v2 hook lane 3 left as a comment — is live since lane 14 of
/// #3691 emitted the fact (design §3.10: "the app-defect leaf of three chains — blocking, xmin, saturation"),
/// and this file declares the idle fact's edges into all three chains; see the body for why the vacuum-side
/// edge is declared here and not in <c>PgTargetRelationshipGraph.Vacuum.cs</c>.
///
/// <para><b>Lane 3 added no edge of its own, deliberately.</b> Both destinations the chain names were facts other
/// lanes emit — the <c>PG_WAIT_LOCK*</c> family is lane 5's and <c>PG_IDLE_IN_TRANSACTION</c> was v2 — and an edge
/// whose predicate asks <c>BaseSeverity &gt; 0</c> of a fact nobody emits is inert by construction but reads as a
/// finished chain to the next person. The two context facts the saturation ratio is composed from
/// (<c>CONFIG_PG_MAX_CONNECTIONS</c>, <c>CONFIG_PG_SUPERUSER_RESERVED</c>) score 0 and are stated IN the
/// saturation advice, so an edge to them would never be followed and would say nothing the card does not.
/// The saturation fact therefore rooted a one-card story until the wait family landed, with the
/// parked-connections population named in its own state breakdown (the <c>peak_idle_in_transaction_share</c>
/// amplifier) rather than reached through an edge; with lane 5 in, it meshes with the Lock waits, and with lane
/// 14 in, the idle-in-transaction leaf joins it — gated on that same share, so the edge and the amplifier never
/// disagree about whether parked sessions are what fills the pool.</para>
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

        /* Lane 14 (#3691, design §3.10) — saturation ↔ PG_IDLE_IN_TRANSACTION: the parked-connections leaf with a
           measured duration and a horizon claim. GATED ON THE SHARE, both directions: the edge is active only when
           the saturation fact's OWN peak capture had idle-in-transaction sessions at or over IdleInTransactionShareBar
           (a quarter of the pool). One long-parked session on a pool of 4,000 is a real idle-in-transaction finding
           and has nothing to do with why the pool is near its ceiling; the idle fact joins the saturation story only
           when parked sessions ARE the population filling the slots — the same bar the scorer's self-metadata
           amplifier reads, so the story and the boost agree on what "parked connections fill the pool" means.

           The v1 residue note said this mesh was safe only while saturation had no other leaf; this IS the other
           leaf, so the traversal is stated: from a saturation root the engine follows ONE edge — the higher-severity
           of the fired Lock wait and the fired idle fact — then continues from there. Idle → Lock edges below keep
           saturation → idle → Lock one story, and since the #3691 between-waves batch the reverse hop exists too
           (Lock → idle, the blocking leaf, declared below): whichever of the three fires hardest roots, the other
           two are reached, one story. A saturation finding with NO idle fact in the set sees no active edge from
           any of this and is byte-identical to lane 5's mesh (pinned). */
        AddEdge(PgTargetFactKeys.ConnectionSaturation, PgTargetFactKeys.IdleInTransaction, "connection_saturation",
            "Long idle-in-transaction sessions are a quarter or more of the peak pool — parked transactions are holding the slots the pool is short of",
            facts => facts.TryGetValue(PgTargetFactKeys.IdleInTransaction, out var parked) && parked.BaseSeverity > 0
                && ParkedShareAtBar(facts));
        AddEdge(PgTargetFactKeys.IdleInTransaction, PgTargetFactKeys.ConnectionSaturation, "connection_saturation",
            "The pool is near its ceiling and idle-in-transaction sessions are a quarter or more of it — this parked transaction is one of the slots the next connection will be refused for",
            facts => facts.TryGetValue(PgTargetFactKeys.ConnectionSaturation, out var saturation) && saturation.BaseSeverity > 0
                && ParkedShareAtBar(facts));

        /* The blocking chain, cause → symptom: a transaction that is idle still holds every row and relation lock
           it took, so the Lock waits are on work nobody is doing. One wait is graded once (the rollup scores 0
           when its standout fired), so at most one of these two is active. */
        AddEdge(PgTargetFactKeys.IdleInTransaction, PgTargetFactKeys.WaitKey("Lock", "relation"), "connection_saturation",
            "Lock:relation waits fired while a transaction sat idle holding its locks — the waiters are queued behind work that is not being done",
            facts => facts.TryGetValue(PgTargetFactKeys.WaitKey("Lock", "relation"), out var relation) && relation.BaseSeverity > 0);
        AddEdge(PgTargetFactKeys.IdleInTransaction, PgTargetFactKeys.WaitKey("Lock", null), "connection_saturation",
            "Lock waits fired while a transaction sat idle holding its locks — the waiters are queued behind work that is not being done",
            facts => facts.TryGetValue(PgTargetFactKeys.WaitKey("Lock", null), out var lockWait) && lockWait.BaseSeverity > 0);

        /* The blocking chain, symptom → cause (#3691 between waves; lane 14 reported it out-of-lane because a Lock
           wait's edges were lane 5's, pinned Single in PgTargetWaitTests — that pin moved with this edit, deliberately).
           Design §3.10 names the idle transaction as the blocking leaf: when Lock waits fired AND a long idle-in-
           transaction holder is in the set, the waiters are queued behind that holder's locks, so a Lock-wait root
           walks to the parked transaction rather than ending on the wait. Declared from the Lock waits' OWN partial
           (this file owns their edges — lane 5 wrote the saturation mesh here, and one source node's edges live in
           one place so the traversal from it can be read in one place). Not gated on the saturation share: the pool
           does not have to be full for a parked holder to be what the waiters are behind. Predicate reads the idle
           fact's verdict (BaseSeverity), the file's rule. A Lock wait with no idle fact in the set keeps exactly its
           previous single active edge (saturation), byte-identical to lane 5's mesh — pinned. */
        AddEdge(PgTargetFactKeys.WaitKey("Lock", "relation"), PgTargetFactKeys.IdleInTransaction, "connection_saturation",
            "A long idle-in-transaction holder fired alongside these Lock:relation waits — the waiters are queued behind a parked transaction's locks",
            facts => facts.TryGetValue(PgTargetFactKeys.IdleInTransaction, out var parked) && parked.BaseSeverity > 0);
        AddEdge(PgTargetFactKeys.WaitKey("Lock", null), PgTargetFactKeys.IdleInTransaction, "connection_saturation",
            "A long idle-in-transaction holder fired alongside these Lock waits — the waiters are queued behind a parked transaction's locks",
            facts => facts.TryGetValue(PgTargetFactKeys.IdleInTransaction, out var parked) && parked.BaseSeverity > 0);

        /* The xmin chain, symptom → cause: PG_XMIN_HOLD (lane 4, the vacuum family) names the CLASS of holder —
           when it is a session and the idle fact's longest holder pins the horizon (horizon_age > 0, never the -1
           sentinel), the parked transaction is the holder by name. Declared from this partial rather than
           PgTargetRelationshipGraph.Vacuum.cs because AddEdge is the class's and the vacuum file is lane 4's (the
           STEP brief's file boundary); the category is the vacuum chain's so the story reads as one. XminHold's
           holder_source is the collector's code for the winning source (PgTargetAdvice.HolderSourceCode), and
           only "session" qualifies — a slot or a standby holding the horizon is not this leaf's story. */
        AddEdge(PgTargetFactKeys.XminHold, PgTargetFactKeys.IdleInTransaction, VacuumCategory,
            "PG_IDLE_IN_TRANSACTION fired with a horizon claim — the session holding the xmin horizon back is a parked transaction the application never closed",
            facts => facts.TryGetValue(PgTargetFactKeys.IdleInTransaction, out var parked) && parked.BaseSeverity > 0
                && parked.Metadata.GetValueOrDefault(PgTargetScorer.IdleInTransactionHolderHorizonAgeKey, -1) > 0
                && facts.TryGetValue(PgTargetFactKeys.XminHold, out var hold)
                && (int)hold.Metadata.GetValueOrDefault(PgTargetScorer.XminHolderSourceKey) == PgTargetAdvice.HolderSourceCode("session"));
    }

    /// <summary>The saturation fact's own peak-capture idle-in-transaction share at or over
    /// <see cref="PgTargetScorer.IdleInTransactionShareBar"/> — the gate both directions of the saturation ↔ idle
    /// edge share. False when there is no saturation fact to read.</summary>
    private static bool ParkedShareAtBar(IReadOnlyDictionary<string, Fact> facts) =>
        facts.TryGetValue(PgTargetFactKeys.ConnectionSaturation, out var saturation)
        && saturation.Metadata.GetValueOrDefault("peak_idle_in_transaction_share") >= PgTargetScorer.IdleInTransactionShareBar;
}
