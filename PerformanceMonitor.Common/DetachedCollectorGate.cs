/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;

namespace PerformanceMonitor.Common;

/// <summary>
/// Per-(server, collector) single-flight gate for a collector detached from the sequential per-server
/// body (Darling's <c>RunDueCollectorsAsync</c>) because its cost is BIMODAL — a fast common case with an
/// occasional heavy-tailed run driven by data volume on the monitored server, not by anything this
/// project's own query does wrong.
///
/// <para><b>#2701 was the first instance.</b> <c>query_store</c> (5-minute cadence) could spike from its
/// normal 5-35s to 100-230+ seconds on a server accumulating an abnormally large distinct-plan population
/// (the leaflogix-class decimal-parameter-instability signature). Awaited inline in the sequential body,
/// that spike blocked every OTHER due collector for the same server and could push the whole server's
/// sweep past its 60-second budget (BODY_OVERRUN). The fix was to detach it: fire the collector without
/// awaiting it in the sequential foreach, single-flighted per server so a still-running previous tick
/// skips rather than overlaps.</para>
///
/// <para><b>#2717 is the second.</b> <c>plan_correction</c> (1-minute cadence, a <c>PerItemWallClockBudget</c>
/// of 120 seconds set by #2673 for exactly this tail-risk) showed the identical shape on a fleet server:
/// average ~1 second, occasional 21-second spike, 97% of that spike attributable to one database
/// independently confirmed to carry the same distinct-plan-population signature already root-caused
/// elsewhere in the fleet. The query itself is already correctly seek-based (#2687) — the cost is
/// proportional to real catalog size on an already-affected database, not a tunable defect, so the fix here
/// is the same one #2701 already proved: stop letting one collector's data-driven tail block every other due
/// collector on the same server.</para>
///
/// <para><b>Deliberately generic, and deliberately NOT <see cref="QueryStoreServerGate"/>.</b> That type's
/// mutual exclusion is between TWO SPECIFIC loops (the per-tick collection and the separate first-contact
/// backfill) — an orthogonal concern this type does not need to solve. Reusing it for a second collector
/// would make its own doc comments wrong about what it protects. Keyed by (server, collector name) so two
/// DIFFERENT detached collectors on the same server never share a gate — the only case that actually needs
/// excluding is two overlapping runs of the SAME collector against the SAME server, which risks doubling
/// transient load on a server already in its worst-case tail and writing overlapping rows for the same
/// window. That is a target-load and store-write concern, not a data-race one: each run opens its own
/// connection and each written row is independent, so a caller that skips on a busy gate loses nothing
/// that the next tick's row will not carry.</para>
///
/// <para>Never blocks: <see cref="TryAcquire"/> only, exactly like <see cref="QueryStoreServerGate"/>. A
/// collector whose gate is held simply skips this tick. Only add a collector to this gate's usage sites
/// if a skipped tick is provably safe for it — a watermark-driven, aggregate, or otherwise idempotent
/// window where a miss defers work rather than losing it. A collector whose window is wall-clock derived
/// must not be detached this way.</para>
/// </summary>
public sealed class DetachedCollectorGate
{
    /* Same CompareExchange-flag shape as QueryStoreServerGate and for the same reason: this gate never
       waits, so a SemaphoreSlim would buy blocking/async-wait machinery nothing here ever uses, at the
       cost of a kernel-backed object per (server, collector) pair that nothing disposes (the registry is
       never pruned, by design — one gate per pair lives for the process). */
    private int _taken;

    /// <summary>
    /// True while a detached run holds this (server, collector) slot. Diagnostic only — never branch on
    /// it and then acquire, which is a race; use <see cref="TryAcquire"/>, whose result IS the decision.
    /// </summary>
    public bool IsHeld => Volatile.Read(ref _taken) == 1;

    /// <summary>
    /// Takes the slot if it is free, returning a lease to release it — or <c>null</c> when a previous
    /// detached run still holds it, which the caller must treat as "skip this tick for this collector on
    /// this server".
    /// </summary>
    public IDisposable? TryAcquire() =>
        Interlocked.CompareExchange(ref _taken, 1, 0) == 0 ? new Lease(this) : null;

    private sealed class Lease : IDisposable
    {
        private DetachedCollectorGate? _gate;

        internal Lease(DetachedCollectorGate gate) => _gate = gate;

        /// <summary>
        /// Releases once and only once. Idempotent, and the release is Interlocked, for the same reason
        /// as <see cref="QueryStoreServerGate"/>'s lease: the acquiring tick and its detached continuation
        /// dispose on different threads, and a double-release must never clear a flag a DIFFERENT tick
        /// has since taken.
        /// </summary>
        public void Dispose() => Interlocked.Exchange(ref _gate, null)?.Release();
    }

    private void Release() => Volatile.Write(ref _taken, 0);
}
