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
/// Per-server mutual exclusion between the two independent loops that both run heavy Query Store text
/// extraction against the SAME monitored server: the regular per-tick <c>query_store</c> collection and the
/// #2058 first-contact backfill (#2165).
///
/// <para><b>The observed problem.</b> The two loops had no coordination at all. On a 4-core multi-tenant box
/// mid-consolidation, a 64 MB backfill slice for a freshly restored database ran concurrently with the tick's
/// Query Store collection of a SIBLING database — a 12:50:58 backfill ship overlapping a 12:51:09 tick
/// completion — putting roughly 128 MB of Query Store text extraction in flight at once on the box least able
/// to afford it. The overlap is not bad luck: a big catalog arriving is exactly what triggers BOTH the backfill
/// and budget-bound tick passes, so the two loops are most likely to collide precisely when the server is
/// already drowning.</para>
///
/// <para><b>Nothing ever waits, and that is deliberate.</b> Both callers try-acquire with a zero timeout and
/// SKIP on failure. These are shared fleet loops: one server's in-flight slice can run to a 180-300 second
/// abandonment deadline, so a blocking acquire would let one slow server stall collection for every other
/// server — reintroducing the #2148 wedge this codebase already fixed once, through a lock instead of a hang.
/// A gate that can only ever skip cannot do that.</para>
///
/// <para><b>Why skipping is safe for this collector specifically.</b> Query Store collection is
/// watermark-driven (#1960): each pass resumes from the last shipped boundary rather than re-deriving a window,
/// so a skipped pass defers rows, it does not drop them. That is what makes "skip and retry" the right
/// behaviour here and why this gate must not be reused for a collector whose window is wall-clock derived —
/// for one of those, a skipped pass IS lost data.</para>
///
/// <para><b>How the "tick wins, backfill defers" bias is actually realized.</b> Not by preemption: the loser is
/// whichever loop arrives second, because stopping a statement already running against the monitored server
/// would mean killing it, and cancelling a Query Store read mid-flight buys nothing that waiting one cycle does
/// not. The bias comes from CADENCE instead — the tick retries on its own interval (about a minute) while the
/// backfill retries every five, so the tick recovers roughly five times faster from a collision, and a backfill
/// slice is byte-budgeted so it is short in the healthy case. Over any real window the tick therefore wins the
/// overwhelming majority of collisions without either loop ever blocking.</para>
///
/// <para>One gate per server, held in each host's own keyed dictionary — Darling keys by <c>int</c> server id
/// and Lite by <c>string</c> — so this type is the shared primitive rather than the registry. Same reason
/// <see cref="AbandonableStep"/> is shaped that way, and the two are siblings: that one bounds how long a step
/// may hold a loop, this one bounds what may run beside it.</para>
/// </summary>
public sealed class QueryStoreServerGate
{
    /* A plain interlocked flag rather than a SemaphoreSlim, because this gate NEVER waits. A semaphore buys
       blocking acquire, timeouts and async waits — all three of which this design deliberately refuses — while
       costing a disposable kernel-backed object per monitored server that nothing ever disposes (the registries
       are never pruned, by design, so one gate per server lives for the process). CompareExchange gives the one
       operation actually needed, owns nothing, and cannot be released more times than it was taken. */
    private int _taken;

    /// <summary>
    /// True while either loop holds this server's gate. Diagnostic only — never branch on it and then acquire,
    /// which is a race; use <see cref="TryAcquire"/>, whose result IS the decision.
    /// </summary>
    public bool IsHeld => Volatile.Read(ref _taken) == 1;

    /// <summary>
    /// Takes the server's gate if it is free, returning a lease to release it — or <c>null</c> when the other
    /// loop holds it, which the caller must treat as "skip this server this cycle".
    ///
    /// <para>Returns a disposable rather than exposing a bare release so the two cannot get out of step, and a
    /// leaked lease here would silently stop one server's Query Store collection forever — a failure that looks
    /// like "that server has no Query Store data" rather than like a bug. The tick's site is a <c>using</c>,
    /// whose scope IS the collector run, so an early <c>return</c> or a throw releases the gate on the way out.
    /// The backfill's site hands the lease to its <see cref="AbandonableStep"/> instead
    /// (<c>holdUntilStepEnds</c>), because there a <c>using</c> is wrong: its scope is one loop iteration, and
    /// abandonment ends the iteration while the slice is still executing on the server. The lease must outlive
    /// the abandonment and expire with the step's in-flight guard, which is the only thing that knows when the
    /// slice truly ended.</para>
    /// </summary>
    public IDisposable? TryAcquire() =>
        Interlocked.CompareExchange(ref _taken, 1, 0) == 0 ? new Lease(this) : null;

    /// <summary>
    /// A lease that guards nothing, for a caller whose collector is not gated at all.
    ///
    /// <para>Exists so <c>null</c> from <see cref="TryAcquire"/> keeps exactly ONE meaning — "the other loop
    /// holds this server's gate, skip" — at a call site that decides whether to gate and whether it got the
    /// gate in the same expression. Without it, "not gated" and "gate busy" would both be null and every such
    /// site would need to re-test the predicate to tell a skip from a pass-through; getting that wrong silently
    /// skips a collector nobody meant to gate.</para>
    /// </summary>
    public static IDisposable NotGated { get; } = new NoOpLease();

    private sealed class NoOpLease : IDisposable
    {
        public void Dispose()
        {
            /* Nothing held, nothing to release — and safe to dispose repeatedly, since it is a shared
               singleton that every non-gated collector run disposes. */
        }
    }

    private sealed class Lease : IDisposable
    {
        private QueryStoreServerGate? _gate;

        internal Lease(QueryStoreServerGate gate) => _gate = gate;

        /// <summary>
        /// Releases once and only once. Idempotent because a <c>using</c> plus an explicit <c>Dispose()</c> — or
        /// a double-dispose from any future refactor — would otherwise clear a flag a DIFFERENT loop had since
        /// taken, letting both run against one server at the same time: the exact condition this class exists to
        /// prevent. Interlocked because the tick and the backfill dispose their own leases on different threads.
        /// </summary>
        public void Dispose() => Interlocked.Exchange(ref _gate, null)?.Release();
    }

    private void Release() => Volatile.Write(ref _taken, 0);
}
