/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// How many of this project's store reads are in flight together, declared by the site that fans them out
/// and read by <see cref="ViewerCommandDeadlines.CurrentInteractiveReadSeconds"/> when it stamps a
/// command's deadline (#3004).
///
/// <para><b>Why a deadline needs to know this.</b> #2901 gave every interactive read one ceiling and
/// derived it from the heaviest shipped read measured ALONE — 3.01 s cold for <c>TopQueriesSql</c> over a
/// 30-day custom range at production per-server density. Fifteen sites in this project do not issue reads
/// alone: they await one <c>Task.WhenAll</c> over between two and (unbounded) fleet-many of them. The same
/// rig measured TEN concurrent 30-day reads at 24.4-64.1 s EACH, so ten-way contention on this store costs
/// 8-21x, and a ceiling derived from one read in isolation is not a bound on ten reads at all — it is a
/// bound on a different population that happens to share a code path. The widest fan-out,
/// <see cref="CorrelatedTimelineLanesControl"/>'s ten, sat entirely above it.</para>
///
/// <para><b>Why the width travels with the call rather than the signature.</b> The deadline is stamped at
/// each of this project's 187 interactive command sites, by design: #2901 chose per-site over a wrapper so
/// the regime is visible where the command is built. A width parameter would have to be threaded through
/// every method between the panel and the command to reach those sites, and it is not a property of any of
/// them — it is a property of the CALL. <see cref="AsyncLocal{T}"/> is captured into each task's execution
/// context at the moment the fan-out site creates it, so all ten tasks read the width that site declared
/// even though the declaring frame has long since left the scope by the time they finish. Nothing here is
/// mutable shared state: a write branches the context rather than reaching sibling reads.</para>
///
/// <para><b>Nesting multiplies, it does not take the larger.</b> A read two scopes deep contends with the
/// product of both widths, so that is what it is told. The product is clamped at
/// <see cref="MaxConcurrentReads"/> going in, because a pool cannot serve one more read concurrently than
/// it has permits however many were asked for.</para>
///
/// <para><b>What this does NOT do, and the one thing it now does</b> (#3016). It is still not a throttle:
/// the reads inside a declared scope all start at once and all take a permit, and the width only stops the
/// deadline cutting reads the store was always going to take that long to serve. What changed is that the
/// two per-server fan-outs no longer hand over a raw fleet count — a count above
/// <see cref="MaxConcurrentReads"/> was never a contention count, it was that many contenders plus a queue
/// of reads that could only wait <c>ConnectionTimeoutSeconds</c> for a permit and fail. They split their
/// work into <see cref="Lanes{T}"/> instead, so the width they declare is the concurrency they really
/// have. A cap at the OPTIMUM concurrency — the two-point extrapolation #3004 declined to serialize every
/// panel on — is still not attempted and still needs that sweep; this caps at the permit count, which is a
/// resource fact rather than a performance one.</para>
/// </summary>
public static class ViewerReadFanOut
{
    /// <summary>
    /// The declared width for the current execution context. Zero — the default for a context no scope
    /// has touched — means "nobody declared a fan-out", which <see cref="CurrentWidth"/> reports as one.
    /// </summary>
    private static readonly AsyncLocal<int> s_width = new();

    /// <summary>
    /// The most reads this project will run against the store at once: the smaller of the permits this
    /// seat actually has (<see cref="ViewerStorePool.MaxPoolSize"/>) and the width the per-lane contention
    /// allowance was measured at (<see cref="ViewerCommandDeadlines.MeasuredFanOutWidth"/>).
    ///
    /// <para><b>Both bounds are load-bearing and neither implies the other</b> (#3016). Past the PERMITS a
    /// read is not a contender at all — it is queued in Npgsql waiting <c>ConnectionTimeoutSeconds</c> for
    /// a slot, and it fails there without ever reaching a command deadline. Past the MEASURED WIDTH the
    /// deadline it would be handed is an extrapolation of a single ten-wide batch, which at Npgsql's
    /// default hundred-connection pool would price a read at 700 s. On the shipped managed seat the two
    /// are the same ten, so this changes nothing there; it is a bring-your-own store, where the operator
    /// owns the pool size, that the old single constant described wrongly in both
    /// directions.</para>
    /// </summary>
    public static int MaxConcurrentReads => ConcurrentReadsFor(ViewerStorePool.MaxPoolSize);

    /// <summary>
    /// <see cref="MaxConcurrentReads"/> against an EXPLICIT pool size — the pure form, so the bound can be
    /// pinned at pool sizes this process is not configured for.
    /// </summary>
    public static int ConcurrentReadsFor(int maxPoolSize) =>
        Math.Clamp(maxPoolSize, 1, ViewerCommandDeadlines.MeasuredFanOutWidth);

    /// <summary>
    /// Splits <paramref name="items"/> into at most <see cref="MaxConcurrentReads"/> lanes, so a caller
    /// with fleet-many reads to issue can run one task per lane and walk its own lane sequentially — a
    /// fan-out whose width is bounded by the pool instead of by the fleet (#3016).
    ///
    /// <para>Lanes are CONTIGUOUS and balanced to within one item. Contiguous because concatenating the
    /// lanes' results in lane order then reproduces the input order, which is what lets a caller drop the
    /// per-item ordering it had before; balanced because a lane holding two items while another holds
    /// eight would make the fan-out as slow as the long lane for no reason.</para>
    ///
    /// <para>Returns no lanes for an empty input, so a caller's <c>Task.WhenAll</c> over the lanes is a
    /// no-op rather than a lane that reads nothing. An empty result also means
    /// <see cref="Of(int)"/> is handed zero, which reports a width of one — the honest answer for a
    /// fan-out with no reads in it.</para>
    /// </summary>
    public static IReadOnlyList<IReadOnlyList<T>> Lanes<T>(IReadOnlyList<T> items)
    {
        ArgumentNullException.ThrowIfNull(items);

        var count = items.Count;
        if (count == 0)
        {
            return Array.Empty<IReadOnlyList<T>>();
        }

        var laneCount = Math.Min(count, MaxConcurrentReads);
        var lanes = new List<IReadOnlyList<T>>(laneCount);

        /* Balanced contiguous split: the first (count % laneCount) lanes take one extra item, so no lane
           is ever more than one item longer than another and every item lands in exactly one lane. */
        var quotient = count / laneCount;
        var remainder = count % laneCount;
        var next = 0;

        for (var lane = 0; lane < laneCount; lane++)
        {
            var size = quotient + (lane < remainder ? 1 : 0);
            var slice = new List<T>(size);

            for (var i = 0; i < size; i++)
            {
                slice.Add(items[next++]);
            }

            lanes.Add(slice);
        }

        return lanes;
    }

    /// <summary>
    /// How many concurrent reads the read being constructed right now is one of. One when no enclosing
    /// site declared otherwise, which is the honest answer for a read issued on its own and the reason an
    /// unscoped read keeps exactly #2901's single-read ceiling rather than inheriting a fan-out's.
    /// </summary>
    public static int CurrentWidth
    {
        get
        {
            var declared = s_width.Value;

            return declared < 1 ? 1 : declared;
        }
    }

    /// <summary>
    /// Declares that the store reads created inside the returned scope run concurrently with each other,
    /// so each one's deadline has to cover contention with the other
    /// <paramref name="concurrentReads"/> - 1 of them.
    ///
    /// <para>Pass the ACTUAL width; the clamp to <see cref="MaxConcurrentReads"/> is this type's job, not
    /// the call site's. Declaring a width the site does not really have is the one way to misuse this — it
    /// buys a longer deadline for a read that has no contention to justify it, and a raw fleet count is
    /// that misuse rather than a safe over-declaration, because the reads past the permits are not
    /// contending, they are queued and failing (#3016). A fan-out whose work is fleet-many splits through
    /// <see cref="Lanes{T}"/> first and declares the lane count.</para>
    /// </summary>
    public static Scope Of(int concurrentReads) => new(concurrentReads);

    /// <summary>
    /// The lifetime of a declared width. A struct because a fan-out site is on the refresh path and this
    /// should not allocate; <c>using var</c> disposes it the same either way.
    /// </summary>
    public readonly struct Scope : IDisposable
    {
        private readonly int _restore;

        internal Scope(int concurrentReads)
        {
            _restore = s_width.Value;

            /* Both factors are clamped into 1..cap BEFORE the multiply, so the product cannot overflow
               and cannot be dragged below 1 by a caller passing 0 or a negative count. The cap is read
               once so a re-publish between the two reads cannot produce a product above either value. */
            var cap = MaxConcurrentReads;
            var declared = Math.Clamp(concurrentReads, 1, cap);
            var enclosing = Math.Clamp(_restore, 1, cap);

            s_width.Value = Math.Clamp(declared * enclosing, 1, cap);
        }

        /// <summary>
        /// Ends the declared width EARLY — call it as soon as the reads it describes have been joined, so
        /// that anything the method does afterwards is not priced as contending with reads that have
        /// already finished. Without it a method-scoped <c>using var</c> runs to the closing brace, and a
        /// store read after the join inherits a contention count that is over by the whole fan-out; a
        /// nested fan-out below it then multiplies against that stale count and lands on the pool ceiling.
        ///
        /// <para>Idempotent, and safe to pair with <c>using</c>: both this and <see cref="Dispose"/> write
        /// the same saved value, so the second write is a no-op and an exception between them still
        /// restores. That is why this is a plain method on a <c>readonly struct</c> rather than a flag —
        /// there is no state to keep, only a value to put back.</para>
        /// </summary>
        public void Release() => s_width.Value = _restore;

        /// <summary>
        /// Restores the enclosing width on THIS context. Reads already in flight branched their context
        /// when they were created, so they keep the width they were given — which is the point: the
        /// declaring frame returns from <c>Task.WhenAll</c> long before the store does.
        /// </summary>
        public void Dispose() => Release();
    }
}
