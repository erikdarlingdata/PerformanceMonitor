/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
/// <see cref="ViewerSettings.ManagedMaxPoolSize"/> going in, because a pool of ten cannot serve an
/// eleventh read concurrently however many were asked for — which is also what makes the two unbounded
/// per-server fan-outs (<c>FinOpsTab.Loaders.cs</c>'s inventory overlay and <c>MainWindow</c>'s overview
/// cards) safe to declare with their raw fleet count rather than a hand-capped guess.</para>
///
/// <para><b>What this does NOT do.</b> It does not throttle anything. The reads still all start at once and
/// still all take a permit; this only stops the deadline cutting reads that the store was always going to
/// take that long to serve. Bounding the fan-out itself is the better fix and is tracked on #3004 — it
/// needs a concurrency sweep (per-read duration at width 1, 2, 4, 6, 8, 10) that does not exist yet,
/// because the two data points available extrapolate to a cap of about two, and serializing every panel in
/// the viewer on a two-point extrapolation is not a trade to make blind.</para>
/// </summary>
public static class ViewerReadFanOut
{
    /// <summary>
    /// The declared width for the current execution context. Zero — the default for a context no scope
    /// has touched — means "nobody declared a fan-out", which <see cref="CurrentWidth"/> reports as one.
    /// </summary>
    private static readonly AsyncLocal<int> s_width = new();

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
    /// <para>Pass the ACTUAL width, including a runtime count for a fan-out whose width is the fleet size;
    /// the clamp to <see cref="ViewerSettings.ManagedMaxPoolSize"/> is this type's job, not the call
    /// site's. Declaring a width the site does not really have is the one way to misuse this — it buys a
    /// longer deadline for a read that has no contention to justify it.</para>
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

            /* Both factors are clamped into 1..pool BEFORE the multiply, so the product cannot overflow
               and cannot be dragged below 1 by a caller passing 0 or a negative count. */
            var declared = Math.Clamp(concurrentReads, 1, ViewerSettings.ManagedMaxPoolSize);
            var enclosing = Math.Clamp(_restore, 1, ViewerSettings.ManagedMaxPoolSize);

            s_width.Value = Math.Clamp(declared * enclosing, 1, ViewerSettings.ManagedMaxPoolSize);
        }

        /// <summary>
        /// Restores the enclosing width on THIS context. Reads already in flight branched their context
        /// when they were created, so they keep the width they were given — which is the point: the
        /// declaring frame returns from <c>Task.WhenAll</c> long before the store does.
        /// </summary>
        public void Dispose() => s_width.Value = _restore;
    }
}
