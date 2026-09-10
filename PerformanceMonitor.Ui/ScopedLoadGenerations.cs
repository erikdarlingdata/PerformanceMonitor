/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Ui;

/// <summary>
/// <para>Orders the loads that paint one surface, so the paint that lands is the one from the load that
/// started LAST. A loader claims a generation for its surface on entry, and after each await asks whether
/// a newer load for that same surface has begun; if one has, it returns without painting.</para>
///
/// <para><b>Why a generation and not a scope re-verify.</b> #2924 closed three sites by re-reading the UI
/// scope after the store read and dropping the paint when it had moved. That is only a drop, rather than a
/// silent loss, where something else is certain to repaint — in the Darling viewer that is the coalescing
/// replay in <c>RefreshVisibleAsync</c> / <c>RefreshActiveInnerTabAsync</c> / <c>RefreshActiveSubTabAsync</c>,
/// which re-runs the load and re-reads the scope at the re-entered load's own entry. Lite has no replay
/// anywhere (#2933): its guards are bail-only, so a scope re-verify there suppresses a paint that nothing
/// will ever redo and leaves the tab showing the previous scope's data. A generation cannot do that. The
/// only condition under which it drops is that a newer load for the same surface has already started, so
/// the surface always has a live writer, and the writer with the highest generation always paints.</para>
///
/// <para><b>And it orders reads that carry the SAME scope</b>, which a scope re-verify cannot do by
/// construction: after A to B and back to A, the third read's scope is indistinguishable from the first's,
/// so an earlier answer still in flight compares equal and lands last. A generation compares identity, not
/// value, so the first read is stale whether or not its scope came back.</para>
///
/// <para><b>Keyed per surface, not per control.</b> The safety argument is "a newer load for THIS surface
/// is running", so one counter shared across surfaces would let a single-grid refresh discard a whole-tab
/// load's other thirteen paints. Callers key by <c>nameof</c> the loader, which is what keeps the claim and
/// the check from drifting into two spellings of the same question — the failure #2924's pin exists to
/// prevent, arrived at from the other direction.</para>
///
/// <para><b>Not thread-safe, deliberately.</b> Every caller is a WPF event handler or a continuation on the
/// dispatcher, so claims and checks are already serialised on the UI thread; a lock here would buy nothing
/// and would suggest, wrongly, that the counters may be touched from a worker.</para>
/// </summary>
internal sealed class ScopedLoadGenerations
{
    private readonly Dictionary<string, int> _generations = new(StringComparer.Ordinal);

    /// <summary>
    /// Claims the next generation for <paramref name="surface"/> and returns it. Call this at the top of a
    /// loader, BELOW any early return that means no load happens: a claim that does not go on to load is a
    /// claim that discards its predecessor's paint without replacing it.
    /// </summary>
    /// <param name="surface">The painted surface, spelled the same at the claim and at every check —
    /// <c>nameof</c> the loader.</param>
    internal int Claim(string surface)
    {
        ArgumentException.ThrowIfNullOrEmpty(surface);

        var next = (_generations.TryGetValue(surface, out var current) ? current : 0) + 1;
        _generations[surface] = next;
        return next;
    }

    /// <summary>
    /// True when a load newer than <paramref name="generation"/> has claimed <paramref name="surface"/> —
    /// so the caller must return without painting, because that newer load is going to.
    /// </summary>
    /// <remarks>
    /// An unknown surface reads as superseded rather than current. A check against a surface that was never
    /// claimed is a caller whose claim and check are spelled differently, and treating that as "still the
    /// newest" would make the check pass while guarding nothing.
    /// </remarks>
    internal bool Superseded(string surface, int generation)
    {
        ArgumentException.ThrowIfNullOrEmpty(surface);

        return !_generations.TryGetValue(surface, out var current) || current != generation;
    }
}
