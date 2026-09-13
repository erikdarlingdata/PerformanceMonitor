/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Collections.Concurrent;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// In-process memoization of <see cref="QueryStoreFetchProbe"/>'s verdict, so a reference the cache
/// already confirmed resolved-and-current recently never reaches the store's probe round trip at all.
///
/// <para><b>The problem this answers.</b> #3189 measured the touch-and-probe's final <c>SELECT</c> —
/// documented on <see cref="QueryStoreLivenessTouchGuard"/> as running "for every reference regardless
/// of [the touch guard]" — at 52.7% of <c>query_store</c>'s wall time fleet-wide: 30,950,148 references
/// examined in 24 hours to find 437,876 (1.4%) missing. #3216 widened the guard from 1 to 6 hours and
/// closed that issue, but the guard only ever gated the WRITE (the map/dimension <c>last_seen</c>
/// touch) — the verdict SELECT it guards is unconditional, which
/// <see cref="QueryStoreLivenessTouchGuard"/>'s own doc comment states outright. Measured again after
/// #3216 shipped (2026-09-12, ayr-01, two non-overlapping windows): the probe's share of
/// <c>query_store</c> duration is 47-52%, essentially where #3189 left it. #3216 reduced write volume;
/// it never touched the examinations #3189 said were the actual cost.</para>
///
/// <para><b>Why memoization, not a faster probe.</b> #3189's own recommendation: "the recommendation
/// is to stop re-examining references, not to make the probe faster... that rests on an assumption I
/// did not measure: that the same references recur across runs... measure recurrence before designing
/// anything." Measured (2026-09-12, ayr-01, top-150-by-duration plan ids and top-146 query ids across
/// two non-overlapping ~1-hour windows two hours apart): 116/150 (77.3%) plan ids and 117/146 (80.1%)
/// query ids recur. A top-N-by-duration sample is a lower bound on true recurrence — it excludes the
/// high-frequency, low-duration statements that are, if anything, MORE likely to repeat every cycle —
/// so the real hit rate this cache sees in production should meet or beat these figures.</para>
///
/// <para><b>Why this stays correct.</b> A cache entry is trusted for at most
/// <see cref="QueryStoreLivenessTouchGuard.GuardHours"/> — the SAME width the touch guard already
/// treats as an acceptable staleness bound for the same rows, so this does not loosen any liveness
/// promise the store's own design already makes: "a row that stays referenced is re-stamped at least
/// once per guard window" (<see cref="QueryStoreLivenessTouchGuard"/>) becomes, with this cache in
/// front of it, "…at least once per guard window, driven by the cache's own expiry" rather than "on
/// every cycle regardless" — the WINDOW is unchanged, only the cadence of paying for it inside that
/// window. Keying on <c>(serverId, databaseName, id, hash)</c> — hash included — means an in-place
/// plan rewrite (a live hash that no longer matches what was last confirmed) is a cache MISS by
/// construction: it falls straight through to a real probe, which is what detects and reports
/// <c>HashStale</c>. A reference that comes back <c>!Resolved || HashStale</c> is never confirmed here,
/// so it is re-probed every cycle until it resolves — unchanged from today's behavior for exactly the
/// 1.4% this was never meant to skip.</para>
///
/// <para><b>Why no <c>collectorName</c> in the key</b>, unlike the fetch-carryover dictionaries
/// (<c>_planFetchCarryover</c> / <c>_textFetchCarryover</c>) this sits next to. Carryover tracks DEBT —
/// which collector still owes a fetch — and #2902 keyed that per collector because one collector's
/// completed fetch must not silently mark another's debt paid. This cache answers a different
/// question with no collector-scoped analogue: "does the STORE already resolve this id", which is a
/// fact about <c>collect.query_store_plan_map</c> / <c>collect.query_store_text</c> content, true or
/// false independent of who is asking. Scoping it per collector would only lower the hit rate for no
/// correctness gain.</para>
///
/// <para>Deliberately unbounded rather than an LRU: entries are small (a struct key plus a
/// <see cref="DateTime"/>), the referenced-id population is dimension-shaped per
/// <see cref="QueryStorePlanMap"/>'s own sizing note (~175k distinct plans fleet-wide at the time that
/// was measured), and an id that stops being referenced simply stops being refreshed and ages out of
/// USE (expired entries are inert, just unused memory) even though nothing proactively evicts the map
/// entry itself. If fleet growth ever makes that unbounded-but-unused footprint worth reclaiming, the
/// fix is a periodic sweep keyed on the same expiry this class already tracks — not a redesign.</para>
/// </summary>
public sealed class QueryStoreProbeCache
{
    private readonly record struct Key(int ServerId, string DatabaseName, long Id, string? Hash);

    private readonly ConcurrentDictionary<Key, DateTime> _confirmedAtUtc = new();

    /// <summary>
    /// True when this exact (server, database, id, hash) was confirmed resolved-and-current within
    /// <paramref name="ttl"/> of <paramref name="nowUtc"/>. A caller that gets true may treat the
    /// reference as <c>Resolved: true, HashStale: false</c> without a store round trip.
    /// </summary>
    public bool IsFresh(int serverId, string databaseName, long id, string? hash, DateTime nowUtc, TimeSpan ttl)
    {
        if (databaseName is null)
        {
            throw new ArgumentNullException(nameof(databaseName));
        }

        return _confirmedAtUtc.TryGetValue(new Key(serverId, databaseName, id, hash), out var confirmedAtUtc)
            && nowUtc - confirmedAtUtc < ttl;
    }

    /// <summary>
    /// Records that a real probe just confirmed this (server, database, id, hash) resolved and
    /// current. Call ONLY for a verdict with <c>Resolved: true, HashStale: false</c> — anything else
    /// must keep being probed every cycle, which is what leaving it unconfirmed achieves.
    /// </summary>
    public void Confirm(int serverId, string databaseName, long id, string? hash, DateTime nowUtc)
    {
        if (databaseName is null)
        {
            throw new ArgumentNullException(nameof(databaseName));
        }

        _confirmedAtUtc[new Key(serverId, databaseName, id, hash)] = nowUtc;
    }

    /// <summary>
    /// Splits <paramref name="references"/> into what a fresh cache entry already answers for and what
    /// still needs a real probe, given the shared TTL. Every id NOT selected for probing is implicitly
    /// resolved-and-current per the cache — the caller does not need to re-check <see cref="IsFresh"/>
    /// for those. A pure partition: no store I/O, no mutation of the cache.
    /// </summary>
    public List<(long Id, string? Hash)> SelectNeedingProbe(
        int serverId,
        string databaseName,
        IReadOnlyList<(long Id, string? Hash)> references,
        DateTime nowUtc,
        TimeSpan ttl)
    {
        if (references is null)
        {
            throw new ArgumentNullException(nameof(references));
        }

        var needingProbe = new List<(long Id, string? Hash)>(references.Count);
        foreach (var reference in references)
        {
            if (!IsFresh(serverId, databaseName, reference.Id, reference.Hash, nowUtc, ttl))
            {
                needingProbe.Add(reference);
            }
        }

        return needingProbe;
    }
}
