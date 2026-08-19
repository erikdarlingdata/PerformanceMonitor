/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The STORED contract of the Query Store backfill worker (#2022/#2058) — the collector_state
/// identity and the hole-range codec, shared by both SKUs' workers and both hosts' clamp-site
/// recording so rows written by one build always decode in the next (and a Lite store opened
/// after a Darling-informed fix, or vice versa, can never disagree on what a hole row means).
/// Lives beside <see cref="WatermarkPolicy"/> for the same reason it does: this is watermark-shaped
/// state that must mean the same thing everywhere.
///
/// <para>The worker itself stays per-SKU (each host owns its tick, its connections, and its
/// HORIZON — Darling's is derived from its raw retention tier, Lite's from its resolved
/// query_store retention; deliberately not shared, per the different staging decisions on
/// #2022/#2058). What is shared is only what is PERSISTED.</para>
/// </summary>
public static class QueryStoreBackfillState
{
    /// <summary>The collector_state owner name for the worker's rows — distinct from the
    /// query_store definition on purpose, so the definition keeps declaring NO state keys and the
    /// state-contract pins stay honest.</summary>
    public const string StateCollectorName = "query_store_backfill";

    /// <summary>State key prefix marking a database's first-contact tail as drained (value: when).</summary>
    public const string DoneKeyPrefix = "done:";

    /// <summary>State key prefix for a recorded clamp hole (value: <see cref="EncodeHole"/>).</summary>
    public const string HoleKeyPrefix = "hole:";

    /// <summary>
    /// The widest window a single backfill slice may hand the per-database query (#2102) — matched
    /// to <see cref="WatermarkPolicy.MaxCatchup"/> so NO path, live or backfill, ever windows wider
    /// than the steady state the fleet proves. The backfill query aggregates and sorts its whole
    /// window before the byte budget can bound anything (the same row-cap-is-not-a-cost-cap flaw
    /// that wedged the live path), so an unchunked wide hole on a big database re-times-out forever
    /// instead of draining.
    /// </summary>
    public static readonly TimeSpan MaxSliceSpan = TimeSpan.FromHours(1);

    /// <summary>
    /// How recently the live path may have failed a server's query_store collection before the
    /// backfill worker yields that server's slice (#2111). Two poll cycles: a failure inside the
    /// current or previous cycle means the live path is struggling NOW, and a backfill slice
    /// scanning the same QS internal tables on a MAXDOP-1 replica is exactly the contention that
    /// keeps it struggling. The class doc's contract — "backfill can be slow forever without
    /// delaying collection" — is what this enforces; holes wait, live recovers, backfill resumes.
    /// </summary>
    public static readonly TimeSpan YieldToLiveWindow = TimeSpan.FromMinutes(10);

    /// <summary>
    /// True when the backfill worker should skip a server's slice this tick because its live
    /// query_store collection failed within <see cref="YieldToLiveWindow"/> (#2111). Server-grain
    /// on purpose: the contention is server-wide, and any database's live failure vouches for the
    /// whole replica being contended. A pure function so the placement is pinnable in isolation,
    /// like its siblings above.
    /// </summary>
    public static bool ShouldYieldToLive(DateTime? lastLiveFailureUtc, DateTime nowUtc)
        => lastLiveFailureUtc is DateTime failure && nowUtc - failure < YieldToLiveWindow;

    /// <summary>
    /// The narrowest window the adaptive shrink may reach (#2111 reserve, promoted on field
    /// evidence): a member whose 1h window exceeds the command timeout halves per consecutive
    /// failure toward this floor — 15 minutes fits inside a 60s read on every store the fleet has
    /// shown us, and anything narrower than a flush interval would mostly return empty.
    /// </summary>
    public static readonly TimeSpan MinAdaptiveSpan = TimeSpan.FromMinutes(15);

    /// <summary>
    /// The window a member gets after <paramref name="consecutiveFailures"/> straight failures:
    /// the full span halved per failure, floored at <see cref="MinAdaptiveSpan"/> (the exponent is
    /// capped so the shift math cannot wrap). Success resets the counter at the call sites, so a
    /// recovered member is back at full span on its next cycle. Pure and pinned like its siblings —
    /// the live clamp and the backfill slicing share it, so the two paths cannot drift on how fast
    /// they back off.
    /// </summary>
    public static TimeSpan AdaptiveSpan(TimeSpan fullSpan, int consecutiveFailures)
    {
        if (consecutiveFailures <= 0)
        {
            return fullSpan;
        }

        var halvings = Math.Min(consecutiveFailures, 6);
        var shrunk = TimeSpan.FromTicks(fullSpan.Ticks >> halvings);
        return shrunk < MinAdaptiveSpan ? MinAdaptiveSpan : shrunk;
    }

    /// <summary>
    /// Bounds one newest-first slice to the top <see cref="MaxSliceSpan"/> of the remaining range:
    /// returns the floor the slice should actually query, which is the requested floor once the
    /// remainder is narrow enough. A pure function so the placement is pinnable in isolation, like
    /// <see cref="WatermarkPolicy.ClampCatchup"/>. The caller distinguishes "chunk exhausted"
    /// (result &gt; <paramref name="floorUtc"/>: an empty slice means only this CHUNK is quiet —
    /// shrink the ceiling and keep walking) from "range exhausted" (result ==
    /// <paramref name="floorUtc"/>: an empty slice is terminal, exactly the pre-chunking semantics).
    /// </summary>
    public static DateTime BoundSliceFloor(DateTime floorUtc, DateTime ceilingUtc)
        => BoundSliceFloor(floorUtc, ceilingUtc, MaxSliceSpan);

    /// <summary>The adaptive form (#2111 promoted): the caller passes
    /// <see cref="AdaptiveSpan"/>'s result so a server whose slices keep timing out digs in
    /// progressively narrower chunks until one fits its command timeout.</summary>
    public static DateTime BoundSliceFloor(DateTime floorUtc, DateTime ceilingUtc, TimeSpan span)
    {
        var chunkFloor = ceilingUtc - span;
        return chunkFloor > floorUtc ? chunkFloor : floorUtc;
    }

    /// <summary>Encodes a hole range as <c>from|to</c> in round-trip format — deliberately not
    /// JSON, so the state row stays greppable and the codec dependency-free.</summary>
    public static string EncodeHole(DateTime fromUtc, DateTime toUtc)
        => fromUtc.ToString("o", CultureInfo.InvariantCulture) + "|" + toUtc.ToString("o", CultureInfo.InvariantCulture);

    /// <summary>Decodes <see cref="EncodeHole"/>; false on any malformed value, which the scan
    /// treats as "no hole recorded" — the conservative direction (the tail logic still runs).</summary>
    public static bool TryDecodeHole(string encoded, out DateTime fromUtc, out DateTime toUtc)
    {
        fromUtc = default;
        toUtc = default;

        var split = encoded.Split('|');
        if (split.Length != 2)
        {
            return false;
        }

        return DateTime.TryParseExact(split[0], "o", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out fromUtc)
            && DateTime.TryParseExact(split[1], "o", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out toUtc)
            && fromUtc < toUtc;
    }

    /// <summary>Merges a newly-clamped hole into whatever is already recorded — a repeat outage
    /// WIDENS the range rather than overwriting it, so the earlier hole cannot be lost.</summary>
    public static (DateTime FromUtc, DateTime ToUtc) MergeHole(string? existingEncoded, DateTime fromUtc, DateTime toUtc)
    {
        if (existingEncoded is not null && TryDecodeHole(existingEncoded, out var f, out var t))
        {
            return (fromUtc < f ? fromUtc : f, toUtc > t ? toUtc : t);
        }

        return (fromUtc, toUtc);
    }
}
