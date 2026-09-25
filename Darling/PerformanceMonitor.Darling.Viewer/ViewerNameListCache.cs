/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #4234: TTL memoization for the Wait Stats and Perfmon picker's name-list reads (<c>DistinctWaitTypesSql</c>,
/// <c>DistinctPerfmonCountersSql</c>), each a full-window <c>DISTINCT</c> that used to run on every 1-minute
/// auto-refresh (measured 2,079 ms cold for the Perfmon picker over a 7-day window on a production store). A
/// server's set of collected names barely moves inside its own window, so this caches the list per (server,
/// window length) for <see cref="Ttl"/> and lets a refresh inside that window reuse it — window LENGTH, not
/// bounds, because a sliding preset's start/end both move every tick while the length stays constant, and
/// keying on the moving bounds would never hit.
///
/// <para><paramref name="nowUtc"/>-style clock parameters throughout are the seam
/// <see cref="PerformanceMonitor.Darling.Storage.QueryStoreProbeCache"/> already uses: the caller's wall clock
/// by default, an injected time in a test, so a live test can fast-forward past the TTL without a real
/// 15-minute sleep.</para>
/// </summary>
internal sealed class ViewerNameListCache
{
    /// <summary>How long a fetched list is trusted before the next call re-runs the DISTINCT (the #4234 ruling's
    /// number).</summary>
    public static readonly TimeSpan Ttl = TimeSpan.FromMinutes(15);

    private readonly record struct Key(int ServerId, int WindowLengthMinutes);
    private readonly record struct Entry(List<string> Names, DateTime FetchedAtUtc);

    private readonly ConcurrentDictionary<Key, Entry> _entries = new();

    /// <summary>True (with <paramref name="names"/> populated) when a list for this server and window length was
    /// fetched within <see cref="Ttl"/> of <paramref name="nowUtc"/>.</summary>
    public bool TryGet(int serverId, TimeSpan windowLength, DateTime nowUtc, out List<string> names)
    {
        if (_entries.TryGetValue(KeyFor(serverId, windowLength), out var entry) && nowUtc - entry.FetchedAtUtc < Ttl)
        {
            names = entry.Names;
            return true;
        }

        names = new List<string>();
        return false;
    }

    /// <summary>Records a freshly fetched list, stamped <paramref name="nowUtc"/>.</summary>
    public void Set(int serverId, TimeSpan windowLength, List<string> names, DateTime nowUtc) =>
        _entries[KeyFor(serverId, windowLength)] = new Entry(names, nowUtc);

    /// <summary>Rounded to the whole minute: a preset window's length is already a whole number of hours and a
    /// custom range is quarter-hour granular, so rounding only absorbs floating-point noise in the subtraction,
    /// never two genuinely different windows into one key.</summary>
    private static Key KeyFor(int serverId, TimeSpan windowLength) =>
        new(serverId, (int)Math.Round(windowLength.TotalMinutes));
}
