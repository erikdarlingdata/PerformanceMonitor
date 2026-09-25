/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// #4234: TTL memoization for the Wait Stats and Perfmon picker's name-list reads
/// (<see cref="LocalDataService.GetDistinctWaitTypesForPickerAsync"/>,
/// <see cref="LocalDataService.GetDistinctPerfmonCountersForPickerAsync"/>), each a full-window <c>DISTINCT</c>
/// that used to run on every 1-minute auto-refresh. MCP shares the same underlying reads
/// (<see cref="LocalDataService.GetDistinctWaitTypesAsync"/>, <see cref="LocalDataService.GetDistinctPerfmonCountersAsync"/>)
/// but calls them directly, uncached, so an MCP answer is never stale by <see cref="Ttl"/> — only the picker
/// trades that freshness for fewer full-window scans. A server's set of collected names barely moves inside
/// its own window, so this caches the list per (server, window length) for <see cref="Ttl"/> and lets a
/// refresh inside that window reuse it — window LENGTH, not bounds, because a sliding preset's start/end both
/// move every tick while the length stays constant, and keying on the moving bounds would never hit. The
/// Darling twin is <c>ViewerNameListCache</c> (#4234, PR #4304); this port keeps its rules so the two SKUs
/// cache the same way.
///
/// <para><b>The key is (server, length) alone, but a hit ALSO needs the cached window's END to still be
/// recent.</b> Keying on length alone would let a stale entry answer for a window it never fetched: a user
/// viewing this week's 7-day range, then switching to a custom 7-day range from last month, would get THIS
/// WEEK's list under last month's window, because both windows share one length-keyed slot. A hit therefore
/// also requires the cached <see cref="Entry.WindowEndUtc"/> to sit within <see cref="Ttl"/> of the requested
/// end — the same freshness distance the fetch timestamp already checks, applied to the window itself rather
/// than the wall clock, and checked BOTH directions (<c>Duration()</c>) since a custom range can move the end
/// either way, not just forward like a sliding preset's auto-refresh does.</para>
///
/// <para>Meant to be reused beside any other Lite picker that reads a name list over a window the same way
/// (one instance per <see cref="LocalDataService"/>, matching that service's own per-tab lifetime).</para>
/// </summary>
internal sealed class LiteNameListCache
{
    /// <summary>How long a fetched list is trusted before the next call re-runs the DISTINCT (the #4234 ruling's
    /// number).</summary>
    public static readonly TimeSpan Ttl = TimeSpan.FromMinutes(15);

    private readonly record struct Key(int ServerId, int WindowLengthMinutes);
    private readonly record struct Entry(List<string> Names, DateTime WindowEndUtc, DateTime FetchedAtUtc);

    private readonly ConcurrentDictionary<Key, Entry> _entries = new();

    /// <summary>True (with <paramref name="names"/> populated) when a list for this server and window length was
    /// fetched within <see cref="Ttl"/> of <paramref name="nowUtc"/> AND that fetch's window end sits within
    /// <see cref="Ttl"/> of <paramref name="endUtc"/> — two independent freshness checks, because a same-length
    /// window from a different point in time is a different window, not a refresh of this one.</summary>
    public bool TryGet(int serverId, TimeSpan windowLength, DateTime endUtc, DateTime nowUtc, out List<string> names)
    {
        if (_entries.TryGetValue(KeyFor(serverId, windowLength), out var entry)
            && nowUtc - entry.FetchedAtUtc < Ttl
            && (endUtc - entry.WindowEndUtc).Duration() < Ttl)
        {
            names = entry.Names;
            return true;
        }

        names = new List<string>();
        return false;
    }

    /// <summary>Records a freshly fetched list for the window ending <paramref name="endUtc"/>, stamped
    /// <paramref name="nowUtc"/>.</summary>
    public void Set(int serverId, TimeSpan windowLength, DateTime endUtc, List<string> names, DateTime nowUtc) =>
        _entries[KeyFor(serverId, windowLength)] = new Entry(names, endUtc, nowUtc);

    /// <summary>Rounded to the whole minute: a preset window's length is already a whole number of hours and a
    /// custom range is quarter-hour granular, so rounding only absorbs floating-point noise in the subtraction,
    /// never two genuinely different windows into one key.</summary>
    private static Key KeyFor(int serverId, TimeSpan windowLength) =>
        new(serverId, (int)Math.Round(windowLength.TotalMinutes));
}
