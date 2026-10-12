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
using System.Threading.Tasks;
using PerformanceMonitor.Analysis.Baselines;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The fleet's server clocks, read from the store at most once per <see cref="Lifetime"/> (#4766). The alert-history
/// read stamps every row with its own server's clock, and the shell polls that read on every refresh tick, which is
/// 30 seconds by default and 10 at the fastest. The clock read (<c>ViewerDataService.GetServerClocksAsync</c>) returns
/// one row per server but sorts every retained <c>server_properties</c> row to find each server's newest, because the
/// table is indexed on <c>(server_id, collection_time)</c> and nothing rides the <c>DISTINCT ON</c>. That table changes
/// only when a server connects and once a day after that, so re-reading it on every poll was the same answer over and
/// over. A load cycle that reads through this gets the last read's clocks until they are <see cref="Lifetime"/> old.
///
/// <para>A failed read is not held: the exception reaches the caller and the next call reads again. A snapshot dated
/// after the current time (the clock moved back) counts as stale rather than being served for as long as it takes the
/// clock to catch up. Two calls that both find the snapshot stale each read, which costs one extra query and can
/// return the same answer twice; it is not worth a lock across an await.</para>
/// </summary>
internal sealed class ServerClockCache
{
    private readonly Func<CancellationToken, Task<Dictionary<int, ServerClock>>> _read;
    private readonly Func<DateTime> _utcNow;
    private volatile Snapshot? _held;

    /// <param name="read">The store read that builds the clocks (all servers).</param>
    /// <param name="lifetime">How long a read's answer is served before the store is asked again.</param>
    /// <param name="utcNow">The current UTC time; a test names its own so it never waits on the real clock.</param>
    public ServerClockCache(
        Func<CancellationToken, Task<Dictionary<int, ServerClock>>> read, TimeSpan lifetime, Func<DateTime>? utcNow = null)
    {
        _read = read ?? throw new ArgumentNullException(nameof(read));
        Lifetime = lifetime;
        _utcNow = utcNow ?? (static () => DateTime.UtcNow);
    }

    /// <summary>How long a read's answer is served.</summary>
    public TimeSpan Lifetime { get; }

    /// <summary>The clocks of every server, from the held read while it is fresh, else from a new one.</summary>
    public async Task<IReadOnlyDictionary<int, ServerClock>> GetAsync(CancellationToken cancellationToken)
    {
        /* The snapshot is dated at the START of its read, so it is never presented as younger than it is. */
        var now = _utcNow();
        var held = _held;
        if (held is not null && IsFresh(held.ReadUtc, now, Lifetime))
        {
            return held.Clocks;
        }

        var clocks = await _read(cancellationToken);
        _held = new Snapshot(now, clocks);
        return clocks;
    }

    /// <summary>Whether a read made at <paramref name="readUtc"/> is still served at <paramref name="nowUtc"/>. Pure.</summary>
    internal static bool IsFresh(DateTime readUtc, DateTime nowUtc, TimeSpan lifetime)
        => nowUtc >= readUtc && nowUtc - readUtc < lifetime;

    private sealed record Snapshot(DateTime ReadUtc, IReadOnlyDictionary<int, ServerClock> Clocks);
}
