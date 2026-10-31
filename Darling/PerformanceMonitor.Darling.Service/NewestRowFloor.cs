/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #6968: where to start looking for a server's newest row, so a read that wants only that row does not open every chunk
/// between "now" and wherever the row happens to be.
///
/// <para><b>The cost this removes.</b> The alert pass reads the newest <c>cpu_utilization_stats</c> and <c>tempdb_stats</c> row of
/// every server every 30 seconds, ordered on the hypertable's time column. For a server with a row in the newest chunk that is
/// one chunk, but TimescaleDB's DeferredChunkAppend plans each chunk it VISITS at run time (about 25 buffers a chunk warm, about
/// 200 on a backend that has not seen the chunk), and a server whose newest row is old, or that has none, visits every chunk
/// between its newest row and now, compressed ones included. A server that was shut down for a fortnight, or whose collector
/// never writes the table, therefore paid that on every tick for as long as the service ran: measured 1,300 buffers a read for
/// a server five days stale and 4,400 for one with no rows, at twenty-one daily chunks.</para>
///
/// <para><b>Why a floor from the row itself, and not a window.</b> A fixed window (<c>collection_time</c> in the last hour)
/// would be cheap and wrong: the engine still evaluates a server whose runtime dropped, from its last stored row, with the
/// sample's age stamped on the alert, so the last row of a server offline for days has to come back, and a window would turn it
/// into "no data", which freezes the gate instead of feeding it. A floor taken from the newest <c>collection_time</c> a
/// previous read returned has no such cost: the newest row can only be at or after that row, so the bounded read returns
/// the row an unbounded one would. It is a value off the column itself, so it carries no clock frame (the reason
/// <c>DarlingWorker.LatestCpuSql</c> refuses a <c>now()</c> bound) and cannot be re-framed by the store session's TimeZone.</para>
///
/// <para><b>A server with no row.</b> Nothing to take a floor from, so the first read is unbounded. When it finds nothing, the
/// floor becomes <see cref="EmptyProbeSlack"/> before that moment: every collector appends rows stamped with the current
/// time (a day-old chunk never takes another write, the premise the compression policy relies on), so a row that
/// appears later is at or after the floor. Each later empty read moves the floor forward, so the bounded range never grows.</para>
///
/// <para><b>When the floor row is gone</b> (retention dropped its chunk, or the server's data was purged), a bounded read finds
/// nothing, and the floor cannot tell that from "no newer row". The caller reads again without a bound, which is today's read, and
/// the floor starts over from whatever that finds. State is in memory only: the first read of a server after a restart is
/// unbounded, which is the read that shipped before this.</para>
/// </summary>
internal sealed class NewestRowFloor
{
    /// <summary>How far before an empty read the floor sits: far longer than any collector takes between stamping a row and
    /// committing it (its command deadlines are seconds), short enough to keep the bounded read to the newest chunk or two.</summary>
    internal static readonly TimeSpan EmptyProbeSlack = TimeSpan.FromHours(1);

    private readonly ConcurrentDictionary<int, (DateTime Since, bool FromRow)> _floors = new();

    /// <summary>The <c>collection_time</c> to bound the server's next read by (naive UTC, Kind Unspecified), or null to read
    /// without a bound.</summary>
    internal DateTime? SinceFor(int serverId)
        => _floors.TryGetValue(serverId, out var floor) ? floor.Since : null;

    /// <summary>
    /// Records what a read found. Returns true when the caller must read again with no bound, because a read bounded by a
    /// row's own time found nothing (the row is gone).
    /// </summary>
    /// <param name="serverId">The server read.</param>
    /// <param name="sinceUsed">The bound the read carried, null for an unbounded read.</param>
    /// <param name="newestFound">The <c>collection_time</c> of the row the read returned, null when it returned none.</param>
    /// <param name="utcNow">The current UTC time, taken by the caller after the read.</param>
    internal bool Settle(int serverId, DateTime? sinceUsed, DateTime? newestFound, DateTime utcNow)
    {
        if (newestFound is { } found)
        {
            _floors[serverId] = (DateTime.SpecifyKind(found, DateTimeKind.Unspecified), true);
            return false;
        }

        if (sinceUsed is not null && _floors.TryGetValue(serverId, out var floor) && floor.FromRow)
        {
            _floors.TryRemove(serverId, out _);
            return true;
        }

        _floors[serverId] = (DateTime.SpecifyKind(utcNow - EmptyProbeSlack, DateTimeKind.Unspecified), false);
        return false;
    }
}
