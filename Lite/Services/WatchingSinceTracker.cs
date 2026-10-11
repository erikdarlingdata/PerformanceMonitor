/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// When Lite's CURRENT connection to each server began collecting (#5642), the value it hands the shared alert
/// engine as <c>AlertServerSnapshot.WatchingSinceUtc</c>. The engine feeds the CPU persistence gate one sample at
/// a time only for samples at or after it; older ones are the history Lite found when it started watching, and a
/// spike that ended before then must not alert.
///
/// <para>Two things set a new value: an app restart (the tracker starts empty, so the first online observation of
/// a server stamps it) and a reconnect (an offline observation forgets the server, so the next online one stamps
/// it again). An online observation of a server already online changes nothing, which is why the connection tick
/// and the alert sweep can both call <see cref="Observe"/> every time.</para>
/// </summary>
public sealed class WatchingSinceTracker
{
    private readonly ConcurrentDictionary<string, DateTime> _since = new(StringComparer.Ordinal);

    /// <summary>Records one connection reading for <paramref name="serverId"/> at <paramref name="nowUtc"/>.</summary>
    public void Observe(string serverId, bool isOnline, DateTime nowUtc)
    {
        if (isOnline)
        {
            _since.TryAdd(serverId, nowUtc);
        }
        else
        {
            _since.TryRemove(serverId, out _);
        }
    }

    /// <summary>The instant the current connection began collecting, or null while the server is not online.</summary>
    public DateTime? Get(string serverId) => _since.TryGetValue(serverId, out var since) ? since : null;

    /// <summary>Drops a removed server so a later add of the same id starts from nothing.</summary>
    public void Forget(string serverId) => _since.TryRemove(serverId, out _);
}
