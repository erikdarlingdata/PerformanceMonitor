/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #4999: one detached daily run as the hang watchdog sees it (<see cref="DarlingWorker.WatchDailyRuns"/>): which
/// server and collector it is, when it began executing, and whether the watchdog has warned about it yet. It is
/// created once the run holds its permit, so the clock it carries is the run's own execution time and never the
/// time it spent waiting for a permit, and it ends when the run does.
/// </summary>
internal sealed class DailyRunWatch : IDisposable
{
    private readonly Action<DailyRunWatch> _onEnded;
    private int _warned;
    private int _ended;

    /// <summary>A watch for the run of <paramref name="collectorName"/> on one server, which began at <paramref name="startedUtc"/>.</summary>
    /// <param name="serverId">The store's id for the server, which keys the run's single-flight slot.</param>
    /// <param name="serverName">The server's display name, which is what the warning names.</param>
    /// <param name="collectorName">The collector the run is for.</param>
    /// <param name="startedUtc">When the run began executing.</param>
    /// <param name="onEnded">Called once, when the run ends and the watch is disposed.</param>
    internal DailyRunWatch(int serverId, string serverName, string collectorName, DateTime startedUtc, Action<DailyRunWatch> onEnded)
    {
        ServerId = serverId;
        ServerName = serverName;
        CollectorName = collectorName;
        StartedUtc = startedUtc;
        _onEnded = onEnded;
    }

    /// <summary>The store's id for the server the run is for.</summary>
    internal int ServerId { get; }

    /// <summary>The display name of the server the run is for.</summary>
    internal string ServerName { get; }

    /// <summary>The collector the run is for.</summary>
    internal string CollectorName { get; }

    /// <summary>When the run began executing.</summary>
    internal DateTime StartedUtc { get; }

    /// <summary>Whether the watchdog has already warned about this run. It warns once per run.</summary>
    internal bool Warned => Volatile.Read(ref _warned) != 0;

    /// <summary>Records that the watchdog has warned about this run. Written on the sweep loop and read when the run ends.</summary>
    internal void MarkWarned() => Volatile.Write(ref _warned, 1);

    /// <summary>Ends the watch when the run ends. Safe to call more than once: only the first call counts.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _ended, 1) == 0)
        {
            _onEnded(this);
        }
    }
}
