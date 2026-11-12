/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #8549: the first-open clock of a fleet tab (Alert History, FinOps). A fleet tab's first open spent time before its own load began: the wait
/// behind another fleet tab's refresh in the shell's fleet lane (<see cref="ViewerRefreshLanes"/>) and, for FinOps, the picker's server read. The
/// load's own clock started inside the loader, so a first open that took five seconds wrote no SlowLoad line when the loader itself took two. This
/// clock starts when the shell makes the tab visible, is handed to the tab's first load, and so counts the whole wait. The server tab does the same
/// with its own clock (<c>ViewerServerTab.BeginFirstOpen</c>).
/// </summary>
internal sealed class ViewerFirstOpen
{
    private ViewerLoadTimer? _timer;
    private bool _taken;

    /// <summary>The slow-load threshold of the first open. A test lowers it to read a line without waiting three seconds.</summary>
    internal int SlowThresholdMs { get; set; } = ViewerLoadTimer.SlowLoadThresholdMs;

    /// <summary>The shell calls this when it makes the tab visible. A tab shown again before its first load started the clock a second time: a tab built
    /// in the background is not slow until the reader first shows it, so the later show restarts the clock.</summary>
    internal void Begin()
    {
        if (!_taken)
        {
            _timer = new ViewerLoadTimer(SlowThresholdMs);
        }
    }

    /// <summary>The shell calls this when the fleet lane starts the tab's refresh: everything on the clock until now was a wait behind another refresh.</summary>
    internal void RefreshStarted() => _timer?.BookPhaseEndingNow("waited for the shell to start this refresh", 0);

    /// <summary>Times a read made for the tab before its own load (the picker's server list) as a store read of the first open. With no clock running it is just the read.</summary>
    internal Task<T> Track<T>(string phase, Task<T> read) => _timer?.Track(phase, read) ?? read;

    /// <summary>The clock for the load now starting, or null when this is not the tab's first load. Handed out once.</summary>
    internal ViewerLoadTimer? Take()
    {
        if (_taken)
        {
            return null;
        }

        _taken = true;
        /* A host that never showed the tab through the shell (a test, a deep link): the clock starts at the first load. */
        var timer = _timer ?? new ViewerLoadTimer(SlowThresholdMs);
        _timer = null;
        timer.Lead = "first open > ";
        return timer;
    }
}
