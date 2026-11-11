/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #8549: the shell's overlap guard for "refresh the tab on screen", in two lanes: the fleet tabs (Overview, Alerts, FinOps and the rest)
/// and the per-server tabs. The shell used to have one slot for all of them. A server tab opened while a fleet tab's load was running
/// (the Overview's first read at startup, or its own timer's refresh) found the slot taken, left a request and returned, and its reads
/// started only when the fleet load finished: 7.9 s of one first open in the field, 0.8 to 1.7 s of three later ones. A per-server tab
/// has its own overlap guard, so it needs the shell's slot only to keep two server tabs from loading at once, as before. The lanes
/// keep the old rule inside each lane: a trigger that arrives mid-load sets a request, and the running loop loads once more. A
/// trigger for one lane never waits on the other.
/// </summary>
internal sealed class ViewerRefreshLanes
{
    /// <summary>One lane's guard. The fields keep the leading underscore so ViewerFleetTimerGuardTests, which finds a single-flight guard by its
    /// shape (tested, claimed and bailed out of before the first await, released in a finally), reads this as the guard it is.</summary>
    private sealed class Lane
    {
        public bool _inFlight;
        public bool _requested;
    }

    private readonly Lane _fleet = new();
    private readonly Lane _server = new();

    /// <summary>
    /// Runs <paramref name="load"/> for the tab on screen unless a load is already running in the same lane, in which case the running
    /// loop is asked to go round again and this returns at once. <paramref name="serverTabSelected"/> is read at the start of every pass:
    /// a pass that finds the other lane's tab on screen does nothing, because that tab's own trigger already started or queued its load
    /// in its own lane.
    /// </summary>
    public async Task RunLaneAsync(Func<bool> serverTabSelected, Func<Task> load)
    {
        var server = serverTabSelected();
        var lane = server ? _server : _fleet;
        if (lane._inFlight)
        {
            lane._requested = true;
            return;
        }

        lane._inFlight = true;
        try
        {
            do
            {
                lane._requested = false;
                if (serverTabSelected() == server)
                {
                    await load();
                }
            }
            while (lane._requested);
        }
        finally
        {
            lane._inFlight = false;
        }
    }
}
